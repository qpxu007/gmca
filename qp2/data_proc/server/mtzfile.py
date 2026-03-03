# qp2/data_proc/server/mtzfile.py
from qp2.log.logging_config import get_logger
import os
import subprocess

logger = get_logger(__name__)

XDS_ASCII_TEMPLATE = """!FORMAT=XDS_ASCII    MERGE=TRUE    FRIEDEL'S_LAW={flaw}
!SPACE_GROUP_NUMBER=   {spg_no}
!UNIT_CELL_CONSTANTS=    {cell}
!NUMBER_OF_ITEMS_IN_EACH_DATA_RECORD=5
!ITEM_H=1
!ITEM_K=2
!ITEM_L=3
!ITEM_IOBS=4
!ITEM_SIGMA(IOBS)=5
!END_OF_HEADER
{reflections}
!END_OF_DATA
"""

class Mtzfile:
    def __init__(self, mtzin):
        self.mtzin = os.path.abspath(mtzin)
        self.wdir = os.path.dirname(self.mtzin)
        if not os.path.exists(self.mtzin):
            raise FileNotFoundError(f"MTZ file not found: {mtzin}")
        
        self.header_info = self._parse_mtzdmp()

    def _parse_mtzdmp(self):
        """Run mtzdmp and parse key metadata."""
        info = {'spacegroup': '', 'spg_num': '0', 'cell': '', 'labels': [], 'types': []}
        try:
            res = subprocess.run(['mtzdmp', self.mtzin, '-e'], capture_output=True, text=True, timeout=10)
            lines = res.stdout.splitlines()
            
            for i, line in enumerate(lines):
                if "* Space group" in line:
                    parts = line.split("'")
                    if len(parts) > 1: info['spacegroup'] = parts[1].replace(" ", "")
                    if ")" in line:
                        info['spg_num'] = line.split(")")[-2].split("(")[-1].strip()
                
                if "* Dataset ID" in line:
                    # Cell usually follows dataset block
                    try:
                        cell_line = lines[i+2].strip() # Heuristic skip
                        # Check if it looks like 6 numbers
                        if len(cell_line.split()) == 6:
                            info['cell'] = cell_line
                    except: pass

                if "* Column Labels" in line:
                     try: info['labels'] = lines[i+2].strip().split()
                     except: pass
                
                if "* Column Types" in line:
                     try: info['types'] = lines[i+2].strip().split()
                     except: pass
                     
        except Exception as e:
            logger.error(f"mtzdmp failed: {e}", exc_info=True)
        
        return info

    def get_spacegroup(self):
        return self.header_info['spacegroup'], self.header_info['spg_num']

    def get_cell(self):
        return self.header_info['cell']

    def convert_to_XDS(self):
        """Convert MTZ to XDS_ASCII using mtz2various."""
        labels, types = self.header_info['labels'], self.header_info['types']
        if not labels or not types:
            logger.error("Could not parse column info from MTZ.")
            return None

        # Heuristic to find I/SigI or F/SigF
        labin = ""
        fsquared = ""
        
        # Prioritize Intensities
        if 'J' in types and 'Q' in types:
            i_idx = types.index('J')
            q_idx = types.index('Q')
            labin = f"I={labels[i_idx]} SIGI={labels[q_idx]}"
        # Fallback to Amplitudes
        elif 'F' in types and 'Q' in types:
            f_idx = types.index('F')
            q_idx = types.index('Q')
            labin = f"FP={labels[f_idx]} SIGFP={labels[q_idx]}"
            fsquared = "FSQUARED"
        
        if not labin:
            logger.error("Could not find suitable I/SigI or F/SigF columns.")
            return None

        hklout = self.mtzin.replace(".mtz", ".HKL")
        
        cmd = f"""mtz2various hklin {self.mtzin} hklout {hklout} <<eof
LABIN {labin}
OUTPUT USER '(3i6,3f15.1)'
{fsquared}
END
eof
"""
        try:
            subprocess.run(cmd, shell=True, cwd=self.wdir, check=True)
            
            # Post-process to add XDS Header
            if os.path.exists(hklout):
                with open(hklout, 'r') as f: content = f.read()
                
                header = XDS_ASCII_TEMPLATE.format(
                    flaw="TRUE",
                    spg_no=self.header_info['spg_num'],
                    cell=self.header_info['cell'],
                    reflections=content
                )
                
                with open(hklout, 'w') as f: f.write(header)
                return hklout
                
        except subprocess.CalledProcessError as e:
            logger.error(f"mtz2various failed: {e}", exc_info=True)
        
        return None
