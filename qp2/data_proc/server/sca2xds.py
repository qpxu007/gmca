# qp2/data_proc/server/sca2xds.py
import os
from qp2.log.logging_config import get_logger

# Assuming Spacegroup is available here. If not, you can implement a simple lookup.
try:
    from qp2.pipelines.gmcaproc.spacegroup import Spacegroup
except ImportError:
    # Minimal fallback if module missing
    class Spacegroup:
        @staticmethod
        def get_laue_number_from_symbol(sym): return 0

logger = get_logger(__name__)

class SCA2XDS:
    def __init__(self, sca_file):
        self.sca_file = os.path.abspath(sca_file)
        self.wdir = os.path.dirname(self.sca_file)
        self.outfile_lines = []
        self.unit_cell = None
        self.space_group = None
        self.anomalous = False
        self._read()

    def convert(self, dest=None):
        if not self.outfile_lines:
            logger.error(f"No data read from {self.sca_file}, cannot convert.")
            return None

        if not dest:
            dest = os.path.splitext(self.sca_file)[0] + ".HKL"

        try:
            header = self._get_header()
            with open(dest, "w") as f:
                f.write(header)
                for data in self.outfile_lines:
                    # XDS_ASCII format: H K L Iobs Sigma
                    f.write("{:5d}{:5d}{:5d}{:8.1f}{:8.1f}\n".format(*data))
                f.write("!END_OF_DATA\n")
            
            logger.info(f"Converted {self.sca_file} -> {dest}")
            return dest
        except Exception as e:
            logger.error(f"Error writing XDS file: {e}", exc_info=True)
            return None

    def _get_header(self):
        flaw = "FALSE" if self.anomalous else "TRUE"
        spg_no = 0
        if self.space_group:
            try:
                spg_no = Spacegroup.get_laue_number_from_symbol(self.space_group)
            except: pass
        
        # Default cell if missing
        cell = self.unit_cell if self.unit_cell else "100 100 100 90 90 90"

        return (
            f"!FORMAT=XDS_ASCII    MERGE=TRUE    FRIEDEL'S_LAW={flaw}\n"
            f"!SPACE_GROUP_NUMBER=   {spg_no}\n"
            f"!UNIT_CELL_CONSTANTS=    {cell}\n"
            "!NUMBER_OF_ITEMS_IN_EACH_DATA_RECORD=5\n"
            "!ITEM_H=1\n"
            "!ITEM_K=2\n"
            "!ITEM_L=3\n"
            "!ITEM_IOBS=4\n"
            "!ITEM_SIGMA(IOBS)=5\n"
            "!END_OF_HEADER\n"
        )

    def _read(self):
        if not os.path.exists(self.sca_file):
            logger.warning(f"File does not exist: {self.sca_file}")
            return

        try:
            with open(self.sca_file) as f:
                # Skip comments (lines starting with !)
                lines = [l for l in f.readlines() if not l.strip().startswith('!')]

            if len(lines) < 3:
                logger.warning("Not enough lines in SCA file.")
                return

            # Header line logic varies by scalepack version, assume standard line 3 (index 2)
            # Typically: cell(6) spacegroup(remainder)
            header_parts = lines[2].split()
            if len(header_parts) >= 6:
                self.unit_cell = ' '.join(header_parts[:6])
                # Heuristic cleanup for space group string
                self.space_group = ''.join(header_parts[6:]).replace("1", "")

            # Parse data lines
            for line in lines[3:]:
                flds = line.split()
                if not flds: continue
                
                try:
                    # Standard Scalepack: h k l I sigI
                    if len(flds) >= 5: 
                        h, k, l = int(flds[0]), int(flds[1]), int(flds[2])
                        
                        if len(flds) == 5: # Non-anomalous
                            i, sig = float(flds[3]), float(flds[4])
                            self.outfile_lines.append([h, k, l, i, sig])
                        elif len(flds) >= 7: # Anomalous: h k l I+ sig+ I- sig-
                            self.anomalous = True
                            ip, sigp = float(flds[3]), float(flds[4])
                            im, sigm = float(flds[5]), float(flds[6])
                            
                            # Write I+
                            if ip != 0.0 or sigp > 0:
                                self.outfile_lines.append([h, k, l, ip, sigp])
                            # Write I- (inverted indices)
                            if im != 0.0 or sigm > 0:
                                self.outfile_lines.append([-h, -k, -l, im, sigm])
                except ValueError:
                    continue # Skip malformed lines

        except Exception as e:
            logger.error(f"Error reading SCA file: {e}", exc_info=True)
