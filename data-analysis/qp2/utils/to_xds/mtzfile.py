from __future__ import absolute_import

import logging
import os
import subprocess

from .fileparser import FileParser
from ..tasks.scriptjob import ScriptJob

logger = logging.getLogger(__name__)


xds_ascii = """!FORMAT=XDS_ASCII    MERGE=TRUE    FRIEDEL'S_LAW={flaw}
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

mtz2various = """
mtz2various hklin {mtzin} hklout {hklout} <<eof
LABIN {labin}
OUTPUT USER '(3i6,3f15.1)'
{fsquared}
END
eof
"""


def get_string_between(string, str1, str2):
    return string[string.find(str1)+1:string.rfind(str2)]


class Mtzfile:
    def __init__(self, mtzin):
        self.mtzin = mtzin
        self.wdir, self.mtz_filename = os.path.split(mtzin)
        self.xds_filename = self.mtz_filename.replace(".mtz", ".HKL")
        if not os.path.exists(mtzin):
            raise RuntimeError("mtzin does not exist.")
        self.parser = FileParser(lines=self.mtz_header())

    def mtz_header(self):
        mtzdmp = ['mtzdmp', self.mtzin, '-e']
        header = subprocess.run(mtzdmp, stdout=subprocess.PIPE).stdout.decode('utf-8').splitlines()
        return header

    def convert_to_XDS(self):
        """return output filename"""
        labels = self.get_IFQ_labels()
        if labels:
            x, q, i_or_f = labels
            if 'amplitude' in i_or_f:
                labin = "FP={} SIGFP={}".format(x,q)
                fsquared = "FSQUARED"
            else:
                labin = "I={} SIGI={}".format(x,q)
                fsquared = ""

            tempfile = os.path.join(self.wdir, self.xds_filename)
            cmd = mtz2various.format(labin=labin, fsquared=fsquared, mtzin=self.mtzin, hklout=tempfile)
            ScriptJob(wdir=self.wdir, script_text=cmd, script_name='mtz2various.sh', runner='local').run_sync()

            if os.path.exists(tempfile):
                reflections = open(tempfile).read().rstrip()

                with open(tempfile, "w") as fh:
                    fh.write(xds_ascii.format(flaw="TRUE",
                                              spg_no=self.get_spacegroup()[-1],
                                              cell=self.get_cell(),
                                              reflections=reflections))
                logger.info("converted mtz into XDS format, output {}".format(tempfile))
                return tempfile

        return None

    def get_spacegroup(self):
        lines = self.parser.get_lines_contain("* Space group")
        if lines:
            line = lines[0]

            spg = get_string_between(line, "'", "'").replace(" ", "")
            spgnum = get_string_between(line, "(", ")").replace("number", "").replace(" ", "")
            return spg, spgnum

        return None

    def get_cell(self):
        lines = self.parser.get_blocks_between("* Dataset ID", "* Number of Columns")[0]
        for line in lines:
            fields = line.split()
            if len(fields) == 6:
                return " ".join(fields)
        return None

    def get_column_labels(self):
        labels = None
        lines = self.parser.get_lines_around("* Column Labels :", nLines=1, offset=2)
        if lines:
            labels = lines[0].split()

        return labels

    def get_column_types(self):
        types = None
        lines = self.parser.get_lines_around("* Column Types :", nLines=1, offset=2)
        if lines:
            types = lines[0].split()

        return types

    def get_IFQ_labels(self):
        labels = self.get_column_labels()
        types = self.get_column_types()
        if all(x in types for x in ['J', 'Q']):
            return labels[types.index('J')], labels[types.index('Q')], 'intensity'

        if all(x in types for x in ['F', 'Q']):
            return labels[types.index('F')], labels[types.index('Q')], 'amplitude'

        return None


if __name__ == "__main__":
    mtz = Mtzfile("/mnt/beegfs/qxu/23BM_2017_08_08/4z2x-p21212/repo/gmcaproc/scale-1/196802_1_w0.9788-truncate-TRIMDATA.mtz")
    print(mtz.get_spacegroup())
    print(mtz.get_cell())
    print(mtz.get_IFQ_labels())
    print(mtz.convert_to_XDS())
