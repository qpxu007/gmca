from __future__ import absolute_import

import logging
import os

from qp2.pipelines.gmcaproc.symmetry import Symmetry

logger = logging.getLogger(__name__)


class SCA2XDS(object):
    def __init__(self, sca_file='./scalepack.sca'):
        self.wdir, self.sca_file = os.path.split(sca_file)
        self.sca_file = sca_file
        self.outfile = None
        self.data = None
        self.unit_cell = None
        self.space_group = None
        self.anomalous = False
        self.outfile_lines = []
        self.__read()

    def set_sca_dir(self, wdir):
        self.wdir = wdir

    def set_sca_filename(self, fname):
        self.sca_file = fname

    def convert(self, dest=None):
        if not dest:
            dest = os.path.join(self.wdir, "XDS_ASCII.HKL")

        with open(dest, "w") as f:
            f.write(self.__get_header())
            for data in self.outfile_lines:
                line = "{:5d}{:5d}{:5d}{:8.1f}{:8.1f}\n".format(*data)
                f.write(line)
            f.write("!END_OF_DATA")
        logger.info("scalepack {} in {} converted XDS format, file saved to: {}".format(self.sca_file, self.wdir, dest))
        return dest

    def __get_header(self):
        if self.anomalous:
            flaw = "FALSE"
        else:
            flaw = "TRUE"

        spg_no = Symmetry.symbol_to_number(self.space_group)
        logger.info(f"space group: {self.space_group} no: {spg_no}")

        header = """!FORMAT=XDS_ASCII    MERGE=TRUE    FRIEDEL'S_LAW={flaw}
!SPACE_GROUP_NUMBER=   {spg_no}
!UNIT_CELL_CONSTANTS=    {cell}
!NUMBER_OF_ITEMS_IN_EACH_DATA_RECORD=5
!ITEM_H=1
!ITEM_K=2
!ITEM_L=3
!ITEM_IOBS=4
!ITEM_SIGMA(IOBS)=5
!END_OF_HEADER
""".format(flaw=flaw, spg_no=spg_no, cell=self.unit_cell)
        return header

    def __read(self):
        infile = os.path.join(self.wdir, self.sca_file)
        if not os.path.exists(infile):
            logger.warning("file does not exist: {}".format(infile))
            return

        with open(infile) as f:
            lines = f.readlines()

        if len(lines) < 10:
            logger.warning("no enough reflection read: {}".format(infile))
            return

        cell_spg = lines[2].split()
        try:
            self.unit_cell = ' '.join(cell_spg[:6])
            self.space_group = ''.join(cell_spg[6:]).replace("1", "")
        except:
            logger.warning("unable to get cell and space group: {}".format(infile))
            return

        real_data = lines[3:]

        for refl in real_data:
            flds = refl.split()
            if len(flds) == 5:
                h = flds[0]
                k = flds[1]
                l = flds[2]
                i = flds[3]
                sig = flds[4]
                self.outfile_lines.append([int(h),int(k),int(l), float(i), float(sig)])
            elif len(flds) == 7:
                h = flds[0]
                k = flds[1]
                l = flds[2]
                ip = flds[3]
                sigp = flds[4]
                im = flds[5]
                sigm = flds[6]
                self.anomalous = True

                if ip != '0.0' and sigp != '-1.0':
                    self.outfile_lines.append([int(h),int(k),int(l), float(ip), float(sigp)])

                if im != '0.0' and sigm != '-1.0':
                    self.outfile_lines.append([-int(h),-int(k),-int(l), float(im), float(sigm)])


if __name__ == "__main__":
    cnv = SCA2XDS(sca_file="/mnt/beegfs/qxu/23BM_2017_08_08/rlefkowitz/_kamoproc/merge_180410-082451/blend_3.1A_framecc_b+B/cluster_0064/manual-run1", sca_file='xscale.sca')
    cnv.convert(dest="/mnt/beegfs/qxu/xx.XDS")