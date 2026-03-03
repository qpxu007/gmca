from __future__ import absolute_import

import logging
import os

from qp2.pipelines.gmcaproc.symmetry import Symmetry
from qp2.utils.auxillary import getNumbers

logger = logging.getLogger(__name__)


class XdsAsciiParser(object):
    def __init__(self, wdir=".", filename='XDS_ASCII.HKL'):
        self.wdir = wdir
        self.filename = filename

    def get_spg_cell(self, spg_type='number'):
        # parse XDS_ASCII.HKL to get space group (in symbol--str) and cell (list)
        # return two strings if there are results, otherwise, (None, None)
        spg_num = cell = None
        if os.path.isfile(self.filename):
            with open(self.filename) as hkl:
                for line in hkl:
                    if line.startswith("!SPACE_GROUP_NUMBER="):
                        spg_num = getNumbers(line)[0]
                    if line.startswith("!UNIT_CELL_CONSTANTS="):
                        cell = getNumbers(line)
                    if line.startswith("!END_OF_HEADER"):
                        break

        if spg_type == 'number':
            return spg_num, cell
        else:
            spg_symbol = Symmetry.number_to_symbol(spg_num)
            return spg_symbol, cell