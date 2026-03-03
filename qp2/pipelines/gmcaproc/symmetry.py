import re
from collections import OrderedDict
from typing import Optional, Tuple

import numpy as np

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class IdxrefTable:
    # Class attribute containing the crystallographic data
    CRYSTAL_DATA = OrderedDict(
        {
            "aP": [(1, "P1")],
            "mP": [(3, "P2"), (4, "P2(1)")],
            "mI": [(5, "C2")],
            "mC": [(5, "C2")],
            "oP": [
                (16, "P222"),
                (17, "P222(1)"),
                (18, "P2(1)2(1)2"),
                (19, "P2(1)2(1)2(1)"),
            ],
            "oC": [(21, "C222"), (20, "C222(1)")],
            "oF": [(22, "F222")],
            "oI": [(23, "I222"), (24, "I2(1)2(1)2(1)")],
            "tP": [
                (75, "P4"),
                (76, "P4(1)"),
                (77, "P4(2)"),
                (78, "P4(3)"),
                (89, "P422"),
                (90, "P42(1)2"),
                (91, "P4(1)22"),
                (92, "P4(1)2(1)2"),
                (93, "P4(2)22"),
                (94, "P4(2)2(1)2"),
                (95, "P4(3)22"),
                (96, "P4(3)2(1)2"),
            ],
            "tI": [(79, "I4"), (80, "I4(1)"), (97, "I422"), (98, "I4(1)22")],
            "hP": [
                (143, "P3"),
                (144, "P3(1)"),
                (145, "P3(2)"),
                (149, "P312"),
                (150, "P321"),
                (151, "P3(1)12"),
                (152, "P3(1)21"),
                (153, "P3(2)12"),
                (154, "P3(2)21"),
                (168, "P6"),
                (169, "P6(1)"),
                (170, "P6(5)"),
                (171, "P6(2)"),
                (172, "P6(4)"),
                (173, "P6(3)"),
                (177, "P622"),
                (178, "P6(1)22"),
                (179, "P6(5)22"),
                (180, "P6(2)22"),
                (181, "P6(4)22"),
                (182, "P6(3)22"),
            ],
            "hR": [(146, "R3"), (155, "R32")],
            "cP": [
                (195, "P23"),
                (198, "P2(1)3"),
                (207, "P432"),
                (208, "P4(2)32"),
                (212, "P4(3)32"),
                (213, "P4(1)32"),
            ],
            "cF": [(196, "F23"), (209, "F432"), (210, "F4(1)32")],
            "cI": [(197, "I23"), (199, "I2(1)3"), (211, "I432"), (214, "I4(1)32")],
        }
    )

    def __init__(self):
        # Create a reverse lookup dictionary for efficient value-to-number mapping
        self.reverse_lookup = {}
        for key, tuples in self.CRYSTAL_DATA.items():
            for num, val in tuples:
                # Add both original string and string without parentheses to the reverse lookup
                self.reverse_lookup[val] = num
                self.reverse_lookup[self._remove_parentheses(val)] = num
                # if a number is given, return itself
                self.reverse_lookup[str(num)] = num

        for alias, num in {
            "H3": 146,
            "H32": 155,
            # optionally accept spaced variants (normalized anyway)
            "H 3": 146,
            "H 32": 155,
            # optionally accept explicit setting tags seen in practice
            "R3:H": 146,
            "R32:H": 155,
        }.items():
            self.reverse_lookup[self._remove_parentheses(alias)] = num

    def get_lattices(self):
        return self.CRYSTAL_DATA.keys()

    def get_equivalent_space_groups(self, space_group, remove_parentheses=True):
        """given a space group, return all equivalent space groups for use in structure solution (shelx/mr)"""
        point_group = self.get_point_group_number(space_group)
        lattice = self.space_group_to_lattice(space_group)
        if point_group and lattice:
            equivalent_space_groups = []
            for num, symbol in self.CRYSTAL_DATA.get(lattice):
                if self.get_point_group_number(symbol) == point_group:
                    if remove_parentheses:
                        equivalent_space_groups.append(self._remove_parentheses(symbol))
                    else:
                        equivalent_space_groups.append(symbol)
            return equivalent_space_groups

    def space_group_to_lattice(self, spg):
        """
        Given a space group number or symbol, return the lattice type
        """
        # NEW: normalize string symbols via the unified resolver first
        if isinstance(spg, str):
            num = self.symbol_to_number(spg)
            if num is not None:
                spg = num

        for key, tuples in self.CRYSTAL_DATA.items():
            for num, val in tuples:
                if (isinstance(spg, int) and spg == num) or (
                    isinstance(spg, str)
                    and self._remove_parentheses(val) == self._remove_parentheses(spg)
                ):
                    return key
        return None

    def number_to_symbol(self, number, remove_parentheses=True):
        """
        Given a number, return the corresponding value(s) in the tuple.
        """
        for key, tuples in self.CRYSTAL_DATA.items():
            for num, val in tuples:
                if num == int(number):
                    if remove_parentheses:
                        return self._remove_parentheses(val)
                    else:
                        return val
        return None  # Return None if the number is not found

    def symbol_to_number(self, value):
        """
        Given a value (original string or string without parentheses),
        return the corresponding number.
        """

        if value is None:
            return None
        if isinstance(value, int):
            return value
        # Try to convert to int first, in case a number is passed as a string
        try:
            return int(value)
        except (ValueError, TypeError):
            # If that fails, do the symbol lookup
            return self.reverse_lookup.get(str(value).upper().replace(" ", ""), None)

    def get_lowest_spacegroup_number(self, lattice):
        if lattice in self.CRYSTAL_DATA.keys():
            lattices = self.CRYSTAL_DATA.get(lattice)
            if lattices:
                return lattices[0][0]
        logger.error(
            f"failed to get lowest symmetry space group, check input lattice: {lattice}"
        )
        return None

    def get_point_group_number(self, sg):
        # return corresponding point group number given a space group in symbol or number
        hm_symbol = None
        if isinstance(sg, int):
            hm_symbol = self.number_to_symbol(sg, remove_parentheses=False)
        elif isinstance(sg, str):
            num = self.reverse_lookup.get(sg.strip().replace(" ", "").upper(), None)
            if num:
                hm_symbol = self.number_to_symbol(num, remove_parentheses=False)

        if hm_symbol:
            point_group_name = self._remove_screw_axes(hm_symbol)
            return self.symbol_to_number(point_group_name)

        return None

    def get_lowest_point_group_number(self, sg):
        # Return the lowest possible space group number given space group
        if isinstance(sg, int):
            for key, tuples in self.CRYSTAL_DATA.items():
                for n, v in tuples:
                    if n == sg:
                        return tuples[0][0]
        elif isinstance(sg, str):
            sg = self.symbol_to_number(sg.upper().replace(" ", ""))
            return self.get_lowest_point_group_number(sg)

    def same_point_group(self, sg1, sg2):
        return self.get_point_group_number(sg1) == self.get_point_group_number(sg2)

    @staticmethod
    def _remove_screw_axes(symbol):
        """remove screw axes  (within parenthese) from space group symbol"""
        return re.sub(r"\([^()]*\)", "", symbol)

    @staticmethod
    def _remove_parentheses(value):
        """
        Helper method to remove parentheses, spaces, /, - and from a string.
        """
        return str(value).translate(str.maketrans("", "", "()/- ")).upper()

    @staticmethod
    def correct_cell_enforced_by_lattice(lattice_type, unit_cell):
        """Adjust unit cell parameters to conform to crystallographic conventions.

        Args:
            lattice_type: Crystal system code (e.g., 'cP', 'hP')
            unit_cell: List of 6 parameters [a, b, c, α, β, γ]

        Returns:
            List of adjusted unit cell parameters rounded to 1 decimal
        """
        # Unpack cell parameters for clarity
        a, b, c, alpha, beta, gamma = map(float, unit_cell)
        angles = [alpha, beta, gamma]

        def handle_monoclinic(lengths, angles):
            """Handle monoclinic system by preserving one non-90° angle, usually beta"""
            # Find angle furthest from 90° to preserve
            non_right_angle = max(angles, key=lambda x: abs(x - 90))
            return lengths + [
                90 if angle != non_right_angle else angle for angle in angles
            ]

        # Define lattice transformations using dictionary dispatch
        lattice_handlers = {
            ("tP", "tI"): lambda: [np.mean([a, b])] * 2 + [c] + [90] * 3,
            ("oI", "oP", "oF", "oC"): lambda: [a, b, c] + [90] * 3,
            ("cP", "cI", "cF"): lambda: [np.mean([a, b, c])] * 3 + [90] * 3,
            ("mP", "mC", "mI"): lambda: handle_monoclinic([a, b, c], angles),
            ("hP",): lambda: [np.mean([a, b])] * 2 + [c] + [90, 90, 120],
            ("hR",): lambda: [np.mean([a, b, c])] * 3 + [np.mean(angles)] * 3,
            ("aP",): lambda: [a, b, c] + angles,
        }

        # Find matching lattice handler or use identity
        for lattice_group, handler in lattice_handlers.items():
            if lattice_type in lattice_group:
                adjusted = handler()
                break
        else:  # No match found
            adjusted = unit_cell

        return [round(param, 1) for param in adjusted]

    @staticmethod
    def are_unitcell_similar_with_permutation(
        cell1_params: Tuple[float, float, float, float, float, float],
        cell2_params: Tuple[float, float, float, float, float, float],
        tol: float = 0.03,
    ) -> Optional[str]:
        """Compare unit cells under right-hand-preserving permutations."""
        if len(cell1_params) != 6 or len(cell2_params) != 6:
            raise ValueError("Each cell_params tuple must contain exactly 6 elements.")

        params1 = cell1_params
        a2, b2, c2, alpha2, beta2, gamma2 = cell2_params
        permutations = [
            ((a2, b2, c2, alpha2, beta2, gamma2), "h,k,l"),
            ((b2, c2, a2, beta2, gamma2, alpha2), "k,l,h"),
            ((c2, a2, b2, gamma2, alpha2, beta2), "l,h,k"),
        ]

        for perm_params, reindex in permutations:
            if all(abs(p1 - p2) < tol for p1, p2 in zip(params1, perm_params)):
                return reindex
        return None


Symmetry = IdxrefTable()
