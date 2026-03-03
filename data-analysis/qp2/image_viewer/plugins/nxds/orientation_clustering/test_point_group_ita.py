#!/usr/bin/env python3
"""
Validate get_point_group_operators() against International Tables for
Crystallography (ITA) symmetry operations.

Two levels of validation:
1. Group-theory properties: det=+1, orthogonality, closure, identity, inverses
2. Cross-check against ITA reference: parse ITA symop strings, extract proper
   rotations, convert hex/trig fractional→Cartesian, compare operator sets.
"""

import re
import sys

import numpy as np
import pytest

from nxds_orientation_analysis import get_point_group_operators, orthogonalization_matrix


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_symop(s: str):
    """Parse ITA 'X,Y,Z' notation into a 3×3 rotation matrix W and 3-vector t."""
    s = s.strip().upper().replace(' ', '')
    parts = s.split(',')
    if len(parts) != 3:
        return None, None

    W = np.zeros((3, 3))
    t = np.zeros(3)

    for row, part in enumerate(parts):
        if part and part[0] not in '+-':
            part = '+' + part
        pos = 0
        while pos < len(part):
            sign = 1
            if part[pos] == '+':
                sign = 1; pos += 1
            elif part[pos] == '-':
                sign = -1; pos += 1

            fm = re.match(r'(\d+)/(\d+)', part[pos:])
            if fm:
                fv = sign * int(fm.group(1)) / int(fm.group(2))
                pos += fm.end()
                if pos < len(part) and part[pos] in 'XYZ':
                    W[row, {'X': 0, 'Y': 1, 'Z': 2}[part[pos]]] += fv
                    pos += 1
                else:
                    t[row] += fv
                continue

            dm = re.match(r'(\d+)', part[pos:])
            if dm:
                val = int(dm.group(1))
                pos += dm.end()
                if pos < len(part) and part[pos] in 'XYZ':
                    W[row, {'X': 0, 'Y': 1, 'Z': 2}[part[pos]]] += sign * val
                    pos += 1
                else:
                    t[row] += sign * val
                continue

            if pos < len(part) and part[pos] in 'XYZ':
                W[row, {'X': 0, 'Y': 1, 'Z': 2}[part[pos]]] += sign
                pos += 1
                continue

            pos += 1

    return W, t


def get_crystal_system(sg: int) -> str:
    if sg <= 2:   return "TRICLINIC"
    if sg <= 15:  return "MONOCLINIC"
    if sg <= 74:  return "ORTHORHOMBIC"
    if sg <= 142: return "TETRAGONAL"
    if sg <= 167: return "TRIGONAL"
    if sg <= 194: return "HEXAGONAL"
    return "CUBIC"


def unique_proper_rotations(matrices, need_hex_convert=False):
    """Extract unique proper rotations (det=+1) from a list of 3×3 matrices.

    If need_hex_convert, apply B_hex @ W @ B_hex_inv to convert from
    hexagonal fractional coordinates to Cartesian.
    """
    B_hex = orthogonalization_matrix(1, 1, 1, 90, 90, 120)
    B_inv = np.linalg.inv(B_hex)
    result = []
    for W in matrices:
        if need_hex_convert:
            W = B_hex @ W @ B_inv
        if abs(np.linalg.det(W) - 1.0) > 0.01:
            continue
        if not any(np.allclose(W, e, atol=1e-6) for e in result):
            result.append(W)
    return result


# ── ITA Reference Data ───────────────────────────────────────────────────────
# Representative space groups covering every point group.
# Symops are from International Tables Volume A.

ITA_REFERENCE = {
    # Triclinic
    1:   ["X,Y,Z"],
    2:   ["X,Y,Z", "-X,-Y,-Z"],
    # Monoclinic (unique axis b)
    3:   ["X,Y,Z", "-X,Y,-Z"],
    4:   ["X,Y,Z", "-X,Y+1/2,-Z"],
    6:   ["X,Y,Z", "X,-Y,Z"],
    7:   ["X,Y,Z", "X,-Y,1/2+Z"],
    10:  ["X,Y,Z", "X,-Y,Z", "-X,Y,-Z", "-X,-Y,-Z"],
    14:  ["X,Y,Z", "-X,-Y,-Z", "-X,1/2+Y,1/2-Z", "X,1/2-Y,1/2+Z"],
    # Orthorhombic
    16:  ["X,Y,Z", "-X,-Y,Z", "-X,Y,-Z", "X,-Y,-Z"],
    19:  ["X,Y,Z", "1/2-X,-Y,1/2+Z", "-X,1/2+Y,1/2-Z", "1/2+X,1/2-Y,-Z"],
    25:  ["X,Y,Z", "-X,-Y,Z", "X,-Y,Z", "-X,Y,Z"],
    62:  ["X,Y,Z", "-X+1/2,-Y,Z+1/2", "-X,Y+1/2,-Z", "X+1/2,-Y+1/2,-Z+1/2",
          "-X,-Y,-Z", "X+1/2,Y,-Z+1/2", "X,-Y+1/2,Z", "-X+1/2,Y+1/2,Z+1/2"],
    # Tetragonal
    75:  ["X,Y,Z", "-X,-Y,Z", "-Y,X,Z", "Y,-X,Z"],
    81:  ["X,Y,Z", "-X,-Y,Z", "Y,-X,-Z", "-Y,X,-Z"],
    89:  ["X,Y,Z", "-X,-Y,Z", "-Y,X,Z", "Y,-X,Z",
          "-X,Y,-Z", "X,-Y,-Z", "Y,X,-Z", "-Y,-X,-Z"],
    139: ["X,Y,Z", "-X,-Y,Z", "-Y,X,Z", "Y,-X,Z",
          "-X,Y,-Z", "X,-Y,-Z", "Y,X,-Z", "-Y,-X,-Z",
          "-X,-Y,-Z", "X,Y,-Z", "Y,-X,-Z", "-Y,X,-Z",
          "X,-Y,Z", "-X,Y,Z", "-Y,-X,Z", "Y,X,Z"],
    # Trigonal (hexagonal axes → need frac→cart conversion)
    143: ["X,Y,Z", "-Y,X-Y,Z", "Y-X,-X,Z"],
    147: ["X,Y,Z", "-Y,X-Y,Z", "Y-X,-X,Z",
          "-X,-Y,-Z", "Y,Y-X,-Z", "X-Y,X,-Z"],
    150: ["X,Y,Z", "-Y,X-Y,Z", "Y-X,-X,Z",
          "Y,X,-Z", "X-Y,-Y,-Z", "-X,Y-X,-Z"],
    156: ["X,Y,Z", "-Y,X-Y,Z", "Y-X,-X,Z",
          "-Y,-X,Z", "Y-X,Y,Z", "X,X-Y,Z"],
    166: ["X,Y,Z", "-Y,X-Y,Z", "Y-X,-X,Z",
          "Y,X,-Z", "X-Y,-Y,-Z", "-X,Y-X,-Z",
          "-X,-Y,-Z", "Y,Y-X,-Z", "X-Y,X,-Z",
          "-Y,-X,Z", "Y-X,Y,Z", "X,X-Y,Z"],
    # Hexagonal (need frac→cart conversion)
    168: ["X,Y,Z", "-Y,X-Y,Z", "Y-X,-X,Z",
          "-X,-Y,Z", "Y,Y-X,Z", "X-Y,X,Z"],
    174: ["X,Y,Z", "-Y,X-Y,Z", "Y-X,-X,Z",
          "X,Y,-Z", "-Y,X-Y,-Z", "Y-X,-X,-Z"],
    177: ["X,Y,Z", "-Y,X-Y,Z", "Y-X,-X,Z",
          "-X,-Y,Z", "Y,Y-X,Z", "X-Y,X,Z",
          "Y,X,-Z", "X-Y,-Y,-Z", "-X,Y-X,-Z",
          "-Y,-X,-Z", "Y-X,Y,-Z", "X,X-Y,-Z"],
    189: ["X,Y,Z", "-Y,X-Y,Z", "Y-X,-X,Z",
          "X,Y,-Z", "-Y,X-Y,-Z", "Y-X,-X,-Z",
          "Y,X,-Z", "X-Y,-Y,-Z", "-X,Y-X,-Z",
          "Y,X,Z", "X-Y,-Y,Z", "-X,Y-X,Z"],
    194: ["X,Y,Z", "-Y,X-Y,Z", "Y-X,-X,Z",
          "-X,-Y,1/2+Z", "Y,Y-X,1/2+Z", "X-Y,X,1/2+Z",
          "Y,X,-Z", "X-Y,-Y,-Z", "-X,Y-X,-Z",
          "-Y,-X,1/2-Z", "Y-X,Y,1/2-Z", "X,X-Y,1/2-Z",
          "-X,-Y,-Z", "Y,Y-X,-Z", "X-Y,X,-Z",
          "X,Y,1/2-Z", "Y-X,-X,1/2-Z", "-Y,X-Y,1/2-Z",
          "-Y,-X,Z", "Y-X,Y,Z", "X,X-Y,Z",
          "Y,X,1/2+Z", "X-Y,-Y,1/2+Z", "-X,Y-X,1/2+Z"],
    # Cubic
    195: ["X,Y,Z", "-X,-Y,Z", "-X,Y,-Z", "X,-Y,-Z",
          "Z,X,Y", "Z,-X,-Y", "-Z,-X,Y", "-Z,X,-Y",
          "Y,Z,X", "-Y,Z,-X", "Y,-Z,-X", "-Y,-Z,X"],
    207: ["X,Y,Z", "-X,-Y,Z", "-X,Y,-Z", "X,-Y,-Z",
          "Z,X,Y", "Z,-X,-Y", "-Z,-X,Y", "-Z,X,-Y",
          "Y,Z,X", "-Y,Z,-X", "Y,-Z,-X", "-Y,-Z,X",
          "Y,X,-Z", "-Y,-X,-Z", "Y,-X,Z", "-Y,X,Z",
          "X,Z,-Y", "-X,Z,Y", "-X,-Z,-Y", "X,-Z,Y",
          "Z,Y,-X", "Z,-Y,X", "-Z,Y,X", "-Z,-Y,-X"],
    215: ["X,Y,Z", "-X,-Y,Z", "-X,Y,-Z", "X,-Y,-Z",
          "Z,X,Y", "Z,-X,-Y", "-Z,-X,Y", "-Z,X,-Y",
          "Y,Z,X", "-Y,Z,-X", "Y,-Z,-X", "-Y,-Z,X",
          "Y,X,Z", "-Y,-X,Z", "Y,-X,-Z", "-Y,X,-Z",
          "X,Z,Y", "-X,Z,-Y", "-X,-Z,Y", "X,-Z,-Y",
          "Z,Y,X", "Z,-Y,-X", "-Z,Y,-X", "-Z,-Y,X"],
    225: ["X,Y,Z", "-X,-Y,Z", "-X,Y,-Z", "X,-Y,-Z",
          "Z,X,Y", "Z,-X,-Y", "-Z,-X,Y", "-Z,X,-Y",
          "Y,Z,X", "-Y,Z,-X", "Y,-Z,-X", "-Y,-Z,X",
          "Y,X,-Z", "-Y,-X,-Z", "Y,-X,Z", "-Y,X,Z",
          "X,Z,-Y", "-X,Z,Y", "-X,-Z,-Y", "X,-Z,Y",
          "Z,Y,-X", "Z,-Y,X", "-Z,Y,X", "-Z,-Y,-X",
          "-X,-Y,-Z", "X,Y,-Z", "X,-Y,Z", "-X,Y,Z",
          "-Z,-X,-Y", "-Z,X,Y", "Z,X,-Y", "Z,-X,Y",
          "-Y,-Z,-X", "Y,-Z,X", "-Y,Z,X", "Y,Z,-X",
          "-Y,-X,Z", "Y,X,Z", "-Y,X,-Z", "Y,-X,-Z",
          "-X,-Z,Y", "X,-Z,-Y", "X,Z,Y", "-X,Z,-Y",
          "-Z,-Y,X", "-Z,Y,-X", "Z,-Y,-X", "Z,Y,X"],
}

# Expected proper rotation counts per SG (for the group-theory test)
EXPECTED_ORDERS = [
    (1,   1),  (2,   1),  (3,   2),  (4,  2),  (6,  1),  (7,  1),
    (10,  2),  (14,  2),  (16,  4),  (19, 4),  (25, 2),  (36, 2),
    (62,  4),  (75,  4),  (81,  2),  (82, 2),  (89, 8),  (115, 4),
    (139, 8),  (143, 3),  (147, 3),  (148, 3), (150, 6), (156, 3),
    (166, 6),  (168, 6),  (174, 3),  (177, 12), (189, 6), (194, 12),
    (195, 12), (207, 24), (215, 12), (225, 24), (229, 24),
]


# ── Tests ────────────────────────────────────────────────────────────────────

class TestGroupTheoryProperties:
    """Verify that each operator set forms a valid finite rotation group."""

    @pytest.mark.parametrize("sg,expected_n", EXPECTED_ORDERS)
    def test_correct_order(self, sg, expected_n):
        ops = get_point_group_operators(sg)
        assert len(ops) == expected_n, f"SG {sg}: got {len(ops)}, expected {expected_n}"

    @pytest.mark.parametrize("sg,expected_n", EXPECTED_ORDERS)
    def test_proper_rotations(self, sg, expected_n):
        """Every operator must be a proper rotation: det=+1, orthogonal."""
        ops = get_point_group_operators(sg)
        for i, S in enumerate(ops):
            det = np.linalg.det(S)
            assert abs(det - 1.0) < 1e-8, f"SG {sg} op[{i}]: det={det}"
            assert np.allclose(S @ S.T, np.eye(3), atol=1e-8), \
                f"SG {sg} op[{i}]: not orthogonal"

    @pytest.mark.parametrize("sg,expected_n", EXPECTED_ORDERS)
    def test_contains_identity(self, sg, expected_n):
        ops = get_point_group_operators(sg)
        assert any(np.allclose(S, np.eye(3), atol=1e-8) for S in ops), \
            f"SG {sg}: identity not found"

    @pytest.mark.parametrize("sg,expected_n", EXPECTED_ORDERS)
    def test_closure(self, sg, expected_n):
        """Product of any two operators must also be in the set."""
        ops = get_point_group_operators(sg)
        n = len(ops)
        for i in range(n):
            for j in range(n):
                prod = ops[i] @ ops[j]
                found = any(np.allclose(prod, ops[k], atol=1e-6) for k in range(n))
                assert found, f"SG {sg}: ops[{i}] @ ops[{j}] not in set"

    @pytest.mark.parametrize("sg,expected_n", EXPECTED_ORDERS)
    def test_inverses(self, sg, expected_n):
        """Every operator must have its inverse in the set."""
        ops = get_point_group_operators(sg)
        n = len(ops)
        for i in range(n):
            inv_S = ops[i].T  # orthogonal → inverse = transpose
            found = any(np.allclose(inv_S, ops[k], atol=1e-6) for k in range(n))
            assert found, f"SG {sg}: inverse of ops[{i}] not in set"

    @pytest.mark.parametrize("sg,expected_n", EXPECTED_ORDERS)
    def test_no_duplicates(self, sg, expected_n):
        ops = get_point_group_operators(sg)
        n = len(ops)
        for i in range(n):
            for j in range(i + 1, n):
                assert not np.allclose(ops[i], ops[j], atol=1e-8), \
                    f"SG {sg}: ops[{i}] == ops[{j}]"


class TestAgainstITA:
    """Cross-check our operators against ITA reference symmetry operations.

    ITA ops include all space group operations (translations, centering,
    improper rotations). We extract the unique proper rotation parts and
    compare against get_point_group_operators().

    For trigonal/hexagonal systems, ITA ops are in fractional (hexagonal)
    coordinates and must be converted to Cartesian via B @ W @ B⁻¹.
    """

    @pytest.mark.parametrize("sg", sorted(ITA_REFERENCE.keys()))
    def test_matches_ita(self, sg):
        system = get_crystal_system(sg)
        ops_str = ITA_REFERENCE[sg]
        need_convert = system in ("TRIGONAL", "HEXAGONAL")

        # Parse ITA symops
        ref_matrices = []
        for s in ops_str:
            W, _ = parse_symop(s)
            assert W is not None, f"Failed to parse: {s}"
            ref_matrices.append(W)

        # Extract unique proper rotations (with hex→cart conversion if needed)
        ref_proper = unique_proper_rotations(ref_matrices, need_hex_convert=need_convert)

        # Get our operators
        our_ops = get_point_group_operators(sg)

        # Compare counts
        assert len(ref_proper) == len(our_ops), \
            f"SG {sg}: ITA proper={len(ref_proper)}, ours={len(our_ops)}"

        # Check every ITA proper rotation is in our set
        for i, W_ref in enumerate(ref_proper):
            found = any(np.allclose(W_ref, our_ops[j], atol=1e-4)
                        for j in range(len(our_ops)))
            assert found, (
                f"SG {sg}: ITA proper rotation [{i}] not found in our operators\n"
                f"  W = {W_ref.tolist()}"
            )

        # Check every our operator is in the ITA set
        for j in range(len(our_ops)):
            found = any(np.allclose(our_ops[j], W_ref, atol=1e-4)
                        for W_ref in ref_proper)
            assert found, (
                f"SG {sg}: our operator [{j}] not in ITA proper rotations\n"
                f"  W = {our_ops[j].tolist()}"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
