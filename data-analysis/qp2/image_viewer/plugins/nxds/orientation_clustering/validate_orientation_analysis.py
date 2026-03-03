
import sys
import numpy as np
import pytest
from pathlib import Path


from nxds_orientation_analysis import (
    orthogonalization_matrix,
    axes_to_orientation_matrix,
    misorientation_angle,
    pairwise_misorientation_condensed,
    get_point_group_operators,
    _rot
)

def test_orthogonalization_matrix():
    # Test orthorhombic (easy)
    a, b, c = 10, 20, 30
    alpha, beta, gamma = 90, 90, 90
    B = orthogonalization_matrix(a, b, c, alpha, beta, gamma)
    expected = np.diag([10, 20, 30])
    assert np.allclose(B, expected)

    # Test hexagonal (gamma=120)
    a, b, c = 10, 10, 20
    alpha, beta, gamma = 90, 90, 120
    B = orthogonalization_matrix(a, b, c, alpha, beta, gamma)
    # For gamma=120, cos(120)=-0.5, sin(120)=sqrt(3)/2 ~ 0.866
    # Row 0: [a, b*cos(120), 0] = [10, -5, 0]
    # Row 1: [0, b*sin(120), 0] = [0, 8.66, 0]
    # Row 2: [0, 0, c] = [0, 0, 20]
    expected_row0 = [10, -5.0, 0]
    assert np.allclose(B[0], expected_row0)
    assert np.isclose(B[1, 1], 10 * np.sin(np.radians(120)))
    assert np.isclose(B[2, 2], 20)

def test_axes_to_orientation_matrix_orthorhombic():
    # Orthorhombic cell, aligned with lab axes
    a = np.array([10, 0, 0])
    b = np.array([0, 20, 0])
    c = np.array([0, 0, 30])
    
    U = axes_to_orientation_matrix(a, b, c)
    
    # Should be identity
    assert np.allclose(U, np.eye(3))
    # Det should be 1
    assert np.isclose(np.linalg.det(U), 1.0)
    # Should be orthogonal
    assert np.allclose(U @ U.T, np.eye(3))

def test_axes_to_orientation_matrix_rotated():
    # Rotate crystal by 90 deg around Z
    # Old X becomes Y, old Y becomes -X
    # Orthorhombic cell
    a = np.array([0, 10, 0])   # was x, now y
    b = np.array([-20, 0, 0])  # was y, now -x
    c = np.array([0, 0, 30])   # z stays z
    
    U = axes_to_orientation_matrix(a, b, c)
    
    # Expected rotation: 90 deg around Z
    # [[0, -1, 0], [1, 0, 0], [0, 0, 1]]
    expected = np.array([[0, -1, 0], [1, 0, 0], [0, 0, 1]])
    
    # Note: axes_to_orientation_matrix aligns A matrix columns [a_hat, b_hat, c_hat]? 
    # Wait, the logic is U = A @ B_inv.
    # B = diag(10, 20, 30)
    # A = [[0, -20, 0], [10, 0, 0], [0, 0, 30]]
    # B_inv = diag(0.1, 0.05, 0.033)
    # U = [[0, -1, 0], [1, 0, 0], [0, 0, 1]]
    
    assert np.allclose(U, expected)

def test_misorientation_angle():
    U1 = np.eye(3)
    # 90 deg rotation around Z
    U2 = np.array([[0, -1, 0], [1, 0, 0], [0, 0, 1]])
    
    angle = misorientation_angle(U1, U2)
    assert np.isclose(angle, 90.0)
    
    # Small rotation check
    theta = 5.0 # degrees
    rad = np.radians(theta)
    c, s = np.cos(rad), np.sin(rad)
    U3 = np.array([[c, -s, 0], [s, c, 0], [0, 0, 1]])
    
    angle3 = misorientation_angle(U1, U3)
    assert np.isclose(angle3, 5.0)

def test_symmetry_hexagonal_P6422():
    # SG 181 is P6422 -> Point Group 622
    ops = get_point_group_operators(181)
    
    assert len(ops) == 12
    
    # Check for 6-fold axis along Z (60 deg rotation)
    Rz60 = _rot("z", 60)
    found_z60 = any(np.allclose(op, Rz60) for op in ops)
    assert found_z60, "Missing 6-fold rotation about Z"
    
    # Check for 2-fold axis along X (180 deg)
    Rx180 = _rot("x", 180)
    found_x180 = any(np.allclose(op, Rx180) for op in ops)
    assert found_x180, "Missing 2-fold rotation about X"

def test_symmetry_aware_misorientation():
    # SG 181 (622)
    # U1 = Identity
    # U2 = Rotated by 60 degrees around Z (which is a symmetry op)
    # The symmetry-aware misorientation should be 0 because they are equivalent
    
    U1 = np.eye(3)
    U2 = _rot("z", 60)
    
    U_stack = np.stack([U1, U2])
    sym_ops = get_point_group_operators(181)
    
    # Pairwise condensed: returns [dist(0,1)]
    dist = pairwise_misorientation_condensed(U_stack, sym_ops=sym_ops)
    
    # Should be 0
    assert np.isclose(dist[0], 0.0, atol=1e-5), f"Expected 0 deg, got {dist[0]}"
    
    # Now rotate U2 by 60 + 5 degrees
    U3 = _rot("z", 65)
    U_stack2 = np.stack([U1, U3])
    dist2 = pairwise_misorientation_condensed(U_stack2, sym_ops=sym_ops)
    
    # Should be 5 degrees (distance to nearest symmetry equivalent)
    assert np.isclose(dist2[0], 5.0, atol=1e-5), f"Expected 5 deg, got {dist2[0]}"

if __name__ == "__main__":
    # Manually run tests if pytest not handy
    try:
        test_orthogonalization_matrix()
        print("test_orthogonalization_matrix: PASS")
        test_axes_to_orientation_matrix_orthorhombic()
        print("test_axes_to_orientation_matrix_orthorhombic: PASS")
        test_axes_to_orientation_matrix_rotated()
        print("test_axes_to_orientation_matrix_rotated: PASS")
        test_misorientation_angle()
        print("test_misorientation_angle: PASS")
        test_symmetry_hexagonal_P6422()
        print("test_symmetry_hexagonal_P6422: PASS")
        test_symmetry_aware_misorientation()
        print("test_symmetry_aware_misorientation: PASS")
        print("\nAll validation tests passed!")
    except AssertionError as e:
        print(f"\nTEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)
