#!/usr/bin/env python3
"""
nXDS Crystal Orientation Analysis
==================================

Parses XPARM.nXDS or GXPARM.nXDS files to extract per-image crystal
orientations and analyze whether crystals show preferred orientation
or are randomly oriented. This is useful for serial crystallography
experiments where many crystals are indexed independently.

Input Format
------------
The body of XPARM.nXDS contains 9 lines per image:
  1. Image filename
  2. Wavelength + incident beam wavevector (lab)
  3. Unit cell a-axis (lab coords, Å)
  4. Unit cell b-axis (lab coords, Å)
  5. Unit cell c-axis (lab coords, Å)
  6. Detector origin (pixels) + signed distance (mm)
  7. Detector X-axis (lab coords)
  8. Detector Y-axis (lab coords)
  9. Detector Z-axis (lab coords)

Methods
-------
Two methods are available for computing pairwise misorientation angles:

**Fast method (default)**: Vectorized einsum on proper rotation matrices.
  Each crystal's raw basis vectors A = [a|b|c] are decomposed as A = U·B,
  where B is the orthogonalization matrix (a along x, b in xy-plane) and U
  is the proper rotation matrix (orientation). The misorientation angle
  between two crystals is:

      θ = arccos( (tr(Uᵢᵀ · S · Uⱼ) - 1) / 2 )

  minimized over all point group symmetry operators S. This is computed
  for all N(N-1)/2 pairs simultaneously using NumPy einsum, making it
  orders of magnitude faster than loop-based approaches.

  The trace formula for rotation angle follows from Euler's rotation
  theorem; the symmetry-reduced misorientation is the standard approach
  in crystallographic texture analysis.

**Kabsch method (--slow)**: Aligns raw basis vectors using Kabsch SVD
  alignment (scipy.spatial.transform.Rotation.align_vectors) for each
  pair, then extracts the rotation angle. This involves nested Python
  loops over all pairs and symmetry operators, making it very slow for
  large datasets (>1000 images).

Symmetry
--------
Point group symmetry operators are generated from the space group number
using get_point_group_operators(). Only proper rotations (det=+1) are
used. Operators are defined in the crystal Cartesian frame (a along x,
b in xy-plane, same convention as the orthogonalization matrix B).

For trigonal/hexagonal systems, the operators are computed directly in
Cartesian coordinates, equivalent to B · S_frac · B⁻¹ where S_frac is
the fractional coordinate operator from International Tables Vol. A.

Output Plots
------------
The analysis generates a 2×3 panel figure:

  1. **Misorientation Histogram** (top-left): Distribution of pairwise
     misorientation angles. A random orientation distribution follows the
     Mackenzie distribution — the red dashed curve shows either the
     analytical P(θ) ∝ (1-cosθ) for triclinic or a Monte Carlo estimate
     for higher symmetry. Preferred orientation appears as a peak at low
     angles; random orientation matches the red curve.

  2. **c-axis Pole Figure** (top-center): Stereographic projection of
     crystal c-axis directions onto the XY-plane (beam along Z). Clustered
     points indicate preferred orientation of the c-axis; uniform scatter
     over the circle indicates random orientation.

  3. **a-axis Pole Figure** (top-right): Same as c-axis but for the a-axis.
     Together with the c-axis plot, these reveal the full texture: e.g.,
     a fiber texture shows c-axis clustering with a-axis forming a ring.

  4. **Euler Angles** (bottom-left): Scatter plot of (φ₁, Φ) from ZYZ
     Euler angle decomposition. Clusters indicate preferred orientations.
     For random orientations, φ₁ is uniform [0°, 360°] and Φ follows
     a sin(Φ) distribution.

  5. **Unit Cell Parameters** (bottom-center): Box plots of refined cell
     edge lengths (a, b, c) across all images. Tight distributions confirm
     consistent indexing; outliers may indicate misindexing.

  6. **Summary** (bottom-right): Text panel with statistics including
     number of crystals, distinct orientations (clustering), misorientation
     statistics, resultant lengths (1.0 = perfectly aligned, ~0 = random),
     and an overall assessment.

References
----------
Kabsch, W. (1976). A solution for the best rotation to relate two sets
  of vectors. Acta Cryst. A32, 922-923. doi:10.1107/S0567739476001873

Kabsch, W. (1978). A discussion of the solution for the best rotation to
  relate two sets of vectors. Acta Cryst. A34, 827-828.
  doi:10.1107/S0567739478001680

Mackenzie, J.K. (1958). Second paper on statistics associated with the
  random disorientation of cubes. Biometrika 45(1-2), 229-240.
  doi:10.1093/biomet/45.1-2.229

Morawiec, A. (2004). Orientations and Rotations: Computations in
  Crystallographic Textures. Springer-Verlag, Berlin.
  doi:10.1007/978-3-662-09156-2

Usage
-----
    python nxds_orientation_analysis.py /path/to/nxds_run_directory
    python nxds_orientation_analysis.py /path/to/XPARM.nXDS
    python nxds_orientation_analysis.py /path/to/GXPARM.nXDS --slow
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import numpy as np


# ======================================================================
# Point group symmetry operators (proper rotations only)
# ======================================================================

def _rot(axis, angle_deg):
    """Rotation matrix for given axis ('x','y','z') and angle in degrees."""
    c = np.cos(np.radians(angle_deg))
    s = np.sin(np.radians(angle_deg))
    c = round(c, 10)  # avoid floating point noise for 90/120/180
    s = round(s, 10)
    if axis == "x":
        return np.array([[1, 0, 0], [0, c, -s], [0, s, c]], dtype=np.float64)
    elif axis == "y":
        return np.array([[c, 0, s], [0, 1, 0], [-s, 0, c]], dtype=np.float64)
    elif axis == "z":
        return np.array([[c, -s, 0], [s, c, 0], [0, 0, 1]], dtype=np.float64)


def _unique_matrices(mats):
    """De-duplicate rotation matrices."""
    unique = [mats[0]]
    for m in mats[1:]:
        if not any(np.allclose(m, u, atol=1e-8) for u in unique):
            unique.append(m)
    return unique


def get_point_group_operators(space_group: int) -> np.ndarray:
    """
    Return proper rotation operators (n, 3, 3) for the point group
    corresponding to the given space group number.
    
    Returns the PROPER rotation subgroup of the space group's point group.
    """
    I = np.eye(3, dtype=np.float64)

    # --- Helper Generatros ---
    def make_cyclic(axis, order):
        ops = [I]
        for i in range(1, order):
            ops.append(_rot(axis, 360.0 / order * i))
        return ops

    def make_dihedral(axis_principal, order, axis_secondary):
        ops = make_cyclic(axis_principal, order)
        # Add 180 rotations perpendicular to principal
        # One along axis_secondary, others generated by principal symmetry
        # Basically: Secondary_180, and (Principal_rot @ Secondary_180)
        
        # Base seconary flip
        flip = _rot(axis_secondary, 180)
        
        new_ops = []
        for op in ops:
            new_ops.append(op @ flip)
            
        ops.extend(new_ops)
        return _unique_matrices(ops)

    def make_cubic_23():
        # T group: 12 elements
        # Identity
        ops = [I]
        # 3-fold about <111>
        # 4 axes: (1,1,1), (-1,1,1), (1,-1,1), (1,1,-1)
        for x, y, z in [(1,1,1), (-1,1,1), (1,-1,1), (1,1,-1)]:
            axis = np.array([x, y, z], dtype=np.float64)
            axis = axis / np.linalg.norm(axis)
            # Rodrigues rotation for 120 and 240
            K = np.array([[0, -axis[2], axis[1]], [axis[2], 0, -axis[0]], [-axis[1], axis[0], 0]])
            # 120 deg
            c, s = -0.5, np.sqrt(3)/2
            ops.append(c * I + s * K + (1 - c) * np.outer(axis, axis))
            # 240 deg
            c, s = -0.5, -np.sqrt(3)/2
            ops.append(c * I + s * K + (1 - c) * np.outer(axis, axis))
            
        # 2-fold about <100> (x, y, z)
        ops.append(_rot("x", 180))
        ops.append(_rot("y", 180))
        ops.append(_rot("z", 180))
        
        return _unique_matrices(ops)

    def make_cubic_432():
        # O group: 24 elements (T group + 4-folds & 2-folds)
        ops = make_cubic_23()
        # Add 90/270 about x, y, z
        for axis in ["x", "y", "z"]:
            ops.append(_rot(axis, 90))
            ops.append(_rot(axis, 270))
        
        # Add 2-fold about <110> (6 axes)
        # Axes: (1,1,0), (1,-1,0), (1,0,1), (1,0,-1), (0,1,1), (0,1,-1)
        # 180 deg rot: 2(n.n^T) - I
        for x,y,z in [(1,1,0), (1,-1,0), (1,0,1), (1,0,-1), (0,1,1), (0,1,-1)]:
            v = np.array([x,y,z], dtype=np.float64)
            v = v / np.linalg.norm(v)
            ops.append(2 * np.outer(v, v) - I)
            
        return _unique_matrices(ops)

    # --- Select Point Group ---
    ops = [I]
    
    # TRICLINIC
    if space_group <= 2:
        # 1, -1 -> 1
        ops = [I]
        
    # MONOCLINIC (Standard setting: unique axis b)
    elif space_group <= 15:
        # 3-5 (2): 2
        # 6-9 (m): 1 (m proper = 1) -> Actually m = i*2. Proper part is 1!
        # 10-15 (2/m): 2
        if 6 <= space_group <= 9: # Pm, Pc, Cm, Cc
            ops = [I]
        else:
            ops = make_cyclic("y", 2)

    # ORTHORHOMBIC
    elif space_group <= 74:
        # 16-24 (222): 222
        # 25-46 (mm2): 2 (Standard setting c-unique? mm2 usually z-axis 2-fold)
        # 47-74 (mmm): 222
        if 25 <= space_group <= 46:
            ops = make_cyclic("z", 2)
        else:
            ops = [I, _rot("x", 180), _rot("y", 180), _rot("z", 180)]

    # TETRAGONAL
    elif space_group <= 142:
        # 75-80 (4): 4
        # 81-82 (-4): 2 (z)
        # 83-88 (4/m): 4
        # 89-98 (422): 422
        # 99-110 (4mm): 4
        # 111-122 (-42m): 222 (D2). x,y,z 2-folds.
        # 123-142 (4/mmm): 422
        
        is_422 = (89 <= space_group <= 98) or (123 <= space_group <= 142)
        is_222 = (111 <= space_group <= 122)
        is_2 = (81 <= space_group <= 82)
        
        if is_422:
            ops = make_dihedral("z", 4, "x")
        elif is_222:
             ops = [I, _rot("x", 180), _rot("y", 180), _rot("z", 180)]
        elif is_2:
             ops = make_cyclic("z", 2)
        else:
             # P4, P4/m, 4mm -> 4
             ops = make_cyclic("z", 4)

    # TRIGONAL
    elif space_group <= 167:
        # 143-146 (3): 3
        # 147-148 (-3): 3
        # 149-155 (32): 32
        # 156-161 (3m): 3
        # 162-167 (-3m): 32
        
        # Groups with 32 sym: 149-155, 162-167
        # Others 3
        is_32 = (149 <= space_group <= 155) or (162 <= space_group <= 167)
        if is_32:
            ops = make_dihedral("z", 3, "x") # 2-fold along x (a)
        else:
            ops = make_cyclic("z", 3)

    # HEXAGONAL
    elif space_group <= 194:
        # 168-173 (6): 6
        # 174 (-6): 3 (C3h proper = C3)
        # 175-176 (6/m): 6
        # 177-186 (622): 622
        # 187-190 (-6m2): 32 (D3h proper = D3)
        # 191-194 (6/mmm): 622
        
        if space_group == 174:
            ops = make_cyclic("z", 3)
        elif 187 <= space_group <= 190:
            ops = make_dihedral("z", 3, "x") # D3
        elif (177 <= space_group <= 186) or (191 <= space_group <= 194):
             ops = make_dihedral("z", 6, "x") # D6
        else:
             ops = make_cyclic("z", 6)

    # CUBIC
    else:
        # 195-206 (23, m-3): 23 (T)
        # 207-214 (432): 432 (O)
        # 215-224 (-43m): 23 (T)
        # 225-230 (m-3m): 432 (O)
        
        is_432 = (207 <= space_group <= 214) or (225 <= space_group <= 230)
        
        if is_432:
            ops = make_cubic_432()
        else:
            ops = make_cubic_23()

    return np.array(ops, dtype=np.float64)


def parse_nxds_xparm(filepath: str) -> Dict:
    """
    Parse an XPARM.nXDS or GXPARM.nXDS file.

    Returns:
        dict with:
          - header: dict with space_group, osc_range, rotation_axis, n_images,
                    n_segments, nx, ny, pixel_x, pixel_y
          - images: list of dicts, each with:
              filename, wavelength, beam_vector, a_axis, b_axis, c_axis,
              det_origin, det_distance, det_x, det_y, det_z
    """
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f if line.strip()]

    result = {"header": {}, "images": []}

    # --- Header ---
    # Line 0: directory path
    header_dir = lines[0]
    result["header"]["directory"] = header_dir

    # Line 1: n_images, n_segments, nx, ny, pixel_x_mm, pixel_y_mm
    parts = lines[1].split()
    n_images = int(parts[0])
    n_segments = int(parts[1])
    nx, ny = int(parts[2]), int(parts[3])
    pixel_x, pixel_y = float(parts[4]), float(parts[5])
    result["header"].update({
        "n_images": n_images,
        "n_segments": n_segments,
        "nx": nx, "ny": ny,
        "pixel_x_mm": pixel_x, "pixel_y_mm": pixel_y,
    })

    # Line 2: space group number
    result["header"]["space_group"] = int(lines[2].split()[0])

    # Line 3: oscillation range + rotation axis direction cosines
    parts = lines[3].split()
    osc_range = float(parts[0])
    rot_axis = np.array([float(parts[1]), float(parts[2]), float(parts[3])])
    result["header"]["osc_range_deg"] = osc_range
    result["header"]["rotation_axis"] = rot_axis.tolist()

    # Lines 4 to 4+n_segments-1: segment definitions
    segments = []
    for j in range(n_segments): # nXDS has 1 line per segment (13 numbers)
        segments.append(lines[4 + j])
        
    result["header"]["segments"] = segments
    
    body_start = 4 + n_segments

    # --- Body: 9 lines per image ---
    i = body_start
    while i + 8 < len(lines):
        img = {}

        # Line 0: image filename
        img["filename"] = lines[i]

        # Line 1: wavelength, beam_x, beam_y, beam_z
        parts = lines[i + 1].split()
        img["wavelength"] = float(parts[0])
        img["beam_vector"] = np.array([float(parts[1]), float(parts[2]), float(parts[3])])

        # Line 2: a-axis (lab coords, Å)
        parts = lines[i + 2].split()
        img["a_axis"] = np.array([float(parts[0]), float(parts[1]), float(parts[2])])

        # Line 3: b-axis
        parts = lines[i + 3].split()
        img["b_axis"] = np.array([float(parts[0]), float(parts[1]), float(parts[2])])

        # Line 4: c-axis
        parts = lines[i + 4].split()
        img["c_axis"] = np.array([float(parts[0]), float(parts[1]), float(parts[2])])

        # Line 5: detector origin (pixels) + signed distance (mm)
        parts = lines[i + 5].split()
        img["det_origin_px"] = np.array([float(parts[0]), float(parts[1])])
        img["det_distance_mm"] = float(parts[2])

        # Lines 6-8: detector axes
        parts = lines[i + 6].split()
        img["det_x"] = np.array([float(parts[0]), float(parts[1]), float(parts[2])])

        parts = lines[i + 7].split()
        img["det_y"] = np.array([float(parts[0]), float(parts[1]), float(parts[2])])

        parts = lines[i + 8].split()
        img["det_z"] = np.array([float(parts[0]), float(parts[1]), float(parts[2])])

        result["images"].append(img)
        i += 9

    return result


def write_xds_file(image_data: Dict, header_data: Dict, output_path: str, image_num: int = 1):
    """Write an XDS XPARM/GXPARM file for a single crystal."""
    
    # Extract data
    h = header_data
    img = image_data
    
    # Prepare lines
    lines = []
    
    # 1. Version line
    lines.append(" XPARM.XDS    VERSION Jan 19, 2025  BUILT=20251103")
    
    # 2. Starting image, starting angle, oscillation range, rotation axis
    rot_axis = h["rotation_axis"]
    lines.append(f" {image_num:5d}      0.0000    {h['osc_range_deg']:.4f}  {rot_axis[0]:.6f}  {rot_axis[1]:.6f}  {rot_axis[2]:.6f}")

    # 3. Wavelength, incident beam
    beam = img["beam_vector"]
    lines.append(f" {img['wavelength']:.6f}       {beam[0]:.6f}       {beam[1]:.6f}       {beam[2]:.6f}")

    # 4. Space group, unit cell
    uc = unit_cell_from_axes(img["a_axis"], img["b_axis"], img["c_axis"])
    lines.append(f"   {h['space_group']}    {uc['a']:.4f}    {uc['b']:.4f}    {uc['c']:.4f}  {uc['alpha']:.3f}  {uc['beta']:.3f}  {uc['gamma']:.3f}")

    # 5. a-axis
    a = img["a_axis"]
    lines.append(f"     {a[0]:.6f}      {a[1]:.6f}     {a[2]:.6f}")

    # 6. b-axis
    b = img["b_axis"]
    lines.append(f"     {b[0]:.6f}      {b[1]:.6f}     {b[2]:.6f}")

    # 7. c-axis
    c = img["c_axis"]
    lines.append(f"    {c[0]:.6f}      {c[1]:.6f}     {c[2]:.6f}")

    # 8. Segments, nx, ny, pixel sizes
    lines.append(f"         {h['n_segments']}      {h['nx']}      {h['ny']}    {h['pixel_x_mm']:.6f}    {h['pixel_y_mm']:.6f}")

    # 9. Detector origin (ORGX, ORGY, F)
    # The XDS format: ORGX ORGY F
    origin = img["det_origin_px"]
    dist = img["det_distance_mm"]
    lines.append(f"    {origin[0]:.6f}    {origin[1]:.6f}     {dist:.6f}")

    # 10. Detector X-axis
    dx = img["det_x"]
    lines.append(f"       {dx[0]:.6f}       {dx[1]:.6f}       {dx[2]:.6f}")

    # 11. Detector Y-axis
    dy = img["det_y"]
    lines.append(f"       {dy[0]:.6f}       {dy[1]:.6f}       {dy[2]:.6f}")

    # 12. Detector Z-axis (Normal)
    dz = img["det_z"]
    lines.append(f"       {dz[0]:.6f}       {dz[1]:.6f}       {dz[2]:.6f}")

    # 13+. Segments
    # nXDS has 1 line with 13 numbers: x1 x2 y1 y2 ORGX ORGY F ...
    # XDS needs 2 lines:
    # 1. iseg x1 x2 y1 y2
    # 2. ORGX ORGY F ...
    for idx, seg_line in enumerate(h["segments"]):
        parts = seg_line.split()
        if len(parts) >= 13:
            # We assume the first 4 are x1 x2 y1 y2
            # Prepend iseg (idx+1)
            line1_parts = [str(idx + 1)] + parts[:4]
            line2_parts = parts[4:]
            lines.append(" ".join(line1_parts))
            lines.append(" ".join(line2_parts))
        else:
            # Fallback if format is unexpected
            lines.append(seg_line)
        
    with open(output_path, "w") as f:
        f.write("\n".join(lines) + "\n")


def orthogonalization_matrix(a: float, b: float, c: float,
                              alpha: float, beta: float, gamma: float) -> np.ndarray:
    """Compute the orthogonalization matrix B from unit cell parameters.

    Convention: a along x, b in xy-plane (PDB/IUCr convention).
    B transforms fractional coordinates to Cartesian (Å).

    Args:
        a, b, c: cell lengths (Å)
        alpha, beta, gamma: cell angles (degrees)
    """
    alpha_r = np.radians(alpha)
    beta_r = np.radians(beta)
    gamma_r = np.radians(gamma)

    cos_a = np.cos(alpha_r)
    cos_b = np.cos(beta_r)
    cos_g = np.cos(gamma_r)
    sin_g = np.sin(gamma_r)

    # Volume factor
    val = 1 - cos_a**2 - cos_b**2 - cos_g**2 + 2 * cos_a * cos_b * cos_g
    omega = np.sqrt(max(val, 0.0))

    B = np.array([
        [a,  b * cos_g,  c * cos_b],
        [0,  b * sin_g,  c * (cos_a - cos_b * cos_g) / sin_g],
        [0,  0,          c * omega / sin_g],
    ], dtype=np.float64)
    return B


def axes_to_orientation_matrix(a: np.ndarray, b: np.ndarray, c: np.ndarray) -> np.ndarray:
    """Extract the pure rotation matrix U from crystal axes in lab frame.

    The raw axes [a|b|c] form the A matrix (= U @ B in crystallographic notation),
    where B is the orthogonalization matrix. We compute U = A @ B⁻¹, which is a
    proper rotation matrix (orthogonal, det=+1).

    This is the correct way to compare orientations: the trace formula
    θ = arccos((tr(U₁ᵀU₂) - 1)/2) requires U to be a rotation matrix.
    """
    # A matrix: columns are crystal axes in lab frame
    A = np.column_stack([a, b, c])

    # Compute cell parameters to build B
    uc = unit_cell_from_axes(a, b, c)
    B = orthogonalization_matrix(uc["a"], uc["b"], uc["c"],
                                 uc["alpha"], uc["beta"], uc["gamma"])

    # U = A @ B^{-1}
    U = A @ np.linalg.inv(B)

    # Clean up numerical noise — ensure U is a proper rotation
    # by using SVD to project onto SO(3)
    u_svd, _, vt_svd = np.linalg.svd(U)
    U = u_svd @ vt_svd
    if np.linalg.det(U) < 0:
        u_svd[:, -1] *= -1
        U = u_svd @ vt_svd

    return U


def unit_cell_from_axes(a: np.ndarray, b: np.ndarray, c: np.ndarray) -> Dict:
    """Compute unit cell parameters from direct axes in lab frame."""
    a_len = np.linalg.norm(a)
    b_len = np.linalg.norm(b)
    c_len = np.linalg.norm(c)
    alpha = np.degrees(np.arccos(np.clip(np.dot(b, c) / (b_len * c_len), -1, 1)))
    beta = np.degrees(np.arccos(np.clip(np.dot(a, c) / (a_len * c_len), -1, 1)))
    gamma = np.degrees(np.arccos(np.clip(np.dot(a, b) / (a_len * b_len), -1, 1)))
    return {
        "a": round(a_len, 3), "b": round(b_len, 3), "c": round(c_len, 3),
        "alpha": round(alpha, 2), "beta": round(beta, 2), "gamma": round(gamma, 2),
    }


def misorientation_angle(U1: np.ndarray, U2: np.ndarray) -> float:
    """Compute the misorientation angle (degrees) using Kabsch alignment.
    
    U1, U2 are interpreted as raw basis vector sets (3x3 matrices where columns are vectors).
    Aligned U2' = R @ U2. R minimizes RMSD(U2' - U1).
    Angle is angle of R.
    """
    # Use standard Kabsch algorithm (SVD based)
    # H = U1 @ U2.T
    # But wait, align_vectors aligns specific points.
    # We treat columns as paired vectors.
    # U1 = [[a1x, b1x, c1x], ...].
    # Points 1: a1, b1, c1.
    # Points 2: a2, b2, c2.
    # Align P2 (U2) to P1 (U1).
    # H = sum w_i b_i a_i.T (b=Points1, a=Points2?)
    # R * a ~ b.
    # Covariance H = U1 @ U2.T.
    try:
        from scipy.spatial.transform import Rotation
    except ImportError:
        # Fallback to trace logic if scipy missing (less robust for shear)
        dU = U1.T @ U2 # Assuming orthogonal U
        trace = np.clip(np.trace(dU), -1.0, 3.0)
        return np.degrees(np.arccos((trace - 1.0) / 2.0))

    # Scipy align_vectors expects (N, 3).
    # Transpose to get rows as vectors (since our columns are vectors)
    vecs1 = U1.T
    vecs2 = U2.T
    
    # Weights? Uniform.
    rot, rssd = Rotation.align_vectors(vecs1, vecs2)
    return rot.magnitude() * (180.0 / np.pi)

def get_fractional_operators(sym_ops_cart: np.ndarray, B: np.ndarray) -> np.ndarray:
    """Convert Cartesian symmetry operators to Integer Fractional operators.
    S_frac = round(inv(B) @ S_cart @ B).
    """
    B_inv = np.linalg.inv(B)
    ops_frac = []
    for S in sym_ops_cart:
        S_f = B_inv @ S @ B
        ops_frac.append(np.round(S_f).astype(int))
    return np.array(ops_frac)


def pairwise_misorientation_condensed(
    orientations: np.ndarray,
    sym_ops: Optional[np.ndarray] = None,
    chunk_size: int = 500_000,
) -> np.ndarray:
    """Compute condensed pairwise misorientation angles (vectorized, memory-bounded).

    Uses Kabsch Alignment on raw basis vectors to be robust against lattice strain.
    Requires `orientations` to be (N, 3, 3) array of basis vectors A (columns a,b,c).
    
    If sym_ops is provided, it should be in Cartesian form (from get_point_group_operators).
    Will be converted to Fractional form using the first orientation's ideal B-matrix.
    
    Args:
        orientations: (n, 3, 3) matrix of basis vectors (A).
        sym_ops: (n_sym, 3, 3) Cartesian symmetry operators or None.
        chunk_size: max pairs per chunk.

    Returns:
        1-D condensed distance array of length n*(n-1)/2.
    """
    n = orientations.shape[0]
    n_pairs = n * (n - 1) // 2
    result = np.empty(n_pairs, dtype=np.float64)
    
    # Prepare fractional operators if symmetry provided
    ops_to_use = [np.eye(3)]
    if sym_ops is not None and len(sym_ops) > 0 and n > 0:
        # Derive B from first image (Approximation for entire set)
        # Recalculate cell params from first A to get ideal B
        # A1 = orientations[0]
        # a, b, c = A1[:,0], A1[:,1], A1[:,2]
        # p = unit_cell_from_axes(a, b, c)
        # B_ideal = orthogonalization_matrix(p["a"], p["b"], p["c"], p["alpha"], p["beta"], p["gamma"])
        
        # Actually, let's just make a helper to invert.
        # But we need integer matrices.
        # Use simpler approach: For each image, we preserve A.
        # A_new = A @ S_frac.
        # But we pass Cartesian sym_ops.
        # To get robust S_frac, we need a reference B.
        # Let's use the first image.
        A0 = orientations[0]
        a0, b0, c0 = A0[:,0], A0[:,1], A0[:,2]
        p0 = unit_cell_from_axes(a0, b0, c0)
        B0 = orthogonalization_matrix(p0["a"], p0["b"], p0["c"], p0["alpha"], p0["beta"], p0["gamma"])
        
        ops_frac = get_fractional_operators(sym_ops, B0)
        ops_to_use = ops_frac

    # Function to calculate min distance between A_i and A_j
    # Iterate pairs and ops.
    # Note: Optimization - Pre-calculate A @ S_frac for all S?
    # orientations: (N, 3, 3).
    # expanded: (N, n_ops, 3, 3).
    # Then pairwise compare (N, 1, 3, 3) vs (N, n_ops, 3, 3)?
    # Memory: 1000 * 12 * 9 * 8 bytes ~ 100 KB. Trivial.
    # Pre-calculate all symmetry variants of A
    
    from scipy.spatial.transform import Rotation
    
    # Create (N, n_ops, 3, 3) array of A matrices
    n_ops = len(ops_to_use)
    A_sym = np.zeros((n, n_ops, 3, 3))
    
    # ops_to_use is list of 3x3 arrays
    # A @ S_frac causes permutation of columns (new basis vectors)
    # A is (3, 3) columns. S_frac (3, 3).
    # A_new = A @ S_frac (Standard basis transform)
    
    for i in range(n):
        Ai = orientations[i]
        for k, S in enumerate(ops_to_use):
            A_sym[i, k] = Ai @ S.T # Transpose S? S_frac maps indices?
            # Standard: v_frac_new = S v_frac.
            # Real space basis: A_new = A S^-1?
            # Geometry: S is symmetry of Lattice.
            # S maps basis to basis. A_new IS A.
            # We want to re-index A.
            # If S maps a->b (Rot 120), then we want to treat 'b' as 'a'?
            # Kabsch aligns A_observed to A_reference.
            # A_reference_permuted = A_reference @ S_frac.
            # Yes. A @ S.
            # Check integer logic: S=[[0,1],[1,0]]. A=[a,b]. A@S = [b,a].
            # Align [b,a] to [a,b]. R maps b->a. Correct.
            A_sym[i, k] = Ai @ S

    # Compute pairwise distances
    cursor = 0
    for i in range(n):
        # A_i reference (3, 3)
        vecs_i = orientations[i].T # (3, 3)
        
        # Compare with j > i
        # Candidates for j: A_sym[j, :, :, :]
        # Shape (n-1-i, n_ops, 3, 3)
        
        n_rem = n - 1 - i
        if n_rem <= 0: break
        
        # Loop j
        for j in range(i + 1, n):
            # Find Best S for pair (i, j)
            # vecs_j_stack = A_sym[j] # (n_ops, 3, 3) - columns are vectors
            # transpose each to (n_ops, 3, 3) - rows are vectors
            vecs_j_stack = np.transpose(A_sym[j], (0, 2, 1))
            
            # Align all ops
            rot, rssd = Rotation.align_vectors(vecs_j_stack.reshape(-1, 3), 
                                             np.tile(vecs_i, (n_ops, 1)))
            # Wait, align_vectors aligns batches?
            # align_vectors(a, b) aligns ONE set of vectors.
            # It supports weights.
            # It does NOT broadcast over multiple sets.
            # We must loop over ops.
            
            min_angle = 180.0
            
            # Optimization: Try Identity first?
            # Just loop
            for k in range(n_ops):
                vecs_j = vecs_j_stack[k] # (3, 3)
                r, _ = Rotation.align_vectors(vecs_j, vecs_i)
                ang = r.magnitude() * (180.0 / np.pi)
                if ang < min_angle:
                    min_angle = ang
            
            result[cursor] = min_angle
            cursor += 1
            
    return result



def pairwise_misorientation_condensed_fast(
    orientations_A: np.ndarray,
    sym_ops: Optional[np.ndarray] = None,
    chunk_size: int = 500_000,
) -> np.ndarray:
    """Fast vectorized pairwise misorientation using einsum on rotation matrices.

    Extracts proper rotation matrices U = A @ B⁻¹ from raw basis vectors,
    then computes θ = arccos((tr(Uᵢᵀ·S·Uⱼ) - 1)/2) for all pairs using
    vectorized einsum. Orders of magnitude faster than the Kabsch approach.

    Args:
        orientations_A: (n, 3, 3) array of raw basis vectors A = [a|b|c].
        sym_ops: (n_sym, 3, 3) Cartesian symmetry operators or None.
        chunk_size: max pairs per chunk (bounds memory).

    Returns:
        1-D condensed distance array of length n*(n-1)/2.
    """
    n = orientations_A.shape[0]
    n_pairs = n * (n - 1) // 2
    result = np.empty(n_pairs, dtype=np.float64)

    # Extract proper rotation matrices U from raw A matrices
    U = np.empty_like(orientations_A)
    for i in range(n):
        a, b, c = orientations_A[i, :, 0], orientations_A[i, :, 1], orientations_A[i, :, 2]
        U[i] = axes_to_orientation_matrix(a, b, c)

    # Build index pairs
    ii, jj = np.triu_indices(n, k=1)

    if sym_ops is None or len(sym_ops) <= 1:
        # No symmetry — fast path
        for start in range(0, n_pairs, chunk_size):
            end = min(start + chunk_size, n_pairs)
            Ui = U[ii[start:end]]
            Uj = U[jj[start:end]]
            traces = np.einsum("mab,mab->m", Ui, Uj)
            traces = np.clip(traces, -1.0, 3.0)
            result[start:end] = np.degrees(
                np.arccos(np.clip((traces - 1.0) / 2.0, -1.0, 1.0))
            )
    else:
        # Symmetry-aware: min over S of angle(Uᵢᵀ @ Uⱼ @ S)
        n_sym = len(sym_ops)
        for start in range(0, n_pairs, chunk_size):
            end = min(start + chunk_size, n_pairs)
            Ui = U[ii[start:end]]
            Uj = U[jj[start:end]]

            # Uj @ S for all S: (n_sym, chunk, 3, 3)
            UjS = np.einsum("mab,sbc->smac", Uj, sym_ops)

            # trace(Uiᵀ @ Uj @ S) for all S: (n_sym, chunk)
            all_traces = np.einsum("mab,smab->sm", Ui, UjS)

            # Max trace = min angle
            best_traces = np.max(all_traces, axis=0)
            best_traces = np.clip(best_traces, -1.0, 3.0)
            result[start:end] = np.degrees(
                np.arccos(np.clip((best_traces - 1.0) / 2.0, -1.0, 1.0))
            )

    return result


def compute_euler_angles(U: np.ndarray) -> Tuple[float, float, float]:
    """Extract ZYZ Euler angles (phi1, Phi, phi2) in degrees from orientation matrix U."""
    # ZYZ convention
    Phi = np.degrees(np.arccos(np.clip(U[2, 2], -1.0, 1.0)))
    if abs(np.sin(np.radians(Phi))) > 1e-6:
        phi1 = np.degrees(np.arctan2(U[1, 2], U[0, 2]))
        phi2 = np.degrees(np.arctan2(U[2, 1], -U[2, 0]))
    else:
        phi1 = np.degrees(np.arctan2(-U[1, 0], U[1, 1]))
        phi2 = 0.0
    return phi1, Phi, phi2


def cluster_orientations(
    orientations: List[np.ndarray],
    threshold_deg: float = 5.0,
    sym_ops: Optional[np.ndarray] = None,
    fast: bool = True,
) -> Dict:
    """
    Cluster crystal orientations into distinct groups using hierarchical
    clustering on the pairwise misorientation distance matrix.

    Two crystals whose misorientation angle is ≤ threshold_deg are
    considered to have the "same" orientation.

    Args:
        fast: If True, use vectorized einsum approach instead of Kabsch.

    Returns:
        dict with:
          - n_distinct: number of distinct orientation clusters
          - labels: cluster label for each image (0-indexed)
          - cluster_sizes: list of (cluster_id, count) sorted largest first
          - cluster_members: dict mapping cluster_id -> list of image indices
    """
    from scipy.cluster.hierarchy import fcluster, linkage
    from scipy.spatial.distance import squareform

    n = len(orientations)
    if n <= 1:
        return {
            "n_distinct": n,
            "labels": [0] * n,
            "cluster_sizes": [(0, n)] if n == 1 else [],
            "cluster_members": {0: list(range(n))} if n == 1 else {},
        }

    # Build condensed distance matrix
    U_stack = np.array(orientations)  # (n, 3, 3)
    if fast:
        condensed = pairwise_misorientation_condensed_fast(U_stack, sym_ops=sym_ops)
    else:
        condensed = pairwise_misorientation_condensed(U_stack, sym_ops=sym_ops)

    # Hierarchical clustering (complete linkage: all members within threshold)
    Z = linkage(condensed, method="complete")
    labels = fcluster(Z, t=threshold_deg, criterion="distance") - 1  # 0-indexed

    # Summarize clusters
    cluster_members = {}
    for idx, lbl in enumerate(labels):
        cluster_members.setdefault(int(lbl), []).append(idx)

    cluster_sizes = sorted(
        [(cid, len(members)) for cid, members in cluster_members.items()],
        key=lambda x: -x[1],
    )

    return {
        "n_distinct": len(cluster_sizes),
        "labels": [int(l) for l in labels],
        "cluster_sizes": cluster_sizes,
        "cluster_members": cluster_members,
    }


def analyze_orientations(xparm_data: Dict, use_gxparm: bool = False,
                         cluster_threshold_deg: float = 5.0,
                         space_group: Optional[int] = None,
                         fast: bool = True) -> Dict:
    """
    Analyze crystal orientations from parsed XPARM/GXPARM data.

    Returns a dict with:
      - n_crystals: number of successfully indexed images
      - unit_cells: list of unit cell parameters
      - euler_angles: list of (phi1, Phi, phi2) Euler angles
      - misorientation_matrix: pairwise misorientation angles
      - statistics: orientation distribution statistics
    """
    images = xparm_data["images"]
    n = len(images)

    if n == 0:
        return {"n_crystals": 0, "error": "No images found in XPARM file"}

    # Extract orientations
    orientations_U = [] # Orthogonal U matrices for Euler angles
    orientations_A = [] # Raw Basis A matrices for Kabsch
    unit_cells = []
    euler_angles_list = []
    c_axis_directions = []  # For pole figure analysis
    a_axis_directions = []

    for img in images:
        a, b, c = img["a_axis"], img["b_axis"], img["c_axis"]

        # Unit cell
        uc = unit_cell_from_axes(a, b, c)
        unit_cells.append(uc)

        # Orientation matrix (Orthogonal)
        U = axes_to_orientation_matrix(a, b, c)
        orientations_U.append(U)
        
        # Raw Basis Matrix A (for Kabsch)
        A = np.column_stack([a, b, c])
        orientations_A.append(A)

        # Euler angles
        phi1, Phi, phi2 = compute_euler_angles(U)
        euler_angles_list.append((phi1, Phi, phi2))

        # Normalized axis directions for pole figure
        c_hat = c / np.linalg.norm(c)
        a_hat = a / np.linalg.norm(a)
        c_axis_directions.append(c_hat)
        a_axis_directions.append(a_hat)

    # Symmetry operators from space group
    sg = space_group or xparm_data.get("header", {}).get("space_group", 1)
    sym_ops = get_point_group_operators(sg) # Cartesian operators
    n_sym = len(sym_ops)

    # Pairwise misorientation angles
    A_stack = np.stack(orientations_A)  # (n, 3, 3)
    if fast:
        misorientations = pairwise_misorientation_condensed_fast(A_stack, sym_ops=sym_ops)
    else:
        misorientations = pairwise_misorientation_condensed(A_stack, sym_ops=sym_ops)

    # Statistics
    euler_arr = np.array(euler_angles_list)
    c_dirs = np.array(c_axis_directions)
    a_dirs = np.array(a_axis_directions)

    # Check for preferred orientation using the c-axis
    # For random orientations, c-axis directions should be uniformly distributed on a sphere
    # Compute the resultant vector (R) — for uniform distribution, |R|/N → 0
    c_resultant = np.linalg.norm(np.mean(c_dirs, axis=0))
    a_resultant = np.linalg.norm(np.mean(a_dirs, axis=0))

    # March-Dollase-like texture index:
    # R-bar = |mean(unit vectors)| ranges from 0 (random) to 1 (perfectly aligned)
    stats = {
        "n_crystals": n,
        "space_group": sg,
        "n_symmetry_operators": n_sym,
        "mean_misorientation_deg": round(float(np.mean(misorientations)), 2) if len(misorientations) > 0 else None,
        "std_misorientation_deg": round(float(np.std(misorientations)), 2) if len(misorientations) > 0 else None,
        "min_misorientation_deg": round(float(np.min(misorientations)), 2) if len(misorientations) > 0 else None,
        "max_misorientation_deg": round(float(np.max(misorientations)), 2) if len(misorientations) > 0 else None,
        "c_axis_resultant_length": round(float(c_resultant), 4),
        "a_axis_resultant_length": round(float(a_resultant), 4),
        "orientation_assessment": "",
    }

    # Cluster orientations to find distinct groups
    clustering = cluster_orientations(orientations_A, threshold_deg=cluster_threshold_deg, sym_ops=sym_ops, fast=fast)
    stats["n_distinct_orientations"] = clustering["n_distinct"]
    stats["cluster_threshold_deg"] = cluster_threshold_deg
    stats["cluster_sizes"] = clustering["cluster_sizes"]

    # Orientation multiplicity: avg images per distinct orientation
    # For random orientations at 5° threshold, multiplicity ≈ 1 (each image unique)
    n_distinct = clustering["n_distinct"]
    multiplicity = n / n_distinct if n_distinct > 0 else 1.0
    stats["orientation_multiplicity"] = round(multiplicity, 2)

    # Classify orientation distribution using both multiplicity and resultant
    if multiplicity > 3.0 or c_resultant > 0.7:
        if c_resultant > 0.4:
            stats["orientation_assessment"] = (
                f"STRONGLY PREFERRED — multiplicity {multiplicity:.1f}x, "
                f"c-axes aligned (R={c_resultant:.3f})"
            )
        else:
            stats["orientation_assessment"] = (
                f"STRONGLY PREFERRED — multiplicity {multiplicity:.1f}x "
                f"({n_distinct} distinct / {n} total), "
                f"multiple preferred orientations"
            )
    elif multiplicity > 1.5 or c_resultant > 0.4:
        stats["orientation_assessment"] = (
            f"MODERATELY PREFERRED — multiplicity {multiplicity:.1f}x "
            f"({n_distinct} distinct / {n} total)"
        )
    elif multiplicity > 1.1 or c_resultant > 0.2:
        stats["orientation_assessment"] = (
            f"WEAKLY PREFERRED — multiplicity {multiplicity:.1f}x"
        )
    else:
        stats["orientation_assessment"] = "RANDOM — no significant preferred orientation"

    # Unit cell statistics
    uc_arrays = {k: np.array([uc[k] for uc in unit_cells]) for k in ["a", "b", "c", "alpha", "beta", "gamma"]}
    uc_stats = {}
    for k, v in uc_arrays.items():
        uc_stats[k] = {
            "mean": round(float(np.mean(v)), 3),
            "std": round(float(np.std(v)), 3),
            "min": round(float(np.min(v)), 3),
            "max": round(float(np.max(v)), 3),
        }

    return {
        "n_crystals": n,
        "unit_cells": unit_cells,
        "unit_cell_stats": uc_stats,
        "euler_angles": euler_angles_list,
        "c_axis_directions": [d.tolist() for d in c_axis_directions],
        "a_axis_directions": [d.tolist() for d in a_axis_directions],
        "misorientation_histogram": np.histogram(misorientations, bins=18, range=(0, 180))[0].tolist() if len(misorientations) > 0 else [],
        "misorientation_bin_edges": np.histogram(misorientations, bins=18, range=(0, 180))[1].tolist() if len(misorientations) > 0 else [],
        "clustering": clustering,
        "statistics": stats,
    }


def find_xparm_files(run_dir: str, prefer_gxparm: bool = True) -> List[Path]:
    """Find XPARM.nXDS or GXPARM.nXDS files in a run directory tree."""
    run_path = Path(run_dir)
    if run_path.is_file():
        return [run_path]

    target = "GXPARM.nXDS" if prefer_gxparm else "XPARM.nXDS"
    fallback = "XPARM.nXDS" if prefer_gxparm else "GXPARM.nXDS"

    files = sorted(run_path.rglob(target))
    if not files:
        files = sorted(run_path.rglob(fallback))
    return files


def plot_orientation_analysis(results: Dict, output_path: Optional[str] = None):
    """Generate orientation analysis plots."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.gridspec import GridSpec
    except ImportError:
        print("matplotlib not available — skipping plots")
        return

    fig = plt.figure(figsize=(16, 12))
    fig.suptitle(
        f"nXDS Crystal Orientation Analysis (N={results['n_crystals']})",
        fontsize=14, fontweight="bold"
    )
    gs = GridSpec(2, 3, figure=fig, hspace=0.35, wspace=0.35)

    stats = results["statistics"]

    # --- 1. Misorientation histogram ---
    ax1 = fig.add_subplot(gs[0, 0])
    if results["misorientation_histogram"]:
        edges = results["misorientation_bin_edges"]
        centers = [(edges[i] + edges[i + 1]) / 2 for i in range(len(edges) - 1)]
        counts = results["misorientation_histogram"]
        ax1.bar(centers, counts, width=(edges[1] - edges[0]) * 0.9, color="#4C72B0", edgecolor="white")
        
        # Overlay expected random distribution
        if results["n_crystals"] > 2:
            total = sum(counts)
            n_bins = len(counts)
            sg = stats.get("space_group", 1)
            n_sym = stats.get("n_symmetry_operators", 1)

            if n_sym <= 1:
                # No symmetry — use exact analytical curve: P(θ) ∝ (1 - cos θ)
                theta = np.linspace(0, 180, 200)
                random_pdf = (1 - np.cos(np.radians(theta))) / 2
                random_pdf = random_pdf / np.trapz(random_pdf, theta) * total * (180 / n_bins)
                ax1.plot(theta, random_pdf, "r--", linewidth=1.5, label="Random (exact)")
            else:
                # With symmetry — use Monte Carlo for correct Mackenzie distribution
                sym_ops = get_point_group_operators(sg)
                n_mc = 5000
                rng = np.random.default_rng(42)
                random_mats = rng.standard_normal((n_mc, 3, 3))
                Q, _ = np.linalg.qr(random_mats)
                dets = np.linalg.det(Q)
                Q[dets < 0] *= -1
                n_ref = min(200, n_mc)
                mc_misor = pairwise_misorientation_condensed(Q[:n_ref], sym_ops=sym_ops)
                mc_counts, _ = np.histogram(mc_misor, bins=edges)
                mc_counts = mc_counts.astype(float)
                if mc_counts.sum() > 0:
                    mc_counts = mc_counts / mc_counts.sum() * total
                ax1.plot(centers, mc_counts, "r--", linewidth=1.5, label="Random (MC)")
            ax1.legend(fontsize=8)

    ax1.set_xlabel("Misorientation angle (°)")
    ax1.set_ylabel("Count")
    ax1.set_title("Pairwise Misorientation Distribution")

    # --- 2. Stereographic projection of c-axis ---
    ax2 = fig.add_subplot(gs[0, 1])
    if results["c_axis_directions"]:
        c_dirs = np.array(results["c_axis_directions"])
        # Stereographic projection: project onto XY plane
        # Use the beam direction (Z) as the projection pole
        # Ensure all vectors point into the upper hemisphere (z > 0)
        for i in range(len(c_dirs)):
            if c_dirs[i, 2] < 0:
                c_dirs[i] = -c_dirs[i]
        
        # Stereographic projection: (x, y) = (X/(1+Z), Y/(1+Z))
        denom = 1 + c_dirs[:, 2]
        denom[denom < 1e-10] = 1e-10
        sx = c_dirs[:, 0] / denom
        sy = c_dirs[:, 1] / denom

        circle = plt.Circle((0, 0), 1, fill=False, color="gray", linewidth=0.5)
        ax2.add_patch(circle)
        ax2.scatter(sx, sy, c="#E8575B", s=20, alpha=0.7, edgecolors="black", linewidth=0.3)
        ax2.set_xlim(-1.1, 1.1)
        ax2.set_ylim(-1.1, 1.1)
        ax2.set_aspect("equal")
        ax2.axhline(0, color="gray", linewidth=0.3)
        ax2.axvline(0, color="gray", linewidth=0.3)
    ax2.set_title("c-axis Pole Figure (stereographic)")
    ax2.set_xlabel("X")
    ax2.set_ylabel("Y")

    # --- 3. Stereographic projection of a-axis ---
    ax3 = fig.add_subplot(gs[0, 2])
    if results["a_axis_directions"]:
        a_dirs = np.array(results["a_axis_directions"])
        for i in range(len(a_dirs)):
            if a_dirs[i, 2] < 0:
                a_dirs[i] = -a_dirs[i]

        denom = 1 + a_dirs[:, 2]
        denom[denom < 1e-10] = 1e-10
        sx = a_dirs[:, 0] / denom
        sy = a_dirs[:, 1] / denom

        circle = plt.Circle((0, 0), 1, fill=False, color="gray", linewidth=0.5)
        ax3.add_patch(circle)
        ax3.scatter(sx, sy, c="#64B5CD", s=20, alpha=0.7, edgecolors="black", linewidth=0.3)
        ax3.set_xlim(-1.1, 1.1)
        ax3.set_ylim(-1.1, 1.1)
        ax3.set_aspect("equal")
        ax3.axhline(0, color="gray", linewidth=0.3)
        ax3.axvline(0, color="gray", linewidth=0.3)
    ax3.set_title("a-axis Pole Figure (stereographic)")
    ax3.set_xlabel("X")
    ax3.set_ylabel("Y")

    # --- 4. Euler angles scatter ---
    ax4 = fig.add_subplot(gs[1, 0])
    if results["euler_angles"]:
        euler = np.array(results["euler_angles"])
        ax4.scatter(euler[:, 0], euler[:, 1], c="#55A868", s=20, alpha=0.7, edgecolors="black", linewidth=0.3)
    ax4.set_xlabel("φ₁ (°)")
    ax4.set_ylabel("Φ (°)")
    ax4.set_title("Euler Angles (ZYZ: φ₁ vs Φ)")

    # --- 5. Unit cell box plot ---
    ax5 = fig.add_subplot(gs[1, 1])
    if results["unit_cells"]:
        uc_a = [uc["a"] for uc in results["unit_cells"]]
        uc_b = [uc["b"] for uc in results["unit_cells"]]
        uc_c = [uc["c"] for uc in results["unit_cells"]]
        bp = ax5.boxplot([uc_a, uc_b, uc_c], labels=["a", "b", "c"], patch_artist=True)
        colors = ["#4C72B0", "#55A868", "#E8575B"]
        for patch, color in zip(bp["boxes"], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
    ax5.set_ylabel("Length (Å)")
    ax5.set_title("Unit Cell Parameter Distribution")

    # --- 6. Summary text ---
    ax6 = fig.add_subplot(gs[1, 2])
    ax6.axis("off")
    # Format cluster sizes for display
    cluster_str = ""
    if "cluster_sizes" in stats:
        top_clusters = stats["cluster_sizes"][:5]  # Show top 5
        cluster_str = ", ".join(f"{sz}" for _, sz in top_clusters)
        if len(stats["cluster_sizes"]) > 5:
            cluster_str += f", ... ({len(stats['cluster_sizes'])} total)"

    summary_text = (
        f"Crystals indexed: {stats['n_crystals']}\n"
        f"Distinct orientations: {stats.get('n_distinct_orientations', '?')}"
        f" (threshold: {stats.get('cluster_threshold_deg', '?')}°)\n"
        f"Cluster sizes: [{cluster_str}]\n\n"
        f"Misorientation:\n"
        f"  Mean: {stats['mean_misorientation_deg']}°\n"
        f"  Std:  {stats['std_misorientation_deg']}°\n"
        f"  Range: {stats['min_misorientation_deg']}° – {stats['max_misorientation_deg']}°\n\n"
        f"Resultant lengths:\n"
        f"  c-axis: {stats['c_axis_resultant_length']}\n"
        f"  a-axis: {stats['a_axis_resultant_length']}\n\n"
        f"Assessment:\n  {stats['orientation_assessment']}"
    )
    ax6.text(0.05, 0.95, summary_text, transform=ax6.transAxes,
             fontsize=10, verticalalignment="top", fontfamily="monospace",
             bbox=dict(boxstyle="round,pad=0.5", facecolor="#f0f0f0", edgecolor="gray"))
    ax6.set_title("Summary")

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Plot saved to: {output_path}")
    else:
        plt.show()

    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze crystal orientations from nXDS XPARM/GXPARM files"
    )
    parser.add_argument(
        "path",
        nargs="+",
        help="Path(s) to XPARM.nXDS/GXPARM.nXDS file(s), or directories containing them"
    )
    parser.add_argument(
        "--prefer-xparm", action="store_true",
        help="Prefer XPARM.nXDS over GXPARM.nXDS (default: prefer GXPARM)"
    )
    parser.add_argument(
        "--output-plot", "-o", default=None,
        help="Save plot to this path (e.g., orientation_analysis.png)"
    )
    parser.add_argument(
        "--output-json", "-j", default=None,
        help="Save analysis results as JSON"
    )
    parser.add_argument(
        "--threshold", "-t", type=float, default=5.0,
        help="Misorientation threshold (degrees) for clustering distinct orientations (default: 5.0)"
    )
    parser.add_argument(
        "--no-symmetry", action="store_true",
        help="Ignore crystal symmetry in misorientation calculation (treat as triclinic)"
    )
    parser.add_argument(
        "--slow", action="store_true",
        help="Use original Kabsch alignment instead of fast vectorized einsum (default)"
    )
    parser.add_argument(
        "--compare", action="store_true",
        help="Run both Kabsch and fast methods and compare results"
    )
    parser.add_argument(
        "--to_xds", action="store_true",
        help="Convert each crystal orientation to a standard XDS XPARM file"
    )
    args = parser.parse_args()

    # Find XPARM files from all input paths
    files = []
    for p in args.path:
        files.extend(find_xparm_files(p, prefer_gxparm=not args.prefer_xparm))

    if not files:
        print(f"No XPARM.nXDS or GXPARM.nXDS files found in: {args.path}")
        sys.exit(1)

    print(f"Found {len(files)} file(s):")
    for f in files:
        print(f"  {f}")

    # Parse and merge all orientations
    all_images = []
    header = None
    for fp in files:
        print(f"\nParsing: {fp}")
        data = parse_nxds_xparm(str(fp))
        if header is None:
            header = data["header"]
        
        # If running in conversion mode, process immediately
        if args.to_xds:
             print(f"  Converting {len(data['images'])} orientations to XDS files...")
             import re
             for img in data["images"]:
                 # Determine output filename
                 # Try to extract image number
                 img_num = 1
                 # First try: specific pattern (digits before dot extension)
                 match = re.search(r"(\d+)(?=\.\w+$)", img["filename"])
                 if match:
                     img_num = int(match.group(1))
                 else:
                     # Second try: any sequence of digits at end of string
                     match = re.search(r"(\d+)$", img["filename"])
                     if match:
                         img_num = int(match.group(1))
                     else:
                        # Third try: any digits?
                        match = re.search(r"(\d+)", img["filename"])
                        if match:
                            img_num = int(match.group(1))
                 
                 # Determine prefix from directory header (first line of nXDS)
                 # Example: /path/to/L1..._run10_R93_??????.h5
                 # We want: L1..._run10_R93
                 dir_line = data["header"].get("directory", "")
                 prefix = "XPARM"
                 if dir_line:
                      template_name = Path(dir_line).name
                      # Remove _??????.h5 or similar pattern
                      # Matches _ followed by ?, *, or digits, then dot ext at end
                      prefix = re.sub(r'_[?*#\d]+\.[a-zA-Z0-9]+$', '', template_name)
                      if not prefix:
                          prefix = "XPARM"
                 
                 out_name = f"{prefix}_XPARM_{img_num}.XDS"
                 out_path = fp.parent / out_name
                 write_xds_file(img, data["header"], str(out_path), image_num=img_num)
             print(f"  Done. Files saved to {fp.parent}")

        n_img = len(data["images"])
        print(f"  Images indexed: {n_img}")
        print(f"  Space group: {data['header']['space_group']}")
        all_images.extend(data["images"])

    # If only converting, exit here
    if args.to_xds:
        print("\nConversion complete.")
        return

    print(f"\nTotal images from all files: {len(all_images)}")

    combined = {"header": header, "images": all_images}
    sg_override = 1 if args.no_symmetry else None
    use_fast = not args.slow or args.compare
    results = analyze_orientations(combined, cluster_threshold_deg=args.threshold,
                                   space_group=sg_override, fast=use_fast)

    # If --compare, also run the other method and report differences
    if args.compare:
        import time
        print("\n--- COMPARISON: Fast (einsum) vs Kabsch ---")
        sg = sg_override or header.get("space_group", 1)
        sym_ops = get_point_group_operators(sg)
        all_A = []
        for img in all_images:
            all_A.append(np.column_stack([img["a_axis"], img["b_axis"], img["c_axis"]]))
        A_stack = np.stack(all_A)

        t0 = time.perf_counter()
        dist_fast = pairwise_misorientation_condensed_fast(A_stack, sym_ops=sym_ops)
        t_fast = time.perf_counter() - t0

        t0 = time.perf_counter()
        dist_kabsch = pairwise_misorientation_condensed(A_stack, sym_ops=sym_ops)
        t_kabsch = time.perf_counter() - t0

        diff = np.abs(dist_fast - dist_kabsch)
        print(f"  Pairs compared: {len(diff):,}")
        print(f"  Fast time:   {t_fast:.2f}s")
        print(f"  Kabsch time: {t_kabsch:.2f}s")
        print(f"  Speedup:     {t_kabsch/t_fast:.1f}x")
        print(f"  Max |diff|:  {np.max(diff):.6f}°")
        print(f"  Mean |diff|: {np.mean(diff):.6f}°")
        print(f"  Median |diff|: {np.median(diff):.6f}°")
        print(f"  P95 |diff|:  {np.percentile(diff, 95):.6f}°")
        print(f"  P99 |diff|:  {np.percentile(diff, 99):.6f}°")
        n_large = np.sum(diff > 1.0)
        print(f"  Pairs with |diff| > 1°: {n_large} ({n_large/len(diff)*100:.2f}%)")

    # Print summary
    stats = results["statistics"]
    print("\n" + "=" * 60)
    print("ORIENTATION ANALYSIS RESULTS")
    print("=" * 60)
    print(f"Crystals analyzed: {stats['n_crystals']}")
    print(f"\nMisorientation statistics:")
    print(f"  Mean:  {stats['mean_misorientation_deg']}°")
    print(f"  Std:   {stats['std_misorientation_deg']}°")
    print(f"  Range: {stats['min_misorientation_deg']}° – {stats['max_misorientation_deg']}°")
    print(f"\nResultant vector lengths (0=random, 1=aligned):")
    print(f"  c-axis: {stats['c_axis_resultant_length']}")
    print(f"  a-axis: {stats['a_axis_resultant_length']}")
    print(f"\nAssessment: {stats['orientation_assessment']}")
    print(f"\nDistinct orientations: {stats['n_distinct_orientations']} "
          f"(threshold: {stats['cluster_threshold_deg']}°)")
    if stats['cluster_sizes']:
        print(f"  Cluster sizes (largest first): "
              f"{[sz for _, sz in stats['cluster_sizes']]}")

    # Unit cell statistics
    print(f"\nUnit cell statistics:")
    for param, vals in results["unit_cell_stats"].items():
        print(f"  {param:6s}: {vals['mean']:8.3f} ± {vals['std']:.3f}  "
              f"[{vals['min']:.3f} – {vals['max']:.3f}]")

    # Generate plot
    output_plot = args.output_plot
    if output_plot is None:
        # Default output next to first input
        p = Path(args.path[0])
        if p.is_dir():
            output_plot = str(p / "orientation_analysis.png")
        else:
            output_plot = str(p.parent / "orientation_analysis.png")

    plot_orientation_analysis(results, output_path=output_plot)

    # Save JSON if requested
    if args.output_json:
        # Convert numpy types for JSON serialization
        json_results = {
            "n_crystals": results["n_crystals"],
            "unit_cell_stats": results["unit_cell_stats"],
            "statistics": results["statistics"],
            "clustering": results["clustering"],
            "misorientation_histogram": results["misorientation_histogram"],
            "misorientation_bin_edges": results["misorientation_bin_edges"],
        }
        with open(args.output_json, "w") as f:
            json.dump(json_results, f, indent=2)
        print(f"\nJSON results saved to: {args.output_json}")


if __name__ == "__main__":
    main()
