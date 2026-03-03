import math
import numpy as np
from typing import Dict, Any, List, Tuple, Optional

try:
    from tqdm import tqdm
except ImportError:

    def tqdm(iterable, *args, **kwargs):
        return iterable


# ---- File parsing ----
def parse_mosflm_mat_full(
    mat_file_path: str,
) -> Tuple[np.ndarray, np.ndarray, List[float]]:
    """
    Parse a MOSFLM ASCII matrix file (e.g., autoindex.mat/NEWMAT-like):
    lines[0:3] -> A matrix rows
    lines[4:7] -> U matrix rows
    lines[7]   -> unit cell (a b c alpha beta gamma)

    Returns:
        A (3x3), U (3x3), unit_cell [a,b,c,alpha,beta,gamma]
    """
    with open(mat_file_path, "r") as f:
        lines = f.readlines()
    if len(lines) < 10:
        raise ValueError(f"Invalid Mosflm matrix file: {mat_file_path}")
    a_matrix_rows = [list(map(float, lines[i].strip().split())) for i in range(0, 3)]
    u_matrix_rows = [list(map(float, lines[i].strip().split())) for i in range(4, 7)]
    unit_cell = list(map(float, lines[7].strip().split()))
    return (
        np.array(a_matrix_rows, dtype=float),
        np.array(u_matrix_rows, dtype=float),
        unit_cell,
    )


# ---- Geometry helpers ----
def normalize_angle_deg(phi: float) -> float:
    """Normalize angle to (-180, 180]."""
    x = (phi + 180.0) % 360.0 - 180.0
    # Map -180 to 180 for a consistent half-open interval
    return 180.0 if abs(x + 180.0) < 1e-12 else x


def interval_contains_angle(phi: float, start: float, end: float) -> bool:
    """
    Check if angle phi (deg) lies in circular interval [start, end] (deg), allowing wrap-around.
    Both phi, start, end may be any real numbers.
    """
    phi = (phi % 360.0 + 360.0) % 360.0
    s = (start % 360.0 + 360.0) % 360.0
    e = (end % 360.0 + 360.0) % 360.0
    if s <= e:
        return s - 1e-9 <= phi <= e + 1e-9
    else:
        # wrapped interval
        return phi >= s - 1e-9 or phi <= e + 1e-9


def get_rotation_matrix(axis: np.ndarray, angle_rad: float) -> np.ndarray:
    """
    Rodrigues' rotation formula for unit axis and angle.
    """
    axis = np.asarray(axis, dtype=float)
    axis = axis / np.linalg.norm(axis)
    kx, ky, kz = axis
    c, s, vc = math.cos(angle_rad), math.sin(angle_rad), 1.0 - math.cos(angle_rad)
    return np.array(
        [
            [kx * kx * vc + c, kx * ky * vc - kz * s, kx * kz * vc + ky * s],
            [ky * kx * vc + kz * s, ky * ky * vc + c, ky * kz * vc - kx * s],
            [kz * kx * vc - ky * s, kz * ky * vc + kx * s, kz * kz * vc + c],
        ],
        dtype=float,
    )


def solve_ewald_roots_general_axis(
    p0: np.ndarray,
    u_axis: np.ndarray,
    s0: np.ndarray,
) -> Optional[Tuple[float, float]]:
    """
    Solve f(φ) = 2 s0·pφ + |p0|^2 = 0 for rotation about unit axis u_axis.

    Decompose p0 into components parallel and perpendicular to u:
      p_par   = (u·p0) u
      p_perp  = p0 - p_par
      q       = u × p_perp

    Then s0·pφ = s0·p_par + cosφ s0·p_perp + sinφ s0·q.

    Let:
      A = 2 (s0·p_perp)
      B = 2 (s0·q)
      C = 2 (s0·p_par) + |p0|^2

    Solve A cosφ + B sinφ + C = 0 → R cos(φ - φ0) + C = 0
      with R = sqrt(A^2 + B^2), φ0 = atan2(B, A),
      solutions exist if |C| ≤ R, giving φ = φ0 ± arccos(-C / R).

    Returns:
      (phi1_rad, phi2_rad) if solutions exist; otherwise None.
    """
    u = u_axis / np.linalg.norm(u_axis)
    p0 = p0.reshape(3)
    p_par = np.dot(u, p0) * u
    p_perp = p0 - p_par
    q = np.cross(u, p_perp)

    A = 2.0 * np.dot(s0, p_perp)
    B = 2.0 * np.dot(s0, q)
    C = 2.0 * np.dot(s0, p_par) + float(np.dot(p0, p0))

    R = math.hypot(A, B)
    if R < 1e-12:
        # No φ dependence; accept only if C ≈ 0 (tangent case), else no solution
        if abs(C) < 1e-12:
            # Entire circle satisfies; return two canonical angles
            return 0.0, math.pi
        return None

    x = -C / R
    if x < -1.0 - 1e-12 or x > 1.0 + 1e-12:
        return None
    # Clamp for numeric stability
    x = min(1.0, max(-1.0, x))

    phi0 = math.atan2(B, A)
    delta = math.acos(x)
    return (phi0 - delta, phi0 + delta)


# ---- Main predictor ----
def generate_predictions(
    mat_file_path: str,
    params: Dict[str, Any],
    image_number: Optional[int] = None,
    osc_range_deg: Optional[Tuple[float, float]] = None,
    resolution_limit: float = 1.5,
    mosaicity_deg: float = 0.0,
    rotation_axis: Tuple[float, float, float] = (0.0, 1.0, 0.0),
) -> List[Dict[str, Any]]:
    """
    Predict reflections for a given image or explicit φ range, using:
      - MOSFLM orientation A, U and column-norm reciprocal bounds,
      - general-axis rotation with exact φ root solving,
      - mosaicity as a ±η/2 expansion of the allowed φ wedge.

    Assumptions:
      - Beam is along +X of the lab frame: s0 = (1/λ, 0, 0).
      - Detector is planar and orthogonal to +X with distance 'det_dist' (mm).
      - Beam center (pixels) and pixel size (mm) map millimeters to pixels.
    """
    # 0) Determine φ range (deg)
    if osc_range_deg is None and image_number is None:
        raise ValueError("Provide either 'image_number' or 'osc_range_deg'.")

    if osc_range_deg is not None:
        phi_start_deg, phi_end_deg = float(osc_range_deg[0]), float(osc_range_deg[1])
    else:
        omega_start = float(params["omega_start"])
        omega_range = float(params["omega_range"])
        phi_start_deg = omega_start + (int(image_number) - 1) * omega_range
        phi_end_deg = phi_start_deg + omega_range

    # 1) Parse matrices and compute reciprocal basis B and HKL bounds
    A, U, _ = parse_mosflm_mat_full(mat_file_path)
    # A = U B  ⇒  B = U^T A
    B = U.T @ A
    # Column norms give |a*|, |b*|, |c*|
    a_star_mag, b_star_mag, c_star_mag = np.linalg.norm(B, axis=0)

    s_max = 1.0 / float(resolution_limit)
    h_max = math.ceil(s_max / a_star_mag) if a_star_mag > 1e-12 else 0
    k_max = math.ceil(s_max / b_star_mag) if b_star_mag > 1e-12 else 0
    l_max = math.ceil(s_max / c_star_mag) if c_star_mag > 1e-12 else 0

    # 2) Experimental geometry
    wavelength = float(params["wavelength"])
    det_dist_mm = float(params["det_dist"])
    pixel_size_mm = float(params["pixel_size"])
    beam_x_px = float(params["beam_x"])
    beam_y_px = float(params["beam_y"])

    # Lab-frame incident vector and rotation axis
    s0 = np.array([1.0 / wavelength, 0.0, 0.0], dtype=float)
    u_axis = np.array(rotation_axis, dtype=float)
    u_axis /= np.linalg.norm(u_axis)

    # φ in radians
    phi_start_deg = normalize_angle_deg(phi_start_deg)
    phi_end_deg = normalize_angle_deg(phi_end_deg)
    eta_deg = float(mosaicity_deg)

    # Expand wedge by ±η/2 for mosaic acceptance
    phi_gate_start = phi_start_deg - eta_deg / 2.0
    phi_gate_end = phi_end_deg + eta_deg / 2.0

    all_predictions: List[Dict[str, Any]] = []

    h_range = range(-h_max, h_max + 1)
    k_range = range(-k_max, k_max + 1)
    l_range = range(-l_max, l_max + 1)

    for h in tqdm(h_range, desc=f"Predict φ {phi_start_deg:.2f}-{phi_end_deg:.2f}°"):
        for k in k_range:
            for l in l_range:
                if h == 0 and k == 0 and l == 0:
                    continue

                hkl = np.array([h, k, l], dtype=float)
                p0 = A @ hkl  # reciprocal vector in lab frame
                p0_norm = float(np.linalg.norm(p0))
                if p0_norm < 1e-16:
                    continue
                # d-spacing filter: d = 1/||p0||  <= d_min ⇒ ||p0|| >= 1/d_min = s_max
                if p0_norm > s_max + 1e-12:
                    continue

                roots = solve_ewald_roots_general_axis(p0, u_axis, s0)
                if roots is None:
                    continue

                for phi_rad in roots:
                    # Normalize φ to degrees for gating on the circle
                    phi_deg = math.degrees(phi_rad)
                    phi_deg = normalize_angle_deg(phi_deg)

                    # Check if root is within wedge expanded by mosaicity
                    if not interval_contains_angle(
                        phi_deg, phi_gate_start, phi_gate_end
                    ):
                        continue

                    # Project at this actual diffracting φ
                    R = get_rotation_matrix(u_axis, math.radians(phi_deg))
                    p_phi = R @ p0
                    s_diff = s0 + p_phi

                    # Intersect with detector plane x = +det_dist (beam along +X)
                    if s_diff[0] <= 1e-12:
                        continue

                    y_mm = det_dist_mm * (s_diff[1] / s_diff[0])
                    z_mm = det_dist_mm * (s_diff[2] / s_diff[0])
                    x_px = beam_x_px + (z_mm / pixel_size_mm)
                    y_px = beam_y_px - (y_mm / pixel_size_mm)

                    all_predictions.append(
                        {
                            "hkl": (int(h), int(k), int(l)),
                            "phi_deg": round(phi_deg, 4),
                            "x_px": round(float(x_px), 2),
                            "y_px": round(float(y_px), 2),
                        }
                    )

    return all_predictions


if __name__ == "__main__":
    mat_file = "autoindex.mat"
    experimental_params = {
        "wavelength": 0.9778,
        "det_dist": 350.5,
        "pixel_size": 0.075,
        "beam_x": 1552.0,
        "beam_y": 1610.0,
        "omega_start": 0.0,
        "omega_range": 0.2,
    }

    # Example 1: vertical spindle (Y-axis), image number with mosaicity
    spots_y = generate_predictions(
        mat_file_path=mat_file,
        params=experimental_params,
        image_number=451,
        resolution_limit=2.0,
        mosaicity_deg=0.3,
        rotation_axis=(0.0, 1.0, 0.0),
    )
    print(f"Y-axis: {len(spots_y)} predictions")
    print(spots_y[:5])

    # Example 2: horizontal spindle (Z-axis), same conditions
    spots_z = generate_predictions(
        mat_file_path=mat_file,
        params=experimental_params,
        image_number=451,
        resolution_limit=2.0,
        mosaicity_deg=0.3,
        rotation_axis=(0.0, 0.0, 1.0),
    )
    print(f"Z-axis: {len(spots_z)} predictions")
    print(spots_z[:5])

    # Example 3: tilted spindle, explicit wedge
    spots_tilt = generate_predictions(
        mat_file_path=mat_file,
        params=experimental_params,
        osc_range_deg=(90.0, 95.0),
        resolution_limit=2.0,
        mosaicity_deg=0.3,
        rotation_axis=(0.0, 1.0, 1.0),
    )
    print(f"Tilted-axis (90-95°): {len(spots_tilt)} predictions")
    print(spots_tilt[:5])
