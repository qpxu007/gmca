# google pro 2.5

import math
import numpy as np
from typing import Dict, Any, List, Tuple, Optional

try:
    from tqdm import tqdm
except ImportError:

    def tqdm(iterable, *args, **kwargs):
        return iterable


# 'parse_mosflm_mat_full' function remains the same.
def parse_mosflm_mat_full(
    mat_file_path: str,
) -> Tuple[np.ndarray, np.ndarray, List[float]]:
    with open(mat_file_path, "r") as f:
        lines = f.readlines()
    if len(lines) < 10:
        raise ValueError(f"Invalid Mosflm matrix file: {mat_file_path}")
    a_matrix_rows = [list(map(float, line.strip().split())) for line in lines[0:3]]
    u_matrix_rows = [list(map(float, line.strip().split())) for line in lines[4:7]]
    unit_cell = list(map(float, lines[7].strip().split()))
    return np.array(a_matrix_rows), np.array(u_matrix_rows), unit_cell


def get_rotation_matrix(axis: np.ndarray, angle_rad: float) -> np.ndarray:
    """
    Calculates the 3D rotation matrix for a given axis and angle
    using Rodrigues' rotation formula.
    """
    axis = axis / np.linalg.norm(axis)
    kx, ky, kz = axis
    c, s, vc = math.cos(angle_rad), math.sin(angle_rad), 1 - math.cos(angle_rad)
    return np.array(
        [
            [kx * kx * vc + c, kx * ky * vc - kz * s, kx * kz * vc + ky * s],
            [ky * kx * vc + kz * s, ky * ky * vc + c, ky * kz * vc - kx * s],
            [kz * kx * vc - ky * s, kz * ky * vc + kx * s, kz * kz * vc + c],
        ]
    )


def generate_predictions_all_inclusive(
    mat_file_path: str,
    params: Dict[str, Any],
    image_number: Optional[int] = None,
    osc_range_deg: Optional[Tuple[float, float]] = None,
    resolution_limit: float = 1.5,
    mosaicity_deg: float = 0.0,
    rotation_axis: Tuple[float, float, float] = (0, 1, 0),
) -> List[Dict[str, Any]]:
    """
    Generates predictions with a generalized rotation axis, mosaicity,
    and flexible image/phi range input.
    """
    # --- Step 0: Validate inputs and determine oscillation range ---
    if osc_range_deg is None and image_number is None:
        raise ValueError("You must provide either 'image_number' or 'osc_range_deg'.")

    if osc_range_deg:
        phi_start_deg, phi_end_deg = osc_range_deg
        desc = f"Predicting for φ {phi_start_deg:.2f}-{phi_end_deg:.2f}°"
    else:
        omega_start = params["omega_start"]
        omega_range = params["omega_range"]
        phi_start_deg = omega_start + (image_number - 1) * omega_range
        phi_end_deg = phi_start_deg + omega_range
        desc = f"Predicting for Image {image_number}"

    # --- Step 1 & 2: Parse matrix, calculate B, get HKL limits ---
    a_matrix, u_matrix, _ = parse_mosflm_mat_full(mat_file_path)
    b_matrix = u_matrix.T @ a_matrix
    s_max = 1.0 / resolution_limit
    a_star_mag, b_star_mag, c_star_mag = np.linalg.norm(b_matrix, axis=1)
    h_max = math.ceil(s_max / a_star_mag) if a_star_mag > 1e-9 else 0
    k_max = math.ceil(s_max / b_star_mag) if b_star_mag > 1e-9 else 0
    l_max = math.ceil(s_max / c_star_mag) if c_star_mag > 1e-9 else 0

    # --- Step 3: Extract parameters and set up geometry ---
    wavelength = params["wavelength"]
    det_dist_mm = params["det_dist"]
    beam_center_px = (params["beam_x"], params["beam_y"])
    pixel_size_mm = params["pixel_size"]

    phi_start_rad, phi_end_rad = math.radians(phi_start_deg), math.radians(phi_end_deg)
    eta_rad = math.radians(mosaicity_deg)
    axis_vec = np.array(rotation_axis)

    R_start = get_rotation_matrix(axis_vec, phi_start_rad)
    R_end = get_rotation_matrix(axis_vec, phi_end_rad)
    s_inc = np.array([1 / wavelength, 0, 0])

    # --- Step 4: Loop through HKLs and predict ---
    all_predictions = []
    h_range = range(-h_max, h_max + 1)
    k_range = range(-k_max, k_max + 1)
    l_range = range(-l_max, l_max + 1)

    for h_idx in tqdm(h_range, desc=desc):
        for k_idx in k_range:
            for l_idx in l_range:
                if h_idx == 0 and k_idx == 0 and l_idx == 0:
                    continue

                hkl = (h_idx, k_idx, l_idx)
                p0 = a_matrix @ np.array(hkl).reshape(3, 1)
                p0_mag_sq = np.sum(p0**2)

                p_start, p_end = (R_start @ p0).flatten(), (R_end @ p0).flatten()

                f_start = 2 * np.dot(p_start, s_inc) + p0_mag_sq
                f_end = 2 * np.dot(p_end, s_inc) + p0_mag_sq

                # Mosaicity-broadened "active zone" for the diffraction condition function
                delta_f = p0_mag_sq * eta_rad

                # Check for overlap: [min(f_start, f_end), max(f_start, f_end)] vs [-delta_f, +delta_f]
                if min(f_start, f_end) <= delta_f and max(f_start, f_end) >= -delta_f:
                    # Reflection is visible. Approximate its position at the wedge center.
                    phi_mid_rad = (phi_start_rad + phi_end_rad) / 2.0
                    R_mid = get_rotation_matrix(axis_vec, phi_mid_rad)
                    p_mid = (R_mid @ p0).flatten()

                    s_diff = s_inc + p_mid

                    if s_diff[0] > 1e-9:
                        y_mm = det_dist_mm * s_diff[1] / s_diff[0]
                        z_mm = det_dist_mm * s_diff[2] / s_diff[0]
                        x_px = beam_center_px[0] + (z_mm / pixel_size_mm)
                        y_px = beam_center_px[1] - (y_mm / pixel_size_mm)

                        all_predictions.append(
                            {
                                "hkl": hkl,
                                "phi_deg": round(math.degrees(phi_mid_rad), 3),
                                "x_px": round(x_px, 2),
                                "y_px": round(y_px, 2),
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

    # --- Example 1: Vertical Spindle (Y-axis), 0.3 deg mosaicity ---
    print("\n--- PREDICTING FOR VERTICAL SPINDLE (Y-axis) ---")
    spots_Y = generate_predictions_all_inclusive(
        mat_file_path=mat_file,
        params=experimental_params,
        image_number=451,
        resolution_limit=2.0,
        mosaicity_deg=0.3,
        rotation_axis=(0, 1, 0),
    )
    print(f"Found {len(spots_Y)} predictions.")
    for spot in spots_Y[:5]:
        print(spot)

    # --- Example 2: Horizontal Spindle (Z-axis), same conditions ---
    print("\n--- PREDICTING FOR HORIZONTAL SPINDLE (Z-axis) ---")
    spots_Z = generate_predictions_all_inclusive(
        mat_file_path=mat_file,
        params=experimental_params,
        image_number=451,
        resolution_limit=2.0,
        mosaicity_deg=0.3,
        rotation_axis=(0, 0, 1),  # <-- Changed axis
    )
    print(f"Found {len(spots_Z)} predictions.")
    for spot in spots_Z[:5]:
        print(spot)

    # --- Example 3: 45-degree tilted spindle, large wedge ---
    print("\n--- PREDICTING FOR TILTED SPINDLE (45 deg Y-Z), 5-degree wedge ---")
    axis = [0, 1, 1]  # A vector tilted 45 degrees between Y and Z
    spots_tilted = generate_predictions_all_inclusive(
        mat_file_path=mat_file,
        params=experimental_params,
        osc_range_deg=(90.0, 95.0),  # Use explicit phi range
        resolution_limit=2.0,
        mosaicity_deg=0.3,
        rotation_axis=axis,
    )
    print(f"Found {len(spots_tilted)} predictions.")
    for spot in spots_tilted[:5]:
        print(spot)
