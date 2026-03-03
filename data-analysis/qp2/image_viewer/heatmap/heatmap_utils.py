# In qp2/image_viewer/utils/heatmap_utils.py

import numpy as np


def find_heatmap_hotspots(
        data_matrix: np.ndarray,
        percentile_threshold: float,
        mode: str = "peaks",
        min_size: int = 1,
):
    """
    Finds contiguous regions (hotspots) in a 2D data matrix.

    Args:
        data_matrix (np.ndarray): The 2D heatmap data.
        percentile_threshold (float): The percentile (0-100) to use as the cutoff.
        mode (str): 'peaks' (higher is better) or 'valleys' (lower is better).
        min_size (int): The minimum number of cells a region must have.
    ...
    """
    from scipy import ndimage

    if data_matrix is None or data_matrix.size == 0:
        return []

    valid_data = data_matrix[~np.isnan(data_matrix)]
    if valid_data.size == 0:
        return []

    # --- START: CORRECTED LOGIC FOR VALLEYS ---
    if mode == "valleys":
        # For valleys, the percentile defines the *lower* bound.
        # e.g., a threshold of 10 means "find everything in the bottom 10%".
        # We find the threshold value at the LOW end of the distribution.
        threshold = np.percentile(valid_data, percentile_threshold)
        binary_mask = data_matrix <= threshold
    else:  # Default to 'peaks'
        # For peaks, the percentile defines the *upper* bound.
        # e.g., a threshold of 95 means "find everything in the top 5%".
        threshold = np.percentile(valid_data, percentile_threshold)
        binary_mask = data_matrix >= threshold
    # --- END: CORRECTED LOGIC ---

    nan_replacement = np.min(valid_data) - 1
    data_for_processing = np.nan_to_num(data_matrix, nan=nan_replacement)

    labels, num_features = ndimage.label(binary_mask)
    if num_features == 0:
        return []

    hotspots = []
    found_objects = ndimage.find_objects(labels)
    x_axis_vector = np.array([1.0, 0.0])  # Define the horizontal axis

    for i, obj_slice in enumerate(found_objects):
        label_index = i + 1

        region_mask = labels[obj_slice] == label_index
        region_size = np.sum(region_mask)

        if region_size >= min_size:
            coords = np.argwhere(region_mask)
            covariance_matrix = np.cov(coords, rowvar=False)

            if covariance_matrix.ndim == 2:
                eigenvalues, eigenvectors = np.linalg.eigh(covariance_matrix)
                order = eigenvalues.argsort()[::-1]
                eigenvalues = eigenvalues[order]
                eigenvectors = eigenvectors[:, order]

                # --- START: NEW ANGLE CALCULATION ---
                # Eigenvectors are (row, col) which is (y, x). We need (x, y) for angle math.
                # The columns of eigenvectors are the principal axes (Length, Width).
                axis_L = eigenvectors[:, 0][::-1]  # Major axis vector (x,y)
                axis_W = eigenvectors[:, 1][::-1]  # Minor axis vector (x,y)

                # Ensure vectors are consistently directed (e.g., pointing right) for stable angle calculation
                if axis_L[0] < 0:
                    axis_L *= -1
                if axis_W[0] < 0:
                    axis_W *= -1

                # Angle between major axis and x-axis
                angle_L_rad = np.arccos(np.dot(axis_L, x_axis_vector))

                # Angle from the horizontal axis to the major axis eigenvector
                angle_deg = np.degrees(np.arctan2(axis_L[1], axis_L[0]))

                angles_to_x = (
                    np.degrees(angle_L_rad),
                    90.0,
                )  # The second angle is always 90 deg off in 2D
                # --- END: NEW ANGLE CALCULATION ---

                width = 4 * np.sqrt(eigenvalues[0])
                height = 4 * np.sqrt(eigenvalues[1])
            else:
                angle_deg = 0
                width = 2
                height = 2
                angles_to_x = (0, 90)

            center_y_rel, center_x_rel = ndimage.center_of_mass(
                binary_mask[obj_slice], labels[obj_slice], label_index
            )

            center_x_abs = center_x_rel + obj_slice[1].start
            center_y_abs = center_y_rel + obj_slice[0].start

            hotspots.append(
                {
                    "center": (center_x_abs, center_y_abs),
                    "width": width,
                    "height": height,
                    "angle": angle_deg,
                    "angles_to_x": angles_to_x,
                }
            )

    return hotspots
