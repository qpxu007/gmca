# Create new file: qp2/image_viewer/utils/volume_utils.py

import numpy as np


def reconstruct_volume_with_shift(
        data_xy: np.ndarray,
        data_xz: np.ndarray,
        shift: float = 0.0,
        interpolation_order: int = 1
) -> np.ndarray:
    """
    Constructs a 3D volume from two orthogonal scans, handling different
    scan widths and applying a shift to the XZ scan via interpolation.

    The final volume's width (X-dimension) is determined by the narrower
    of the two input scans.

    Args:
        data_xy (np.ndarray): The XY scan data with shape (num_y, num_x_xy).
        data_xz (np.ndarray): The XZ scan data with shape (num_z, num_x_xz).
        shift (float): The shift of the XZ scan relative to the XY scan.
                       A positive value shifts XZ to the right.
        interpolation_order (int): The order of spline interpolation (1=linear, 3=cubic).

    Returns:
        np.ndarray: The reconstructed 3D volume.
    """
    from scipy import ndimage

    if data_xy is None or data_xz is None:
        return np.array([])

    # An optimization: if no shift and scans are same width, use simple multiplication
    if shift == 0.0 and data_xy.shape[1] == data_xz.shape[1]:
        return reconstruct_volume_from_scans(data_xy, data_xz)

    # --- Interpolation Logic ---
    # 1. Identify the narrower and wider scans. The narrower scan defines the output dimensions.
    if data_xy.shape[1] <= data_xz.shape[1]:
        narrower_scan, wider_scan = data_xy, data_xz
        # XZ is wider. To align, we sample it at coordinates shifted LEFT relative to its own grid.
        # This is equivalent to applying a NEGATIVE shift to the narrower scan's coordinates.
        applied_shift = -shift
    else:
        narrower_scan, wider_scan = data_xz, data_xy
        # XY is wider. To align, we sample it at coordinates shifted RIGHT relative to its own grid.
        # The 'shift' is XZ relative to XY, so this is a POSITIVE shift to the narrower scan's coords.
        applied_shift = shift

    h_narrow, w_narrow = narrower_scan.shape
    h_wide, w_wide = wider_scan.shape

    # 2. Prepare the wider scan for interpolation by handling existing NaNs.
    valid_data = wider_scan[~np.isnan(wider_scan)]
    nan_fill_value = np.min(valid_data) - 1 if valid_data.size > 0 else 0
    wider_scan_filled = np.nan_to_num(wider_scan, nan=nan_fill_value)

    # 3. Create the coordinate grid where we want to sample the wider scan.
    # The grid has the dimensions of the *output*, i.e., (h_wide, w_narrow).
    row_coords, col_coords = np.meshgrid(
        np.arange(h_wide),
        np.arange(w_narrow),
        indexing='ij'
    )

    # 4. Apply the shift to the column coordinates. This is the core of the alignment.
    col_coords_shifted = col_coords + applied_shift

    # 5. Perform the interpolation.
    interpolated_wider_scan = ndimage.map_coordinates(
        wider_scan_filled,
        [row_coords, col_coords_shifted],
        order=interpolation_order,
        mode='constant',
        cval=np.nan
    )

    # 6. Assign the interpolated and original scans back to their XY/XZ roles.
    if data_xy.shape[1] <= data_xz.shape[1]:
        final_xy = narrower_scan
        final_xz = interpolated_wider_scan
    else:
        final_xy = interpolated_wider_scan
        final_xz = narrower_scan

    # 7. Reconstruct the volume using the aligned scans.
    return reconstruct_volume_from_scans(final_xy, final_xz)


def reconstruct_volume_from_scans(data_xy: np.ndarray, data_xz: np.ndarray) -> np.ndarray:
    """
    Constructs a 3D volume from two orthogonal 2D scans by multiplying them.
    Assumes scans are already aligned and have the same width.

    Args:
        data_xy (np.ndarray): The XY scan data with shape (num_y, num_x).
        data_xz (np.ndarray): The XZ scan data with shape (num_z, num_x).

    Returns:
        np.ndarray: The reconstructed 3D volume with shape (num_z, num_y, num_x).
    """
    # Reshape arrays to be (1, num_y, num_x) and (num_z, 1, num_x)
    # NumPy's broadcasting will then automatically compute the outer product
    # along the new axes, resulting in a (num_z, num_y, num_x) volume.
    volume = data_xz[:, np.newaxis, :] * data_xy[np.newaxis, :, :]

    # Normalize the result to prevent extremely large values
    max_val = np.nanmax(volume)
    if max_val > 0:
        volume /= max_val

    return volume


def find_3d_hotspots(volume: np.ndarray, percentile_threshold: float, min_size: int = 3):
    """
    Finds contiguous 3D regions (hotspots) and calculates their center, value,
    dimensions, orientation, and volume metrics.

    Returns:
        List[dict]: A list of dictionaries, each describing a hotspot with keys:
                    'coords': (x, y, z) tuple for the center of mass.
                    'value': The data value at the center of mass.
                    'dimensions': (length, width, height) of the representative ellipsoid.
                    'orientation': The three principal axis vectors.
                    'voxel_count': The number of voxels in the hotspot.
                    'integrated_intensity': The background-subtracted sum of voxel values.
                    'angles_to_x': (angle_L, angle_W, angle_H) angles in degrees to the X-axis.
    """
    from scipy import ndimage

    if volume is None or volume.size == 0:
        return []

    valid_data = volume[~np.isnan(volume)]
    if valid_data.size == 0:
        return []

    nan_replacement = np.min(valid_data) - 1
    data_for_processing = np.nan_to_num(volume, nan=nan_replacement)

    threshold = np.percentile(valid_data, percentile_threshold)
    binary_mask = data_for_processing >= threshold

    structure = ndimage.generate_binary_structure(3, 1)
    labels, num_features = ndimage.label(binary_mask, structure=structure)
    if num_features == 0:
        return []

    hotspots = []
    found_objects = ndimage.find_objects(labels)

    # Define the experimental rotation axis (X-axis)
    x_axis_vector = np.array([1.0, 0.0, 0.0])

    for i, obj_slice in enumerate(found_objects):
        label_index = i + 1

        region_mask = (labels[obj_slice] == label_index)
        voxel_count = np.sum(region_mask)

        if voxel_count >= min_size:
            # --- START: VOLUME & INTENSITY CALCULATION ---
            hotspot_voxels = volume[obj_slice][region_mask]
            hotspot_voxels = hotspot_voxels[~np.isnan(hotspot_voxels)]
            integrated_intensity = np.sum(hotspot_voxels - threshold)
            # --- END: VOLUME & INTENSITY CALCULATION ---

            coords = np.argwhere(region_mask)
            covariance_matrix = np.cov(coords, rowvar=False)

            if np.linalg.matrix_rank(covariance_matrix) >= 3:
                eigenvalues, eigenvectors = np.linalg.eigh(covariance_matrix)
                order = eigenvalues.argsort()[::-1]
                eigenvalues = eigenvalues[order]
                eigenvectors = eigenvectors[:, order]
                dimensions = 4 * np.sqrt(eigenvalues)
                orientation = eigenvectors
            else:
                dimensions = np.array([1.0, 1.0, 1.0])
                orientation = np.identity(3)

            # --- START: NEW ANGLE CALCULATION ---
            # The eigenvectors are in (z, y, x) order because of np.argwhere.
            # We need to swap them to (x, y, z) to match our coordinate system.
            # Eigenvector columns correspond to Length, Width, Height axes.
            axis_L = orientation[:, 0][[2, 1, 0]]  # L-axis vector in (x,y,z)
            axis_W = orientation[:, 1][[2, 1, 0]]  # W-axis vector in (x,y,z)
            axis_H = orientation[:, 2][[2, 1, 0]]  # H-axis vector in (x,y,z)

            # Calculate the angle between each axis and the x-axis vector using the dot product.
            # cos(theta) = (A . B) / (|A| * |B|)
            # Since our vectors are unit vectors, |A|*|B| = 1, so cos(theta) = A . B
            angle_L_rad = np.arccos(np.dot(axis_L, x_axis_vector))
            angle_W_rad = np.arccos(np.dot(axis_W, x_axis_vector))
            angle_H_rad = np.arccos(np.dot(axis_H, x_axis_vector))

            angles_to_x = (
                np.degrees(angle_L_rad),
                np.degrees(angle_W_rad),
                np.degrees(angle_H_rad),
            )
            # --- END: NEW ANGLE CALCULATION ---

            center_z_rel, center_y_rel, center_x_rel = ndimage.center_of_mass(
                binary_mask[obj_slice], labels[obj_slice], label_index
            )

            abs_x = int(round(center_x_rel + obj_slice[2].start))
            abs_y = int(round(center_y_rel + obj_slice[1].start))
            abs_z = int(round(center_z_rel + obj_slice[0].start))

            peak_value = volume[abs_z, abs_y, abs_x]

            hotspots.append({
                "coords": (abs_x, abs_y, abs_z),
                "value": peak_value,
                "dimensions": tuple(dimensions),
                "orientation": orientation,
                "voxel_count": int(voxel_count),
                "integrated_intensity": float(integrated_intensity),
                "angles_to_x": angles_to_x,
            })

    hotspots.sort(key=lambda p: p['integrated_intensity'], reverse=True)
    return hotspots
