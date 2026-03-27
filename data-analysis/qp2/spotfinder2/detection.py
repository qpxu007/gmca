"""Spot detection: box-sum, NMS, CCL, and dispersion filter for spotfinder2.

Implements DOZOR-style integrate-then-threshold detection with DIALS-style
dispersion filtering for false positive reduction.

Performance: ~1s per frame for Eiger 16M (4150x4371) on CPU.
"""

import numpy as np
from scipy import ndimage as scipy_ndimage

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def detect_spots(
    frame, background, mask, geometry, backend, threshold_table,
    box_size=3, nms_size=5, min_pixels=2, max_pixels=200,
    n_sigma_b=6.0, n_sigma_s=3.0, dispersion_kernel=7,
):
    """Full detection pipeline: box-sum → NMS → CCL → dispersion filter.

    Returns:
        labels: 2D int32 array of connected-component labels (0 = background)
        n_spots: number of detected spots
        properties: dict with per-component properties (centroids, sizes, etc.)
    """
    xp = backend.xp
    ndi = backend.ndimage

    frame_f = frame.astype(np.float32)
    bg_f = background.astype(np.float32) if background.ndim == 2 else background.reshape(frame.shape).astype(np.float32)

    # Stage 1: Box-sum + NMS detection
    candidates = _box_sum_nms(frame_f, bg_f, mask, threshold_table, box_size, nms_size, ndi)

    # Stage 2: Expand candidates and apply CCL
    labels, n_spots = _expand_and_label(frame_f, bg_f, mask, candidates, threshold_table, ndi)

    # Stage 3: Size filter
    if n_spots > 0:
        labels, n_spots = _filter_by_size(labels, n_spots, min_pixels, max_pixels)

    # Stage 4: Dispersion filter (using scipy.ndimage for vectorized queries)
    if n_spots > 0:
        labels, n_spots = _dispersion_filter_fast(
            frame_f, labels, n_spots, ~mask,
            n_sigma_b, n_sigma_s, dispersion_kernel
        )

    # Compute per-component properties (vectorized)
    properties = _compute_properties_fast(frame_f, bg_f, labels, n_spots, geometry)

    return labels, n_spots, properties


def _box_sum_nms(frame, bg, mask, threshold_table, box_size, nms_size, ndi):
    """DOZOR-style: uniform_filter convolution + NMS."""
    # Box-sum convolution
    box_sum = ndi.uniform_filter(frame, size=box_size, mode="constant", cval=0)
    box_sum *= (box_size ** 2)

    # Background sum in box
    bg_sum = ndi.uniform_filter(bg, size=box_size, mode="constant", cval=0)
    bg_sum *= (box_size ** 2)

    # Threshold for box sum
    bg_per_pixel = np.maximum(bg_sum / (box_size ** 2), 0)
    thresh = threshold_table.for_box_sum(bg_per_pixel, box_size)

    # Above threshold
    above = box_sum > thresh

    # Non-maximum suppression
    local_max = ndi.maximum_filter(box_sum, size=nms_size, mode="constant", cval=0)
    is_max = (box_sum >= local_max) & (box_sum > 0)

    # Center pixel must exceed background
    center_above_bg = frame > bg

    # Combined
    candidates = above & is_max & center_above_bg & (~mask)
    return candidates


def _expand_and_label(frame, bg, mask, candidates, threshold_table, ndi):
    """Expand candidate centers into connected regions and label."""
    # Per-pixel threshold for expansion
    per_pixel_thresh = threshold_table(np.maximum(bg, 0))

    # Pixels above per-pixel threshold
    above_pixel = (frame > per_pixel_thresh) & (~mask)

    # Dilate candidates, intersect with above-threshold
    struct = ndi.generate_binary_structure(2, 2)  # 8-connectivity
    dilated = ndi.binary_dilation(candidates, structure=struct, iterations=2)
    region_mask = (dilated & above_pixel) | candidates

    labels, n_features = ndi.label(region_mask, structure=struct)
    return labels, n_features


def _filter_by_size(labels, n_spots, min_pixels, max_pixels):
    """Remove components outside size range using bincount (no loops)."""
    sizes = np.bincount(labels.ravel())

    keep = np.zeros(len(sizes), dtype=bool)
    keep[1:] = (sizes[1:] >= min_pixels) & (sizes[1:] <= max_pixels)

    remap = np.zeros(len(sizes), dtype=np.int32)
    new_id = 0
    for i in range(1, len(sizes)):
        if keep[i]:
            new_id += 1
            remap[i] = new_id

    labels = remap[labels]
    return labels, new_id


def _dispersion_filter_fast(frame, labels, n_spots, valid_mask, n_sigma_b, n_sigma_s, kernel_size):
    """Vectorized DIALS-style dispersion test.

    Uses scipy.ndimage.find_objects for fast component access
    and uniform_filter for local statistics (avoids integral image loops).
    """
    if n_spots == 0:
        return labels, 0

    # Compute local statistics using uniform_filter (vectorized over whole image)
    half_k = kernel_size // 2
    frame_valid = np.where(valid_mask, frame, 0).astype(np.float64)
    frame_sq = frame_valid ** 2
    count_valid = valid_mask.astype(np.float64)

    # Local sums via uniform_filter (O(N), kernel-size independent)
    ksize = 2 * half_k + 1
    local_sum = scipy_ndimage.uniform_filter(frame_valid, size=ksize, mode="constant") * (ksize ** 2)
    local_sum_sq = scipy_ndimage.uniform_filter(frame_sq, size=ksize, mode="constant") * (ksize ** 2)
    local_count = scipy_ndimage.uniform_filter(count_valid, size=ksize, mode="constant") * (ksize ** 2)

    # Use find_objects for fast per-component access (returns bounding box slices)
    slices = scipy_ndimage.find_objects(labels)

    keep = np.ones(n_spots + 1, dtype=bool)
    keep[0] = False

    for comp_id in range(1, n_spots + 1):
        sl = slices[comp_id - 1]
        if sl is None:
            keep[comp_id] = False
            continue

        comp_mask = labels[sl] == comp_id
        if comp_mask.sum() == 0:
            keep[comp_id] = False
            continue

        # Centroid within bounding box
        yy, xx = np.where(comp_mask)
        cy = sl[0].start + int(yy.mean())
        cx = sl[1].start + int(xx.mean())

        # Bounds check
        ny, nx = frame.shape
        cy = np.clip(cy, half_k, ny - half_k - 1)
        cx = np.clip(cx, half_k, nx - half_k - 1)

        m = local_count[cy, cx]
        if m < 4:
            keep[comp_id] = False
            continue

        x_sum = local_sum[cy, cx]
        y_sum = local_sum_sq[cy, cx]

        # Dispersion test: variance exceeds Poisson expectation?
        a = m * y_sum - x_sum * x_sum - x_sum * (m - 1)
        c = x_sum * n_sigma_b * np.sqrt(2.0 * max(m - 1, 1))

        # Strong pixel test at peak pixel in component
        comp_frame = frame[sl]
        peak_val = float(comp_frame[comp_mask].max())
        b = m * peak_val - x_sum
        d = n_sigma_s * np.sqrt(max(x_sum * m, 0))

        if not (a > c and b > d):
            keep[comp_id] = False

    # Remap
    remap = np.zeros(n_spots + 1, dtype=np.int32)
    new_id = 0
    for i in range(1, n_spots + 1):
        if keep[i]:
            new_id += 1
            remap[i] = new_id

    n_rejected = n_spots - new_id
    if n_rejected > 0:
        logger.debug(f"Dispersion filter: rejected {n_rejected}/{n_spots} components")

    return remap[labels], new_id


def _compute_properties_fast(frame, background, labels, n_spots, geometry):
    """Vectorized per-component property computation using find_objects."""
    empty = {
        "x": np.array([], dtype=np.float32),
        "y": np.array([], dtype=np.float32),
        "intensity": np.array([], dtype=np.float32),
        "background": np.array([], dtype=np.float32),
        "snr": np.array([], dtype=np.float32),
        "resolution": np.array([], dtype=np.float32),
        "size": np.array([], dtype=np.int32),
    }
    if n_spots == 0:
        return empty

    slices = scipy_ndimage.find_objects(labels)

    x_arr = np.zeros(n_spots, dtype=np.float32)
    y_arr = np.zeros(n_spots, dtype=np.float32)
    intensity_arr = np.zeros(n_spots, dtype=np.float32)
    bg_arr = np.zeros(n_spots, dtype=np.float32)
    snr_arr = np.zeros(n_spots, dtype=np.float32)
    res_arr = np.zeros(n_spots, dtype=np.float32)
    size_arr = np.zeros(n_spots, dtype=np.int32)

    for comp_id in range(1, n_spots + 1):
        idx = comp_id - 1
        sl = slices[idx]
        if sl is None:
            continue

        comp_mask = labels[sl] == comp_id
        yy_local, xx_local = np.where(comp_mask)
        n_pix = len(yy_local)
        if n_pix == 0:
            continue

        size_arr[idx] = n_pix

        # Global coordinates
        yy = yy_local + sl[0].start
        xx = xx_local + sl[1].start

        # Intensities
        pixel_vals = frame[yy, xx].astype(np.float64)
        bg_vals = background[yy, xx].astype(np.float64)
        net_vals = np.maximum(pixel_vals - bg_vals, 0)
        total_net = net_vals.sum()

        # Background-subtracted weighted centroid
        if total_net > 0:
            x_arr[idx] = (net_vals * xx).sum() / total_net
            y_arr[idx] = (net_vals * yy).sum() / total_net
        else:
            x_arr[idx] = xx.mean()
            y_arr[idx] = yy.mean()

        intensity_arr[idx] = total_net
        bg_arr[idx] = bg_vals.mean()

        # SNR
        bg_total = bg_vals.sum()
        snr_arr[idx] = total_net / np.sqrt(max(bg_total, 1.0))

        # Resolution at centroid
        cx_int = int(round(x_arr[idx]))
        cy_int = int(round(y_arr[idx]))
        ny, nx = geometry.resolution_map.shape
        if 0 <= cy_int < ny and 0 <= cx_int < nx:
            res_arr[idx] = geometry.resolution_map[cy_int, cx_int]

    return {
        "x": x_arr, "y": y_arr,
        "intensity": intensity_arr, "background": bg_arr,
        "snr": snr_arr, "resolution": res_arr,
        "size": size_arr,
    }
