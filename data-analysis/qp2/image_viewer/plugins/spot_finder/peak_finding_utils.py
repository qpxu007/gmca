# qp2/image_viewer/plugins/spot_finder/peak_finding_utils.py

import numpy as np
from typing import Optional, Tuple
import functools
import time

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def timeit(func):
    """
    Decorator that reports the execution time of the decorated function.
    """

    # Preserves function metadata like __name__ and __doc__
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = (
            time.time()
        )  # perf_counter() if python 3.3  # Get start time using a high-resolution clock
        value = func(*args, **kwargs)  # Call the original function
        end_time = time.time()  # perf_counter()  # Get end time
        run_time = end_time - start_time  # Calculate duration
        # Print the function name, arguments, and execution time
        # print(f"Function {func.__name__}{args} {kwargs} Took {run_time:.4f} seconds")
        logger.debug(f"timeit: Function {func.__name__} Took {run_time:.4f} seconds")

        return value  # Return the original function's result

    return wrapper_timer


def peak_radial_distribution(
    peaks, beam_x, beam_y, bin_width=1.0, r_min=None, r_max=None
):
    # peaks: Nx2 array of (row, col) coordinates from find_peaks_in_annulus
    # beam_x, beam_y: center coordinates
    # Calculate radial distances
    # r_centers, counts = radial_distribution(peaks, beam_x, beam_y, bin_width=2.0)
    dists = np.sqrt((peaks[:, 1] - beam_x) ** 2 + (peaks[:, 0] - beam_y) ** 2)
    if r_min is None:
        r_min = dists.min()
    if r_max is None:
        r_max = dists.max()
    bins = np.arange(r_min, r_max + bin_width, bin_width)
    hist, edges = np.histogram(dists, bins=bins)
    return edges[:-1], hist  # bin centers and counts


@timeit
def find_peaks_in_annulus(image, detector_mask, beam_x, beam_y, r1, r2, **kwargs):
    """
    Find local maxima in an annular region between r1 and r2, with advanced filtering.
    """
    from skimage.morphology import disk
    from scipy import ndimage  # Import ndimage for labeling
    from skimage.feature import peak_local_max

    if image is None or image.ndim != 2:
        logger.error("find_peaks_in_annulus: Invalid image data for peak finding.")
        return np.empty((0, 2), dtype=int)
    if r1 >= r2:
        logger.warning(
            f"find_peaks_in_annulus: Inner radius r1 ({r1}) >= outer radius r2 ({r2}). No annulus."
        )
        return np.empty((0, 2), dtype=int)

    h, w = image.shape

    # 1. Crop image to bounding box
    x_min = max(0, int(np.floor(beam_x - r2)))
    x_max = min(w, int(np.ceil(beam_x + r2)) + 1)
    y_min = max(0, int(np.floor(beam_y - r2)))
    y_max = min(h, int(np.ceil(beam_y + r2)) + 1)

    if x_min >= x_max or y_min >= y_max:
        logger.warning("find_peaks_in_annulus: Annulus bounding box is empty.")
        return np.empty((0, 2), dtype=int)

    sub_img = image[y_min:y_max, x_min:x_max]

    # 2. Create masks relative to the sub-image
    y_sub_coords, x_sub_coords = np.indices(sub_img.shape)
    x_orig_coords = x_sub_coords + x_min
    y_orig_coords = y_sub_coords + y_min
    dist_sq = (x_orig_coords - beam_x) ** 2 + (y_orig_coords - beam_y) ** 2
    annulus_mask_sub = (dist_sq >= r1**2) & (dist_sq <= r2**2)

    if detector_mask is not None and detector_mask.shape == image.shape:
        detector_mask_sub = ~detector_mask[y_min:y_max, x_min:x_max]
    else:
        detector_mask_sub = np.ones(sub_img.shape, dtype=bool)

    final_mask_sub = annulus_mask_sub & detector_mask_sub

    if not np.any(final_mask_sub):
        logger.warning("find_peaks_in_annulus: Annular mask is empty.")
        return np.empty((0, 2), dtype=int)

    # 3. Prepare image for peak finding
    image_for_peaks = sub_img.astype(np.float32, copy=True)
    valid_data_sub = sub_img[final_mask_sub]
    masked_value = (
        float(np.min(valid_data_sub)) - 1 if valid_data_sub.size > 0 else -np.inf
    )
    image_for_peaks[~final_mask_sub] = masked_value

    # 4. Calculate threshold
    default_threshold = (
        np.percentile(valid_data_sub, 95) if valid_data_sub.size > 0 else 0.0
    )
    threshold_abs = float(kwargs.get("threshold_abs", default_threshold))

    # 5. Apply median filter if configured
    median_filter_size = kwargs.get("median_filter_size", None)
    if median_filter_size is not None:
        from cv2 import medianBlur

        image_for_peaks = medianBlur(image_for_peaks, median_filter_size)

    # 6. Get other peak finding parameters
    min_distance0 = 3
    num_peaks = kwargs.get("num_peaks", 200)
    exclude_border = kwargs.get("exclude_border", False)
    footprint = disk(max(1, min_distance0 // 2))
    zscore_cutoff = kwargs.get("zscore_cutoff", 3)
    min_pixels_per_peak = kwargs.get("min_pixels_per_peak", 1)  # NEW

    # 7. Find initial peaks
    try:
        peaks_sub = peak_local_max(
            image_for_peaks,
            min_distance=min_distance0,
            threshold_abs=threshold_abs,
            num_peaks=num_peaks,
            exclude_border=exclude_border,
            footprint=footprint,
        )
    except Exception as e:
        logger.error(
            f"find_peaks_in_annulus: Error during peak_local_max: {e}", exc_info=True
        )
        return np.empty((0, 2), dtype=int)

    if peaks_sub.size == 0:
        return np.empty((0, 2), dtype=int)

    # --- START: NEW FAST PIXEL COUNT FILTERING ---
    # if min_pixels_per_peak > 1:
    #     # a. Create a binary mask of all pixels above threshold
    #     binary_mask = image_for_peaks >= threshold_abs

    #     # b. Label contiguous regions (blobs)
    #     labels, num_features = ndimage.label(binary_mask)

    #     if num_features > 0:
    #         # c. Get the label ID for each peak coordinate
    #         peak_labels = labels[peaks_sub[:, 0], peaks_sub[:, 1]]

    #         # d. Count the size of each labeled region
    #         # np.bincount is faster than ndimage.sum for this
    #         label_sizes = np.bincount(labels.ravel())

    #         # e. Find which peaks are in blobs that are large enough
    #         # We look up the size for each peak's label
    #         is_large_enough = label_sizes[peak_labels] >= min_pixels_per_peak

    #         # f. Keep only the peaks that meet the criteria
    #         peaks_sub = peaks_sub[is_large_enough]
    #         logger.debug(
    #             f"After min_pixels filter ({min_pixels_per_peak} px), {len(peaks_sub)} peaks remain."
    #         )
    # --- END: NEW FAST PIXEL COUNT FILTERING ---

    # 8. Filter by Z-score (SNR) and number of significant pixels in a peak
    if peaks_sub.size > 0:
        snr_results = calculate_peak_significance_poisson(
            image_for_peaks, peaks_sub, r0=3, r1=5, r2=7
        )
        results_array = np.array(snr_results)
        z_mask = results_array[:, -1] >= zscore_cutoff

        num_above_vec = np.fromiter(
            (
                d.get("num_above_bg_robust", 0) for d in results_array[:, -2]
            ),  # dict at index -2
            dtype=int,
            count=len(results_array),
        )
        size_mask = num_above_vec >= min_pixels_per_peak

        peaks_sub = peaks_sub[z_mask & size_mask]
        logger.debug(
            f"After Z-score and size filter (>{zscore_cutoff} and {min_pixels_per_peak} px), {len(peaks_sub)} peaks remain."
        )

    # 9. Filter by final minimum distance
    if peaks_sub.size > 0:
        min_distance = kwargs.get("min_distance", min_distance0)
        if min_distance > min_distance0:
            values = image_for_peaks[peaks_sub[:, 0], peaks_sub[:, 1]]
            filtered_peaks, _ = filter_close_peaks(peaks_sub, values, d0=min_distance)
            peaks_sub = filtered_peaks
            logger.debug(
                f"After min_distance filter ({min_distance} px), {len(peaks_sub)} peaks remain."
            )

    # 10. Convert back to original image coordinates and return
    resolution_bins_in_pixels = kwargs.get("resolutions_bins_in_pixels", None)
    if peaks_sub.size > 0:
        peaks = peaks_sub + np.array([y_min, x_min])
        if resolution_bins_in_pixels:
            peaks = analyze_peak_distribution(
                peaks, beam_x, beam_y, resolution_bins_in_pixels
            )
    else:
        peaks = np.empty((0, 2), dtype=int)

    return peaks


@timeit
def filter_close_peaks_sort(peaks, values, d0):
    from scipy.spatial import cKDTree

    order = np.argsort(-values)
    peaks_sorted = peaks[order]
    kept_indices = []
    tree = cKDTree(peaks_sorted)
    removed = np.zeros(len(peaks_sorted), dtype=bool)
    for i, coord in enumerate(peaks_sorted):
        if removed[i]:
            continue
        kept_indices.append(order[i])
        idxs = tree.query_ball_point(coord, d0)
        for idx in idxs:
            if idx > i:
                removed[idx] = True
    kept_indices = np.array(kept_indices)
    return peaks[kept_indices], values[kept_indices]


@timeit
def filter_close_peaks(peaks, values, d0):
    from scipy.spatial import cKDTree

    # No sorting: operate in input order
    tree = cKDTree(peaks)
    removed = np.zeros(len(peaks), dtype=bool)
    kept_indices = []

    for i, coord in enumerate(peaks):
        if removed[i]:
            continue
        kept_indices.append(i)  # keep original index
        idxs = tree.query_ball_point(coord, d0)
        for idx in idxs:
            if idx > i:
                removed[idx] = True

    kept_indices = np.array(kept_indices, dtype=int)
    return peaks[kept_indices], values[kept_indices]


@timeit
def calculate_peak_significance_poisson(image, peak_coords, r0=3, r1=5, r2=7):
    """
    Calculate the significance (Z-score) for each peak assuming Poisson noise.

    The Z-score measures how many standard deviations the observed signal sum
    in the peak region (disk r0) is above the expected sum if that region
    contained only background noise, assuming the background follows Poisson statistics
    (variance = mean).

    Parameters:
    - image (np.ndarray): 2D numpy array representing the image.
    - peak_coords (np.ndarray): Nx2 array of (row, col) coordinates for peaks.
    - r0 (float): Radius of the disk for summing signal intensity.
    - r1 (float): Inner radius of the annulus for background estimation.
    - r2 (float): Outer radius of the annulus for background estimation.

    Returns:
    - list: A list of tuples, where each tuple contains:
        (peak_row, peak_col, signal_sum, num_signal_pixels,
         background_mean, background_std, z_score)
        Returns np.nan for background/z_score if the background region is empty
        or background_mean is non-positive.

    # Example Usage (assuming you have an image 'img' and peak coordinates 'peaks'):
    # r0, r1, r2 = 3, 5, 8 # Example radii
    # peak_stats = calculate_peak_significance_poisson(img, peaks, r0, r1, r2)
    #
    # # Filter for significant peaks (e.g., Z-score > 3)
    # significant_peaks = [p for p in peak_stats if not np.isnan(p[-1]) and p[-1] > 3.0]
    #
    # for peak_info in significant_peaks:
    #    print(f"Peak at ({peak_info[0]}, {peak_info[1]}) is significant with Z-score: {peak_info[-1]:.2f}")


    """
    if not (0 <= r0 and 0 <= r1 < r2):
        raise ValueError("Radii must satisfy 0 <= r0, 0 <= r1 < r2")

    h, w = image.shape
    results = []
    # Create coordinate grids relative to a potential patch center
    max_radius = int(np.ceil(r2))
    patch_diameter = 2 * max_radius + 1
    y_grid, x_grid = np.indices((patch_diameter, patch_diameter))
    center_coord = max_radius  # Center coordinate in the grid

    # Calculate distance squared from the center for the grid
    dist_sq = (y_grid - center_coord) ** 2 + (x_grid - center_coord) ** 2

    # Pre-calculate masks based on distances relative to the center
    signal_mask_template = dist_sq <= r0**2
    background_mask_template = (dist_sq > r1**2) & (dist_sq <= r2**2)

    for peak_row, peak_col in peak_coords:
        # Define the bounding box for the patch around the peak
        y_min = max(0, peak_row - max_radius)
        y_max = min(h, peak_row + max_radius + 1)
        x_min = max(0, peak_col - max_radius)
        x_max = min(w, peak_col + max_radius + 1)

        # Extract the patch from the image
        patch = image[y_min:y_max, x_min:x_max]
        patch_h, patch_w = patch.shape

        # Determine the peak's position relative to the patch's top-left corner
        peak_row_in_patch = peak_row - y_min
        peak_col_in_patch = peak_col - x_min

        # Determine the slice of the template masks that corresponds to the actual patch size
        template_y_start = center_coord - peak_row_in_patch
        template_y_end = template_y_start + patch_h
        template_x_start = center_coord - peak_col_in_patch
        template_x_end = template_x_start + patch_w

        # Extract the relevant part of the masks
        signal_mask = signal_mask_template[
            template_y_start:template_y_end, template_x_start:template_x_end
        ]
        background_mask = background_mask_template[
            template_y_start:template_y_end, template_x_start:template_x_end
        ]

        # Ensure masks have the same shape as the patch (important for edge cases)
        if signal_mask.shape != patch.shape or background_mask.shape != patch.shape:
            logger.warning(
                f"Mask shape mismatch for peak ({peak_row}, {peak_col}) "
                f"due to edge proximity and slicing. Patch: {patch.shape}, "
                f"Signal Mask: {signal_mask.shape}. Skipping."
            )
            results.append(
                (peak_row, peak_col, np.nan, 0, np.nan, np.nan, None, np.nan)
            )
            continue

        # 1. Calculate peak intensity (sum of disk r0)
        signal_pixels = patch[signal_mask]
        signal_sum = np.sum(signal_pixels)
        num_signal_pixels = signal_mask.sum()  # Count of True values

        # 2. Calculate background statistics (mean and std dev of annulus r1 to r2)
        background_pixels = patch[background_mask]

        if background_pixels.size > 0:
            background_mean = np.mean(background_pixels)
            background_std = np.std(background_pixels)  # Still useful info
        else:
            # Handle cases where the background annulus is empty
            background_mean = np.nan
            background_std = np.nan

        # Guard for empty/invalid background
        if background_pixels.size > 0 and np.isfinite(background_mean):
            # 1) Sigma threshold using Poisson variance
            k = 3.0  # choose your sigma level
            bg_std_poisson = np.sqrt(background_mean) if background_mean > 0 else np.nan
            thr_sigma = background_mean + k * bg_std_poisson
            num_above_bg_sigma = int(np.count_nonzero(signal_pixels > thr_sigma))

            # 2) Sigma threshold using sample std from annulus
            thr_sample = background_mean + k * background_std
            num_above_bg_sample = int(np.count_nonzero(signal_pixels > thr_sample))

            # 3) Per-pixel Poisson test with Bonferroni correction
            from scipy.stats import poisson

            N_signal = max(1, signal_pixels.size)
            alpha = 0.01
            alpha_bonf = alpha / N_signal
            # one-sided tail: P(X >= x | mu)
            p_vals = 1.0 - poisson.cdf(signal_pixels - 1, mu=background_mean)
            num_sig_poisson = int(np.count_nonzero(p_vals < alpha_bonf))

            # 4) Robust (median/MAD) background estimate
            bg_med = float(np.median(background_pixels))
            mad = float(np.median(np.abs(background_pixels - bg_med)))
            bg_sigma_robust = 1.4826 * mad
            thr_robust = bg_med + k * bg_sigma_robust
            num_above_bg_robust = int(np.count_nonzero(signal_pixels > thr_robust))
        else:
            num_above_bg_sigma = 0
            num_above_bg_sample = 0
            num_sig_poisson = 0
            num_above_bg_robust = 0

        # 3. Calculate Z-score based on Poisson assumption
        z_score = np.nan  # Default value

        if num_signal_pixels > 0 and not np.isnan(background_mean):
            # Expected signal sum if region contained only background
            expected_background_sum = num_signal_pixels * background_mean
            # Net signal above expected background
            net_signal = signal_sum - expected_background_sum

            # Variance of the sum of N Poisson variables = N * mean
            # Avoid sqrt of negative or zero if background_mean <= 0
            if background_mean > 0:
                expected_variance_sum = num_signal_pixels * background_mean
                # Standard deviation = sqrt(variance)
                noise_std_dev = np.sqrt(expected_variance_sum)

                if noise_std_dev > 0:
                    z_score = net_signal / noise_std_dev
                elif net_signal > 0:  # Positive signal, zero expected noise
                    z_score = np.inf
                else:  # Zero or negative signal, zero expected noise
                    z_score = 0.0  # Or could be np.nan, but 0 seems reasonable
            elif net_signal > 0:
                # Positive signal over non-positive background -> infinite significance
                # in the context of Poisson counts (which assume positive means)
                z_score = np.inf
            else:
                # Non-positive signal over non-positive background
                z_score = 0.0  # No significant deviation above zero

        num_pixels_in_peak = {
            "num_above_bg_sigma": num_above_bg_sigma,
            "num_above_bg_sample": num_above_bg_sample,
            "num_sig_poisson": num_sig_poisson,
            "num_above_bg_robust": num_above_bg_robust,
        }

        results.append(
            (
                peak_row,
                peak_col,
                signal_sum,
                num_signal_pixels,
                background_mean,
                background_std,  # Keep returning std dev for potential analysis
                num_pixels_in_peak,
                z_score,
            )
        )

    # sort by signal_sum
    results.sort(key=lambda t: t[2], reverse=True)

    return results


@timeit
def analyze_peak_distribution(peaks, beam_x, beam_y, resolution_bins_in_pixels, bin1_min_count=2):
    # peaks: Nx2 array of (row, col)
    # resolution_bins_in_pixels: pixel radii converted from e.g. [20, 5.5, 3.93, 3.87, 3.7, 3.64, 3.0, 2.0]
    logger.debug(
        f"analyze_peak_distribution: Distribution of peaks with predefined resolution bins (px): {resolution_bins_in_pixels}"
    )
    if peaks is None or len(peaks) == 0:
        return peaks

    # Radial distances from beam center
    dists = np.sqrt((peaks[:, 1] - beam_x) ** 2 + (peaks[:, 0] - beam_y) ** 2)

    # Ensure bins are strictly increasing for np.histogram
    bins_px = np.asarray(resolution_bins_in_pixels, dtype=float)
    if not np.all(np.diff(bins_px) > 0):
        bins_px = np.sort(bins_px)

    hist, edges = np.histogram(dists, bins=bins_px)
    logger.debug(f"analyze_peak_distribution: peak distribution: {hist}")

    total = int(hist.sum())
    if total == 0:
        return peaks

    # Indexing assumes bins were built from [20, 5.5, 3.93, 3.87, 3.7, 3.64, 3.0, 2.0] in that order then converted to pixels
    # Bin indices (edges -> bins): 0:(20–5.5), 2:(3.93–3.87), 4:(3.7–3.64)
    n_bins = len(edges) - 1
    lowres_idx = 0 if n_bins >= 1 else None
    ring1_idx = 2 if n_bins >= 3 else None  # 3.93–3.87 Å
    ring2_idx = 4 if n_bins >= 5 else None  # 3.7–3.64 Å

    lowres_count = int(hist[lowres_idx]) if lowres_idx is not None else 0
    ring1_count = int(hist[ring1_idx]) if ring1_idx is not None else 0
    ring2_count = int(hist[ring2_idx]) if ring2_idx is not None else 0

    # Condition 1: 20–5.5 Å bin has < bin1_min_count spots
    cond1 = lowres_count < bin1_min_count

    # Condition 2: majority in ice-ring bins or a single narrow ring bin, unless low-res > 3
    frac_ring1 = ring1_count / total if total > 0 else 0.0
    frac_ring2 = ring2_count / total if total > 0 else 0.0
    majority_in_ice = (frac_ring1 > 0.5) or (frac_ring2 > 0.5)

    # Generic "narrow ring" detection: max bin occupies > 50% and its pixel width is narrow
    widths_px = np.diff(edges)
    max_bin = int(np.argmax(hist)) if n_bins > 0 else 0
    frac_max = hist[max_bin] / total if total > 0 else 0.0
    narrow_thresh_px = np.median(widths_px) * 0.25 if widths_px.size else np.inf
    is_narrow_bin = widths_px[max_bin] <= narrow_thresh_px
    majority_in_narrow = (frac_max > 0.5) and is_narrow_bin

    cond2 = (majority_in_ice or majority_in_narrow) and (lowres_count <= 3)

    if cond1 or cond2:
        logger.warning(
            "analyze_peak_distribution: Peaks indicate ice-ring/false-spot pattern; trimming to 1 peak"
        )
        return peaks[:1]
    return peaks


def find_peaks(
    image_data: np.ndarray, detector_mask: Optional[np.ndarray], params: dict
) -> Tuple[Optional[np.ndarray], Optional[str]]:
    """
    Synchronously runs the peak finding algorithm on an image.

    This function contains the core logic previously inside PeakFinderWorker.

    Args:
        image_data: The 2D numpy array of the image.
        detector_mask: A boolean mask where True indicates a masked pixel.
        params: A dictionary of parameters for find_peaks_in_annulus.

    Returns:
        A tuple of (peaks, error_message).
        - peaks: A numpy array of peak coordinates (y, x) if successful.
        - error_message: A string containing an error message if it failed.
    """
    if image_data is None:
        return None, "No image data provided."
    if not params:
        return None, "Peak finding parameters not provided."

    try:
        # The core call to the algorithm
        peaks = find_peaks_in_annulus(image=image_data, mask=detector_mask, **params)
        return peaks, None
    except Exception as e:
        error_msg = f"Peak finding algorithm failed: {e}"
        logger.error(error_msg, exc_info=True)
        return None, error_msg
