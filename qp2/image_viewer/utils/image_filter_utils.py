import numpy as np

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def apply_maximum_filter(image, se_size, detector_mask=None):
    from scipy.ndimage import maximum_filter

    image_for_filter = image.copy()
    if detector_mask is not None and detector_mask.shape == image_for_filter.shape:
        image_for_filter[detector_mask] = 0
        logger.debug(
            f"Applied detector mask, set {np.sum(detector_mask)} pixels to 0 before maximum filtering."
        )
    return maximum_filter(image_for_filter, size=se_size)


def apply_median_filter(image, se_size, detector_mask=None):
    from cv2 import medianBlur

    image_for_filter = image.copy()
    if detector_mask is not None and detector_mask.shape == image_for_filter.shape:
        image_for_filter[detector_mask] = 0
        logger.debug(
            f"Applied detector mask, set {np.sum(detector_mask)} pixels to 0 before median filtering."
        )
    if se_size % 2 == 0 or se_size <= 1:
        raise ValueError("Median filter size (se_size) must be an odd integer > 1.")
    img_for_cv = image_for_filter
    if img_for_cv.dtype != np.uint8:
        logger.warning(
            f"Input image dtype {img_for_cv.dtype} is not supported by OpenCV medianBlur. Converting to uint8."
        )
        img_min, img_max = img_for_cv.min(), img_for_cv.max()
        if img_max > img_min:
            img_for_cv = ((img_for_cv - img_min) / (img_max - img_min) * 255).astype(
                np.uint8
            )
        else:
            img_for_cv = np.zeros_like(img_for_cv, dtype=np.uint8)
    return medianBlur(img_for_cv, se_size)


# --- Advanced filter methods ---
def apply_spot_enhancement(image, se_size):
    try:
        from scipy.ndimage import gaussian_filter, maximum_filter
        from skimage.morphology import disk, white_tophat

        img_float = image.astype(np.float32)
        radius = max(3, se_size // 2)
        background = white_tophat(img_float, disk(radius))
        sigma = max(1.5, se_size / 6.0)
        blurred = gaussian_filter(img_float, sigma=sigma)
        enhanced = img_float - blurred
        result = background + enhanced * 0.5
        result = maximum_filter(result, size=3)
        return result.astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for spot enhancement: {e}")
        return image


def apply_spot_detection(image, se_size):
    try:
        from scipy.ndimage import gaussian_laplace

        img_float = image.astype(np.float32)
        sigma = max(1.0, se_size / 8.0)
        log_result = gaussian_laplace(img_float, sigma=sigma)
        result = -log_result
        result = np.clip(result, 0, None)
        if result.max() > 0:
            result = result * (image.max() / result.max())
        return result.astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for spot detection: {e}")
        return image


def apply_spot_sharpening(image, se_size):
    try:
        from scipy.ndimage import gaussian_filter

        img_float = image.astype(np.float32)
        sigma = max(1.0, se_size / 6.0)
        blurred = gaussian_filter(img_float, sigma=sigma)
        sharpened = img_float + 0.5 * (img_float - blurred)
        sharpened = np.clip(sharpened, 0, image.max())
        return sharpened.astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for spot sharpening: {e}")
        return image


def apply_spot_contrast(image, se_size):
    try:
        from scipy.ndimage import gaussian_filter
        from skimage.morphology import disk, white_tophat

        img_float = image.astype(np.float32)
        radius = max(3, se_size // 2)
        background_removed = white_tophat(img_float, disk(radius))
        sigma = max(2.0, se_size / 4.0)
        local_mean = gaussian_filter(img_float, sigma=sigma)
        contrast_enhanced = background_removed + (img_float - local_mean) * 0.3
        p2, p98 = np.percentile(contrast_enhanced, (2, 98))
        if p98 > p2:
            result = (contrast_enhanced - p2) / (p98 - p2) * image.max()
        else:
            result = contrast_enhanced
        return np.clip(result, 0, image.max()).astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for spot contrast: {e}")
        return image


def apply_tophat_filter(image, se_size):
    try:
        from skimage.morphology import white_tophat, disk

        img_float = image.astype(np.float32)
        radius = max(3, se_size // 2)
        tophat_result = white_tophat(img_float, disk(radius))
        if tophat_result.max() > 0:
            enhanced = tophat_result * 2.0
            result = np.clip(enhanced, 0, image.max())
        else:
            result = tophat_result
        return result.astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for top-hat filter: {e}")
        return image


def apply_log_filter(image, se_size):
    try:
        from scipy.ndimage import gaussian_laplace

        img_float = image.astype(np.float32)
        sigma = max(1.0, se_size / 6.0)
        log_result = gaussian_laplace(img_float, sigma=sigma)
        result = -log_result
        result = np.clip(result, 0, None)
        if result.max() > 0:
            result = result * (image.max() / result.max())
        return result.astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for LoG filter: {e}")
        return image


def apply_dog_filter(image, se_size):
    try:
        from scipy.ndimage import gaussian_filter

        img_float = image.astype(np.float32)
        sigma1 = max(1.0, se_size / 8.0)
        sigma2 = max(2.0, se_size / 4.0)
        blur1 = gaussian_filter(img_float, sigma=sigma1)
        blur2 = gaussian_filter(img_float, sigma=sigma2)
        dog_result = blur1 - blur2
        if dog_result.max() > 0:
            enhanced = dog_result * 1.5
            result = np.clip(enhanced, 0, image.max())
        else:
            result = dog_result
        return result.astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for DoG filter: {e}")
        return image


def apply_matched_filter(image, se_size):
    try:
        from scipy.signal import correlate2d

        img_float = image.astype(np.float32)
        sigma = max(1.5, se_size / 6.0)
        template_size = max(7, se_size * 2)
        y, x = np.ogrid[:template_size, :template_size]
        center = template_size // 2
        template = np.exp(-((x - center) ** 2 + (y - center) ** 2) / (2 * sigma ** 2))
        template = template / template.sum()
        matched_result = correlate2d(img_float, template, mode="same")
        if matched_result.max() > 0:
            result = matched_result * (image.max() / matched_result.max())
        else:
            result = matched_result
        return np.clip(result, 0, image.max()).astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for matched filter: {e}")
        return image


def apply_bandpass_filter(image, se_size):
    try:
        from scipy.ndimage import gaussian_filter

        img_float = image.astype(np.float32)
        sigma_low = max(1.0, se_size / 8.0)
        sigma_high = max(2.0, se_size / 4.0)
        low_pass = gaussian_filter(img_float, sigma=sigma_high)
        high_pass = img_float - gaussian_filter(img_float, sigma=sigma_low)
        bandpass = high_pass - low_pass
        if bandpass.max() > 0:
            result = bandpass * (image.max() / bandpass.max())
        else:
            result = bandpass
        return np.clip(result, 0, image.max()).astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for bandpass filter: {e}")
        return image


def apply_clahe_enhancement(image, se_size):
    from cv2 import createCLAHE

    try:
        img_uint8 = image.astype(np.uint8)
        clahe = createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        clahe_result = clahe.apply(img_uint8)
        if image.dtype != np.uint8:
            result = clahe_result.astype(np.float32) * (image.max() / 255.0)
            return result.astype(image.dtype)
        else:
            return clahe_result.astype(image.dtype)
    except Exception as e:
        logger.error(f"Error applying CLAHE enhancement: {e}")
        return image


# works pretty well, but slow
def apply_radial_background_removal(
        image, se_size, detector_mask=None, beam_center=None
):
    """
    Removes the radial background from an image using an iterative
    sigma-clipping method to robustly calculate the radial profile.
    """
    from scipy.ndimage import gaussian_filter1d

    if beam_center is None:
        logger.error("Radial Background Removal requires a beam_center.")
        return image

    img_float = image.astype(np.float32, copy=False)
    center_x, center_y = beam_center

    # 1. Create a radial distance map for every pixel
    y_coords, x_coords = np.indices(img_float.shape)
    distances = np.sqrt((x_coords - center_x) ** 2 + (y_coords - center_y) ** 2)
    r_int = distances.astype(int)

    # 2. Calculate the radial average using robust sigma-clipping
    valid_mask = (
        ~detector_mask
        if detector_mask is not None
        else np.ones_like(img_float, dtype=bool)
    )

    # Get the maximum radius to define the size of our profile array
    max_radius = np.max(r_int[valid_mask])
    radial_mean = np.zeros(max_radius + 1, dtype=float)

    # --- THIS IS THE NEW ROBUST LOGIC ---
    # Instead of a single bincount, iterate through each radial bin
    for r in range(max_radius + 1):
        # Find all valid pixels at this integer radius
        bin_mask = (r_int == r) & valid_mask
        if np.sum(bin_mask) < 10:  # Skip bins with too few pixels
            continue

        pixel_values = img_float[bin_mask]

        # Iterative Sigma Clipping to reject outliers (Bragg peaks)
        # Start with all pixels in the bin
        current_selection = pixel_values

        for i in range(3):  # Iterate 3 times for refinement
            if len(current_selection) < 2:
                break  # Not enough points to calculate stats

            mean = np.mean(current_selection)
            std = np.std(current_selection)

            # Define the threshold for outliers (e.g., 2 standard deviations above the mean)
            threshold = mean + 2.0 * std

            # Keep only the pixels that are *below* the threshold
            new_selection = current_selection[current_selection < threshold]

            # If we didn't remove any pixels, or we removed too many, stop iterating
            if len(new_selection) == len(current_selection) or len(new_selection) < 10:
                break

            current_selection = new_selection

        # The final mean is the robust background value for this radius
        if len(current_selection) > 0:
            radial_mean[r] = np.mean(current_selection)
    # --- END NEW ROBUST LOGIC ---

    # Fill in any gaps for bins that were skipped
    # (e.g., masked areas or very center)
    non_zero_indices = np.where(radial_mean > 0)[0]
    if len(non_zero_indices) > 1:
        # Interpolate to fill gaps
        zero_indices = np.where(radial_mean == 0)[0]
        radial_mean[zero_indices] = np.interp(
            zero_indices, non_zero_indices, radial_mean[non_zero_indices]
        )

    # 3. Heavily smooth the robust radial profile
    smoothing_sigma = max(5.0, se_size)
    smoothed_background_profile = gaussian_filter1d(radial_mean, sigma=smoothing_sigma)

    # 4. Create the 2D background map
    max_valid_radius_index = len(smoothed_background_profile) - 1
    r_int_clipped = np.clip(r_int, 0, max_valid_radius_index)
    background_map_2d = smoothed_background_profile[r_int_clipped]

    # 5. Subtract the background and finalize the image
    corrected_image = img_float - background_map_2d
    corrected_image = np.maximum(corrected_image, 0)

    if detector_mask is not None:
        corrected_image[detector_mask] = image[detector_mask]

    return corrected_image.astype(image.dtype)


def apply_radial_spot_enhancement(image, se_size, beamcenter):
    try:
        from scipy.ndimage import gaussian_filter, median_filter
        from skimage.morphology import white_tophat, disk

        img_float = image.astype(np.float32)
        height, width = img_float.shape
        center_y, center_x = beamcenter
        y_coords, x_coords = np.mgrid[0:height, 0:width]
        distances = np.sqrt((x_coords - center_x) ** 2 + (y_coords - center_y) ** 2)
        bg_kernel_size = max(15, se_size * 2)
        if bg_kernel_size % 2 == 0:
            bg_kernel_size += 1
        background = median_filter(img_float, size=bg_kernel_size)
        corrected = img_float - background
        corrected = np.maximum(corrected, 0)
        radius = max(3, se_size // 2)
        tophat_result = white_tophat(corrected, disk(radius))
        sigma1 = max(1, se_size // 4)
        sigma2 = max(2, se_size // 2)
        gaussian1 = gaussian_filter(corrected, sigma=sigma1)
        gaussian2 = gaussian_filter(corrected, sigma=sigma2)
        spot_enhanced = gaussian1 - gaussian2
        result = tophat_result + spot_enhanced
        result = np.maximum(result, 0)
        if result.max() > 0:
            result = result / result.max() * image.max()
        return result.astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for radial spot enhancement: {e}")
        return image


def apply_beam_center_correction(image, se_size, beamcenter):
    try:
        from scipy.ndimage import gaussian_filter
        from scipy.interpolate import griddata

        img_float = image.astype(np.float32)
        height, width = img_float.shape
        center_y, center_x = beamcenter
        y_coords, x_coords = np.mgrid[0:height, 0:width]
        distances = np.sqrt((x_coords - center_x) ** 2 + (y_coords - center_y) ** 2)
        sigma = max(5, se_size)
        background = gaussian_filter(img_float, sigma=sigma)
        max_distance = np.sqrt(center_x ** 2 + center_y ** 2)
        correction_factor = 1.0 - 0.3 * (distances / max_distance)
        correction_factor = np.clip(correction_factor, 0.5, 1.0)
        corrected = img_float / (background * correction_factor + 1e-6)
        corrected = np.maximum(corrected, 0)
        if corrected.max() > 0:
            corrected = corrected / corrected.max() * image.max()
        return corrected.astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for beam center correction: {e}")
        return image


def apply_radial_tophat(image, se_size, beamcenter):
    try:
        from skimage.morphology import white_tophat, disk
        from scipy.ndimage import gaussian_filter

        img_float = image.astype(np.float32)
        height, width = img_float.shape
        center_y, center_x = beamcenter
        y_coords, x_coords = np.mgrid[0:height, 0:width]
        distances = np.sqrt((x_coords - center_x) ** 2 + (y_coords - center_y) ** 2)
        max_distance = np.sqrt(center_x ** 2 + center_y ** 2)
        radial_background = gaussian_filter(img_float, sigma=max(3, se_size))
        corrected = img_float - radial_background
        corrected = np.maximum(corrected, 0)
        radius_base = max(3, se_size // 2)
        adaptive_radius = radius_base + (distances / max_distance * radius_base).astype(
            int
        )
        adaptive_radius = np.clip(adaptive_radius, 2, 10)
        tophat_result = np.zeros_like(corrected)
        radius = max(3, se_size // 2)
        tophat_result = white_tophat(corrected, disk(radius))
        center_weight = 1.0 - 0.3 * (distances / max_distance)
        center_weight = np.clip(center_weight, 0.7, 1.0)
        tophat_result = tophat_result * center_weight
        if tophat_result.max() > 0:
            tophat_result = tophat_result / tophat_result.max() * image.max()
        return tophat_result.astype(image.dtype)
    except ImportError as e:
        logger.error(f"Required library not available for radial top-hat: {e}")
        return image


def apply_local_background_subtraction(image, se_size):
    try:
        from scipy.ndimage import uniform_filter

        img_float = image.astype(np.float32)
        window_size = max(5, se_size * 2)
        if window_size % 2 == 0:
            window_size += 1
        local_background = uniform_filter(img_float, size=window_size)
        result = img_float - local_background
        result = np.maximum(result, 0)
        if result.max() > 0:
            result = result / result.max() * image.max()
        return result.astype(image.dtype)
    except ImportError as e:
        logger.error(
            f"Required library not available for local background subtraction: {e}"
        )
        return image


def apply_poisson_threshold(image, se_size, detector_mask=None):
    try:
        from scipy import stats

        img_float = image.astype(np.float32)
        if detector_mask is not None and detector_mask.shape == image.shape:
            valid_mask = ~detector_mask
            valid_pixels = img_float[valid_mask]
        else:
            valid_pixels = img_float.flatten()
        if len(valid_pixels) == 0:
            return image, 0.0
        mean_intensity = np.mean(valid_pixels)
        std_intensity = np.std(valid_pixels)
        poisson_std = np.sqrt(mean_intensity)
        k_factor = max(2.0, se_size / 4.0)
        threshold_method1 = mean_intensity + k_factor * std_intensity
        poisson_factor = max(2.0, se_size / 6.0)
        threshold_method2 = mean_intensity + poisson_factor * poisson_std
        percentile = min(95, 100 - se_size)
        threshold_method3 = np.percentile(valid_pixels, percentile)
        threshold = (threshold_method1 + threshold_method2 + threshold_method3) / 3.0
        threshold_uint32 = np.uint32(np.ceil(threshold))
        thresholded_image = np.where(img_float > threshold_uint32, img_float, 0.0)
        if detector_mask is not None and detector_mask.shape == image.shape:
            thresholded_image[detector_mask] = image[detector_mask]
        if thresholded_image.max() > 0:
            thresholded_image = (
                    thresholded_image / thresholded_image.max() * image.max()
            )
        thresholded_image = np.nan_to_num(
            thresholded_image, nan=0.0, posinf=0.0, neginf=0.0
        ).astype(np.uint32)
        logger.debug(
            f"Poisson threshold calculation: mean={mean_intensity:.2f}, "
            f"std={std_intensity:.2f}, poisson_std={poisson_std:.2f}, "
            f"threshold={threshold:.2f}, threshold_uint32={threshold_uint32}"
        )
        return thresholded_image, float(threshold_uint32)
    except ImportError as e:
        logger.error(f"Required library not available for Poisson threshold: {e}")
        return image, 0.0


def apply_radial_poisson_threshold(
        image, se_size, detector_mask=None, beam_center=None
):
    """
    Applies a threshold to an image based on Poisson statistics derived from
    a smoothed radial average of the background.

    Args:
        image: The input 2D numpy array.
        se_size: Used as a factor to determine the significance (k-factor) for the threshold.
        detector_mask: A boolean mask where True indicates pixels to ignore.
        beam_center: A tuple (x, y) of the beam center coordinates.

    Returns:
        A tuple of (thresholded_image, threshold_info_dict).
    """
    if beam_center is None:
        logger.error("Radial Poisson Threshold requires a beam_center.")
        return image, {}

    from scipy.ndimage import gaussian_filter1d

    logger.debug("Applying Radial Poisson Threshold filter.")
    img_float = image.astype(np.float32)
    height, width = img_float.shape
    center_x, center_y = beam_center

    # 1. Create a radial distance map for every pixel
    y_coords, x_coords = np.indices(img_float.shape)
    distances = np.sqrt((x_coords - center_x) ** 2 + (y_coords - center_y) ** 2)
    r_int = distances.astype(int)

    # 2. Calculate the radial average, respecting the detector mask
    valid_mask = (
        ~detector_mask
        if detector_mask is not None
        else np.ones_like(img_float, dtype=bool)
    )

    # Use bincount for a fast radial average
    radial_sum = np.bincount(r_int[valid_mask], weights=img_float[valid_mask])
    pixel_counts = np.bincount(r_int[valid_mask])

    # Avoid division by zero for bins with no pixels
    radial_mean = np.zeros_like(radial_sum, dtype=float)
    valid_bins = pixel_counts > 0
    radial_mean[valid_bins] = radial_sum[valid_bins] / pixel_counts[valid_bins]

    # 3. Smooth the radial average curve to remove noise from strong reflections
    # A Gaussian filter is excellent for this. The sigma is heuristic.
    smoothing_sigma = 5.0
    smoothed_radial_mean = gaussian_filter1d(radial_mean, sigma=smoothing_sigma)

    # 4. Calculate the Poisson threshold for each radial bin
    # Threshold = mean(r) + k * sqrt(mean(r))
    # 'se_size' is repurposed here as a significance factor 'k'
    k_factor = max(2.0, se_size)

    # The standard deviation for a Poisson distribution is sqrt(mean)
    poisson_std = np.sqrt(
        np.maximum(smoothed_radial_mean, 0)
    )  # Ensure no sqrt of negative
    radial_threshold = smoothed_radial_mean + k_factor * poisson_std
    max_valid_radius_index = len(radial_threshold) - 1
    r_int_clipped = np.clip(r_int, 0, max_valid_radius_index)

    # 5. Create a 2D threshold map by mapping the radial threshold back to each pixel
    threshold_map = radial_threshold[r_int_clipped]

    # 6. Apply the threshold
    thresholded_image = np.where(img_float > threshold_map, img_float, 0)

    # 7. Restore the original masked pixel values
    if detector_mask is not None:
        thresholded_image[detector_mask] = image[detector_mask]

    # Normalize the output to the original image's max value for better visualization
    if thresholded_image.max() > 0:
        thresholded_image = (thresholded_image / thresholded_image.max()) * image.max()

    # The second return value can be used for debugging or extra info
    # For now, it's an empty dict, but could contain the radial profiles
    extra_info = {
        "radial_profile": (radial_mean.tolist(), smoothed_radial_mean.tolist())
    }

    return thresholded_image.astype(image.dtype), extra_info


def apply_visual_spot_enhancement(image, se_size, beamcenter, detector_mask=None):
    try:
        from scipy.ndimage import uniform_filter

        img_float = image.astype(np.float32)
        height, width = img_float.shape
        center_y, center_x = beamcenter
        y, x = np.indices(img_float.shape)
        r = np.sqrt((x - center_x) ** 2 + (y - center_y) ** 2)
        r_int = r.astype(int)
        if detector_mask is not None and detector_mask.shape == image.shape:
            valid_mask = ~detector_mask
        else:
            valid_mask = np.ones_like(img_float, dtype=bool)
        r_valid = r_int[valid_mask]
        img_valid = img_float[valid_mask]
        radial_sum = np.bincount(r_valid.ravel(), img_valid.ravel())
        radial_count = np.bincount(r_valid.ravel())
        radial_mean = radial_sum / np.maximum(radial_count, 1)
        bg = radial_mean[r_int]
        image_bgsub = img_float - bg
        image_bgsub = np.clip(image_bgsub, 0, None)
        image_bgsub[~valid_mask] = 0
        image_local = image_bgsub
        image_disp = np.log1p(image_local)
        image_disp = (
            image_disp / image_disp.max() * 255 if image_disp.max() > 0 else image_disp
        )
        image_disp = image_disp.astype(np.uint8)
        return image_disp
    except Exception as e:
        logger.error(f"Visual Spot Enhancement filter failed: {e}")
        return image


def apply_cutoff_filter(image, cutoff_value, detector_mask=None):
    """
    Sets all pixel values <= cutoff_value to 0.
    """
    try:
        # Create a copy to avoid modifying original if it's mutable/shared
        result = image.copy()
        
        # Apply mask first if provided (optional, but consistent with other filters)
        if detector_mask is not None and detector_mask.shape == result.shape:
             result[detector_mask] = 0

        # Apply cutoff
        result[result <= cutoff_value] = 0
        
        return result
    except Exception as e:
        logger.error(f"Error applying cutoff filter: {e}")
        return image
