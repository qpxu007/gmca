import numpy as np
from scipy.stats import binned_statistic
from scipy.optimize import minimize
import time
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

def generate_asymmetric_image(shape=(512, 512), center=(256, 256), pixel_size=0.1):
    """
    Generates an image with:
    1. A radially symmetric background (the signal we use for centering).
    2. Randomly distributed diffraction spots (the 'noise' we must ignore).
    3. A beamstop shadow (low values in center).
    No Friedel pairs, no rings.
    """
    H, W = shape
    cx, cy = center
    
    y, x = np.indices((H, W))
    r = np.sqrt((x - cx)**2 + (y - cy)**2) * pixel_size
    
    image = np.zeros(shape)
    
    # 1. Symmetric Background (e.g., air scatter / solvent)
    # Falling off with radius
    image += 200 * np.exp(-r / (0.8 * r.max())) 
    
    # 3. Beamstop Shadow (e.g., radius 20 pixels * 0.1 = 2.0 units)
    # Set center to low value (noise floor)
    beamstop_mask = r < (20 * pixel_size)
    image[beamstop_mask] = 10 # Low background
    
    # 2. Asymmetric Spots (No symmetry, no shared resolution)
    # Just random spots placed anywhere
    np.random.seed(42)
    num_spots = 40
    for _ in range(num_spots):
        # Random location
        sx = np.random.randint(0, W)
        sy = np.random.randint(0, H)
        
        # Distance to this spot center
        d2 = (x - sx)**2 + (y - sy)**2
        
        # Spot profile (Gaussian)
        intensity = 5000 * np.exp(-d2 / (2 * 1.5**2)) # Bright spot
        image += intensity
        
    # Re-apply beamstop to kill spots behind it
    image[beamstop_mask] = 10

    # Add Poisson noise
    noise = np.random.poisson(image)
    return noise.astype(float)

def estimate_beamstop_radius(center, image, mask=None, max_search_radius=500, step=2):
    """
    Estimates the beamstop radius by analyzing the radial intensity profile.
    Assumes the beamstop is a central region of low intensity followed by a rise.
    Returns the radius (in pixels) where the intensity transitions.
    """
    cx, cy = center
    H, W = image.shape
    
    # Extract ROI to speed up
    r_max_int = int(max_search_radius)
    x_min = max(0, int(cx - r_max_int))
    x_max = min(W, int(cx + r_max_int))
    y_min = max(0, int(cy - r_max_int))
    y_max = min(H, int(cy + r_max_int))
    
    sub_img = image[y_min:y_max, x_min:x_max]
    if sub_img.size == 0: return 0
    
    y_sub, x_sub = np.indices(sub_img.shape)
    r = np.sqrt((x_sub + x_min - cx)**2 + (y_sub + y_min - cy)**2)
    
    if mask is not None:
        sub_mask = mask[y_min:y_max, x_min:x_max]
        valid = ~sub_mask
        r = r[valid]
        vals = sub_img[valid]
    else:
        vals = sub_img.flatten()
        r = r.flatten()
        
    if vals.size == 0: return 0
    
    # Calculate radial profile (median to ignore spots)
    bins = np.arange(0, max_search_radius, step)
    profile, bin_edges, _ = binned_statistic(r, vals, statistic='median', bins=bins)
    
    # Clean NaNs
    profile = np.nan_to_num(profile)
    
    # Analyze profile for a step
    # We look for the first point where intensity rises significantly above the "shadow" level.
    # Shadow level = median of first few bins (assuming center is inside beamstop)
    shadow_level = np.median(profile[:5]) if len(profile) > 5 else profile[0]
    
    # Simple threshold: Rise to 2x shadow or shadow + std?
    # Or derivative.
    # Let's use a dynamic threshold based on the range of the profile
    
    # Global median of the search area (likely background)
    global_bg = np.median(profile)
    
    # If shadow is not significantly darker than global, maybe no beamstop visible or centered wrong
    if global_bg < shadow_level * 1.2:
        # Fallback: maybe just look for gradient
        pass
        
    threshold = (shadow_level + global_bg) / 2.0
    
    # Find first crossing
    idx = np.where(profile > threshold)[0]
    if idx.size > 0:
        radius = bin_edges[idx[0]]
        return radius
        
    return 0

def calculate_inner_intensity_score(center, image, radius, mask=None):
    """
    Calculates the mean intensity within the given radius.
    Used to penalize beam centers that fall on bright pixels (implying the center is wrong,
    as the beamstop should be dark).
    Excludes masked pixels (e.g. saturation) to prevent skewing the mean.
    """
    cx, cy = center
    H, W = image.shape
    
    # Optimization: slice small ROI around center
    r_int = int(np.ceil(radius))
    x_min = max(0, int(cx - r_int))
    x_max = min(W, int(cx + r_int + 1))
    y_min = max(0, int(cy - r_int))
    y_max = min(H, int(cy + r_int + 1))
    
    sub_img = image[y_min:y_max, x_min:x_max]
    if sub_img.size == 0: return 1e9
    
    if mask is not None:
        sub_mask = mask[y_min:y_max, x_min:x_max]
    else:
        sub_mask = None
    
    y_sub, x_sub = np.indices(sub_img.shape)
    # Adjust indices to global coords
    y_sub += y_min
    x_sub += x_min
    
    r2 = (x_sub - cx)**2 + (y_sub - cy)**2
    # Include pixels within radius AND not masked
    valid_r = r2 <= radius**2
    
    if sub_mask is not None:
        final_mask = valid_r & (~sub_mask)
    else:
        final_mask = valid_r
    
    if np.sum(final_mask) == 0: return 1e9
    return np.mean(sub_img[final_mask])

def calculate_robust_radial_score(center, image, mask=None, bin_size=1.0, min_radius=150, max_radius=None):
    """
    Calculates radial 'spread' but uses robust statistics (IQR) instead of Stdev.
    This effectively IGNORES the bright spots (outliers) and focuses on the 
    background symmetry.
    """
    cx, cy = center
    H, W = image.shape
    
    y, x = np.indices((H, W))
    r = np.sqrt((x - cx)**2 + (y - cy)**2)
    
    if mask is not None:
        valid = ~mask
    else:
        valid = np.ones((H, W), dtype=bool)

    if min_radius > 0:
        valid &= (r >= min_radius)
    if max_radius is not None:
        valid &= (r <= max_radius)

    r = r[valid]
    intensities = image[valid]
        
    if r.size == 0: return 1e9
    max_r = int(np.max(r))
    if max_r == 0: return 1e9
    
    # Define a custom statistic for binned_statistic
    # Sort by radius
    sorted_indices = np.argsort(r)
    r_sorted = r[sorted_indices]
    i_sorted = intensities[sorted_indices]
    
    # Find indices where radius changes (bin edges)
    # Integer bins
    r_int = r_sorted.astype(int)
    bin_changes = np.where(np.diff(r_int))[0] + 1
    
    splits_i = np.split(i_sorted, bin_changes)
    
    total_score = 0
    count = 0
    
    for bin_vals in splits_i:
        if len(bin_vals) < 10: continue
        
        # Fast robust spread: 
        # approximate IQR or just Mean Absolute Deviation from Median?
        # Let's use percentile range which is safest for very bright spots.
        q75, q25 = np.percentile(bin_vals, [75, 25])
        spread = q75 - q25
        
        total_score += spread
        count += 1
        
    if count == 0: return 1e9
    return total_score / count # Average spread per bin

def calculate_variance_score(center, image, mask=None, bin_size=1.0, min_radius=150, max_radius=None):
    """
    Calculates radial 'spread' using simple variance.
    This is faster than robust stats but more sensitive to outliers.
    Ideally used after spot removal.
    """
    cx, cy = center
    H, W = image.shape
    
    y, x = np.indices((H, W))
    r = np.sqrt((x - cx)**2 + (y - cy)**2)
    
    if mask is not None:
        valid = ~mask
    else:
        valid = np.ones((H, W), dtype=bool)

    if min_radius > 0:
        valid &= (r >= min_radius)
    if max_radius is not None:
        valid &= (r <= max_radius)

    r = r[valid]
    intensities = image[valid]
        
    if r.size == 0: return 1e9
    max_r = int(np.max(r))
    if max_r == 0: return 1e9
    
    # Use binned_statistic for fast variance calculation
    # We want standard deviation per bin, then mean of that.
    
    # Note: scipy.stats.binned_statistic can calculate std directly
    # range needs to cover all r
    bin_edges = np.arange(0, max_r + 1, 1)
    
    # Compute standard deviation in each bin
    stdevs, _, _ = binned_statistic(r, intensities, statistic='std', bins=bin_edges)
    
    # Filter out NaNs (empty bins)
    valid_stdevs = stdevs[np.isfinite(stdevs)]
    
    if len(valid_stdevs) == 0: return 1e9
    
    # Return mean standard deviation
    return np.mean(valid_stdevs)

def remove_spots(image, mask):
    """
    Refines the mask to exclude bright diffraction spots, which act as noise 
    for background symmetry detection.
    """
    valid = image[~mask]
    if valid.size == 0: return mask
    
    # Diffraction spots are typically high-intensity outliers.
    # We use the 99.99th percentile to identify them, ensuring we don't
    # mask the diffuse scattering background which is essential for centering.
    threshold = np.percentile(valid, 99.99)
    
    logger.debug(f"Removing spots > {threshold:.2f} (99.99th percentile)")
    
    # Update mask: old_mask OR (pixel > threshold)
    new_mask = mask | (image > threshold)
    
    newly_masked = np.sum(new_mask) - np.sum(mask)
    logger.debug(f"Additional {newly_masked} pixels masked as spots.")
    
    return new_mask

def optimize_beam_center(image, start_guess, mask=None, method='robust', verbose=False, limit=None, min_radius=150, max_radius=None, beamstop_radius=None):
    if verbose:
        logger.info(f"Starting optimization ({method}) from guess: {start_guess}")
    t0 = time.time()
    
    # Downsample for speed
    scale = 2
    img_small = image[::scale, ::scale]
    guess_small = [x / scale for x in start_guess]
    mask_small = None
    if mask is not None:
        mask_small = mask[::scale, ::scale]

    # Pre-calculate global mean for normalization
    valid_pixels = img_small
    if mask_small is not None:
        valid_pixels = img_small[~mask_small]
    
    if valid_pixels.size > 0:
        global_mean = np.mean(valid_pixels)
    else:
        global_mean = 1.0
        
    # Safety floor for global_mean to avoid division by zero or extreme scaling
    if global_mean < 1e-6: global_mean = 1.0

    # Iteration counter for logging
    iteration_count = [0]

    def objective(c):
        score = 0
        symmetry_score = 0
        
        if limit is not None:
            dist = np.sqrt((c[0] - guess_small[0])**2 + (c[1] - guess_small[1])**2)
            if dist > (limit / scale):
                return 1e9 + dist

        if method == 'variance':
            symmetry_score = calculate_variance_score(c, img_small, mask=mask_small, min_radius=min_radius/scale, max_radius=max_radius/scale if max_radius else None)
        else:
            symmetry_score = calculate_robust_radial_score(c, img_small, mask=mask_small, min_radius=min_radius/scale, max_radius=max_radius/scale if max_radius else None)
            
        score += symmetry_score
        
        inner_log_str = ""
        # Add beamstop constraint (penalize bright centers)
        bs_radius = beamstop_radius if beamstop_radius is not None else min_radius
        if bs_radius > 0:
             # Scale radius for downsampled image, and use 0.8 factor to stay safely inside beamstop
             r_inner = (bs_radius * 0.8) / scale
             inner_score = calculate_inner_intensity_score(c, img_small, r_inner, mask=mask_small)
             
             # Normalize inner score using global mean intensity
             normalized_inner_score = inner_score / global_mean
             
             score += normalized_inner_score
             inner_log_str = f", Inner: {inner_score:.1f}, NormInner: {normalized_inner_score:.4f}"
             
        if verbose and iteration_count[0] % 10 == 0:
             logger.debug(f"Iter {iteration_count[0]}: Center: {c}, Sym: {symmetry_score:.4f}{inner_log_str}, Total: {score:.4f}")
        
        iteration_count[0] += 1
        return score

    res = minimize(objective, guess_small, method='Nelder-Mead', tol=1e-1, 
                   options={'disp': verbose, 'maxiter': 100})
    
    final_center = [x * scale for x in res.x]
    t1 = time.time()
    if verbose:
        logger.info(f"Optimization ({method}) finished in {t1-t0:.2f}s")
    return final_center, t1-t0

def optimize_beam_center_iterative(image, start_guess, mask=None, method='robust', verbose=False, limit=None, min_radius=None):
    """
    Iterative beam center optimization that:
    1. Finds the rough center by minimizing intensity in a small fixed radius (darkest hole).
    2. Estimates the beamstop radius from that rough center.
    3. Runs the full refinement using the estimated radius as a constraint.

    Args:
        min_radius (float): Optional. Minimum radius for symmetry scoring. 
                            If provided and larger than the estimated beamstop radius, 
                            symmetry analysis starts from this radius.
                            The region between beamstop and min_radius is ignored.
    """
    t0 = time.time()
    logger.info(f"Starting iterative optimization from guess: {start_guess}")
    
    # --- Step 1: Rough Center Finding (Darkest Hole) ---
    # Minimizing intensity in a small radius (e.g. 50px)
    # This helps if the start_guess is on the edge of the beamstop or outside.
    
    # Downsample for speed
    scale = 4 # Aggressive downsample for rough search
    img_small = image[::scale, ::scale]
    guess_small = [x / scale for x in start_guess]
    mask_small = None
    if mask is not None:
        mask_small = mask[::scale, ::scale]
        
    # Pre-calc global mean for normalization
    valid_pixels = img_small
    if mask_small is not None:
        valid_pixels = img_small[~mask_small]
    
    if valid_pixels.size > 0:
        global_mean = np.mean(valid_pixels)
    else:
        global_mean = 1.0
    if global_mean < 1e-6: global_mean = 1.0

    rough_search_radius_px = 50.0
    r_inner_rough = rough_search_radius_px / scale
    
    def objective_rough(c):
        # Only minimize inner intensity
        if limit is not None:
            dist = np.sqrt((c[0] - guess_small[0])**2 + (c[1] - guess_small[1])**2)
            if dist > (limit / scale):
                return 1e9 + dist
        
        inner = calculate_inner_intensity_score(c, img_small, r_inner_rough, mask=mask_small)
        return inner / global_mean

    logger.info("Step 1: finding rough center (darkest region)...")
    res_rough = minimize(objective_rough, guess_small, method='Nelder-Mead', tol=1e-1, options={'maxiter': 50})
    rough_center = [x * scale for x in res_rough.x]
    logger.info(f"Rough center found: {rough_center}")
    
    # --- Step 2: Estimate Beamstop Radius ---
    logger.info("Step 2: estimating beamstop radius...")
    # Use the rough center to estimate radius
    est_radius = estimate_beamstop_radius(rough_center, image, mask=mask, max_search_radius=500)
    logger.info(f"Estimated beamstop radius: {est_radius:.1f} px")
    
    if est_radius < 10:
        est_radius = 50 # Fallback
        logger.warning("Estimated radius too small, using fallback 50px")
        
    # --- Step 3: Full Refinement ---
    logger.info("Step 3: full refinement with estimated radius...")
    
    # Determine min_radius for symmetry analysis
    # Use user provided min_radius if it's larger than the beamstop (est_radius)
    # Otherwise use est_radius to avoid including beamstop in symmetry calc
    symmetry_start_radius = est_radius
    if min_radius is not None and min_radius > est_radius:
        symmetry_start_radius = min_radius
        logger.info(f"Using provided min_radius {min_radius} for symmetry (beamstop is {est_radius:.1f})")

    # Use the rough center as the new start guess, and the estimated radius
    final_center, _ = optimize_beam_center(
        image, 
        rough_center, # Use improved guess
        mask=mask, 
        method=method, 
        verbose=verbose, 
        limit=limit, 
        min_radius=symmetry_start_radius, # Use our calculated start radius
        max_radius=None,
        beamstop_radius=est_radius # Explicitly pass estimated beamstop radius
    )
    
    t1 = time.time()
    logger.info(f"Iterative optimization finished in {t1-t0:.2f}s. Final: {final_center}")
    return final_center, t1-t0

def calculate_center_of_mass(image, mask=None):
    """
    Calculates the center of mass (intensity-weighted centroid) of the image.
    If a mask is provided, masked pixels (True in mask) are ignored (weight 0).
    """
    H, W = image.shape
    y, x = np.indices((H, W))
    
    weights = image.astype(float)
    if mask is not None:
        weights[mask] = 0
        
    total_mass = np.sum(weights)
    if total_mass == 0:
        return W/2, H/2
        
    cx = np.sum(x * weights) / total_mass
    cy = np.sum(y * weights) / total_mass
    
    return cx, cy