"""
Optimized radial analysis utilities for the image viewer.

This module provides high-performance radial sum and averaging calculations
with memory optimization and caching.
"""

from typing import Dict, Tuple, Optional

import numpy as np

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def calculate_radial_statistics_optimized(
        image: np.ndarray,
        center: Tuple[float, float],
        max_radius: Optional[int] = None,
        detector_mask: Optional[np.ndarray] = None,
) -> Dict[str, np.ndarray]:
    """
    Calculate radial statistics with optimized memory usage and performance.
    
    This is a drop-in replacement for the original calculate_radial_statistics
    function with significant performance improvements:
    - Avoids creating full coordinate arrays
    - Uses efficient distance calculation
    - Implements early exit for pixels beyond max_radius
    - Optimized binning operations
    
    Args:
        image: The image data to analyze
        center: Center point (cx, cy) for radial analysis
        max_radius: Maximum radius to analyze (auto-calculated if None)
        detector_mask: Boolean mask for masked pixels (True = masked)
        
    Returns:
        Dictionary containing radial statistics
    """
    from .performance_cache import get_performance_cache

    if image is None or image.size == 0:
        logger.warning("calculate_radial_statistics_optimized: Empty or None image data")
        return {
            "radii": np.array([]),
            "radial_sum": np.array([]),
            "radial_average": np.array([]),
            "pixel_counts": np.array([]),
            "mean_intensity": 0.0,
            "max_intensity": 0.0,
            "total_intensity": 0.0,
        }

    h, w = image.shape
    cx, cy = center

    # Calculate max_radius if not provided
    if max_radius is None:
        dist_to_left = cx
        dist_to_right = w - 1 - cx
        dist_to_top = cy
        dist_to_bottom = h - 1 - cy
        max_radius = min(dist_to_left, dist_to_right, dist_to_top, dist_to_bottom)
        max_radius = max(1, int(max_radius))
        logger.debug(f"calculate_radial_statistics_optimized: Using max_radius: {max_radius}")

    max_radius = int(max_radius)

    # Check cache first
    cache = get_performance_cache()
    cached_result = cache.get_radial_sum(image, center, max_radius, detector_mask)
    if cached_result is not None:
        return cached_result

    # Optimized distance calculation - avoid creating full coordinate arrays
    # Create 1D arrays for x and y coordinates
    y_coords = np.arange(h, dtype=np.float32) - cy
    x_coords = np.arange(w, dtype=np.float32) - cx

    # Use broadcasting to create distance array efficiently
    # This is more memory efficient than np.indices for large images
    y_grid = y_coords[:, np.newaxis]  # Shape: (h, 1)
    x_grid = x_coords[np.newaxis, :]  # Shape: (1, w)

    # Calculate squared distances (avoid sqrt until necessary)
    dist_sq = y_grid ** 2 + x_grid ** 2

    # Early exit: only process pixels within max_radius
    valid_region = dist_sq <= (max_radius ** 2)

    # Apply detector mask if provided
    if detector_mask is not None and detector_mask.shape == image.shape:
        valid_region &= ~detector_mask

    if not np.any(valid_region):
        logger.warning("calculate_radial_statistics_optimized: No valid pixels in region")
        return {
            "radii": np.arange(max_radius + 1),
            "radial_sum": np.zeros(max_radius + 1),
            "radial_average": np.zeros(max_radius + 1),
            "pixel_counts": np.zeros(max_radius + 1),
            "mean_intensity": 0.0,
            "max_intensity": 0.0,
            "total_intensity": 0.0,
        }

    # Extract only valid pixels and their distances
    valid_distances_sq = dist_sq[valid_region]
    valid_intensities = image[valid_region]

    # Convert to integer distances (now we can safely take sqrt of subset)
    valid_distances = np.sqrt(valid_distances_sq).astype(np.int32)

    # Clip distances to max_radius (shouldn't be necessary but safety check)
    valid_distances = np.clip(valid_distances, 0, max_radius)

    # Optimized binning using numpy's bincount
    radial_sum = np.bincount(
        valid_distances,
        weights=valid_intensities,
        minlength=max_radius + 1
    )
    pixel_counts = np.bincount(valid_distances, minlength=max_radius + 1)

    # Calculate radial average efficiently
    radii = np.arange(len(radial_sum))
    radial_average = np.divide(
        radial_sum,
        pixel_counts,
        out=np.zeros_like(radial_sum, dtype=np.float64),
        where=pixel_counts > 0
    )

    # Calculate statistics
    valid_sums = radial_sum[radial_sum > 0]
    if len(valid_sums) > 0:
        mean_intensity = float(np.mean(valid_sums))
        max_intensity = float(np.max(radial_sum))
        total_intensity = float(np.sum(radial_sum))
    else:
        mean_intensity = max_intensity = total_intensity = 0.0

    result = {
        "radii": radii,
        "radial_sum": radial_sum,
        "radial_average": radial_average,
        "pixel_counts": pixel_counts,
        "mean_intensity": mean_intensity,
        "max_intensity": max_intensity,
        "total_intensity": total_intensity,
    }

    # Cache the result
    cache.cache_radial_sum(image, center, max_radius, detector_mask, result)

    logger.debug(f"calculate_radial_statistics_optimized: Processed {np.sum(valid_region)} valid pixels")
    return result


def calculate_radial_profile_fast(
        image: np.ndarray,
        center: Tuple[float, float],
        max_radius: Optional[int] = None,
        num_bins: Optional[int] = None,
        detector_mask: Optional[np.ndarray] = None,
) -> Dict[str, np.ndarray]:
    """
    Fast radial profile calculation with optional binning.
    
    This function provides an alternative to the full radial statistics
    calculation when you only need a binned radial profile.
    
    Args:
        image: The image data to analyze
        center: Center point (cx, cy) for radial analysis
        max_radius: Maximum radius to analyze
        num_bins: Number of radial bins (if None, uses pixel resolution)
        detector_mask: Boolean mask for masked pixels (True = masked)
        
    Returns:
        Dictionary containing binned radial profile
    """
    if num_bins is None:
        # Use full pixel resolution
        return calculate_radial_statistics_optimized(image, center, max_radius, detector_mask)

    h, w = image.shape
    cx, cy = center

    if max_radius is None:
        max_radius = min(cx, cy, w - cx, h - cy)
        max_radius = max(1, int(max_radius))

    # Create binned distance calculation
    bin_size = max_radius / num_bins

    # Efficient coordinate calculation for binning
    y_coords = np.arange(h, dtype=np.float32) - cy
    x_coords = np.arange(w, dtype=np.float32) - cx

    y_grid = y_coords[:, np.newaxis]
    x_grid = x_coords[np.newaxis, :]

    distances = np.sqrt(y_grid ** 2 + x_grid ** 2)

    # Convert to bin indices
    bin_indices = (distances / bin_size).astype(np.int32)
    bin_indices = np.clip(bin_indices, 0, num_bins - 1)

    # Apply masks
    valid_region = distances <= max_radius
    if detector_mask is not None and detector_mask.shape == image.shape:
        valid_region &= ~detector_mask

    if not np.any(valid_region):
        return {
            "bin_centers": np.linspace(0, max_radius, num_bins),
            "radial_sum": np.zeros(num_bins),
            "radial_average": np.zeros(num_bins),
            "pixel_counts": np.zeros(num_bins),
        }

    # Extract valid data
    valid_bins = bin_indices[valid_region]
    valid_intensities = image[valid_region]

    # Bin the data
    radial_sum = np.bincount(valid_bins, weights=valid_intensities, minlength=num_bins)
    pixel_counts = np.bincount(valid_bins, minlength=num_bins)

    # Calculate averages
    radial_average = np.divide(
        radial_sum,
        pixel_counts,
        out=np.zeros_like(radial_sum, dtype=np.float64),
        where=pixel_counts > 0
    )

    # Bin centers for plotting
    bin_centers = np.linspace(bin_size / 2, max_radius - bin_size / 2, num_bins)

    return {
        "bin_centers": bin_centers,
        "radial_sum": radial_sum,
        "radial_average": radial_average,
        "pixel_counts": pixel_counts,
    }
