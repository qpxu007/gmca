"""
Contrast and image processing utilities for the image viewer.

This module provides functions for calculating and applying image contrast,
including percentile-based contrast adjustment and view-based contrast updates.
"""

import os
import sys
from typing import Optional, Tuple

import numpy as np

# Allow running directly by adding parent directory to path
if __name__ == "__main__":
    current_dir = os.path.abspath(os.path.dirname(__file__))
    # Walk up until we find a directory containing "qp2"
    path_cursor = current_dir
    while path_cursor != "/" and os.path.basename(path_cursor) != "qp2":
        path_cursor = os.path.dirname(path_cursor)
    if os.path.basename(path_cursor) == "qp2":
        project_root = os.path.dirname(path_cursor)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def calculate_contrast_levels(
        image_data: np.ndarray,
        low_percentile: float = 50.0,
        high_percentile: float = 99.5,
        detector_mask: Optional[np.ndarray] = None,
) -> Tuple[float, float]:
    """
    Calculate contrast levels for an image using percentile-based approach.
    Optimized version with caching and memory-efficient processing.
    
    Args:
        image_data: The image data to calculate contrast for
        low_percentile: Lower percentile for contrast calculation
        high_percentile: Upper percentile for contrast calculation
        detector_mask: Boolean mask for masked pixels (True = masked)
        
    Returns:
        Tuple of (vmin, vmax) contrast levels
    """
    from .performance_cache import get_performance_cache

    vmin = vmax = 0.0
    try:
        if image_data is None or image_data.size == 0:
            logger.warning("calculate_contrast_levels: Empty or None image data for contrast calculation")
            return 0.0, 1.0

        # Check cache first
        cache = get_performance_cache()
        cached_result = cache.get_contrast(image_data, low_percentile, high_percentile, detector_mask)
        if cached_result is not None:
            return cached_result

        # Ensure percentiles are within valid range
        low_percentile = max(0.0, min(100.0, low_percentile))
        high_percentile = max(0.0, min(100.0, high_percentile))

        # Memory-efficient processing: avoid unnecessary copies
        if detector_mask is not None and detector_mask.shape == image_data.shape:
            # Extract only valid pixels
            valid_pixels = image_data[~detector_mask]
        else:
            valid_pixels = image_data.flat
            
        # For large arrays, use sampling to reduce memory and speed up percentile calculation
        if valid_pixels.size > 1e6:  # 1M pixels
            # Sample 100k pixels deterministically
            sample_size = min(100000, valid_pixels.size // 10)
            step = max(1, valid_pixels.size // sample_size)
            valid_pixels = valid_pixels[::step]
            logger.debug(
                f"calculate_contrast_levels: Using {len(valid_pixels)} sampled pixels from true {image_data.size} total")

        # Convert to float only if needed and only for the subset
        if np.issubdtype(image_data.dtype, np.integer):
            if valid_pixels.size > 0:
                # Convert only the valid pixels subset
                valid_pixels = valid_pixels.astype(np.float32)

        # Remove non-finite values
        if valid_pixels.size > 0:
            finite_mask = np.isfinite(valid_pixels)
            valid_pixels = valid_pixels[finite_mask]

        if valid_pixels.size > 0:
            vmin, vmax = np.percentile(valid_pixels, [low_percentile, high_percentile])
            if not (np.isfinite(vmin) and np.isfinite(vmax)):
                vmin, vmax = np.min(valid_pixels), np.max(valid_pixels)
            if vmax <= vmin:
                vmax = vmin + 1.0

            # Add sanity check for reasonable contrast range
            if vmax - vmin > 1e6:  # Very large range might indicate issues
                logger.warning(f"calculate_contrast_levels: Unusually large contrast range: {vmin} to {vmax}")
                # Fall back to a more conservative range
                vmin, vmax = np.percentile(valid_pixels, [1.0, 99.0])
                if vmax <= vmin:
                    vmax = vmin + 1.0
        else:
            logger.warning("calculate_contrast_levels: No valid pixels found for contrast calculation")
            vmin, vmax = 0.0, 1.0

        # Cache the result
        result = (float(vmin), float(vmax))
        cache.cache_contrast(image_data, low_percentile, high_percentile, detector_mask, vmin, vmax)

        return result

    except Exception as e:
        logger.error(f"calculate_contrast_levels: Error calculating contrast: {e}", exc_info=True)
        vmin, vmax = 0.0, 1.0

    return float(vmin), float(vmax)


def calculate_histogram_range(vmin: float, vmax: float, padding_factor: float = 0.1) -> Tuple[float, float]:
    """
    Calculate histogram range with padding around the contrast levels.
    
    Args:
        vmin: Minimum contrast level
        vmax: Maximum contrast level
        padding_factor: Factor for padding around the range
        
    Returns:
        Tuple of (hist_min, hist_max) histogram range
    """
    range_size = vmax - vmin
    padding = range_size * padding_factor
    hist_min = vmin - padding
    hist_max = vmax + padding

    if hist_max <= hist_min:
        hist_max = hist_min + 1.0

    return float(hist_min), float(hist_max)


def extract_view_subset(
        full_image: np.ndarray,
        view_range: Tuple[Tuple[float, float], Tuple[float, float]]
) -> Optional[np.ndarray]:
    """
    Extract a subset of an image based on the current view range.
    
    Args:
        full_image: The full image data
        view_range: View range as ((x_min, x_max), (y_min, y_max)) from PyQtGraph
        
    Returns:
        Image subset for the view range, or None if invalid
    """
    try:
        # PyQtGraph viewRange() returns ((x_min, x_max), (y_min, y_max))
        x_range, y_range = view_range
        x_min, x_max = x_range
        y_min, y_max = y_range

        # Convert to image coordinates (ensure they're within bounds)
        y_min = max(0, int(np.floor(y_min)))
        y_max = min(full_image.shape[0], int(np.ceil(y_max)))
        x_min = max(0, int(np.floor(x_min)))
        x_max = min(full_image.shape[1], int(np.ceil(x_max)))

        # Ensure we have a valid subset
        if y_max > y_min and x_max > x_min:
            subset = full_image[y_min:y_max, x_min:x_max]
            # Check if subset is too small for reliable contrast calculation
            min_size = 10  # Minimum 10x10 pixels
            if subset.shape[0] < min_size or subset.shape[1] < min_size:
                logger.debug(
                    f"extract_view_subset: View subset too small for contrast calculation: {subset.shape}, using full image")
                return None
            logger.debug(
                f"extract_view_subset: Extracted view subset: shape={subset.shape}, range=({x_min},{x_max}),({y_min},{y_max})")
            return subset
        else:
            logger.warning(
                f"extract_view_subset: Invalid view range for subset extraction: x=({x_min},{x_max}), y=({y_min},{y_max})")
            return None

    except Exception as e:
        logger.error(f"extract_view_subset: Error extracting view subset: {e}", exc_info=True)
        return None


def calculate_zoom_ratio(view_pixel_size: Tuple[float, float]) -> float:
    """
    Calculate zoom ratio from view pixel size.
    
    Args:
        view_pixel_size: Tuple of (sx, sy) view pixel sizes
        
    Returns:
        Zoom ratio (1.0 = no zoom, >1.0 = zoomed in)
    """
    sx, sy = view_pixel_size
    if sx > 1e-12 and sy > 1e-12:
        return min(1.0 / sx, 1.0 / sy)
    else:
        return 0.0
