"""
Image statistics and analysis utilities for the image viewer.

This module provides functions for calculating image statistics, including
mean, standard deviation, maximum values, and other statistical measures.
"""

from typing import Optional, Dict, Any, Tuple

import numpy as np

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def calculate_image_statistics(
        image: np.ndarray,
        detector_mask: Optional[np.ndarray] = None
) -> Dict[str, Any]:
    """
    Calculate comprehensive image statistics.
    
    Args:
        image: The image data
        detector_mask: Boolean mask for masked pixels (True = masked)
        
    Returns:
        Dictionary containing image statistics
    """
    if image is None:
        return {
            "mean": 0.0,
            "std": 0.0,
            "max_val": 0.0,
            "max_x": 0,
            "max_y": 0,
            "min_val": 0.0,
            "total_pixels": 0,
            "valid_pixels": 0
        }

    # Create valid pixel mask
    valid_mask = np.isfinite(image)
    if detector_mask is not None and detector_mask.shape == image.shape:
        valid_mask &= ~detector_mask

    valid_pixels = image[valid_mask]

    if valid_pixels.size == 0:
        return {
            "mean": 0.0,
            "std": 0.0,
            "max_val": 0.0,
            "max_x": 0,
            "max_y": 0,
            "min_val": 0.0,
            "total_pixels": image.size,
            "valid_pixels": 0
        }

    # Calculate basic statistics
    mean = float(np.mean(valid_pixels))
    std = float(np.std(valid_pixels))
    max_val = float(np.max(valid_pixels))
    min_val = float(np.min(valid_pixels))

    # Find maximum position
    max_positions = np.where(image == max_val)
    if len(max_positions[0]) > 0:
        max_y, max_x = int(max_positions[0][0]), int(max_positions[1][0])
    else:
        max_x, max_y = image.shape[1] // 2, image.shape[0] // 2

    return {
        "mean": mean,
        "std": std,
        "max_val": max_val,
        "max_x": max_x,
        "max_y": max_y,
        "min_val": min_val,
        "total_pixels": image.size,
        "valid_pixels": valid_pixels.size
    }


def format_statistics_text(stats: Dict[str, Any]) -> str:
    """
    Format image statistics into a readable text string.
    
    Args:
        stats: Dictionary containing image statistics
        
    Returns:
        Formatted statistics text
    """
    return (
        f"Image Statistics:\n"
        f"Mean: {stats['mean']:.2f}\n"
        f"Std: {stats['std']:.2f}\n"
        f"Max: {stats['max_val']} at ({stats['max_x']}, {stats['max_y']})"
    )


def calculate_percentile_statistics(
        image: np.ndarray,
        percentiles: list = [1, 5, 25, 50, 75, 95, 99],
        detector_mask: Optional[np.ndarray] = None
) -> Dict[str, float]:
    """
    Calculate percentile-based statistics for an image.
    
    Args:
        image: The image data
        percentiles: List of percentiles to calculate
        detector_mask: Boolean mask for masked pixels (True = masked)
        
    Returns:
        Dictionary containing percentile values
    """
    if image is None:
        return {f"p{p}": 0.0 for p in percentiles}

    # Create valid pixel mask
    valid_mask = np.isfinite(image)
    if detector_mask is not None and detector_mask.shape == image.shape:
        valid_mask &= ~detector_mask

    valid_pixels = image[valid_mask]

    if valid_pixels.size == 0:
        return {f"p{p}": 0.0 for p in percentiles}

    # Calculate percentiles
    percentile_values = np.percentile(valid_pixels, percentiles)

    return {f"p{p}": float(val) for p, val in zip(percentiles, percentile_values)}


def calculate_region_statistics(
        image: np.ndarray,
        region: Tuple[int, int, int, int],  # (x_min, x_max, y_min, y_max)
        detector_mask: Optional[np.ndarray] = None
) -> Dict[str, Any]:
    """
    Calculate statistics for a specific region of an image.
    
    Args:
        image: The image data
        region: Region coordinates (x_min, x_max, y_min, y_max)
        detector_mask: Boolean mask for masked pixels (True = masked)
        
    Returns:
        Dictionary containing region statistics
    """
    x_min, x_max, y_min, y_max = region

    # Validate region bounds
    if (x_min < 0 or x_max > image.shape[1] or
            y_min < 0 or y_max > image.shape[0] or
            x_max <= x_min or y_max <= y_min):
        logger.warning(f"calculate_region_statistics: Invalid region bounds: {region}")
        return calculate_image_statistics(image, detector_mask)

    # Extract region
    region_image = image[y_min:y_max, x_min:x_max]
    region_mask = None

    if detector_mask is not None:
        region_mask = detector_mask[y_min:y_max, x_min:x_max]

    # Calculate statistics for the region
    stats = calculate_image_statistics(region_image, region_mask)
    stats["region"] = region

    return stats
