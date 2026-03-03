"""
Pixel information and coordinate calculation utilities for the image viewer.

This module provides functions for calculating pixel information, coordinates,
distances, and resolution-related calculations.
"""

from typing import Optional, Dict, Any, Tuple

import numpy as np

from qp2.image_viewer.utils.ring_math import angstrom_to_pixels
from qp2.image_viewer.utils.ring_math import radius_to_resolution
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def get_pixel_value(image_data: np.ndarray, x: int, y: int) -> Optional[float]:
    """
    Get pixel value at specified coordinates.
    
    Args:
        image_data: The image data
        x: X coordinate
        y: Y coordinate
        
    Returns:
        Pixel value or None if coordinates are invalid
    """
    if (
            image_data is not None
            and 0 <= y < image_data.shape[0]
            and 0 <= x < image_data.shape[1]
    ):
        return float(image_data[y, x])
    return None


def calculate_pixel_info(
        x: float,
        y: float,
        image_data: np.ndarray,
        params: Optional[Dict[str, Any]] = None
) -> Dict[str, str]:
    """
    Calculate comprehensive pixel information including coordinates, intensity, and resolution.
    
    Args:
        x: X coordinate
        y: Y coordinate
        image_data: The image data
        params: Dictionary containing beam center and detector parameters
        
    Returns:
        Dictionary containing pixel information
    """
    info = {"coords": f"({int(x)}, {int(y)})"}

    # Get pixel intensity
    intensity = get_pixel_value(image_data, int(x), int(y))
    if intensity is not None:
        info["intensity"] = (
            f"{intensity:.0f}"
            if isinstance(intensity, int)
            else f"{intensity:.2f}" if isinstance(intensity, float) else str(intensity)
        )
    else:
        info["intensity"] = "N/A"

    # Calculate resolution-related information if parameters are available
    required_keys = ["beam_x", "beam_y", "pixel_size", "det_dist", "wavelength"]
    if (
            params
            and all(
        params.get(k) is not None and np.isfinite(params[k]) for k in required_keys
    )
            and params["det_dist"] > 0  # MODIFICATION: Ensure detector distance is not zero
    ):
        try:
            bx, by, ps, dist, wl = (
                params["beam_x"],
                params["beam_y"],
                params["pixel_size"],
                params["det_dist"],
                params["wavelength"],
            )

            # Calculate relative position in mm
            rel_x_mm, rel_y_mm = (x - bx) * ps, (y - by) * ps
            info["relative"] = f"({rel_x_mm:.2f}, {rel_y_mm:.2f}) mm"

            # Calculate radius
            rad_px = np.sqrt((x - bx) ** 2 + (y - by) ** 2)
            rad_mm = rad_px * ps
            info["radius"] = f"{rad_px:.1f} pix ({rad_mm:.2f} mm)"

            # Calculate 2θ and resolution
            theta_rad = np.arctan(rad_mm / dist)
            info["two_theta"] = f"{np.degrees(theta_rad):.3f}°"

            theta_rad_half = theta_rad / 2.0
            if theta_rad_half > 1e-9:
                resolution = wl / (2 * np.sin(theta_rad_half))
                info["resolution"] = f"{resolution:.3f} Å"
            else:
                info["resolution"] = "-"

        except Exception as e:
            logger.debug(f"calculate_pixel_info: Error calculating resolution info: {e}", exc_info=True)
            info["relative"] = info["radius"] = info["two_theta"] = info["resolution"] = "-"
    else:
        info["relative"] = info["radius"] = info["two_theta"] = info["resolution"] = "-"

    return info


def calculate_distance(
        p1: Tuple[float, float],
        p2: Tuple[float, float],
        params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Calculate distance between two points in pixels and resolution.
    
    Args:
        p1: First point (x, y)
        p2: Second point (x, y)
        params: Dictionary containing detector parameters
        
    Returns:
        Dictionary containing distance information
    """
    dist_px = np.sqrt((p2[0] - p1[0]) ** 2 + (p2[1] - p1[1]) ** 2)

    result = {
        "distance_px": dist_px,
        "distance_A": None,
        "label": f"Distance: {dist_px:.2f} px"
    }

    # Calculate resolution if parameters are available
    required_keys = ["wavelength", "det_dist", "pixel_size"]
    if (
            params
            and all(
        params.get(k) is not None and np.isfinite(params[k]) for k in required_keys
    )
            and params["det_dist"] > 0  # MODIFICATION: Ensure detector distance is not zero
    ):
        try:
            dist_A = radius_to_resolution(
                params["wavelength"],
                params["det_dist"],
                dist_px,
                params["pixel_size"],
            )
            result["distance_A"] = dist_A
            result["label"] = f"Distance: {dist_px:.2f} px | {dist_A:.2f} Å"
        except Exception as e:
            logger.debug(f"calculate_distance: Error calculating resolution distance: {e}", exc_info=True)

    return result


def calculate_resolution_ring_radius(
        resolution: float,
        params: Dict[str, Any]
) -> Optional[float]:
    """
    Calculate the radius in pixels for a given resolution.
    
    Args:
        resolution: Resolution in Angstroms
        params: Dictionary containing detector parameters
        
    Returns:
        Radius in pixels or None if calculation fails
    """
    try:
        required_keys = ["wavelength", "det_dist", "pixel_size"]
        if (
                params
                and all(
            params.get(k) is not None and np.isfinite(params[k]) for k in required_keys
        )
                and params["det_dist"] > 0  # MODIFICATION: Ensure detector distance is not zero
        ):
            return angstrom_to_pixels(
                resolution,
                params["wavelength"],
                params["det_dist"],
                params["pixel_size"],
            )
    except Exception as e:
        logger.error(f"calculate_resolution_ring_radius: Error calculating resolution ring radius: {e}", exc_info=True)

    return None


def validate_coordinates(x: float, y: float, image_shape: Tuple[int, int]) -> bool:
    """
    Validate if coordinates are within image bounds.
    
    Args:
        x: X coordinate
        y: Y coordinate
        image_shape: Image shape (height, width)
        
    Returns:
        True if coordinates are valid, False otherwise
    """
    return (
            0 <= x < image_shape[1] and
            0 <= y < image_shape[0] and
            np.isfinite(x) and np.isfinite(y)
    )
