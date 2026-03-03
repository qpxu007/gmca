"""
Parameter validation utilities for the image viewer.

This module provides functions for validating detector parameters,
coordinates, and other common validation tasks.
"""

from typing import Dict, Any, List, Optional

import numpy as np

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def validate_detector_parameters(
        params: Dict[str, Any],
        required_keys: List[str]
) -> bool:
    """
    Validate that detector parameters are present and finite.
    
    Args:
        params: Dictionary containing detector parameters
        required_keys: List of required parameter keys
        
    Returns:
        True if all parameters are valid, False otherwise
    """
    if not params:
        return False

    for key in required_keys:
        value = params.get(key)
        if value is None or not np.isfinite(value):
            return False

    return True


def validate_parameter_list(
        params: List[Any],
        min_value: Optional[float] = None,
        max_value: Optional[float] = None
) -> bool:
    """
    Validate a list of parameters are finite and within bounds.
    
    Args:
        params: List of parameters to validate
        min_value: Minimum allowed value (optional)
        max_value: Maximum allowed value (optional)
        
    Returns:
        True if all parameters are valid, False otherwise
    """
    for param in params:
        if not isinstance(param, (int, float, np.number)) or not np.isfinite(param):
            return False

        if min_value is not None and param < min_value:
            return False

        if max_value is not None and param > max_value:
            return False

    return True


def validate_coordinates(x: float, y: float, image_shape: tuple) -> bool:
    """
    Validate that coordinates are within image bounds.
    
    Args:
        x: X coordinate
        y: Y coordinate
        image_shape: Image shape (height, width)
        
    Returns:
        True if coordinates are valid, False otherwise
    """
    if len(image_shape) != 2:
        return False

    height, width = image_shape
    return (
            0 <= x < width and
            0 <= y < height and
            np.isfinite(x) and np.isfinite(y)
    )


def validate_image_data(image: np.ndarray) -> bool:
    """
    Validate that image data is valid for processing.
    
    Args:
        image: Image data to validate
        
    Returns:
        True if image is valid, False otherwise
    """
    if image is None:
        return False

    if not isinstance(image, np.ndarray) or image.ndim != 2:
        return False

    if image.size == 0:
        return False

    return True


def extract_valid_parameters(
        params: Dict[str, Any],
        keys: List[str]
) -> Optional[tuple]:
    """
    Extract and validate parameters from a dictionary.
    
    Args:
        params: Dictionary containing parameters
        keys: List of keys to extract
        
    Returns:
        Tuple of parameter values if all are valid, None otherwise
    """
    if not validate_detector_parameters(params, keys):
        return None

    return tuple(params[key] for key in keys)
