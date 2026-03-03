"""
Mask computation utilities for the image viewer.

This module provides functions for computing detector masks, including
masked circles, rectangles, and mask values.
"""

from typing import Optional, Dict, Any, Tuple

import numpy as np

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def compute_detector_mask_v0(
    image: np.ndarray,
    params: Optional[Dict[str, Any]] = None,
    mask_values: Optional[set] = None,
    masked_circles: Optional[list] = None,
    masked_rectangles: Optional[list] = None,
) -> Optional[np.ndarray]:
    """
    Compute a boolean mask for the detector, combining mask values, MASKED_CIRCLES, and MASKED_RECTANGLES.
    Supports expressions like 'beam_x-100'. Optimized version with caching.

    Args:
        image: The image data to compute mask for
        params: Dictionary containing beam center and other parameters
        mask_values: Set of pixel values to mask
        masked_circles: List of (cx, cy, r) tuples for circular masks
        masked_rectangles: List of (x0, y0, x1, y1) tuples for rectangular masks

    Returns:
        Boolean mask where True indicates masked pixels, or None if computation fails
    """
    from .performance_cache import get_performance_cache

    if image is None:
        logger.warning(
            "compute_detector_mask: No image provided, cannot compute detector mask."
        )
        return None

    # Check cache first
    cache = get_performance_cache()
    cached_mask = cache.get_detector_mask(image.shape, params or {})
    if cached_mask is not None:
        return cached_mask

    img_shape = image.shape
    mask = np.zeros(img_shape, dtype=bool)
    params = params or {}
    total_masked = 0

    # Helper to eval expressions
    def eval_expr(expr, axis):
        if isinstance(expr, (int, float)):
            return float(expr)
        if isinstance(expr, str):
            # Accept 'beam_x'/'beam_y' in config, not 'xbeam'/'ybeam'
            for key in ["beam_x"] if axis == "x" else ["beam_y"]:
                if key in expr:
                    val = params.get(key, 0)
                    expr = expr.replace(key, str(val))
            try:
                return float(eval(expr))
            except Exception as e:
                logger.warning(
                    f"compute_detector_mask: Failed to evaluate mask expression '{expr}': {e}",
                    exc_info=True,
                )
                return 0.0
        return 0.0

    # Optimized coordinate generation - create once and reuse
    if masked_circles or masked_rectangles:
        rr, cc = np.ogrid[: img_shape[0], : img_shape[1]]

    # Apply circular masks
    if masked_circles:
        for cx, cy, r in masked_circles:
            x0 = eval_expr(cx, "x")
            y0 = eval_expr(cy, "y")
            # Use pre-computed coordinate arrays
            dist_sq = (cc - x0) ** 2 + (rr - y0) ** 2  # Avoid sqrt until necessary
            circle_mask = dist_sq <= (float(r) ** 2)
            mask |= circle_mask
            total_masked += np.sum(circle_mask)

    # Apply rectangular masks
    if masked_rectangles:
        for x0, y0, x1, y1 in masked_rectangles:
            x0f = eval_expr(x0, "x")
            x1f = eval_expr(x1, "x")
            y0f = eval_expr(y0, "y")
            y1f = eval_expr(y1, "y")
            xmin, xmax = sorted([x0f, x1f])
            ymin, ymax = sorted([y0f, y1f])
            # Use pre-computed coordinate arrays
            rect_mask = (cc >= xmin) & (cc <= xmax) & (rr >= ymin) & (rr <= ymax)
            mask |= rect_mask
            total_masked += np.sum(rect_mask)

    # Apply mask values (optimized for common case)
    if mask_values:
        if len(mask_values) == 1:
            # Single value case - more efficient
            value = next(iter(mask_values))
            value_mask = image == value
        else:
            # Multiple values case
            mask_values_arr = np.array(list(mask_values)).astype(image.dtype)
            value_mask = np.isin(image, mask_values_arr)
        mask |= value_mask
        total_masked += np.sum(value_mask)

    logger.info(f"compute_detector_mask: Total masked pixels: {np.sum(mask)}")

    # Cache the result
    cache.cache_detector_mask(image.shape, params, mask)

    return mask


def compute_detector_mask(
    image: np.ndarray,
    params: Optional[Dict[str, Any]] = None,
    mask_values: Optional[set] = None,
    masked_circles: Optional[list] = None,
    masked_rectangles: Optional[list] = None,
) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
    """
    Compute boolean masks for the detector.

    Returns a tuple of two masks:
    1. A combined analysis mask (values + geometric regions).
    2. A display mask (values only).

    Both are boolean arrays where True indicates a masked pixel.
    """

    if image is None:
        logger.warning(
            "compute_detector_mask: No image provided, cannot compute masks."
        )
        return None, None

    # Caching is more complex with two masks; for simplicity, we re-compute here.
    # A more advanced cache could store the tuple.

    img_shape = image.shape
    params = params or {}

    # --- 1. Compute the display_mask (values only) ---
    display_mask = np.zeros(img_shape, dtype=bool)
    if mask_values:
        if len(mask_values) == 1:
            value = next(iter(mask_values))
            display_mask = image == value
        else:
            mask_values_arr = np.array(list(mask_values)).astype(image.dtype)
            display_mask = np.isin(image, mask_values_arr)

    # --- 2. Compute the combined analysis_mask ---
    # Start with the display mask and add geometric regions to it.
    analysis_mask = display_mask.copy()

    # Helper to eval expressions
    def eval_expr(expr, axis):
        if isinstance(expr, (int, float)):
            return float(expr)
        if isinstance(expr, str):
            for key in ["beam_x"] if axis == "x" else ["beam_y"]:
                if key in expr:
                    val = params.get(key, 0)
                    expr = expr.replace(key, str(val))
            try:
                return float(eval(expr))
            except Exception as e:
                logger.warning(
                    f"compute_detector_mask: Failed to evaluate mask expression '{expr}': {e}",
                )
                return 0.0
        return 0.0

    # Optimized coordinate generation - create once if needed
    if masked_circles or masked_rectangles:
        rr, cc = np.ogrid[: img_shape[0], : img_shape[1]]

    # Apply circular masks to analysis_mask
    if masked_circles:
        for cx, cy, r in masked_circles:
            x0 = eval_expr(cx, "x")
            y0 = eval_expr(cy, "y")
            dist_sq = (cc - x0) ** 2 + (rr - y0) ** 2
            circle_mask = dist_sq <= (float(r) ** 2)
            analysis_mask |= circle_mask

    # Apply rectangular masks to analysis_mask
    if masked_rectangles:
        for x0, y0, x1, y1 in masked_rectangles:
            x0f, x1f = sorted([eval_expr(x0, "x"), eval_expr(x1, "x")])
            y0f, y1f = sorted([eval_expr(y0, "y"), eval_expr(y1, "y")])
            rect_mask = (cc >= x0f) & (cc <= x1f) & (rr >= y0f) & (rr <= y1f)
            analysis_mask |= rect_mask

    # Mask saturated pixels in analysis mask
    sat_val = params.get("saturation_value", None)
    try:
        sat_val = float(sat_val) if sat_val is not None else None
    except Exception:
        sat_val = None

    if sat_val is not None:
        sat_mask = image > sat_val  # strictly greater-than, as requested
        analysis_mask |= sat_mask
        logger.info(
            f"compute_detector_mask: Saturated pixels masked (analysis): {int(np.sum(sat_mask))}"
        )

    logger.info(
        f"compute_detector_mask: Total analysis masked pixels: {np.sum(analysis_mask)}"
    )
    logger.info(
        f"compute_detector_mask: Display-only masked pixels: {np.sum(display_mask)}"
    )

    # Caching could be re-enabled here by storing the tuple
    # cache.cache_detector_mask(image.shape, params, (analysis_mask, display_mask))

    return analysis_mask, display_mask


def compute_annular_mask(
    image: np.ndarray,
    beam_x: float,
    beam_y: float,
    inner_radius: float,
    outer_radius: float,
) -> np.ndarray:
    """
    Compute an annular mask between inner_radius and outer_radius from beam center.

    Args:
        image: The image data
        beam_x: Beam center x coordinate
        beam_y: Beam center y coordinate
        inner_radius: Inner radius of annulus
        outer_radius: Outer radius of annulus

    Returns:
        Boolean mask where True indicates pixels within the annulus
    """
    Y, X = np.indices(image.shape)
    rr = np.sqrt((X - beam_x) ** 2 + (Y - beam_y) ** 2)
    return (rr >= inner_radius) & (rr <= outer_radius)


def compute_valid_pixel_mask(
    image: np.ndarray,
    detector_mask: Optional[np.ndarray] = None,
) -> np.ndarray:
    """
    Compute a mask for valid (non-masked) pixels.

    Args:
        image: The image data
        detector_mask: Precomputed detector mask (True = masked)

    Returns:
        Boolean mask where True indicates valid pixels
    """
    valid_mask = np.isfinite(image)
    if detector_mask is not None and detector_mask.shape == image.shape:
        valid_mask &= ~detector_mask
    return valid_mask


def get_mask_statistics(
    detector_mask: np.ndarray,
    image_shape: Tuple[int, int],
) -> Dict[str, Any]:
    """
    Get statistics about the detector mask.

    Args:
        detector_mask: Boolean detector mask
        image_shape: Shape of the original image

    Returns:
        Dictionary containing mask statistics
    """
    total_pixels = np.prod(image_shape)
    masked_pixels = np.sum(detector_mask)
    masked_percentage = (masked_pixels / total_pixels) * 100

    return {
        "total_pixels": int(total_pixels),
        "masked_pixels": int(masked_pixels),
        "valid_pixels": int(total_pixels - masked_pixels),
        "masked_percentage": float(masked_percentage),
        "mask_shape": detector_mask.shape,
    }
