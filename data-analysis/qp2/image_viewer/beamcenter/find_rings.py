import math
from contextlib import contextmanager
from functools import lru_cache
from typing import Dict, Tuple, List, Optional, Any

import numpy as np
from PyQt5.QtCore import QRunnable, QObject, pyqtSignal
from numpy.linalg import eig, inv

from qp2.image_viewer.utils.ring_math import (
    resolution_to_distance,
    resolution_to_energy,
)
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def prepare_calibration_message(
    refined_circle, header, calibration_resolution=3.03, refine_circle_radii=None
):
    logger.debug(
        f"prepare_calibration_message: refined_circle: {refined_circle}, refine_circle_radii: {refine_circle_radii}"
    )
    logger.debug(f"prepare_calibration_message: header: {header}")
    logger.debug(
        f"prepare_calibration_message: calibration_resolution: {calibration_resolution}"
    )
    try:
        beam_x = header.get("beam_x", 0)
        beam_y = header.get("beam_y", 0)
        wavelength = header.get("wavelength", 1.0)
        distance = header.get("det_dist", 150)
        pixel_size = header.get("pixel_size", 0.075)
        # beam center check
        refined_x, refined_y, refined_radius = refined_circle
        dx = refined_x - beam_x
        dy = refined_y - beam_y
        max_diff = np.sqrt(dx**2 + dy**2)

        # Prepare pop-up message content
        popup_text = f"Beam Center Calibration Results:\n\n"
        popup_text += f"1) Beam Center Comparison:\n"
        popup_text += f"   - Existing Beam Center: ({beam_x:.1f}, {beam_y:.1f}) px\n"
        popup_text += (
            f"   - Refined Beam Center: ({refined_x:.1f}, {refined_y:.1f}) px\n"
        )
        popup_text += (
            f"   - Max Difference: {max_diff:.1f} px (dx={dx:.1f}, dy={dy:.1f})\n"
        )

        if (
            refine_circle_radii is not None
            and len(refine_circle_radii) == 2
            and all(x > 0 for x in refine_circle_radii)
        ):
            rx, ry = refine_circle_radii
            diff = abs(rx - ry)
            popup_text += f"   - Difference in ellipse radii: {diff:.1f} pixels\n\n"

        # distance check
        calculated_det_dist = resolution_to_distance(
            calibration_resolution, wavelength, refined_radius, pixel_size
        )
        det_dist_diff = calculated_det_dist - distance
        popup_text += f"2) Detector Distance Calculation (based on {calibration_resolution:.2f} Å ring):\n"
        popup_text += f"   - Calculated Distance: {calculated_det_dist:.2f} mm\n"
        popup_text += f"   - Metadata Distance: {distance:.2f} mm\n"
        popup_text += f"   - Difference: {det_dist_diff:.2f} mm\n\n"

        # energy check
        calculated_energy = resolution_to_energy(
            calibration_resolution, distance, refined_radius, pixel_size
        )
        energy_diff = calculated_energy - 12398.4 / wavelength
        popup_text += (
            f"3) Energy Calculation (based on {calibration_resolution:.4f} Å ring):\n"
        )
        popup_text += f"   - Calculated Energy: {calculated_energy:.2f} eV\n"
        popup_text += f"   - Metadata Energy: {12398.4 / wavelength:.2f} eV\n"
        popup_text += f"   - Difference: {energy_diff:.2f} eV"
    except Exception as e:
        logger.error(e)
        return f"Calibration failed: {e}"

    return popup_text


def find_local_maxima_in_annulus(
    image, center, r1, r2, min_distance=10, threshold_abs=None, detector_mask=None
):
    """
    Find local maxima in an image within the region between two concentric circles.

    Args:
        image: Input image as a 2D numpy array.
        center: Tuple (x0, y0) representing the center of the concentric circles.
        r1: Inner radius of the annular region.
        r2: Outer radius of the annular region (r2 > r1).
        min_distance: Minimum distance between detected peaks (default=10).
        threshold_abs: Absolute intensity threshold for peaks (default=None, no threshold).
        detector_mask: Boolean mask for masked pixels (True = masked).

    Returns:
        numpy.ndarray: Array of shape (N, 3) where each row contains (x, y, value)
                       of local maxima within the annular region.
                       Returns empty array if no maxima are found.
    """

    if r2 <= r1:
        logger.error("Error: Outer radius r2 must be greater than inner radius r1")
        return np.array([], dtype=np.float64).reshape(0, 3)

    if image is None or image.size == 0:
        logger.error("Error: Empty or invalid image provided")
        return np.array([], dtype=np.float64).reshape(0, 3)

    x0, y0 = center
    y_indices, x_indices = np.indices(image.shape)
    distances = np.sqrt((x_indices - x0) ** 2 + (y_indices - y0) ** 2)

    # Create a mask for the annular region between r1 and r2
    annulus_mask = (distances >= r1) & (distances <= r2)

    # Apply detector mask to exclude masked pixels
    if (
        detector_mask is not None
        and isinstance(detector_mask, np.ndarray)
        and detector_mask.shape == image.shape
    ):
        annulus_mask &= ~detector_mask
        logger.debug(
            f"Applied detector mask to annular search, excluding {np.sum(detector_mask)} masked pixels"
        )

    # Apply the mask to the image (set values outside the annulus to a low value)
    masked_image = np.where(annulus_mask, image, float(np.min(image)) - 1)

    # Find local maxima using peak_local_max
    from skimage.feature import peak_local_max

    coordinates = peak_local_max(
        masked_image,
        min_distance=min_distance,
        threshold_abs=threshold_abs,
        exclude_border=False,
    )

    # Filter coordinates to ensure they are within the annular region and not masked
    if coordinates.size > 0:
        coord_y, coord_x = coordinates[:, 0], coordinates[:, 1]
        coord_distances = np.sqrt((coord_x - x0) ** 2 + (coord_y - y0) ** 2)
        valid_coords = (coord_distances >= r1) & (coord_distances <= r2)

        # Additional check for detector mask
        if detector_mask is not None and isinstance(detector_mask, np.ndarray):
            valid_coords &= ~detector_mask[coord_y, coord_x]

        coordinates = coordinates[valid_coords]

    # If no valid coordinates are found, return empty array
    if coordinates.size == 0:
        return np.array([], dtype=np.float64).reshape(0, 3)

    # Extract pixel values at the coordinates
    pixel_values = image[coordinates[:, 0], coordinates[:, 1]]
    logger.info(f"find_local_maxima_in_annulus_: found {len(coordinates)} peaks")
    # Drop weak peaks: keep only those above mean + std (SNR-based)
    if len(pixel_values) > 0:
        mean = np.mean(pixel_values)
        std = np.std(pixel_values)
        threshold = mean + std
        strong_mask = pixel_values > threshold
        # Fallback: if all dropped, keep top 10% by value
        if np.sum(strong_mask) <= 20:
            perc90 = np.percentile(pixel_values, 90)
            strong_mask = pixel_values > perc90
            logger.debug(f"find_local_maxima_in_annulus_: fallback to 90th percentile")
        coordinates = coordinates[strong_mask]
        pixel_values = pixel_values[strong_mask]
    logger.info(
        f"find_local_maxima_in_annulus_: found {len(coordinates)} peaks after dropping weak peaks"
    )
    result = np.column_stack((coordinates[:, 1], coordinates[:, 0], pixel_values))

    return result


def extract_high_intensity_points(
    image,
    center,
    inner_radius,
    outer_radius,
    intensity_percentile=90.0,
    detector_mask=None,
):
    """
    Extract coordinates and values of high-intensity points within an annular region of an image.

    Args:
        image: Input image as a 2D numpy array.
        center: Tuple (x0, y0) representing the center of the annular region.
        inner_radius: Inner radius of the annular region.
        outer_radius: Outer radius of the annular region (must be greater than inner_radius).
        intensity_percentile: Percentile value for intensity threshold (default=90.0).
        detector_mask: Boolean mask for masked pixels (True = masked).

    Returns:
        numpy.ndarray: Array of shape (N, 3) where each row contains (x, y, value)
                       of high-intensity points within the annular region.
                       Returns empty array if no points are found or input is invalid.
    """
    if outer_radius <= inner_radius:
        logger.error("Error: Outer radius must be greater than inner radius")
        return np.array([], dtype=np.float64).reshape(0, 3)

    if image is None or image.size == 0:
        logger.error("Error: Empty or invalid image provided")
        return np.array([], dtype=np.float64).reshape(0, 3)

    x0, y0 = center
    y_indices, x_indices = np.indices(image.shape)

    # Calculate distance matrix from the center
    r = np.sqrt((x_indices - x0) ** 2 + (y_indices - y0) ** 2)

    # Create annular mask between inner_radius and outer_radius
    annulus_mask = (r >= inner_radius) & (r <= outer_radius)

    # Apply detector mask to exclude masked pixels
    if (
        detector_mask is not None
        and isinstance(detector_mask, np.ndarray)
        and detector_mask.shape == image.shape
    ):
        annulus_mask &= ~detector_mask
        logger.debug(
            f"Applied detector mask to high intensity search, excluding {np.sum(detector_mask)} masked pixels"
        )

    # Calculate intensity threshold based on percentile within the annular region (excluding masked pixels)
    valid_pixels = image[annulus_mask]
    if valid_pixels.size == 0:
        logger.error(
            "Warning: No valid pixels found in the annular region after masking"
        )
        return np.array([], dtype=np.float64).reshape(0, 3)

    threshold = np.percentile(valid_pixels, intensity_percentile)

    # Create mask for high-intensity points within the annular region
    high_intensity_mask = (image > threshold) & annulus_mask

    # Extract coordinates and values of high-intensity points
    y, x = np.where(high_intensity_mask)
    if x.size == 0:
        logger.error("Warning: No high-intensity points found in the annular region")
        return np.array([], dtype=np.float64).reshape(0, 3)

    v = image[y, x]
    data_points = np.column_stack((x, y, v))

    return data_points


def apply_gamma_transform(img, gamma=0.5):
    invGamma = 1.0 / gamma
    # Build the lookup table
    table = np.array(
        [((i / 255.0) ** invGamma) * 255 for i in np.arange(0, 256)]
    ).astype("uint8")
    from cv2 import LUT

    corrected_image = LUT(img, table)
    return corrected_image


class RingFinder:
    def __init__(self, debug: bool = False):
        self.debug = debug

    def _preprocess_image(
        self, image: np.ndarray, detector_mask: Optional[np.ndarray] = None
    ) -> np.ndarray:
        """Preprocess image by masking pixels and applying median filter.

        Args:
            image: Input image array.
            detector_mask: Boolean mask for masked pixels.

        Returns:
            Processed image array.
        """
        img_processed = image.copy()

        # Apply detector_mask if provided
        if (
            detector_mask is not None
            and isinstance(detector_mask, np.ndarray)
            and detector_mask.shape == image.shape
        ):
            img_processed[detector_mask] = 0
            logger.debug(
                f"Applied detector mask, setting {np.sum(detector_mask)} pixels to 0"
            )

        # denoised_image = cv2.medianBlur(img_processed, 3)
        from scipy.ndimage import median_filter

        denoised_image = median_filter(img_processed, size=3)
        # denoised_image = apply_log_filter(img_processed, 11)
        return denoised_image

    def _normalize_image(self, image: np.ndarray, weak=True) -> np.ndarray:
        """Normalize image to 8-bit range (0-255).

        Args:
            image: Input image array.

        Returns:
            Normalized 8-bit image, robust to hot pixels.
        """
        # vmax = np.max(image)
        # vmin = np.min(image)
        # if vmax == vmin:
        #     return np.zeros_like(image, dtype=np.uint8)
        # normalized_image = (
        #     ((image - vmin) / (vmax - vmin) * 255).clip(0, 255).astype(np.uint8)
        # )
        # if weak:
        #     return apply_gamma_transform(normalized_image, gamma=0.8)
        # return normalized_image

        vmin, vmax = np.percentile(image, (5, 99.5))
        if vmax <= vmin:  # Fallback for low-contrast images
            vmin, vmax = np.min(image), np.max(image)

        if vmax == vmin:
            return np.zeros_like(image, dtype=np.uint8)

        # Clip the image data to the percentile range before scaling
        image = np.clip(image, vmin, vmax)
        normalized_image = (
            ((image - vmin) / (vmax - vmin) * 255).clip(0, 255).astype(np.uint8)
        )
        return normalized_image

    def fit_ellipse_with_parameters(self, data_points, weighted=True):
        """
        Fit an ellipse to data points using weighted least squares and calculate its parameters.

        Args:
            data_points: Array of shape (N, 3) with x, y coordinates and weights (e.g., pixel intensities).
            weighted: Boolean to toggle weighted fitting (default True).

        Returns:
            dict: Dictionary containing the ellipse's center, radii (semi-major and semi-minor axes),
                and rotation angle. Returns zero values if fitting fails.
        """
        try:
            if data_points.shape[0] < 6:  # Need at least 6 points for ellipse fitting
                logger.error(
                    "Error: Insufficient points for ellipse fitting (minimum 6 required)"
                )
                return {"radius": np.array([0, 0]), "center": [0, 0], "angle": 0}

            x = data_points[:, 0]
            y = data_points[:, 1]
            # Use pixel values as weights (normalize to prevent extreme values)
            if weighted:
                intensities = data_points[:, 2]
                # Normalize weights to avoid numerical issues; clip to avoid division by zero
                w = intensities / np.max(intensities)
                w = np.clip(w, 1e-6, 1.0)
            else:
                w = np.ones_like(x)

            x = x[:, np.newaxis]
            y = y[:, np.newaxis]
            # Design matrix for ellipse equation: ax² + bxy + cy² + dx + ey + f = 0
            D = np.hstack((x * x, x * y, y * y, x, y, np.ones_like(x)))
            # Apply weights to design matrix
            w_matrix = np.tile(w, (6, 1)).T
            D_weighted = D * w_matrix
            S = np.dot(D_weighted.T, D_weighted)

            # Constraint matrix for ellipse condition (4ac - b² = 1)
            C = np.zeros([6, 6])
            C[0, 2] = C[2, 0] = 2
            C[1, 1] = -1

            # Solve generalized eigenvalue problem
            try:
                E, V = eig(np.dot(inv(S), C))
                # Select eigenvector corresponding to the largest absolute eigenvalue
                n = np.argmax(np.abs(E))
                coeffs = V[:, n]
            except np.linalg.LinAlgError:
                logger.error("Error: Singular matrix in eigenvalue computation")
                return {"radius": np.array([0, 0]), "center": [0, 0], "angle": 0}

            # Extract coefficients with scaling as per original code
            a = coeffs[0]  # coefficient for x²
            b = coeffs[1]  # coefficient for xy
            c = coeffs[2]  # coefficient for y²
            d = coeffs[3]  # coefficient for x
            e = coeffs[4]  # coefficient for y
            f = coeffs[5]  # constant term

            # Adjust b, d, e as per original code's scaling for parameter calculation
            b_half = b / 2
            d_half = d / 2
            e_half = e / 2

            # Calculate the denominator for center computation
            denom = b_half * b_half - a * c
            if abs(denom) < 1e-20:  # Adjusted threshold for numerical stability
                logger.error(
                    f"Warning: Denominator close to zero in ellipse center calculation: denom={denom}"
                )
                return {"radius": np.array([0, 0]), "center": [0, 0], "angle": 0}

            # Calculate center coordinates
            x0 = (c * d_half - b_half * e_half) / denom
            y0 = (a * e_half - b_half * d_half) / denom
            center = [x0, y0]

            # Check if it's an ellipse (determinant condition b² - 4ac < 0 for ellipse)
            if denom >= 0:
                logger.error(
                    "Warning: Coefficients do not define an ellipse (determinant condition failed)"
                )
                return {"radius": np.array([0, 0]), "center": [0, 0], "angle": 0}

            # Calculate rotation angle (tilt of major axis)
            if b == 0:
                angle = 0 if a < c else math.pi / 2
            else:
                root = math.sqrt((a - c) ** 2 + b**2)
                angle = math.atan2(c - a + root, -b)  # Angle of major axis

            # Calculate semi-major and semi-minor axes lengths
            up = 2 * (
                a * e_half * e_half
                + c * d_half * d_half
                + f * b_half * b_half
                - 2 * b_half * d_half * e_half
                - a * c * f
            )
            down1 = denom * (
                (c - a) * math.sqrt(1 + 4 * b_half * b_half / ((a - c) * (a - c)))
                - (c + a)
            )
            down2 = denom * (
                (a - c) * math.sqrt(1 + 4 * b_half * b_half / ((a - c) * (a - c)))
                - (c + a)
            )

            # Handle potential numerical issues in axes calculation
            try:
                res1 = math.sqrt(up / down1) if (up / down1) > 0 else 0
                res2 = math.sqrt(up / down2) if (up / down2) > 0 else 0
                radii = np.array(
                    [max(res1, res2), min(res1, res2)]
                )  # Ensure semi-major is larger
            except (ValueError, ZeroDivisionError) as e:
                logger.error(f"Error in axes calculation: {e}")
                return {"radius": np.array([0, 0]), "center": [0, 0], "angle": 0}

            return {
                "radius": radii,
                "center": [round(x, 2) for x in center],
                "angle": round(angle * 180.0 / math.pi, 2),  # Convert to degrees
            }
        except Exception as e:
            logger.error(f"Error in fit_ellipse_with_parameters: {e}")
            return {"radius": np.array([0, 0]), "center": [0, 0], "angle": 0}

    def fit_ellipse(self, data_points):
        """Fit an ellipse to data points using scikit-image's EllipseModel.

        Args:
            data_points: Array of shape (N, 3) with x, y coordinates and values.

        Returns:
            dict: Dictionary with ellipse parameters (center, radius, angle).
        """
        if data_points.shape[0] < 5:
            return {"radius": np.array([0, 0]), "center": [0, 0], "angle": 0}

        # Extract x, y coordinates (ignore values for fitting)
        xy = data_points[:, :2]

        # Fit ellipse using EllipseModel
        from skimage.measure import EllipseModel

        model = EllipseModel()
        if model.estimate(xy):
            xc, yc, a, b, theta = model.params
            return {"radius": np.array([a, b]), "center": [xc, yc], "angle": theta}
        else:
            return {"radius": np.array([0, 0]), "center": [0, 0], "angle": 0}

    def _select_peaks_by_valley_ratio(
        self,
        signal: np.ndarray,
        peak_indices: np.ndarray,
        properties: Dict,
        x_factor: float = 2.0,
    ) -> np.ndarray:
        """Select peaks where peak height is at least x_factor times the valley height.

        Args:
            signal: The 1D signal array.
            peak_indices: Indices of peaks found by find_peaks.
            properties: Properties dictionary from find_peaks (must contain 'prominences').
            x_factor: The minimum ratio required (Peak Height / Valley Height).

        Returns:
            Indices of the peaks satisfying the condition.
        """
        if "prominences" not in properties or peak_indices.size == 0:
            return np.array([], dtype=int)

        prominences = properties["prominences"]
        peak_heights = signal[peak_indices]
        valley_heights = peak_heights - prominences
        condition = np.logical_or(
            np.logical_and(
                valley_heights > 0, peak_heights >= x_factor * valley_heights
            ),
            np.logical_and(valley_heights <= 0, peak_heights > 0),
        )
        return peak_indices[condition]

    @lru_cache(maxsize=32)
    def _calculate_distance_matrix(
        self, shape: Tuple[int, int], center_x: float, center_y: float
    ) -> np.ndarray:
        """Calculate and cache the distance matrix for a given shape and center.

        Args:
            shape: Shape of the image.
            center_x: X-coordinate of the center.
            center_y: Y-coordinate of the center.

        Returns:
            Distance matrix.
        """
        y_indices, x_indices = np.indices(shape)
        return np.sqrt((x_indices - center_x) ** 2 + (y_indices - center_y) ** 2)

    @contextmanager
    def _debug_visualization(self):
        """Context manager for conditional debugging visualization."""
        if self.debug:
            import matplotlib.pyplot as plt

            yield plt
            plt.show()
        else:

            class NullContext:
                def __getattr__(self, name):
                    return lambda *args, **kwargs: None

            yield NullContext()

    def _visualize_results(
        self,
        image: np.ndarray,
        refined_center: List[float],
        selected_peak_indices: np.ndarray,
        filtered_image: np.ndarray,
        high_intensity_points: Optional[np.ndarray] = None,
    ):
        """Visualize ring detection results.

        Args:
            image: Original image.
            refined_center: Center of the refined ellipse.
            selected_peak_indices: Selected peak indices for rings.
            filtered_image: Filtered image for display.
            high_intensity_points: Points of high intensity for scatter plot.
        """
        import matplotlib.pyplot as plt
        from matplotlib.patches import Circle

        vmin = np.percentile(filtered_image, 5)
        vmax = np.percentile(filtered_image, 95)
        plt.figure(figsize=(10, 8))
        plt.imshow(filtered_image, cmap="gray", vmin=vmin, vmax=vmax)
        if high_intensity_points is not None:
            x, y = high_intensity_points[:, 0], high_intensity_points[:, 1]
            plt.scatter(x, y, alpha=0.5, color="red")
        for radius in selected_peak_indices:
            circ = Circle(
                refined_center, radius, fill=False, edgecolor="lime", linewidth=1
            )
            plt.gca().add_patch(circ)
        plt.title("Detected Rings")
        plt.colorbar(label="Intensity")
        plt.tight_layout()

    def find_rings(
        self,
        image: np.ndarray,
        start_circle: Optional[Tuple[float, float, float]] = None,
        detector_mask: Optional[np.ndarray] = None,
        band_width: int = 2,
        intensity_percentile: float = 90.0,
        peak_distance: int = 50,
        peak_prominence: int = 1,
        valley_ratio: float = 1.5,
        mode="Refine",
    ) -> Optional[Dict[str, Any]]:
        """Find circular rings in an image.

        Args:
            image: Input image array.
            start_circle: Initial guess for circle (x, y, radius).
            detector_mask: Boolean mask for masked pixels.
            band_width: Width of annular band for ellipse fitting.
            intensity_percentile: Percentile for threshold calculation.
            peak_distance: Minimum distance between peaks.
            peak_prominence: Minimum prominence for peak detection.
            valley_ratio: Ratio threshold for peak selection.

        Returns:
            Dictionary containing detection results or None if failed.
        """
        if image is None or image.size == 0:
            logger.error("Empty or invalid image provided")
            return None

        logger.debug(f"start_circle: {start_circle}")

        # Initialize starting circle if not provided
        if start_circle is None:
            cx_guess, cy_guess = image.shape[0] // 2, image.shape[1] // 2
            r2 = max(cx_guess, cy_guess)
            r1 = 300
            cr_guess = r2 // 2
        else:
            cx_guess, cy_guess, cr_guess = start_circle
            r1 = max(300, cr_guess - 200)
            r2 = min(cr_guess + 200, max(image.shape) // 2)

        if cx_guess <= 0 or cy_guess <= 0 or cr_guess <= 0:
            logger.warning(f"invalid start_circle: {start_circle}")
            cx_guess, cy_guess = image.shape[0] // 2, image.shape[1] // 2
            cr_guess = max(image.shape) // 2
            mode = "StartScratch"
            r2 = cr_guess
            r1 = 300

        logger.info(f"find_rings: mode: {mode}, max beam error: {band_width}px")
        # Preprocess and normalize image
        filtered_image = self._preprocess_image(image, detector_mask=detector_mask)

        if mode == "StartScratch":  # use Hough transform to find the best circle
            # --- OPTIMIZATION FOR LARGE IMAGES ---
            # Downscale the image before running the expensive Hough transform.
            TARGET_DIMENSION = 512.0
            h, w = filtered_image.shape
            scale_factor = TARGET_DIMENSION / max(h, w)

            if scale_factor >= 1.0:
                scale_factor = 1.0
                image_small = filtered_image
            else:
                import cv2

                image_small = cv2.resize(
                    filtered_image.astype(np.float32),
                    (int(w * scale_factor), int(h * scale_factor)),
                    interpolation=cv2.INTER_AREA,
                )
            logger.info(
                f"Downscaled image from {h}x{w} to {image_small.shape[0]}x{image_small.shape[1]} (factor: {scale_factor:.3f}) for Hough transform."
            )

            image_gray = self._normalize_image(image_small)

            # Detect circles using Hough Transform
            param1 = np.percentile(image_gray, intensity_percentile) + 1
            param2 = 30.0
            r1_scaled = int(r1 * scale_factor)
            r2_scaled = int(r2 * scale_factor)
            min_dist_scaled = int(image_small.shape[1])
            logger.info(
                f"Hough transform parameters: param1={param1} param2={param2} r_in={r1_scaled}px r_out={r2_scaled}px"
            )

            from cv2 import HoughCircles, HOUGH_GRADIENT

            circles_scaled = HoughCircles(
                image_gray,
                HOUGH_GRADIENT,
                dp=2,
                minDist=min_dist_scaled,
                param1=param1,
                param2=param2,
                minRadius=r1_scaled,
                maxRadius=r2_scaled,
            )
            logger.info(f"Hough transform: done")

            if circles_scaled is None:
                logger.warning("No circles detected by Hough Transform")
                return None

            circles = circles_scaled[0, :] / scale_factor
            circles = np.round(circles).astype("float")

            circle0 = None
            if len(circles) > 0:
                circle_centers = circles[:, :2]
                distances_sq = np.sum(
                    (circle_centers - np.array([cx_guess, cy_guess])) ** 2, axis=1
                )
                closest_idx = np.argmin(distances_sq)
                circle0 = tuple(circles[closest_idx])
                logger.info(
                    f"Found center vs provided center: dx={cx_guess - circle0[0]:.2f}, dy={cy_guess - circle0[1]:.2f}, dr={cr_guess - circle0[2]:.2f}"
                )

            # Refine center using ellipse fitting
            hough_center = [circle0[0], circle0[1]]
            hough_radius = circle0[2]

            # grid search to find the best center
            logger.info(
                f"brute force search to find refined center: center: {hough_center} radius: {hough_radius} band_width: 5"
            )
            result = annulus_search_radial_sum(
                filtered_image, hough_center, hough_radius, 5, detector_mask
            )
            hough_center = result[:2]
            logger.info(
                f"search done: refined hough center: {hough_center} radius: {hough_radius}"
            )
        else:
            hough_center = [cx_guess, cy_guess]
            hough_radius = cr_guess
            circle0 = None

        min_distance = 10
        threshold_abs = 2
        # locate local maxima within the around the ring to refine start circle
        # from hough transform or current beam center
        data_points = find_local_maxima_in_annulus(
            filtered_image,
            hough_center,
            hough_radius - band_width,
            hough_radius + band_width,
            min_distance=min_distance,
            threshold_abs=threshold_abs,
            detector_mask=detector_mask,
        )

        # use percentile if only too few pixels found
        if len(data_points) <= 20:
            data_points = extract_high_intensity_points(
                filtered_image,
                hough_center,
                hough_radius - band_width,
                hough_radius + band_width,
                intensity_percentile=intensity_percentile,
                detector_mask=detector_mask,
            )

        ellipse = self.fit_ellipse(data_points)
        logger.info(f"refined ellipse A: {ellipse}")
        logger.info(
            f"refined ellipse B(weighted): {self.fit_ellipse_with_parameters(data_points, weighted=True)}"
        )
        logger.info(
            f"refined ellipse B(un weighted): {self.fit_ellipse_with_parameters(data_points, weighted=False)}"
        )

        refined_center = ellipse["center"]
        refined_radius = np.mean(ellipse["radius"])
        logger.info(
            f"Refine ellipse results: {ellipse}, using {len(data_points)} pixels"
        )
        logger.info(
            f"Difference in ellipse radii: {abs(ellipse['radius'][0] - ellipse['radius'][1])} pixels"
        )

        # Calculate radial sum with refined center
        r_refined = self._calculate_distance_matrix(
            image.shape, refined_center[0], refined_center[1]
        )
        r_int = r_refined.astype(int)
        max_radius = int(np.max(r_int))
        radial_sum = np.bincount(
            r_int.ravel(), weights=filtered_image.ravel(), minlength=max_radius + 1
        )
        # np.save('radial_sum.npy', radial_sum)

        # Find and select peaks
        from scipy.signal import find_peaks

        peak_indices, properties = find_peaks(
            radial_sum, height=0, distance=peak_distance, prominence=peak_prominence
        )
        selected_peak_indices = self._select_peaks_by_valley_ratio(
            radial_sum, peak_indices, properties, x_factor=valley_ratio
        )
        # Debug visualization if enabled
        with self._debug_visualization() as plt:
            if self.debug:
                plt.figure(figsize=(10, 6))
                plt.plot(radial_sum, label="Radial Sum")
                plt.plot(peak_indices, radial_sum[peak_indices], "x", label="All Peaks")
                plt.plot(
                    selected_peak_indices,
                    radial_sum[selected_peak_indices],
                    "o",
                    label="Selected Peaks",
                )
                plt.legend()
                plt.xlabel("Radius (pixels)")
                plt.ylabel("Intensity Sum")
                plt.title("Radial Intensity Profile with Detected Rings")
                plt.grid(True, alpha=0.3)
                self._visualize_results(
                    image,
                    refined_center,
                    selected_peak_indices,
                    filtered_image,
                    data_points,
                )

        return {
            "start_circle": (cx_guess, cy_guess, cr_guess),
            "hough_circle": circle0,
            "refined_circle": (
                refined_center[0],
                refined_center[1],
                refined_radius,
            ),
            "refine_circle_radii": ellipse["radius"],
            "selected_peak_indices": selected_peak_indices,
            "strong_pixels": data_points,
        }


class WorkerSignals(QObject):
    result = pyqtSignal(dict)  # Signal to emit the result of the ring detection
    error = pyqtSignal(str)  # Signal to emit any error messages


class CalibrationWorker(QRunnable):
    def __init__(
        self,
        image_data,
        start_circle,
        calibration_mode,
        detector_mask=None,
        band_width=50,
    ):
        super().__init__()
        self.image_data = image_data
        self.calibration_mode = calibration_mode
        self.start_circle = start_circle
        self.detector_mask = detector_mask
        self.band_width = band_width
        self.signals = WorkerSignals()

    def run(self):
        """Perform ring detection in a background thread."""
        try:
            finder = RingFinder(debug=False)
            result = finder.find_rings(
                self.image_data,
                start_circle=self.start_circle,
                mode=self.calibration_mode,
                detector_mask=self.detector_mask,
                band_width=self.band_width,
            )
            self.signals.result.emit(result if result else {})
        except Exception as e:
            error_msg = f"Error in background ring detection: {str(e)}"
            self.signals.error.emit(error_msg)


def grid_search_radial_sum(
    image,
    start_center,
    start_radius,
    detector_mask=None,
    center_range=3,
    radius_range=3,
    step=1,
):
    """
    Perform a grid search around the given center and radius to maximize the radial sum.
    Masked values (if detector_mask is provided) are ignored.
    Args:
        image: 2D numpy array
        start_center: (x, y) tuple
        start_radius: float
        detector_mask: 2D boolean array (True=masked)
        center_range: int, +/- pixels to search around center
        radius_range: int, +/- pixels to search around radius
        step: int, step size for grid search
    Returns:
        best_center_x, best_center_y, best_radius, max_sum
    """
    logger.info(
        f"grid_search_radial_sum: start_center: {start_center} start_radius: {start_radius} center_range: {center_range} radius_range: {radius_range} step: {step}"
    )
    x0, y0 = start_center
    best_sum = -np.inf
    best_params = (x0, y0, start_radius)
    h, w = image.shape
    # Precompute coordinate grid
    yy, xx = np.indices(image.shape)
    for dx in range(-center_range, center_range + 1, step):
        for dy in range(-center_range, center_range + 1, step):
            for dr in range(-radius_range, radius_range + 1, step):
                cx = x0 + dx
                cy = y0 + dy
                r = start_radius + dr
                if r <= 0:
                    continue
                # Compute mask for pixels at radius r from (cx, cy)
                rr = np.sqrt((xx - cx) ** 2 + (yy - cy) ** 2)
                ring_mask = np.abs(rr - r) < 1.0  # 1-pixel wide ring
                if detector_mask is not None:
                    ring_mask &= ~detector_mask
                if not np.any(ring_mask):
                    continue
                ring_sum = image[ring_mask].sum()
                if ring_sum > best_sum:
                    best_sum = ring_sum
                    best_params = (cx, cy, r)
    return (*best_params, best_sum)


def annulus_search_radial_sum(
    image,
    start_center,
    radius,
    band_width=2,
    detector_mask=None,
    center_range=3,
    step=1,
):
    """
    Efficiently search for the center (cx, cy) that maximizes the sum in an annulus (r0-band_width < r < r0+band_width).
    Only center is optimized; radius is fixed. Masked values are ignored.
    Args:
        image: 2D numpy array
        start_center: (x, y) tuple
        radius: float, fixed radius
        band_width: float, half-width of the annulus
        detector_mask: 2D boolean array (True=masked)
        center_range: int, +/- pixels to search around center
        step: int, step size for grid search
    Returns:
        best_center_x, best_center_y, max_sum
    """
    logger.debug(
        f"annulus_search_radial_sum: start_center: {start_center} radius: {radius} band_width: {band_width} center_range: {center_range} step: {step}"
    )
    x0, y0 = start_center
    best_sum = -np.inf
    best_center = (x0, y0)
    h, w = image.shape
    yy, xx = np.indices(image.shape)
    # Precompute annulus mask for each candidate center
    for dx in range(-center_range, center_range + 1, step):
        for dy in range(-center_range, center_range + 1, step):
            cx = x0 + dx
            cy = y0 + dy
            rr = np.sqrt((xx - cx) ** 2 + (yy - cy) ** 2)
            annulus_mask = np.abs(rr - radius) < band_width
            if detector_mask is not None:
                annulus_mask &= ~detector_mask
            if not np.any(annulus_mask):
                continue
            annulus_sum = image[annulus_mask].sum()
            if annulus_sum > best_sum:
                best_sum = annulus_sum
                best_center = (cx, cy)
    logger.debug(
        f"annulus_search_radial_sum: best_center: {best_center} best_sum: {best_sum}"
    )
    return (*best_center, best_sum)


if __name__ == "__main__":
    import argparse
    import sys
    from qp2.xio.hdf5_manager import HDF5Reader

    parser = argparse.ArgumentParser(
        description="Find rings in an image using RingFinder."
    )
    parser.add_argument(
        "--image",
        type=str,
        required=True,
        help="Path to the image file (e.g., .tif, .png, .jpg)",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug mode for RingFinder"
    )
    args = parser.parse_args()

    reader = HDF5Reader(args.image)
    n_frames = reader.get_n_frames()

    try:
        image = reader.get_frame(n_frames - 1)
    except Exception as e:
        image = reader.get_frame(0)
        print(f"Error: Could not load image: {args.image}")

    # Instantiate and run RingFinder
    ring_finder = RingFinder(debug=args.debug)
    result = ring_finder.find_rings(image)

    if result is None:
        print("No rings found or ring finding failed.")
        sys.exit(2)

    print("Ring finding result:")
    for k, v in result.items():
        print(f"  {k}: {v}")
