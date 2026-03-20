# -*- coding: utf-8 -*-
import time
import traceback

import numpy as np
# Use pyqtgraph's Qt wrapper for compatibility
from PyQt5.QtCore import (
    QRunnable,
    QObject,
    pyqtSignal,
    pyqtSlot,
)  # Added QRunnable, QObject, pyqtSignal, pyqtSlot

from qp2.image_viewer.utils.image_filter_utils import (
    apply_maximum_filter,
    apply_median_filter,
    apply_spot_enhancement,
    apply_spot_detection,
    apply_spot_sharpening,
    apply_spot_contrast,
    apply_tophat_filter,
    apply_log_filter,
    apply_dog_filter,
    apply_matched_filter,
    apply_bandpass_filter,
    apply_clahe_enhancement,
    apply_radial_background_removal,
    apply_radial_spot_enhancement,
    apply_beam_center_correction,
    apply_radial_tophat,
    apply_local_background_subtraction,
    apply_poisson_threshold,
    apply_radial_poisson_threshold,
    apply_visual_spot_enhancement,
    apply_cutoff_filter,
)
# Import improved utilities
from qp2.log.logging_config import get_logger

# Assume these imports are correct and available in the environment
# Scikit-image and Scipy (used in worker)

logger = get_logger(__name__)


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++ Worker Class for Background Image Filtering ++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class ImageFilterSignals(QObject):
    """Defines signals available from the ImageFilterWorker thread."""

    finished = pyqtSignal(object)  # Emits (filtered image, extra info dict or None)
    error = pyqtSignal(str)  # Emits error message string


# --- Modify ImageFilterWorker ---
class ImageFilterWorker(QRunnable):
    """
    Worker thread for applying image filters (Maximum, Median, Smooth).
    Inherits from QRunnable to run on the thread pool.
    Handles mask values by setting them to 0 before filtering.
    """

    def __init__(
            self, image_data, filter_type, se_size, detector_mask=None, params=None
    ):
        super().__init__()
        self.image_data = image_data.copy()
        self.filter_type = filter_type
        self.se_size = se_size
        self.detector_mask = detector_mask  # Store the boolean mask
        self.signals = ImageFilterSignals()
        self.params = params if params is not None else {}

    @pyqtSlot()
    def run(self):
        start_time = time.time()
        beam_center = (self.params.get("beam_x"), self.params.get("beam_y"))
        _radial_filters = {
            "Radial Poisson Threshold", "Beam Center Correction",
            "Radial Background Removal", "Radial Spot Enhancement", "Radial Top-hat",
        }
        if beam_center[0] is None or beam_center[1] is None:
            if self.filter_type in _radial_filters:
                self.signals.error.emit(
                    f"Beam center is required for {self.filter_type} filter."
                )
                return
            height, width = self.image_data.shape
            beam_center = (height // 2, width // 2)

        from cv2 import blur, GaussianBlur
        try:
            image_for_filter = self.image_data
            supported_cv_types = (np.uint8, np.uint16, np.int16, np.float32, np.float64)
            if image_for_filter.dtype not in supported_cv_types:
                logger.warning(
                    f"Input image dtype {image_for_filter.dtype} "
                    f"is not typically supported by OpenCV filters. Converting to float32."
                )
                image_for_filter = image_for_filter.astype(np.float32)
            elif self.filter_type == "Median" and image_for_filter.dtype not in (
                    np.uint8,
                    np.uint16,
                    np.float32,
            ):
                logger.warning(
                    f"Median filter input type {image_for_filter.dtype} not ideal for cv2. Converting to float32."
                )
                image_for_filter = image_for_filter.astype(np.float32)

            # --- Use detector_mask to mask pixels ---
            if (
                    self.detector_mask is not None
                    and self.detector_mask.shape == image_for_filter.shape
            ):
                image_for_filter[self.detector_mask] = 0
                logger.debug(
                    f"Applied detector mask, setting {np.sum(self.detector_mask)} pixels to 0 before filtering."
                )

            logger.debug(
                f"Applying {self.filter_type} filter (size={self.se_size}) in background..."
            )

            # --- Filter Application Logic (Keep previous logic) ---
            if self.filter_type == "Maximum":
                filtered = apply_maximum_filter(
                    image_for_filter, self.se_size, self.detector_mask
                )
            elif self.filter_type == "Median":
                filtered = apply_median_filter(
                    image_for_filter, self.se_size, self.detector_mask
                )
            elif self.filter_type == "Smooth":
                ksize_tuple = (self.se_size, self.se_size)
                filtered = blur(image_for_filter, ksize_tuple)
            elif self.filter_type == "Gaussian Smooth":
                filtered = GaussianBlur(
                    image_for_filter, (self.se_size, self.se_size), 0
                )
            elif self.filter_type == "Dilation":
                from skimage.morphology import dilation, square

                filtered = dilation(image_for_filter, square(self.se_size))
            elif self.filter_type == "Erosion":
                from skimage.morphology import erosion, square

                filtered = erosion(image_for_filter, square(self.se_size))
            elif self.filter_type == "Opening":
                from skimage.morphology import opening, square

                filtered = opening(image_for_filter, square(self.se_size))
            elif self.filter_type == "Closing":
                from skimage.morphology import closing, square

                filtered = closing(image_for_filter, square(self.se_size))
            elif self.filter_type == "Spot Enhancement":
                filtered = apply_spot_enhancement(image_for_filter, self.se_size)
            elif self.filter_type == "Spot Detection":
                filtered = apply_spot_detection(image_for_filter, self.se_size)
            elif self.filter_type == "Spot Sharpening":
                filtered = apply_spot_sharpening(image_for_filter, self.se_size)
            elif self.filter_type == "Spot Contrast":
                filtered = apply_spot_contrast(image_for_filter, self.se_size)
            elif self.filter_type == "Top-hat Filter":
                filtered = apply_tophat_filter(image_for_filter, self.se_size)
            elif self.filter_type == "Laplacian of Gaussian":
                filtered = apply_log_filter(image_for_filter, self.se_size)
            elif self.filter_type == "Difference of Gaussians":
                filtered = apply_dog_filter(image_for_filter, self.se_size)
            elif self.filter_type == "Matched Filter":
                filtered = apply_matched_filter(image_for_filter, self.se_size)
            elif self.filter_type == "Bandpass Filter":
                filtered = apply_bandpass_filter(image_for_filter, self.se_size)
            elif self.filter_type == "CLAHE Enhancement":
                filtered = apply_clahe_enhancement(image_for_filter, self.se_size)
            elif self.filter_type == "Local Background Subtraction":
                filtered = apply_local_background_subtraction(
                    image_for_filter, self.se_size
                )
            elif self.filter_type == "Poisson Threshold":
                filtered, threshold = apply_poisson_threshold(
                    image_for_filter, self.se_size, self.detector_mask
                )
                extra_info = {"poisson_threshold": threshold}
            elif self.filter_type == "Radial Poisson Threshold":
                filtered, extra_info = apply_radial_poisson_threshold(
                    image_for_filter, self.se_size, self.detector_mask, beam_center
                )

            elif self.filter_type == "Visual Spot Enhancement":
                filtered = apply_visual_spot_enhancement(
                    image_for_filter, self.se_size, self.detector_mask
                )
            elif self.filter_type == "Beam Center Correction":
                filtered = apply_beam_center_correction(
                    image_for_filter, self.se_size, beamcenter=beam_center
                )
            elif self.filter_type == "Radial Background Removal":
                filtered = apply_radial_background_removal(
                    image_for_filter, self.se_size, beam_center=beam_center
                )
            elif self.filter_type == "Radial Spot Enhancement":
                filtered = apply_radial_spot_enhancement(
                    image_for_filter, self.se_size, beamcenter=beam_center
                )
            elif self.filter_type == "Radial Top-hat":
                filtered = apply_radial_tophat(
                    image_for_filter, self.se_size, beamcenter=beam_center
                )
            elif self.filter_type == "Experimental":
                filtered = self._apply_niblack_threshold(image_for_filter, self.se_size)
            elif self.filter_type == "Cut-off Filter":
                filtered = apply_cutoff_filter(image_for_filter, cutoff_value=self.se_size, detector_mask=self.detector_mask)
            else:
                raise NotImplementedError(
                    f"Filter type '{self.filter_type}' not implemented."
                )
            if self.filter_type not in ("Poisson Threshold", "Radial Poisson Threshold"):
                extra_info = None

            # --- Post-processing (Optional): Restore masked pixels in the *output* ---
            # If you want the masked areas to remain masked (e.g., as 0) in the final
            # filtered image, you can optionally re-apply the boolean_mask here.
            # if self.mask_values and 'boolean_mask' in locals():
            #     filtered[boolean_mask] = 0 # Or np.nan if output is float and downstream handles it
            #     print("Restored masked pixels to 0 in the filtered output.")
            # For now, we let the filter operate on the 0-masked input and return its result directly.

            duration = time.time() - start_time
            logger.debug(f"{self.filter_type} filter completed in {duration:.3f}s")
            # restore mask value
            if self.detector_mask is not None and self.detector_mask.shape == filtered.shape:
                filtered[self.detector_mask] = self.image_data[self.detector_mask]
            self.signals.finished.emit((filtered, extra_info))

        except Exception as e:
            logger.error(f"Error applying {self.filter_type} filter: {e}", exc_info=True)
            self.signals.error.emit(str(e))

    def _apply_niblack_threshold(self, image, se_size):
        """
        Apply Niblack thresholding to the image.
        se_size: window size for local mean/std (should be odd)
        """
        # Convert to float for processing
        img = image.astype(np.float32)
        window = se_size if se_size % 2 == 1 else se_size + 1
        # Niblack parameters
        k = -0.2
        # Compute local mean and std
        from scipy.ndimage import uniform_filter

        mean = uniform_filter(img, window)
        mean_sq = uniform_filter(img ** 2, window)
        std = np.sqrt(np.abs(mean_sq - mean ** 2))
        # Niblack threshold
        thresh = mean + k * std
        # Apply threshold
        binary = (img > thresh).astype(img.dtype) * img.max()
        return binary
