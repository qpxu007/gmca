import logging
import numpy as np
from PyQt5 import QtCore

from qp2.image_viewer.config import (
    MASKED_CIRCLES,
    MASKED_RECTANGLES,
)
from qp2.image_viewer.utils.mask_computation import compute_detector_mask

logger = logging.getLogger(__name__)

class DetectorMaskManager(QtCore.QObject):
    """
    Manages detector mask computation and overlay toggling.
    """

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.ui_manager = main_window.ui_manager
        
        # State
        self._mask = None
        self._display_mask = None # For overlay
        self.mask_overlay_visible = False
        self._last_mask_params = None

    @property
    def graphics_manager(self):
        return self.main_window.graphics_manager
        
    @property
    def mask(self):
        """Public accessor for the current mask."""
        return self._mask

    @property
    def display_mask(self):
        """Public accessor for the display mask."""
        return self._display_mask

    def update_detector_mask(self):
        """Re-computes the mask and updates the display if needed."""
        self._mask = self._compute_detector_mask()
        self.ui_manager.show_status_message(
            "Detector mask updated for current dataset.", 2000
        )
        if self.mask_overlay_visible and self._mask is not None:
            self.graphics_manager.show_mask_overlay(self._mask)

    def _compute_detector_mask(self):
        """
        Compute a boolean mask for the detector, combining mask values, MASKED_CIRCLES, and MASKED_RECTANGLES.
        Supports expressions like 'beam_x-100'.
        Logs details for debugging.
        """
        # We need original image and params from main window
        if self.main_window._original_image is None:
            return None

        analysis_mask, display_mask = compute_detector_mask(
            image=self.main_window._original_image,
            params=self.main_window.params,
            mask_values=self.main_window.mask_values,
            masked_circles=MASKED_CIRCLES,
            masked_rectangles=MASKED_RECTANGLES,
        )
        self._display_mask = display_mask
        self._last_mask_params = self._get_mask_params()
        return analysis_mask

    def toggle_mask_overlay(self, checked):
        self.mask_overlay_visible = checked
        if checked:
            if self._mask is not None:
                self.graphics_manager.show_mask_overlay(self._mask)
            elif self.main_window._original_image is not None:
                # Try to compute if not yet exists
                self.update_detector_mask()
        else:
            self.graphics_manager.hide_mask_overlay()

    def _get_mask_params(self):
        # Return a tuple of parameters that affect the mask
        params = self.main_window.params
        image = self.main_window._original_image
        return (
            params.get("beam_x"),
            params.get("beam_y"),
            params.get("wavelength"),
            params.get("det_dist"),
            params.get("pixel_size"),
            image.shape if image is not None else None,
        )

    def mask_params_changed(self):
        """Checks if parameters affecting the mask have changed."""
        return self._last_mask_params != self._get_mask_params()
    
    def ensure_mask_up_to_date(self):
        """Computes mask if it's missing or params changed."""
        if self._mask is None or self.mask_params_changed():
            self.update_detector_mask()
