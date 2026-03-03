# qp2/image_viewer/eiger_mask/bad_pixel_manager.py

import numpy as np
from pyqtgraph.Qt import QtCore, QtWidgets

from qp2.image_viewer.eiger_mask.bad_pixel_worker import BadPixelWorker
from qp2.image_viewer.eiger_mask.bad_pixel_dialog import BadPixelDialog
from qp2.log.logging_config import get_logger
from .detector_mask_dialog import DetectorMaskDialog

logger = get_logger(__name__)


class BadPixelManager(QtCore.QObject):
    """Orchestrates bad pixel detection, analysis, and UI interaction."""

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.ui_manager = main_window.ui_manager
        self.graphics_manager = main_window.graphics_manager
        self.dialog = None
        self.active_worker = None

    def run_detection(self):
        """Starts the bad pixel detection process."""
        if not self.main_window.reader or self.main_window.reader.total_frames < 10:
            self.ui_manager.show_warning_message(
                "Not Enough Data",
                "Bad pixel detection requires a dataset with at least 10 frames.",
            )
            return

        num_frames_to_sample, ok = QtWidgets.QInputDialog.getInt(
            self.main_window,
            "Bad Pixel Detection",
            "Number of random frames to analyze:",
            value=10,
            min=5,
            max=min(100, self.main_window.reader.total_frames),
            step=1,
        )
        if not ok:
            return

        self.ui_manager.show_status_message(
            f"Starting bad pixel analysis on {num_frames_to_sample} frames...", 0
        )
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)

        # Select random frames
        total_frames = self.main_window.reader.total_frames
        rng = np.random.default_rng()
        indices = rng.choice(total_frames, size=num_frames_to_sample, replace=False)

        # Get current detector mask
        current_mask = (
            self.main_window.detector_mask
            if self.main_window.detector_mask is not None
            else np.zeros(self.main_window.get_analysis_image().shape, dtype=bool)
        )
        max_results = self.main_window.settings_manager.get(
            "bad_pixel_max_results", 100
        )

        worker = BadPixelWorker(
            reader=self.main_window.reader,
            frame_indices=indices,
            detector_mask=current_mask,
            params=self.main_window.params,
            max_results=max_results,
        )
        self.active_worker = worker
        worker.signals.finished.connect(self._on_detection_finished)
        worker.signals.error.connect(self._on_detection_error)
        worker.signals.progress.connect(
            lambda msg: self.ui_manager.show_status_message(msg, 0)
        )

        self.main_window.threadpool.start(worker)

    def _on_detection_finished(self, results):
        """Handles the results from the worker."""
        QtWidgets.QApplication.restoreOverrideCursor()
        coords = results["bad_pixel_coords"]
        reasons = results["bad_pixel_reasons"]
        warning = results.get("warning", "")

        if warning:
            self.ui_manager.show_status_message(warning, 10000)
        else:
            self.ui_manager.show_status_message(
                f"Found {len(coords)} potential bad pixels.", 5000
            )

        self.ui_manager.show_status_message(
            f"Found {len(coords)} potential bad pixels.", 5000
        )

        # 1. Show overlay on the image
        self.graphics_manager.show_bad_pixels_overlay(coords)

        # 2. Open the results dialog
        if self.dialog:
            self.dialog.close()

        self.dialog = BadPixelDialog(coords, reasons, self.main_window)

        if warning:
            self.dialog.setWindowTitle(f"Bad Pixel Candidates (Top {len(coords)})")

        self.dialog.zoom_requested.connect(self.main_window.zoom_to_pixel)
        self.dialog.update_hardware_mask_requested.connect(
            self.launch_hardware_update_dialog
        )
        self.dialog.apply_to_mask_requested.connect(
            self.main_window.update_detector_mask_with_new_pixels
        )
        self.dialog.analyze_pixel_requested.connect(self.analyze_single_pixel)
        self.dialog.finished.connect(self.cleanup)
        self.dialog.show()

    def _on_detection_error(self, error_msg):
        QtWidgets.QApplication.restoreOverrideCursor()
        self.ui_manager.show_critical_message("Bad Pixel Detection Failed", error_msg)
        self.cleanup()

    def analyze_single_pixel(self):
        """Prompt for a pixel coordinate and show its temporal stats."""
        if self.active_worker is None or self.active_worker.stack is None:
            self.ui_manager.show_warning_message(
                "No Data", "Analysis data not available. Please run detection first."
            )
            return

        text, ok = QtWidgets.QInputDialog.getText(
            self.dialog, "Analyze Single Pixel", "Enter coordinates (col-x, row-y):"
        )
        if not ok or not text:
            return

        try:
            c_str, r_str = text.replace("(", "").replace(")", "").split(",")
            c, r = int(c_str.strip()), int(r_str.strip())
            pixel_series = self.active_worker.stack[:, r, c]

            median_val = np.median(pixel_series)
            mad_val = np.median(np.abs(pixel_series - median_val))
            std_val = np.std(pixel_series)
            min_val, max_val = np.min(pixel_series), np.max(pixel_series)

            status = "Normal"
            if std_val < 1e-6:
                status = "Stuck/Low Variance"
            # More sophisticated logic could be added here

            info = (
                f"<b>Statistics for Pixel ({r}, {c}):</b><br>"
                f"Median Value: {median_val:.4f}<br>"
                f"Median Abs Dev (MAD): {mad_val:.4f}<br>"
                f"Standard Deviation: {std_val:.4f}<br>"
                f"Min/Max: {min_val:.4f} / {max_val:.4f}<br><br>"
                f"<b>Suggested Status: {status}</b>"
            )
            QtWidgets.QMessageBox.information(self.dialog, "Pixel Analysis", info)

        except Exception as e:
            self.ui_manager.show_warning_message(
                "Invalid Input", f"Could not parse coordinates: {e}"
            )

    def launch_hardware_update_dialog(self, pixels_to_add):
        """Creates and shows the dialog for updating the hardware mask."""
        try:
            hw_dialog = DetectorMaskDialog(pixels_to_add, self.main_window)
            hw_dialog.exec_()  # Use exec_ for a modal dialog
        except Exception as e:
            self.ui_manager.show_critical_message(
                "Error Launching Dialog", f"Could not open hardware mask dialog:\n{e}"
            )

    def cleanup(self):
        """Clean up visuals and references when the dialog is closed."""
        self.graphics_manager.clear_bad_pixels_overlay()
        self.dialog = None
        self.active_worker = None
