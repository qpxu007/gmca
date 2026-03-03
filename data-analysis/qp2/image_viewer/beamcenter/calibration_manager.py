# qp2/image_viewer/beamcenter/calibration_manager.py
import numpy as np

from PyQt5.QtCore import QObject, pyqtSlot, Qt
from PyQt5.QtWidgets import (
    QApplication,
    QDialog,
)

from PyQt5.QtCore import QObject, pyqtSlot, Qt, pyqtSignal, QPointF
from PyQt5.QtWidgets import (
    QApplication,
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QPushButton,
    QDialogButtonBox,
    QAbstractItemView,
)

from qp2.image_viewer.utils.ring_math import angstrom_to_pixels
from qp2.image_viewer.utils.validation_utils import validate_detector_parameters
from qp2.image_viewer.beamcenter.find_rings import CalibrationWorker, RingFinder
from qp2.image_viewer.beamcenter.calibration_settings import (
    CalibrationSettingsDialog,
)
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class CalibrationManager(QObject):
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window

    @property
    def ui_manager(self):
        return self.main_window.ui_manager

    @property
    def graphics_manager(self):
        return self.main_window.graphics_manager

    @property
    def settings_manager(self):
        return self.main_window.settings_manager

    @pyqtSlot()
    def run(self):
        mw = self.main_window
        if mw._original_image is None:
            self.ui_manager.show_status_message(
                "No image loaded for calibration.", 3000
            )
            return

        dialog = CalibrationSettingsDialog(self.settings_manager, mw)
        if dialog.exec_() != QDialog.Accepted:
            self.ui_manager.show_status_message("Calibration cancelled.", 2000)
            return

        calibration_mode = self.settings_manager.get("calibration_mode", "Refine")

        # Handle manual calibration mode separately
        if calibration_mode == "ManualFit":
            self.start_manual_calibration()
            return

        self.graphics_manager.clear_calibration_visuals()

        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.main_window.ui_manager.show_status_message(
            "Running beam center calibration...", 0
        )

        self.graphics_manager.show_calibration_label()

        image_data = mw._original_image.copy()
        initial_center = (
            mw.params.get("beam_x", image_data.shape[1] // 2),
            mw.params.get("beam_y", image_data.shape[0] // 2),
        )
        initial_radius = None
        if validate_detector_parameters(
            mw.params, ["wavelength", "det_dist", "pixel_size"]
        ):
            try:
                initial_radius = angstrom_to_pixels(
                    self.settings_manager.get("calibration_ring_resolution"),
                    mw.params["wavelength"],
                    mw.params["det_dist"],
                    mw.params["pixel_size"],
                )
            except Exception:
                pass

        start_circle = (initial_center[0], initial_center[1], initial_radius)
        band_width = self.settings_manager.get("calibration_band_width", 15)

        self.ui_manager.show_status_message("Running beam center calibration...", 0)
        worker = CalibrationWorker(
            image_data,
            start_circle,
            calibration_mode,
            detector_mask=mw.detector_mask,
            band_width=band_width,
        )
        worker.signals.result.connect(self._on_finished)
        worker.signals.error.connect(self._on_error)

        mw.active_workers.add(worker)
        worker.signals.result.connect(
            lambda result, w=worker: mw._on_worker_finished(w)
        )
        worker.signals.error.connect(
            lambda err_msg, w=worker: mw._on_worker_finished(w)
        )

        mw.threadpool.start(worker)

    @pyqtSlot(dict)
    def _on_finished(self, result: dict):
        QApplication.restoreOverrideCursor()
        self.ui_manager.clear_status_message_if("Running beam center calibration")
        self.graphics_manager.hide_calibration_label()
        if not result:
            self.ui_manager.show_status_message(
                "Calibration failed: No results returned.", 4000
            )
            return

        self.ui_manager.show_status_message("Calibration successful.", 3000)
        self.graphics_manager.display_calibration_results(result)
        if result.get("refined_circle"):
            self.ui_manager.show_calibration_results_dialog(
                result,
                self.main_window.params,
                self.settings_manager.get("calibration_ring_resolution"),
            )

    @pyqtSlot(str)
    def _on_error(self, error_msg: str):
        QApplication.restoreOverrideCursor()
        self.ui_manager.clear_status_message_if("Running beam center calibration")
        self.graphics_manager.hide_calibration_label()
        self.ui_manager.show_warning_message("Calibration Error", error_msg)
        self.graphics_manager.clear_calibration_visuals()

    def start_manual_calibration(self):
        """Creates and shows the dialog for manual point selection."""
        mw = self.main_window
        # Prevent opening multiple dialogs
        if mw.is_manual_calibration_mode and mw.manual_calibration_dialog:
            mw.manual_calibration_dialog.raise_()
            mw.manual_calibration_dialog.activateWindow()
            return

        # Create the dialog and connect its signals
        dialog = ManualCalibrationDialog(mw)
        dialog.points_selected.connect(self.run_refinement_from_manual_points)
        # Ensure cleanup when the dialog is closed for any reason
        dialog.finished.connect(lambda: mw.set_manual_calibration_mode(False))

        # Set the main window into point selection mode
        mw.set_manual_calibration_mode(True, dialog)
        dialog.show()

    @pyqtSlot(list)
    def run_refinement_from_manual_points(self, points: list):
        """Takes points from the manual dialog and runs the calibration worker."""
        mw = self.main_window
        if len(points) < 5:
            mw.ui_manager.show_warning_message(
                "Manual Fit Error", "Please select at least 5 points to fit a circle."
            )
            return

        # Fit an initial circle to the user-provided points
        finder = RingFinder()
        # Use dummy weights (1.0) for the initial fit
        data_points = np.array([(p.x(), p.y(), 1.0) for p in points])
        ellipse_params = finder.fit_ellipse(data_points)

        if not ellipse_params or np.mean(ellipse_params["radius"]) == 0:
            mw.ui_manager.show_warning_message(
                "Manual Fit Error", "Could not fit a circle to the selected points."
            )
            return

        cx, cy = ellipse_params["center"]
        radius = np.mean(ellipse_params["radius"])
        start_circle = (cx, cy, radius)

        # Now, launch the worker as if it were in 'Refine' mode with the new start_circle
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.ui_manager.show_status_message("Refining manually fitted circle...", 0)
        self.graphics_manager.show_calibration_label()

        image_data = mw._original_image.copy()
        band_width = self.settings_manager.get("calibration_band_width", 15)

        # Create and run the worker, forcing 'Refine' mode
        worker = CalibrationWorker(
            image_data, start_circle, "Refine", mw.detector_mask, band_width
        )
        worker.signals.result.connect(self._on_finished)
        worker.signals.error.connect(self._on_error)
        mw.active_workers.add(worker)
        worker.signals.result.connect(
            lambda result, w=worker: mw._on_worker_finished(w)
        )
        worker.signals.error.connect(
            lambda err_msg, w=worker: mw._on_worker_finished(w)
        )
        mw.threadpool.start(worker)


class ManualCalibrationDialog(QDialog):
    """A dialog for manually selecting points on an image for circle fitting."""

    points_selected = pyqtSignal(list)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manual Circle Fit")
        self.setModal(False)  # Allow interaction with the main window
        self.points = []

        # --- UI Setup ---
        layout = QVBoxLayout(self)
        layout.addWidget(
            QLabel(
                "Click on the image to select points on a ring.\n"
                "Select at least 5 points for a stable fit."
            )
        )

        self.list_widget = QListWidget()
        self.list_widget.setSelectionMode(QAbstractItemView.SingleSelection)
        layout.addWidget(self.list_widget)

        button_layout = QHBoxLayout()
        self.remove_button = QPushButton("Remove Selected")
        self.clear_button = QPushButton("Clear All")
        button_layout.addWidget(self.remove_button)
        button_layout.addWidget(self.clear_button)
        layout.addLayout(button_layout)

        self.button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        self.button_box.button(QDialogButtonBox.Ok).setText("Fit Circle")
        layout.addWidget(self.button_box)

        # --- Connections ---
        self.remove_button.clicked.connect(self.remove_selected_point)
        self.clear_button.clicked.connect(self.clear_all_points)
        self.button_box.accepted.connect(self.on_accept)
        self.button_box.rejected.connect(self.reject)

    def add_point(self, point: QPointF):
        """Adds a point to the internal list and the UI list widget."""
        self.points.append(point)
        self.list_widget.addItem(f"({point.x():.1f}, {point.y():.1f})")
        self.list_widget.setCurrentRow(self.list_widget.count() - 1)

    def remove_selected_point(self):
        """Removes the currently selected point from the list."""
        current_row = self.list_widget.currentRow()
        if current_row >= 0:
            self.list_widget.takeItem(current_row)
            self.points.pop(current_row)

    def clear_all_points(self):
        """Removes all selected points."""
        self.points.clear()
        self.list_widget.clear()

    def on_accept(self):
        """Emits the list of points and accepts the dialog."""
        self.points_selected.emit(self.points)
        self.accept()

    def closeEvent(self, event):
        """Ensures cleanup happens when the dialog is closed."""
        super().closeEvent(event)
