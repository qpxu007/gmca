# qp2/image_viewer/actions/image_filter_manager.py
import numpy as np
from PyQt5.QtCore import Qt, QObject, pyqtSlot, pyqtSignal
from PyQt5.QtWidgets import QApplication

# Import the dialog locally to avoid circular dependencies at the module level
from qp2.image_viewer.ui.filter_settings import FilterSettingsDialog
from qp2.image_viewer.workers.image_filter import ImageFilterWorker
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class ImageFilterManager(QObject):
    # Signals to communicate results back to the main window
    filter_applied = pyqtSignal(np.ndarray, str)
    filter_error = pyqtSignal(str)
    filter_stopped = pyqtSignal()

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self._dialog = None
        self.is_active = False

    @property
    def ui_manager(self):
        return self.main_window.ui_manager

    @property
    def settings_manager(self):
        return self.main_window.settings_manager

    def open_settings_dialog(self):
        """Opens the filter settings dialog. This is the main entry point."""
        if self._dialog is None:
            self._dialog = FilterSettingsDialog(
                self.settings_manager.get("image_filter_type"),
                self.settings_manager.get("se_size"),
                self.main_window,
            )
            self._dialog.filter_params_changed.connect(self._on_filter_params_changed)
            self._dialog.finished.connect(self._on_dialog_closed)

        self._dialog.show()
        self.is_active = True
        self.apply_filter(self.main_window._original_image)
        self._dialog.raise_()
        self._dialog.activateWindow()

    def apply_filter(self, image_data: np.ndarray):
        """Applies the current filter settings to the given image data."""
        if not self.is_active or image_data is None:
            return

        mw = self.main_window
        self.ui_manager.show_status_message(
            f"Applying {self.settings_manager.get('image_filter_type')} filter...", 0
        )
        # Display the unfiltered image while the worker runs
        mw.graphics_manager.display_image(image_data)

        QApplication.setOverrideCursor(Qt.WaitCursor)

        worker = ImageFilterWorker(
            image_data,
            self.settings_manager.get("image_filter_type"),
            self.settings_manager.get("se_size"),
            mw.detector_mask,
            mw.params,
        )
        # Connect worker signals to the manager's slots
        worker.signals.finished.connect(self._on_worker_finished)
        worker.signals.error.connect(self._on_worker_error)

        # Ensure the worker is cleaned up from the main window's active set
        mw.active_workers.add(worker)
        worker.signals.finished.connect(
            lambda result, w=worker: mw._on_worker_finished(w)
        )
        worker.signals.error.connect(
            lambda err_msg, w=worker: mw._on_worker_finished(w)
        )

        mw.threadpool.start(worker)

    def _on_dialog_closed(self):
        self.stop()

    def stop(self):
        """Stops the filter mode and closes the dialog if it's open."""
        if self.is_active:
            self.is_active = False
            self.filter_stopped.emit()
            self.ui_manager.show_status_message("Image filter disabled.", 2000)
        if self._dialog:
            self._dialog.close()
            self._dialog = None

    @pyqtSlot(str, int)
    def _on_filter_params_changed(self, filter_type: str, se_size: int):
        """Called when settings are changed in the dialog."""
        self.settings_manager.set("image_filter_type", filter_type)
        self.settings_manager.set("se_size", se_size)
        self.is_active = True  # Activating the filter

        # Trigger an immediate update of the current frame
        self.main_window.update_frame_display(self.main_window.current_frame_index)

    @pyqtSlot(int)
    def _on_dialog_finished(self, result: int):
        """Called when the dialog is closed."""
        # If the user closes the dialog without ever applying a filter, stop the mode.
        if not self.is_active:
            self.stop()
        self._dialog = None

    @pyqtSlot(object)
    def _on_worker_finished(self, result: tuple):
        """Handles the successful result from the ImageFilterWorker."""
        self.ui_manager.clear_status_message_if("Applying")

        filtered_image, extra_info = result
        filter_type = self.settings_manager.get('image_filter_type')

        # Emit the processed image back to the main window for display
        self.filter_applied.emit(filtered_image, filter_type)

    @pyqtSlot(str)
    def _on_worker_error(self, error_msg: str):
        """Handles an error from the ImageFilterWorker."""
        self.ui_manager.clear_status_message_if("Applying")
        self.is_active = False  # Disable on error
        self.filter_error.emit(error_msg)
