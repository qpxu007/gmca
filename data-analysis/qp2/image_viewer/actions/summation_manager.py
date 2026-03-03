# qp2/image_viewer/actions/summation_manager.py
import numpy as np
from PyQt5.QtCore import QObject, pyqtSlot, pyqtSignal, Qt
from PyQt5.QtWidgets import QApplication

from qp2.image_viewer.ui.sum_settings_dialog import SumSettingsDialog
from qp2.image_viewer.workers.image_sum import FrameSumWorker
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class SummationManager(QObject):
    """Manages the frame summation process, including the settings dialog and worker."""

    # Emits the final summed image to be displayed
    summation_complete = pyqtSignal(np.ndarray, int, int)
    # Emitted when the summation worker fails
    summation_error = pyqtSignal(str)
    # Emitted when summation mode is stopped (dialog closed)
    summation_stopped = pyqtSignal()

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self._sum_dialog = None
        self.sum_frame_count = 1
        self.is_active = False

    @property
    def ui_manager(self):
        return self.main_window.ui_manager

    @property
    def graphics_manager(self):
        return self.main_window.graphics_manager

    @pyqtSlot()
    def open_settings_dialog(self):
        """Opens and manages the summation settings dialog."""
        if self._sum_dialog:
            self._sum_dialog.show()
            self._sum_dialog.raise_()
            self._sum_dialog.activateWindow()
            return

        self._sum_dialog = SumSettingsDialog(self.sum_frame_count, self.main_window)
        self._sum_dialog.sum_params_changed.connect(self._on_params_changed)
        self._sum_dialog.finished.connect(self._on_dialog_closed)
        self._sum_dialog.show()
        self.is_active = True
        self.trigger_summation()

    @pyqtSlot(int)
    def _on_params_changed(self, count):
        self.sum_frame_count = count
        if self.is_active:
            self.trigger_summation()

    def _on_dialog_closed(self):
        self.stop()

    def stop(self):
        """
        Stops summation mode and cleans up. This method is idempotent and
        safe to call multiple times.
        """
        if not self.is_active:
            return  # Already stopped, nothing to do.

        # 1. Set state to inactive FIRST to prevent any re-entrant calls.
        self.is_active = False
        logger.debug("Stopping SummationManager.")

        # 2. If the dialog window exists, close it.
        if self._sum_dialog is not None:
            # Block signals temporarily to prevent the finished signal from
            # calling _on_dialog_closed, which would call stop() again.
            self._sum_dialog.blockSignals(True)
            self._sum_dialog.close()
            self._sum_dialog = None  # Clear the reference.

        # 3. Perform the rest of the cleanup.
        self.graphics_manager.hide_sum_label()
        self.summation_stopped.emit()

    def trigger_summation(self):
        mw = self.main_window
        if not self.is_active or mw.waiting_for_sum_worker:
            return

        frame_index = mw.current_frame_index
        if not mw.reader or not (0 <= frame_index < mw.reader.total_frames):
            return

        num_to_sum = min(
            self.sum_frame_count, mw.latest_available_frame_index - frame_index + 1
        )
        if num_to_sum < 1:
            # Not enough frames to sum, show single frame instead
            self.summation_complete.emit(
                mw.reader.get_frame(frame_index), frame_index, frame_index
            )
            return

        self.ui_manager.show_status_message(f"Summing {num_to_sum} frames...", 0)
        self.graphics_manager.show_sum_label(frame_index, frame_index + num_to_sum - 1)

        # --- SET BUSY CURSOR ---
        QApplication.setOverrideCursor(Qt.WaitCursor)
        mw.waiting_for_sum_worker = True

        worker = FrameSumWorker(mw.reader, frame_index, num_to_sum, mw.detector_mask)
        worker.signals.finished.connect(
            lambda img: self._on_worker_finished(img, frame_index, num_to_sum)
        )
        worker.signals.error.connect(self._on_worker_error)

        mw.active_workers.add(worker)
        worker.signals.finished.connect(lambda img, w=worker: mw._on_worker_finished(w))
        worker.signals.error.connect(lambda err, w=worker: mw._on_worker_finished(w))

        mw.threadpool.start(worker)

    def _on_worker_finished(self, summed_image, start_frame, num_summed):
        mw = self.main_window
        mw.waiting_for_sum_worker = False
        if not self.is_active:
            # If dialog was closed while worker was running, do nothing
            return

        end_frame = start_frame + num_summed - 1
        self.summation_complete.emit(summed_image, start_frame, end_frame)

    def _on_worker_error(self, error_msg):
        mw = self.main_window
        mw.waiting_for_sum_worker = False
        self.ui_manager.show_warning_message("Frame Sum Error", error_msg)
        self.summation_error.emit(error_msg)
