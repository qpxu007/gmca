# qp2/image_viewer/actions/live_peak_finding_manager.py
import numpy as np
from PyQt5.QtCore import QObject, pyqtSlot, QTimer

from qp2.image_viewer.actions.playback_manager import PlaybackState
from qp2.image_viewer.plugins.spot_finder.find_spots_worker import PeakFinderWorker
from qp2.image_viewer.plugins.spot_finder.spot_finder_settings import (
    SpotFinderSettingsDialog,
)
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class LivePeakFindingManager(QObject):
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self._settings_dialog = None
        # Defer access to other managers until they are needed

    @property
    def ui_manager(self):
        return self.main_window.ui_manager

    @property
    def graphics_manager(self):
        return self.main_window.graphics_manager

    @property
    def settings_manager(self):
        return self.main_window.settings_manager

    def _get_or_create_settings_dialog(self):
        """Gets or creates the singleton settings dialog."""
        if self._settings_dialog is None:
            self._settings_dialog = SpotFinderSettingsDialog(
                self.main_window.settings_manager, self.main_window
            )
            self._settings_dialog.peak_params_changed.connect(
                self._on_peak_params_changed
            )
        return self._settings_dialog

    @pyqtSlot(dict)
    def _on_peak_params_changed(self, new_peak_settings: dict):
        """Slot to handle live updates from the settings dialog."""
        # 1. Update the central settings manager
        self.main_window.settings_manager.update_from_dict(new_peak_settings)
        # 2. Trigger a new peak finding run to reflect changes
        if self.main_window.auto_peak_finding_enabled:
            self.run()

    @pyqtSlot(bool)
    def set_auto_mode(self, checked):
        self.main_window.auto_peak_finding_enabled = checked
        self.ui_manager.show_status_message(
            f"Auto Peak Finding: {'Enabled' if checked else 'Disabled'}", 2000
        )

        if checked:
            # If auto-mode is enabled, run it immediately on the current frame.
            if self.main_window._original_image is not None:
                QTimer.singleShot(10, self.run)
        else:
            self.graphics_manager.clear_peaks()
            if self.main_window.waiting_for_peaks:
                self.main_window.waiting_for_peaks = False
                if self.main_window.playback_manager.state == PlaybackState.WAITING:
                    self.main_window.playback_manager.play()

    def open_settings_dialog(self):
        """Public method to be called from other managers."""
        dialog = self._get_or_create_settings_dialog()
        dialog.show()
        dialog.raise_()

    @pyqtSlot()
    def run(self):
        logger.debug("LivePeakFindingManager.run() called, clearing plugin visuals.")
        self.graphics_manager.clear_spots()
        self.graphics_manager.clear_indexed_reflections()
        self.graphics_manager.clear_plugin_info_text()
        self.graphics_manager.clear_peaks()

        if self.main_window._original_image is None:
            self.ui_manager.show_status_message(
                "No image loaded for peak finding", 3000
            )

            if self.main_window.waiting_for_peaks:
                self.main_window.waiting_for_peaks = False
                if self.main_window.playback_manager.state == PlaybackState.WAITING:
                    self.main_window.playback_manager.play()
            return

        peak_finder_kwargs = self.main_window.get_peak_finder_kwargs()
        if peak_finder_kwargs is None:
            # Error message is shown by get_peak_finder_kwargs
            if self.main_window.waiting_for_peaks:
                self.main_window.waiting_for_peaks = False
                if self.main_window.playback_manager.state == PlaybackState.WAITING:
                    self.main_window.playback_manager.play()
            return

        self.ui_manager.show_status_message(
            f"Running peak finding on frame {self.main_window.current_frame_index + 1}...",
            0,
        )

        if self.main_window.playback_manager.state == PlaybackState.PLAYING:
            self.main_window.playback_manager.pause(is_user_request=False)
            self.main_window.waiting_for_peaks = True

        worker = PeakFinderWorker(
            image=self.main_window._original_image,
            detector_mask=self.main_window.detector_mask,
            beam_x=self.main_window.params.get("beam_x"),
            beam_y=self.main_window.params.get("beam_y"),
            frame_index=self.main_window.current_frame_index,
            **peak_finder_kwargs,
        )

        worker.signals.finished.connect(self._on_finished)
        worker.signals.error.connect(self._on_error)

        # Maintain reference to prevent garbage collection
        self.main_window.active_workers.add(worker)
        worker.signals.finished.connect(
            lambda idx, peaks, w=worker: self.main_window._on_worker_finished(w)
        )
        worker.signals.error.connect(
            lambda err_msg, w=worker: self.main_window._on_worker_finished(w)
        )

        self.main_window.threadpool.start(worker)

    @pyqtSlot(int, object)
    def _on_finished(self, frame_index: int, peaks: np.ndarray):
        if frame_index != self.main_window.current_frame_index:
            logger.warning(
                f"Peak finder result for frame {frame_index} is stale (current is {self.main_window.current_frame_index}). Ignoring."
            )
            return

        self.graphics_manager.update_peaks(peaks)
        peak_count = len(peaks) if peaks is not None else 0
        self.ui_manager.show_status_message(
            f"Found {peak_count} peaks on frame {frame_index + 1}", 3000
        )

        if self.main_window.waiting_for_peaks:
            self.main_window.waiting_for_peaks = False
            if self.main_window.playback_manager.state == PlaybackState.WAITING:
                self.main_window.playback_manager.play()
        self.ui_manager.clear_status_message_if("Running peak finding")

    @pyqtSlot(str)
    def _on_error(self, error_msg: str):
        self.ui_manager.show_status_message(f"Peak finding failed: {error_msg}", 5000)
        self.graphics_manager.clear_peaks()
        if self.main_window.waiting_for_peaks:
            self.main_window.waiting_for_peaks = False
            if self.main_window.playback_manager.state == PlaybackState.WAITING:
                self.main_window.playback_manager.play()
        self.ui_manager.clear_status_message_if("Running peak finding")
