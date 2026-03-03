import math
import time
from enum import Enum, auto

from PyQt5.QtCore import QObject, QTimer, pyqtSignal

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class PlaybackState(Enum):
    """Defines the possible states for the playback manager."""

    STOPPED = auto()
    PLAYING = auto()
    PAUSED = auto()
    WAITING = auto()


class PlaybackManager(QObject):
    """Manages playback state, timing, and frame navigation."""

    frame_changed = pyqtSignal(int)
    state_changed = pyqtSignal(PlaybackState)

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.state = PlaybackState.STOPPED
        self.play_timer = QTimer()
        self.play_timer.timeout.connect(self._play_next_frame)
        initial_interval = self.main_window.settings_manager.get("playback_interval_ms")
        self.play_timer.setInterval(initial_interval)
        self._last_lag_log_time = 0.0
        self._last_turbo_state = False

    def _set_state(self, new_state: PlaybackState):
        """Centralized method for changing state and notifying listeners."""
        if self.state != new_state:
            logger.debug(f"Playback State Transition: {self.state} -> {new_state}")
            self.state = new_state
            self.state_changed.emit(self.state)

    def toggle_playback(self):
        """User-facing method to start or pause playback."""
        if self.state in [PlaybackState.PLAYING, PlaybackState.WAITING]:
            self.pause(is_user_request=True)
        else:
            self.play()

    def play(self):
        """Starts playback if possible, otherwise enters a waiting state."""
        logger.debug("PlaybackManager.play() called.")
        if self.state == PlaybackState.PLAYING:
            return

        if not self.main_window.reader or self.main_window.reader.total_frames <= 0:
            self.main_window.ui_manager.show_status_message(
                "No data loaded to play.", 3000
            )
            self._set_state(PlaybackState.STOPPED)
            return

        total = self.main_window.reader.total_frames
        latest_avail = self.main_window.latest_available_frame_index
        next_frame_index = (
            self.main_window.current_frame_index
            + self.main_window.settings_manager.get("playback_skip")
        )

        # If we can jump forward *at all* without exceeding what's available,
        # or if we are just starting and have available frames, we should PLAY.
        if self.main_window.current_frame_index < latest_avail:
            self._set_state(PlaybackState.PLAYING)
            self.play_timer.start()
            self.main_window.waiting_for_peaks = False
        elif next_frame_index >= total:
            # Skip overshoots the dataset entirely — jump to the last available frame and stop.
            logger.debug(
                f"Play: skip overshoots total ({next_frame_index} >= {total}). Jumping to last frame {latest_avail}."
            )
            self._set_state(PlaybackState.STOPPED)
            if self.main_window.current_frame_index != latest_avail:
                self.frame_changed.emit(latest_avail)
        else:
            logger.debug(
                f"Play requested but data not ready. Next: {next_frame_index}, Available: {latest_avail}. Entering WAITING."
            )
            self._set_state(PlaybackState.WAITING)

    def pause(self, is_user_request: bool = True):
        """Pauses the timer and sets the state to PAUSED or WAITING."""
        logger.debug(f"PlaybackManager.pause() called. User request: {is_user_request}")
        self.play_timer.stop()
        if is_user_request:
            self._set_state(PlaybackState.PAUSED)
        else:
            self._set_state(PlaybackState.WAITING)
        self.main_window.waiting_for_peaks = False

    def stop(self):
        """Stops playback completely."""
        logger.debug("PlaybackManager.stop() called.")
        self.play_timer.stop()
        self._set_state(PlaybackState.STOPPED)

    def _play_next_frame(self):
        """Advances to the next frame or changes state if at the end of data."""
        if self.state != PlaybackState.PLAYING:
            self.play_timer.stop()
            return

        # --- ADAPTIVE PLAYBACK LOGIC ---
        mw = self.main_window
        settings = mw.settings_manager
        
        current_skip = settings.get("playback_skip")
        current_interval = settings.get("playback_interval_ms")
        
        latest_avail = mw.latest_available_frame_index
        total = mw.reader.total_frames
        
        if settings.get("adaptive_live_playback") and mw.is_live_mode:
            lag = latest_avail - mw.current_frame_index

            # Use cached parameters from main window to minimize overhead
            exposure_ms = mw.params.get("exposure", 0.1) * 1000

            if lag <= 5:
                # Close to real-time: Smooth and accurate
                current_skip = 1
                current_interval = max(20, int(exposure_ms))
                in_turbo = False
            elif lag <= 50:
                # Moderate lag: Speed up refresh, skip slightly
                current_skip = 2
                current_interval = 30
                in_turbo = False
            else:
                # Heavy lag: Turbo mode
                current_skip = max(5, int(math.ceil(lag / 10)))
                current_interval = 20  # Max UI speed
                in_turbo = True

            # Log turbo-mode transitions and periodic state
            now = time.monotonic()
            if in_turbo != self._last_turbo_state:
                if in_turbo:
                    logger.warning(
                        f"Playback entering TURBO mode: lag={lag} frames "
                        f"(at {mw.current_frame_index+1}/{total}, available={latest_avail+1}). "
                        f"skip={current_skip}, interval={current_interval}ms."
                    )
                else:
                    logger.info(
                        f"Playback leaving turbo mode: lag={lag} frames, back to normal speed."
                    )
                self._last_turbo_state = in_turbo

            if now - self._last_lag_log_time > 5.0:
                logger.info(
                    f"Playback lag: {lag} frames | current={mw.current_frame_index+1} "
                    f"available={latest_avail+1}/{total} | "
                    f"skip={current_skip} interval={current_interval}ms"
                    + (" [TURBO]" if in_turbo else "")
                )
                self._last_lag_log_time = now

            # Dynamically update timer if it significantly differs from current
            if abs(self.play_timer.interval() - current_interval) > 5:
                self.play_timer.setInterval(current_interval)

        next_index = mw.current_frame_index + current_skip
        
        if next_index <= latest_avail:
            mw.current_frame_index = next_index
            self.frame_changed.emit(next_index)
        elif next_index < total:
            logger.debug(
                f"Auto-Wait: Next {next_index} > Available {latest_avail}, but < Total {total}."
            )
            self.play_timer.stop()
            self._set_state(PlaybackState.WAITING)
        else:
            logger.debug(
                f"Auto-Stop: Next {next_index} >= Total {total}. Playback finished."
            )
            self.play_timer.stop()
            self._set_state(PlaybackState.STOPPED)
            if mw.current_frame_index != latest_avail:
                self.frame_changed.emit(latest_avail)

    def _pause_if_playing(self):
        """Helper to pause playback before a manual navigation action."""
        if self.state == PlaybackState.PLAYING:
            self.pause(is_user_request=True)

    def prev_frame(self):
        self._pause_if_playing()
        target_index = max(
            0,
            self.main_window.current_frame_index
            - self.main_window.settings_manager.get("playback_skip"),
        )
        self.main_window.current_frame_index = target_index
        self.frame_changed.emit(target_index)

    def next_frame(self):
        self._pause_if_playing()
        target_index = min(
            self.main_window.latest_available_frame_index,
            self.main_window.current_frame_index
            + self.main_window.settings_manager.get("playback_skip"),
        )
        if target_index > self.main_window.current_frame_index:
            self.main_window.current_frame_index = target_index
            self.frame_changed.emit(target_index)
        else:
            self._set_state(PlaybackState.WAITING)

    def slider_changed(self, value):
        self._pause_if_playing()
        self.main_window.current_frame_index = value
        self.frame_changed.emit(value)

    def go_to_frame(self, value):
        self._pause_if_playing()
        self.main_window.current_frame_index = value
        self.frame_changed.emit(value)
