# qp2/image_viewer/actions/file_io_manager.py
import os
import time
from typing import Optional, List

from PyQt5.QtCore import QObject, pyqtSlot, pyqtSignal, QTimer, Qt
from pyqtgraph.Qt import QtWidgets

from qp2.image_viewer.actions.playback_manager import PlaybackState
from qp2.log.logging_config import get_logger
from qp2.xio.hdf5_manager import HDF5Reader

logger = get_logger(__name__)


class FileIOManager(QObject):
    """Manages all data source loading, including files and Redis streams."""

    # Emits the new reader, params, and path on successful load
    data_loaded = pyqtSignal(object, dict, str)
    # Emitted when loading fails, instructing the main window to reset
    load_failed = pyqtSignal()

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window

        # State owned by this manager
        self.reader: Optional[HDF5Reader] = main_window.reader
        self.current_master_file: Optional[str] = main_window.current_master_file
        self.latest_requested_file: Optional[str] = None

        # Connect to Redis signals if the manager exists
        if self.main_window.redis_manager:
            self.main_window.redis_manager.new_master_file_stream.connect(
                self._on_new_master_file_from_redis
            )

    def _check_can_manual_load(self) -> bool:
        """
        Checks if manual loading is allowed.
        Blocks if Live Mode is active AND playback is ongoing (Playing or Waiting).
        If playback is paused/stopped, manual loading is allowed and Live Mode remains active.
        """
        mw = self.main_window
        
        # If following Redis AND actively playing/waiting, block manual load to prevent interruptions.
        if mw.is_live_mode and mw.playback_manager.state in [PlaybackState.PLAYING, PlaybackState.WAITING]:
            QtWidgets.QMessageBox.warning(
                mw,
                "Live Stream Active",
                "Cannot load files manually while the live stream is playing or waiting for data.\n\n"
                "Please PAUSE or STOP playback first if you wish to examine another dataset.",
            )
            return False
            
        return True

    @pyqtSlot()
    def open_file_dialog(self):
        """Shows the file open dialog and loads the selected file(s)."""
        if not self._check_can_manual_load():
            return

        file_paths = self.main_window.ui_manager.get_file_dialog()
        if file_paths:
            # Load all selected files. The UI will show the last one.
            for path in file_paths:
                self.load_file(path)

    @pyqtSlot()
    def load_from_list_file(self):
        """Loads a list of datasets from a text file (one path per line)."""
        if not self._check_can_manual_load():
            return

        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self.main_window,
            "Select List File",
            os.path.expanduser("~"),
            "Text Files (*.txt);;All Files (*)",
        )

        if not file_path:
            return

        try:
            with open(file_path, "r") as f:
                lines = f.readlines()
            
            valid_paths = []
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("!"):
                    continue
                
                # Remove quotes if they exist (common in some exported lists)
                path = line.strip('"').strip("'")
                
                if path and os.path.isfile(path):
                     valid_paths.append(os.path.abspath(path))
            
            if not valid_paths:
                self.main_window.ui_manager.show_warning_message(
                    "No Valid Paths", "No valid file paths found in the selected list."
                )
                return

            self.main_window.ui_manager.show_status_message(f"Loading {len(valid_paths)} datasets from list...", 3000)
            
            # Load all valid files
            # Load all valid files using the parallel loader
            self.main_window.load_datasets_parallel(valid_paths)
            
            # Select the last file in the list to mimic previous behavior
            if valid_paths:
                last_path = valid_paths[-1]
                logger.info(f"Selecting last loaded file: {last_path}")
                # We need to manually trigger the selection and ensure checks
                self.load_file_if_different(last_path)
                
        except Exception as e:
            logger.error(f"Error loading list file: {e}", exc_info=True)
            self.main_window.ui_manager.show_critical_message(
                "Error", f"Failed to load list file:\n{e}"
            )

    @pyqtSlot()
    def load_latest_from_redis(self):
        """Queries Redis for the latest dataset and loads it."""
        if not self._check_can_manual_load():
            return

        redis_manager = self.main_window.redis_manager
        if not redis_manager:
            self.main_window.ui_manager.show_status_message(
                "Redis Manager not available.", 3000
            )
            return

        self.main_window.ui_manager.show_status_message(
            "Querying Redis for latest image...", 2000
        )
        QtWidgets.QApplication.processEvents()

        try:
            latest_path = redis_manager.get_latest_dataset_path()
            if latest_path:
                self.load_file_if_different(latest_path)
            else:
                self.main_window.ui_manager.show_status_message(
                    "No new dataset found in Redis.", 3000
                )
        except Exception as e:
            self.main_window.ui_manager.show_critical_message(
                "Redis Load Error", f"Failed to load latest image from Redis:\n{e}"
            )

    @pyqtSlot()
    def show_recent_datasets_dialog(self):
        """Queries Redis for recent datasets and shows a selection dialog."""
        if not self._check_can_manual_load():
            return

        redis_manager = self.main_window.redis_manager
        if not redis_manager:
            self.main_window.ui_manager.show_warning_message(
                "Redis Error", "Redis Manager not available."
            )
            return

        try:
            # Get a list of recent, valid, absolute file paths
            recent_paths = redis_manager.get_recent_dataset_paths(count=20)
            if not recent_paths:
                self.main_window.ui_manager.show_information_message(
                    "No Datasets Found", "No recent datasets were found in Redis."
                )
                return

            # Create a user-friendly list of choices (basenames)
            display_items = [os.path.basename(p) for p in recent_paths]

            # Show a selection dialog
            item, ok = QtWidgets.QInputDialog.getItem(
                self.main_window,
                "Select Recent Dataset",
                "Choose a dataset to load:",
                display_items,
                0,
                False,  # editable=False
            )

            if ok and item:
                # Find the full path corresponding to the selected basename
                selected_index = display_items.index(item)
                selected_path = recent_paths[selected_index]
                self.load_file_if_different(selected_path)

        except Exception as e:
            logger.error(f"Failed to show recent datasets: {e}", exc_info=True)
            self.main_window.ui_manager.show_critical_message(
                "Redis Error", f"Could not retrieve recent datasets:\n{e}"
            )

    @pyqtSlot(QtWidgets.QTreeWidgetItem)
    def on_dataset_selected_from_history(self, item: QtWidgets.QTreeWidgetItem):
        """Loads a dataset selected from the history tree."""
        # This is triggered on double-click
        if not self._check_can_manual_load():
            return

        file_path = item.data(0, Qt.ItemDataRole.UserRole)
        # Update user intention: user explicitly clicked this, so it becomes the target
        self.latest_requested_file = file_path
        if file_path:
            self.load_file_if_different(file_path)

    @pyqtSlot(str, dict)
    def _on_new_master_file_from_redis(self, file_path: str, metadata: dict):
        """Handles new master file notifications from the Redis stream."""
        t0 = time.monotonic()
        # How long since the detector fired this frame
        msg_ts = metadata.get("timestamp")
        end_to_end_so_far = (t0 - msg_ts) if msg_ts else None
        if end_to_end_so_far is not None:
            logger.info(
                f"Redis→IV signal received for {os.path.basename(file_path)}: "
                f"{end_to_end_so_far:.2f}s since detector timestamp."
            )
        else:
            logger.info(f"Redis→IV signal received for {os.path.basename(file_path)} (no timestamp in metadata).")

        self.main_window.ui_manager.show_status_message(
            f"Redis: New master file: {os.path.basename(file_path)}", 5000
        )

        # Track this as the latest target for display
        self.latest_requested_file = file_path

        # Start the robust loading process with retries
        self._attempt_load_with_fs_retry(file_path, metadata, attempt=1, t0=t0)

    def _attempt_load_with_fs_retry(self, file_path: str, metadata: dict, attempt: int, t0: float = None):
        """
        Checks if the file exists and has content before loading.
        Retries up to 5 times (500ms total) if the file system is lagging.

        Logic:
        1. Wait for file to exist (Retry if not).
        2. Create Reader and Register in History (ALWAYS, once file exists).
        3. Load for Display (ONLY if it matches latest_requested_file).
        """
        if t0 is None:
            t0 = time.monotonic()
        max_attempts = 5
        retry_delay_ms = 100

        try:
            # Check 1: Does path exist?
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"not on disk yet: {file_path}")

            # Check 2: Is file non-empty? (HDF5 files must have a header)
            fsize = os.path.getsize(file_path)
            if fsize == 0:
                raise OSError(f"file is 0 bytes (still being written?): {file_path}")

            # --- SUCCESS: File is ready ---
            elapsed = time.monotonic() - t0
            if attempt > 1:
                logger.info(
                    f"File ready after {attempt} attempts ({elapsed:.2f}s elapsed, {fsize} bytes): "
                    f"{os.path.basename(file_path)}"
                )
            else:
                logger.info(
                    f"File ready immediately ({elapsed:.2f}s elapsed, {fsize} bytes): "
                    f"{os.path.basename(file_path)}"
                )

            mw = self.main_window

            # Check if this reader is already managed
            new_reader = mw.dataset_manager.get_reader(os.path.abspath(file_path))
            if not new_reader:
                # Create new reader (this opens the file)
                new_reader = HDF5Reader(file_path, initial_metadata=metadata)
                params = new_reader.get_parameters()
                # Add to manager immediately so it appears in the tree
                mw.dataset_manager.add_dataset(new_reader, params)

                # TRIGGER PLUGIN EXECUTION: Even if display is skipped, notify the plugin
                # so it can start processing this dataset in the background.
                if mw.analysis_plugin_manager.active_plugin:
                    mw.analysis_plugin_manager.active_plugin.update_source(new_reader, file_path)

            # 2. CONDITIONAL Display
            # Only update the main view if this is still the most recently requested file.
            if self.latest_requested_file == file_path:
                self.reader = new_reader
                self.current_master_file = file_path

                params = self.reader.get_parameters()
                total_elapsed = time.monotonic() - t0
                logger.info(
                    f"Emitting data_loaded for {os.path.basename(file_path)} "
                    f"({total_elapsed:.2f}s total from Redis signal)."
                )
                self.data_loaded.emit(self.reader, params, file_path)
            else:
                logger.info(f"Skipping display of {os.path.basename(file_path)} (newer dataset arrived), but added to history.")

        except FileNotFoundError as e:
            if attempt < max_attempts:
                logger.warning(
                    f"FS Lag attempt {attempt}/{max_attempts} — {e}. Retrying in {retry_delay_ms}ms..."
                )
                QTimer.singleShot(
                    retry_delay_ms,
                    lambda: self._attempt_load_with_fs_retry(file_path, metadata, attempt + 1, t0)
                )
            else:
                elapsed = time.monotonic() - t0
                logger.error(
                    f"FS Timeout ({elapsed:.2f}s): master file never appeared on disk after "
                    f"{max_attempts} attempts: {file_path}"
                )
                if self.latest_requested_file == file_path:
                    self.load_file_if_different(file_path, metadata_from_stream=metadata)
        except OSError as e:
            if attempt < max_attempts:
                logger.warning(
                    f"FS Lag attempt {attempt}/{max_attempts} — {e}. Retrying in {retry_delay_ms}ms..."
                )
                QTimer.singleShot(
                    retry_delay_ms,
                    lambda: self._attempt_load_with_fs_retry(file_path, metadata, attempt + 1, t0)
                )
            else:
                elapsed = time.monotonic() - t0
                logger.error(
                    f"FS Timeout ({elapsed:.2f}s): file exists but stayed empty/unreadable after "
                    f"{max_attempts} attempts: {file_path}"
                )
                if self.latest_requested_file == file_path:
                    self.load_file_if_different(file_path, metadata_from_stream=metadata)
        except Exception as e:
            elapsed = time.monotonic() - t0
            logger.error(
                f"Unexpected error loading {file_path} at attempt {attempt} ({elapsed:.2f}s): {e}",
                exc_info=True,
            )
            if self.latest_requested_file == file_path:
                self.load_file_if_different(file_path, metadata_from_stream=metadata)

    def load_file_if_different(
        self, file_path: str, metadata_from_stream: Optional[dict] = None
    ):
        """Checks if the path is new before attempting to load."""
        if not file_path or not isinstance(file_path, str):
            return
        try:
            is_same_file = (
                self.current_master_file
                and os.path.exists(self.current_master_file)
                and os.path.exists(file_path)
                and os.path.samefile(self.current_master_file, file_path)
            )
            if is_same_file:
                return
        except FileNotFoundError:
            pass
        self.load_file(file_path, metadata_from_stream)

    def load_file(self, file_path: str, metadata_from_stream: Optional[dict] = None):
        """The core file loading logic."""
        mw = self.main_window
        if not file_path or not os.path.exists(file_path):
            mw.ui_manager.show_warning_message(
                "File Not Found", f"File not found: {file_path}"
            )
            return

        is_same_file = (
            self.current_master_file 
            and os.path.exists(self.current_master_file) 
            and os.path.samefile(self.current_master_file, file_path)
        )

        if not is_same_file:
            mw.ui_manager.show_status_message(f"Loading {os.path.basename(file_path)}...")
            QtWidgets.QApplication.processEvents()

            if mw.playback_manager.state == PlaybackState.PLAYING:
                mw.playback_manager.pause(is_user_request=True)
        else:
            mw.ui_manager.show_status_message(f"Reloading {os.path.basename(file_path)}...", 1000)

        # Do not close the previous reader here. Let the DatasetManager handle it
        # on application close. We might want to switch back.

        try:
            # Check if this reader is already managed
            new_reader = mw.dataset_manager.get_reader(os.path.abspath(file_path))
            if not new_reader:
                new_reader = HDF5Reader(
                    file_path, initial_metadata=metadata_from_stream
                )

            # The currently active reader and file are updated in the FileIOManager
            self.reader = new_reader
            self.current_master_file = file_path

            params = self.reader.get_parameters()

            # The data_loaded signal will trigger the main window to update its state
            # and add the dataset to the manager if it's new.
            self.data_loaded.emit(self.reader, params, file_path)

        except Exception as e:
            logger.error(f"Failed to load file '{file_path}': {e}", exc_info=True)
            mw.ui_manager.show_critical_message(
                "File Load Error", f"Failed to load file:\n{file_path}\n\nError: {e}"
            )
            self.load_failed.emit()

    def close(self):
        """
        Closes the currently active HDF5Reader.
        This is now less critical as the DatasetManager will manage all readers,
        but it can be used to release the handle on the currently viewed file.
        """
        if self.reader:
            # We don't close the reader here anymore, as it's managed by the DatasetManager.
            # Closing it would prevent switching back to it.
            # The DatasetManager's clear() method on app exit will handle closing.
            self.reader = None
            self.current_master_file = None

    def reload_files(self, file_paths: List[str]):
        """
        Removes and re-adds the specified files to the dataset manager to
        force a fresh read from disk.
        """
        if not file_paths:
            return

        dm = self.main_window.dataset_manager

        # Determine if the currently displayed file is among those being reloaded.
        is_current_file_affected = False
        if self.current_master_file:
            for path in file_paths:
                try:
                    if os.path.samefile(self.current_master_file, path):
                        is_current_file_affected = True
                        break
                except FileNotFoundError:
                    continue

        # Step 1: Remove all specified datasets from the manager.
        # This will close their file handles and remove them from the cache.
        for path in file_paths:
            dm.remove_single_dataset(path)

        # Step 2: Re-load each file.
        for path in file_paths:
            # The regular load_file will handle creating a new reader and
            # adding it back to the dataset manager.
            # It will also emit the `data_loaded` signal, which updates the UI.
            self.load_file(path)

        # Step 3: If the currently active file was not reloaded, we don't need to do anything.
        # If it was, the last call to load_file in the loop will have already made it active again.
        # We just need to ensure it's selected in the tree.
        if is_current_file_affected and self.current_master_file:
            self.main_window.ui_manager.select_dataset_in_tree(self.current_master_file)

        self.main_window.ui_manager.show_status_message(
            f"Rescanned {len(file_paths)} dataset(s).", 3000
        )

    def load_specific_reader(self, reader: HDF5Reader, params: dict):
        """
        Loads a pre-opened HDF5Reader instance. Used after a background
        worker has already prepared the reader.
        """
        try:
            self.data_loaded.emit(reader, params, reader.master_file_path)
            self.main_window.ui_manager.select_dataset_in_tree(reader.master_file_path)
        except Exception as e:
            logger.error(
                f"Failed to emit data_loaded for pre-opened reader: {e}", exc_info=True
            )
            self.load_failed.emit()
