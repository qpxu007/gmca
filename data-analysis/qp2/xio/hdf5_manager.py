import math
import os
import time
from collections import OrderedDict
from pathlib import Path
from typing import Optional, Dict, Any

# Third-party imports
try:
    # Ensure hdf5plugin is installed: pip install hdf5plugin
    import hdf5plugin
    import h5py
except ImportError:
    print("hdf5plugin not found, compressed HDF5 files might not be supported.")

from pyqtgraph.Qt import QtCore

from qp2.xio.user_group_manager import get_esaf_from_data_path
from qp2.xio.proc_utils import extract_master_prefix
from qp2.log.logging_config import get_logger
from qp2.config.servers import ServerConfig

logger = get_logger(__name__)


class FileMonitorWorker(QtCore.QObject):
    """
    Background worker to monitor HDF5 file growth and existence.
    Runs in a separate thread to prevent blocking the UI during I/O.
    """
    frames_updated = QtCore.pyqtSignal(int, int)
    data_files_ready_batch = QtCore.pyqtSignal(list)
    series_completed = QtCore.pyqtSignal(str, int, dict)
    monitor_state_updated = QtCore.pyqtSignal(int)
    finished = QtCore.pyqtSignal()

    def __init__(self, master_file_path, frame_map, total_frames, params):
        super().__init__()
        self.master_file_path = master_file_path
        self.frame_map = frame_map
        self.total_frames = total_frames
        self.params = params
        self.prefix = extract_master_prefix(master_file_path)
        
        self.next_expected_map_index = 0
        self.processed_data_files = set()
        self._last_reported_latest_index = -2
        self._last_logged_wait_file = None
        self.series_completion_signal_emitted = False
        self.running = False
        self.timer = None

    @QtCore.pyqtSlot()
    def start_monitoring(self):
        """Starts the monitoring timer in the worker thread."""
        self.running = True
        self.timer = QtCore.QTimer()
        self.timer.setInterval(ServerConfig.HDF5_POLL_INTERVAL_MS)
        self.timer.timeout.connect(self._check_files)
        self.timer.start()
        logger.info(
            f"FileMonitorWorker started for {Path(self.master_file_path).name}: "
            f"{self.total_frames} expected frames, "
            f"poll interval={ServerConfig.HDF5_POLL_INTERVAL_MS}ms."
        )
        # Run an immediate check
        QtCore.QTimer.singleShot(0, self._check_files)

    @QtCore.pyqtSlot()
    def stop_monitoring(self):
        """Stops the monitoring timer."""
        self.running = False
        if self.timer:
            self.timer.stop()
        self.finished.emit()

    def _check_files(self):
        """
        Checks file existence and growth using isolated handles.
        """
        if not self.running or not self.frame_map:
            return

        current_check_latest_index = -1
        found_new_file = False
        newly_ready_files_info_batch = []
        is_last_file_in_series_found = False
        state_changed = False

        start_check_index = min(self.next_expected_map_index, len(self.frame_map))
        
        i = start_check_index
        while i < len(self.frame_map):
            start_idx, end_idx, expected_fpath, primary_dset_path = self.frame_map[i]
            
            try:
                if Path(expected_fpath).exists():
                    actual_frames_ready = end_idx # Default assumption
                    
                    # For the current file, check its actual size to support live playback
                    # Use a private handle to be thread-safe and robust
                    try:
                        with h5py.File(expected_fpath, 'r') as f:
                            if primary_dset_path in f:
                                # Standard h5py sees the size at open time, which is perfect here
                                dset = f[primary_dset_path]
                                frames_in_file = dset.shape[0]
                                actual_frames_ready = start_idx + frames_in_file
                    except Exception:
                        pass # Keep default assumption on error

                    current_check_latest_index = max(current_check_latest_index, actual_frames_ready - 1)
                    
                    # Logic for moving to next file
                    if actual_frames_ready >= end_idx:
                        self.next_expected_map_index = i + 1
                        state_changed = True
                        found_new_file = True 
                    else:
                        found_new_file = True
                        # File growing, stay here
                        break

                    if expected_fpath not in self.processed_data_files:
                        file_info = {
                            "file_path": expected_fpath,
                            "start_frame": start_idx,
                            "end_frame": end_idx - 1,
                            "metadata": self.params,
                        }
                        newly_ready_files_info_batch.append(file_info)
                        self.processed_data_files.add(expected_fpath)

                    if i == len(self.frame_map) - 1:
                        is_last_file_in_series_found = True
                    
                    i += 1
                else:
                    break
            except OSError as e:
                # logger.warning(...) # Avoid spamming logs in tight loop
                break

        if state_changed:
            self.monitor_state_updated.emit(self.next_expected_map_index)

        if newly_ready_files_info_batch:
            self.data_files_ready_batch.emit(newly_ready_files_info_batch)

        if newly_ready_files_info_batch:
            for fi in newly_ready_files_info_batch:
                logger.info(
                    f"Data file ready: {Path(fi['file_path']).name} "
                    f"(frames {fi['start_frame'] + 1}–{fi['end_frame'] + 1}/{self.total_frames})"
                )

        if is_last_file_in_series_found and not self.series_completion_signal_emitted:
            logger.info(
                f"FileMonitor: ALL files on disk for {Path(self.master_file_path).name}. "
                f"Reporting {self.total_frames} frames complete. Stopping monitor."
            )
            self.series_completed.emit(
                self.master_file_path, self.total_frames, self.params
            )
            self.series_completion_signal_emitted = True
            self.stop_monitoring()

        if not found_new_file and start_check_index > 0:
            current_check_latest_index = self._last_reported_latest_index
        elif start_check_index == 0 and not found_new_file:
            current_check_latest_index = -1

        if current_check_latest_index != self._last_reported_latest_index:
            logger.info(
                f"FileMonitor: frames available updated: "
                f"{current_check_latest_index + 1}/{self.total_frames} "
                f"(+{current_check_latest_index - self._last_reported_latest_index} new)"
            )
            self.frames_updated.emit(current_check_latest_index, self.total_frames)
            self._last_reported_latest_index = current_check_latest_index


class HDF5Reader(QtCore.QObject):
    """
    Reads HDF5 files, optimizing initialization for scenarios with many data files
    by building a speculative frame map based on master file metadata.
    Uses a background worker for non-blocking file monitoring.
    """

    frames_updated = QtCore.pyqtSignal(int, int)
    data_files_ready_batch = QtCore.pyqtSignal(list)
    series_completed = QtCore.pyqtSignal(str, int, dict)

    def __init__(
        self,
        master_file: str,
        initial_metadata: Optional[Dict[str, Any]] = None,
        start_timer: bool = True,
    ):
        super().__init__()
        self.master_file_path = str(Path(master_file).resolve())
        self.master = None
        self.prefix = ""
        self.total_frames = 0
        self.bit_depth = 32
        self.params = {}
        
        # Track availability state for UI initialization
        self.last_known_available_index = -1

        # Internal state for synchronous polling compatibility
        self.next_expected_map_index = 0
        self.series_completion_signal_emitted = False

        self.data_file_paths = []
        self.frame_map = []
        
        self.open_file_handles = OrderedDict()
        self.MAX_OPEN_FILES = 12
        self.dset_paths = ["/entry/data/data", "/entry/data/raw_data"]

        # Defaults
        self.nimages = 0
        self.wavelength = 1.0
        self.det_dist = 100.0
        self.pixel_size = 0.075
        self.beam_x = 512.0
        self.beam_y = 512.0
        self.saturation_value = None
        self.underload_value = -1
        self.nx = None
        self.ny = None
        self.omega_start = 0
        self.omega_range = 0.2
        self.exposure = 0.2

        # Threading for Monitor
        self.monitor_thread = None
        self.monitor_worker = None

        # --- Optimized Initialization ---
        try:
            max_wait_sec = 10
            # Use configured poll interval (converted to seconds) for initial check too
            poll_interval_sec = max(0.5, ServerConfig.HDF5_POLL_INTERVAL_MS / 1000.0)
            wait_start_time = time.time()
            file_found = False

            master_path_obj = Path(self.master_file_path)

            while time.time() - wait_start_time < max_wait_sec:
                if master_path_obj.exists():
                    file_found = True
                    break
                logger.warning(
                    f"Master file not yet found: {self.master_file_path}. Retrying in {poll_interval_sec}s..."
                )
                time.sleep(poll_interval_sec)

            if not file_found:
                raise FileNotFoundError(
                    f"Master file not found after waiting {max_wait_sec}s: {self.master_file_path}"
                )

            logger.info(
                f"Initializing HDF5Reader for: {Path(self.master_file_path).name} (Optimized Mode)"
            )
            init_start_time = time.time()

            self.master = h5py.File(self.master_file_path, "r")
            
            # Robust prefix extraction
            self.prefix = extract_master_prefix(self.master_file_path)
            
            self._read_detector_params()

            if initial_metadata:
                self.params.update(initial_metadata)

            if "primary_group" not in self.params:
                ownership_info = get_esaf_from_data_path(self.master_file_path)
                if ownership_info:
                    self.params.update(ownership_info)

            if not self.params.get("images_per_hdf"):
                self._calculate_images_per_hdf_fallback()

            self.nimages = self.params.get("nimages", 0)
            self.images_per_hdf = self.params.get("images_per_hdf", 1)
            self.total_frames = self.nimages

            if self.nimages <= 0:
                raise ValueError("Failed to read a valid 'nimages' value (> 0).")
            if self.images_per_hdf <= 0:
                raise ValueError("Failed to determine a valid 'images_per_hdf' value.")

            self._initialize_frame_map_from_master()
            if not self.frame_map:
                raise RuntimeError("Failed to build speculative frame map.")

            self.total_frames = self.nimages
            
            # Initial state
            if start_timer:
                # Monitor will update this shortly
                self.frames_updated.emit(-1, self.total_frames)
            else:
                # Static mode: Assume all frames are available immediately
                self.last_known_available_index = self.total_frames - 1
                self.frames_updated.emit(self.last_known_available_index, self.total_frames)

            init_duration = time.time() - init_start_time
            logger.info(f"Optimized initialization complete in {init_duration:.3f} seconds.")

            if start_timer:
                self._start_monitor_thread()

        except Exception as e:
            logger.error(f"ERROR during HDF5Reader initialization: {e}", exc_info=True)
            self.total_frames = 0
            if self.master:
                try:
                    self.master.close()
                except:
                    pass
            self.master = None
            raise RuntimeError(f"HDF5Reader initialization failed: {e}") from e

    def _start_monitor_thread(self):
        """Initializes and starts the background monitor thread."""
        self.monitor_thread = QtCore.QThread()
        # Pass a copy of params/map to avoid thread safety issues
        self.monitor_worker = FileMonitorWorker(
            self.master_file_path,
            list(self.frame_map), # copy
            self.total_frames,
            self.params.copy()
        )
        self.monitor_worker.moveToThread(self.monitor_thread)

        # Connect signals
        self.monitor_thread.started.connect(self.monitor_worker.start_monitoring)
        self.monitor_worker.finished.connect(self.monitor_thread.quit)
        self.monitor_worker.finished.connect(self.monitor_worker.deleteLater)
        self.monitor_thread.finished.connect(self.monitor_thread.deleteLater)

        # Proxy signals from worker to update local state
        self.monitor_worker.frames_updated.connect(self._on_worker_frames_updated)
        self.monitor_worker.monitor_state_updated.connect(self._on_worker_state_updated)
        
        self.monitor_worker.data_files_ready_batch.connect(self.data_files_ready_batch)
        self.monitor_worker.series_completed.connect(self._on_series_completed)

        self.monitor_thread.start()

    @QtCore.pyqtSlot(int, int)
    def _on_worker_frames_updated(self, latest_index, total_frames):
        """Updates internal state and re-emits the signal."""
        # logger.debug(f"HDF5Reader frames updated: available={latest_index}, total={total_frames}")
        self.last_known_available_index = latest_index
        self.frames_updated.emit(latest_index, total_frames)

    @QtCore.pyqtSlot(int)
    def _on_worker_state_updated(self, next_index):
        """Updates internal state for compatibility with polling scripts."""
        self.next_expected_map_index = next_index

    @QtCore.pyqtSlot(str, int, dict)
    def _on_series_completed(self, master_file, total_frames, metadata):
        """Updates internal state and re-emits the signal."""
        self.series_completion_signal_emitted = True
        self.series_completed.emit(master_file, total_frames, metadata)

    def _read_detector_params():
        pass

    def _read_detector_params(self):
        """
        Reads parameters from the master file. Crucially determines 'nimages'
        and 'images_per_hdf'. Other parameters are read as available.
        """
        logger.info("Reading parameters from master file...")
        # Set defaults first
        self.nimages = 0
        self.wavelength = 1.0
        self.det_dist = 100.0
        self.pixel_size = 0.075
        self.beam_x = 512.0
        self.beam_y = 512.0
        self.saturation_value = None
        self.underload_value = -1
        self.detector = ""

        if not self.master or not self.master.id.valid:
            raise IOError("Master file handle is not valid for reading parameters.")

        try:
            # --- Helper to read scalar value safely ---
            def read_scalar(path, default=None, dtype=None):
                if path in self.master:
                    try:
                        dset = self.master[path]
                        # Read scalar value correctly using [()]
                        value = dset[()]
                        # Optional type conversion
                        if dtype is not None:
                            return dtype(value)
                        # Handle numpy types -> python types for convenience
                        return value.item() if hasattr(value, "item") else value
                    except Exception as e:
                        logger.warning(
                            f"Warning: Failed to read scalar parameter '{path}': {e}"
                        )
                return default

            # --- Read nimages (Total static number of frames) ---
            # Common Eiger paths, adjust if different detector/format
            nimg_path = "/entry/instrument/detector/detectorSpecific/nimages"
            ntrig_path = "/entry/instrument/detector/detectorSpecific/ntrigger"
            nimg_val = read_scalar(nimg_path, dtype=int)
            ntrig_val = read_scalar(ntrig_path, dtype=int)

            if nimg_val is not None and ntrig_val is not None:
                self.nimages = nimg_val * ntrig_val
                logger.info(
                    f" Read nimages={nimg_val}, ntrigger={ntrig_val} -> total_frames={self.nimages}"
                )
            else:
                logger.warning(
                    f"Warning: Could not read '{nimg_path}' or '{ntrig_path}'. Attempting other paths..."
                )

            # --- Read other parameters (using helper for cleaner code) ---
            wl_m = read_scalar(
                "/entry/instrument/beam/incident_wavelength",
                default=1.0e-10,
                dtype=float,
            )
            self.wavelength = wl_m

            dist_m = read_scalar(
                "/entry/instrument/detector/detector_distance", default=0.1, dtype=float
            )
            self.det_dist = dist_m * 1000

            px_size_m = read_scalar(
                "/entry/instrument/detector/x_pixel_size", default=7.5e-5, dtype=float
            )
            self.pixel_size = px_size_m * 1000

            self.beam_x = read_scalar(
                "/entry/instrument/detector/beam_center_x",
                default=self.beam_x,
                dtype=float,
            )
            self.beam_y = read_scalar(
                "/entry/instrument/detector/beam_center_y",
                default=self.beam_y,
                dtype=float,
            )

            self.saturation_value = read_scalar(
                "/entry/instrument/detector/detectorSpecific/countrate_correction_count_cutoff",
                default=60000,
            )

            self.underload_value = read_scalar(
                "/entry/instrument/detector/underload_value", default=-1
            )

            self.sensor_thickness = read_scalar(
                "/entry/instrument/detector/sensor_thickness", default=None, dtype=float
            )

            self.nx = read_scalar(
                "/entry/instrument/detector/detectorSpecific/x_pixels_in_detector",
                default=None,
            )
            self.ny = read_scalar(
                "/entry/instrument/detector/detectorSpecific/y_pixels_in_detector",
                default=None,
            )
            self.omega_start = self.master["/entry/sample/goniometer/omega"][()][0]
            self.omega_range = read_scalar(
                "/entry/sample/goniometer/omega_range_average", 0.2
            )
            self.exposure = read_scalar("/entry/instrument/detector/count_time", 0.2)
            self.detector = self.master["/entry/instrument/detector/description"][()]
            bit_depth_val = read_scalar(
                "/entry/instrument/detector/bit_depth_image", default=None, dtype=int
            )
            if bit_depth_val is not None and bit_depth_val > 0:
                self.bit_depth = bit_depth_val
                logger.info(f" Read detector bit_depth_image: {self.bit_depth}")
            else:
                logger.warning(
                    f"Warning: Could not read 'bit_depth_image'. Defaulting to {self.bit_depth}-bit."
                )
            logger.info(
                f" Params Read: nimages={self.nimages},  bit_depth={self.bit_depth}"
            )

            self.params.update(
                {
                    "wavelength": self.wavelength,
                    "det_dist": self.det_dist,
                    "pixel_size": self.pixel_size,
                    "beam_x": self.beam_x,
                    "beam_y": self.beam_y,
                    "saturation_value": self.saturation_value,
                    "underload_value": self.underload_value,
                    "sensor_thickness": self.sensor_thickness,
                    "nimages": self.nimages,
                    "nx": self.nx,
                    "ny": self.ny,
                    "omega_start": self.omega_start,
                    "omega_range": self.omega_range,
                    "exposure": self.exposure,
                    "master_file": self.master_file_path,
                    "detector": (
                        self.detector.decode("utf-8")
                        if isinstance(self.detector, bytes)
                        else self.detector
                    ),
                    "bit_depth": self.bit_depth,
                    "proc_dir_root": self.get_default_proc_dir(),
                }
            )

        except Exception as e:
            logger.error(
                f"ERROR: Unexpected error reading detector parameters: {e}",
                exc_info=True,
            )
            raise

    def _calculate_images_per_hdf_fallback(self, max_wait_sec=5):
        """
        Fallback to determine images_per_hdf by attempting to read the
        first data file directly. This is more accurate than heuristics.
        If this fails, it reverts to a time-based heuristic.
        """
        try:
            # 1. Construct the expected path to the first data file
            master_dir = Path(self.master_file_path).parent
            first_data_filename = f"{self.prefix}_data_000001.h5"
            first_data_filepath = master_dir / first_data_filename

            logger.info(f"Attempting direct read fallback from: {first_data_filename}")

            # 2. Poll for the file's existence with a short timeout
            #    This handles the latency between master file creation and data file creation.
            poll_interval_sec = 0.2
            wait_start_time = time.time()
            file_found = False
            while time.time() - wait_start_time < max_wait_sec:
                if first_data_filepath.exists():
                    file_found = True
                    break
                time.sleep(poll_interval_sec)

            if not file_found:
                logger.warning(
                    f"Timeout: First data file '{first_data_filename}' not found after {max_wait_sec}s. "
                    "Reverting to time-based heuristic."
                )
                self._calculate_images_per_hdf_heuristic()  # Fallback to the old method
                return

            # 3. Read the file and get the dimension of the primary dataset
            logger.info(
                f"File '{first_data_filename}' found. Reading dataset dimensions."
            )
            with h5py.File(first_data_filepath, "r") as f:
                primary_dset_path = self.dset_paths[
                    0
                ]  # Assume the first path is the most likely
                if primary_dset_path in f:
                    dset = f[primary_dset_path]
                    if dset.ndim > 0:
                        # The first dimension is the number of images/frames
                        images_in_file = dset.shape[0]
                        if images_in_file > 0:
                            self.params["images_per_hdf"] = images_in_file
                            logger.info(
                                f"SUCCESS: Determined images_per_hdf = {images_in_file} directly from data file."
                            )
                            return

            # If we reach here, reading the dimension failed.
            logger.warning(
                "Could not determine dimensions from the first data file. Reverting to heuristic."
            )
            self._calculate_images_per_hdf_heuristic()

        except Exception as e:
            logger.error(
                f"Error during direct read fallback for images_per_hdf: {e}. Reverting to heuristic.",
                exc_info=True,
            )
            self._calculate_images_per_hdf_heuristic()

    def _calculate_images_per_hdf_heuristic(self):
        """Original heuristic calculation based on frame_time. Used as a final fallback."""
        try:
            frame_time_path = "/entry/instrument/detector/frame_time"
            if frame_time_path in self.master:
                frame_time = float(self.master[frame_time_path][()])
                if frame_time > 1e-9:  # Avoid division by zero
                    # Eiger heuristic: chunk size is often ~0.5 seconds worth of frames + 1
                    calculated_ipf = int(math.ceil(0.5 / frame_time))
                    if calculated_ipf > 0:
                        self.params["images_per_hdf"] = calculated_ipf

                        logger.info(
                            f" Calculated images_per_hdf using time heuristic: {calculated_ipf}"
                        )
                        return
        except Exception as e:
            logger.warning(
                f"Warning: Failed heuristic calculation for images_per_hdf: {e}"
            )

        # If all else fails, keep the default value
        logger.warning(f"Using default images_per_hdf = 1")

    def _initialize_frame_map_from_master(self):
        """
        Builds a speculative frame map based on nimages and images_per_hdf
        read from the master file. Does NOT open data files.
        """
        logger.info("Building speculative frame map from master metadata...")
        self.frame_map = []
        self.data_file_paths = []

        if self.nimages <= 0 or self.images_per_hdf <= 0:
            logger.error(
                "ERROR: Cannot build frame map - nimages or images_per_hdf is invalid."
            )
            return

        num_full_files = self.nimages // self.images_per_hdf
        remainder_frames = self.nimages % self.images_per_hdf
        total_files = num_full_files + (1 if remainder_frames > 0 else 0)

        logger.info(
            f" Expecting {total_files} data file(s) based on {self.nimages} frames and {self.images_per_hdf} frames/file."
        )

        current_frame_start_idx = 0
        master_dir = Path(self.master_file_path).parent

        primary_dset_path = (
            self.dset_paths[0] if self.dset_paths else "/entry/data/data"
        )

        for file_idx in range(total_files):
            is_last_file = file_idx == total_files - 1
            frames_in_this_file = (
                remainder_frames
                if is_last_file and remainder_frames > 0
                else self.images_per_hdf
            )

            if frames_in_this_file <= 0:
                continue

            frame_end_idx = current_frame_start_idx + frames_in_this_file

            # IMPLEMENTATION: Using pathlib for path construction
            expected_filename = f"{self.prefix}_data_{file_idx + 1:06d}.h5"
            expected_file_path = str(master_dir / expected_filename)

            map_entry = (
                current_frame_start_idx,
                frame_end_idx,  # Exclusive end index
                expected_file_path,
                primary_dset_path,
            )
            self.frame_map.append(map_entry)
            self.data_file_paths.append(expected_file_path)
            current_frame_start_idx = frame_end_idx

        logger.info(
            f"Speculative frame map built for {len(self.frame_map)} expected data file segments."
        )
        if not self.frame_map or self.frame_map[-1][1] != self.nimages:
            logger.warning(
                f"WARNING: Frame map calculation mismatch. Last end index {self.frame_map[-1][1] if self.frame_map else 'N/A'} != nimages {self.nimages}. Check logic."
            )

    def get_frame(self, frame_number):
        """
        Gets a specific frame using the speculative map.
        """
        if not (0 <= frame_number < self.total_frames):
            logger.error(
                f"Error: Requested frame {frame_number} is out of range (0-{self.total_frames - 1})."
            )
            return None

        target_file_path = None
        primary_dset_path = None
        local_index = -1
        for start_idx, end_idx, fpath, dpath in self.frame_map:
            if start_idx <= frame_number < end_idx:
                target_file_path = fpath
                primary_dset_path = dpath
                local_index = frame_number - start_idx
                break

        if target_file_path is None:
            logger.error(
                f"ERROR: Frame {frame_number} not found in speculative frame map. Map might be corrupt."
            )
            return None

        if not Path(target_file_path).exists():
            return None

        file_handle = self._get_file_handle(target_file_path)
        if file_handle is None:
            self._close_specific_handle(target_file_path)
            return None

        try:
            if primary_dset_path not in file_handle:
                raise KeyError(f"Primary dataset '{primary_dset_path}' not found.")
            dset = file_handle[primary_dset_path]
            actual_frames_in_dset = dset.shape[0] if dset.ndim >= 1 else 0
            if not (0 <= local_index < actual_frames_in_dset):
                logger.error(
                    f"ERROR: Frame index mismatch for requested frame {frame_number}. "
                    f"Local index {local_index} is out of bounds for dataset '{primary_dset_path}' with shape {dset.shape} in {Path(target_file_path).name}."
                )
                self._close_specific_handle(target_file_path)
                return None
            return dset[local_index]
        except KeyError:
            for alt_dset_path in self.dset_paths:
                if alt_dset_path != primary_dset_path and alt_dset_path in file_handle:
                    try:
                        dset = file_handle[alt_dset_path]
                        if 0 <= local_index < dset.shape[0]:
                            self._update_map_dset_path(target_file_path, alt_dset_path)
                            return dset[local_index]
                    except Exception:
                        continue
            logger.error(
                f"ERROR: Frame {frame_number} not found in any alternative dataset path in {Path(target_file_path).name}."
            )
            self._close_specific_handle(target_file_path)
            return None
        except Exception as e:
            logger.error(
                f"ERROR: Failed to read frame {frame_number} from {Path(target_file_path).name}.",
                exc_info=True,
            )
            self._close_specific_handle(target_file_path)
            return None

    def _update_map_dset_path(self, file_path_to_update, new_dset_path):
        """Helper to update the dataset path in the frame map for a specific file path."""
        self.frame_map = [
            (s, e, fp, new_dset_path) if fp == file_path_to_update else (s, e, fp, dp)
            for s, e, fp, dp in self.frame_map
        ]

    def _get_file_handle(self, file_path):
        """
        Gets an open file handle for the given path, managing a cache.
        """
        if file_path in self.open_file_handles:
            self.open_file_handles.move_to_end(file_path)
            return self.open_file_handles[file_path]

        if len(self.open_file_handles) >= self.MAX_OPEN_FILES:
            oldest_path, handle_to_close = self.open_file_handles.popitem(last=False)
            try:
                if handle_to_close and handle_to_close.id.valid:
                    handle_to_close.close()
            except Exception as e:
                logger.warning(
                    f"Warning: Error closing evicted file handle {Path(oldest_path).name}: {e}"
                )

        try:
            handle = h5py.File(file_path, "r")
            self.open_file_handles[file_path] = handle
            return handle
        except Exception as e:
            logger.error(
                f"ERROR: Unexpected error opening data file {Path(file_path).name}: {e}"
            )
            return None

    def _close_specific_handle(self, file_path, from_cache=True):
        """
        Closes a specific file handle associated with file_path.
        """
        handle_to_close = (
            self.open_file_handles.pop(file_path, None)
            if from_cache
            else self.open_file_handles.get(file_path)
        )
        if (
            handle_to_close
            and getattr(handle_to_close, "id", None)
            and handle_to_close.id.valid
        ):
            try:
                handle_to_close.close()
            except Exception as e:
                logger.warning(
                    f"Warning: Error closing file handle {Path(file_path).name}: {e}"
                )

    def close(self):
        """Closes all cached open file handles and the master file."""
        logger.info("Closing HDF5 files (Optimized Reader)...")
        
        # Clean shutdown of the monitor thread
        if self.monitor_worker:
            try:
                # Invoking stop_monitoring on the worker thread is safer
                QtCore.QMetaObject.invokeMethod(
                    self.monitor_worker, "stop_monitoring", QtCore.Qt.QueuedConnection
                )
            except RuntimeError:
                # Worker might already be deleted
                pass
        
        if self.monitor_thread:
            try:
                self.monitor_thread.quit()
                self.monitor_thread.wait(1000) # Wait up to 1 second
                if self.monitor_thread.isRunning():
                    logger.warning("Monitor thread did not quit cleanly, forcing termination.")
                    self.monitor_thread.terminate()
            except RuntimeError:
                # Thread object might already be deleted
                pass

        paths_to_close = list(self.open_file_handles.keys())
        for file_path in paths_to_close:
            self._close_specific_handle(file_path)
        self.open_file_handles.clear()

        if self.master and getattr(self.master, "id", None) and self.master.id.valid:
            try:
                self.master.close()
            except Exception as e:
                logger.error(f" Error closing master file {self.master.filename}: {e}")
        self.master = None
        logger.info(f"Closed HDF5 file handles.")

    def get_parameters(self):
        """Returns a dictionary of the read detector parameters."""
        return self.params.copy()

    def get_default_proc_dir(self):
        """Set default proc_dir_root with PROC_DIR_ROOT override."""
        filepath = Path(self.master_file_path)
        full_data_dir = str(filepath.parent)

        # 1) Honor PROC_DIR_ROOT first (expand envs and ~)
        proc_from_env = os.getenv("PROC_DIR_ROOT")
        if proc_from_env:
            # Expand ~ and $VARS (or %VARS% on Windows)
            expanded = os.path.expanduser(os.path.expandvars(proc_from_env))
            # Check writability of the directory (or its parent if it doesn't exist yet)
            check_path = (
                expanded
                if os.path.isdir(expanded)
                else (os.path.dirname(expanded) or ".")
            )
            if os.access(check_path, os.W_OK):
                return expanded

        # 2) Default mapping: /DATA -> /PROCESSING (and lowercase variant)
        proc_root_dir_str = full_data_dir.replace("/DATA/", "/PROCESSING/", 1)
        proc_root_dir_str = proc_root_dir_str.replace("/data/", "/processing/", 1)

        # 3) Beamline user fallback if mapped directory not writable
        parts = Path(full_data_dir).parts
        beamline_user = (
            parts[4]
            if len(parts) > 4
            else (os.environ.get("USER") or os.environ.get("USERNAME") or "unknown")
        )

        if not os.access(proc_root_dir_str, os.W_OK):
            fallback = Path(
                "/", "mnt", "beegfs", "PROCESSING", beamline_user, "processing"
            )
            return str(fallback)

        return proc_root_dir_str

    def __del__(self):
        """Destructor to log when the object is being garbage collected."""
        logger.warning(
            f"HDF5Reader for master file '{self.master_file_path}' is being destroyed (garbage collected)."
        )
        try:
            self.close()
        except Exception as e:
            # We can't do much if closing fails during GC, but we should log it
            # Note: Logger might already be gone during interpreter shutdown
            try:
                print(f"Error closing HDF5Reader during GC: {e}")
            except:
                pass
