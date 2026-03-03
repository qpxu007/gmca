# Create new file: qp2/image_viewer/volume_map/volume_data_worker.py

import json
import re

import numpy as np
from PyQt5.QtCore import QRunnable, QObject, pyqtSignal

from qp2.image_viewer.volume_map.volume_utils import (
    reconstruct_volume_with_shift,
)
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class VolumeDataSignals(QObject):
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)
    progress = pyqtSignal(str)


class VolumeDataWorker(QRunnable):
    """Fetches data for two runs, reconstructs a 3D volume, and finds peaks."""

    def __init__(self, run_xy, run_xz, shift, redis_conn, plugin_config, scan_mode: str):
        super().__init__()
        self.signals = VolumeDataSignals()
        self.run_xy = run_xy
        self.run_xz = run_xz
        self.shift = shift
        self.redis_conn = redis_conn
        self.plugin_config = plugin_config
        self.scan_mode = scan_mode

    def _fetch_and_build_matrix(self, run_datasets) -> (np.ndarray, str, dict):
        """Helper to build a 2D data matrix for a single run."""
        # --- START: MODIFIED HELPER ---
        is_serpentine = "serpentine" in self.scan_mode
        is_column_scan = "column" in self.scan_mode
        scan_idx_pattern = r"_C(\d+)" if is_column_scan else r"_R(\d+)"

        max_scan_idx, max_frames = 0, 0
        scan_map = {}
        for r in run_datasets:
            match = re.search(scan_idx_pattern, r.master_file_path, re.IGNORECASE)
            if match:
                idx = int(match.group(1)) - 1
                max_scan_idx = max(max_scan_idx, idx)
                scan_map[idx] = r
            max_frames = max(max_frames, r.total_frames)

        if is_column_scan:
            grid_shape = (max_frames, max_scan_idx + 1)
        else:  # Row scan
            grid_shape = (max_scan_idx + 1, max_frames)

        metric = self.plugin_config["default_y_axis"]
        matrix = np.full(grid_shape, np.nan)
        raw_data_map = {}
        key_template = self.plugin_config["redis_key_template"]
        x_axis_key = self.plugin_config["x_axis_key"]

        for reader in run_datasets:
            match = re.search(scan_idx_pattern, reader.master_file_path, re.IGNORECASE)
            if not match:
                continue
            scan_idx = int(match.group(1)) - 1

            redis_key = key_template.format(master_file=reader.master_file_path)
            redis_results = self.redis_conn.hgetall(redis_key)

            for frame_json in redis_results.values():
                frame = json.loads(frame_json)
                original_frame_idx = frame.get(x_axis_key)

                if original_frame_idx is not None and metric in frame:
                    original_frame_idx -= 1

                    final_frame_idx = original_frame_idx
                    if is_serpentine and scan_idx % 2 == 1:
                        num_frames_in_scan = reader.total_frames
                        final_frame_idx = (num_frames_in_scan - 1) - original_frame_idx

                    if is_column_scan:
                        matrix_row, matrix_col = final_frame_idx, scan_idx
                    else:  # Row scan
                        matrix_row, matrix_col = scan_idx, final_frame_idx

                    if 0 <= matrix_row < grid_shape[0] and 0 <= matrix_col < grid_shape[1]:
                        # NOTE: Key for raw_data_map is ALWAYS (scan_idx, original_frame_idx)
                        # This is because the VolumeManager needs to find the original data
                        # for a given scan line (Y or Z) and its original frame index.
                        raw_data_map[(scan_idx, original_frame_idx)] = frame
                        if metric in frame:
                            matrix[matrix_row, matrix_col] = frame[metric]

        # For volume reconstruction, the matrix must be (scan_indices, frames).
        # If it's a column scan, our matrix is (frames, columns), so we transpose it.
        if is_column_scan:
            matrix = matrix.T

        return matrix, metric, raw_data_map
        # --- END: MODIFIED HELPER ---

    def run(self):
        try:
            # 1. Fetch data and build the 2D matrices
            self.signals.progress.emit("Fetching XY scan data...")
            data_xy, metric, raw_data_xy = self._fetch_and_build_matrix(self.run_xy)

            self.signals.progress.emit("Fetching XZ scan data...")

            data_xz, _, raw_data_xz = self._fetch_and_build_matrix(self.run_xz)

            # 2. Reconstruct the 3D volume using the new function
            self.signals.progress.emit(f"Reconstructing 3D volume with shift={self.shift}...")
            volume = reconstruct_volume_with_shift(data_xy, data_xz, self.shift)

            self.signals.finished.emit(
                {
                    "volume": volume,
                    "metric": metric,
                    "raw_data_xy": raw_data_xy,
                    "raw_data_xz": raw_data_xz,
                }
            )

        except Exception as e:
            logger.error(f"Volume data worker failed: {e}", exc_info=True)
            self.signals.error.emit(str(e))
