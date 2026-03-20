# qp2/image_viewer/heatmap/heatmap_data_worker.py

import json
import re

import numpy as np
from PyQt5.QtCore import QRunnable, QObject, pyqtSignal

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class HeatmapDataSignals(QObject):
    # Emit a single dictionary containing all necessary results
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)


class HeatmapDataWorker(QRunnable):
    """Fetches and assembles all data for a run into 2D matrices."""

    def __init__(self, datasets, redis_conn, plugin_config, scan_mode: str):
        super().__init__()
        self.signals = HeatmapDataSignals()
        self.datasets = datasets
        self.redis_conn = redis_conn
        self.plugin_config = plugin_config
        self.scan_mode = scan_mode

    def run(self):
        try:
            if not self.datasets or not self.redis_conn:
                raise ValueError("Datasets or Redis connection not provided.")

            # --- START: MODIFIED GEOMETRY LOGIC ---
            is_serpentine = "serpentine" in self.scan_mode
            is_column_scan = "column" in self.scan_mode

            # Auto-detect scan index pattern from filenames, independent of
            # scan_mode.  scan_mode controls grid orientation only; the
            # filename convention (_R vs _C) just identifies scan-line numbers.
            scan_idx_pattern = None
            for candidate in [r"_R(\d+)", r"_C(\d+)"]:
                if any(re.search(candidate, r.master_file_path, re.IGNORECASE)
                       for r in self.datasets):
                    scan_idx_pattern = candidate
                    break

            if not scan_idx_pattern:
                raise ValueError(
                    "Could not detect scan index pattern (_R or _C) in any dataset filename."
                )

            max_scan_idx = 0
            max_frames = 0
            scan_map = {}  # Maps 0-indexed scan number to its HDF5Reader
            raw_data_map = {}  # { (matrix_row, matrix_col): full_frame_dict }

            for reader in self.datasets:
                filename = reader.master_file_path
                match = re.search(scan_idx_pattern, filename, re.IGNORECASE)
                if match:
                    idx = int(match.group(1)) - 1  # 0-indexed
                    max_scan_idx = max(max_scan_idx, idx)
                    max_frames = max(max_frames, reader.total_frames)
                    scan_map[idx] = reader

            if not scan_map:
                raise ValueError(
                    f"Could not parse scan indices from any dataset filenames using pattern '{scan_idx_pattern}'."
                )

            if is_column_scan:
                grid_shape = (max_frames, max_scan_idx + 1)
            else:  # Row scan
                grid_shape = (max_scan_idx + 1, max_frames)
            # --- END: MODIFIED GEOMETRY LOGIC ---

            # 2. Fetch all data and identify all numeric metrics
            all_metrics = set()
            raw_data = {}

            key_template = self.plugin_config["redis_key_template"]
            x_axis_key = self.plugin_config["x_axis_key"]

            for reader in self.datasets:
                filename = reader.master_file_path
                redis_key = key_template.format(master_file=filename)

                redis_results = self.redis_conn.hgetall(redis_key)

                parsed_frames = []
                # --- START MODIFICATION ---
                for frame_key, frame_data_json in redis_results.items():
                    try:
                        # Gracefully skip empty or non-string values
                        if not frame_data_json or not isinstance(frame_data_json, str):
                            continue
                        
                        frame_data = json.loads(frame_data_json)

                        # Ensure the loaded data is a dictionary before processing
                        if not isinstance(frame_data, dict):
                            logger.warning(
                                f"Skipping non-dictionary data in Redis key '{redis_key}', field '{frame_key}'."
                            )
                            continue

                        parsed_frames.append(frame_data)
                        for key, value in frame_data.items():
                            if isinstance(value, (int, float)) and key != x_axis_key:
                                all_metrics.add(key)

                    except json.JSONDecodeError:
                        logger.warning(
                            f"Skipping invalid JSON in Redis key '{redis_key}', field '{frame_key}'. "
                            f"Content might be incomplete."
                        )
                        continue  # Move to the next item
                # --- END MODIFICATION ---
                raw_data[filename] = parsed_frames

            # 3. Assemble data matrices
            data_matrices = {
                metric: np.full(grid_shape, np.nan) for metric in all_metrics
            }

            for filename, frame_results in raw_data.items():
                # Find the corresponding row index for this file
                current_scan_idx = -1
                for idx, rdr in scan_map.items():
                    if rdr.master_file_path == filename:
                        current_scan_idx = idx
                        break

                if current_scan_idx == -1:
                    continue

                for frame in frame_results:
                    original_frame_idx = frame.get(x_axis_key)
                    if original_frame_idx is not None:
                        original_frame_idx -= 1  # to 0-indexed

                        # --- START: MODIFIED INDEXING & PLACEMENT LOGIC ---
                        final_frame_idx = original_frame_idx
                        if is_serpentine and current_scan_idx % 2 == 1:
                            num_frames_in_scan = scan_map[
                                current_scan_idx
                            ].total_frames
                            final_frame_idx = (
                                num_frames_in_scan - 1
                            ) - original_frame_idx

                        if is_column_scan:
                            matrix_row, matrix_col = final_frame_idx, current_scan_idx
                        else:  # Row scan
                            matrix_row, matrix_col = current_scan_idx, final_frame_idx

                        if 0 <= matrix_row < grid_shape[0] and 0 <= matrix_col < grid_shape[1]:
                            raw_data_map[(matrix_row, matrix_col)] = frame
                            for metric in all_metrics:
                                if metric in frame:
                                    data_matrices[metric][
                                        matrix_row, matrix_col
                                    ] = frame[metric]
                        # --- END: MODIFIED INDEXING & PLACEMENT LOGIC ---

            # 4. Emit a dictionary containing everything needed by the manager
            final_result = {
                "data_matrices": data_matrices,
                "scan_map": scan_map,
                "raw_data_map": raw_data_map,
            }
            self.signals.finished.emit(final_result)

        except Exception as e:
            logger.error(f"Heatmap data worker failed: {e}", exc_info=True)
            self.signals.error.emit(str(e))
