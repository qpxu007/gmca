# qp2/image_viewer/heatmap/heatmap_data_from_file_worker.py
import json
import re
from pathlib import Path

import numpy as np
from PyQt5.QtCore import QRunnable, QObject, pyqtSignal

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class HeatmapDataFromFileSignals(QObject):
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)


class HeatmapDataFromFileWorker(QRunnable):
    """
    Fetches and assembles heatmap data for a run where results are stored
    in an external JSON file referenced in Redis.
    """

    def __init__(self, datasets, redis_conn, plugin_config, scan_mode: str):
        super().__init__()
        self.signals = HeatmapDataFromFileSignals()
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
            scan_idx_pattern = r"_C(\d+)" if is_column_scan else r"_R(\d+)"

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

            all_metrics = set()
            raw_data = {}
            key_template = self.plugin_config["redis_key_template"]
            x_axis_key = self.plugin_config["x_axis_key"]

            for reader in self.datasets:
                filename = reader.master_file_path
                redis_key = key_template.format(master_file=filename)
                json_path_str = self.redis_conn.hget(redis_key, "_results_json_path")
                if not json_path_str or not Path(json_path_str).exists():
                    logger.warning(
                        f"No results JSON file found for {filename}. Skipping."
                    )
                    continue

                with open(json_path_str, "r") as f:
                    data_from_json = json.load(f)

                # --- FIX: Handle both list and dict formats and add img_num ---
                parsed_frames = []
                if isinstance(data_from_json, dict):
                    # This is the nXDS case: {"1": {...}, "2": {...}}
                    for frame_num_str, frame_data in data_from_json.items():
                        try:
                            frame_data[x_axis_key] = int(
                                frame_num_str
                            )  # Add the img_num
                            parsed_frames.append(frame_data)
                        except (ValueError, TypeError):
                            continue  # Skip non-numeric keys like "_proc_dir"
                else:
                    # This handles the case where the file might be a list of dicts
                    parsed_frames = data_from_json
                # --- END FIX ---

                raw_data[filename] = parsed_frames
                for frame_data in parsed_frames:
                    for key, value in frame_data.items():
                        if isinstance(value, (int, float)) and key != x_axis_key:
                            all_metrics.add(key)

            all_metrics_list = sorted(list(all_metrics))
            for filename, frame_list in raw_data.items():
                for i in range(len(frame_list)):
                    for metric in all_metrics_list:
                        if metric not in frame_list[i]:
                            frame_list[i][
                                metric
                            ] = 0.0  # Use float for np.nan compatibility

            data_matrices = {
                metric: np.full(grid_shape, np.nan) for metric in all_metrics_list
            }

            for filename, frame_results in raw_data.items():
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
                        original_frame_idx -= 1

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
                            for metric in all_metrics_list:
                                data_matrices[metric][
                                    matrix_row, matrix_col
                                ] = frame[metric]
                        # --- END: MODIFIED INDEXING & PLACEMENT LOGIC ---

            final_result = {
                "data_matrices": data_matrices,
                "scan_map": scan_map,
                "raw_data_map": raw_data_map,
            }
            self.signals.finished.emit(final_result)

        except Exception as e:
            logger.error(f"Heatmap data worker (from file) failed: {e}", exc_info=True)
            self.signals.error.emit(str(e))
