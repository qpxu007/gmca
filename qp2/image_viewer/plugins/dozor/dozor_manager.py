# qp2/image_viewer/plugins/dozor/dozor_manager.py
import os
from typing import Optional

import numpy as np
from PyQt5 import QtCore, QtWidgets

from qp2.image_viewer.config import REDIS_DOZOR_KEY_PREFIX, DOZOR_PLOT_REFRESH_INTERVAL
from qp2.image_viewer.plugins.dozor.dozor_settings_dialog import DozorSettingsDialog
from qp2.image_viewer.plugins.dozor.find_spots_dozor_batch import DozorBatchWorker
from qp2.image_viewer.plugins.generic_plot_manager import GenericPlotManager
from qp2.log.logging_config import get_logger
from qp2.xio.proc_utils import determine_proc_base_dir

logger = get_logger(__name__)


class DozorManager(GenericPlotManager):
    def __init__(self, parent):
        dozor_config = {
            "worker_class": DozorBatchWorker,  # Use the new batch worker
            "redis_connection": parent.redis_output_server,
            "redis_key_template": f"{REDIS_DOZOR_KEY_PREFIX}:{{master_file}}",
            "spot_field_key": "spots",
            "x_axis_key": "img_num",
            "default_y_axis": "Main Score",
            "refresh_interval_ms": DOZOR_PLOT_REFRESH_INTERVAL,
            "default_source_type": "redis",
            "status_key_type": "hash",
        }
        super().__init__(parent=parent, name="Dozor", config=dozor_config)
        self.file_batch_queue = []
        self.batch_timer = QtCore.QTimer()
        self.batch_timer.setSingleShot(True)
        self.batch_timer.setInterval(5000)  # Wait 5s for more files to arrive
        self.batch_timer.timeout.connect(self._process_batch_queue)

    def update_source(self, new_reader, new_master_file):
        """Overrides GenericPlotManager to connect to series_completed signal."""
        if self.reader and hasattr(self.reader, "series_completed"):
            try:
                self.reader.series_completed.disconnect(self.handle_series_completed)
            except (TypeError, RuntimeError):
                pass

        # Also clear the queue when the source changes
        self.file_batch_queue.clear()
        if self.batch_timer.isActive():
            self.batch_timer.stop()

        super().update_source(new_reader, new_master_file)

        if self.reader and hasattr(self.reader, "series_completed"):
            self.reader.series_completed.connect(self.handle_series_completed)
            if self.reader.series_completion_signal_emitted and self.main_window.is_live_mode:
                self.handle_series_completed(
                    self.reader.master_file_path,
                    self.reader.total_frames,
                    self.reader.get_parameters(),
                )

    @QtCore.pyqtSlot(str, int, dict)
    def handle_series_completed(self, master_file_path, total_frames, metadata):
        """Forces the batch queue to be processed for small or completed datasets."""
        logger.info(
            f"Dataset series completed for {os.path.basename(master_file_path)}. Flushing Dozor batch queue."
        )
        if self.batch_timer.isActive():
            self.batch_timer.stop()
        # Process whatever is left in the queue for this dataset
        self._process_batch_queue()

    def _setup_ui(self):
        super()._setup_ui()
        self.dozor_settings_button = QtWidgets.QPushButton("⚙️ Settings")
        self.dozor_settings_button.setToolTip("Open Dozor-specific settings")
        # self.dozor_settings_button.setFixedSize(QtCore.QSize(30, 25))
        actions_button_index = (
            self.container_widget.layout()
            .itemAt(0)
            .layout()
            .indexOf(self.actions_button)
        )
        self.container_widget.layout().itemAt(0).layout().insertWidget(
            actions_button_index, self.dozor_settings_button
        )
        self.dozor_settings_button.clicked.connect(self._open_dozor_settings)

    def _open_dozor_settings(self):
        dialog = DozorSettingsDialog(
            current_settings=self.main_window.settings_manager.as_dict(),
            parent=self.main_window,
        )
        dialog.settings_changed.connect(
            self.main_window.settings_manager.update_from_dict
        )
        dialog.show()

    def _prepare_worker_kwargs(self) -> dict:
        settings = self.main_window.settings_manager
        
        user_root = settings.get("processing_common_proc_dir_root", "")
        data_path = self.reader.master_file_path if self.reader else ""
        proc_base = determine_proc_base_dir(user_root, data_path)
        dozor_log_dir = proc_base / "dozor" / "logs"
        dozor_log_dir.mkdir(parents=True, exist_ok=True)

        res_low = settings.get("dozor_res_cutoff_low", 20.0)
        if res_low == 20.0:
            common_low = settings.get("processing_common_res_cutoff_low")
            if common_low is not None:
                res_low = common_low
            
        res_high = settings.get("dozor_res_cutoff_high", 2.5)
        if res_high == 2.5:
            common_high = settings.get("processing_common_res_cutoff_high")
            if common_high is not None:
                res_high = common_high

        return {
            "dozor_beamstop_size": settings.get("dozor_beamstop_size", 100),
            "dozor_spot_size": settings.get("dozor_spot_size", 3),
            "dozor_spot_level": settings.get("dozor_spot_level", 6),
            "dozor_dist_cutoff": settings.get("dozor_dist_cutoff", 20.0),
            "dozor_res_cutoff_low": res_low,
            "dozor_res_cutoff_high": res_high,
            "dozor_check_ice_rings": settings.get("dozor_check_ice_rings", "T"),
            "dozor_exclude_resolution_ranges": settings.get(
                "dozor_exclude_resolution_ranges", []
            ),
            "dozor_min_spot_range_low": settings.get("dozor_min_spot_range_low", 15.0),
            "dozor_min_spot_range_high": settings.get("dozor_min_spot_range_high", 4.0),
            "dozor_min_spot_count": settings.get("dozor_min_spot_count", 2),
            "debug": settings.get("dozor_debug", False),
            "processing_common_proc_dir_root": user_root,
            "redis_key_prefix": self.config["redis_key_template"].split(
                ":{master_file}"
            )[0],
            "proc_dir": str(dozor_log_dir),
        }

    def _get_min_frames_for_batch(self, mode="live"):
        """Calculates dynamic batch size based on settings and file structure."""
        if not self.reader:
            return 100
        
        # Get base size (images per HDF5 file), defaulting to 100
        img_per_hdf = self.reader.params.get("images_per_hdf", 100)
        
        settings = self.main_window.settings_manager
        if mode == "live":
            multiplier = settings.get("dozor_live_batch_multiplier", 1.0)
        else:
            multiplier = settings.get("dozor_rerun_batch_multiplier", 5.0)
            
        return int(img_per_hdf * multiplier)

    # --- START: REWRITTEN BATCHING LOGIC ---
    @QtCore.pyqtSlot(list)
    def handle_data_files_ready(self, files_batch: list):
        """
        Intelligently batches incoming data files to create efficiently sized jobs.
        """
        if not self.run_processing_enabled:
            return

        # Stop any pending timer. We are receiving new data.
        if self.batch_timer.isActive():
            self.batch_timer.stop()

        # 1. Add new files and sort the entire queue to process in order.
        self.file_batch_queue.extend(files_batch)
        self.file_batch_queue.sort(key=lambda f: f["start_frame"])

        # 2. Greedily pack files into batches and submit them.
        files_to_keep_in_queue = []
        current_batch = []
        current_batch_frames = 0
        
        # Dynamic batch size
        min_frames_per_job = self._get_min_frames_for_batch(mode="live")

        for file_info in self.file_batch_queue:
            num_frames_in_segment = (
                file_info["end_frame"] - file_info["start_frame"] + 1
            )

            # Add the current file to the potential batch
            current_batch.append(file_info)
            current_batch_frames += num_frames_in_segment

            # If the batch is now large enough, process it.
            if current_batch_frames >= min_frames_per_job:
                logger.info(
                    f"Dozor queue formed a batch of {current_batch_frames} frames. Submitting job."
                )
                self._process_file_list(current_batch)
                # Reset for the next batch
                current_batch = []
                current_batch_frames = 0

        # 3. Any files left in current_batch are the new queue.
        self.file_batch_queue = current_batch

        # 4. If there are any "straggler" files left, start the timeout timer for them.
        if self.file_batch_queue:
            logger.debug(
                f"Dozor queue has {current_batch_frames} frames remaining. Starting 5s timer."
            )
            self.batch_timer.start()

    def _process_batch_queue(self):
        """Processes whatever is currently in the queue. Called by timer or completion signal."""
        if not self.file_batch_queue:
            return

        jobs_to_process = self.file_batch_queue.copy()
        self.file_batch_queue.clear()

        logger.info(
            f"Processing final batch of {len(jobs_to_process)} segments from queue."
        )
        self._process_file_list(jobs_to_process)

    def _process_file_list(self, file_list: list):
        """The core logic to turn a list of file info dicts into a worker."""
        worker_kwargs = self._prepare_worker_kwargs()
        if worker_kwargs is None:
            return

        job_batch_definitions = []
        for file_info in file_list:
            segment_id = (
                file_info["metadata"].get("master_file"),
                file_info["start_frame"],
            )
            if segment_id in self.processed_segments:
                continue

            self.processed_segments.add(segment_id)

            metadata_for_job = file_info["metadata"].copy()
            metadata_for_job.update(worker_kwargs)

            job_batch_definitions.append(
                {
                    "metadata": metadata_for_job,
                    "start_frame": file_info["start_frame"] + 1,
                    "nimages": file_info["end_frame"] - file_info["start_frame"] + 1,
                }
            )

        if not job_batch_definitions:
            return

        self._submit_batch_worker(job_batch_definitions, worker_kwargs)

    # --- END: REWRITTEN BATCHING LOGIC ---

    def _submit_batch_worker(self, job_definitions, worker_kwargs):
        """Helper method to create and submit a DozorBatchWorker."""
        self.status_update.emit(
            f"[{self.name}] Submitting batch job for {len(job_definitions)} data segments...",
            3000,
        )
        worker = DozorBatchWorker(
            job_batch=job_definitions,
            redis_conn=self.redis_connection,
            **worker_kwargs,
        )
        worker.signals.error.connect(self._handle_worker_error)
        worker.signals.result.connect(self._handle_worker_result)
        self.request_main_threadpool.emit(worker)

    @QtCore.pyqtSlot()
    def _rerun_analysis(self, reader=None, master_file=None):
        """
        Overrides GenericPlotManager to submit multiple, correctly sized batch jobs
        for an entire pre-existing dataset.
        """
        # Use passed args or fallback to current state
        target_reader = reader if reader else self.reader
        
        if not target_reader or target_reader.total_frames == 0:
            self.status_update.emit(
                f"[{self.name}] Cannot re-run: No data loaded.", 3000
            )
            return

        self.status_update.emit(
            f"[{self.name}] Staging re-run for entire dataset...", 3000
        )

        worker_kwargs = self._prepare_worker_kwargs()
        if worker_kwargs is None:
            self.status_update.emit(
                f"[{self.name}] Cannot re-run: Failed to prepare worker arguments.",
                4000,
            )
            return

        full_metadata = target_reader.get_parameters()
        full_metadata.update(worker_kwargs)

        current_batch = []
        current_batch_frames = 0
        
        # Dynamic batch size for reruns
        min_frames_per_job = self._get_min_frames_for_batch(mode="rerun")

        # Create a sorted list of all file segments for the dataset to process them in order.
        all_segments = sorted(target_reader.frame_map, key=lambda x: x[0])

        # --- START: APPLY THE BATCHING LOGIC ---
        for start_idx, end_idx, fpath, _ in all_segments:
            if not os.path.exists(fpath):
                logger.warning(f"Data file for re-run not found, skipping: {fpath}")
                continue

            num_frames_in_segment = (end_idx - 1) - start_idx + 1

            # Add the current segment to the batch-in-progress
            current_batch.append(
                {
                    "metadata": full_metadata,
                    "start_frame": start_idx + 1,  # Dozor is 1-based
                    "nimages": num_frames_in_segment,
                }
            )
            current_batch_frames += num_frames_in_segment

            # If the current batch is "full", submit it.
            if current_batch_frames >= min_frames_per_job:
                logger.info(
                    f"Submitting re-run batch with {current_batch_frames} frames."
                )
                self._submit_batch_worker(current_batch, worker_kwargs)
                # Reset for the next batch
                current_batch = []
                current_batch_frames = 0

        # After the loop, if there are any remaining "straggler" files in the last batch, submit them.
        if current_batch:
            logger.info(
                f"Submitting final re-run batch with {current_batch_frames} frames."
            )
            self._submit_batch_worker(current_batch, worker_kwargs)
        # --- END: APPLY THE BATCHING LOGIC ---

    def _parse_spot_data(self, spots_raw: list) -> Optional[np.ndarray]:
        if not spots_raw or not isinstance(spots_raw, list):
            return None
        try:
            return np.array([[s[2], s[1]] for s in spots_raw])
        except (IndexError, TypeError) as e:
            logger.error(f"[{self.name}] Failed to parse Dozor-specific spot data: {e}")
            return None

    def cleanup(self):
        """Extends base cleanup to also stop the batch timer."""
        self.batch_timer.stop()
        self.file_batch_queue.clear()
        super().cleanup()
