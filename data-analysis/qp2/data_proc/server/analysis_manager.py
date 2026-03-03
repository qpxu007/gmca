# qp2/data_proc/server/analysis_manager.py

import json
import threading
import time
import math
from collections import OrderedDict
from pathlib import Path
from typing import Optional, Dict, TYPE_CHECKING, List, Any

from PyQt5.QtCore import QObject, QRunnable, pyqtSlot

from qp2.image_viewer.plugins.crystfel.find_spots_crystfel import CrystfelDataFileWorker
from qp2.image_viewer.plugins.dials.find_spots_dials import DialsSpotfinderWorker
from qp2.image_viewer.plugins.dozor.find_spots_dozor import DozorWorker
from qp2.image_viewer.plugins.dozor.find_spots_dozor_batch import DozorBatchWorker
from qp2.image_viewer.utils.ring_math import angstrom_to_pixels
from qp2.image_viewer.plugins.spot_finder.find_spots_worker import (
    PeakFinderDataFileWorker,
)
from qp2.data_proc.server import xprocess
from qp2.xio.hdf5_to_cbf import convert_hdf5_to_cbf_for_strategy

from qp2.image_viewer.plugins.xds.submit_xds_job import XDSProcessDatasetWorker
from qp2.image_viewer.plugins.nxds.submit_nxds_job import NXDSProcessDatasetWorker
from qp2.image_viewer.plugins.xia2.submit_xia2_job import Xia2ProcessDatasetWorker
from qp2.image_viewer.plugins.xia2_ssx.submit_xia2_ssx_job import Xia2SSXProcessDatasetWorker
from qp2.image_viewer.plugins.crystfel.submit_crystfel_job import CrystfelProcessDatasetWorker
from qp2.image_viewer.plugins.autoproc.submit_autoproc_job import (
    AutoPROCProcessDatasetWorker,
)
from qp2.image_viewer.strategy.run_strategy import StrategyWorker

from qp2.data_proc.server.save_run_to_db import create_dataset_run, update_dataset_run, RunCreationError
from sqlalchemy.exc import IntegrityError

from qp2.log.logging_config import get_logger

if TYPE_CHECKING:
    from .data_processing_server import ProcessingServer

from qp2.config.redis_keys import AnalysisRedisKeys

logger = get_logger(__name__)

REDIS_KEYS = {
    "xds": AnalysisRedisKeys.XDS,
    "nxds": AnalysisRedisKeys.NXDS,
    "xia2": AnalysisRedisKeys.XIA2,
    "xia2_ssx": AnalysisRedisKeys.XIA2_SSX,
    "autoproc": AnalysisRedisKeys.AUTOPROC,
    "crystfel": AnalysisRedisKeys.CRYSTFEL,
}


class SegmentCache:
    """LRU + optional TTL cache for processed segment IDs."""

    def __init__(self, max_entries: int = 200000, ttl_seconds: int = 7200):
        # Use None to disable a bound when value <= 0
        self.max_entries = (
            max_entries if (isinstance(max_entries, int) and max_entries > 0) else None
        )
        self.ttl_seconds = (
            ttl_seconds if (isinstance(ttl_seconds, int) and ttl_seconds > 0) else None
        )
        self._store = OrderedDict()  # key -> last_seen_ts
        self._lock = threading.Lock()

    def _now(self) -> float:
        return time.time()

    def _purge_expired(self, now: float) -> None:
        if self.ttl_seconds is None:
            return
        # Evict from oldest side until the head is fresh
        while self._store:
            k, ts = next(iter(self._store.items()))
            if now - ts > self.ttl_seconds:
                self._store.popitem(last=False)
            else:
                break

    def contains(self, key) -> bool:
        now = self._now()
        with self._lock:
            self._purge_expired(now)
            if key in self._store:
                # Refresh LRU and timestamp
                self._store.move_to_end(key, last=True)
                self._store[key] = now
                return True
            return False

    def add(self, key) -> None:
        now = self._now()
        with self._lock:
            self._purge_expired(now)
            if key in self._store:
                self._store.move_to_end(key, last=True)
            self._store[key] = now
            if self.max_entries is not None:
                while len(self._store) > self.max_entries:
                    self._store.popitem(last=False)

    def clear_by_predicate(self, predicate) -> int:
        """Optional: selectively clear entries; returns number removed."""
        removed = 0
        with self._lock:
            to_remove = [k for k in self._store if predicate(k)]
            for k in to_remove:
                self._store.pop(k, None)
                removed += 1
        return removed


class AnalysisDispatcher(QRunnable):
    """
    A QRunnable worker that offloads the dispatching of analysis jobs
    from the main event loop to prevent it from blocking. It creates one
    processing directory per series and shares it among all jobs for that series.
    """

    def __init__(
        self, files_batch: list, pipeline: str, analysis_manager: "AnalysisManager"
    ):
        super().__init__()
        self.files_batch = files_batch
        self.pipeline = pipeline
        self.manager = analysis_manager

    @pyqtSlot()
    def run(self):
        """
        Iterates through the batch and submits analysis workers from a background thread.
        """
        logger.info(
            f"Background dispatcher starting for {len(self.files_batch)} files for pipeline '{self.pipeline}'."
        )

        # This dictionary will cache the proc_dir for each master file within this batch.
        series_proc_dirs: Dict[str, Optional[Path]] = {}

        for file_info in self.files_batch:
            # The segment cache check prevents re-processing of a specific data file chunk.
            segment_id = (
                file_info.get("file_path"),
                self.pipeline,
                file_info["start_frame"],
                file_info["end_frame"],
            )
            if self.manager.processed_segments.contains(segment_id):
                continue

            metadata = file_info.get("metadata", {})
            
            if self.manager.server.dry_run or metadata.get("dry_run"):
                logger.info(f"DRY RUN: Skipping {self.pipeline} dispatch for {file_info.get('file_path')}")
                continue

            master_file = metadata.get("master_file")

            if not master_file:
                logger.warning(
                    f"Skipping file_info, master_file is missing: {file_info}"
                )
                continue

            # Get or create the processing directory for this file's series.
            proc_dir = series_proc_dirs.get(master_file)

            if proc_dir is None and master_file not in series_proc_dirs:
                # We haven't seen this master file yet in this batch. Create its directory.
                run_prefix = metadata.get("run_prefix")
                prefix = metadata.get("prefix")
                series_subdir = None
                if run_prefix and prefix and run_prefix != prefix:
                    series_subdir = prefix
                
                proc_dir = self.manager.server.get_and_create_proc_dir_for_analysis(
                    master_file, self.pipeline, 
                    run_prefix_override=run_prefix,
                    series_subdir=series_subdir
                )
                # Cache the result (Path object on success, None on failure).
                series_proc_dirs[master_file] = proc_dir

            if not proc_dir:
                # Directory creation failed for this series, so we skip this job.
                # The 'None' value in the cache will cause subsequent files from the same
                # failed series to be skipped as well.
                continue

            # If we reach here, we have a valid proc_dir for this series.
            self.manager.processed_segments.add(segment_id)

            worker = None
            if "dozor" in self.pipeline.lower():
                # For Dozor, we use a batching mechanism.
                self.manager.add_to_dozor_batch(file_info, proc_dir)
                continue
            elif "spotfinder" in self.pipeline.lower():
                worker = self.manager._create_spotfinder_worker(file_info)
            elif "dials" in self.pipeline.lower():
                worker = self.manager._create_dials_spotfinder_worker(file_info)
            elif "crystfel" in self.pipeline.lower():
                worker = self.manager._create_crystfel_worker(file_info, proc_dir)
            else:
                logger.warning(
                    f"No worker configured for pipeline: '{self.pipeline}' in dispatcher"
                )
                continue

            if worker:
                # Submit the actual analysis worker to the server's main pool.
                # QThreadPool is thread-safe.
                self.manager.server.worker_pool.start(worker)

        logger.info(f"Background dispatcher finished for pipeline '{self.pipeline}'.")


class AnalysisManager(QObject):
    """
    Manages the creation and submission of analysis workers (Dozor, SpotFinder)
    in response to new data files.
    """

    REDIS_SPOTFINDER_KEY_PREFIX = AnalysisRedisKeys.SPOTFINDER
    REDIS_DOZOR_KEY_PREFIX = AnalysisRedisKeys.DOZOR
    REDIS_DIALS_KEY_PREFIX = AnalysisRedisKeys.DIALS
    REDIS_CRYSTFEL_KEY_PREFIX = AnalysisRedisKeys.CRYSTFEL_STREAM

    def __init__(self, processing_server: "ProcessingServer"):
        super().__init__()
        self.server = processing_server
        self.config = self._load_analysis_config()

        # Use the stored config to initialize the cache.
        ps_cfg = self.config.get("processed_segments", {})
        max_entries = int(ps_cfg.get("max_entries", 200000))
        ttl_seconds = int(ps_cfg.get("ttl_seconds", 7200))
        self.processed_segments = SegmentCache(
            max_entries=max_entries, ttl_seconds=ttl_seconds
        )

        # Batching for Dozor
        self.dozor_queues: Dict[str, List[tuple]] = {}
        self.dozor_lock = threading.Lock()

    def _load_analysis_config(self) -> dict:
        """Loads analysis parameters from a JSON config file."""
        # The config file should be located relative to this script.
        config_path = Path(__file__).parent / "analysis_config.json"
        logger.info(f"Loading analysis configuration from: {config_path}")

        if not config_path.exists():
            logger.error(
                f"CRITICAL: Analysis config file not found at {config_path}. Workers may fail."
            )
            return {}  # Return empty dict on failure

        try:
            with open(config_path, "r") as f:
                config_data = json.load(f)
            logger.info("Successfully loaded analysis configuration.")
            return config_data
        except (IOError, json.JSONDecodeError) as e:
            logger.error(
                f"CRITICAL: Failed to read or parse analysis_config.json: {e}",
                exc_info=True,
            )
            return {}

    @pyqtSlot(list, str)
    def handle_data_files_ready(self, files_batch: list, pipeline: str):
        """
        Receives a batch of data files and immediately offloads the dispatching
        to a background worker to keep the main event loop free.
        """
        if not files_batch:
            return

        logger.debug(
            f"Received {len(files_batch)} files for '{pipeline}'. Offloading dispatch to background worker."
        )

        dispatcher_worker = AnalysisDispatcher(files_batch, pipeline, self)

        # Submit the dispatcher to the thread pool. This is a fast, non-blocking call.
        self.server.worker_pool.start(dispatcher_worker)

    @pyqtSlot(str, int, dict)
    def handle_series_completed(
        self, master_file: str, total_frames: int, metadata: dict
    ):
        """
        Slot that receives the series_completed signal directly from the HDF5Reader.
        """
        run_prefix = metadata.get("run_prefix", "unknown_run")
        logger.info(
            f"SERIES COMPLETED signal received for run '{run_prefix}'. "
            f"File: {Path(master_file).name}"
        )
        
        # Flush any remaining Dozor batches for this series
        self.flush_dozor_batches(master_file)

        # Trigger the appropriate per-series analysis logic
        self.handle_series_completion_logic(master_file, total_frames, metadata)

    def add_to_dozor_batch(self, file_info: dict, proc_dir: Path):
        """Adds a data segment to the Dozor batching queue."""
        master_file = file_info["metadata"]["master_file"]
        dozor_cfg = self.config.get("dozor", {})
        batch_size = dozor_cfg.get("batch_size", 5)
        
        with self.dozor_lock:
            if master_file not in self.dozor_queues:
                self.dozor_queues[master_file] = []
            
            queue = self.dozor_queues[master_file]
            queue.append((file_info, proc_dir))
            
            if len(queue) >= batch_size:
                batch_to_submit = queue[:]
                self.dozor_queues[master_file] = []
                self._submit_dozor_batch(batch_to_submit)

    def flush_dozor_batches(self, master_file: str):
        """Submits any remaining segments in the Dozor queue for a series."""
        with self.dozor_lock:
            queue = self.dozor_queues.pop(master_file, [])
            if queue:
                logger.info(f"Flushing Dozor queue for {Path(master_file).name} with {len(queue)} segments.")
                self._submit_dozor_batch(queue)

    def _submit_dozor_batch(self, batch_data: list):
        """Converts internal queue data into a DozorBatchWorker and submits it."""
        if not batch_data:
            return
            
        first_info, proc_dir = batch_data[0]
        master_file = first_info["metadata"]["master_file"]
        
        # Prepare Dozor parameters from config
        dozor_cfg = self.config.get("dozor", {})
        dozor_params = {f"dozor_{k}": v for k, v in dozor_cfg.items() if k != "batch_size"}
        
        job_batch_definitions = []
        for file_info, _ in batch_data:
            job_batch_definitions.append({
                "metadata": file_info["metadata"],
                "start_frame": file_info["start_frame"] + 1, # Dozor is 1-based
                "nimages": file_info["end_frame"] - file_info["start_frame"] + 1,
            })
            
        worker = DozorBatchWorker(
            job_batch=job_batch_definitions,
            redis_conn=self.server.redis_manager.get_analysis_connection(),
            proc_dir=str(proc_dir.resolve()),
            redis_key_prefix=self.REDIS_DOZOR_KEY_PREFIX,
            **dozor_params
        )

        logger.debug(f"Submitting Dozor batch job for {Path(master_file).name} with {len(job_batch_definitions)} segments.")
        self.server.worker_pool.start(worker)

    def _create_crystfel_worker(
        self, file_info: dict, proc_dir: Path
    ) -> Optional[QRunnable]:
        """Creates and configures a CrystfelDataFileWorker instance."""
        logger.debug(
            f"Creating CrystfelDataFileWorker for {Path(file_info['file_path']).name}"
        )

        crystfel_kwargs = self.config.get("crystfel", {})
        if not crystfel_kwargs:
            logger.error(
                "'crystfel' configuration not found in analysis_config.json. Cannot run job."
            )
            return None

        master_file = file_info["metadata"]["master_file"]
        run_prefix = file_info["metadata"].get("run_prefix", Path(master_file).stem)
        readers = self.server.run_hdf5_readers.get(run_prefix, [])
        reader = next(
            (
                r
                for r in readers
                if Path(getattr(r, "master_file", "")).resolve()
                == Path(master_file).resolve()
            ),
            None,
        )
        file_info["metadata"]["hdf5_reader_instance"] = reader

        return CrystfelDataFileWorker(
            file_path=file_info["file_path"],
            start_frame=file_info["start_frame"],
            end_frame=file_info["end_frame"],
            metadata=file_info["metadata"],
            redis_conn=self.server.redis_manager.get_analysis_connection(),
            redis_key_prefix=self.REDIS_CRYSTFEL_KEY_PREFIX,
            proc_dir=str(proc_dir.resolve()),
            **crystfel_kwargs,
        )

    def _create_dials_spotfinder_worker(self, file_info: dict) -> Optional[QRunnable]:
        """Creates and configures a DialsSpotfinderWorker instance."""
        logger.debug(
            f"Creating DialsSpotfinderWorker for {Path(file_info['file_path']).name}"
        )

        dials_kwargs = {
            "min_spot_size": 3,
            "method": "auto",
        }

        return DialsSpotfinderWorker(
            file_path=file_info["file_path"],
            start_frame=file_info["start_frame"],
            end_frame=file_info["end_frame"],
            metadata=file_info["metadata"],
            redis_conn=self.server.redis_manager.get_analysis_connection(),
            redis_key_prefix=self.REDIS_DIALS_KEY_PREFIX,
            **dials_kwargs,
        )

    def _create_dozor_worker(
        self, file_info: dict, proc_dir: Path
    ) -> Optional[QRunnable]:
        """Creates and configures a DozorWorker instance."""
        dozor_cfg = self.config.get("dozor", {})
        dozor_params = {f"dozor_{k}": v for k, v in dozor_cfg.items() if k != "batch_size"}

        return DozorWorker(
            file_path=file_info["file_path"],
            start_frame=file_info["start_frame"],
            end_frame=file_info["end_frame"],
            metadata=file_info["metadata"],
            redis_key_prefix=self.REDIS_DOZOR_KEY_PREFIX,
            redis_conn=self.server.redis_manager.get_analysis_connection(),
            proc_dir=str(proc_dir.resolve()),
            method="auto",
            **dozor_params
        )

    def _create_spotfinder_worker(self, file_info: dict) -> Optional[QRunnable]:
        """Creates and configures a PeakFinderDataFileWorker instance."""
        logger.debug(
            f"Creating PeakFinderDataFileWorker for {Path(file_info['file_path']).name}"
        )

        spotfinder_kwargs = self._get_peak_finder_kwargs(
            file_info["metadata"]["params"]
        )

        if spotfinder_kwargs is None:
            logger.error(
                f"Could not get valid spot finder parameters for {file_info['metadata']['master_file']}. Skipping worker."
            )
            return None

        worker_metadata = {
            "master_file": file_info["metadata"]["master_file"],
            "params": file_info["metadata"]["params"],
        }

        return PeakFinderDataFileWorker(
            file_path=file_info["file_path"],
            start_frame=file_info["start_frame"],
            end_frame=file_info["end_frame"],
            metadata=worker_metadata,
            redis_conn=self.server.redis_manager.get_analysis_connection(),
            redis_key_prefix=self.REDIS_SPOTFINDER_KEY_PREFIX,
            **spotfinder_kwargs,
        )

    def _get_peak_finder_kwargs(self, params: dict) -> Optional[dict]:
        """
        Gathers all necessary parameters for peak finding from the class defaults
        and the provided detector parameters.
        """
        if not params:
            logger.warning(
                "get_peak_finder_kwargs: Cannot get parameters, params dict is empty."
            )
            return None

        spotfinder_config = self.config.get("spotfinder", {})
        if not spotfinder_config:
            logger.error("'spotfinder' section not found in analysis_config.json.")
            return None

        kwargs = {
            "num_peaks": spotfinder_config.get("peak_finding_num_peaks"),
            "min_distance": spotfinder_config.get("peak_finding_min_distance"),
            "threshold_abs": spotfinder_config.get("peak_finding_min_intensity"),
            "median_filter_size": spotfinder_config.get(
                "peak_finding_median_filter_size"
            ),
            "zscore_cutoff": spotfinder_config.get("peak_finding_zscore_cutoff"),
        }

        try:
            r1_px = angstrom_to_pixels(
                spotfinder_config["peak_finding_min_resolution_A"],
                params["wavelength"],
                params["det_dist"],
                params["pixel_size"],
            )
            r2_px = angstrom_to_pixels(
                spotfinder_config["peak_finding_max_resolution_A"],
                params["wavelength"],
                params["det_dist"],
                params["pixel_size"],
            )
            kwargs["r1"] = min(r1_px, r2_px)
            kwargs["r2"] = max(r1_px, r2_px)
        except (KeyError, ZeroDivisionError, TypeError) as e:
            logger.error(f"Could not calculate pixel radii: {e}", exc_info=True)
            return None

        logger.debug(f"Prepared spot finder kwargs: {kwargs}")
        return kwargs

    def clear_segments_for_run(self, run_prefix: str) -> int:
        return self.processed_segments.clear_by_predicate(
            lambda seg: isinstance(seg, tuple)
            and isinstance(seg[0], str)
            and (
                Path(seg[0]).name.startswith(run_prefix) or f"/{run_prefix}_" in seg[0]
            )
        )

    def _ensure_dataset_run_db_record(
        self,
        run_prefix: str,
        total_frames: int,
        master_files: List[str],
        metadata_list: List[dict],
        opt: Optional[Dict[str, Any]] = None,
    ):
        """
        Ensures a DatasetRun record exists in the database.
        Uses cached options if provided, otherwise calculates them.
        """
        if not self.server.enable_db_logging or not self.server.db_manager:
            return

        logger.info(f"Saving run {run_prefix} to DB.")

        # Calculate enriched options if not provided
        if opt is None:
            try:
                opt = self.server.get_opt(metadata_list)
            except Exception as e:
                logger.warning(f"Could not calculate enriched options for DB save: {e}")
                opt = {}

        mounted_val = opt.get("robot_mounted")
        meta_user_val = opt.get("meta_user")

        try:
            new_run = create_dataset_run(
                self.server.db_manager,
                run_prefix,
                total_frames,
                master_files,
                metadata_list,
                mounted=mounted_val,
                meta_user=meta_user_val,
            )
            logger.info(f"Run created successfully in DB (data_id {new_run.data_id})")
        except IntegrityError:
            logger.info(f"Run '{run_prefix}' exists. Updating record with latest info.")
            try:
                update_dataset_run(
                    self.server.db_manager,
                    run_prefix,
                    total_frames,
                    master_files,
                    metadata_list,
                    mounted=mounted_val,
                    meta_user=meta_user_val,
                )
            except Exception as e:
                logger.error(f"Failed to update existing run '{run_prefix}': {e}")
        except RunCreationError as e:
            logger.error(f"Error creating run in DB: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error creating run '{run_prefix}' in DB.")

    def handle_run_start_logic(
        self,
        run_prefix: str,
        total_frames: int,
        master_files: List[str],
        metadata_list: List[dict],
    ):
        """Logic for when a run starts."""
        if not metadata_list:
            return

        # Ensure DB record exists early for milestone linking
        self._ensure_dataset_run_db_record(
            run_prefix, total_frames, master_files, metadata_list
        )

    def submit_plugin_job(
        self,
        worker_class,
        master_file: str,
        metadata: dict,
        redis_key_prefix: str,
        **kwargs,
    ):
        """Generic method to submit a dataset-level worker."""
        try:
            # BUGFIX: Add input validation at the top
            if not worker_class or not callable(getattr(worker_class, '__init__', None)):
                logger.error(f"Invalid worker_class: {worker_class}. Must be a valid class.")
                return
            
            if not master_file:
                logger.error("submit_plugin_job: master_file cannot be empty")
                return
            
            master_path = Path(master_file)
            if not master_path.exists():
                logger.error(f"submit_plugin_job: master_file does not exist: {master_file}")
                return
            
            if not isinstance(metadata, dict):
                logger.error(f"submit_plugin_job: metadata must be a dict, got {type(metadata)}")
                return
            
            required_metadata_keys = ["run_prefix", "prefix"]
            missing_keys = [k for k in required_metadata_keys if k not in metadata]
            if missing_keys:
                logger.error(f"submit_plugin_job: Missing required metadata keys: {missing_keys}")
                return
            
            if not redis_key_prefix:
                logger.error("submit_plugin_job: redis_key_prefix cannot be empty")
                return
            
            # 1. Determine Pipeline Name from worker class
            pipeline_name = "unknown"
            name_lower = worker_class.__name__.lower()
            if "xds" in name_lower and "nxds" not in name_lower:
                pipeline_name = "xds"
            elif "nxds" in name_lower:
                pipeline_name = "nxds"
                # Strict enforcement: nXDS only for RASTER mode
                collect_mode = metadata.get("collect_mode", "STANDARD").upper()
                if collect_mode != "RASTER":
                    logger.warning(
                        f"AnalysisManager: nXDS job requested for non-RASTER collection mode: {collect_mode}. Skipping submission."
                    )
                    return
            elif "xia2" in name_lower:
                pipeline_name = "xia2"
            elif "autoproc" in name_lower:
                pipeline_name = "autoproc"
            elif "dials" in name_lower:
                pipeline_name = "dials_ssx"

            # 2. Determine if series_subdir should be used
            # Rules: 
            # - Omit if only 1 series in run
            # - Omit if Mode is STRATEGY
            # - Omit for run-level merged jobs (which pass series_subdir=None explicitly)
            
            run_prefix = metadata.get("run_prefix")
            prefix = metadata.get("prefix")
            collect_mode = metadata.get("collect_mode", "STANDARD").upper()
            
            # Use the server's tracking to get actual series count
            num_series = 1
            if run_prefix:
                with self.server.run_readers_lock:
                    m_files = self.server.run_master_files.get(run_prefix, [])
                    num_series = len(m_files) if m_files else 1

            series_subdir = kwargs.pop("series_subdir", None)
            
            # Force omit if conditions met
            # Robust logic: If prefixes differ, strictly enforce hierarchy even if num_series=1 (e.g. streaming lag)
            prefixes_differ = (run_prefix and prefix and run_prefix != prefix)
            
            if collect_mode == "STRATEGY":
                series_subdir = None
            elif num_series <= 1 and not prefixes_differ:
                # Only flatten if truly single series AND prefixes match
                series_subdir = None

            # 3. Calculate standard processing directory
            if "output_proc_dir" not in kwargs and "proc_dir" not in kwargs:
                milestone_suffix = None
                if "job_tag" in kwargs:
                     milestone_suffix = kwargs["job_tag"]
                     # Append suffix to redis key prefix to ensure uniqueness for milestones
                     redis_key_prefix = f"{redis_key_prefix}:{milestone_suffix}"
                
                proc_dir_path = self.server.get_and_create_proc_dir_for_analysis(
                    master_file, 
                    pipeline_name, 
                    milestone_suffix_str=milestone_suffix,
                    series_subdir=series_subdir,
                    run_prefix_override=run_prefix,
                )

                if proc_dir_path:
                    kwargs["output_proc_dir"] = str(proc_dir_path)
            elif "proc_dir" in kwargs and "output_proc_dir" not in kwargs:
                kwargs["output_proc_dir"] = kwargs["proc_dir"]

            if "opt" in kwargs:
                opt = kwargs["opt"]
                if opt.get("model_pdb"):
                    kwargs[f"{pipeline_name}_model_pdb"] = opt["model_pdb"]
                if opt.get("space_group"):
                    kwargs[f"{pipeline_name}_space_group"] = opt["space_group"]
                if opt.get("unit_cell"):
                    kwargs[f"{pipeline_name}_unit_cell"] = opt["unit_cell"]
                if opt.get("reference_hkl"):
                    kwargs[f"{pipeline_name}_reference_hkl"] = opt["reference_hkl"]
                if opt.get("spreadsheet"):
                    kwargs["spreadsheet"] = opt["spreadsheet"]
                # Default to native if not specified
                is_native = opt.get("native", True)
                # Fallback for legacy calls
                if "anomalous" in opt:
                    is_native = not opt["anomalous"]
                
                kwargs[f"{pipeline_name}_native"] = is_native


            logger.debug(f"Preparing to submit {worker_class.__name__} with parameters: {kwargs}")
            logger.debug(f"metadata: {metadata}")

            # --- DRY RUN CHECK ---
            if self.server.dry_run or kwargs.get("opt", {}).get("dry_run"):
                kwargs["dry_run"] = True

            is_dry_run = kwargs.get("dry_run")
            if is_dry_run:
                logger.info(f"DRY RUN: Skipping worker submission for {pipeline_name}")
                logger.info(f"  Worker Class: {worker_class.__name__}")
                logger.info(f"  Master File:  {master_file}")
                logger.info(f"  Proc Dir:     {kwargs.get('output_proc_dir', 'N/A')}")
                logger.info(f"  Parameters:   {kwargs}")
                return

            logger.info(
                f"AnalysisManager: Submitting {worker_class.__name__} for {Path(master_file).name} (Subdir: {series_subdir or 'root'})"
            )

            # Get connection from server
            redis_conn = self.server.redis_manager.get_analysis_connection()

            # Instantiate worker
            worker = worker_class(
                master_file=master_file,
                metadata=metadata,
                redis_conn=redis_conn,
                redis_key_prefix=redis_key_prefix,
                **kwargs,
            )

            # Connect signals
            worker.signals.result.connect(
                lambda s, m, f: logger.debug(f"Job Result ({Path(f).name}): {s} - {m}")
            )
            worker.signals.error.connect(
                lambda f, e: logger.error(f"Job Error ({Path(f).name}): {e}")
            )

            # Launch
            self.server.worker_pool.start(worker)
        except Exception as e:
            logger.error(f"Failed to submit plugin job: {e}", exc_info=True)

    def update_dataset_run_with_new_series(
        self,
        run_prefix: str,
        master_files: List[str],
        metadata_list: List[dict],
    ):
        """
        Updates the existing DatasetRun record with the new list of master files
        and updated metadata list (accumulated from all series).
        """
        # We try to get n_images from the first series as a best guess for total run length
        # if available, or just use 0. Ideally, this should come from run_started info.
        total_frames = 0
        if metadata_list:
            total_frames = metadata_list[0].get("n_images", 0)
        
        try:
            update_dataset_run(
                self.server.db_manager,
                run_prefix,
                total_frames,
                master_files,
                metadata_list,
                # mounted=None,  # Don't overwrite
                # meta_user=None, # Don't overwrite
            )
            logger.debug(f"Updated DatasetRun for '{run_prefix}' with {len(master_files)} series.")
            # Notify data viewer of the update via Redis pub/sub
            self._publish_dataset_run_update(run_prefix, len(master_files))
        except Exception:
            logger.exception(f"Failed to update DatasetRun for '{run_prefix}' on new series.")

    def _publish_dataset_run_update(self, run_prefix: str, num_series: int):
        """Publishes a notification to the pipeline_updates Redis channel for DatasetRun changes."""
        try:
            redis_conn = self.server.redis_manager.get_analysis_connection()
            if redis_conn:
                notification = json.dumps({
                    "pipeline_name": "dataset_run_update",
                    "status": "UPDATED",
                    "sample_name": run_prefix,
                    "num_series": num_series,
                })
                redis_conn.publish("pipeline_updates", notification)
                logger.debug(f"Published dataset_run_update notification for '{run_prefix}'")
        except Exception:
            logger.debug(f"Failed to publish dataset_run_update for '{run_prefix}' (non-critical)")

    def handle_series_completion_logic(
        self, master_file: str, total_frames: int, metadata: dict
    ):
        """Logic for when a single series (file) finishes."""
        collect_mode = metadata.get("collect_mode", "STANDARD").upper()
        run_prefix = metadata.get("run_prefix")
        prefix = metadata.get("prefix")
        
        # Determine number of series in the run from server
        num_series = 1
        with self.server.run_readers_lock:
            m_files = self.server.run_master_files.get(run_prefix, [])
            num_series = len(m_files) if m_files else 1

        # Use series subdir ONLY if num_series > 1 or prefixes differ (and not STRATEGY)
        series_subdir = None
        if (num_series > 1 or (run_prefix and prefix and run_prefix != prefix)) and collect_mode != "STRATEGY":
            series_subdir = prefix
            
        logger.info(
            f"AnalysisManager: Series complete {Path(master_file).name}. Mode: {collect_mode} (num_series: {num_series})"
        )

        if collect_mode.upper() in ["SINGLE", "STANDARD", "VECTOR", "SITE"]:
            opt = self.server.get_opt([metadata])

            # Run XDS on the series
            xds_defaults = {"xds_nproc": 32, "xds_njobs": 4, "xds_native": True, "series_subdir": series_subdir}
            self.submit_plugin_job(
                XDSProcessDatasetWorker,
                master_file,
                metadata,
                REDIS_KEYS["xds"],
                opt=opt,
                **xds_defaults,
            )
        elif collect_mode.upper() == "RASTER":
            # Run nXDS on the series
            nxds_defaults = {
                "nxds_nproc": 16,
                "nxds_njobs": 4,
                "nxds_auto_merge": False,
                "series_subdir": series_subdir
            }
            self.submit_plugin_job(
                NXDSProcessDatasetWorker,
                master_file,
                metadata,
                REDIS_KEYS["nxds"],
                **nxds_defaults,
            )

    def handle_milestone_logic(
        self, run_prefix: str, milestone: str, master_files: List[str], metadata: dict
    ):
        """Logic for 25% / 50% milestones."""
        collect_mode = metadata.get("collect_mode", "STANDARD").upper()
        prefix = metadata.get("prefix")
        num_series = len(master_files)

        if milestone in ["25%", "50%"]:
            if collect_mode.upper() in ["SINGLE", "STANDARD", "VECTOR"] and num_series == 1:
                master_file = master_files[0]
                
                # Check for series subdir
                series_subdir = None
                if run_prefix and prefix and run_prefix != prefix:
                    series_subdir = prefix
                
                # BUGFIX: Validate total_frames before calculation
                total_frames = metadata.get("n_images", 0)
                if total_frames <= 0:
                    logger.warning(
                        f"Invalid total_frames ({total_frames}) for milestone {milestone} on {run_prefix}. Skipping milestone job."
                    )
                    return
                
                # Calculate end frame based on percentage
                percent_val = int(milestone.replace("%", ""))
                end_frame = max(1, int(total_frames * (percent_val / 100.0)))  # At least frame 1

                xds_defaults = {
                    "xds_nproc": 32,
                    "xds_njobs": 4,
                    "xds_end": end_frame,
                    "job_tag": f"{milestone.replace('%','pct')}",
                    "series_subdir": series_subdir
                }
                logger.info(
                    f"AnalysisManager: Triggering Milestone XDS ({milestone}, end frame: {end_frame}) for {run_prefix}"
                )
                self.submit_plugin_job(
                    XDSProcessDatasetWorker,
                    master_file,
                    metadata,
                    REDIS_KEYS["xds"],
                    **xds_defaults,
                )

    def handle_run_completion_logic(
        self, run_prefix: str, master_files: List[str], metadata_list: List[dict]
    ):
        """Logic for when the entire run is finished."""
        if not metadata_list:
            return
        meta = metadata_list[0]
        collect_mode = meta.get("collect_mode", "STANDARD").upper()

        # --- CHECK: Minimum Frames Threshold ---
        if collect_mode in ["SINGLE", "STANDARD", "VECTOR"]:
            total_images = meta.get("n_images")
            if total_images is not None:
                min_frames = self.config.get("minimum_frames_for_pipelines", 20)
                if total_images < min_frames:
                    logger.info(
                        f"AnalysisManager: Skipping pipelines for run '{run_prefix}'. Total frames ({total_images}) < minimum ({min_frames})."
                    )
                    return

        logger.info(
            f"AnalysisManager: Run complete '{run_prefix}'. Mode: {collect_mode}. Files: {len(master_files)}"
        )

        # Pre-calculate enriched options (uses cached xlsReader internally)
        opt = self.server.get_opt(metadata_list)

        if collect_mode in ["SINGLE", "STANDARD", "HELICAL", "SITE", "VECTOR"]:
            # Option A: Run merged job if multiple files exist
            if len(master_files) > 1:
                logger.info(
                    f"Submitting MERGED autoPROC and xia2 jobs for {len(master_files)} files."
                )
                primary_file = master_files[0]
                extra_files = master_files[1:]

                # AutoPROC Merged
                self.submit_plugin_job(
                    AutoPROCProcessDatasetWorker,
                    primary_file,
                    meta,
                    REDIS_KEYS["autoproc"],
                    autoproc_nproc=24,
                    autoproc_njobs=4,
                    autoproc_fast=True,
                    extra_data_files=extra_files,  # Worker must support this kwarg
                    opt=opt,
                )

                # Xia2 Merged
                self.submit_plugin_job(
                    Xia2ProcessDatasetWorker,
                    primary_file,
                    meta,
                    REDIS_KEYS["xia2"],
                    xia2_pipeline="xia2_dials",
                    xia2_nproc=24,
                    xia2_njobs=4,
                    extra_data_files=extra_files,  # Worker must support this kwarg
                    opt=opt,
                )

                # XDS Merged
                xds_defaults = {"xds_nproc": 32, "xds_njobs": 4, "xds_native": True}
                self.submit_plugin_job(
                    XDSProcessDatasetWorker,
                    primary_file,
                    meta,
                    REDIS_KEYS["xds"],
                    extra_data_files=extra_files,
                    opt=opt,
                    **xds_defaults,
                )

            # Option B: Run per-series jobs (Original logic - implied if list length is 1, or if we iterate)
            else:
                # Single file case
                if self.config.get("run_for_each_series_upon_run_completion", None):
                    for i, master_file in enumerate(master_files):
                        series_meta = (
                            metadata_list[i] if i < len(metadata_list) else meta
                        )
                        # Determine if we need a subdirectory for this series
                        # Only if it differs from run_prefix
                        series_prefix = series_meta.get("prefix")
                        series_subdir = None
                        if run_prefix and series_prefix and run_prefix != series_prefix:
                            series_subdir = series_prefix

                        self.submit_plugin_job(
                            AutoPROCProcessDatasetWorker,
                            master_file,
                            series_meta,
                            REDIS_KEYS["autoproc"],
                            autoproc_nproc=16,
                            autoproc_njobs=2,
                            autoproc_fast=True,
                            series_subdir=series_subdir,
                            opt=opt
                        )
                        self.submit_plugin_job(
                            Xia2ProcessDatasetWorker,
                            master_file,
                            series_meta,
                            REDIS_KEYS["xia2"],
                            xia2_pipeline="xia2_dials",
                            xia2_nproc=16,
                            xia2_njobs=2,
                            series_subdir=series_subdir,
                            opt=opt
                        )


        elif collect_mode == "STRATEGY":
            logger.info(
                f"AnalysisManager: Launching Strategy Pipelines for {run_prefix}"
            )

            # 1. Construct Mapping
            # Strategy datasets usually imply processing frame 1.
            # If metadata contains specific frames (e.g. 'run_fr_start'), we could use that,
            # but [1] is the standard default for strategy auto-indexing.
            mapping = {mf: [1] for mf in master_files}

            # 2. Construct Pipeline Params for DB Logging
            # We use enriched 'opt' instead of raw 'meta' to get fallbacks
            pipeline_params = {
                "username": opt.get("username"),
                "beamline": opt.get("beamline"),
                "primary_group": opt.get("groupname") or opt.get("username"),
                "esaf_id": opt.get("esaf_id"),
                "pi_id": opt.get("pi_id"),
                "sampleName": opt.get("sample_id") or run_prefix,
                "run_prefix": run_prefix,
                "mounted": opt.get("robot_mounted"),
            }
            # Filter out None values
            pipeline_params = {
                k: v for k, v in pipeline_params.items() if v is not None
            }

            # 3. Create and Start Strategy Worker
            # This runs both XDS and MOSFLM strategies in parallel (via the list ["xds", "mosflm"])
            # It handles HDF5 files directly, so no CBF conversion is needed.
            try:
                worker = StrategyWorker(
                    programs=["xds", "mosflm"],
                    mapping=mapping,
                    pipeline_params=pipeline_params,
                    redis_conn=self.server.redis_manager.get_analysis_connection(),
                    delete_workdir=True,  # Clean up temp dirs after finish
                )

                # Connect signals for server logging
                worker.signals.finished.connect(
                    lambda p, r, m, rp=run_prefix: logger.info(
                        f"Strategy {p} finished for {rp}."
                    )
                )
                worker.signals.error.connect(
                    lambda p, e, rp=run_prefix: logger.error(f"Strategy {p} failed for {rp}: {e}")
                )

                self.server.worker_pool.start(worker)

            except Exception as e:
                logger.error(f"Failed to submit StrategyWorker: {e}", exc_info=True)

    def handle_external_job_request(self, job_data: dict) -> bool:
        """
        Handles job submission from external JSON payload (e.g. from client.py).
        Parses the payload, determines the pipeline, and submits the appropriate Worker.
        """
        pipeline = job_data.get("pipeline", "").lower()
        
        # Map pipeline string (case-insensitive) to Worker Class
        # and standard pipeline name for REDIS_KEYS
        pipeline_map = {
            "xds": (XDSProcessDatasetWorker, "xds"),
            "nxds": (NXDSProcessDatasetWorker, "nxds"),
            "xia2": (Xia2ProcessDatasetWorker, "xia2"),
            "xia2_dials": (Xia2ProcessDatasetWorker, "xia2"), # Alias
            "xia2_ssx": (Xia2SSXProcessDatasetWorker, "xia2_ssx"),
            "autoproc": (AutoPROCProcessDatasetWorker, "autoproc"),
            "crystfel": (CrystfelProcessDatasetWorker, "crystfel"),
        }
        
        target = pipeline_map.get(pipeline.lower())
        if not target:
            logger.warning(f"Unknown pipeline '{pipeline}' requested via external API. Falling back to xprocess.")
            return False

        WorkerClass, pipeline_key = target
        redis_key = REDIS_KEYS.get(pipeline_key, f"analysis:out:{pipeline_key}")

        # Normalize common keys to prefixed versions expected by workers
        common_mappings = {
            "start": [f"{pipeline_key}_start", "start_frame"],
            "end": [f"{pipeline_key}_end", "end_frame"],
            "highres": [f"{pipeline_key}_highres", f"{pipeline_key}_resolution"],
            "space_group": [f"{pipeline_key}_space_group"],
            "unit_cell": [f"{pipeline_key}_unit_cell"],
            "model": [f"{pipeline_key}_model", f"{pipeline_key}_model_pdb"],
            "model_pdb": [f"{pipeline_key}_model_pdb", f"{pipeline_key}_model"],
            "reference_hkl": [f"{pipeline_key}_reference_hkl"],
            "nproc": [f"{pipeline_key}_nproc"],
            "njobs": [f"{pipeline_key}_njobs"],
            "fast": [f"{pipeline_key}_fast"],
        }
        
        for base_key, targets in common_mappings.items():
            if base_key in job_data:
                for t in targets:
                    if t not in job_data:
                        job_data[t] = job_data[base_key]

        # Parse datasets
        datasets = job_data.get("datasets", [])
        
        # If flat format, convert to list
        if not datasets:
            data_dir = job_data.get("data_dir")
            prefix = job_data.get("prefix")
            # Or explicit master_file if provided
            master_file = job_data.get("master_file") 
            
            if not master_file and data_dir and prefix:
                 master_file = str(Path(data_dir) / f"{prefix}_master.h5")
            
            if master_file:
                datasets.append({
                    "path": master_file,
                    "start": job_data.get("start"),
                    "end": job_data.get("end")
                })

        if not datasets:
            logger.error("External job request has no valid datasets.")
            return True # Handled, but failed

        # Handle merging if requested or implied by multiple datasets
        can_merge = pipeline_key in ["xia2", "autoproc", "xia2_ssx", "xds"]
        
        # We need to read metadata for at least the primary dataset
        primary_ds = datasets[0]
        primary_master = primary_ds["path"]
        
        # Read metadata locally on server
        try:
            from qp2.xio.hdf5_manager import HDF5Reader
            # Don't start timer/monitor, just read params
            reader = HDF5Reader(primary_master, start_timer=False)
            metadata = reader.get_parameters()
            reader.close()
        except Exception as e:
            logger.warning(f"Could not read metadata from {primary_master}: {e}")
            metadata = {}
        
        # If `datasets` has multiple items, and we merge:
        if len(datasets) > 1 and can_merge:
             extra_files = [d["path"] for d in datasets[1:]]
             job_data["extra_data_files"] = extra_files
             
             self.submit_plugin_job(
                 WorkerClass,
                 primary_master,
                 metadata,
                 redis_key,
                 **job_data 
             )
        else:
             # Submit for each dataset
             for ds in datasets:
                 m_file = ds["path"]
                 # Read metadata for this specific file if different
                 if m_file != primary_master:
                     try:
                         from qp2.xio.hdf5_manager import HDF5Reader
                         r = HDF5Reader(m_file, start_timer=False)
                         meta = r.get_parameters()
                         r.close()
                     except Exception:
                         meta = metadata  # Fallback
                 else:
                     meta = metadata
                 
                 ds_kwargs = job_data.copy()
                 # Override frame ranges for this specific dataset if present in its info
                 if ds.get("start") is not None:
                     ds_kwargs["start_frame"] = ds["start"]
                     if f"{pipeline_key}_start" in ds_kwargs: ds_kwargs[f"{pipeline_key}_start"] = ds["start"]
                 if ds.get("end") is not None:
                     ds_kwargs["end_frame"] = ds["end"]
                     if f"{pipeline_key}_end" in ds_kwargs: ds_kwargs[f"{pipeline_key}_end"] = ds["end"]
                 
                 self.submit_plugin_job(
                     WorkerClass,
                     m_file,
                     meta,
                     redis_key,
                     **ds_kwargs
                 )
        
        return True

    # --- LEGACY HANDLERS (Uncommented and integrated) ---

    def _submit_gmcaproc_job(
        self, run_prefix, milestone, opt_base, data_dir, proc_dir_root
    ):
        """Helper to submit a legacy gmcaproc job."""
        if not self.config.get("enable_legacy_pipelines", True):
            return

        try:
            normalized_percent = milestone.replace("%", "")
            job_name_component = "legacy/gmcaproc"

            # Access internal server method to create directory
            proc_dir = self.server._create_processing_directory(
                Path(proc_dir_root),
                run_prefix,
                job_name_component,
                normalized_percent + "pct" if milestone != "completion" else None,
            )

            opt_sub = opt_base.copy()
            opt_sub.update(
                {
                    "prefix": run_prefix,
                    "pipeline": "gmcaproc",
                    "proc_dir": str(proc_dir.resolve()),
                    "data_dir": str(data_dir.resolve()),
                    "program": "process",
                }
            )

            if milestone != "completion":
                opt_sub["percent"] = normalized_percent
                opt_sub["job_tag"] = f"gmcaproc_{normalized_percent}pct"
            else:
                opt_sub["job_tag"] = "gmcaproc_complete"

            logger.info(
                f"AnalysisManager: Submitting legacy gmcaproc job for {run_prefix} ({milestone})"
            )
            xprocess.xprocess(opt_sub, job_tag=opt_sub.get("job_tag"))

        except Exception as e:
            logger.error(f"Failed to submit legacy gmcaproc job: {e}", exc_info=True)

    def handle_legacy_milestone(self, run_prefix, milestone, metadata_list):
        """Handles 25%/50% milestones for gmcaproc."""
        if not metadata_list:
            return
        if not self.config.get("enable_legacy_pipelines", True):
            return
        
        # FIX: Respect only_run_legacy_strategy flag for milestones too
        if self.config.get("only_run_legacy_strategy", False):
            return

        meta = metadata_list[0]
        mode = meta.get("collect_mode", "STANDARD").lower()
        if mode == "raster":
            return

        opt_base = self.server.get_opt(metadata_list)
        data_dir = self.server._get_data_dir_from_metadata(meta)

        if data_dir and data_dir.exists():
            self._submit_gmcaproc_job(
                run_prefix, milestone, opt_base, data_dir, opt_base["proc_root_dir"]
            )

    def handle_legacy_completion(self, run_prefix, master_files, metadata_list):
        """Handles completion for gmcaproc and legacy strategy."""
        if not metadata_list:
            return
        
        # Run if legacy is enabled generally OR specifically for strategy
        if not self.config.get("enable_legacy_pipelines", True) and not self.config.get("only_run_legacy_strategy", False):
            return

        meta = metadata_list[0]
        mode = meta.get("collect_mode", "STANDARD").lower()
        opt_base = self.server.get_opt(metadata_list)

        # ---------------------------------------

        if mode in ["standard", "single"]:
            # If only legacy strategy is requested, skip standard legacy pipelines
            if self.config.get("only_run_legacy_strategy", False):
                logger.info(
                    f"AnalysisManager: Skipping legacy standard pipelines for run '{run_prefix}' (only_run_legacy_strategy is enabled)."
                )
                return

            # --- CHECK: Minimum Frames Threshold ---
            total_images = meta.get("n_images")
            if total_images is not None:
                min_frames = self.config.get("minimum_frames_for_pipelines", 20)
                if total_images < min_frames:
                    logger.info(
                        f"AnalysisManager (Legacy): Skipping gmcaproc/strategy for run '{run_prefix}'. Total frames ({total_images}) < minimum ({min_frames})."
                    )
                    return

            data_dir = self.server._get_data_dir_from_metadata(meta)
            if data_dir and data_dir.exists():
                # 1. gmcaproc
                self._submit_gmcaproc_job(
                    run_prefix,
                    "completion",
                    opt_base,
                    data_dir,
                    opt_base["proc_root_dir"],
                )

                # 2. legacy autoproc
                ap_dir = self.server._create_processing_directory(
                    Path(opt_base["proc_root_dir"]), run_prefix, "legacy/autoproc", "completion"
                )
                self._submit_xprocess_job(opt_base, run_prefix, "autoproc", ap_dir, data_dir)

                # 3. legacy xia2
                x2_dir = self.server._create_processing_directory(
                    Path(opt_base["proc_root_dir"]), run_prefix, "legacy/xia2", "completion"
                )
                self._submit_xprocess_job(opt_base, run_prefix, "xia2", x2_dir, data_dir)

        elif mode == "strategy":

            proc_root = Path(opt_base["proc_root_dir"])
            cbf_dir = self.server._create_processing_directory(
                proc_root, run_prefix, "legacy/cbf_conversion", "strategy"
            )
            cbf_data_dir, cbf_filelist = convert_hdf5_to_cbf_for_strategy(
                run_prefix, master_files, metadata_list, cbf_dir
            )

            if cbf_data_dir and cbf_filelist:
                pipelines = [
                    "mosflm_strategy",
                    "xds_strategy",
                    "dials_strategy",
                    "labelit_strategy",
                ]
                data_dir_for_jobs = Path(cbf_data_dir)

                for p_name in pipelines:
                    p_dir = self.server._create_processing_directory(
                        proc_root, run_prefix, f"legacy/{p_name}", "completion"
                    )
                    try:
                        self._submit_xprocess_job(
                            opt_base,
                            run_prefix,
                            p_name,
                            p_dir,
                            data_dir_for_jobs,
                            file_list=cbf_filelist,
                        )
                    except Exception as e:
                        logger.error(f"Error submitting {p_name}: {e}", exc_info=True)

    def _submit_xprocess_job(
        self,
        opt_base,
        run_prefix,
        pipeline_name,
        proc_dir,
        data_dir,
        milestone_percent=None,
        file_list=None,
    ):
        # Helper for the legacy strategy calls
        opt_sub = opt_base.copy()
        opt_sub.update(
            {
                "prefix": run_prefix,
                "pipeline": pipeline_name,
                "proc_dir": str(proc_dir),
                "data_dir": str(data_dir),
                "program": "strategy" if "strategy" in pipeline_name else "process",
            }
        )
        if file_list:
            opt_sub["filelist"] = file_list
        if milestone_percent:
            opt_sub["percent"] = milestone_percent
        xprocess.xprocess(opt_sub, job_tag=f"{pipeline_name}")
