# qp2/data_proc/server/data_processing_server.py

import argparse
import json
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
import json

from pyqtgraph.Qt import QtCore

MILESTONE_FRAME_THRESHOLD = 200

from qp2.xio.redis_manager import RedisManager
from qp2.xio.hdf5_manager import HDF5Reader
from qp2.xio.hdf5_to_cbf import convert_hdf5_to_cbf_for_strategy
from qp2.data_proc.server import xprocess
from qp2.xio.user_group_manager import UserGroupManager
from qp2.data_proc.config import DATAPROC_SERVER_HTTP_PORT, WEBSOCKET_PORT
from qp2.config.servers import ServerConfig
from qp2.log.logging_config import setup_logging, get_logger

from qp2.data_proc.server.analysis_manager import AnalysisManager
from qp2.data_proc.server.http_server import HTTPServerManager
from qp2.data_proc.server.websocket_server import WebSocketServerManager
from qp2.data_proc.server.xls_reader import xlsReader
from qp2.xio.db_manager import DBManager
import os


logger = get_logger(__name__)


def submit_job(opt: Dict[str, Any]):
    """Helper function to submit jobs via xprocess."""
    logger.debug(f"submit_job: Submitting job with opt: {opt}")
    xprocess.xprocess(opt, job_tag=opt.get("pipeline", "auto"))


def derive_proc_root_dir(full_data_dir: str) -> Path:
    """
    Derives, creates, and returns the root processing directory from a full data directory path.

    This function encapsulates the logic for mapping a data collection directory
    (e.g., .../DATA/user/experiment/collect) to a clean processing root
    (e.g., .../PROCESSING/user/experiment).

    Args:
        full_data_dir: The absolute path to the data collection directory.

    Returns:
        A Path object representing the absolute path to the created processing root directory.
    """
    # Ensure we have a string to work with
    proc_root_dir_str = str(full_data_dir)

    # 1. Map from DATA to PROCESSING (case-insensitive)
    if "/DATA/" in proc_root_dir_str:
        proc_root_dir_str = proc_root_dir_str.replace("/DATA/", "/PROCESSING/", 1)
    elif "/data/" in proc_root_dir_str:
        proc_root_dir_str = proc_root_dir_str.replace("/data/", "/processing/", 1)
    elif "/test-data/" in proc_root_dir_str:
        proc_root_dir_str = proc_root_dir_str.replace("/test-data/", "/processing/", 1)

    # 2. Clean up common collection-specific subdirectories to create a clean project root
    for keyword in ["/collect/", "/raster/", "/screen/"]:
        if keyword in proc_root_dir_str:
            proc_root_dir_str = proc_root_dir_str.replace(keyword, "")

    # 3. Create the directory path
    proc_root_dir_path = Path(proc_root_dir_str)
    proc_root_dir_path.mkdir(parents=True, exist_ok=True)

    # Use logger if available, otherwise basic logging
    logger.info(f"Derived processing root directory: {proc_root_dir_path.resolve()}")

    return proc_root_dir_path.resolve()


class ProcessingServer(QtCore.QObject):
    """
    Standalone server that listens for Redis signals and runs external processing programs
    when data milestones are reached.
    """

    status_update = QtCore.pyqtSignal(str)
    connection_error = QtCore.pyqtSignal(str)

    def __init__(self, enable_db_logging=False, max_workers=4, max_analysis_workers=8, dry_run=False):
        super().__init__()
        self.dry_run = dry_run
        self.status_update.connect(self.log_status)
        self.connection_error.connect(self.log_error)
        
        
        self.enable_db_logging = enable_db_logging
        self.db_manager = None
        if self.enable_db_logging:
            self.db_manager = DBManager(
                status_update_callback=self.status_update.emit,
                error_callback=self.connection_error.emit
            )

        self.redis_manager = RedisManager()
        self.redis_manager.run_started.connect(self.handle_run_started)
        self.redis_manager.new_master_file_stream.connect(self.handle_new_series)
        self.redis_manager.run_progress_25.connect(self.handle_progress_25)
        self.redis_manager.run_progress_50.connect(self.handle_progress_50)
        self.redis_manager.run_completed.connect(self.handle_run_completed)
        self.redis_manager.status_update.connect(self.status_update)
        self.redis_manager.connection_error.connect(self.handle_connection_error)

        # run milestone and completion jobs in a ThreadPoolExecutor
        self.executor = ThreadPoolExecutor(max_workers)
        # run analysis workers use QThreadPool for Qt signal compatibility
        self.worker_pool = QtCore.QThreadPool()
        self.worker_pool.setMaxThreadCount(max_analysis_workers)
        logger.info(
            f"QThreadPool for analysis workers initialized with max {max_analysis_workers} threads."
        )

        self.active_jobs: Dict[str, Dict[str, Any]] = {}
        self.active_jobs_lock = threading.Lock()
        self.running = False
        self.reconnect_timer = QtCore.QTimer()
        self.reconnect_timer.setInterval(10000)
        self.reconnect_timer.timeout.connect(self.attempt_reconnect)

        self.processed_runs_lock = threading.Lock()
        self.processed_runs: Dict[str, Set[str]] = {
            "progress_start": set(),
            "progress_25": set(),
            "progress_50": set(),
            "completed": set(),
        }
        self.run_hdf5_readers: Dict[str, List[HDF5Reader]] = {}
        self.run_master_files: Dict[str, List[str]] = {}
        self.run_metadata_list: Dict[str, List[Dict[str, Any]]] = {}
        self.run_readers_lock = threading.Lock()
        self.analysis_manager = AnalysisManager(self)

        self.run_setup_events: Dict[str, threading.Event] = {}
        self.run_setup_events_lock = threading.Lock()
        self.pending_series_setups: Dict[str, int] = {}
        self.pending_series_setups_lock = threading.Lock()

        self.run_start_times: Dict[str, float] = {}
        self.run_start_times_lock = threading.Lock()
        
        # Track active series to prevent premature cleanup
        self.active_series: Dict[str, Set[str]] = {}
        self.active_series_lock = threading.Lock()

        self.janitor_timer = QtCore.QTimer()
        self.janitor_timer.setInterval(2 * 60 * 60 * 1000)  # Check every 2 hours
        self.janitor_timer.timeout.connect(self.run_janitor)
        self.redis_manager.run_started.connect(self.track_run_start_time)

    def log_status(self, message: str):
        logger.info(message)

    def log_error(self, message: str):
        logger.error(message)

    def start(self):
        if self.running:
            self.status_update.emit("Server is already running.")
            return
        self.running = True
        self.status_update.emit("Starting Processing Server...")
        try:
            if self.redis_manager.start_monitoring():
                self.status_update.emit("Redis monitoring started.")
        except Exception as e:
            self.connection_error.emit(f"Failed to start Redis monitoring: {e}")
            logger.exception("Exception during initial Redis monitoring start.")
            if self.running:
                self.attempt_reconnect()
        self.janitor_timer.start()

    def stop(self):
        if not self.running:
            return
        self.status_update.emit("Stopping Processing Server...")
        self.running = False
        if self.reconnect_timer.isActive():
            self.reconnect_timer.stop()
        self.redis_manager.stop_monitoring()
        if hasattr(self.worker_pool, "clear"):
            self.worker_pool.clear()
        self.worker_pool.waitForDone()  # Waits for running tasks to complete
        self.status_update.emit("Shutting down thread pool...")
        self.executor.shutdown(wait=True)
        self.status_update.emit("Processing Server stopped.")
        self.janitor_timer.stop()

    def launch_job_from_external_request(self, job_data: Dict[str, Any]):
        """
        Launches a processing job based on JSON data from an external request.
        Delegates to AnalysisManager for plugin jobs, or falls back to xprocess.
        """
        thread_name = threading.current_thread().name
        logger.info(f"[{thread_name}] Received external request to launch job.")
        logger.debug(f"[{thread_name}] Job data: {json.dumps(job_data, indent=2)}")

        try:
            if not isinstance(job_data, dict):
                logger.error(
                    f"[{thread_name}] External job data must be a dictionary. Received type: {type(job_data)}"
                )
                return

            if self.dry_run:
                job_data["dry_run"] = True

            # 1. Try plugin-based submission via AnalysisManager
            if self.analysis_manager.handle_external_job_request(job_data):
                logger.info(
                    f"[{thread_name}] Successfully submitted plugin job via AnalysisManager for pipeline: {job_data.get('pipeline')}"
                )
                return

            # 2. Fallback to legacy xprocess submission
            logger.info(f"[{thread_name}] Pipeline not handled by AnalysisManager. Attempting legacy xprocess submission.")

            required_keys = {
                "proc_dir": str,
                "data_dir": str,
                "beamline": str,
                "pipeline": str,
            }
            missing_keys = [key for key in required_keys if key not in job_data]
            if missing_keys:
                logger.error(
                    f"[{thread_name}] External job data missing critical keys for legacy submission: {missing_keys}. Job submission aborted."
                )
                return

            type_errors = []
            for key, expected_type in required_keys.items():
                if not isinstance(job_data.get(key), expected_type):
                    type_errors.append(
                        f"Key '{key}' must be a {expected_type.__name__}."
                    )

            if type_errors:
                logger.error(
                    f"[{thread_name}] External job data has type errors: {type_errors}. Job submission aborted."
                )
                return

            logger.info(
                f"[{thread_name}] Submitting job via xprocess. Pipeline: {job_data.get('pipeline', 'N/A')}"
            )
            submit_job(job_data)
            logger.info(
                f"[{thread_name}] Successfully submitted job from external request for pipeline: {job_data.get('pipeline', 'N/A')}"
            )

        except Exception as e:
            logger.exception(
                f"[{thread_name}] Failed to launch job from external request. Data: {job_data}"
            )

    def handle_connection_error(self, error_message: str):
        logger.error(f"Received connection error from RedisManager: {error_message}")
        if self.running and not self.reconnect_timer.isActive():
            logger.info("Scheduling Redis reconnection attempt.")
            self.reconnect_timer.start()

    def attempt_reconnect(self):
        if not self.running:
            self.reconnect_timer.stop()
            return
        logger.info("Attempting to reconnect to Redis...")
        try:
            self.redis_manager.stop_monitoring()
            self.redis_manager.start_monitoring()
            logger.info("Successfully reconnected to Redis and restarted monitoring.")
            self.reconnect_timer.stop()
        except Exception as e:
            logger.error(f"Failed to reconnect to Redis: {e}")

    def get_and_create_proc_dir_for_analysis(
        self,
        master_file: str,
        pipeline_name: str,
        milestone_suffix_str: Optional[str] = None,
        series_subdir: Optional[str] = None,
        run_prefix_override: Optional[str] = None,
    ) -> Optional[Path]:
        """
        A public helper to calculate and create the processing directory for an
        analysis pipeline based solely on the master file path.
        """
        try:
            master_path = Path(master_file)
            if run_prefix_override:
                run_prefix = run_prefix_override
            else:
                run_prefix = master_path.stem.replace("_master", "")
            full_data_dir = str(master_path.parent)

            proc_root_dir = derive_proc_root_dir(full_data_dir)

            # Dozor requires a unique directory for every run/batch as it cannot handle
            # concurrent execution in the same directory.
            if pipeline_name == "dozor":
                import uuid
                # Use a random suffix to ensure uniqueness and avoid the "runN" collision loop
                unique_suffix = uuid.uuid4().hex[:6]
                pipeline_name = f"dozor_jobs/dozor_{unique_suffix}"

            # Use the internal helper to create the final, unique subdirectory.
            proc_dir = self._create_processing_directory(
                proc_root_dir,
                run_prefix,
                pipeline_name,
                milestone_suffix_str=milestone_suffix_str,
                series_subdir=series_subdir,
            )
            return proc_dir

        except Exception as e:
            logger.error(
                f"Failed to create processing directory for pipeline '{pipeline_name}' from master file '{master_file}'. Error: {e}",
                exc_info=True,
            )
            return None

    def _create_processing_directory(
        self,
        proc_root_dir: Path,
        run_prefix: str,
        job_name_component: str,
        milestone_suffix_str: Optional[str] = None,
        series_subdir: Optional[str] = None,
    ) -> Path:
        
        logger.debug(f"proc_root_dir: {proc_root_dir}")
        run_dir = proc_root_dir / run_prefix
        
        if series_subdir:
            work_root = run_dir / series_subdir
        else:
            work_root = run_dir

        if milestone_suffix_str and milestone_suffix_str not in ["start"]:
            subdir_name = f"{job_name_component}_{milestone_suffix_str}"
        else:
            subdir_name = job_name_component

        proposed_path = work_root / subdir_name
        final_path = proposed_path
        run_number = 1

        while final_path.exists():
            final_path = work_root / f"{subdir_name}_run{run_number}"
            run_number += 1
            if run_number > 100:
                raise OSError(
                    "Exceeded 100 attempts to create a unique processing directory."
                )

        try:
            final_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured processing directory exists: {final_path.resolve()}")
            return final_path.resolve()
        except Exception as e:
            logger.exception(f"Failed to create processing directory {final_path}")
            raise

    def get_opt(self, metadata_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not metadata_list:
            raise ValueError("Metadata list cannot be empty for get_opt.")

        opt = metadata_list[0].copy()
        data_dir_root_str = opt.get("data_dir_root")
        data_rel_dir = opt.get("data_rel_dir")

        if not data_dir_root_str or data_rel_dir is None:
            raise ValueError("Essential directory information missing in metadata.")

        data_dir_root_path = Path(data_dir_root_str)
        full_data_dir = str(data_dir_root_path / data_rel_dir.lstrip("/\\"))
        proc_root_dir_path = derive_proc_root_dir(full_data_dir)
        opt["proc_root_dir"] = str(proc_root_dir_path)

        bluice_srv = None
        try:
            bluice_srv = self.redis_manager.get_bluice_connection()
            if not opt.get("robot_mounted"):
                opt["robot_mounted"] = self.redis_manager.get_robot_mounted()

            beamline_name = (
                self.redis_manager.get_beamline_name() or ""
            ) or opt.get("beamline_config_name", "")
            bl = (
                "23i"
                if beamline_name.lower().endswith("d")
                else ("23o" if beamline_name.lower().endswith("b") else "23b")
            )

            username = opt.get("username") or (
                self.redis_manager.get_beamline_user() or "unknown_user"
            )
            spreadsheet_rel = opt.get("spreadsheet_input_rel") or (
                self.redis_manager.get_spreadsheet_rel()
            )

            spreadsheet_path = (
                data_dir_root_path / spreadsheet_rel if spreadsheet_rel else None
            )
            spreadsheet = (
                str(spreadsheet_path)
                if spreadsheet_path and spreadsheet_path.is_file()
                else None
            )

            pi_id, esaf_id, groupname = 0, 0, username
            
            # Localize change for testing: Skip DB lookup in test environment or if it fails
            if ServerConfig.is_test_env():
                logger.info(f"Test Env: Bypassing UserGroupManager for user '{username}'.")
            else:
                try:
                    user_group_mgr = UserGroupManager()
                    group_info = user_group_mgr.groupinfo_from_groupname(username)
                    if group_info:
                        # group_info is a dict from fetchone() or similar depending on version
                        # The code previously assumed a list [0], let's be robust
                        info = group_info[0] if isinstance(group_info, list) else group_info
                        pi_id = info.get("pi_badge", 0)
                        esaf_id = info.get("esaf_number", 0)
                        groupname = info.get("group_name", username)
                    else:
                        logger.debug(f"No group info found for user '{username}', using defaults")
                except ConnectionError as e:
                    logger.warning(f"Database connection error while retrieving group info for '{username}': {e}. Using defaults.")
                except KeyError as e:
                    logger.warning(f"Missing expected field in group info for '{username}': {e}. Using defaults.")
                except Exception as e:
                    logger.warning(f"Could not retrieve group info for '{username}': {e}. Using defaults.", exc_info=True)

            run_prefix = opt.get("run_prefix")
            series_prefix = opt.get("prefix")
            sample_id = (
                run_prefix
                if (len(metadata_list) > 1 and run_prefix)
                else (series_prefix or "unknown_sample")
            )

            opt.update(
                {
                    "sample_id": sample_id,
                    "prefix": series_prefix,
                    "run_prefix_from_meta": run_prefix,
                    "redis_key": f"bluice:strategy:table#{data_rel_dir}:{sample_id}",
                    "redis_manager": self.redis_manager,
                    "beamline": bl,
                    "username": username,
                    "groupname": groupname,
                    "spreadsheet": spreadsheet,
                    "pi_id": pi_id,
                    "esaf_id": esaf_id,
                }
            )

            # Save collection parameters from bluice Redis to analysis Redis.
            # This captures raster scan mode, cell size, beam size, and
            # attenuation at collection time so downstream tools (image
            # viewer) can read them without bluice access.
            if run_prefix and groupname:
                try:
                    from qp2.config.redis_keys import AnalysisRedisKeys

                    collection_params = self.redis_manager.get_raster_params(
                        run_prefix
                    )
                    opt["_bluice_collection_params"] = collection_params
                    if collection_params:
                        analysis_srv = self.redis_manager.get_analysis_connection()
                        if analysis_srv:
                            params_key = AnalysisRedisKeys.collection_params_key(
                                groupname, run_prefix
                            )
                            analysis_srv.hset(params_key, mapping={
                                k: str(v) for k, v in collection_params.items()
                            })
                            analysis_srv.expire(params_key, 30 * 24 * 3600)  # 30 days
                            logger.info(
                                f"Saved collection params to {params_key}: "
                                f"{collection_params}"
                            )
                except Exception as e:
                    logger.debug(
                        f"Could not save collection params: {e}"
                    )

            try:
                spreadsheet_path = opt.get("spreadsheet")
                mounted_crystal = opt.get("robot_mounted")

                if (
                    spreadsheet_path
                    and mounted_crystal
                    and os.path.exists(spreadsheet_path)
                ):
                    logger.debug(f"Reading spreadsheet: {spreadsheet_path} for crystal: {mounted_crystal}")
                    xls = xlsReader(spreadsheet_path)

                    updates = {
                        "model_pdb": xls.get_model_path(mounted_crystal),
                        "sequence": xls.get_sequence_path(mounted_crystal),
                        "space_group": xls.get_space_group(mounted_crystal),
                        "unit_cell": xls.get_unit_cell(mounted_crystal),
                        "reference_hkl": xls.get_reference_dataset(mounted_crystal),
                        "heavy_atom": xls.get_metal(mounted_crystal),
                        "nmol": xls.get_nmol(mounted_crystal),
                    }

                    for k, v in updates.items():
                        if v:
                            opt[k] = v

                    if updates.get("heavy_atom"):
                        opt["native"] = False
                    
                    # Store full spreadsheet row as JSON
                    row_df = xls.get_row(mounted_crystal)
                    if not row_df.empty:
                        # Convert to dict, then JSON string. 
                        # fillna("") in xlsReader should handle NaNs, but to_dict might need care if types vary
                        row_dict = row_df.to_dict(orient='records')[0]
                        opt["meta_user"] = json.dumps(row_dict, default=str)
                else:
                    if spreadsheet_path and not os.path.exists(spreadsheet_path):
                        logger.warning(f"Spreadsheet file not found: {spreadsheet_path}")
                    elif not mounted_crystal:
                        logger.debug("No mounted crystal specified, skipping spreadsheet lookup")

            except FileNotFoundError as e:
                logger.warning(f"Spreadsheet file not found: {e}. Continuing without spreadsheet data.")
            except PermissionError as e:
                logger.warning(f"Permission denied reading spreadsheet: {e}. Continuing without spreadsheet data.")
            except Exception as e:
                logger.error(f"Error reading spreadsheet: {e}. Continuing without spreadsheet data.", exc_info=True)

            try:
                # Apply user overrides from Image Viewer settings (group-scoped)
                analysis_srv = self.redis_manager.get_analysis_connection()
                if analysis_srv:
                    from qp2.config.redis_keys import AnalysisRedisKeys

                    # 1. Processing Common Parameters Overrides (Space Group, Res Cutoff, etc)
                    #    Try group-scoped key first, fall back to global key
                    key = AnalysisRedisKeys.scoped_processing_overrides(groupname)
                    overrides = analysis_srv.hgetall(key)
                    if not overrides:
                        key = AnalysisRedisKeys.KEY_PROCESSING_OVERRIDES
                        overrides = analysis_srv.hgetall(key)
                    if overrides:
                        logger.info(f"Applying user processing overrides from Redis key '{key}': {overrides}")
                        if overrides.get("space_group"):
                            opt["space_group"] = overrides.get("space_group")
                        if overrides.get("unit_cell"):
                            opt["unit_cell"] = overrides.get("unit_cell")
                        if overrides.get("model_pdb"):
                            opt["model_pdb"] = overrides.get("model_pdb")
                        if overrides.get("res_cutoff_low"):
                            opt["processing_common_res_cutoff_low"] = float(overrides.get("res_cutoff_low"))
                        if overrides.get("res_cutoff_high"):
                            opt["processing_common_res_cutoff_high"] = float(overrides.get("res_cutoff_high"))
                        if overrides.get("native") is not None:
                            opt["native"] = overrides.get("native").lower() == "true"
                        if overrides.get("proc_dir_root"):
                            opt["proc_root_dir"] = overrides.get("proc_dir_root")
                            logger.info(f"User override: proc_root_dir = {opt['proc_root_dir']}")

                    # 2. Pipeline Enable/Disable Overrides by Collection Mode
                    #    Try group-scoped key first, fall back to global key
                    collect_mode = opt.get("collect_mode", "STANDARD").upper()

                    key_by_mode = AnalysisRedisKeys.scoped_pipelines_by_mode(groupname)
                    user_mappings_str = analysis_srv.get(key_by_mode)
                    if not user_mappings_str:
                        key_by_mode = AnalysisRedisKeys.KEY_PROCESSING_OVERRIDES_BY_MODE
                        user_mappings_str = analysis_srv.get(key_by_mode)
                    
                    pipelines_mapping = {}
                    if user_mappings_str:
                         try:
                             pipelines_mapping = json.loads(user_mappings_str)
                             logger.debug("Loaded pipeline mappings from Redis")
                         except Exception as e:
                             logger.warning(f"Failed to parse user pipeline overrides from Redis: {e}")
                    
                    if not pipelines_mapping:
                         # Fall back to server config JSON
                         from qp2.data_proc.server.analysis_manager import AnalysisManager
                         # Note: using a hack to get the config, but we can load it here safely or AnalysisManager will read it
                         config_path = Path(__file__).parent / "analysis_config.json"
                         try:
                             with open(config_path, "r") as f:
                                 config_data = json.load(f)
                                 pipelines_mapping = config_data.get("default_pipelines_by_mode", {})
                             logger.debug(f"Loaded default pipeline mappings from config for mode {collect_mode}")
                         except Exception as e:
                             logger.warning(f"Failed to load analysis_config defaults: {e}")
                             # absolute fallback
                             pipelines_mapping = {
                                  "STANDARD": ["xds", "xia2", "autoproc", "dozor"],
                                  "VECTOR": ["xds", "xia2", "autoproc", "dozor"],
                                  "SINGLE": ["xds", "xia2", "autoproc", "dozor"],
                                  "SITE": ["xds", "xia2", "autoproc", "dozor"],
                                  "RASTER": ["nxds", "dozor"],
                                  "STRATEGY": ["xds_strategy", "mosflm_strategy", "dozor"]
                             }
                    
                    # Resolve active pipelines for current mode
                    active_pipelines_for_mode = pipelines_mapping.get(collect_mode, [])
                    logger.info(f"Pipelines enabled for mode {collect_mode}: {active_pipelines_for_mode}")
                    
                    # Inject enable flags into opt for AnalysisManager to consume
                    all_known_pipelines = ["xds", "nxds", "xia2", "autoproc", "xia2_ssx", "crystfel", "xds_strategy", "mosflm_strategy", "dozor"]
                    for pipe in all_known_pipelines:
                         opt[f"enable_{pipe}"] = (pipe in active_pipelines_for_mode)
                         
            except Exception as e:
                logger.error(f"Error fetching user processing overrides: {e}", exc_info=True)

            return opt
        finally:
            if bluice_srv:
                self.redis_manager.bluice_connection_manager.close_bluice_connection()

    def _get_data_dir_from_metadata(
        self,
        first_series_metadata: Dict[str, Any],
    ) -> Optional[Path]:
        data_dir_root = first_series_metadata.get("data_dir_root")
        data_rel_dir = first_series_metadata.get("data_rel_dir")
        if not data_dir_root or data_rel_dir is None:
            logger.error(
                f"Could not determine data directory from metadata: {first_series_metadata}"
            )
            return None
        return Path(data_dir_root) / data_rel_dir.lstrip("/\\")

    def process_completion(
        self,
        run_prefix: str,
        accumulated_frames: int,
        total_frames: int,
        master_files: List[str],
        metadata_list: List[Dict[str, Any]],
    ):

        job_id = f"{run_prefix}_completion_phase"
        with self.active_jobs_lock:
            self.active_jobs[job_id] = {"status": "starting", "start_time": time.time()}

        try:
            # Wait for the setup-complete event before proceeding.
            with self.run_setup_events_lock:
                setup_event = self.run_setup_events.get(run_prefix)

            if setup_event:
                logger.info(
                    f"Completion for '{run_prefix}' is waiting for setup to finish..."
                )
                # Wait for up to 120 seconds. This should be more than enough.
                is_set = setup_event.wait(timeout=120.0)
                if not is_set:
                    # BUGFIX: Better error reporting on timeout
                    with self.run_readers_lock:
                        expected_readers = len(self.run_master_files.get(run_prefix, []))
                        actual_readers = len(self.run_hdf5_readers.get(run_prefix, []))
                    
                    error_msg = (
                        f"Timed out waiting for HDF5 watcher setup for run '{run_prefix}'. "
                        f"Expected {expected_readers} readers, got {actual_readers}. "
                        f"This usually indicates master files are missing or inaccessible."
                    )
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                logger.info(
                    f"Setup for '{run_prefix}' is finished. Proceeding with completion."
                )
            else:
                logger.warning(
                    f"No setup event found for run '{run_prefix}'. Proceeding without waiting."
                )

            if not metadata_list or total_frames <= 0:
                raise ValueError("Invalid metadata or total_frames for completion.")

            # --- RESTORED WAIT LOGIC ---
            if not self.wait_for_required_files(run_prefix, total_frames):
                raise ValueError("Not all required data files found for completion.")
            # ---------------------------

            # --- NEW: ACTIVE SERIES WAIT LOGIC ---
            # Wait for all individual series to signal completion before proceeding.
            # This ensures that per-series logic (like XDS single-file processing) 
            # has finished and emitted its signals before we clean up readers.
            wait_start = time.time()
            max_series_wait = 60.0
            active_count = -1
            while time.time() - wait_start < max_series_wait:
                with self.active_series_lock:
                    active_count = len(self.active_series.get(run_prefix, []))
                
                if active_count == 0:
                    logger.info(f"All series for run '{run_prefix}' have completed. Proceeding with run completion.")
                    break
                
                if time.time() - wait_start > 5.0 and (time.time() - wait_start) % 5.0 < 0.2:
                     logger.info(f"Waiting for {active_count} active series to complete for run '{run_prefix}'...")
                     
                time.sleep(0.2)
            else:
                logger.warning(
                    f"Timed out waiting for {active_count} series to complete for run '{run_prefix}' after {max_series_wait}s. "
                    "Proceeding with run completion, but some per-series jobs might be skipped."
                )
            # -------------------------------------

            self.analysis_manager.handle_run_completion_logic(
                run_prefix, master_files, metadata_list
            )

            self.analysis_manager.handle_legacy_completion(
                run_prefix, master_files, metadata_list
            )

            with self.processed_runs_lock:
                self.processed_runs["completed"].add(run_prefix)
            logger.info(f"Successfully processed completion for run '{run_prefix}'.")
        except Exception as e:
            logger.exception(f"Error processing completion for run '{run_prefix}'")
            with self.active_jobs_lock:
                if job_id in self.active_jobs:
                    self.active_jobs[job_id].update(
                        {"status": "failed", "error": str(e)}
                    )
        finally:
            with self.active_jobs_lock:
                if job_id in self.active_jobs:
                    self.active_jobs[job_id]["end_time"] = time.time()

    @QtCore.pyqtSlot(str, int, dict)
    def handle_series_completed(
        self,
        master_file: str,
        total_frames: int,
        metadata: dict,
    ):
        run_prefix = metadata.get("run_prefix", "unknown_run")
        
        # Remove from active series tracking
        with self.active_series_lock:
            if run_prefix in self.active_series:
                self.active_series[run_prefix].discard(master_file)
                logger.debug(f"Removed '{Path(master_file).name}' from active series for run '{run_prefix}'")

        logger.info(f"SERIES COMPLETED: Run '{run_prefix}' ({Path(master_file).name})")

        # DELEGATION: Analysis Manager determines what to run per series
        # self.analysis_manager.handle_series_completion_logic(
        #     master_file, total_frames, metadata
        # )

    @QtCore.pyqtSlot(str, dict)
    def handle_new_series(self, master_file: str, metadata: dict):
        """
        Handles the detection of a new series by creating and starting its HDF5Reader
        and storing its crystallographic data in Redis.
        """
        run_prefix = metadata.get("run_prefix", "unknown_run")
        series_prefix = metadata.get("prefix", "unknown_series")
        
        # Add to active series tracking
        with self.active_series_lock:
            if run_prefix not in self.active_series:
                self.active_series[run_prefix] = set()
            self.active_series[run_prefix].add(master_file)

        logger.info(
            "======================================================================"
        )
        logger.info(f"SERIES START: '{series_prefix}' (Part of Run: '{run_prefix}')")
        logger.info(f"Master File: {master_file}")

        # Store crystal data as soon as a new series is detected
        self.executor.submit(self._store_crystal_data_in_redis, master_file, metadata)

        # --- UPDATE: Run Master Files and Database ---
        with self.run_readers_lock:
            if run_prefix not in self.run_master_files:
                self.run_master_files[run_prefix] = []
            if run_prefix not in self.run_metadata_list:
                 self.run_metadata_list[run_prefix] = []

            # Check if this master file is already known
            if master_file not in self.run_master_files[run_prefix]:
                self.run_master_files[run_prefix].append(master_file)
                self.run_master_files[run_prefix].sort()
                
                # Append metadata for this series
                # We should ensure we don't duplicate metadata if we somehow get called twice for same file
                # But since we check master_file existence above, we are safe to append here.
                self.run_metadata_list[run_prefix].append(metadata)

                # Trigger DB update with FULL lists
                if self.enable_db_logging:
                     self.executor.submit(
                        self.analysis_manager.update_dataset_run_with_new_series,
                        run_prefix,
                        list(self.run_master_files[run_prefix]),
                        list(self.run_metadata_list[run_prefix])
                    )

        reader = None
        try:
            # The HDF5Reader's constructor already includes wait-for-file logic.
            reader = HDF5Reader(
                master_file,
                metadata,
                start_timer=True,
            )

            reader.series_completed.connect(
                self.analysis_manager.handle_series_completed
            )
            # Connect to server handler to clear active series state
            reader.series_completed.connect(
                self.handle_series_completed
            )

            # Connect per-file/segment pipelines (e.g., Dozor)
            # We explicitly connect Dozor as requested.
            reader.data_files_ready_batch.connect(
                lambda files: self.analysis_manager.handle_data_files_ready(files, "dozor")
            )

            # Store the reader so we can clean it up later.
            with self.run_readers_lock:
                if run_prefix not in self.run_hdf5_readers:
                    self.run_hdf5_readers[run_prefix] = []
                self.run_hdf5_readers[run_prefix].append(reader)
                reader = None  # Successfully tracked, don't clean up in except block

        except Exception as e:
            logger.error(
                f"Failed to initialize HDF5Reader for {master_file}: {e}", exc_info=True
            )
            # BUGFIX: Clean up reader if it was created but not successfully tracked
            if reader is not None:
                logger.info(f"Cleaning up HDF5Reader for {master_file} after initialization failure")
                try:
                    reader.close()
                except Exception as cleanup_error:
                    logger.error(f"Error closing HDF5Reader during cleanup: {cleanup_error}")
        finally:
            with self.pending_series_setups_lock:
                if run_prefix in self.pending_series_setups:
                    self.pending_series_setups[run_prefix] -= 1
                    logger.debug(
                        f"Run '{run_prefix}': {self.pending_series_setups[run_prefix]} series setups remaining."
                    )
                    if self.pending_series_setups[run_prefix] <= 0:
                        logger.info(
                            f"All HDF5 readers for run '{run_prefix}' have been initialized."
                        )
                        with self.run_setup_events_lock:
                            if run_prefix in self.run_setup_events:
                                self.run_setup_events[run_prefix].set()
                        self.pending_series_setups.pop(run_prefix, None)

    def _store_crystal_data_in_redis(self, master_file: str, metadata: dict):
        """
        Fetches crystal data using get_opt and stores it in a Redis hash.
        """
        max_retries = 3
        retry_delay = 1.0  # Start with 1 second
        
        for attempt in range(max_retries):
            try:
                opts = self.get_opt([metadata])
                crystal_data = {
                    "space_group": opts.get("space_group"),
                    "unit_cell": opts.get("unit_cell"),
                    "model_pdb": opts.get("model_pdb"),
                    "reference_hkl": opts.get("reference_hkl"),
                    "sequence": opts.get("sequence"),
                    "heavy_atom": opts.get("heavy_atom"),
                    "nmol": opts.get("nmol"),
                }
                # Filter out None values to keep the hash clean
                crystal_data_to_store = {k: v for k, v in crystal_data.items() if v is not None}

                if not crystal_data_to_store:
                    logger.info(f"No crystal data found in spreadsheet for {Path(master_file).name}.")
                    return

                redis_conn = self.redis_manager.get_analysis_connection()
                if redis_conn:
                    redis_key = f"dataset:info:{master_file}"
                    redis_conn.hset(name=redis_key, mapping=crystal_data_to_store)
                    redis_conn.expire(redis_key, 30 * 24 * 3600)  # 1-month expiration (approx)
                    logger.info(f"Stored crystal data for {Path(master_file).name} in Redis key '{redis_key}'.")
                    return  # Success, exit retry loop
                else:
                    raise ConnectionError("Could not get Redis connection")

            except ConnectionError as e:
                # BUGFIX: Retry on connection errors
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Redis connection error storing crystal data for {master_file} "
                        f"(attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(
                        f"Failed to store crystal data in Redis after {max_retries} attempts for {master_file}: {e}",
                        exc_info=True
                    )
            except Exception as e:
                logger.error(f"Failed to store crystal data for {master_file}: {e}", exc_info=True)
                return  # Don't retry on non-connection errors


    def handle_run_started(
        self,
        run_prefix: str,
        acc_frames: int,
        total_frames: int,
        m_files: List[str],
        m_list: List[Dict[str, Any]],
    ):
        logger.info(
            "######################################################################"
        )
        logger.info(f"RUN START: '{run_prefix}'")
        logger.info(
            f"Total Expected Frames: {total_frames}, Total Series: {len(m_files)}"
        )
        logger.info(
            "######################################################################"
        )

        # NOTE: Do NOT update run_master_files here.
        # handle_new_series is the sole, incremental writer to run_master_files.
        # Touching the list here risks overwriting accumulated state.

        with self.processed_runs_lock:
            if run_prefix in self.processed_runs["completed"]:
                logger.info(
                    f"Clearing previous completed state for new run '{run_prefix}'."
                )
                for key in self.processed_runs:
                    self.processed_runs[key].discard(run_prefix)

                # Clear stale file lists from the previous run
                with self.run_readers_lock:
                    self.run_master_files.pop(run_prefix, None)
                    self.run_metadata_list.pop(run_prefix, None)

                cleared_count = self.analysis_manager.clear_segments_for_run(run_prefix)
                if cleared_count > 0:
                    logger.info(
                        f"Cleared {cleared_count} cached analysis segments for re-run of '{run_prefix}'."
                    )

            if run_prefix in self.processed_runs["progress_start"]:
                logger.warning(
                    f"Duplicate run_started signal for '{run_prefix}'. Ignoring."
                )
                return

            self.processed_runs["progress_start"].add(run_prefix)

        with self.run_setup_events_lock:
            self.run_setup_events[run_prefix] = threading.Event()

        # Ensure DatasetRun exists in DB for pipelines to link to
        if self.enable_db_logging:
            self.executor.submit(self.analysis_manager.handle_run_start_logic, run_prefix, total_frames, m_files, m_list)

        with self.pending_series_setups_lock:
            # Check for already initialized readers (race condition fix)
            with self.run_readers_lock:
                existing_readers = len(self.run_hdf5_readers.get(run_prefix, []))

            expected = len(m_files)
            remaining = expected - existing_readers
            
            logger.info(
                f"Run '{run_prefix}' started. Expecting {expected} series. {existing_readers} already active. Remaining: {remaining}."
            )

            if remaining <= 0:
                self.run_setup_events[run_prefix].set()
            else:
                self.pending_series_setups[run_prefix] = remaining

        logger.info(f"Tracking started for run '{run_prefix}'.")

    def handle_progress_25(
        self,
        run_prefix: str,
        acc_frames: int,
        total_frames: int,
        m_files: List[str],
        m_list: List[Dict[str, Any]],
    ):
        logger.info(f"25% progress for run '{run_prefix}'. ")
        if total_frames <= MILESTONE_FRAME_THRESHOLD:
            logger.info(
                f"Skipping 25% milestone processing for run '{run_prefix}' "
                f"because total frames ({total_frames}) is less than threshold ({MILESTONE_FRAME_THRESHOLD}). "
            )
            return
        with self.processed_runs_lock:
            stage_key = "progress_25"
            if run_prefix in self.processed_runs[stage_key]:
                return
            # Mark early to avoid race
            self.processed_runs[stage_key].add(run_prefix)
        self.executor.submit(
            self.process_milestone,
            run_prefix,
            "25%",
            acc_frames,
            total_frames,
            m_files,
            m_list,
            "gmca_quick",
        )

    def handle_progress_50(
        self,
        run_prefix: str,
        acc_frames: int,
        total_frames: int,
        m_files: List[str],
        m_list: List[Dict[str, Any]],
    ):
        logger.info(f"50% progress for run '{run_prefix}'. ")
        if total_frames < MILESTONE_FRAME_THRESHOLD:
            logger.info(
                f"Skipping 50% milestone processing for run '{run_prefix}' "
                f"because total frames ({total_frames}) is less than threshold ({MILESTONE_FRAME_THRESHOLD}). "
            )
            return
        with self.processed_runs_lock:
            stage_key = "progress_50"
            if run_prefix in self.processed_runs[stage_key]:
                return
            # Mark early to avoid race
            self.processed_runs[stage_key].add(run_prefix)

        self.executor.submit(
            self.process_milestone,
            run_prefix,
            "50%",
            acc_frames,
            total_frames,
            m_files,
            m_list,
            "gmca_mid",
        )

    def handle_run_completed(
        self,
        run_prefix: str,
        acc_frames: int,
        total_frames: int,
        m_files: List[str],
        m_list: List[Dict[str, Any]],
    ):
        logger.info(f"RUN COMPLETE: '{run_prefix}'. Finalizing processing.")
        logger.info(
            "######################################################################"
        )

        with self.processed_runs_lock:
            if run_prefix in self.processed_runs["completed"]:
                logger.warning(
                    f"Ignoring duplicate completion signal for run '{run_prefix}'."
                )
                return
            # BUGFIX: Move add inside lock to make check-and-add atomic
            self.processed_runs["completed"].add(run_prefix)

        def process_and_then_cleanup():
            logger.debug(f"Processing completion for run '{run_prefix}'.")
            try:
                self.process_completion(
                    run_prefix,
                    acc_frames,
                    total_frames,
                    m_files,
                    m_list,
                )
            finally:
                self.cleanup_hdf5_watchers_for_run(run_prefix)
                # This state clearing is CRITICAL for the raster runs to work sequentially.
                with self.processed_runs_lock:
                    logger.info(
                        f"Clearing processed state for completed run '{run_prefix}'."
                    )
                    for key in self.processed_runs:
                        self.processed_runs[key].discard(run_prefix)
                with self.run_setup_events_lock:
                    self.run_setup_events.pop(run_prefix, None)
                with self.run_start_times_lock:
                    self.run_start_times.pop(run_prefix, None)
                with self.active_series_lock:
                    self.active_series.pop(run_prefix, None)

        self.executor.submit(process_and_then_cleanup)



    def wait_for_required_files(
        self,
        run_prefix: str,
        required_frame_count: int,
    ) -> bool:
        """
        Waits for all HDF5 data files necessary to contain up to `required_frame_count` frames.

        This function correctly handles multi-series runs and both partial (milestone)
        and complete (100%) file checks.

        Args:
            run_prefix: The prefix of the run to check.
            required_frame_count: The number of frames that must be available.
                                  For a completion job, this is the total_frames.
                                  For a milestone, it's the accumulated_frames at that point.
        """
        if required_frame_count <= 0:
            return True

        # BUGFIX: Copy readers while holding lock to prevent TOCTOU
        with self.run_readers_lock:
            series_readers = self.run_hdf5_readers.get(run_prefix)
            if not series_readers:
                logger.warning(
                    f"wait_for_required_files called for run '{run_prefix}', but no HDF5 readers were found."
                )
                return False
            # Make a copy to work with outside the lock
            series_readers = series_readers.copy()

        max_wait = 300
        interval = ServerConfig.DATA_POLL_INTERVAL_SEC
        start_time = time.time()

        # 1. Build a complete, sorted map of all file segments from all series.
        #    The `HDF5Reader`'s frame_map is a list of (start_idx, end_idx, fpath, dset).
        #    The start_idx should be globally unique across a run if data collection is sane.
        all_file_segments = sorted(
            [item for reader in series_readers for item in reader.frame_map],
            key=lambda x: x[0],  # Sort by the starting frame index
        )

        if not all_file_segments:
            logger.warning(
                f"No expected data files found in readers for run '{run_prefix}'."
            )
            return False

        # 2. Determine the subset of files needed to satisfy `required_frame_count`.
        files_to_check: List[Path] = []
        frames_covered = 0
        for start_idx, end_idx, fpath, _ in all_file_segments:
            files_to_check.append(Path(fpath))
            # Sum the number of frames in this segment rather than relying on absolute end index
            # This supports both global indexing and series-relative (reset) indexing.
            frames_covered += (end_idx - start_idx)
            if frames_covered >= required_frame_count:
                break  # We have now gathered all the files needed for this milestone.

        total_files_needed = len(files_to_check)
        logger.info(
            f"Run '{run_prefix}': Waiting for {total_files_needed} data files to cover {required_frame_count} frames."
        )

        # 3. Wait for that specific subset of files to exist.
        missing: List[Path] = []
        while time.time() - start_time < max_wait:
            missing = [f for f in files_to_check if not f.exists()]

            if not missing:
                logger.info(
                    f"Run '{run_prefix}': All {total_files_needed} required data files are available."
                )
                return True

            logger.info(
                f"Run '{run_prefix}': Waiting for {len(missing)} of {total_files_needed} files. Example: {missing[0].name}"
            )
            time.sleep(interval)

        logger.error(
            f"Run '{run_prefix}': Timeout waiting for data files. {len(missing)} files still missing."
        )
        return False

    def process_milestone(
        self,
        run_prefix: str,
        milestone: str,
        accumulated_frames: int,
        total_frames: int,
        master_files: List[str],
        metadata_list: List[Dict[str, Any]],
        job_name_component: str,
    ):

        job_id = f"{run_prefix}_{milestone.replace('%','pct')}"
        with self.active_jobs_lock:
            self.active_jobs[job_id] = {"status": "starting", "start_time": time.time()}
        try:
            # Milestones must also wait for the per-series setup to finish enough to cover frames.
            max_wait_sec = 30
            poll_interval_sec = 0.5
            wait_start_time = time.time()
            readers_are_ready = False

            while time.time() - wait_start_time < max_wait_sec:
                with self.run_readers_lock:
                    series_readers = self.run_hdf5_readers.get(run_prefix, [])
                    all_available_segments = sorted(
                        [
                            item
                            for reader in series_readers
                            for item in reader.frame_map
                        ],
                        key=lambda x: x[0],
                    )

                    if all_available_segments:
                        # Calculate total frames covered by summing the length of all segments
                        # This supports both global indexing and series-relative (reset) indexing.
                        frames_covered_by_setup = sum(end - start for start, end, _, _ in all_available_segments)
                        if frames_covered_by_setup >= accumulated_frames:
                            readers_are_ready = True
                            break  # Exit the while loop, we have what we need.

                logger.debug(
                    f"Milestone '{milestone}' for '{run_prefix}': Waiting for HDF5 readers to be set up..."
                )
                time.sleep(poll_interval_sec)

            if not readers_are_ready:
                raise RuntimeError(
                    f"Timed out waiting for HDF5 readers to cover {accumulated_frames} frames for run '{run_prefix}'. Aborting milestone."
                )

            logger.info(
                f"Sufficient HDF5 readers for milestone '{milestone}' are ready. Proceeding."
            )

            if not metadata_list:
                raise ValueError("Metadata list is empty.")

            # --- RESTORED WAIT LOGIC ---
            if not self.wait_for_required_files(run_prefix, accumulated_frames):
                raise ValueError(
                    f"Required data files not found for {milestone} milestone."
                )
            # ---------------------------

            # DELEGATION: Analysis Manager determines milestone logic (e.g., XDS at 25%)
            self.analysis_manager.handle_milestone_logic(
                run_prefix, milestone, master_files, metadata_list[0]
            )

            self.analysis_manager.handle_legacy_milestone(
                run_prefix, milestone, metadata_list
            )

            normalized_percent = milestone.replace("%", "")

            with self.processed_runs_lock:
                self.processed_runs[f"progress_{normalized_percent}"].add(run_prefix)
            logger.info(
                f"Successfully processed milestone '{milestone}' for run '{run_prefix}'."
            )
        except Exception:
            logger.exception(
                f"Error processing milestone '{milestone}' for run '{run_prefix}'"
            )
        finally:
            with self.active_jobs_lock:
                if job_id in self.active_jobs:
                    self.active_jobs[job_id].update(
                        {"status": "finished", "end_time": time.time()}
                    )

    def cleanup_hdf5_watchers_for_run(self, run_prefix: str):
        """
        Thread-safely requests the cleanup of HDF5 watchers for a completed run.
        """
        QtCore.QMetaObject.invokeMethod(
            self,
            "_close_hdf5_readers_for_run",
            QtCore.Qt.QueuedConnection,
            QtCore.Q_ARG(str, run_prefix),
        )

    @QtCore.pyqtSlot(str)
    def _close_hdf5_readers_for_run(self, run_prefix: str):
        """
        This slot executes on the main Qt thread. It safely modifies the
        run_hdf5_readers dictionary and calls close on each reader.
        """
        with self.run_readers_lock:  # Protect write access
            readers_to_close = self.run_hdf5_readers.pop(run_prefix, [])
            # IMPORTANT: Do NOT clear master_files/metadata_list here.
            # Late arriving series (race condition) rely on this state to append correctly.
            # Cleanup of these lists is handled by run_janitor for stale runs.

        if readers_to_close:
            logger.info(
                f"Cleaning up {len(readers_to_close)} HDF5 watchers for run '{run_prefix}'."
            )
            for reader in readers_to_close:
                # reader.close() is already a thread-safe slot/invokeMethod call
                # in the HDF5Reader, so this is safe.
                reader.close()
        else:
            logger.warning(
                f"Cleanup called for run '{run_prefix}', but no active readers were found."
            )

    @QtCore.pyqtSlot(str, int, int, list, list)
    def track_run_start_time(
        self,
        run_prefix: str,
        acc_frames: int,
        total_frames: int,
        m_files: list,
        m_list: list,
    ):
        """Records the start time of a run when the run_started signal is received."""
        with self.run_start_times_lock:
            logger.debug(f"Janitor: Tracking start time for run '{run_prefix}'.")
            self.run_start_times[run_prefix] = time.time()

    def run_janitor(self):
        """Periodically cleans up state for runs that are presumed to be stale or abandoned."""
        logger.info("Janitor: Running cleanup for stale runs...")
        RUN_TIMEOUT_SECONDS = ServerConfig.RUN_TIMEOUT_SECONDS

        stale_runs = []
        now = time.time()

        # We only need to check runs that have NOT been marked as completed.
        with self.processed_runs_lock:
            completed_runs = self.processed_runs["completed"].copy()

        with self.run_start_times_lock:
            # Iterate over a copy of the items to allow modification
            for run_prefix, start_time in list(self.run_start_times.items()):
                if run_prefix not in completed_runs and (
                    now - start_time > RUN_TIMEOUT_SECONDS
                ):
                    stale_runs.append(run_prefix)

        if not stale_runs:
            logger.info("Janitor: No stale runs found.")
            return

        for run_prefix in stale_runs:
            logger.warning(
                f"Janitor: Cleaning up stale run '{run_prefix}' (started > {RUN_TIMEOUT_SECONDS}s ago and not completed)."
            )

            # Use the existing, thread-safe cleanup function for HDF5 readers
            self.cleanup_hdf5_watchers_for_run(run_prefix)

            # Manually pop from other state dictionaries
            with self.processed_runs_lock:
                for key in self.processed_runs:
                    self.processed_runs[key].discard(run_prefix)
            with self.run_setup_events_lock:
                self.run_setup_events.pop(run_prefix, None)
            with self.pending_series_setups_lock:
                self.pending_series_setups.pop(run_prefix, None)
            with self.run_start_times_lock:
                self.run_start_times.pop(run_prefix, None)
            
            # Explicitly cleanup file lists (moved out of _close_hdf5_readers_for_run)
            with self.run_readers_lock:
                 self.run_master_files.pop(run_prefix, None)
                 self.run_metadata_list.pop(run_prefix, None)
            with self.active_series_lock:
                 self.active_series.pop(run_prefix, None)


def main():
    parser = argparse.ArgumentParser(description="Data Processing Server")
    parser.add_argument("--enable-db-logging", action="store_true")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Max workers for milestone/completion jobs.",
    )
    parser.add_argument(
        "--max-analysis-workers",
        type=int,
        default=8,
        help="Max workers for real-time analysis jobs (Dozor, etc.).",
    )
    parser.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="DEBUG"
    )
    parser.add_argument("--enable-http-server", action="store_true")
    parser.add_argument("--http-port", type=int, default=DATAPROC_SERVER_HTTP_PORT)
    parser.add_argument("--enable-websocket-server", action="store_true")
    parser.add_argument("--websocket-port", type=int, default=WEBSOCKET_PORT)
    parser.add_argument("--dry-run", action="store_true", help="Enable dry run mode (no execution).")
    args = parser.parse_args()

    app = QtCore.QCoreApplication(sys.argv)
    setup_logging(root_name="qp2", log_level=args.log_level.upper())

    try:
        from qp2.config.servers import ServerConfig
        ServerConfig.log_all_configs()
    except Exception as e:
        logger.warning(f"Failed to log server configurations: {e}")

    server = ProcessingServer(
        enable_db_logging=args.enable_db_logging,
        max_workers=args.max_workers,
        max_analysis_workers=args.max_analysis_workers,
        dry_run=args.dry_run,
    )

    # --- UPDATED HTTP Server Management ---
    http_server_manager = None
    if args.enable_http_server:
        try:
            http_server_manager = HTTPServerManager(
                port=args.http_port, server_instance=server
            )
            http_server_manager.start()
        except Exception as e:
            # The manager already logs the error, we can just note the failure here
            get_logger(__name__).critical(
                f"HTTP Server failed to start. The server will run without the external job submission API. Error: {e}"
            )
            http_server_manager = None

    # --- NEW WebSocket Server Management ---
    websocket_server_manager = None
    if args.enable_websocket_server:
        try:
            websocket_server_manager = WebSocketServerManager(
                port=args.websocket_port, server_instance=server
            )
            websocket_server_manager.start()
        except Exception as e:
            get_logger(__name__).critical(
                f"WebSocket Server failed to start. Error: {e}"
            )
            websocket_server_manager = None

    def signal_handler(sig, frame):
        logger = get_logger(__name__)
        logger.info(f"Received signal {signal.Signals(sig).name}. Shutting down...")
        if http_server_manager:
            http_server_manager.stop()
        if websocket_server_manager:
            websocket_server_manager.stop()
        server.stop()
        QtCore.QCoreApplication.instance().quit()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # This timer allows Python's signal handler to be called periodically.
    signal_timer = QtCore.QTimer()
    signal_timer.start(100)
    signal_timer.timeout.connect(lambda: None)

    server.start()
    exit_code = app.exec_()

    # The HTTPServerManager's stop() method now handles the join,
    # so we don't need to manage the thread directly here.
    get_logger(__name__).info(f"Application finished with exit code {exit_code}.")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()