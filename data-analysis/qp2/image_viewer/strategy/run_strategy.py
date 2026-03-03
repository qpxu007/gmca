# qp2/image_viewer/strategy/run_strategy.py

import concurrent.futures
import json
import time
import os
from pathlib import Path
from contextlib import contextmanager

from pyqtgraph.Qt import QtCore

from qp2.log.logging_config import get_logger
from qp2.pipelines.utils.image_set import get_image_set_string
from qp2.xio.proc_utils import determine_proc_base_dir, extract_master_prefix

logger = get_logger(__name__)


class StrategyWorkerSignals(QtCore.QObject):
    """Defines signals available from a running StrategyWorker thread."""

    finished = QtCore.pyqtSignal(str, object, dict)  # program, result_data, mapping
    error = QtCore.pyqtSignal(str, str)  # program, error_message
    all_done = QtCore.pyqtSignal()


class StrategyWorker(QtCore.QRunnable):
    """Worker thread for running XDS or MOSFLM strategy."""

    def __init__(
        self,
        programs: list,
        mapping: dict,
        pipeline_params: dict,
        redis_conn=None,
        redis_key_prefix: str = "analysis:out:strategy",
        delete_workdir: bool = True,
    ):
        super().__init__()
        self.programs = programs
        self.mapping = mapping
        self.pipeline_params = pipeline_params
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.delete_workdir_on_done = delete_workdir
        self.signals = StrategyWorkerSignals()

    def _report_status(self, program: str, status: str, error: str = None):
        """Helper to report status to Redis."""
        if not self.redis_conn:
            return

        # Strategy typically involves multiple master files. 
        # For simplicity and consistency with other workers, we pick the first master file
        # as the identifier for the dataset results.
        master_files = list(self.mapping.keys())
        if not master_files:
            return
        
        primary_master = master_files[0]
        # Schema: analysis:out:xds_strategy:/path/to/master:status
        status_key = f"analysis:out:{program}_strategy:{primary_master}:status"
        
        status_data = {
            "status": status,
            "timestamp": time.time(),
        }
        if error:
            status_data["error"] = error

        try:
            self.redis_conn.set(status_key, json.dumps(status_data), ex=7 * 24 * 3600)
        except Exception as e:
            logger.warning(f"Failed to update strategy status in Redis: {e}")

    def _store_results(self, program: str, result_data: dict):
        """Helper to store results in Redis."""
        if not self.redis_conn:
            return

        master_files = list(self.mapping.keys())
        if not master_files:
            return
        
        primary_master = master_files[0]
        results_key = f"analysis:out:{program}_strategy:{primary_master}"

        try:
            # 1. Store the main result JSON
            self.redis_conn.hset(results_key, "data", json.dumps(result_data))
            
            # 2. Store all available metadata from pipeline_params
            if self.pipeline_params:
                # Filter out None values and convert everything to string for Redis HSET
                metadata_to_store = {
                    str(k): str(v) 
                    for k, v in self.pipeline_params.items() 
                    if v is not None
                }
                if metadata_to_store:
                    self.redis_conn.hset(results_key, mapping=metadata_to_store)
                    
            # 3. Set expiration (consistent with other workers)
            self.redis_conn.expire(results_key, 7 * 24 * 3600)
            
        except Exception as e:
            logger.warning(f"Failed to store strategy results/metadata in Redis: {e}")

    @QtCore.pyqtSlot()
    def run(self):
        from qp2.utils.tempdirectory import temporary_directory
        from qp2.pipelines.strategy.xds.xds_strategy import run_xds_strategy
        from qp2.pipelines.strategy.mosflm.mosflm_strategy import (
            run_strategy as run_mosflm_strategy,
        )

        @contextmanager
        def get_workdir(program):
            if self.delete_workdir_on_done:
                with temporary_directory(prefix=f"iv_strategy_{program}_") as tmp:
                    yield tmp
            else:
                # Use permanent standard processing directory
                master_file = next(iter(self.mapping.keys()))
                prefix = extract_master_prefix(master_file)
                user_root = self.pipeline_params.get("processing_common_proc_dir_root")
                base = determine_proc_base_dir(user_root, master_file)
                p_workdir = base / f"{program}_strategy" / prefix
                p_workdir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Strategy workdir for {program}: {p_workdir}")
                yield str(p_workdir)

        def run_single_strategy(program):
            self._report_status(program, "RUNNING")
            # Ensure imageSet is standardized in pipeline_params for DB/Redis
            if "imageSet" not in self.pipeline_params:
                self.pipeline_params["imageSet"] = get_image_set_string(self.mapping)

            try:
                with get_workdir(program) as workdir:
                    logger.info(f"Running {program} in {workdir}")
                    if program == "xds":
                        result = run_xds_strategy(
                            self.mapping,
                            workdir=workdir,
                            pipeline_params=self.pipeline_params,
                        )
                    elif program == "mosflm":
                        result = run_mosflm_strategy(
                            self.mapping,
                            workdir=workdir,
                            pipeline_params=self.pipeline_params,
                        )
                    elif program == "crystfel":
                        from qp2.image_viewer.plugins.crystfel.run_crystfel_strategy import run_crystfel_strategy
                        result = run_crystfel_strategy(
                            self.mapping,
                            workdir=workdir,
                            pipeline_params=self.pipeline_params,
                        )
                    else:
                        raise ValueError(f"Unknown strategy program: {program}")

                    if result is None:
                        raise RuntimeError(
                            f"{program.upper()} strategy returned no result."
                        )

                    self._store_results(program, result)
                    self._report_status(program, "COMPLETED")
                    self.signals.finished.emit(program, result, self.mapping)
            except Exception as e:
                logger.error(
                    f"Strategy worker for {program} failed: {e}", exc_info=True
                )
                self._report_status(program, "FAILED", error=str(e))
                self.signals.error.emit(program, str(e))

        # Initially set all to SUBMITTED
        for prog in self.programs:
            self._report_status(prog, "SUBMITTED")

        if len(self.programs) == 1:
            # Run synchronously if only one program is requested
            run_single_strategy(self.programs[0])
        else:
            # Run in parallel if multiple programs are requested
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(self.programs)
            ) as executor:
                # map() ensures we wait for all tasks to complete before the 'with' block exits
                executor.map(run_single_strategy, self.programs)

        # Signal that all tasks submitted to the worker have completed
        self.signals.all_done.emit()
