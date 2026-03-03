import json
import os
import shlex  # For safely quoting command-line arguments
import sys  # Required for sys.executable
from typing import Optional

import redis  # For Redis checks, if still done in DozorWorker before submission
from PyQt5.QtCore import QRunnable, QObject, pyqtSignal, pyqtSlot

from qp2.image_viewer.plugins.dozor.dozor_process import (
    check_frames_exist_in_redis_hash,
)
from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.log.logging_config import get_logger
from qp2.utils.auxillary import can_read_path

logger = get_logger(__name__)

# It's good practice to make paths configurable
# It's good practice to make paths configurable
# Cluster-safe logic
CLUSTER_PYTHON = os.environ.get("CLUSTER_PYTHON")
CLUSTER_ROOT = os.environ.get("CLUSTER_PROJECT_ROOT")

if CLUSTER_PYTHON and CLUSTER_ROOT:
    PYTHON_EXECUTABLE_PATH = CLUSTER_PYTHON
    # Calculate relative path
    # Local: image_viewer/plugins/dozor/dozor_process.py
    # This file: image_viewer/plugins/dozor/find_spots_dozor.py
    DOZOR_PROCESS_SCRIPT_PATH = os.path.join(
        CLUSTER_ROOT, "image_viewer/plugins/dozor/dozor_process.py"
    )
else:
    PYTHON_EXECUTABLE_PATH = sys.executable
    DOZOR_PROCESS_SCRIPT_PATH = os.path.join(
        os.path.dirname(__file__), "dozor_process.py"
    )


class DozorSignals(QObject):
    STATUS_SUBMITTED_TO_SLURM = "SUBMITTED_TO_SLURM"
    STATUS_SKIPPED_REDIS_FOUND = "SKIPPED_REDIS_FOUND"
    STATUS_SKIPPED_NO_REDIS_CONFIG = "SKIPPED_NO_REDIS_CONFIG"
    STATUS_SKIPPED_MASTER_FILE_MISSING = "SKIPPED_MASTER_FILE_MISSING"
    result = pyqtSignal(str, str, str)  # file_path, status_code, message
    error = pyqtSignal(str, str)  # file_path, error_message


class DozorWorker(QRunnable):
    def __init__(
            self,
            file_path,
            start_frame,
            end_frame,
            metadata,
            redis_conn=None,
            redis_key_prefix="analysis:out:spots:dozor2",
            skip_if_redis_none=True,
            method: str = "auto",  # "auto", "slurm", or "shell"
            slurm_job_name_prefix="dozor_job",
            slurm_time="00:03:00",  # Default time limit
            slurm_mem_per_cpu="500M",  # Default memory
            proc_dir: Optional[str] = None,
            **kwargs, 
    ):
        super().__init__()
        self.signals = DozorSignals()
        self.file_path = file_path
        self.start_frame = start_frame
        self.end_frame = end_frame
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.method = method
        if redis_conn:
            self.redis_host = redis_conn.connection_pool.connection_kwargs.get(
                "host", None
            )
            self.redis_port = redis_conn.connection_pool.connection_kwargs.get(
                "port", None
            )
        self.skip_if_redis_none = skip_if_redis_none

        # Slurm configuration
        self.slurm_job_name_prefix = slurm_job_name_prefix
        self.slurm_time = slurm_time
        self.slurm_mem_per_cpu = slurm_mem_per_cpu
        self.proc_dir = proc_dir
        self.kwargs = kwargs

    def _perform_pre_checks(self):
        """Performs pre-checks before submitting to Slurm."""
        master_file_path = self.metadata.get("master_file")

        if not master_file_path:
            err_msg = "Master file path missing in metadata."
            logger.error(err_msg)
            return False

        if not can_read_path(master_file_path):
            self.signals.error.emit(
                master_file_path,
                f"Cannot access file (no read permission):\n{master_file_path}",
            )
            return False

        if not self.redis_conn and self.skip_if_redis_none:
            msg = f"Redis server not configured. Dozor job for {os.path.basename(self.file_path)} skipped."
            logger.info(msg)
            return False

        if self.redis_conn is not None:
            try:
                redis_key = f"{self.redis_key_prefix}:{master_file_path}"
                start_frame_1based = self.start_frame + 1
                num_frames = (self.end_frame - self.start_frame) + 1

                if check_frames_exist_in_redis_hash(
                        self.redis_conn, redis_key, start_frame_1based, num_frames
                ):
                    msg = f"Dozor results for all frames {self.start_frame}-{self.end_frame} already exist in Redis. Skipping."
                    logger.info(msg)
                    self.signals.result.emit(
                        self.file_path, self.signals.STATUS_SKIPPED_REDIS_FOUND, msg
                    )
                    return False

            except redis.exceptions.RedisError as e_redis:
                err_msg = f"Redis error during pre-check for {os.path.basename(self.file_path)}: {e_redis}. Slurm job not submitted."
                # self.signals.error.emit(self.file_path, err_msg)
                logger.error(err_msg)
                return False
        return True

    @pyqtSlot()
    def run(self):
        """
        Constructs a command to run dozor_process.py and executes it
        via run_command, automatically choosing between Slurm and a local shell.
        """
        if not self._perform_pre_checks():
            return

        serializable_metadata = self.metadata.copy()
        serializable_metadata.update(self.kwargs) # user configurable beamstop size & spot size

        # Remove any keys that hold non-serializable objects.
        serializable_metadata.pop("hdf5_reader_instance", None)
        serializable_metadata.pop(
            "detector_mask", None
        )  # NumPy arrays can sometimes cause issues

        # The 'params' dictionary should already be serializable, but it's good practice
        # to ensure it doesn't contain complex objects if its source changes.

        logger.debug(serializable_metadata)

        # Now, dump the clean dictionary to JSON.
        metadata_json_str = json.dumps(serializable_metadata)
        safe_metadata_arg = shlex.quote(metadata_json_str)

        # 1. Determine the execution method
        run_method = self.method
        if run_method == "auto":
            run_method = "slurm" if is_sbatch_available() else "shell"

        logger.info(f"DozorWorker will execute using method: '{run_method}'")

        # 2. Construct the Python command to be executed.
        #    This is the command that will be passed to `run_command`.
        start_image_dozor = self.start_frame + 1
        nimages_dozor = self.end_frame - self.start_frame + 1

        python_command_args = [
            PYTHON_EXECUTABLE_PATH,
            DOZOR_PROCESS_SCRIPT_PATH,
            "--metadata",
            safe_metadata_arg,  # Use quotes for safety
            "--start",
            str(start_image_dozor),
            "--nimages",
            str(nimages_dozor),
        ]
        if self.redis_host:
            python_command_args.extend(["--redis_host", self.redis_host])
        if self.redis_port:
            python_command_args.extend(["--redis_port", str(self.redis_port)])
        if self.redis_key_prefix:
            python_command_args.extend(["--redis_key_prefix", self.redis_key_prefix])

        if self.proc_dir:
            job_output_dir = self.proc_dir
        else:
            logger.warning(
                "proc_dir not provided to DozorWorker. Falling back to default directory logic."
            )
            fallback_proc_dir = self.metadata.get("proc_dir", os.getenv("HOME", "/tmp"))
            job_output_dir = os.path.join(fallback_proc_dir, "dozor_logs")

        # 4. Define a unique job name
        job_name = f"{self.slurm_job_name_prefix}_{os.path.basename(self.file_path)}_{self.start_frame}"

        # 5. Call run_command
        try:
            job_id = run_command(
                cmd=python_command_args,
                cwd=job_output_dir,
                method=run_method,
                job_name=job_name,
                walltime=self.slurm_time,
                memory=self.slurm_mem_per_cpu,
                background=True,  # Always run in the background
                quiet=True,
            )

            if run_method == "slurm":
                if job_id:
                    msg = f"Successfully submitted to Slurm. Job ID: {job_id}"
                    self.signals.result.emit(
                        self.file_path, self.signals.STATUS_SUBMITTED_TO_SLURM, msg
                    )
                else:
                    msg = "Slurm submission failed to return a Job ID."
                    self.signals.error.emit(self.file_path, msg)
            else:  # Shell method
                msg = f"Successfully launched local background process for job {job_name}."
                self.signals.result.emit(self.file_path, "SUBMITTED_LOCAL", msg)

        except Exception as e:
            err_msg = f"Failed to execute Dozor job via run_command: {e}"
            logger.error(err_msg, exc_info=True)
            self.signals.error.emit(self.file_path, err_msg)
