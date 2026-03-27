# qp2/image_viewer/plugins/dozor/find_spots_dozor_batch.py
import json
import os
import shlex
import sys
import time
from datetime import datetime
from typing import List

from PyQt5.QtCore import QRunnable, QObject, pyqtSignal

from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.log.logging_config import get_logger
from qp2.xio.proc_utils import determine_proc_base_dir

logger = get_logger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class DozorBatchSignals(QObject):
    result = pyqtSignal(str, str, str)  # job_name, status_code, message
    error = pyqtSignal(str, str)  # job_name, error_message


class DozorBatchWorker(QRunnable):
    """Submits a single, larger Dozor job that processes a batch of files."""

    def __init__(self, job_batch: List[dict], redis_conn, **kwargs):
        super().__init__()
        self.signals = DozorBatchSignals()
        self.job_batch = job_batch
        self.redis_conn = redis_conn
        self.kwargs = kwargs

    def run(self):
        if not self.job_batch:
            return

        master_file = self.job_batch[0]["metadata"]["master_file"]
        redis_key_prefix = self.kwargs.get(
            "redis_key_prefix", "analysis:out:spots:dozor2"
        )
        status_key = f"{redis_key_prefix}:{master_file}:status"

        # Prepare arguments for the batch processing script
        jobs_json_str = json.dumps(self.job_batch, cls=DateTimeEncoder)
        safe_jobs_json_arg = shlex.quote(jobs_json_str)

        redis_host = self.redis_conn.connection_pool.connection_kwargs.get("host")
        redis_port = self.redis_conn.connection_pool.connection_kwargs.get("port")
        redis_key_prefix = self.kwargs.get(
            "redis_key_prefix", "analysis:out:spots:dozor2"
        )

        script_path = os.path.join(os.path.dirname(__file__), "dozor_batch_process.py")
        
        # Cluster-safe logic
        cluster_python = os.environ.get("CLUSTER_PYTHON")
        cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")
        
        if cluster_python and cluster_root:
            python_exe = cluster_python
            relative_script_path = "image_viewer/plugins/dozor/dozor_batch_process.py"
            execution_script = os.path.join(cluster_root, relative_script_path)
        else:
            python_exe = sys.executable
            execution_script = script_path

        command_list = [
            python_exe,
            execution_script,
            "--jobs_json",
            safe_jobs_json_arg,
            "--redis_host",
            redis_host,
            "--redis_port",
            str(redis_port),
            "--redis_key_prefix",
            redis_key_prefix,
            "--status_key",
            status_key,
        ]

        if self.kwargs.get("debug"):
            command_list.append("--debug")

        run_method = "slurm" if is_sbatch_available() else "shell"

        # Create a unique job name by including the starting frame of the first task in the batch.
        master_basename = os.path.basename(self.job_batch[0]["metadata"]["master_file"])
        start_frame_of_batch = self.job_batch[0]["start_frame"]
        job_name = f"dozor_batch_{master_basename}_f{start_frame_of_batch}"

        command_list.extend(["--job_name", job_name])

        # Estimate total runtime - simple but better than nothing
        total_frames = sum(job["nimages"] for job in self.job_batch)
        walltime_minutes = max(5, int(total_frames / 100) * 3)  # ~3 min per 100 frames
        walltime = f"00:{walltime_minutes:02d}:00"

        try:
            initial_status = {"status": "SUBMITTED", "timestamp": time.time()}
            self.redis_conn.hset(status_key, job_name, json.dumps(initial_status))
            self.redis_conn.expire(status_key, 24 * 3600)  # match results TTL

            # Determine working directory
            if self.kwargs.get("proc_dir"):
                job_cwd = self.kwargs.get("proc_dir")
            else:
                user_root = self.kwargs.get("processing_common_proc_dir_root")
                job_cwd = str(determine_proc_base_dir(user_root, master_file) / "dozor_logs")
            os.makedirs(job_cwd, exist_ok=True)

            job_id = run_command(
                cmd=command_list,
                cwd=job_cwd,
                method=run_method,
                job_name=job_name,  # Use the new, unique job name
                walltime=walltime,
                background=True,
                processors=4,  # this is gmca default, set by OMP environment variable
                quiet=True,
            )

            if job_id:
                msg = f"Successfully submitted batch job '{job_name}' ({len(self.job_batch)} tasks) to {run_method}. Job ID: {job_id}"
                self.signals.result.emit(job_name, "SUBMITTED", msg)
            else:
                raise RuntimeError("Job submission failed to return an ID.")

        except Exception as e:
            err_msg = f"Failed to submit Dozor batch job '{job_name}': {e}"
            logger.error(err_msg, exc_info=True)
            self.signals.error.emit(job_name, err_msg)
            failed_status = {
                "status": "FAILED",
                "timestamp": time.time(),
                "error": str(e),
            }
            self.redis_conn.hset(status_key, job_name, json.dumps(failed_status))
            self.redis_conn.expire(status_key, 24 * 3600)
