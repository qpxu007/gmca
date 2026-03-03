# qp2/data_proc/server/pipelines/dials/find_spots_dials.py
import json
import os
import shlex

from PyQt5.QtCore import QRunnable, pyqtSignal, QObject

from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.log.logging_config import get_logger
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)

DIALS_PYTHON_EXECUTABLE = ProgramConfig.get_program_path("dials_python")


class DialsSpotfinderSignals(QObject):
    result = pyqtSignal(str, str, str)
    error = pyqtSignal(str, str)


class DialsSpotfinderWorker(QRunnable):
    """Submits a distributed DIALS spot finding job for a single data file."""

    def __init__(
        self,
        file_path,
        start_frame,
        end_frame,
        metadata,
        redis_conn,
        redis_key_prefix,
        **kwargs,
    ):
        super().__init__()
        self.file_path = file_path
        self.start_frame = start_frame
        self.end_frame = end_frame
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.kwargs = kwargs
        self.signals = DialsSpotfinderSignals()
        self.method = self.kwargs.get("method", "auto")

    def run(self):
        """Prepares and submits the DIALS spot finding job."""
        try:
            if not os.path.exists(DIALS_PYTHON_EXECUTABLE):
                raise FileNotFoundError(
                    f"DIALS Python executable not found at: {DIALS_PYTHON_EXECUTABLE}"
                )

            run_method = self.method
            if run_method == "auto":
                run_method = "slurm" if is_sbatch_available() else "shell"

            serializable_metadata = {"master_file": self.metadata.get("master_file")}

            script_path = os.path.join(
                os.path.dirname(__file__), "dials_find_spots_process.py"
            )

            project_root = os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    )
                )
            )

            command_list = [
                DIALS_PYTHON_EXECUTABLE,
                script_path,
                "--project_root",
                project_root,
                "--metadata",
                json.dumps(serializable_metadata),
                "--start_frame",
                str(self.start_frame),
                "--end_frame",
                str(self.end_frame),
                "--redis_key_prefix",
                self.redis_key_prefix,
            ]
            if self.redis_conn:
                host = self.redis_conn.connection_pool.connection_kwargs.get("host")
                port = self.redis_conn.connection_pool.connection_kwargs.get("port")
                command_list.extend(["--redis_host", host, "--redis_port", str(port)])

            job_name = f"dials_spots_{os.path.basename(self.file_path)}"
            proc_dir = self.metadata.get("params", {}).get("proc_dir", "/tmp")
            job_output_dir = os.path.join(proc_dir, "dials_spotfinder_logs")

            safe_python_command = " ".join(
                shlex.quote(str(arg)) for arg in command_list
            )

            full_command_for_script = f"""
set -e
echo "Loading DIALS module..."
{ProgramConfig.get_setup_command('dials')}

echo "DIALS module loaded. Executing spot finding..."
srun {safe_python_command}
"""

            run_command(
                cmd=full_command_for_script,
                cwd=job_output_dir,
                method=run_method,
                job_name=job_name,
                background=True,
                walltime="00:15:00",
                memory="2gb",
            )

            self.signals.result.emit(
                "SUBMITTED",
                f"Submitted DIALS job '{job_name}' via {run_method}.",
                self.file_path,
            )

        except Exception as e:
            logger.error(
                f"DialsSpotfinderWorker failed to submit job: {e}", exc_info=True
            )
            self.signals.error.emit(self.file_path, str(e))
