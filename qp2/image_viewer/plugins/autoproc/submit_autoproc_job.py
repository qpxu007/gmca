# qp2/image_viewer/plugins/autoproc/submit_autoproc_job.py
import json
import os
import sys
import time
from pathlib import Path
from PyQt5.QtCore import QRunnable, pyqtSignal, QObject
from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.log.logging_config import get_logger
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.utils.auxillary import sanitize_unit_cell
from qp2.xio.proc_utils import determine_proc_base_dir
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)


class AutoPROCJobSignals(QObject):
    result = pyqtSignal(str, str, str)
    error = pyqtSignal(str, str)


class AutoPROCProcessDatasetWorker(QRunnable):
    """Submits a single autoPROC job for an ENTIRE dataset."""

    def __init__(self, master_file, metadata, redis_conn, redis_key_prefix, **kwargs):
        super().__init__()
        self.master_file = master_file
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.kwargs = kwargs
        self.signals = AutoPROCJobSignals()

    def run(self):
        try:
            if not self.redis_conn:
                return

            results_key = f"{self.redis_key_prefix}:{self.master_file}"
            status_key = f"{results_key}:status"

            if self.kwargs.get("force_rerun", False):
                logger.info(f"Force rerun requested. Clearing status for {self.master_file}")
                self.redis_conn.delete(status_key)

            initial_status = {"status": "SUBMITTED", "timestamp": time.time()}
            
            if self.kwargs.get("force_rerun", False):
                success = self.redis_conn.set(status_key, json.dumps(initial_status), ex=7 * 24 * 3600)
            else:
                success = self.redis_conn.set(status_key, json.dumps(initial_status), ex=7 * 24 * 3600, nx=True)

            if not success:
                logger.info(
                    f"autoPROC job for {os.path.basename(self.master_file)} already submitted. Skipping."
                )
                return

            master_basename = os.path.splitext(os.path.basename(self.master_file))[0]

            if self.kwargs.get("output_proc_dir"):
                proc_dir = Path(self.kwargs.get("output_proc_dir"))
            else:
                # Use common root logic
                user_root = self.kwargs.get("processing_common_proc_dir_root") or self.kwargs.get("autoproc_proc_dir_root")
                proc_base = determine_proc_base_dir(user_root, self.master_file)
                
                extra_files = self.kwargs.get("extra_data_files", [])
                if extra_files:
                    all_files = [self.master_file] + extra_files
                    basenames = [os.path.splitext(os.path.basename(f))[0] for f in all_files]
                    common = os.path.commonprefix(basenames)
                    
                    if common:
                        dir_name = f"{common}_{len(all_files)}datasets"
                    else:
                        dir_name = f"{master_basename}_{len(all_files)}datasets"
                else:
                    dir_name = master_basename

                proc_dir = proc_base / "autoproc" / dir_name

            proc_dir.mkdir(parents=True, exist_ok=True)
            self.redis_conn.hset(results_key, "_proc_dir", str(proc_dir))

            # AutoProc script path
            script_path = os.path.join(
                os.path.dirname(__file__), "autoproc_process_dataset.py"
            )
            script_path = os.path.normpath(script_path)

            # Helper to resolve cluster-safe paths
            cluster_python = os.environ.get("CLUSTER_PYTHON")
            cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")
            
            if cluster_python and cluster_root:
                python_exe = cluster_python
                relative_script_path = "image_viewer/plugins/autoproc/autoproc_process_dataset.py"
                execution_script = os.path.join(cluster_root, relative_script_path)
            else:
                python_exe = sys.executable
                execution_script = script_path

            command_list = [
                python_exe,
                execution_script,
                "--pipeline",
                "autoPROC",
                "--data",
                self.master_file,
                "--work_dir",
                str(proc_dir),
                "--beamline",
                get_beamline_from_hostname(),
                "--status_key",
                status_key,
                "--redis_host",
                self.redis_conn.connection_pool.connection_kwargs.get("host"),
                "--redis_port",
                str(self.redis_conn.connection_pool.connection_kwargs.get("port")),
                "--runner",
                "shell",
            ]

            if "extra_data_files" in self.kwargs:
                for extra_file in self.kwargs["extra_data_files"]:
                    command_list.extend(["--data", extra_file])

            group_name_to_use = self.metadata.get("primary_group") or self.metadata.get(
                "username"
            )
            if group_name_to_use:
                command_list.extend(["--group_name", group_name_to_use])

            # --- Forward Run Prefix ---
            run_prefix = self.metadata.get("run_prefix") or self.kwargs.get("run_prefix")
            if run_prefix:
                command_list.extend(["--run_prefix", str(run_prefix)])

            if self.metadata.get("pi_badge"):
                command_list.extend(["--pi_badge", str(self.metadata.get("pi_badge"))])

            if self.metadata.get("esaf_id"):
                command_list.extend(
                    ["--esaf_number", str(self.metadata.get("esaf_id"))]
                )

            if self.kwargs.get("autoproc_highres"):
                command_list.extend(["--highres", str(self.kwargs["autoproc_highres"])])

            if self.kwargs.get("autoproc_native"):
                command_list.append("--native")

            if self.kwargs.get("autoproc_space_group"):
                command_list.extend(
                    ["--space_group", self.kwargs["autoproc_space_group"]]
                )
            if self.kwargs.get("autoproc_unit_cell"):
                command_list.extend(
                    [
                        "--unit_cell",
                        f"'{sanitize_unit_cell(self.kwargs['autoproc_unit_cell'])}'",
                    ]
                )
            if self.kwargs.get("autoproc_model"):
                command_list.extend(["--model", self.kwargs["autoproc_model"]])
            if self.kwargs.get("autoproc_nproc"):
                command_list.extend(["--nproc", str(self.kwargs["autoproc_nproc"])])
            if self.kwargs.get("autoproc_njobs"):
                command_list.extend(["--njobs", str(self.kwargs["autoproc_njobs"])])
            if self.kwargs.get("autoproc_fast"):
                command_list.append("--fast")

            job_name = f"autoproc_{master_basename}"

            logger.debug(f"Submitting autoPROC job with kwargs: {self.kwargs}")
            logger.debug(f"autoPROC Command List: {command_list}")

            run_command(
                cmd=command_list,
                cwd=str(proc_dir),
                method="slurm" if is_sbatch_available() else "shell",
                job_name=job_name,
                background=True,
                processors=self.kwargs.get("autoproc_nproc", 32) * self.kwargs.get("autoproc_njobs", 1),
                nodes=1,
                walltime="06:00:00",
                pre_command=ProgramConfig.get_setup_command('autoproc'),
            )
            self.signals.result.emit(
                "SUBMITTED", f"Submitted autoPROC job '{job_name}'", self.master_file
            )

        except Exception as e:
            logger.error(f"AutoPROCProcessDatasetWorker failed: {e}", exc_info=True)
            self.signals.error.emit(self.master_file, str(e))
            if "status_key" in locals():
                failed_status = {
                    "status": "FAILED",
                    "timestamp": time.time(),
                    "error": str(e),
                }
                self.redis_conn.set(status_key, json.dumps(failed_status), ex=7 * 24 * 3600)