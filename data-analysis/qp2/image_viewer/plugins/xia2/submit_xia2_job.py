# qp2/image_viewer/plugins/xia2/submit_xia2_job.py
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


class Xia2JobSignals(QObject):
    result = pyqtSignal(str, str, str)
    error = pyqtSignal(str, str)


class Xia2ProcessDatasetWorker(QRunnable):
    """Submits a single xia2 job for an ENTIRE dataset."""

    def __init__(self, master_file, metadata, redis_conn, redis_key_prefix, **kwargs):
        super().__init__()
        self.master_file = master_file
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.kwargs = kwargs
        self.signals = Xia2JobSignals()

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
                    f"xia2 job for {os.path.basename(self.master_file)} already submitted. Skipping."
                )
                return

            master_basename = os.path.splitext(os.path.basename(self.master_file))[0]

            if self.kwargs.get("output_proc_dir"):
                proc_dir = Path(self.kwargs.get("output_proc_dir"))
            else:
                # Use common root logic
                user_root = self.kwargs.get("processing_common_proc_dir_root") or self.kwargs.get("xia2_proc_dir_root")
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

                proc_dir = proc_base / "xia2" / dir_name

            proc_dir.mkdir(parents=True, exist_ok=True)
            self.redis_conn.hset(results_key, "_proc_dir", str(proc_dir))

            # This script will wrap the call to the original main.py
            script_path = os.path.join(
                os.path.dirname(__file__), "xia2_process_dataset.py"
            )
            script_path = os.path.normpath(script_path)

            # Helper to resolve cluster-safe paths
            cluster_python = os.environ.get("CLUSTER_PYTHON")
            cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")
            
            if cluster_python and cluster_root:
                python_exe = cluster_python
                relative_script_path = "image_viewer/plugins/xia2/xia2_process_dataset.py"
                execution_script = os.path.join(cluster_root, relative_script_path)
            else:
                python_exe = sys.executable
                execution_script = script_path

            pipeline_choice = self.kwargs.get("xia2_pipeline", "xia2_dials")

            # Format --data as path:start:end when frame range is specified
            start_frame = self.kwargs.get("start_frame")
            end_frame = self.kwargs.get("end_frame")
            if start_frame is not None and end_frame is not None:
                data_arg = f"{self.master_file}:{start_frame}:{end_frame}"
            else:
                data_arg = self.master_file

            command_list = [
                python_exe,
                execution_script,
                "--pipeline", pipeline_choice,
                "--data",
                data_arg,
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

            if self.metadata.get("primary_group"):
                command_list.extend(
                    ["--group_name", str(self.metadata.get("primary_group"))]
                )
            elif self.metadata.get("username"):
                command_list.extend(
                    ["--group_name", str(self.metadata.get("username"))]
                )

            if self.metadata.get("pi_badge"):
                command_list.extend(["--pi_badge", str(self.metadata.get("pi_badge"))])

            # --- Forward Run Prefix ---
            run_prefix = self.metadata.get("run_prefix") or self.kwargs.get("run_prefix")
            if run_prefix:
                command_list.extend(["--run_prefix", str(run_prefix)])

            if self.metadata.get("esaf_id"):
                command_list.extend(
                    ["--esaf_number", str(self.metadata.get("esaf_id"))]
                )

            # Map kwargs from settings to command-line arguments
            if self.kwargs.get("xia2_highres"):
                command_list.extend(["--highres", str(self.kwargs["xia2_highres"])])
            if self.kwargs.get("xia2_native"):
                command_list.append("--native")
            if self.kwargs.get("xia2_space_group"):
                command_list.extend(["--space_group", self.kwargs["xia2_space_group"]])
            if self.kwargs.get("xia2_unit_cell"):
                command_list.extend(
                    [
                        "--unit_cell",
                        f"'{sanitize_unit_cell(self.kwargs['xia2_unit_cell'])}'",
                    ]
                )
            if self.kwargs.get("xia2_model"):
                command_list.extend(["--model", self.kwargs["xia2_model"]])
            if self.kwargs.get("xia2_nproc"):
                command_list.extend(["--nproc", str(self.kwargs["xia2_nproc"])])
            if self.kwargs.get("xia2_njobs"):
                command_list.extend(["--njobs", str(self.kwargs["xia2_njobs"])])
            if self.kwargs.get("xia2_fast"):
                command_list.append("--fast")
            if "xia2_trust_beam_centre" in self.kwargs:
                command_list.extend(["--trust_beam_centre", str(self.kwargs["xia2_trust_beam_centre"])])

            if self.kwargs.get("xia2_override_geometry"):
                beam_x = self.kwargs.get("xia2_beam_x")
                beam_y = self.kwargs.get("xia2_beam_y")
                distance = self.kwargs.get("xia2_distance")
                if beam_x and beam_y:
                    command_list.extend(["--beam_x", str(beam_x), "--beam_y", str(beam_y)])
                if distance:
                    command_list.extend(["--distance", str(distance)])

            job_name = f"xia2_{master_basename}"

            logger.debug(f"Submitting xia2 job with kwargs: {self.kwargs}")
            logger.debug(f"xia2 Command List: {command_list}")

            run_command(
                cmd=command_list,
                cwd=str(proc_dir),
                method="slurm" if is_sbatch_available() else "shell",
                job_name=job_name,
                background=True,
                processors=self.kwargs.get("xia2_nproc", 32)
                * self.kwargs.get(
                    "xia2_njobs", 4
                ),  # xia2 uses nproc*njobs on a single node
                nodes=1,
                walltime="10:00:00",
                memory="16gb",
                pre_command=ProgramConfig.get_setup_command('dials'),  # Assuming a module for xia2 environment
            )
            self.signals.result.emit(
                "SUBMITTED", f"Submitted xia2 job '{job_name}'", self.master_file
            )

        except Exception as e:
            logger.error(f"Xia2ProcessDatasetWorker failed: {e}", exc_info=True)
            self.signals.error.emit(self.master_file, str(e))
            if "status_key" in locals():
                failed_status = {
                    "status": "FAILED",
                    "timestamp": time.time(),
                    "error": str(e),
                }
                self.redis_conn.set(status_key, json.dumps(failed_status), ex=7 * 24 * 3600)