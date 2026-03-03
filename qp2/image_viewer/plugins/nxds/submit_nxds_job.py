# qp2/image_viewer/plugins/nxds/submit_nxds_job.py
import json
import math
import os
import sys
import time
from pathlib import Path

from PyQt5.QtCore import QRunnable, pyqtSignal, QObject

from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.log.logging_config import get_logger
from qp2.utils.auxillary import sanitize_unit_cell
from qp2.xio.proc_utils import determine_proc_base_dir
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)

NXDS_PERSISTENT_DIR = Path(
    os.getenv("NXDS_PERSISTENT_DIR", f"/mnt/beegfs/{os.getenv('USER')}/nxds_runs")
)


class NXDSJobSignals(QObject):
    result = pyqtSignal(str, str, str)
    error = pyqtSignal(str, str)


class NXDSProcessDatasetWorker(QRunnable):
    """Submits a single, distributed nXDS job for an ENTIRE dataset."""

    def __init__(self, master_file, metadata, redis_conn, redis_key_prefix, **kwargs):
        super().__init__()
        self.master_file = master_file
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.kwargs = kwargs
        self.signals = NXDSJobSignals()

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
                    f"nXDS job for {os.path.basename(self.master_file)} already submitted. Skipping."
                )
                return

            master_basename = os.path.splitext(os.path.basename(self.master_file))[0]

            if self.kwargs.get("output_proc_dir"):
                proc_dir = Path(self.kwargs.get("output_proc_dir"))
            else:
                # Use common root logic
                user_root = self.kwargs.get("processing_common_proc_dir_root") or self.kwargs.get("nxds_proc_dir_root")
                proc_base = determine_proc_base_dir(user_root, self.master_file)
                proc_dir = proc_base / "nxds" / master_basename

            proc_dir.mkdir(parents=True, exist_ok=True)
            self.redis_conn.hset(results_key, "_proc_dir", str(proc_dir))
            logger.info(f"[nXDS] Processing directory: {proc_dir}")

            run_method = "slurm" if is_sbatch_available() else "shell"

            # Get nproc from user settings, with a fallback
            nproc = self.kwargs.get("nxds_nproc", 8)
            
            # Get user-defined njobs, defaulting to 0 if not set (0 implies auto-calc)
            user_njobs = self.kwargs.get("nxds_njobs", 0)

            # Get total_frames from the metadata passed to the worker
            total_frames = self.metadata.get("nimages", 0)

            logger.debug(f"Metadata: {self.metadata}")
            logger.debug(f"Total frames: {total_frames}, nproc: {nproc}")
            
            if user_njobs > 0:
                njobs = user_njobs
                logger.info(f"Using user-defined njobs for nXDS: {njobs}")
            elif total_frames > 0 and nproc > 0:
                # Calculate njobs based on frames and nproc
                # Use math.ceil to ensure you get enough jobs for leftover frames
                # e.g., 100 frames / 8 nproc = 12.5 -> 13 jobs
                njobs = math.ceil(total_frames / nproc)
                # Cap the number of jobs at 16 (or 8 as per previous logic)
                njobs = min(njobs, 8)
                logger.info(
                    f"Calculated njobs for nXDS: {njobs} (based on {total_frames} frames and {nproc} nproc/job)"
                )
            else:
                # Fallback default
                njobs = 1
                logger.warning(
                    f"Could not calculate njobs and no user setting provided. Defaulting to: {njobs}"
                )

            # Construct Command
            script_path = os.path.join(
                os.path.dirname(__file__), "nxds_process_dataset.py"
            )
            
            # Helper to resolve cluster-safe paths
            cluster_python = os.environ.get("CLUSTER_PYTHON")
            cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")
            
            if cluster_python and cluster_root:
                python_exe = cluster_python
                relative_script_path = "image_viewer/plugins/nxds/nxds_process_dataset.py"
                execution_script = os.path.join(cluster_root, relative_script_path)
            else:
                python_exe = sys.executable
                execution_script = script_path

            # Common args
            command_list = [
                python_exe,
                execution_script,
                "--master_file",
                self.master_file,
                "--proc_dir",
                str(proc_dir),
                "--redis_key",
                results_key,
                "--status_key",
                status_key,
                "--nproc",
                str(nproc),
                "--njobs",
                str(njobs),
            ]

            if self.kwargs.get("nxds_powder"):
                command_list.append("--powder")
            
            if self.kwargs.get("nxds_native"):
                command_list.append("--native")

            res_val = self.kwargs.get("nxds_resolution")
            if res_val:
                command_list.extend(["--resolution", str(res_val)])

            if self.kwargs.get("nxds_space_group"):
                command_list.extend(["--space_group", self.kwargs["nxds_space_group"]])
            unit_cell_value = self.kwargs.get("nxds_unit_cell")
            if unit_cell_value:
                # Since run_command joins the list into a string for a shell script,
                # we must explicitly quote the unit cell string to prevent the shell
                # from splitting it into multiple arguments.
                quoted_unit_cell = f"'{sanitize_unit_cell(unit_cell_value)}'"
                command_list.extend(["--unit_cell", quoted_unit_cell])

            if self.kwargs.get("nxds_reference_hkl"):
                command_list.extend(
                    ["--reference_hkl", self.kwargs["nxds_reference_hkl"]]
                )

            # --- Forward DB Logging Info ---
            if self.metadata.get("primary_group"):
                command_list.extend(["--group_name", str(self.metadata["primary_group"])])
            elif self.metadata.get("username"):
                command_list.extend(["--group_name", str(self.metadata["username"])])

            run_prefix = self.metadata.get("run_prefix") or self.kwargs.get("run_prefix")
            if run_prefix:
                command_list.extend(["--run_prefix", str(run_prefix)])

            if self.metadata.get("pi_badge"):
                command_list.extend(["--pi_badge", str(self.metadata["pi_badge"])])
            if self.metadata.get("esaf_id"):
                command_list.extend(["--esaf_number", str(self.metadata["esaf_id"])])
            # Always determine and pass beamline from submission host
            beamline = self.metadata.get("beamline") or get_beamline_from_hostname()
            command_list.extend(["--beamline", str(beamline)])

            # Extra parameters
            extra_params = self.kwargs.get("nxds_extra_params", "")
            if extra_params:
                for line in extra_params.splitlines():
                    line = line.strip()
                    if line:
                        command_list.extend(["--xds_param", f"'{line}'"])

            host = self.redis_conn.connection_pool.connection_kwargs.get("host")
            port = self.redis_conn.connection_pool.connection_kwargs.get("port")
            command_list.extend(["--redis_host", host, "--redis_port", str(port)])

            pre_command_str = f"set -e\n{ProgramConfig.get_setup_command('nxds')}"
            job_name = f"nxds_{master_basename}"

            logger.info(f"[nXDS] Submitting command: {' '.join(command_list)}")

            run_command(
                cmd=command_list,
                pre_command=pre_command_str,
                cwd=str(proc_dir),
                method=run_method,
                job_name=job_name,
                background=True,
                processors=nproc,
                nodes=None,
                walltime="02:00:00",
                memory="16gb",
            )
            self.signals.result.emit(
                "SUBMITTED", f"Submitted nXDS job '{job_name}'", self.master_file
            )

        except Exception as e:
            logger.critical(f"NXDSProcessDatasetWorker failed: {e}", exc_info=True)
            self.signals.error.emit(self.master_file, str(e))
            if "status_key" in locals():
                failed_status = {
                    "status": "FAILED",
                    "timestamp": time.time(),
                    "error": str(e),
                }
                self.redis_conn.set(status_key, json.dumps(failed_status), ex=7 * 24 * 3600)