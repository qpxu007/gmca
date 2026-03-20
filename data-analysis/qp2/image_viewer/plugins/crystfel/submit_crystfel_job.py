# image_viewer/plugins/crystfel/submit_crystfel_job.py
import json
import os
import sys
import time
from pathlib import Path

from PyQt5.QtCore import QRunnable, pyqtSignal, QObject

from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.log.logging_config import get_logger
from qp2.xio.proc_utils import determine_proc_base_dir
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)

class CrystfelJobSignals(QObject):
    result = pyqtSignal(str, str, str)
    error = pyqtSignal(str, str)

class CrystfelProcessDatasetWorker(QRunnable):
    """Submits a single, distributed CrystFEL job for an ENTIRE dataset."""

    def __init__(self, master_file, metadata, redis_conn, redis_key_prefix, **kwargs):
        super().__init__()
        self.master_file = master_file
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.kwargs = kwargs
        self.signals = CrystfelJobSignals()

    def run(self):
        try:
            if not self.redis_conn:
                return
            
            # Redis Keys
            results_key = f"{self.redis_key_prefix}:{self.master_file}"
            status_key = f"{results_key}:status"

            # Check for existing job (Gatekeeper)
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
                    f"Crystfel job for {os.path.basename(self.master_file)} already submitted. Skipping."
                )
                return

            # Determine Output Directory
            if self.kwargs.get("output_proc_dir"):
                proc_dir = Path(self.kwargs.get("output_proc_dir"))
            else:
                # Use common root logic
                user_root = self.kwargs.get("processing_common_proc_dir_root") or self.kwargs.get("crystfel_proc_dir_root")
                proc_base = determine_proc_base_dir(user_root, self.master_file)
                
                master_basename = os.path.splitext(os.path.basename(self.master_file))[0]
                proc_dir = proc_base / "crystfel" / master_basename

            proc_dir.mkdir(parents=True, exist_ok=True)
            self.redis_conn.hset(results_key, "_proc_dir", str(proc_dir))
            logger.info(f"[CrystFEL] Processing directory: {proc_dir}")

            # Environment setup
            pre_command_str = f"set -e\n{ProgramConfig.get_setup_command('crystfel')}"
            
            # Helper to resolve cluster-safe paths
            cluster_python = os.environ.get("CLUSTER_PYTHON")
            cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")
            
            _local_script = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "crystfel_process_dataset.py"
            )
            if cluster_python and cluster_root:
                python_exe = cluster_python
                relative_script_path = "image_viewer/plugins/crystfel/crystfel_process_dataset.py"
                execution_script = os.path.join(cluster_root, relative_script_path)
            else:
                python_exe = sys.executable
                execution_script = _local_script

            # Common args
            command_list = [
                python_exe,
                execution_script,
                "--master_file", self.master_file,
                "--proc_dir", str(proc_dir),
                "--redis_key", results_key,
                "--nproc", str(self.kwargs.get("nproc", 32)),
            ]

            if self.metadata.get("run_prefix"):
                command_list.extend(["--run_prefix", self.metadata["run_prefix"]])
            
            # Optional args from settings
            if self.kwargs.get("peak_method"):
                command_list.extend(["--peak_method", str(self.kwargs["peak_method"])])
            if self.kwargs.get("min_snr"):
                command_list.extend(["--min_snr", str(self.kwargs["min_snr"])])
            if self.kwargs.get("min_snr_biggest_pix"):
                command_list.extend(["--min_snr_biggest_pix", str(self.kwargs["min_snr_biggest_pix"])])
            if self.kwargs.get("min_snr_peak_pix"):
                command_list.extend(["--min_snr_peak_pix", str(self.kwargs["min_snr_peak_pix"])])
            if self.kwargs.get("min_peaks"):
                command_list.extend(["--min_peaks", str(self.kwargs["min_peaks"])])
            if self.kwargs.get("no_non_hits"):
                command_list.append("--no_non_hits")
            
            # Indexing
            if self.kwargs.get("indexing_methods"):
                command_list.extend(["--indexing_methods", self.kwargs["indexing_methods"]])
            if self.kwargs.get("xgandalf_fast"):
                command_list.append("--xgandalf_fast")
            if self.kwargs.get("no_refine"):
                command_list.append("--no_refine")
            if self.kwargs.get("no_check_peaks"):
                command_list.append("--no_check_peaks")

            # New Speed options
            if self.kwargs.get("peakfinder8_fast"):
                command_list.append("--peakfinder8_fast")
            if self.kwargs.get("asdf_fast"):
                command_list.append("--asdf_fast")
            if self.kwargs.get("no_retry"):
                command_list.append("--no_retry")
            if self.kwargs.get("no_multi"):
                command_list.append("--no_multi")
            if self.kwargs.get("push_res"):
                command_list.extend(["--push_res", str(self.kwargs["push_res"])])
            if self.kwargs.get("integration_mode"):
                command_list.extend(["--integration_mode", self.kwargs["integration_mode"]])

            if self.kwargs.get("debug"):
                command_list.append("--debug")

            if self.kwargs.get("min_sig"):
                command_list.extend(["--min_sig", str(self.kwargs["min_sig"])])
            if self.kwargs.get("local_bg_radius"):
                command_list.extend(["--local_bg_radius", str(self.kwargs["local_bg_radius"])])
            
            # PDB / Indexing
            if self.kwargs.get("pdb_file"):
                command_list.extend(["--pdb", self.kwargs["pdb_file"]])
            
            # Extra options
            if self.kwargs.get("extra_options"):
                command_list.extend(["--extra_options", self.kwargs["extra_options"]])

            # Integration radii
            if self.kwargs.get("int_radius"):
                command_list.extend(["--int_radius", str(self.kwargs["int_radius"])])

            # Peakfinder8 specific
            if self.kwargs.get("peakfinder8_threshold") is not None:
                command_list.extend(["--peakfinder8_threshold", str(self.kwargs["peakfinder8_threshold"])])
            if self.kwargs.get("peakfinder8_min_pix_count") is not None:
                command_list.extend(["--peakfinder8_min_pix_count", str(self.kwargs["peakfinder8_min_pix_count"])])
            if self.kwargs.get("peakfinder8_max_pix_count") is not None:
                command_list.extend(["--peakfinder8_max_pix_count", str(self.kwargs["peakfinder8_max_pix_count"])])
            if self.kwargs.get("peakfinder8_auto_threshold"):
                command_list.append("--peakfinder8_auto_threshold")

            # Redis connection info for the worker script
            host = self.redis_conn.connection_pool.connection_kwargs.get("host")
            port = self.redis_conn.connection_pool.connection_kwargs.get("port")
            if host:
                command_list.extend(["--redis_host", host])
            if port:
                command_list.extend(["--redis_port", str(port)])

            # Environment setup (Moved to top of block)
            # pre_command_str = f"set -e\n{ProgramConfig.get_setup_command('crystfel')}"
            
            job_name = f"crystfel_{os.path.basename(self.master_file)}"
            
            # Submit Job
            run_command(
                cmd=command_list,
                pre_command=pre_command_str,
                cwd=str(proc_dir),
                method="slurm" if is_sbatch_available() else "shell",
                job_name=job_name,
                background=True,
                processors=self.kwargs.get("nproc", 32),
                walltime="06:00:00", # Increased from 04:00:00
                memory="32gb",
            )
            
            self.signals.result.emit(
                self.master_file, "SUBMITTED", f"Submitted CrystFEL job '{job_name}'"
            )

        except Exception as e:
            logger.error(f"CrystfelProcessDatasetWorker failed: {e}", exc_info=True)
            self.signals.error.emit(self.master_file, str(e))
            if "status_key" in locals():
                failed_status = {
                    "status": "FAILED",
                    "timestamp": time.time(),
                    "error": str(e),
                }
                self.redis_conn.set(status_key, json.dumps(failed_status), ex=7 * 24 * 3600)
