# qp2/image_viewer/plugins/xds/submit_xds_job.py
import json
import os
import sys
import time
from pathlib import Path

from PyQt5.QtCore import QRunnable, QObject, pyqtSignal

from qp2.data_proc.server.reference_converter import process_reference_data
from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.log.logging_config import get_logger
from qp2.utils.auxillary import sanitize_unit_cell
from qp2.xio.proc_utils import determine_proc_base_dir
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)


class XDSJobSignals(QObject):
    result = pyqtSignal(str, str, str)
    error = pyqtSignal(str, str)


class XDSProcessDatasetWorker(QRunnable):
    """Submits a single, distributed XDS job for an ENTIRE dataset."""

    def __init__(self, master_file, metadata, redis_conn, redis_key_prefix, **kwargs):
        super().__init__()
        self.master_file = master_file
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.kwargs = kwargs
        self.signals = XDSJobSignals()

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
                    f"XDS job for {os.path.basename(self.master_file)} already submitted. Skipping."
                )
                return

            master_basename = os.path.splitext(os.path.basename(self.master_file))[0].replace("_master", "")

            if self.kwargs.get("output_proc_dir"):
                proc_dir = Path(self.kwargs.get("output_proc_dir"))
            else:
                # Use common root logic
                user_root = self.kwargs.get("processing_common_proc_dir_root") or self.kwargs.get("xds_proc_dir_root")
                proc_base = determine_proc_base_dir(user_root, self.master_file)
                
                proc_dir = proc_base / "xds" / master_basename

            proc_dir.mkdir(parents=True, exist_ok=True)

            # Determine correct script based on mode
            extra_data_files = self.kwargs.get("extra_data_files", [])
            if extra_data_files:
                relative_script_path = "image_viewer/plugins/xds/xscale_process_dataset.py"
                script_basename = "xscale_process_dataset.py"
                logger.info(f"Submitting MERGED XDS job with {len(extra_data_files)+1} datasets.")
            else:
                relative_script_path = "image_viewer/plugins/xds/xds_process_dataset.py"
                script_basename = "xds_process_dataset.py"

            script_path = os.path.join(os.path.dirname(__file__), script_basename)

            # Cluster-safe logic
            cluster_python = os.environ.get("CLUSTER_PYTHON")
            cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")
            
            if cluster_python and cluster_root:
                python_exe = cluster_python
                execution_script = os.path.join(cluster_root, relative_script_path)
            else:
                python_exe = sys.executable
                execution_script = script_path

            ref_hkl = self.kwargs.get("xds_reference_hkl")
            if ref_hkl:
                # Convert and extract info
                # ... existing logic ...
                pass 
                
            # Note: I need to preserve the ref_hkl block which I am not replacing here, 
            # I am checking the context of replace tool.
            # I will only replace the path/command setup block.

            # Re-implementing ref_hkl logic inside replace block to ensure context match if needed?
            # Actually, I can just replace the block UP TO command_list definition.
            
            # Let's target lines 75-86 (script_path logic) AND lines 99-100 (command_list setup)
            # The ref_hkl logic is between them (lines 88-97).
            # I will use separate chunks or include it.
            # Including it is safer to align variable references.
            
            # Wait, ref_hkl logic uses `self.kwargs`.
            
            # Let's try replacing JUST the script_path definition area first.
            

            ref_hkl = self.kwargs.get("xds_reference_hkl")
            if ref_hkl:
                # Convert and extract info
                conv_path, ref_sg, ref_cell = process_reference_data(ref_hkl)

                # Update paths and fill in gaps
                self.kwargs["xds_reference_hkl"] = conv_path
                if not self.kwargs.get("xds_space_group") and ref_sg:
                    self.kwargs["xds_space_group"] = ref_sg
                if not self.kwargs.get("xds_unit_cell") and ref_cell:
                    self.kwargs["xds_unit_cell"] = ref_cell

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
            ]
            
            # For merged job, append extra files as additional --master_file args
            if extra_data_files:
                for edf in extra_data_files:
                    command_list.extend(["--master_file", str(edf)])

            if self.metadata.get("primary_group"):
                command_list.extend(
                    ["--group_name", str(self.metadata.get("primary_group"))]
                )
            elif self.metadata.get("username"):
                # Fallback if primary_group is missing but username exists
                command_list.extend(
                    ["--group_name", str(self.metadata.get("username"))]
                )

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

            # Add all kwargs from settings dialog
            if self.kwargs.get("xds_native"):
                command_list.append("--native")
            if self.kwargs.get("xds_space_group"):
                command_list.extend(["--space_group", self.kwargs["xds_space_group"]])
            if self.kwargs.get("xds_unit_cell"):
                command_list.extend(
                    [
                        "--unit_cell",
                        f"'{sanitize_unit_cell(self.kwargs['xds_unit_cell'])}'",
                    ]
                )
            if self.kwargs.get("xds_resolution"):
                command_list.extend(
                    ["--resolution", str(self.kwargs["xds_resolution"])]
                )
            if self.kwargs.get("xds_reference_hkl"):
                command_list.extend(
                    ["--reference_hkl", self.kwargs["xds_reference_hkl"]]
                )
            if self.kwargs.get("xds_model_pdb"):
                command_list.extend(["--model", self.kwargs["xds_model_pdb"]])
            if self.kwargs.get("xds_nproc"):
                command_list.extend(["--nproc", str(self.kwargs["xds_nproc"])])
            if self.kwargs.get("xds_njobs"):
                command_list.extend(["--njobs", str(self.kwargs["xds_njobs"])])
            
            # Always determine and pass beamline from submission host
            beamline = self.kwargs.get("beamline") or get_beamline_from_hostname()
            command_list.extend(["--beamline", str(beamline)])
            
            if self.kwargs.get("xds_start"):
                command_list.extend(["--start", str(self.kwargs["xds_start"])])
            if self.kwargs.get("xds_end"):
                command_list.extend(["--end", str(self.kwargs["xds_end"])])
            # Only add optimization flag for non-merge jobs (xds_process_dataset.py)
            if self.kwargs.get("xds_optimization") and not extra_data_files:
                command_list.append("--optimization")
            if extra_data_files and self.kwargs.get("xds_merge_method"):
                command_list.extend(["--merge_method", str(self.kwargs["xds_merge_method"])])

            # Extra parameters
            extra_params = self.kwargs.get("xds_extra_params", "")
            if extra_params:
                for line in extra_params.splitlines():
                    line = line.strip()
                    if line:
                        command_list.extend(["--xds_param", f"'{line}'"])

            host = self.redis_conn.connection_pool.connection_kwargs.get("host")
            port = self.redis_conn.connection_pool.connection_kwargs.get("port")
            if host and port:
                command_list.extend(["--redis_host", host, "--redis_port", str(port)])

            pre_command_str = f"set -e\n{ProgramConfig.get_setup_command('xds')}"
            job_name = f"xds_{master_basename}"

            logger.debug(f"Submitting XDS job with kwargs: {self.kwargs}")
            logger.debug(f"XDS Command List: {command_list}")

            run_command(
                cmd=command_list,
                pre_command=pre_command_str,
                cwd=str(proc_dir),
                method="slurm" if is_sbatch_available() else "shell",
                job_name=job_name,
                background=True,
                processors=self.kwargs.get("xds_nproc"),
                nodes=None,
                walltime="02:00:00",
                memory="16gb",
            )
            self.signals.result.emit(
                "SUBMITTED", f"Submitted XDS job '{job_name}'", self.master_file
            )

        except Exception as e:
            logger.error(f"XDSProcessDatasetWorker failed: {e}", exc_info=True)
            self.signals.error.emit(self.master_file, str(e))
            if "status_key" in locals():
                failed_status = {
                    "status": "FAILED",
                    "timestamp": time.time(),
                    "error": str(e),
                }
                self.redis_conn.set(status_key, json.dumps(failed_status), ex=7 * 24 * 3600)

