# qp2/image_viewer/plugins/xia2_ssx/submit_xia2_ssx_job.py
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


class Xia2SSXJobSignals(QObject):
    result = pyqtSignal(str, str, str)
    error = pyqtSignal(str, str)


class Xia2SSXProcessDatasetWorker(QRunnable):
    """Submits a single xia2.ssx job for an ENTIRE dataset."""

    def __init__(self, master_file, metadata, redis_conn, redis_key_prefix, **kwargs):
        super().__init__()
        self.master_file = master_file
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.kwargs = kwargs
        self.signals = Xia2SSXJobSignals()

    def run(self):
        try:
            if not self.redis_conn:
                return

            # --- 1. Identify all involved datasets and their status keys ---
            all_files = [self.master_file] + self.kwargs.get("extra_data_files", [])
            status_keys = []
            
            for f in all_files:
                # Key structure: analysis:out:xia2_ssx:{path}:status
                r_key = f"{self.redis_key_prefix}:{f}"
                s_key = f"{r_key}:status"
                status_keys.append(s_key)

            # --- 2. Set Initial Status for ALL datasets ---
            initial_status = {"status": "SUBMITTED", "timestamp": time.time()}
            
            for i, s_key in enumerate(status_keys):
                if self.kwargs.get("force_rerun", False):
                    logger.info(f"Force rerun: Clearing status for {all_files[i]}")
                    self.redis_conn.delete(s_key)
                    success = self.redis_conn.set(s_key, json.dumps(initial_status), ex=7 * 24 * 3600)
                else:
                    success = self.redis_conn.set(s_key, json.dumps(initial_status), ex=7 * 24 * 3600, nx=True)
                
                if not success and i == 0:
                     # Only skip if the PRIMARY dataset is already submitted? 
                     # Or should we check all? 
                     # Current logic: if primary fails, we assume job exists.
                     # But with force_rerun, we proceed.
                     if not self.kwargs.get("force_rerun", False):
                         logger.info(f"Job for {os.path.basename(self.master_file)} already submitted. Skipping.")
                         return

            # --- 3. Determine Output Directory ---
            if self.kwargs.get("output_proc_dir"):
                proc_dir = Path(self.kwargs.get("output_proc_dir"))
            else:
                # Use common root logic
                user_root = self.kwargs.get("processing_common_proc_dir_root") or self.kwargs.get("xia2_ssx_proc_dir_root")
                proc_base = determine_proc_base_dir(user_root, self.master_file)
                
                master_basename = os.path.splitext(os.path.basename(self.master_file))[0]
                
                if len(all_files) > 1:
                    basenames = [os.path.splitext(os.path.basename(f))[0] for f in all_files]
                    common = os.path.commonprefix(basenames)
                    if common:
                        dir_name = f"{common}_{len(all_files)}datasets"
                    else:
                        dir_name = f"{master_basename}_{len(all_files)}datasets"
                else:
                    dir_name = master_basename

                proc_dir = proc_base / "xia2_ssx" / dir_name

            proc_dir.mkdir(parents=True, exist_ok=True)
            
            # Record output dir for primary dataset (others might share it?)
            # Ideally record for all?
            # Existing code only recorded for master.
            # Let's record for all to be safe for "Open Directory" feature.
            for f in all_files:
                 r_key = f"{self.redis_key_prefix}:{f}"
                 self.redis_conn.hset(r_key, "_proc_dir", str(proc_dir))

            # --- 4. Construct Command ---
            script_path = os.path.join(
                os.path.dirname(__file__), "xia2_ssx_process_dataset.py"
            )
            script_path = os.path.normpath(script_path)
            
            # Helper to resolve cluster-safe paths
            cluster_python = os.environ.get("CLUSTER_PYTHON")
            cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")
            
            if cluster_python and cluster_root:
                python_exe = cluster_python
                relative_script_path = "image_viewer/plugins/xia2_ssx/xia2_ssx_process_dataset.py"
                execution_script = os.path.join(cluster_root, relative_script_path)
            else:
                python_exe = sys.executable
                execution_script = script_path

            command_list = [
                python_exe,
                execution_script,
                "--data",
                self.master_file,
                "--work_dir",
                str(proc_dir),
                "--beamline",
                get_beamline_from_hostname(),
                "--redis_host",
                self.redis_conn.connection_pool.connection_kwargs.get("host"),
                "--redis_port",
                str(self.redis_conn.connection_pool.connection_kwargs.get("port")),
            ]
            
            # Pass all status keys
            for sk in status_keys:
                command_list.extend(["--status_key", sk])

            # Pass extra data files
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
            if self.kwargs.get("xia2_ssx_space_group"):
                command_list.extend(["--space_group", self.kwargs["xia2_ssx_space_group"]])
            
            if self.kwargs.get("xia2_ssx_d_min"):
                command_list.extend(["--d_min", str(self.kwargs["xia2_ssx_d_min"])])
            
            if self.kwargs.get("xia2_ssx_d_max"):
                command_list.extend(["--d_max", str(self.kwargs["xia2_ssx_d_max"])])
            
            if self.kwargs.get("xia2_ssx_native"):
                command_list.extend(["--native"])
            
            if self.kwargs.get("xia2_ssx_steps"):
                command_list.extend(["--steps", self.kwargs["xia2_ssx_steps"]])

            if self.kwargs.get("xia2_ssx_max_lattices"):
                command_list.extend(["--max_lattices", str(self.kwargs["xia2_ssx_max_lattices"])])
            
            if self.kwargs.get("xia2_ssx_min_spots"):
                command_list.extend(["--min_spots", str(self.kwargs["xia2_ssx_min_spots"])])

            if self.kwargs.get("xia2_ssx_override_geometry"):
                beam_x = self.kwargs.get("xia2_ssx_beam_x")
                beam_y = self.kwargs.get("xia2_ssx_beam_y")
                distance = self.kwargs.get("xia2_ssx_distance")
                if beam_x and beam_y:
                    command_list.extend(["--beam_x", str(beam_x), "--beam_y", str(beam_y)])
                if distance:
                    command_list.extend(["--distance", str(distance)])


            unit_cell_value = self.kwargs.get("xia2_ssx_unit_cell")                                                                                                                                               
            if unit_cell_value:
                # Since run_command joins the list into a string for a shell script,
                # we must explicitly quote the unit cell string to prevent the shell
                # from splitting it into multiple arguments.
                quoted_unit_cell = f"'{sanitize_unit_cell(unit_cell_value)}'"
                command_list.extend(["--unit_cell", quoted_unit_cell])

            if self.kwargs.get("xia2_ssx_model"):
                command_list.extend(["--model", self.kwargs["xia2_ssx_model"]])
            if self.kwargs.get("xia2_ssx_reference_hkl"):
                command_list.extend(["--reference_hkl", self.kwargs["xia2_ssx_reference_hkl"]])
            if self.kwargs.get("xia2_ssx_nproc"):
                command_list.extend(["--nproc", str(self.kwargs["xia2_ssx_nproc"])])
            if self.kwargs.get("xia2_ssx_njobs"):
                command_list.extend(["--njobs", str(self.kwargs["xia2_ssx_njobs"])])

            master_basename = os.path.splitext(os.path.basename(self.master_file))[0]
            job_name = f"xia2_ssx_{master_basename}"

            run_command(
                cmd=command_list,
                cwd=str(proc_dir),
                method="slurm" if is_sbatch_available() else "shell",
                job_name=job_name,
                background=True,
                processors=self.kwargs.get("xia2_ssx_nproc", 8)
                * self.kwargs.get(
                    "xia2_ssx_njobs", 4
                ),
                nodes=1,
                walltime="06:00:00",
                memory="64gb", # SSX might need more memory
                pre_command=ProgramConfig.get_setup_command('dials'),
            )
            self.signals.result.emit(
                "SUBMITTED", f"Submitted xia2.ssx job '{job_name}'", self.master_file
            )

        except Exception as e:
            logger.error(f"Xia2SSXProcessDatasetWorker failed: {e}", exc_info=True)
            self.signals.error.emit(self.master_file, str(e))
            if "status_keys" in locals():
                failed_status = {
                    "status": "FAILED",
                    "timestamp": time.time(),
                    "error": str(e),
                }
                for sk in status_keys:
                    self.redis_conn.set(sk, json.dumps(failed_status), ex=7 * 24 * 3600)



class Xia2SSXDistributedWorker(QRunnable):
    """Submits a distributed xia2.ssx workflow with individual jobs per dataset."""

    def __init__(self, master_file, metadata, redis_conn, redis_key_prefix, **kwargs):
        super().__init__()
        self.master_file = master_file
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.kwargs = kwargs
        self.signals = Xia2SSXJobSignals()

    def run(self):
        try:
            if not self.redis_conn:
                return

            # --- 1. Identify all involved datasets and their status keys ---
            all_files = [self.master_file] + self.kwargs.get("extra_data_files", [])
            status_keys = []
            
            for f in all_files:
                r_key = f"{self.redis_key_prefix}:{f}"
                s_key = f"{r_key}:status"
                status_keys.append(s_key)

            # --- 2. Set Initial Status for ALL datasets ---
            initial_status = {"status": "SUBMITTED", "timestamp": time.time()}
            
            for i, s_key in enumerate(status_keys):
                if self.kwargs.get("force_rerun", False):
                    self.redis_conn.delete(s_key)
                    self.redis_conn.set(s_key, json.dumps(initial_status), ex=7 * 24 * 3600)
                else:
                    self.redis_conn.set(s_key, json.dumps(initial_status), ex=7 * 24 * 3600, nx=True)
                
            # --- 3. Determine Output Directory ---
            if self.kwargs.get("output_proc_dir"):
                proc_dir = Path(self.kwargs.get("output_proc_dir"))
            elif "output_dir" in self.kwargs:
                proc_dir = Path(self.kwargs["output_dir"])
            else:
                user_root = self.kwargs.get("processing_common_proc_dir_root") or self.kwargs.get("xia2_ssx_proc_dir_root")
                proc_base = determine_proc_base_dir(user_root, self.master_file)
                master_basename = os.path.splitext(os.path.basename(self.master_file))[0]
                dir_name = f"{master_basename}_{len(all_files)}datasets_dist"
                proc_dir = proc_base / "xia2_ssx" / dir_name
            
            proc_dir.mkdir(parents=True, exist_ok=True)

            for f in all_files:
                  r_key = f"{self.redis_key_prefix}:{f}"
                  self.redis_conn.hset(r_key, "_proc_dir", str(proc_dir))

            # --- 4. Deploy Scripts & Config ---
            self.deploy_scripts(all_files, status_keys, proc_dir)
            
            # --- 5. Launch Orchestrator ---
            # run_command to launch python orchestrator.py
            cmd = [sys.executable, "orchestrator.py"]
            
            run_command(
                cmd=cmd,
                cwd=str(proc_dir),
                method="shell", # Shell background job
                job_name="xia2_orchestrator",
                background=True
            )

            self.signals.result.emit(
                "SUBMITTED", f"Submitted Distributed Orchestrator", str(proc_dir)
            )

        except Exception as e:
            logger.error(f"Xia2SSXDistributedWorker failed: {e}", exc_info=True)
            self.signals.error.emit(self.master_file, str(e))
            failed_status = {"status": "FAILED", "timestamp": time.time(), "error": str(e)}
            if "status_keys" in locals():
                for sk in status_keys:
                    self.redis_conn.set(sk, json.dumps(failed_status), ex=7 * 24 * 3600)

    def deploy_scripts(self, all_files, status_keys, proc_dir):
        """Copies script templates and writes job_config.json."""
        import shutil
        import uuid
        
        # Generator unique group ID for this run
        group_id = str(uuid.uuid4())
        
        # 1. Prepare Configuration
        config = {
            "datasets": all_files,
            "status_keys": status_keys,
            "redis_group_id": group_id,
            "redis_host": self.redis_conn.connection_pool.connection_kwargs.get("host"),
            "redis_port": self.redis_conn.connection_pool.connection_kwargs.get("port"),
            "incremental_merging": self.kwargs.get("xia2_ssx_incremental_merging", False),
            "force_reprocessing": self.kwargs.get("xia2_ssx_force_reprocessing", False),
            "nproc": self.kwargs.get("xia2_ssx_nproc", 8),
            "walltime": self.kwargs.get("xia2_ssx_walltime", "10:00:00"),
            # xia2 parameters
            "d_min": self.kwargs.get("xia2_ssx_d_min"),
            "d_max": self.kwargs.get("xia2_ssx_d_max"),
            "native": self.kwargs.get("xia2_ssx_native", True),
            "max_lattices": self.kwargs.get("xia2_ssx_max_lattices", 3),
            "min_spots": self.kwargs.get("xia2_ssx_min_spots", 10),
            "space_group": self.kwargs.get("xia2_ssx_space_group"),
            "unit_cell": sanitize_unit_cell(self.kwargs.get("xia2_ssx_unit_cell")) if self.kwargs.get("xia2_ssx_unit_cell") else None,
            "reference_hkl": self.kwargs.get("xia2_ssx_reference_hkl"),
            "steps": self.kwargs.get("xia2_ssx_steps", "find_spots+index+integrate"),
            "model_pdb": self.kwargs.get("xia2_ssx_model"),
            "setup_cmd": ProgramConfig.get_setup_command('dials'),
            "override_geometry": self.kwargs.get("xia2_ssx_override_geometry", False),
            "beam_x": self.kwargs.get("xia2_ssx_beam_x"),
            "beam_y": self.kwargs.get("xia2_ssx_beam_y"),
            "distance": self.kwargs.get("xia2_ssx_distance"),
        }
        
        # Write config
        with open(proc_dir / "job_config.json", "w") as f:
            json.dump(config, f, indent=4)
            
        # 2. Copy Scripts
        src_dir = Path(__file__).parent / "distributed"
        scripts = ["orchestrator.py", "reduce.py", "integrate.sh", "integrate_wrapper.py", "update_status.py"]
        
        for s in scripts:
            src = src_dir / s
            dst = proc_dir / s
            if src.exists():
                shutil.copy(src, dst)
                os.chmod(dst, 0o755)
            else:
                logger.warning(f"Distributed script template not found: {src}")

        # 3. Create setup_env.sh
        with open(proc_dir / "setup_env.sh", "w") as f:
            f.write("#!/bin/bash\n")
            f.write(config["setup_cmd"] + "\n")
        os.chmod(proc_dir / "setup_env.sh", 0o755)
