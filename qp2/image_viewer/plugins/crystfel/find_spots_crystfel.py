# qp2/image_viewer/plugins/crystfel/find_spots_crystfel.py
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

from PyQt5.QtCore import QRunnable, pyqtSignal, QObject

# Import the refactored geometry generator
from qp2.image_viewer.plugins.crystfel.crystfel_geometry import (
    generate_crystfel_geometry_file,
)
from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.log.logging_config import get_logger
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)

# This can be a configurable path
QP2_SHARED_ASSETS_DIR = Path(
    os.getenv(
        "QP2_SHARED_ASSETS_DIR", f"/mnt/beegfs/{os.getenv('USER')}/qp2_shared_assets"
    )
)


def reject_spot_by_resolution(line, i_column, n_column, high_res_limit):
    """Filters spots based on resolution, if a high_res_limit is provided.
    i_column: Index of the resolution column in the line.
    n_column: Total number of columns in the line.
    high_res_limit: If provided, only spots with resolution <= high_res_limit are kept.
    Returns True if the spot should be rejected, False if it should be kept.
    """
    if high_res_limit is not None:
        try:
            parts = line.split()
            if len(parts) == n_column:
                res = float(parts[i_column])
                if res <= high_res_limit:  # Only keep spots within the resolution limit
                    return True
        except (ValueError, IndexError):
            logger.warning(
                f"line: {line.strip()}, not expected to contain resolution? Skipping."
            )
    return False


class CrystfelSpotfinderSignals(QObject):
    result = pyqtSignal(str, str, str)
    error = pyqtSignal(str, str)


class CrystfelDataFileWorker(QRunnable):
    """Submits a distributed CrystFEL job for a SINGLE data file."""

    def __init__(
        self,
        file_path,
        start_frame,
        end_frame,
        metadata,
        redis_conn,
        redis_key_prefix,
        proc_dir: Optional[str] = None,
        **kwargs,
    ):
        super().__init__()
        self.file_path = file_path
        self.start_frame = start_frame
        self.end_frame = end_frame
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.proc_dir = proc_dir
        self.kwargs = kwargs
        self.signals = CrystfelSpotfinderSignals()

    def _create_config_hash(self) -> str:
        """Creates a unique hash based on the detector configuration."""
        params = self.metadata.get("params", {})
        config_data = {
            "detector_name": params.get("detector", "unknown"),
            "nx": params.get("nx"),
            "ny": params.get("ny"),
            "beam_x": params.get("beam_x"),
            "beam_y": params.get("beam_y"),
        }
        config_string = json.dumps(config_data, sort_keys=True)
        return hashlib.sha1(config_string.encode("utf-8")).hexdigest()

    def run(self):
        """Generates/retrieves shared assets and submits the processing job."""
        try:

            # --- START: GATEKEEPER LOGIC ---
            # This is a gatekeeper to ensure that only one worker submits a job for a given frame.
            # It uses Redis to check if the job has already been submitted.
            if not self.redis_conn:
                logger.warning(
                    "Redis not available. Cannot check job status or submit."
                )
                return

            master_file = self.metadata["master_file"]

            # 1. Construct BOTH keys using the new schema
            results_key = f"{self.redis_key_prefix}:{master_file}"
            status_key = f"{results_key}:status"  # <-- The new schema
            status_field = str(self.start_frame)

            # 2. Atomically set the status to "SUBMITTED" ONLY if the field doesn't already exist.
            initial_status = {"status": "SUBMITTED", "timestamp": time.time()}
            was_set = self.redis_conn.hsetnx(
                status_key, status_field, json.dumps(initial_status)
            )

            if not was_set:
                # The field already existed. Another worker has claimed this job.
                logger.info(
                    f"Job for {os.path.basename(master_file)} frame {self.start_frame} already submitted. Skipping."
                )
                return  # CRITICAL: Exit immediately

            logger.info(
                f"Successfully claimed job for {os.path.basename(master_file)} frame {self.start_frame}. Proceeding."
            )
            # --- END: GATEKEEPER LOGIC ---

            run_method = "slurm" if is_sbatch_available() else "shell"

            # 1. Generate hash and define shared asset paths
            config_hash = self._create_config_hash()
            assets_dir = QP2_SHARED_ASSETS_DIR / "crystfel"
            assets_dir.mkdir(parents=True, exist_ok=True)
            geom_file_path = assets_dir / f"{config_hash}.geom"
            lock_file_path = assets_dir / f"{config_hash}.geom.lock"

            # 2. Generate the shared geometry and mask file IF it doesn't exist

            if not geom_file_path.exists():

                # The geometry file doesn't exist, so we might need to create it.
                # We must acquire a lock before proceeding.
                lock_fd = None
                try:
                    # 3. Try to acquire the lock atomically.
                    #    os.O_CREAT | os.O_EXCL means: create the file, but only if it does not already exist.
                    #    This is an atomic operation on POSIX-compliant filesystems (like NFS, BeeGFS).
                    lock_fd = os.open(
                        lock_file_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY
                    )

                    # If we get here, we are the ONLY process that holds the lock.
                    logger.info(
                        f"Acquired lock for config hash: {config_hash}. Generating new assets..."
                    )

                    # 4. CRITICAL: Double-check if the file was created by another process
                    #    that held the lock just before us.
                    if not geom_file_path.exists():
                        bad_pixels_file_path = assets_dir / f"{config_hash}_mask.h5"
                        generate_crystfel_geometry_file(
                            master_file_path=self.metadata["master_file"],
                            output_geom_path=str(geom_file_path),
                            bad_pixels_file_path=str(bad_pixels_file_path),
                        )
                    else:
                        logger.info(
                            "Assets were created by another process while waiting for lock. Proceeding."
                        )

                except FileExistsError:
                    # This is not an error. It means another process created the lock file first.
                    # We must now wait for that process to finish and for the lock to be released.
                    logger.info(
                        f"Could not acquire lock for {config_hash}, another process is generating assets. Waiting..."
                    )
                    max_wait_sec = 120
                    wait_interval = 1
                    start_wait = time.time()
                    while lock_file_path.exists():
                        if time.time() - start_wait > max_wait_sec:
                            raise TimeoutError(
                                f"Timed out waiting for lock file {lock_file_path} to be released."
                            )
                        time.sleep(wait_interval)
                    logger.info(
                        "Lock for {config_hash} has been released. Proceeding to use assets."
                    )

                finally:
                    # 5. VERY IMPORTANT: Release the lock.
                    if lock_fd is not None:
                        os.close(lock_fd)
                        os.remove(lock_file_path)
                        logger.debug(f"Released lock for {config_hash}")

            else:
                logger.info(
                    f"Found existing geometry file for this configuration: {geom_file_path}"
                )

            params = self.metadata.get("params", {})

            # resolution limit for displaying spots/reflections
            # These could come from a config file or be hardcoded for the server
            max_res_A = 3.0
            max_reflections = 1000

            # 3. Prepare and submit the job for this specific data file
            script_path = os.path.join(
                os.path.dirname(__file__), "crystfel_process_file.py"
            )
            
            # Cluster-safe logic
            cluster_python = os.environ.get("CLUSTER_PYTHON")
            cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")
            
            if cluster_python and cluster_root:
                python_exe = cluster_python
                relative_script_path = "image_viewer/plugins/crystfel/crystfel_process_file.py"
                execution_script = os.path.join(cluster_root, relative_script_path)
            else:
                python_exe = sys.executable
                execution_script = script_path
            num_frames = (self.end_frame - self.start_frame) + 1

            # The metadata passed to the script is now much smaller
            serializable_metadata = {
                "master_file": self.metadata["master_file"],
                "params": self.metadata["params"],
            }
            nproc_to_request = min(self.kwargs.get("nproc", 8), num_frames)
            command_list = [
                python_exe,
                execution_script,
                "--geometry_file",
                str(geom_file_path),
                "--data_file",
                self.file_path,
                "--start_frame",
                str(self.start_frame),
                "--num_frames",
                str(num_frames),
                "--master_file",
                self.metadata["master_file"],
                "--redis_key",
                results_key,  # Pass the full results key
                "--status_key",
                status_key,  # Pass the full status key
                "--high_res_limit",
                str(max_res_A),
                "--max_reflections",
                str(max_reflections),
                "--nproc",
                str(nproc_to_request),
                "--peak_method",
                self.kwargs.get("peak_method", "peakfinder9"),
                "--min_snr",
                str(self.kwargs.get("min_snr", 4.0)),
                "--min_snr_biggest_pix",
                str(self.kwargs.get("min_snr_biggest_pix", 3.0)),
                "--min_snr_peak_pix",
                str(self.kwargs.get("min_snr_peak_pix", 2.0)),
                "--min_sig",
                str(self.kwargs.get("min_sig", 5.0)),
                "--local_bg_radius",
                str(self.kwargs.get("local_bg_radius", 3)),
            ]
            pdb_file = self.kwargs.get("pdb_file")
            if pdb_file and os.path.exists(pdb_file):
                command_list.extend(["--pdb", pdb_file])

            extra_options_str = self.kwargs.get("extra_options", "")
            if extra_options_str:
                command_list.extend([f"{extra_options_str}"])

            if self.redis_conn:
                host = self.redis_conn.connection_pool.connection_kwargs.get("host")
                port = self.redis_conn.connection_pool.connection_kwargs.get("port")
                command_list.extend(["--redis_host", host, "--redis_port", str(port)])

            pre_command_str = (
                "set -e\n"
                "echo running on machine `hostname` at `date`\n"
                f"{ProgramConfig.get_setup_command('crystfel')}"
            )

            job_name = f"crystfel_{os.path.basename(self.file_path)}"

            if self.proc_dir:
                job_output_dir = self.proc_dir
            else:
                logger.warning(
                    "proc_dir not provided to CrystfelDataFileWorker. Falling back to default directory logic."
                )
                fallback_proc_dir = self.metadata.get("params", {}).get(
                    "proc_dir", f"{os.getenv('HOME')}"
                )
                job_output_dir = os.path.join(fallback_proc_dir, "crystfel_logs")
            os.makedirs(job_output_dir, exist_ok=True)

            run_command(
                cmd=command_list,
                pre_command=pre_command_str,
                cwd=job_output_dir,
                method=run_method,
                job_name=job_name,
                background=True,
                processors=self.kwargs.get("nproc", 8),
                walltime="02:00:00",
                memory="8gb",
            )

            self.signals.result.emit(
                "SUBMITTED", f"Submitted CrystFEL job '{job_name}'", self.file_path
            )

        except Exception as e:
            if "lock_file_path" in locals() and os.path.exists(lock_file_path):
                os.remove(lock_file_path)
            logger.error(
                f"CrystfelDataFileWorker failed to submit job: {e}", exc_info=True
            )
            self.signals.error.emit(self.file_path, str(e))
            ## --- CRITICAL: Update status to FAILED on submission error ---
            if "status_key" in locals() and "status_field" in locals():
                failed_status = {
                    "status": "FAILED",
                    "timestamp": time.time(),
                    "error": str(e),
                }
                self.redis_conn.hset(
                    status_key, status_field, json.dumps(failed_status)
                )
