import json
import os
import shlex
import sys
import time

from PyQt5.QtCore import QRunnable, QObject, pyqtSignal


from qp2.image_viewer.utils.redis_cache import save_numpy_array_to_redis
from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from .peak_finding_utils import find_peaks_in_annulus

from qp2.log.logging_config import get_logger


logger = get_logger(__name__)


class PeakFinderSignals(QObject):
    """Signals for the peak finder worker."""

    finished = pyqtSignal(int, object)  # Signal to emit when done, passes peaks

    # These signals are better for the batch worker, providing more context
    result = pyqtSignal(str, str, str)  # file_path, status_code, message
    error = pyqtSignal(str, str)  # file_path, error_message

    STATUS_COMPLETED = "COMPLETED"


class PeakFinderWorker(QRunnable):
    """
    Worker to run peak finding on a SINGLE image and emit the results.
    This is used for the manual "Find Peaks" action.
    """

    def __init__(
        self, image, detector_mask, beam_x, beam_y, frame_index, r1, r2, **kwargs
    ):
        super().__init__()
        self.image = image
        self.frame_index = frame_index
        self.detector_mask = detector_mask
        self.beam_x = beam_x
        self.beam_y = beam_y
        self.r1 = r1
        self.r2 = r2
        self.kwargs = kwargs
        self.signals = PeakFinderSignals()

    def run(self):
        """Execute the peak finding and emit the resulting NumPy array."""
        try:
            peaks = find_peaks_in_annulus(
                self.image,
                self.detector_mask,
                self.beam_x,
                self.beam_y,
                self.r1,
                self.r2,
                **self.kwargs,
            )
            # Emits the raw peak array, just like the original version
            self.signals.finished.emit(self.frame_index, peaks)
        except Exception as e:
            logger.error(f"PeakFinderWorker failed: {e}", exc_info=True)
            # The error signal needs a file_path, but we don't have one.
            # We can emit a generic error or a more specialized one.
            # For simplicity, we'll stick to the original design's error path.
            # A more advanced design would have a separate signal.
            self.signals.error.emit("manual_run", str(e))


class PeakFinderDataFileWorker(QRunnable):
    """
    Submits a distributed spot finding job for a SINGLE data file.
    This worker is lightweight and does not perform the computation itself.
    """

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
        self.file_path = file_path  # Path to the specific _data_...h5 file
        self.start_frame = start_frame
        self.end_frame = end_frame
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_key_prefix = redis_key_prefix
        self.kwargs = kwargs
        self.signals = PeakFinderSignals()
        self.method = self.kwargs.get("method", "auto")

    def run(self):
        """Prepares and submits the spot finding job via run_job."""

        try:

            master_file = self.metadata["master_file"]
            results_key = f"{self.redis_key_prefix}:{master_file}"
            status_key = f"{results_key}:status"
            status_field = str(self.start_frame)

            # --- Gatekeeper Logic ---
            if not self.redis_conn:
                raise ConnectionError("Redis connection is not available.")

            initial_status = {"status": "SUBMITTED", "timestamp": time.time()}
            was_set = self.redis_conn.hsetnx(
                status_key, status_field, json.dumps(initial_status)
            )

            if not was_set:
                logger.info(
                    f"Job for {os.path.basename(master_file)} frame segment {self.start_frame} already submitted. Skipping."
                )
                return

            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
            )

            # 2. Find the path to the script to be executed
            script_path = os.path.join(
                project_root,
                "qp2",
                "image_viewer",
                "plugins",
                "spot_finder",
                "find_spots_process.py",
            )
            
            # Cluster-safe logic
            cluster_python = os.environ.get("CLUSTER_PYTHON")
            cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")
            
            if cluster_python and cluster_root:
                python_exe = cluster_python
                relative_script_path = "image_viewer/plugins/spot_finder/find_spots_process.py"
                execution_script = os.path.join(cluster_root, relative_script_path)
            else:
                python_exe = sys.executable
                execution_script = script_path
                
                if not os.path.exists(script_path):
                    raise FileNotFoundError(
                        f"Could not find processing script at: {script_path}"
                    )

            run_method = self.method
            if run_method == "auto":
                run_method = "slurm" if is_sbatch_available() else "shell"

            # 1. Cache the detector mask in Redis
            mask_redis_key = None
            detector_mask = self.metadata.get("detector_mask")
            detector_name = self.metadata.get("params", {}).get(
                "detector", "unknown_detector"
            )
            if detector_mask is not None and self.redis_conn:
                redis_host = (
                    self.redis_conn.connection_pool.connection_kwargs.get("host")
                    if self.redis_conn
                    else None
                )
                redis_port = (
                    self.redis_conn.connection_pool.connection_kwargs.get("port")
                    if self.redis_conn
                    else 6379
                )

                # Create a unique key based on detector name and shape
                mask_redis_key = (
                    f"{detector_name}_{'x'.join(map(str, detector_mask.shape))}"
                )
                save_numpy_array_to_redis(
                    redis_host, redis_port, mask_redis_key, detector_mask
                )

            # 2. Prepare serializable metadata for the command line
            serializable_metadata = {
                "master_file": self.metadata.get("master_file"),
                "params": self.metadata.get("params"),
                "mask_values": list(self.metadata.get("mask_values", [])),
                "peak_finder_kwargs": self.kwargs,  # Pass all algorithm settings
            }
            data_file_info = {
                "path": self.file_path,
                "start": self.start_frame,
                "end": self.end_frame,
            }

            command_list = [
                python_exe,
                os.path.normpath(execution_script),
                # "--project_root", project_root,
                "--metadata",
                json.dumps(serializable_metadata),
                # "--data_file_info",
                # json.dumps(data_file_info),
                "--start_frame",
                str(self.start_frame),
                "--end_frame",
                str(self.end_frame),
                "--redis_key_prefix",
                self.redis_key_prefix,
                "--status_key",
                status_key,
            ]
            if self.redis_conn:
                host = self.redis_conn.connection_pool.connection_kwargs.get("host")
                port = self.redis_conn.connection_pool.connection_kwargs.get("port")
                command_list.extend(["--redis_host", host, "--redis_port", str(port)])
            if mask_redis_key:
                command_list.extend(["--mask_redis_key", mask_redis_key])

            # 4. Submit the job using run_command
            job_name = f"spotfind_{os.path.basename(self.file_path)}"
            job_output_dir = os.path.join(
                self.metadata.get("params", {}).get("proc_dir", f"{os.getenv('HOME')}"),
                "spotfinder_logs",
            )
            safe_command_string = " ".join(
                shlex.quote(str(arg)) for arg in command_list
            )

            run_command(
                cmd=safe_command_string,
                cwd=job_output_dir,
                method=run_method,
                job_name=job_name,
                background=True,
                walltime="00:15:00",  # Longer time for a whole file
                memory="4gb",
                processors=self.kwargs.get("nproc", 4),  # Pass nproc to slurm
            )

            self.signals.result.emit(
                self.file_path,
                "SUBMITTED",
                f"Submitted job '{job_name}' via {run_method}.",
            )

        except Exception as e:
            logger.error(
                f"PeakFinderDataFileWorker failed to submit job: {e}", exc_info=True
            )
            self.signals.error.emit(self.file_path, str(e))

            if "status_key" in locals() and "status_field" in locals():
                failed_status = {
                    "status": "FAILED",
                    "timestamp": time.time(),
                    "error": f"Submission failed: {e}",
                }
                if self.redis_conn:
                    self.redis_conn.hset(
                        status_key, status_field, json.dumps(failed_status)
                    )
