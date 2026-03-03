# Add near other imports or in a separate workers.py file and import
import os
import traceback
from typing import Optional

import redis
from PyQt5.QtCore import QRunnable, QObject, pyqtSignal, pyqtSlot

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class ProcessingWorkerSignals(QObject):
    finished = pyqtSignal(
        str, str, object
    )  # master_file_basename, status, result_data (dict)
    error = pyqtSignal(str, str)  # master_file_basename, error_message
    progress = pyqtSignal(str, str)  # master_file_basename, message


class ProcessingWorker(QRunnable):
    def __init__(self, config: dict, redis_conn: Optional[redis.Redis]):
        super().__init__()
        self.config = config
        self.redis_conn = redis_conn
        self.signals = ProcessingWorkerSignals()

        # Assuming 'datasets' is a list and we use the first one for master_file info
        # The dialog supports multiple, but current processing logic points to one primary dataset
        if self.config.get("datasets"):
            self.master_file_path = self.config["datasets"][0]["path"]
            self.master_file_basename = os.path.basename(self.master_file_path)
        else:
            # This case should be handled before worker creation, but as a fallback:
            self.master_file_path = "unknown_file"
            self.master_file_basename = "unknown_file"

    @pyqtSlot()
    def run(self):
        if self.master_file_basename == "unknown_file":
            self.signals.error.emit(
                "unknown_file", "No dataset path found in configuration."
            )
            return

        try:
            self.signals.progress.emit(
                self.master_file_basename, "Processing started..."
            )

            pipeline_type = self.config.get("pipeline", "gmcaproc")
            proc_dir = self.config.get("proc_dir")
            # Ensure dataset_config is valid if datasets list might be empty
            dataset_config = (
                self.config["datasets"][0] if self.config.get("datasets") else {}
            )

            result_payload = {
                "status": "unknown",
                "message": "",
                "output_path": proc_dir,
            }

            # IMPORTANT: The actual xds and validate_and_submit calls need to be
            # correctly imported and called here. The sys.path manipulation for gmcaproc
            # must also be handled, potentially by passing the gmcaproc_path in config
            # or ensuring it's in PYTHONPATH.
            # For demonstration, processing is simulated with time.sleep().

            if "experimental" in pipeline_type:
                # current_viewer_path = os.path.dirname(__file__)  # process_data.py
                # gmcaproc_module_path = os.path.join(
                #     os.path.dirname(os.path.dirname(current_viewer_path)), "gmcaproc"
                # )
                # if gmcaproc_module_path not in sys.path:
                #     sys.path.insert(0, gmcaproc_module_path)

                from qp2.pipelines.gmcaproc import HDF5Reader as XDS_HDF5Reader, XDS

                self.signals.progress.emit(
                    self.master_file_basename,
                    f"Preparing XDS for {self.master_file_basename}...",
                )
                dataset = XDS_HDF5Reader(dataset_config["path"])
                xds_instance = XDS(
                    dataset,
                    proc_dir=proc_dir,
                    user_start=dataset_config.get("start_frame"),
                    user_end=dataset_config.get("end_frame"),
                )
                xds_instance.process()
                self.signals.progress.emit(
                    self.master_file_basename, "XDS process() complete."
                )

                # Simulated processing for "experimental"
                # self.signals.progress.emit(
                #     self.master_file_basename, "Simulating XDS processing..."
                # )
                # time.sleep(5)  # Simulate XDS work

                # result_payload["status"] = "success"
                # result_payload["message"] = (
                #     f"Simulated XDS processing and dimple completed for {self.master_file_basename} in {proc_dir}."
                # )
                # self.signals.progress.emit(
                #     self.master_file_basename, "XDS processing finished."
                # )

            else:  # Other pipelines
                from qp2.data_proc.client.client import validate_and_submit

                self.signals.progress.emit(
                    self.master_file_basename,
                    f"Submitting {self.master_file_basename} via server client...",
                )
                server_response = validate_and_submit(self.config)

                # Simulated processing for other pipelines
                # self.signals.progress.emit(
                #     self.master_file_basename, "Simulating server submission..."
                # )
                # time.sleep(3)  # Simulate server work
                # server_response = f"Simulated submission for {self.master_file_basename} with pipeline {pipeline_type} successful."

                # result_payload["status"] = "success"
                # result_payload["message"] = str(server_response)
                # self.signals.progress.emit(
                #     self.master_file_basename, "Server submission finished."
                # )

            # Write to Redis
            # if self.redis_conn:
            #     redis_key = f"analysis:out:data:gmcaproc:{self.master_file_basename}"
            #     try:
            #         self.redis_conn.set(redis_key, json.dumps(result_payload))
            #         self.signals.progress.emit(
            #             self.master_file_basename,
            #             f"Results written to Redis key {redis_key}.",
            #         )
            #         result_payload["redis_key"] = redis_key
            #     except Exception as e:
            #         error_msg_redis = (
            #             f"Failed to write to Redis for {self.master_file_basename}: {e}"
            #         )
            #         self.signals.error.emit(
            #             self.master_file_basename, error_msg_redis
            #         )  # Also emit as error
            #         result_payload["status"] = "error_redis"
            #         result_payload["message"] += f" | Redis Write Error: {e}"
            # else:
            #     result_payload[
            #         "message"
            #     ] += " | Redis output server not available, results not stored."
            #     self.signals.progress.emit(
            #         self.master_file_basename, "Redis output server not available."
            #     )

            self.signals.finished.emit(
                self.master_file_basename, result_payload["status"], result_payload
            )

        except Exception as e:
            tb = traceback.format_exc()
            error_message = (
                f"Processing failed for {self.master_file_basename}: {e}\n{tb}"
            )
            self.signals.error.emit(self.master_file_basename, error_message)

            # Optionally write error status to Redis
            # if self.redis_conn:
            #     redis_key = f"analysis:out:data:gmcaproc:{self.master_file_basename}"
            #     error_payload_redis = {
            #         "status": "failure",
            #         "message": error_message,
            #         "output_path": self.config.get("proc_dir"),
            #     }
            #     try:
            #         self.redis_conn.set(redis_key, json.dumps(error_payload_redis))
            #     except Exception as redis_e:
            #         print(
            #             f"Could not write error status to Redis for {self.master_file_basename}: {redis_e}"
            #         )
