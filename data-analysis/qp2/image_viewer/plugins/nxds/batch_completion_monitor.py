import json
import time
import uuid

from PyQt5.QtCore import QRunnable, QObject, pyqtSignal

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

CHECK_INTERVAL_SECONDS = 10  # How often to check Redis for status updates


class MonitorSignals(QObject):
    finished = pyqtSignal(list)  # Emits list of successful dataset paths
    error = pyqtSignal(str, list, list)  # error_msg, successful_paths, failed_paths
    progress = pyqtSignal(str)


class BatchCompletionMonitorWorker(QRunnable):
    """
    Monitors a list of nXDS jobs via Redis and signals when all are complete.
    """

    def __init__(self, dataset_paths, redis_conn):
        super().__init__()
        self.signals = MonitorSignals()
        self.dataset_paths = dataset_paths
        self.redis_conn = redis_conn
        self.total_datasets = len(dataset_paths)
        self.batch_id = str(uuid.uuid4())[:8]
        self.is_stopped = False

    def stop(self):
        self.is_stopped = True

    def run(self):
        logger.info(
            f"[{self.batch_id}] Starting to monitor {self.total_datasets} datasets."
        )

        while not self.is_stopped:
            try:
                completed_count = 0
                failed_count = 0
                successful_paths = []
                failed_paths = []

                for path in self.dataset_paths:
                    status_key = f"analysis:out:nxds:{path}:status"
                    status_raw = self.redis_conn.get(status_key)

                    if status_raw:
                        status_data = json.loads(status_raw)
                        status = status_data.get("status")
                        if status == "COMPLETED":
                            completed_count += 1
                            successful_paths.append(path)
                        elif status == "FAILED":
                            failed_count += 1
                            failed_paths.append(path)

                total_finished = completed_count + failed_count
                progress_msg = (
                    f"Monitoring batch [{self.batch_id}]: "
                    f"{total_finished}/{self.total_datasets} jobs finished."
                )
                self.signals.progress.emit(progress_msg)
                logger.debug(progress_msg)

                if total_finished == self.total_datasets:
                    if failed_count > 0:
                        err_msg = f"Batch [{self.batch_id}] finished with {failed_count} failed job(s)."
                        logger.warning(err_msg)
                        self.signals.error.emit(err_msg, successful_paths, failed_paths)
                    else:
                        msg = f"Batch [{self.batch_id}] completed successfully."
                        logger.info(msg)
                        self.signals.finished.emit(successful_paths)
                    break  # Exit the monitoring loop

                time.sleep(CHECK_INTERVAL_SECONDS)

            except Exception as e:
                err_msg = f"Error in monitoring worker [{self.batch_id}]: {e}"
                logger.error(err_msg, exc_info=True)
                self.signals.error.emit(err_msg, [], self.dataset_paths)
                break
