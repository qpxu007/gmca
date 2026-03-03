# qp2/image_viewer/workers/recent_datasets_loader.py

import os
import concurrent.futures

from pyqtgraph.Qt import QtCore

from qp2.log.logging_config import get_logger
from qp2.xio.hdf5_manager import HDF5Reader

logger = get_logger(__name__)

class RecentDatasetsWorkerSignals(QtCore.QObject):
    """Signals for the RecentDatasetsLoaderWorker."""
    found_batch = QtCore.pyqtSignal(list)
    finished = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)


class RecentDatasetsLoaderWorker(QtCore.QRunnable):
    """
    Worker thread that fetches recent dataset paths from Redis and
    loads their metadata without blocking the main UI thread.
    Used for seamlessly backfilling the dataset tree.
    """

    def __init__(self, redis_manager, count=50):
        super().__init__()
        self.redis_manager = redis_manager
        self.count = count
        self.signals = RecentDatasetsWorkerSignals()

    @QtCore.pyqtSlot()
    def run(self):
        try:
            logger.debug(f"Fetching up to {self.count} recent datasets from Redis for backfill...")
            # 1. Fetch paths from Redis. This is synchronous and involves network,
            # so it's good we are in a worker thread.
            recent_paths = self.redis_manager.get_recent_dataset_paths(count=self.count)
            
            if not recent_paths:
                self.signals.finished.emit()
                return

            logger.debug(f"Found {len(recent_paths)} recent datasets. Checking metadata...")

            # 2. Extract metadata in parallel
            def process_file_in_worker(path):
                if not os.path.exists(path):
                    return None
                try:
                    reader = HDF5Reader(path, start_timer=False)
                    params = reader.get_parameters()
                    # Move reader to main thread for Qt signal compatibility
                    if QtCore.QCoreApplication.instance():
                         reader.moveToThread(QtCore.QCoreApplication.instance().thread())
                    return (reader, params)
                except Exception as e:
                    logger.debug(f"Skipping backfill load for {path}: {e}")
                    return None

            batch = []
            BATCH_SIZE = 10
            
            # Using threads within this QRunnable thread
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                # Reverse paths so they are loaded chronological: oldest to newest
                future_to_path = {executor.submit(process_file_in_worker, p): p for p in recent_paths[::-1]}
                
                for future in concurrent.futures.as_completed(future_to_path):
                    result = future.result()
                    if result:
                        batch.append(result)
                        
                        if len(batch) >= BATCH_SIZE:
                            self.signals.found_batch.emit(batch)
                            batch = []
                            QtCore.QThread.msleep(10)
            
            if batch:
                self.signals.found_batch.emit(batch)

            self.signals.finished.emit()

        except Exception as e:
            logger.error(f"Error in RecentDatasetsLoaderWorker: {e}", exc_info=True)
            self.signals.error.emit(str(e))
