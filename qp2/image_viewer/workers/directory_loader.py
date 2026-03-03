# qp2/image_viewer/workers/directory_loader.py

import glob
import os
import re

from pyqtgraph.Qt import QtCore

from qp2.log.logging_config import get_logger
from qp2.xio.hdf5_manager import HDF5Reader
from qp2.image_viewer.utils.sort_files import natural_sort_key

logger = get_logger(__name__)


class WorkerSignals(QtCore.QObject):
    """Defines the signals available from a running worker thread."""

    finished = QtCore.pyqtSignal(list)  # Emits list of (reader, params) tuples (Deprecated usage, can be empty list if using found)
    error = QtCore.pyqtSignal(str)
    progress = QtCore.pyqtSignal(str)
    found = QtCore.pyqtSignal(str) # Emits file path when a valid dataset is found
    found_batch = QtCore.pyqtSignal(list) # Emits a list of file paths


class DirectoryLoaderWorker(QtCore.QRunnable):
    """
    Worker thread for finding and opening HDF5 master files in a directory
    without blocking the main UI thread.
    """

    def __init__(
        self,
        directory_paths: list,
        recursive: bool,
        min_images: int = 0,
        max_images: int = None,
        path_contains: str = "",
        path_not_contains: str = "",
    ):
        super().__init__()
        self.directory_paths = directory_paths
        self.recursive = recursive
        self.min_images = min_images
        self.max_images = max_images
        self.path_contains = path_contains
        self.path_not_contains = path_not_contains
        self.signals = WorkerSignals()

    @QtCore.pyqtSlot()
    def run(self):
        try:
            self.signals.progress.emit("Searching for master files...")

            all_paths = []
            for directory_path in self.directory_paths:
                if self.recursive:
                    pattern = os.path.join(directory_path, "**", "*_master.h5")
                    paths = glob.glob(pattern, recursive=True)
                else:
                    pattern = os.path.join(directory_path, "*_master.h5")
                    paths = glob.glob(pattern)
                all_paths.extend(paths)

            if not all_paths:
                self.signals.finished.emit([])
                return

            sorted_paths = sorted(all_paths, key=natural_sort_key)
            total_files = len(sorted_paths)
            self.signals.progress.emit(
                f"Found {total_files} files. Reading metadata..."
            )

            results = [] # kept for backward compatibility if needed, but now we use signals
            batch = []
            BATCH_SIZE = 10 # Reduced batch size to keep UI fluid

            # Parallelize metadata reading similar to load_datasets_parallel
            import concurrent.futures
            
            def process_file_in_worker(path):
                # Filter by path string inclusion/exclusion
                if self.path_contains and self.path_contains not in path:
                    return None
                if self.path_not_contains and self.path_not_contains in path:
                    return None
                    
                try:
                    # Open and initialize in the worker thread
                    reader = HDF5Reader(path, start_timer=False)
                    
                    # Filter by frame count
                    if reader.total_frames < self.min_images:
                        reader.close()
                        return None
                    if self.max_images is not None and reader.total_frames > self.max_images:
                        reader.close()
                        return None
                    
                    # Get params while we are here (worker thread)
                    params = reader.get_parameters()

                    # IMPORTANT: Move to main thread so signals/slots work correctly there
                    if QtCore.QCoreApplication.instance():
                         reader.moveToThread(QtCore.QCoreApplication.instance().thread())
                         
                    return (reader, params)
                except Exception as e:
                    logger.warning(f"Skipping corrupted or unreadable file: {path} - Error: {e}")
                    return None

            batch = []
            
            # Use threads within this runnable
            # Since this QRunnable is already in a thread, we are spawning threads from a thread.
            # This is fine for I/O bound tasks.
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                future_to_path = {executor.submit(process_file_in_worker, p): p for p in sorted_paths}
                
                processed_count = 0
                for future in concurrent.futures.as_completed(future_to_path):
                    processed_count += 1
                    if processed_count % 20 == 0:
                        self.signals.progress.emit(f"Reading metadata... ({processed_count}/{total_files})")
                        
                    result = future.result()
                    if result:
                        batch.append(result)
                        
                        if len(batch) >= BATCH_SIZE:
                            self.signals.found_batch.emit(batch)
                            batch = []
                            # Small sleep to yield GIL/EventLoop if needed
                            QtCore.QThread.msleep(10)
            
            # Emit remaining items
            if batch:
                self.signals.found_batch.emit(batch)

            self.signals.finished.emit([]) # Emit empty list as we handled items individually
        except Exception as e:
            logger.error(f"Error in DirectoryLoaderWorker: {e}", exc_info=True)
            self.signals.error.emit(str(e))
