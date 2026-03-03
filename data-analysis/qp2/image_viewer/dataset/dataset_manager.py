# In qp2/image_viewer/dataset/dataset_manager.py

import os
import re
import time
from collections import OrderedDict  # Import OrderedDict
from typing import Dict, List, Optional, Set

from PyQt5.QtCore import QObject, pyqtSignal

from qp2.log.logging_config import get_logger
from qp2.xio.hdf5_manager import HDF5Reader

logger = get_logger(__name__)

# Define a reasonable limit for the number of readers to keep in memory
MAX_CACHED_READERS = 200


class DatasetManager(QObject):
    """
    Manages all loaded datasets, grouping them into a two-level hierarchy:
    Sample -> Run -> Datasets.
    Includes an LRU cache to limit the number of open HDF5Reader instances.
    """

    runs_changed = pyqtSignal()

    def __init__(self):
        super().__init__()
        # {sample_prefix: {'runs': {run_prefix: {'datasets': [HDF5Reader]}}, 'creation_time': float}}
        self.samples: Dict[str, Dict] = {}
        # --- MODIFICATION: Use OrderedDict for LRU cache behavior ---
        self.reader_cache: OrderedDict[str, HDF5Reader] = OrderedDict()

    def _add_to_cache(self, reader: HDF5Reader):
        """
        Adds a reader to the LRU cache, evicting the oldest if the limit is reached.
        """
        master_path = reader.master_file_path
        
        # If already in cache, just refresh position
        if self.reader_cache.get(master_path) is reader:
            self.reader_cache.move_to_end(master_path)
            return

        # Evict if full
        if len(self.reader_cache) >= MAX_CACHED_READERS:
            if master_path not in self.reader_cache:
                oldest_path, reader_to_close = self.reader_cache.popitem(last=False)
                logger.debug(
                    f"Reader cache limit reached. Closing oldest reader for: {os.path.basename(oldest_path)}"
                )
                if reader_to_close:
                    try:
                        reader_to_close.close()
                    except Exception as e:
                        logger.warning(f"Error closing evicted reader {oldest_path}: {e}")

        self.reader_cache[master_path] = reader
        self.reader_cache.move_to_end(master_path)

    def add_dataset(self, reader: HDF5Reader, metadata: dict):
        """
        Adds a single dataset.
        """
        if self._add_dataset_no_emit(reader, metadata):
            self.runs_changed.emit()

    def add_datasets(self, items: List[tuple]):
        """
        Adds multiple datasets at once to minimize UI updates.
        items: List of (reader, metadata) tuples.
        """
        tree_changed = False
        for reader, metadata in items:
            if self._add_dataset_no_emit(reader, metadata):
                tree_changed = True
        
        if tree_changed:
            self.runs_changed.emit()

    def _add_dataset_no_emit(self, reader: HDF5Reader, metadata: dict) -> bool:
        """
        Internal method to add a dataset without emitting signals.
        Returns True if the tree structure changed (new sample or run), False otherwise.
        """
        master_path = reader.master_file_path

        # Manage Cache
        self._add_to_cache(reader)

        filename = os.path.basename(master_path).replace("_master.h5", "")

        # First, check if metadata provides an explicit run_prefix
        run_prefix = metadata.get("run_prefix")

        if run_prefix:
            # If metadata provides the run_prefix, derive the sample_prefix from it.
            # Example: run_prefix="my_sample_run01" -> sample_prefix="my_sample"
            match = re.match(r"(.*)_run\d{1,2}", run_prefix)
            if match:
                sample_prefix = match.group(1)
            else:
                # Fallback for generic prefixes like "Q2_scr"
                sample_prefix = (
                    run_prefix.rsplit("_", 1)[0] if "_" in run_prefix else run_prefix
                )
        else:

            # If no metadata, parse the filename.

            # 1. Standard "_run##" pattern.
            match = re.match(r"(.*)_run(\d{1,2})", filename)
            if match:
                sample_prefix = match.group(1)
                run_prefix = f"{sample_prefix}_run{match.group(2)}"

            # 2. NEW: Specific handling for Screen (_scr_) datasets
            elif "_scr_" in filename:
                # Example: A2_1_scr_-180 -> Sample: A2_1, Run: A2_1_scr
                # Example: A3_scr_-90    -> Sample: A3,   Run: A3_scr
                try:
                    # Split at _scr_ to isolate the sample name
                    sample_prefix = filename.split("_scr_")[0]
                    # Group all angles for this screen together under one run
                    run_prefix = f"{sample_prefix}_scr"
                except IndexError:
                    # Fallback if split fails unexpectedly
                    run_prefix = filename
                    sample_prefix = "Uncategorized Samples"

            # 3. Generic "_##" pattern (fallback for standard numbering).
            else:
                match = re.match(r"(.+)_(\d+)$", filename)
                if match:
                    run_prefix = match.group(1)
                    sample_prefix = (
                        run_prefix.rsplit("_", 1)[0]
                        if "_" in run_prefix
                        else run_prefix
                    )
                else:
                    # 4. Final Fallback
                    run_prefix = filename
                    sample_prefix = "Uncategorized Samples"

        logger.info(
            f"Categorizing dataset under Sample: '{sample_prefix}', Run: '{run_prefix}'"
        )

        # --- MODIFICATION: Handle adding to the tree, replacing if necessary ---
        was_new_to_tree = False
        if sample_prefix not in self.samples:
            self.samples[sample_prefix] = {"runs": {}, "creation_time": 0}
            was_new_to_tree = True

        self.samples[sample_prefix]["creation_time"] = time.time()

        if run_prefix not in self.samples[sample_prefix]["runs"]:
            self.samples[sample_prefix]["runs"][run_prefix] = {"datasets": []}
            was_new_to_tree = True

        datasets_list = self.samples[sample_prefix]["runs"][run_prefix]["datasets"]

        # Search for an existing (potentially closed) reader and replace it.
        try:
            # Use samefile for robust comparison across different path representations
            idx = next(
                i
                for i, r in enumerate(datasets_list)
                if os.path.samefile(r.master_file_path, master_path)
            )
            datasets_list[idx] = reader  # Replace the old instance
            logger.debug(f"Replaced existing reader instance for {master_path}")
        except (StopIteration, FileNotFoundError):
            # If not found, it's a new entry for this run's list.
            datasets_list.append(reader)
            was_new_to_tree = True  # Mark that the tree structure changed.

        return was_new_to_tree

    def get_reader(self, master_file_path: str) -> Optional[HDF5Reader]:
        """
        Retrieves an HDF5Reader instance by its master file path.
        If the reader is in the cache, returns it.
        If it's in the tree but evicted (closed), re-opens it, updates the cache and tree.
        """
        # 1. Check Cache
        reader = self.reader_cache.get(master_file_path)
        if reader:
            self.reader_cache.move_to_end(master_file_path)
            return reader

        # 2. Search in Tree (Slow path for evicted items)
        # We need to find which sample/run contains this file to update the list in place.
        for sample_prefix, sample_data in self.samples.items():
            for run_prefix, run_info in sample_data.get("runs", {}).items():
                datasets = run_info["datasets"]
                for idx, r in enumerate(datasets):
                    # Check if this is the dataset we are looking for
                    try:
                        if os.path.samefile(r.master_file_path, master_file_path):
                            # Found it! It must be the closed/evicted instance.
                            logger.info(f"Reloading evicted reader from disk: {master_file_path}")
                            
                            try:
                                # Re-instantiate the reader
                                new_reader = HDF5Reader(master_file_path)
                                
                                # Update Tree: Replace the old closed object with the new active one
                                datasets[idx] = new_reader
                                
                                # Update Cache: Add to cache (triggers eviction of something else if full)
                                self._add_to_cache(new_reader)
                                
                                return new_reader
                            except Exception as e:
                                logger.error(f"Failed to reload reader {master_file_path}: {e}")
                                return None
                    except (FileNotFoundError, OSError):
                         # Handle cases where file might have been deleted
                         if r.master_file_path == master_file_path:
                             logger.warning(f"File not found during reload attempt: {master_file_path}")
                             return None
        
        logger.warning(f"Reader not found in cache or tree: {master_file_path}")
        return None

    def get_all_data(self) -> Dict[str, Dict]:
        """Returns the entire sample data structure."""
        return self.samples

    def get_dataset_paths_for_run(self, run_prefix: str) -> List[str]:
        """
        Finds a run across all samples and returns its dataset paths.
        This assumes run prefixes are unique across samples, which should be true.
        """
        for sample_data in self.samples.values():
            if run_prefix in sample_data.get("runs", {}):
                return [
                    reader.master_file_path
                    for reader in sample_data["runs"][run_prefix]["datasets"]
                ]
        return []

    def get_datasets_for_run(self, run_prefix: str) -> List[HDF5Reader]:
        """Finds a run and returns its HDF5Reader instances."""
        for sample_data in self.samples.values():
            if run_prefix in sample_data.get("runs", {}):
                return sample_data["runs"][run_prefix]["datasets"]
        return []

    def get_sample_prefix_for_run(self, run_prefix: str) -> Optional[str]:
        """Finds which sample a given run belongs to."""
        for sample_prefix, sample_data in self.samples.items():
            if run_prefix in sample_data.get("runs", {}):
                return sample_prefix
        return None

    def clear(self):
        """Clears all samples, runs, and datasets from the manager."""
        for reader in self.reader_cache.values():
            reader.close()
        self.samples.clear()
        self.reader_cache.clear()
        self.runs_changed.emit()

    def remove_items(
        self, sample_prefixes: Set[str], run_prefixes: Set[str], dataset_paths: Set[str]
    ):
        """Removes specified samples, runs, or datasets from the manager."""
        # Remove datasets first
        for path in dataset_paths:
            reader = self.reader_cache.pop(path, None)
            if reader:
                reader.close()

        # Iterate over a copy of samples to allow modification
        for sample_prefix, sample_data in list(self.samples.items()):
            if sample_prefix in sample_prefixes:
                # Remove the entire sample
                del self.samples[sample_prefix]
                continue

            runs_data = sample_data.get("runs", {})
            # Iterate over a copy of runs to allow modification
            for run_prefix, run_info in list(runs_data.items()):
                if run_prefix in run_prefixes:
                    del self.samples[sample_prefix]["runs"][run_prefix]
                    continue

                # Filter out removed datasets
                run_info["datasets"] = [
                    r
                    for r in run_info["datasets"]
                    if r.master_file_path not in dataset_paths
                ]
                # If a run becomes empty, remove it
                if not run_info["datasets"]:
                    del self.samples[sample_prefix]["runs"][run_prefix]

            # If a sample becomes empty, remove it
            if not self.samples[sample_prefix]["runs"]:
                del self.samples[sample_prefix]

        self.runs_changed.emit()  # Signal the UI to update

    def remove_single_dataset(self, master_file_path: str):
        """
        Removes a single dataset from the manager by its file path,
        cleaning up the cache and the tree structure.
        """
        if master_file_path not in self.reader_cache:
            return  # Nothing to do

        logger.info(f"Removing single dataset for refresh: {master_file_path}")
        # Close the file handle and remove from cache
        reader_to_remove = self.reader_cache.pop(master_file_path)
        if reader_to_remove:
            reader_to_remove.close()

        # Find and remove the reader from the samples tree
        found_and_removed = False
        for sample_prefix, sample_data in list(self.samples.items()):
            for run_prefix, run_info in list(sample_data.get("runs", {}).items()):
                initial_count = len(run_info["datasets"])
                # Rebuild the list, excluding the reader to be removed
                run_info["datasets"] = [
                    r for r in run_info["datasets"] if r is not reader_to_remove
                ]
                if len(run_info["datasets"]) < initial_count:
                    found_and_removed = True

                # If the run is now empty, remove it
                if not run_info["datasets"]:
                    del self.samples[sample_prefix]["runs"][run_prefix]

            # If the sample is now empty, remove it
            if not self.samples[sample_prefix]["runs"]:
                del self.samples[sample_prefix]

        if found_and_removed:
            self.runs_changed.emit()

    def get_all_datasets(self) -> List[str]:
        """Returns a list of all master file paths currently managed."""
        all_paths = []
        for sample_data in self.samples.values():
            for run_info in sample_data.get("runs", {}).values():
                for reader in run_info["datasets"]:
                    all_paths.append(reader.master_file_path)
        return all_paths
