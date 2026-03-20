import json
import os
import shlex
import shutil
import operator
import sys
import time
import math
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ProcessPoolExecutor, as_completed

# Ensure the project root is on sys.path so 'qp2' is importable when
# this script is executed directly on a Slurm worker node.
def _find_project_root(file_path):
    path = Path(file_path).resolve()
    for parent in path.parents:
        if (parent / "qp2").is_dir():
            return str(parent)
    return None

_project_root = _find_project_root(__file__)
if _project_root and _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import h5py
import numpy as np

try:
    import hdf5plugin
except ImportError:
    pass

from qp2.log.logging_config import get_logger, setup_logging
from qp2.xio.proc_utils import extract_master_prefix

logger = get_logger(__name__)


def h5repack_file(file_path):
    """
    Runs h5repack on the file to reclaim space from deleted objects or optimize layout.
    """
    if not shutil.which("h5repack"):
        logger.warning("h5repack not found in PATH. File size will not be reduced.")
        return False
    
    temp_path = str(file_path) + ".tmp_repack"
    try:
        logger.info(f"Repacking {file_path} to reduce size...")
        subprocess.run(["h5repack", str(file_path), temp_path], check=True)
        os.replace(temp_path, str(file_path))
        logger.info(f"Repack complete. File size reduced.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"h5repack failed: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False
    except Exception as e:
        logger.error(f"Error during repack: {e}")
        return False


def generate_combiner_map(
    redis_conn,
    plugin_name: str,
    metric_name: str,
    condition: str,
    files: Optional[List[str]] = None,
) -> Dict[str, List[int]]:
    """
    Scans Redis hashes for a plugin and selects frames meeting a condition.

    Args:
        redis_conn: Active Redis connection.
        plugin_name: e.g. "dozor"
        metric_name: e.g. "Main Score"
        condition: e.g. "> 10", "<= 0.5"
        files: Optional list of master filenames (or paths) to restrict the search.
               If provided, only these specific files are checked in Redis.

    Returns:
        Mapping of {master_file: [frames]}
    """
    # 1. Parse condition
    ops = {
        ">": operator.gt,
        "<": operator.lt,
        ">=": operator.ge,
        "<=": operator.le,
        "==": operator.eq,
        "!=": operator.ne,
    }

    parts = condition.split()
    if len(parts) != 2:
        raise ValueError(f"Invalid condition format: {condition}. Expected 'op value'")

    op_str, threshold_str = parts
    if op_str not in ops:
        raise ValueError(f"Unsupported operator: {op_str}")

    compare = ops[op_str]
    threshold = float(threshold_str)

    # 2. Determine key prefix
    # Based on analysis_manager.py REDIS_*_KEY_PREFIX
    prefixes = {
        "dozor": "analysis:out:spots:dozor2",
        "spotfinder": "analysis:out:spots:spotfinder",
        "dials": "analysis:out:spots:dials",
    }

    prefix = prefixes.get(plugin_name.lower())
    if not prefix:
        # Fallback to literal if not standard
        prefix = plugin_name

    mapping = {}

    # 3. Determine keys to iterate
    if files:
        logger.info(f"Checking Redis for {len(files)} specific master files with prefix: {prefix}")
        keys_iterator = (f"{prefix}:{f}" for f in files)
    else:
        logger.info(f"Scanning Redis for {plugin_name} results with prefix: {prefix}*")
        keys_iterator = redis_conn.scan_iter(f"{prefix}:*")

    # 4. Iterate through keys (one hash per master file)
    for key in keys_iterator:
        # Skip status keys if scanning (though strict construction in 'files' mode likely avoids this)
        if key.endswith(":status"):
            continue

        # The master file is often part of the key
        # Use slicing to remove the prefix and the colon, ensuring we only affect the start
        master_file = key[len(prefix) + 1 :]

        # Get all fields (frames) from the hash
        results = redis_conn.hgetall(key)
        
        # If explicitly requested file not found in Redis, hgetall returns {}
        if not results:
            if files:
                logger.warning(f"No results found in Redis for explicitly requested file: {master_file}")
            continue

        selected_frames = []

        for frame_id, data_json in results.items():
            try:
                data = json.loads(data_json)
                val = data.get(metric_name)

                if val is not None and compare(float(val), threshold):
                    selected_frames.append(int(frame_id))
            except (json.JSONDecodeError, ValueError, TypeError):
                continue

        if selected_frames:
            mapping[master_file] = sorted(selected_frames)
            logger.info(
                f"Selected {len(selected_frames)} frames from {Path(master_file).name}: {mapping[master_file]}"
            )

    return mapping


def _worker_copy_frames(task_batch: List[dict], output_path: str, frame_shape: tuple, dtype: Any, chunks: tuple):
    """
    Worker process function to copy a batch of frames into a single output data file.
    
    This function runs in an isolated process to bypass the GIL and maximize throughput
    when reading/writing HDF5 data with compression filters (e.g. Bitshuffle/LZ4).
    
    Args:
        task_batch: List of dictionaries describing each frame to copy.
                    Each dict contains: 'src_file', 'src_dset', 'src_idx'.
        output_path: Absolute path to the HDF5 file this worker should create.
        frame_shape: Shape of a single 2D frame (height, width).
        dtype: Numpy data type of the pixel data (e.g., uint16).
        chunks: Chunking configuration for the output dataset.
        
    Returns:
        int: Number of frames successfully written.
    """
    try:
        # Open a new HDF5 file for this batch. 'w' creates a new file, overwriting if exists.
        with h5py.File(output_path, "w") as out_f:
            # Create the standard Nexus structure /entry/data
            data_group = out_f.create_group("/entry/data")
            
            # Create the dataset with the exact size needed for this batch
            # We use Bitshuffle compression as it is standard for Eiger/HDF5 data
            out_dset = data_group.create_dataset(
                "data",
                shape=(len(task_batch),) + frame_shape,
                dtype=dtype,
                chunks=chunks,
                **hdf5plugin.Bitshuffle(),
            )

            # Iterate through the tasks and perform the copy
            for i, task in enumerate(task_batch):
                src_file = task["src_file"]
                src_dset = task["src_dset"]
                src_idx = task["src_idx"]
                
                # Open source file in read-only mode for each frame or block
                # Opening/closing inside the loop is safer for file handles in long-running processes,
                # though slightly slower. For maximal speed on local SSD, keeping open might help,
                # but on networked filesystems, this pattern is robust.
                with h5py.File(src_file, "r") as sf:
                    # Direct slicing copy: reads from source, decompresses, re-compresses, writes to dest
                    out_dset[i] = sf[src_dset][src_idx]
                    
        return len(task_batch)
    except Exception as e:
        # Logger might not be configured in the worker process context depending on setup,
        # so we return 0 to indicate failure to the main process.
        # Ideally, we should print to stderr or configure worker logging.
        print(f"Worker Error for {output_path}: {e}")
        return 0


class DatasetCombiner:
    """
    Combines selected frames from multiple HDF5 datasets into a single unified dataset.
    
    Features:
    - Parallel processing using multiple CPU cores for high throughput.
    - Creates chunks of HDF5 data files to avoid lock contention.
    - Generates a master file using Virtual Datasets (VDS) to link all chunks into a seamless logical view.
    """

    def __init__(self, output_dir: str, output_prefix: str, nproc: int = 8):
        self.output_dir = Path(output_dir)
        self.output_prefix = output_prefix
        self.output_master_path = self.output_dir / f"{output_prefix}_master.h5"
        self.nproc = nproc

        self.max_images_per_file = 1000
        self.metadata_source_master = None

        # Internal state to track progress and created files
        self.total_frames_combined = 0
        self.data_files_created = []
        
        # Cache for frame mapping to avoid re-reading master files repeatedly
        self.frame_map_cache = {}  # {master_file: [(start, end, file_path, dset_path), ...]}
        self.dset_paths = ["/entry/data/data", "/entry/data/raw_data"]
        self.current_mapping = None # Store mapping for metadata reconstruction


    def combine(
        self, mapping: Dict[str, List[int]], images_per_file: int = 1000
    ) -> bool:
        """
        Executes the combination process.

        Args:
            mapping: { master_file_path: [1-based frame numbers], ... }
            images_per_file: Maximum number of images per output data file.
        """
        if not mapping:
            logger.error("Empty mapping provided to DatasetCombiner.")
            return False

        self.current_mapping = mapping


        self.max_images_per_file = images_per_file
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 1. Identify source for metadata (first master in mapping)
        # We need to find the first *existing* master file to use as a template
        first_master = None
        for m in mapping.keys():
            if os.path.exists(m):
                first_master = m
                break
        
        if not first_master:
            logger.error("No valid source master files found on disk.")
            return False

        self.metadata_source_master = first_master

        logger.info(
            f"Starting parallel dataset combination. Metadata source: {Path(first_master).name}"
        )

        # 2. Prepare frame extraction tasks
        # We perform a pre-scan to resolve every requested frame number (1-based)
        # to its physical location (file path, dataset path, index).
        all_source_tasks = []
        for m_path, frames in mapping.items():
            if not frames: continue
            if not os.path.exists(m_path):
                logger.warning(f"Source file not found: {m_path}. Skipping.")
                continue

            # Ensure we have a map for this master file
            self._build_frame_map(m_path)
            
            for f_num in sorted(list(set(frames))):
                src_info = self._get_source_info(m_path, f_num)
                if src_info:
                    data_file_path, dset_path, local_index = src_info
                    all_source_tasks.append({
                        "src_file": data_file_path,
                        "src_dset": dset_path,
                        "src_idx": local_index,
                        "src_master": m_path,
                        "src_frame": f_num,
                    })

        if not all_source_tasks:
            logger.error("No valid frames could be located for combination.")
            return False

        total_to_combine = len(all_source_tasks)
        logger.info(f"Total frames to combine: {total_to_combine}")

        # Keep provenance info for the master file
        self._source_provenance = all_source_tasks

        # 3. Execute parallel copying
        try:
            self._process_frames_parallel(all_source_tasks)
        except Exception as e:
            logger.error(f"Error during parallel frame processing: {e}", exc_info=True)
            return False

        # 4. Create the master file with VDS
        # This links all the newly created data files into one logical dataset.
        try:
            self._create_master_file(self.total_frames_combined)
            # Repack to optimize master file
            h5repack_file(self.output_master_path)
        except Exception as e:
            logger.error(f"Error creating master file: {e}", exc_info=True)
            return False

        logger.info(f"Dataset combination complete. Total frames written: {self.total_frames_combined}")
        logger.info(f"Output Master: {self.output_master_path}")
        return True

    def _process_frames_parallel(self, all_source_tasks: List[dict]):
        """
        Distributes frame extraction tasks to a process pool.
        
        This method groups the extraction tasks into chunks (batches) corresponding
        to the target output files. Each batch is sent to a worker process.
        """
        
        # 1. Inspect the first source file to determine data shape and type.
        # This ensures the output files match the input format.
        first_src = all_source_tasks[0]
        with h5py.File(first_src["src_file"], "r") as f:
            dset = f[first_src["src_dset"]]
            frame_shape = dset.shape[1:]
            dtype = dset.dtype
            chunks = (1,) + frame_shape

        # 2. Divide the flat list of tasks into batches.
        # Each batch will correspond to exactly one physical HDF5 data file.
        batches = []
        for i in range(0, len(all_source_tasks), self.max_images_per_file):
            batches.append(all_source_tasks[i : i + self.max_images_per_file])

        logger.info(f"Distributing tasks into {len(batches)} data files using {self.nproc} processes.")

        # 3. Submit batches to the executor.
        total_combined = 0
        with ProcessPoolExecutor(max_workers=self.nproc) as executor:
            futures = {}
            for idx, batch in enumerate(batches):
                # Deterministic naming: output_data_000001.h5, etc.
                fname = f"{self.output_prefix}_data_{idx+1:06d}.h5"
                fpath = self.output_dir / fname
                self.data_files_created.append(str(fpath))
                
                # Submit the task. Note we pass file paths as strings to avoid pickling issues with objects.
                future = executor.submit(
                    _worker_copy_frames, batch, str(fpath), 
                    frame_shape, dtype, chunks
                )
                futures[future] = fname

            # 4. Wait for completion and track progress.
            for future in as_completed(futures):
                fname = futures[future]
                try:
                    count = future.result()
                    total_combined += count
                    logger.info(f"  [DONE] {fname} written with {count} frames.")
                except Exception as e:
                    logger.error(f"  [FAIL] {fname} failed: {e}")

        self.total_frames_combined = total_combined

    def _build_frame_map(self, master_path):
        """
        Builds a speculative frame map for a master file.
        Populates self.frame_map_cache[master_path] with list of (start, end, file_path, dset_path).
        """
        logger.info(f"Building frame map for {Path(master_path).name}...")
        
        try:
            with h5py.File(master_path, "r") as f:
                # 1. Read nimages
                def read_scalar(path, default=None, dtype=None):
                    if path in f:
                        try:
                            val = f[path][()]
                            if dtype: return dtype(val)
                            return val.item() if hasattr(val, "item") else val
                        except: pass
                    return default

                nimages = read_scalar("/entry/instrument/detector/detectorSpecific/nimages", dtype=int)
                ntrigger = read_scalar("/entry/instrument/detector/detectorSpecific/ntrigger", default=1, dtype=int)
                
                if nimages:
                    total_frames = nimages * ntrigger
                else:
                    logger.warning("Could not read nimages from master file.")
                    return

                # 2. Determine images_per_hdf
                images_per_hdf = 0
                
                # Method A: Direct read of first data file
                prefix = Path(master_path).name.replace("_master.h5", "")
                master_dir = Path(master_path).parent
                first_data_filename = f"{prefix}_data_000001.h5"
                first_data_path = master_dir / first_data_filename
                
                if first_data_path.exists():
                    try:
                        with h5py.File(first_data_path, "r") as df:
                            for dpath in self.dset_paths:
                                if dpath in df:
                                    images_per_hdf = df[dpath].shape[0]
                                    break
                    except Exception as e:
                        logger.warning(f"Failed to read images_per_hdf from data file: {e}")

                # Method B: Heuristic
                if images_per_hdf == 0:
                    frame_time = read_scalar("/entry/instrument/detector/frame_time", dtype=float)
                    if frame_time and frame_time > 1e-9:
                        images_per_hdf = int(math.ceil(0.5 / frame_time))
                        logger.info(f"Calculated heuristic images_per_hdf: {images_per_hdf}")
                
                if images_per_hdf == 0:
                    images_per_hdf = 1000 # Default fallback
                    logger.warning(f"Using fallback images_per_hdf: {images_per_hdf}")

                # 3. Build Map
                frame_map = []
                num_full_files = total_frames // images_per_hdf
                remainder = total_frames % images_per_hdf
                total_files = num_full_files + (1 if remainder > 0 else 0)
                
                current_start = 0
                for i in range(total_files):
                    is_last = (i == total_files - 1)
                    count = remainder if is_last and remainder > 0 else images_per_hdf
                    end_idx = current_start + count
                    filename = f"{prefix}_data_{i+1:06d}.h5"
                    path = str(master_dir / filename)
                    dset = self.dset_paths[0] 
                    frame_map.append((current_start, end_idx, path, dset))
                    current_start = end_idx
                
                self.frame_map_cache[master_path] = frame_map
        except Exception as e:
            logger.error(f"Error building frame map for {master_path}: {e}")

    def _get_source_info(self, master_path, frame_number_1based):
        """
        Returns (data_file_path, dset_path, local_0based_index) for a given 1-based frame number.
        """
        if master_path not in self.frame_map_cache:
            return None
            
        frame_0based = frame_number_1based - 1
        for start, end, path, dset in self.frame_map_cache[master_path]:
            if start <= frame_0based < end:
                return path, dset, frame_0based - start
        
        return None

    def _create_master_file(self, total_frames: int):
        """
        Copies metadata structure and creates a Virtual Dataset (VDS).
        
        The VDS allows the master file to act as a single entry point, transparently
        mapping to the underlying data chunks created by the parallel workers.
        """
        if not self.data_files_created:
            logger.error("No data files created. Cannot create master file.")
            return

        # Identify datasets to reconstruct BEFORE visiting, so we can skip them
        per_frame_paths = self._find_per_frame_datasets(self.metadata_source_master)
        per_frame_paths_set = set(per_frame_paths)


        with h5py.File(self.metadata_source_master, "r") as src:
            # Get shape and dtype from the first output file to initialize VDS layout
            first_data_path = self.data_files_created[0]
            with h5py.File(first_data_path, "r") as f:
                # We know we created it at /entry/data/data
                if "/entry/data/data" in f:
                    ds = f["/entry/data/data"]
                    frame_shape = ds.shape[1:]
                    dtype = ds.dtype
                else:
                    logger.error(f"Could not find /entry/data/data in {first_data_path}")
                    return

            with h5py.File(self.output_master_path, "w") as dst:
                # 1. Copy the entire structure without the data group contents initially
                def copy_visitor(name, obj):
                    # name in visititems usually does not have leading slash (e.g. "entry/data")
                    
                    if isinstance(obj, h5py.Group):
                        if name not in dst:
                            dst.create_group(name)
                            for k, v in obj.attrs.items():
                                dst[name].attrs[k] = v
                    elif isinstance(obj, h5py.Dataset):
                        # Don't copy datasets under /entry/data or specific detector params we'll update
                        # Robust check for path presence
                        if name.startswith("entry/data") or "/entry/data" in name:
                            return
                        if "entry/instrument/detector/detectorSpecific" in name or "/entry/instrument/detector/detectorSpecific" in name:
                            return
                        
                        # Also skip the data link in detector group if it exists, we will recreate it pointing to new VDS
                        if name == "entry/instrument/detector/data" or name.endswith("/entry/instrument/detector/data"):
                            return
                        
                        # Skip per-frame datasets that we will reconstruct manually
                        # (Need to check if this dataset is in our list to reconstruct)
                        # optimization: checking equality directly
                        # We need access to the list 'per_frame_paths' here. 
                        # Since visitor is inner function, we can capture it IF we compute it before visiting.
                        if name in per_frame_paths_set:
                            return

                        if name not in dst:
                            src.copy(name, dst, name=name, shallow=True)


                src.visititems(copy_visitor)

                # Copy root-level attributes (visititems doesn't visit root)
                for k, v in src.attrs.items():
                    dst.attrs[k] = v

                # 2. Re-create /entry/data group
                if "/entry/data" not in dst:
                    data_group = dst.create_group("/entry/data")
                    # Copy attributes from source /entry/data if it exists
                    if "/entry/data" in src:
                        for k, v in src["/entry/data"].attrs.items():
                            data_group.attrs[k] = v
                else:
                    data_group = dst["/entry/data"]

                # 3. Create VDS Layout
                layout = h5py.VirtualLayout(shape=(total_frames,) + frame_shape, dtype=dtype)
                
                current_frame_start = 0
                for i, fpath in enumerate(self.data_files_created):
                    rel_path = os.path.basename(fpath)
                    
                    # Inspect file to get exact number of frames (last file might be smaller)
                    with h5py.File(fpath, "r") as f:
                        n_frames_in_file = f["/entry/data/data"].shape[0]
                    
                    vsource = h5py.VirtualSource(rel_path, "/entry/data/data", shape=(n_frames_in_file,) + frame_shape)
                    layout[current_frame_start : current_frame_start + n_frames_in_file] = vsource
                    
                    # Also create the external links data_000001, etc. for compatibility
                    link_name = f"data_{i+1:06d}"
                    data_group[link_name] = h5py.ExternalLink(rel_path, "/entry/data/data")
                    
                    current_frame_start += n_frames_in_file

                # Create the virtual dataset
                dst.create_virtual_dataset("/entry/data/data", layout, fillvalue=0)

                # Ensure standard link /entry/instrument/detector/data -> /entry/data/data
                if "/entry/instrument/detector" in dst:
                    det_group = dst["/entry/instrument/detector"]
                    if "data" in det_group:
                         del det_group["data"]
                    det_group["data"] = h5py.SoftLink("/entry/data/data")

                # 4. Update specific metadata
                # We need to manually copy/update detectorSpecific items since we skipped them above
                det_spec_src = "/entry/instrument/detector/detectorSpecific"
                det_spec_dst = "/entry/instrument/detector/detectorSpecific"
                
                if det_spec_src in src:
                    dst_group = dst.require_group(det_spec_dst)
                    src_group = src[det_spec_src]
                    
                    for k, v in src_group.attrs.items():
                        dst_group.attrs[k] = v
                        
                    for name in src_group:
                        if name in ["nimages", "ntrigger", "flatfield"]:
                            continue # We set nimages/ntrigger manually and skip large/redundant datasets
                        
                        # Check existence and delete if necessary
                        if name in dst_group:
                            del dst_group[name]
                            
                        src.copy(f"{det_spec_src}/{name}", dst_group, name=name)


                    # Update nimages/ntrigger
                    if "nimages" in dst_group:
                        del dst_group["nimages"]
                    dst_group.create_dataset("nimages", data=total_frames, dtype="u8")
                    
                    if "ntrigger" in dst_group:
                        del dst_group["ntrigger"]
                    dst_group.create_dataset("ntrigger", data=1, dtype="u8")

                # 5. Reconstruct per-frame arrays (e.g. omega, chi, phi)
                # Find arrays that match the original nimages and extend them
                per_frame_paths = self._find_per_frame_datasets(self.metadata_source_master)
                if per_frame_paths:
                    logger.info(f"Found {len(per_frame_paths)} per-frame datasets to reconstruct: {per_frame_paths}")
                    self._reconstruct_per_frame_datasets(dst, per_frame_paths)
                else:
                    logger.info("No per-frame datasets found for reconstruction.")

                # 6. Write provenance — source master file and 1-based frame
                #    number for every combined frame, so the original raster
                #    row/column position can be recovered later.
                self._write_provenance(dst)


    def _find_per_frame_datasets(self, master_path: str) -> List[str]:
        """
        Scans the master file for 1D datasets that match the number of images.
        """
        paths = []
        try:
            with h5py.File(master_path, "r") as f:
                # Get nimages
                nimages = None
                if "/entry/instrument/detector/detectorSpecific/nimages" in f:
                    nimages = f["/entry/instrument/detector/detectorSpecific/nimages"][()]
                
                if nimages is None:
                    return []
                
                def visitor(name, obj):
                    if isinstance(obj, h5py.Dataset):
                        # skip data/raw_data/detector data
                        if "entry/data" in name or "entry/instrument/detector/data" in name:
                            return
                        
                        # Check shape
                        if len(obj.shape) == 1 and obj.shape[0] == nimages:
                             # Exclude nimages itself from re-writing (handled separately)
                             if name.endswith("nimages"):
                                 return
                             paths.append(name)

                f.visititems(visitor)
        except Exception as e:
            logger.error(f"Error scanning for per-frame datasets: {e}")
        
        return paths

    def _reconstruct_per_frame_datasets(self, dst_file_obj, dataset_paths: List[str]):
        """
        Reconstructs the given datasets by concatenating values from all source master files.
        """
        if not self.current_mapping:
            logger.warning("No mapping available for metadata reconstruction.")
            return

        # Prepare a list of (master_path, [frames]) tuples in the order we want to write them?
        # WAIT. The order of frames in the OUTPUT file depends on how we processed them.
        # process_frames_parallel iterates:
        #   for m_path, frames in mapping.items():
        # But wait, 'mapping.items()' order is not guaranteed in older python, but is insertion ordered in 3.7+.
        # However, we built 'all_source_tasks' by iterating mapping.items().
        # Then we batched 'all_source_tasks'.
        # So the order of frames in the output data files matches the order we iterated 'mapping'.
        # We must iterate 'mapping' in the EXACT SAME ORDER here.
        
        # 1. Build a consolidated list of all values for each dataset
        # This might be memory intensive if arrays are huge, but usually metadata is small (doubles/ints).
        # We can process one dataset at a time.
        
        for i, dpath in enumerate(dataset_paths):
            logger.info(f"  [{i+1}/{len(dataset_paths)}] Extending dataset: {dpath}")
            
            all_values = []

            
            # Helper to open master files efficiently? 
            # We might reopen the same master file multiple times if we iterate datasets first.
            # But iterating datasets first keeps 'all_values' for one dataset in memory, which is better.
            
            for m_path, frames in self.current_mapping.items():
                 # Filter valid/existing files
                 if not frames or not os.path.exists(m_path):
                     continue
                 
                 # Indices in mapping are 1-based frame numbers.
                 # Python slice needs 0-based indices.
                 # Also, the frames might be non-contiguous (e.g. [1, 5, 10]).
                 # So we need to select specific elements.
                 
                 try:
                     with h5py.File(m_path, "r") as src:
                         if dpath in src:
                             data = src[dpath][()] # Read entire dataset into memory (usually small)
                             
                             # Select specific indices
                             indices = [f - 1 for f in frames if 0 <= f - 1 < len(data)]
                             
                             if indices:
                                 selected = data[indices]
                                 all_values.append(selected)
                             else:
                                 logger.warning(f"No valid frames found for {dpath} in {m_path}")
                         else:
                             # Dataset missing in this master? 
                             # We should probably fill with zeros or skip?
                             # Combining disparate masters might be tricky.
                             # For now, append zeros/nans of appropriate length?
                             # Or just fail?
                             # Let's assume homogeneous masters. If missing, warn and fill with default?
                             logger.warning(f"Dataset {dpath} missing in {m_path}. Filling with zeros.")
                             # We need to know shape/dtype.
                             # Use first master as reference (which we know has it).
                             pass 
                             # If we skip, the final array will be short. 
                             # Better to fill.
                             # BUT we don't know the shape easily without reference.
                             # Let's verify logic: we only picked paths from metadata_source_master.
                             # If others lack it, we are in trouble.
                 except Exception as e:
                     logger.error(f"Error reading {dpath} from {m_path}: {e}")

            if all_values:
                 # Concatenate
                 final_array = np.concatenate(all_values, axis=0)
                 logger.info(f"    -> Reconstructed {dpath} with total length {len(final_array)}")

                 # Ensure parent group exists
                 parent = os.path.dirname(dpath)
                 if parent not in dst_file_obj:
                     dst_file_obj.create_group(parent)

                 dst_file_obj.create_dataset(dpath, data=final_array)

                 # Copy HDF5 attributes from the first source that has this dataset.
                 # NeXus transformation datasets carry critical attrs (@vector,
                 # @transformation_type, @units, @depends_on) that dxtbx needs.
                 for m_path in self.current_mapping.keys():
                     if not os.path.exists(m_path):
                         continue
                     try:
                         with h5py.File(m_path, "r") as src:
                             if dpath in src:
                                 for attr_name, attr_val in src[dpath].attrs.items():
                                     dst_file_obj[dpath].attrs[attr_name] = attr_val
                                 break
                     except Exception as e:
                         logger.warning(f"Could not copy attributes for {dpath}: {e}")




    def _write_provenance(self, dst_file_obj):
        """Write per-frame provenance datasets into /entry/combiner/.

        Stores the source master file path and 1-based frame number for
        every frame in the combined dataset, so the original raster
        position (row/column) can be recovered.
        """
        provenance = getattr(self, "_source_provenance", None)
        if not provenance:
            return

        n = len(provenance)
        grp = dst_file_obj.require_group("/entry/combiner")

        # source_file: variable-length string per frame
        masters = [task["src_master"] for task in provenance]
        dt = h5py.string_dtype()
        grp.create_dataset("source_file", data=masters, dtype=dt)

        # source_frame: 1-based frame number per frame
        frames = np.array([task["src_frame"] for task in provenance], dtype=np.int32)
        grp.create_dataset("source_frame", data=frames)

        logger.info(f"Wrote provenance for {n} frames to /entry/combiner/")

    def _find_data_path(self, f: h5py.File) -> Optional[str]:
        """Finds the dataset path containing the image data."""
        for path in ["/entry/data/data", "/entry/data/raw_data"]:
            if path in f:
                return path
        # Fallback: look for any dataset under /entry/data
        if "/entry/data" in f:
            group = f["/entry/data"]
            for k in group.keys():
                if isinstance(group[k], h5py.Dataset):
                    return f"/entry/data/{k}"
        return None


def main():
    import argparse
    import sys
    import redis
    from qp2.config.servers import ServerConfig

    setup_logging()

    parser = argparse.ArgumentParser(
        description="Combine selected frames from multiple HDF5 datasets in parallel."
    )

    # Mode 1: Static Mapping
    parser.add_argument(
        "--mapping",
        help="JSON string or path to JSON file containing {master_path: [frames]}",
    )

    # Mode 2: Redis Scan
    parser.add_argument("--plugin", help="Plugin to scan (e.g. dozor, spotfinder)")
    parser.add_argument("--metric", help="Metric name to filter by (e.g. 'Main Score')")
    parser.add_argument("--condition", help="Filter condition (e.g. '> 10')")
    parser.add_argument("--redis_host", help="Redis host for scanning")
    parser.add_argument("--files", nargs="*", help="List of specific master files to process")

    # Common params
    parser.add_argument(
        "--prefix", required=True, help="Prefix for the output combined dataset"
    )
    parser.add_argument(
        "--outdir", default=".", help="Output directory (default: current)"
    )
    parser.add_argument(
        "--n", type=int, default=1000, help="Max images per data file (default: 1000)"
    )

    # Slurm/Parallel arguments
    parser.add_argument("--submit", action="store_true", help="Submit job to Slurm")
    parser.add_argument("--time", default="02:00:00", help="Slurm walltime (default: 02:00:00)")
    parser.add_argument("--mem", default="32gb", help="Slurm memory (default: 32gb)")
    parser.add_argument("--nproc", type=int, default=8, help="Number of parallel processes (default: 8)")

    args = parser.parse_args()

    # Handle Slurm submission
    if args.submit:
        try:
            from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
        except ImportError:
            print("Error: Could not import run_job utility. Ensure qp2 package is installed.")
            sys.exit(1)

        if not is_sbatch_available():
            print("Error: sbatch command not found. Cannot submit to Slurm.")
            sys.exit(1)

        # Use cluster-safe Python/paths if available (submitting machine may
        # have a different mount layout than the Slurm worker node).
        cluster_python = os.environ.get("CLUSTER_PYTHON")
        cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")

        if cluster_python and cluster_root:
            python_exe = cluster_python
            relative_script = os.path.join("xio", "hdf5_combiner.py")
            execution_script = os.path.join(cluster_root, relative_script)
        else:
            python_exe = sys.executable
            execution_script = os.path.abspath(__file__)

        # Reconstruct command line with raw values, then shell-quote the
        # entire list before passing to run_command (which joins with
        # plain " ".join(), so spaces/metacharacters must be escaped).
        cmd = [python_exe, execution_script]

        if args.mapping:
            cmd.extend(["--mapping", args.mapping])

        if args.plugin:
            cmd.extend(["--plugin", args.plugin])
        if args.metric:
            cmd.extend(["--metric", args.metric])
        if args.condition:
            cmd.extend(["--condition", args.condition])
        if args.redis_host:
            cmd.extend(["--redis_host", args.redis_host])
        if args.files:
            cmd.append("--files")
            cmd.extend(args.files)

        cmd.extend(["--prefix", args.prefix])
        cmd.extend(["--outdir", args.outdir])
        cmd.extend(["--n", str(args.n)])
        cmd.extend(["--nproc", str(args.nproc)])

        slurm_cmd = [shlex.quote(c) for c in cmd]

        # Environment setup for worker node.
        # The combiner only needs the Python venv (already set via
        # CLUSTER_PYTHON) — no external module load required.
        pre_command_str = "set -e"

        job_name = f"combine_{args.prefix}"
        print(f"Submitting job {job_name} to Slurm...")

        job_id = run_command(
            cmd=slurm_cmd,
            cwd=os.getcwd(),
            method="slurm",
            job_name=job_name,
            walltime=args.time,
            memory=args.mem,
            processors=args.nproc,
            background=True,
            pre_command=pre_command_str,
        )

        if job_id:
            print(f"Job submitted successfully. Job ID: {job_id}")
        else:
            print("Job submission failed.")

        sys.exit(0)

    mapping = {}

    if args.plugin and args.metric and args.condition:
        # Use Redis scan mode
        host = args.redis_host or ServerConfig.get_redis_hosts().get(
            "analysis_results", "localhost"
        )
        try:
            logger.info(f"Connecting to Redis at {host}...")
            r = redis.Redis(host=host, port=6379, decode_responses=True)
            mapping = generate_combiner_map(r, args.plugin, args.metric, args.condition, files=args.files)
        except Exception as e:
            print(f"Error: Redis scan failed: {e}")
            sys.exit(1)
    elif args.mapping:
        # Use static mapping mode
        try:
            if os.path.exists(args.mapping):
                with open(args.mapping, "r") as f:
                    mapping = json.load(f)
            else:
                mapping = json.loads(args.mapping)
        except Exception as e:
            print(f"Error: Failed to parse mapping: {e}")
            sys.exit(1)
    else:
        print(
            "Error: Must provide either --mapping OR (--plugin, --metric, and --condition)"
        )
        parser.print_help()
        sys.exit(1)

    if not mapping:
        print("No frames selected. Exiting.")
        sys.exit(0)

    combiner = DatasetCombiner(args.outdir, args.prefix, nproc=args.nproc)
    success = combiner.combine(mapping, images_per_file=args.n)

    if success:
        print(f"Successfully created combined dataset: {combiner.output_master_path}")
        print(f"Total frames: {combiner.total_frames_combined}")
    else:
        print("Dataset combination failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()