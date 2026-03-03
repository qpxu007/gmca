import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Set

import h5py
import numpy as np

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class HDF5Reader:
    """A class to read frames from an HDF5 file with multiple data_XXXXXX datasets.

    Frame numbering starts at 1. Supports metadata parsing, frame retrieval, and
    datafile existence checks.
    """

    KEY_MAP = {
        "detector": "/entry/instrument/detector/description",
        "nimages": "/entry/instrument/detector/detectorSpecific/nimages",
        "ntrigger": "/entry/instrument/detector/detectorSpecific/ntrigger",
        "x_pixels_in_detector": "/entry/instrument/detector/detectorSpecific/x_pixels_in_detector",
        "y_pixels_in_detector": "/entry/instrument/detector/detectorSpecific/y_pixels_in_detector",
        "count_cutoff": "/entry/instrument/detector/detectorSpecific/countrate_correction_count_cutoff",
        "sensor_thickness": "/entry/instrument/detector/sensor_thickness",
        "x_pixel_size": "/entry/instrument/detector/x_pixel_size",
        "y_pixel_size": "/entry/instrument/detector/y_pixel_size",
        "beam_center_x": "/entry/instrument/detector/beam_center_x",
        "beam_center_y": "/entry/instrument/detector/beam_center_y",
        "detector_distance": "/entry/instrument/detector/detector_distance",
        "omega_range_average": "/entry/sample/goniometer/omega_range_average",
        "incident_wavelength": "/entry/instrument/beam/incident_wavelength",
        "omega": "/entry/sample/goniometer/omega",
        "datafiles": "/entry/data",
    }

    # convert m into mm
    UNITS_IN_METERS = {
        "x_pixel_size": 1000,
        "y_pixel_size": 1000,
        "detector_distance": 1000,
        "sensor_thickness": 1000,
    }

    def __init__(
            self,
            filename: Union[str, Path],
            frames_per_dataset: Optional[int] = None,
            wait_interval: float = 0.2,
            timeout: float = 300.0,
    ):
        """Initialize the HDF5Reader.

        Args:
            filename: Path to the HDF5 file.
            frames_per_dataset: Number of frames per dataset. If None, inferred from the first dataset.
            wait_interval: Interval (seconds) to wait when checking for data files.
            timeout: Maximum time (seconds) to wait for data files.

        Raises:
            FileNotFoundError: If the HDF5 file does not exist.
            ValueError: If the file is invalid or datasets are missing.
        """
        self.filename = str(Path(filename))
        self.wait_interval = wait_interval
        self.frames_per_dataset = frames_per_dataset
        self.timeout = timeout
        self._h5file: Optional[h5py.File] = None

        if not os.path.exists(self.filename):
            raise FileNotFoundError(f"HDF5 file not found: {self.filename}")

        try:
            self._h5file = h5py.File(self.filename, "r")
        except OSError as e:
            raise ValueError(f"Failed to open HDF5 file {self.filename}: {e}")

        self.metadata = self._parse_metadata(include_arrays=True)
        # convert the hdf5 into standardized keys
        path_to_key_map = {value: key for key, value in HDF5Reader.KEY_MAP.items()}
        self.metadata = {path_to_key_map.get(k, k): v for k, v in self.metadata.items()}

        # convert the m to mm for use in XDS
        for key in self.UNITS_IN_METERS:
            if key in self.metadata:
                self.metadata[key] *= self.UNITS_IN_METERS[key]

        self.metadata["nimages"] = self.metadata.get("ntrigger", 1) * self.metadata.get(
            "nimages", 1
        )
        self.metadata["master_file"] = self.filename
        self.metadata["prefix"] = os.path.basename(self.filename).replace(
            "master.h5", ""
        )
        self.metadata["start"] = 1
        self.metadata["end"] = int(self.metadata.get("nimages", 1))
        self.metadata["omega"] = self.metadata.get("omega", [0])[0]
        self.total_frames = self.metadata.get("nimages", 1)
        self.datasets = self.metadata.get("datasets", {})
        self.datafiles = self.metadata.get("datafiles", {})
        self.frames_per_dataset = frames_per_dataset

    def get_metadata(self):
        return self.metadata

    def get_parameters(self):
        return {
            "det_dist": float(self.metadata.get("detector_distance", 100)),
            "wavelength": float(self.metadata.get("incident_wavelength", 1.0e-10)),
            "pixel_size": float(self.metadata.get("x_pixel_size", 0.075)),
            "beam_x": float(self.metadata.get("beam_center_x", 2200.0)),
            "beam_y": float(self.metadata.get("beam_center_y", 2200.0)),
            "nx": int(self.metadata.get("x_pixels_in_detector", 4371)),
            "ny": int(self.metadata.get("y_pixels_in_detector", 4150)),
            "omega_start": int(self.metadata.get("omega", 0.0)),
            "omega_range": float(self.metadata.get("omega_range_average", 0.2)),
        }

    def __enter__(self):
        """Support context manager for safe file handling."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the HDF5 file on context exit."""
        self.close()

    def __del__(self):
        """Ensure the HDF5 file is closed when the object is destroyed."""
        self.close()

    def close(self):
        """Close the HDF5 file."""
        if hasattr(self, '_h5file') and self._h5file:
            try:
                self._h5file.close()
            except Exception as e:
                logger.warning(f"Error closing HDF5 file {self.filename}: {e}")
            finally:
                self._h5file = None

    def _infer_frames_per_dataset(self) -> int:
        """Infer the number of frames per dataset from the first dataset.

        Returns:
            Number of frames in the first dataset.

        Raises:
            ValueError: If no datasets are found.
            KeyError: If the first dataset is inaccessible.
        """
        if not self.datasets or 0 not in self.datasets:
            raise ValueError("No datasets found in the HDF5 file.")

        first_dataset = f"{HDF5Reader.KEY_MAP['datafiles']}/{self.datasets[0]}"
        first_datafile = self.datafiles.get(0)

        # Wait for the first data file if it's external
        if first_datafile and first_datafile != self.filename:
            logger.info(f"Waiting for first data file: {first_datafile}")
            waited = 0.0
            while not os.path.exists(first_datafile) and waited < self.timeout:
                time.sleep(self.wait_interval)
                waited += self.wait_interval
            if not os.path.exists(first_datafile):
                raise FileNotFoundError(
                    f"First data file {first_datafile} not found after {self.timeout}s."
                )

        try:
            dataset = self._h5file[first_dataset]
            frames = dataset.shape[0]
            logger.info(f"Inferred frames per dataset: {frames}")
            return frames
        except KeyError as e:
            raise KeyError(f"Dataset {first_dataset} not found in {self.filename}: {e}")

    def _parse_metadata(self, include_arrays: bool = False) -> Dict[str, any]:
        """Parse metadata from the HDF5 file.

        Args:
            include_arrays: Whether to include non-scalar array datasets.

        Returns:
            Dictionary of metadata key-value pairs.
        """
        result: Dict[str, any] = {}

        def visitor(name: str, obj) -> None:
            if isinstance(obj, h5py.Dataset):
                try:
                    if h5py.check_string_dtype(obj.dtype):
                        value = obj.asstr()[()]
                    else:
                        value = obj[()]
                except Exception as e:
                    logger.warning(f"Failed to read dataset /{name}: {e}")
                    return

                if isinstance(value, np.ndarray) and value.dtype.kind == "S":
                    value = value.astype(str)
                elif isinstance(value, bytes):
                    value = value.decode("utf-8", errors="ignore")

                if (
                        not include_arrays
                        and isinstance(value, np.ndarray)
                        and value.shape != ()
                ):
                    return

                result[f"/{name}"] = value
                for aname, avalue in obj.attrs.items():
                    if isinstance(avalue, bytes):
                        avalue = avalue.decode("utf-8", errors="ignore")
                    result[f"/{name}/{aname}"] = avalue

            elif isinstance(obj, h5py.Group):
                if name.startswith("entry/data"):
                    result["template"] = self.filename.replace(
                        "_master.h5", "_??????.h5"
                    )
                    result["datasets"] = {i: k for i, k in enumerate(obj.keys())}
                    try:
                        result["datafiles"] = {
                            i: v.file.filename for i, (k, v) in enumerate(obj.items())
                        }
                    except:  # infer from keys if datafiles do not exist
                        prefix = self.filename.replace("_master.h5", "")
                        result["datafiles"] = {
                            i: f"{prefix}_{k}.h5" for i, k in enumerate(obj.keys())
                        }

        try:
            self._h5file.visititems(visitor)
        except Exception as e:
            logger.error(f"Error parsing metadata from {self.filename}: {e}")
            raise
        logger.debug(
            f"Datasets: {result.get('datasets')}, Datafiles: {result.get('datafiles')}"
        )
        return result

    def get_frame(self, frame_index: int) -> np.ndarray:
        """Retrieve a specific frame from the HDF5 file.

        Args:
            frame_index: Frame index (1 to total_frames).

        Returns:
            2D numpy array of the requested frame.

        Raises:
            IndexError: If the frame index is out of range.
            KeyError: If the dataset is not found.
        """
        dataset_idx, local_frame_idx = self._map_frame_to_dataset(frame_index)
        dataset_name = self.datasets.get(dataset_idx)
        if not dataset_name:
            raise KeyError(f"Dataset index {dataset_idx} not found.")

        dataset_path = f"{HDF5Reader.KEY_MAP['datafiles']}/{dataset_name}"
        try:
            dataset = self._h5file[dataset_path]
            return np.array(dataset[local_frame_idx])
        except KeyError as e:
            raise KeyError(f"Dataset {dataset_path} not found in {self.filename}: {e}")

    def get_frames(self, frame_indices: List[int]) -> List[np.ndarray]:
        """Retrieve multiple frames efficiently.

        Args:
            frame_indices: List of frame indices (1-based).

        Returns:
            List of 2D numpy arrays for the requested frames.

        Raises:
            IndexError: If any frame index is out of range.
            KeyError: If a dataset is not found.
        """
        frames = []
        for frame_index in frame_indices:
            frames.append(self.get_frame(frame_index))
        return frames

    def _map_frame_to_dataset(self, frame_index: int) -> Tuple[int, int]:
        """Map a global frame index to its dataset and local frame index.

        Args:
            frame_index: Frame index (1-based).

        Returns:
            Tuple of (dataset_index, local_frame_index).

        Raises:
            IndexError: If the frame index is out of range.
        """
        if self.frames_per_dataset is None:
            raise ValueError(f"frames_per_dataset is needed for continue")

        if not 1 <= frame_index <= self.total_frames:
            raise IndexError(
                f"Frame {frame_index} out of range (1 to {self.total_frames})."
            )
        internal_index = frame_index - 1
        dataset_idx = internal_index // self.frames_per_dataset
        local_frame_idx = internal_index % self.frames_per_dataset
        return dataset_idx, local_frame_idx

    def get_datafile_name(self, frame_index: int) -> str:
        """Get the datafile name for a given frame index.

        Args:
            frame_index: Frame index (1-based).

        Returns:
            Path to the datafile.

        Raises:
            IndexError: If the frame index is out of range.
            KeyError: If the datafile is not found.
        """
        dataset_idx, _ = self._map_frame_to_dataset(frame_index)
        datafile = self.datafiles.get(dataset_idx)
        if not datafile:
            raise KeyError(f"Datafile for dataset index {dataset_idx} not found.")
        return datafile

    def check_frame_on_disk(self, frame_index: int) -> bool:
        """Check if the datafile for a given frame index exists on disk.

        Args:
            frame_index: Frame index (1-based).

        Returns:
            True if the datafile exists, False otherwise.
        """
        try:
            datafile = self.get_datafile_name(frame_index)
            return os.path.exists(datafile)
        except (IndexError, KeyError) as e:
            logger.warning(f"Cannot check frame {frame_index}: {e}")
            return False

    def get_datafiles(self, frame_ranges: Union[List[str], str]) -> List[str]:
        """Get unique datafile names for a list of frame ranges.

        Args:
            frame_ranges: a str (e.g. "0 50") or  List of frame range strings (e.g.,  ["1 50", "51 100"]).

        Returns:
            List of unique datafile paths.

        Raises:
            ValueError: If a frame range is invalid.
            IndexError: If a frame is out of range.
        """
        datafiles: Set[str] = set()
        if isinstance(frame_ranges, str):
            frame_ranges = [frame_ranges]
        for frame_range in frame_ranges:
            try:
                start, end = map(int, frame_range.strip().split())
                if start > end:
                    raise ValueError(f"Invalid range: start ({start}) > end ({end})")
            except ValueError as e:
                raise ValueError(
                    f"Invalid frame range '{frame_range}': expected 'start end', got error: {e}"
                )

            for frame_index in range(start, end + 1):
                datafiles.add(self.get_datafile_name(frame_index))
        return sorted(datafiles)

    def calculate_datafile_time_delay(self, num_files: int = 5) -> Optional[float]:
        """Calculate the average time delay between consecutive data files.

        Args:
            num_files: Number of data files to inspect (default: 5).

        Returns:
            Average time delay in seconds, or None if insufficient data.

        Raises:
            ValueError: If num_files is non-positive.
        """
        if num_files <= 0:
            raise ValueError("Number of files must be positive.")

        # Get the first num_files entries from self.datafiles
        datafile_indices = sorted(self.datafiles.keys())[:num_files]
        if len(datafile_indices) < 2:
            logger.warning(
                f"Need at least 2 data files to calculate delay, found {len(datafile_indices)}."
            )
            return None

        creation_times = []
        file_paths = []

        # Collect creation times for existing files
        logger.info(f"Inspecting first {len(datafile_indices)} data files:")
        for idx in datafile_indices:
            filepath = self.datafiles[idx]
            try:
                if os.path.exists(filepath):
                    ctime = os.path.getctime(filepath)
                    creation_times.append(ctime)
                    file_paths.append(filepath)
                    logger.info(
                        f"  Dataset {idx}: {filepath}, created {time.ctime(ctime)}"
                    )
                else:
                    logger.warning(
                        f"  Dataset {idx}: {filepath} does not exist, skipping."
                    )
            except OSError as e:
                logger.error(
                    f"  Dataset {idx}: Failed to access {filepath}: {e}, skipping."
                )

        if len(creation_times) < 2:
            logger.warning(
                f"Insufficient valid data files ({len(creation_times)}) to calculate delay."
            )
            return None

        # Calculate time differences between consecutive files
        time_diffs = [
            creation_times[i + 1] - creation_times[i]
            for i in range(len(creation_times) - 1)
        ]

        # Compute average delay
        avg_delay = sum(time_diffs) / len(time_diffs)
        logger.info(f"Time differences between consecutive files: {time_diffs}")
        logger.info(f"Average time delay: {avg_delay:.2f} seconds")
        return avg_delay

    def wait_for_datafiles(
            self, frame_ranges: List[str], wait: bool = False
    ) -> List[str]:
        """Check if datafiles for frame ranges exist, optionally waiting.

        Args:
            frame_ranges: List of frame range strings (e.g., ["1 50"]).
            wait: If True, wait for missing files until timeout.

        Returns:
            List of missing datafile paths after waiting (empty if all found).

        Raises:
            ValueError: If frame ranges are invalid.
            IndexError: If frames are out of range.
        """
        try:
            self.frames_per_dataset = (
                    self.frames_per_dataset or self._infer_frames_per_dataset()
            )
        except:
            logger.error("frames_per_dataset unknown, cannot continue")
            raise

        datafiles = self.get_datafiles(frame_ranges)
        missing_files = [df for df in datafiles if not os.path.exists(df)]
        if not wait or not missing_files:
            return missing_files

        waited = 0.0
        while missing_files and waited < self.timeout:
            average_delay = self.calculate_datafile_time_delay(5)
            logger.info(
                f"Waiting for files {missing_files}, waited {waited:.1f}s, average delay {average_delay}s"
            )
            time.sleep(self.wait_interval)
            waited += self.wait_interval
            missing_files = [df for df in missing_files if not os.path.exists(df)]

        if missing_files:
            logger.error(f"Timeout waiting for files: {missing_files}")
        return missing_files


if __name__ == "__main__":
    # Example usage with context manager
    from pprint import pprint

    with HDF5Reader("/mnt/beegfs/qxu/L1_run6_master.h5") as reader:
        pprint(reader.metadata)
        print(f"Total frames: {reader.total_frames}")
        print(f"Datasets: {list(reader.datasets.values())}")

        # Retrieve example frames
        frames = reader.get_frames([1, 50, 51])
        for i, (frame_idx, frame) in enumerate(zip([1, 50, 51], frames), 1):
            print(f"Frame {frame_idx} shape: {frame.shape}")

        # Check datafile existence
        missing = reader.wait_for_datafiles(["50 51"], wait=True)
        print(f"Missing files: {missing}")
