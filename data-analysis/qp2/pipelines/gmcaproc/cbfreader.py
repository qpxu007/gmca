import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def extract_numbers(line: str) -> List[float]:
    """
    Extract all numbers (int, float, scientific notation) from a string.

    Args:
        line (str): Input string to parse.

    Returns:
        List[float]: List of extracted numbers.
    """
    # More precise pattern for numbers (int, float, scientific)
    pattern = r"-?\d*\.?\d+(?:[eE][-+]?\d+)?|-?\d+"
    matches = re.findall(pattern, line)
    return [float(match) for match in matches]


class CbfReader:
    """Parser for miniCBF file headers to extract metadata."""

    KEY_MAP = {
        "detector": "Detector:",
        "x_pixels_in_detector": "X-Binary-Size-Fastest-Dimension",
        "y_pixels_in_detector": "X-Binary-Size-Second-Dimension",
        "count_cutoff": "Count_cutoff",
        "sensor_thickness": "thickness",
        "x_pixel_size": "Pixel_size",
        "y_pixel_size": "Pixel_size",
        "beam_center_y": "Beam_xy",
        "beam_center_x": "Beam_xy",
        "detector_distance": "Detector_distance",
        "omega_range_average": "Angle_increment",
        "incident_wavelength": "Wavelength",
        "omega": "Start_angle",
    }

    UNITS_IN_METERS = {"Detector_distance", "thickness", "Pixel_size"}

    def __init__(
            self, file_path: str, start: Optional[int] = None, end: Optional[int] = None
    ):
        if not file_path or not isinstance(file_path, str):
            raise ValueError("file_path must be a non-empty string")

        self.file_path = Path(file_path).resolve()
        self.start = start
        self.end = end
        self.generate_template()

        if start is None or end is None:
            self.start, self.end, error = self.get_number_range()
            if error:
                logger.warning(f"Error getting number range: {error}")

        self.metadata = self._parse_metadata()

        # specific metadata adjustments
        self.metadata["beam_center_x"] = self.metadata["beam_center_x"][0]
        self.metadata["beam_center_y"] = self.metadata["beam_center_y"][1]
        self.metadata["x_pixel_size"] = self.metadata["x_pixel_size"][0]
        self.metadata["y_pixel_size"] = self.metadata["y_pixel_size"][1]

        self.metadata.update(
            {
                "start": self.start,
                "end": self.end,
                "template": self.template,
                "prefix": self.prefix,
                "suffix": self.suffix,
                "digits": self.digits,
                "master_file": self.file_path,
            }
        )
        # Interface compatibility with HDF5Reader
        self.total_frames = int(self.end - self.start + 1) if (self.start and self.end) else 0
        self.master_file_path = str(self.file_path)

    def get_metadata(self):
        return self.metadata

    def get_parameters(self) -> dict:
        """Return detector/geometry parameters in the same format as HDF5Reader.get_parameters()."""
        return {
            "det_dist": float(self.metadata.get("detector_distance", 100)),
            "wavelength": float(self.metadata.get("incident_wavelength", 1.0e-10)),
            "pixel_size": float(self.metadata.get("x_pixel_size", 0.075)),
            "beam_x": float(self.metadata.get("beam_center_x", 2200.0)),
            "beam_y": float(self.metadata.get("beam_center_y", 2200.0)),
            "nx": int(self.metadata.get("x_pixels_in_detector", 4371)),
            "ny": int(self.metadata.get("y_pixels_in_detector", 4150)),
            "omega_start": float(self.metadata.get("omega", 0.0)),
            "omega_range": float(self.metadata.get("omega_range_average", 0.2)),
            "nimages": self.total_frames,
        }

    def generate_template(self) -> bool:
        """
        Generate a template string from the file path for pattern matching.

        Returns:
            bool: True if template was generated, False otherwise.
        """
        # Match prefix, digits, and .cbf suffix
        pattern = r"^(.*?)(\d+)(\.cbf)$"
        match = re.match(pattern, self.file_path.name)
        if not match:
            logger.warning(f"File {self.file_path} does not match expected pattern")
            return False

        self.prefix, self.digits, self.suffix = match.groups()
        # Create template with same number of digits replaced by '?'
        self.template = f"{self.prefix}{'?' * len(self.digits)}{self.suffix}"
        return True

    def get_number_range(
            self, directory: Optional[str] = None
    ) -> Tuple[Optional[int], Optional[int], Optional[str]]:
        """
        Determine the range of numbered files in a directory matching the file's prefix.

        Args:
            directory (Optional[str]): Directory to search. Defaults to file's parent directory.

        Returns:
            Tuple[Optional[int], Optional[int], Optional[str]]: Start number, end number, and error message (if any).
        """
        dir_path = Path(directory).resolve() if directory else self.file_path.parent
        if not dir_path.is_dir():
            return None, None, f"Directory {dir_path} does not exist"

        # Extract prefix assuming pattern like prefix_000001.cbf
        match = re.match(r"(.+)_(\d+)\.cbf$", self.file_path.name)
        if not match:
            return (
                None,
                None,
                "Filename does not match expected pattern (prefix_number.cbf)",
            )

        prefix = match.group(1)
        pattern = re.compile(rf"^{re.escape(prefix)}_(\d+)\.cbf$")
        numbers = []

        try:
            for file in dir_path.iterdir():
                if file.is_file():
                    file_match = pattern.match(file.name)
                    if file_match:
                        numbers.append(int(file_match.group(1)))
        except OSError as e:
            return None, None, f"Error reading directory {dir_path}: {e}"

        if not numbers:
            return None, None, f"No files found with prefix '{prefix}' in {dir_path}"

        self.start = min(numbers)
        self.end = max(numbers)
        return self.start, self.end, None

    def _parse_metadata(self) -> Dict[str, any]:
        """
        Parse the header of a miniCBF file into a metadata dictionary.

        Returns:
            Dict[str, any]: Metadata with keys like 'Detector_distance' (mm), etc.
        """
        metadata = {}

        try:
            with open(self.file_path, "rb") as f:
                while True:
                    line = f.readline()
                    if not line or b"\x0c\x1a\x04\xd5" in line:
                        break
                    try:
                        line = line.decode("ascii", errors="ignore").strip()
                    except UnicodeDecodeError:
                        continue
                    if not line:
                        continue

                    for key, pattern in CbfReader.KEY_MAP.items():
                        if pattern in line:
                            if pattern == CbfReader.KEY_MAP["detector"]:
                                parts = line.split(":", 1)
                                metadata[key] = (
                                    parts[1].strip() if len(parts) > 1 else "Unknown"
                                )
                            else:
                                nums = extract_numbers(line)
                                if nums:
                                    # Convert meters to millimeters where applicable
                                    if pattern in CbfReader.UNITS_IN_METERS:
                                        nums = [n * 1000 for n in nums]
                                    metadata[key] = nums if len(nums) > 1 else nums[0]

        except FileNotFoundError:
            logger.error(f"File not found: {self.file_path}")
        except OSError as e:
            logger.error(f"Error reading file {self.file_path}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error parsing file {self.file_path}: {e}")

        return metadata

    def wait_for_datafiles(self):
        pass


if __name__ == "__main__":
    # Example usage
    file_path = "insu6_1_000001.cbf"
    cbf_reader = CbfReader(file_path)
    print(cbf_reader.metadata)
    start, end, error = cbf_reader.get_number_range()
    if error:
        print(f"Error: {error}")
    else:
        print(f"Start: {start}, End: {end}")
