# qp2/image_viewer/plugins/crystfel/crystfel_process_file.py
"""
Standalone script for distributed CrystFEL processing of a SINGLE HDF5 data file.

This script:
1. Takes a pre-generated geometry file and a single HDF5 data file.
2. Creates a temporary file list (.lst) for the data file.
3. Runs `indexamajig` with peak finding and indexing.
4. Uses a detailed StreamParser to extract rich information from the output.
5. Saves the parsed results to a Redis HASH.
"""
import argparse
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import redis

DEBUG = False
CRYSTFEL_STREAM_DIR = Path(
    os.getenv(
        "CRYSTFEL_STREAM_DIR", f"/mnt/beegfs/{os.getenv('USER')}/crystfel_streams"
    )
)


def find_qp2_parent(file_path):
    """
    Robustly finds the project root directory by walking up from the current
    script's location until it finds the 'qp2' package directory.
    """
    path = os.path.abspath(file_path)
    # Stop when we reach the filesystem root (e.g., '/')
    while path != os.path.dirname(path):
        if os.path.basename(path) == "qp2":
            # We found the 'qp2' directory, so its parent is the project root.
            return os.path.dirname(path)
        path = os.path.dirname(path)
    return None  # Return None if not found


project_root = find_qp2_parent(__file__)
if project_root:
    if project_root not in sys.path:
        print(f"Adding project root to sys.path: {project_root}")
        sys.path.insert(0, project_root)
else:
    # If the root isn't found, we cannot import qp2 modules. This is a fatal error.
    print(
        f"CRITICAL: Could not find the 'qp2' project root directory from the path '{__file__}'.",
        file=sys.stderr,
    )
    sys.exit(1)

from qp2.log.logging_config import setup_logging, get_logger

logger = get_logger(__name__)


# --- Helper Functions ---
def run_crystfel_command(cmd, cwd):
    """Helper to run a CrystFEL command and log its output."""
    logger.info(f"Executing command in {cwd}: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)
    if result.stdout:
        logger.debug(f"CrystFEL STDOUT:\n{result.stdout}")
    if result.stderr:
        logger.warning(f"CrystFEL STDERR:\n{result.stderr}")
    if result.returncode != 0:
        raise RuntimeError(
            f"CrystFEL command failed with exit code {result.returncode}:\n{' '.join(cmd)}"
        )
    return result


class TriclinicCalculator:
    """
    Pre-compute reciprocal metric tensor for efficient d-spacing calculations.

    # Usage example
    cell = np.array([5.0, 6.0, 7.0, 80.0, 85.0, 75.0])
    calc = TriclinicCalculator(cell)

    # For many Miller indices
    miller_indices = np.array([
        [1, 0, 0], [0, 1, 0], [0, 0, 1],
        [1, 1, 0], [1, 0, 1], [0, 1, 1],
        [1, 1, 1], [2, 0, 0], [0, 2, 0]
    ])

    # Fast batch calculation
    d_spacings = calc.d_spacing_batch(miller_indices)
    print("d-spacings:", d_spacings)


    """

    def __init__(self, cell):
        """
        Initialize with unit cell parameters and pre-compute constants.

        Parameters:
        -----------
        cell : numpy array [a, b, c, alpha, beta, gamma]
            Unit cell parameters (angles in degrees)
        """
        self.cell = cell
        self._compute_reciprocal_metric()

    def _compute_reciprocal_metric(self):
        """Pre-compute all the constant terms."""
        a, b, c, alpha, beta, gamma = self.cell

        # Convert angles to radians
        alpha = np.radians(alpha)
        beta = np.radians(beta)
        gamma = np.radians(gamma)

        # Calculate trigonometric values
        c_alpha = np.cos(alpha)
        c_beta = np.cos(beta)
        c_gamma = np.cos(gamma)
        s_alpha = np.sin(alpha)
        s_beta = np.sin(beta)
        s_gamma = np.sin(gamma)

        # Volume factor
        V2 = 1 - c_alpha ** 2 - c_beta ** 2 - c_gamma ** 2 + 2 * c_alpha * c_beta * c_gamma

        # Store reciprocal metric tensor elements
        self.g11 = (s_alpha ** 2) / (a ** 2 * V2)
        self.g22 = (s_beta ** 2) / (b ** 2 * V2)
        self.g33 = (s_gamma ** 2) / (c ** 2 * V2)
        self.g12 = (c_alpha * c_beta - c_gamma) / (a * b * V2)
        self.g13 = (c_beta * c_gamma - c_alpha) / (a * c * V2)
        self.g23 = (c_gamma * c_alpha - c_beta) / (b * c * V2)

    def d_spacing(self, miller):
        """
        Calculate d-spacing for single Miller index (fast).

        Parameters:
        -----------
        miller : numpy array [h, k, l]

        Returns:
        --------
        float : d-spacing
        """
        h, k, l = miller
        inv_d_sq = (
                self.g11 * h ** 2
                + self.g22 * k ** 2
                + self.g33 * l ** 2
                + 2 * self.g12 * h * k
                + 2 * self.g13 * h * l
                + 2 * self.g23 * k * l
        )
        return 1 / np.sqrt(inv_d_sq)

    def d_spacing_batch(self, miller_list):
        """
        Calculate d-spacings for multiple Miller indices (vectorized).

        Parameters:
        -----------
        miller_list : numpy array of shape (N, 3)

        Returns:
        --------
        numpy array : d-spacings for each Miller index
        """
        h, k, l = miller_list.T
        inv_d_sq = (
                self.g11 * h ** 2
                + self.g22 * k ** 2
                + self.g33 * l ** 2
                + 2 * self.g12 * h * k
                + 2 * self.g13 * h * l
                + 2 * self.g23 * k * l
        )
        return 1 / np.sqrt(inv_d_sq)


# --- Stream Parser (Adapted from your streamparser.py) ---
class StreamParser:
    """
    Parses a CrystFEL .stream file to extract detailed information
    about peaks, indexing, and cell parameters for each image chunk.
    """

    def __init__(self, stream_file_path, high_res_limit=None, max_reflections=500):
        self.stream_file = stream_file_path
        self.all_results = []
        if not os.path.exists(self.stream_file):
            logger.warning(f"Stream file not found: {self.stream_file}")
            return

        self.header = ""

        self.high_res_limit = high_res_limit
        self.max_reflections = max_reflections
        logger.info(f"max reflections/spots sets to {self.max_reflections}")
        self._parse_header_and_chunks()

    def get_header_content(self) -> str:
        """Returns the parsed header content."""
        return self.header

    def _parse_header_and_chunks(self):
        """
        Reads the stream file once, separating the initial header from all subsequent chunks.
        """
        header_lines = []
        is_in_header = True

        with open(self.stream_file, "r") as f:
            for chunk in self._chunk_generator(f):
                if is_in_header:
                    # The first "chunk" from our generator might be the header
                    if "----- Begin chunk -----" in chunk[0]:
                        self.header = "".join(header_lines)
                        is_in_header = False
                        # This chunk is a real chunk, so process it
                        parsed_chunk = self._parse_chunk(chunk)
                        if parsed_chunk:
                            self.all_results.append(parsed_chunk)
                    else:
                        header_lines.extend(chunk)
                else:
                    # Process all subsequent chunks normally
                    parsed_chunk = self._parse_chunk(chunk)
                    if parsed_chunk:
                        self.all_results.append(parsed_chunk)

        if is_in_header:  # In case the file had a header but no chunks
            self.header = "".join(header_lines)

    def _chunk_generator(self, file_handle):
        """Generator to yield sections from the stream file, including the initial header."""
        chunk = []
        # The first part of the file before any chunk is the header.
        # We'll treat it as the first "chunk" to be yielded.
        for line in file_handle:
            if line.startswith("----- Begin chunk -----"):
                if chunk:
                    yield chunk  # Yield the header or the previous chunk
                chunk = [line]
            else:
                chunk.append(line)
        if chunk:
            yield chunk  # Yield the last chunk

    def _parse_chunk(self, chunk: list) -> dict:
        """Parses a single chunk of text from the stream file."""
        results = {"indexed_by": "none", "chunk": "".join(chunk)}  # Default value
        peak_start = peak_end = refl_start = refl_end = None
        for i, line in enumerate(chunk):
            line = line.strip()
            if not line:
                continue

            parts = line.split()
            if line.startswith("Image filename:"):
                full_filename = parts[-1]
                results["image_filename"] = full_filename
            elif line.startswith("Event:"):
                try:
                    event_num = int(parts[-1].replace("//", ""))
                    results["event_num"] = event_num
                except ValueError:
                    logger.warning(f"Invalid event number in line: {line}")
            elif line.startswith("Image serial number:"):
                try:
                    serial_num = int(parts[-1])
                    results["image_serial_number"] = serial_num
                except ValueError:
                    logger.warning(f"Invalid serial number in line: {line}")

            elif line.startswith("num_peaks ="):
                results["num_peaks"] = int(parts[-1])
            elif line.startswith("Peaks from peak search"):
                peak_start = i + 2  # Data starts 2 lines after header
            elif line.startswith("End of peak list"):
                peak_end = i
            elif line.startswith("Cell parameters"):
                # Cell parameters 3.76313 7.78202 7.81337 nm, 89.87862 90.00602 89.97159 deg
                try:
                    cell = (
                        line.replace("Cell parameters", "")
                        .replace("nm,", "")
                        .replace("deg", "")
                        .split()
                    )
                    # Convert nm to Angstrom for first 3 values
                    cell_len = [
                        np.round(10 * float(p.strip(",")), 2) for p in cell[0:3]
                    ]
                    cell_ang = [np.round(float(p.strip(",")), 2) for p in cell[3:6]]
                    results["unit_cell_crystfel"] = cell_len + cell_ang
                except (ValueError, IndexError):
                    pass
            elif line.startswith("lattice_type ="):
                results["lattice_type"] = parts[-1]
            elif line.startswith("centering ="):
                results["centering"] = parts[-1]
            elif line.startswith("indexed_by ="):
                results["indexed_by"] = parts[-1]
            elif line.startswith("Reflections measured after indexing"):
                refl_start = i + 2
            elif line.startswith("End of reflections"):
                refl_end = i

        if peak_start and peak_end and peak_start < peak_end:

            peaks = [p.split() for p in chunk[peak_start:peak_end]]
            # only keep sports within resolution limit if specified
            if self.high_res_limit:
                peaks = [p for p in peaks if 10.0 / float(p[2]) >= self.high_res_limit]
            results["spots_crystfel"] = [
                (float(p[0]), float(p[1])) for p in peaks if len(p) >= 2
            ][: self.max_reflections]

        if refl_start and refl_end and refl_start < refl_end:
            # h    k    l          I   sigma(I)       peak background  fs/px  ss/px panel
            reflns = [p.split() for p in chunk[refl_start:refl_end]]
            miller_indices = np.array(
                [[int(r[0]), int(r[1]), int(r[2])] for r in reflns]
            )

            calc = TriclinicCalculator(results.get("unit_cell_crystfel"))

            d_spacings = calc.d_spacing_batch(miller_indices)
            if len(d_spacings) == len(reflns):
                if self.high_res_limit:
                    # Filter reflections based on high resolution limit
                    reflns = [
                        r
                        for r, d in zip(reflns, d_spacings)
                        if d >= self.high_res_limit
                    ]
            results["reflections_crystfel"] = reflns[: self.max_reflections]
        return results


# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(
        description="Run CrystFEL processing on a single data file."
    )
    parser.add_argument(
        "--geometry_file", required=True, help="Path to the pre-generated .geom file."
    )
    parser.add_argument(
        "--data_file",
        required=True,
        help="Path to the single HDF5 data file to process.",
    )
    parser.add_argument(
        "--start_frame",
        type=int,
        required=True,
        help="Absolute 0-based start frame index for this file.",
    )
    parser.add_argument(
        "--num_frames",
        type=int,
        required=True,
        help="Number of frames in this data file.",
    )
    parser.add_argument(
        "--master_file",
        required=True,
        help="Path to the master file (for Redis key construction).",
    )

    parser.add_argument(
        "--high_res_limit",
        type=float,
        default=None,
        help="High resolution limit in Angstroms. Spots higher than this will be filtered out.",
    )

    parser.add_argument(
        "--max_reflections",
        type=int,
        default=300,
        help="Maximum number of spots/reflections to keep for each chunk.",
    )

    parser.add_argument("--peak_method", type=str, default="peakfinder9")
    parser.add_argument("--min_snr", type=float, default=4.0)
    parser.add_argument("--min_snr_biggest_pix", type=float, default=3.0)
    parser.add_argument("--min_snr_peak_pix", type=float, default=2.0)
    parser.add_argument("--min_sig", type=float, default=5.0)
    parser.add_argument("--local_bg_radius", type=int, default=3)
    parser.add_argument(
        "--pdb",
        type=str,
        default=None,
        help="Path to a .pdb or .cell file for indexing.",
    )

    parser.add_argument("--redis_host", type=str)
    parser.add_argument("--redis_port", type=int, default=6379)

    parser.add_argument(
        "--redis_key",
        required=True,
        help="The full Redis HASH key for storing results.",
    )
    # ADDITION: Add the new status key argument
    parser.add_argument(
        "--status_key",
        required=True,
        help="The full Redis HASH key for storing job status.",
    )

    parser.add_argument(
        "--nproc",
        "-j",
        type=int,
        default=8,
        help="Number of parallel processors for indexamajig.",
    )
    parser.add_argument(
        "--extra_options",
        type=str,
        default="",
        help="A string of additional command-line options for indexamajig.",
    )
    args, extra_args = parser.parse_known_args()

    setup_logging(root_name="qp2", log_level=logging.INFO)
    logger = get_logger(__name__)

    wdir = tempfile.mkdtemp(prefix="crystfel_file_")
    logger.info(f"Using temporary working directory: {wdir}")
    status_field = str(args.start_frame)
    redis_conn = (
        redis.Redis(host=args.redis_host, port=args.redis_port)
        if args.redis_host
        else None
    )

    try:
        # --- 1. Create file list (.lst) with event numbers ---
        file_list_path = os.path.join(wdir, "images.lst")
        with open(file_list_path, "w") as f:
            f.write(f"{args.data_file}\n")
            # NB no need to write event numbers here, unless you want to filter specific frames
            # for i in range(args.num_frames):
            #     f.write(f"{args.data_file} //{i}\n")

        # --- 2. Run indexamajig with indexing enabled ---
        stream_file = os.path.join(wdir, "output.stream")
        cmd = [
            "indexamajig",
            "-i",
            file_list_path,
            "-g",
            args.geometry_file,
            "-o",
            stream_file,
            "-j",
            str(args.nproc),
            f"--peaks={args.peak_method}",
            f"--min-snr={args.min_snr}",
            f"--min-snr-biggest-pix={args.min_snr_biggest_pix}",
            f"--min-snr-peak-pix={args.min_snr_peak_pix}",
            f"--min-sig={args.min_sig}",
            f"--local-bg-radius={args.local_bg_radius}",
        ]
        if args.pdb:
            cmd.extend([f"-p {args.pdb}"])  # "--indexing=mosflm-latt-cell",

        if extra_args:
            try:
                logger.info(f"Adding extra user-defined options: {extra_args}")
                for arg in extra_args:
                    cmd.append(arg)
            except Exception as e:
                logger.error(f"Could not parse extra options string: '{extra_args}'. Error: {e}")
                raise ValueError("Invalid extra_options string provided.")

        run_crystfel_command(cmd, wdir)
        # --- 3. Parse the output stream ---
        parser = StreamParser(
            stream_file,
            high_res_limit=args.high_res_limit,
            max_reflections=args.max_reflections,
        )
        results_from_stream = parser.all_results

        # 1. Write the entire output stream from this job to a persistent, shared file
        stream_file_in_wdir = os.path.join(wdir, "output.stream")
        persistent_stream_path = None  # Define in this scope
        if os.path.exists(stream_file_in_wdir):
            master_file_hash = hashlib.sha1(args.master_file.encode()).hexdigest()
            output_dir = CRYSTFEL_STREAM_DIR / master_file_hash
            output_dir.mkdir(parents=True, exist_ok=True)
            persistent_stream_path = output_dir / f"{args.start_frame}.stream"
            shutil.move(stream_file_in_wdir, persistent_stream_path)
            logger.info(f"Saved bundled stream segment to: {persistent_stream_path}")

        logger.info(f"Parsed {len(results_from_stream)} chunks from the stream file.")
        # --- 4. Save results to Redis ---
        if redis_conn:

            if persistent_stream_path:
                segments_key = f"{args.redis_key}:segments"
                segment_field = str(args.start_frame)
                segment_value = str(persistent_stream_path)
                redis_conn.hset(segments_key, segment_field, segment_value)
                logger.info(
                    f"Registered new stream segment in Redis key '{segments_key}'"
                )
                # Set expiration for the segments key as well
                redis_conn.expire(segments_key, 24 * 3600)  # 24 hours

            redis_key = args.redis_key
            with redis_conn.pipeline() as pipe:
                for res in results_from_stream:
                    # Map local event number back to absolute frame index
                    absolute_frame_idx = args.start_frame + res.get("event_num", -1)
                    if absolute_frame_idx < args.start_frame:
                        continue

                    # filter reflections
                    reflections = res.get("reflections_crystfel", [])
                    # Create a final dictionary for Redis
                    result_dict = {
                        "img_num": absolute_frame_idx + 1,
                        "num_spots_crystfel": res.get("num_peaks", 0),
                        "spots_crystfel": res.get("spots_crystfel", []),
                        "unit_cell_crystfel": res.get("unit_cell_crystfel"),
                        "crystfel_indexed_by": res.get("indexed_by", "none"),
                        "crystfel_lattice": res.get("lattice_type"),
                        "crystfel_centering": res.get("centering"),
                        "reflections_crystfel": reflections,
                        "is_indexed": 1.0 if res.get("indexed_by") != "none" else 0.0,
                        "timestamp": time.time(),
                    }
                    pipe.hset(
                        redis_key, result_dict["img_num"], json.dumps(result_dict)
                    )

                # Set expiration for the main results hash key
                pipe.expire(redis_key, 24 * 3600)  # 24 hours

                pipe.execute()

            completed_status = {"status": "COMPLETED", "timestamp": time.time()}
            redis_conn.hset(args.status_key, status_field, json.dumps(completed_status))
            logger.info(
                f"Updated job status to COMPLETED for frame {args.start_frame}."
            )
            logger.info(
                f"Successfully saved results for {len(results_from_stream)} frames to Redis."
            )

    except Exception as e:
        logger.error(f"CrystFEL process failed: {e}", exc_info=True)
        if redis_conn:
            failed_status = {
                "status": "FAILED",
                "timestamp": time.time(),
                "error": str(e),
            }
            redis_conn.hset(args.status_key, status_field, json.dumps(failed_status))
            logger.error(f"Updated job status to FAILED for frame {args.start_frame}.")
        sys.exit(1)
    finally:
        if not DEBUG:
            shutil.rmtree(wdir)


if __name__ == "__main__":
    main()
