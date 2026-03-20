#!/usr/bin/env python3

import argparse
import glob
import json
import logging
import os
import re
import sys

import numpy as np
import redis

# --- Assumed Project Structure ---
# This script assumes it's in a 'scripts' directory, and the 'qp2' package is a sibling.
# /your_project/
# |-- qp2/
# |   |-- image_viewer/
# |   |-- xio/
# |   `-- ...
# `-- scripts/
#     `-- find_raster_peaks.py
#
# If your structure is different, you may need to adjust the PYTHONPATH.
# For example: export PYTHONPATH=/path/to/your_project:$PYTHONPATH
try:
    from qp2.xio.hdf5_manager import HDF5Reader
    from qp2.image_viewer.volume_map.volume_utils import (
        reconstruct_volume_with_shift,
        find_3d_hotspots,
    )
except ImportError:
    print(
        "ERROR: Could not import qp2 modules. Please ensure the parent directory of 'qp2' is in your PYTHONPATH.",
        file=sys.stderr,
    )
    sys.exit(1)

# --- Configuration ---
# These values should match the configuration of your analysis plugin (e.g., Dozor)
REDIS_KEY_TEMPLATE = "analysis:out:spots:dozor2:{master_file}"
METRIC_TO_USE = (
    "num_spots"  # The field from the Redis data to use for the heatmap value
)
FRAME_INDEX_KEY = "frame_num"  # The field indicating the frame number

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def find_master_files(data_dir: str, run_prefix: str) -> list:
    """Finds all master files for a given run prefix in the data directory."""
    # Try _R first (most common), fall back to _C
    for tag in ["_R", "_C"]:
        pattern = os.path.join(data_dir, f"{run_prefix}{tag}*_master.h5")
        files = glob.glob(pattern)
        if files:
            return sorted(files)
    logging.warning(
        f"No master files found for prefix '{run_prefix}' in '{data_dir}'"
    )
    return []


def build_data_matrix(master_files: list, redis_conn: redis.Redis) -> np.ndarray:
    """Fetches analysis results from Redis and constructs a 2D numpy matrix."""
    if not master_files:
        return np.array([])

    # Auto-detect scan index pattern from filenames
    scan_idx_pattern = None
    for candidate in [r"_R(\d+)", r"_C(\d+)"]:
        if any(re.search(candidate, f, re.IGNORECASE) for f in master_files):
            scan_idx_pattern = candidate
            break
    if not scan_idx_pattern:
        logging.error("Could not detect scan index pattern (_R or _C) in filenames.")
        return np.array([])

    max_row, max_frames = 0, 0
    readers = {}
    for f_path in master_files:
        try:
            reader = HDF5Reader(f_path, start_timer=False)
            match = re.search(scan_idx_pattern, f_path, re.IGNORECASE)
            if not match:
                logging.warning(f"Could not parse scan index from {f_path}, skipping.")
                reader.close()
                continue

            row_idx = int(match.group(1)) - 1
            max_row = max(max_row, row_idx)
            max_frames = max(max_frames, reader.total_frames)
            readers[row_idx] = {"reader": reader, "path": f_path}
        except Exception as e:
            logging.error(f"Failed to open HDF5 file {f_path}: {e}")
            continue

    if not readers:
        logging.error("No valid HDF5 files could be read.")
        return np.array([])

    grid_shape = (max_row + 1, max_frames)
    matrix = np.full(grid_shape, np.nan)

    logging.info(
        f"Building data matrix with shape {grid_shape} using metric '{METRIC_TO_USE}'..."
    )

    for row_idx, data in readers.items():
        redis_key = REDIS_KEY_TEMPLATE.format(master_file=data["path"])
        try:
            redis_results = redis_conn.hgetall(redis_key)
            if not redis_results:
                logging.warning(f"No data found in Redis for key: {redis_key}")
                continue

            for frame_json in redis_results.values():
                frame_data = json.loads(frame_json)
                frame_idx = frame_data.get(FRAME_INDEX_KEY)
                metric_val = frame_data.get(METRIC_TO_USE)
                if frame_idx is not None and metric_val is not None:
                    col_idx = frame_idx - 1  # to 0-based
                    if 0 <= col_idx < grid_shape[1]:
                        matrix[row_idx, col_idx] = metric_val
        except redis.RedisError as e:
            logging.error(f"Redis error fetching key {redis_key}: {e}")
        finally:
            data["reader"].close()

    return matrix


def numpy_safe_json_serializer(obj):
    """Converts numpy types to native Python types for JSON serialization."""
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Automate 3D raster peak finding from Redis analysis data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--data_dir",
        required=True,
        help="Root directory containing the HDF5 master files.",
    )
    parser.add_argument(
        "--run1_prefix",
        required=True,
        help="Prefix of the first orthogonal run (e.g., 'B1_ras_run1').",
    )
    parser.add_argument(
        "--run2_prefix",
        required=True,
        help="Prefix of the second orthogonal run (e.g., 'B1_ras_run3').",
    )
    parser.add_argument(
        "--shift",
        type=float,
        default=0.0,
        help="Shift of run2 relative to run1 (in frames/columns).",
    )
    parser.add_argument(
        "--max_peaks", type=int, default=10, help="Maximum number of peaks to output."
    )
    parser.add_argument(
        "--min_size",
        type=int,
        default=3,
        help="Minimum number of voxels to be considered a hotspot.",
    )
    parser.add_argument(
        "--percentile_threshold",
        type=float,
        default=95.0,
        help="Data percentile to use as the hotspot threshold.",
    )
    parser.add_argument(
        "--redis_host", default="localhost", help="Redis server hostname."
    )
    parser.add_argument(
        "--redis_port", type=int, default=6379, help="Redis server port."
    )
    parser.add_argument(
        "--redis_db", type=int, default=0, help="Redis database number."
    )

    args = parser.parse_args()

    try:
        redis_conn = redis.Redis(
            host=args.redis_host,
            port=args.redis_port,
            db=args.redis_db,
            decode_responses=True,
        )
        redis_conn.ping()
        logging.info(f"Connected to Redis at {args.redis_host}:{args.redis_port}")
    except redis.RedisError as e:
        logging.error(f"Could not connect to Redis: {e}")
        sys.exit(1)

    # 1. Find master files for both runs
    files1 = find_master_files(args.data_dir, args.run1_prefix)
    files2 = find_master_files(args.data_dir, args.run2_prefix)

    if not files1 or not files2:
        logging.error("Master files for one or both runs were not found. Exiting.")
        sys.exit(1)

    # 2. Determine which run is XY and which is XZ based on omega angle
    try:
        reader1 = HDF5Reader(files1[0])
        angle1 = reader1.get_parameters().get("omega_start", 0)
        reader1.close()
        reader2 = HDF5Reader(files2[0])
        angle2 = reader2.get_parameters().get("omega_start", 0)
        reader2.close()
    except Exception as e:
        logging.error(
            f"Could not read metadata from HDF5 files to determine orientation: {e}"
        )
        sys.exit(1)

    # Normalize angles to determine which is closer to horizontal (0/180) vs vertical (90/270)
    angle1_mod = abs(angle1 % 180)
    angle2_mod = abs(angle2 % 180)
    if angle1_mod > 90:
        angle1_mod = 180 - angle1_mod
    if angle2_mod > 90:
        angle2_mod = 180 - angle2_mod

    if angle1_mod < angle2_mod:
        xy_files, xz_files = files1, files2
        logging.info(
            f"Assigning '{args.run1_prefix}' as XY scan and '{args.run2_prefix}' as XZ scan."
        )
    else:
        xy_files, xz_files = files2, files1
        logging.info(
            f"Assigning '{args.run2_prefix}' as XY scan and '{args.run1_prefix}' as XZ scan."
        )

    # 3. Build the 2D data matrices from Redis
    data_xy = build_data_matrix(xy_files, redis_conn)
    data_xz = build_data_matrix(xz_files, redis_conn)

    if data_xy.size == 0 or data_xz.size == 0:
        logging.error("Failed to build one or both data matrices. Exiting.")
        sys.exit(1)

    # 4. Reconstruct the 3D volume
    logging.info(f"Reconstructing volume with shift={args.shift}...")
    volume = reconstruct_volume_with_shift(data_xy, data_xz, shift=args.shift)

    if volume.size == 0 or np.all(np.isnan(volume)):
        logging.error(
            "Volume reconstruction resulted in an empty or all-NaN volume. Check shift value and data."
        )
        sys.exit(1)

    logging.info(f"Reconstructed volume shape: {volume.shape}")

    # 5. Find hotspots
    logging.info(
        f"Finding hotspots with percentile >= {args.percentile_threshold}% and min_size >= {args.min_size}..."
    )
    hotspots = find_3d_hotspots(
        volume, percentile_threshold=args.percentile_threshold, min_size=args.min_size
    )
    logging.info(f"Found {len(hotspots)} total hotspots.")

    # 6. Format and print the output
    output_peaks = hotspots[: args.max_peaks]

    # Convert numpy arrays in the output to lists for clean JSON output
    for peak in output_peaks:
        if "orientation" in peak:
            peak["orientation"] = peak["orientation"].tolist()

    json_output = json.dumps(output_peaks, indent=2, default=numpy_safe_json_serializer)
    print(json_output)


if __name__ == "__main__":
    main()
