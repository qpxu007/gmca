# qp2/data_proc/server/pipelines/spot_finder/find_spots_process.py
"""
Standalone script for distributed spot finding.

This script is designed to be executed on a cluster node (or as a local background process).
It processes a specific range of frames from an HDF5 master file, finds spots,
and saves the results to a Redis HASH.

It is intended to be called by a submitter worker (e.g., PeakFinderDataFileWorker)
which provides all necessary parameters via command-line arguments.
"""

import argparse
import json
import logging
import os
import sys
from time import time

import redis


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
        sys.path.insert(0, project_root)
else:
    # If the root isn't found, we cannot import qp2 modules. This is a fatal error.
    print(
        f"CRITICAL: Could not find the 'qp2' project root directory from the path '{__file__}'.",
        file=sys.stderr,
    )
    sys.exit(1)

from qp2.log.logging_config import get_logger, setup_logging
from qp2.image_viewer.plugins.spot_finder.peak_finding_utils import (
    find_peaks_in_annulus,
)
from qp2.xio.hdf5_manager import HDF5Reader
from qp2.image_viewer.utils.redis_cache import load_numpy_array_from_redis
from qp2.image_viewer.utils.mask_computation import compute_detector_mask
from qp2.image_viewer.config import MASKED_CIRCLES, MASKED_RECTANGLES


def main():
    """
    Main execution function for the spot finding process.
    """
    # --- 1. Argument Parsing ---
    # Define and parse all command-line arguments required for the job.
    parser = argparse.ArgumentParser(
        description="Run distributed spot finding for a range of frames."
    )
    parser.add_argument(
        "--metadata",
        type=json.loads,
        required=True,
        help="JSON string containing dataset metadata (params, master_file, etc.)",
    )
    parser.add_argument(
        "--start_frame",
        type=int,
        required=True,
        help="0-based starting frame index (inclusive)",
    )
    parser.add_argument(
        "--end_frame",
        type=int,
        required=True,
        help="0-based ending frame index (inclusive)",
    )
    parser.add_argument(
        "--redis_host", type=str, default=None, help="Redis server host"
    )
    parser.add_argument(
        "--redis_port", type=int, default=6379, help="Redis server port"
    )
    parser.add_argument(
        "--redis_key_prefix",
        type=str,
        required=True,
        help="Redis key prefix for storing results",
    )
    parser.add_argument(
        "--status_key",
        required=True,
        help="The full Redis HASH key for storing job status.",
    )
    parser.add_argument(
        "--mask_redis_key",
        type=str,
        default=None,
        help="Redis key for the cached detector mask (if available)",
    )
    args = parser.parse_args()

    # Configure logging for this script's output
    setup_logging(root_name="qp2.spot_finder_process", log_level=logging.INFO)
    logger = get_logger(__name__)

    redis_conn = None  # Define in this scope
    status_field = str(args.start_frame)

    try:
        # --- 2. Connect to Redis (if configured) ---
        redis_conn = None
        if args.redis_host:
            try:
                redis_conn = redis.Redis(
                    host=args.redis_host, port=args.redis_port, decode_responses=True
                )
                redis_conn.ping()
                logger.info(
                    f"Successfully connected to Redis at {args.redis_host}:{args.redis_port}"
                )

                running_status = {"status": "RUNNING", "timestamp": time()}
                redis_conn.hset(
                    args.status_key, status_field, json.dumps(running_status)
                )
                redis_conn.expire(args.status_key, 24 * 3600)
            except redis.RedisError as e:
                logger.error(
                    f"Could not connect to Redis: {e}. Results will not be saved."
                )
                redis_conn = None

        # --- 3. Load or Re-compute the Detector Mask ---
        detector_mask = None
        if args.mask_redis_key and redis_conn:
            logger.info(
                f"Attempting to load detector mask from Redis key: {args.mask_redis_key}"
            )
            # NB mask is binary, need to use binary mode, decode_response = False
            detector_mask = load_numpy_array_from_redis(
                args.redis_host, args.redis_port, args.mask_redis_key
            )

        if detector_mask is None:
            logger.warning(
                "Could not load mask from Redis cache. Re-computing mask on the fly as a fallback."
            )
            reader = HDF5Reader(args.metadata["master_file"], start_timer=False)
            sample_image = reader.get_frame(
                args.start_frame
            )  # Use the first frame of the batch as a template
            reader.close()

            if sample_image is not None:
                mask_values = set(args.metadata.get("mask_values", []))
                detector_mask, _ = compute_detector_mask(
                    image=sample_image,
                    params=args.metadata.get("params", {}),
                    mask_values=mask_values,
                    masked_circles=MASKED_CIRCLES,
                    masked_rectangles=MASKED_RECTANGLES,
                )
                logger.info(
                    f"Successfully re-computed mask. Shape: {detector_mask.shape}"
                )
            else:
                logger.error(
                    "Could not read a sample frame to compute mask. Proceeding without a detector mask."
                )

        # --- 4. Determine Which Frames to Process (Pre-check) ---
        redis_key = f"{args.redis_key_prefix}:{args.metadata['master_file']}"
        frames_to_process = []

        if redis_conn:
            existing_frames_str = redis_conn.hkeys(redis_key)
            existing_frames = {int(f) for f in existing_frames_str}
            for frame_idx in range(args.start_frame, args.end_frame + 1):
                if (frame_idx + 1) not in existing_frames:
                    frames_to_process.append(frame_idx)
            num_skipped = (args.end_frame - args.start_frame + 1) - len(
                frames_to_process
            )
            if num_skipped > 0:
                logger.info(f"Skipped {num_skipped} frames already found in Redis.")
        else:
            # If no Redis, we must process all frames in the given range
            frames_to_process = list(range(args.start_frame, args.end_frame + 1))

        if not frames_to_process:
            logger.info(
                "All frames in the assigned range have already been processed. Exiting."
            )
            return

        # --- 5. Main Processing Loop ---
        logger.info(f"Starting processing for {len(frames_to_process)} new frames...")

        # Open the HDF5 reader once for the entire job.
        reader = HDF5Reader(args.metadata["master_file"], start_timer=False)

        results_to_save = []
        task_kwargs = args.metadata.get("peak_finder_kwargs", {})

        # This script runs sequentially on one compute node. Internal parallelism was removed for simplicity and robustness.
        # The higher-level system is responsible for submitting multiple instances of this script in parallel.
        for frame_idx in frames_to_process:
            image = reader.get_frame(frame_idx)
            if image is None:
                logger.warning(f"Could not read frame {frame_idx}. Skipping.")
                continue

            peaks_yx = find_peaks_in_annulus(
                image,
                detector_mask,
                args.metadata["params"]["beam_x"],
                args.metadata["params"]["beam_y"],
                **task_kwargs,
            )
            peaks_xy = peaks_yx[:, [1, 0]] if peaks_yx.size > 0 else peaks_yx

            results_to_save.append(
                {
                    "img_num": frame_idx + 1,
                    "num_spots": len(peaks_xy),
                    "spots": peaks_xy.tolist(),
                    "timestamp": time(),
                }
            )

        reader.close()

        # --- 6. Save Results to Redis ---
        if redis_conn and results_to_save:
            results_to_save.sort(key=lambda x: x["img_num"])
            with redis_conn.pipeline() as pipe:
                for res in results_to_save:
                    pipe.hset(redis_key, res["img_num"], json.dumps(res))
                pipe.execute()
            logger.info(
                f"Successfully processed and saved {len(results_to_save)} frames to Redis key '{redis_key}'."
            )

        completed_status = {"status": "COMPLETED", "timestamp": time()}
        redis_conn.hset(args.status_key, status_field, json.dumps(completed_status))
        redis_conn.expire(args.status_key, 24 * 3600)
        logger.info(
            f"Job completed successfully for frames {args.start_frame}-{args.end_frame}."
        )

    except Exception as e:
        logger.error(
            f"Spot finding process failed with a critical error: {e}", exc_info=True
        )
        if redis_conn:
            failed_status = {"status": "FAILED", "timestamp": time(), "error": str(e)}
            redis_conn.hset(args.status_key, status_field, json.dumps(failed_status))
            redis_conn.expire(args.status_key, 24 * 3600)
        sys.exit(1)  # Exit with a non-zero code to indicate failure


if __name__ == "__main__":
    main()
