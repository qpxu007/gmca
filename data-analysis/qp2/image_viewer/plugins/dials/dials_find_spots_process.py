# qp2/data_proc/server/pipelines/dials/dials_find_spots_process.py
"""
Standalone script for distributed DIALS spot finding.

This script runs `dials.import` and `dials.find_spots` for a specific
frame range, parses the resulting reflection table, and saves the
spot information to a Redis HASH.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from time import time

import redis

# --- Path Setup ---
try:
    # Assumes this script is at '.../qp2/data_proc/server/pipelines/dials/'
    project_root = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
    )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from qp2.log.logging_config import setup_logging, get_logger

    # DIALS imports are needed to read the results
except ImportError as e:
    print(
        f"CRITICAL: Failed to import modules. Ensure DIALS is in the PYTHONPATH. Error: {e}",
        file=sys.stderr,
    )
    sys.exit(1)


# --- End Path Setup ---


def run_dials_command(cmd, cwd):
    """Helper to run a DIALS command and log its output."""
    logger = get_logger(__name__)
    logger.info(f"Executing command in {cwd}: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.stdout:
        logger.debug(f"DIALS STDOUT:\n{result.stdout}")
    if result.stderr:
        logger.warning(f"DIALS STDERR:\n{result.stderr}")
    if result.returncode != 0:
        raise RuntimeError(
            f"DIALS command failed with exit code {result.returncode}:\n{' '.join(cmd)}"
        )
    return result


def main():
    parser = argparse.ArgumentParser(description="Run distributed DIALS spot finding.")
    parser.add_argument(
        "--project_root",
        required=True,
        help="Path to the qp2 project root for imports.",
    )

    parser.add_argument("--metadata", type=json.loads, required=True)
    parser.add_argument("--start_frame", type=int, required=True)
    parser.add_argument("--end_frame", type=int, required=True)
    parser.add_argument("--redis_host", type=str)
    parser.add_argument("--redis_port", type=int, default=6379)
    parser.add_argument("--redis_key_prefix", type=str, required=True)
    # DIALS-specific parameters can be added here
    parser.add_argument("--min_spot_size", type=int, default=3)

    args = parser.parse_args()

    setup_logging(
        root_name="qp2.dials_spot_process", log_level="INFO", use_console=True
    )
    logger = get_logger(__name__)

    # Create a unique temporary directory for this job
    wdir = tempfile.mkdtemp(prefix="dials_tmp_")
    logger.info(f"Using temporary working directory: {wdir}")

    try:
        # --- 1. Run dials.import ---
        master_file = args.metadata["master_file"]
        # DIALS uses 1-based indexing for image_range
        image_range = f"{args.start_frame + 1},{args.end_frame + 1}"
        import_cmd = [
            "dials.import",
            master_file,
            f"image_range={image_range}",
            "output.experiments=imported.expt",
        ]
        run_dials_command(import_cmd, wdir)

        # --- 2. Run dials.find_spots ---
        find_spots_cmd = [
            "dials.find_spots",
            "imported.expt",
            "output.reflections=strong.refl",
            f"min_spot_size={args.min_spot_size}",
        ]
        run_dials_command(find_spots_cmd, wdir)

        # --- 3. Parse the results ---
        refl_path = os.path.join(wdir, "strong.refl")
        if not os.path.exists(refl_path):
            logger.warning(
                "dials.find_spots did not produce a strong.refl file. No spots found."
            )
            return

        from dials.array_family.flex import reflection_table

        rt = reflection_table.from_file(refl_path)
        logger.info(f"DIALS found {len(rt)} total spots in the frame range.")

        # Group spots by frame number
        results_by_frame = {}
        # 'xyzobs.px.value' is a flex.vec3_double array of (x_px, y_px, z_frame)
        for spot in rt["xyzobs.px.value"]:
            # z_frame is 0-based within the imported range
            frame_in_batch = int(round(spot[2]))
            # Convert to absolute 0-based frame index
            absolute_frame_idx = args.start_frame + frame_in_batch

            if absolute_frame_idx not in results_by_frame:
                results_by_frame[absolute_frame_idx] = []

            # Store as (x, y)
            results_by_frame[absolute_frame_idx].append((spot[0], spot[1]))

        # --- 4. Save results to Redis ---
        redis_conn = (
            redis.Redis(host=args.redis_host, port=args.redis_port)
            if args.redis_host
            else None
        )
        if redis_conn:
            redis_key = f"{args.redis_key_prefix}:{master_file}"
            with redis_conn.pipeline() as pipe:
                for frame_idx, spots in results_by_frame.items():
                    result_dict = {
                        "img_num": frame_idx + 1,  # Use 1-based index for consistency
                        "num_spots_dials": len(spots),
                        "spots_dials": spots,  # Already in (x, y) format
                        "timestamp": time(),
                    }
                    pipe.hset(
                        redis_key, result_dict["img_num"], json.dumps(result_dict)
                    )
                pipe.execute()
            logger.info(
                f"Successfully saved results for {len(results_by_frame)} frames to Redis."
            )

    except Exception as e:
        logger.error(f"DIALS spot finding process failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # --- 5. Cleanup ---
        logger.info(f"Cleaning up temporary directory: {wdir}")
        shutil.rmtree(wdir)


if __name__ == "__main__":
    main()
