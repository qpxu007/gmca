# image_viewer/plugins/crystfel/crystfel_process_dataset.py
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


# Ensure qp2 is in path
def find_qp2_parent(file_path):
    path = os.path.abspath(file_path)
    while path != os.path.dirname(path):
        if os.path.basename(path) == "qp2":
            return os.path.dirname(path)
        path = os.path.dirname(path)
    return None


project_root = find_qp2_parent(__file__)
if project_root and project_root not in sys.path:
    sys.path.insert(0, project_root)

from qp2.log.logging_config import setup_logging, get_logger
from qp2.config.programs import ProgramConfig
from qp2.image_viewer.plugins.crystfel.crystfel_geometry import (
    generate_crystfel_geometry_file,
)
from qp2.image_viewer.plugins.crystfel.stream_utils import StreamParser
from qp2.xio.hdf5_manager import HDF5Reader
from qp2.image_viewer.plugins.crystfel.utils import calculate_robust_threshold_mad

logger = get_logger(__name__)


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


def main():
    parser = argparse.ArgumentParser(
        description="Run CrystFEL processing on a dataset (series)."
    )

    # Inputs
    parser.add_argument(
        "--master_file", required=True, help="Path to the HDF5 master file."
    )
    parser.add_argument(
        "--proc_dir", required=True, help="Output directory for processing results."
    )
    parser.add_argument(
        "--run_prefix", help="Run prefix for linking to DatasetRun (for future use)."
    )

    # Redis
    parser.add_argument("--redis_host", default="localhost")
    parser.add_argument("--redis_port", type=int, default=6379)
    parser.add_argument(
        "--redis_key", required=True, help="Redis key for storing job status/results."
    )
    parser.add_argument(
        "--status_key",
        help="Redis key for status (if different from results_key:status)",
    )

    # Processing Parameters
    parser.add_argument("--nproc", type=int, default=8, help="Number of processors.")
    parser.add_argument("--peak_method", default="peakfinder9")
    parser.add_argument("--min_snr", type=float, default=4.0)
    parser.add_argument("--min_snr_biggest_pix", type=float, default=3.0)
    parser.add_argument("--min_snr_peak_pix", type=float, default=2.0)
    parser.add_argument("--min_peaks", type=int, default=10)
    parser.add_argument("--no_non_hits", action="store_true")

    # Indexing
    parser.add_argument("--indexing_methods", default="xgandalf")
    parser.add_argument("--xgandalf_fast", action="store_true")
    parser.add_argument("--no_refine", action="store_true")
    parser.add_argument("--no_check_peaks", action="store_true")

    # Speed & Optimization
    parser.add_argument("--peakfinder8_fast", action="store_true")
    parser.add_argument("--asdf_fast", action="store_true")
    parser.add_argument("--no_retry", action="store_true")
    parser.add_argument("--no_multi", action="store_true")
    parser.add_argument("--push_res", type=float, default=None)
    parser.add_argument("--integration_mode", default="Standard")

    parser.add_argument("--min_sig", type=float, default=5.0)
    parser.add_argument("--local_bg_radius", type=int, default=3)
    parser.add_argument("--high_res_limit", type=float, default=None)
    parser.add_argument("--pdb", default=None, help="PDB/Cell file for indexing")
    parser.add_argument("--extra_options", default="", help="Extra indexamajig options")
    parser.add_argument("--int_radius", default="3,4,5", help="Integration radii: inner,middle,outer (e.g. 3,4,5)")

    # Peakfinder8 specific
    parser.add_argument("--peakfinder8_threshold", type=float, default=20.0)
    parser.add_argument("--peakfinder8_auto_threshold", action="store_true")
    parser.add_argument("--peakfinder8_min_pix_count", type=int, default=2)
    parser.add_argument("--peakfinder8_max_pix_count", type=int, default=200)

    args = parser.parse_args()

    setup_logging(root_name="qp2", log_level="INFO")

    # 1. Setup Redis
    try:
        redis_conn = redis.Redis(host=args.redis_host, port=args.redis_port)
        redis_conn.ping()
    except Exception as e:
        logger.error(f"Cannot connect to Redis: {e}")
        sys.exit(1)

    status_key = args.status_key or f"{args.redis_key}:status"

    # Update status to RUNNING
    redis_conn.set(
        status_key, json.dumps({"status": "RUNNING", "timestamp": time.time()})
    )

    try:
        # 2. Prepare Output Directory
        proc_dir = Path(args.proc_dir)
        proc_dir.mkdir(parents=True, exist_ok=True)

        # 3. Generate Geometry
        geom_file = proc_dir / f"{os.path.basename(args.master_file)}.geom"
        bad_pixel_file = proc_dir / f"{os.path.basename(args.master_file)}_mask.h5"

        generate_crystfel_geometry_file(
            master_file_path=args.master_file,
            output_geom_path=str(geom_file),
            bad_pixels_file_path=str(bad_pixel_file),
        )

        # 4. Generate File List (images.lst)
        file_list_path = proc_dir / "images.lst"

        data_files = []
        try:
            import h5py

            with h5py.File(args.master_file, "r") as f:
                if "/entry/data" in f:
                    for key in sorted(f["/entry/data"]):
                        link = f["/entry/data"].get(key, getlink=True)
                        if isinstance(link, h5py.ExternalLink):
                            master_dir = os.path.dirname(args.master_file)
                            data_path = os.path.join(master_dir, link.filename)
                            data_files.append(data_path)
        except Exception as e:
            logger.error(f"Failed to extract data files from master: {e}")
            raise

        if not data_files:
            # If no external links, use the master file itself
            data_files = [args.master_file]

        # Sort files naturally
        from qp2.image_viewer.utils.sort_files import natural_sort_key

        data_files.sort(key=natural_sort_key)

        with open(file_list_path, "w") as f:
            for df in data_files:
                f.write(f"{df}\n")

        # 5. Run indexamajig
        stream_file = proc_dir / "crystfel.stream"

        # --- Auto Threshold Calculation Logic ---
        if args.peak_method == "peakfinder8" and args.peakfinder8_auto_threshold:
            try:
                logger.info("Calculating Auto MAD threshold from first image...")
                # 1. Read first image
                # We can use the first file from data_files list.
                # However, HDF5Reader typically handles master files best.
                # Let's try to read the first frame (index 0) from the master file.
                reader = HDF5Reader(args.master_file, start_timer=False)
                # get_image(0) returns (data, header_dict)
                first_img_data, _ = reader.get_image(0)
                reader.close()

                if first_img_data is not None:
                    # 2. Read mask if available
                    mask = None
                    if bad_pixel_file.exists():
                         try:
                             with h5py.File(bad_pixel_file, "r") as mf:
                                 # Usually mask is at /data/data or similar, depends on generation
                                 # The generator puts it at /data/data
                                 if "/data/data" in mf:
                                     mask = mf["/data/data"][:]
                                     # Mask in file: 1=bad, 0=good usually?
                                     # utils expect mask where True=Bad. 
                                     # Let's check generation... it normally uses bitmask.
                                     # Assuming standard behavior, let's convert to boolean if needed or pass as is.
                                     # calculate_robust_threshold_mad handles standard numpy arrays.
                                     pass
                         except Exception as e:
                             logger.warning(f"Could not read mask for auto-threshold: {e}")

                    # 3. Calculate
                    calc_threshold = calculate_robust_threshold_mad(first_img_data, mask)

                    if calc_threshold is not None:
                        logger.info(f"Auto Calculated Threshold: {calc_threshold:.2f} (replacing {args.peakfinder8_threshold})")
                        args.peakfinder8_threshold = float(calc_threshold)
                    else:
                        logger.warning("Auto threshold calculation returned None. Using default.")
                else:
                    logger.warning("Could not read first image data. Using default threshold.")

            except Exception as e:
                logger.error(f"Failed to calculate auto threshold: {e}", exc_info=True)
                # Fallback to default/arg value

        # Sanitize int_radius: strip internal whitespace so "3, 4, 5" -> "3,4,5"
        args.int_radius = ",".join(p.strip() for p in args.int_radius.split(","))

        cmd = [
            "indexamajig",
            "-i",
            str(file_list_path),
            "-g",
            str(geom_file),
            "-o",
            str(stream_file),
            "-j",
            str(args.nproc),
            f"--peaks={args.peak_method}",
            f"--min-snr={args.min_snr}",
            f"--min-snr-biggest-pix={args.min_snr_biggest_pix}",
            f"--min-snr-peak-pix={args.min_snr_peak_pix}",
            f"--min-sig={args.min_sig}",
            f"--local-bg-radius={args.local_bg_radius}",
            f"--min-peaks={args.min_peaks}",
            f"--indexing={args.indexing_methods}",
            f"--int-radius={args.int_radius}",
        ]

        # Append peakfinder8 specific options
        if args.peak_method == "peakfinder8":
            cmd.append(f"--threshold={args.peakfinder8_threshold}")
            cmd.append(f"--min-pix-count={args.peakfinder8_min_pix_count}")
            cmd.append(f"--max-pix-count={args.peakfinder8_max_pix_count}")

        if args.xgandalf_fast:
            cmd.append("--xgandalf-fast")
        if args.no_refine:
            cmd.append("--no-refine")
        if args.no_check_peaks:
            cmd.append("--no-check-peaks")

        if args.no_non_hits:
            cmd.append("--no-non-hits-in-stream")
        if args.peakfinder8_fast:
            cmd.append("--peakfinder8-fast")
        if args.asdf_fast:
            cmd.append("--asdf-fast")
        if args.no_retry:
            cmd.append("--no-retry")
        if args.no_multi:
            cmd.append("--no-multi")
        if args.push_res is not None and args.push_res > 0:
            cmd.append(f"--push-res={args.push_res}")

        if args.integration_mode == "None (No Intensity)":
            cmd.append("--integration=none")
        elif args.integration_mode == "Cell Only (No Prediction)":
            cmd.append("--cell-parameters-only")

        if args.pdb:
            cmd.extend(["-p", args.pdb])

        if args.extra_options:
            import shlex

            cmd.extend(shlex.split(args.extra_options))

        logger.info(f"Running indexamajig...")
        run_crystfel_command(cmd, str(proc_dir))

        # 6. Parse Stream and Save JSON
        logger.info(
            f"Parsing stream file... {stream_file}, high_res = {args.high_res_limit}"
        )
        parser = StreamParser(
            stream_file_path=str(stream_file),
            high_res_limit=args.high_res_limit,
            max_reflections=99999,
        )

        results_map = {}
        for res in parser.all_results:
            img_num = res.get("image_serial_number", None) or res.get("event_num", 0) + 1
            if img_num is None:
                continue

            # Create result dictionary consistent with the viewer's expectations
            result_dict = {
                "img_num": img_num,
                "num_spots_crystfel": res.get("num_peaks", 0),
                "spots_crystfel": res.get("spots_crystfel", []),
                "unit_cell_crystfel": res.get("unit_cell_crystfel"),
                "crystfel_indexed_by": res.get("indexed_by", "none"),
                "crystfel_lattice": res.get("lattice_type"),
                "hit": res.get("hit", 0),
                "crystfel_centering": res.get("centering"),
                "reflections_crystfel": res.get("reflections_crystfel", []),
                "is_indexed": 1.0 if res.get("indexed_by") != "none" else 0.0,
                "timestamp": time.time(),
            }
            results_map[img_num] = result_dict

        # Save to JSON
        json_path = proc_dir / "crystfel_results.json"
        with open(json_path, "w") as f:
            json.dump(results_map, f, indent=2)

        logger.info(f"Saved {len(results_map)} results to {json_path}")

        # 7. Update Redis
        # Register the stream file as a segment for the merging tool
        segments_key = f"{args.redis_key}:segments"
        redis_conn.hset(segments_key, "0", str(stream_file))
        redis_conn.expire(segments_key, 7 * 24 * 3600)  # 1 week

        # Save metadata to the main hash
        redis_conn.hset(args.redis_key, "results_json", str(json_path))
        redis_conn.hset(args.redis_key, "_proc_dir", str(proc_dir))
        redis_conn.expire(args.redis_key, 7 * 24 * 3600)

        # Update status
        redis_conn.set(
            status_key,
            json.dumps(
                {
                    "status": "COMPLETED",
                    "timestamp": time.time(),
                    "results_json": str(json_path),
                    "stream_file": str(stream_file),
                }
            ),
        )

    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        redis_conn.set(
            status_key,
            json.dumps({"status": "FAILED", "timestamp": time.time(), "error": str(e)}),
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
