# qp2/image_viewer/plugins/dials/parse_dials.py
import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import redis  # need to install redis-py to dials env
# This script is now run by dials.python, so imports will work.
from dials.array_family import flex
from dxtbx.model.experiment_list import ExperimentListFactory


# Add qp2 to path for logging config (still good practice)
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

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Parse DIALS results and save them to Redis."
    )

    parser.add_argument("--proc_dir", required=True)
    parser.add_argument("--redis_key", required=True)
    parser.add_argument("--status_key", required=True)
    parser.add_argument("--redis_host", type=str, required=True)
    parser.add_argument("--redis_port", type=int, default=6379)
    args = parser.parse_args()

    setup_logging(root_name="qp2", log_level=logging.INFO)
    redis_conn = redis.Redis(
        host=args.redis_host, port=args.redis_port, decode_responses=True
    )

    try:
        wdir = Path(args.proc_dir)

        # The DIALS commands are now assumed to have been run by the calling shell script.
        # This script's only job is to parse the output.

        # 1. Parse strong.refl for spot counts
        strong_refl_path = wdir / "strong.refl"
        if not strong_refl_path.exists():
            raise FileNotFoundError("dials.find_spots did not produce strong.refl.")

        strong_rt = flex.reflection_table.from_file(str(strong_refl_path))
        logger.info(f"DIALS found {len(strong_rt)} total spots.")

        # 2. Parse indexed.expt for indexing results
        indexed_expt_path = wdir / "indexed.expt"
        indexed_crystals = {}
        if indexed_expt_path.exists():
            indexed_expts = ExperimentListFactory.from_json_file(
                str(indexed_expt_path), check_format=False
            )
            for i, crystal in enumerate(indexed_expts.crystals()):
                image_index = int(indexed_expts[i].identifier)
                indexed_crystals[image_index] = crystal
        else:
            logger.warning(
                "indexed.expt not found. Indexing step likely failed. Only spot data will be reported."
            )

        # 3. Aggregate data per frame and prepare for Redis
        results_by_frame = {}
        imageset = ExperimentListFactory.from_json_file(
            str(wdir / "imported.expt"), check_format=False
        )[0].imageset
        total_frames = len(imageset)

        for i in range(total_frames):
            frame_num_1based = i + 1
            frame_spots_table = strong_rt.select(
                flex.abs(strong_rt["xyzobs.px.value"].parts()[2] - i) <= 0.5
            )

            # Extract the (x, y) coordinates for each spot in this frame
            spot_coords = []
            for spot in frame_spots_table["xyzobs.px.value"]:
                spot_coords.append([spot[0], spot[1]])  # Store as [x, y]

            result_dict = {
                "img_num": frame_num_1based,
                "num_spots_dials": len(frame_spots_table),
                "spots_dials": spot_coords,
                "dials_indexed": False,
                "timestamp": time.time(),
            }

            if i in indexed_crystals:
                crystal = indexed_crystals[i]
                result_dict["dials_indexed"] = True
                result_dict["unit_cell_dials"] = list(
                    crystal.get_unit_cell().parameters()
                )
                result_dict["space_group_dials"] = (
                    crystal.get_space_group().info().symbol_and_number()
                )

            results_by_frame[frame_num_1based] = result_dict

        if redis_conn:
            redis_conn.delete(args.redis_key)
            with redis_conn.pipeline() as pipe:
                for frame_num, data in results_by_frame.items():
                    pipe.hset(args.redis_key, frame_num, json.dumps(data))
                pipe.hset(args.redis_key, "_proc_dir", args.proc_dir)
                pipe.expire(args.redis_key, 24 * 3600)  # 24-hour expiration
                pipe.execute()
            logger.info(
                f"Successfully saved results for {len(results_by_frame)} frames to Redis."
            )

        if not indexed_expt_path.exists():
            # If indexing failed, the overall job is considered failed from a user perspective.
            raise FileNotFoundError(
                "Processing failed at indexing stage (indexed.expt not found)."
            )

        final_status = {"status": "COMPLETED", "timestamp": time.time()}
        redis_conn.set(args.status_key, json.dumps(final_status), ex=24 * 3600)  # 24-hour expiration

    except Exception as e:
        logger.error(f"DIALS result parsing failed: {e}", exc_info=True)
        if redis_conn:
            failed_status = {
                "status": "FAILED",
                "timestamp": time.time(),
                "error": str(e),
            }
            redis_conn.set(args.status_key, json.dumps(failed_status), ex=24 * 3600)
        sys.exit(1)


if __name__ == "__main__":
    main()
