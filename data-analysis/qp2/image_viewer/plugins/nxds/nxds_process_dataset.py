# qp2/image_viewer/plugins/nxds/nxds_process_dataset.py
import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import redis


# Add the project root to sys.path to find qp2
def find_project_root(file_path):
    path = Path(file_path).resolve()
    for parent in path.parents:
        if (parent / "qp2").is_dir():
            return str(parent)
    return None


project_root = find_project_root(__file__)
if project_root and project_root not in sys.path:
    sys.path.insert(0, project_root)

from qp2.pipelines.gmcaproc.xds2 import nXDS
from qp2.pipelines.gmcaproc.hdf5reader import HDF5Reader
from qp2.pipelines.gmcaproc.cbfreader import CbfReader
from qp2.pipelines.gmcaproc.symmetry import Symmetry
from qp2.log.logging_config import setup_logging, get_logger
from qp2.xio.db_manager import get_beamline_from_hostname

logger = get_logger(__name__)


def validate_and_format_unit_cell(cell_str: str) -> str:
    """
    Validates that the unit cell string contains 6 numbers and returns it
    in a canonical space-separated format. Exits on failure.
    """
    if not cell_str:
        return None  # It's an optional argument

    try:
        sanitized_str = cell_str.replace(",", " ")
        params = [float(p) for p in sanitized_str.split()]
        if len(params) != 6:
            raise ValueError(f"Expected 6 parameters, but got {len(params)}")
        return " ".join(map(str, params))
    except (ValueError, TypeError) as e:
        logger.error(
            f"Invalid --unit_cell format: '{cell_str}'. Must be 6 numbers. Error: {e}"
        )
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Run nXDS processing and report result location."
    )
    parser.add_argument("--master_file", required=True)
    parser.add_argument("--proc_dir", required=True)
    parser.add_argument("--redis_key", required=True)
    parser.add_argument("--status_key", required=True)
    parser.add_argument("--nproc", type=int, default=8)
    parser.add_argument("--njobs", type=int, default=1)
    parser.add_argument("--powder", action="store_true")
    parser.add_argument("--native", action="store_true", help="Process native data")
    parser.add_argument("--resolution", type=float, help="High resolution cutoff")
    parser.add_argument("--space_group", type=str)
    parser.add_argument("--unit_cell", type=str)
    parser.add_argument("--reference_hkl", type=str)
    parser.add_argument("--redis_host", type=str, required=True)
    parser.add_argument("--redis_port", type=int, default=6379)
    parser.add_argument(
        "--xds_param",
        action="append",
        help="Additional XDS.INP parameters in KEY=VALUE format (can be used multiple times).",
    )
    # New arguments for database logging, consistent with other scripts
    parser.add_argument(
        "--group_name", type=str, help="Primary group name for logging."
    )
    parser.add_argument(
        "--run_prefix", type=str, help="Run prefix for linking to DatasetRun."
    )
    parser.add_argument("--pi_badge", type=int, help="PI badge number for logging.")
    parser.add_argument("--esaf_number", type=int, help="ESAF number for logging.")
    parser.add_argument("--beamline", type=str, help="Beamline for logging.")

    args = parser.parse_args()

    setup_logging(root_name="qp2", log_level=logging.INFO)
    redis_conn = redis.Redis(
        host=args.redis_host, port=args.redis_port, decode_responses=True
    )

    try:
        redis_conn.set(
            args.status_key, json.dumps({"status": "RUNNING", "timestamp": time.time()}),
            ex=7 * 24 * 3600,
        )

        # Parse xds_param into a dictionary
        extra_xds_params = {}
        if args.xds_param:
            for param in args.xds_param:
                if "=" in param:
                    key, value = param.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    if key in extra_xds_params:
                        if isinstance(extra_xds_params[key], list):
                            extra_xds_params[key].append(value)
                        else:
                            extra_xds_params[key] = [extra_xds_params[key], value]
                    else:
                        extra_xds_params[key] = value
                else:
                    logger.warning(f"Ignoring invalid parameter format: {param}. Expected KEY=VALUE.")

        validated_unit_cell = validate_and_format_unit_cell(args.unit_cell)
        space_group_for_nxds = args.space_group
        if args.space_group and not args.space_group.isdigit():
            sg_number = Symmetry.symbol_to_number(args.space_group)
            if sg_number:
                logger.info(
                    f"Converted space group symbol '{args.space_group}' to number {sg_number}."
                )
                space_group_for_nxds = str(sg_number)
            else:
                logger.warning(
                    f"Could not convert space group symbol '{args.space_group}' to a number. Passing it as is."
                )

        if args.master_file.endswith((".h5", ".hdf5")):
            dataset_reader = HDF5Reader(args.master_file)
        elif args.master_file.endswith(".cbf"):
            dataset_reader = CbfReader(args.master_file)
        else:
            raise ValueError(f"Unsupported file type: {args.master_file}")

        pipeline_params = {
            "username": os.getenv("USER"),
            "primary_group": args.group_name,
            "run_prefix": args.run_prefix,
            "pi_id": args.pi_badge,
            "esaf_id": args.esaf_number,
            "beamline": args.beamline or get_beamline_from_hostname(),
        }

        nxds_proc = nXDS(
            dataset=dataset_reader,
            proc_dir=args.proc_dir,
            nproc=args.nproc,
            njobs=args.njobs,
            user_space_group=space_group_for_nxds,
            user_unit_cell=validated_unit_cell,
            user_resolution_cutoff=args.resolution,
            reference_hkl=args.reference_hkl,
            powder=args.powder,
            user_native=args.native,
            use_slurm=False,  # Job is already on a cluster node
            pipeline_params=pipeline_params,
            extra_xds_inp_params=extra_xds_params,
        )

        # This runs the entire nXDS workflow and generates nXDS.json
        nxds_proc.process()

        # Check for failure
        if "error_step" in nxds_proc.results:
            raise RuntimeError(
                f"nXDS failed at step {nxds_proc.results.get('error_step')}: {nxds_proc.results.get('error_message')}"
            )

        # The result of the processing is the path to the JSON file.
        # output_json_path = Path(args.proc_dir) / "nXDS.json"
        output_json_path = nxds_proc.results.get("nxds_json_path")
        if not output_json_path or not os.path.exists(output_json_path):
            raise FileNotFoundError(
                f"nXDS processing finished but did not create {output_json_path}"
            )

        if redis_conn:
            # Clear any old data in the hash
            redis_conn.delete(args.redis_key)
            # Store the path to the results file and the processing directory
            with redis_conn.pipeline() as pipe:
                pipe.hset(args.redis_key, "_results_json_path", str(output_json_path))
                pipe.hset(args.redis_key, "_proc_dir", args.proc_dir)
                pipe.expire(args.redis_key, 7 * 24 * 3600)  # 1-week expiration
                pipe.execute()

            # Update the master status key to COMPLETED
            final_status = {"status": "COMPLETED", "timestamp": time.time()}
            redis_conn.set(args.status_key, json.dumps(final_status), ex=7 * 24 * 3600)  # 1-week expiration
            logger.info(
                f"nXDS processing completed. Result path saved to Redis for {args.master_file}."
            )

    except Exception as e:
        logger.error(f"nXDS dataset process failed: {e}", exc_info=True)
        if redis_conn:
            failed_status = {
                "status": "FAILED",
                "timestamp": time.time(),
                "error": str(e),
            }
            redis_conn.set(args.status_key, json.dumps(failed_status), ex=7 * 24 * 3600)
        sys.exit(1)


if __name__ == "__main__":
    main()
