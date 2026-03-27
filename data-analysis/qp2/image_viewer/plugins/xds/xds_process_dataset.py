# qp2/image_viewer/plugins/xds/xds_process_dataset.py
import argparse
import json
import logging
import os
import re
import socket
import sys
import time
from pathlib import Path

import redis


def find_project_root(file_path):
    path = Path(file_path).resolve()
    for parent in path.parents:
        if (parent / "qp2").is_dir():
            return str(parent)
    return None


project_root = find_project_root(__file__)
if project_root and project_root not in sys.path:
    sys.path.insert(0, project_root)

from qp2.pipelines.gmcaproc.xds2 import XDS
from qp2.pipelines.gmcaproc.cbfreader import CbfReader
from qp2.pipelines.gmcaproc.hdf5reader import HDF5Reader
from qp2.log.logging_config import setup_logging, get_logger
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.xio.user_group_manager import UserGroupManager

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Run XDS processing and report results."
    )
    parser.add_argument("--master_file", required=True)
    parser.add_argument("--proc_dir", required=True)
    parser.add_argument("--redis_key", required=True)
    parser.add_argument("--status_key", required=True)
    parser.add_argument("--redis_host", type=str)
    parser.add_argument("--redis_port", type=int)

    parser.add_argument("--nproc", type=int)
    parser.add_argument("--njobs", type=int)
    parser.add_argument("--space_group", type=str)
    parser.add_argument("--unit_cell", type=str)

    parser.add_argument("--resolution", type=float, help="High resolution cutoff.")
    parser.add_argument(
        "--native", action="store_true", help="Process native data."
    )
    parser.add_argument(
        "--optimization",
        action="store_true",
        help="iterate XDS integrate and correct until convergence",
    )
    parser.add_argument("--model", type=str, help="PDB model for Dimple.")

    parser.add_argument("--reference_hkl", type=str)
    deafult_beamline = get_beamline_from_hostname()
    parser.add_argument("--beamline", default=deafult_beamline, type=str)

    # New arguments for database logging, consistent with other scripts
    parser.add_argument(
        "--group_name", type=str, help="Primary group name for logging."
    )
    parser.add_argument(
        "--run_prefix", type=str, help="Run prefix for linking to DatasetRun."
    )
    parser.add_argument("--pi_badge", type=int, help="PI badge number for logging.")
    parser.add_argument("--esaf_number", type=int, help="ESAF number for logging.")
    parser.add_argument(
        "--xds_param",
        action="append",
        help="Additional XDS.INP parameters in KEY=VALUE format (can be used multiple times).",
    )
    parser.add_argument("--start", type=int, help="Start frame number.")
    parser.add_argument("--end", type=int, help="End frame number.")

    args = parser.parse_args()

    log_file_path = os.path.join(args.proc_dir, "xds.log")
    setup_logging(root_name="qp2", log_level=logging.INFO, log_file=log_file_path)
    redis_conn = None
    if args.redis_host and args.redis_port:
        redis_conn = redis.Redis(
            host=args.redis_host, port=args.redis_port, decode_responses=True
        )

    try:
        if redis_conn:
            redis_conn.set(
                args.status_key,
                json.dumps({"status": "RUNNING", "timestamp": time.time()}),
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

        if args.master_file.endswith((".h5", ".hdf5")):
            dataset_reader = HDF5Reader(args.master_file)
        else:
            dataset_reader = CbfReader(args.master_file)

        metadata = dataset_reader.get_metadata()

        # Prioritize command-line arguments for DB logging
        primary_group = args.group_name
        pi_id = args.pi_badge
        esaf_id = args.esaf_number

        # If not all info is provided, try to infer it from the master file path
        if not all([primary_group, pi_id, esaf_id]):
            match = re.search(r"(esaf\d+)", args.master_file)
            if match:
                groupname = match.group(1)
                try:
                    user_group_mgr = UserGroupManager()
                    group_info = user_group_mgr.groupinfo_from_groupname(groupname)
                    if group_info:
                        # Only fill in the missing pieces
                        if not primary_group:
                            primary_group = group_info.get("group_name")
                        if not pi_id:
                            pi_id = group_info.get("pi_badge")
                        if not esaf_id:
                            esaf_id = group_info.get("esaf_number")
                    # If lookup fails, use the esaf string as the group name if it's still missing
                    elif not primary_group:
                        primary_group = groupname
                except Exception as e:
                    logger.warning(f"Could not get group info for '{groupname}': {e}")
                    # Fallback if lookup fails
                    if not primary_group:
                        primary_group = groupname

        pipeline_params = {
            "command": " ".join(sys.argv),
            "workdir": args.proc_dir,
            "imagedir": os.path.dirname(args.master_file),
            "beamline": args.beamline,
            "username": os.getenv("USER"),
            "hostname": socket.gethostname(),
            "sampleName": metadata.get("prefix", "unknown"),
            "logfile": log_file_path,
            "primary_group": primary_group,
            "run_prefix": args.run_prefix,
            "pi_id": pi_id,
            "esaf_id": esaf_id,
        }
        logger.info(f"Pipeline parameters: {pipeline_params}")
        xds_proc = XDS(
            dataset=dataset_reader,
            proc_dir=args.proc_dir,
            nproc=args.nproc,
            njobs=args.njobs,
            optimization=args.optimization,
            user_space_group=args.space_group,
            user_unit_cell=args.unit_cell,
            user_resolution_cutoff=args.resolution,
            user_native=args.native,
            reference_hkl=args.reference_hkl,
            user_model=args.model,
            use_slurm=False,
            use_redis=bool(redis_conn),
            pipeline_params=pipeline_params,
            extra_xds_inp_params=extra_xds_params,
            user_start=args.start,
            user_end=args.end,
        )
        xds_proc.process()

        if "error_step" in xds_proc.results:
            raise RuntimeError(f"XDS failed: {xds_proc.results.get('error_message')}")

        if redis_conn:
            redis_conn.delete(args.redis_key)
            output_json_path = os.path.join(args.proc_dir, "XDS.json")
            with redis_conn.pipeline() as pipe:
                pipe.hset(args.redis_key, "_results_json_path", str(output_json_path))
                pipe.hset(args.redis_key, "_proc_dir", args.proc_dir)
                pipe.expire(args.redis_key, 7 * 24 * 3600)  # 1-week expiration
                pipe.execute()

            final_status = {"status": "COMPLETED", "timestamp": time.time()}
            redis_conn.set(args.status_key, json.dumps(final_status), ex=7 * 24 * 3600)  # 1-week expiration


    except Exception as e:
        logger.error(f"XDS process script failed: {e}", exc_info=True)
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
