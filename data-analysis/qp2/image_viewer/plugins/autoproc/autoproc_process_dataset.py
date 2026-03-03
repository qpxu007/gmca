# qp2/image_viewer/plugins/autoproc/autoproc_process_dataset.py
import argparse
import json
import logging
import re
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

# Import the main function from your existing script
from qp2.pipelines.autoproc_xia2.main import main as run_pipeline_main
from qp2.log.logging_config import setup_logging, get_logger
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.xio.user_group_manager import UserGroupManager

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Run autoPROC processing and report status."
    )
    # This script will receive all the same arguments as the original main.py
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--data", required=True, action="append")
    parser.add_argument("--work_dir", required=True)
    parser.add_argument("--highres", type=float)
    parser.add_argument("--space_group", type=str)
    parser.add_argument("--unit_cell", type=str)
    parser.add_argument("--model", type=str)
    parser.add_argument("--nproc", type=int)
    parser.add_argument("--njobs", type=int)
    parser.add_argument("--fast", action="store_true")
    parser.add_argument("--native", action="store_true")

    # Add redis arguments
    parser.add_argument("--status_key", required=True)
    parser.add_argument("--redis_host", required=True)
    parser.add_argument("--redis_port", required=True)

    # New arguments for database logging
    parser.add_argument(
        "--group_name", type=str, help="Primary group name for logging."
    )
    parser.add_argument(
        "--run_prefix", type=str, help="Run prefix for linking to DatasetRun."
    )
    parser.add_argument("--pi_badge", type=int, help="PI badge number for logging.")
    parser.add_argument("--esaf_number", type=int, help="ESAF number for logging.")
    default_beamline = get_beamline_from_hostname()
    parser.add_argument("--beamline", default=default_beamline, type=str)
    parser.add_argument("--runner", default="slurm", type=str, help="Job runner (slurm/shell)")

    args = parser.parse_args()

    setup_logging(root_name="qp2.autoproc_process", log_level=logging.INFO)
    redis_conn = redis.Redis(
        host=args.redis_host, port=args.redis_port, decode_responses=True
    )

    try:
        # --- 1. Report RUNNING status ---
        running_status = {"status": "RUNNING", "timestamp": time.time()}
        redis_conn.set(args.status_key, json.dumps(running_status), ex=7 * 24 * 3600)

        # --- Gather DB logging info, prioritizing command-line args ---
        primary_group = args.group_name
        pi_id = args.pi_badge
        esaf_id = args.esaf_number

        # Infer if not provided
        if not all([primary_group, pi_id, esaf_id]) and args.data:
            match = re.search(r"(esaf\d+)", args.data[0])
            if match:
                groupname = match.group(1)
                try:
                    user_group_mgr = UserGroupManager()
                    group_info = user_group_mgr.groupinfo_from_groupname(groupname)
                    if group_info:
                        if not primary_group:
                            primary_group = group_info.get("group_name")
                        if not pi_id:
                            pi_id = group_info.get("pi_badge")
                        if not esaf_id:
                            esaf_id = group_info.get("esaf_number")
                    elif not primary_group:
                        primary_group = groupname
                except Exception as e:
                    logger.warning(f"Could not get group info for '{groupname}': {e}")
                    if not primary_group:
                        primary_group = groupname

        # --- 2. Call the original main function ---
        # We need to temporarily replace sys.argv so run_pipeline_main can parse it
        original_argv = sys.argv
        sys.argv = original_argv[0:1] + [
            "--pipeline",
            args.pipeline,
            "--work_dir",
            args.work_dir,
            "--nproc",
            str(args.nproc or 1),
            "--njobs",
            str(args.njobs or 1),
        ]
        # Append data args
        for d in args.data:
            sys.argv.extend(["--data", d])
        # Append optional args
        if args.highres:
            sys.argv.extend(["--highres", str(args.highres)])
        if args.space_group:
            sys.argv.extend(["--space_group", args.space_group])
        if args.unit_cell:
            sys.argv.extend(["--unit_cell", args.unit_cell])
        if args.model:
            sys.argv.extend(["--model", args.model])
        if args.fast:
            sys.argv.append("--fast")
        if args.native:
            sys.argv.append("--native")

        # Add pipeline parameters for DB logging
        if primary_group:
            sys.argv.extend(["--primary_group", primary_group])
        if args.run_prefix:
            sys.argv.extend(["--run_prefix", args.run_prefix])
        if pi_id:
            sys.argv.extend(["--pi_id", str(pi_id)])
        if esaf_id:
            sys.argv.extend(["--esaf_id", str(esaf_id)])
        if args.beamline:
            sys.argv.extend(["--beamline", args.beamline])
        if args.runner:
            sys.argv.extend(["--runner", args.runner])

        logger.info(f"Executing underlying pipeline with args: {sys.argv}")
        run_pipeline_main()  # This will run the actual processing
        sys.argv = original_argv  # Restore original argv

        # --- 3. Report COMPLETED status ---
        completed_status = {"status": "COMPLETED", "timestamp": time.time()}
        redis_conn.set(args.status_key, json.dumps(completed_status), ex=7 * 24 * 3600)
        logger.info("autoPROC process completed successfully.")

    except Exception as e:
        logger.error(f"autoPROC process failed: {e}", exc_info=True)
        # --- 4. Report FAILED status ---
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