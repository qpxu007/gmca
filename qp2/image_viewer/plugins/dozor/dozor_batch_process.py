# qp2/image_viewer/plugins/dozor/dozor_batch_process.py
import argparse
import json
import logging
import sys
import time
import os

import redis


try:
    # Assumes this script is at '.../qp2/image_viewer/plugins/dozor/'
    project_root = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
    )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from qp2.log.logging_config import setup_logging, get_logger

except ImportError as e:
    print(
        f"CRITICAL: Failed to import modules. Ensure the project root is in the PYTHONPATH. Error: {e}",
        file=sys.stderr,
    )
    sys.exit(1)


# This assumes the script is run from a location where qp2 is in the python path
from qp2.log.logging_config import setup_logging, get_logger
from qp2.image_viewer.plugins.dozor.dozor_process import (
    dozor_job,
    check_frames_exist_in_redis_hash,
)


def main():
    parser = argparse.ArgumentParser(
        description="Run a batch of Dozor jobs sequentially."
    )
    parser.add_argument(
        "--jobs_json",
        type=str,
        required=True,
        help="JSON string of a list of job descriptions.",
    )
    parser.add_argument("--redis_host", type=str, required=True)
    parser.add_argument("--redis_port", type=int, default=6379)
    parser.add_argument(
        "--redis_key_prefix",
        type=str,
        default="analysis:out:spots:dozor2",
    )
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument(
        "--status_key",
        type=str,
        required=True,
        help="Redis HASH key for status updates.",
    )
    parser.add_argument(
        "--job_name",
        type=str,
        required=True,
        help="The name of this batch job (field in the status hash).",
    )
    args = parser.parse_args()

    setup_logging(root_name="qp2.dozor_batch", log_level=logging.INFO)
    logger = get_logger(__name__)

    try:
        jobs = json.loads(args.jobs_json)
        if not isinstance(jobs, list):
            raise TypeError("jobs_json must be a list of job objects.")
    except (json.JSONDecodeError, TypeError) as e:
        logger.error(f"Failed to parse --jobs_json argument: {e}")
        sys.exit(1)

    try:
        redis_conn = redis.Redis(
            host=args.redis_host, port=args.redis_port, decode_responses=True
        )
        redis_conn.ping()
    except redis.RedisError as e:
        logger.error(f"Could not connect to Redis: {e}. Aborting batch.")
        sys.exit(1)

    try:
        running_status = {"status": "RUNNING", "timestamp": time.time()}
        redis_conn.hset(args.status_key, args.job_name, json.dumps(running_status))
    except redis.RedisError as e:
        logger.warning(f"Could not update job status to RUNNING: {e}")

    logger.info(f"Starting Dozor batch processing for {len(jobs)} tasks.")

    batch_failed = False  # Flag to track if any task fails
    first_error = None

    for i, job_desc in enumerate(jobs):
        try:
            metadata = job_desc["metadata"]
            start_frame = job_desc["start_frame"]
            nimages = job_desc["nimages"]
            master_file = metadata["master_file"]

            logger.info(
                f"--- Task {i+1}/{len(jobs)}: Processing {master_file} frames {start_frame}-{start_frame+nimages-1} ---"
            )

            # Pre-check if results already exist in Redis
            redis_key = f"{args.redis_key_prefix}:{master_file}"

            # --- START: MORE ROBUST CHECK AND LOGGING ---
            logger.info(f"Checking for existing results in Redis key: {redis_key}")
            # Use a non-decoded connection for the check function if it expects bytes
            redis_conn_bytes = redis.Redis(host=args.redis_host, port=args.redis_port)
            all_frames_exist = check_frames_exist_in_redis_hash(
                redis_conn_bytes, redis_key, start_frame, nimages
            )

            if all_frames_exist:
                logger.info(
                    f"All frames {start_frame}-{start_frame+nimages-1} already found in Redis. Skipping this task."
                )
                continue
            else:
                logger.info(
                    "Not all frames found in Redis. Proceeding with processing."
                )
            # --- END: MORE ROBUST CHECK AND LOGGING ---

            # Run the imported dozor_job function
            # Pass the original decoded connection to dozor_job
            dozor_job(
                metadata=metadata,
                redis_conn=redis_conn,
                redis_key_prefix=args.redis_key_prefix,
                start=start_frame,
                nimages=nimages,
                debug=args.debug,
            )
            logger.info(f"--- Task {i+1} completed successfully. ---")

        except Exception as e:
            logger.error(f"Task {i+1} failed: {job_desc}. Error: {e}", exc_info=True)
            batch_failed = True
            if first_error is None:
                first_error = str(e)
            # Continue to the next job in the batch

    try:
        if batch_failed:
            final_status = {
                "status": "FAILED",
                "timestamp": time.time(),
                "error": f"At least one task failed. First error: {first_error}",
            }
        else:
            final_status = {"status": "COMPLETED", "timestamp": time.time()}

        redis_conn.hset(args.status_key, args.job_name, json.dumps(final_status))
        logger.info(
            f"Dozor batch processing finished. Final status: {final_status['status']}"
        )

    except redis.RedisError as e:
        logger.error(f"Failed to set final job status in Redis: {e}")

    if batch_failed:
        sys.exit(1)  # Exit with an error code if any part of the batch failed

    logger.info("Dozor batch processing finished.")


if __name__ == "__main__":
    main()
