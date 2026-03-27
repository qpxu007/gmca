import argparse
import logging
import os
import sys
from PyQt5 import QtCore
import redis

from qp2.pipelines.raster_3d.matrix_builder import find_master_files
from qp2.pipelines.raster_3d.scan_mode import detect_raster_scan_mode
from qp2.pipelines.raster_3d.config import DEFAULT_CONFIG
from qp2.pipelines.raster_3d.paths import get_raster_3d_proc_dir
from qp2.pipelines.raster_3d.pipeline_worker import Raster3DPipelineWorker
from qp2.xio.redis_manager import RedisManager

# Setup basic logging to stdout
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
logger = logging.getLogger("raster3d_cli")

def main():
    parser = argparse.ArgumentParser(description="Run the 3D Raster Pipeline standalone.")
    parser.add_argument("--run1", required=True, help="Prefix of the first run (e.g., Q3_ras_run6)")
    parser.add_argument("--run2", required=True, help="Prefix of the second orthogonal run (e.g., Q3_ras_run7)")
    parser.add_argument("--data-dir", required=True, help="Directory containing the HDF5 master files")
    parser.add_argument("--proc-dir", default="/tmp/raster3d_cli", help="Output processing directory root or raster_3d workdir (default: /tmp/raster3d_cli)")
    parser.add_argument("--redis-host", default="localhost", help="Redis host to use for dozor results and bluice info")
    parser.add_argument("--standalone", action="store_true", help="Run without real Redis (uses FakeRedis, requires dozor results to be mock-populated separately)")
    parser.add_argument("--beam", type=float, default=10.0, help="Beam size in microns (default: 10.0)")
    parser.add_argument("--energy", type=float, default=12000.0, help="Energy in eV (default: 12000.0)")
    parser.add_argument("--step", type=float, default=10.0, help="Raster step size in microns (default: 10.0)")
    
    args = parser.parse_args()

    # PyQt requires a QCoreApplication to use signals/slots in some environments
    app = QtCore.QCoreApplication.instance()
    if app is None:
        app = QtCore.QCoreApplication(sys.argv)

    if args.standalone:
        import fakeredis
        redis_conn = fakeredis.FakeRedis(decode_responses=True)
        redis_manager = None
        logger.info("Using FakeRedis (Standalone mock mode)")
    else:
        logger.info(f"Connecting to Redis at {args.redis_host}")
        # RedisManager uses internal config for host discovery.
        # For CLI with explicit --redis-host, create a direct connection
        # and optionally try RedisManager for bluice access.
        redis_conn = redis.Redis(
            host=args.redis_host, port=6379, db=0, decode_responses=True
        )
        try:
            redis_conn.ping()
            logger.info(f"Connected to analysis Redis at {args.redis_host}")
        except redis.ConnectionError as e:
            logger.error(f"Cannot connect to Redis at {args.redis_host}: {e}")
            sys.exit(1)

        redis_manager = None
        try:
            redis_manager = RedisManager()
            logger.info("RedisManager initialized (bluice access available)")
        except Exception as e:
            logger.info(f"RedisManager not available (no bluice access): {e}")

    # 1. Discover master files
    run1_files = find_master_files(args.data_dir, args.run1)
    run2_files = find_master_files(args.data_dir, args.run2)

    if not run1_files:
        logger.error(f"No master files found for {args.run1} in {args.data_dir}")
        sys.exit(1)
    if not run2_files:
        logger.error(f"No master files found for {args.run2} in {args.data_dir}")
        sys.exit(1)

    logger.info(f"Found {len(run1_files)} master files for run1 ({args.run1})")
    logger.info(f"Found {len(run2_files)} master files for run2 ({args.run2})")

    # 2. Mock or fetch metadata
    meta = {
        "collect_mode": "RASTER",
        "energy_ev": args.energy,
        "beam_size_x_um": args.beam,
        "beam_size_y_um": args.beam,
        "step_size_um": args.step,
        "flux": 1e12,
        "osc_range": 0.0,
        "exposure_sec": 0.02,
    }
    
    # 3. Detect scan modes
    run1_mode = detect_raster_scan_mode(meta, run1_files, redis_manager, run_prefix=args.run1)
    run2_mode = detect_raster_scan_mode(meta, run2_files, redis_manager, run_prefix=args.run2)

    logger.info(f"Run1 scan mode detected: {run1_mode}")
    logger.info(f"Run2 scan mode detected: {run2_mode}")

    # 4. Get config and set up output dir
    r3d_cfg = DEFAULT_CONFIG.copy()
    r3d_cfg["step_size_um"] = args.step
    r3d_cfg["wait_timeout_s"] = 10    # short timeout for CLI
    r3d_cfg["poll_interval_s"] = 2
    r3d_cfg["max_retries"] = 0

    proc_dir = get_raster_3d_proc_dir(args.proc_dir, args.run1, args.run2)
    proc_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output processing directory: {proc_dir}")

    # 5. Build worker
    worker = Raster3DPipelineWorker(
        run1_prefix=args.run1,
        run2_prefix=args.run2,
        run1_master_files=run1_files,
        run2_master_files=run2_files,
        run1_scan_mode=run1_mode,
        run2_scan_mode=run2_mode,
        data_dir=args.data_dir,
        metadata=meta,
        redis_conn=redis_conn,
        proc_dir=str(proc_dir),
        config=r3d_cfg,
        pipeline_params={"username": os.environ.get("USER", "cli_user"), "command_line": True},
        redis_manager=redis_manager,
    )

    # 6. Listen to signals
    def on_completed(results):
        logger.info("pipeline_completed signal received!")
        recs = results.get("recommendations", [])
        if recs:
            logger.info(f"Generated {len(recs)} recommendation(s). Best peak crystal size: {recs[0].get('crystal_position', {}).get('dimensions_um')}")
        else:
            logger.info("No recommendations generated.")
        
    def on_error(stage, msg):
        logger.error(f"Error emitted at {stage}: {msg}")

    worker.signals.pipeline_completed.connect(on_completed)
    worker.signals.error.connect(on_error)

    # 7. Run synchronously
    logger.info("--------------------------------------------------")
    logger.info("Starting Raster3DPipelineWorker (synchronous run)")
    logger.info("--------------------------------------------------")
    
    worker.run()
    
    logger.info("Worker run() returned. Check output logs and JSON payload for final results.")

if __name__ == "__main__":
    main()
