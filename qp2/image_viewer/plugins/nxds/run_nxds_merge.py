#!/usr/bin/env python3
import os
import sys
import argparse
import time
import logging
import json
import redis
from pathlib import Path
# Try to find qp2 package and add to path
def find_project_root(file_path):
    path = Path(file_path).resolve()
    for parent in path.parents:
        if (parent / "qp2").is_dir():
            return str(parent)
    return None

project_root = find_project_root(__file__)
if project_root and project_root not in sys.path:
    sys.path.insert(0, project_root)

import shutil
import subprocess
from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.config.programs import ProgramConfig
from qp2.pipelines.gmcaproc.symmetry import Symmetry

F2MTZ_SCRIPT_TEMPLATE = """#!/bin/bash
set -e
echo "Running f2mtz..."
f2mtz HKLOUT temp.mtz < F2MTZ.INP
echo "Running cad..."
cad HKLIN1 temp.mtz HKLOUT XDS_ASCII.mtz <<EOF
LABIN FILE 1 ALL
END
echo "Script finished."
"""

from qp2.log.logging_config import setup_logging, get_logger

# Try importing h5py
try:
    import h5py
except ImportError:
    h5py = None

# Setup logging
setup_logging()
logger = get_logger("run_nxds_merge")

def get_redis_connection(host="localhost"):
    """Establishes connection to Redis."""
    try:
        # Prioritize argument, fall back to env var, then localhost
        redis_host = host if host else os.getenv("REDIS_HOST", "localhost")
        return redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return None

def read_hdf5_metadata(master_file):
    """
    Reads OVERLOAD (saturation_value) and SENSOR_THICKNESS from the HDF5 master file.
    Returns a dict with values or defaults if reading fails.
    """
    defaults = {
        "overload": 65000,
        "sensor_thickness": 0  # 0 usually means XDS default or ignored if not set? 
                               # Actually XDS default is often detector dependent.
                               # But user requested it, so we should try to get it.
    }
    
    if not h5py:
        logger.warning("h5py not installed. Using defaults.")
        return defaults
        
    if not master_file or not os.path.exists(master_file):
        logger.warning(f"Master file {master_file} not found. Using defaults.")
        return defaults
        
    try:
        with h5py.File(master_file, 'r') as f:
            # 1. OVERLOAD / Saturation Value
            # Path: /entry/instrument/detector/detectorSpecific/countrate_correction_count_cutoff
            try:
                dset = f["/entry/instrument/detector/detectorSpecific/countrate_correction_count_cutoff"]
                val = dset[()]
                defaults["overload"] = int(val) if hasattr(val, "item") else int(val)
                logger.info(f"Read OVERLOAD from HDF5: {defaults['overload']}")
            except KeyError:
                 logger.warning("Could not read countrate_correction_count_cutoff. Using default.")

            # 2. SENSOR_THICKNESS
            # Path: /entry/instrument/detector/sensor_thickness
            try:
                dset = f["/entry/instrument/detector/sensor_thickness"]
                val = dset[()] # usually in meters
                thickness_m = float(val) if hasattr(val, "item") else float(val)
                # XDS expects mm usually. 
                # Converting meters to mm:
                defaults["sensor_thickness"] = thickness_m * 1000.0
                logger.info(f"Read SENSOR_THICKNESS from HDF5: {defaults['sensor_thickness']} mm")
            except KeyError:
                 logger.warning("Could not read sensor_thickness. Skipping.")
                 
    except Exception as e:
        logger.error(f"Error reading HDF5 metadata: {e}")
        
    return defaults

def wait_for_datasets(redis_conn, master_files, poll_interval=10, timeout=36000):
    """
    Polls Redis for the completion status of given master files.
    Returns True if all completed successfully, False otherwise.
    """
    # Log connection details for debugging
    connection_kwargs = redis_conn.connection_pool.connection_kwargs
    host = connection_kwargs.get("host", "unknown")
    port = connection_kwargs.get("port", "unknown")
    logger.info(f"Polling Redis at {host}:{port} for {len(master_files)} datasets...")

    pending_files = set(master_files)
    start_time = time.time()
    last_log_time = 0
    log_interval = 60  # Log summary every 60 seconds

    # Initial log of keys/paths to ensure we are looking for the right things
    if pending_files:
        sample_mf = next(iter(pending_files))
        sample_key = f"analysis:out:nxds:{sample_mf}:status"
        logger.info(f"Sample polling key: {sample_key}")

    while pending_files:
        if time.time() - start_time > timeout:
            logger.error("Timeout waiting for jobs to complete.")
            return False
            
        completed = set()
        status_counts = {"unknown": 0, "pending": 0, "failed": 0, "finished": 0}
        
        for mf in pending_files:
            # Check status key which is a JSON string
            status_key = f"analysis:out:nxds:{mf}:status"
            status_json_str = redis_conn.get(status_key)
            
            status = "unknown"
            if status_json_str:
                try:
                    status_data = json.loads(status_json_str)
                    status = status_data.get("status", "unknown").lower() # Normalize to lowercase
                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode JSON status for {mf}")
            
            # Map various status strings to categories
            if status in ["finished", "completed", "success"]:
                logger.info(f"Dataset {os.path.basename(mf)} finished.")
                completed.add(mf)
                status_counts["finished"] += 1
            elif status in ["failed", "error"]:
                 logger.warning(f"Dataset {os.path.basename(mf)} FAILED. Proceeding without it.")
                 completed.add(mf) # Treat as done but maybe don't include in merge?
                 status_counts["failed"] += 1
            elif status in ["running", "submitted", "processing"]:
                status_counts["pending"] += 1
            else:
                status_counts["unknown"] += 1

        pending_files -= completed
        
        # Periodic logging
        now = time.time()
        if now - last_log_time > log_interval:
            remaining = len(pending_files)
            if remaining > 0:
                logger.info(
                    f"Waiting for {remaining} datasets. "
                    f"Current states in this cycle [Pending: {status_counts['pending']}, "
                    f"Finished: {status_counts['finished']}, Failed: {status_counts['failed']}, "
                    f"Unknown: {status_counts['unknown']}]"
                )
            last_log_time = now

        if pending_files:
            time.sleep(poll_interval)
            
    return True

def _run_dimple(merge_dir, args):
    logger.info("Step 4: Running Dimple...")
    if not args.pdb_file:
         return "No PDB file provided. Skipping Dimple."
         
    pdb_file = Path(args.pdb_file)
    if not pdb_file.exists():
        logger.warning(f"PDB file for Dimple not found: {pdb_file}")
        return "PDB file not found."

    # Copy PDB file to merge dir to keep run self-contained
    shutil.copy(pdb_file, merge_dir / pdb_file.name)

    run_command(
        cmd=["dimple", "XDS_ASCII.mtz", pdb_file.name, "dimple"],
        pre_command=ProgramConfig.get_setup_command('ccp4'),
        cwd=str(merge_dir),
        method="slurm" if is_sbatch_available() else "shell",
        job_name="dimple_solve",
        background=False,
        processors=1,
        walltime="01:00:00",
        memory="2gb",
    )

    # Check for a successful dimple run indicator
    if not (merge_dir / "dimple" / "final.pdb").exists():
        logger.warning("Dimple finished, but final.pdb was not found.")
        return f"Dimple run completed with warnings. Check results in:\n{merge_dir}"

    return f"Dimple structure solution successful!\nResults are in:\n{merge_dir / 'dimple'}"

def _launch_coot(merge_dir):
    logger.info("Step 5: Checking for files and launching Coot...")
    dimple_dir = merge_dir / "dimple"
    final_pdb = dimple_dir / "final.pdb"
    final_mtz = dimple_dir / "final.mtz"

    if final_pdb.exists() and final_mtz.exists():
        if os.environ.get("DISPLAY"):
            logger.info(f"Found files, launching Coot for {final_pdb} and {final_mtz}")
            run_command(
                cmd=["coot", "--pdb", str(final_pdb), "--auto", str(final_mtz)],
                pre_command=ProgramConfig.get_setup_command('ccp4'),
                cwd=str(dimple_dir),
                method="shell",
                job_name="coot_launcher",
                background=True,  # Launch and detach
            )
        else:
            logger.info("DISPLAY not set. Skipping Coot launch.")
            logger.info(f"To view results, run: coot --pdb {final_pdb} --auto {final_mtz}")
    else:
        logger.warning(
            "Could not find final.pdb and/or final.mtz. Skipping Coot launch."
        )

def run_merge_pipeline(hkl_list_file: Path, args, metadata_defaults=None):
    """
    Executes the nXDS merge steps: nXSCALE -> XDSCONV -> F2MTZ -> Dimple.
    """
    merge_dir = Path(args.output_dir)
    nproc = args.nproc
    
    # Check inputs
    if not hkl_list_file.exists():
        logger.error(f"HKL list file not found: {hkl_list_file}")
        return 1
        
    # Read HKL paths
    with open(hkl_list_file, 'r') as f:
        hkl_paths = [line.strip() for line in f if line.strip()]
        
    if not hkl_paths:
        logger.error("No HKL files found in list.")
        return 1

    logger.info(f"Starting merge pipeline with {len(hkl_paths)} datasets in {merge_dir}")
    
    # 1. nXSCALE (Scaling)
    # ----------------------------------------------------------------
    xscale_inp = merge_dir / "nXSCALE.INP"
    
    # Defaults not exposed in CLI yet
    OVERLOAD = 65000
    SENSOR_THICKNESS = None
    
    if metadata_defaults:
        OVERLOAD = metadata_defaults.get("overload", OVERLOAD)
        SENSOR_THICKNESS = metadata_defaults.get("sensor_thickness", None)
        
    INCLUDE_RESOLUTION_RANGE = "50 1.5"
    
    # Resolve space group number
    space_group_num = args.space_group
    try:
        converted = Symmetry.symbol_to_number(space_group_num)
        if converted:
            space_group_num = str(converted)
    except:
        pass

    with open(xscale_inp, 'w') as f:
        f.write("OUTPUT_FILE=MERGED.HKL\n")
        f.write(f"SPACE_GROUP_NUMBER={space_group_num}\n")
        f.write(f"UNIT_CELL_CONSTANTS={args.unit_cell}\n")
        f.write(f"OVERLOAD={OVERLOAD}\n")
        f.write(f"MAXIMUM_NUMBER_OF_PROCESSORS={args.nproc}\n")
        f.write(f"INCLUDE_RESOLUTION_RANGE= {INCLUDE_RESOLUTION_RANGE}\n")
        f.write("POSTREFINE= SKALA B-FACTOR MOSAICITY CELL POSITION\n")
        
        if SENSOR_THICKNESS is not None and SENSOR_THICKNESS > 0:
             f.write(f"!SENSOR_THICKNESS={SENSOR_THICKNESS:.4f}\n")
        
        if args.reference_hkl and os.path.exists(args.reference_hkl):
             f.write(f"REFERENCE_DATA_SET={args.reference_hkl}\n")
        
        for p in hkl_paths:
             f.write(f"INPUT_FILE={p}\n")
    


    logger.info("Running XSCALE...")
    # Using run_command which handles environment setup via ProgramConfig
    try:
        run_command(
            cmd=["nxscale_par"],
            cwd=str(merge_dir),
            pre_command=ProgramConfig.get_setup_command('nxds'),
            job_name="nxscale",
            background=False
        )
    except Exception as e:
        logger.error(f"XSCALE (parallel) failed: {e}")
        # Try serial xscale
        try:
             run_command(
                cmd=["nxscale"],
                cwd=str(merge_dir),
                pre_command=ProgramConfig.get_setup_command('nxds'),
                job_name="nxscale_serial",
                background=False
            )
        except:
             return 1

    if not (merge_dir / "MERGED.HKL").exists():
        logger.error("MERGED.HKL not created.")
        return 1

    # 2. XDSCONV (Conversion)
    # ----------------------------------------------------------------
    xdsconv_inp = merge_dir / "XDSCONV.INP"
    with open(xdsconv_inp, 'w') as f:
        f.write("INPUT_FILE=MERGED.HKL\n")
        f.write("OUTPUT_FILE=temp.hkl CCP4\n") 
        f.write(f"INCLUDE_RESOLUTION_RANGE= {INCLUDE_RESOLUTION_RANGE}\n")
        f.write("FRIEDEL'S_LAW=TRUE\n") 
    
    logger.info("Running XDSCONV...")
    try:
        run_command(
            cmd=["xdsconv"],
            cwd=str(merge_dir),
            pre_command=ProgramConfig.get_setup_command('xds'),
            job_name="xdsconv",
            background=False
        )
    except Exception as e:
        logger.error(f"XDSCONV failed: {e}")
        return 1
        
    # 3. F2MTZ / CAD (Final MTZ creation)
    # ----------------------------------------------------------------
    logger.info("Step 3: Running F2MTZ and CAD...")
    script_path = merge_dir / "f2mtz.sh"
    with open(script_path, "w") as f:
        f.write(F2MTZ_SCRIPT_TEMPLATE)
    script_path.chmod(0o755)

    try:
        run_command(
            cmd=[str(script_path)],
            pre_command=ProgramConfig.get_setup_command('nxds'),
            cwd=str(merge_dir),
            method="shell",
            job_name="f2mtz_cad_run",
            background=False,
        )
    except Exception as e:
        logger.error(f"MTZ conversion failed: {e}")
        pass # Try to continue? Or return 1? Worker continues sort of.

    final_mtz = merge_dir / "XDS_ASCII.mtz"
    if not final_mtz.exists():
         logger.error("MTZ conversion script did not produce XDS_ASCII.mtz.")
         # But maybe we still want to renaming if temp.mtz exists from old logic? 
         # Sticking to worker logic: if it fails, it fails.
    
    # 4. Dimple
    if args.pdb_file:
         result_msg = _run_dimple(merge_dir, args)
         logger.info(result_msg)
         
         # 5. Coot
         _launch_coot(merge_dir)

    logger.info(f"Merge pipeline completed. Output directory: {merge_dir}")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Run nXDS Merge Pipeline with optional Redis polling.")
    
    # Original Merge Args
    parser.add_argument("--hkl_list", required=True, help="File containing list of INTEGRATE.HKL files (or master files if polling)")
    parser.add_argument("--output_dir", required=True, help="Directory for merge output")
    parser.add_argument("--space_group", default="0", help="Space group number or name")
    parser.add_argument("--unit_cell", default="", help="Unit cell constants 'a b c al be ga'")
    parser.add_argument("--reference_hkl", default="", help="Reference HKL file")
    parser.add_argument("--pdb_file", default="", help="Reference PDB file")
    parser.add_argument("--nproc", type=int, default=32, help="Number of processors")
    parser.add_argument("--master_file", default="", help="Path to a master file for reading metadata (optional)")
    
    # New Polling Args
    parser.add_argument("--wait_for_keys", action="store_true", help="If set, hkl_list is treated as master files list, and script polls Redis for them.")
    parser.add_argument("--redis_host", default="localhost", help="Redis host")
    
    args = parser.parse_args()
    
    master_file_for_metadata = args.master_file
    
    if args.wait_for_keys:
        conn = get_redis_connection(args.redis_host)
        if not conn:
            logger.error("Wait mode requested but Redis connection failed.")
            sys.exit(1)
            
        # Read master files from the provided list
        with open(args.hkl_list, 'r') as f:
             master_files = [l.strip() for l in f if l.strip()]
             
        # Pick the first one for metadata if not provided explicitly
        if not master_file_for_metadata and master_files:
            master_file_for_metadata = master_files[0]
             
        if not wait_for_datasets(conn, master_files):
             logger.error("Polling failed or timed out.")
             sys.exit(1)
             
        # Resolve HKL paths after waiting
        # We need to rewrite hkl_list with actual INTEGRATE.HKL paths
        # because the original inputs were master files
        real_hkl_paths = []
        for mf in master_files:
             key = f"analysis:out:nxds:{mf}"
             proc_dir = conn.hget(key, "_proc_dir")
             if proc_dir:
                 hkl_path = os.path.join(proc_dir, "INTEGRATE.HKL")
                 if os.path.exists(hkl_path):
                     real_hkl_paths.append(hkl_path)
                 else:
                     logger.warning(f"INTEGRATE.HKL missing for {mf} despite success status.")
        
        # Overwrite list file with actual paths
        with open(args.hkl_list, 'w') as f:
             for p in real_hkl_paths:
                 f.write(p + "\n")
                 
    # Read metadata
    metadata = {}
    if master_file_for_metadata:
        metadata = read_hdf5_metadata(master_file_for_metadata)
                 
    # Proceed with merge
    sys.exit(run_merge_pipeline(Path(args.hkl_list), args, metadata_defaults=metadata))

if __name__ == "__main__":
    main()
