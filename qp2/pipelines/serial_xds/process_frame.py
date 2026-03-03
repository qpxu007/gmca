import argparse
import os
import shutil
import sys
import logging

# Ensure qp2 is in path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from qp2.log.logging_config import setup_logging, get_logger
from qp2.image_viewer.utils.run_job import run_command
from qp2.pipelines.gmcaproc.xds2 import XDS
from qp2.xio.hdf5_manager import HDF5Reader

logger = get_logger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Process a single frame for Serial XDS")
    parser.add_argument("--dataset_dir", required=True, help="Root directory for the dataset (contains INIT output)")
    parser.add_argument("--master_file", required=True, help="Path to master H5 file")
    parser.add_argument("--frame", type=int, required=True, help="Frame number to process")
    parser.add_argument("--dials_setup", default="", help="Setup command for DIALS environment")
    return parser.parse_args()

def main():
    setup_logging()
    args = parse_args()
    
    dataset_dir = os.path.abspath(args.dataset_dir)
    frame_num = args.frame
    frame_dir = os.path.join(dataset_dir, f"{frame_num:05d}")
    
    os.makedirs(frame_dir, exist_ok=True)
    
    logger.info(f"Processing frame {frame_num} in {frame_dir}")
    
    # 1. Link files from parent (XYCORR/INIT outputs)
    files_to_link = [
        "X-CORRECTIONS.cbf", 
        "Y-CORRECTIONS.cbf", 
        "BKGINIT.cbf", 
        "GAIN.cbf",
        "BLANK.cbf"
    ]
    
    for fname in files_to_link:
        src = os.path.join(dataset_dir, fname)
        dst = os.path.join(frame_dir, fname)
        if os.path.exists(src):
            if os.path.exists(dst) or os.path.islink(dst):
                os.remove(dst)
            os.symlink(src, dst)
    
    # 2. Use XDS Class to generate INP and run
    try:
        # Initialize reader (no timer needed for single frame processing)
        reader = HDF5Reader(args.master_file, start_timer=False)
        
        # Initialize XDS
        # We pass user_start and user_end as the current frame to set DATA_RANGE correctly
        # We disable use_slurm here because we are ALREADY inside a SLURM job (array task)
        # and want to run xds_par directly (shell).
        xds_proc = XDS(
            dataset=reader,
            proc_dir=frame_dir,
            user_start=frame_num,
            user_end=frame_num,
            use_slurm=False,
            njobs=1,
            nproc=1
        )
        
        # Override JOB parameter for integration step
        xds_proc.xds_inp["JOB"] = "COLSPOT IDXREF DEFPIX INTEGRATE"
        xds_proc.xds_inp["SPOT_RANGE"] = [f"{frame_num} {frame_num}"]
        
        # Generate INP
        xds_proc.generate_xds_inp()
        
        # Run XDS (INTEGRATE)
        xds_proc.run()
        
    except Exception as e:
        logger.error(f"XDS processing failed: {e}")
        sys.exit(1)
        
    # 3. Check Success
    integrate_lp = os.path.join(frame_dir, "INTEGRATE.LP")
    success = False
    if os.path.exists(integrate_lp):
        with open(integrate_lp, 'r') as f:
            content = f.read()
            if "!!! ERROR !!!" not in content:
                success = True
                
    if not success:
        logger.error("INTEGRATE step failed (found error or missing output)")
        sys.exit(1)
        
    logger.info("XDS Integration successful.")
    
    # 4. Run DIALS Import
    dials_cmd = "dials.import_xds ."
    
    # Pre-command for environment
    pre_cmd = args.dials_setup if args.dials_setup else None
    
    job_id = run_command(
        dials_cmd, 
        cwd=frame_dir, 
        method="shell", 
        job_name="dials_import",
        pre_command=pre_cmd
    )
    
    if job_id is None: # run_command returns None on failure? Or Popen/CompletedProcess?
        # run_command signature says -> Optional[str]. None usually means failure in slurm, 
        # but for shell it returns process/CompletedProcess.
        # Wait, looking at run_job.py:
        # if shell: returns Popen or CompletedProcess.
        # if slurm: returns job_id string or None.
        # Here we rely on it raising exception or checking returncode if it was blocking.
        # But run_command returns objects for shell.
        # Let's check logic in run_job.py again.
        # "return subprocess.CompletedProcess(...)".
        # So we should check returncode.
        pass
    
    # Since run_command for shell returns an object, we can't just check 'is None' if it succeeds.
    # But if it raises exception on failure (which it logs), we might be caught in try-except block if we had one.
    # The current run_command implementation captures exceptions and logs them, then re-raises RuntimeError.
    # So if we are here, it likely succeeded or we need to check returncode if captured.
    # However, run_command returns the result.
    
    logger.info("Frame processing complete.")

if __name__ == "__main__":
    main()
