import argparse
import os
import sys
import glob
import logging
import shutil
import subprocess
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# Adjust path to find qp2 modules
sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

from qp2.pipelines.gmcaproc.xds2 import XDS
from qp2.pipelines.serial_xds.dozor_runner import run_dozor
from qp2.xio.hdf5_manager import HDF5Reader
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.image_viewer.utils.run_job import run_command
from qp2.config.programs import ProgramConfig

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def is_slurm_available():
    return shutil.which("sbatch") is not None


def run_process_frame_locally(dataset_dir, master_file, frame_num, dials_setup, qp2_root):
    """
    Run the process_frame.py script locally for a single frame.
    """
    worker_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "process_frame.py")
    cmd = [
        sys.executable,
        worker_script,
        "--dataset_dir", dataset_dir,
        "--master_file", master_file,
        "--frame", str(frame_num),
        "--dials_setup", dials_setup
    ]
    
    try:
        # Run process_frame.py
        # We suppress output to avoid spamming the console, but capture errors
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            logger.error(f"Frame {frame_num} failed: {res.stderr}")
            return False
        return True
    except Exception as e:
        logger.error(f"Error running frame {frame_num}: {e}")
        return False


def submit_array_job(dataset_dir, master_file, num_tasks, qp2_root, dials_setup):
    """
    Submits a SLURM array job.
    Expects 'frames.txt' to exist in dataset_dir, mapping task ID (1-based line number) to frame number.
    """
    script_path = os.path.join(dataset_dir, "submit_frames.sh")
    frames_list_path = os.path.join(dataset_dir, "frames.txt")
    log_dir = os.path.join(dataset_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    worker_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "process_frame.py")

    script_content = f"""#!/bin/bash
#SBATCH --job-name=serial_xds_{os.path.basename(dataset_dir)}
#SBATCH --output={log_dir}/task_%a.log
#SBATCH --array=1-{num_tasks}
#SBATCH --time=00:30:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1

export PYTHONPATH={qp2_root}:$PYTHONPATH

# Get the actual frame number from the frames.txt mapping file
# sed -n 'Kp' prints the K-th line
FRAME=$(sed -n "${{SLURM_ARRAY_TASK_ID}}p" {frames_list_path})

if [ -z "$FRAME" ]; then
    echo "Error: Could not retrieve frame number for task $SLURM_ARRAY_TASK_ID"
    exit 1
fi

echo "Processing frame $FRAME (Task $SLURM_ARRAY_TASK_ID)"

python {worker_script} \\
    --dataset_dir {dataset_dir} \\
    --master_file {master_file} \\
    --frame $FRAME \\
    --dials_setup "{dials_setup}"
"""

    with open(script_path, "w") as f:
        f.write(script_content)

    try:
        res = subprocess.run(
            ["sbatch", script_path], capture_output=True, text=True, cwd=dataset_dir
        )
        if res.returncode == 0:
            job_id = res.stdout.strip().split()[-1]
            logger.info(f"Submitted array job {job_id} for {dataset_dir} ({num_tasks} tasks)")
            return job_id
        else:
            logger.error(f"Failed to submit array job: {res.stderr}")
            return None
    except Exception as e:
        logger.error(f"Submission failed: {e}")
        return None


def run_scaling_locally(output_dir, dials_setup):
    """Run xia2.multiplex locally."""
    logger.info("Running scaling locally...")
    
    # Find directories
    dirs = []
    for root, dirs_in_root, files in os.walk(output_dir):
        if "imported.expt" in files:
            dirs.append(root)
    
    if not dirs:
        logger.error("No successful frames found for scaling.")
        return

    # Construct command
    # dials_setup might be 'module load dials' which doesn't work in subprocess unless shell=True
    # We assume environment is set or user passed setup string
    
    cmd_str = f"{dials_setup}; xia2.multiplex {' '.join(dirs)}" if dials_setup else f"xia2.multiplex {' '.join(dirs)}"
    
    try:
        # Using shell=True to handle the setup command/env vars
        subprocess.check_call(cmd_str, shell=True, cwd=output_dir)
        logger.info("Scaling finished successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Scaling failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Serial XDS Processing Pipeline")
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="Input datasets (glob patterns allowed)",
    )
    parser.add_argument("--output", default="serial_proc", help="Output directory")
    parser.add_argument("--spacegroup", help="Space group (symbol or number)")
    parser.add_argument("--unitcell", help="Unit cell constants 'a b c al be ga'")
    parser.add_argument("--reference", help="Reference dataset path")
    parser.add_argument("--highres", type=float, help="High resolution cutoff")
    parser.add_argument(
        "--dials_setup",
        default="module load dials",
        help="Command to setup DIALS environment",
    )
    parser.add_argument(
        "--jobs", type=int, default=8, help="Number of local parallel jobs (if SLURM unavailable)"
    )
    
    # Dozor Arguments
    parser.add_argument("--dozor", action="store_true", help="Run Dozor to filter frames")
    parser.add_argument("--dozor_spot_level", type=float, default=6.0, help="Dozor spot level")
    parser.add_argument("--dozor_min_spots", type=int, default=10, help="Min spots to process frame")
    parser.add_argument("--dozor_min_score", type=float, default=0, help="Min score to process frame")
    parser.add_argument("--dozor_spot_size", type=int, default=3, help="Dozor spot size")

    args = parser.parse_args()

    # Determine Project Root
    qp2_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

    os.makedirs(args.output, exist_ok=True)

    # Expand inputs
    datasets = []
    for pattern in args.inputs:
        datasets.extend(glob.glob(pattern))

    logger.info(f"Found {len(datasets)} datasets.")

    use_slurm = is_slurm_available()
    if use_slurm:
        logger.info("SLURM detected. Will submit array jobs.")
    else:
        logger.info(f"SLURM not detected. Will run locally with {args.jobs} workers.")

    slurm_job_ids = []
    
    # Process Datasets
    for ds in datasets:
        ds_name = os.path.basename(ds).split(".")[0]
        ds_work_dir = os.path.join(args.output, ds_name)

        logger.info(f"Preparing {ds_name}...")
        
        valid_frames = []

        # 1. Read Metadata and Run Initial Step
        try:
            reader = HDF5Reader(ds, start_timer=False)

            pipeline_params = {
                "sampleName": ds_name,
                "imagedir": os.path.dirname(os.path.abspath(ds)),
                "workdir": os.path.abspath(ds_work_dir),
                "command": " ".join(sys.argv),
                "beamline": get_beamline_from_hostname(),
            }
            
            xds_proc = XDS(
                dataset=reader,
                proc_dir=ds_work_dir,
                user_space_group=args.spacegroup,
                user_unit_cell=args.unitcell,
                user_resolution_cutoff=args.highres,
                reference_hkl=args.reference,
                use_slurm=False, # Init run locally
                njobs=1,
                nproc=4,
                pipeline_params=pipeline_params
            )

            logger.info(f"Running XYCORR/INIT for {ds_name}...")
            if xds_proc.xds_init():
                logger.error(f"Initial run failed for {ds_name}")
                continue

            total_frames = reader.total_frames
            metadata = reader.get_parameters()
            reader.close()
            
            # --- Dozor Filtering ---
            if args.dozor:
                logger.info(f"Running Dozor filtering for {ds_name}...")
                valid_frames = run_dozor(
                    metadata=metadata,
                    work_dir=ds_work_dir,
                    start_frame=1,
                    end_frame=total_frames,
                    spot_level=args.dozor_spot_level,
                    spot_size=args.dozor_spot_size,
                    min_spots=args.dozor_min_spots,
                    min_score=args.dozor_min_score
                )
                logger.info(f"Dozor selected {len(valid_frames)} / {total_frames} frames.")
            else:
                valid_frames = list(range(1, total_frames + 1))

        except Exception as e:
            logger.error(f"Failed to prepare {ds}: {e}")
            continue

        if not valid_frames:
            logger.warning(f"No valid frames to process for {ds_name}")
            continue
            
        # Write frames.txt for mapping
        frames_list_path = os.path.join(ds_work_dir, "frames.txt")
        with open(frames_list_path, "w") as f:
            for frame in valid_frames:
                f.write(f"{frame}\n")

        # 3. Process Frames (SLURM vs Local)
        if use_slurm:
            jid = submit_array_job(
                ds_work_dir, ds, len(valid_frames), qp2_root, args.dials_setup
            )
            if jid:
                slurm_job_ids.append(jid)
        else:
            logger.info(f"Processing {len(valid_frames)} frames locally for {ds_name}...")
            with ProcessPoolExecutor(max_workers=args.jobs) as executor:
                futures = [
                    executor.submit(
                        run_process_frame_locally, 
                        ds_work_dir, ds, f, args.dials_setup, qp2_root
                    ) for f in valid_frames
                ]
                
                # Monitor progress
                completed_count = 0
                for future in as_completed(futures):
                    completed_count += 1
                    if completed_count % 10 == 0:
                        logger.info(f"Progress: {completed_count}/{len(valid_frames)}")
            logger.info(f"Finished frames for {ds_name}")

    # 4. Scaling
    if use_slurm:
        if not slurm_job_ids:
            logger.info("No jobs submitted.")
            return

        logger.info(f"Submitted {len(slurm_job_ids)} array jobs. Submitting scaling job...")
        
        scaling_script_path = os.path.join(args.output, "run_scaling.sh")
        dependency_str = ":".join(slurm_job_ids)

        scaling_content = f"""#!/bin/bash
#SBATCH --job-name=xia2_scaling
#SBATCH --output={args.output}/scaling.log
#SBATCH --dependency=afterany:{dependency_str}
#SBATCH --time=04:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8

export PYTHONPATH={qp2_root}:$PYTHONPATH
{args.dials_setup}

cd {os.path.abspath(args.output)}

echo "Collecting successful imports..."
DIRS=$(find . -maxdepth 2 -name "imported.expt" -exec dirname {{}} \\;)

if [ -z "$DIRS" ]; then
    echo "No successful frames found."
    exit 1
fi

echo "Running xia2.multiplex..."
xia2.multiplex $DIRS
"""

        with open(scaling_script_path, "w") as f:
            f.write(scaling_content)

        try:
            res = subprocess.run(
                ["sbatch", scaling_script_path],
                capture_output=True,
                text=True,
                cwd=args.output,
            )
            if res.returncode == 0:
                logger.info(f"Submitted scaling job: {res.stdout.strip()}")
            else:
                logger.error(f"Failed to submit scaling job: {res.stderr}")
        except Exception as e:
            logger.error(f"Scaling submission failed: {e}")
            
    else:
        # Local Scaling
        run_scaling_locally(args.output, args.dials_setup)


if __name__ == "__main__":
    main()
