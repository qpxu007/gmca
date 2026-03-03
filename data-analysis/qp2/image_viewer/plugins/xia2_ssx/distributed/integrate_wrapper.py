#!/usr/bin/env python3
import sys
import json
import subprocess
import os
import time

def main():
    dataset_path = sys.argv[1]
    status_key = sys.argv[2]
    config_path = sys.argv[3]
    
    with open(config_path, 'r') as f:
        config = json.load(f)
        
    # Build xia2 command
    cmd = ["xia2.ssx"]
    cmd.append(f"image={dataset_path}")
    
    # Check for existing results to skip processing
    force_reprocessing = config.get('force_reprocessing', False)
    if not force_reprocessing:
        # Check for integrated files in current directory
        # The job runs in a specific subdirectory, so we check local DataFiles
        import glob
        expts = glob.glob(os.path.join("DataFiles", "integrated*.expt"))
        refls = glob.glob(os.path.join("DataFiles", "integrated*.refl"))
        
        if expts and refls:
            print(f"Found existing integration results in {os.getcwd()}")
            print("Skipping processing as force_reprocessing is False.")
            check_and_trigger_reduction(config)
            sys.exit(0)
    
    if config.get('steps'):
        cmd.append(f"steps={config['steps']}")
    if config.get('d_min'):
        cmd.append(f"d_min={config['d_min']}")
    if config.get('unit_cell'):
        cmd.append(f"unit_cell={config['unit_cell']}")
    if config.get('space_group'):
        cmd.append(f"space_group={config['space_group']}")
    if config.get('reference_hkl'):
        cmd.append(f"reference={config['reference_hkl']}")
    if config.get('max_lattices'):
        cmd.append(f"indexing.max_lattices={config['max_lattices']}")
    if config.get('min_spots'):
        cmd.append(f"indexing.min_spots={config['min_spots']}")
        
    print(f"Running: {' '.join(cmd)}")
    subprocess.check_call(cmd)
    
    # Trigger reduction if threshold reached
    check_and_trigger_reduction(config)

def check_and_trigger_reduction(config):
    """Increments counter and triggers reduction if threshold reached."""
    if not config.get('incremental_merging'):
        return

    group_id = config.get('redis_group_id')
    r_host = config.get('redis_host')
    r_port = config.get('redis_port')
    
    if not group_id or not r_host:
        return
        
    try:
        import redis
        import math
        r = redis.Redis(host=r_host, port=r_port)
        key = f"xia2_ssx:{group_id}:finished_count"
        
        # Atomic increment
        new_count = r.incr(key)
        
        total_datasets = len(config.get('datasets', []))
        if total_datasets == 0: return

        # Thresholds: 25%, 50%, 95%
        # Use a map to handle small datasets where multiple % map to same integer (e.g. 1)
        # We keep the highest percentage for a given count
        threshold_map = {} # count -> max_pct
        
        for pct in [25, 50, 95]:
            t = math.ceil(total_datasets * pct / 100.0)
            if 1 <= t < total_datasets:
                if t not in threshold_map or pct > threshold_map[t]:
                    threshold_map[t] = pct
        
        if new_count in threshold_map:
            pct = threshold_map[new_count]
            print(f"Triggering {pct}% reduction (count={new_count}/{total_datasets})")
            submit_partial_reduce(config, pct, new_count)
                
    except Exception as e:
        print(f"Failed to trigger reduction: {e}", file=sys.stderr)

def submit_partial_reduce(config, pct, limit):
    """Submits a partial reduction job."""
    # We need to construct the sbatch command similar to orchestrator
    # We are in job_N directory, need to go up to root
    work_root = os.path.dirname(os.getcwd())
    reduce_wrapper = os.path.join(work_root, "reduce_wrapper.sh")
    
    if not os.path.exists(reduce_wrapper):
        print(f"Error: {reduce_wrapper} not found", file=sys.stderr)
        return

    job_name = f"xia2_reduce_{pct}pct"
    walltime = config.get('walltime', '04:00:00')
    nproc = config.get('nproc', 8)
    
    # Create subdir for reduction
    sub_dir = os.path.join(work_root, f"reduce_{pct}pct")
    os.makedirs(sub_dir, exist_ok=True)
    
    cmd = [
        "sbatch",
        f"--job-name={job_name}",
        f"--output=slurm-%j.out",
        f"--time={walltime}",
        "--nodes=1",
        "--ntasks=1",
        f"--chdir={sub_dir}",
        f"--cpus-per-task={nproc}",
        reduce_wrapper,
        "--limit", str(limit)
    ]
    
    print(f"Submitting partial reduction: {' '.join(cmd)}")
    subprocess.run(cmd)

if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
