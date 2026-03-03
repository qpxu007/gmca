#!/usr/bin/env python3
import json
import os
import sys
import subprocess
import time
import math

def update_status(redis_conn, key, status, message=None):
    if not redis_conn: return
    try:
        data = {'status': status, 'timestamp': time.time()}
        if message: data['message'] = message
        redis_conn.set(key, json.dumps(data), ex=604800)
    except Exception as e:
        print(f"Redis update failed: {e}", file=sys.stderr)

def main():
    # 1. Load Configuration
    config_path = "job_config.json"
    if not os.path.exists(config_path):
        print("Error: job_config.json not found", file=sys.stderr)
        sys.exit(1)
        
    with open(config_path, 'r') as f:
        config = json.load(f)

    # 2. Setup Redis
    import redis
    r_host = config.get('redis_host')
    r_port = config.get('redis_port')
    r_conn = None
    if r_host and r_port:
        try:
            r_conn = redis.Redis(host=r_host, port=r_port)
        except Exception:
            pass

    # 3. Setup Environment
    work_root = os.getcwd()
    
    # Initialize counter if group_id present
    group_id = config.get('redis_group_id')
    if group_id and r_conn:
        try:
            r_conn.set(f"xia2_ssx:{group_id}:finished_count", 0)
        except Exception:
            pass

    setup_script = os.path.join(work_root, "setup_env.sh")
    if not os.path.exists(setup_script):
        print("Warning: setup_env.sh not found", file=sys.stderr)
        
    # Ensure reduce_wrapper.sh exists or create it
    reduce_wrapper = os.path.join(work_root, "reduce_wrapper.sh")
    if not os.path.exists(reduce_wrapper):
        with open(reduce_wrapper, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write(f"source {setup_script}\n")
            f.write(f"python3 {os.path.join(work_root, 'reduce.py')} --config {os.path.join(work_root, 'job_config.json')} $@\n")
        os.chmod(reduce_wrapper, 0o755)

    datasets = config['datasets']
    status_keys = config['status_keys']
    nproc = config.get('nproc', 8)
    walltime = config.get('walltime', "04:00:00")
    
    print(f"Submitting {len(datasets)} integration jobs...")
    
    job_ids = []
    
    # 4. Submit Integration Jobs
    for i, (dataset_path, status_key) in enumerate(zip(datasets, status_keys)):
        # Create subdir
        basename = os.path.basename(dataset_path)
        name_part = os.path.splitext(basename)[0]
        subdir = f"job_{i}_{name_part}"
        os.makedirs(subdir, exist_ok=True)
        
        # Link/Copy integrate script wrapper? 
        # Easier to just reference it from root
        script_path = os.path.join(work_root, "integrate.sh")
        
        # Sbatch command
        # Passing arguments to integrate.sh: <dataset_path> <status_key> <config_path>
        cmd = [
            "sbatch",
            "--parsable",
            f"--job-name=xia2_int_{i}",
            f"--output={subdir}/slurm-%j.out",
            f"--time={walltime}",
            "--nodes=1",
            "--ntasks=1",
            f"--chdir={os.path.abspath(subdir)}",
            f"--cpus-per-task={nproc}",
            script_path,
            dataset_path,
            status_key,
            os.path.abspath(config_path)
        ]
        
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode == 0:
            jid = res.stdout.strip()
            print(f"Submitted job {jid} for {basename}")
            job_ids.append(jid)
        else:
            print(f"Failed to submit job for {basename}: {res.stderr}")
            update_status(r_conn, status_key, "FAILED", f"Submission failed: {res.stderr}")

    # 5. Handle Dependencies & Reduction
    if not job_ids:
        print("No jobs submitted successfully.")
        sys.exit(1)

    dependency_list = ":".join(job_ids)
    print(f"All integration jobs submitted. Dependencies: {dependency_list}")

    # Helper for reduce submission
    def submit_reduce(name, dep_list, limit=None):
        sub_dir = name
        os.makedirs(sub_dir, exist_ok=True)
        
        # Determine time limit for reduce (maybe longer?)
        # For now use same walltime or separate if needed. 
        # User requested support for long waits -> run times.
        # Let's default reduce time to same walltime.
        
        cmd = [
            "sbatch",
            "--parsable",
            f"--dependency=afterany:{dep_list}",
            f"--job-name=xia2_{name}",
            f"--output=slurm-%j.out",
            f"--time={walltime}",
            "--nodes=1",
            "--ntasks=1",
            f"--chdir={os.path.abspath(sub_dir)}",
            f"--cpus-per-task={nproc}",
            os.path.join(work_root, "reduce_wrapper.sh")
        ]
        
        if limit:
            cmd.extend(["--limit", str(limit)])
        
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode == 0:
            print(f"Submitted Reduce Job {name} with ID {res.stdout.strip()}")
        else:
            print(f"Failed to submit Reduce Job {name}: {res.stderr}")

    # 7. Incremental Merging
    # Logic moved to integrate_wrapper.py (triggered by counter)
    if config.get('incremental_merging'):
        print("Incremental merging enabled (dynamic triggering).")
    else:
        print("Incremental merging disabled in config.")

    # 8. Final Merge
    submit_reduce("reduce_final", dependency_list)

if __name__ == "__main__":
    main()
