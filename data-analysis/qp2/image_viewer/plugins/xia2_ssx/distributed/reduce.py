#!/usr/bin/env python3
import os
import glob
import subprocess
import sys
import json
import argparse

def run_reduce(config_path, limit=None, output_dir=None):
    print("Starting Reduction...")
    
    with open(config_path, 'r') as f:
        config = json.load(f)
        
    # Root dir is where config lives (usually parent of this script execution)
    root_dir = os.path.dirname(os.path.abspath(config_path))
    
    valid_expts = []
    valid_refls = []
    
    # Find all job subdirectories
    subdirs = glob.glob(os.path.join(root_dir, "job_*"))
    
    # Sort by index
    def get_index(folder):
        try:
            parts = os.path.basename(folder).split('_')
            # job_N_name
            if len(parts) > 1 and parts[0] == 'job':
                return int(parts[1])
        except ValueError:
            pass
        return 999999
    
    subdirs.sort(key=get_index)
    
    subdirs.sort(key=get_index)
    
    # We want to find *any* valid integration results up to the limit
    # So we iterate through ALL subdirs, find valid ones, and THEN apply limit
    
    for sd in subdirs:
        # Stop search if we have enough
        if limit is not None and len(valid_expts) >= limit:
            break

        # Check specific locations for xia2 output
        # 1. DataFiles/integrated*.expt
        data_files = os.path.join(sd, "DataFiles")
        expts = glob.glob(os.path.join(data_files, "integrated*.expt"))
        refls = glob.glob(os.path.join(data_files, "integrated*.refl"))
        
        if not expts:
            # 2. Recursive fallback
            expts = glob.glob(os.path.join(sd, "**", "integrated*.expt"), recursive=True)
            refls = glob.glob(os.path.join(sd, "**", "integrated*.refl"), recursive=True)
            
        if expts and refls:
            expts.sort()
            refls.sort()
            valid_expts.append(expts[0])
            valid_refls.append(refls[0])
            
    if not valid_expts:
        print("No valid integrated datasets found.")
        sys.exit(1)
        
    print(f"Found {len(valid_expts)} valid datasets. Merging...")
    
    # Build xia2 command
    cmd = ["xia2.ssx_reduce"]
    
    # Input files
    for e in valid_expts:
        cmd.append(f"input.experiments={e}")
    for r in valid_refls:
        cmd.append(f"input.reflections={r}")
        
    # Config parameters
    if config.get('d_min'):
        cmd.append(f"d_min={config['d_min']}")
    if config.get('space_group'): # space_group is often auto-determined, but pass if enforced
        cmd.append(f"space_group={config['space_group']}")
    if config.get('unit_cell'):
        cmd.append(f"unit_cell={config['unit_cell']}")

    if config.get('nproc'):
        cmd.append(f"nproc={config['nproc']}")
        
    # Execute
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        cwd = output_dir
    else:
        cwd = os.getcwd()
        

    # Update status to MERGING
    # update_all_status(config, "MERGING", "Starting reduction...", limit=limit)
        
    print(f"Running: {' '.join(cmd)}")
    try:
        subprocess.check_call(cmd, cwd=cwd)

        # Run Dimple
        model_pdb = config.get('model_pdb')
        # Use cwd to construct absolute path, safer if we later decouple cwd from os.getcwd()
        merged_mtz = os.path.join(cwd, "DataFiles", "merged.mtz")
        if model_pdb and os.path.exists(merged_mtz):

            print(f"Running Dimple with model: {model_pdb}")
            # commands: module load ccp4; mkdir dimple; dimple ...
            # We use strict chain matching user request but with -p for mkdir to be safe
            dimple_cmds = [
                "module load ccp4",
                "mkdir -p dimple",
                f"dimple {merged_mtz} {model_pdb} dimple"
            ]
            # execute as a single shell command to maintain environment (module load)
            full_cmd = " && ".join(dimple_cmds)
            # We use /bin/bash -l to ensure profile is sourced for 'module' command if possible
            subprocess.check_call(["/bin/bash", "-l", "-c", full_cmd], cwd=cwd)
            
            # update_all_status(config, "SUCCESS", "Reduction and Dimple complete", limit=limit)
            print("Reduction and Dimple complete.")
        else:
             msg = "Reduction complete"
             if not model_pdb:
                 msg += " (No model provided for Dimple)"
             elif not os.path.exists(merged_mtz):
                 msg += " (merged.mtz not found for Dimple)"
             # update_all_status(config, "SUCCESS", msg, limit=limit)
             print(f"{msg}. Skipping Dimple.")

    except Exception as e:
        # update_all_status(config, "FAILED", f"Reduction failed: {e}", limit=limit)
        raise

def update_all_status(config, status, message=None, limit=None):
    import redis
    import time
    
    r_host = config.get('redis_host')
    r_port = config.get('redis_port')
    
    if not r_host or not r_port:
        return
        
    try:
        r = redis.Redis(host=r_host, port=r_port)
        keys = config.get('status_keys', [])
        
        # Limit keys if requested
        if limit is not None:
             keys = keys[:limit]
        
        data = {'status': status, 'timestamp': time.time()}
        if message: data['message'] = message
        json_data = json.dumps(data)
        
        for k in keys:
            r.set(k, json_data, ex=604800)
            
    except Exception as e:
        print(f"Failed to update redis: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to job_config.json")
    parser.add_argument("--limit", type=int, help="Limit number of datasets")
    args = parser.parse_args()
    
    run_reduce(args.config, args.limit)
