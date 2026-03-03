import argparse
import hashlib
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis
import numpy as np

# Import XDS worker for submitting individual jobs
from qp2.image_viewer.plugins.xds.submit_xds_job import XDSProcessDatasetWorker

# Add the project root to sys.path to find qp2
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
from qp2.pipelines.gmcaproc.hdf5reader import HDF5Reader
from qp2.pipelines.gmcaproc.cbfreader import CbfReader
from qp2.pipelines.gmcaproc.symmetry import Symmetry
from qp2.pipelines.gmcaproc.xds_config import Filenames, XdsConfig
from qp2.image_viewer.utils.run_job import run_command
from qp2.log.logging_config import setup_logging, get_logger
from qp2.config.programs import ProgramConfig
from qp2.pipelines.gmcaproc.cell_clustering import run_networkx_community_analysis
from qp2.pipelines.gmcaproc.xscale_parsers import parse_xscale_lp
from qp2.pipelines.gmcaproc.xds_parsers import parse_correct_lp
from qp2.pipelines.utils.pipeline_tracker import PipelineTracker
from qp2.pipelines.utils.image_set import get_image_set_string
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.pipelines.autoproc_xia2.xia2_parser import Xia2Parser
from qp2.utils.hklop2matrix import hkl_to_matrix
import re

logger = get_logger(__name__)

def check_and_wait_for_jobs(master_files: List[str], redis_conn: redis.Redis, timeout: int = 3600):
    """
    Checks if individual XDS jobs are running for the given master files.
    Waits until they complete or timeout.
    """
    if not redis_conn:
        return

    logger.info("Checking status of individual XDS jobs...")
    start_time = time.time()
    
    pending_files = set(master_files)
    
    while pending_files:
        if time.time() - start_time > timeout:
            logger.warning("Timeout waiting for individual XDS jobs to complete. Proceeding with merge attempts.")
            break
            
        completed = set()
        for mf in pending_files:
            status_key = f"analysis:out:xds:{mf}:status"
            status_json = redis_conn.get(status_key)
            logger.debug(f"Checking Redis key: {status_key} -> {status_json}")
            
            if status_json:
                try:
                    status_data = json.loads(status_json)
                    state = status_data.get("status")
                    if state in ["COMPLETED", "FAILED", "DONE"]:
                        completed.add(mf)
                    elif state in ["SUBMITTED", "RUNNING"]:
                        pass # Still waiting
                    else:
                        # Unknown state, treat as completed/not-running to avoid deadlock
                        completed.add(mf)
                except:
                    completed.add(mf)
            else:
                # No status key means no job is running (or it expired).
                completed.add(mf)
        
        pending_files -= completed
        
        if pending_files:
            # Show the first few waiting files
            wait_list = list(pending_files)[:3]
            logger.info(f"Waiting for {len(pending_files)} jobs... {wait_list}...")
            time.sleep(10)

def get_reindex_operator(
    proc_dir: str,
    reference_hkl: str,
    input_hkl: str
):
    """
    Runs POINTLESS to find the reindexing operator relative to reference.
    Returns the operator string (e.g., "k,h-l,l") or None.
    Does NOT generate an output file.
    """
    cmd_str = (
        f"{XdsConfig.POINTLESS_EXECUTABLE} "
        f"HKLREF {reference_hkl} "
        f"HKLIN {input_hkl} "
    )
    
    # We do not define HKLOUT, so it just calculates stats
    
    job_name = f"ptl_check_{os.path.basename(input_hkl)}"
    # Create input file for Pointless
    inp_file = os.path.join(proc_dir, f"{job_name}.inp")
    with open(inp_file, "w") as f:
        f.write("SETTING SYMMETRY-BASED\n")

    full_cmd = f"{cmd_str} < {inp_file}"

    try:
        run_command(
            full_cmd,
            cwd=proc_dir,
            job_name=job_name,
            method="shell",
            pre_command=ProgramConfig.get_setup_command("ccp4")
        )
        
        # Parse output file
        out_file = os.path.join(proc_dir, f"{job_name}.out")
        if not os.path.exists(out_file):
             logger.error(f"Pointless output file not found: {out_file}")
             return None

        with open(out_file, 'r') as f:
            content = f.read()

        # Strategy 1: Look for "Best Solution" block
        match = re.search(r"Best Solution:.*?Reindex operator:\s*\[(.*?)\].*?CC:\s*([\d\.-]+).*?Likelihood:\s*([\d\.-]+)", content, re.DOTALL)
        if match:
             op, cc, lk = match.groups()
             logger.info(f"  Pointless found best solution: [{op.strip()}] CC={cc} Likelihood={lk}")
             return op.strip()

        # Strategy 2: Look for "Alternative indexing relative to reference file" table
        # Example line: "  1        [h,k,l]           0.959     0.994"
        if "Alternative indexing relative to reference file" in content:
             matches = re.findall(r"^\s*\d+\s*\[(.*?)\]\s*([\d\.-]+)\s*([\d\.-]+)", content, re.MULTILINE)
             if matches:
                 # Return the first one (highest likelihood usually/sorted)
                 op, cc, lk = matches[0]
                 logger.info(f"  Pointless table top hit: [{op.strip()}] CC={cc} Likelihood={lk}")
                 return op.strip()
        
        # If no explicit reindexing found, assume consistent if checks passed? 
        # Or return identity "h,k,l"?
        logger.warning(f"  Could not parse reindex operator for {os.path.basename(input_hkl)}, assuming h,k,l")
        return "h,k,l"

    except Exception as e:
        logger.error(f"Pointless check failed: {e}")
        return None

def run_xscale(
    proc_dir: str,
    input_data: List[Dict[str, Any]],
    output_hkl: str,
    space_group: str = None,
    unit_cell: str = None,
    friedel_law: str = "FALSE",
    resolution_shells: str = "20 3.0 1.0"
):
    """
    Generates XSCALE.INP and runs xscale_par.
    """
    xscale_inp_path = os.path.join(proc_dir, "XSCALE.INP")
    
    with open(xscale_inp_path, "w") as f:
        # XSCALE has a 50 character limit on filenames
        # We ensure output file is written to cwd using just filename
        f.write(f"OUTPUT_FILE={os.path.basename(output_hkl)}\n")
        f.write("MAXIMUM_NUMBER_OF_PROCESSORS=16\n")
        f.write(f"FRIEDEL'S_LAW={friedel_law}\n")
        if space_group:
             f.write(f"SPACE_GROUP_NUMBER={space_group}\n")
        if unit_cell:
             f.write(f"UNIT_CELL_CONSTANTS={unit_cell}\n")
        
        f.write("MERGE=TRUE\n")
        f.write("STRICT_ABSORPTION_CORRECTION=TRUE\n")
        # f.write(f"RESOLUTION_SHELLS={resolution_shells}\n")
        
        for item in input_data:
            f.write(f"INPUT_FILE={item['path']}\n")
            
            # Handle Reindexing
            mat = item.get('reidx_matrix')
            if mat is not None:
                 # Check if not identity
                 if not np.allclose(mat, np.eye(3)):
                     # XSCALE expects 12 integers: m11 m12 m13 t1  m21 m22 m23 t2  m31 m32 m33 t3
                     reidx_values = []
                     for row in mat:
                         reidx_values.extend([int(round(x)) for x in row])
                         reidx_values.append(0)
                     
                     s = " ".join(map(str, reidx_values))
                     f.write(f"REIDX_ISET={s}\n")

            # f.write(f"DO_CORRECTIONS=TRUE\n") 
    
    logger.info(f"Generated XSCALE.INP at {xscale_inp_path}")
    
    try:
        run_command(
            [XdsConfig.XSCALE_EXECUTABLE],
            cwd=proc_dir,
            job_name="xscale",
            method="shell",
            pre_command=ProgramConfig.get_setup_command("xds")
        )
        logger.info("XSCALE execution finished.")
    except Exception as e:
        logger.error(f"XSCALE execution failed: {e}")
        raise

    if not os.path.exists(output_hkl):
        raise FileNotFoundError(f"XSCALE did not produce {output_hkl}")
    
    return output_hkl

def _import_single_dataset(idx, ds, multiplex_dir, dials_setup_cmd):
    """Helper function to import a single dataset for parallel execution."""
    xds_dir = os.path.dirname(ds["hkl_file"])
    import_dir = multiplex_dir / f"import_{idx}"
    import_dir.mkdir(exist_ok=True)
    
    # We need to run dials.import_xds in the import dir, pointing to xds_dir
    # input_dir needs to be absolute
    abs_xds_dir = os.path.abspath(xds_dir)
    
    cmd = f"dials.import_xds {abs_xds_dir} output.experiments=imported.expt output.reflections=imported.refl"
    
    try:
        logger.info(f"Importing {abs_xds_dir} (Task {idx})...")
        run_command(
            cmd,
            cwd=str(import_dir),
            method="shell",
            job_name=f"dials_import_{idx}",
            pre_command=dials_setup_cmd
        )
        
        expt_file = import_dir / "imported.expt"
        refl_file = import_dir / "imported.refl"
        
        if expt_file.exists() and refl_file.exists():
            return str(import_dir)
        else:
            logger.warning(f"DIALS import failed for {abs_xds_dir}")
            return None
            
    except Exception as e:
        logger.error(f"Error importing {abs_xds_dir}: {e}")
        return None

def run_xia2_multiplex(
    proc_dir: str,
    selected_datasets: List[Dict],
    space_group: str = None,
    unit_cell: str = None,
    resolution: float = None,
    dials_setup_cmd: str = None
):
    """
    Runs dials.import_xds for each dataset and then xia2.multiplex to merge.
    Uses ThreadPoolExecutor to parallelize imports.
    """
    logger.info("Starting Xia2 Multiplex merge...")
    
    multiplex_dir = Path(proc_dir) / "xia2_multiplex"
    multiplex_dir.mkdir(exist_ok=True)
    
    imported_experiments = []
    
    # Use default if not provided
    if not dials_setup_cmd:
        dials_setup_cmd = ProgramConfig.get_setup_command('dials')

    # Parallelize Imports
    with ThreadPoolExecutor(max_workers=min(len(selected_datasets), 8)) as executor:
        futures = []
        for i, ds in enumerate(selected_datasets):
            futures.append(executor.submit(_import_single_dataset, i, ds, multiplex_dir, dials_setup_cmd))
            
        for future in as_completed(futures):
            res = future.result()
            if res:
                imported_experiments.append(res)

    if not imported_experiments:
        raise RuntimeError("No datasets successfully imported for Xia2 Multiplex.")
        
    if len(imported_experiments) < 2:
        logger.warning("Less than 2 datasets imported. Multiplex might not be meaningful.")

    # Run xia2.multiplex
    # We pass the directories containing the imported files
    multiplex_cmd = ["xia2.multiplex"]
    for d in imported_experiments:
        multiplex_cmd.append(d)
        
    if space_group:
        multiplex_cmd.append(f"space_group={space_group}")
    if unit_cell:
        # Quote unit cell to handle spaces safely in shell construction if needed, 
        # but run_command with list handles args. 
        # However, xia2 expects unit_cell="a b c al be ga"
        multiplex_cmd.append(f"unit_cell={unit_cell}")
    if resolution:
        multiplex_cmd.append(f"d_min={resolution}")
        
    # Add parallel flags
    multiplex_cmd.append("multiprocessing.nproc=8") 
    
    try:
        logger.info(f"Running xia2.multiplex with {len(imported_experiments)} datasets...")
        run_command(
            multiplex_cmd,
            cwd=str(multiplex_dir),
            method="shell",
            job_name="xia2_multiplex",
            pre_command=dials_setup_cmd,
            walltime="04:00:00",
            memory="32gb"
        )
        logger.info("Xia2 Multiplex finished.")
        
        # Check output and Parse Results
        results = {}
        html_report = multiplex_dir / "xia2-multiplex.html"
        scaled_mtz = multiplex_dir / "DataFiles" / "scaled.mtz"
        
        if html_report.exists():
            results["report_url"] = str(html_report)
        
        if scaled_mtz.exists():
            results["final_mtz"] = str(scaled_mtz)
        else:
            # If no MTZ, we consider it a failure for now
            raise FileNotFoundError("xia2.multiplex did not produce DataFiles/scaled.mtz")

        # --- Parse Statistics ---
        
        # 1. Try xia2.json
        json_file = multiplex_dir / "xia2.json"
        if json_file.exists():
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    
                # Extract stats from standard xia2 JSON structure
                # Typically data['_statistics']['merged'] or similar
                # We look for the main dataset stats.
                # Assuming first crystal/wavelength if multiple
                
                # Helper to recursive search? Or hardcoded path.
                # Common path: _statistics -> [crystal] -> [wavelength] -> [sweep] -> but this is multiplex
                # Let's try to find keys: 'high_resolution_limit', 'completeness', etc.
                
                def extract_stats_from_dict(d):
                    # Mapping xia2 json keys to our internal keys
                    mapping = {
                        "high_resolution_limit": "highresolution",
                        "completeness": "completeness",
                        "cc_half": "cchalf",
                        "i_over_sigma_mean": "isigmai",
                        "r_merge": "rmerge",
                        "r_meas": "rmeas",
                        "r_pim": "rpim",
                        "anomalous_completeness": "anom_completeness",
                        "anomalous_multiplicity": "anom_multiplicity",
                        "anomalous_cc_half": "anom_cchalf",
                        "total_observations": "nobs", # case sensitive match later
                        "total_unique_observations": "nuniq"
                    }
                    
                    extracted = {}
                    # We usually want 'Overall' stats
                    # Structure usually: key -> { 'Overall': val, 'Low': val, 'High': val }
                    for k, v in d.items():
                        if k in mapping and isinstance(v, dict):
                            if 'Overall' in v:
                                extracted[mapping[k]] = v['Overall']
                            elif isinstance(v, (float, int, str)):
                                extracted[mapping[k]] = v
                    return extracted

                # Traverse to find the statistics block
                # Usually: data['_statistics']
                stats_root = data.get('_statistics', {})
                # It might be nested by crystal/wavelength. 
                # Let's flatten the values and find the one with 'high_resolution_limit'
                
                found_stats = {}
                
                # Simple recursive search for a dict containing 'high_resolution_limit'
                def find_stats_block(obj):
                    if isinstance(obj, dict):
                        if 'high_resolution_limit' in obj:
                            return obj
                        for v in obj.values():
                            res = find_stats_block(v)
                            if res: return res
                    return None
                
                stats_block = find_stats_block(stats_root)
                if stats_block:
                    found_stats = extract_stats_from_dict(stats_block)
                    results.update(found_stats)
                    logger.info("Parsed statistics from xia2.json")
                    
            except Exception as e:
                logger.warning(f"Failed to parse xia2.json: {e}")

        # 2. Fallback to Xia2Parser (xia2.txt)
        if "highresolution" not in results:
            xia2_txt = multiplex_dir / "xia2.txt"
            if xia2_txt.exists():
                try:
                    parser = Xia2Parser(wdir=str(multiplex_dir), filename="xia2.txt")
                    parsed_summary = parser.summarize()
                    if parsed_summary:
                        results.update(parsed_summary)
                        logger.info("Parsed statistics from xia2.txt using Xia2Parser")
                except Exception as e:
                    logger.warning(f"Failed to parse xia2.txt: {e}")

        return results

    except Exception as e:
        logger.error(f"Xia2 Multiplex failed: {e}")
        raise

def get_sql_mapped_results(proc_dir: str, master_file: str, results_dict: dict) -> dict:
    """
    Maps parsed XSCALE or Xia2 results to DB fields.
    """
    total_stats = {}
    
    # Path A: XSCALE Table extraction
    if results_dict.get("table1_total"):
        header = results_dict.get("table1_header", [])
        total_values = results_dict["table1_total"]
        total_stats = {
            header[i].lower(): total_values[i]
            for i in range(min(len(header), len(total_values)))
        }
    
    # Path B: Generic Keys (from Xia2Parser or JSON)
    # We prefer direct keys if present in results_dict
    
    spgn = results_dict.get("SPACE_GROUP_NUMBER") or results_dict.get("spacegroup", "")
    spg_symbol = spgn
    if str(spgn).isdigit():
        try:
            spg_symbol = Symmetry.number_to_symbol(int(spgn)).replace(" ", "")
        except:
            pass

    unit_cell_value = results_dict.get("UNIT_CELL_CONSTANTS") or results_dict.get("unitcell", "")
    
    # Helper to get value from either total_stats (XSCALE) or results_dict (Xia2)
    def get_val(key_xscale, key_generic):
        return results_dict.get(key_generic) or total_stats.get(key_xscale, "")

    # Standardize result mapping for database
    mapped = {
        "sampleName": "merged_run",
        "workdir": proc_dir,
        "imagedir": os.path.dirname(master_file),
        "highresolution": str(results_dict.get("resolution_highres") or results_dict.get("resolution_based_on_cchalf") or results_dict.get("highresolution") or ""),
        "spacegroup": str(spg_symbol),
        "unitcell": str(unit_cell_value),
        
        "rmerge": str(get_val("r_factor_observed", "rmerge")),
        "rmeas": str(get_val("r_meas", "rmeas")),
        "rpim": str(results_dict.get("R-pim") or results_dict.get("rpim") or ""),
        "isigmai": str(get_val("i_sigma", "isigmai")),
        "completeness": str(get_val("completeness", "completeness")),
        "anom_completeness": str(results_dict.get("anomalous_completeness") or results_dict.get("anom_completeness") or ""),
        
        "table1": results_dict.get("table1_text") or results_dict.get("table1", ""),
        "scale_log": results_dict.get("scale_log_path") or results_dict.get("logfile", ""),
        "truncate_mtz": results_dict.get("final_mtz") or results_dict.get("truncate_mtz", ""),
        
        "run_stats": json.dumps(results_dict, default=str),
        
        "cchalf": str(get_val("cc_half", "cchalf")),
        "nobs": str(get_val("number_observed", "nobs") or results_dict.get("Nobs", "")),
        "nuniq": str(get_val("number_unique", "nuniq") or results_dict.get("Nuniq", "")),
        
        "multiplicity": str(results_dict.get("multiplicity") or ""),
        
        "report_url": str(results_dict.get("report_url") or ""),
    }
    
    # Calculate Multiplicity if missing
    if not mapped["multiplicity"] and mapped["nobs"] and mapped["nuniq"]:
        try:
            nobs_val = float(mapped["nobs"])
            nuniq_val = float(mapped["nuniq"])
            if nuniq_val > 0:
                mapped["multiplicity"] = f"{nobs_val / nuniq_val:.2f}"
        except ValueError:
            pass

    return {k: v for k, v in mapped.items() if v is not None}

def main():
    parser = argparse.ArgumentParser(
        description="Run XDS processing for multiple datasets and merge with XSCALE or Xia2 Multiplex."
    )
    parser.add_argument("--master_file", required=True, action="append")
    parser.add_argument("--proc_dir", required=True)
    parser.add_argument("--redis_key", required=True)
    parser.add_argument("--status_key", required=True)
    parser.add_argument("--nproc", type=int, default=32)
    parser.add_argument("--njobs", type=int, default=4)
    parser.add_argument("--space_group", type=str)
    parser.add_argument("--unit_cell", type=str)
    parser.add_argument("--resolution", type=float)
    parser.add_argument("--native", action="store_true")
    parser.add_argument("--reference_hkl", type=str)
    parser.add_argument("--model", type=str)
    parser.add_argument("--start", type=int)
    parser.add_argument("--end", type=int)
    parser.add_argument("--beamline", type=str)
    parser.add_argument("--redis_host", type=str)
    parser.add_argument("--redis_port", type=int, default=6379)
    parser.add_argument("--xds_param", action="append")
    parser.add_argument("--group_name", type=str)
    parser.add_argument("--run_prefix", type=str, help="Run prefix for linking to DatasetRun")
    parser.add_argument("--pi_badge", type=str)
    parser.add_argument("--esaf_number", type=str)
    # New arg
    parser.add_argument("--merge_method", choices=["xscale", "xia2_multiplex"], default="xscale", help="Method for merging datasets")

    args = parser.parse_args()
    
    # --- Setup Processing Directory ---
    # For merge jobs, create a merge-specific directory instead of using the passed proc_dir
    # which is based on the first dataset name
    passed_proc_dir = Path(args.proc_dir)
    
    # Create merge directory name from hash of all datasets
    datasets_str = "".join(sorted(args.master_file))
    datasets_hash = hashlib.md5(datasets_str.encode()).hexdigest()[:8]
    merge_dir_name = f"merge_{len(args.master_file)}datasets_{datasets_hash}"
    
    # Replace the last component (dataset name) with merge directory name
    proc_root = passed_proc_dir.parent / merge_dir_name
    proc_root.mkdir(parents=True, exist_ok=True)
    
    
    # Initialize logging with a file in the proc_dir
    log_file = proc_root / "xscale_merge.log"
    setup_logging(root_name="qp2", log_level=logging.INFO, log_file=str(log_file))
    
    logger.info(f"Starting merged processing in {proc_root}")

    redis_config = None
    redis_conn = None
    if args.redis_host:
        redis_config = {"host": args.redis_host, "port": args.redis_port, "db": 0}
        redis_conn = redis.Redis(**redis_config)
    
    # Create unique Redis key for merge job using hash of sorted dataset paths
    datasets_str = "".join(sorted(args.master_file))
    datasets_hash = hashlib.md5(datasets_str.encode()).hexdigest()[:8]
    merge_redis_key = f"analysis:out:xds_merge:{datasets_hash}"
    merge_status_key = f"{merge_redis_key}:status"
    logger.info(f"Merge job Redis key: {merge_redis_key}")

    # --- Construct ImageSet String ---
    run_map = {}
    for mf in args.master_file:
        start_frame = args.start
        end_frame = args.end
        if start_frame is None or end_frame is None:
            try:
                if mf.endswith((".h5", ".hdf5")):
                    with HDF5Reader(mf) as r:
                        if start_frame is None: start_frame = 1
                        if end_frame is None: end_frame = r.total_frames
                elif mf.endswith(".cbf"):
                    if start_frame is None: start_frame = 1
                    if end_frame is None: end_frame = 9999 
            except Exception:
                start_frame, end_frame = 1, 9999
        run_map[mf] = list(range(start_frame, end_frame + 1))

    image_set_str = get_image_set_string(run_map)

    # --- Initialize PipelineTracker ---
    # Create descriptive job name
    num_datasets = len(args.master_file)
    merge_job_name = f"merge_{num_datasets}datasets_{args.merge_method}"
    merge_log_file = os.path.join(proc_root, f"xds_merge_{args.merge_method}.log")
    
    initial_params = {
        "sampleName": merge_job_name,
        "imageSet": image_set_str,
        "imagedir": os.path.dirname(os.path.abspath(args.master_file[0])),  # Required by database
        "logfile": merge_log_file,  # Required by database
        "username": os.getenv("USER"),
        "beamline": args.beamline or get_beamline_from_hostname(),
        "workdir": str(proc_root),
        "command": " ".join(sys.argv),
        "primary_group": args.group_name,
        "run_prefix": args.run_prefix,
        "pi_id": args.pi_badge,
        "esaf_id": args.esaf_number,
        "datasets": json.dumps(args.master_file),
        "merge_method": args.merge_method
    }
    
    tracker = PipelineTracker(
        pipeline_name=f"xds_merge_{args.merge_method}",
        run_identifier=merge_redis_key,  # Use unique merge key
        initial_params=initial_params,
        result_mapper=lambda d: get_sql_mapped_results(str(proc_root), args.master_file[0], d),
        redis_config=redis_config
    )
    tracker.start()

    try:
        tracker.update_progress("RUNNING", {"message": "Processing individual datasets..."})
        
        # Parse extra params
        extra_xds_params = {}
        if args.xds_param:
            for param in args.xds_param:
                if "=" in param:
                    key, value = param.split("=", 1)
                    extra_xds_params[key.strip()] = value.strip()

        space_group_number = None
        if args.space_group:
             if args.space_group.isdigit():
                 space_group_number = args.space_group
             else:
                 space_group_number = str(Symmetry.symbol_to_number(args.space_group))

        # --- WAIT FOR CONCURRENT JOBS (BEFORE PROCESSING) ---
        logger.info(f"=== Processing {len(args.master_file)} datasets for merge ===")
        for i, mf in enumerate(args.master_file, 1):
            logger.info(f"  {i}. {os.path.basename(mf)}")
        
        # Check for pre-existing running jobs and wait for them
        if redis_conn:
            # Initial wait for jobs availability (mitigate race condition with submission)
            wait_for_registration = 60
            waited = 0
            while waited < wait_for_registration:
                all_registered = True
                missing_registration = []
                for mf in args.master_file:
                    status_key = f"analysis:out:xds:{mf}:status"
                    if not redis_conn.exists(status_key):
                         # If it's missing, is the result file there?
                         ds_name = os.path.splitext(os.path.basename(mf))[0].replace("_master", "")
                         hkl = proc_root / ds_name / Filenames.XDS_ASCII_HKL
                         if not hkl.exists():
                             all_registered = False
                             missing_registration.append(ds_name)
                             break
                
                if all_registered:
                    break
                
                if waited % 5 == 0:
                     logger.info(f"Waiting for {len(missing_registration)} jobs to register in Redis... ({waited}s)")
                time.sleep(1)
                waited += 1

            running_jobs = []
            for mf in args.master_file:
                status_key = f"analysis:out:xds:{mf}:status"
                status_json = redis_conn.get(status_key)
                if status_json:
                    try:
                        status_data = json.loads(status_json)
                        state = status_data.get("status")
                        if state in ["SUBMITTED", "RUNNING"]:
                            running_jobs.append(mf)
                    except:
                        pass
            
            if running_jobs:
                logger.info(f"Found {len(running_jobs)} pre-existing jobs, waiting for completion...")
                check_and_wait_for_jobs(running_jobs, redis_conn)
                logger.info("Pre-check complete.")
            else:
                logger.info("No pre-existing running jobs found.")

        # 1. Check for existing results for each dataset
        # (Individual XDS jobs should be submitted by the dialog first)
        processed_datasets = []
        missing_datasets = []
        
        for master_file in args.master_file:
            try:
                dataset_name = os.path.splitext(os.path.basename(master_file))[0].replace("_master", "")
                
                # Try to get proc_dir from Redis first (most robust)
                r_key = f"analysis:out:xds:{master_file}"
                redis_proc_dir = redis_conn.hget(r_key, "_proc_dir") if redis_conn else None
                
                if redis_proc_dir:
                    dataset_proc_dir = Path(redis_proc_dir.decode('utf-8') if isinstance(redis_proc_dir, bytes) else redis_proc_dir)
                    logger.info(f"Got proc dir from Redis for {dataset_name}: {dataset_proc_dir}")
                else:
                    # Fallback to standard XDS structure: parent of passed_proc_dir (which is xds root) / dataset_name
                    # passed_proc_dir is usually .../xds/<first_dataset>, so parent is .../xds
                    dataset_proc_dir = passed_proc_dir.parent / dataset_name
                    logger.info(f"Calculated proc dir for {dataset_name}: {dataset_proc_dir}")

                logger.info(f"\n--- Checking dataset: {dataset_name} ---")
                
                hkl_file = os.path.join(dataset_proc_dir, Filenames.XDS_ASCII_HKL)
                stats_file = os.path.join(dataset_proc_dir, Filenames.XDS_STATS_JSON)
                
                logger.debug(f"Looking for HKL file at: {hkl_file}")
                
                if os.path.exists(hkl_file) and os.path.exists(stats_file):
                    logger.info(f"✓ Found existing results for {dataset_name}")
                    with open(stats_file, 'r') as f:
                        stats = json.load(f)
                    cell = stats.get("UNIT_CELL_CONSTANTS")
                    cell_str = " ".join(map(str, cell)) if isinstance(cell, list) else str(cell)
                    sg = stats.get("SPACE_GROUP_NUMBER")
                    logger.debug(f"Loaded stats: SG={sg}, Cell={cell_str}")
                    processed_datasets.append({
                        "hkl_file": hkl_file, "unit_cell": cell_str,
                        "space_group": sg,
                        "master_file": master_file,
                        "original_stats": stats 
                    })
                    logger.info(f"✓ Successfully added {dataset_name} to processed list")
                else:
                    logger.warning(f"✗ No results found for {dataset_name}")
                    missing_datasets.append(dataset_name)
            except Exception as e:
                logger.error(f"Failed to check {master_file}: {e}")
                missing_datasets.append(dataset_name)

        # --- REPORT MISSING DATASETS ---
        if missing_datasets:
            logger.warning(f"\n=== Missing results for {len(missing_datasets)} datasets ===")
            for ds_name in missing_datasets:
                logger.warning(f"  - {ds_name}")
            logger.warning("\nEnsure individual XDS jobs completed successfully before merging.")

        # --- CHECK FOR FAILURES ---
        logger.info(f"\n=== Processing Summary ===")
        total_datasets = len(args.master_file)
        successful_datasets = len(processed_datasets)
        failed_count = total_datasets - successful_datasets
        
        logger.info(f"Total datasets: {total_datasets}")
        logger.info(f"Successful: {successful_datasets}")
        logger.info(f"Failed: {failed_count}")
        
        if failed_count > 0:
            failed_files = [mf for mf in args.master_file if not any(d["master_file"] == mf for d in processed_datasets)]
            logger.warning(f"WARNING: {failed_count} out of {total_datasets} datasets failed to process:")
            for ff in failed_files:
                logger.warning(f"  - {os.path.basename(ff)}")
            
            if successful_datasets < 2:
                raise RuntimeError(
                    f"Insufficient datasets for merging: {successful_datasets} successful, {failed_count} failed. "
                    f"Need at least 2 datasets to merge."
                )
            else:
                logger.warning(f"Continuing merge with {successful_datasets} datasets (out of {total_datasets} requested)")

        if not processed_datasets:
            raise RuntimeError("No datasets processed successfully.")

        # 2. Clustering
        logger.info(f"\n=== Cell Clustering ===")
        selected_datasets = processed_datasets
        if len(processed_datasets) > 1:
            logger.info(f"Clustering {len(processed_datasets)} datasets by unit cell and space group...")
            import networkx as nx
            G = nx.Graph()
            for i in range(len(processed_datasets)):
                G.add_node(i)
                logger.debug(f"Node {i}: {os.path.basename(processed_datasets[i]['master_file'])} "
                           f"SG={processed_datasets[i]['space_group']} Cell={processed_datasets[i]['unit_cell']}")
            clusters_formed = 0
            for i in range(len(processed_datasets)):
                for j in range(i + 1, len(processed_datasets)):
                    try:
                        # Check Space Group Compatibility
                        sg1 = processed_datasets[i].get("space_group")
                        sg2 = processed_datasets[j].get("space_group")
                        if not Symmetry.same_point_group(sg1, sg2):
                            continue

                        # Check Unit Cell Compatibility
                        p1 = np.fromstring(processed_datasets[i]["unit_cell"], sep=" ")
                        p2 = np.fromstring(processed_datasets[j]["unit_cell"], sep=" ")
                        if len(p1) == 6 and len(p2) == 6:
                            max_diff = np.max(np.abs(p1 - p2) / ((p1 + p2) / 2.0 + 1e-9))
                            if max_diff < 0.05:
                                G.add_edge(i, j)
                                clusters_formed += 1
                                logger.debug(f"Linked {i}-{j} (cell diff={max_diff:.3f})")
                    except Exception as e:
                        logger.debug(f"Failed to compare {i}-{j}: {e}")
                        continue
            
            logger.info(f"Formed {clusters_formed} cell compatibility links")
            communities = list(nx.connected_components(G))
            communities.sort(key=len, reverse=True)
            logger.info(f"Found {len(communities)} cluster(s)")
            for idx, comm in enumerate(communities):
                logger.info(f"  Cluster {idx+1}: {len(comm)} datasets")
            
            if communities:
                selected_datasets = [processed_datasets[i] for i in communities[0]]
                logger.info(f"Using largest cluster with {len(selected_datasets)} datasets")
                # Sort by ISa to use the best dataset as reference
                selected_datasets.sort(key=lambda x: float(x.get("original_stats", {}).get("ISa", 0) or 0), reverse=True)
                logger.debug(f"Sorted by I/sigma(I), best dataset: {os.path.basename(selected_datasets[0]['master_file'])}")
        else:
            logger.info("Only 1 dataset, skipping clustering")

        logger.info(f"\n=== Merging Strategy ===")
        logger.info(f"Merge method: {args.merge_method}")
        logger.info(f"Datasets to merge: {len(selected_datasets)}")
        
        tracker.update_progress("MERGING", {"message": f"Merging {len(selected_datasets)} datasets with {args.merge_method}..."})

        # --- Branch based on Merge Method ---
        if len(selected_datasets) == 1:
            logger.info("Only one dataset suitable for merging. Skipping merge step to avoid duplicate DB entry.")
            tracker.update_progress("SKIPPED", {"message": "Merge skipped: only one valid dataset."})
            
            # Update Redis status only
            if redis_conn:
                final_status = {"status": "SKIPPED", "timestamp": time.time(), "message": "Only one dataset"}
                redis_conn.set(args.status_key, json.dumps(final_status), ex=7 * 24 * 3600)
            
            sys.exit(0)

        elif args.merge_method == "xia2_multiplex":
            # --- New Xia2 Multiplex Path ---
            logger.info(f"\n=== Running Xia2 Multiplex ===")
            logger.info(f"Input datasets: {len(selected_datasets)}")
            results = run_xia2_multiplex(
                str(proc_root), 
                selected_datasets, 
                space_group=space_group_number or str(selected_datasets[0]["space_group"]),
                unit_cell=args.unit_cell or selected_datasets[0]["unit_cell"],
                resolution=args.resolution
            )
            
            # Populate basic stats if available from report, or leave minimal
            tracker.succeed(results)
            
            # Redis update
            if redis_conn:
                with redis_conn.pipeline() as pipe:
                    pipe.hset(args.redis_key, "final_mtz", results.get("final_mtz", ""))
                    pipe.hset(args.redis_key, "merged_datasets", json.dumps([d["master_file"] for d in selected_datasets]))
                    pipe.expire(args.redis_key, 7 * 24 * 3600)
                    pipe.execute()
                
                final_status = {"status": "COMPLETED", "timestamp": time.time()}
                redis_conn.set(args.status_key, json.dumps(final_status), ex=7 * 24 * 3600)

        else:
            # --- Original XSCALE Path ---
            logger.info(f"\n=== Running XSCALE Merge ===")
            logger.info(f"Input datasets: {len(selected_datasets)}")
            
            # 3. Resolve Ambiguity & Merge
            reference_hkl = selected_datasets[0]["hkl_file"]
            logger.info(f"\n=== Reindexing against Reference ===")
            logger.info(f"Reference Dataset (full path): {str(Path(reference_hkl).resolve())}")
            
            xscale_inputs = [{"path": reference_hkl}]
            
            for i in range(1, len(selected_datasets)):
                ds = selected_datasets[i]
                logger.info(f"\nProcessing {os.path.basename(ds['hkl_file'])} ...")
                op_str = get_reindex_operator(str(proc_root), reference_hkl, ds["hkl_file"])
                
                reidx_mat = None
                if op_str:
                    try:
                        reidx_mat = hkl_to_matrix(op_str)
                        logger.info(f"  -> Selected Reindex Operator: {op_str}")
                    except Exception as e:
                        logger.warning(f"  Failed to convert operator '{op_str}' to matrix: {e}")

                xscale_inputs.append({"path": ds["hkl_file"], "reidx_matrix": reidx_mat})

            merged_dir = proc_root / "merged"
            merged_dir.mkdir(exist_ok=True)
            merged_hkl = str(merged_dir / "XSCALE.HKL")
            
            run_xscale(
                str(merged_dir), xscale_inputs, merged_hkl,
                space_group=space_group_number or str(selected_datasets[0]["space_group"]),
                unit_cell=args.unit_cell or selected_datasets[0]["unit_cell"],
                friedel_law="TRUE" if args.native else "FALSE"
            )

            # 4. Final Parse & DB Save
            xscale_log = os.path.join(str(merged_dir), "XSCALE.LP")
            xscale_results = parse_xscale_lp(xscale_log)
            if xscale_results:
                results_to_report = xscale_results
                results_to_report["scale_log_path"] = xscale_log
                final_hkl = merged_hkl
                merged_mtz = "" 
                
                # --- Run XDSCONV ---
                logger.info("Running XDSCONV on merged result...")
                xdsconv_inp_path = merged_dir / Filenames.XDSCONV_INPUT
                merged_mtz_path = str(merged_dir / "merged.mtz")
                
                # Determine cell/sg for XDSCONV
                final_sg = results_to_report.get("SPACE_GROUP_NUMBER")
                final_cell = results_to_report.get("UNIT_CELL_CONSTANTS")
                
                with open(xdsconv_inp_path, "w") as f:
                    f.write(f"INPUT_FILE={final_hkl}\n")
                    f.write(f"OUTPUT_FILE={merged_dir / f'{Filenames.F2MTZ_INPUT}'} CCP4_I+F\n") 
                    f.write(f"FRIEDEL'S_LAW=FALSE\n") 
                    if final_sg: f.write(f"SPACE_GROUP_NUMBER={final_sg}\n")
                    if final_cell: f.write(f"UNIT_CELL_CONSTANTS={final_cell}\n")
                
                try:
                    run_command(
                        [XdsConfig.XDSCONV_EXECUTABLE], cwd=str(merged_dir),
                        method="shell", job_name="xdsconv",
                        pre_command=ProgramConfig.get_setup_command("xds")
                    )
                    
                    # F2MTZ
                    f2mtz_inp_path = merged_dir / Filenames.F2MTZ_INPUT
                    if f2mtz_inp_path.exists():
                         cmd_f2mtz = f"{XdsConfig.F2MTZ_EXECUTABLE} hklout {merged_mtz_path} < {f2mtz_inp_path}"
                         run_command(
                            cmd_f2mtz, cwd=str(merged_dir), method="shell",
                            job_name="f2mtz", pre_command=ProgramConfig.get_setup_command("ccp4")
                         )
                         if os.path.exists(merged_mtz_path):
                             logger.info(f"Merged MTZ created: {merged_mtz_path}")
                             results_to_report["final_mtz"] = merged_mtz_path
                             merged_mtz = merged_mtz_path
                except Exception as e:
                    logger.error(f"Post-processing failed: {e}")

                tracker.succeed(results_to_report)
                
                # Update Redis
                if redis_conn:
                    with redis_conn.pipeline() as pipe:
                        pipe.hset(args.redis_key, "final_hkl", final_hkl)
                        pipe.hset(args.redis_key, "final_mtz", merged_mtz)
                        pipe.hset(args.redis_key, "merged_datasets", json.dumps([d["hkl_file"] for d in selected_datasets]))
                        pipe.expire(args.redis_key, 7 * 24 * 3600)
                        pipe.execute()

                    final_status = {"status": "COMPLETED", "timestamp": time.time()}
                    redis_conn.set(args.status_key, json.dumps(final_status), ex=7 * 24 * 3600)

            else:
                raise RuntimeError("Failed to parse XSCALE output.")

    except Exception as e:
        logger.error(f"Merged processing failed: {e}", exc_info=True)
        tracker.fail(str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
