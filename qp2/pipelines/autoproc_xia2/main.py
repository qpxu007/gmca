#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Command-line runner for crystallographic data processing pipelines (autoPROC, xia2).

This script provides a unified interface to run different processing pipelines,
handle dataset definitions, and track results in a database.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any

from qp2.xio.db_manager import get_beamline_from_hostname


def find_project_root(file_path):
    path = Path(file_path).resolve()
    for parent in path.parents:
        if (parent / "qp2").is_dir():
            return str(parent)
    return None


project_root = find_project_root(__file__)
if project_root and project_root not in sys.path:
    sys.path.insert(0, project_root)


from qp2.pipelines.utils.pipeline_tracker import PipelineTracker
from qp2.data_viewer.models import DataProcessResults
from qp2.log.logging_config import get_logger, setup_logging
from qp2.pipelines.autoproc_xia2.pipeline_runners import (
    AutoPROCRunner,
    Xia2Runner,
    Xia2SSXRunner,
    DimpleRunner,
)
from qp2.pipelines.gmcaproc.rcsb import RCSB
from qp2.pipelines.utils.image_set import get_image_set_string

logger = get_logger(__name__)


def update_db_with_postprocessing(
    tracker: PipelineTracker, post_proc_results: Dict[str, Any]
):
    """
    Updates the existing database record with post-processing results.

    Args:
        tracker: The PipelineTracker instance from the primary run.
        post_proc_results: A dictionary with results from cell search and Dimple.
    """
    if not post_proc_results or not tracker.result_pk_value:
        logger.warning("No post-processing results or primary DB record ID to update.")
        return

    try:
        logger.info("Updating database with post-processing results...")

        with tracker.db_manager.get_session() as session:
            # 1. Fetch the existing record within the active session
            existing_record = session.get(DataProcessResults, tracker.result_pk_value)

            if not existing_record:
                logger.error(
                    f"Could not find DataProcessResults record with PK {tracker.result_pk_value} to update."
                )
                return

            # 2. Safely access and update the run_stats JSON field
            if existing_record.run_stats:
                try:
                    run_stats = json.loads(existing_record.run_stats)
                except (json.JSONDecodeError, TypeError):
                    run_stats = {}
            else:
                run_stats = {}

            if (
                "dimple_pdb" in post_proc_results
                and "dimple_mtz" not in post_proc_results
            ):
                final_pdb_path = post_proc_results["dimple_pdb"]
                final_mtz_path = os.path.join(
                    os.path.dirname(final_pdb_path), "final.mtz"
                )
                if os.path.exists(final_mtz_path):
                    post_proc_results["dimple_mtz"] = final_mtz_path

            run_stats.update(post_proc_results)

            # 3. Modify the object directly. SQLAlchemy tracks the changes.
            existing_record.run_stats = json.dumps(run_stats, default=str)
            if "dimple_pdb" in post_proc_results:
                existing_record.solve = post_proc_results["dimple_pdb"]

            # The session will automatically commit the changes when the 'with' block exits.

        # 4. Update the separate PipelineStatus record (this is a separate transaction and is okay)
        dimple_status = "SUCCESS" if "dimple_pdb" in post_proc_results else "FAILED"
        tracker.update_progress(f"POSTPROC_{dimple_status}", post_proc_results)
        logger.info("Database updated successfully.")

    except Exception as e:
        logger.error(
            f"Failed to update database with post-processing results: {e}",
            exc_info=True,
        )


def main():
    """Main function to parse arguments and launch the selected pipeline runner."""
    setup_logging()
    parser = argparse.ArgumentParser(
        description="A command-line interface for running crystallography pipelines.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # --- Required Arguments ---
    parser.add_argument(
        "--pipeline",
        required=True,
        choices=["autoPROC", "xia2_dials", "xia2_dials_aimless", "xia2_xds", "xia2_ssx"],
        help="The processing pipeline to run.",
    )
    parser.add_argument(
        "--data",
        required=True,
        action="append",
        help="Dataset to process. Format:\n"
        "/path/to/master.h5\n"
        "/path/to/master.h5:start:end\n"
        "This argument can be specified multiple times for multi-sweep processing.",
    )
    parser.add_argument(
        "--work_dir",
        default=".",
        help="Working directory for the processing job. Defaults to the current directory.",
    )

    # --- Optional Processing Parameters ---
    parser.add_argument(
        "--highres", type=float, help="High resolution cutoff (in Angstroms)."
    )
    parser.add_argument("--space_group", help="Space group symbol or number.")
    parser.add_argument(
        "--unit_cell",
        help='Unit cell parameters as a quoted string: "a b c alpha beta gamma".',
    )
    parser.add_argument(
        "--native", action="store_true", help="Process native data (implies no anomalous)."
    )
    parser.add_argument(
        "--model", help="Path to a PDB model for post-processing (e.g., Dimple)."
    )
    parser.add_argument(
        "--reference_hkl", help="Reference hkl of previous collected data."
    )    
    parser.add_argument(
        "--nproc", type=int, default=4, help="Number of processors to use per job/node."
    )
    parser.add_argument(
        "--njobs",
        type=int,
        default=8,
        help="Number of parallel jobs (for autoPROC/XDS) or sweeps (for xia2).",
    )
    parser.add_argument(
        "--runner",
        default="slurm",
        choices=["slurm", "shell"],
        help="Job submission method.",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Enable fast processing mode for supported pipelines.",
    )
    parser.add_argument(
        "--trust_beam_centre",
        default="True",
        help="Trust the beam centre found in images.",
    )
    parser.add_argument(
        "--steps",
        help="Processing steps (e.g. find_spots,index,integrate).",
    )
    parser.add_argument("--max_lattices", type=int, help="Maximum number of lattices to search for.")
    parser.add_argument("--min_spots", type=int, help="Minimum number of spots for indexing.")

    # --- Tracking and Metadata Parameters ---
    parser.add_argument(
        "--sampleName", help="Sample name for tracking and output files."
    )
    parser.add_argument(
        "--username", default=os.getenv("USER"), help="Username for job attribution."
    )
    # START: Added arguments for database logging
    parser.add_argument(
        "--beamline", default=get_beamline_from_hostname(), help="Beamline name."
    )
    parser.add_argument("--primary_group", type=str, help="Primary group for ESAF.")
    parser.add_argument("--run_prefix", type=str, help="Run prefix for linking to DatasetRun.")
    parser.add_argument("--pi_id", type=int, help="Badge number of the PI.")
    parser.add_argument("--esaf_id", type=int, help="ESAF number for the experiment.")
    # END: Added arguments

    args = parser.parse_args()

    # --- Prepare arguments for runners ---
    datasets = {}
    for data_string in args.data:
        parts = data_string.split(":")
        path = parts[0]
        if len(parts) == 1:
            datasets[path] = []  # standardized empty list for all frames
        elif len(parts) == 3:
            # We convert [start, end] to a full list of frames for get_image_set_string
            # or we could adjust the utility to handle [start, end]
            # but standardizing on list[int] is safer.
            datasets[path] = list(range(int(parts[1]), int(parts[2]) + 1))
        else:
            logging.error(f"Invalid format for --data argument: {data_string}")
            sys.exit(1)

    kwargs = vars(args).copy()
    sample_name = args.sampleName or os.path.basename(list(datasets.keys())[0]).replace(
        "_master.h5", ""
    ).replace(".h5", "")
    if not args.sampleName:
        logging.info(f"No --sampleName provided. Using default: {sample_name}")

    pipeline_params = {
        "sampleName": sample_name,
        "username": args.username,
        "imagedir": os.path.dirname(list(datasets.keys())[0]),
        "imageSet": get_image_set_string(datasets),
        "datasets": json.dumps(list(datasets.keys())),
        "beamline": args.beamline,
        "primary_group": args.primary_group,
        "run_prefix": args.run_prefix,
        "pi_id": args.pi_id,
        "esaf_id": args.esaf_id,
    }
    kwargs.pop("pipeline")
    kwargs.pop("data")
    kwargs.pop("work_dir")
    # START: Remove new args from kwargs passed to runners
    kwargs.pop("beamline", None)
    kwargs.pop("primary_group", None)
    kwargs.pop("run_prefix", None)
    kwargs.pop("pi_id", None)
    kwargs.pop("esaf_id", None)
    # END: Remove new args

    # --- Select and run the primary pipeline ---
    runner = None
    try:
        if args.pipeline == "autoPROC":
            runner = AutoPROCRunner(
                datasets, args.work_dir, pipeline_params=pipeline_params, **kwargs
            )
        elif args.pipeline == "xia2_ssx":
            runner = Xia2SSXRunner(
                datasets, args.work_dir, pipeline_params=pipeline_params, **kwargs
            )
        elif args.pipeline.startswith("xia2"):
            runner = Xia2Runner(
                datasets, args.work_dir, pipeline_params=pipeline_params, **kwargs
            )
        else:
            raise ValueError(f"Unknown pipeline specified: {args.pipeline}")

        logging.info(f"Starting pipeline '{args.pipeline}' in '{args.work_dir}'...")
        results = runner.run()

    except Exception as e:
        logging.critical(
            f"A critical error occurred during primary processing: {e}", exc_info=True
        )
        sys.exit(1)

    # --- Post-processing after a successful run ---
    if not results:
        logging.error(
            "Primary processing did not yield any results. Skipping post-processing."
        )
        sys.exit(0)

    post_proc_results = {}
    model_for_dimple = args.model
    if model_for_dimple:
        post_proc_results["user_model_pdb_path"] = model_for_dimple
        logging.info(f"User provided model for Dimple: {model_for_dimple}")

    try:
        # Step 3: Cell Search (always run)
        logging.info("--- Starting Post-Processing: Cell Search ---")
        rcsb = RCSB(default_directory=args.work_dir)
        unit_cell = results.get("unitcell")
        space_group = results.get("spacegroup")

        if unit_cell and space_group:
            found_pdb_file = rcsb.search_with_unit_cell_and_spg(unit_cell, space_group)
            if found_pdb_file:
                pdb_id = os.path.basename(found_pdb_file).replace(".pdb", "")
                post_proc_results["model_pdb_id_from_cell_search"] = pdb_id
                post_proc_results["model_pdb_path_from_cell_search"] = found_pdb_file
                if not model_for_dimple:  # Use this model if user didn't provide one
                    model_for_dimple = found_pdb_file
                    logging.info(f"Using model from cell search for Dimple: {pdb_id}")
            else:
                logging.warning("Cell search did not find a suitable model.")
        else:
            logging.warning("Cannot run cell search: unit cell or space group missing.")

        # Step 4: Run Dimple if a model is available
        if model_for_dimple:
            logging.info(
                f"--- Starting Post-Processing: Dimple with model {model_for_dimple} ---"
            )
            mtz_file = results.get("truncate_mtz")
            if not mtz_file or not os.path.exists(mtz_file):
                raise FileNotFoundError(
                    "Final MTZ file from processing is required for Dimple but was not found."
                )

            # Use the simplified DimpleRunner
            dimple_runner = DimpleRunner(
                mtz_file=mtz_file,
                pdb_file=model_for_dimple,
                work_dir=args.work_dir,
                **kwargs,
            )
            dimple_results = dimple_runner.run()
            post_proc_results.update(dimple_results)
        else:
            logging.info("No model provided or found. Skipping Dimple.")

    except Exception as e:
        logging.error(f"An error occurred during post-processing: {e}", exc_info=True)
        # Update DB to show Post-processing failed
        if runner and runner.tracker:
            runner.tracker.update_progress("POSTPROC_FAILED", {"error": str(e)})

    finally:
        # Step 5: Update database with any post-processing results found
        if post_proc_results and runner and runner.tracker:
            update_db_with_postprocessing(runner.tracker, post_proc_results)

        if runner and runner.results and post_proc_results:
            runner.results.update(post_proc_results)
            json_path = runner.results.get("json_summary")
            if json_path and os.path.exists(os.path.dirname(json_path)):
                with open(json_path, "w") as f:
                    json.dump(runner.results, f, indent=4, default=str)
                logger.info(
                    f"Updated local results summary with post-processing data: {json_path}"
                )

    logging.info("Pipeline and post-processing execution finished.")


if __name__ == "__main__":
    main()
