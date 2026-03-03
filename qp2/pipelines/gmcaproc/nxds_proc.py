#!/usr/bin/env python
# qp2/gmcaproc/run_proc.py
import argparse
import os
import sys
from multiprocessing import Pool, cpu_count
from pathlib import Path


# --- Add project root to sys.path for library imports ---
def find_project_root(file_path):
    path = Path(file_path).resolve()
    for parent in path.parents:
        if (parent / "qp2").is_dir():
            return str(parent)
    return None


project_root = find_project_root(__file__)
if project_root and project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now we can import the necessary classes
from qp2.pipelines.gmcaproc.xds2 import nXDS
from qp2.pipelines.gmcaproc.hdf5reader import HDF5Reader
from qp2.pipelines.gmcaproc.cbfreader import CbfReader
from qp2.log.logging_config import setup_logging, get_logger

# Setup logging for the command-line tool
setup_logging(log_file="nxds_run_proc.log")
logger = get_logger(__name__)


def find_master_files(start_path: Path, recursive: bool) -> list:
    """Finds all HDF5 master files or CBF templates in a given path."""
    master_files = []
    if not start_path.exists():
        logger.error(f"Provided data path does not exist: {start_path}")
        return []

    if start_path.is_file():
        if "_master.h5" in start_path.name or start_path.name.endswith(".cbf"):
            return [start_path]
        else:
            logger.warning(
                f"File is not a recognized master file/template: {start_path}"
            )
            return []

    # If it's a directory, search for files
    if recursive:
        glob_pattern = "**/*_master.h5"
        cbf_pattern = "**/*.cbf"
    else:
        glob_pattern = "*_master.h5"
        cbf_pattern = "*.cbf"

    master_files.extend(list(start_path.glob(glob_pattern)))
    # For CBF, we need to find unique templates, not every file
    cbf_files = list(start_path.glob(cbf_pattern))
    cbf_templates = set()
    for cbf in cbf_files:
        # A simple template is the filename with numbers replaced by '?'
        template = Path(cbf.parent) / cbf.name.replace(f"{cbf.stem[-4:]}", "????")
        cbf_templates.add(template)
    master_files.extend(list(cbf_templates))

    return master_files


def run_nxds_for_dataset(args_tuple):
    """
    Wrapper function to be called by the multiprocessing Pool.
    It takes a tuple of arguments to allow for easy mapping.
    """
    master_file, cli_args = args_tuple
    logger.info(f"--- Starting nXDS processing for: {master_file} ---")

    try:
        if str(master_file).endswith((".h5", ".hdf5")):
            dataset_reader = HDF5Reader(str(master_file))
        elif str(master_file).endswith(".cbf"):
            dataset_reader = CbfReader(str(master_file))
        else:
            logger.error(f"Unsupported file type: {master_file}")
            return

        # Create a unique processing directory for this dataset
        master_basename = os.path.splitext(os.path.basename(master_file))[0]
        proc_dir = Path(cli_args.proc_dir_root) / master_basename
        proc_dir.mkdir(parents=True, exist_ok=True)

        # Instantiate and run the nXDS class
        nxds_proc = nXDS(
            dataset=dataset_reader,
            proc_dir=str(proc_dir),
            nproc=cli_args.nproc,
            njobs=cli_args.njobs,
            user_space_group=cli_args.symm,
            user_unit_cell=cli_args.unitcell,
            reference_hkl=cli_args.reference_dataset,
            powder=cli_args.powder,
            # In CLI mode, we assume jobs are submitted to a cluster if available
            use_slurm=True,
            extra_xds_inp_params=cli_args.extra_xds_params,
        )
        nxds_proc.process()
        logger.info(f"--- Finished nXDS processing for: {master_file} ---")

    except Exception as e:
        logger.error(f"!!! FAILED nXDS processing for: {master_file} !!!")
        logger.error(f"Error: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(
        description="Command-line runner for nXDS processing pipelines.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--data",
        required=True,
        help="Path to a single master file/template or a directory containing datasets.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="If --data is a directory, search recursively for datasets.",
    )
    parser.add_argument(
        "--proc_dir_root",
        default="./nxds_cli_runs",
        help="The root directory where processing subdirectories will be created. Default: ./nxds_cli_runs",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=cpu_count(),
        help=f"Number of datasets to process in parallel. Default: Number of CPU cores ({cpu_count()})",
    )

    # --- nXDS Specific Parameters ---
    nxds_group = parser.add_argument_group("nXDS Processing Parameters")
    nxds_group.add_argument(
        "--symm", help="Space group for indexing (e.g., 'P43212' or 96)."
    )
    nxds_group.add_argument(
        "--unitcell", help="Unit cell for indexing (e.g., 'a,b,c,alpha,beta,gamma')."
    )
    nxds_group.add_argument(
        "--reference_dataset", help="Path to a reference HKL file for scaling."
    )
    nxds_group.add_argument(
        "--powder",
        action="store_true",
        help="Enable the POWDER step in nXDS for ice ring detection.",
    )
    nxds_group.add_argument(
        "--nproc",
        type=int,
        default=8,
        help="Number of processors per nXDS job (for XDS itself).",
    )
    nxds_group.add_argument(
        "--njobs",
        type=int,
        default=1,
        help="Number of nodes per nXDS job (for XDS itself).",
    )
    nxds_group.add_argument(
        "--xds_param",
        action="append",
        help="Additional XDS.INP parameters in KEY=VALUE format (can be used multiple times).",
    )

    args = parser.parse_args()

    # Parse xds_param into a dictionary
    extra_xds_params = {}
    if args.xds_param:
        for param in args.xds_param:
            if "=" in param:
                key, value = param.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key in extra_xds_params:
                    if isinstance(extra_xds_params[key], list):
                        extra_xds_params[key].append(value)
                    else:
                        extra_xds_params[key] = [extra_xds_params[key], value]
                else:
                    extra_xds_params[key] = value
            else:
                logger.warning(f"Ignoring invalid parameter format: {param}. Expected KEY=VALUE.")
    
    # Store in args for passing to worker
    args.extra_xds_params = extra_xds_params

    # 1. Discover all datasets
    start_path = Path(args.data).resolve()
    master_files = find_master_files(start_path, args.recursive)

    if not master_files:
        logger.error(f"No datasets found in '{args.data}'. Exiting.")
        sys.exit(1)

    logger.info(f"Found {len(master_files)} dataset(s) to process:")
    for mf in master_files:
        logger.info(f"  - {mf}")

    # 2. Prepare arguments for parallel processing
    # We need to pass both the specific master_file and the shared CLI args to each worker.
    tasks = [(mf, args) for mf in master_files]

    # 3. Run processing in parallel
    logger.info(
        f"Starting processing with up to {args.parallel} parallel job submissions..."
    )
    with Pool(processes=args.parallel) as pool:
        pool.map(run_nxds_for_dataset, tasks)

    logger.info("All processing tasks have been submitted.")


if __name__ == "__main__":
    main()
