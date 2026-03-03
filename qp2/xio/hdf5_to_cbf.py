import argparse
import glob
import os
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Tuple

try:
    from qp2.xio.hdf5_manager import HDF5Reader
except ImportError:
    from hdf5_manager import HDF5Reader

from qp2.log.logging_config import get_logger
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)


def convert_hdf5_to_cbf_for_strategy(
        run_prefix: str,
        master_files: List[str],
        metadata_list: List[Dict[str, Any]],
        processing_dir_path: Path,
) -> Tuple:
    """
    Converts HDF5 master files to CBF format when metadata mode is "strategy".

    Args:
        run_prefix: The run prefix for naming output files
        master_files: List of master HDF5 files to convert
        metadata_list: List of metadata dictionaries for each master file
        processing_dir_path: Directory to store converted files

    Returns:
        List of paths to the converted CBF files
    """
    # Check if we need to perform conversion
    if not master_files or not metadata_list:
        return (None, None)

    # Check if mode is strategy
    is_strategy_mode = False
    for meta in metadata_list:
        if meta.get("collect_mode", "").lower() == "strategy":
            is_strategy_mode = True
            break

    if not is_strategy_mode:
        return (None, None)

    # Create output directory for CBF files
    cbf_output_dir = processing_dir_path / "cbf_files"
    cbf_output_dir.mkdir(exist_ok=True)
    logger.info(f"Created CBF output directory: {cbf_output_dir}")

    # Initialize variables to track omega values
    first_omega_start = None
    omega_width = 0.2
    converted_cbf_files = []

    # Process each master file
    for idx, master_file in enumerate(master_files):
        logger.info(f"Processing master file: {master_file}")
        # Use HDF5Reader to get omega values
        try:
            reader = HDF5Reader(master_file)
            current_omega_start = reader.omega_start
            current_omega_width = reader.omega_range
            nimages = reader.nimages

            # Set first omega values if this is the first file
            if idx == 0:
                first_omega_start = current_omega_start
                omega_width = current_omega_width

            # Calculate starting sequence number for this file
            if first_omega_start is not None and omega_width > 0:
                start_seq = int(
                    (current_omega_start - first_omega_start) / omega_width + 1
                )
                if start_seq < 1:
                    start_seq = 1  # Ensure sequence starts at 1 if calculation gives lower value
            else:
                # Fallback if omega values aren't available
                start_seq = idx * 1000 + 1
            logger.info(f"Calculated starting sequence number: {start_seq}")

            # Run eiger2cbf to convert the master file
            conversion_cmd = [
                ProgramConfig.get_program_path("eiger2cbf"), master_file]
            try:
                logger.info(f"Running eiger2cbf command: {' '.join(conversion_cmd)}")
                subprocess.run(conversion_cmd, check=True, cwd=cbf_output_dir)

                # Get the original CBF files created by eiger2cbf
                master_basename = os.path.basename(master_file).replace(
                    "_master.h5", ""
                )
                original_cbf_pattern = os.path.join(
                    cbf_output_dir, f"{master_basename}_*.cbf"
                )
                original_cbf_files = sorted(glob.glob(original_cbf_pattern))
                logger.debug(f"Original CBF files found: {original_cbf_files}")

                # Rename files with proper sequence numbers
                for i, cbf_file in enumerate(original_cbf_files):
                    if i >= nimages:
                        break

                    seq_num = start_seq + i
                    new_cbf_name = f"{run_prefix}_{seq_num:06d}.cbf"
                    new_cbf_path = cbf_output_dir / new_cbf_name

                    # Rename file
                    logger.info(f"Renaming {cbf_file} to {new_cbf_path}")
                    os.rename(cbf_file, new_cbf_path)
                    # converted_cbf_files.append(str(new_cbf_path))
                    # filenames only, no path
                    converted_cbf_files.append(str(new_cbf_name))

            except subprocess.SubprocessError as e:
                logger.error(f"Error converting {master_file} to CBF: {e}")

            # Close the reader
            reader.close()

        except Exception as e:
            logger.error(f"Error processing master file {master_file}: {e}", exc_info=True)
            continue

    if converted_cbf_files:
        filelist = f"{cbf_output_dir}/filelist.txt"
        with open(filelist, "w") as fh:
            fh.write("\n".join(converted_cbf_files))
        logger.info(f"Created filelist: {filelist}")

        return cbf_output_dir, filelist
    else:
        return None, None


def get_common_prefix(strings: List[str]) -> str:
    """
    Finds the longest common prefix among a list of strings.
    """
    if not strings:
        return ""

    prefix = strings[0]
    if len(strings) == 1:
        return prefix.replace("_master.h5", "")
    for s in strings[1:]:
        i = 0
        while i < len(prefix) and i < len(s) and prefix[i] == s[i]:
            i += 1
        prefix = prefix[:i]
        if not prefix:
            return ""
    return prefix


def main():
    # ./image_viewer/bin/h5_to_cbf  --h5 /mnt/beegfs/DATA/user2/mb_may28/screen/strategy_test_2_run0_00_master.h5 /mnt/beegfs/DATA/user2/mb_may28/screen/strategy_test_2_run0_90_master.h5
    parser = argparse.ArgumentParser(
        description="Convert HDF5 master files to CBF format."
    )
    parser.add_argument(
        "--h5", nargs="+", help="List of HDF5 master files", required=True
    )
    parser.add_argument(
        "--run_prefix",
        type=str,
        help="Run prefix for naming output files. If not given, use common prefix of master files.",
        default=None,
    )
    parser.add_argument(
        "--processing_dir",
        type=str,
        help="Directory to store converted files. Defaults to current directory.",
        default=".",
    )

    args = parser.parse_args()

    master_files = args.h5
    run_prefix = args.run_prefix
    processing_dir = Path(args.processing_dir).resolve()

    if len(master_files) == 1:
        run_prefix = master_files[0].replace("_master.h5", "")

    if not run_prefix:
        run_prefix = get_common_prefix(
            [os.path.basename(f) for f in master_files])
        if not run_prefix:
            run_prefix = "cbfconvert"  # Default prefix if no common prefix found
        logger.info(f"Using common prefix '{run_prefix}' as run prefix.")

    # Minimal metadata list to trigger "strategy" mode
    metadata_list = [{"collect_mode": "strategy"}] * len(master_files)

    cbf_out_dir, filelist = convert_hdf5_to_cbf_for_strategy(
        run_prefix=run_prefix,
        master_files=master_files,
        metadata_list=metadata_list,
        processing_dir_path=processing_dir,
    )

    logger.info(f"CBF output directory: {cbf_out_dir}")
    logger.info(f"File list: {filelist}")


if __name__ == "__main__":
    main()
