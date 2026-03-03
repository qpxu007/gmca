import os
import re
from pathlib import Path

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def determine_proc_base_dir(user_provided_path, data_path_str):
    """
    Determines the base root directory for processing outputs.
    Returns /mnt/beegfs/PROCESSING/esaf... if writable, otherwise ~/.
    """
    # 1. User Override
    if user_provided_path:
        logger.info(f"Using user-provided processing base root: {user_provided_path}")
        return Path(user_provided_path).resolve()

    # 2. Intelligent Default
    try:
        data_path = Path(data_path_str).resolve()

        # Search for esaf pattern, e.g., /mnt/beegfs/DATA/esaf281191/...
        esaf_match = re.search(r"(esaf\d+)", str(data_path))

        # Check if 'DATA' is in the path parts
        has_data_component = "DATA" in [p.upper() for p in data_path.parts]

        if esaf_match and has_data_component:
            esaf_id = esaf_match.group(1)

            # Find the part of the path up to and including the 'DATA' component
            path_parts = list(data_path.parts)
            try:
                data_index = [p.upper() for p in path_parts].index("DATA")
                base_path_parts = path_parts[:data_index]

                # Construct the potential processing path root
                processing_base = Path(*base_path_parts) / "PROCESSING" / esaf_id

                # Check permissions. We need to walk up until we find an existing dir.
                parent_to_check = processing_base
                while not parent_to_check.exists():
                    parent_to_check = parent_to_check.parent
                    if parent_to_check == parent_to_check.parent:  # Reached root
                        break

                if os.access(parent_to_check, os.W_OK):
                    logger.info(
                        f"Intelligently determined processing base root: {processing_base}"
                    )
                    return processing_base
                else:
                    logger.warning(
                        f"No write permission for potential processing base root {parent_to_check}. Using fallback."
                    )
            except ValueError:
                pass
    except Exception as e:
        logger.error(
            f"Error during intelligent path determination: {e}. Using fallback."
        )

    # 3. Simple Fallback (Home directory)
    fallback_path = Path(os.path.expanduser("~")).resolve()
    logger.info(f"Using fallback processing base root: {fallback_path}")
    return fallback_path


def determine_proc_dir_root(user_provided_path, data_path_str, pipeline_name):
    """
    Determines the root directory for processing outputs with intelligent defaults.
    Maintained for backward compatibility.
    """
    base = determine_proc_base_dir(user_provided_path, data_path_str)
    return base / f"{pipeline_name}_cli_runs"


def extract_master_prefix(master_file_path: str) -> str:
    """
    Extracts the dataset prefix from a master file name by removing common extensions.
    e.g. sample_master.h5 -> sample
         sample.nxs -> sample
    """
    filename = Path(master_file_path).name
    if filename.endswith("_master.h5"):
        return filename[:-10]
    elif filename.endswith("_master.hdf5"):
        return filename[:-12]
    elif filename.endswith(".nxs"):
        prefix = filename[:-4]
        if prefix.endswith("_master"):
            return prefix[:-7]
        return prefix
    elif filename.endswith(".h5"):
        prefix = filename[:-3]
        if prefix.endswith("_master"):
            return prefix[:-7]
        return prefix
    elif filename.endswith(".hdf5"):
        prefix = filename[:-5]
        if prefix.endswith("_master"):
            return prefix[:-7]
        return prefix
    else:
        # Fallback to legacy behavior for unmatched cases
        return filename.replace("_master.h5", "")
