# qp2/image_viewer/utils/settings_utils.py

from pathlib import Path
from qp2.log.logging_config import get_logger
from qp2.utils.auxillary import sanitize_space_group

logger = get_logger(__name__)

def populate_settings_from_redis(redis_conn, master_file, settings_prefix, current_settings):
    """
    Fetches stored crystallographic data from Redis and updates the settings dictionary.

    :param redis_conn: An active Redis connection object.
    :param master_file: The full path to the master file of the dataset.
    :param settings_prefix: The prefix for the settings keys (e.g., 'xds_').
    :param current_settings: The settings dictionary to update.
    :return: The updated settings dictionary.
    """
    if not redis_conn or not master_file:
        return current_settings

    try:
        redis_key = f"dataset:info:{master_file}"
        stored_data = redis_conn.hgetall(redis_key)

        if stored_data:
            updated_settings = current_settings.copy()
            # Update current_settings with the fetched data
            if "space_group" in stored_data:
                updated_settings[f"{settings_prefix}space_group"] = sanitize_space_group(stored_data["space_group"]) or ""
            if "unit_cell" in stored_data:
                updated_settings[f"{settings_prefix}unit_cell"] = stored_data["unit_cell"]
            if "model_pdb" in stored_data:
                updated_settings[f"{settings_prefix}model_pdb"] = stored_data["model_pdb"]
                # Add a generic 'model' key for pipelines like xia2/autoPROC
                updated_settings[f"{settings_prefix}model"] = stored_data["model_pdb"]
            if "reference_hkl" in stored_data:
                updated_settings[f"{settings_prefix}reference_hkl"] = stored_data["reference_hkl"]

            logger.info(f"Pre-populated settings with data from Redis key '{redis_key}'.")
            return updated_settings

    except Exception as e:
        logger.warning(f"Could not fetch crystal data from Redis for {Path(master_file).name}: {e}")

    return current_settings
