# --- START OF FILE data_services.py ---

import json
from datetime import datetime
from typing import List, Dict

from sqlalchemy.exc import IntegrityError, SQLAlchemyError

# Import the ORM model and the low-level DBManager
from qp2.data_viewer.models import DatasetRun
from qp2.log.logging_config import get_logger
from qp2.xio.db_manager import DBManager

logger = get_logger(__name__)


class RunCreationError(Exception):
    """Custom exception for errors during run creation."""

    pass


def create_dataset_run(
    db_manager: DBManager,
    run_prefix: str,
    total_frames: int,
    master_files: List[str],
    metadata: List[Dict],
    mounted: str = None,
    meta_user: str = None,
) -> DatasetRun:
    """
    Creates and saves a new dataset run record using a provided DBManager.

    This function contains the business logic for:
    1. Parsing raw metadata.
    2. Creating a DatasetRun ORM object.
    3. Using the DBManager to save it.

    Args:
        db_manager: An initialized instance of the DBManager.
        run_prefix: The unique prefix for the run.
        total_frames: Total number of frames in the dataset.
        master_files: List of master file paths.
        metadata: The raw metadata from the data collection headers.
        mounted: Optional mounted crystal identifier (overrides metadata).
        meta_user: Optional JSON string of the spreadsheet row.

    Returns:
        The created DatasetRun object if successful.

    Raises:
        RunCreationError: If there's an issue with metadata or saving.
        IntegrityError: If the run already exists (can be caught by the caller).
    """
    logger.info(f"Service: Creating dataset run for prefix: '{run_prefix}'")

    # 1. Business Logic: Process and validate raw inputs
    try:
        header0 = metadata[0] if metadata else {}
        username = header0.get("username")
        collect_type = header0.get("collect_mode")
        
        if not mounted:
            mounted = header0.get("robot_mounted") or header0.get("mounted")
            
        master_files_json = json.dumps(master_files)
        metadata_headers_json = json.dumps(metadata)
    except (IndexError, TypeError, json.JSONDecodeError) as e:
        msg = f"Failed to process metadata for run '{run_prefix}': {e}"
        logger.error(msg)
        raise RunCreationError(msg) from e

    # 2. Create the ORM object
    new_run_entry = DatasetRun(
        username=username,
        run_prefix=run_prefix,
        collect_type=collect_type,
        master_files=master_files_json,
        total_frames=total_frames,
        headers=metadata_headers_json,
        mounted=mounted,
        meta_user=meta_user,
        created_at=datetime.now(),
    )

    # 3. Use the DBManager to persist the object
    try:
        with db_manager.get_session() as session:
            session.add(new_run_entry)
            # The session commit is handled by the context manager.
            # We need to expire and refresh to get the DB-generated ID.
            session.flush()  # Flushes to DB to get ID, but doesn't commit yet.
            session.refresh(new_run_entry)
            logger.info(f"Successfully saved run '{run_prefix}' with ID: {new_run_entry.data_id}")
            session.expunge(new_run_entry)
            
        return new_run_entry

    except IntegrityError:
        # This is a specific, expected error. Re-raise it so the caller can handle it.
        logger.warning(
            f"Run '{run_prefix}' already exists in the database (IntegrityError)."
        )
        raise
    except SQLAlchemyError as e:
        # This is an unexpected database error.
        msg = f"A database error occurred while saving run '{run_prefix}': {e}"
        logger.error(msg)
        raise RunCreationError(msg) from e


def update_dataset_run(
    db_manager: DBManager,
    run_prefix: str,
    total_frames: int,
    master_files: List[str],
    metadata: List[Dict],
    mounted: str = None,
    meta_user: str = None,
) -> DatasetRun:
    """
    Updates an existing dataset run record.
    """
    logger.info(f"Service: Updating dataset run for prefix: '{run_prefix}'")

    try:
        header0 = metadata[0] if metadata else {}
        collect_type = header0.get("collect_mode")
        if not mounted:
            mounted = header0.get("robot_mounted") or header0.get("mounted")
        master_files_json = json.dumps(master_files)
        metadata_headers_json = json.dumps(metadata)
    except (IndexError, TypeError, json.JSONDecodeError) as e:
        msg = f"Failed to process metadata for update of run '{run_prefix}': {e}"
        logger.error(msg)
        raise RunCreationError(msg) from e

    try:
        with db_manager.get_session() as session:
            # Sort by created_at desc to get the latest run with this prefix
            run_entry = (
                session.query(DatasetRun)
                .filter_by(run_prefix=run_prefix)
                .order_by(DatasetRun.created_at.desc())
                .first()
            )
            
            if run_entry:
                # Defensive guard: never overwrite a longer file list with a shorter one.
                # This prevents race conditions from regressing the series count.
                existing_files = json.loads(run_entry.master_files or "[]")
                if len(master_files) < len(existing_files):
                    logger.warning(
                        f"Skipping update for '{run_prefix}': new list has {len(master_files)} files "
                        f"but DB already has {len(existing_files)}. Possible race condition."
                    )
                    return run_entry
                
                run_entry.total_frames = total_frames
                run_entry.master_files = master_files_json
                run_entry.headers = metadata_headers_json
                if collect_type:
                    run_entry.collect_type = collect_type
                if mounted:
                    run_entry.mounted = mounted
                if meta_user:
                    run_entry.meta_user = meta_user
                
                session.flush()
                logger.info(f"Successfully updated run '{run_prefix}' (ID: {run_entry.data_id})")
                return run_entry
            else:
                logger.warning(f"Run '{run_prefix}' not found for update.")
                return None
    except SQLAlchemyError as e:
        msg = f"A database error occurred while updating run '{run_prefix}': {e}"
        logger.error(msg)
        raise RunCreationError(msg) from e
