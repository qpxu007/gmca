# qp2/pipelines/raster_3d/scan_mode.py

"""Raster scan mode detection with 3-level fallback.

Scan modes:
    row_wise              — rows scanned left-to-right, top-to-bottom
    row_wise_serpentine   — alternating row direction
    column_wise           — columns scanned top-to-bottom, left-to-right
    column_wise_serpentine — alternating column direction
"""

import re
from typing import Dict, List, Optional

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

VALID_SCAN_MODES = {
    "row_wise",
    "column_wise",
    "row_wise_serpentine",
    "column_wise_serpentine",
}

# Filename suffix → scan mode mapping
_FILENAME_PATTERNS = [
    (re.compile(r"_RX\d+", re.IGNORECASE), "row_wise_serpentine"),
    (re.compile(r"_CX\d+", re.IGNORECASE), "column_wise_serpentine"),
    (re.compile(r"_R\d+", re.IGNORECASE), "row_wise"),
    (re.compile(r"_C\d+", re.IGNORECASE), "column_wise"),
]


def detect_raster_scan_mode(
    metadata: Dict,
    master_files: List[str],
    redis_manager=None,
    run_prefix: str = "",
) -> str:
    """Detect raster scan mode using a 3-level fallback.

    Priority:
        1. HDF5/collection metadata (``scan_mode`` or ``raster_mode`` field)
        2. Bluice Redis — ``bluice:run:r#{run_idx}$vertical`` and
           ``bluice:run:r#{run_idx}$serpentine`` where run_idx is the
           trailing number in ``run_prefix`` (e.g. 6 from ``Q3_ras_run6``)
        3. Filename pattern inference (_R, _RX, _C, _CX)

    Falls back to ``"row_wise"`` if nothing else matches.

    Parameters
    ----------
    metadata : dict
        Run metadata dict (from redis_manager._extract_metadata).
    master_files : list of str
        Master file paths for the run.
    redis_manager : optional
        Server's RedisManager instance (for bluice connection fallback).
    run_prefix : str, optional
        Run prefix (e.g. ``"Q3_ras_run6"``).  The trailing number is used
        as the bluice run index.

    Returns
    -------
    str
        One of the VALID_SCAN_MODES values.
    """
    # --- Priority 1: metadata ---
    mode = _from_metadata(metadata)
    if mode:
        logger.info(f"Scan mode from metadata: {mode}")
        return mode

    # --- Priority 2: bluice Redis ---
    mode = _from_bluice_redis(redis_manager, run_prefix)
    if mode:
        logger.info(f"Scan mode from bluice Redis: {mode}")
        return mode

    # --- Priority 3: filename pattern ---
    mode = _from_filename(master_files)
    if mode:
        logger.info(f"Scan mode from filename pattern: {mode}")
        return mode

    logger.info("Scan mode: using default 'row_wise'")
    return "row_wise"


def _from_metadata(metadata: Dict) -> Optional[str]:
    """Check metadata for scan_mode / raster_mode field."""
    for key in ("scan_mode", "raster_mode", "raster_scan_mode"):
        value = metadata.get(key)
        if value and str(value).lower().replace("-", "_") in VALID_SCAN_MODES:
            return str(value).lower().replace("-", "_")
    return None


def _from_bluice_redis(redis_manager, run_prefix: str = "") -> Optional[str]:
    """Query bluice Redis for raster scan mode via RedisManager."""
    if redis_manager is None or not run_prefix:
        return None
    try:
        return redis_manager.get_raster_scan_mode(run_prefix)
    except Exception as e:
        logger.debug(f"Could not read scan mode from bluice Redis: {e}")
    return None


def resolve_auto_scan_mode(
    run_prefix: str,
    master_files: List[str],
    analysis_conn=None,
    group_name: str = "",
) -> str:
    """Resolve scan mode for the image viewer's "auto" setting.

    Fallback chain (no bluice Redis needed):

    1. Analysis Redis ``analysis:collection_params:{group}:{run_prefix}``
       — captured from bluice at collection time by the server.
    2. Analysis Redis wildcard ``analysis:collection_params:*:{run_prefix}``
       — if group is unknown.
    3. Filename pattern inference (_R, _RX, _C, _CX).
    4. Default ``"row_wise"``.

    Parameters
    ----------
    run_prefix : str
        Run prefix (e.g. ``"Q3_ras_run6"``).
    master_files : list of str
        Master file paths for filename pattern fallback.
    analysis_conn : redis.Redis, optional
        Analysis Redis connection.
    group_name : str, optional
        ESAF group / bluice username for scoped key lookup.
    """
    if analysis_conn and run_prefix:
        try:
            from qp2.config.redis_keys import AnalysisRedisKeys

            # Try exact key with group name
            if group_name:
                key = AnalysisRedisKeys.collection_params_key(
                    group_name, run_prefix
                )
                mode = analysis_conn.hget(key, "scan_mode")
                if mode:
                    decoded = mode if isinstance(mode, str) else mode.decode()
                    if decoded in VALID_SCAN_MODES:
                        logger.info(
                            f"Scan mode from analysis Redis ({key}): {decoded}"
                        )
                        return decoded

            # Wildcard fallback — scan for any group
            pattern = f"{AnalysisRedisKeys.COLLECTION_PARAMS}:*:{run_prefix}"
            for matched_key in analysis_conn.scan_iter(
                match=pattern, count=10
            ):
                k = matched_key if isinstance(matched_key, str) else matched_key.decode()
                mode = analysis_conn.hget(k, "scan_mode")
                if mode:
                    decoded = mode if isinstance(mode, str) else mode.decode()
                    if decoded in VALID_SCAN_MODES:
                        logger.info(
                            f"Scan mode from analysis Redis ({k}): {decoded}"
                        )
                        return decoded
                break  # only check first match
        except Exception as e:
            logger.debug(f"Could not resolve scan mode from analysis Redis: {e}")

    # Filename pattern fallback
    mode = _from_filename(master_files)
    if mode:
        logger.info(f"Scan mode auto-detect from filename: {mode}")
        return mode

    logger.info("Scan mode auto-detect: using default 'row_wise'")
    return "row_wise"


def _from_filename(master_files: List[str]) -> Optional[str]:
    """Infer scan mode from master file naming convention.

    _RX → row_wise_serpentine, _CX → column_wise_serpentine,
    _R  → row_wise,           _C  → column_wise.

    Order matters: check _RX/_CX before _R/_C to avoid false matches.
    """
    if not master_files:
        return None
    # Check first master file — all files in a run share the same pattern
    filename = master_files[0]
    for pattern, mode in _FILENAME_PATTERNS:
        if pattern.search(filename):
            return mode
    return None
