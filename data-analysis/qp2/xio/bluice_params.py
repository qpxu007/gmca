# qp2/xio/bluice_params.py

"""Bluice Redis parameter query helpers.

All pybluice Redis key lookups are centralised here so that key formats,
decode logic, and fallback order are defined in one place.  ``RedisManager``
delegates to these functions; callers should use ``RedisManager`` methods
rather than importing this module directly.

Pybluice key conventions
------------------------
Per-run raster config : ``bluice:run:r#{run_idx}``  (hash)
Per-run beam/collect  : ``bluice:run:b#{run_idx}``  (hash)
Beamline state        : ``bluice:sampleenv``         (hash)
Attenuation device    : ``bluice:attenuation``       (hash)

``run_idx`` is the trailing number in the run prefix
(e.g. ``"Q3_ras_run6"`` → 6).

**Storage format**: pybluice uses Redis hashes for all ``$``-separated keys.
``bluice:run:r#6$cell_w_um`` → ``HGET bluice:run:r#6 cell_w_um``.
"""

import re
from typing import Dict, Optional, Tuple

from qp2.config.redis_keys import BluiceRedisKeys
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


# ------------------------------------------------------------------
# Run-index extraction
# ------------------------------------------------------------------

def extract_run_index(run_prefix: str) -> Optional[int]:
    """Extract the trailing run number from a run prefix.

    ``"Q3_ras_run6"`` → ``6``, ``"B1_ras_run12"`` → ``12``.
    """
    match = re.search(r"(\d+)$", run_prefix)
    return int(match.group(1)) if match else None


# ------------------------------------------------------------------
# Helpers to safely read bluice Redis hash fields
# ------------------------------------------------------------------

def _hget(redis_conn, hash_key: str, field: str) -> Optional[str]:
    """HGET with decode, returns str or None."""
    val = redis_conn.hget(hash_key, field)
    if val is None:
        return None
    return val if isinstance(val, str) else val.decode()


def _hget_float(redis_conn, hash_key: str, field: str) -> Optional[float]:
    """HGET and convert to float, or None."""
    s = _hget(redis_conn, hash_key, field)
    if s is None:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _hget_int(redis_conn, hash_key: str, field: str) -> Optional[int]:
    """HGET and convert to int, or None."""
    s = _hget(redis_conn, hash_key, field)
    if s is None:
        return None
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def _hget_dollar_key(redis_conn, key_with_dollar: str) -> Optional[str]:
    """Read a ``$``-separated bluice key via HGET.

    ``"bluice:robot$mounted"`` → ``hget("bluice:robot", "mounted")``.
    """
    hash_name, field = key_with_dollar.split("$", 1)
    return _hget(redis_conn, hash_name, field)


# ------------------------------------------------------------------
# Per-run raster queries
# ------------------------------------------------------------------

def get_raster_cell_size(
    redis_conn, run_prefix: str
) -> Optional[Tuple[float, float]]:
    """Read raster cell (step) size from bluice Redis.

    Returns ``(width_um, height_um)`` or ``None`` if unavailable.
    """
    run_idx = extract_run_index(run_prefix)
    if run_idx is None:
        return None
    try:
        h = BluiceRedisKeys.raster_run_hash(run_idx)
        w = _hget_float(redis_conn, h, BluiceRedisKeys.FIELD_CELL_W_UM)
        ht = _hget_float(redis_conn, h, BluiceRedisKeys.FIELD_CELL_H_UM)
        if w is not None and ht is not None:
            logger.info(f"Step size from bluice: {w} x {ht} um (run {run_idx})")
            return (w, ht)
    except Exception as e:
        logger.debug(f"Could not read cell size from bluice Redis: {e}")
    return None


def get_scan_mode(
    redis_conn, run_prefix: str
) -> Optional[str]:
    """Read scan mode flags from bluice Redis.

    Returns one of ``row_wise``, ``row_wise_serpentine``,
    ``column_wise``, ``column_wise_serpentine``, or ``None``.
    """
    run_idx = extract_run_index(run_prefix)
    if run_idx is None:
        return None
    try:
        h = BluiceRedisKeys.raster_run_hash(run_idx)
        vertical = _hget_int(redis_conn, h, BluiceRedisKeys.FIELD_VERTICAL)
        serpentine = _hget_int(redis_conn, h, BluiceRedisKeys.FIELD_SERPENTINE)
        if vertical is None and serpentine is None:
            return None
        v = vertical or 0
        s = serpentine or 0
        if v and s:
            return "column_wise_serpentine"
        elif v:
            return "column_wise"
        elif s:
            return "row_wise_serpentine"
        else:
            return "row_wise"
    except Exception as e:
        logger.debug(f"Could not read scan mode from bluice Redis: {e}")
    return None


def get_raster_grid_params(
    redis_conn, run_prefix: str
) -> Optional[Dict]:
    """Read raster grid geometry from bluice Redis.

    Returns dict with ``grid_ref`` (list of 4 floats: x,y,z,omega in mm/deg),
    ``act_bounds`` (list of 4 floats: x1,y1,x2,y2 in microns),
    ``rows``, ``cols``, or ``None`` if unavailable.
    """
    run_idx = extract_run_index(run_prefix)
    if run_idx is None:
        return None
    try:
        h = BluiceRedisKeys.raster_run_hash(run_idx)
        grid_ref_s = _hget(redis_conn, h, BluiceRedisKeys.FIELD_GRID_REF)
        act_bounds_s = _hget(redis_conn, h, BluiceRedisKeys.FIELD_ACT_BOUNDS)
        rows = _hget_int(redis_conn, h, BluiceRedisKeys.FIELD_ROWS)
        cols = _hget_int(redis_conn, h, BluiceRedisKeys.FIELD_COLS)
        if not grid_ref_s or not act_bounds_s or not rows or not cols:
            return None
        grid_ref = [float(v) for v in grid_ref_s.split(",") if v]
        act_bounds = [float(v) for v in act_bounds_s.split(",") if v]
        if len(grid_ref) < 4 or len(act_bounds) < 4:
            return None
        return {
            "grid_ref": grid_ref,
            "act_bounds": act_bounds,
            "rows": rows,
            "cols": cols,
        }
    except Exception as e:
        logger.debug(f"Could not read grid params from bluice Redis: {e}")
    return None


def get_beam_size(
    redis_conn, run_prefix: str
) -> Optional[Tuple[float, float]]:
    """Read beam (guard-slit) size from bluice Redis.

    Per-run fallback → beamline-level ``sampleenv``.
    Returns ``(width_um, height_um)`` or ``None``.
    """
    run_idx = extract_run_index(run_prefix)

    # Per-run guard slit size
    if run_idx is not None:
        try:
            h = BluiceRedisKeys.beam_run_hash(run_idx)
            gs_x = _hget_float(redis_conn, h, BluiceRedisKeys.FIELD_GS_X_UM)
            gs_y = _hget_float(redis_conn, h, BluiceRedisKeys.FIELD_GS_Y_UM)
            if gs_x is not None and gs_y is not None:
                logger.info(f"Beam size from bluice: {gs_x}x{gs_y} um (run {run_idx})")
                return (gs_x, gs_y)
        except Exception as e:
            logger.debug(f"Could not read per-run beam size: {e}")

    # Beamline-level fallback
    try:
        act = _hget(
            redis_conn,
            BluiceRedisKeys.SAMPLEENV_HASH,
            BluiceRedisKeys.FIELD_CUR_ACT_BEAMSIZE_UM,
        )
        if act:
            parts = act.split(",")
            if len(parts) >= 2:
                w, h = float(parts[0]), float(parts[1])
                logger.info(f"Beam size from bluice sampleenv: {w}x{h} um")
                return (w, h)
    except Exception as e:
        logger.debug(f"Could not read beamline beam size: {e}")

    return None


def get_attenuation(
    redis_conn, run_prefix: str
) -> Optional[float]:
    """Read attenuation factor from bluice Redis.

    Per-run fallback → beamline-level ``attenuation`` device.
    Returns attenuation factor (e.g. 10.0) or ``None``.
    """
    run_idx = extract_run_index(run_prefix)

    # Per-run attenuation
    if run_idx is not None:
        try:
            val = _hget_float(
                redis_conn,
                BluiceRedisKeys.beam_run_hash(run_idx),
                BluiceRedisKeys.FIELD_ATTEN_FACTORS,
            )
            if val is not None:
                logger.info(f"Attenuation from bluice: {val}x (run {run_idx})")
                return val
        except Exception as e:
            logger.debug(f"Could not read per-run attenuation: {e}")

    # Beamline-level fallback
    try:
        val = _hget_float(
            redis_conn,
            BluiceRedisKeys.ATTENUATION_HASH,
            BluiceRedisKeys.FIELD_ACT_POS_FACTORS,
        )
        if val is not None:
            logger.info(f"Attenuation from bluice state: {val}x")
            return val
    except Exception as e:
        logger.debug(f"Could not read beamline attenuation: {e}")

    return None


# ------------------------------------------------------------------
# Beamline state queries
# ------------------------------------------------------------------

def get_beamline_name(redis_conn) -> Optional[str]:
    """Read beamline name from bluice Redis."""
    try:
        return _hget_dollar_key(redis_conn, BluiceRedisKeys.KEY_BEAMLINE_NAME)
    except Exception as e:
        logger.debug(f"Could not read beamline name: {e}")
        return None


def get_beamline_user(redis_conn) -> Optional[str]:
    """Read current beamline user from bluice Redis."""
    try:
        return _hget_dollar_key(redis_conn, BluiceRedisKeys.KEY_USER)
    except Exception as e:
        logger.debug(f"Could not read beamline user: {e}")
        return None


def get_robot_mounted(redis_conn) -> Optional[str]:
    """Read robot mounted status from bluice Redis."""
    try:
        return _hget_dollar_key(redis_conn, BluiceRedisKeys.KEY_ROBOT_MOUNTED)
    except Exception as e:
        logger.debug(f"Could not read robot mounted: {e}")
        return None


def get_spreadsheet_rel(redis_conn) -> Optional[str]:
    """Read spreadsheet relative path from bluice Redis."""
    try:
        return _hget_dollar_key(redis_conn, BluiceRedisKeys.KEY_SPREADSHEET_INPUT_REL)
    except Exception as e:
        logger.debug(f"Could not read spreadsheet path: {e}")
        return None


# ------------------------------------------------------------------
# Camera calibration and paths
# ------------------------------------------------------------------

def get_camera_calibration(redis_conn) -> Optional[Tuple[float, float]]:
    """Read high-res camera mm/pixel calibration.

    Returns ``(mm_per_px_h, mm_per_px_v)`` or ``None``.
    """
    try:
        h = _hget_float(
            redis_conn,
            BluiceRedisKeys.CONFIG_HASH,
            BluiceRedisKeys.FIELD_MM_PER_PX_HR_H,
        )
        v = _hget_float(
            redis_conn,
            BluiceRedisKeys.CONFIG_HASH,
            BluiceRedisKeys.FIELD_MM_PER_PX_HR_V,
        )
        if h is not None and v is not None:
            return (h, v)
    except Exception as e:
        logger.debug(f"Could not read camera calibration: {e}")
    return None


def get_processing_dir(redis_conn) -> Optional[str]:
    """Read the current processing directory from bluice Redis."""
    try:
        return _hget(
            redis_conn,
            BluiceRedisKeys.PATHS_HASH,
            BluiceRedisKeys.FIELD_PROCESSING_DIR,
        )
    except Exception as e:
        logger.debug(f"Could not read processing dir: {e}")
        return None


def get_snapshot_prefix(redis_conn) -> Optional[str]:
    """Read the current snapshot prefix (sample/mount ID) from bluice Redis."""
    try:
        return _hget(
            redis_conn,
            BluiceRedisKeys.PATHS_HASH,
            BluiceRedisKeys.FIELD_PREFIX,
        )
    except Exception as e:
        logger.debug(f"Could not read snapshot prefix: {e}")
        return None


# ------------------------------------------------------------------
# Strategy publishing
# ------------------------------------------------------------------

def publish_strategy(redis_conn, redis_key: str, mapping: dict) -> bool:
    """Write strategy results to a bluice strategy table hash.

    Returns True on success, False on failure.
    """
    try:
        redis_conn.hset(redis_key, mapping=mapping)
        logger.info(f"Published strategy to {redis_key}")
        return True
    except Exception as e:
        logger.warning(f"Failed to publish strategy to {redis_key}: {e}")
        return False


def bump_strategy_version(redis_conn) -> None:
    """Increment the bluice strategy version counter."""
    try:
        redis_conn.incr(BluiceRedisKeys.STRATEGY_VERSION_KEY)
    except Exception as e:
        logger.warning(f"Failed to bump strategy version: {e}")


# ------------------------------------------------------------------
# Convenience: fetch all raster params in one call
# ------------------------------------------------------------------

def get_all_bluice_params(
    redis_conn, run_prefix: str
) -> Dict:
    """Query all relevant bluice Redis parameters for a run.

    Returns a dict with available values (absent keys = not found):

    - ``cell_w_um``, ``cell_h_um``  — raster step size
    - ``beam_size_x_um``, ``beam_size_y_um`` — beam (guard slit) size
    - ``raster_attenuation`` — attenuation factor
    - ``scan_mode`` — row_wise / column_wise / *_serpentine
    """
    params: Dict = {}

    cell = get_raster_cell_size(redis_conn, run_prefix)
    if cell:
        params["cell_w_um"] = cell[0]
        params["cell_h_um"] = cell[1]

    beam = get_beam_size(redis_conn, run_prefix)
    if beam:
        params["beam_size_x_um"] = beam[0]
        params["beam_size_y_um"] = beam[1]

    atten = get_attenuation(redis_conn, run_prefix)
    if atten is not None:
        params["raster_attenuation"] = atten

    mode = get_scan_mode(redis_conn, run_prefix)
    if mode:
        params["scan_mode"] = mode

    return params
