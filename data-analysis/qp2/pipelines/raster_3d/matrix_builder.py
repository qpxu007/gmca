# qp2/pipelines/raster_3d/matrix_builder.py

"""Scan-mode-aware 2D data matrix builder for 3D raster reconstruction.

Adapted from ``VolumeDataWorker._fetch_and_build_matrix()``
(qp2/image_viewer/volume_map/volume_data_worker.py:36-116) to work with
raw master file paths and a Redis connection instead of QRunnable dataset
objects.
"""

import json
import re
from typing import Dict, List, Tuple

import numpy as np
import redis

from qp2.xio.hdf5_manager import HDF5Reader
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

# Ordered so _RX/_CX are tested before _R/_C (avoid partial match)
_SCAN_IDX_PATTERNS = [
    re.compile(r"_RX(\d+)", re.IGNORECASE),
    re.compile(r"_CX(\d+)", re.IGNORECASE),
    re.compile(r"_R(\d+)", re.IGNORECASE),
    re.compile(r"_C(\d+)", re.IGNORECASE),
]


def _detect_scan_idx_pattern(master_files: List[str]) -> re.Pattern:
    """Auto-detect the scan-index regex from filenames."""
    for pat in _SCAN_IDX_PATTERNS:
        if any(pat.search(f) for f in master_files):
            return pat
    raise ValueError(
        "Could not detect scan index pattern (_R, _RX, _C, _CX) "
        f"in filenames: {master_files[:3]}"
    )


def sort_master_files_numeric(master_files: List[str]) -> List[str]:
    """Sort master files by numeric scan index (R/C number), not lexically.

    ``_R2`` before ``_R10``, ``_C1`` before ``_C12``, etc.
    """
    pat = _detect_scan_idx_pattern(master_files)

    def sort_key(path: str):
        m = pat.search(path)
        return int(m.group(1)) if m else 0

    return sorted(master_files, key=sort_key)


def find_master_files(data_dir: str, run_prefix: str) -> List[str]:
    """Find and numerically sort master files for a run prefix.

    Tries ``_R`` pattern first, then ``_RX``, ``_C``, ``_CX``.
    Returns files sorted by scan-line number (numeric, not lexical).
    """
    import glob as _glob
    import os as _os

    for tag in ["_R", "_RX", "_C", "_CX"]:
        pattern = _os.path.join(data_dir, f"{run_prefix}{tag}*_master.h5")
        files = _glob.glob(pattern)
        if files:
            return sort_master_files_numeric(files)

    logger.warning(
        f"No master files found for prefix '{run_prefix}' in '{data_dir}'"
    )
    return []


def build_scan_aware_matrix(
    master_files: List[str],
    redis_conn: redis.Redis,
    scan_mode: str,
    source_cfg: Dict[str, str],
) -> Tuple[np.ndarray, Dict[Tuple[int, int], Dict], int]:
    """Build a 2D data matrix from Redis analysis results.

    Parameters
    ----------
    master_files : list of str
        Master HDF5 file paths for one run (e.g., all _R files).
    redis_conn : redis.Redis
        Redis connection with analysis results.
    scan_mode : str
        One of: row_wise, row_wise_serpentine, column_wise,
        column_wise_serpentine.
    source_cfg : dict
        Analysis source config with keys:
        ``redis_key_template``, ``x_axis_key``, ``metric``.

    Returns
    -------
    matrix : np.ndarray
        2D array of shape ``(num_scan_lines, num_frames_per_line)``,
        normalised for volume reconstruction (column scans are transposed).
        Uses compact indexing — scan indices are shifted so the first scan
        line is row 0 regardless of the file numbering (e.g., _R10 → row 0).
    raw_data_map : dict
        ``{(scan_idx, original_frame_idx): frame_dict}`` for coordinate
        mapping back to master files.  ``scan_idx`` here is the *absolute*
        0-based index from the filename (not the compact matrix row).
    """
    if not master_files:
        return np.array([]), {}, 0

    is_serpentine = "serpentine" in scan_mode
    is_column_scan = "column" in scan_mode

    key_template = source_cfg["redis_key_template"]
    x_axis_key = source_cfg["x_axis_key"]
    metric = source_cfg["metric"]

    pattern = _detect_scan_idx_pattern(master_files)

    # --- Build scan map: scan_idx → {path, total_frames} ---
    max_frames = 0
    scan_map: Dict[int, Dict] = {}

    for mf in master_files:
        match = pattern.search(mf)
        if not match:
            logger.warning(f"Cannot parse scan index from '{mf}', skipping.")
            continue
        idx = int(match.group(1)) - 1  # 0-based
        try:
            reader = HDF5Reader(mf, start_timer=False)
            total_frames = reader.total_frames
            reader.close()
        except Exception as e:
            logger.error(f"Failed to read HDF5 {mf}: {e}")
            continue

        max_frames = max(max_frames, total_frames)
        scan_map[idx] = {"path": mf, "total_frames": total_frames}

    if not scan_map:
        logger.error("No valid master files could be parsed.")
        return np.array([]), {}, 0

    # Compact indexing: shift so first scan line = row 0
    min_scan_idx = min(scan_map.keys())
    num_scan_lines = max(scan_map.keys()) - min_scan_idx + 1

    # --- Allocate grid ---
    if is_column_scan:
        grid_shape = (max_frames, num_scan_lines)
    else:
        grid_shape = (num_scan_lines, max_frames)

    matrix = np.full(grid_shape, np.nan)
    raw_data_map: Dict[Tuple[int, int], Dict] = {}

    # --- Populate from Redis ---
    for scan_idx, info in scan_map.items():
        compact_idx = scan_idx - min_scan_idx

        redis_key = key_template.format(master_file=info["path"])
        try:
            redis_results = redis_conn.hgetall(redis_key)
        except redis.RedisError as e:
            logger.error(f"Redis error for key {redis_key}: {e}")
            continue

        if not redis_results:
            logger.warning(f"No data in Redis for {redis_key}")
            continue

        for frame_json in redis_results.values():
            frame = json.loads(frame_json)
            original_frame_idx = frame.get(x_axis_key)

            if original_frame_idx is None or metric not in frame:
                continue

            original_frame_idx -= 1  # 0-based

            # Serpentine correction: reverse odd scan lines
            final_frame_idx = original_frame_idx
            if is_serpentine and compact_idx % 2 == 1:
                final_frame_idx = (info["total_frames"] - 1) - original_frame_idx

            # Place in matrix using compact index
            if is_column_scan:
                row, col = final_frame_idx, compact_idx
            else:
                row, col = compact_idx, final_frame_idx

            if 0 <= row < grid_shape[0] and 0 <= col < grid_shape[1]:
                matrix[row, col] = frame[metric]
                # raw_data_map uses absolute scan_idx for back-mapping
                raw_data_map[(scan_idx, original_frame_idx)] = frame

    # Transpose column scans to normalise to (scan_lines, frames)
    if is_column_scan:
        matrix = matrix.T

    # --- Detect and interpolate missing rows/columns ---
    # After transpose, matrix is (num_scan_lines, num_frames).
    # A missing row = entire scan line with no data (all NaN).
    missing_rows = []
    for r in range(matrix.shape[0]):
        if np.all(np.isnan(matrix[r, :])):
            missing_rows.append(r)

    if missing_rows:
        logger.warning(
            f"Missing scan lines: {len(missing_rows)}/{matrix.shape[0]} "
            f"(rows {missing_rows}). Interpolating from neighbors."
        )
        matrix = _interpolate_missing_rows(matrix, missing_rows)

    filled = np.count_nonzero(~np.isnan(matrix))
    total = matrix.size
    logger.info(
        f"Matrix built: shape={matrix.shape}, "
        f"filled={filled}/{total} ({100 * filled / max(total, 1):.0f}%), "
        f"scan_idx_offset={min_scan_idx}"
        + (f", interpolated {len(missing_rows)} missing row(s)" if missing_rows else "")
    )
    return matrix, raw_data_map, min_scan_idx


def _interpolate_missing_rows(
    matrix: np.ndarray, missing_rows: List[int]
) -> np.ndarray:
    """Fill all-NaN rows by linear interpolation from nearest valid neighbors.

    Edge rows with no neighbor on one side are filled with the nearest
    valid row (nearest-neighbor extrapolation).
    """
    nrows = matrix.shape[0]
    missing_set = set(missing_rows)

    for r in missing_rows:
        # Find nearest valid row above
        above = None
        for a in range(r - 1, -1, -1):
            if a not in missing_set:
                above = a
                break
        # Find nearest valid row below
        below = None
        for b in range(r + 1, nrows):
            if b not in missing_set:
                below = b
                break

        if above is not None and below is not None:
            # Linear interpolation
            weight = (r - above) / (below - above)
            matrix[r, :] = (
                matrix[above, :] * (1 - weight) + matrix[below, :] * weight
            )
        elif above is not None:
            matrix[r, :] = matrix[above, :]
        elif below is not None:
            matrix[r, :] = matrix[below, :]
        # else: both None = all rows missing, leave as NaN

    return matrix
