"""Server-side image rendering for crystallography diffraction data."""

import asyncio
import io
import json
import os
import glob
import threading
import time
import numpy as np
from functools import lru_cache
from typing import Optional

import logging
import redis as redis_lib
from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.orm import Session

# ── Colormap LUTs (pre-computed once at import — avoids per-frame matplotlib overhead) ──
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.cm as _cm
    from PIL import Image as _Image

    _SUPPORTED_COLORMAPS = ("plasma", "viridis", "gray", "hot", "inferno")
    _COLORMAP_LUTS: dict = {
        name: (_cm.get_cmap(name)(np.linspace(0, 1, 256))[:, :3] * 255).astype(np.uint8)
        for name in _SUPPORTED_COLORMAPS
    }
except Exception as _cmap_err:
    _COLORMAP_LUTS = {}
    _Image = None
    print(f"Colormap LUT init failed: {_cmap_err}")

logger = logging.getLogger(__name__)

try:
    from qp2.xio.hdf5_manager import HDF5Reader
    from qp2.image_viewer.utils.contrast_utils import calculate_contrast_levels
    from qp2.image_viewer.utils.pixel_utils import calculate_pixel_info
    from qp2.image_viewer.utils.ring_math import resolution_to_radius
    from qp2.db import DatasetRun
    from qp2.config.servers import ServerConfig
    from qp2.web_app.backend.auth import is_staff_member
    from qp2.web_app.backend.security import verify_token
except ImportError as e:
    print(f"Viewer import error: {e}")
    ServerConfig = None

# In-process ZMQ subscriber. Defaults off while the EPU PUB at :9900 is not
# supported — keeps idle subscriber threads from starting. The monitor-API
# fallback is owned by the EPU sidecar now, not this process.
_LIVE_ZMQ_FALLBACK_ENABLED = os.environ.get("LIVE_ZMQ_FALLBACK_ENABLED", "0") == "1"

if _LIVE_ZMQ_FALLBACK_ENABLED:
    try:
        from qp2.web_app.backend import live_zmq as _live_zmq
        _live_zmq.start()
    except Exception as _e:
        _live_zmq = None
        print(f"live_zmq not available: {_e}")
else:
    _live_zmq = None

# Viewer-presence key bumped by /live/latest and the SSE loop. The EPU
# sidecar's monitor-API producer reads this; TTL must outlast the SSE bump
# interval (~2 s) plus the producer's poll period (~1 s) with margin.
_VIEWER_PRESENT_KEY = 'live:viewer_present:{bl}'
_VIEWER_PRESENT_TTL = 5
_VIEWER_PRESENT_BUMP_INTERVAL_S = 2.0


def _bump_viewer_presence(r, bl: str) -> None:
    try:
        r.setex(_VIEWER_PRESENT_KEY.format(bl=bl), _VIEWER_PRESENT_TTL, b'1')
    except Exception:
        pass

try:
    from qp2.xio.user_group_manager import UserGroupManager as _UGM
    _ugm = _UGM()
except Exception:
    _ugm = None

router = APIRouter(prefix="/viewer", tags=["viewer"])

# --- Path security ---
_ALLOWED_DATA_DIR = os.environ.get("QP2_DATA_DIR", "/mnt/beegfs/DATA")

def _validate_path(path: str):
    """Ensure path is within the allowed data directory."""
    resolved = os.path.realpath(path)
    allowed = os.path.realpath(_ALLOWED_DATA_DIR)
    if not resolved.startswith(allowed + os.sep) and resolved != allowed:
        raise HTTPException(status_code=403, detail="Path outside allowed data directory")
    return resolved

# --- Dependencies ---
def get_db_session():
    raise RuntimeError("get_db_session not overridden")

# --- Reader cache ---
_reader_cache: dict = {}
_reader_lock = threading.Lock()
_MAX_READERS = 10


def _get_reader(master_path: str) -> HDF5Reader:
    # Fast path — check cache under lock without opening the file
    with _reader_lock:
        if master_path in _reader_cache:
            return _reader_cache[master_path]

    # Open HDF5 file outside lock so slow BeeGFS opens don't serialise other readers
    try:
        reader = HDF5Reader(master_path)
    except (FileNotFoundError, RuntimeError) as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    # Re-acquire lock to insert; another thread may have raced us
    with _reader_lock:
        if master_path in _reader_cache:
            try:
                reader.close()
            except Exception:
                pass
            return _reader_cache[master_path]
        if len(_reader_cache) >= _MAX_READERS:
            oldest = next(iter(_reader_cache))
            try:
                _reader_cache[oldest].close()
            except Exception:
                pass
            del _reader_cache[oldest]
        _reader_cache[master_path] = reader
        return reader


def _parse_master_files(raw: str):
    """Parse master_files JSON array, return list of paths."""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    return [raw] if raw else []


def _apply_colormap(data_norm, colormap="plasma"):
    """Apply colormap to normalized [0,1] array via pre-computed LUT (fast path)
    or matplotlib (fallback for unsupported colormaps)."""
    lut = _COLORMAP_LUTS.get(colormap)
    if lut is not None:
        indices = (np.clip(data_norm, 0.0, 1.0) * 255).astype(np.uint8)
        return lut[indices]
    # Fallback — unsupported colormap
    import matplotlib.cm as cm
    rgba = cm.get_cmap(colormap)(data_norm, bytes=True)
    return rgba[:, :, :3]


def _render_frame(frame_data, bit_depth=32, vmin=None, vmax=None, colormap="plasma", width=None):
    """Render raw frame to JPEG bytes. Pure sync — call from run_in_executor."""
    if frame_data is None:
        raise ValueError("No frame data")

    data = frame_data.astype(np.float32)

    mask_val = 2 ** int(bit_depth) - 1 if bit_depth else 2**32 - 1
    mask = frame_data >= mask_val
    data[mask] = 0

    if vmin is None or vmax is None:
        auto_vmin, auto_vmax = calculate_contrast_levels(
            data, low_percentile=50.0, high_percentile=99.99, detector_mask=mask
        )
        if vmin is None:
            vmin = auto_vmin
        if vmax is None:
            vmax = auto_vmax

    if vmax <= vmin:
        vmax = vmin + 1
    data_clipped = np.clip(data, vmin, vmax)
    data_norm = (data_clipped - vmin) / (vmax - vmin)
    data_norm[mask] = 0

    # Peak downsample — preserves diffraction spots
    if width and width < data_norm.shape[1]:
        factor = data_norm.shape[1] // width
        if factor > 1:
            h, w = data_norm.shape
            h_trim = h - (h % factor)
            w_trim = w - (w % factor)
            blocks = data_norm[:h_trim, :w_trim].reshape(h_trim // factor, factor, w_trim // factor, factor)
            data_norm = blocks.max(axis=(1, 3))

    rgb = _apply_colormap(data_norm, colormap)

    Image = _Image
    if Image is None:
        from PIL import Image  # type: ignore
    img = Image.fromarray(rgb, "RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue(), float(vmin), float(vmax)


def _read_and_render(reader, frame, bit_depth, vmin, vmax, colormap, width):
    """Sync helper combining HDF5 read + render — run via run_in_executor."""
    frame_data = reader.get_frame(frame)
    if frame_data is None:
        return None, None, None
    return _render_frame(frame_data, bit_depth=bit_depth, vmin=vmin, vmax=vmax,
                         colormap=colormap, width=width)


# --- Endpoints ---

@router.get("/datasets")
async def list_viewer_datasets(
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """List datasets available for viewing."""
    from sqlalchemy import desc
    query = session.query(DatasetRun)
    if not is_staff_member(user):
        allowed = [user]
        try:
            groups = _ugm.groupnames_from_username(user) if _ugm else []
            if groups:
                allowed.extend([g["group_name"] for g in groups])
        except Exception:
            pass
        query = query.filter(DatasetRun.username.in_(allowed))

    results = query.order_by(desc(DatasetRun.created_at)).limit(200).all()
    datasets = []
    for ds in results:
        paths = _parse_master_files(ds.master_files)
        datasets.append({
            "id": ds.data_id,
            "name": ds.run_prefix,
            "master_file": paths[0] if paths else None,
            "master_files": paths,
            "total_frames": ds.total_frames,
            "created_at": str(ds.created_at),
        })
    return datasets


@router.get("/browse")
async def browse_directory(
    path: str = Query(default=""),
    user: str = Depends(verify_token),
):
    """List subdirectories for directory browser. Staff only."""
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")

    # Default to root data dir if no path given
    target = path.strip() if path.strip() else _ALLOWED_DATA_DIR
    _validate_path(target)

    def _list_subdirs(p):
        if not os.path.isdir(p):
            return None
        try:
            return sorted(
                e for e in os.listdir(p)
                if os.path.isdir(os.path.join(p, e)) and not e.startswith(".")
            )
        except PermissionError:
            return PermissionError

    loop = asyncio.get_running_loop()
    entries = await loop.run_in_executor(None, _list_subdirs, target)
    if entries is None:
        raise HTTPException(status_code=404, detail="Directory not found")
    if entries is PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")

    # Compute parent (clamped to allowed root)
    resolved = os.path.realpath(target)
    allowed = os.path.realpath(_ALLOWED_DATA_DIR)
    parent = str(os.path.dirname(resolved)) if resolved != allowed else None

    return {
        "path": target,
        "root": _ALLOWED_DATA_DIR,
        "parent": parent,
        "subdirs": entries,
    }


@router.get("/scan")
async def scan_directory(
    path: str = Query(...),
    user: str = Depends(verify_token),
):
    """Scan a directory for master files. Staff only."""
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    _validate_path(path)
    loop = asyncio.get_running_loop()
    is_dir = await loop.run_in_executor(None, os.path.isdir, path)
    if not is_dir:
        raise HTTPException(status_code=404, detail="Directory not found")

    masters = await loop.run_in_executor(
        None, lambda: sorted(glob.glob(os.path.join(path, "**/*_master.h5"), recursive=True))
    )
    results = []
    for m in masters[:50]:
        results.append({
            "name": os.path.basename(m).replace("_master.h5", ""),
            "master_file": m,
        })
    return results


@router.get("/params/{dataset_id}")
async def get_params(
    dataset_id: int,
    path: Optional[str] = Query(None),
    master_index: int = 0,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """Get detector parameters for a dataset."""
    if dataset_id == 0 and path:
        if not is_staff_member(user):
            raise HTTPException(status_code=403, detail="Staff only")
        _validate_path(path)
        master = path
    else:
        ds = session.get(DatasetRun, dataset_id)
        if not ds:
            raise HTTPException(status_code=404, detail="Dataset not found")

        paths = _parse_master_files(ds.master_files)
        if not paths:
            raise HTTPException(status_code=404, detail="No master file")

        master = paths[min(master_index, len(paths) - 1)]

    if not os.path.exists(master):
        raise HTTPException(status_code=404, detail="Requested file not found")

    reader = _get_reader(master)
    p = reader.params

    # Get actual image dimensions from first frame
    img_width = p.get("width") or 4150
    img_height = p.get("height") or 4371

    return {
        "beam_x": p.get("beam_x"),
        "beam_y": p.get("beam_y"),
        "wavelength_a": p.get("wavelength"),
        "det_dist_mm": p.get("det_dist"),
        "pixel_size_mm": p.get("pixel_size"),
        "bit_depth": p.get("bit_depth", 32),
        "total_frames": reader.total_frames,
        "image_width": img_width,
        "image_height": img_height,
    }


@router.get("/frame/{dataset_id}")
async def get_frame(
    dataset_id: int,
    path: Optional[str] = Query(None),
    frame: int = 0,
    master_index: int = 0,
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    colormap: str = "plasma",
    width: Optional[int] = None,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """Render a single frame as JPEG."""
    if dataset_id == 0 and path:
        if not is_staff_member(user):
            raise HTTPException(status_code=403, detail="Staff only")
        _validate_path(path)
        master = path
    else:
        ds = session.get(DatasetRun, dataset_id)
        if not ds:
            logger.warning(f"frame: dataset {dataset_id} not found in DB")
            raise HTTPException(status_code=404, detail="Dataset not found")

        paths = _parse_master_files(ds.master_files)
        if not paths:
            logger.warning(f"frame: dataset {dataset_id} has no master files (raw: {ds.master_files!r})")
            raise HTTPException(status_code=404, detail="No master file")

        master = paths[min(master_index, len(paths) - 1)]

    if not os.path.exists(master):
        logger.warning(f"frame: master file does not exist: {master}")
        raise HTTPException(status_code=404, detail="Requested file not found")

    reader = _get_reader(master)

    if frame < 0 or frame >= reader.total_frames:
        raise HTTPException(status_code=400, detail=f"Frame {frame} out of range [0, {reader.total_frames})")

    bit_depth = reader.params.get("bit_depth", 32)
    loop = asyncio.get_running_loop()
    try:
        jpeg_bytes, actual_vmin, actual_vmax = await loop.run_in_executor(
            None, _read_and_render, reader, frame, bit_depth, vmin, vmax, colormap, width
        )
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to render frame")
    if jpeg_bytes is None:
        raise HTTPException(status_code=404, detail=f"Could not read frame {frame}")

    return Response(
        content=jpeg_bytes,
        media_type="image/jpeg",
        headers={
            "X-Vmin": str(actual_vmin),
            "X-Vmax": str(actual_vmax),
            "X-Frame": str(frame),
            "X-Total-Frames": str(reader.total_frames),
            "Cache-Control": "no-cache",
        },
    )


@router.get("/frame_by_path")
async def get_frame_by_path(
    path: str = Query(...),
    frame: int = 0,
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    colormap: str = "plasma",
    width: Optional[int] = None,
    user: str = Depends(verify_token),
):
    """Render a frame from a master file path (for directory scan results)."""
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    _validate_path(path)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")

    reader = _get_reader(path)
    if frame < 0 or frame >= reader.total_frames:
        raise HTTPException(status_code=400, detail=f"Frame {frame} out of range")

    bit_depth = reader.params.get("bit_depth", 32)
    loop = asyncio.get_running_loop()
    jpeg_bytes, actual_vmin, actual_vmax = await loop.run_in_executor(
        None, _read_and_render, reader, frame, bit_depth, vmin, vmax, colormap, width
    )
    if jpeg_bytes is None:
        raise HTTPException(status_code=404, detail=f"Could not read frame {frame}")

    return Response(
        content=jpeg_bytes,
        media_type="image/jpeg",
        headers={
            "X-Vmin": str(actual_vmin),
            "X-Vmax": str(actual_vmax),
            "X-Frame": str(frame),
            "X-Total-Frames": str(reader.total_frames),
        },
    )


@router.get("/pixel/{dataset_id}")
async def get_pixel_info(
    dataset_id: int,
    path: Optional[str] = Query(None),
    frame: int = 0,
    x: float = 0,
    y: float = 0,
    master_index: int = 0,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """Get pixel information including resolution."""
    if dataset_id == 0 and path:
        if not is_staff_member(user):
            raise HTTPException(status_code=403, detail="Staff only")
        _validate_path(path)
        master = path
    else:
        ds = session.get(DatasetRun, dataset_id)
        if not ds:
            raise HTTPException(status_code=404, detail="Dataset not found")

        paths = _parse_master_files(ds.master_files)
        if not paths:
            raise HTTPException(status_code=404, detail="No master file")

        master = paths[min(master_index, len(paths) - 1)]

    if not os.path.exists(master):
        raise HTTPException(status_code=404, detail="Master file not found")

    reader = _get_reader(master)

    def _read_pixel():
        fd = reader.get_frame(frame)
        if fd is None:
            return None
        return calculate_pixel_info(x, y, fd, reader.params)

    loop = asyncio.get_running_loop()
    info = await loop.run_in_executor(None, _read_pixel)
    if info is None:
        raise HTTPException(status_code=404, detail="Could not read frame")
    return info


@router.get("/rings/{dataset_id}")
async def get_rings(
    dataset_id: int,
    path: Optional[str] = Query(None),
    master_index: int = 0,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """Get resolution ring positions for overlay."""
    if dataset_id == 0 and path:
        if not is_staff_member(user):
            raise HTTPException(status_code=403, detail="Staff only")
        _validate_path(path)
        master = path
    else:
        ds = session.get(DatasetRun, dataset_id)
        if not ds:
            raise HTTPException(status_code=404, detail="Dataset not found")

        paths = _parse_master_files(ds.master_files)
        if not paths:
            raise HTTPException(status_code=404, detail="No master file")

        master = paths[min(master_index, len(paths) - 1)]

    if not os.path.exists(master):
        raise HTTPException(status_code=404, detail="Master file not found")

    reader = _get_reader(master)
    p = reader.params

    beam_x = p.get("beam_x")
    beam_y = p.get("beam_y")
    wavelength = p.get("wavelength")
    det_dist = p.get("det_dist")
    pixel_size = p.get("pixel_size")

    # Get actual image dimensions from first frame
    img_width = p.get("width") or 4150
    img_height = p.get("height") or 4371

    if not all([beam_x, beam_y, wavelength, det_dist, pixel_size]):
        return {"beam_x": beam_x, "beam_y": beam_y, "rings": [],
                "image_width": img_width, "image_height": img_height}

    # Standard resolution rings (Angstroms)
    d_spacings = [20, 10, 7, 5, 4, 3.5, 3, 2.5, 2, 1.8, 1.5, 1.2, 1.0]
    max_radius = max(img_width, img_height)

    rings = []
    for d in d_spacings:
        try:
            radius_px = resolution_to_radius(d, wavelength, det_dist, pixel_size)
            if radius_px > 0 and radius_px < max_radius:
                rings.append({
                    "d_spacing": d,
                    "radius_px": round(float(radius_px), 1),
                    "label": f"{d}\u00c5",
                })
        except Exception:
            continue

    return {
        "beam_x": beam_x,
        "beam_y": beam_y,
        "rings": rings,
        "image_width": img_width,
        "image_height": img_height,
    }


# ── Live Viewer (Redis "eiger" stream) ────────────────────────────────────────

_EIGER_STREAM = "eiger"
# Separate stream for synthetic monitor-API frames written by the EPU sidecar.
# Shielded from other `eiger` consumers (image_viewer, autoproc, gmcaproc).
_MONITOR_STREAM = "eiger_monitor"
_LIVE_STREAMS = (_EIGER_STREAM, _MONITOR_STREAM)
_redis_hosts = ServerConfig.get_redis_hosts() if ServerConfig else {}

# Binary Redis clients for the sidecar JPEG/raw cache on each EPU machine.
# decode_responses=False because values are JPEG bytes, not strings.
_live_cache_redis: dict = {}
for _bl_key in ("bl1", "bl2"):
    _host = _redis_hosts.get(_bl_key)
    if _host:
        try:
            _r = redis_lib.Redis(host=_host, port=6379, decode_responses=False,
                                 socket_connect_timeout=2)
            _r.ping()
            _live_cache_redis[_bl_key] = _r
        except Exception:
            pass


def _get_current_eiger_owner(beamline: str):
    """Return the most recent `eiger` stream entry's owner, or None.

    Used to authorize monitor-preview reads against the ACTUAL active
    acquisition's owner (not the request's `owner` param), so a member of one
    ESAF group can't peek at another group's exposure via the preview path.
    """
    r = _live_cache_redis.get(beamline)
    if r is None:
        return None
    try:
        last = r.xrevrange(_EIGER_STREAM, max='+', min='-', count=1)
        if not last:
            return None
        fields = last[0][1]
        raw = fields.get(b'message') or fields.get('message')
        if not raw:
            return None
        msg = json.loads(raw)
        img = msg.get('4', {})
        return img.get('username') or None
    except Exception:
        return None


def _get_sidecar_frame(beamline: str):
    """Read the latest JPEG + metadata from the sidecar Redis cache.

    Returns (jpeg_bytes, meta_dict) or (None, None) if not available or stale.
    A frame is considered stale if its timestamp is older than 60 seconds.
    """
    r = _live_cache_redis.get(beamline)
    if not r:
        return None, None
    try:
        pipe = r.pipeline()
        pipe.get(f'live:jpeg:{beamline}')
        pipe.get(f'live:meta:{beamline}')
        jpeg, meta_raw = pipe.execute()
        if not jpeg or not meta_raw:
            return None, None
        meta = json.loads(meta_raw)
        if time.time() - meta.get('timestamp', 0) > 60:
            return None, None   # stale — sidecar may have stopped
        return jpeg, meta
    except Exception as e:
        logger.debug(f"Sidecar frame read failed for {beamline}: {e}")
        return None, None


def _get_live_redis_clients(beamline: Optional[str] = None):
    """Return list of (beamline_key, Redis) pairs for reachable beamline hosts.

    Creates one connection per call; connections are closed in the SSE
    finally block. Validates connectivity with ping so unreachable hosts
    are skipped cleanly rather than causing silent failures in xread.
    """
    keys = [beamline] if beamline in ("bl1", "bl2") else ["bl1", "bl2"]
    clients = []
    for key in keys:
        host = _redis_hosts.get(key)
        if not host:
            continue
        try:
            r = redis_lib.Redis(host=host, port=6379, decode_responses=True,
                                socket_connect_timeout=2)
            r.ping()
            clients.append((key, r))
        except Exception:
            pass
    return clients


def _get_allowed_names(user: str) -> set:
    names = {user}
    if _ugm:
        try:
            groups = _ugm.groupnames_from_username(user)
            if groups:
                names.update(g["group_name"] for g in groups)
        except Exception:
            pass
    return names


def _parse_eiger_message(raw: dict):
    """Parse a raw Redis stream entry. Returns dict or None."""
    try:
        msg = json.loads(raw.get("message", "{}"))
        block0 = msg.get("0", {})
        if block0.get("htype") != "dimage-1.0" or "4" not in msg:
            return None
        img = msg["4"]
        prefix = img.get("prefix")
        data_dir_root = img.get("data_dir")
        user_dir = img.get("user_dir", "")
        if not prefix or not data_dir_root:
            return None
        data_dir = os.path.join(data_dir_root, user_dir)
        return {
            "master_file": os.path.join(data_dir, f"{prefix}_master.h5"),
            "series_id": block0.get("series"),
            "frame": int(block0.get("frame", -1)),
            "prefix": prefix,
            "owner": img.get("username", ""),
            "total_frames": img.get("run_fr_count"),
            "beam_x": img.get("xbeam_px"),
            "beam_y": img.get("ybeam_px"),
            "energy_ev": img.get("energy_eV"),
            "det_dist_m": img.get("detector_dist_m"),
            "exposure_sec": img.get("exposure_sec"),
            "beamline": img.get("beamline"),
        }
    except Exception:
        return None


def _check_live_access(owner: str, user: str) -> bool:
    """Staff always allowed; others must own the collection."""
    return is_staff_member(user) or (owner in _get_allowed_names(user))


def _check_live_path_access(path: str, owner: str, user: str) -> bool:
    """Like _check_live_access but also validates the path contains the owner
    as a directory component, preventing a user from supplying their own group
    name as owner while pointing at another group's data path.
    Staff bypass both checks.
    """
    if is_staff_member(user):
        return True
    allowed = _get_allowed_names(user)
    if owner not in allowed:
        return False
    # Verify at least one allowed name appears as a path component
    path_parts = set(os.path.normpath(path).split(os.sep))
    return any(name in path_parts for name in allowed)


@router.get("/live/events")
async def live_events(
    user: str = Depends(verify_token),
    beamline: Optional[str] = Query(default=None, pattern="^(bl1|bl2)$"),
):
    """SSE: push frame-ready events from the Redis eiger stream.

    beamline: restrict to "bl1" or "bl2". Staff should specify this when both
    beamlines are collecting simultaneously. Regular users can omit it — the
    owner filter ensures they only see their own collection.

    Staff receive all events on the selected beamline(s). Regular users receive
    only events where the collection owner matches one of their group names.
    """
    staff = is_staff_member(user)
    allowed = None if staff else _get_allowed_names(user)

    async def generate():
        loop = asyncio.get_running_loop()
        clients = await loop.run_in_executor(None, lambda: _get_live_redis_clients(beamline))

        if not clients:
            yield f"data: {json.dumps({'type': 'error', 'message': 'No beamline Redis available'})}\n\n"
            return

        last_ids = {id(r): {s: "$" for s in _LIVE_STREAMS} for _, r in clients}
        current_series = {}        # id(r) → series_id
        file_ready_series = set()  # (rid, series_id) pairs whose master file exists
        event_queue: asyncio.Queue = asyncio.Queue()
        pending_tasks: set = set()  # track create_task refs; auto-pruned on completion
        last_frame_emit = 0.0
        last_heartbeat = time.time()
        last_presence_bump: dict = {}   # bl_key → ts of last bump
        THROTTLE = 0.1             # max frame_update frequency (seconds)
        FILE_WAIT_S = 120          # max seconds to wait for master file on disk

        async def _poll_file_then_enqueue(parsed, bl_key, rid):
            """Emit new_series when data is readable.

            Priority order:
            1. Sidecar Redis cache  — instant, no disk latency (preferred)
            2. HDF5 master file     — poll up to FILE_WAIT_S seconds (fallback)
            """
            sid = parsed['series_id']
            master = parsed['master_file']
            owner_p = parsed.get('owner', '')
            _loop = asyncio.get_running_loop()

            # 1. Check sidecar Redis cache (available immediately when sidecar runs)
            # 2. Fall back to HDF5 master file on disk (BeeGFS stat via executor)
            # Owner mismatch on sidecar = leftover frame from another series
            # (e.g. monitor) — ignore it and use the file path.
            for _ in range(FILE_WAIT_S):
                jpeg, meta = _get_sidecar_frame(bl_key)
                if jpeg and meta and meta.get('owner', '') == owner_p:
                    file_ready_series.add((rid, sid))
                    await event_queue.put(('new_series', bl_key, parsed))
                    return
                exists = await _loop.run_in_executor(None, os.path.exists, master)
                if exists:
                    file_ready_series.add((rid, sid))
                    await event_queue.put(('new_series', bl_key, parsed))
                    return
                await asyncio.sleep(1)
            await event_queue.put(('file_timeout', bl_key, parsed))

        try:
            while True:
                now = time.time()

                if now - last_heartbeat > 15:
                    yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
                    last_heartbeat = now

                # Drain queued file-ready / timeout events
                while not event_queue.empty():
                    kind, bl_key_q, parsed_q = event_queue.get_nowait()
                    if kind == 'new_series':
                        yield f"data: {json.dumps({'type': 'new_series', 'beamline_key': bl_key_q, **parsed_q})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'error', 'message': f'Master file did not appear after {FILE_WAIT_S}s', 'prefix': parsed_q.get('prefix', '')})}\n\n"
                    await asyncio.sleep(0)

                for bl_key, r in clients:
                    rid = id(r)
                    if now - last_presence_bump.get(bl_key, 0) >= _VIEWER_PRESENT_BUMP_INTERVAL_S:
                        _bump_viewer_presence(r, bl_key)
                        last_presence_bump[bl_key] = now
                    try:
                        messages = await loop.run_in_executor(
                            None,
                            lambda _r=r, _sid=dict(last_ids[rid]): _r.xread(
                                _sid, count=50, block=500
                            ),
                        )
                    except Exception:
                        continue

                    for _sn, msg_list in (messages or []):
                        for msg_id, raw in msg_list:
                            last_ids[rid][_sn] = msg_id
                            parsed = _parse_eiger_message(raw)
                            if not parsed:
                                continue
                            if not staff and parsed["owner"] not in allowed:
                                continue

                            sid = parsed["series_id"]
                            now2 = time.time()

                            if current_series.get(rid) != sid:
                                # Prune old series so the set stays bounded
                                old_sid = current_series.get(rid)
                                if old_sid is not None:
                                    file_ready_series.discard((rid, old_sid))
                                current_series[rid] = sid
                                yield f"data: {json.dumps({'type': 'pending', 'beamline_key': bl_key, 'prefix': parsed['prefix'], 'total_frames': parsed['total_frames'], 'owner': parsed['owner'], 'beamline': parsed['beamline']})}\n\n"
                                task = asyncio.create_task(
                                    _poll_file_then_enqueue(parsed, bl_key, rid))
                                pending_tasks.add(task)
                                task.add_done_callback(pending_tasks.discard)
                                last_frame_emit = now2
                                await asyncio.sleep(0)
                            elif (rid, sid) in file_ready_series and now2 - last_frame_emit >= THROTTLE:
                                yield f"data: {json.dumps({'type': 'frame_update', 'beamline_key': bl_key, 'master_file': parsed['master_file'], 'frame': parsed['frame'], 'owner': parsed['owner']})}\n\n"
                                last_frame_emit = now2
                                await asyncio.sleep(0)

        finally:
            # Cancel any in-flight file-polling tasks (client disconnected)
            for t in list(pending_tasks):
                t.cancel()
            # Release Redis connections back to the pool
            for _, r in clients:
                try:
                    r.close()
                except Exception:
                    pass

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/live/frame")
async def get_live_frame(
    path: str = Query(...),
    frame: int = 0,
    owner: str = Query(...),
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    colormap: str = "plasma",
    width: Optional[int] = None,
    user: str = Depends(verify_token),
):
    """Render a live frame as JPEG. owner must be in user's allowed group names
    and must appear as a path component of the requested file."""
    if not _check_live_path_access(path, owner, user):
        raise HTTPException(status_code=403, detail="Access denied")
    _validate_path(path)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Master file not found")

    try:
        reader = _get_reader(path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail="File not ready yet") from e

    # Retry for up to 5s if the specific frame hasn't been flushed yet,
    # then render — all blocking I/O in executor to avoid event loop stalls.
    bit_depth = reader.params.get("bit_depth", 32)
    _loop = asyncio.get_running_loop()

    def _retry_read_and_render():
        for _ in range(10):  # 1s max — frame should be written by the time SSE fires
            result = _read_and_render(reader, frame, bit_depth, vmin, vmax, colormap, width)
            if result[0] is not None:
                return result
            time.sleep(0.1)
        return None, None, None

    try:
        jpeg_bytes, actual_vmin, actual_vmax = await _loop.run_in_executor(
            None, _retry_read_and_render
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail="Render failed") from e

    if jpeg_bytes is None:
        raise HTTPException(status_code=404, detail=f"Frame {frame} not yet available")

    return Response(
        content=jpeg_bytes,
        media_type="image/jpeg",
        headers={
            "X-Vmin": str(actual_vmin),
            "X-Vmax": str(actual_vmax),
            "X-Frame": str(frame),
            "X-Total-Frames": str(reader.total_frames),
            "Cache-Control": "no-store",
        },
    )


@router.get("/live/params")
async def get_live_params(
    path: str = Query(...),
    owner: str = Query(...),
    user: str = Depends(verify_token),
):
    """Return detector parameters for a live master file (path-based)."""
    if not _check_live_path_access(path, owner, user):
        raise HTTPException(status_code=403, detail="Access denied")
    _validate_path(path)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Master file not found")

    try:
        reader = _get_reader(path)
    except HTTPException:
        raise

    p = reader.params
    img_width = p.get("width") or 4150
    img_height = p.get("height") or 4371

    return {
        "beam_x": p.get("beam_x"),
        "beam_y": p.get("beam_y"),
        "wavelength_a": p.get("wavelength"),
        "det_dist_mm": p.get("det_dist"),
        "pixel_size_mm": p.get("pixel_size"),
        "bit_depth": p.get("bit_depth", 32),
        "total_frames": reader.total_frames,
        "image_width": img_width,
        "image_height": img_height,
    }


@router.get("/live/latest")
async def get_live_latest(
    beamline: str = Query(..., pattern="^(bl1|bl2)$"),
    owner: str = Query(...),
    user: str = Depends(verify_token),
):
    """Return the most recent live JPEG for a beamline.

    Priority:
    1. Sidecar Redis cache  (live:jpeg:{beamline}) — preferred, no disk latency
    2. live_zmq in-memory cache                    — gated by LIVE_ZMQ_FALLBACK_ENABLED

    Each request also refreshes `live:viewer_present:{beamline}` (TTL 5 s) so
    the EPU sidecar's monitor-API producer knows a viewer is connected.

    owner is validated against the requesting user's allowed group names.
    """
    if not _check_live_access(owner, user):
        raise HTTPException(status_code=403, detail="Access denied")

    r_pres = _live_cache_redis.get(beamline)
    if r_pres is not None:
        _bump_viewer_presence(r_pres, beamline)

    # ── 1. Sidecar Redis cache ────────────────────────────────────────────────
    # Two serve paths:
    #   - owner match → canonical sidecar frame (ZMQ-decoded for this series)
    #   - source=detector-monitor → fast preview during real acquisitions when
    #     the recorded frame from disk would otherwise be the only option;
    #     not bit-identical to the recorded frame but updates at ~10 Hz
    # Any other mismatch falls through to the file path.
    jpeg, meta = _get_sidecar_frame(beamline)
    if jpeg and meta:
        meta_owner = meta.get('owner', '')
        is_owner_match = meta_owner == owner
        is_monitor_preview = meta.get('source') == 'detector-monitor'
        # Monitor preview shows the currently exposing experiment regardless of
        # the request's owner param. Re-authorize against the real acquisition's
        # owner so a member of group A can't peek at group B via the preview.
        if is_monitor_preview and not is_owner_match:
            current_owner = _get_current_eiger_owner(beamline) or ''
            if not _check_live_access(current_owner, user):
                is_monitor_preview = False
        if is_owner_match or is_monitor_preview:
            if is_owner_match and not _check_live_access(meta_owner, user):
                raise HTTPException(status_code=403, detail="Access denied")
            x_source = 'monitor-preview' if (is_monitor_preview and not is_owner_match) else 'sidecar'
            return Response(
                content=jpeg,
                media_type="image/jpeg",
                headers={
                    "X-Frame":        str(meta.get('frame_num', meta.get('frame', ''))),
                    "X-Series":       str(meta.get('series_id', '')),
                    "X-Beamline":     str(meta.get('beamline', beamline)),
                    "X-Image-Width":  str(meta.get('image_width', '')),
                    "X-Image-Height": str(meta.get('image_height', '')),
                    "X-Source":       x_source,
                    "Cache-Control":  "no-store",
                },
            )

    # ── 2. live_zmq in-memory cache (fallback) ────────────────────────────────
    if _LIVE_ZMQ_FALLBACK_ENABLED and _live_zmq is not None:
        entry = _live_zmq.get_cache().get_latest(beamline)
        if entry and _check_live_access(entry.owner, user):
            return Response(
                content=entry.jpeg,
                media_type="image/jpeg",
                headers={
                    "X-Vmin":         str(entry.vmin),
                    "X-Vmax":         str(entry.vmax),
                    "X-Frame":        str(entry.frame_num),
                    "X-Series":       str(entry.series_id or ""),
                    "X-Beamline":     entry.beamline_name,
                    "X-Image-Width":  str(entry.image_width),
                    "X-Image-Height": str(entry.image_height),
                    "X-Source":       "zmq-cache",
                    "Cache-Control":  "no-store",
                },
            )

    raise HTTPException(status_code=404, detail="No live frame available for this beamline")
