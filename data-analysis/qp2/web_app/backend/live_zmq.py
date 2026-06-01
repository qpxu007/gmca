"""Background ZMQ subscriber + JPEG frame cache for the live viewer.

Subscribes to the eiger_pub_proxy PUB socket for each beamline, decodes
bitshuffle-compressed frames, renders to JPEG, and stores the latest result
in memory. The web API then serves these cached JPEGs without any HDF5
file I/O — eliminating the latency between the Redis notification and the
file becoming readable on disk.

Configuration via environment variables:
    EIGER_PUB_BL1   ZMQ PUB address for BL1 (default: tcp://10.20.103.85:9900)
    EIGER_PUB_BL2   ZMQ PUB address for BL2 (default: tcp://10.20.103.154:9900)
"""

import io
import json
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

import numpy as np

try:
    import zmq
    import bitshuffle
    _ZMQ_OK = True
except ImportError:
    _ZMQ_OK = False

try:
    from qp2.log.logging_config import get_logger
    logger = get_logger(__name__)
except Exception:
    import logging
    logger = logging.getLogger(__name__)

# Default PUB addresses — override via env vars
_DEFAULT_PUB = {
    'bl1': os.environ.get('EIGER_PUB_BL1', 'tcp://10.20.103.85:9900'),
    'bl2': os.environ.get('EIGER_PUB_BL2', 'tcp://10.20.103.154:9900'),
}


@dataclass
class FrameEntry:
    jpeg: bytes
    vmin: float
    vmax: float
    frame_num: int
    series_id: Optional[int]
    timestamp: float
    owner: str = ''
    beam_x: Optional[float] = None
    beam_y: Optional[float] = None
    energy_ev: Optional[float] = None
    det_dist_m: Optional[float] = None
    exposure_sec: Optional[float] = None
    beamline_name: str = ''
    image_width: int = 0
    image_height: int = 0


def _decompress(raw: bytes, shape, dtype) -> np.ndarray:
    """Decompress bitshuffle LZ4 frame bytes into a numpy array."""
    block_size = int.from_bytes(raw[8:12], byteorder='big') / dtype.itemsize
    buf = np.frombuffer(raw[12:], dtype=np.uint8)
    return bitshuffle.decompress_lz4(buf, [shape[1], shape[0]], dtype, block_size)


# Pre-compute colormap LUT at import (mirrors viewer_routes for consistency)
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.cm as _cm
    from PIL import Image as _PIL_Image
    from qp2.image_viewer.utils.contrast_utils import calculate_contrast_levels as _calc_contrast
    _PLASMA_LUT = (_cm.get_cmap('plasma')(np.linspace(0, 1, 256))[:, :3] * 255).astype(np.uint8)
except Exception:
    _PLASMA_LUT = None
    _PIL_Image = None
    _calc_contrast = None


def _render_jpeg(frame: np.ndarray, bit_depth: int = 32, width: int = 800) -> tuple:
    """Render a frame array to JPEG bytes. Returns (jpeg_bytes, vmin, vmax)."""
    data = frame.astype(np.float32)
    mask_val = 2 ** int(bit_depth) - 1
    mask = frame >= mask_val
    data[mask] = 0

    if _calc_contrast:
        vmin, vmax = _calc_contrast(data, low_percentile=50.0, high_percentile=99.99, detector_mask=mask)
    else:
        valid = data[~mask]
        vmin = float(np.percentile(valid, 50)) if valid.size else float(data.min())
        vmax = float(np.percentile(valid, 99.99)) if valid.size else float(data.max())

    if vmax <= vmin:
        vmax = vmin + 1
    data_norm = np.clip((data - vmin) / (vmax - vmin), 0.0, 1.0)
    data_norm[mask] = 0

    if width and width < data_norm.shape[1]:
        factor = data_norm.shape[1] // width
        if factor > 1:
            h, w = data_norm.shape
            h_trim, w_trim = h - h % factor, w - w % factor
            blocks = data_norm[:h_trim, :w_trim].reshape(
                h_trim // factor, factor, w_trim // factor, factor)
            data_norm = blocks.max(axis=(1, 3))

    # Use pre-computed LUT (fast path) or fall back to matplotlib
    if _PLASMA_LUT is not None:
        rgb = _PLASMA_LUT[(data_norm * 255).astype(np.uint8)]
    else:
        import matplotlib.cm as cm
        rgb = (cm.get_cmap('plasma')(data_norm, bytes=True))[:, :, :3]

    Image = _PIL_Image
    if Image is None:
        from PIL import Image  # type: ignore
    img = Image.fromarray(rgb, 'RGB')
    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=85)
    return buf.getvalue(), float(vmin), float(vmax)


class LiveFrameCache:
    """Thread-safe in-memory cache of the latest rendered JPEG per beamline."""

    def __init__(self):
        self._cache: Dict[str, FrameEntry] = {}
        self._lock = threading.RLock()
        self._threads: list = []
        self._running = False

    def start(self, pub_addresses: Optional[Dict[str, str]] = None):
        if not _ZMQ_OK:
            logger.warning('live_zmq: zmq or bitshuffle not installed — ZMQ cache disabled')
            return
        addresses = pub_addresses or _DEFAULT_PUB
        self._running = True
        for beamline, addr in addresses.items():
            t = threading.Thread(
                target=self._subscribe_loop,
                args=(beamline, addr),
                daemon=True,
                name=f'live-zmq-{beamline}',
            )
            t.start()
            self._threads.append(t)
            logger.info(f'live_zmq: [{beamline}] subscribing to {addr}')

    def stop(self):
        self._running = False

    def get_latest(self, beamline: str) -> Optional[FrameEntry]:
        with self._lock:
            return self._cache.get(beamline)

    def is_active(self, beamline: str, max_age_s: float = 30.0) -> bool:
        """Return True if a recent frame exists for this beamline."""
        entry = self.get_latest(beamline)
        return entry is not None and (time.time() - entry.timestamp) < max_age_s

    def _subscribe_loop(self, beamline: str, addr: str):
        ctx = zmq.Context.instance()
        sub = ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.SUBSCRIBE, b'')
        sub.setsockopt(zmq.RCVHWM, 3)
        sub.connect(addr)
        logger.info(f'live_zmq: [{beamline}] SUB connected to {addr}')

        while self._running:
            try:
                if not sub.poll(timeout=1000):
                    continue
                parts = sub.recv_multipart()
                self._process_frame(beamline, parts)
            except zmq.ZMQError as e:
                logger.warning(f'live_zmq: [{beamline}] ZMQ error: {e}')
                time.sleep(1)
            except Exception as e:
                logger.debug(f'live_zmq: [{beamline}] frame error: {e}')

        sub.close()
        logger.info(f'live_zmq: [{beamline}] subscriber stopped')

    def _process_frame(self, beamline: str, parts: list):
        if len(parts) < 3:
            return
        part_0 = json.loads(parts[0])
        if part_0.get('htype') != 'dimage-1.0':
            return

        part_1 = json.loads(parts[1])
        shape = part_1['shape']          # [y_pixels, x_pixels]
        img_dtype = np.dtype(part_1.get('type', 'uint32'))

        # Decode compressed frame
        frame = _decompress(parts[2], shape, img_dtype)

        # Bluice metadata (part index 4)
        img_meta = {}
        if len(parts) > 4:
            try:
                img_meta = json.loads(parts[4])
            except Exception:
                pass

        h, w = frame.shape
        jpeg, vmin, vmax = _render_jpeg(frame, bit_depth=img_dtype.itemsize * 8, width=800)

        entry = FrameEntry(
            jpeg=jpeg,
            vmin=vmin,
            vmax=vmax,
            frame_num=int(part_0.get('frame', -1)),
            series_id=part_0.get('series'),
            timestamp=time.time(),
            owner=img_meta.get('username', ''),
            beam_x=img_meta.get('xbeam_px'),
            beam_y=img_meta.get('ybeam_px'),
            energy_ev=img_meta.get('energy_eV'),
            det_dist_m=img_meta.get('detector_dist_m'),
            exposure_sec=img_meta.get('exposure_sec'),
            beamline_name=img_meta.get('beamline', beamline),
            image_width=w,
            image_height=h,
        )
        with self._lock:
            self._cache[beamline] = entry


# Module-level singleton — started by viewer_routes at import time
_cache = LiveFrameCache()


def get_cache() -> LiveFrameCache:
    return _cache


def start(pub_addresses: Optional[Dict[str, str]] = None):
    _cache.start(pub_addresses)
