#!/usr/bin/env python3
"""Eiger live frame cache — EPU sidecar for the web live viewer.

Subscribes to the ZMQ PUB socket produced by eiger_pub_proxy.py (or the
patched production proxy), decodes each bitshuffle-compressed frame, renders
a JPEG, and writes it to the local Redis instance. The web backend reads these
keys to serve the live viewer with zero disk latency.

Optionally stores the original compressed raw bytes alongside the JPEG for
in-memory frame analysis without reading from disk (--store-raw).

Additionally, when the ZMQ PUB stream is silent (no frames for >2 s) AND at
least one viewer is connected to the web backend, a background producer thread
pulls frames from the detector's HTTP monitor API at up to 10 Hz, writes them
to the same Redis keys, and appends a synthetic entry to the `eiger` stream so
the backend's SSE handler wakes up and drives the frontend.

Hostname auto-detection:
    bl1* → beamline=bl1, redis=localhost, pub=localhost:9900
    bl2* → beamline=bl2, redis=localhost, pub=localhost:9900

Usage (run on bl1epu or bl2epu — args override auto-detected defaults):
    python qp2_eiger_live_cache.py
    python qp2_eiger_live_cache.py --store-raw
    python qp2_eiger_live_cache.py --beamline bl1 --pub-address localhost:9900
    python qp2_eiger_live_cache.py --detector-address bl1dcu
    python qp2_eiger_live_cache.py --no-monitor-fallback

Viewer-presence key (written by the web backend, read by this sidecar):
    live:viewer_present:{beamline}   TTL ~5s — producer pulls only when present

Redis keys written (TTL 300s by default):
    live:jpeg:{beamline}   JPEG bytes (~300 KB)
    live:meta:{beamline}   JSON metadata (frame#, owner, energy, beam centre …)
    live:raw:{beamline}    bitshuffle-compressed uint32 bytes (~3 MB) [--store-raw]
    live:shape:{beamline}  JSON {"shape":[h,w], "type":"uint32"}      [--store-raw]

Reading raw frames from any machine:
    import bitshuffle, numpy as np, json, redis
    db    = redis.Redis(host='10.20.103.85')
    raw   = db.get('live:raw:bl1')
    info  = json.loads(db.get('live:shape:bl1'))
    dtype = np.dtype(info['type'])
    h, w  = info['shape']
    block = int.from_bytes(raw[8:12], 'big') / dtype.itemsize
    frame = bitshuffle.decompress_lz4(np.frombuffer(raw[12:], np.uint8),
                                       [w, h], dtype, block)
"""

import argparse
import hashlib
import io
import json
import socket
import sys
import time
import threading

import numpy as np
import redis
import requests
import zmq

try:
    import bitshuffle
except ImportError:
    print('bitshuffle not installed. Install with: pip install bitshuffle', file=sys.stderr)
    sys.exit(1)

try:
    from PIL import Image
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.cm as cm
except ImportError:
    print('Pillow / matplotlib not installed.', file=sys.stderr)
    sys.exit(1)


# Pre-computed plasma LUT (mirrors live_zmq.py and viewer_routes.py). Avoids
# rebuilding the matplotlib colormap on every frame — ~10 ms/frame savings.
if hasattr(matplotlib, 'colormaps'):
    _PLASMA_LUT = (matplotlib.colormaps['plasma'](np.linspace(0, 1, 256))[:, :3] * 255).astype(np.uint8)
else:
    _PLASMA_LUT = (cm.get_cmap('plasma')(np.linspace(0, 1, 256))[:, :3] * 255).astype(np.uint8)

EIGER_STREAM         = 'eiger'
# Separate stream from `eiger` so other consumers (image_viewer, autoproc,
# gmcaproc) don't see synthetic monitor entries. Only the web SSE handler
# subscribes to this.
MONITOR_STREAM       = 'eiger_monitor'
MONITOR_API_VERSION  = '1.8.0'
STREAM_MAXLEN        = 1000
# ZMQ-silence threshold gating the monitor producer. Must be >> typical
# inter-frame gap of a real acquisition (~10 ms at 100 Hz) to avoid bouncing.
ZMQ_SILENCE_S        = 2.0
# Same idea but for the `eiger` Redis stream. Covers the case where a
# production proxy (without ZMQ PUB 9900) is writing real frames to the
# stream — we must NOT shadow it with monitor pulls.
EIGER_STREAM_SILENCE_S = 2.0
# DCU HTTP long-poll budget. 100 ms caps the producer at ~10 Hz, which is the
# Eiger monitor API's own hardware ceiling.
MONITOR_POLL_MS      = 100
# Back off after a full sweep of DCU candidates failed.
MONITOR_BACKOFF_S    = 1.0
# After N consecutive failures, re-attempt monitor-mode enable (handles DCU
# restarts mid-session without periodic polling on the happy path).
MODE_REENABLE_AFTER  = 5

_DCU_HOSTS = {
    'bl1': ['bl1dcu', '10.42.42.10'],
    'bl2': ['bl2dcu', '10.42.103.10'],
}


class EigerLiveCache:
    def __init__(self, args, hostname):
        self.args = args
        self.hostname = hostname
        self.BL = args.beamline
        
        self.KEY_JPEG     = f'live:jpeg:{self.BL}'
        self.KEY_META     = f'live:meta:{self.BL}'
        self.KEY_RAW      = f'live:raw:{self.BL}'
        self.KEY_SHAPE    = f'live:shape:{self.BL}'
        self.KEY_PRESENCE = f'live:viewer_present:{self.BL}'

        self.monitor_series_id = f'monitor-{int(time.time())}'
        self.monitor_frame_num = 0
        self.last_zmq_frame_ts = 0.0
        self._last_monitor_hash = b''

        # Reused HTTP session keeps the TCP connection to the DCU alive across
        # ~10 Hz polls — saves a handshake per request.
        self._mon_session = requests.Session()

        print(f'Beamline  : {self.BL} (hostname: {self.hostname})')
        print(f'PUB addr  : tcp://{self.args.pub_address}')
        print(f'Redis     : {self.args.redis_address}:{self.args.redis_port}')
        print(f'Store raw : {self.args.store_raw}')

        self.db = redis.Redis(host=self.args.redis_address, port=self.args.redis_port)
        try:
            self.db.ping()
        except Exception as e:
            print(f'Cannot connect to Redis at {self.args.redis_address}:{self.args.redis_port}: {e}', file=sys.stderr)
            sys.exit(1)
        print('Redis connected.')

        # ZMQ connection
        self.context = zmq.Context()
        self.sub = self.context.socket(zmq.SUB)
        self.sub.setsockopt(zmq.SUBSCRIBE, b'')
        self.sub.setsockopt(zmq.RCVHWM, 3)
        self.sub.connect(f'tcp://{self.args.pub_address}')
        print(f'ZMQ SUB connected: tcp://{self.args.pub_address}')

    def decompress(self, raw: bytes, shape, dtype: np.dtype) -> np.ndarray:
        block_size = int.from_bytes(raw[8:12], byteorder='big') / dtype.itemsize
        buf = np.frombuffer(raw[12:], dtype=np.uint8)
        return bitshuffle.decompress_lz4(buf, [shape[1], shape[0]], dtype, block_size)

    def render_jpeg(self, frame: np.ndarray, bit_depth: int) -> bytes:
        data = frame.astype(np.float32)
        mask_val = 2 ** int(bit_depth) - 1
        mask = frame >= mask_val
        data[mask] = 0

        valid = data[~mask]
        if valid.size:
            vmin = float(np.percentile(valid, 50))
            vmax = float(np.percentile(valid, 99.99))
        else:
            vmin, vmax = 0.0, 1.0
        if vmax <= vmin:
            vmax = vmin + 1

        data_norm = np.clip((data - vmin) / (vmax - vmin), 0, 1)
        data_norm[mask] = 0

        if self.args.jpeg_width and self.args.jpeg_width < data_norm.shape[1]:
            factor = data_norm.shape[1] // self.args.jpeg_width
            if factor > 1:
                h, w = data_norm.shape
                h_trim, w_trim = h - h % factor, w - w % factor
                blocks = data_norm[:h_trim, :w_trim].reshape(
                    h_trim // factor, factor, w_trim // factor, factor)
                data_norm = blocks.max(axis=(1, 3))

        idx = (data_norm * 255).astype(np.uint8)
        img = Image.fromarray(_PLASMA_LUT[idx], 'RGB')
        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=self.args.jpeg_quality)
        return buf.getvalue()

    def _monitor_hosts(self):
        if getattr(self.args, 'detector_address', None):
            return [self.args.detector_address]
        return _DCU_HOSTS.get(self.BL, [])

    def _viewer_active(self) -> bool:
        try:
            return bool(self.db.exists(self.KEY_PRESENCE))
        except Exception:
            return False

    def _eiger_stream_silent(self) -> bool:
        """True if the `eiger` stream has had no entry in EIGER_STREAM_SILENCE_S.

        Uses the XID's millisecond timestamp prefix so we don't have to parse
        the message body. Treats any error (stream missing, parse failure) as
        "silent" — conservative for our gating use.
        """
        try:
            last = self.db.xrevrange(EIGER_STREAM, max='+', min='-', count=1)
            if not last:
                return True
            msg_id = last[0][0]
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode()
            ms = int(msg_id.split('-', 1)[0])
            return (time.time() * 1000 - ms) > EIGER_STREAM_SILENCE_S * 1000
        except Exception:
            return True

    def _enable_monitor_mode(self, host: str) -> bool:
        mode_url = f'http://{host}/monitor/api/{MONITOR_API_VERSION}/config/mode'
        try:
            resp = self._mon_session.get(mode_url, timeout=1.0)
            if resp.status_code == 200 and resp.json().get('value', '') != 'enabled':
                self._mon_session.put(mode_url, json={'value': 'enabled'}, timeout=1.0)
            return True
        except Exception as ex:
            print(f'Failed to enable monitor mode on {host}: {ex}')
            return False

    def _publish_monitor_frame(self, jpeg: bytes, frame_data: np.ndarray):
        """Write JPEG/meta keys; XADD to eiger_monitor only when truly idle.

        live:jpeg/meta is always refreshed so the backend can serve a fast
        "monitor preview" during real acquisitions (when /live/frame from disk
        would otherwise be the only option). The XADD is suppressed while the
        `eiger` stream itself has recent entries, so the SSE handler doesn't
        flap between the real series and a synthetic monitor series.
        """
        self.monitor_frame_num += 1
        meta = {
            'frame_num':    self.monitor_frame_num,
            'series_id':    self.monitor_series_id,
            'timestamp':    time.time(),
            'owner':        'monitor',
            'source':       'detector-monitor',
            'beamline':     self.BL,
            'image_width':  int(frame_data.shape[1]),
            'image_height': int(frame_data.shape[0]),
        }
        pipe = self.db.pipeline()
        pipe.set(self.KEY_JPEG, jpeg, ex=self.args.ttl)
        pipe.set(self.KEY_META, json.dumps(meta), ex=self.args.ttl)
        pipe.execute()

        if not self._eiger_stream_silent():
            return   # real acquisition is active — don't fire a synthetic series

        stream_msg = {
            '0': {
                'htype':  'dimage-1.0',
                'series': self.monitor_series_id,
                'frame':  self.monitor_frame_num,
            },
            '4': {
                'prefix':       'monitor',
                'data_dir':     '/tmp',
                'user_dir':     '',
                'username':     'monitor',
                'run_fr_count': 0,
                'beamline':     self.BL,
            },
            'timestamp': time.time(),
        }
        try:
            self.db.xadd(MONITOR_STREAM, {'message': json.dumps(stream_msg)},
                         maxlen=STREAM_MAXLEN, approximate=True)
        except Exception as ex:
            print(f'XADD {MONITOR_STREAM} failed: {ex}')

    def start_monitor_producer(self):
        if not getattr(self.args, 'monitor_fallback', True):
            print('Monitor producer disabled by --no-monitor-fallback.')
            return
        t = threading.Thread(target=self._monitor_producer_loop, daemon=True)
        t.start()

    def _monitor_producer_loop(self):
        """Pull from the DCU monitor API at ~10 Hz when ZMQ is idle and a
        viewer is connected. Long-poll on the DCU side caps the rate naturally;
        no client-side sleep is needed in the happy path."""
        hosts = self._monitor_hosts()
        if not hosts:
            print('No DCU hosts configured for monitor fallback; producer exiting.')
            return

        print(f'Monitor producer started for {self.BL} (DCU candidates: {hosts}).')

        consecutive_failures = MODE_REENABLE_AFTER   # forces enable on first pass
        last_fail_log = ''

        while True:
            # If the sidecar's own ZMQ recv is producing real JPEGs into
            # live:jpeg, there is no reason to also pull from the DCU.
            if time.time() - self.last_zmq_frame_ts < ZMQ_SILENCE_S:
                time.sleep(0.5)
                continue

            if not self._viewer_active():
                time.sleep(1.0)
                continue

            # Re-enable monitor mode on first run and after a streak of failures
            # so a DCU restart mid-session self-heals without periodic polling.
            if consecutive_failures >= MODE_REENABLE_AFTER:
                for h in hosts:
                    if self._enable_monitor_mode(h):
                        break

            fetched = False
            last_err = ''
            for h in hosts:
                try:
                    url = f'http://{h}/monitor/api/{MONITOR_API_VERSION}/images/monitor'
                    resp = self._mon_session.get(url, params={'timeout': MONITOR_POLL_MS}, timeout=1.5)
                    if resp.status_code == 408:
                        # 408 Request Timeout just means no new image was collected
                        # within MONITOR_POLL_MS. The DCU is healthy.
                        fetched = True
                        break
                    resp.raise_for_status()
                    # Dedupe: the DCU returns the same frame to repeated polls
                    # if no new exposure has landed yet (collection slower than
                    # MONITOR_POLL_MS). Skip render+XADD on identical content.
                    content_hash = hashlib.sha1(resp.content).digest()
                    fetched = True
                    if content_hash == self._last_monitor_hash:
                        break
                    self._last_monitor_hash = content_hash
                    img = Image.open(io.BytesIO(resp.content))
                    frame_data = np.array(img)
                    bit_depth = frame_data.dtype.itemsize * 8
                    jpeg = self.render_jpeg(frame_data, bit_depth=bit_depth)
                    self._publish_monitor_frame(jpeg, frame_data)
                    break
                except Exception as e:
                    last_err = f'{h}: {e}'

            if fetched:
                consecutive_failures = 0
                last_fail_log = ''
            else:
                consecutive_failures += 1
                if last_err != last_fail_log:
                    print(f'Monitor pull failed: {last_err}')
                    last_fail_log = last_err
                time.sleep(MONITOR_BACKOFF_S)

    def run(self):
        self.start_monitor_producer()

        print(f'qp2_eiger_live_cache running for {self.BL}.')
        frames_processed = 0
        t_last_log = time.time()
        
        while True:
            try:
                if not self.sub.poll(timeout=2000):
                    continue
                parts = self.sub.recv_multipart()
            except zmq.ZMQError as e:
                print(f'ZMQ error: {e}', file=sys.stderr)
                time.sleep(1)
                continue

            if len(parts) < 3:
                continue

            try:
                part_0 = json.loads(parts[0])
                if part_0.get('htype') != 'dimage-1.0':
                    continue

                part_1 = json.loads(parts[1])
                shape     = part_1['shape']   # [y_pixels, x_pixels]
                dtype     = np.dtype(part_1.get('type', 'uint32'))
                bit_depth = dtype.itemsize * 8

                frame = self.decompress(parts[2], shape, dtype)
                self.last_zmq_frame_ts = time.time()

                img_meta = {}
                if len(parts) > 4:
                    try:
                        img_meta = json.loads(parts[4])
                    except Exception:
                        pass

                meta = {
                    'frame_num':    int(part_0.get('frame', -1)),
                    'series_id':    part_0.get('series'),
                    'timestamp':    time.time(),
                    'owner':        img_meta.get('username', ''),
                    'beam_x':       img_meta.get('xbeam_px'),
                    'beam_y':       img_meta.get('ybeam_px'),
                    'energy_ev':    img_meta.get('energy_eV'),
                    'det_dist_m':   img_meta.get('detector_dist_m'),
                    'exposure_sec': img_meta.get('exposure_sec'),
                    'beamline':     img_meta.get('beamline', self.BL),
                    'image_width':  frame.shape[1],
                    'image_height': frame.shape[0],
                }

                jpeg = self.render_jpeg(frame, bit_depth)

                pipe = self.db.pipeline()
                pipe.set(self.KEY_JPEG, jpeg,                ex=self.args.ttl)
                pipe.set(self.KEY_META, json.dumps(meta),    ex=self.args.ttl)
                if self.args.store_raw:
                    pipe.set(self.KEY_RAW,   parts[2],       ex=self.args.ttl)
                    pipe.set(self.KEY_SHAPE,
                             json.dumps({'shape': shape, 'type': str(dtype)}),
                             ex=self.args.ttl)
                pipe.execute()

                frames_processed += 1
                now = time.time()
                if now - t_last_log >= 30:
                    rate = frames_processed / (now - t_last_log)
                    print(f'[{self.BL}] {rate:.1f} fps | frame={meta["frame_num"]} '
                          f'owner={meta["owner"]}')
                    frames_processed = 0
                    t_last_log = now

            except Exception as e:
                print(f'Frame error: {e}', file=sys.stderr)


def main():
    _hostname = socket.gethostname()
    if _hostname.startswith('bl2'):
        _default_beamline = 'bl2'
        _default_redis    = 'localhost'
    elif _hostname.startswith('bl1'):
        _default_beamline = 'bl1'
        _default_redis    = 'localhost'
    else:
        _default_beamline = 'bl1'
        _default_redis    = 'localhost'

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--beamline', default=_default_beamline,
                        choices=['bl1', 'bl2'],
                        help=f'Beamline identifier (auto-detected: {_default_beamline})')
    parser.add_argument('--pub-address', default='localhost:9900',
                        help='ZMQ PUB address to subscribe to (default: localhost:9900)')
    parser.add_argument('--redis-address', default=_default_redis,
                        help=f'Redis host (auto-detected: {_default_redis})')
    parser.add_argument('--redis-port', type=int, default=6379,
                        help='Redis port (default: 6379)')
    parser.add_argument('--detector-address', default=None,
                        help='Eiger DCU address for monitor API fallback (auto-detected if None)')
    parser.add_argument('--no-monitor-fallback', dest='monitor_fallback',
                        action='store_false',
                        help='Disable the monitor-API producer thread')
    parser.add_argument('--store-raw', action='store_true',
                        help='Also store compressed raw frame bytes in Redis')
    parser.add_argument('--jpeg-width', type=int, default=800,
                        help='Rendered JPEG width in pixels (default: 800)')
    parser.add_argument('--jpeg-quality', type=int, default=85,
                        help='JPEG quality 1-95 (default: 85)')
    parser.add_argument('--ttl', type=int, default=300,
                        help='Redis key TTL in seconds (default: 300)')
    
    args = parser.parse_args()

    cache = EigerLiveCache(args, _hostname)
    cache.run()


if __name__ == '__main__':
    main()
