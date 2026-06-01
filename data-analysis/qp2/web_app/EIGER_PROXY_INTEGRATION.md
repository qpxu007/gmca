# Eiger Proxy — Live Viewer Integration

Two deployment strategies for adding a ZMQ PUB socket to the Eiger proxy
so the web live viewer can receive frames without stealing from processing workers.

---

## Architecture overview

```
Detector (10.42.x.x)
    ↓  ZMQ PULL
eiger_proxy  (bl1epu / bl2epu)
    ├─ ZMQ PUSH  → processing workers (unchanged)
    ├─ Redis XADD → "eiger" stream (metadata, unchanged)
    └─ ZMQ PUB   → eiger_live_cache.py  [new]
                        ↓
                   Redis SET live:jpeg:{bl}
                        ↓
                   Web backend GET /viewer/live/latest
                        ↓
                   Browser live viewer
```

---

## Strategy A — Minimal patch to production `eiger_proxy.py`

10 lines added (marked `+`). The `--pub_port` argument is **optional** — if
not supplied the behaviour is identical to the original script.

```diff
  parser.add_argument('--eiger_address')
  parser.add_argument('--listen_port', help='Port to forward ZMQ stream to')
+ parser.add_argument('--pub_port', default=None, help='Port to publish frames for viewers')
  args = parser.parse_known_args()[0]

  send = context.socket(zmq.PUSH)
  send.setsockopt(zmq.SNDHWM, 2000)
  send.bind('tcp://0.0.0.0:{}'.format(args.listen_port))

+ pub = None
+ if args.pub_port:
+     pub = context.socket(zmq.PUB)
+     pub.setsockopt(zmq.SNDHWM, 3)   # drop if viewer is slow — never buffer
+     pub.bind('tcp://0.0.0.0:{}'.format(args.pub_port))

  # ... rest of file unchanged ...

  # inside the loop, after the existing send block:
+     if pub and part_0['htype'] == 'dimage-1.0':
+         try:
+             pub.send_multipart(parts, flags=zmq.NOBLOCK)
+         except zmq.error.Again:
+             pass
```

Invocation (add `--pub_port`; everything else unchanged):

```bash
# BL1
python eiger_proxy.py --eiger_address 10.42.42.10 --listen_port 9800 --pub_port 9900

# BL2
python eiger_proxy.py --eiger_address 10.42.103.10 --listen_port 9800 --pub_port 9900
```

---

## Strategy B — Drop-in replacement (`eiger_pub_proxy.py`)

`eiger_pub_proxy.py` (in this directory) is a self-contained replacement for
the production proxy. Use this if patching the production script is not preferred.

```bash
# BL1 — stop production proxy, start replacement
python eiger_pub_proxy.py \
    --eiger_address 10.42.42.10 \
    --redis_address 10.20.103.85 \
    --push_port 9800 \
    --pub_port  9900

# BL2
python eiger_pub_proxy.py \
    --eiger_address 10.42.103.10 \
    --redis_address 10.20.103.154 \
    --push_port 9800 \
    --pub_port  9900
```

---

## Live frame cache (`eiger_live_cache.py`)

Runs on the EPU machine alongside the proxy. Subscribes to the PUB socket,
decodes bitshuffle frames, renders JPEGs, and writes them to the local Redis
instance so the web backend can serve live frames without ZMQ or bitshuffle.

**Beamline and Redis address are auto-detected from the machine hostname**
(`bl1*` → bl1, `bl2*` → bl2). All arguments can be overridden explicitly.

```bash
# Minimal — hostname auto-detects beamline and Redis address
python eiger_live_cache.py

# With raw frame caching for in-memory analysis
python eiger_live_cache.py --store-raw

# Explicit overrides
python eiger_live_cache.py --beamline bl1 --pub-address localhost:9900 --store-raw
```

### Redis keys written (TTL 300s)

| Key | Content | Size |
|-----|---------|------|
| `live:jpeg:{bl}` | Rendered JPEG | ~300 KB |
| `live:meta:{bl}` | JSON metadata (frame#, owner, energy, beam centre, …) | ~1 KB |
| `live:raw:{bl}` | bitshuffle-compressed uint32 bytes `[--store-raw]` | ~3 MB |
| `live:shape:{bl}` | JSON `{"shape":[h,w], "type":"uint32"}` `[--store-raw]` | <1 KB |

**Total memory on Redis per beamline:** ~300 KB (JPEG only) or ~3.3 MB (with raw).
Both beamlines: ~600 KB or ~6.6 MB — negligible on EPU machines.

### Reading raw frames from any machine

```python
import bitshuffle, numpy as np, json, redis

db    = redis.Redis(host='10.20.103.85')   # bl1epu
raw   = db.get('live:raw:bl1')
info  = json.loads(db.get('live:shape:bl1'))
dtype = np.dtype(info['type'])
h, w  = info['shape']
block = int.from_bytes(raw[8:12], 'big') / dtype.itemsize
frame = bitshuffle.decompress_lz4(np.frombuffer(raw[12:], np.uint8),
                                   [w, h], dtype, block)
```

---

## Systemd service (`eiger-live-cache.service`)

Install on bl1epu and bl2epu:

```bash
sudo cp eiger-live-cache.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable eiger-live-cache
sudo systemctl start  eiger-live-cache
sudo systemctl status eiger-live-cache

# Logs
sudo journalctl -u eiger-live-cache -f
# or
tail -f /var/log/eiger-live-cache.log
```

The service uses the **pybluice virtualenv** (`/opt/venv/pybluice/bin/python`)
which already has `zmq`, `bitshuffle`, `numpy`, `Pillow`, and `matplotlib`
installed from the streamproc stack. It starts after `streamproc.service`
and restarts automatically on failure.

To enable raw frame caching, edit the service file and uncomment the
`--store-raw` ExecStart line, then `sudo systemctl daemon-reload && sudo systemctl restart eiger-live-cache`.

---

## Web backend

The web backend reads from the sidecar's Redis keys via `GET /viewer/live/latest`.
No configuration needed — Redis clients for bl1epu and bl2epu are initialised
at startup using `ServerConfig.get_redis_hosts()`.

**Priority chain for a live frame request:**

```
1. Sidecar Redis (live:jpeg:{bl})   — instant, preferred
2. live_zmq in-process ZMQ cache   — fallback if sidecar absent
3. HDF5 file via /live/frame        — last resort, up to 120s wait
```

**Web backend env vars** (only needed if EPU IPs differ from defaults):

```bash
EIGER_PUB_BL1=tcp://10.20.103.85:9900
EIGER_PUB_BL2=tcp://10.20.103.154:9900
```

---

## ZMQ PUB overhead on the proxy

- **CPU**: one extra non-blocking `send_multipart` per frame — microseconds
- **Memory**: SNDHWM=3 caps the PUB send buffer at ~6 MB per subscriber
- **Network**: ~10–20 MB/s to the live_cache subscriber on localhost — negligible
- **Impact on workers**: zero — PUB is independent of the PUSH socket
- **When no subscriber connected**: ZMQ drops immediately, no buffering

---

## Address reference

| Machine | Role | ZMQ PUSH | ZMQ PUB | Redis |
|---------|------|-----------|---------|-------|
| bl1epu (`10.20.103.85`) | BL1 proxy + cache | :9800 | :9900 | :6379 |
| bl2epu (`10.20.103.154`) | BL2 proxy + cache | :9800 | :9900 | :6379 |
