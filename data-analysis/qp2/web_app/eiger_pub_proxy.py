#!/usr/bin/env python3
"""Eiger ZMQ proxy with viewer PUB socket.

Drop-in replacement for the production eiger_proxy.py that adds a ZMQ PUB
socket so web viewers can subscribe without stealing frames from processing
workers. Production files are not modified.

Usage:
    python eiger_pub_proxy.py \\
        --eiger_address 10.42.42.10 \\
        --redis_address 10.20.103.85 \\
        --push_port 9800 \\
        --pub_port  9900

The --push_port (PUSH to workers) replicates what the production proxy does on
zmq_data_servers ports. The --pub_port is new and only used by web viewers.
"""

import argparse
import json
import sys
import time

import redis
import zmq

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--eiger_address', required=True, help='Eiger detector IP')
parser.add_argument('--redis_address', default='localhost', help='Redis server IP')
parser.add_argument('--push_port', type=int, default=9800, help='PUSH port for processing workers')
parser.add_argument('--pub_port',  type=int, default=9900, help='PUB port for web viewers')
args = parser.parse_args()

# ── Redis ──────────────────────────────────────────────────────────────────────
db = redis.Redis(host=args.redis_address, decode_responses=True)
if not db.ping():
    print(f'Cannot connect to Redis at {args.redis_address}. Exiting.', file=sys.stderr)
    sys.exit(1)
print(f'Redis connected: {args.redis_address}')

# ── ZMQ sockets ───────────────────────────────────────────────────────────────
context = zmq.Context()

# PULL from detector (same as production proxy)
recv = context.socket(zmq.PULL)
recv.setsockopt(zmq.RCVHWM, 1)
recv.connect(f'tcp://{args.eiger_address}:9999')
print(f'PULL connected: {args.eiger_address}:9999')

# PUSH to processing workers (same as production proxy)
send = context.socket(zmq.PUSH)
send.setsockopt(zmq.SNDHWM, 2000)
send.bind(f'tcp://0.0.0.0:{args.push_port}')
print(f'PUSH bound: 0.0.0.0:{args.push_port}')

# PUB to web viewers (new — zero impact when no subscriber)
pub = context.socket(zmq.PUB)
pub.setsockopt(zmq.SNDHWM, 3)   # drop rather than buffer for slow viewers
pub.bind(f'tcp://0.0.0.0:{args.pub_port}')
print(f'PUB  bound: 0.0.0.0:{args.pub_port}')

# Parts to skip when writing to Redis (matches production proxy exactly)
_SKIP = {
    'dheader-1.0':    [4, 6, 8],
    'dimage-1.0':     [3],
    'dseries_end-1.0': [],
}

print('Proxy loop started.')
while True:
    parts = recv.recv_multipart()
    part_0 = json.loads(parts[0])
    htype = part_0.get('htype', '')

    # ── Redis stream (metadata only, same as production) ──────────────────────
    skip = _SKIP.get(htype)
    if skip is not None:
        all_parts = {}
        for idx, msg in enumerate(parts):
            if idx + 1 not in skip:
                try:
                    all_parts[idx] = json.loads(msg)
                except Exception:
                    pass
        all_parts['timestamp'] = time.time()
        db.xadd('eiger', {'message': json.dumps(all_parts)})

    # ── Forward to processing workers (same as production) ────────────────────
    try:
        send.send_multipart(parts, flags=zmq.NOBLOCK)
    except zmq.error.Again:
        pass   # worker queue full — drop (same behaviour as production)
    except zmq.ZMQError as e:
        print(f'PUSH error: {e}', file=sys.stderr)

    # ── Publish image frames to web viewers (new) ─────────────────────────────
    if htype == 'dimage-1.0':
        try:
            pub.send_multipart(parts, flags=zmq.NOBLOCK)
        except zmq.error.Again:
            pass   # no viewer or viewer too slow — drop silently
