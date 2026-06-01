"""Crystal-snapshot indexer.

Polls pybluice's Redis event list (``ra.sample.events__l``) for CAMERA
events and inserts one ``CrystalSnapshot`` row per snapshot. Reads
non-destructively (LRANGE) so existing pybluice GUI consumers
(``sample_tree.py``, ``exp_log.py``) are not affected. Idempotency is
enforced by the unique constraint on ``file_path``.

Upstream dependency
-------------------
This indexer depends on pybluice emitting CAMERA events. As of writing
the ``ha.log_event(type='CAMERA', ...)`` call in
``pbs/scripts/camera_snapshot.py`` is commented out, so no events
arrive. The indexer runs benignly in that state — it polls a missing
or empty key and inserts nothing. When pybluice ships the patch,
indexing starts automatically, no qp2 code change needed.

Expected event payload
----------------------
Each entry in ``ra.sample.events__l`` is a JSON-serialised dict. We
require at minimum::

    {"type": "CAMERA", "data": "/abs/path/to/snap.jpg", "unix_ts": 1700000000.0}

Optional fields we extract when present::

    sample_id, port, esaf_id, beamline, username, omega, camera_id

If the payload format pybluice eventually ships differs from this, only
``_event_to_row`` needs adjusting.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from qp2.db import CrystalSnapshot, DatasetRun
from qp2.log.logging_config import get_logger
from qp2.xio.db_manager import DBManager
from qp2.xio.redis_manager import get_redis_server

logger = get_logger(__name__)

EVENT_LIST_KEY = "ra.sample.events__l"
LAST_SEEN_KEY = "qp2:snapshot_indexer:last_seen_unix_ts"
DEFAULT_POLL_SECONDS = 30

# Auto-screening filename: "{prefix}_{angle:.0f}__HighRes.jpg"
_AUTO_NAME_RE = re.compile(r"^(?P<prefix>.+)_(?P<angle>-?\d+)__(?P<cam>\w+)\.jpe?g$", re.IGNORECASE)


class SnapshotIndexer:
    """Background poller that mirrors pybluice CAMERA events into qp2.db."""

    def __init__(
        self,
        redis_conn: Any,
        db_manager: DBManager,
        poll_seconds: int = DEFAULT_POLL_SECONDS,
    ):
        self._redis = redis_conn
        self._db = db_manager
        self._poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @classmethod
    def from_bluice(
        cls,
        db_manager: DBManager,
        beamline: Optional[str] = None,
        poll_seconds: int = DEFAULT_POLL_SECONDS,
    ) -> Optional["SnapshotIndexer"]:
        """Build an indexer pointed at the local bluice Redis.

        Returns None if no bluice Redis host is configured (production
        deployments without bluice integration). Caller can ignore the
        None — qp2 simply runs without snapshot indexing.
        """
        host_string = get_redis_server(beamline=beamline, location="redis")
        if not host_string:
            logger.info(
                "snapshot indexer: no bluice Redis host configured; skipping start"
            )
            return None
        try:
            import redis as _redis
            host, _, port = host_string.partition(":")
            client = _redis.Redis(
                host=host or "localhost",
                port=int(port) if port else 6379,
                decode_responses=True,
                socket_timeout=5,
            )
            client.ping()
        except Exception as e:
            logger.warning(
                "snapshot indexer: cannot connect to bluice Redis at %s (%s); skipping start",
                host_string, e,
            )
            return None
        return cls(client, db_manager, poll_seconds=poll_seconds)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="snapshot-indexer", daemon=True
        )
        self._thread.start()
        logger.info("snapshot indexer started (polling every %ss)", self._poll_seconds)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._poll_once()
            except Exception:
                logger.exception("snapshot indexer poll failed; will retry")
            self._stop.wait(self._poll_seconds)

    def _poll_once(self) -> None:
        if self._redis is None:
            return
        try:
            raw_items = self._redis.lrange(EVENT_LIST_KEY, 0, -1)
        except Exception as e:
            # Redis temporarily unavailable; next poll will retry
            logger.debug("redis lrange failed: %s", e)
            return
        if not raw_items:
            return

        last_seen = self._get_last_seen_ts()
        max_seen = last_seen
        inserted = 0
        for raw in raw_items:
            event = _parse_event(raw)
            if event is None:
                continue
            if event.get("type") != "CAMERA":
                continue
            ts = float(event.get("unix_ts") or 0)
            if ts <= last_seen:
                continue
            if self._insert_one(event):
                inserted += 1
            if ts > max_seen:
                max_seen = ts

        if max_seen > last_seen:
            self._set_last_seen_ts(max_seen)
        if inserted:
            logger.info("snapshot indexer: %s new CAMERA event(s) indexed", inserted)

    def _insert_one(self, event: dict) -> bool:
        row = _event_to_row(event)
        if row is None:
            return False
        try:
            with self._db.get_session() as session:
                # Resolve dataset_run_id via port+beamline+time window
                row.dataset_run_id = _resolve_dataset_run_id(session, row)
                session.add(row)
                session.commit()
                return True
        except IntegrityError:
            # Already indexed (unique file_path) — fine, this is the dedupe path
            return False
        except Exception:
            logger.exception("failed to insert snapshot row for %s", row.file_path)
            return False

    def _get_last_seen_ts(self) -> float:
        try:
            v = self._redis.get(LAST_SEEN_KEY)
            return float(v) if v is not None else 0.0
        except Exception:
            return 0.0

    def _set_last_seen_ts(self, ts: float) -> None:
        try:
            self._redis.set(LAST_SEEN_KEY, str(ts))
        except Exception:
            logger.debug("failed to update last_seen_ts; non-fatal")


def _parse_event(raw: Any) -> Optional[dict]:
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(raw, str):
        return None
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def _event_to_row(event: dict) -> Optional[CrystalSnapshot]:
    """Map a CAMERA event dict to a CrystalSnapshot row.

    The only strictly required field is ``data`` (the absolute JPEG
    path). Everything else is best-effort.
    """
    file_path = event.get("data")
    if not isinstance(file_path, str) or not file_path:
        return None

    ts = event.get("unix_ts")
    captured_at = datetime.utcfromtimestamp(float(ts)) if ts else datetime.utcnow()

    # Extract prefix/angle/camera from the filename if it matches the
    # auto-screening pattern; otherwise leave those fields blank.
    name = os.path.basename(file_path)
    m = _AUTO_NAME_RE.match(name)
    sample_prefix = event.get("sample_id") or (m.group("prefix") if m else None)
    omega = event.get("omega")
    if omega is None and m:
        try:
            omega = float(m.group("angle"))
        except (TypeError, ValueError):
            omega = None
    camera_id = event.get("camera_id") or (m.group("cam") if m else "HighRes")

    file_size: Optional[int] = None
    try:
        file_size = Path(file_path).stat().st_size
    except OSError:
        file_size = None

    return CrystalSnapshot(
        file_path=file_path,
        captured_at=captured_at,
        esaf_id=event.get("esaf_id"),
        beamline=event.get("beamline"),
        username=event.get("username"),
        port=event.get("port"),
        sample_prefix=sample_prefix,
        omega=omega,
        camera_id=camera_id,
        file_size=file_size,
    )


def _resolve_dataset_run_id(session: Session, row: CrystalSnapshot) -> Optional[int]:
    """Find a matching DatasetRun by port + beamline + time window.

    Returns None if no clear match exists; the row stays unlinked and
    the API endpoint will fall back to the same implicit-match heuristic
    at query time.
    """
    if not row.port or not row.beamline:
        return None
    window_start = row.captured_at - timedelta(hours=1)
    window_end = row.captured_at + timedelta(hours=4)
    run = (
        session.query(DatasetRun.data_id)
        .filter(
            DatasetRun.mounted == row.port,
            DatasetRun.beamline == row.beamline,
            DatasetRun.created_at >= window_start,
            DatasetRun.created_at <= window_end,
        )
        .order_by(DatasetRun.created_at.desc())
        .first()
    )
    return run[0] if run else None
