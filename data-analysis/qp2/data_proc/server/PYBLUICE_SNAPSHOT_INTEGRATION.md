# Pybluice patch required for crystal-snapshot indexing

The `CrystalSnapshot` model (`qp2/db/models/crystals.py`) and the
`SnapshotIndexer` thread (`qp2/data_proc/server/snapshot_indexer.py`)
are deployed but **dormant**. They depend on a small pybluice change
that has not yet been made.

## What qp2 needs from pybluice

For each JPEG captured by the sample-viewer camera, pybluice must push
a JSON-encoded CAMERA event onto the Redis list `ra.sample.events__l`.

The exact change is to **uncomment** the existing call at
`pybluice/src/pbs/scripts/camera_snapshot.py:42-45`:

```python
ha.log_event(
    type='CAMERA', result='OK', data=abs_img_path, output=''
).post()
```

That alone unblocks indexing. The indexer will start writing rows as
soon as events appear in Redis — no qp2 change, restart, or
configuration is required when this happens.

## Recommended enrichment (optional but better UX)

The minimal event above gives qp2 only the file path and timestamp.
Indexer rows will have `port`, `beamline`, `esaf_id`, `username`,
`omega` all `NULL`, which means the dataset-viewer UI cannot link
snapshots to the right `DatasetRun` automatically — it falls back to
manual file-path inspection.

If pybluice also includes these fields in the event payload, the link
happens automatically:

```python
ha.log_event(
    type='CAMERA',
    result='OK',
    data=abs_img_path,
    output='',
    # Enrichment — qp2 picks these up if present, gracefully degrades if not:
    port=ra.robot.mounted.get(),         # e.g. "A1"
    esaf_id=ra.user.esaf_id.get(),       # e.g. "123456"
    beamline=ra.config.beamline.get(),   # e.g. "23IDB"
    username=ra.user.username.get(),
    omega=ra.pmac_motor.actPos.get('omega'),
    camera_id='HighRes',                 # or 'LowRes'
).post()
```

All enrichment fields are optional. qp2's `_event_to_row` reads
`event.get(...)` for each, so missing keys produce `NULL` columns
rather than errors.

## What qp2 does today (pre-patch)

| Component | Behaviour |
|---|---|
| `crystal_snapshots` table | Exists, empty |
| `SnapshotIndexer` thread | Started by `ProcessingServer.start()`. Polls `ra.sample.events__l` every 30s via LRANGE. Finds nothing. Inserts nothing. No errors logged. |
| `GET /datasets/{id}/snapshots` | Returns `[]` |
| `GET /snapshots/{id}/image` | Returns `404` (no rows exist to fetch) |

## Verification once pybluice is patched

After enabling `log_event`, take one snapshot and verify:

```bash
# 1. event landed in Redis
redis-cli -h <bluice-redis> LRANGE ra.sample.events__l -3 -1

# 2. indexer picked it up (within ~30s)
psql -c "SELECT id, file_path, captured_at, port FROM crystal_snapshots ORDER BY id DESC LIMIT 5"

# 3. API returns it
curl -s -H "Authorization: Bearer $TOKEN" https://<host>/datasets/<data_id>/snapshots | jq
```

## Constraints assumed by the indexer

- The Redis list contains JSON-serialised dicts (one event per list
  entry). If pybluice uses a different serialisation (pickle, msgpack),
  `_parse_event` in `snapshot_indexer.py` needs adjustment.
- Reads are non-destructive (LRANGE only). Existing pybluice GUI
  consumers (`sample_tree.py`, `exp_log.py`) are not affected.
- Duplicates are deduped by the unique constraint on
  `crystal_snapshots.file_path` — safe to replay events.

## Disk lifecycle

qp2 does **not** delete JPEGs. Snapshot files accumulate on BeegFS at
their original paths. If a JPEG is purged externally (storage policy,
manual cleanup, ArchiveJob), the qp2 row persists with a now-dead
`file_path` and the image-stream endpoint returns `410 Gone`.
