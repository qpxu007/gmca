#!/mnt/beegfs/qxu/data-analysis/qp2_env/bin/python
"""
migrate_mysql_to_postgres.py

Migrates per-beamline MySQL databases (bl1upper, bl2upper) into the shared
PostgreSQL database on bl1ws1.

Primary key conflicts between the two beamline databases are resolved by
applying a large numeric offset to all BL2 IDs (same offset applied to both
PKs and FKs so referential integrity is preserved):

    BL1 offset =          0  (IDs stay as-is)
    BL2 offset = 10_000_000

The combined data reaches at most ~87k rows per table, well below 10M.
After import the Postgres sequences are advanced to 20_000_001 so all
future writes from either beamline are safely above both import ranges.

Tables migrated (in FK dependency order):
    dataset_runs          PK: data_id
    pipelinestatus        PK: id          FK: dataset_run_id → dataset_runs
    dataprocessresults    PK: id          FK: pipelinestatus_id → pipelinestatus
    screenstrategyresults PK: sampleNumber FK: pipelinestatus_id → pipelinestatus
    spreadsheets          PK: id

Usage:
    python migrate_mysql_to_postgres.py [--dry-run] [--beamline bl1|bl2|both]

    --dry-run               Print counts and bail; nothing is written.
    --beamline bl1|bl2|both Which source to migrate (default: both).
    --skip-reset-sequences  Don't advance Postgres sequences after import.
"""

import argparse
import logging
import sys
from contextlib import contextmanager

from sqlalchemy import create_engine, inspect, text

# Allow running from the repo root without installing the package
sys.path.insert(0, '/mnt/beegfs/qxu/data-analysis')
from qp2.config.servers import ServerConfig  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger('migrate')

# ---------------------------------------------------------------------------
# Connection config (pulled from the project's ServerConfig)
# ---------------------------------------------------------------------------

def _mysql_url(host):
    user = ServerConfig.MYSQL_USER
    pw = ServerConfig.MYSQL_PASS
    db = ServerConfig.MYSQL_DB_USER_DATA
    auth = f'{user}:{pw}@' if pw else f'{user}@'
    return f'mysql+pymysql://{auth}{host}/{db}'


BL_CONFIGS = {
    'bl1': {
        'mysql_url': _mysql_url(ServerConfig.MYSQL_HOST_BL1),
        'offset':    0,
    },
    'bl2': {
        'mysql_url': _mysql_url(ServerConfig.MYSQL_HOST_BL2),
        'offset':    10_000_000,
    },
}

PG_URL = ServerConfig.get_postgres_url()

# ---------------------------------------------------------------------------
# Migration plan
# Each tuple: (table_name, pk_column, [fk_columns_to_remap])
# FK columns get the same offset applied as the PK (same-table offset rule).
# ---------------------------------------------------------------------------

MIGRATION_ORDER = [
    ('dataset_runs',          'data_id',       []),
    ('pipelinestatus',        'id',            ['dataset_run_id']),
    ('dataprocessresults',    'id',            ['pipelinestatus_id']),
    ('screenstrategyresults', 'sampleNumber',  ['pipelinestatus_id']),
    ('spreadsheets',          'id',            []),
]

# These tables have no beamline column in MySQL; we inject it from the source label.
INJECT_BEAMLINE_TABLES = {'dataset_runs', 'spreadsheets'}

# Postgres sequence names (SQLAlchemy / Postgres auto-naming convention).
SEQUENCES = [
    ('dataset_runs',          'data_id',       'dataset_runs_data_id_seq'),
    ('pipelinestatus',        'id',            'pipelinestatus_id_seq'),
    ('dataprocessresults',    'id',            'dataprocessresults_id_seq'),
    ('screenstrategyresults', 'sampleNumber',  'screenstrategyresults_sampleNumber_seq'),
    ('spreadsheets',          'id',            'spreadsheets_id_seq'),
]

MIN_NEXT_VAL = 20_000_001  # Sequences start here after import


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextmanager
def engine_ctx(url, label):
    eng = create_engine(url, pool_pre_ping=True)
    try:
        yield eng
    finally:
        eng.dispose()
        log.debug(f'Disposed engine for {label}')


def table_columns(engine, table_name):
    """Return set of column names in *table_name*, or None if table absent."""
    insp = inspect(engine)
    if not insp.has_table(table_name):
        return None
    return {col['name'] for col in insp.get_columns(table_name)}


def ensure_beamline_columns(pg_engine):
    """Add beamline VARCHAR(20) to Postgres tables that need it, if absent.

    Requires table ownership. If the current user lacks ALTER TABLE privilege,
    logs a warning and continues — migrate_table() will skip beamline injection
    for any table where the column is absent, so the migration still completes.
    To add the column manually run as a superuser:
        ALTER TABLE dataset_runs ADD COLUMN IF NOT EXISTS beamline VARCHAR(20);
    """
    with pg_engine.connect() as conn:
        for tbl in INJECT_BEAMLINE_TABLES:
            cols = table_columns(pg_engine, tbl)
            if cols is None:
                log.warning(f'Table {tbl!r} missing in Postgres — skipping beamline column')
                continue
            if 'beamline' not in cols:
                try:
                    log.info(f'Adding beamline column to {tbl}')
                    conn.execute(text(f'ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS beamline VARCHAR(20)'))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    log.warning(
                        f'Could not add beamline column to {tbl} ({e}). '
                        f'Run as superuser: '
                        f'ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS beamline VARCHAR(20);'
                        f' — beamline will be NULL for migrated rows in this table.'
                    )


def reset_sequences(pg_engine):
    """Advance each sequence so new rows land safely above all imported IDs.

    Requires sequence ownership. If the current user lacks privilege, prints
    the manual commands to run as a superuser.
    """
    manual_cmds = []
    with pg_engine.connect() as conn:
        for tbl, pk_col, seq_name in SEQUENCES:
            max_id = conn.execute(text(f'SELECT MAX("{pk_col}") FROM {tbl}')).scalar() or 0
            next_val = max(max_id + 1, MIN_NEXT_VAL)
            exists = conn.execute(
                text("SELECT 1 FROM pg_sequences WHERE sequencename = :s"),
                {'s': seq_name},
            ).scalar()
            if not exists:
                log.warning(f'  sequence {seq_name!r} not found — skipping')
                continue
            try:
                conn.execute(text(f'SELECT setval(\'"{seq_name}"\', {next_val}, false)'))
                conn.commit()
                log.info(f'  sequence {seq_name!r} → next = {next_val:,}')
            except Exception as e:
                conn.rollback()
                log.warning(f'  sequence {seq_name!r} — permission denied, skipping')
                manual_cmds.append(f'SELECT setval(\'"{seq_name}"\', {next_val}, false);')

    if manual_cmds:
        log.warning(
            'Could not reset sequences (insufficient privilege). '
            'Run the following as a superuser:\n    ' + '\n    '.join(manual_cmds)
        )


# ---------------------------------------------------------------------------
# Per-table migration
# ---------------------------------------------------------------------------

def migrate_table(table_name, pk_col, fk_cols,
                  src_conn, pg_engine, offset, beamline_label, dry_run):
    """Read all rows from MySQL, remap IDs, bulk-insert into Postgres."""

    rows = src_conn.execute(text(f'SELECT * FROM `{table_name}`')).mappings().all()
    if not rows:
        log.info(f'    {table_name}: 0 rows — nothing to do')
        return 0

    src_col_set = set(rows[0].keys())
    dst_col_set = table_columns(pg_engine, table_name)
    if dst_col_set is None:
        log.error(f'    {table_name}: not found in Postgres — skipping')
        return 0

    # Columns present in both source and destination
    common_cols = [c for c in rows[0].keys() if c in dst_col_set]

    # Decide whether to inject beamline
    inject_bl = (table_name in INJECT_BEAMLINE_TABLES
                 and 'beamline' not in src_col_set
                 and 'beamline' in dst_col_set)
    if inject_bl:
        common_cols = common_cols + ['beamline']

    col_list    = ', '.join(f'"{c}"' for c in common_cols)
    placeholder = ', '.join(f':{c}' for c in common_cols)
    insert_sql  = text(
        f'INSERT INTO {table_name} ({col_list}) VALUES ({placeholder}) '
        f'ON CONFLICT ("{pk_col}") DO NOTHING'
    )

    # Build remapped row dicts
    batch = []
    for row in rows:
        r = dict(row)

        # Remap primary key
        r[pk_col] = r[pk_col] + offset

        # Remap foreign keys (same offset)
        for fk in fk_cols:
            if fk in r and r[fk] is not None:
                r[fk] = r[fk] + offset

        # Inject or backfill beamline
        if inject_bl:
            r['beamline'] = beamline_label
        elif 'beamline' in src_col_set and not r.get('beamline'):
            r['beamline'] = beamline_label

        batch.append({c: r.get(c) for c in common_cols})

    if dry_run:
        log.info(f'    [DRY RUN] {table_name}: {len(batch):,} rows would be inserted (offset={offset:,})')
        return len(batch)

    with pg_engine.connect() as dst_conn:
        result = dst_conn.execute(insert_sql, batch)
        dst_conn.commit()

    inserted = result.rowcount if result.rowcount >= 0 else len(batch)
    skipped  = len(batch) - inserted
    log.info(f'    {table_name}: {len(batch):,} read → {inserted:,} inserted, {skipped:,} skipped (conflict)')
    return inserted


# ---------------------------------------------------------------------------
# Per-beamline orchestration
# ---------------------------------------------------------------------------

def migrate_beamline(bl_name, config, pg_engine, dry_run):
    offset = config['offset']
    log.info(f'{"═"*60}')
    log.info(f'  Migrating {bl_name.upper()}  (offset = {offset:,})')
    log.info(f'{"═"*60}')

    with engine_ctx(config['mysql_url'], f'MySQL/{bl_name}') as mysql_eng, \
         mysql_eng.connect() as src_conn:

        total = 0
        for table_name, pk_col, fk_cols in MIGRATION_ORDER:
            n = migrate_table(
                table_name, pk_col, fk_cols,
                src_conn, pg_engine,
                offset=offset,
                beamline_label=bl_name,
                dry_run=dry_run,
            )
            total += n

    log.info(f'  {bl_name.upper()} complete — {total:,} rows processed\n')


# ---------------------------------------------------------------------------
# Verification query (post-import sanity check)
# ---------------------------------------------------------------------------

def verify(pg_engine):
    checks = [
        # Orphaned dataprocessresults (FK → pipelinestatus)
        ("Orphaned dataprocessresults",
         "SELECT COUNT(*) FROM dataprocessresults d "
         "LEFT JOIN pipelinestatus p ON d.pipelinestatus_id = p.id "
         "WHERE p.id IS NULL"),
        # Orphaned screenstrategyresults
        ("Orphaned screenstrategyresults",
         "SELECT COUNT(*) FROM screenstrategyresults s "
         "LEFT JOIN pipelinestatus p ON s.pipelinestatus_id = p.id "
         "WHERE p.id IS NULL"),
        # Orphaned pipelinestatus → dataset_runs
        ("Orphaned pipelinestatus (dataset_run_id)",
         "SELECT COUNT(*) FROM pipelinestatus p "
         "LEFT JOIN dataset_runs d ON p.dataset_run_id = d.data_id "
         "WHERE p.dataset_run_id IS NOT NULL AND d.data_id IS NULL"),
    ]

    log.info('─── FK integrity verification ───')
    all_ok = True
    with pg_engine.connect() as conn:
        for label, sql in checks:
            n = conn.execute(text(sql)).scalar()
            status = 'OK' if n == 0 else f'WARN — {n} orphaned rows!'
            log.info(f'  {label}: {status}')
            if n:
                all_ok = False

        # Row counts per beamline
        log.info('─── Row counts per beamline ───')
        for tbl, pk_col, _ in SEQUENCES:
            bl_col_exists = 'beamline' in (table_columns(pg_engine, tbl) or set())
            if bl_col_exists:
                rows = conn.execute(text(
                    f'SELECT beamline, COUNT(*) FROM {tbl} GROUP BY beamline ORDER BY beamline'
                )).all()
                for bl, cnt in rows:
                    log.info(f'  {tbl}: {bl or "(null)":8s}  {cnt:,} rows')
            else:
                cnt = conn.execute(text(f'SELECT COUNT(*) FROM {tbl}')).scalar()
                log.info(f'  {tbl}: {cnt:,} rows total')

    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Migrate per-beamline MySQL user_data → shared Postgres on bl1ws1'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Count rows without writing anything')
    parser.add_argument('--beamline', choices=['bl1', 'bl2', 'both'], default='both',
                        help='Which beamline to migrate (default: both)')
    parser.add_argument('--skip-reset-sequences', action='store_true',
                        help='Skip advancing Postgres sequences after import')
    parser.add_argument('--verify-only', action='store_true',
                        help='Run FK integrity checks only, no migration')
    args = parser.parse_args()

    if args.dry_run:
        log.info('*** DRY RUN mode — no data will be written ***\n')

    with engine_ctx(PG_URL, 'Postgres') as pg_engine:

        if args.verify_only:
            ok = verify(pg_engine)
            sys.exit(0 if ok else 1)

        # Step 1: ensure beamline columns exist in Postgres
        if not args.dry_run:
            ensure_beamline_columns(pg_engine)

        # Step 2: migrate selected beamlines
        beamlines = ['bl1', 'bl2'] if args.beamline == 'both' else [args.beamline]
        for bl in beamlines:
            migrate_beamline(bl, BL_CONFIGS[bl], pg_engine, dry_run=args.dry_run)

        # Step 3: reset sequences
        if not args.dry_run and not args.skip_reset_sequences:
            log.info('─── Resetting Postgres sequences ───')
            reset_sequences(pg_engine)

        # Step 4: verify
        if not args.dry_run:
            verify(pg_engine)

    log.info('Done.')


if __name__ == '__main__':
    main()
