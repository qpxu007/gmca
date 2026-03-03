#!/bin/bash

# Configuration with defaults
HOST=${QP2_PG_HOST:-localhost}
USER=${QP2_PG_USER:-dhs}
DB=${QP2_PG_DB:-user_data}

echo "Applying V5 schema migration to database '${DB}' on '${HOST}' as user '${USER}'..."

# Run the migration
# We use IF NOT EXISTS to make it idempotent
psql -h "$HOST" -U "$USER" -d "$DB" -c '
ALTER TABLE "dataset_runs" ADD COLUMN IF NOT EXISTS "meta_user" TEXT DEFAULT NULL;
ALTER TABLE "dataset_runs" ADD COLUMN IF NOT EXISTS "mounted" VARCHAR(255) DEFAULT NULL;
'

if [ $? -eq 0 ]; then
    echo "Migration completed successfully."
else
    echo "Migration failed."
    exit 1
fi
