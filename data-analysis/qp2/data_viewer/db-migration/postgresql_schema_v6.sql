-- Migration V6: Add performance indexes
--
-- The username column on dataset_runs is queried every 5 seconds by the
-- polling timer (query_latest_dataset_run_id) and on every tab refresh
-- (query_dataset_run).  Without an index the database performs a full
-- table scan each time.
--
-- Adding this index makes those queries 10-100x faster.

-- Index for the polling query: WHERE username = ? ORDER BY data_id DESC
CREATE INDEX IF NOT EXISTS idx_dataset_runs_username_data_id
    ON dataset_runs (username, data_id DESC);
