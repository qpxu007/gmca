-- Migration V5: Add meta_user to DatasetRun

-- Add meta_user column to dataset_runs table
ALTER TABLE "dataset_runs" ADD COLUMN "meta_user" TEXT DEFAULT NULL;