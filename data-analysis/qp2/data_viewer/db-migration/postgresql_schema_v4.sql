-- Migration V4: Add run_prefix to PipelineStatus

-- Add run_prefix column to pipelinestatus table
ALTER TABLE "pipelinestatus" ADD COLUMN "run_prefix" VARCHAR(255) DEFAULT NULL;
