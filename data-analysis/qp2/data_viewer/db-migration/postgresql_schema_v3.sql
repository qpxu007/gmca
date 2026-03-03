-- Migration V3: Link PipelineStatus to DatasetRun

-- Add dataset_run_id column to pipelinestatus table
ALTER TABLE "pipelinestatus" ADD COLUMN "dataset_run_id" INTEGER DEFAULT NULL;

-- Create index on the new column
CREATE INDEX "pipelinestatus_dataset_run_id_idx" ON "pipelinestatus" ("dataset_run_id");

-- Add foreign key constraint
ALTER TABLE "pipelinestatus" 
ADD CONSTRAINT "pipelinestatus_dataset_run_id_fkey" 
FOREIGN KEY ("dataset_run_id") 
REFERENCES "dataset_runs" ("data_id") 
ON DELETE SET NULL;
