# MariaDB Database Update Instructions (Schema v1 to v5)

This guide provides step-by-step instructions to update an existing MariaDB database to the latest schema version. These changes include new columns for metadata tracking and establishing relationships between dataset runs and pipeline jobs.

## 1. Prerequisites
*   **Database User:** `dhs` (or your specific DB user)
*   **Database Name:** `user_data` (or your specific database name)
*   **Tool:** Access to the `mysql` command-line client.

## 2. Backup Your Data (CRITICAL)
Always create a backup before performing schema alterations.

```bash
mysqldump -u dhs -p user_data > user_data_backup_$(date +%F).sql
```

## 3. Standardize Table Names
If your existing tables use PascalCase (e.g., `DatasetRuns`), they must be renamed to lowercase to match the application's ORM and the new consolidated schema.

Login to MariaDB:
```bash
mysql -u dhs -p user_data
```

Execute renaming if necessary:
```sql
RENAME TABLE `DatasetRuns` TO `dataset_runs`;
RENAME TABLE `PipelineStatus` TO `pipelinestatus`;
RENAME TABLE `ProcessingResults` TO `dataprocessresults`;
RENAME TABLE `StrategyResults` TO `screenstrategyresults`;
```

## 4. Apply Schema Alterations
Execute the following SQL blocks to add new columns and indices.

### A. Update `dataset_runs`
```sql
-- Convert headers and master_files to MEDIUMTEXT
ALTER TABLE `dataset_runs` MODIFY COLUMN `headers` MEDIUMTEXT DEFAULT NULL;
ALTER TABLE `dataset_runs` MODIFY COLUMN `master_files` MEDIUMTEXT DEFAULT NULL;

-- Add 'mounted' column
SELECT count(*) INTO @exist FROM information_schema.columns 
WHERE table_schema = DATABASE() AND table_name = 'dataset_runs' AND column_name = 'mounted';

SET @query = IF(@exist=0, 
    'ALTER TABLE `dataset_runs` ADD COLUMN `mounted` varchar(255) DEFAULT NULL', 
    'SELECT "Column mounted already exists"');
PREPARE stmt FROM @query; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Add 'meta_user' column
SELECT count(*) INTO @exist FROM information_schema.columns 
WHERE table_schema = DATABASE() AND table_name = 'dataset_runs' AND column_name = 'meta_user';

SET @query = IF(@exist=0, 
    'ALTER TABLE `dataset_runs` ADD COLUMN `meta_user` text DEFAULT NULL', 
    'SELECT "Column meta_user already exists"');
PREPARE stmt FROM @query; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Ensure index on run_prefix exists
CREATE INDEX IF NOT EXISTS `ix_dataset_runs_run_prefix` ON `dataset_runs` (`ix_dataset_runs_run_prefix`);
```

### B. Update `pipelinestatus`
```sql
-- Add 'dataset_run_id' column (Foreign Key field)
SELECT count(*) INTO @exist FROM information_schema.columns 
WHERE table_schema = DATABASE() AND table_name = 'pipelinestatus' AND column_name = 'dataset_run_id';

SET @query = IF(@exist=0, 
    'ALTER TABLE `pipelinestatus` ADD COLUMN `dataset_run_id` int(11) DEFAULT NULL', 
    'SELECT "Column dataset_run_id already exists"');
PREPARE stmt FROM @query; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Add 'run_prefix' column (Metadata field)
SELECT count(*) INTO @exist FROM information_schema.columns 
WHERE table_schema = DATABASE() AND table_name = 'pipelinestatus' AND column_name = 'run_prefix';

SET @query = IF(@exist=0, 
    'ALTER TABLE `pipelinestatus` ADD COLUMN `run_prefix` varchar(255) DEFAULT NULL', 
    'SELECT "Column run_prefix already exists"');
PREPARE stmt FROM @query; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Add index for the new Foreign Key
CREATE INDEX IF NOT EXISTS `pipelinestatus_dataset_run_id` ON `pipelinestatus` (`dataset_run_id`);
```

## 5. Establish Relationships
Establish the foreign key constraint to link processing logs to dataset metadata.

```sql
-- Create Foreign Key Constraint with SET NULL on delete
SET @fk_exists := (SELECT COUNT(*) FROM information_schema.table_constraints 
                   WHERE table_name = 'pipelinestatus' 
                   AND constraint_name = 'pipelinestatus_dataset_run_id_ibfk' 
                   AND table_schema = DATABASE());

SET @query = IF(@fk_exists=0,
    'ALTER TABLE `pipelinestatus` 
     ADD CONSTRAINT `pipelinestatus_dataset_run_id_ibfk` 
     FOREIGN KEY (`dataset_run_id`) REFERENCES `dataset_runs` (`data_id`) 
     ON DELETE SET NULL',
    'SELECT "Foreign Key already exists"');
PREPARE stmt FROM @query; EXECUTE stmt; DEALLOCATE PREPARE stmt;
```

## 6. Verification
Run these commands to confirm the structure:

```sql
DESCRIBE dataset_runs;
DESCRIBE pipelinestatus;

-- Check for the active Foreign Key
SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE REFERENCED_TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'pipelinestatus';
```
