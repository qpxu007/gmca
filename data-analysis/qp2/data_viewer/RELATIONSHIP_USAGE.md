# Utilizing the Relationship between DatasetRun and PipelineStatus

The relationship between `DatasetRun` and `PipelineStatus` (established via the `dataset_run_id` foreign key) enables powerful queries and data navigation that were previously difficult or impossible.

Here are the key ways to utilize this relationship:

### 1. Linking Processing Results to Collection Metadata
**Goal:** Find all processing jobs (XDS, autoPROC, etc.) for a specific data collection run.

*   **Before:** You had to rely on string matching (e.g., `run_prefix` vs `sampleName` or parsing file paths), which was fragile and error-prone.
*   **After (SQL/ORM):** You can directly join the tables or use the ORM relationship.

```python
# Using SQLAlchemy ORM (e.g., in a script or backend route)
run = session.query(DatasetRun).filter_by(run_prefix="my_run_001").first()

# Access all processing jobs for this run directly
for status in run.pipeline_statuses:
    print(f"Pipeline: {status.pipeline}, State: {status.state}, Results ID: {status.id}")
```

### 2. Traceability from Results back to Source
**Goal:** Given a processing result (e.g., a high-quality structure solution), find the exact conditions under which the data was collected.

*   **After:**
```python
# Given a PipelineStatus object 'status'
dataset_run = status.dataset_run

if dataset_run:
    print(f"Collection Date: {dataset_run.created_at}")
    print(f"Mounted Crystal: {dataset_run.mounted}")
    print(f"Spreadsheet Data: {dataset_run.meta_user}") # Access full row info
    print(f"Total Frames: {dataset_run.total_frames}")
```

### 3. Integrated Dashboard Views
**Goal:** Build a "Master View" in the web app or data viewer that shows a run and its processing status in a single row.

*   **Implementation:** You can now query `DatasetRun` and eagerly load `pipeline_statuses` to display a summary.
*   **Example UI Logic:**
    *   **Row:** Run `2025-run-005` | 1000 frames | Mounted: A1
    *   **Columns/Badges:**
        *   XDS: ✅ (Done)
        *   autoPROC: ❌ (Failed)
        *   Dozor: ⚠️ (Running)

### 4. Automated Reprocessing Triggers
**Goal:** Automatically reprocess all runs that used a specific crystal or protein if a better model becomes available.

*   **Logic:**
    1.  Query `DatasetRun` filtering by `meta_user` (JSON) to find runs with `Protein="Lysozyme"`.
    2.  For each run, check existing `pipeline_statuses`.
    3.  Trigger a new job linked to that `dataset_run_id`.

### 5. Data Lifecycle Management (Cleanup)
**Goal:** Delete old processing results but keep the primary collection metadata.

*   **Logic:** Since the foreign key is on `PipelineStatus`, you can delete rows from `pipelinestatus` without affecting `dataset_runs`. Conversely, if you delete a `DatasetRun`, the database constraint (`ON DELETE SET NULL`) ensures `PipelineStatus` records remain but become unlinked (or you could change it to `CASCADE` to clean up everything).

### 6. Simplified Statistics
**Goal:** Calculate success rates.
*   **Query:** Count `DatasetRun` entries vs. count of `DatasetRun` entries that have at least one linked `PipelineStatus` with `state='DONE'`.

### Example SQL Query
To see this in action directly in the database:

```sql
SELECT 
    dr.run_prefix, 
    dr.mounted, 
    ps.pipeline, 
    ps.state, 
    dpr.highresolution
FROM dataset_runs dr
JOIN pipelinestatus ps ON dr.data_id = ps.dataset_run_id
LEFT JOIN dataprocessresults dpr ON ps.id = dpr.pipelinestatus_id
WHERE dr.username = 'jdoe'
ORDER BY dr.created_at DESC;
```
