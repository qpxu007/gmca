# Output Directory Structure

The data processing server (`data_proc/server`) organizes output files based on the collection mode (single vs. multi-series) and the processing stage (milestones vs. final completion).

## Base Structure

All processing occurs under a root processing directory derived from the raw data directory.
*   **Raw Data:** `.../DATA/<user>/<experiment>/<subdir>`
*   **Processing Root:** `.../PROCESSING/<user>/<experiment>`

## Directory Organization

### Single-Series Runs (Standard/Single/Vector)
For runs consisting of a single series (where `run_prefix` typically matches `series_prefix`), the structure is flat under the run prefix:

`.../PROCESSING/<user>/<experiment>/<run_prefix>/<job_subdir>`

**Examples:**
*   **XDS 25% Milestone:** `.../J8_vec_hel/xds_25pct`
*   **XDS 50% Milestone:** `.../J8_vec_hel/xds_50pct`
*   **XDS Completion:** `.../J8_vec_hel/xds`
*   **AutoPROC:** `.../J8_vec_hel/autoproc`
*   **Xia2:** `.../J8_vec_hel/xia2`

### Multi-Series Runs (Raster/Multi-Wedge)
For runs consisting of multiple series (where `series_prefix` differs from `run_prefix`), per-series jobs are nested within a subdirectory named after the series prefix:

`.../PROCESSING/<user>/<experiment>/<run_prefix>/<series_prefix>/<job_subdir>`

**Examples:**
*   **XDS (Series 1):** `.../J8_raster/J8_raster_run1/xds`
*   **XDS (Series 2):** `.../J8_raster/J8_raster_run2/xds`

### Merged Jobs
If a multi-series run triggers a "merged" processing job (e.g., merging all series in `autoproc` or `xia2`), it uses the flat structure under the run prefix, as it applies to the whole run:

`.../PROCESSING/<user>/<experiment>/<run_prefix>/autoproc`

## Duplicate Handling
If a directory already exists (e.g., from a previous run), the server automatically appends a run number suffix to avoid overwriting data:

*   `.../xds` (First run)
*   `.../xds_run1` (Second run)
*   `.../xds_run2` (Third run)

## Logic Implementation
The directory logic is centralized in `data_proc/server/data_processing_server.py` within `_create_processing_directory`:

```python
run_dir = proc_root_dir / run_prefix

if series_subdir:
    work_root = run_dir / series_subdir
else:
    work_root = run_dir

# ... subdir_name calculation ...

final_path = work_root / subdir_name
```

`AnalysisManager` determines whether to pass a `series_subdir` based on the collection mode and prefix matching.
