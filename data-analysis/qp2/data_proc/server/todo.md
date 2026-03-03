# Data Processing Server Refactoring Plan

This document outlines the architectural improvement plan for the `data_proc/server` directory based on the deep analysis of the current state.

## 1. Standardize Modern and Legacy Pipelines
Currently, there is a deep rift between the "modern" pipelines (managed by `worker` classes in `AnalysisManager`) and the "legacy" pipelines (managed by `xprocess.py` and `script.py`). `AnalysisManager` has dedicated duplicated logic paths for legacy tools that bypass the `QRunnable` worker abstraction entirely, instantiating raw subprocesses via the `xprocess` / `Script` helpers.
**Recommendation**: Create `LegacyGmcaProcWorker`, `LegacyAutoprocWorker` wrapper classes that inherit from the same base class as the modern workers. Move the logic from `xprocess.py` into these workers. This collapses the duplicate dispatch logic in `AnalysisManager` into single, clean pathways.

## 2. Consolidate Configuration Management
Configuration parameters are scattered across multiple sources: `data_processing_server.py`, `analysis_config.json`, `xprocess.py` (which has hardcoded absolute paths like `"/mnt/software/px/miniconda3/envs/data-analysis/bin"`), and `xls_reader.py`.
**Recommendation**: Consolidate into a single, strongly-typed `pydantic` settings class for the server, initialized once and passed down. Remove hardcoded absolute paths from modules like `xprocess.py` and enforce they come from the central config or environment variables.

## 3. Extract Tracking State from `ProcessingServer`
The `ProcessingServer` class is handling too many responsibilities: Redis connection management, state tracking for milestones (25%, 50%, 100%), HDF5 dataset monitoring (`run_hdf5_readers`), HTTP Server delegation, and WebSocket Server delegation.
**Recommendation**: Extract the HDF5 monitoring logic and Milestone tracking into a dedicated `RunStateTracker` or `SessionManager` class. `ProcessingServer` should solely concern itself with server lifecycle (start, stop, network binds) and routing incoming messages to the `RunStateTracker` or `AnalysisManager`.

## 4. Modernize File I/O Polling (`wait_for_required_files`)
In `data_processing_server.py`, the method `wait_for_required_files` relies on a synchronous blocking loop (`time.sleep`) running inside the `ThreadPoolExecutor`. High concurrency (many parallel data collections) could exhaust the thread pool if many runs are spending minutes sleeping and checking `Path.exists()`.
**Recommendation**: Transition file-system readiness checks to use asynchronous non-blocking waits, or a dedicated `inotify` (via the `watchdog` library) monitor that emits a signal when files arrive.

## 5. Better Database Layer Isolation
Database update logic is found in `save_run_to_db.py` (`create_dataset_run` and `update_dataset_run`), but `AnalysisManager.handle_run_completion_logic` also has DB modification logic scattered near its worker submissions.
**Recommendation**: Strictly enforce that *only* dedicated Data Access Objects (DAOs) or specific services modify the DB. Workers should update UI/Status tables, but core Run states should be managed centrally.
