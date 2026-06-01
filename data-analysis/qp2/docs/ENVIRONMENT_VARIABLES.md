# QP2 Environment Variables Reference

Complete reference for all environment variables used across the qp2 codebase.
Variables with defaults are optional; only `QP2_JWT_SECRET` is required in production.

---

## 1. Application Mode

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_ENV` | Set to `test` to enable test mode (uses localhost for all services, disables LDAP) | `prod` |

---

## 2. Security & Authentication

| Variable | Purpose | Default | Required |
|----------|---------|---------|---------|
| `QP2_JWT_SECRET` | JWT token signing key for the web app | — | **Yes** (prod) |
| `AI_API_KEY` | API key for Argo AI service | — | No |
| `GLOBUS_CLI_CLIENT_ID` | Globus OAuth client ID (ALCF transfers) | `9fc37d87-...` | No |
| `GLOBUS_CLI_CLIENT_SECRET` | Globus OAuth client secret | `""` | No |

Set `QP2_JWT_SECRET` in `/mnt/beegfs/qxu/data-analysis/qp2/.env` or in the
systemd service environment.

---

## 3. PostgreSQL Database

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_PG_HOST` | PostgreSQL hostname | `bl1ws1` |
| `QP2_PG_PORT` | PostgreSQL port | `5432` |
| `QP2_PG_USER` | PostgreSQL username | `dhs` |
| `QP2_PG_PASS` | PostgreSQL password | `""` |
| `QP2_PG_DB` | PostgreSQL database name | `user_data` |
| `POSTGRES_URL` | Full connection string (overrides above) | — |

---

## 4. MySQL Databases

| Variable | Purpose | Default |
|----------|---------|---------|
| `MYSQL_GMCA_ACCOUNTS` | Host for GMCA accounts DB | `bl1upper` |
| `MYSQL_HOST_BL1` | Host for BL1 user-data DB | `bl1upper` |
| `MYSQL_HOST_BL2` | Host for BL2 user-data DB | `bl2upper` |
| `MYSQL_USER` | MySQL username | `dhs` |
| `MYSQL_PASS` | MySQL password | `""` |

---

## 5. Redis

| Variable | Purpose | Default |
|----------|---------|---------|
| `REDIS_HOST_BL1` | BL1 beamline Redis (eiger stream) | `10.20.103.85` |
| `REDIS_HOST_BL2` | BL2 beamline Redis (eiger stream) | `10.20.103.154` |
| `REDIS_HOST_ANALYSIS_RESULTS` | Analysis results Redis | `10.20.103.67` |
| `REDIS_HOST_ANALYSIS_FALLBACK` | Fallback for analysis Redis | `127.0.0.1` |
| `REDIS_HOST_FALLBACK_REDIS` | Generic Redis fallback | `127.0.0.1` |

---

## 6. Server Endpoints & Ports

| Variable | Purpose | Default |
|----------|---------|---------|
| `DATAPROC_SERVER_URL` | Full data processing server URL | auto-detected from hostname |
| `DATAPROC_HOST` | Data processing server hostname | auto-detected |
| `DATAPROC_PORT` | Data processing server port | `8025` |
| `WEBSOCKET_SERVER_URL` | Full WebSocket server URL | auto-detected |
| `WEBSOCKET_HOST` | WebSocket server hostname | `localhost` |
| `WEBSOCKET_PORT` | WebSocket server port | `8000` |
| `WEB_APP_URL` | Web application base URL | `http://localhost:8000` |
| `WEB_APP_PORT` | Web application port | `8000` |
| `DOSE_PLANNER_URL` | Full dose planner server URL | auto-detected |
| `DOSE_PLANNER_HOST` | Dose planner hostname | `localhost` |
| `DOSE_PLANNER_PORT` | Dose planner port | `5000` |
| `PBS_RPC_URL` | PBS/Bluice RPC server URL | auto-detected via DB |

---

## 7. Web Application

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_CORS_ORIGINS` | Comma-separated allowed CORS origins | `http://localhost:5173,http://localhost:3000` |
| `QP2_DATA_DIR` | Root directory for HDF5/data files served by the viewer | `/mnt/beegfs/DATA` |
| `H5GROVE_BASE_DIR` | H5Grove base directory (falls back to `QP2_DATA_DIR`) | `/mnt/beegfs/DATA` |

---

## 8. Live Viewer (EIGER ZMQ)

| Variable | Purpose | Default |
|----------|---------|---------|
| `EIGER_PUB_BL1` | ZMQ PUB address for BL1 live frame cache | `tcp://10.20.103.85:9900` |
| `EIGER_PUB_BL2` | ZMQ PUB address for BL2 live frame cache | `tcp://10.20.103.154:9900` |

See `web_app/EIGER_PROXY_INTEGRATION.md` for setup details.

---

## 9. Cluster & Job Submission

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_SLURM_WEB_QOS` | Slurm QOS name for web-submitted reprocess jobs. Set to `web` after running the sacctmgr setup (see `docs/SLURM_QOS_PRIORITY.md`). Unset = no QOS directive. | — |
| `SLURM_JOB_ID` | Set by Slurm when running inside a job. Causes `run_command()` to downgrade from slurm→shell to prevent nested submissions. | system-set |
| `CLUSTER_PYTHON` | Python interpreter path on cluster nodes | — |
| `CLUSTER_PROJECT_ROOT` | Project root path on cluster nodes | — |

---

## 10. Program Paths

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_PX_ROOT` | PX software root | `/mnt/software/px/` |
| `QP2_MODULE_PATH` | Environment modules path | `/mnt/software/px/modulefiles` |
| `QP2_PROFILE_SCRIPT` | Shell profile init command | `. /usr/share/modules/init/bash` |
| `QP2_PROGRAMS_CONFIG` | Path to `programs.json` config | auto-detected |
| `QP2_PROG_DOZOR` | Dozor executable | `/mnt/software/px/DOZOR/dozor2q` |
| `QP2_PROG_DIALS_PYTHON` | DIALS Python | `/mnt/software/px/dials/build/bin/dials.python` |
| `QP2_PROG_CAGET` | EPICS `caget` binary | `/mnt/software/epics/base/bin/linux-x86_64/caget` |
| `QP2_PROG_PYTHON` | Python interpreter | `/mnt/software/px/miniconda3/envs/opencv/bin/python` |
| `QP2_PROG_ADXV` | adxv viewer | `/mnt/software/px/bin/adxv` |
| `QP2_PROG_EIGER2CBF` | eiger2cbf converter | `/mnt/software/px/bin/eiger2cbf-omp` |
| `QP2_LIB_XDS_ZCBF` | XDS ZCBF plugin library | `/mnt/software/px/XDS/xds-zcbf.so` |
| `QP2_LIB_DECTRIS_NEGGIA` | Dectris Neggia plugin | `/mnt/software/px/XDS/dectris-neggia.so` |
| `QP2_LIB_RADDOSE3D` | RADDOSE-3D JAR | `/mnt/software/px/bin/raddose3d.jar` |
| `QP2_SETUP_<PROGRAM>` | Override setup command for a specific program (e.g. `QP2_SETUP_XDS`) | module load command |

---

## 11. Performance Tuning

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_DATA_POLL_INTERVAL_SEC` | Data polling interval (s) | `2` |
| `QP2_REDIS_CONNECT_TIMEOUT_SEC` | Redis connection timeout (s) | `5` |
| `QP2_ANALYSIS_REFRESH_INTERVAL_MS` | Analysis UI refresh interval (ms) | `5000` |
| `QP2_HDF5_POLL_INTERVAL_MS` | HDF5 file polling interval (ms) | `200` |
| `QP2_RUN_TIMEOUT_SECONDS` | Job run timeout (s) | `3600` |

---

## 12. Image Processing & Analysis

| Variable | Purpose | Default |
|----------|---------|---------|
| `CRYSTFEL_STREAM_DIR` | CrystFEL stream output directory | `/mnt/beegfs/{USER}/crystfel_streams` |
| `QP2_SHARED_ASSETS_DIR` | Shared assets for image processing | `/mnt/beegfs/{USER}/qp2_shared_assets` |
| `NXDS_PERSISTENT_DIR` | nXDS run storage directory | `/mnt/beegfs/{USER}/nxds_runs` |
| `HKL3000_CMD` | HKL3000 command path | `HKL3000` |
| `HDF5_USE_FILE_LOCKING` | HDF5 file locking (`FALSE` to disable on shared FS) | `FALSE` (set at startup) |

---

## 13. Database Engine

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_DB_ENGINE` | Force a specific DB engine (`postgresql`, `sqlite`, etc.) | auto-detected from hostname |

---

## 14. Beamline & Facility

| Variable | Purpose | Default |
|----------|---------|---------|
| `BEAMLINE` | Beamline identifier override | `23b` |
| `DM_STATION_NAME` | Data management station name | `23ID` |
| `DM_BEAMLINE_NAME` | Data management beamline name | — |

---

## 15. Logging

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_LOG_FILE` | Log file path (stdout if unset) | — |

---

## 16. DM / APS Archive

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_DM_NODE` | Slurm node where DM commands run (bl2ws5) | `bl2ws5` |
| `QP2_DM_SETUP` | Shell commands to source DM environment on the DM node | `source /home/dm/etc/dm.setup.sh; source /mnt/beegfs/dmadmin/.bashrc` |
| `QP2_DM_AGENT_PATH` | Full path to `dm_agent.py` on the DM node | `/mnt/beegfs/qxu/data-analysis/qp2/dm_gmca/dm_agent.py` |
| `QP2_GLOBUS_ENDPOINT` | Globus endpoint UUID for the APS DM archive (`APS:DM:23ID`) | `4be6a66d-291e-4383-987e-c3c0162d645c` |
| `QP2_ARCHIVE_DATA_ROOT` | Full path to local DATA directory | `/mnt/beegfs/DATA` |
| `QP2_ARCHIVE_PROCESSING_ROOT` | Full path to local PROCESSING directory (can be on a different filesystem, e.g. `/mnt/nvme/PROCESSING`) | `/mnt/beegfs/PROCESSING` |
| `QP2_APS_ARCHIVE_ROOT` | APS-side root path prefix used in `dm-23id-upload --root-path` and Globus URLs | `/DATA` |

---

## 17. AlphaFold 3

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_PREDICTION_BASE` | Base directory for prediction job storage | `/mnt/beegfs/dmadmin` |
| `QP2_AF3_SIF` | Path to AlphaFold 3 Singularity image | `/mnt/alphafold3/alphafold3/alphafold3.sif` |
| `QP2_AF3_DBS` | Path to AF3 databases | `/mnt/alphafold3/af3-DBs` |
| `QP2_AF3_MODELS` | Path to AF3 model weights | `/mnt/alphafold3/af3-models` |
| `QP2_AF3_RUN_SCRIPT` | Path to `run_alphafold.py` inside the SIF | `/mnt/alphafold3/alphafold3/run_alphafold.py` |

---

## 18. Notifications

| Variable | Purpose | Default |
|----------|---------|---------|
| `QP2_NOTIFICATION_FROM_EMAIL` | Sender address for experiment prep email notifications | `qxu@anl.gov` |
| `QP2_DEFAULT_HOST_EMAIL` | Fallback recipient when no staff host is assigned to an ESAF | `gmcahosts@anl.gov` |
| `QP2_ADMIN_EMAIL` | Recipient for admin failure alerts (web_app backend). If unset, admin alerts are skipped. | *(none)* |

---

## Summary

| Category | Count |
|----------|-------|
| Security & auth | 4 |
| Database (PG + MySQL) | 10 |
| Redis | 5 |
| Server endpoints | 12 |
| Web app | 3 |
| Live viewer / EIGER | 2 |
| Cluster / Slurm | 4 |
| Program paths | 14 |
| Performance tuning | 5 |
| Image processing | 5 |
| Beamline / facility | 3 |
| Logging / DB engine | 2 |
| DM / APS Archive | 7 |
| AlphaFold 3 | 5 |
| Notifications | 2 |
| **Total** | **83** |

**Only one variable is required in production: `QP2_JWT_SECRET`.**
All others have sensible defaults or are auto-detected from the hostname.
