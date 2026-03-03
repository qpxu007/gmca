# QP2

Crystallographic data processing and visualization platform for synchrotron beamlines.

- **Image viewer** — live streaming + offline HDF5/EIGER diffraction viewer with peak finding and analysis plugins
- **Data viewer** — database-backed browser for historical datasets and results
- **Processing server** — manages XDS, CrystFEL, DIALS, autoproc, xia2 pipeline jobs
- **Web app** — browser-based interface for remote access

The code lives in [`data-analysis/qp2/`](data-analysis/qp2/).

---

## Table of Contents

1. [System requirements](#1-system-requirements)
2. [Install with venv](#2-install-with-venv)
3. [Set up services](#3-set-up-services)
   - [Redis](#31-redis-required-for-live-mode)
   - [PostgreSQL](#32-postgresql-optional)
   - [MySQL](#33-mysqlmariadb-optional)
4. [Configure environment variables](#4-configure-environment-variables)
5. [Minimal test environment](#5-minimal-test-environment)
6. [Running the applications](#6-running-the-applications)

---

## 1. System requirements

**OS:** Linux (Ubuntu 22.04+ or RHEL 8+)
**Python:** 3.9 or newer (3.12 recommended)

Install system packages first:

```bash
# Ubuntu / Debian
sudo apt update && sudo apt install -y \
    git python3-dev python3-pip python3-venv \
    libhdf5-dev libpq-dev \
    libgl1-mesa-glx libglib2.0-0 libsm6 libxrender1 libxext6 \
    redis-server

# RHEL / Rocky / AlmaLinux
sudo dnf install -y \
    git python3-devel python3-pip \
    hdf5-devel postgresql-devel \
    mesa-libGL libSM libXrender libXext \
    redis
```

---

## 2. Install with venv

```bash
# Clone the repo
git clone git@github.com:qpxu007/gmca.git
cd gmca

# Create and activate virtual environment
python3 -m venv ~/qp2-env
source ~/qp2-env/bin/activate

# Install qp2 and all dependencies (editable mode)
cd data-analysis/qp2
pip install --upgrade pip
pip install -e .
```

> **Editable mode** (`-e`) means changes to the source files take effect immediately — no reinstall needed.

To activate the environment in future sessions:
```bash
source ~/qp2-env/bin/activate
```

---

## 3. Set up services

### 3.1 Redis (required for live mode)

Redis carries the real-time detector data stream to the image viewer. It is **not** needed for opening files offline.

```bash
# Install (if not already installed)
sudo apt install redis-server        # Ubuntu
# or
sudo dnf install redis               # RHEL

# Start Redis
sudo systemctl enable --now redis

# Verify
redis-cli ping                       # should print: PONG
```

By default QP2 connects to `127.0.0.1:6379`. Override with environment variables if Redis runs on a different host:
```bash
export REDIS_HOST_BL1=192.168.1.10
export REDIS_HOST_BL2=192.168.1.10
export REDIS_HOST_ANALYSIS_RESULTS=192.168.1.10
```

---

### 3.2 PostgreSQL (optional)

The data viewer uses PostgreSQL to store dataset history and metadata. If PostgreSQL is unavailable it falls back to a local SQLite file at `~/.data_viewer/user_data.db` — no setup required for basic use.

To use PostgreSQL:

```bash
# Install and start
sudo apt install postgresql
sudo systemctl enable --now postgresql

# Create database and user
sudo -u postgres psql <<'SQL'
CREATE USER qp2user WITH PASSWORD 'choose-a-password';
CREATE DATABASE user_data OWNER qp2user;
\q
SQL

# Set connection variables
export QP2_PG_HOST=localhost
export QP2_PG_PORT=5432
export QP2_PG_USER=qp2user
export QP2_PG_PASS=choose-a-password
export QP2_PG_DB=user_data
```

---

### 3.3 MySQL/MariaDB (optional)

Only needed if your facility uses a MySQL-backed sample tracking system (e.g. for user/ESAF lookup).

```bash
sudo apt install mariadb-server
sudo systemctl enable --now mariadb

sudo mysql <<'SQL'
CREATE USER 'qp2user'@'localhost' IDENTIFIED BY 'choose-a-password';
CREATE DATABASE beamline_data;
GRANT ALL PRIVILEGES ON beamline_data.* TO 'qp2user'@'localhost';
FLUSH PRIVILEGES;
SQL

export MYSQL_HOST_BL1=localhost
export MYSQL_USER=qp2user
export MYSQL_PASS=choose-a-password
```

---

## 4. Configure environment variables

Copy the template and edit it:

```bash
cd data-analysis/qp2
cp .env.example .env
```

Edit `.env` — the minimum settings for a standalone workstation:

```bash
# Force all connections to localhost — safest for a non-beamline machine
QP2_ENV=test

# Required only if you use the web app
QP2_JWT_SECRET_KEY=<run: python -c "import secrets; print(secrets.token_hex(32))">
```

Load the file before starting any QP2 application:

```bash
set -a; source data-analysis/qp2/.env; set +a
```

Add that line to your `~/.bashrc` to load it automatically on login.

### Full variable reference

| Variable | Default | Description |
|----------|---------|-------------|
| `QP2_ENV` | `prod` | Set to `test` to force all connections to localhost |
| `QP2_JWT_SECRET_KEY` | *(none)* | Required for the web app |
| `QP2_PG_HOST` | `localhost` | PostgreSQL host |
| `QP2_PG_PORT` | `5432` | PostgreSQL port |
| `QP2_PG_USER` | `qp2user` | PostgreSQL user |
| `QP2_PG_PASS` | | PostgreSQL password |
| `QP2_PG_DB` | `user_data` | PostgreSQL database |
| `REDIS_HOST_BL1` | `127.0.0.1` | Redis host for beamline 1 stream |
| `REDIS_HOST_BL2` | `127.0.0.1` | Redis host for beamline 2 stream |
| `REDIS_HOST_ANALYSIS_RESULTS` | `127.0.0.1` | Redis host for analysis results |
| `MYSQL_HOST_BL1` | `localhost` | MySQL host |
| `MYSQL_USER` | `qp2user` | MySQL user |
| `MYSQL_PASS` | | MySQL password |
| `DATAPROC_SERVER_URL` | `http://localhost:8025` | Data processing server |
| `AI_SERVER_URL` | `http://localhost:8888/v1` | AI assistant backend |

---

## 5. Minimal test environment

This section gets the image viewer running end-to-end **without** a real detector, using simulated data and a local Redis stream.

### Step 1 — confirm prerequisites

```bash
source ~/qp2-env/bin/activate
redis-cli ping                  # must return PONG
python -c "import qp2; print(qp2.__version__)"   # must print 2.0.0
```

### Step 2 — set test mode

```bash
export QP2_ENV=test
```

This routes all service connections to localhost and disables production safety checks.

### Step 3 — open a file directly (offline test)

The simplest test — open any HDF5 master file:

```bash
qp2-image-viewer /path/to/your_data_master.h5
```

Or launch the viewer and use **File → Open** to browse to a `*_master.h5` file.
No Redis or database is needed for this.

### Step 4 — simulate live streaming (live mode test)

This tests the full Redis → image viewer pipeline without a real detector.

**Terminal 1 — start the mock streamer:**
```bash
source ~/qp2-env/bin/activate
export QP2_ENV=test
cd gmca/data-analysis/qp2

# Stream an existing HDF5 file as if it were arriving live from a detector
bin/mock_streamer --file /path/to/your_data_master.h5 --rate 10
# --rate N  simulates N frames per second
# --loop    repeats the file continuously
```

**Terminal 2 — launch the image viewer in live mode:**
```bash
source ~/qp2-env/bin/activate
export QP2_ENV=test
qp2-image-viewer --live
```

The viewer connects to the local Redis stream. You should see it pick up the simulated series automatically and begin playing frames.

### Step 5 — run the test suite

```bash
cd gmca/data-analysis/qp2
QP2_ENV=test pytest tests/ -q
```

### What each `QP2_ENV=test` skips

| Feature | Behavior in test mode |
|---------|----------------------|
| Redis connections | All point to `127.0.0.1:6379` |
| PostgreSQL | Falls back to SQLite at `~/.data_viewer/user_data.db` |
| MySQL | Connection errors are suppressed |
| EPICS / beamline hardware | Not contacted |
| LDAP / Kerberos login | Bypassed (use `QP2_TEST_USER` / `QP2_TEST_PASS` for web app login) |

---

## 6. Running the applications

### Image viewer
```bash
qp2-image-viewer                        # open file browser on launch
qp2-image-viewer /path/to/master.h5    # open a specific file
qp2-image-viewer --live                 # connect to Redis stream immediately
```

### Data viewer
```bash
qp2-data-viewer
```
Uses SQLite automatically if PostgreSQL is not configured.

### Data processing server
```bash
qp2-dp-server          # listens on port 8025
```
Must be running for the image viewer's processing plugins (XDS, CrystFEL, etc.) to submit jobs.

### Web app
```bash
qp2-web-server         # API backend on port 8000
```
Build the frontend once (requires Node.js / npm):
```bash
cd data-analysis/qp2/web_app/frontend
npm install && npm run build
```
Then visit `http://localhost:8000` in a browser.

---

## External crystallography programs (optional)

Processing plugins are disabled if the corresponding program is not found — the viewer still works normally for display. To enable them, edit `data-analysis/qp2/config/programs.json` with the setup command for each tool on your system:

```json
{
    "dials":    "source /opt/dials/setup.sh",
    "crystfel": "source /opt/crystfel/setup.sh",
    "xds":      "source /opt/xds/setup.sh"
}
```

Or override per-program with environment variables:
```bash
export QP2_SETUP_DIALS="source /opt/dials/setup.sh"
export QP2_LIB_XDS_ZCBF="/opt/xds/xds-zcbf.so"
```
