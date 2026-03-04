# QP2

Crystallographic data processing and visualization platform for synchrotron beamlines.

- **Image viewer** — live streaming + offline HDF5/EIGER diffraction viewer with peak finding and analysis plugins
- **Data viewer** — database-backed browser for historical datasets and results
- **Processing server** — manages XDS, CrystFEL, DIALS, autoproc, xia2 pipeline jobs
- **Web app** — browser-based interface for remote access

The code lives in [`data-analysis/qp2/`](data-analysis/qp2/).

---

## Quick Start

Get the image viewer running in under 5 minutes.

**Prerequisites:** Linux, Python 3.9+, a display (or Xvfb), and an HDF5 master file to open.

```bash
# 1. Clone and enter the repo
git clone git@github.com:qpxu007/gmca.git
cd gmca

# 2. Create virtual environment and install
python3 -m venv ~/qp2-env
source ~/qp2-env/bin/activate
pip install -e data-analysis/qp2

# 3. Set test mode (routes all services to localhost)
export QP2_ENV=test

# 4. Launch the image viewer
qp2-image-viewer
```

Use **File → Open** to open a `*_master.h5` diffraction file.
The `bin/iv` shortcut works the same way once the venv is active.

**That's it** — no database, no Redis, no external programs needed for offline file viewing.

To re-activate in a new terminal:
```bash
source ~/qp2-env/bin/activate && export QP2_ENV=test
```

---

## Table of Contents

1. [System requirements](#1-system-requirements)
2. [Install with venv](#2-install-with-venv)
3. [Set up services](#3-set-up-services)
   - [Redis](#31-redis-required-for-live-mode)
   - [PostgreSQL](#32-postgresql-optional)
   - [MySQL](#33-mysqlmariadb-optional)
4. [Configure environment variables](#4-configure-environment-variables)
5. [Test the installation](#5-test-the-installation)
6. [Running the applications](#6-running-the-applications)
7. [External crystallography programs](#7-external-crystallography-programs-optional)

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

> **Headless server?** Install `xvfb` and run `Xvfb :99 -screen 0 1920x1080x24 & export DISPLAY=:99` before launching the viewer.

---

## 2. Install with venv

```bash
# Clone the repo
git clone git@github.com:qpxu007/gmca.git
cd gmca

# Create and activate virtual environment
python3 -m venv ~/qp2-env
source ~/qp2-env/bin/activate

# Install qp2 and all Python dependencies
pip install --upgrade pip
pip install -e data-analysis/qp2
```

> **Editable mode** (`-e`) means local source changes take effect immediately — no reinstall needed.

To activate in future sessions:
```bash
source ~/qp2-env/bin/activate
```

### Using the `bin/` scripts

The `data-analysis/qp2/bin/` directory contains shell shortcuts (`iv`, `dv`, `xtallife`, etc.).
Add it to your PATH so they're available anywhere:

```bash
echo 'export PATH="$HOME/gmca/data-analysis/qp2/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

The scripts automatically detect the active virtual environment — just make sure to `source ~/qp2-env/bin/activate` before using them. You can also set `QP2_PYTHON` to point to a specific interpreter if needed.

---

## 3. Set up services

### 3.1 Redis (required for live mode)

Redis carries the real-time detector data stream to the image viewer. It is **not** needed for opening files offline.

```bash
# Install (if not already installed)
sudo apt install redis-server        # Ubuntu
sudo dnf install redis               # RHEL

# Start and enable on boot
sudo systemctl enable --now redis

# Verify
redis-cli ping                       # must print: PONG
```

By default QP2 connects to `127.0.0.1:6379`. Override if Redis runs elsewhere:
```bash
export REDIS_HOST_BL1=192.168.1.10
export REDIS_HOST_BL2=192.168.1.10
export REDIS_HOST_ANALYSIS_RESULTS=192.168.1.10
```

---

### 3.2 PostgreSQL (optional)

The data viewer falls back to a local SQLite file (`~/.data_viewer/user_data.db`) when PostgreSQL is unavailable — no setup needed for basic use.

To use PostgreSQL:

```bash
sudo apt install postgresql
sudo systemctl enable --now postgresql

sudo -u postgres psql <<'SQL'
CREATE USER qp2user WITH PASSWORD 'choose-a-password';
CREATE DATABASE user_data OWNER qp2user;
SQL

export QP2_PG_HOST=localhost
export QP2_PG_PORT=5432
export QP2_PG_USER=qp2user
export QP2_PG_PASS=choose-a-password
export QP2_PG_DB=user_data
```

---

### 3.3 MySQL/MariaDB (optional)

Only needed if your facility uses a MySQL-backed sample tracking system.

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
cp data-analysis/qp2/.env.example data-analysis/qp2/.env
```

Minimum settings for a standalone workstation:

```bash
# Force all connections to localhost
QP2_ENV=test

# Required only if using the web app
QP2_JWT_SECRET_KEY=<generate: python -c "import secrets; print(secrets.token_hex(32))">
```

Load the file in your shell (add to `~/.bashrc` to make it permanent):

```bash
set -a; source ~/gmca/data-analysis/qp2/.env; set +a
```

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
| `DATAPROC_SERVER_URL` | `http://localhost:8025` | Data processing server URL |
| `AI_SERVER_URL` | `http://localhost:8888/v1` | AI assistant backend URL |
| `QP2_PYTHON` | *(auto)* | Override Python interpreter for `bin/` scripts |
| `QP2_LIVE_GROUPS` | *(unset)* | Colon-separated Unix groups allowed live mode; unset = unrestricted |
| `QP2_BASHRC` | *(unset)* | Path to a facility bashrc to source before launching (e.g. for module system) |

---

## 5. Test the installation

### Offline (no services needed)

```bash
source ~/qp2-env/bin/activate
export QP2_ENV=test

# Confirm the package imports correctly
python -c "import qp2; print(qp2.__version__)"   # → 2.0.0

# Open the image viewer and load any *_master.h5 file via File → Open
qp2-image-viewer
# or using the bin/ shortcut:
iv
```

### Live streaming simulation (requires Redis)

Simulates a detector writing frames live — tests the full Redis → image viewer pipeline.

**Terminal 1 — start the mock streamer:**
```bash
source ~/qp2-env/bin/activate && export QP2_ENV=test
cd ~/gmca/data-analysis/qp2
bin/mock_streamer --file /path/to/your_data_master.h5 --rate 10
# --rate N   frames per second
# --loop     repeat the file continuously
```

**Terminal 2 — launch the image viewer in live mode:**
```bash
source ~/qp2-env/bin/activate && export QP2_ENV=test
qp2-image-viewer --live
# or: iv --live
```

The viewer should connect automatically and begin playing frames as they arrive.

### Run the test suite

```bash
cd ~/gmca/data-analysis/qp2
QP2_ENV=test pytest tests/ -q
```

### What `QP2_ENV=test` does

| Feature | Behavior in test mode |
|---------|----------------------|
| Redis | All connections → `127.0.0.1:6379` |
| PostgreSQL | Falls back to SQLite (`~/.data_viewer/user_data.db`) |
| MySQL | Connection errors suppressed |
| EPICS / beamline hardware | Not contacted |
| LDAP / Kerberos | Bypassed; use `QP2_TEST_USER` / `QP2_TEST_PASS` for web app login |

---

## 6. Running the applications

### Entry points (installed by pip)

| Command | Description |
|---------|-------------|
| `qp2-image-viewer` | Qt diffraction image viewer |
| `qp2-data-viewer` | Database-backed dataset browser |
| `qp2-dp-server` | Data processing job server (port 8025) |
| `qp2-web-server` | Web app API backend (port 8000) |

### `bin/` shortcuts

| Script | Description |
|--------|-------------|
| `bin/iv` | Image viewer (same as `qp2-image-viewer`) |
| `bin/dv` | Data viewer |
| `bin/dp` | Data processing client |
| `bin/xtallife` | Crystal radiation lifetime calculator |
| `bin/strategy` | Data collection strategy tool |
| `bin/serial_xds` | Serial crystallography XDS pipeline |
| `bin/mock_streamer` | Simulate a live Redis detector stream |
| `bin/mock_collect` | Simulate a data collection with GUI |

All `bin/` scripts use the active venv automatically. Add `data-analysis/qp2/bin` to your `PATH` (see [section 2](#2-install-with-venv)).

### Image viewer flags

```bash
qp2-image-viewer                         # open with file browser
qp2-image-viewer /path/to/master.h5     # open a specific file
qp2-image-viewer --live                  # connect to Redis stream on startup
qp2-image-viewer --nolive               # force offline mode
```

### Web app

Build the frontend once (requires Node.js):
```bash
cd data-analysis/qp2/web_app/frontend
npm install && npm run build
```
Then start the backend and visit `http://localhost:8000`:
```bash
qp2-web-server
```

---

## 7. External crystallography programs (optional)

Processing plugins (XDS, CrystFEL, DIALS, autoproc, xia2, DOZOR) are **disabled** if the corresponding program is not found — the viewer works normally for display without them.

Edit `data-analysis/qp2/config/programs.json` to configure each tool:

```json
{
    "dials":    "source /opt/dials/setup.sh",
    "crystfel": "source /opt/crystfel/setup.sh",
    "xds":      "source /opt/xds/setup.sh",
    "ccp4":     "source /opt/ccp4/setup.sh"
}
```

Or use environment variables (no file edit needed):
```bash
export QP2_SETUP_DIALS="source /opt/dials/setup.sh"
export QP2_LIB_XDS_ZCBF="/opt/xds/xds-zcbf.so"
export QP2_BASHRC="/opt/facility/setup.sh"   # sourced before every bin/ script launch
```
