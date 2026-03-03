# QP2 Installation Guide

Step-by-step instructions for installing QP2 on a Linux workstation or server.

---

## Table of Contents

1. [System Requirements](#1-system-requirements)
2. [Clone the Repository](#2-clone-the-repository)
3. [Set Up a Python Environment](#3-set-up-a-python-environment)
4. [Install QP2 and Dependencies](#4-install-qp2-and-dependencies)
5. [Configure Environment Variables](#5-configure-environment-variables)
6. [Configure External Programs](#6-configure-external-programs)
7. [Set Up Optional Services](#7-set-up-optional-services)
8. [Verify the Installation](#8-verify-the-installation)
9. [Running the Applications](#9-running-the-applications)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. System Requirements

### Operating System
- **Linux** (Ubuntu 22.04+ or RHEL 8+ recommended)
- macOS may work but is not tested

### Python
- Python **3.9 or newer** (3.12 recommended)

### System packages (install before proceeding)

**Ubuntu / Debian:**
```bash
sudo apt update
sudo apt install -y \
    git python3-dev python3-pip python3-venv \
    libhdf5-dev libpq-dev default-libmysqlclient-dev \
    libgl1-mesa-glx libglib2.0-0 libsm6 libxrender1 libxext6 \
    redis-server
```

**RHEL / Rocky / AlmaLinux:**
```bash
sudo dnf install -y \
    git python3-devel python3-pip \
    hdf5-devel postgresql-devel mysql-devel \
    mesa-libGL libSM libXrender libXext \
    redis
```

### Hardware
- **Display** (or virtual framebuffer `Xvfb`) — required for the Qt image viewer
- **RAM**: 8 GB minimum; 32 GB recommended for large HDF5 datasets
- **Storage**: depends on your data volume; at least 10 GB for software

---

## 2. Clone the Repository

```bash
git clone https://github.com/your-org/qp2.git   # replace with actual URL
cd qp2
```

Or, if you received a tarball:
```bash
tar -xf qp2-2.0.0.tar.gz
cd qp2-2.0.0
```

---

## 3. Set Up a Python Environment

Choose **one** of the following methods.

### Option A — Conda (recommended for scientific use)

```bash
# Install Miniconda if not already installed
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b -p ~/miniconda3
source ~/miniconda3/etc/profile.d/conda.sh

# Create an environment
conda create -n qp2 python=3.12 -y
conda activate qp2
```

Alternatively, use the provided conda build script:
```bash
bash build_conda_env.sh
conda activate qp2
```

### Option B — Python venv

```bash
python3.12 -m venv ~/qp2-env
source ~/qp2-env/bin/activate
pip install --upgrade pip
```

---

## 4. Install QP2 and Dependencies

From inside the repository root (with your environment activated):

```bash
# Core install — installs qp2 and all Python dependencies
pip install -e .

# If you also need MySQL support for the server component
pip install -e ".[server]"

# If you are a developer
pip install -e ".[dev]"
```

The `-e` flag installs in **editable mode** so local code changes take effect immediately without reinstalling.

This step installs: PyQt5, numpy, scipy, h5py, redis, fastapi, sqlalchemy, gemmi, CrystFEL integration libraries, and ~50 other packages.  It may take 5–10 minutes on first install.

---

## 5. Configure Environment Variables

QP2 reads all site-specific settings from environment variables. All defaults point to `localhost`, so the software will start without any configuration — but you will need to set variables to connect to real services.

### Minimal setup for a standalone workstation

```bash
# Copy the template
cp .env.example .env

# Generate a secure JWT key (required if you use the web app)
python -c "import secrets; print('QP2_JWT_SECRET_KEY=' + secrets.token_hex(32))" >> .env

# Force test/local mode (all services → localhost)
echo "QP2_ENV=test" >> .env
```

Then load the environment before starting any QP2 application:
```bash
set -a; source .env; set +a
```

> **Tip:** Add `set -a; source /path/to/qp2/.env; set +a` to your `~/.bashrc` or shell activation script so it loads automatically.

### Full variable reference

| Variable | Default | Description |
|----------|---------|-------------|
| `QP2_ENV` | `prod` | Set to `test` to force all connections to localhost |
| `QP2_JWT_SECRET_KEY` | *(none)* | **Required** for web app; generate with `secrets.token_hex(32)` |
| `QP2_LDAP_SERVER` | `ldap.example.org` | LDAP server for web app login |
| `QP2_LDAP_USER_DN_TEMPLATE` | `uid={username},...` | LDAP DN template |
| `QP2_KRB5_REALM` | | Kerberos realm (optional) |
| `QP2_TEST_USER` / `QP2_TEST_PASS` | | Dev login bypass (never use in production) |
| `QP2_PG_HOST` | `localhost` | PostgreSQL host |
| `QP2_PG_PORT` | `5432` | PostgreSQL port |
| `QP2_PG_USER` | `qp2user` | PostgreSQL username |
| `QP2_PG_PASS` | | PostgreSQL password |
| `QP2_PG_DB` | `user_data` | PostgreSQL database name |
| `MYSQL_HOST_BL1` | `localhost` | MySQL host (beamline 1) |
| `MYSQL_HOST_BL2` | `localhost` | MySQL host (beamline 2) |
| `MYSQL_USER` | `qp2user` | MySQL username |
| `MYSQL_PASS` | | MySQL password |
| `REDIS_HOST_BL1` | `127.0.0.1` | Redis host for beamline 1 stream |
| `REDIS_HOST_BL2` | `127.0.0.1` | Redis host for beamline 2 stream |
| `REDIS_HOST_ANALYSIS_RESULTS` | `127.0.0.1` | Redis host for analysis results |
| `DATAPROC_SERVER_URL` | `http://localhost:8025` | Data processing server URL |
| `AI_SERVER_URL` | `http://localhost:8888/v1` | AI assistant backend URL |
| `QP2_LOG_FILE` | | Path for log file (stdout if unset) |

---

## 6. Configure External Programs

QP2 can integrate with optional crystallography software: XDS, CrystFEL, DIALS, DOZOR, autoPROC, CCP4, xia2, and SHELX.

Edit `config/programs.json` to point to how each program is loaded on your system:

```json
{
    "dials":    "module load dials",
    "crystfel": "module load crystfel",
    "xds":      "module load xds",
    "xia2":     "module load dials",
    "autoproc": "module load autoproc",
    "dozor":    "module load dozor",
    "ccp4":     "module load ccp4",

    "lib_xds-zcbf":        "/path/to/xds-zcbf.so",
    "lib_dectris-neggia":  "/path/to/dectris-neggia.so",
    "lib_raddose3d":       "/path/to/raddose3d.jar",

    "prog_dozor":          "dozor2q",
    "prog_dials_python":   "/path/to/dials.python"
}
```

Alternatively, set per-program environment variables without editing the file:

```bash
export QP2_SETUP_XDS="source /opt/xds/setup.sh"
export QP2_PROG_DOZOR="dozor2q"
export QP2_LIB_XDS_ZCBF="/opt/xds/xds-zcbf.so"
```

If a program is not installed, its corresponding plugin tab will be disabled in the UI — QP2 will still run normally.

---

## 7. Set Up Optional Services

### Redis (required for live-mode streaming)

Redis is needed if you want the image viewer to receive live data from a detector controller. For standalone (offline) viewing it is not required.

```bash
# Start Redis locally
sudo systemctl start redis
# or
redis-server --daemonize yes

# Verify
redis-cli ping    # should return: PONG
```

### PostgreSQL (optional — for the data viewer database)

The data viewer falls back to a local SQLite file (`~/.data_viewer/user_data.db`) if PostgreSQL is unavailable. To use PostgreSQL:

```bash
# Install and start
sudo apt install postgresql
sudo systemctl start postgresql

# Create a database and user
sudo -u postgres psql <<'SQL'
CREATE USER qp2user WITH PASSWORD 'yourpassword';
CREATE DATABASE user_data OWNER qp2user;
SQL

# Set the environment variables
export QP2_PG_HOST=localhost
export QP2_PG_USER=qp2user
export QP2_PG_PASS=yourpassword
export QP2_PG_DB=user_data
```

Detailed PostgreSQL setup instructions are in [install_postgresql_instructions.md](install_postgresql_instructions.md).

### MySQL / MariaDB (optional — for beamline sample tracking)

Only needed if your facility uses a MySQL-backed sample tracking system. Set `MYSQL_HOST_BL1`, `MYSQL_USER`, and `MYSQL_PASS` to point to your instance.

---

## 8. Verify the Installation

```bash
# Check the package is importable
python -c "import qp2; print(qp2.__version__)"
# Expected: 2.0.0

# Check all core imports work
python -c "
from qp2.config.servers import ServerConfig
from qp2.xio.hdf5_manager import HDF5Reader
from qp2.image_viewer.ui.main import main
print('Core imports OK')
"

# Run the test suite
QP2_ENV=test pytest tests/ -q
```

---

## 9. Running the Applications

Make sure your environment variables are loaded (`source .env` or equivalent) before starting.

### Image Viewer (main application)

```bash
qp2-image-viewer
# or equivalently:
python -m qp2.image_viewer.ui.main
```

Opens the Qt-based diffraction image viewer. Supports HDF5/EIGER files, live-mode streaming from a Redis-connected detector, peak finding, and crystallographic analysis plugins.

**Live mode** requires Redis to be running and the detector controller to be publishing to the configured Redis stream.

### Data Viewer

```bash
qp2-data-viewer
```

Database-backed viewer for browsing historical datasets and analysis results. Works standalone with the SQLite fallback database.

### Data Processing Server

```bash
qp2-dp-server
# Listens on port 8025 by default
```

HTTP server that manages pipeline jobs (XDS, CrystFEL, DIALS, etc.) on the local machine or via SLURM. Must be running for the image viewer's "Run Processing" actions to work.

### Web Application

```bash
# Start the backend API server
qp2-web-server
# Listens on port 8000

# The frontend is served as static files from web_app/frontend/dist/
# Build it first (requires Node.js / npm):
cd web_app/frontend
npm install
npm run build
```

The web app provides a browser-based interface. Navigate to `http://localhost:8000` after starting.

### Command-line tools (in `bin/`)

| Command | Description |
|---------|-------------|
| `bin/iv` | Shortcut to launch image viewer |
| `bin/dv` | Shortcut to launch data viewer |
| `bin/dp` | Data processing client |
| `bin/serial_xds` | Serial crystallography XDS pipeline |
| `bin/xtallife` | Crystal radiation lifetime calculator |
| `bin/strategy` | Data collection strategy tool |
| `bin/mock_collect` | Simulate a detector data collection (for testing) |
| `bin/mock_streamer` | Simulate a Redis data stream (for testing live mode) |

Add the `bin/` directory to your `PATH`:
```bash
export PATH="$PATH:/path/to/qp2/bin"
```

---

## 10. Troubleshooting

### `ImportError: No module named 'PyQt5'`
Your environment is not activated. Run `conda activate qp2` (or `source ~/qp2-env/bin/activate`) and retry.

### `qt.qpa.xcb: could not connect to display`
The image viewer needs a display. On a headless server, use a virtual framebuffer:
```bash
sudo apt install xvfb
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99
qp2-image-viewer
```

### `RuntimeError: QP2_JWT_SECRET_KEY environment variable must be set`
The web app requires this variable. Set it in your `.env` file:
```bash
python -c "import secrets; print(secrets.token_hex(32))"
# paste the output as QP2_JWT_SECRET_KEY=... in .env
```

### `redis.exceptions.ConnectionError: Error 111 connecting to 127.0.0.1:6379`
Redis is not running. Start it:
```bash
sudo systemctl start redis    # systemd
# or
redis-server --daemonize yes
```

### Image viewer opens but shows no data
Open a file with **File → Open** and select an HDF5 master file (`*_master.h5`). For live mode, check that Redis is running and the detector is streaming.

### `OperationalError` or `ProgrammingError` from the data viewer
The database is not set up. Either set `QP2_ENV=test` to use SQLite, or follow [step 7](#7-set-up-optional-services) to configure PostgreSQL.

### Processing plugins (XDS, CrystFEL, etc.) are greyed out
The corresponding external program is not found. Check `config/programs.json` and make sure the setup command (e.g., `module load xds`) works in your shell. See [step 6](#6-configure-external-programs).

---

## Appendix: Quick-Start Summary

```bash
# 1. Clone
git clone https://github.com/your-org/qp2.git && cd qp2

# 2. Environment
conda create -n qp2 python=3.12 -y && conda activate qp2

# 3. Install
pip install -e .

# 4. Configure
cp .env.example .env
# edit .env — at minimum set QP2_ENV=test for local use
set -a; source .env; set +a

# 5. Start Redis (for live mode)
redis-server --daemonize yes

# 6. Launch
qp2-image-viewer
```
