# QP2 Local Installation Guide

This document describes how to install and run the QP2 data processing suite on a local computer.

## System Requirements
- **OS**: Linux (Ubuntu recommended)
- **Python**: 3.12 (Recommended) or 3.9+
- **Database**: PostgreSQL (Optional) and MariaDB/MySQL (Optional). *If no database is installed, QP2 will automatically create and use a local SQLite file (`~/.data_viewer/user_data.db`).*
- **Cache/Broker**: Redis

## The Test Environment (`QP2_ENV=test`)
For local development and testing, you must set the environment variable **`QP2_ENV=test`**. 

When this variable is set to `test`, the QP2 `config/servers.py` module forces all internal connections (Data Processing Server, WebSocket Server, Redis, PostgreSQL, MySQL fallback, Dose Planner, AI Server, PBS RPC, etc.) to bind to `localhost` (`127.0.0.1`). This prevents the application from attempting to connect to the facility's cluster or beamline production servers, ensuring a safe and isolated testing environment.

## Installation Steps

### 1. System Dependencies
Install Redis first. On Ubuntu:
```bash
sudo apt update
sudo apt install redis-server
```
*(Optional: If you want to use the full database stack instead of the SQLite fallback, you can also install `postgresql postgresql-contrib mariadb-server`)*

### 2. Configure PostgreSQL for Local Access (OPTIONAL)
*Note: You can skip this step entirely. If PostgreSQL is missing, the application will automatically fall back to saving all user data in a local SQLite database file at `~/.data_viewer/user_data.db`.*

If you prefer to test with PostgreSQL, you need to create the database and a user named `dhs`. You also need to configure trust authentication for local testing connection.

1. Open the PostgreSQL shell:
   ```bash
   sudo -u postgres psql
   ```
2. Execute the setup queries:
   ```sql
   CREATE USER dhs;
   CREATE DATABASE user_data;
   ALTER DATABASE user_data OWNER TO dhs;
   GRANT ALL PRIVILEGES ON DATABASE user_data TO dhs;
   \q
   ```
3. Edit your PostgreSQL `pg_hba.conf` to add trust authentication:
   ```bash
   # Find your version (e.g. 14) and edit the file
   sudo nano /etc/postgresql/14/main/pg_hba.conf
   ```
   Add the following line under the local connections area to allow password-less access for the `dhs` user on `user_data`:
   ```text
   local   user_data       dhs                                     trust
   ```
4. Restart PostgreSQL:
   ```bash
   sudo systemctl restart postgresql
   ```

*(See `install_postgresql_instructions.md` for more advanced remote subnet connections)*

### 3. Configure MariaDB for Local Access (OPTIONAL)
*Note: You can skip this step entirely if you don't have access to or don't need to test beamline-specific user groups (`gmca_accounts`) or PBS clusters (`blc2004`). If these databases are missing, QP2 falls back to system accounts and local SQLite databases gracefully.*

If you want to set them up, you need to create the required databases (`user_data`, `blc2004`, `gmca_accounts`) and a user named `dhs` for local MySQL connections.

1. Open the MariaDB shell:
   ```bash
   sudo mysql
   ```
2. Execute the setup queries:
   ```sql
   CREATE USER 'dhs'@'localhost' IDENTIFIED BY '';
   CREATE DATABASE user_data;
   CREATE DATABASE blc2004;
   CREATE DATABASE gmca_accounts;
   GRANT ALL PRIVILEGES ON user_data.* TO 'dhs'@'localhost';
   GRANT ALL PRIVILEGES ON blc2004.* TO 'dhs'@'localhost';
   GRANT ALL PRIVILEGES ON gmca_accounts.* TO 'dhs'@'localhost';
   FLUSH PRIVILEGES;
   exit
   ```

### 4. Python Environment Setup
We recommend using a virtual environment (e.g., `venv` or `conda`).

1. Clone the repository and navigate into the project directory:
   ```bash
   git clone https://github.com/AdvancedPhotonSource/qp2.git
   cd qp2
   ```

2. Create and activate a Conda or Python virtual environment:
   ```bash
   # Using conda
   conda create -n qp2 python=3.12
   conda activate qp2
   
   # OR using venv
   python3 -m venv qp2_env
   source qp2_env/bin/activate
   ```

3. Install the package and its dependencies. QP2 is designed to be installable via `pip` from its `pyproject.toml` file:
   ```bash
   pip install -e .
   ```
   *(This will install PyQt5, numpy, scipy, h5py, hdf5plugin, pymysql, psycopg2-binary, redis, fastapi, requests, and other defined requirements).*

### 5. Running the Applications Locally

You can run the suite of applications provided by QP2. **Crucially, ensure you export the test environment variable before running anything.**

```bash
# Set environment to local test mode
export QP2_ENV=test

# Run the Image Viewer
qp2-image-viewer
# (or optionally use scripts in bin: ./bin/iv)

# Run the Data Viewer
qp2-data-viewer
# (or optionally use scripts in bin: ./bin/dv)

# Run Data Processing Server
qp2-dp-server

# Run Web Backend Server
qp2-web-server
```

### Configuring External Programs locally
By default, QP2 outputs cluster `module load` commands for crystallography tools (DIALS, XDS, etc) unless configured otherwise. To run these programs locally, specify their executable setup instructions in `qp2/config/programs.json`, or override them via environment variables:

```bash
export QP2_SETUP_XDS="/usr/local/bin/xds_par"
export QP2_SETUP_DIALS="source /opt/dials/dials_env.sh"
```
Or use the empty string `""` to execute binaries directly if they are already in your system `$PATH`:
```bash
export QP2_SETUP_XDS=""
```
