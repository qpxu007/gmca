# QP2 Configuration System

This directory contains the core configuration logic for the QP2 data processing suite. The system is designed to be flexible, allowing deployment across different environments (clusters, local workstations, test servers) without code modification.

## Core Components

### 1. `programs.py` & `programs.json` (External Program Management)
This component manages the environment setup and execution paths for external crystallographic software (DIALS, XDS, CrystFEL, etc.).

**The Problem:** 
Different facilities install software in different locations and use different environment modules. Hardcoding `module load dials` or `/usr/local/bin/xds` breaks portability.

**The Solution:**
`ProgramConfig` generates the necessary shell commands dynamically. It follows a strict priority order to resolve how to run a program:

1.  **Environment Variable Override:** (Highest Priority)
    If `QP2_SETUP_<PROGRAM_NAME>` is set, its value is used directly.
    *   *Example:* `export QP2_SETUP_DIALS="source /opt/dials/dials_env.sh"`
    *   *Note:* Setting this to an empty string `""` tells QP2 to run the program directly without any setup command.

2.  **JSON Configuration:**
    The system looks for a `programs.json` file (defaulting to this directory). If a key exists for the program, that command is used.
    *   *Example:* `"dials": "module load dials-v3"`

3.  **Standard Default:** (Lowest Priority)
    If neither of the above are found, it generates a standard facility command:
    `export PX=...; export MODULEPATH=...; . /etc/profile.d/modules.sh; module load <program> || exit 1`

**Library Management:**
Similarly, `get_library_path("lib_name")` resolves shared object paths (like `dectris-neggia.so`) using the same priority logic (Env Var `QP2_LIB_<NAME>` -> JSON -> Default).

### 2. `servers.py` (Service Configuration)
This file manages the connection details for internal services (Data Processing Server, Redis, Database, Dose Planner).

**Key Features:**
*   **Environment Modes:** Checks `QP2_ENV` environment variable.
    *   `prod` (default): Uses standard production IPs and auto-detects beamline-specific services based on hostname.
    *   `test`: Forces all connections to `localhost` / `127.0.0.1` for safe local development.
*   **Service URLs:** Defaults to `localhost` for all services; override via environment variables (e.g., `DATAPROC_SERVER_URL`, `QP2_PG_HOST`) for remote deployments.

## Usage Guide

### For Users / Deployment
To customize QP2 for your environment, you do not need to change python code.

**Option A: Edit `programs.json`**
Create or edit `qp2/config/programs.json`:
```json
{
    "dials": "module load dials-3.14",
    "xds": "/opt/xds/xds_par",
    "lib_dectris-neggia": "/usr/local/lib/dectris-neggia.so"
}
```

**Option B: Environment Variables**
Set variables in your shell or startup script (`.bashrc`):
```bash
export QP2_SETUP_DIALS="module load my-dials"
export QP2_LIB_RADDOSE3D="/home/user/apps/raddose.jar"
```

### For Developers
When writing code that needs to run an external program, **never** hardcode the setup command.

**Bad:**
```python
cmd = "module load dials && dials.find_spots ..."
```

**Good:**
```python
from qp2.config.programs import ProgramConfig

setup = ProgramConfig.get_setup_command("dials")
cmd = f"{setup} && dials.find_spots ..."
```

This ensures your code will run anywhere, respecting the user's local configuration.
