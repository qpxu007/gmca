#!/bin/bash

# ==============================================================================
# QP2 Environment Setup Script
# ==============================================================================
# This script standardizes the setup for all QP2 bash launchers.
# It locates the project root, sets PYTHONPATH, and finds a Python interpreter.
#
# Usage:
#   Source this script at the beginning of your bash script:
#   SCRIPT_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
#   source "$SCRIPT_DIR/qp2_env.sh"
# ==============================================================================

# Function to find an ancestor directory by name
find_ancestor_dir() {
    local current_path="$1"
    local target_ancestor_name="$2"

    if [ ! -d "$current_path" ]; then
        return 1
    fi

    while true; do
        if [ "$(basename "$current_path")" == "$target_ancestor_name" ]; then
            echo "$current_path"
            return 0
        fi
        local parent_of_current=$(dirname "$current_path")
        if [ "$parent_of_current" == "$current_path" ] || [ -z "$parent_of_current" ]; then
            return 1
        fi
        current_path="$parent_of_current"
    done
}

# 1. Determine Project Paths
# ------------------------------------------------------------------------------
# Assuming this script is sourced by a script in qp2/bin or similar depth.
# We start searching from the SCRIPT_DIR provided by the caller, or determine it here.
# Note: When sourced, BASH_SOURCE[0] is this file. We need the caller's location usually,
# but since we are finding 'qp2' upwards, finding it from this file's location works too
# if this file is in qp2/bin.

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PROJECT_NAME="qp2"

# --- Project Root Prioritization Feature (Disabled) ---
# To activate this feature, uncomment the block below and ensure CANDIDATE_LOCATIONS
# is defined. This allows the script to prioritize installations from specific
# locations (e.g., local disk) over the one discovered relative to the script's
# location.

CANDIDATE_LOCATIONS=(
    "/opt/data-analysis/qp2"
    "/usr/local/data-analysis/qp2"
    "/mnt/software/data-analysis/qp2"
    "/mnt/beegfs/qxu/data-analysis/qp2"
)

PROJECT_DIR=""
for loc in "${CANDIDATE_LOCATIONS[@]}"; do
    if [ -d "$loc" ]; then
        PROJECT_DIR="$loc"
        echo "Using installation at: $PROJECT_DIR"
        break
    fi
done
# --- End Project Root Prioritization Feature ---


# Fallback: Determine project directory relative to this script (always used if prioritization is disabled or fails)
# If PROJECT_DIR was set by prioritization, this block will be skipped.
if [ -z "$PROJECT_DIR" ]; then
    PROJECT_DIR=$(find_ancestor_dir "$THIS_DIR" "$PROJECT_NAME")
fi


if [ $? -ne 0 ] || [ -z "$PROJECT_DIR" ]; then
    echo "Error: Could not find project directory '$PROJECT_NAME' starting from '$THIS_DIR'." >&2
    exit 1
fi

PROJECT_ROOT=$(dirname "$PROJECT_DIR") # parent of project
# echo "Project Root: $PROJECT_ROOT"

# Export project variables
export PROJECT_DIR
export PROJECT_ROOT

# --- HDF5 File Monitoring ---
# Frequency of checking for new data on disk (in milliseconds).
# Tuning this can improve responsiveness on slow distributed filesystems (e.g. BeeGFS).
export QP2_HDF5_POLL_INTERVAL_MS="${QP2_HDF5_POLL_INTERVAL_MS:-200}"

# Set ulimit for open files
ulimit -n 4096

# 2. Set PYTHONPATH
# ------------------------------------------------------------------------------
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# 3. Determine Python Interpreter
# ------------------------------------------------------------------------------
# Priority:
# 1. Environment variable QP2_PYTHON
# 2. Active virtual environment ($VIRTUAL_ENV set by venv/conda activate)
# 3. Local environment folders relative to PROJECT_ROOT
# 4. Hardcoded candidate paths

MYPYTHON=""

if [ -n "$QP2_PYTHON" ] && [ -x "$QP2_PYTHON" ]; then
    MYPYTHON="$QP2_PYTHON"
fi

# Use the active virtual environment if one is activated
if [ -z "$MYPYTHON" ] && [ -n "$VIRTUAL_ENV" ] && [ -x "$VIRTUAL_ENV/bin/python" ]; then
    MYPYTHON="$VIRTUAL_ENV/bin/python"
fi

# Check for local environment folders if QP2_PYTHON is not set
if [ -z "$MYPYTHON" ]; then
    LOCAL_ENV_CANDIDATES=(
        "$PROJECT_ROOT/qp2_env/bin/python"
        "$PROJECT_ROOT/qp2-env/bin/python"
        "$PROJECT_DIR/qp2_env/bin/python"
        "$PROJECT_DIR/qp2-env/bin/python"
        "$PROJECT_ROOT/env/bin/python"
        "$PROJECT_ROOT/.venv/bin/python"
        "$PROJECT_DIR/env/bin/python"
        "$PROJECT_DIR/.venv/bin/python"
    )
    for env_py in "${LOCAL_ENV_CANDIDATES[@]}"; do
        if [ -x "$env_py" ]; then
            MYPYTHON="$env_py"
            # echo "Using local environment: $MYPYTHON" >&2
            break
        fi
    done
fi

if [ -z "$MYPYTHON" ]; then
    CANDIDATE_PATHS=(
        "$HOME/qp2-env/bin/python"
        "$HOME/qp2_env/bin/python"
        "/opt/anaconda3/bin/python"
        "/usr/bin/python3"
        "/usr/bin/python"
    )

    for py_path in "${CANDIDATE_PATHS[@]}"; do
        if [ -x "$py_path" ]; then
            MYPYTHON="$py_path"
            break
        fi
    done
fi

if [ -z "$MYPYTHON" ]; then
    echo "Error: No suitable python interpreter found." >&2
    echo "Checked paths:" >&2
    for p in "${CANDIDATE_PATHS[@]}"; do
        echo "  - $p" >&2
    done
    exit 1
fi

# 4. Source Common Environments (Optional)
# ------------------------------------------------------------------------------
# If QP2_BASHRC is set, source it for facility-specific program setup.
if [ -n "$QP2_BASHRC" ] && [ -f "$QP2_BASHRC" ]; then
    . "$QP2_BASHRC"
fi

# 5. Define Cluster Environment Variables (Portable Job Submission)
# ------------------------------------------------------------------------------
# Try to find a python installation that is accessible on cluster nodes (e.g. BeeGFS or NFS)
SHARED_PYTHON_CANDIDATES=(
    "/mnt/beegfs/qxu/data-analysis/qp2_env/bin/python"
    "/mnt/software/data-analysis/qp2_env/bin/python"
    "/opt/data-analysis/qp2/env/bin/python"
)

CLUSTER_PYTHON=""
CLUSTER_PROJECT_ROOT=""

for py in "${SHARED_PYTHON_CANDIDATES[@]}"; do
    if [ -x "$py" ]; then
        CLUSTER_PYTHON="$py"
        # Derive CLUSTER_PROJECT_ROOT.
        # Check if structure is .../qp2_env/bin/python -> .../qp2
        # Or .../qp2/env/bin/python -> .../qp2
        
        env_dir=$(dirname $(dirname "$py")) # e.g. .../qp2_env
        base_dir=$(dirname "$env_dir")      # e.g. .../data-analysis
        
        # Check for 'qp2' directory at base
        if [ -d "$base_dir/qp2" ]; then
            CLUSTER_PROJECT_ROOT="$base_dir/qp2"
        elif [ "$(basename "$(dirname "$py")")" == "bin" ] && [ "$(basename "$(dirname "$(dirname "$py")")")" == "env" ] && [ "$(basename "$(dirname "$(dirname "$(dirname "$py")")")")" == "qp2" ]; then
             # Case: qp2/env/bin/python -> qp2 is parent of env
             CLUSTER_PROJECT_ROOT="$(dirname "$(dirname "$(dirname "$py")")")"
        fi
        
        if [ -n "$CLUSTER_PROJECT_ROOT" ]; then
             break
        fi
    fi
done

export CLUSTER_PYTHON
export CLUSTER_PROJECT_ROOT

# Export the python executable variable for use in scripts
export MYPYTHON
