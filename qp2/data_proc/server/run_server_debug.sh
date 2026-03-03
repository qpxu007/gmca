#!/bin/bash

find_ancestor_dir() {
    # Usage: find_ancestor_dir <start_dir> <ancestor_name>
    local current_path="$1"
    local target_ancestor_name="$2"

    while [ "$current_path" != "/" ] && [ "$current_path" != "." ]; do
        if [ "$(basename "$current_path")" = "$target_ancestor_name" ]; then
            echo "$current_path"
            return 0
        fi
        current_path="$(dirname "$current_path")"
    done

    # If not found, return empty and non-zero exit code
    return 1
}

# Find the correct module initialization script for your system.
# Common locations/names include:
# - /etc/profile.d/modules.sh  (Environment Modules - Tmod)
# - /etc/profile.d/lmod.sh     (Lmod)
# - /usr/share/modules/init/bash (Some Tmod versions)
# - Or a path specific to your HPC environment or software stack.
# Replace the line below with the correct one for your system.
MODULE_INIT_SCRIPT="/etc/profile.d/modules.sh" # <--- !!! IMPORTANT: VERIFY AND CHANGE THIS PATH !!!

if [ -f "$MODULE_INIT_SCRIPT" ]; then
  # Source the module system's initialization script to make 'module' command available
  # The '.' command is an alias for 'source'
  . "$MODULE_INIT_SCRIPT"
else
  echo "ERROR: Module initialization script not found at $MODULE_INIT_SCRIPT" >&2
  exit 1
fi

export PX=/mnt/software/px
export MODULEPATH=/mnt/software/px/modulefiles:$MODULEPATH

# Load your specific module
eval "${QP2_SETUP_OPENCV:-. /etc/profile.d/modules.sh; module load opencv}"


# Determine the Python interpreter to use
if [ -x "/mnt/software/px/miniconda3/envs/opencv/bin/python" ]; then
    MYPYTHON="/mnt/software/px/miniconda3/envs/opencv/bin/python"
else
    echo "Error: No suitable python interpreter found."
    echo "Checked paths:"
    echo "  - /mnt/software/px/miniconda3/envs/opencv/bin/python"
    exit 1
fi

# Resolve the script's actual directory, even if called via a symlink
SCRIPT_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
echo "Script_dir: $SCRIPT_DIR"
project_name="qp2"


PROJECT_DIR=$(find_ancestor_dir "$SCRIPT_DIR" "$project_name")
if [ $? -eq 0 ] && [ -n "$PROJECT_DIR" ]; then
    echo "Found 'quickProcess2/qp2' for mock SCRIPT_DIR: $PROJECT_DIR"
else
    echo "'quickProcess2/qp2' not found above $SCRIPT_DIR."
fi

PROJECT_ROOT=$(dirname "${PROJECT_DIR}") # parent of project
echo "PROJECT ROOT $PROJECT_ROOT"



# Set the PYTHONPATH to include the project root
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Execute the Python script, check config.py for port to be used

# The --enable-http-server flag is used to enable the HTTP server
# The --max-workers flag is used to set the number of worker processes
# The --enable-db-logging flag is used to enable database logging of datasets to mysql database
echo "Starting Python application with PYTHONPATH: $PYTHONPATH"
echo "Using Python interpreter: $MYPYTHON"
exec $MYPYTHON -m qp2.data_proc.server.data_processing_server --run-dozor-only

