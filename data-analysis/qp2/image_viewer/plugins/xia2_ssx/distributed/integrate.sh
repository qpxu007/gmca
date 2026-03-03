#!/bin/bash
# Wraps a single xia2.ssx integration job
# Args: <dataset_path> <status_key> <config_path>

DATASET=$1
STATUS_KEY=$2
CONFIG_PATH=$3

# Extract work root from config path (assumed to be in the same dir)
WORK_ROOT=$(dirname "$CONFIG_PATH")

# Source environment setup
if [ -f "$WORK_ROOT/setup_env.sh" ]; then
    source "$WORK_ROOT/setup_env.sh"
else
    echo "Warning: setup_env.sh not found in $WORK_ROOT"
fi

# Python helper for Redis updates
update_status() {
    STATUS=$1
    MSG=$2
    # Ensure update_status.py exists
    if [ -f "$WORK_ROOT/update_status.py" ]; then
        python3 "$WORK_ROOT/update_status.py" "$STATUS_KEY" "$STATUS" "$MSG"
    fi
}

# Trap signals
trap 'update_status "FAILED" "Job terminated"' SIGTERM SIGINT

update_status "RUNNING"

# Execute integration wrapper
# arguments: dataset_path status_key config_path
python3 "$WORK_ROOT/integrate_wrapper.py" "$DATASET" "$STATUS_KEY" "$CONFIG_PATH"
RET=$?

if [ $RET -eq 0 ]; then
    update_status "SUCCESS"
else
    update_status "FAILED" "Process exited with $RET"
fi
exit $RET
