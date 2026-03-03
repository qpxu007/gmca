#!/bin/bash

# Source the standardized environment configuration
SCRIPT_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
source "$SCRIPT_DIR/../../bin/qp2_env.sh"

# Load your specific module
# eval "${QP2_SETUP_OPENCV:-. /etc/profile.d/modules.sh; module load opencv}"

echo "Starting Python application with PYTHONPATH: $PYTHONPATH"
echo "Using Python interpreter: $MYPYTHON"

# Execute the Python script, check config.py for port to be used
# The --enable-http-server flag is used to enable the HTTP server
# The --max-workers flag is used to set the number of worker processes
# The --enable-db-logging flag is used to enable database logging of datasets to mysql database
exec $MYPYTHON -m qp2.data_proc.server.data_processing_server --enable-http-server --max-workers=6 --enable-db-logging
