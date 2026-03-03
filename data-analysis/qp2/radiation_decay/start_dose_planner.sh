#!/bin/bash

# This script finds the server's IP in the 10.20.x.x subnet
# and launches the Dose Planner Uvicorn server, binding to that IP.

# --- Find the IP Address ---
# 1. `ip addr`: List all network interfaces and addresses.
# 2. `grep 'inet 10.20.'`: Filter for lines containing an IPv4 address starting with "10.20.".
# 3. `awk '{print $2}'`: From the filtered lines, extract the second field (e.g., "10.20.5.100/16").
# 4. `cut -d'/' -f1`: Treat "/" as a delimiter and get the first part (the IP address).
# 5. `head -n 1`: In case there are multiple matching IPs, just take the first one.
IP_ADDRESS=$(ip addr | grep 'inet 10.20.' | awk '{print $2}' | cut -d'/' -f1 | head -n 1)

# --- Validate that an IP was found ---
if [ -z "$IP_ADDRESS" ]; then
    # Log an error to stderr (which will be visible in `journalctl`) and exit.
    echo "Error: Could not find an IP address in the 10.20.x.x range. Aborting." >&2
    exit 1
fi

# --- Launch the Server ---
# Log the IP we are using for debugging purposes.
echo "Found IP: $IP_ADDRESS. Starting Dose Planner server on http://${IP_ADDRESS}:5000"

# Use 'exec' to replace the shell process with the uvicorn process.
# This is good practice for systemd services.
# The $UVICORN_PATH variable will be set by the systemd service file.
exec "$UVICORN_PATH" qp2.radiation_decay.server:app --host "$IP_ADDRESS" --port 5000 --workers 4
