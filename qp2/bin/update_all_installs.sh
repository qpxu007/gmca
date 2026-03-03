#!/bin/bash

# List of QP2 installation directories to update
DIRECTORIES=(
    "/opt/data-analysis/"
    "/mnt/beegfs/.software_bl2/data-analysis"
    "/mnt/beegfs/.software_bl1/data-analysis"
    "/mnt/beegfs/qxu/data-analysis"
)

# Iterate through each directory
for dir in "${DIRECTORIES[@]}"; do
    if [ -d "$dir" ]; then
        echo "Updating $dir..."
        # Use a subshell to avoid changing the script's current working directory permanently
        (
            cd "$dir" || exit
            # Check if it is a git repository
            if [ -d ".git" ]; then
                git pull
            else
                echo "Warning: $dir is not a git repository."
            fi
        )
        echo "---------------------------------------------------"
    else
        echo "Directory $dir does not exist. Skipping."
        echo "---------------------------------------------------"
    fi
done
