#!/bin/bash
set -e

# Configuration
OUTPUT_NAME="qp2_environment.tar.gz"
ENV_NAME="${CONDA_DEFAULT_ENV:-base}" # Uses current active env by default

echo "=== QP2 Conda Environment Packer ==="
echo "Target Environment: $ENV_NAME"
echo "Output File: $OUTPUT_NAME"

# Check if conda-pack is installed
if ! command -v conda-pack &> /dev/null; then
    echo "conda-pack not found. Installing..."
    # Try pip first as it is often faster/easier in existing envs
    pip install conda-pack
fi

echo "Packing environment..."
# --ignore-missing-files is useful if some cached files are gone
conda-pack -o "$OUTPUT_NAME" --ignore-missing-files

echo ""
echo "=== Build Complete ==="
echo "Environment packed to: $OUTPUT_NAME"
echo ""
echo "To distribute:"
echo "1. Copy $OUTPUT_NAME to the target machine."
echo "2. Run the following on the target machine:"
echo "   mkdir qp2_env"
echo "   tar -xzf $OUTPUT_NAME -C qp2_env"
echo "   source qp2_env/bin/activate"
echo "   python -m qp2.image_viewer.ui.main"
