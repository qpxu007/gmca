#!/bin/bash
#
# Batch script to run plot_masked_pixels on a single master file or
# find HDF5 master files recursively in a directory and run on each.
#
# Usage: ./batch_plot_masked_pixels.sh [options] <master_file_or_directory>
#
# Options:
#   -q, --quit              Automatically quit after displaying each plot (no GUI interaction)
#   -t, --threshold N       Threshold for highlighting large differences (default: 100)
#   -s, --skip-threshold N  Skip displaying plot if max absolute difference is less than this value (default: 10)
#   -n, --no-display        Disable GUI display. Only output paths of master files with issues.
#   -e, --suppress-errors   Suppress error messages (redirect stderr to /dev/null)
#
# Examples:
#   ./batch_plot_masked_pixels.sh /path/to/data_master.h5
#   ./batch_plot_masked_pixels.sh /path/to/data
#   ./batch_plot_masked_pixels.sh -q /path/to/data
#   ./batch_plot_masked_pixels.sh -n -t 50 -s 20 /path/to/data
#   ./batch_plot_masked_pixels.sh -n -e /path/to/data
#

set -e

# Initialize option variables
QUIT_FLAG=""
THRESHOLD_FLAG=""
SKIP_THRESHOLD_FLAG=""
NO_DISPLAY_FLAG=""
SUPPRESS_ERRORS=""

# Parse options
while [[ $# -gt 0 ]]; do
    case "$1" in
        -q|--quit)
            QUIT_FLAG="-q"
            shift
            ;;
        -t|--threshold)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: --threshold requires a numeric argument"
                exit 1
            fi
            THRESHOLD_FLAG="-t $2"
            shift 2
            ;;
        -s|--skip-threshold)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: --skip-threshold requires a numeric argument"
                exit 1
            fi
            SKIP_THRESHOLD_FLAG="-s $2"
            shift 2
            ;;
        -n|--no-display)
            NO_DISPLAY_FLAG="-n"
            shift
            ;;
        -e|--suppress-errors)
            SUPPRESS_ERRORS="1"
            shift
            ;;
        -*)
            echo "Unknown option: $1"
            echo "Usage: $0 [options] <master_file_or_directory>"
            echo ""
            echo "Options:"
            echo "  -q, --quit              Automatically quit after displaying each plot (no GUI interaction)"
            echo "  -t, --threshold N       Threshold for highlighting large differences (default: 100)"
            echo "  -s, --skip-threshold N  Skip displaying plot if max absolute difference is less than this value (default: 10)"
            echo "  -n, --no-display        Disable GUI display. Only output paths of master files with issues."
            echo "  -e, --suppress-errors   Suppress error messages (redirect stderr to /dev/null)"
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

# Check arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 [options] <master_file_or_directory>"
    echo "  If a file is provided, runs plot_masked_pixels on that file."
    echo "  If a directory is provided, recursively finds master files (*_master.h5) and runs plot_masked_pixels on each."
    echo ""
    echo "Options:"
    echo "  -q, --quit              Automatically quit after displaying each plot (no GUI interaction)"
    echo "  -t, --threshold N       Threshold for highlighting large differences (default: 100)"
    echo "  -s, --skip-threshold N  Skip displaying plot if max absolute difference is less than this value (default: 10)"
    echo "  -n, --no-display        Disable GUI display. Only output paths of master files with issues."
    echo "  -e, --suppress-errors   Suppress error messages (redirect stderr to /dev/null)"
    exit 1
fi

INPUT="$1"

# Build the flags string
FLAGS="$QUIT_FLAG $THRESHOLD_FLAG $SKIP_THRESHOLD_FLAG $NO_DISPLAY_FLAG"
# Trim leading/trailing whitespace
FLAGS=$(echo "$FLAGS" | xargs)

# Change to the data-analysis directory and load the module
cd ~qxu/data-analysis
eval "${QP2_SETUP_PY313:-. /etc/profile.d/modules.sh; module load py313}"

# Function to run the python command with optional error suppression
# Outputs directly to stdout, returns exit code
run_plot() {
    local master_file="$1"
    if [ -n "$SUPPRESS_ERRORS" ]; then
        python -m qp2.tools.plot_masked_pixels $FLAGS "$master_file" 2>/dev/null || true
    else
        python -m qp2.tools.plot_masked_pixels $FLAGS "$master_file" || true
    fi
}

# Function to print progress to stderr - overwrites current line
print_progress() {
    printf "\r\033[K%s" "$1" >&2
}

# Function to clear progress line and move to new line
clear_progress() {
    printf "\r\033[K" >&2
}

# Check if input is a file or directory
if [ -f "$INPUT" ]; then
    # Single file mode
    if [ -z "$NO_DISPLAY_FLAG" ]; then
        echo "Processing single file: $INPUT"
        echo "=============================================="
        run_plot "$INPUT"
        echo ""
        echo "Completed: $INPUT"
    else
        print_progress "[1/1] Processing: $(basename "$INPUT")"
        clear_progress
        run_plot "$INPUT"
        echo "Completed: 1 file(s) processed." >&2
    fi

elif [ -d "$INPUT" ]; then
    # Directory mode - find and process all master files
    if [ -z "$NO_DISPLAY_FLAG" ]; then
        echo "Searching for master files in: $INPUT"
        echo "=============================================="
    else
        echo "Searching for master files in: $INPUT" >&2
    fi

    # Find all master files recursively
    # Common patterns: *_master.h5, *_master.hdf5
    MASTER_FILES=$(find "$INPUT" -type f \( -name "*_master.h5" -o -name "*_master.hdf5" \) 2>/dev/null | sort)

    if [ -z "$MASTER_FILES" ]; then
        if [ -z "$NO_DISPLAY_FLAG" ]; then
            echo "No master files found in $INPUT"
        else
            echo "No master files found in $INPUT" >&2
        fi
        exit 0
    fi

    # Count files
    FILE_COUNT=$(echo "$MASTER_FILES" | wc -l)
    if [ -z "$NO_DISPLAY_FLAG" ]; then
        echo "Found $FILE_COUNT master file(s)"
        echo ""
    else
        echo "Found $FILE_COUNT master file(s)" >&2
    fi

    # Process each file
    CURRENT=0
    while IFS= read -r master_file; do
        CURRENT=$((CURRENT + 1))
        if [ -z "$NO_DISPLAY_FLAG" ]; then
            echo "[$CURRENT/$FILE_COUNT] Processing: $master_file"
            echo "----------------------------------------------"
            run_plot "$master_file"
            echo ""
            echo "Completed: $master_file"
            echo ""
        else
            # Show progress on stderr with carriage return - overwrites previous line
            print_progress "[$CURRENT/$FILE_COUNT] Processing: $(basename "$master_file")"
            
            # Clear progress line before running command so output appears on fresh line
            clear_progress
            
            # Run the command - output goes directly to stdout
            run_plot "$master_file"
        fi
    done <<< "$MASTER_FILES"

    if [ -z "$NO_DISPLAY_FLAG" ]; then
        echo "=============================================="
        echo "Batch processing complete. Processed $FILE_COUNT file(s)."
    else
        echo "Batch processing complete. Processed $FILE_COUNT file(s)." >&2
    fi

else
    echo "Error: '$INPUT' is not a valid file or directory."
    exit 1
fi
