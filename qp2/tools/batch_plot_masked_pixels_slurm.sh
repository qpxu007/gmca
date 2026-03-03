#!/bin/bash
#
# SLURM-based batch script to run plot_masked_pixels on HDF5 master files.
# Submits one SLURM job per subdirectory for parallel processing across nodes,
# then aggregates all results together.
#
# Usage: ./batch_plot_masked_pixels_slurm.sh [options] <directory>
#
# Options:
#   -t, --threshold N       Threshold for highlighting large differences (default: 100)
#   -s, --skip-threshold N  Skip if max absolute difference is less than this value (default: 10)
#   -p, --partition NAME    SLURM partition to use (default: main)
#   -j, --max-jobs N        Maximum number of concurrent SLURM jobs (default: 100)
#   -w, --wait-interval N   Seconds to wait between checking job status (default: 5)
#   -o, --output-dir DIR    Directory for temporary output files (default: ~/masked_pixels_tmp_$$)
#                           NOTE: Must be on a shared filesystem accessible from all nodes!
#   -k, --keep-temp         Keep temporary files after completion
#
# Examples:
#   ./batch_plot_masked_pixels_slurm.sh /mnt/beegfs/DATA/esaf283015
#   ./batch_plot_masked_pixels_slurm.sh -p long -j 200 /mnt/beegfs/DATA/esaf283015
#   ./batch_plot_masked_pixels_slurm.sh -t 50 -s 20 /mnt/beegfs/DATA/esaf283015
#   ./batch_plot_masked_pixels_slurm.sh -o /mnt/beegfs/scratch/myresults /mnt/beegfs/DATA/esaf283015
#

set -e

# Initialize option variables
THRESHOLD="100"
SKIP_THRESHOLD="10"
PARTITION="main"
MAX_JOBS="100"
WAIT_INTERVAL="5"
OUTPUT_DIR=""
KEEP_TEMP=""

# Parse options
while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--threshold)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: --threshold requires a numeric argument"
                exit 1
            fi
            THRESHOLD="$2"
            shift 2
            ;;
        -s|--skip-threshold)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: --skip-threshold requires a numeric argument"
                exit 1
            fi
            SKIP_THRESHOLD="$2"
            shift 2
            ;;
        -p|--partition)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: --partition requires an argument"
                exit 1
            fi
            PARTITION="$2"
            shift 2
            ;;
        -j|--max-jobs)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: --max-jobs requires a numeric argument"
                exit 1
            fi
            MAX_JOBS="$2"
            shift 2
            ;;
        -w|--wait-interval)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: --wait-interval requires a numeric argument"
                exit 1
            fi
            WAIT_INTERVAL="$2"
            shift 2
            ;;
        -o|--output-dir)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: --output-dir requires an argument"
                exit 1
            fi
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -k|--keep-temp)
            KEEP_TEMP="1"
            shift
            ;;
        -*)
            echo "Unknown option: $1"
            echo "Usage: $0 [options] <directory>"
            echo ""
            echo "Options:"
            echo "  -t, --threshold N       Threshold for highlighting large differences (default: 100)"
            echo "  -s, --skip-threshold N  Skip if max absolute difference is less than this value (default: 10)"
            echo "  -p, --partition NAME    SLURM partition to use (default: main)"
            echo "  -j, --max-jobs N        Maximum number of concurrent SLURM jobs (default: 100)"
            echo "  -w, --wait-interval N   Seconds to wait between checking job status (default: 5)"
            echo "  -o, --output-dir DIR    Directory for temporary output files (default: ~/masked_pixels_tmp_$$)"
            echo "                          NOTE: Must be on a shared filesystem accessible from all nodes!"
            echo "  -k, --keep-temp         Keep temporary files after completion"
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

# Check arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 [options] <directory>"
    echo "  Submits SLURM jobs to process HDF5 master files in parallel across nodes."
    echo "  One job is submitted per subdirectory containing master files."
    echo ""
    echo "Options:"
    echo "  -t, --threshold N       Threshold for highlighting large differences (default: 100)"
    echo "  -s, --skip-threshold N  Skip if max absolute difference is less than this value (default: 10)"
    echo "  -p, --partition NAME    SLURM partition to use (default: main)"
    echo "  -j, --max-jobs N        Maximum number of concurrent SLURM jobs (default: 100)"
    echo "  -w, --wait-interval N   Seconds to wait between checking job status (default: 5)"
    echo "  -o, --output-dir DIR    Directory for temporary output files (default: ~/masked_pixels_tmp_$$)"
    echo "                          NOTE: Must be on a shared filesystem accessible from all nodes!"
    echo "  -k, --keep-temp         Keep temporary files after completion"
    exit 1
fi

INPUT="$1"

# Check if input is a directory
if [ ! -d "$INPUT" ]; then
    echo "Error: '$INPUT' is not a valid directory."
    exit 1
fi

# Set default output directory if not specified
# Use home directory (typically shared via NFS) instead of /tmp (local to each node)
if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="$HOME/masked_pixels_tmp_$$"
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/logs"
mkdir -p "$OUTPUT_DIR/results"
mkdir -p "$OUTPUT_DIR/errors"

echo "=============================================="
echo "SLURM Batch Masked Pixel Analysis"
echo "=============================================="
echo "Input: $INPUT"
echo "Threshold: $THRESHOLD"
echo "Skip Threshold: $SKIP_THRESHOLD"
echo "Partition: $PARTITION"
echo "Max Concurrent Jobs: $MAX_JOBS"
echo "Output Directory: $OUTPUT_DIR"
echo ""
echo "NOTE: Output directory must be on a shared filesystem!"
echo "=============================================="

# Find all immediate subdirectories that contain master files
echo "Searching for subdirectories with master files in: $INPUT"
SUBDIRS=()
while IFS= read -r -d '' subdir; do
    # Check if this subdirectory (or its children) contains any master files
    if find "$subdir" -type f \( -name "*_master.h5" -o -name "*_master.hdf5" \) 2>/dev/null | grep -q .; then
        SUBDIRS+=("$subdir")
    fi
done < <(find "$INPUT" -mindepth 1 -maxdepth 1 -type d -print0 2>/dev/null | sort -z)

# Also check if the input directory itself has master files (not in subdirs)
if find "$INPUT" -maxdepth 1 -type f \( -name "*_master.h5" -o -name "*_master.hdf5" \) 2>/dev/null | grep -q .; then
    # Add the input directory itself as a "subdirectory" to process
    SUBDIRS=("$INPUT" "${SUBDIRS[@]}")
fi

if [ ${#SUBDIRS[@]} -eq 0 ]; then
    echo "No subdirectories with master files found in $INPUT"
    exit 0
fi

SUBDIR_COUNT=${#SUBDIRS[@]}
echo "Found $SUBDIR_COUNT subdirectory(ies) with master files"
echo ""

# Save the list of subdirectories
printf '%s\n' "${SUBDIRS[@]}" > "$OUTPUT_DIR/subdirs.txt"

# Count total master files for reference
TOTAL_MASTER_FILES=$(find "$INPUT" -type f \( -name "*_master.h5" -o -name "*_master.hdf5" \) 2>/dev/null | wc -l)
echo "Total master files to process: $TOTAL_MASTER_FILES"
echo ""

# Track submitted jobs
JOB_IDS=()
SUBMITTED=0
COMPLETED=0

# Function to count running jobs for this batch
count_running_jobs() {
    local count=0
    for job_id in "${JOB_IDS[@]}"; do
        if squeue -j "$job_id" 2>/dev/null | grep -q "$job_id"; then
            count=$((count + 1))
        fi
    done
    echo $count
}

# Function to wait for job slots
wait_for_slot() {
    while true; do
        local running=$(count_running_jobs)
        if [ "$running" -lt "$MAX_JOBS" ]; then
            break
        fi
        echo -ne "\rWaiting for job slots... ($running/$MAX_JOBS running, $COMPLETED/$SUBDIR_COUNT completed)"
        sleep "$WAIT_INTERVAL"
    done
}

# Get the absolute path to the batch script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BATCH_SCRIPT="$SCRIPT_DIR/batch_plot_masked_pixels.sh"

if [ ! -f "$BATCH_SCRIPT" ]; then
    echo "Error: batch_plot_masked_pixels.sh not found at $BATCH_SCRIPT"
    exit 1
fi

# Submit jobs
echo "Submitting SLURM jobs..."
CURRENT=0

for subdir in "${SUBDIRS[@]}"; do
    CURRENT=$((CURRENT + 1))
    
    # Wait if we've hit the max concurrent jobs
    wait_for_slot
    
    # Create a unique job name based on the subdirectory
    JOB_NAME="mp_$(basename "$subdir" | head -c 20)"
    RESULT_FILE="$OUTPUT_DIR/results/result_${CURRENT}.txt"
    ERROR_FILE="$OUTPUT_DIR/errors/error_${CURRENT}.txt"
    LOG_FILE="$OUTPUT_DIR/logs/job_${CURRENT}.log"
    
    # Submit the job
    # Run the batch script on the subdirectory
    JOB_ID=$(sbatch --parsable \
        --partition="$PARTITION" \
        --job-name="$JOB_NAME" \
        --output="$LOG_FILE" \
        --error="$LOG_FILE" \
        --time=02:00:00 \
        --ntasks=1 \
        --cpus-per-task=16 \
        --mem=32G \
        --wrap="/bin/bash -l -c '$BATCH_SCRIPT -e -n -t $THRESHOLD -s $SKIP_THRESHOLD \"$subdir\" > \"$RESULT_FILE\" 2>\"$ERROR_FILE\"'")
    
    JOB_IDS+=("$JOB_ID")
    SUBMITTED=$((SUBMITTED + 1))
    
    echo -ne "\rSubmitted: $SUBMITTED/$SUBDIR_COUNT (Job ID: $JOB_ID) - $(basename "$subdir")    "
    
done

echo ""
echo "All $SUBMITTED jobs submitted."
echo ""

# Wait for all jobs to complete
echo "Waiting for jobs to complete..."
while true; do
    RUNNING=$(count_running_jobs)
    COMPLETED=$((SUBMITTED - RUNNING))
    
    echo -ne "\rProgress: $COMPLETED/$SUBMITTED completed, $RUNNING running    "
    
    if [ "$RUNNING" -eq 0 ]; then
        break
    fi
    
    sleep "$WAIT_INTERVAL"
done

echo ""
echo ""
echo "=============================================="
echo "All jobs completed. Aggregating results..."
echo "=============================================="
echo ""

# Aggregate results
RESULT_FILE="$OUTPUT_DIR/aggregated_results.txt"
> "$RESULT_FILE"

ISSUES_FOUND=0
for result in "$OUTPUT_DIR/results"/result_*.txt; do
    if [ -f "$result" ] && [ -s "$result" ]; then
        # Count lines that start with / (file paths with issues)
        path_count=$(grep -c "^/" "$result" 2>/dev/null || echo 0)
        if [ "$path_count" -gt 0 ]; then
            cat "$result" >> "$RESULT_FILE"
            echo "" >> "$RESULT_FILE"
            ISSUES_FOUND=$((ISSUES_FOUND + path_count))
        fi
    fi
done

# Display results
if [ -s "$RESULT_FILE" ]; then
    echo "Files with potential issues ($ISSUES_FOUND found):"
    echo "----------------------------------------------"
    cat "$RESULT_FILE"
else
    echo "No issues found in any of the $TOTAL_MASTER_FILES master files."
fi

echo ""
echo "=============================================="
echo "Summary"
echo "=============================================="
echo "Total subdirectories processed: $SUBDIR_COUNT"
echo "Total master files: $TOTAL_MASTER_FILES"
echo "Files with issues: $ISSUES_FOUND"
echo "Aggregated results: $RESULT_FILE"

# Check for any real errors in error files (not just warnings)
ERROR_COUNT=0
for err_file in "$OUTPUT_DIR/errors"/error_*.txt; do
    if [ -f "$err_file" ] && [ -s "$err_file" ]; then
        # Check for actual Python errors/exceptions, not just MySQL warnings
        if grep -q "Traceback\|Error:\|Exception:" "$err_file" 2>/dev/null; then
            # Ignore MySQL connection errors as they are expected on compute nodes
            if ! grep -q "mysql_native_password\|MySQLInterfaceError\|DatabaseError" "$err_file" 2>/dev/null; then
                ERROR_COUNT=$((ERROR_COUNT + 1))
            fi
        fi
    fi
done

if [ "$ERROR_COUNT" -gt 0 ]; then
    echo ""
    echo "WARNING: $ERROR_COUNT job(s) had unexpected errors. Check errors in: $OUTPUT_DIR/errors/"
fi

# Cleanup if not keeping temp files
if [ -z "$KEEP_TEMP" ]; then
    echo ""
    echo "Cleaning up temporary files..."
    rm -rf "$OUTPUT_DIR"
    echo "Done."
else
    echo ""
    echo "Temporary files kept in: $OUTPUT_DIR"
    echo "  - Subdirectory list: $OUTPUT_DIR/subdirs.txt"
    echo "  - Individual results: $OUTPUT_DIR/results/"
    echo "  - Error output: $OUTPUT_DIR/errors/"
    echo "  - Job logs: $OUTPUT_DIR/logs/"
fi
