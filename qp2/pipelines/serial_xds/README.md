# Serial XDS Pipeline

This package implements a serial XDS processing pipeline for large datasets (e.g., serial crystallography). It processes datasets frame-by-frame after an initial global initialization step, then scales the results using `xia2.multiplex`.

## Workflow

1.  **Initialization**:
    *   Iterates over input HDF5 datasets.
    *   Reads metadata (beam center, distance, wavelength, etc.) directly from the HDF5 master file using `qp2.xio.hdf5_manager.HDF5Reader`.
    *   Runs `XYCORR` and `INIT` steps for the entire dataset to generate correction tables (`X-CORRECTIONS.cbf`, `Y-CORRECTIONS.cbf`, `BKGINIT.cbf`, `GAIN.cbf`).

2.  **Frame Processing**:
    *   Iterates through each frame in the dataset.
    *   Creates a subdirectory for each frame (e.g., `dataset_name/00005`).
    *   Links the correction tables and `XDS.INP` from the parent directory.
    *   Updates `XDS.INP` to process only that specific frame (`DATA_RANGE`, `SPOT_RANGE`).
    *   Runs XDS steps: `COLSPOT`, `IDXREF`, `DEFPIX`, `INTEGRATE`.
    *   Verifies the success of the integration step.
    *   Converts the results using `dials.import_xds`.

3.  **Scaling**:
    *   Collects all successfully processed frames (directories containing `imported.expt`).
    *   Runs `xia2.multiplex` on these directories to scale and merge the data.

## Execution Modes

The pipeline automatically detects if the SLURM workload manager (`sbatch`) is available.

### Cluster Mode (SLURM available)
*   Submits a **SLURM Array Job** for the frame processing step. This efficiently handles thousands of frames as parallel tasks.
*   Submits a dependent scaling job that starts only after the array job completes.

### Workstation Mode (No SLURM)
*   Runs frame processing locally using Python's `ProcessPoolExecutor`.
*   The number of parallel workers is controlled by the `--jobs` argument.
*   Runs scaling locally after all frames are processed.

## Usage

Use the `qp2-serial-xds` launcher script (ensure `qp2/bin` is in your PATH).

```bash
qp2-serial-xds --inputs /path/to/data/*.h5 --output /path/to/results [OPTIONS]
```

### Arguments

*   `--inputs`: One or more input HDF5 master files (glob patterns allowed).
*   `--output`: Output directory for processing results.
*   `--spacegroup`: Space group symbol or number (e.g., `P212121` or `19`).
*   `--unitcell`: Unit cell constants (e.g., `"10 20 30 90 90 90"`).
*   `--reference`: Path to a reference dataset (HKL) for consistency.
*   `--highres`: High resolution cutoff (Angstroms).
*   `--dials_setup`: Command to setup the DIALS environment (default: `module load dials`).
*   `--jobs`: Number of parallel jobs for local execution (default: 8).

### Example

```bash
qp2-serial-xds \
    --inputs /data/experiment/run1_master.h5 \
    --output /data/processing/serial_run1 \
    --spacegroup 96 \
    --unitcell "78.1 78.1 36.9 90 90 90" \
    --highres 1.8
```

## Structure

*   `serial_xds.py`: The main driver script. Orchestrates the workflow and job submission.
*   `process_frame.py`: The worker script executed for each frame (either by SLURM task or local worker).
