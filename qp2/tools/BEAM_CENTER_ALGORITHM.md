# Beam Center Finding Algorithm

This document describes the algorithms implemented in `qp2/image_viewer/beamcenter/auto_center.py` for finding the beam center of diffraction images. These algorithms are available both as a command-line tool (`qp2.tools.guess_beam_center`) and an interactive dialog in the Image Viewer.

## Overview

The goal is to find the center $(c_x, c_y)$ of the diffraction pattern such that the radial profile of the background (diffuse scattering, solvent ring, air scatter) is as sharp as possible. The algorithm relies on the assumption that the background signal is radially symmetric around the true beam center.

## Core Concepts

### 1. Radial Binning
The image is divided into radial bins centered at a candidate beam center $(c_x, c_y)$. For each pixel $(x, y)$, the radius is $r = \sqrt{(x-c_x)^2 + (y-c_y)^2}$. Pixels are grouped by integer values of $r$.

### 2. Scoring Functions
We define an objective function that measures the "spread" of pixel intensities within each radial bin. A correct beam center minimizes this spread.

#### Method A: Robust Radial Score (Default)
This method is designed to be robust against outliers, such as bright diffraction spots (Bragg peaks), which break radial symmetry.
- **Metric**: For each radial bin, calculate the **Interquartile Range (IQR)** or a similar robust spread metric (75th percentile - 25th percentile).
- **Objective**: The average of these spreads across all valid bins.
- **Advantage**: Highly resistant to bright spots; effectively "ignores" them to focus on the underlying symmetric background.

#### Method B: Variance Score
- **Metric**: Standard deviation of pixel intensities within each radial bin.
- **Objective**: The mean of standard deviations across all bins.
- **Advantage**: Faster calculation.
- **Disadvantage**: Very sensitive to outliers (spots). Requires rigorous masking of spots beforehand.

### 3. Spot Removal (Masking)
To improve the performance of both methods (especially the Variance method), we heuristically identify and mask diffraction spots.
- **Algorithm**:
  1. Compute the 99.99th percentile of valid pixel intensities.
  2. Mask all pixels above this threshold.
  3. This effectively removes the brightest Bragg peaks while preserving the diffuse background.

## Optimization

The beam center is found by minimizing the chosen scoring function using the **Nelder-Mead** simplex algorithm (`scipy.optimize.minimize`).

- **Downsampling**: The image is downsampled (e.g., by a factor of 2) to speed up the optimization.
- **Search Limit**: A `limit` parameter can be specified to constrain the search within a certain radius (in pixels) from the initial guess. If the optimizer drifts beyond this limit, a large penalty is added to the objective function to steer it back.

## UI Integration

The algorithm is integrated into the Image Viewer via the **Beam Center Correction** dialog (`Tools` -> `Beam Center Correction (from background)`).
- **Interactive**: Users can adjust search parameters (Method, Search Limit, Min Radius) and run the calculation on the currently loaded image.
- **Visual Feedback**: The proposed beam center is displayed as a magenta marker on the image.
- **Update**: Users can apply the calculated center directly to the master HDF5 file. The system handles file locking and backup automatically.

## Command-Line Usage

The tool can be run directly from the command line to test the algorithm on datasets or process lists of files.

```bash
python -m qp2.tools.guess_beam_center [ARGUMENT]
```

### Arguments

1.  **Single File Mode** (`.h5` or `.cbf` file):
    *   Loads the file (frame 0).
    *   Runs the optimization starting from a synthetic offset (to test convergence).
    *   Compares both "Robust" and "Variance" methods against the metadata beam center.
    *   **Example**: `python -m qp2.tools.guess_beam_center /path/to/data_master.h5`

2.  **Batch Mode** (`.txt` file):
    *   Reads a list of file paths from the text file (one per line).
    *   Processes each file, calculating the beam center using both methods.
    *   Writes results to a new file: `<input_list>.optimized_centers.txt`.
    *   **Example**: `python -m qp2.tools.guess_beam_center datasets.txt`

3.  **Default Mode** (No argument):
    *   Runs on a hardcoded test file path (mainly for internal development/testing).