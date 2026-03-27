# Autocenter and Raster Grid Setup

## Overview

During automated screening, the raster grid bounding box is determined automatically by image processing — no user clicks required. The autocenter script analyzes camera images to find the crystal/loop boundary, centers the sample, and outputs the bounding box dimensions. Pybluice then uses these to programmatically define two orthogonal raster grids.

## Autocenter Script

**Script:** `/mnt/software/gmca_epics/pythonScripts/autocenter_loop.py`

### Centering Stages

The autocenter runs four progressive stages:

| Stage | Camera | What | Output |
|-------|--------|------|--------|
| 1. Pin centering | Low-res | Difference image (0° vs 90°), SG filter + peak finding | Move XYZ to rough pin center |
| 2. Loop centering | Low-res | 4 images (0°, 90°, 180°, 270°), paired to compensate depth-of-field | Move XYZ to loop center |
| 3. Flat orientation | High-res | 10 images at 36° steps, fit `\|a·sin(ω-ω₀)\|+b` to binary areas | Rotate to face-on omega |
| 4. Bounding box | High-res | Threshold + fill → binary mask, SG derivative for edges | Print `"width_z height_x height_y"` |

### Bounding Box Detection (Stage 4)

At the face-on orientation:
1. Capture high-res image, subtract background reference
2. Gaussian blur + threshold → binary loop mask, fill holes
3. **Z extent** (horizontal): Savitzky-Golay 1st derivative of column sums → find rising/falling edges
4. **Y extent** (vertical): SG 2nd derivative of row sums near Z center → find top/bottom edges
5. If loop is wider than tall, make the box square

Rotate 90° to edge-on:
6. **Y extent at edge-on** → crystal thickness (X dimension)

### Output Format

The script prints three space-separated numbers to stdout:

```
width_z  height_x  height_y
```

All values in **microns**:
- `width_z` — horizontal extent on camera (Z motor direction) at face-on
- `height_x` — vertical extent on camera at edge-on (X motor, along rotation axis)
- `height_y` — vertical extent on camera at face-on (Y motor, perpendicular to rotation)

Example: `"75.0 85.0 60.0"` means a crystal region 75 × 85 × 60 microns.

## Autocenter Output → Raster Grid

**File:** `/mnt/software/pybluice/src/pbs/scripts/autocenter.py`

### Step 1: Parse stdout and create volume dict

```python
# Line 60: Parse the three dimensions
volume = [float(i) for i in autocenter_output.split(' ')]

# Lines 62-72: Create centered volume (symmetric around 0)
v = {
    "x1": -0.5 * volume[0],  "x2": 0.5 * volume[0],   # ±width_z/2
    "y1": -0.5 * volume[1],  "y2": 0.5 * volume[1],   # ±height_x/2
    "z1": -0.5 * volume[2],  "z2": 0.5 * volume[2],   # ±height_y/2
    "ref_x": sample_x,       # current motor positions
    "ref_y": sample_y,       #   (sample is already centered
    "ref_z": sample_z,       #    by stage 4)
    "ref_o": gonio_omega,    # face-on omega angle
}
```

Saved to Redis as `autocenter_volume` (JSON).

### Step 2: Create two orthogonal raster grids

```python
# Grid 1 (XY plane, face-on omega):
pt1 = [v['x1'], v['y1']]     # e.g., [-37.5, -42.5]
pt2 = [v['x2'], v['y2']]     # e.g., [37.5, 42.5]
grid_ref = [ref_x, ref_y, ref_z, ref_o]

# Grid 2 (XZ plane, omega - 90°):
pt1 = [v['x1'], v['z1']]     # e.g., [-37.5, -30.0]
pt2 = [v['x2'], v['z2']]     # e.g., [37.5, 30.0]
grid_ref = [ref_x, ref_y, ref_z, ref_o - 90]
```

Both grids share the same X extent (`width_z`) — this is the frame axis (horizontal on camera).

### Step 3: pt1/pt2 → act_bounds

Setting pt1/pt2 in Redis triggers `raster_fields.validate()` which calls:

```python
# shared_fields.py: pts_to_bounds_rows_cols()
grid_center = [(pt1_x + pt2_x) / 2, (pt1_y + pt2_y) / 2]  # = [0, 0]
rows = ceil((pt2_y - pt1_y) / cell_h_um)
cols = ceil((pt2_x - pt1_x) / cell_w_um)
act_bounds = [
    grid_center_x - (cols/2 * cell_w_um),   # x1
    grid_center_y - (rows/2 * cell_h_um),   # y1
    grid_center_x + (cols/2 * cell_w_um),   # x2
    grid_center_y + (rows/2 * cell_h_um),   # y2
]
```

Since the volume is centered at [0, 0], the `act_bounds` are also centered at [0, 0] relative to `grid_ref`. The bounds may be slightly larger than the autocenter output because they snap to integer cell counts.

## Complete Data Flow

```
autocenter_loop.py
  │  Analyzes camera images at multiple omega angles
  │  Detects loop/crystal boundary via thresholding + edge detection
  │  Centers sample motors on crystal
  │
  ▼  stdout: "75.0 85.0 60.0"  (width_z, height_x, height_y in um)

autocenter.py (pbs/scripts/)
  │  Parses stdout → volume dict {x1,x2,y1,y2,z1,z2,ref_x/y/z/o}
  │  All coordinates centered at 0 (sample already centered)
  │  Saves to Redis: autocenter_volume
  │
  ▼  Sets pt1/pt2/grid_ref for two grids

raster_fields.validate()
  │  Normalizes pt1/pt2
  │  Calculates rows, cols from cell size
  │
  ▼  Sets act_bounds, rows, cols in Redis

raster_calc.calc_partitions()
  │  Reads act_bounds + cell sizes
  │  Generates motor path for each scan line (partition)
  │
  ▼  Stores partition data in Redis (part_xyzo_i/f)

Eiger collects raster frames
  │
  ▼  Grid 1 at omega, Grid 2 at omega-90°

Raster3D pipeline
  │  Reads dozor scores from analysis Redis
  │  Builds 2D matrices from scan results
  │  Reconstructs 3D volume
  │
  ▼  Crystal positions, strategy, dose recommendations
```

## Coordinate Systems

| Quantity | Units | Reference | Meaning |
|----------|-------|-----------|---------|
| Autocenter output | microns | Crystal center | Physical extent of loop/crystal |
| Volume dict | microns | Crystal center (0,0) | Symmetric bounds ±dim/2 |
| pt1, pt2 | microns | grid_ref motor position | Camera screen coordinates |
| grid_ref | mm/deg | Absolute motor space | Motor position when grid was defined |
| act_bounds | microns | grid_ref | Camera screen coordinates, snapped to cell grid |

## Mapping: Autocenter Dimensions → Grid Axes

| Autocenter dimension | Direction | Grid 1 (XY) | Grid 2 (XZ) |
|---------------------|-----------|-------------|-------------|
| `width_z` (horizontal on camera) | Z motor (sample_z) | X axis (frames) | X axis (frames) |
| `height_x` (vertical at edge-on) | X motor (rotation axis) | — | Y axis (scan lines) |
| `height_y` (vertical at face-on) | Y motor (perpendicular) | Y axis (scan lines) | — |

## Screening Tab Flow

In the Screening tab, the sequence is:

```
Mount → Center (autocenter) → Pause → Sample JPEG → Strategy → Collect
```

When "Center" mode is "Raster":
1. Autocenter runs → determines crystal bounding box
2. Two raster grids are set up automatically (XY and XZ)
3. Raster data collected at face-on and edge-on omega
4. Dozor analyzes each frame in real-time
5. Raster3D pipeline detects the consecutive pair and runs automatically (if enabled)

## Key Files

| File | Purpose |
|------|---------|
| `/mnt/software/gmca_epics/pythonScripts/autocenter_loop.py` | Image processing: pin/loop centering + bounding box detection |
| `/mnt/software/pybluice/src/pbs/scripts/autocenter.py` | Parses autocenter output, creates volume, sets raster grids |
| `/mnt/software/pybluice/src/plugins/collect/raster/raster_fields.py` | pt1/pt2 → act_bounds/rows/cols conversion |
| `/mnt/software/pybluice/src/plugins/collect/raster/shared/shared_fields.py` | Core `pts_to_bounds_rows_cols()` math |
| `/mnt/software/pybluice/src/plugins/collect/raster/raster_calc.py` | act_bounds → motor partition paths |
| `/mnt/software/pybluice/src/plugins/collect/autoraster/autoraster_lib.py` | 3D autoraster cell centering |
