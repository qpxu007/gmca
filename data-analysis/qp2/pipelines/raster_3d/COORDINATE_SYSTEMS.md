# Coordinate System Conventions

## Physical Setup

High-res camera view at omega=0°:

```
                    beam direction (X-ray source → sample)
                    ←←←←←←←←←←←←←←←←

        ↑ sample_y (vertical, positive = down on camera)
        |
        |     sample_z (horizontal, positive = right on camera)
        |    ──────→     *** ROTATION AXIS ***
        |
        ⊙ sample_x (into page, away from camera = along beam)

        omega: positive rotation moves top of sample toward camera
```

The high-res camera looks along the beam direction. The rotation axis
(goniometer omega) is horizontal (sample_z). When omega rotates,
sample_x and sample_y mix, but sample_z stays fixed.

## Three Coordinate Systems

### 1. Bluice (motor coordinates)

Defined in `/mnt/software/pybluice/src/gui/common/camera_calc.py`:

| Motor | Direction at omega=0° | Camera (HR) | Rotates with omega? |
|-------|----------------------|-------------|-------------------|
| `sample_x` | Away from camera (along beam) | Into page | Yes |
| `sample_y` | Down on camera (vertical) | Vertical ↓ | Yes |
| `sample_z` | Right on camera (horizontal) | Horizontal → | **No** (rotation axis) |
| `gonio_omega` | Crystal rotation angle | — | — |

Motor-to-screen transform (`add_hr_xyz_to_motor_pos`):
```
motor_x = ref_x + sin(omega) * screen_y
motor_y = ref_y + cos(omega) * screen_y
motor_z = ref_z + screen_x
```

### 2. Raster3D (volume coordinates)

The 3D volume is reconstructed from two orthogonal 2D raster scans:

- **XY scan** at omega~0°: shape `(num_y, num_x)` — horizontal × vertical on camera
- **XZ scan** at omega~90°: shape `(num_z, num_x)` — horizontal × vertical on camera
- **Volume**: shape `(num_z, num_y, num_x)`

| Volume axis | Index | Physical direction | Bluice motor | From scan |
|-------------|-------|-------------------|-------------|-----------|
| **x** | axis 2 | Rotation axis (horizontal) | `sample_z` | Frame index (shared by XY and XZ) |
| **y** | axis 1 | Vertical (⊥ beam, ⊥ rotation) | `sample_y` | XY scan line index |
| **z** | axis 0 | Beam direction | `sample_x` | XZ scan line index |

The "X" in "XY" and "XZ" refers to the **shared frame axis** = rotation axis = sample_z.

Peak coordinates from `find_3d_hotspots`: `coords = (x, y, z)` — note the swap from
numpy's `(z, y, x)` argwhere order to `(x, y, z)` at line 210-212 of `volume_utils.py`.

### 3. RADDOSE-3D (crystal coordinates)

From the RADDOSE-3D Command Reference (July 2024), section 2.4:

> **DIMENSION X Y Z**: X defines the length of the crystal orthogonal to both
> the beam and the goniometer at L=P=0. Y defines the length along the
> goniometer axis at L=P=0. Z defines the length along the beam axis.

| RADDOSE axis | Direction | At AngleP=AngleL=0 |
|-------------|-----------|-------------------|
| **X** | Orthogonal to beam AND goniometer | Vertical |
| **Y** | Along goniometer (rotation) axis | Horizontal |
| **Z** | Along beam direction | Into detector |

Orientation angles:
- **AngleP** (plane angle): rotation about Z axis (beam axis) — tilts crystal in the loop plane
- **AngleL** (loop angle): rotation about X axis — tilts crystal out of the loop plane

Beam FWHM in the code is swapped: `beam_size.split()[::-1]` (line 188 of `raddose3d.py`).

## Axis Mapping Table

| Physical direction | Bluice | Raster3D volume | RADDOSE-3D | Camera (HR, ω=0°) |
|-------------------|--------|----------------|------------|-------------------|
| **Rotation axis** | `sample_z` | x (axis 2, frames) | **Y** | Horizontal → |
| **Vertical** (⊥ beam+rot) | `sample_y` | y (axis 1, XY lines) | **X** | Vertical ↓ |
| **Beam direction** | `sample_x` | z (axis 0, XZ lines) | **Z** | Into page ⊙ |

## Crystal Dimension Mapping

To pass crystal dimensions from raster3D to RADDOSE-3D:

| Crystal extent along... | Raster3D | RADDOSE Dimension position |
|------------------------|----------|--------------------------|
| Rotation axis (sample_z) | `extent_x` (frame axis) | **Y** (2nd value) |
| Vertical (sample_y) | `extent_y` (XY scan axis) | **X** (1st value) |
| Beam (sample_x) | `extent_z` (XZ scan axis) | **Z** (3rd value) |

```python
# Correct mapping:
# RADDOSE "Dimension X Y Z" = vertical, rotation_axis, beam
crystal_size_str = f"{extent_y} {extent_x} {extent_z}"

# Use swap_xy=False since we've already mapped correctly
run_raddose3d(sample, beam, wedges, swap_xy=False)
```

## Beam Size Mapping

Beam size in bluice is `(gs_x_um, gs_y_um)` = `(horizontal, vertical)`.

RADDOSE FWHM expects `(vertical, horizontal)` — the code swaps this at line 188:
```python
beam_size = beam.beam_size.split()[::-1]  # swap
```

So the beam input `beam_size = "gs_x gs_y"` (horizontal, vertical) becomes
`"gs_y gs_x"` (vertical, horizontal) = RADDOSE `(FWHM_X, FWHM_Y)` which is correct.

## PCA Dimensions vs Axis-Aligned Extents

The pipeline's `find_3d_hotspots` returns PCA eigenvalue-based dimensions sorted
**largest to smallest** (L ≥ W ≥ H). These are principal axis dimensions, **not**
aligned with the volume x/y/z axes.

For RADDOSE-3D, the dimensions should be the crystal extent along each physical
axis. Two approaches:

1. **Bounding box** (simple): measure the extent of the labeled region along each
   volume axis → directly maps to RADDOSE X/Y/Z.

2. **PCA with orientation** (current): pass PCA dimensions + set AngleP/AngleL from
   the eigenvectors to tell RADDOSE the crystal orientation.

For typical use (dose estimation), the bounding box approach is sufficient and
avoids the complexity of computing AngleP/AngleL from eigenvectors.

## Source References

- Bluice camera_calc: `/mnt/software/pybluice/src/gui/common/camera_calc.py` lines 31-52
- Raster3D volume: `qp2/image_viewer/volume_map/volume_utils.py` lines 91-113
- Raster3D peaks: `qp2/image_viewer/volume_map/volume_utils.py` lines 206-224
- RADDOSE-3D input: `qp2/radiation_decay/raddose3d.py` lines 126-206
- RADDOSE-3D manual: Section 2.4 (DIMENSION), 2.6 (ANGLEP), 2.7 (ANGLEL)
