# 3D Raster Analysis Pipeline

## Summary

The 3D raster pipeline automates crystal finding, characterization, and dose-aware collection planning from two orthogonal raster scans. When two consecutive RASTER runs are collected at ~90° omega difference (e.g., `sample_ras_run6` at 0° and `sample_ras_run7` at 90°), the pipeline:

1. **Detects the pair** — consecutive run numbers with the same base name
2. **Waits for analysis results** — polls Redis for dozor/nXDS frame scores, retries failed jobs
3. **Quality gate** — checks for minimum diffraction signal before proceeding
4. **Reconstructs a 3D volume** — multiplies two orthogonal 2D heatmaps into a 3D crystal map
5. **Finds crystal hotspots** — connected-component labeling with eigenvalue-based size/shape analysis
6. **Saves diagnostic images** — heatmaps with peak markers, diffraction frames with resolution rings
7. **Detects overlaps** — flags crystals that overlap along the rotation axis
8. **Runs strategy** — XDS + MOSFLM indexing on best peak frames (dual or single orientation)
9. **Recommends collection** — dose-aware parameter search validated with RADDOSE-3D, detector distance from dozor resolution

The output is a per-crystal collection recommendation including centering coordinates, crystal size, beam size, detector distance, exposure time, attenuation, start/end angles, and estimated dose.

## Architecture

```
qp2/pipelines/raster_3d/
├── config.py            # Analysis sources, search space defaults
├── tracker.py           # RasterRunTracker — consecutive pair detection
├── scan_mode.py         # Scan mode detection (metadata → bluice → filename)
├── matrix_builder.py    # Scan-mode-aware 2D matrix from Redis, gap interpolation
├── pipeline_worker.py   # Raster3DPipelineWorker (QRunnable) — full pipeline
└── test_pipeline.py     # Integration tests (fakeredis + real Redis)

qp2/xio/
├── bluice_params.py     # Bluice Redis query helpers (cell size, beam, attenuation, grid geometry)
└── redis_manager.py     # RedisManager high-level API (delegates to bluice_params)
```

### Integration with Processing Server

The pipeline integrates into the data processing server with minimal changes. The complete call chain:

```
RASTER run completed (Eiger stream → ProcessingServer)
  │
  ├── ProcessingServer._build_options()
  │     ├── Read bluice Redis: beamline name, user, robot mounted
  │     ├── Read bluice raster params: scan_mode, cell_size, beam_size, attenuation
  │     ├── Save collection params → analysis Redis (collection_params:{group}:{prefix})
  │     └── Cache bluice params in opt["_bluice_collection_params"]
  │
  ├── AnalysisManager.handle_run_completion_logic(collect_mode="RASTER")
  │     ├── Submit dozor/nXDS analysis jobs (per-frame spot finding)
  │     ├── Submit xia2_ssx / crystfel jobs (if enabled)
  │     │
  │     └── 3D Raster pair detection (if raster_3d.enabled):
  │           ├── detect_raster_scan_mode() via RedisManager
  │           ├── raster_tracker.register_completed_raster()
  │           │     └── Check for consecutive partner (run_num ± 1)
  │           │
  │           └── If pair found:
  │                 ├── Create Raster3DPipelineWorker(redis_manager=...)
  │                 └── Submit to worker_pool → runs stages 0-3
  │
  └── AnalysisManager._ensure_dataset_run_db_record()
        └── Merge cached bluice params into metadata headers → DB
```

**Modified files:**
- `qp2/data_proc/server/analysis_manager.py` — tracker init + pair detection + bluice params in DB headers
- `qp2/data_proc/server/data_processing_server.py` — save collection params to analysis Redis
- `qp2/config/redis_keys.py` — `RASTER_3D`, `COLLECTION_PARAMS`, `BluiceRedisKeys` extensions

## Pipeline Stages

### Stage 0: Wait for Analysis Results (~2s with cached data)
Polls Redis until dozor/nXDS results are available for all master files in both runs. Retries failed analysis jobs. Proceeds with partial data if coverage >= threshold.

### Quality Gate (pre-reconstruction) (<1s)
Scans all frame scores and rejects samples with no diffraction. Configurable thresholds for minimum score, resolution, and number of strong frames.

### Stage 1: Peak Finding (~14s)
Builds scan-mode-aware 2D matrices from Redis (3s), validates orthogonality (omega ~90°), reconstructs 3D volume, finds hotspots with PCA-based size/shape analysis, saves diagnostic heatmap images with diffraction frames (11s).

### Overlap Detection (<1s)
Checks if crystals overlap along the rotation axis (X). Overlapping crystals produce multi-crystal diffraction patterns. Configurable policy: keep best (default), keep all, or skip overlapping.

### Stage 2: Strategy (~43s)
Runs XDS + MOSFLM in parallel on the best peak frame. Two modes:

- **Default (single-orientation):** Tries XY orientation first (omega~0°), falls back to XZ (omega~90°)
- **Dual-orientation (experimental):** Merges both XY and XZ frames into one mapping so the indexing program sees two orthogonal images — better reciprocal space coverage

Extracts space group, unit cell, mosaicity, oscillation, detector distance.

### Stage 3: Dose Recommendation (~59s)
Uses `find_experimental_recommendations()` to search a discrete grid of (beam size, attenuation, exposure time, n_images). Target dose is based on resolution:

**Resolution priority chain:**
1. **Dozor `Resol Visible`** at the peak frame — best per-frame estimate
2. **Strategy `resolution_from_spots`** — fallback from indexing
3. **Default 30 MGy** — when no resolution available

Target dose = resolution × 10 MGy (Howells criterion).

**Detector distance recalculation:** If the dozor resolution is near the detector edge (within 10%), the pipeline recalculates the detector distance to put `resolution × 0.9` at the edge, overriding the strategy distance if it would lose high-resolution data.

Validates the best candidate with RADDOSE-3D.

## Timing

Measured on real test data (22 + 25 scan lines, 29 frames each, 8 um step):

| Stage | Duration | % of total | Notes |
|-------|----------|-----------|-------|
| Stage 0 (wait) | 2s | 2% | With cached dozor data |
| Quality gate | <1s | <1% | |
| Stage 1 (peaks) | 14s | 12% | Includes heatmap + diffraction image saving |
| Stage 2 (strategy) | 43s | 36% | XDS + MOSFLM parallel, single orientation |
| Stage 3 (RADDOSE) | 14s | 12% | Initial dose calculation |
| Recommendation search | 45s | 38% | Discrete grid search + RADDOSE validation |
| **Total** | **~118s** | **100%** | |

In production, Stage 0 may take longer (waiting for dozor to finish processing). The pipeline itself takes ~2 minutes after results are available.

## Output

### File Output

```
{proc_dir}/raster_3d/
├── results.json        # Collection recommendations (compact by default)
├── pipeline.log        # Timestamped step-by-step log
├── heatmap_xy.png      # XY heatmap + peaks + diffraction frame
└── heatmap_xz.png      # XZ heatmap + peaks + diffraction frame
```

### Diagnostic Images

Each heatmap image contains up to 3 panels (side-by-side):

| Panel | Content | When present |
|-------|---------|-------------|
| Sample snapshot | Camera JPEG with raster grid bounding box | If `{data_dir}/screen/*__HighRes.jpg` found |
| Heatmap | Dozor score heatmap with peak markers (x) and bounding rectangles | Always |
| Diffraction | Frame at peak position with red resolution rings (4.5, 3, 2, 1.5 A) | If HDF5 data accessible |

The diffraction panel uses:
- `gist_yarg` colormap (white background, dark spots — crystallography convention)
- 7×7 maximum filter to dilate spots for visibility at reduced image size
- Eiger mask handling (`2^bit_depth - 1` pixels masked)
- Percentile-based contrast (0 to P99.9 of valid pixels)
- Master file name and frame number in the title
- Summary text at bottom (peak coords, crystal size, energy, wavelength, beam size)

### Pipeline Log

`pipeline.log` records every decision point with timestamps:

```
[2026-03-23 15:20:36] [init] Pipeline started: Q3_ras_run6 + Q3_ras_run7, source=Main Score
[2026-03-23 15:20:38] [stage0] Results available: 22 run1 files, 25 run2 files
[2026-03-23 15:20:38] [quality_gate] pass=True, max_score=None, resolution=NoneA
[2026-03-23 15:20:41] [stage1] XY matrix: (22, 29), XZ matrix: (25, 29), volume: (25, 22, 29), peaks: 1
[2026-03-23 15:20:52] [stage1]   Peak 1: coords=[14.0, 4.0, 4.0], size=[91, 90, 77]um, intensity=149.0
[2026-03-23 15:20:52] [overlap] groups=0, policy=best, selected=1/1 peaks
[2026-03-23 15:21:35] [stage2] Strategy OK: SG=196, cell=[234.1, 234.1, 234.1, 90, 90, 90], resolution=3.85A
[2026-03-23 15:21:49] [stage3] RADDOSE peak 1: crystal=91 90 77um, avg_dwd=91.8MGy, lifetime=25.0s
[2026-03-23 15:21:49] [resolution] WARNING: resolution 2.33A near detector edge 2.55A
[2026-03-23 15:21:49] [detector] Recommended distance: 277.8 mm (target 2.10A at edge, strategy: 513.0 mm)
[2026-03-23 15:22:34] [recommendation] Peak 1: beam=[50,50]um, atten=1x, exposure=0.05s, n_images=3600, dose=23.3MGy
[2026-03-23 15:22:34] [final] COMPLETED: 1 recommendation(s) in 118.1s
```

### Results JSON

Written to `{proc_dir}/raster_3d/results.json`. By default, `compact_results: true` produces a clean output with only the information needed for collection:

```json
{
  "run1_prefix": "Q3_ras_run6",
  "run2_prefix": "Q3_ras_run7",
  "timestamp": 1774400026.0,
  "status": "completed",
  "n_peaks": 1,
  "recommendations": [
    {
      "crystal_position": {
        "peak_voxel": [14.0, 4.0, 4.0],
        "dimensions_um": [91, 90, 77],
        "motor_position": {"sample_x": 0.203, "sample_y": 0.379, "sample_z": -1.04},
        "collection_mode": "standard"
      },
      "collection": {
        "start_angle": 160.0,
        "end_angle": 1420.0,
        "total_rotation": 1260.0,
        "osc_width": 0.35,
        "n_images": 3600,
        "exposure_time_s": 0.05,
        "attenuation": 1,
        "detector_distance_mm": 277.8,
        "energy_kev": 12.0,
        "wavelength_A": 1.0332,
        "beam_size_um": [50, 50],
        "flux": 1e12,
        "target_dose_mgy": 23.3,
        "estimated_dose_mgy": 23.08,
        "dose_source": "resolution(2.3A/dozor) * 10",
        "total_collection_time_s": 180.0,
        "translation_x_um": 0
      },
      "crystal": {
        "space_group": 196,
        "lattice": "cF",
        "unit_cell": ["234.1", "234.1", "234.1", "90", "90", "90"],
        "mosaicity": 0.2,
        "resolution_A": 2.33,
        "resolution_source": "dozor",
        "resolution_strategy_A": 3.85,
        "resolution_dozor_A": 2.33,
        "resolution_at_edge": true,
        "edge_resolution_A": 2.55,
        "completeness": 95.51,
        "screen_score": 0.595,
        "strategy_source": "xds"
      },
      "dose": {
        "raddose3d_avg_dwd_mgy": 7.68,
        "raddose3d_max_dose_mgy": 31.23
      },
      "alternatives": [...]
    }
  ]
}
```

Set `compact_results: false` for full diagnostic output including raw strategy data, RADDOSE-3D summaries, peak eigenvalues, and all intermediate stage results.

**Compact vs full output:**

| Field | Compact | Full |
|-------|---------|------|
| `crystal_position` (voxel, dimensions, motor, mode) | yes | yes |
| `collection` (angles, exposure, beam, dose, distance) | yes | yes |
| `crystal` (SG, cell, resolution, completeness) | yes | yes |
| `dose` (RADDOSE-3D avg DWD, max dose) | yes | yes |
| `alternatives` (top N-1 candidates when `n_recommendations > 1`) | yes | yes |
| Raw XDS/MOSFLM indexing tables | no | yes |
| RADDOSE-3D summary text, crystal coefficients | no | yes |
| Peak PCA eigenvalues, orientation matrices | no | yes |
| Quality gate statistics | no | yes |
| Overlap analysis details | no | yes |

**Note:** `motor_position` is populated only when bluice grid geometry is available (live beamline). In offline/CLI mode it is `null`. Set `compute_motor_positions: true` (default) to enable.

### Accessing Results from Downstream Code

#### From the JSON file

```python
import json

with open(f"{proc_dir}/raster_3d/results.json") as f:
    results = json.load(f)

for rec in results["recommendations"]:
    pos = rec["crystal_position"]
    coll = rec["collection"]
    crys = rec["crystal"]
    dose = rec["dose"]

    print(f"Peak: {pos['peak_voxel']}, size: {pos['dimensions_um']} um")
    print(f"Motor: {pos['motor_position']}")
    print(f"SG {crys['space_group']}, resolution {crys['resolution_A']}A")
    print(f"Beam {coll['beam_size_um']}um, exposure {coll['exposure_time_s']}s")
    print(f"Detector {coll['detector_distance_mm']}mm, dose {coll['target_dose_mgy']}MGy")
    print(f"RADDOSE avg DWD: {dose['raddose3d_avg_dwd_mgy']}MGy")

    for i, alt in enumerate(rec.get("alternatives", [])):
        print(f"  Alt {i+1}: beam={alt['beam_size_um']}, dose={alt['estimated_dose_mgy']}MGy")
```

#### From Redis (quick-access fields)

The pipeline stores results in a Redis hash with individual fields for fast access without parsing JSON. TTL: 7 days.

**Key:** `analysis:out:raster_3d:{run1_prefix}`

```python
import redis, json

r = redis.Redis(host="10.20.103.67", port=6379, decode_responses=True)
key = "analysis:out:raster_3d:Q3_ras_run6"

# Individual fields (no JSON parsing needed)
n_peaks       = r.hget(key, "n_peaks")
resolution    = r.hget(key, "resolution_A")
space_group   = r.hget(key, "space_group")
exposure      = r.hget(key, "exposure_time_s")
beam_size     = r.hget(key, "beam_size_um")       # JSON list: [50, 50]
crystal_size  = r.hget(key, "crystal_size_um")     # JSON list: [91, 90, 77]
crystal_coords = r.hget(key, "crystal_coords")     # JSON list: [14.0, 4.0, 4.0]
det_distance  = r.hget(key, "detector_distance_mm")
dose          = r.hget(key, "target_dose_mgy")
attenuation   = r.hget(key, "attenuation")
n_images      = r.hget(key, "n_images")
start_angle   = r.hget(key, "start_angle")
end_angle     = r.hget(key, "end_angle")
osc_width     = r.hget(key, "osc_width")

# Full recommendation list (JSON)
recs = json.loads(r.hget(key, "recommendations"))

# Full results blob (compact or full, matches results.json)
data = json.loads(r.hget(key, "data"))
```

```bash
# Command-line quick access
redis-cli -h 10.20.103.67 HGET analysis:out:raster_3d:Q3_ras_run6 n_peaks
redis-cli -h 10.20.103.67 HGET analysis:out:raster_3d:Q3_ras_run6 resolution_A
redis-cli -h 10.20.103.67 HGET analysis:out:raster_3d:Q3_ras_run6 crystal_size_um
redis-cli -h 10.20.103.67 HGET analysis:out:raster_3d:Q3_ras_run6 detector_distance_mm
redis-cli -h 10.20.103.67 HGET analysis:out:raster_3d:Q3_ras_run6 beam_size_um
redis-cli -h 10.20.103.67 HGET analysis:out:raster_3d:Q3_ras_run6 recommendations
```

#### From the pipeline signal (live integration)

```python
worker.signals.pipeline_completed.connect(on_completed)

def on_completed(results: dict):
    # results dict contains the same data as results.json "stages" key
    for rec in results.get("recommendations", []):
        pos = rec.get("crystal_position", {})
        # ... use directly
```

### Redis Keys

**Status** (real-time progress, 7-day TTL): `analysis:out:raster_3d:{run1_prefix}:status`

```
HGET analysis:out:raster_3d:Q3_ras_run6:status
→ {"status": "COMPLETED", "stage": "final", "timestamp": ...}
```

Possible status values: `WAITING`, `RETRYING`, `CHECKING`, `RUNNING_PEAKS`, `PEAKS_COMPLETED`, `RUNNING_STRATEGY`, `STRATEGY_COMPLETED`, `STRATEGY_FAILED`, `RUNNING_RADDOSE`, `RADDOSE_COMPLETED`, `RADDOSE_FAILED`, `COMPLETED`, `ABORTED`, `FAILED`

**Results** (structured hash, 7-day TTL): `analysis:out:raster_3d:{run1_prefix}`

Available fields: `n_peaks`, `resolution_A`, `space_group`, `strategy_source`, `start_angle`, `end_angle`, `osc_width`, `n_images`, `exposure_time_s`, `attenuation`, `detector_distance_mm`, `target_dose_mgy`, `estimated_dose_mgy`, `energy_kev`, `total_collection_time_s`, `crystal_size_um`, `crystal_coords`, `beam_size_um`, `recommendations` (JSON), `data` (full JSON blob), plus pipeline_params fields (`username`, `beamline`, etc.)

**Collection parameters** (from bluice, captured at run completion, 30-day TTL):
`analysis:collection_params:{group}:{run_prefix}`

```bash
redis-cli HGET analysis:collection_params:esaf12345:Q3_ras_run6 scan_mode
redis-cli HGET analysis:collection_params:esaf12345:Q3_ras_run6 cell_w_um
```

## Configuration

All parameters are in `analysis_config.json` under the `raster_3d` key:

### Core Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enabled` | `false` | Enable/disable the pipeline |
| `analysis_source` | `"dozor"` | Analysis program: `"dozor"` or `"nxds"` |
| `metric` | `null` | Override metric field (null = use source default) |
| `step_size_um` | `10.0` | Raster step size in microns (fallback; prefers bluice Redis `cell_w_um/cell_h_um`) |

### Peak Finding

| Parameter | Default | Description |
|-----------|---------|-------------|
| `shift` | `0.0` | Shift of XZ scan relative to XY scan (frames) |
| `max_peaks` | `10` | Maximum hotspots to report |
| `min_size` | `3` | Minimum voxels for a valid hotspot |
| `percentile_threshold` | `95.0` | Percentile cutoff for hotspot detection |

### Waiting / Retry

| Parameter | Default | Description |
|-----------|---------|-------------|
| `wait_timeout_s` | `600` | Max wait for analysis results (seconds) |
| `retry_timeout_s` | `300` | Wait after resubmitting failed jobs |
| `poll_interval_s` | `15` | Polling interval |
| `max_retries` | `1` | Max resubmission attempts for failed analysis |
| `min_coverage_pct` | `80` | Minimum % of master files with results to proceed |
| `tracker_ttl_seconds` | `3600` | How long to remember a completed raster run |

### Quality Gate

Optional — omit the `quality_gate` section to skip all checks.

```json
"quality_gate": {
    "min_max_score": 10.0,
    "min_resolution_A": 10.0,
    "min_strong_frames": 3,
    "score_threshold": 5.0,
    "min_peaks": 1,
    "min_peak_intensity": 1.0
}
```

### Dose Recommendation Search Space

Override `search_space` to fix parameters. Setting a single-element list locks that parameter.

```json
"search_space": {
    "beam_sizes": [5, 10, 20, 50],
    "attenuations": [1, 2, 3, 5, 10, 50, 100, 500],
    "exposure_times": [0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5],
    "n_images": [900, 1800, 3600],
    "translations": [0]
}
```

### Experimental Features

| Parameter | Default | Description |
|-----------|---------|-------------|
| `compute_motor_positions` | `true` | Convert voxel coords to motor positions using bluice grid geometry |
| `dual_orientation_strategy` | `false` | Use both 0° and 90° frames for strategy indexing |
| `compact_results` | `true` | Save only collection/crystal/dose params to results.json (false = full diagnostic output) |
| `n_recommendations` | `1` | Number of top alternative solutions to include per peak |
| `collection_energies_kev` | `null` | List of energies for multi-energy recommendations |
| `target_dose_mgy` | `null` | Override target dose (null = dozor resolution × 10 or 30 MGy) |

### Overlap and Shape Detection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `overlap_policy` | `"best"` | `"best"` = keep strongest per overlap group, `"all"` = keep all, `"skip"` = remove all overlapping |
| `rod_aspect_ratio` | `2.0` | Aspect ratio threshold for rod-shaped crystal detection |
| `rod_angle_threshold_deg` | `45.0` | Max angle to rotation axis for vector/helical centering |

## Scan Modes

The pipeline handles four raster scan modes, detected from bluice Redis (`vertical`/`serpentine` flags):

| vertical | serpentine | Mode | File pattern |
|----------|-----------|------|-------------|
| 0 | 0 | `row_wise` | `_R` |
| 0 | 1 | `row_wise_serpentine` | `_RX` |
| 1 | 0 | `column_wise` | `_C` |
| 1 | 1 | `column_wise_serpentine` | `_CX` |

Detection priority: HDF5 metadata → bluice Redis (`r#{run_idx}$vertical/serpentine`) → filename pattern → default (`row_wise`).

The image viewer auto-detects scan mode from analysis Redis collection params (saved by the server at run completion). Users can override via the "Grid Scan Mode" setting (default: "Auto Detect").

## Bluice Redis Integration

All bluice queries are consolidated in `qp2/xio/bluice_params.py` with high-level methods on `RedisManager`:

| Method | Bluice key | What |
|--------|-----------|------|
| `get_raster_cell_size()` | `r#{idx}$cell_w_um/cell_h_um` | Step size |
| `get_raster_scan_mode()` | `r#{idx}$vertical/serpentine` | Scan mode |
| `get_beam_size()` | `b#{idx}$gs_x_um/gs_y_um` → `sampleenv$cur_act_beamsize_um` | Beam size |
| `get_attenuation()` | `b#{idx}$atten_factors` → `attenuation$actPos_factors` | Attenuation |
| `get_raster_grid_params()` | `r#{idx}$grid_ref/act_bounds/rows/cols` | Grid geometry (for motor positions) |
| `get_camera_calibration()` | `config$mm_per_px_hr_h/v` | Camera pixel calibration |

`run_idx` = trailing number from run prefix (e.g., `Q3_ras_run6` → 6).

## Resolution and Detector Distance

### Resolution Chain

The pipeline uses the best available resolution estimate for dose calculation:

1. **Dozor `Resol Visible`** at peak frame — reads from analysis Redis for the specific master file and frame at the peak position. Checked for both XY and XZ orientations; best (lowest) value used.
2. **Strategy `resolution_from_spots`** — from XDS/MOSFLM spot finding (typically poorer for raster data due to weak signal).
3. **Default** — 30 MGy target dose.

### Edge Resolution Warning

When the dozor resolution is within 10% of the detector edge resolution, the pipeline warns that the crystal likely diffracts better than what the detector captures at the current distance.

### Detector Distance Recalculation

The pipeline recalculates the optimal detector distance from the dozor resolution:

```
d_target = resolution × 0.9 (10% buffer)
det_dist = edge_mm / tan(2 × arcsin(λ / (2 × d_target)))
```

This is only applied when the recalculated distance is shorter than the strategy recommendation (i.e., the strategy distance would lose high-resolution data).

## Rod-Shaped Crystal Handling

When a crystal's longest dimension exceeds `rod_aspect_ratio` × shortest dimension:
- If the long axis is aligned with the rotation axis (angle < `rod_angle_threshold_deg`): **vector/helical** collection with two centering endpoints
- If perpendicular: **standard** single-center collection

## Rotation-Axis Overlap Detection

When multiple crystals overlap along the rotation axis (X), they produce overlapping diffraction patterns. The pipeline detects this by checking if peak X-ranges intersect.

Overlap policy (`overlap_policy`):
- `"best"` (default): keep the strongest crystal per overlap group
- `"all"`: keep all (multi-crystal data accepted)
- `"skip"`: remove all overlapping crystals, keep only isolated ones

## Running the Tests

```bash
source ./qp2_env/bin/activate
python -m qp2.pipelines.raster_3d.test_pipeline
```

Tests:
1. **tracker** — pair detection, prefix parsing
2. **scan_mode** — filename/metadata detection
3. **matrix_builder** — compact indexing, numeric sorting, gap interpolation
4. **orthogonality** — omega validation
5. **full_pipeline** — end-to-end with fakeredis (synthetic data)
6. **quality_gate_abort** — rejection of weak signal
7. **full_pipeline_real_redis** — end-to-end with real dozor data from `bl1ws1`

Test data location: `/mnt/beegfs/qxu/raster-spots-finding/raster3d/`
- `Q3_ras_run6_R{1..22}_master.h5` (omega=0°, 22 scan lines, 29 frames each)
- `Q3_ras_run7_R{1..25}_master.h5` (omega=90°, 25 scan lines, 29 frames each)
- Step size: 8 um

## Running Offline

The pipeline can be run standalone on an existing raster dataset without the data processing server. This requires:

1. Two consecutive raster runs as HDF5 master/data files in a directory
2. Dozor results in the analysis Redis server

### Prerequisites

```bash
source ./qp2_env/bin/activate
```

### Step 1: Ensure dozor results exist

The pipeline reads per-frame scores from Redis. If dozor hasn't been run on the data, submit it first:

```bash
# Check if dozor data exists
python3 -c "
import redis, glob
conn = redis.Redis(host='bl1ws1', port=6379, db=0, decode_responses=True)
DATA_DIR = '/path/to/your/raster/data'
for prefix in ['sample_ras_run1', 'sample_ras_run2']:
    files = sorted(glob.glob(f'{DATA_DIR}/{prefix}_R*_master.h5'))
    has = sum(1 for f in files if conn.hlen(f'analysis:out:spots:dozor2:{f}') > 0)
    print(f'{prefix}: {has}/{len(files)} files have dozor data')
"
```

### Step 2: Run the pipeline

```python
import redis
from qp2.pipelines.raster_3d.matrix_builder import find_master_files
from qp2.pipelines.raster_3d.pipeline_worker import Raster3DPipelineWorker

DATA_DIR = "/path/to/your/raster/data"
RUN1 = "sample_ras_run1"  # omega ~ 0°
RUN2 = "sample_ras_run2"  # omega ~ 90°
OUT_DIR = "/path/to/output"

worker = Raster3DPipelineWorker(
    run1_prefix=RUN1,
    run2_prefix=RUN2,
    run1_master_files=find_master_files(DATA_DIR, RUN1),
    run2_master_files=find_master_files(DATA_DIR, RUN2),
    run1_scan_mode="column_wise_serpentine",  # or "row_wise" etc.
    run2_scan_mode="column_wise_serpentine",
    data_dir=DATA_DIR,
    metadata={
        "collect_mode": "RASTER",
        "energy_ev": 12000,        # adjust to your energy
        "beam_size_x_um": 10,      # beam size used for raster
        "beam_size_y_um": 10,
        "flux": 1e12,
    },
    redis_conn=redis.Redis(host="bl1ws1", port=6379, db=0, decode_responses=True),
    proc_dir=OUT_DIR,
    config={
        "analysis_source": "dozor",
        "shift": 0.0,
        "max_peaks": 5,
        "min_size": 3,
        "percentile_threshold": 95.0,
        "step_size_um": 8,          # raster step size in microns
        "wait_timeout_s": 5,        # short timeout for offline
        "poll_interval_s": 1,
        "max_retries": 0,
        "min_coverage_pct": 80,
        # Experimental (optional):
        # "dual_orientation_strategy": True,
        # "compute_motor_positions": True,
    },
    pipeline_params={"username": "test", "beamline": "23id"},
)
worker.run()

# Results are in:
#   {OUT_DIR}/raster_3d/results.json    — compact collection recommendations
#   {OUT_DIR}/raster_3d/pipeline.log    — step-by-step log
#   {OUT_DIR}/raster_3d/heatmap_xy.png  — XY heatmap + diffraction
#   {OUT_DIR}/raster_3d/heatmap_xz.png  — XZ heatmap + diffraction
```

### Step 3: Inspect results

```bash
# View pipeline log
cat /path/to/output/raster_3d/pipeline.log

# Extract key results (compact format)
python3 -c "
import json
with open('/path/to/output/raster_3d/results.json') as f:
    data = json.load(f)
for i, rec in enumerate(data['recommendations']):
    pos = rec['crystal_position']
    coll = rec['collection']
    crys = rec['crystal']
    dose = rec['dose']
    print(f'Peak {i+1}: coords={pos[\"peak_voxel\"]}, size={pos[\"dimensions_um\"]}um')
    print(f'  motor={pos[\"motor_position\"]}')
    print(f'  SG={crys[\"space_group\"]}, resolution={crys[\"resolution_A\"]}A ({crys[\"resolution_source\"]})')
    print(f'  beam={coll[\"beam_size_um\"]}um, atten={coll[\"attenuation\"]}x')
    print(f'  exposure={coll[\"exposure_time_s\"]}s, n_images={coll[\"n_images\"]}')
    print(f'  detector={coll[\"detector_distance_mm\"]}mm, dose={coll[\"target_dose_mgy\"]}MGy')
    print(f'  RADDOSE avg_dwd={dose[\"raddose3d_avg_dwd_mgy\"]}MGy')
"

# View heatmap images
display /path/to/output/raster_3d/heatmap_xy.png
```

### Notes

- **Scan mode:** If unsure, check bluice Redis: `redis-cli -h bl1ws3-40g -p 8009 HGET bluice:run:r#6 vertical` and `serpentine`. Or use `"row_wise"` as a safe default.
- **Step size:** Check bluice Redis: `redis-cli -h bl1ws3-40g -p 8009 HGET bluice:run:r#6 cell_w_um`. Or measure from the data directory (file count × step = grid width).
- **Without bluice access:** Set `redis_manager=None`. The pipeline will use metadata and config defaults for step size, beam size, and scan mode.
- **Without strategy:** Strategy failures are non-fatal. The pipeline will produce recommendations using default collection parameters (360° rotation, 0.2° oscillation).

## Enabling in Production

1. Set `"enabled": true` in `analysis_config.json` under `raster_3d`
2. Optionally configure `quality_gate` thresholds
3. Optionally override `search_space` to constrain beam sizes / attenuations
4. Restart the data processing server

The pipeline activates automatically when two consecutive RASTER runs complete. No changes to the data collection workflow are needed.

## TODO

- Support user-provided PDB for RADDOSE-3D composition-based calculation
- Multi-crystal strategy (index multiple peaks independently)
- Integration with automated sample centering (send motor positions to DCS/Bluice)
- Automatic detector distance move based on recalculated recommendation
