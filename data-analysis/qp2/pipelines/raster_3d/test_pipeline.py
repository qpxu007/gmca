#!/usr/bin/env python3
"""
Integration test for the 3D raster pipeline using real HDF5 data.

Test data: /mnt/beegfs/qxu/raster-spots-finding/raster3d/
  - Q3_ras_run6_R{10..31}_master.h5  (omega=0°,   22 files, row-wise)
  - Q3_ras_run7_R{10..34}_master.h5  (omega=90°,  25 files, row-wise)
  - Step size: 8x8 microns

Uses fakeredis to simulate dozor results without a live Redis server.
"""

import glob
import json
import logging
import os
import random
import sys
import tempfile

import fakeredis
import numpy as np

# Ensure project root is on path
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DATA_DIR = "/mnt/beegfs/qxu/raster-spots-finding/raster3d"
RUN1_PREFIX = "Q3_ras_run6"
RUN2_PREFIX = "Q3_ras_run7"
STEP_SIZE_UM = 8.0


def discover_master_files(data_dir: str, run_prefix: str):
    from qp2.pipelines.raster_3d.matrix_builder import find_master_files
    files = find_master_files(data_dir, run_prefix)
    assert files, f"No master files found for {run_prefix} in {data_dir}"
    return files


def populate_fake_dozor(redis_conn, master_files):
    """Generate synthetic dozor results in fakeredis for all master files.

    We read actual frame counts from HDF5 and assign random scores so the
    volume reconstruction has realistic-looking data.
    """
    from qp2.xio.hdf5_manager import HDF5Reader

    for mf in master_files:
        reader = HDF5Reader(mf, start_timer=False)
        nframes = reader.total_frames
        reader.close()

        redis_key = f"analysis:out:spots:dozor2:{mf}"
        for frame_num in range(1, nframes + 1):
            d = {
                "img_num": frame_num,
                "frame_num": frame_num,
                "num_spots": random.randint(0, 50),
                "Main Score": round(random.uniform(0, 200), 1),
                "Spot Score": round(random.uniform(0, 100), 1),
            }
            redis_conn.hset(redis_key, str(frame_num), json.dumps(d))

    logger.info(f"Populated dozor data for {len(master_files)} master files")


# -------------------------------------------------------------------------
# Test 1: Tracker — consecutive run detection
# -------------------------------------------------------------------------

def test_tracker():
    logger.info("=" * 60)
    logger.info("TEST: RasterRunTracker — pair detection")
    logger.info("=" * 60)

    from qp2.pipelines.raster_3d.tracker import RasterRunTracker

    tracker = RasterRunTracker(ttl_seconds=3600)

    # Parse test
    assert tracker.parse_run_prefix("Q3_ras_run6") == ("Q3_ras_run", 6)
    assert tracker.parse_run_prefix("Q3_ras_run7") == ("Q3_ras_run", 7)
    assert tracker.parse_run_prefix("no_number") is None
    logger.info("  parse_run_prefix: OK")

    # Register first run — no pair yet
    run6_files = discover_master_files(DATA_DIR, RUN1_PREFIX)
    result = tracker.register_completed_raster(
        RUN1_PREFIX, run6_files, [{"collect_mode": "RASTER"}], DATA_DIR, "row_wise"
    )
    assert result is None, "Should not have pair after first run"
    logger.info("  First run registered, no pair: OK")

    # Same scan mode should not pair
    run7_files = discover_master_files(DATA_DIR, RUN2_PREFIX)
    result = tracker.register_completed_raster(
        RUN2_PREFIX, run7_files, [{"collect_mode": "RASTER"}], DATA_DIR, "row_wise"
    )
    assert result is None, "Should not pair identical scan orientations"
    logger.info("  Same scan mode rejected: OK")

    # Re-register with an orthogonal scan mode — should detect pair
    tracker.clear()
    tracker.register_completed_raster(
        RUN1_PREFIX, run6_files, [{"collect_mode": "RASTER"}], DATA_DIR, "row_wise"
    )
    result = tracker.register_completed_raster(
        RUN2_PREFIX,
        run7_files,
        [{"collect_mode": "RASTER"}],
        DATA_DIR,
        "column_wise",
    )
    assert result is not None, "Should detect pair"
    run1_info, run2_info = result
    assert run1_info["run_prefix"] == RUN1_PREFIX
    assert run2_info["run_prefix"] == RUN2_PREFIX
    assert run1_info["run_num"] < run2_info["run_num"]
    logger.info(f"  Pair detected: {run1_info['run_prefix']} + {run2_info['run_prefix']}: OK")

    logger.info("PASSED: tracker\n")


# -------------------------------------------------------------------------
# Test 2: Scan mode detection
# -------------------------------------------------------------------------

def test_scan_mode():
    logger.info("=" * 60)
    logger.info("TEST: scan mode detection")
    logger.info("=" * 60)

    from qp2.pipelines.raster_3d.scan_mode import detect_raster_scan_mode

    run6_files = discover_master_files(DATA_DIR, RUN1_PREFIX)

    # From filename _R pattern → row_wise
    mode = detect_raster_scan_mode({}, run6_files, redis_manager=None)
    assert mode == "row_wise", f"Expected row_wise, got {mode}"
    logger.info(f"  Filename pattern detection: {mode}: OK")

    # From metadata override
    mode = detect_raster_scan_mode(
        {"scan_mode": "column_wise_serpentine"}, run6_files
    )
    assert mode == "column_wise_serpentine"
    logger.info(f"  Metadata override: {mode}: OK")

    logger.info("PASSED: scan_mode\n")


# -------------------------------------------------------------------------
# Test 3: Matrix builder
# -------------------------------------------------------------------------

def test_matrix_builder():
    logger.info("=" * 60)
    logger.info("TEST: matrix builder (scan-mode-aware)")
    logger.info("=" * 60)

    from qp2.pipelines.raster_3d.matrix_builder import build_scan_aware_matrix
    from qp2.pipelines.raster_3d.config import get_source_config

    redis_conn = fakeredis.FakeRedis(decode_responses=True)
    run6_files = discover_master_files(DATA_DIR, RUN1_PREFIX)

    populate_fake_dozor(redis_conn, run6_files)

    source_cfg = get_source_config({"analysis_source": "dozor"})

    matrix, raw_data, scan_offset = build_scan_aware_matrix(
        run6_files, redis_conn, "row_wise", source_cfg
    )

    assert matrix.ndim == 2, f"Expected 2D matrix, got {matrix.ndim}D"
    assert matrix.shape[0] > 0 and matrix.shape[1] > 0
    filled = np.count_nonzero(~np.isnan(matrix))
    assert filled > 0, "Matrix has no data"

    logger.info(f"  Matrix shape: {matrix.shape}")
    logger.info(f"  Filled cells: {filled}/{matrix.size} ({100*filled/matrix.size:.0f}%)")
    logger.info(f"  raw_data entries: {len(raw_data)}")
    logger.info(f"  scan_offset: {scan_offset}")

    logger.info("PASSED: matrix_builder\n")
    return redis_conn


# -------------------------------------------------------------------------
# Test 4: Orthogonality validation
# -------------------------------------------------------------------------

def test_orthogonality():
    logger.info("=" * 60)
    logger.info("TEST: orthogonality validation")
    logger.info("=" * 60)

    from qp2.pipelines.raster_3d.pipeline_worker import _validate_orthogonal, _get_omega_start

    run6_files = discover_master_files(DATA_DIR, RUN1_PREFIX)
    run7_files = discover_master_files(DATA_DIR, RUN2_PREFIX)

    omega6 = _get_omega_start(run6_files[0])
    omega7 = _get_omega_start(run7_files[0])
    logger.info(f"  omega run6={omega6}°, run7={omega7}°")

    # Should pass — 0° and 90° are orthogonal
    _validate_orthogonal(omega6, omega7)
    logger.info("  Orthogonal check (0° vs 90°): OK")

    # Should fail — 0° and 0° are not orthogonal
    try:
        _validate_orthogonal(0.0, 10.0)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        logger.info(f"  Non-orthogonal rejection: OK ({e})")

    logger.info("PASSED: orthogonality\n")


# -------------------------------------------------------------------------
# Test 5: Full pipeline (stages 1-3)
# -------------------------------------------------------------------------

def test_full_pipeline():
    logger.info("=" * 60)
    logger.info("TEST: full pipeline (stages 0→1→2→3)")
    logger.info("=" * 60)

    from qp2.pipelines.raster_3d.pipeline_worker import Raster3DPipelineWorker
    from qp2.pipelines.raster_3d.config import get_source_config

    redis_conn = fakeredis.FakeRedis(decode_responses=True)
    run6_files = discover_master_files(DATA_DIR, RUN1_PREFIX)
    run7_files = discover_master_files(DATA_DIR, RUN2_PREFIX)

    # Populate fake dozor data for both runs
    populate_fake_dozor(redis_conn, run6_files)
    populate_fake_dozor(redis_conn, run7_files)

    with tempfile.TemporaryDirectory(prefix="raster3d_test_") as tmpdir:
        config = {
            "analysis_source": "dozor",
            "metric": None,
            "shift": 0.0,
            "max_peaks": 5,
            "min_size": 2,
            "percentile_threshold": 90.0,
            "step_size_um": STEP_SIZE_UM,
            "wait_timeout_s": 5,
            "retry_timeout_s": 5,
            "poll_interval_s": 1,
            "max_retries": 0,
            "min_coverage_pct": 50,
            "compact_results": False,  # tests need full stage data
        }

        metadata = {
            "collect_mode": "RASTER",
            "energy_ev": 12000,
            "beam_size_x_um": 8,
            "beam_size_y_um": 8,
            "flux": 1e12,
            "osc_range": 0.0,
            "exposure_sec": 0.02,
        }

        worker = Raster3DPipelineWorker(
            run1_prefix=RUN1_PREFIX,
            run2_prefix=RUN2_PREFIX,
            run1_master_files=run6_files,
            run2_master_files=run7_files,
            run1_scan_mode="row_wise",
            run2_scan_mode="row_wise",
            data_dir=DATA_DIR,
            metadata=metadata,
            redis_conn=redis_conn,
            proc_dir=tmpdir,
            config=config,
            pipeline_params={"username": "test", "beamline": "23id"},
        )

        # Run the worker synchronously (it's a QRunnable, call run() directly)
        logger.info("  Launching pipeline...")
        worker.run()

        # Check results file
        results_file = os.path.join(tmpdir, "results.json")
        assert os.path.exists(results_file), f"Results file not found: {results_file}"

        with open(results_file) as f:
            results = json.load(f)

        logger.info(f"  Results file: {results_file}")
        logger.info(f"  Run1: {results.get('run1_prefix')}")
        logger.info(f"  Run2: {results.get('run2_prefix')}")

        stages = results.get("stages", {})

        # Check peaks stage
        peaks_stage = stages.get("peaks", {})
        logger.info(f"  Peaks status: {peaks_stage.get('status')}")
        if peaks_stage.get("status") == "completed":
            peaks = peaks_stage.get("data", [])
            logger.info(f"  Found {len(peaks)} peak(s)")
            if peaks:
                p = peaks[0]
                logger.info(f"    Best peak: coords={p.get('coords')}, "
                           f"dims={p.get('dimensions')}, "
                           f"intensity={p.get('integrated_intensity')}")

        # Check strategy stage
        strat_stage = stages.get("strategy", {})
        logger.info(f"  Strategy status: {strat_stage.get('status')}")
        if strat_stage.get("status") == "failed":
            logger.info(f"    (Expected with random data: {strat_stage.get('error', '')[:80]})")

        # Check raddose stage
        dose_stage = stages.get("raddose3d", {})
        logger.info(f"  RADDOSE status: {dose_stage.get('status')}")
        if dose_stage.get("status") == "completed":
            dose = dose_stage.get("data", {})
            logger.info(f"    Crystal size: {dose.get('crystal_size_um')}")
            logger.info(f"    Avg DWD: {dose.get('avg_dwd_mgy')} MGy")
            logger.info(f"    Lifetime: {dose.get('lifetime_s')} s")

        # Check recommendations (one per peak)
        recs = stages.get("recommendations", [])
        logger.info(f"  Recommendations: {len(recs)} peak(s)")
        for rec in recs:
            cp = rec.get("crystal_position") or {}
            logger.info(
                f"    Peak {rec.get('peak_index')}: "
                f"crystal={cp.get('dimensions_um')} um, "
                f"start={rec.get('start_angle')}°→{rec.get('end_angle')}°, "
                f"osc={rec.get('osc_width')}°, "
                f"exposure={rec.get('exposure_time_s')}s, "
                f"dose={rec.get('target_dose_mgy')} MGy ({rec.get('dose_source')})"
            )

        # Check Redis status
        status_key = f"analysis:out:raster_3d:{RUN1_PREFIX}:status"
        status_raw = redis_conn.get(status_key)
        if status_raw:
            status = json.loads(status_raw)
            logger.info(f"  Redis status: {status.get('status')} (stage={status.get('stage')})")

        # Check Redis hash fields for downstream access
        redis_key = f"analysis:out:raster_3d:{RUN1_PREFIX}"
        logger.info(f"  Redis hash fields:")
        for field in ["n_peaks", "n_recommendations", "start_angle", "end_angle", "osc_width",
                       "exposure_time_s", "attenuation", "detector_distance_mm",
                       "target_dose_mgy", "crystal_size_um", "beam_size_um",
                       "strategy_source", "energy_kev"]:
            val = redis_conn.hget(redis_key, field)
            logger.info(f"    {field}: {val}")

        alias_key = f"analysis:out:raster_3d:index:{RUN2_PREFIX}"
        canonical_key = redis_conn.hget(alias_key, "canonical_key")
        assert canonical_key == redis_key, f"Alias should point to {redis_key}, got {canonical_key}"
        logger.info(f"  Alias key resolves to: {canonical_key}")

        # Verify peaks at minimum completed
        assert peaks_stage.get("status") == "completed", \
            f"Peaks stage should have completed, got: {peaks_stage}"

        if peaks_stage.get("status") == "completed":
            peak_count = len(peaks_stage.get("data", []))
            assert redis_conn.hget(redis_key, "n_peaks") == str(peak_count)
            assert redis_conn.hget(redis_key, "n_recommendations") == str(len(recs))

    logger.info("PASSED: full_pipeline\n")


# -------------------------------------------------------------------------
# Test 5b: Quality gate — abort on weak signal
# -------------------------------------------------------------------------

def test_quality_gate_abort():
    logger.info("=" * 60)
    logger.info("TEST: quality gate — abort when data is too weak")
    logger.info("=" * 60)

    from qp2.pipelines.raster_3d.pipeline_worker import Raster3DPipelineWorker

    redis_conn = fakeredis.FakeRedis(decode_responses=True)
    run6_files = discover_master_files(DATA_DIR, RUN1_PREFIX)
    run7_files = discover_master_files(DATA_DIR, RUN2_PREFIX)

    # Populate with very LOW scores (simulate no diffraction)
    for mf in run6_files + run7_files:
        from qp2.xio.hdf5_manager import HDF5Reader
        reader = HDF5Reader(mf, start_timer=False)
        nf = reader.total_frames
        reader.close()
        rk = f"analysis:out:spots:dozor2:{mf}"
        for i in range(1, nf + 1):
            redis_conn.hset(rk, str(i), json.dumps({
                "img_num": i,
                "Main Score": random.uniform(0, 2.0),  # very weak
                "Resol Visible": 50.0,  # no resolution
            }))

    with tempfile.TemporaryDirectory(prefix="raster3d_gate_") as tmpdir:
        config = {
            "analysis_source": "dozor",
            "metric": None,
            "shift": 0.0,
            "max_peaks": 5,
            "min_size": 2,
            "percentile_threshold": 90.0,
            "step_size_um": STEP_SIZE_UM,
            "wait_timeout_s": 5,
            "poll_interval_s": 1,
            "max_retries": 0,
            "min_coverage_pct": 50,
            "compact_results": False,
            # Quality gate: require strong diffraction
            "quality_gate": {
                "min_max_score": 10.0,
                "min_resolution_A": 10.0,
                "min_strong_frames": 5,
                "score_threshold": 5.0,
            },
        }

        metadata = {
            "collect_mode": "RASTER",
            "energy_ev": 12000,
            "beam_size_x_um": 8,
            "beam_size_y_um": 8,
            "flux": 1e12,
        }

        worker = Raster3DPipelineWorker(
            run1_prefix=RUN1_PREFIX,
            run2_prefix=RUN2_PREFIX,
            run1_master_files=run6_files,
            run2_master_files=run7_files,
            run1_scan_mode="row_wise",
            run2_scan_mode="row_wise",
            data_dir=DATA_DIR,
            metadata=metadata,
            redis_conn=redis_conn,
            proc_dir=tmpdir,
            config=config,
            pipeline_params={"username": "test"},
        )

        worker.run()

        results_file = os.path.join(tmpdir, "results.json")
        assert os.path.exists(results_file), "Results file should still be written on abort"

        with open(results_file) as f:
            results = json.load(f)

        stages = results.get("stages", {})
        gate = stages.get("quality_gate", {})

        assert gate.get("pass") is False, f"Quality gate should have failed: {gate}"
        logger.info(f"  Quality gate result: pass={gate['pass']}")
        logger.info(f"  Reason: {gate.get('reason')}")
        logger.info(f"  Stats: {gate.get('stats')}")

        # Should have no recommendations
        recs = stages.get("recommendations", [])
        assert len(recs) == 0, f"Should have no recommendations, got {len(recs)}"
        logger.info(f"  Recommendations: {len(recs)} (expected 0)")

        # Redis status should be ABORTED
        status_raw = redis_conn.get(f"analysis:out:raster_3d:{RUN1_PREFIX}:status")
        if status_raw:
            status = json.loads(status_raw)
            logger.info(f"  Redis status: {status.get('status')}")
            assert status.get("status") == "ABORTED"

    logger.info("PASSED: quality_gate_abort\n")


# -------------------------------------------------------------------------

# -------------------------------------------------------------------------
# Test 6: Full pipeline with REAL Redis dozor data
# -------------------------------------------------------------------------

REDIS_HOST = "bl1ws1"


def test_full_pipeline_real_redis():
    logger.info("=" * 60)
    logger.info("TEST: full pipeline with REAL dozor data (bl1ws1 Redis)")
    logger.info("=" * 60)

    import redis as _redis

    try:
        redis_conn = _redis.Redis(
            host=REDIS_HOST, port=6379, db=0, decode_responses=True
        )
        redis_conn.ping()
    except _redis.ConnectionError as e:
        logger.warning(f"  SKIPPED — cannot connect to {REDIS_HOST}: {e}")
        return

    from qp2.pipelines.raster_3d.pipeline_worker import Raster3DPipelineWorker

    run6_files = discover_master_files(DATA_DIR, RUN1_PREFIX)
    run7_files = discover_master_files(DATA_DIR, RUN2_PREFIX)

    # Verify dozor data is present
    for run_name, files in [(RUN1_PREFIX, run6_files), (RUN2_PREFIX, run7_files)]:
        has_data = sum(
            1 for f in files
            if redis_conn.hlen(f"analysis:out:spots:dozor2:{f}") > 0
        )
        logger.info(f"  {run_name}: {has_data}/{len(files)} files have dozor data")
        assert has_data == len(files), f"Missing dozor data for {run_name}"

    with tempfile.TemporaryDirectory(prefix="raster3d_real_") as tmpdir:
        config = {
            "analysis_source": "dozor",
            "metric": None,
            "shift": 0.0,
            "max_peaks": 5,
            "min_size": 3,
            "percentile_threshold": 95.0,
            "step_size_um": STEP_SIZE_UM,
            "wait_timeout_s": 5,
            "retry_timeout_s": 5,
            "poll_interval_s": 1,
            "max_retries": 0,
            "min_coverage_pct": 80,
            "compact_results": False,
        }

        metadata = {
            "collect_mode": "RASTER",
            "energy_ev": 12000,
            "beam_size_x_um": 8,
            "beam_size_y_um": 8,
            "flux": 1e12,
            "osc_range": 0.0,
            "exposure_sec": 0.02,
        }

        worker = Raster3DPipelineWorker(
            run1_prefix=RUN1_PREFIX,
            run2_prefix=RUN2_PREFIX,
            run1_master_files=run6_files,
            run2_master_files=run7_files,
            run1_scan_mode="row_wise",
            run2_scan_mode="row_wise",
            data_dir=DATA_DIR,
            metadata=metadata,
            redis_conn=redis_conn,
            proc_dir=tmpdir,
            config=config,
            pipeline_params={"username": "test", "beamline": "23id"},
        )

        logger.info("  Launching pipeline with real dozor data...")
        worker.run()

        # Read results
        results_file = os.path.join(tmpdir, "results.json")
        assert os.path.exists(results_file), f"Results file not found: {results_file}"

        with open(results_file) as f:
            results = json.load(f)

        stages = results.get("stages", {})

        # --- Peaks ---
        peaks_stage = stages.get("peaks", {})
        assert peaks_stage.get("status") == "completed", f"Peaks failed: {peaks_stage}"
        peaks = peaks_stage.get("data", [])
        logger.info(f"  Peaks: {len(peaks)} hotspot(s) found")
        for i, p in enumerate(peaks[:3]):
            logger.info(
                f"    #{i+1}: coords={p.get('coords')}, "
                f"dims={[round(d, 1) for d in p.get('dimensions', [])]}, "
                f"intensity={p.get('integrated_intensity', 0):.1f}"
            )

        # --- Strategy ---
        strat_stage = stages.get("strategy", {})
        logger.info(f"  Strategy: {strat_stage.get('status')}")
        if strat_stage.get("status") == "completed":
            sd = strat_stage.get("data", {})
            logger.info(f"    Space group: {sd.get('space_group')}")
            logger.info(f"    Unit cell: {sd.get('unit_cell')}")
            logger.info(f"    Mosaicity: {sd.get('mosaicity')}")
        elif strat_stage.get("status") == "failed":
            logger.info(f"    Error: {strat_stage.get('error', '')[:100]}")

        # --- RADDOSE ---
        dose_stage = stages.get("raddose3d", {})
        logger.info(f"  RADDOSE: {dose_stage.get('status')}")
        if dose_stage.get("status") == "completed":
            dd = dose_stage.get("data", {})
            logger.info(f"    Crystal size: {dd.get('crystal_size_um')} um")
            logger.info(f"    Avg DWD: {dd.get('avg_dwd_mgy')} MGy")
            logger.info(f"    Max Dose: {dd.get('max_dose_mgy')} MGy")
            logger.info(f"    Lifetime: {dd.get('lifetime_s'):.1f} s" if dd.get("lifetime_s") else "    Lifetime: N/A")
            logger.info(f"    Dose rate: {dd.get('dose_rate_mgy_s'):.2f} MGy/s" if dd.get("dose_rate_mgy_s") else "    Dose rate: N/A")

        # Check recommendations
        recs = stages.get("recommendations", [])
        logger.info(f"  === Collection Recommendations ({len(recs)} peak(s)) ===")
        for rec in recs:
            cp = rec.get("crystal_position") or {}
            logger.info(f"  --- Peak {rec.get('peak_index')} ---")
            logger.info(f"    Crystal size: {cp.get('dimensions_um')} um")
            logger.info(f"    Crystal coords: {cp.get('coords_voxel')}")
            logger.info(f"    Strategy: {rec.get('strategy_source')}, SG={rec.get('space_group')}")
            logger.info(f"    Resolution: {rec.get('resolution_A')} A")
            logger.info(f"    Angles: {rec.get('start_angle')}° → {rec.get('end_angle')}° (Δ={rec.get('osc_width')}°)")
            logger.info(f"    N images: {rec.get('n_images')}")
            logger.info(f"    Detector distance: {rec.get('detector_distance_mm')} mm")
            logger.info(f"    Beam size: {rec.get('beam_size_um')} um")
            logger.info(f"    Exposure: {rec.get('exposure_time_s')} s")
            logger.info(f"    Attenuation: {rec.get('attenuation')}")
            logger.info(f"    Target dose: {rec.get('target_dose_mgy')} MGy ({rec.get('dose_source')})")
            logger.info(f"    Estimated dose: {rec.get('estimated_dose_mgy')} MGy")
            logger.info(f"    Total time: {rec.get('total_collection_time_s')} s")

    logger.info("PASSED: full_pipeline_real_redis\n")


# -------------------------------------------------------------------------

def test_strategy_source_uses_selected_program():
    from qp2.pipelines.raster_3d.pipeline_worker import Raster3DPipelineWorker

    worker = Raster3DPipelineWorker(
        run1_prefix="sample_ras_run1",
        run2_prefix="sample_ras_run2",
        run1_master_files=[],
        run2_master_files=[],
        run1_scan_mode="row_wise",
        run2_scan_mode="column_wise",
        data_dir=DATA_DIR,
        metadata={
            "collect_mode": "RASTER",
            "energy_ev": 12000,
            "beam_size_x_um": 8,
            "beam_size_y_um": 8,
            "flux": 1e12,
        },
        redis_conn=fakeredis.FakeRedis(decode_responses=True),
        proc_dir=tempfile.gettempdir(),
        config={
            "step_size_um": STEP_SIZE_UM,
            "compute_motor_positions": False,
        },
        pipeline_params={"username": "test"},
    )

    peak = {
        "coords": [1.0, 2.0, 3.0],
        "dimensions": [2.0, 2.0, 2.0],
        "integrated_intensity": 10.0,
    }
    strategy = {
        "raw": {"final": {}},
        "selected_program": "mosflm",
        "space_group": "P212121",
        "unit_cell": [10, 20, 30, 90, 90, 90],
        "mosaicity": 0.2,
        "osc_start": 0.0,
        "total_rotation": 180.0,
        "max_osc_range": 0.2,
    }

    rec = worker._build_recommendation(peak, strategy, None)
    assert rec["strategy_source"] == "mosflm"
    logger.info("  Strategy source honors selected_program: OK")


def main():
    logger.info("3D Raster Pipeline Integration Tests")
    logger.info(f"Data dir: {DATA_DIR}")
    logger.info("")

    test_tracker()
    test_scan_mode()
    test_matrix_builder()
    test_orthogonality()
    test_full_pipeline()
    test_quality_gate_abort()
    test_full_pipeline_real_redis()
    test_strategy_source_uses_selected_program()

    logger.info("=" * 60)
    logger.info("ALL TESTS PASSED")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
