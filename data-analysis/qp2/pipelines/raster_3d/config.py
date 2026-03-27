# qp2/pipelines/raster_3d/config.py

"""Analysis source definitions and default configuration for 3D raster pipeline."""

import os

CAMERA_SERVER_HOST = os.environ.get("QP2_CAMERA_HOST", "127.0.0.1")
CAMERA_SERVER_PORT = int(os.environ.get("QP2_CAMERA_PORT", "8200"))

ANALYSIS_SOURCES = {
    "dozor": {
        "redis_key_template": "analysis:out:spots:dozor2:{master_file}",
        "x_axis_key": "img_num",
        "default_metric": "Main Score",
    },
    "nxds": {
        "redis_key_template": "analysis:out:nxds:{master_file}",
        "x_axis_key": "img_num",
        "default_metric": "nspots",
    },
}

DEFAULT_CONFIG = {
    "enabled": False,
    "analysis_source": "dozor",
    "metric": None,
    "shift": 0.0,
    "max_peaks": 10,
    "min_size": 3,
    "percentile_threshold": 95.0,
    "step_size_um": 10.0,
    "tracker_ttl_seconds": 3600,
    "wait_timeout_s": 600,
    "retry_timeout_s": 300,
    "poll_interval_s": 15,
    "max_retries": 1,
    "min_coverage_pct": 80,
    "compute_motor_positions": True,  # convert voxel coords to motor positions
    "dual_orientation_strategy": False,  # use both 0° and 90° frames for strategy
    "collection_energies_kev": None,  # list of energies for multi-energy recommendations
    "n_recommendations": 1,  # number of top solutions to return per peak/energy
    "compact_results": True,  # save only collection & crystal params to results.json
}


# Default search space for dose-aware collection recommendation.
# Each list can be overridden by the user in config to fix a parameter.
# e.g. "beam_sizes": [10] to lock beam size to 10x10 um
DEFAULT_SEARCH_SPACE = {
    "beam_sizes": [5, 10, 20, 50],          # um (square beam assumed)
    "attenuations": [1, 2, 3, 5, 10, 50, 100, 500],  # attenuation factors
    "exposure_times": [0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5],  # seconds
    "n_images": [900, 1800, 3600],           # 180°, 360°, 720° at 0.2° osc
    "translations": [0],                      # um, helical translation
}


def get_source_config(config: dict) -> dict:
    """Resolve analysis source configuration from pipeline config.

    Returns dict with keys: redis_key_template, x_axis_key, metric.
    """
    source_name = config.get("analysis_source", "dozor")
    source = ANALYSIS_SOURCES.get(source_name)
    if source is None:
        raise ValueError(
            f"Unknown analysis_source '{source_name}'. "
            f"Available: {list(ANALYSIS_SOURCES.keys())}"
        )
    metric = config.get("metric") or source["default_metric"]
    return {
        "redis_key_template": source["redis_key_template"],
        "x_axis_key": source["x_axis_key"],
        "metric": metric,
    }
