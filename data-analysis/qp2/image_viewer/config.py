# diffraction_viewer/config.py

"""
Central configuration constants for the Diffraction Viewer application.
"""

from qp2.config.servers import ServerConfig

# --- UI Timing (Optimized for Performance) ---
ZOOM_CONTRAST_DEBOUNCE_MS: int = 50  # Reduced from 100ms for more responsive feel
PIXEL_TEXT_UPDATE_DEBOUNCE_MS: int = 100  # Reduced from 150ms for faster updates

# --- Pixel Text Display ---
PIXEL_TEXT_ZOOM_THRESHOLD: float = 25.0  # Reduced from 25.0 to show text sooner
PIXEL_TEXT_GRID_RADIUS: int = 4  # Grid radius around mouse (e.g., 4 -> 9x9)
PIXEL_TEXT_COLOR = "yellow"  # Defined in image_viewer.py using pg.mkColor
IMAGE_COLORMAP = "plasma"  # "inferno"  # "gist_yarg"  "cividis" # "cividis" "viridis"  # for common white/gray use "gist_yarg"

# --- Playback Control ---
PLAYBACK_JUMP_OFFSET: int = 5  # Frames to jump behind latest if lagging significantly
PLAYBACK_LAG_THRESHOLD: int = 10  # Number of frames behind before considered lagging

# --- Peak Finding & Display ---
NUM_PEAKS_TO_LABEL: int = 10  # Number of top peaks to label with rank
PEAK_LABEL_COLOR = "yellow"  # Consistent with measurement/pixel text color
PEAK_LABEL_OFFSET: tuple[int, int] = (5, -5)  # (dx, dy) pixel offset for label
PEAK_LABEL_FONT_SIZE: int = 12  # Font size for peak labels
NUM_REFLECTION_LABELS: int = 10  # Number of CrystFEL reflection labels to show (-1 for all)
MAX_DISPLAYED_REFLECTIONS: int = 5000  # Max number of aligned/indexed reflections to show
MAX_DISPLAYED_SPOTS: int = 5000  # Max number of raw spots to show

# --- Calibration ---
# Default calibration ring resolution (can be overridden by settings)
DEFAULT_CALIBRATION_RING_RESOLUTION: float = 3.022  # Angstrom (e.g., for burn paper)
SUM_FRAME_COUNT: int = 5

# --- Defaults for Settings Dialog (used if not found in persistent settings) ---
# Note: calibration_band_width default depends on calibration_mode:
#   - 'StartFrom Scratch': 5
#   - 'Refine': 15
DEFAULT_SETTINGS = {
    "bad_pixel_max_results": 100,
    "contrast_low_percentile": 50.0,
    "contrast_high_percentile": 99.5,
    "resolution_rings": [3.67, 3.03, 2.25],  # Angstrom
    "calibration_mode": "Refine",  # if the beam center is good enough, just refine, otherwise
    "calibration_ring_resolution": DEFAULT_CALIBRATION_RING_RESOLUTION,  # Angstrom
    # Set default band width based on calibration_mode
    "calibration_band_width": 15,  # Default for 'Refine'; set to 5 for 'StartFrom Scratch' in code
    "adaptive_live_playback": True, # Automatically match playback speed to exposure time
    "playback_skip": 2,  # Frames to sum/skip
    "playback_interval_ms": 50,
    "image_filter_type": "Poisson Threshold",  # Default filter
    "se_size": 5,  # Default filter kernel size
    "scan_mode": "row_wise",
    # --- Common Processing Parameters ---
    "processing_common_mode": "manual",
    "processing_common_space_group": "",
    "processing_common_unit_cell": "",
    "processing_common_model_file": "",
    "processing_common_reference_reflection_file": "",
    "processing_common_proc_dir_root": "",
    "processing_common_res_cutoff_low": None,
    "processing_common_res_cutoff_high": None,
    "processing_common_native": True,
    # XDS Plugin Settings
    "xds_space_group": "",
    "xds_unit_cell": "",
    "xds_resolution": 0.0,
    "xds_native": True,
    "xds_reference_hkl": "",
    "xds_model_pdb": "",
    "xds_nproc": 32,
    "xds_njobs": 6,
    "xds_proc_dir_root": "",
    # builtin spot finder
    "peak_finding_low_resolution_A": 20.0,
    "peak_finding_high_resolution_A": 3.0,
    "peak_finding_zscore_cutoff": 3.0,  # SNR threshold
    "peak_finding_num_peaks": 150,  # Max initial peaks
    "peak_finding_min_distance": 21,  # Pixels
    "peak_finding_min_pixels": 3,
    "peak_finding_min_intensity": 5,  # Intensity units
    "peak_finding_median_filter_size": None,  # Options: None, 3, 5
    "peak_finding_bin1_max_res": 5.5,  # Upper bound of first bin (e.g. 20-5.5)
    "peak_finding_bin1_min_count": 2,  # Min spots in first bin to avoid ice/false detection
    # Dozor specific
    "dozor_beamstop_size": 100,
    "dozor_spot_size": 3,
    "dozor_spot_level": 6,
    "dozor_dist_cutoff": 20.0,
    "dozor_res_cutoff_low": 20.0,
    "dozor_res_cutoff_high": 2.5,
    "dozor_check_ice_rings": "T",
    "dozor_exclude_resolution_ranges": [],
    "dozor_min_spot_range_low": 15.0,
    "dozor_min_spot_range_high": 4.0,
    "dozor_min_spot_count": 2,
    ## crystfel specific
    "crystfel_nproc": 32,
    "crystfel_peaks_method": "peakfinder8",
    "crystfel_min_snr": 5.0,
    "crystfel_local_bg_radius": 3,
    "crystfel_min_snr_biggest_pix": 7.0,
    "crystfel_min_snr_peak_pix": 6.0,
    "crystfel_min_sig": 11.0,
    "crystfel_min_peaks": 15,
    "crystfel_no_non_hits": True,
    "crystfel_indexing_methods": "xgandalf",
    "crystfel_no_refine": False,
    "crystfel_no_check_peaks": False,
    "crystfel_integration_mode": "Standard",
    "crystfel_push_res": 0.0,
    "crystfel_int_radius": "3,4,5",
    "crystfel_extra_options": "",
    # Peakfinder8 specific
    "crystfel_peakfinder8_threshold": 20.0,
    "crystfel_peakfinder8_auto_threshold": True,
    "crystfel_peakfinder8_min_pix_count": 2,
    "crystfel_peakfinder8_max_pix_count": 200,
    "crystfel_peakfinder8_fast": True,
    "crystfel_pdb_file": "",
    "crystfel_include_mask": False,
    "crystfel_delete_workdir": True,
    # CrystFEL Speed Flags
    "crystfel_asdf_fast": True,
    "crystfel_no_retry": True,
    "crystfel_no_multi": True,
    # CrystFEL XGANDALF Specific
    "crystfel_xgandalf_fast": True,
    "crystfel_xgandalf_sampling_pitch": 6,
    "crystfel_xgandalf_grad_desc_iterations": 4,
    "crystfel_xgandalf_tolerance": 0.02,
    "crystfel_xgandalf_no_deviation": False,
    "crystfel_xgandalf_min_lattice": 30.0,
    "crystfel_xgandalf_max_lattice": 250.0,
    "crystfel_xgandalf_max_peaks": 250,
    # --- nXDS Plugin Settings ---
    "nxds_space_group": "",
    "nxds_unit_cell": "",
    "nxds_reference_hkl": "",
    "nxds_pdb_file": "",
    "nxds_native": True,
    "nxds_powder": False,
    "nxds_nproc": 16,
    "nxds_njobs": 8,
    "nxds_auto_merge": False,
    # xia2
    "xia2_pipeline": "xia2_dials",
    "xia2_pipeline_choice": "dials",
    "xia2_space_group": "",
    "xia2_unit_cell": "",
    "xia2_highres": 0.0,
    "xia2_model": "",
    "xia2_nproc": 32,
    "xia2_njobs": 1,
    "xia2_fast": False,
    "xia2_native": True,
    # xia2_ssx
    "xia2_ssx_space_group": "",
    "xia2_ssx_unit_cell": "",
    "xia2_ssx_model": "",
    "xia2_ssx_reference_hkl": "",
    "xia2_ssx_nproc": 32,
    "xia2_ssx_njobs": 1,
    "xia2_ssx_max_lattices": 3,
    "xia2_ssx_min_spots": 10,
    # autoproc
    "autoproc_nproc": 8,
    "autoproc_njobs": 2,
    "autoproc_use_slurm": True,
    "autoproc_model": "",
    "autoproc_space_group": "",
    "autoproc_unit_cell": "",
    "autoproc_highres": 0.0,
    "autoproc_optimize": False,
    "autoproc_native": True,
}
# --- File Dialog ---
# QSettings keys for remembering last directory
SETTINGS_ORGANIZATION = "GMCA-APS"
SETTINGS_APPLICATION = "DiffractionViewer"
SETTINGS_LAST_DIR_KEY = "lastOpenDir"

REDIS_DOZOR_KEY_PREFIX = "analysis:out:spots:dozor2"
DOZOR_PLOT_REFRESH_INTERVAL = ServerConfig.ANALYSIS_REFRESH_INTERVAL_MS  # msec

MASKED_CIRCLES = [
    ("beam_x", "beam_y", 100),
]

MASKED_RECTANGLES = [
    (0, "beam_y-100", "beam_x", "beam_y+100"),
]

# --- AI Assistant ---
COMMON_RAG_CODEBASES = [
    "/mnt/beegfs/qxu/data-analysis/qp2", # Main Project
    "/mnt/beegfs/qxu/data-analysis/qp2/image_viewer",
]
