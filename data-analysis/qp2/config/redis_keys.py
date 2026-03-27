# qp2/config/redis_keys.py

class AnalysisRedisKeys:
    """Redis keys for analysis pipeline outputs."""
    XDS = "analysis:out:xds"
    NXDS = "analysis:out:nxds"
    XIA2 = "analysis:out:xia2"
    XIA2_SSX = "analysis:out:xia2_ssx"
    AUTOPROC = "analysis:out:autoproc"
    CRYSTFEL = "analysis:out:crystfel"
    
    # Spot finding & Indexing live feedback
    SPOTFINDER = "analysis:out:spots:spotfinder"
    DOZOR = "analysis:out:spots:dozor2"
    DIALS = "analysis:out:spots:dials"
    CRYSTFEL_STREAM = "analysis:out:crystfel:stream"
    DIALS_SSX_STREAM = "analysis:out:dials:ssx"
    RASTER_3D = "analysis:out:raster_3d"
    
    # Application settings (base keys — use scoped_*() methods for multi-user isolation)
    KEY_PROCESSING_OVERRIDES = "analysis:settings:processing_overrides"
    KEY_PROCESSING_OVERRIDES_BY_MODE = "analysis:settings:pipelines_by_mode"

    @staticmethod
    def scoped_processing_overrides(group_name: str) -> str:
        """Returns a group-scoped key for processing overrides."""
        return f"{AnalysisRedisKeys.KEY_PROCESSING_OVERRIDES}:{group_name}"

    @staticmethod
    def scoped_pipelines_by_mode(group_name: str) -> str:
        """Returns a group-scoped key for pipeline mode overrides."""
        return f"{AnalysisRedisKeys.KEY_PROCESSING_OVERRIDES_BY_MODE}:{group_name}"

    # Collection parameters captured from bluice at run completion time.
    # Scoped by group + run_prefix to avoid cross-beamline collisions.
    COLLECTION_PARAMS = "analysis:collection_params"

    @staticmethod
    def collection_params_key(group_name: str, run_prefix: str) -> str:
        """Key for collection parameters: ``analysis:collection_params:{group}:{run}``."""
        return f"{AnalysisRedisKeys.COLLECTION_PARAMS}:{group_name}:{run_prefix}"

    # ALCF remote processing status substates
    # These extend the standard status values (SUBMITTED, RUNNING, COMPLETED, FAILED)
    # and are used in the same Redis key pattern: analysis:out:{pipeline}:{file}:status
    ALCF_TRANSFER_OUT = "ALCF_TRANSFER_OUT"    # data transferring to ALCF
    ALCF_QUEUED = "ALCF_QUEUED"                # job submitted, waiting in queue
    ALCF_RUNNING = "ALCF_RUNNING"              # job executing on Polaris
    ALCF_TRANSFER_IN = "ALCF_TRANSFER_IN"      # results transferring back
    # Terminal states reuse existing: COMPLETED, FAILED

class BluiceRedisKeys:
    """Redis keys for Bluice beamline status.

    Pybluice stores values as Redis hashes.  Keys containing ``$`` are
    split into ``(hash_name, field)`` for ``HGET`` / ``HSET``.
    For example ``"bluice:robot$mounted"`` →
    ``HGET bluice:robot mounted``.
    """

    # --- Beamline state ---
    KEY_DATA_DIR = "bluice:paths$data_dir"
    KEY_ROBOT_MOUNTED = "bluice:robot$mounted"
    KEY_BEAMLINE_NAME = "bluice:config$beamline_name"
    KEY_SPREADSHEET_INPUT_REL = "bluice:auto:b$spreadsheet_input_rel"
    KEY_USER = "bluice:collect:state$beamline_user"

    # --- Per-run raster config ---
    # Hash: bluice:run:r#{run_idx}  (run_idx = trailing number in run prefix)
    RASTER_RUN_HASH = "bluice:run:r#{run_idx}"
    FIELD_CELL_W_UM = "cell_w_um"
    FIELD_CELL_H_UM = "cell_h_um"
    FIELD_VERTICAL = "vertical"
    FIELD_SERPENTINE = "serpentine"
    FIELD_GRID_REF = "grid_ref"
    FIELD_ACT_BOUNDS = "act_bounds"
    FIELD_ROWS = "rows"
    FIELD_COLS = "cols"

    # --- Per-run beam / collect config ---
    # Hash: bluice:run:b#{run_idx}
    BEAM_RUN_HASH = "bluice:run:b#{run_idx}"
    FIELD_GS_X_UM = "gs_x_um"
    FIELD_GS_Y_UM = "gs_y_um"
    FIELD_ATTEN_FACTORS = "atten_factors"
    FIELD_COLLI_UM = "colli_um"

    # --- Beamline-level fallbacks ---
    SAMPLEENV_HASH = "bluice:sampleenv"
    FIELD_CUR_ACT_BEAMSIZE_UM = "cur_act_beamsize_um"
    ATTENUATION_HASH = "bluice:attenuation"
    FIELD_ACT_POS_FACTORS = "actPos_factors"

    # --- Camera calibration ---
    CONFIG_HASH = "bluice:config"
    FIELD_MM_PER_PX_HR_H = "mm_per_px_hr_h"
    FIELD_MM_PER_PX_HR_V = "mm_per_px_hr_v"

    # --- Paths ---
    PATHS_HASH = "bluice:paths"
    FIELD_PROCESSING_DIR = "processing_dir"
    FIELD_PREFIX = "prefix"

    # --- Strategy publishing ---
    STRATEGY_TABLE_HASH = "bluice:strategy:table#{dir}:{sample_id}"
    STRATEGY_VERSION_KEY = "bluice:sample:strategy_ver__s"

    @staticmethod
    def raster_run_hash(run_idx: int) -> str:
        """Return the Redis hash name for raster run config."""
        return f"bluice:run:r#{run_idx}"

    @staticmethod
    def beam_run_hash(run_idx: int) -> str:
        """Return the Redis hash name for beam/collect run config."""
        return f"bluice:run:b#{run_idx}"

    @staticmethod
    def hget_key_field(key_with_dollar: str):
        """Split a ``$``-separated key into ``(hash_name, field)`` for HGET."""
        return key_with_dollar.split("$", 1)
