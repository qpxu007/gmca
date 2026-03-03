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

class BluiceRedisKeys:
    """Redis keys for Bluice beamline status."""
    KEY_DATA_DIR = "bluice:paths$data_dir"
    KEY_ROBOT_MOUNTED = "bluice:robot$mounted"
    KEY_BEAMLINE_NAME = "bluice:config$beamline_name"
    KEY_SPREADSHEET_INPUT_REL = "bluice:auto:b$spreadsheet_input_rel"
    KEY_USER = "bluice:collect:state$beamline_user"
