from typing import List

from qp2.log.logging_config import get_logger

from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)


class XdsConfig:
    XDS_EXECUTABLE = "xds_par"
    XSCALE_EXECUTABLE = "xscale_par"
    XDSCONV_EXECUTABLE = "xdsconv"
    POINTLESS_EXECUTABLE = "pointless"
    F2MTZ_EXECUTABLE = "f2mtz"
    LIB_PATH = ProgramConfig.get_library_path("dectris-neggia")
    DEFAULT_BEAMSTOP_RADIUS = 100
    DEFAULT_RESOLUTION_CUTOFF = 0.3
    DEFAULT_NJOBS = 6
    DEFAULT_NPROC = 32
    CC_HALF_TARGET = 35.0
    CC_ANOM_TARGET = 30.0
    ISIGMA_TARGET = 0.5
    OPTIMIZE_ISA_THRESHOLD = 0.2
    OPTIMIZE_ISA_TOLERANCE_INITIAL = -1
    MAX_OPTIMIZE_ITERATIONS = 5
    UNITCELL_EDGE_ERR_TOLERANCE = 0.015
    UNITCELL_ANGLE_ERR_TOLERANCE = 0.01
    JOB_STEPS = [
        "XYCORR",
        "INIT",
        "COLSPOT",
        "IDXREF",
        "XPLAN",
        "DEFPIX",
        "INTEGRATE",
        "CORRECT",
    ]
    COMMAND_TIMEOUT = 3600.0  # Default timeout for external commands (1 hour)
    # NXDS
    NXDS_EXECUTABLE = "nxds_par"
    NXDS_JOB_STEPS = [
        "XYCORR",
        "FILTER",
        "INIT",
        "COLSPOT",
        "POWDER",
        "IDXREF",
        "INTEGRATE",
        "CORRECT",
    ]


class Filenames:
    XDS_INPUT = "XDS.INP"
    XDSCONV_INPUT = "XDSCONV.INP"
    F2MTZ_INPUT = "F2MTZ.INP"  # Input for f2mtz (often generated on the fly)
    COLSPOT_LP = "COLSPOT.LP"
    IDXREF_LP = "IDXREF.LP"
    XPLAN_LP = "XPLAN.LP"
    CORRECT_LP = "CORRECT.LP"
    INTEGRATE_LP = "INTEGRATE.LP"
    XDS_ASCII_HKL = "XDS_ASCII.HKL"
    POINTLESS_XML = "pointless.xml"
    POINTLESS_LOG = "pointless.out"
    GXPARM = "GXPARM.XDS"
    XPARM = "XPARM.XDS"
    # nxds
    NXDS_INPUT = "nXDS.INP"
    NXDS_ASCII_HKL = "nXDS_ASCII.HKL"
    NXDS_SPOT = "SPOT.nXDS"
    NGXPARM = "GXPARM.nXDS"
    NXPARM = "XPARM.nXDS"
    NXDS_JSON = "nXDS.json"
    XDS_JSON = "XDS.json"
    XDS_STATS_JSON = "XDS_stats.json"


DETECTOR_GAPS = {
    (4150, 4371): [
        "0 4150 513 553",
        "0 4150 1064 1104",
        "0 4150 1615 1655",
        "0 4150 2166 2206",
        "0 4150 2717 2757",
        "0 4150 3268 3308",
        "0 4150 3819 3859",
        "1029 1042 0 4371",
        "2069 2082 0 4371",
        "3109 3122 0 4371",
    ],
    (4148, 4362): [
        "1028 1041 0 4363",
        "2068 2081 0 4363",
        "3108 3121 0 4363",
        "0 4149 512 551",
        "0 4149 1062 1101",
        "0 4149 1612 1651",
        "0 4149 2162 2201",
        "0 4149 2712 2751",
        "0 4149 3262 3301",
        "0 4149 3812 3851",
    ],
}


def get_detector_gaps(nx: int, ny: int) -> List[str]:
    """
    Return predefined intermodule gaps for EIGER detectors based on dimensions.
    """
    gaps = DETECTOR_GAPS.get((nx, ny))
    if gaps:
        logger.info(f"Using predefined detector gaps for dimensions {nx} x {ny}")
        return gaps
    else:
        logger.warning(
            f"No predefined detector gaps found for dimensions {nx} x {ny}"
        )
        return []
