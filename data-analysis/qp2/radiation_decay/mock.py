from collections import OrderedDict
from dataclasses import dataclass

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class Sample:
    cell: str = "78 78 39 90 90 90"
    nmon: int = 8
    nres: int = 129
    crystal_size: str = "100 100 100"
    crystal_shape: str = "Cuboid"
    angle_l: float = 0.0  # Add AngleL
    angle_p: float = 0.0  # Add AngleP
    coef_calc: str = "rd3d"
    ndna: int = 0
    nrna: int = 0
    ncarb: int = 0
    solvent_fraction: float = 0.50
    protein_heavy_atoms: str = ""
    solvent_heavy_conc: str = ""


@dataclass
class Beam:
    flux: float = 1.0e12
    energy: float = 12.0
    beam_size: str = "20 20"
    attenuation_factor: float = 1.0


@dataclass
class Wedge:
    start_angle: float = 0.0
    osc: float = 1.0
    exposure_time_per_image: float = 0.2
    nimages: int = 1800
    translate_per_degree: str = "0 0 0"


def run_raddose3d(sample, beam, wedges, swap_xy=False, debug=True):
    logger.debug("--- MOCK RADDOSE-3D RUN (real library not found) ---")
    import time

    time.sleep(0.5)
    simulated_dwd = (
            (beam.flux / 1e12)
            * wedges[0].nimages
            * wedges[0].exposure_time_per_image
            * 0.05
    )
    simulated_max_dose = simulated_dwd * 1.5
    simulated_last_dwd = simulated_dwd * 1.2
    data = [{" Average DWD": f" {simulated_dwd:.4f}"}]
    summary = OrderedDict(
        [
            ("Avg DWD", simulated_dwd),
            ("Max Dose", simulated_max_dose),
            ("Last DWD", simulated_last_dwd),
        ]
    )
    return data, summary
