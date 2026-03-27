"""spotfinder2 — Advanced multi-stage Bragg spot detection for serial crystallography.

Combines the best ideas from EMC, CrystFEL peakfinder8, DOZOR, and DIALS:
  - DOZOR-style box-sum detection for weak-signal sensitivity
  - DIALS-style dispersion test for false-positive reduction
  - Multi-scale + ring-aware background modeling
  - MLE position refinement with Poisson likelihood
  - Optional TDS-aware intensity integration
  - GPU acceleration via CuPy (optional, falls back to numpy/scipy)

Usage:
    from qp2.spotfinder2 import SpotFinderPipeline, SpotFinderConfig
    from qp2.xio.hdf5_manager import HDF5Reader

    reader = HDF5Reader(master_file, start_timer=False)
    pipeline = SpotFinderPipeline(reader.get_parameters())
    spots = pipeline.find_spots(reader.get_frame(0))

    print(f"Found {spots.count} spots")
    print(spots.to_numpy()[:5])  # first 5 spots as Nx10 array
"""

from .pipeline import SpotFinderPipeline, SpotFinderConfig
from .spot_list import SpotList
from .backend import get_backend

__version__ = "0.1.0"
__all__ = ["SpotFinderPipeline", "SpotFinderConfig", "SpotList", "get_backend"]
