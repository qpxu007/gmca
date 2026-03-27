"""GPU/CPU backend abstraction for spotfinder2.

Provides a Backend object that wraps either CuPy (GPU) or NumPy/SciPy (CPU),
so all downstream code can be written once and run on either.
"""

import numpy as np
from scipy import ndimage as scipy_ndimage
from scipy import stats as scipy_stats
from types import ModuleType

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

_HAS_CUPY = False
try:
    import cupy as cp
    from cupyx.scipy import ndimage as cupy_ndimage
    _HAS_CUPY = True
except ImportError:
    pass


class Backend:
    """Namespace holding the active array library and ndimage module.

    Attributes:
        xp: numpy or cupy module
        ndimage: scipy.ndimage or cupyx.scipy.ndimage
        has_gpu: whether GPU backend is active
        name: "gpu" or "cpu"
    """

    def __init__(self, use_gpu: bool = False):
        if use_gpu and _HAS_CUPY:
            self.xp = cp
            self.ndimage = cupy_ndimage
            self.has_gpu = True
            self.name = "gpu"
        else:
            self.xp = np
            self.ndimage = scipy_ndimage
            self.has_gpu = False
            self.name = "cpu"

    def to_device(self, arr: np.ndarray):
        """Transfer numpy array to device (GPU or no-op for CPU)."""
        if self.has_gpu:
            return cp.asarray(arr)
        return arr

    def to_host(self, arr) -> np.ndarray:
        """Transfer array to host numpy (from GPU or no-op for CPU)."""
        if self.has_gpu and hasattr(arr, "get"):
            return arr.get()
        return np.asarray(arr)

    def ensure_float32(self, arr):
        """Convert to float32 on the active device."""
        return self.xp.asarray(arr, dtype=self.xp.float32)

    def zeros(self, shape, dtype=None):
        dtype = dtype or self.xp.float32
        return self.xp.zeros(shape, dtype=dtype)

    def zeros_like(self, arr, dtype=None):
        return self.xp.zeros_like(arr, dtype=dtype)

    def arange(self, *args, **kwargs):
        return self.xp.arange(*args, **kwargs)

    def bincount(self, x, weights=None, minlength=0):
        return self.xp.bincount(x, weights=weights, minlength=minlength)

    def __repr__(self):
        return f"Backend({self.name})"


def get_backend(force_cpu: bool = False) -> Backend:
    """Factory: returns GPU backend if CuPy available, else CPU.

    Args:
        force_cpu: if True, always use CPU even if CuPy is available.
    """
    use_gpu = _HAS_CUPY and not force_cpu
    backend = Backend(use_gpu=use_gpu)
    logger.info(f"spotfinder2 backend: {backend.name} (CuPy available: {_HAS_CUPY})")
    return backend
