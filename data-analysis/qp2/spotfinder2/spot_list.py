"""SpotList data class and I/O for spotfinder2.

Provides a structured container for spot-finding results with serialization
to JSON, HDF5, and numpy formats.
"""

import json
import numpy as np
from dataclasses import dataclass, fields, asdict
from typing import Optional, List

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

# Spot flags (bitfield)
FLAG_ON_RING = 0x1
FLAG_ICE_CANDIDATE = 0x2
FLAG_SATURATED = 0x4
FLAG_EDGE = 0x8
FLAG_TDS_FITTED = 0x10


SPOT_DTYPE = np.dtype([
    ("x", np.float32),
    ("y", np.float32),
    ("intensity", np.float32),
    ("background", np.float32),
    ("snr", np.float32),
    ("resolution", np.float32),
    ("size", np.int32),
    ("aspect_ratio", np.float32),
    ("tds_intensity", np.float32),
    ("flags", np.int32),
])


class SpotList:
    """Collection of detected Bragg spots with array storage.

    Internally stored as a numpy structured array for vectorized access.
    Supports serialization to JSON, HDF5, and plain numpy arrays.
    """

    def __init__(self, data: Optional[np.ndarray] = None, metadata: Optional[dict] = None):
        """
        Args:
            data: structured numpy array with SPOT_DTYPE, or None for empty list.
            metadata: optional dict for per-frame annotations (e.g. n_crystals).
        """
        if data is not None:
            if data.dtype != SPOT_DTYPE:
                raise ValueError(f"Expected dtype {SPOT_DTYPE}, got {data.dtype}")
            self._data = data
        else:
            self._data = np.empty(0, dtype=SPOT_DTYPE)
        self.metadata = metadata or {}

    @classmethod
    def from_arrays(cls, x, y, intensity, background, snr, resolution,
                    size, aspect_ratio=None, tds_intensity=None, flags=None):
        """Construct from parallel arrays."""
        n = len(x)
        data = np.empty(n, dtype=SPOT_DTYPE)
        data["x"] = np.asarray(x, dtype=np.float32)
        data["y"] = np.asarray(y, dtype=np.float32)
        data["intensity"] = np.asarray(intensity, dtype=np.float32)
        data["background"] = np.asarray(background, dtype=np.float32)
        data["snr"] = np.asarray(snr, dtype=np.float32)
        data["resolution"] = np.asarray(resolution, dtype=np.float32)
        data["size"] = np.asarray(size, dtype=np.int32)
        data["aspect_ratio"] = (
            np.asarray(aspect_ratio, dtype=np.float32) if aspect_ratio is not None
            else np.ones(n, dtype=np.float32)
        )
        data["tds_intensity"] = (
            np.asarray(tds_intensity, dtype=np.float32) if tds_intensity is not None
            else np.zeros(n, dtype=np.float32)
        )
        data["flags"] = (
            np.asarray(flags, dtype=np.int32) if flags is not None
            else np.zeros(n, dtype=np.int32)
        )
        return cls(data)

    @property
    def count(self) -> int:
        return len(self._data)

    def __len__(self):
        return self.count

    def __getitem__(self, key):
        """Access by field name (str) or index (int/slice)."""
        if isinstance(key, str):
            return self._data[key]
        return SpotList(self._data[key])

    @property
    def x(self): return self._data["x"]

    @property
    def y(self): return self._data["y"]

    @property
    def intensity(self): return self._data["intensity"]

    @property
    def background(self): return self._data["background"]

    @property
    def snr(self): return self._data["snr"]

    @property
    def resolution(self): return self._data["resolution"]

    @property
    def size(self): return self._data["size"]

    @property
    def aspect_ratio(self): return self._data["aspect_ratio"]

    @property
    def tds_intensity(self): return self._data["tds_intensity"]

    @property
    def flags(self): return self._data["flags"]

    def to_numpy(self) -> np.ndarray:
        """Return as Nx10 float32 array (flags cast to float)."""
        return np.column_stack([self._data[name] for name in SPOT_DTYPE.names])

    def filter(self, mask: np.ndarray) -> "SpotList":
        """Return subset where boolean mask is True."""
        return SpotList(self._data[mask], metadata=self.metadata.copy())

    def sort_by(self, field: str, ascending: bool = True) -> "SpotList":
        """Return sorted copy."""
        order = np.argsort(self._data[field])
        if not ascending:
            order = order[::-1]
        return SpotList(self._data[order], metadata=self.metadata.copy())

    def to_dict(self) -> dict:
        """Serialize to dict-of-lists (JSON/Redis compatible)."""
        d = {
            name: self._data[name].tolist()
            for name in SPOT_DTYPE.names
        }
        if self.metadata:
            d["metadata"] = self.metadata
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "SpotList":
        """Deserialize from dict-of-lists."""
        n = len(d.get("x", []))
        data = np.empty(n, dtype=SPOT_DTYPE)
        for name in SPOT_DTYPE.names:
            if name in d:
                data[name] = d[name]
            else:
                data[name] = 0
        metadata = d.get("metadata", {})
        return cls(data, metadata=metadata)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, s: str) -> "SpotList":
        return cls.from_dict(json.loads(s))

    def to_hdf5(self, group):
        """Write to an h5py Group as separate datasets per field."""
        for name in SPOT_DTYPE.names:
            if name in group:
                del group[name]
            group.create_dataset(name, data=self._data[name])
        group.attrs["count"] = self.count
        # Store simple scalar/string metadata as HDF5 attributes
        for k, v in self.metadata.items():
            if isinstance(v, (int, float, str, bool)):
                group.attrs[k] = v

    @classmethod
    def from_hdf5(cls, group) -> "SpotList":
        """Read from an h5py Group."""
        n = group.attrs.get("count", 0)
        if n == 0:
            return cls()
        data = np.empty(n, dtype=SPOT_DTYPE)
        for name in SPOT_DTYPE.names:
            if name in group:
                data[name] = group[name][:]
        # Restore metadata from HDF5 attributes
        skip_keys = {"count"} | set(SPOT_DTYPE.names)
        metadata = {
            k: v for k, v in group.attrs.items() if k not in skip_keys
        }
        return cls(data, metadata=metadata)

    def resolution_histogram(self, bins=50):
        """Return (bin_edges, counts) histogram of d-spacing values."""
        valid = self.resolution > 0
        if not np.any(valid):
            return np.array([]), np.array([])
        return np.histogram(self.resolution[valid], bins=bins)

    def __repr__(self):
        meta_str = f", metadata={self.metadata}" if self.metadata else ""
        return f"SpotList(count={self.count}{meta_str})"
