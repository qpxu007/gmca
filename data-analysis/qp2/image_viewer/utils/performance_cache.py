"""
Performance caching utilities for the image viewer.

This module provides caching mechanisms to improve performance by avoiding
redundant calculations for contrast levels, masks, and other expensive operations.
"""

import hashlib
from collections import OrderedDict
from typing import Dict, Tuple, Optional, Any

import numpy as np

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class LRUCache:
    """Simple LRU cache implementation with size limits."""

    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.cache = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        """Get item from cache, moving it to end (most recently used)."""
        if key in self.cache:
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    def put(self, key: str, value: Any) -> None:
        """Put item in cache, evicting oldest if necessary."""
        if key in self.cache:
            # Update existing item
            self.cache.move_to_end(key)
            self.cache[key] = value
        else:
            # Add new item
            if len(self.cache) >= self.max_size:
                # Remove least recently used item
                self.cache.popitem(last=False)
            self.cache[key] = value

    def clear(self) -> None:
        """Clear all cached items."""
        self.cache.clear()

    def size(self) -> int:
        """Get current cache size."""
        return len(self.cache)


class PerformanceCache:
    """Centralized performance cache for image viewer operations."""

    def __init__(self, contrast_cache_size: int = 50, mask_cache_size: int = 20):
        self.contrast_cache = LRUCache(contrast_cache_size)
        self.mask_cache = LRUCache(mask_cache_size)
        self.radial_cache = LRUCache(30)

        # Statistics
        self.contrast_hits = 0
        self.contrast_misses = 0
        self.mask_hits = 0
        self.mask_misses = 0
        self.radial_hits = 0
        self.radial_misses = 0

    def _hash_array(self, arr: np.ndarray, sample_size: int = 1000) -> str:
        """Create hash of numpy array using sampling for large arrays."""
        if arr is None:
            return "none"

        # For large arrays, sample to create hash
        if arr.size > sample_size:
            # Sample deterministically
            step = max(1, arr.size // sample_size)
            sample = np.array(arr.flat[::step])
        else:
            sample = np.array(arr.flat)

        # Create hash from sample
        return hashlib.md5(sample.tobytes()).hexdigest()[:16]

    def _get_contrast_key(self, image_hash: str, low_percentile: float,
                          high_percentile: float, mask_hash: str) -> str:
        """Generate cache key for contrast calculation."""
        return f"contrast_{image_hash}_{low_percentile}_{high_percentile}_{mask_hash}"

    def get_contrast(self, image: np.ndarray, low_percentile: float,
                     high_percentile: float, detector_mask: Optional[np.ndarray] = None) -> Optional[
        Tuple[float, float]]:
        """Get cached contrast levels if available."""
        image_hash = self._hash_array(image)
        mask_hash = self._hash_array(detector_mask)
        key = self._get_contrast_key(image_hash, low_percentile, high_percentile, mask_hash)

        result = self.contrast_cache.get(key)
        if result is not None:
            self.contrast_hits += 1
            logger.debug(f"PerformanceCache: Contrast cache hit for key {key[:20]}...")
            return result
        else:
            self.contrast_misses += 1
            return None

    def cache_contrast(self, image: np.ndarray, low_percentile: float,
                       high_percentile: float, detector_mask: Optional[np.ndarray],
                       vmin: float, vmax: float) -> None:
        """Cache contrast levels."""
        image_hash = self._hash_array(image)
        mask_hash = self._hash_array(detector_mask)
        key = self._get_contrast_key(image_hash, low_percentile, high_percentile, mask_hash)

        self.contrast_cache.put(key, (float(vmin), float(vmax)))
        logger.debug(f"PerformanceCache: Cached contrast for key {key[:20]}...")

    def _get_mask_key(self, image_shape: Tuple[int, int], params_hash: str) -> str:
        """Generate cache key for detector mask."""
        return f"mask_{image_shape[0]}x{image_shape[1]}_{params_hash}"

    def get_detector_mask(self, image_shape: Tuple[int, int],
                          params: Dict[str, Any]) -> Optional[np.ndarray]:
        """Get cached detector mask if available."""
        # Create hash from relevant parameters
        mask_params = {
            'beam_x': params.get('beam_x'),
            'beam_y': params.get('beam_y'),
            'wavelength': params.get('wavelength'),
            'det_dist': params.get('det_dist'),
            'pixel_size': params.get('pixel_size'),
        }
        params_str = str(sorted(mask_params.items()))
        params_hash = hashlib.md5(params_str.encode()).hexdigest()[:16]

        key = self._get_mask_key(image_shape, params_hash)
        result = self.mask_cache.get(key)

        if result is not None:
            self.mask_hits += 1
            logger.debug(f"PerformanceCache: Mask cache hit for key {key[:20]}...")
            return result
        else:
            self.mask_misses += 1
            return None

    def cache_detector_mask(self, image_shape: Tuple[int, int],
                            params: Dict[str, Any], mask: np.ndarray) -> None:
        """Cache detector mask."""
        mask_params = {
            'beam_x': params.get('beam_x'),
            'beam_y': params.get('beam_y'),
            'wavelength': params.get('wavelength'),
            'det_dist': params.get('det_dist'),
            'pixel_size': params.get('pixel_size'),
        }
        params_str = str(sorted(mask_params.items()))
        params_hash = hashlib.md5(params_str.encode()).hexdigest()[:16]

        key = self._get_mask_key(image_shape, params_hash)
        self.mask_cache.put(key, mask.copy())
        logger.debug(f"PerformanceCache: Cached mask for key {key[:20]}...")

    def _get_radial_key(self, image_hash: str, center: Tuple[float, float],
                        max_radius: int, mask_hash: str) -> str:
        """Generate cache key for radial sum calculation."""
        return f"radial_{image_hash}_{center[0]:.1f}_{center[1]:.1f}_{max_radius}_{mask_hash}"

    def get_radial_sum(self, image: np.ndarray, center: Tuple[float, float],
                       max_radius: int, detector_mask: Optional[np.ndarray] = None) -> Optional[Dict[str, np.ndarray]]:
        """Get cached radial sum if available."""
        image_hash = self._hash_array(image)
        mask_hash = self._hash_array(detector_mask)
        key = self._get_radial_key(image_hash, center, max_radius, mask_hash)

        result = self.radial_cache.get(key)
        if result is not None:
            self.radial_hits += 1
            logger.debug(f"PerformanceCache: Radial cache hit for key {key[:20]}...")
            return result
        else:
            self.radial_misses += 1
            return None

    def cache_radial_sum(self, image: np.ndarray, center: Tuple[float, float],
                         max_radius: int, detector_mask: Optional[np.ndarray],
                         result: Dict[str, np.ndarray]) -> None:
        """Cache radial sum result."""
        image_hash = self._hash_array(image)
        mask_hash = self._hash_array(detector_mask)
        key = self._get_radial_key(image_hash, center, max_radius, mask_hash)

        # Deep copy arrays to avoid reference issues
        cached_result = {
            k: v.copy() if isinstance(v, np.ndarray) else v
            for k, v in result.items()
        }
        self.radial_cache.put(key, cached_result)
        logger.debug(f"PerformanceCache: Cached radial sum for key {key[:20]}...")

    def clear_all(self) -> None:
        """Clear all caches."""
        self.contrast_cache.clear()
        self.mask_cache.clear()
        self.radial_cache.clear()

        # Reset statistics
        self.contrast_hits = self.contrast_misses = 0
        self.mask_hits = self.mask_misses = 0
        self.radial_hits = self.radial_misses = 0

        logger.info("PerformanceCache: All caches cleared")

    def get_statistics(self) -> Dict[str, Any]:
        """Get cache performance statistics."""

        def hit_rate(hits: int, misses: int) -> float:
            total = hits + misses
            return (hits / total * 100) if total > 0 else 0.0

        return {
            "contrast": {
                "hits": self.contrast_hits,
                "misses": self.contrast_misses,
                "hit_rate": hit_rate(self.contrast_hits, self.contrast_misses),
                "cache_size": self.contrast_cache.size()
            },
            "mask": {
                "hits": self.mask_hits,
                "misses": self.mask_misses,
                "hit_rate": hit_rate(self.mask_hits, self.mask_misses),
                "cache_size": self.mask_cache.size()
            },
            "radial": {
                "hits": self.radial_hits,
                "misses": self.radial_misses,
                "hit_rate": hit_rate(self.radial_hits, self.radial_misses),
                "cache_size": self.radial_cache.size()
            }
        }


# Global cache instance
_global_cache = None


def get_performance_cache() -> PerformanceCache:
    """Get the global performance cache instance."""
    global _global_cache
    if _global_cache is None:
        _global_cache = PerformanceCache()
    return _global_cache
