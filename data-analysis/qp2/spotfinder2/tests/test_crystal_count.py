"""Tests for the crystal count estimator.

Generates synthetic spot patterns from 1, 2, and 3 mock crystal orientations
and verifies estimates.
"""

import numpy as np
import pytest

from qp2.spotfinder2.crystal_count import (
    estimate_n_crystals,
    _azimuthal_uniformity,
    _count_based_estimate,
)
from qp2.spotfinder2.spot_list import SpotList


class MockGeometry:
    """Minimal geometry mock for testing."""

    def __init__(self, beam_x=500.0, beam_y=500.0):
        self.beam_x = beam_x
        self.beam_y = beam_y


def _make_crystal_spots(
    n_spots_per_crystal,
    n_crystals,
    beam_x=500.0,
    beam_y=500.0,
    radius_range=(100.0, 300.0),
    noise_sigma_psi=0.02,
    noise_sigma_r=5.0,
    seed=42,
):
    """Generate synthetic spots from N crystals at random orientations."""
    rng = np.random.RandomState(seed)

    all_x = []
    all_y = []
    all_res = []

    for c in range(n_crystals):
        psi_offset = rng.uniform(0, 2 * np.pi)
        for shell in range(4):
            r = radius_range[0] + (radius_range[1] - radius_range[0]) * (shell + 0.5) / 4
            n_per_shell = n_spots_per_crystal // 4
            if n_per_shell < 3:
                n_per_shell = 3
            base_angles = np.linspace(0, 2 * np.pi, n_per_shell, endpoint=False)
            angles = base_angles + psi_offset + rng.normal(0, noise_sigma_psi, n_per_shell)
            radii = r + rng.normal(0, noise_sigma_r, n_per_shell)
            x = beam_x + radii * np.cos(angles)
            y = beam_y + radii * np.sin(angles)
            d_spacing = 1000.0 / np.maximum(radii, 1.0)
            all_x.extend(x)
            all_y.extend(y)
            all_res.extend(d_spacing)

    n_total = len(all_x)
    spots = SpotList.from_arrays(
        x=np.array(all_x, dtype=np.float32),
        y=np.array(all_y, dtype=np.float32),
        intensity=rng.uniform(100, 1000, n_total).astype(np.float32),
        background=np.full(n_total, 10.0, dtype=np.float32),
        snr=rng.uniform(3, 30, n_total).astype(np.float32),
        resolution=np.array(all_res, dtype=np.float32),
        size=np.full(n_total, 5, dtype=np.int32),
    )
    return spots


class TestCountBasedEstimate:

    def test_zero_spots(self):
        assert _count_based_estimate(0, 50) == 1

    def test_single_crystal(self):
        assert _count_based_estimate(50, 50) == 1

    def test_two_crystals(self):
        assert _count_based_estimate(100, 50) == 2

    def test_three_crystals(self):
        assert _count_based_estimate(150, 50) == 3


class TestAzimuthalUniformity:

    def test_clustered_low_uniformity(self):
        """Spots in a small azimuthal range → low uniformity."""
        psi = np.linspace(0, np.pi / 4, 20)
        u = _azimuthal_uniformity(psi, n_bins=36)
        assert u < 0.5, f"Expected low uniformity, got {u}"

    def test_uniform_high_uniformity(self):
        """Uniformly distributed spots → high uniformity."""
        psi = np.linspace(0, 2 * np.pi, 100, endpoint=False)
        u = _azimuthal_uniformity(psi, n_bins=36)
        assert u > 0.7, f"Expected high uniformity, got {u}"

    def test_more_crystals_more_uniform(self):
        """Adding crystals should increase uniformity."""
        rng = np.random.RandomState(42)
        psi_1 = np.linspace(0, 2 * np.pi, 15, endpoint=False) + rng.normal(0, 0.02, 15)
        u1 = _azimuthal_uniformity(psi_1, n_bins=36)

        # 3 crystals at different offsets
        psi_3 = np.concatenate([
            np.linspace(0, 2 * np.pi, 15, endpoint=False) + rng.normal(0, 0.02, 15),
            np.linspace(0, 2 * np.pi, 15, endpoint=False) + 1.1 + rng.normal(0, 0.02, 15),
            np.linspace(0, 2 * np.pi, 15, endpoint=False) + 2.3 + rng.normal(0, 0.02, 15),
        ]) % (2 * np.pi)
        u3 = _azimuthal_uniformity(psi_3, n_bins=36)

        assert u3 > u1, f"3 crystals ({u3:.3f}) should be more uniform than 1 ({u1:.3f})"


class TestEstimateNCrystals:

    def test_empty_spots(self):
        spots = SpotList()
        geometry = MockGeometry()
        n, conf, details = estimate_n_crystals(spots, geometry)
        assert n == 1
        assert conf == 0.0

    def test_single_crystal(self):
        spots = _make_crystal_spots(n_spots_per_crystal=60, n_crystals=1, seed=42)
        geometry = MockGeometry()
        n, conf, details = estimate_n_crystals(
            spots, geometry, expected_spots_per_crystal=60,
        )
        assert n == 1, f"Expected 1, got {n}"

    def test_two_crystals(self):
        spots = _make_crystal_spots(n_spots_per_crystal=60, n_crystals=2, seed=42)
        geometry = MockGeometry()
        n, conf, details = estimate_n_crystals(
            spots, geometry, expected_spots_per_crystal=60,
        )
        assert 1 <= n <= 3, f"Expected ~2, got {n}"

    def test_three_crystals(self):
        spots = _make_crystal_spots(n_spots_per_crystal=60, n_crystals=3, seed=42)
        geometry = MockGeometry()
        n, conf, details = estimate_n_crystals(
            spots, geometry, expected_spots_per_crystal=60,
        )
        assert 2 <= n <= 4, f"Expected ~3, got {n}"

    def test_returns_metadata_fields(self):
        spots = _make_crystal_spots(n_spots_per_crystal=60, n_crystals=1, seed=42)
        geometry = MockGeometry()
        n, conf, details = estimate_n_crystals(spots, geometry)
        assert "n_spots" in details
        assert "method" in details
        assert "count_based_estimate" in details

    def test_max_crystals_cap(self):
        spots = _make_crystal_spots(n_spots_per_crystal=60, n_crystals=5, seed=42)
        geometry = MockGeometry()
        n, conf, details = estimate_n_crystals(
            spots, geometry, max_crystals=3, expected_spots_per_crystal=60,
        )
        assert n <= 3


class TestSpotListMetadata:

    def test_empty_metadata_by_default(self):
        sl = SpotList()
        assert sl.metadata == {}

    def test_metadata_preserved_on_filter(self):
        spots = _make_crystal_spots(n_spots_per_crystal=20, n_crystals=1, seed=42)
        spots.metadata["n_crystals"] = 1
        mask = np.ones(spots.count, dtype=bool)
        filtered = spots.filter(mask)
        assert filtered.metadata["n_crystals"] == 1

    def test_metadata_in_dict_roundtrip(self):
        spots = _make_crystal_spots(n_spots_per_crystal=20, n_crystals=1, seed=42)
        spots.metadata["n_crystals"] = 2
        d = spots.to_dict()
        restored = SpotList.from_dict(d)
        assert restored.metadata["n_crystals"] == 2

    def test_metadata_in_json_roundtrip(self):
        spots = _make_crystal_spots(n_spots_per_crystal=20, n_crystals=1, seed=42)
        spots.metadata["n_crystals"] = 3
        spots.metadata["n_crystals_confidence"] = 0.85
        json_str = spots.to_json()
        restored = SpotList.from_json(json_str)
        assert restored.metadata["n_crystals"] == 3
        assert restored.metadata["n_crystals_confidence"] == 0.85
