"""Tests for the protein diffraction heuristic classifier.

Uses synthetic SpotList data to verify that the classifier correctly
distinguishes protein-like diffraction patterns from salt/small-molecule
contaminants. No external dependencies (no HDF5, no Qt, no GPU).
"""

import numpy as np
import pytest

from qp2.spotfinder2.filtering import ProteinDiffractionClassifier
from qp2.spotfinder2.spot_list import SpotList


def _make_spots(d_spacings, intensities=None, n=None, seed=42):
    """Helper to build a SpotList from d-spacing values."""
    rng = np.random.RandomState(seed)
    if n is None:
        n = len(d_spacings)
    d_arr = np.array(d_spacings, dtype=np.float32)
    if intensities is None:
        intensities = rng.uniform(100, 500, n).astype(np.float32)
    else:
        intensities = np.array(intensities, dtype=np.float32)
    return SpotList.from_arrays(
        x=rng.uniform(100, 900, n).astype(np.float32),
        y=rng.uniform(100, 900, n).astype(np.float32),
        intensity=intensities,
        background=np.full(n, 10.0, dtype=np.float32),
        snr=rng.uniform(3, 30, n).astype(np.float32),
        resolution=d_arr,
        size=np.full(n, 5, dtype=np.int32),
    )


def _protein_like_dspacings(n=50, dmin=4.0, dmax=15.0, seed=42):
    """Generate d-spacings spread across a wide resolution range (protein-like)."""
    rng = np.random.RandomState(seed)
    return rng.uniform(dmin, dmax, n).astype(np.float32)


def _salt_like_dspacings():
    """Generate d-spacings clustered at NaCl positions."""
    # NaCl d-spacings: 2.821, 1.994, 1.628, 1.410, 1.261
    rng = np.random.RandomState(42)
    d_values = []
    for d in [2.821, 1.994, 1.628]:
        d_values.extend(d + rng.normal(0, 0.01, 3))
    return np.array(d_values, dtype=np.float32)


class TestProteinDiffractionClassifier:

    def setup_method(self):
        self.classifier = ProteinDiffractionClassifier(
            min_cell_A=20.0,
            check_dmin_A=4.0,
            check_dmax_A=15.0,
            n_resolution_bins=20,
            score_threshold=0.5,
        )

    def test_protein_like_spots(self):
        """Spots uniformly spread across 15-4 A should classify as protein."""
        d = _protein_like_dspacings(n=50)
        intensities = np.random.RandomState(42).uniform(100, 500, 50).astype(np.float32)
        spots = _make_spots(d, intensities=intensities)
        result = self.classifier.classify(spots)
        assert result["is_likely_protein"] is True
        assert result["protein_score"] > 0.6

    def test_salt_like_spots(self):
        """Spots at NaCl d-spacings with extreme intensities → non-protein."""
        d = _salt_like_dspacings()
        # Salt: very bright spots
        intensities = np.full(len(d), 50000.0, dtype=np.float32)
        spots = _make_spots(d, intensities=intensities, n=len(d))
        result = self.classifier.classify(spots)
        assert result["is_likely_protein"] is False
        assert result["protein_score"] < 0.4

    def test_few_spots_few_dspacings(self):
        """Spots all outside the check shell → non-protein.

        When no spots fall in the 15-4 A check shell, all shell-based
        heuristics (entropy, spot count) return 0, and the composite
        score falls well below threshold.
        """
        # All spots at d < 4 A (below check shell) — typical of salt
        d = np.array([2.8, 2.82, 2.0, 1.99, 1.63, 1.62], dtype=np.float32)
        intensities = np.full(6, 10000.0, dtype=np.float32)
        spots = _make_spots(d, intensities=intensities, n=6)
        result = self.classifier.classify(spots)
        assert result["is_likely_protein"] is False
        assert result["n_spots_in_shell"] == 0

    def test_empty_spots(self):
        """Empty SpotList → non-protein with score 0."""
        spots = SpotList()
        result = self.classifier.classify(spots)
        assert result["is_likely_protein"] is False
        assert result["protein_score"] == 0.0
        assert result["n_spots_in_shell"] == 0
        assert result["n_occupied_bins"] == 0

    def test_result_keys(self):
        """Verify all expected keys are present in the result dict."""
        spots = _make_spots(_protein_like_dspacings(n=20))
        result = self.classifier.classify(spots)
        assert "protein_score" in result
        assert "is_likely_protein" in result
        assert "n_spots_in_shell" in result
        assert "n_occupied_bins" in result
        assert "heuristics" in result
        h = result["heuristics"]
        assert "resolution_entropy" in h
        assert "spot_count_score" in h
        assert "intensity_distribution_score" in h
        assert "salt_ring_score" in h

    def test_score_bounded(self):
        """Score must always be in [0.0, 1.0]."""
        for n in [0, 1, 5, 50, 200]:
            if n == 0:
                spots = SpotList()
            else:
                rng = np.random.RandomState(n)
                d = rng.uniform(2.0, 20.0, n).astype(np.float32)
                spots = _make_spots(d, n=n, seed=n)
            result = self.classifier.classify(spots)
            assert 0.0 <= result["protein_score"] <= 1.0
            for v in result["heuristics"].values():
                assert 0.0 <= v <= 1.0


class TestResolutionEntropy:

    def setup_method(self):
        self.classifier = ProteinDiffractionClassifier()

    def test_high_entropy(self):
        """Spots spread across many d-spacing bins → high entropy score."""
        d = np.linspace(4.5, 14.5, 40, dtype=np.float32)
        spots = _make_spots(d, n=40)
        result = self.classifier.classify(spots)
        assert result["heuristics"]["resolution_entropy"] > 0.7

    def test_low_entropy(self):
        """Spots concentrated in 1-2 bins → low entropy score."""
        d = np.array([7.0] * 10 + [7.05] * 10, dtype=np.float32)
        spots = _make_spots(d, n=20)
        result = self.classifier.classify(spots)
        assert result["heuristics"]["resolution_entropy"] < 0.3


class TestIntensityDistribution:

    def setup_method(self):
        self.classifier = ProteinDiffractionClassifier()

    def test_protein_like_intensity(self):
        """Moderate max/median ratio → high intensity score."""
        d = _protein_like_dspacings(n=30)
        # Protein-like: fairly uniform intensities (max/median ~ 3-5)
        rng = np.random.RandomState(42)
        intensities = rng.uniform(200, 600, 30).astype(np.float32)
        spots = _make_spots(d, intensities=intensities, n=30)
        result = self.classifier.classify(spots)
        assert result["heuristics"]["intensity_distribution_score"] > 0.7

    def test_salt_like_intensity(self):
        """Extreme max/median ratio → low intensity score."""
        d = _protein_like_dspacings(n=30)
        # Salt-like: most spots moderate, one or two extremely bright
        intensities = np.full(30, 100.0, dtype=np.float32)
        intensities[0] = 100000.0  # one extreme outlier
        spots = _make_spots(d, intensities=intensities, n=30)
        result = self.classifier.classify(spots)
        assert result["heuristics"]["intensity_distribution_score"] < 0.3


class TestSaltRingScore:

    def setup_method(self):
        self.classifier = ProteinDiffractionClassifier()

    def test_nacl_match(self):
        """Spots at NaCl d-spacings → low salt score."""
        # Place enough spots at NaCl positions to trigger
        rng = np.random.RandomState(42)
        nacl_d = []
        for d in [2.821, 1.994, 1.628]:
            nacl_d.extend(d + rng.normal(0, 0.01, 4))
        # Also add some protein-range spots so the overall list has variety
        d = np.array(nacl_d + [6.0, 7.0, 8.0], dtype=np.float32)
        spots = _make_spots(d, n=len(d))
        result = self.classifier.classify(spots)
        assert result["heuristics"]["salt_ring_score"] < 0.5

    def test_no_salt_match(self):
        """Spots at non-salt d-spacings → salt score = 1.0."""
        d = _protein_like_dspacings(n=30)
        spots = _make_spots(d, n=30)
        result = self.classifier.classify(spots)
        assert result["heuristics"]["salt_ring_score"] == 1.0


class TestSpotCountScore:

    def setup_method(self):
        self.classifier = ProteinDiffractionClassifier()

    def test_many_spots_in_shell(self):
        """>= 3 spots in check shell → full spot count score."""
        d = _protein_like_dspacings(n=20)
        spots = _make_spots(d, n=20)
        result = self.classifier.classify(spots)
        assert result["heuristics"]["spot_count_score"] == 1.0

    def test_zero_spots_in_shell(self):
        """All spots outside check shell → zero spot count score."""
        # All spots at d < 4 A (below check shell)
        d = np.array([2.0, 2.5, 3.0, 3.5], dtype=np.float32)
        spots = _make_spots(d, n=4)
        result = self.classifier.classify(spots)
        assert result["heuristics"]["spot_count_score"] == 0.0
        assert result["n_spots_in_shell"] == 0

    def test_one_spot_in_shell(self):
        """1 spot in check shell → partial score (1/3)."""
        d = np.array([6.0, 2.0, 2.5, 3.0], dtype=np.float32)
        spots = _make_spots(d, n=4)
        result = self.classifier.classify(spots)
        score = result["heuristics"]["spot_count_score"]
        assert 0.3 <= score <= 0.4  # 1/3 ≈ 0.333


class TestCellDimensionEstimation:

    def setup_method(self):
        self.classifier = ProteinDiffractionClassifier()

    def _make_crystal_spots(self, cell_a, n_orders=5, n_spots_per_order=3, seed=42):
        """Generate spots at d = cell_a/h for h = 1..n_orders.

        Simulates reflections from a single axis of length cell_a.
        Multiple spots per order simulate different (h,k,l) with same d.
        """
        rng = np.random.RandomState(seed)
        d_values = []
        for h in range(1, n_orders + 1):
            d = cell_a / h
            if d > 1.0:  # within typical detector range
                for _ in range(n_spots_per_order):
                    d_values.append(d + rng.normal(0, 0.02))
        d_arr = np.array(d_values, dtype=np.float32)
        return _make_spots(d_arr, n=len(d_arr), seed=seed)

    def test_result_keys(self):
        """Cell estimate dict has all expected keys."""
        spots = self._make_crystal_spots(cell_a=50.0)
        result = self.classifier.classify(spots)
        cell = result["cell_estimate"]
        assert "min_cell_A" in cell
        assert "max_cell_A" in cell
        assert "candidates" in cell

    def test_candidate_structure(self):
        """Each candidate has the expected fields."""
        spots = self._make_crystal_spots(cell_a=50.0, n_orders=8,
                                          n_spots_per_order=5)
        cell = self.classifier.estimate_cell_dimensions(spots)
        for cand in cell["candidates"]:
            assert "rank" in cand
            assert "cell_A" in cand
            assert "confidence" in cand
            assert "q_spacing" in cand
            assert "n_harmonics" in cand
            assert isinstance(cand["rank"], int)
            assert isinstance(cand["cell_A"], float)
            assert 0.0 <= cand["confidence"] <= 1.0

    def test_candidates_sorted_by_confidence(self):
        """Candidates should be sorted by confidence, highest first."""
        spots = self._make_crystal_spots(cell_a=50.0, n_orders=8,
                                          n_spots_per_order=5)
        cell = self.classifier.estimate_cell_dimensions(spots)
        if len(cell["candidates"]) >= 2:
            confs = [c["confidence"] for c in cell["candidates"]]
            assert confs == sorted(confs, reverse=True)

    def test_candidates_ranked_sequentially(self):
        """Candidate ranks should be 1, 2, 3, ..."""
        spots = self._make_crystal_spots(cell_a=50.0, n_orders=8,
                                          n_spots_per_order=5)
        cell = self.classifier.estimate_cell_dimensions(spots)
        for i, cand in enumerate(cell["candidates"]):
            assert cand["rank"] == i + 1

    def test_cell_estimate_from_classify(self):
        """classify() should include cell_estimate in its result."""
        spots = self._make_crystal_spots(cell_a=50.0)
        result = self.classifier.classify(spots)
        assert "cell_estimate" in result
        cell = result["cell_estimate"]
        assert isinstance(cell["min_cell_A"], float)
        assert isinstance(cell["max_cell_A"], float)
        assert isinstance(cell["candidates"], list)

    def test_known_cell_50A(self):
        """Spots from a 50 A cell should produce a candidate near 50 A."""
        spots = self._make_crystal_spots(cell_a=50.0, n_orders=8,
                                          n_spots_per_order=5, seed=42)
        cell = self.classifier.estimate_cell_dimensions(spots)
        assert len(cell["candidates"]) > 0
        # At least one candidate should be close to 50 A
        all_dims = [c["cell_A"] for c in cell["candidates"]]
        reasonable = [c for c in all_dims if 30.0 <= c <= 80.0]
        assert len(reasonable) > 0, (
            f"Expected a cell dimension near 50 A, got dims={all_dims}"
        )

    def test_known_cell_nacl(self):
        """Spots from NaCl (a=5.64 A) should produce a small cell estimate."""
        # NaCl reflections: d = 5.64/sqrt(h^2+k^2+l^2)
        # (200): d = 2.82 A, (220): d = 1.994 A, (222): d = 1.628 A
        nacl_d = [2.821, 2.821, 2.82, 1.994, 1.994, 1.995,
                  1.628, 1.628, 1.629, 1.410, 1.410, 1.411]
        spots = _make_spots(np.array(nacl_d, dtype=np.float32), n=len(nacl_d))
        cell = self.classifier.estimate_cell_dimensions(spots)
        # NaCl has a very small cell — any detected dimension should be small
        if cell["max_cell_A"] > 0:
            assert cell["max_cell_A"] < 20.0, (
                f"NaCl cell estimate too large: {cell['max_cell_A']:.1f} A"
            )

    def test_harmonic_deduplication(self):
        """Harmonics (e.g. 50 A and 25 A) should be merged into one candidate."""
        # Create spots with strong periodicity at q = 1/50 = 0.02 A^-1
        # Reflections at d = 50, 25, 16.67, 12.5, 10, 8.33 A
        spots = self._make_crystal_spots(cell_a=50.0, n_orders=8,
                                          n_spots_per_order=6, seed=42)
        cell = self.classifier.estimate_cell_dimensions(spots)
        # The fundamental (50 A) should absorb its harmonics
        if len(cell["candidates"]) > 0:
            top = cell["candidates"][0]
            # Top candidate should have harmonics absorbed
            # (exact count depends on which peaks are detected)
            assert top["n_harmonics"] >= 0
            # Should not have both 50 A and 25 A as separate top candidates
            top_dims = [c["cell_A"] for c in cell["candidates"][:3]]
            # Check no pair is a 2:1 ratio
            for i in range(len(top_dims)):
                for j in range(i + 1, len(top_dims)):
                    ratio = max(top_dims[i], top_dims[j]) / min(top_dims[i], top_dims[j])
                    nearest = round(ratio)
                    if 2 <= nearest <= 4:
                        assert abs(ratio - nearest) / nearest > 0.10, (
                            f"Harmonic pair not deduplicated: "
                            f"{top_dims[i]:.1f} and {top_dims[j]:.1f} A "
                            f"(ratio={ratio:.2f})"
                        )

    def test_empty_spots(self):
        """Empty spot list → zero cell estimates."""
        spots = SpotList()
        cell = self.classifier.estimate_cell_dimensions(spots)
        assert cell["min_cell_A"] == 0.0
        assert cell["max_cell_A"] == 0.0
        assert cell["candidates"] == []

    def test_too_few_spots(self):
        """Fewer than 4 spots → zero cell estimates (insufficient data)."""
        d = np.array([5.0, 8.0, 10.0], dtype=np.float32)
        spots = _make_spots(d, n=3)
        cell = self.classifier.estimate_cell_dimensions(spots)
        assert cell["min_cell_A"] == 0.0
        assert cell["max_cell_A"] == 0.0
        assert cell["candidates"] == []

    def test_min_leq_max(self):
        """min_cell should always be <= max_cell."""
        spots = self._make_crystal_spots(cell_a=40.0, n_orders=6,
                                          n_spots_per_order=4, seed=99)
        cell = self.classifier.estimate_cell_dimensions(spots)
        if cell["min_cell_A"] > 0:
            assert cell["min_cell_A"] <= cell["max_cell_A"]

    def test_cell_candidates_metadata_roundtrip(self):
        """Cell candidate metadata should survive JSON roundtrip."""
        spots = self._make_crystal_spots(cell_a=50.0)
        result = self.classifier.classify(spots)
        cell = result["cell_estimate"]
        spots.metadata["estimated_min_cell_A"] = cell["min_cell_A"]
        spots.metadata["estimated_max_cell_A"] = cell["max_cell_A"]
        spots.metadata["cell_candidates"] = cell["candidates"]

        json_str = spots.to_json()
        restored = SpotList.from_json(json_str)
        assert restored.metadata["estimated_min_cell_A"] == cell["min_cell_A"]
        assert restored.metadata["estimated_max_cell_A"] == cell["max_cell_A"]
        assert len(restored.metadata["cell_candidates"]) == len(cell["candidates"])
        if cell["candidates"]:
            assert restored.metadata["cell_candidates"][0]["cell_A"] == cell["candidates"][0]["cell_A"]

    def test_classify_empty_has_cell_estimate(self):
        """classify() on empty SpotList should still include cell_estimate."""
        spots = SpotList()
        result = self.classifier.classify(spots)
        assert "cell_estimate" in result
        assert result["cell_estimate"]["candidates"] == []
        assert result["cell_estimate"]["min_cell_A"] == 0.0


class TestMetadataRoundtrip:

    def test_protein_metadata_survives_json(self):
        """Protein classification metadata should survive to_json/from_json."""
        classifier = ProteinDiffractionClassifier()
        d = _protein_like_dspacings(n=30)
        spots = _make_spots(d, n=30)
        result = classifier.classify(spots)
        spots.metadata["protein_score"] = result["protein_score"]
        spots.metadata["is_likely_protein"] = result["is_likely_protein"]
        spots.metadata["protein_heuristics"] = result["heuristics"]

        json_str = spots.to_json()
        restored = SpotList.from_json(json_str)
        assert restored.metadata["protein_score"] == result["protein_score"]
        assert restored.metadata["is_likely_protein"] == result["is_likely_protein"]
        assert restored.metadata["protein_heuristics"] == result["heuristics"]

    def test_protein_metadata_survives_dict(self):
        """Protein classification metadata should survive to_dict/from_dict."""
        classifier = ProteinDiffractionClassifier()
        d = _protein_like_dspacings(n=30)
        spots = _make_spots(d, n=30)
        result = classifier.classify(spots)
        spots.metadata["protein_score"] = result["protein_score"]
        spots.metadata["is_likely_protein"] = result["is_likely_protein"]

        d_dict = spots.to_dict()
        restored = SpotList.from_dict(d_dict)
        assert restored.metadata["protein_score"] == result["protein_score"]
        assert restored.metadata["is_likely_protein"] == result["is_likely_protein"]

    def test_protein_metadata_survives_filter(self):
        """Protein classification metadata should survive SpotList.filter()."""
        classifier = ProteinDiffractionClassifier()
        d = _protein_like_dspacings(n=30)
        spots = _make_spots(d, n=30)
        result = classifier.classify(spots)
        spots.metadata["protein_score"] = result["protein_score"]
        spots.metadata["is_likely_protein"] = result["is_likely_protein"]

        mask = np.ones(spots.count, dtype=bool)
        mask[0] = False  # remove one spot
        filtered = spots.filter(mask)
        assert filtered.metadata["protein_score"] == result["protein_score"]
        assert filtered.metadata["is_likely_protein"] == result["is_likely_protein"]
