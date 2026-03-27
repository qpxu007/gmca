"""Post-detection filters: shape analysis and ice spot removal.

Removes non-Bragg features (diffuse streaks) and ice crystal spots
from the detected spot list.
"""

import numpy as np
from typing import Optional

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def filter_by_shape(labels, frame, max_aspect_ratio=3.0, min_pixels=2, max_pixels=200):
    """PCA-based shape analysis on connected components.

    Rejects elongated features (diffuse streaks) based on eigenvalue ratio.
    Uses scipy.ndimage.find_objects for fast bounding-box access instead of
    scanning the entire image per component.

    Args:
        labels: 2D integer label array from CCL
        frame: original detector image
        max_aspect_ratio: reject components with aspect_ratio > this
        min_pixels: reject components smaller than this
        max_pixels: reject components larger than this

    Returns:
        filtered_labels: label array with rejected components zeroed
        n_remaining: number of surviving components
        aspect_ratios: dict mapping component_id -> aspect_ratio
    """
    from scipy import ndimage as ndi

    if labels.max() == 0:
        return labels, 0, {}

    n_components = labels.max()

    # Pre-filter by size using bincount (vectorized, no loops)
    sizes = np.bincount(labels.ravel())
    keep = np.zeros(len(sizes), dtype=bool)
    keep[1:n_components + 1] = (
        (sizes[1:n_components + 1] >= min_pixels) &
        (sizes[1:n_components + 1] <= max_pixels)
    )

    # Get bounding boxes for all components at once
    slices = ndi.find_objects(labels)
    aspect_ratios = {}

    for comp_id in range(1, n_components + 1):
        if not keep[comp_id]:
            aspect_ratios[comp_id] = 0.0
            continue

        sl = slices[comp_id - 1]
        if sl is None:
            keep[comp_id] = False
            aspect_ratios[comp_id] = 0.0
            continue

        n_pix = sizes[comp_id]

        if n_pix < 3:
            aspect_ratios[comp_id] = 1.0
            continue

        # Extract pixels within bounding box only (fast)
        comp_mask = labels[sl] == comp_id
        pixels = np.argwhere(comp_mask)

        # PCA: eigenvalues of 2x2 covariance matrix
        cov = np.cov(pixels.T)
        eigenvalues = np.linalg.eigvalsh(cov)
        eigenvalues = np.maximum(eigenvalues, 1e-6)

        aspect = np.sqrt(eigenvalues[-1] / eigenvalues[0])
        aspect_ratios[comp_id] = float(aspect)

        if aspect > max_aspect_ratio:
            keep[comp_id] = False

    # Remap labels
    remap = np.zeros(n_components + 1, dtype=np.int32)
    new_id = 0
    for i in range(1, n_components + 1):
        if keep[i]:
            new_id += 1
            remap[i] = new_id

    n_rejected = n_components - new_id
    if n_rejected > 0:
        logger.debug(f"Shape filter: rejected {n_rejected} elongated components")

    return remap[labels], new_id, aspect_ratios


class IceSpotFilter:
    """Remove spots that index consistently with ice crystal lattices.

    Uses inter-peak distance matching against hexagonal ice (Ih) and
    cubic ice (Ic) unit cells. If >= min_matches spots form a consistent
    ice crystal orientation, they are removed.
    """

    # Ice unit cell parameters
    ICE_IH = {"a": 4.497, "c": 7.322}  # hexagonal, P63/mmc
    ICE_IC = {"a": 6.35}  # cubic, Fd3m

    def __init__(self, geometry, d_min=1.5, tolerance=0.02, min_matches=3):
        """
        Args:
            geometry: DetectorGeometry
            d_min: minimum d-spacing to consider (Angstroms)
            tolerance: distance matching tolerance (Å⁻¹)
            min_matches: minimum number of consistent spots to flag as ice
        """
        self.geometry = geometry
        self.tolerance = tolerance
        self.min_matches = min_matches

        # Generate ice reflection q-vectors
        self.ice_q_ih = self._generate_ice_q(self.ICE_IH, d_min, hexagonal=True)
        self.ice_q_ic = self._generate_ice_q(self.ICE_IC, d_min, hexagonal=False)

        # Precompute all pairwise distances for each ice form
        self.ice_dists_ih = self._pairwise_distances(self.ice_q_ih) if len(self.ice_q_ih) > 1 else np.array([])
        self.ice_dists_ic = self._pairwise_distances(self.ice_q_ic) if len(self.ice_q_ic) > 1 else np.array([])

        logger.info(
            f"IceSpotFilter: Ih={len(self.ice_q_ih)} reflections, "
            f"Ic={len(self.ice_q_ic)} reflections, d_min={d_min}Å"
        )

    def _generate_ice_q(self, cell, d_min, hexagonal=False):
        """Generate list of |q| values for ice reflections within resolution range."""
        a = cell["a"]
        q_list = []

        if hexagonal:
            c = cell["c"]
            # Hexagonal: 1/d^2 = (4/3)(h^2 + hk + k^2)/a^2 + l^2/c^2
            h_max = int(np.ceil(a / d_min)) + 1
            l_max = int(np.ceil(c / d_min)) + 1
            for h in range(-h_max, h_max + 1):
                for k in range(-h_max, h_max + 1):
                    for l in range(-l_max, l_max + 1):
                        if h == 0 and k == 0 and l == 0:
                            continue
                        inv_d2 = (4.0 / 3.0) * (h**2 + h * k + k**2) / a**2 + l**2 / c**2
                        if inv_d2 > 0:
                            d = 1.0 / np.sqrt(inv_d2)
                            if d >= d_min:
                                q_list.append(1.0 / d)
        else:
            # Cubic: 1/d^2 = (h^2 + k^2 + l^2)/a^2
            h_max = int(np.ceil(a / d_min)) + 1
            for h in range(-h_max, h_max + 1):
                for k in range(-h_max, h_max + 1):
                    for l in range(-h_max, h_max + 1):
                        if h == 0 and k == 0 and l == 0:
                            continue
                        inv_d2 = (h**2 + k**2 + l**2) / a**2
                        if inv_d2 > 0:
                            d = 1.0 / np.sqrt(inv_d2)
                            if d >= d_min:
                                q_list.append(1.0 / d)

        # Deduplicate (within tolerance)
        if not q_list:
            return np.array([])
        q_arr = np.unique(np.round(q_list, decimals=4))
        return q_arr

    def _pairwise_distances(self, q_values):
        """Compute all pairwise absolute differences."""
        n = len(q_values)
        dists = []
        for i in range(n):
            for j in range(i + 1, n):
                dists.append(abs(q_values[i] - q_values[j]))
        return np.array(dists)

    # Strong ice powder ring d-spacings (Å) — only the prominent ones
    # These are the actual powder ring positions, not individual HKL reflections
    ICE_POWDER_RINGS_D = [3.67, 3.44, 2.67, 2.25, 2.07, 1.95, 1.92, 1.88]

    def filter(self, spots, geometry=None):
        """Auto-detect and remove spots clustered on ice powder ring positions.

        For each of the 8 known strong ice powder ring d-spacings, checks
        whether >= min_matches spots cluster at that d-spacing. Rings with
        enough spots are considered "active" (ice detected) and those spots
        are removed.

        This is spot-level detection: it works even on photon-counting
        detectors where pixel-level ice detection is unreliable because
        most pixels read zero.

        The detected active rings are returned as part of the result so
        the pipeline can report which rings were found.

        Args:
            spots: SpotList
            geometry: DetectorGeometry (uses self.geometry if None)

        Returns:
            (filtered_spots, ice_mask, active_rings) where:
                filtered_spots: SpotList with ice-ring spots removed
                ice_mask: boolean array (True = flagged as ice)
                active_rings: list of d-spacings (A) where ice was detected
        """
        if spots.count == 0:
            return spots, np.array([], dtype=bool), []

        ice_mask = np.zeros(spots.count, dtype=bool)
        spot_d = spots.resolution.copy()
        active_rings = []

        # Check each ice powder ring independently
        n_flagged_total = 0
        for d_ring in self.ICE_POWDER_RINGS_D:
            # Tolerance in d-spacing: ±0.05 Å (narrow band around the ring)
            d_tol = 0.05
            near_ring = (np.abs(spot_d - d_ring) < d_tol) & (spot_d > 0)
            n_near = near_ring.sum()

            # Only flag if multiple spots cluster on THIS ring
            if n_near >= self.min_matches:
                ice_mask |= near_ring
                n_flagged_total += n_near
                active_rings.append(d_ring)
                logger.debug(
                    f"IceSpotFilter: {n_near} spots on {d_ring:.2f}Å ice ring"
                )

        if n_flagged_total > 0:
            logger.info(
                f"IceSpotFilter: {len(active_rings)} active ice rings "
                f"at {[f'{d:.2f}' for d in active_rings]} A, "
                f"flagged {n_flagged_total} spots (out of {spots.count})"
            )
            return spots.filter(~ice_mask), ice_mask, active_rings
        else:
            logger.debug("IceSpotFilter: no active ice rings detected")
            return spots, ice_mask, active_rings


class ProteinDiffractionClassifier:
    """Heuristic classifier to distinguish protein diffraction from non-protein
    contaminants (salt crystals, small molecules, powder rings).

    Uses a composite score from multiple model-free heuristics plus an optional
    known-salt d-spacing lookup. Operates per-frame and stores results in
    SpotList metadata without removing any spots.

    The key physical insight is that protein crystals have large unit cells
    (>= 20 A), producing many reflections spread across a wide range of
    d-spacings. Salt and small-molecule crystals have small unit cells with
    few, often very bright reflections clustered at specific d-spacings.

    The classifier evaluates spots within a configurable resolution shell
    (default 15-4 A), chosen because:
    - It is above most common salt d-spacings (NaCl strongest lines < 3 A)
    - Protein crystals with cell >= 20 A produce reflections here
    - It avoids very low resolution where beamstop scatter causes noise
    """

    # Known salt d-spacings (Angstroms).
    # Ice is NOT included — handled separately by IceSpotFilter upstream.
    KNOWN_SALTS = {
        "NaCl": [2.821, 1.994, 1.628, 1.410, 1.261],
        "KCl": [3.146, 2.224, 1.816, 1.573],
        "CaCO3_calcite": [3.035, 2.495, 2.285, 2.095, 1.913],
    }

    # Heuristic weights for composite score.
    # Model-free heuristics (H1-H3) dominate at 85% total weight.
    # Salt lookup (H4) is a supplementary 15% bonus.
    WEIGHTS = {
        "resolution_entropy": 0.35,
        "spot_count_score": 0.25,
        "intensity_distribution_score": 0.25,
        "salt_ring_score": 0.15,
    }

    def __init__(
        self,
        min_cell_A=20.0,
        check_dmin_A=4.0,
        check_dmax_A=15.0,
        n_resolution_bins=20,
        score_threshold=0.5,
        salt_tolerance_A=0.05,
        min_salt_matches=3,
    ):
        """
        Args:
            min_cell_A: assumed minimum protein unit cell dimension (Angstroms).
            check_dmin_A: inner (high-resolution) edge of the check shell.
            check_dmax_A: outer (low-resolution) edge of the check shell.
            n_resolution_bins: number of d-spacing bins in the check shell.
            score_threshold: composite score below this → flagged as non-protein.
            salt_tolerance_A: d-spacing tolerance for salt ring matching.
            min_salt_matches: minimum spots matching a salt compound to trigger.
        """
        self.min_cell_A = min_cell_A
        self.check_dmin_A = check_dmin_A
        self.check_dmax_A = check_dmax_A
        self.n_resolution_bins = n_resolution_bins
        self.score_threshold = score_threshold
        self.salt_tolerance_A = salt_tolerance_A
        self.min_salt_matches = min_salt_matches

        # Precompute bin edges for the check shell
        self.bin_edges = np.linspace(check_dmin_A, check_dmax_A, n_resolution_bins + 1)

        logger.info(
            f"ProteinDiffractionClassifier: check shell {check_dmax_A}-{check_dmin_A} A, "
            f"min_cell={min_cell_A} A, threshold={score_threshold}"
        )

    def classify(self, spots):
        """Classify a frame's spots as likely protein or non-protein diffraction.

        Args:
            spots: SpotList (should be post-ice-filter)

        Returns:
            dict with keys:
                protein_score: float 0.0-1.0 (higher = more likely protein)
                is_likely_protein: bool (score >= threshold)
                n_spots_in_shell: int
                n_occupied_bins: int
                heuristics: dict of individual sub-scores
        """
        if spots.count == 0:
            return {
                "protein_score": 0.0,
                "is_likely_protein": False,
                "n_spots_in_shell": 0,
                "n_occupied_bins": 0,
                "heuristics": {k: 0.0 for k in self.WEIGHTS},
                "cell_estimate": {
                    "candidates": [],
                    "min_cell_A": 0.0,
                    "max_cell_A": 0.0,
                },
            }

        # Select spots within the check shell
        d = spots.resolution
        in_shell = (d >= self.check_dmin_A) & (d <= self.check_dmax_A) & (d > 0)
        d_shell = d[in_shell]
        n_in_shell = len(d_shell)

        # Compute occupied bins for diagnostics
        if n_in_shell > 0:
            bin_indices = np.digitize(d_shell, self.bin_edges) - 1
            bin_indices = np.clip(bin_indices, 0, self.n_resolution_bins - 1)
            n_occupied = len(np.unique(bin_indices))
        else:
            n_occupied = 0

        # Also get intensities within the shell for H3
        intensities_shell = spots.intensity[in_shell] if n_in_shell > 0 else np.array([])

        # Estimate cell dimensions from q-space periodicity
        cell_estimate = self.estimate_cell_dimensions(spots)

        # Run all four heuristics
        heuristics = {
            "resolution_entropy": self._resolution_entropy(d_shell),
            "spot_count_score": self._spot_count_score(n_in_shell),
            "intensity_distribution_score": self._intensity_distribution_score(
                intensities_shell
            ),
            "salt_ring_score": self._salt_ring_score(d),
        }

        # Composite weighted score
        protein_score = sum(
            self.WEIGHTS[k] * heuristics[k] for k in self.WEIGHTS
        )

        result = {
            "protein_score": round(float(protein_score), 4),
            "is_likely_protein": protein_score >= self.score_threshold,
            "n_spots_in_shell": int(n_in_shell),
            "n_occupied_bins": int(n_occupied),
            "heuristics": {k: round(float(v), 4) for k, v in heuristics.items()},
            "cell_estimate": cell_estimate,
        }

        n_cands = len(cell_estimate["candidates"])
        if n_cands > 0:
            top = cell_estimate["candidates"][0]
            cell_str = (
                f", cell_candidates={n_cands}, "
                f"best={top['cell_A']:.1f} A (conf={top['confidence']:.2f})"
            )
        else:
            cell_str = ", cell_candidates=0"
        logger.info(
            f"ProteinClassifier: score={protein_score:.3f}, "
            f"is_protein={result['is_likely_protein']}, "
            f"spots_in_shell={n_in_shell}/{spots.count}, "
            f"occupied_bins={n_occupied}/{self.n_resolution_bins}"
            f"{cell_str}"
        )
        return result

    def estimate_cell_dimensions(self, spots):
        """Estimate likely unit cell dimension range from spot d-spacings.

        Converts d-spacings to reciprocal-space q = 1/d and looks for
        periodic spacing in the pairwise q-difference histogram. For a
        crystal with cell axis length `a`, reflections appear at q = h/a
        (h = 1, 2, 3, ...), so pairwise q-differences cluster at multiples
        of 1/a. The fundamental peak in the difference histogram gives 1/a.

        Peaks that are integer multiples of each other (harmonics) are
        grouped together, with the fundamental (largest cell dimension)
        absorbing the harmonic's prominence as supporting evidence.

        Returns ranked cell dimension candidates sorted by confidence.

        Args:
            spots: SpotList with resolution (d-spacing) values.

        Returns:
            dict with:
                candidates: list of dicts, each with:
                    cell_A: float — estimated cell axis length (Angstroms)
                    confidence: float — 0.0-1.0, peak prominence-based
                    q_spacing: float — reciprocal-space peak position (A^-1)
                    n_harmonics: int — number of harmonic peaks absorbed
                min_cell_A: float — smallest candidate (convenience)
                max_cell_A: float — largest candidate (convenience)
        """
        _empty = {
            "candidates": [],
            "min_cell_A": 0.0,
            "max_cell_A": 0.0,
        }

        valid_d = spots.resolution[spots.resolution > 0]
        if len(valid_d) < 4:
            return _empty

        # Convert to reciprocal space
        q = 1.0 / valid_d.astype(np.float64)
        q_sorted = np.sort(q)

        # Compute all pairwise differences in q
        # For large spot counts, subsample to keep O(N^2) manageable
        max_spots = 200
        if len(q_sorted) > max_spots:
            rng = np.random.RandomState(0)
            idx = rng.choice(len(q_sorted), max_spots, replace=False)
            q_sub = np.sort(q_sorted[idx])
        else:
            q_sub = q_sorted

        # Vectorized pairwise differences (upper triangle)
        # q_sub[j] - q_sub[i] for j > i
        n = len(q_sub)
        ii, jj = np.triu_indices(n, k=1)
        diffs = q_sub[jj] - q_sub[ii]

        if len(diffs) == 0:
            return _empty

        # Build histogram of q-differences
        # Range: from smallest meaningful spacing (1/max_cell ~ 0.002 A^-1)
        # to largest meaningful spacing (1/min_cell ~ 0.2 A^-1)
        q_diff_min = 0.002  # corresponds to ~500 A cell
        q_diff_max = 0.2    # corresponds to ~5 A cell
        n_hist_bins = 200

        mask = (diffs >= q_diff_min) & (diffs <= q_diff_max)
        counts, edges = np.histogram(
            diffs[mask], bins=n_hist_bins, range=(q_diff_min, q_diff_max),
        )
        bin_centers = 0.5 * (edges[:-1] + edges[1:])

        if counts.max() == 0:
            return _empty

        # Find peaks: local maxima above 2x median count
        median_count = max(float(np.median(counts[counts > 0])), 1.0)
        peak_threshold = 2.0 * median_count

        raw_peaks = []  # (q_center, count, prominence)
        for i in range(1, len(counts) - 1):
            if (counts[i] > counts[i - 1] and
                    counts[i] >= counts[i + 1] and
                    counts[i] >= peak_threshold):
                prominence = counts[i] / median_count
                raw_peaks.append((bin_centers[i], counts[i], prominence))

        if not raw_peaks:
            # Try global maximum as fallback
            i_max = int(np.argmax(counts))
            if counts[i_max] >= peak_threshold:
                prominence = counts[i_max] / median_count
                raw_peaks.append((bin_centers[i_max], counts[i_max], prominence))

        if not raw_peaks:
            return _empty

        # Sort by prominence (strongest first)
        raw_peaks.sort(key=lambda x: x[2], reverse=True)

        # Convert to cell dimensions and filter to reasonable range (5-500 A)
        peak_entries = []
        for q_peak, count, prom in raw_peaks:
            if q_peak <= 0:
                continue
            cell = 1.0 / q_peak
            if 5.0 <= cell <= 500.0:
                peak_entries.append({
                    "cell_A": cell,
                    "q_spacing": q_peak,
                    "prominence": prom,
                    "absorbed_prominences": [prom],
                })

        if not peak_entries:
            return _empty

        # Harmonic deduplication: if a weaker peak's cell is approximately
        # cell_fundamental / n (for n=2..6), it's a higher-order harmonic —
        # absorb it into the fundamental.
        #
        # Processing order: strongest peak first. Only absorb weaker peaks
        # that are *subdivisions* of the fundamental (smaller cell = higher
        # q-spacing). Never promote a strong peak to a weaker, larger-cell
        # peak — that would let noise hijack a real signal.
        deduplicated = []
        used = set()

        for i, entry in enumerate(peak_entries):
            if i in used:
                continue

            fundamental = entry.copy()
            fundamental["n_harmonics"] = 0
            fundamental["absorbed_prominences"] = list(entry["absorbed_prominences"])

            # Check all weaker peaks for harmonic relationship
            for j in range(i + 1, len(peak_entries)):
                if j in used:
                    continue
                other = peak_entries[j]

                # Is other.cell ≈ fundamental.cell / n for n=2..6?
                # i.e., is the fundamental the larger cell, and other a subdivision?
                ratio = fundamental["cell_A"] / other["cell_A"]
                if ratio >= 1.5:
                    nearest_int = round(ratio)
                    if 2 <= nearest_int <= 6:
                        if abs(ratio - nearest_int) / nearest_int < 0.10:
                            # This is a harmonic — absorb it
                            fundamental["n_harmonics"] += 1
                            fundamental["absorbed_prominences"].append(other["prominence"])
                            used.add(j)

            deduplicated.append(fundamental)

        # Compute confidence for each candidate.
        # Base confidence from prominence; boost for absorbed harmonics.
        # prominence 2 → 0.1, prominence 5 → 0.4, prominence 10+ → 1.0
        # Each harmonic adds ~0.1 (evidence of periodicity).
        for entry in deduplicated:
            total_prom = sum(entry["absorbed_prominences"])
            base_conf = (total_prom - 2.0) / 8.0
            harmonic_boost = 0.1 * entry["n_harmonics"]
            entry["confidence"] = float(np.clip(base_conf + harmonic_boost, 0.05, 1.0))

        # Sort by confidence (most confident first)
        deduplicated.sort(key=lambda x: x["confidence"], reverse=True)

        # Build ranked candidate list
        candidates = []
        for rank, entry in enumerate(deduplicated[:5], start=1):  # top 5
            candidates.append({
                "rank": rank,
                "cell_A": round(entry["cell_A"], 2),
                "confidence": round(entry["confidence"], 3),
                "q_spacing": round(entry["q_spacing"], 6),
                "n_harmonics": entry["n_harmonics"],
            })

        cell_values = [c["cell_A"] for c in candidates]
        min_cell = min(cell_values)
        max_cell = max(cell_values)

        logger.debug(
            f"CellEstimate: {len(candidates)} candidates, "
            f"best={candidates[0]['cell_A']:.1f} A "
            f"(conf={candidates[0]['confidence']:.2f}, "
            f"harmonics={candidates[0]['n_harmonics']})"
        )

        return {
            "candidates": candidates,
            "min_cell_A": round(float(min_cell), 2),
            "max_cell_A": round(float(max_cell), 2),
        }

    def _resolution_entropy(self, d_shell):
        """H1: Shannon entropy of d-spacing distribution in check shell.

        Protein: spots spread across many bins → high entropy → score ~1.0
        Salt/small molecule: spots in few bins → low entropy → score ~0.0
        """
        if len(d_shell) < 2:
            return 0.0

        counts, _ = np.histogram(d_shell, bins=self.bin_edges)
        # Normalize to probability distribution
        total = counts.sum()
        if total == 0:
            return 0.0

        p = counts[counts > 0] / total
        entropy = -np.sum(p * np.log(p))

        # Normalize by maximum possible entropy (uniform distribution)
        max_entropy = np.log(self.n_resolution_bins)
        if max_entropy == 0:
            return 0.0
        normalized = entropy / max_entropy

        # Map to score: entropy < 0.2 → 0, entropy > 0.6 → 1, linear between
        return float(np.clip((normalized - 0.2) / 0.4, 0.0, 1.0))

    def _spot_count_score(self, n_in_shell):
        """H2: Number of spots in the check shell vs expected minimum.

        A protein crystal with cell >= min_cell_A should produce at least
        a few reflections in the 15-4 A range on a single still image.
        Very few spots (0-2) suggests non-protein or very weak diffraction.

        Score ramps linearly: 0 spots → 0.0, >= min_expected → 1.0.
        """
        # Minimum expected: at least 3 spots in the shell for any protein
        # crystal with a reasonable cell. This is conservative — most protein
        # frames have many more.
        min_expected = 3
        if n_in_shell >= min_expected:
            return 1.0
        return float(n_in_shell / min_expected)

    def _intensity_distribution_score(self, intensities):
        """H3: Intensity distribution shape.

        Protein: Wilson-like distribution, max/median ratio typically < 20.
        Salt: few extremely bright spots, max/median ratio often > 50-100.

        Score ramps on log scale: ratio < 10 → 1.0, ratio > 100 → 0.0.
        """
        if len(intensities) < 2:
            return 0.0

        pos = intensities[intensities > 0]
        if len(pos) < 2:
            return 0.0

        median_val = np.median(pos)
        if median_val <= 0:
            return 0.0

        ratio = float(np.max(pos) / median_val)

        # Log-linear mapping: ratio 10 → 1.0, ratio 100 → 0.0
        # log10(10) = 1.0, log10(100) = 2.0
        if ratio <= 1.0:
            return 1.0
        log_ratio = np.log10(ratio)
        return float(np.clip(1.0 - (log_ratio - 1.0), 0.0, 1.0))

    def _salt_ring_score(self, d_spacings):
        """H4: Match against known salt crystal d-spacings.

        Checks if spots cluster at characteristic d-spacings of common
        salt contaminants (NaCl, KCl, CaCO3). If >= min_salt_matches spots
        match any single compound, score drops.

        Score: 1.0 = no salt detected, 0.0 = strong salt match.
        """
        if len(d_spacings) < self.min_salt_matches:
            return 1.0  # Not enough spots to judge

        valid_d = d_spacings[d_spacings > 0]
        if len(valid_d) < self.min_salt_matches:
            return 1.0

        best_match_fraction = 0.0

        for salt_name, salt_d_list in self.KNOWN_SALTS.items():
            # Count spots matching any d-spacing of this salt compound
            salt_mask = np.zeros(len(valid_d), dtype=bool)
            for d_salt in salt_d_list:
                salt_mask |= np.abs(valid_d - d_salt) < self.salt_tolerance_A

            n_matched = salt_mask.sum()
            if n_matched >= self.min_salt_matches:
                fraction = n_matched / len(valid_d)
                if fraction > best_match_fraction:
                    best_match_fraction = fraction
                    logger.debug(
                        f"ProteinClassifier: {n_matched} spots match {salt_name} "
                        f"({fraction:.1%} of spots)"
                    )

        # Map fraction to score: 0% matched → 1.0, >= 50% matched → 0.0
        return float(np.clip(1.0 - 2.0 * best_match_fraction, 0.0, 1.0))
