"""Estimate number of crystal lattices contributing to a diffraction frame.

Two levels of analysis:

Level 1 (no unit cell): Inter-spot distance histogram peakiness.
  Fast (~1ms), detects different-lattice multi-crystal patterns.
  Cannot distinguish same-lattice crystals at different orientations.

Level 2 (requires unit cell): Orientation consistency test.
  Moderate (~3-80ms), detects same-lattice multi-crystal patterns.
  Tries to index all spots with one orientation matrix. If a significant
  fraction is unexplained, tries a second orientation from the remainder.

Usage:
    from qp2.spotfinder2.crystal_count import estimate_n_crystals

    # Level 1 (no unit cell)
    n, conf, details = estimate_n_crystals(spots, geometry)

    # Level 2 (with unit cell)
    n, conf, details = estimate_n_crystals(
        spots, geometry,
        unit_cell=(79.1, 79.1, 37.9, 90, 90, 90),  # a,b,c,α,β,γ
    )
"""

import numpy as np
from typing import Tuple, Dict, Any, Optional

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def estimate_n_crystals(
    spots,
    geometry,
    unit_cell: Optional[tuple] = None,
    max_crystals: int = 10,
    indexing_tolerance: float = 0.15,
    min_spots: int = 5,
    **kwargs,
) -> Tuple[int, float, Dict[str, Any]]:
    """Estimate how many crystal lattices contribute to the spot list.

    Automatically selects Level 1 or Level 2 based on whether unit_cell
    is provided.

    Args:
        spots: SpotList with x, y, resolution fields.
        geometry: DetectorGeometry instance.
        unit_cell: (a, b, c, alpha, beta, gamma) in (Å, Å, Å, °, °, °).
                   If provided, uses Level 2 (orientation consistency).
                   If None, uses Level 1 (distance histogram).
        max_crystals: upper bound on estimate.
        indexing_tolerance: fractional HKL tolerance for Level 2 (default 0.15).
        min_spots: minimum spots to attempt analysis.

    Returns:
        (n_crystals, confidence, details)
    """
    if spots.count < min_spots:
        return 1, 0.0, {"n_spots": spots.count, "method": "insufficient_spots"}

    if unit_cell is not None:
        return _level2_orientation_consistency(
            spots, geometry, unit_cell, max_crystals, indexing_tolerance,
        )
    else:
        return _level1_distance_histogram(spots, geometry, max_crystals)


# ============================================================
# Level 2: Orientation consistency (requires unit cell)
# ============================================================

def _level2_orientation_consistency(
    spots, geometry, unit_cell, max_crystals, tolerance,
) -> Tuple[int, float, Dict[str, Any]]:
    """Detect multiple crystals by testing orientation consistency.

    Algorithm:
      1. Convert spot positions to q-vectors on the Ewald sphere
      2. Build reciprocal lattice from unit cell
      3. Try many triplets of spots → solve for orientation R (Kabsch SVD)
      4. Score R: count fraction of ALL spots that are consistent
      5. Best R explains the primary crystal. Remove its spots.
      6. Repeat on remaining spots to find additional crystals.
    """
    details: Dict[str, Any] = {
        "n_spots": spots.count,
        "method": "orientation_consistency",
        "unit_cell": unit_cell,
        "crystals": [],
    }

    # Convert spots to q-vectors
    q_obs = _spots_to_q_vectors(spots, geometry)
    if q_obs is None or len(q_obs) < 3:
        return 1, 0.0, details

    # Build reciprocal lattice
    rlatt_vecs = _unit_cell_to_reciprocal(unit_cell)
    q_hkl, hkl_indices = _generate_hkl_list(rlatt_vecs, q_max=q_obs.max() * 1.2)

    if len(q_hkl) < 3:
        logger.warning("Too few HKL reflections generated — check unit cell")
        return 1, 0.1, details

    # Subsample if too many HKL — for orientation matching we only need
    # enough reflections to score, not the full reciprocal lattice
    max_hkl = 2000
    if len(q_hkl) > max_hkl:
        rng = np.random.default_rng(42)
        idx = rng.choice(len(q_hkl), max_hkl, replace=False)
        q_hkl = q_hkl[idx]
        hkl_indices = hkl_indices[idx]

    details["n_hkl_generated"] = len(q_hkl)
    details["rlatt_vecs"] = rlatt_vecs.tolist()
    details["q_hkl"] = q_hkl  # stored for predicted reflection overlay

    # Build KD-tree of HKL q-vectors ONCE (reused for all orientation tests)
    from scipy.spatial import cKDTree
    q_hkl_tree = cKDTree(q_hkl)

    # Estimate null-model match rate: how many spots does a RANDOM orientation
    # match just by chance? This depends on lattice density and tolerance.
    random_match_rate = _estimate_random_match_rate(
        q_obs, q_hkl_tree, tolerance, n_random=10,
    )
    # Require match rate to be at least 2× random to count as a real crystal
    min_match_fraction = max(0.3, random_match_rate * 2.5)
    details["random_match_rate"] = round(float(random_match_rate), 3)
    details["min_match_fraction"] = round(float(min_match_fraction), 3)

    logger.debug(
        f"Null model: random match rate = {random_match_rate:.1%}, "
        f"min required = {min_match_fraction:.1%}"
    )

    # Iteratively find crystals
    remaining = np.ones(len(q_obs), dtype=bool)  # which spots are unexplained
    n_crystals = 0

    for crystal_iter in range(max_crystals):
        q_remaining = q_obs[remaining]
        idx_remaining = np.where(remaining)[0]

        if len(q_remaining) < 3:
            break

        # Find best orientation for remaining spots
        R_best, score, matched_mask = _find_best_orientation(
            q_remaining, q_hkl, q_hkl_tree, hkl_indices, rlatt_vecs, tolerance,
        )

        if R_best is None or score < 3:
            break

        frac_matched = score / len(q_remaining)

        # Stop if match rate is not significantly above random
        if n_crystals > 0 and frac_matched < min_match_fraction:
            logger.debug(
                f"Stopping: match rate {frac_matched:.1%} < "
                f"threshold {min_match_fraction:.1%} (random={random_match_rate:.1%})"
            )
            break

        n_crystals += 1

        details["crystals"].append({
            "crystal_id": n_crystals,
            "n_matched": int(score),
            "n_remaining": int(len(q_remaining)),
            "fraction_matched": round(float(frac_matched), 3),
            "R_matrix": R_best.tolist(),
        })

        # Mark matched spots as explained
        matched_global = idx_remaining[matched_mask]
        remaining[matched_global] = False

        logger.debug(
            f"Crystal {n_crystals}: {score}/{len(q_remaining)} spots matched "
            f"({frac_matched:.0%}), {remaining.sum()} remaining"
        )

        # If most spots are explained, stop
        if remaining.sum() < 3:
            break

    # Confidence based on how cleanly spots partition
    if n_crystals == 0:
        n_crystals = 1
        confidence = 0.1
    elif n_crystals == 1:
        frac = details["crystals"][0]["fraction_matched"]
        confidence = min(0.95, frac)  # high match → high confidence it's single
    else:
        # Multiple crystals: confidence scales with total explained fraction
        total_explained = spots.count - remaining.sum()
        frac_total = total_explained / spots.count
        confidence = min(0.9, frac_total * 0.8)

    n_unexplained = int(remaining.sum())
    details["n_unexplained"] = n_unexplained
    details["fraction_explained"] = round(1.0 - n_unexplained / spots.count, 3)

    logger.info(
        f"estimate_n_crystals: {n_crystals} crystal(s) "
        f"(confidence={confidence:.2f}, explained={details['fraction_explained']:.0%}, "
        f"n_spots={spots.count}, method=orientation_consistency)"
    )

    return n_crystals, round(confidence, 3), details


def _find_best_orientation(q_obs, q_hkl, q_hkl_tree, hkl_indices,
                           rlatt_vecs, tolerance,
                           n_trials=50, min_match=3):
    """Find the orientation matrix R that explains the most observed spots.

    Uses a prebuilt KD-tree of HKL positions. Instead of rotating the tree
    per trial, inverse-rotates the observations — O(N_obs × log N_hkl) per score.

    Returns (R_best, best_score, matched_mask) or (None, 0, None).
    """
    n_obs = len(q_obs)
    if n_obs < 3:
        return None, 0, None

    q_obs_norms = np.linalg.norm(q_obs, axis=1)
    q_hkl_norms = np.linalg.norm(q_hkl, axis=1)

    # Precompute: candidate HKL per observed spot (by |q| matching)
    candidates_per_spot = []
    for i in range(n_obs):
        q_norm = q_obs_norms[i]
        if q_norm < 1e-6:
            candidates_per_spot.append(np.array([], dtype=int))
            continue
        frac_diff = np.abs(q_hkl_norms - q_norm) / q_norm
        close = np.where(frac_diff < tolerance)[0]
        if len(close) > 3:
            close = close[np.argsort(frac_diff[close])[:3]]
        candidates_per_spot.append(close)

    usable = [i for i in range(n_obs) if len(candidates_per_spot[i]) > 0]
    if len(usable) < 3:
        return None, 0, None

    best_R = None
    best_score = 0
    best_mask = None
    rng = np.random.default_rng(42)

    for trial in range(n_trials):
        idx3 = rng.choice(usable, 3, replace=False)
        cands = [candidates_per_spot[i] for i in idx3]
        q_triplet = q_obs[idx3]

        for i0 in cands[0]:
            for i1 in cands[1]:
                for i2 in cands[2]:
                    if i0 == i1 or i0 == i2 or i1 == i2:
                        continue

                    R = _solve_rotation(q_triplet, q_hkl[[i0, i1, i2]])
                    if R is None:
                        continue

                    # Score: inverse-rotate observations, query prebuilt tree
                    R_inv = R.T  # R is orthogonal → R⁻¹ = Rᵀ
                    score, matched = _score_orientation(
                        q_obs, q_hkl_tree, R_inv, tolerance, q_obs_norms
                    )

                    if score > best_score:
                        best_score = score
                        best_R = R
                        best_mask = matched

                    if best_score >= n_obs * 0.6:
                        return best_R, best_score, best_mask

    if best_score >= min_match:
        return best_R, best_score, best_mask
    return None, 0, None


def _score_orientation(q_obs, q_hkl_tree, R_inv, tolerance, q_obs_norms):
    """Score an orientation: count observed spots matching lattice positions.

    Instead of rotating all HKL and rebuilding a KD-tree each time,
    we inverse-rotate the observations and query the prebuilt tree.

    Args:
        q_obs: (n_obs, 3) observed q-vectors
        q_hkl_tree: prebuilt cKDTree of HKL q-vectors
        R_inv: inverse rotation matrix (R^T since R is orthogonal)
        tolerance: fractional tolerance
        q_obs_norms: precomputed |q_obs|

    Returns:
        (n_matched, matched_mask)
    """
    # Inverse-rotate observations into crystal frame
    q_crystal = (R_inv @ q_obs.T).T  # (n_obs, 3)

    # Query prebuilt tree
    dists, _ = q_hkl_tree.query(q_crystal)
    thresholds = tolerance * np.maximum(q_obs_norms, 1e-6)
    matched = dists < thresholds

    return int(matched.sum()), matched


def _solve_rotation(q_obs_triplet, q_pred_triplet):
    """Solve for rotation R such that q_obs ≈ R @ q_pred (Kabsch algorithm).

    Returns 3x3 rotation matrix, or None if degenerate.
    """
    # Need at least 3 non-coplanar vectors
    P = q_pred_triplet  # (3, 3)
    Q = q_obs_triplet   # (3, 3)

    # Check for degeneracy
    if np.linalg.matrix_rank(P) < 3 or np.linalg.matrix_rank(Q) < 3:
        return None

    # Kabsch: H = P^T @ Q, then SVD
    H = P.T @ Q
    try:
        U, S, Vt = np.linalg.svd(H)
    except np.linalg.LinAlgError:
        return None

    # Ensure proper rotation (det = +1)
    d = np.linalg.det(Vt.T @ U.T)
    D = np.diag([1, 1, np.sign(d)])
    R = Vt.T @ D @ U.T

    return R


# ============================================================
# Level 1: Distance histogram peakiness (no unit cell)
# ============================================================

def _level1_distance_histogram(spots, geometry, max_crystals,
                                n_distance_bins=200):
    """Level 1: estimate crystal count from inter-spot distance histogram."""
    details: Dict[str, Any] = {
        "n_spots": spots.count,
        "method": "distance_histogram",
    }

    q_vectors = _spots_to_q_vectors(spots, geometry)
    if q_vectors is None or len(q_vectors) < 5:
        return 1, 0.0, details

    # Subsample if too many spots
    if len(q_vectors) > 300:
        rng = np.random.default_rng(42)
        q_vectors = q_vectors[rng.choice(len(q_vectors), 300, replace=False)]

    from scipy.spatial.distance import pdist
    distances = pdist(q_vectors, metric="euclidean")
    details["n_distances"] = len(distances)

    d_max = np.percentile(distances, 95)
    hist, _ = np.histogram(distances[distances <= d_max], bins=n_distance_bins)
    hist_norm = hist.astype(np.float64) / max(hist.sum(), 1)

    peakiness = _histogram_peakiness(hist_norm)
    details["peakiness"] = round(float(peakiness), 4)

    # Map peakiness to crystal count
    if peakiness > 0.35:
        n_crystals, confidence = 1, min(0.9, 0.5 + peakiness)
    elif peakiness > 0.20:
        n_crystals, confidence = 2, 0.5
    elif peakiness > 0.12:
        n_crystals = max(2, min(5, int(round(0.35 / max(peakiness, 0.01)))))
        confidence = 0.4
    elif peakiness > 0.05:
        n_crystals = max(3, min(max_crystals, int(round(0.5 / max(peakiness, 0.01)))))
        confidence = 0.3
    else:
        n_crystals, confidence = 1, 0.1
        details["method"] = "indeterminate"

    max_from_spots = max(1, spots.count // 3)
    n_crystals = max(1, min(n_crystals, max_from_spots, max_crystals))

    logger.info(
        f"estimate_n_crystals: {n_crystals} (confidence={confidence:.2f}, "
        f"peakiness={peakiness:.3f}, method=distance_histogram)"
    )
    return n_crystals, round(confidence, 3), details


# ============================================================
# Shared helpers
# ============================================================

def _spots_to_q_vectors(spots, geometry):
    """Convert spot (x, y) to 3D q-vectors on the Ewald sphere (Å⁻¹)."""
    valid = spots.resolution > 0
    if valid.sum() < 3:
        return None

    dx_mm = (spots.x[valid] - geometry.beam_x) * geometry.pixel_size
    dy_mm = (spots.y[valid] - geometry.beam_y) * geometry.pixel_size
    D = geometry.det_dist
    wl = geometry.wavelength

    r = np.sqrt(dx_mm**2 + dy_mm**2 + D**2)
    qx = dx_mm / (r * wl)
    qy = dy_mm / (r * wl)
    qz = (D / r - 1.0) / wl

    return np.column_stack([qx, qy, qz])


def _unit_cell_to_reciprocal(cell):
    """Convert (a, b, c, alpha, beta, gamma) to reciprocal lattice vectors.

    Args:
        cell: (a, b, c, alpha_deg, beta_deg, gamma_deg)

    Returns:
        3x3 array where rows are reciprocal lattice vectors (a*, b*, c*).
    """
    a, b, c, alpha_d, beta_d, gamma_d = cell
    alpha = np.radians(alpha_d)
    beta = np.radians(beta_d)
    gamma = np.radians(gamma_d)

    ca, cb, cg = np.cos(alpha), np.cos(beta), np.cos(gamma)
    sa, sb, sg = np.sin(alpha), np.sin(beta), np.sin(gamma)

    # Volume
    v = a * b * c * np.sqrt(1 - ca**2 - cb**2 - cg**2 + 2 * ca * cb * cg)

    # Reciprocal cell parameters
    astar = b * c * sa / v
    bstar = a * c * sb / v
    cstar = a * b * sg / v

    cas = (cb * cg - ca) / (sb * sg)
    cbs = (ca * cg - cb) / (sa * sg)
    cgs = (ca * cb - cg) / (sa * sb)
    sas = np.sqrt(max(0, 1 - cas**2))
    sbs = np.sqrt(max(0, 1 - cbs**2))
    sgs = np.sqrt(max(0, 1 - cgs**2))

    # Reciprocal lattice vectors in Cartesian frame
    rlatt = np.array([
        [astar, 0, 0],
        [bstar * cgs, bstar * sgs, 0],
        [cstar * cbs, -cstar * sbs * ca, cstar * sbs * sa],
    ])
    return rlatt


def _generate_hkl_list(rlatt_vecs, q_max, d_min=1.5):
    """Generate all HKL reflections within resolution range.

    Uses vectorized numpy instead of Python triple loop.

    Returns:
        q_hkl: (N, 3) array of q-vectors
        hkl_indices: (N, 3) array of (h, k, l) indices
    """
    norms = np.linalg.norm(rlatt_vecs, axis=1)
    if norms.min() < 1e-10:
        return np.empty((0, 3)), np.empty((0, 3), dtype=int)

    # q_limit: generate all reflections up to this |q|
    q_limit = max(q_max, 1.0 / d_min if d_min > 0 else q_max)

    # h_max per axis: use per-axis reciprocal vector length (not min)
    h_maxes = [int(np.ceil(q_limit / n)) + 1 for n in norms]
    h_maxes = [min(h, 60) for h in h_maxes]  # safety cap per axis

    # Vectorized: generate all (h,k,l) at once
    ranges = [np.arange(-hm, hm + 1) for hm in h_maxes]
    hh, kk, ll = np.meshgrid(*ranges, indexing="ij")
    hkl_all = np.column_stack([hh.ravel(), kk.ravel(), ll.ravel()])

    # Remove (0,0,0)
    nonzero = np.any(hkl_all != 0, axis=1)
    hkl_all = hkl_all[nonzero]

    # Compute q-vectors: q = h*a* + k*b* + l*c*
    q_all = hkl_all @ rlatt_vecs  # (N, 3)
    q_norms = np.linalg.norm(q_all, axis=1)

    # Filter: only keep reflections within the resolution range
    # No lower q-bound — include all low-resolution reflections
    keep = q_norms <= q_limit

    q_hkl = q_all[keep]
    hkl_indices = hkl_all[keep]

    return q_hkl, hkl_indices


def _estimate_random_match_rate(q_obs, q_hkl_tree, tolerance, n_random=10):
    """Estimate how many spots a random orientation matches by chance.

    Generates n_random random rotation matrices, scores each against the
    lattice, and returns the average match fraction. This is the null model
    for the orientation consistency test.
    """
    from scipy.spatial.transform import Rotation

    q_obs_norms = np.linalg.norm(q_obs, axis=1)
    rates = []

    for i in range(n_random):
        R_rand = Rotation.random(random_state=i + 1000).as_matrix()
        R_inv = R_rand.T
        score, _ = _score_orientation(q_obs, q_hkl_tree, R_inv, tolerance, q_obs_norms)
        rates.append(score / len(q_obs))

    return float(np.mean(rates))


def _histogram_peakiness(hist_norm):
    """Ratio of high-frequency to total variance in the histogram."""
    if len(hist_norm) < 10 or hist_norm.sum() < 1e-10:
        return 0.0

    kernel_size = max(3, len(hist_norm) // 10)
    if kernel_size % 2 == 0:
        kernel_size += 1
    smooth = np.convolve(hist_norm, np.ones(kernel_size) / kernel_size, mode="same")
    residual = hist_norm - smooth

    var_original = np.var(hist_norm)
    var_residual = np.var(residual)

    if var_original < 1e-15:
        return 0.0
    return float(np.clip(var_residual / var_original, 0, 1))
