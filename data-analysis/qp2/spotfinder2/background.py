"""Multi-scale and ring-aware background estimation for spotfinder2.

Implements a 3-scale background model:
  1. Coarse radial (50 q-bins) — smooth trends
  2. Fine radial (250 q-bins) — rings, solvent features
  3. 2D (resolution x azimuth) — polarization, anisotropy

Plus explicit modeling of ice/solvent rings as background components
rather than rejecting entire resolution shells.

Performance: ~0.5s per frame for Eiger 16M (4150x4371) on CPU.
"""

import numpy as np
from typing import Optional

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

# Known ice ring d-spacings (Angstrom) — hexagonal ice Ih
ICE_RINGS_D = [3.67, 3.44, 2.67, 2.25, 2.07, 1.95, 1.92, 1.88]


class BackgroundModel:
    """Multi-scale background estimation with ring-aware component.

    background(pixel) = B_smooth(q, psi) + sum_k R_k(q) * A_k(psi)

    where B_smooth is the multi-scale smooth background and R_k * A_k
    are the ring components (Gaussian in q, fitted azimuthal profile).
    """

    def __init__(
        self,
        geometry,  # DetectorGeometry
        backend,   # Backend
        n_radial_coarse: int = 50,
        n_radial_fine: int = 250,
        n_azimuthal: int = 60,
        min_pixels_per_bin: int = 50,
        n_poisson_iterations: int = 3,
        ring_width_q: float = 0.005,
        ring_d_spacings: Optional[list] = None,
    ):
        self.geometry = geometry
        self.backend = backend
        self.n_radial_coarse = n_radial_coarse
        self.n_radial_fine = n_radial_fine
        self.n_azimuthal = n_azimuthal
        self.min_pixels_per_bin = min_pixels_per_bin
        self.n_poisson_iterations = n_poisson_iterations
        self.ring_width_q = ring_width_q
        self.ring_d_spacings = ring_d_spacings if ring_d_spacings is not None else ICE_RINGS_D

        # Precompute bin edges
        q_flat = geometry.q_map[geometry.q_map > 0]
        self.q_min = float(q_flat.min()) if q_flat.size > 0 else 0.01
        self.q_max = float(q_flat.max()) if q_flat.size > 0 else 1.0

        self.q_bins_coarse = np.linspace(self.q_min, self.q_max, n_radial_coarse + 1)
        self.q_bins_fine = np.linspace(self.q_min, self.q_max, n_radial_fine + 1)
        self.psi_bins = np.linspace(0, 2 * np.pi, n_azimuthal + 1)

        # Precompute ALL bin indices once (the expensive digitize calls)
        self._q_idx_coarse = np.clip(
            np.digitize(geometry.q_map, self.q_bins_coarse) - 1,
            0, n_radial_coarse - 1
        ).astype(np.int32)

        self._q_idx_fine = np.clip(
            np.digitize(geometry.q_map, self.q_bins_fine) - 1,
            0, n_radial_fine - 1
        ).astype(np.int32)

        self._psi_idx = np.clip(
            np.digitize(geometry.azimuth_map, self.psi_bins) - 1,
            0, n_azimuthal - 1
        ).astype(np.int32)

        # Precompute flattened 2D bin index (avoids recomputation per frame)
        self._idx_2d_full = (self._q_idx_fine * n_azimuthal + self._psi_idx).ravel()

        # Precompute ring zones and per-ring pixel lists
        self.ring_q_centers = [1.0 / d for d in self.ring_d_spacings if d > 0]
        self._ring_mask = np.zeros(geometry.q_map.shape, dtype=bool)
        self._ring_pixel_indices = {}  # q_ring -> flat indices of ring pixels
        self._ring_psi_indices = {}    # q_ring -> psi bin index per ring pixel

        for q_ring in self.ring_q_centers:
            in_ring = np.abs(geometry.q_map - q_ring) < 3 * ring_width_q
            self._ring_mask |= in_ring
            flat_idx = np.flatnonzero(in_ring)
            if len(flat_idx) > 0:
                self._ring_pixel_indices[q_ring] = flat_idx
                self._ring_psi_indices[q_ring] = self._psi_idx.ravel()[flat_idx]

        logger.info(
            f"BackgroundModel: q=[{self.q_min:.4f}, {self.q_max:.4f}] Å⁻¹, "
            f"coarse={n_radial_coarse}, fine={n_radial_fine}, azimuthal={n_azimuthal}, "
            f"rings={len(self.ring_q_centers)}"
        )

    def estimate(self, frame: np.ndarray, mask: np.ndarray) -> np.ndarray:
        """Estimate per-pixel background for a single frame.

        Uses a fast two-stage approach:
        1. Fine radial background with Poisson truncation
        2. Ring-aware correction at known ice ring positions

        The background model always excludes ring zones and fits ring
        profiles. This is safe even when no ice is present: the ring
        profile amplitudes will simply be near zero, and excluding a
        narrow annular band from the smooth fit has negligible effect
        on background quality.

        Ice ring DETECTION (which rings are actually active) is done
        separately at the spot level by IceSpotFilter after detection,
        since pixel-level detection is unreliable on photon-counting
        detectors where most pixels are zero.

        Args:
            frame: 2D detector image (int or float)
            mask: boolean mask (True = masked/invalid)

        Returns:
            2D float32 array of background estimate, same shape as frame.
        """
        frame_f = frame.astype(np.float32)
        valid = ~mask & (frame >= 0)

        # Flatten once for reuse
        valid_flat = valid.ravel()
        frame_flat = frame_f.ravel()

        # Exclude ring zones from smooth background fitting
        valid_no_rings_flat = valid_flat & ~self._ring_mask.ravel()

        # 1. Fine radial background with Poisson truncation (primary model)
        bg_fine_bins = self._radial_background_flat(
            frame_flat, valid_no_rings_flat,
            self._q_idx_fine.ravel(), self.n_radial_fine
        )

        # 2. Azimuthal correction: compute 2D bin means in ONE pass (no iteration)
        #    This captures polarization/anisotropy without a full iterative pass
        n_2d = self.n_radial_fine * self.n_azimuthal
        sel = valid_no_rings_flat
        sel_frame = frame_flat[sel]
        sel_2d = self._idx_2d_full[sel]
        bg_2d_sums = np.bincount(sel_2d, weights=sel_frame, minlength=n_2d).astype(np.float64)
        bg_2d_counts = np.bincount(sel_2d, minlength=n_2d).astype(np.float64)
        with np.errstate(divide="ignore", invalid="ignore"):
            bg_2d_mean = np.where(bg_2d_counts > 0, bg_2d_sums / bg_2d_counts, 0.0)

        # 3. Map to pixels and compute azimuthal ratio
        bg_flat = bg_fine_bins[self._q_idx_fine.ravel()].astype(np.float32)
        bg_2d_flat = bg_2d_mean[self._idx_2d_full].astype(np.float32)

        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = np.where(bg_flat > 0.1, bg_2d_flat / bg_flat, 1.0)
        np.clip(ratio, 0.5, 2.0, out=ratio)
        bg_flat *= ratio

        # 4. Add ring components (vectorized)
        bg_flat = self._add_ring_component_fast(frame_flat, valid_flat, bg_flat)

        # Ensure non-negative
        np.maximum(bg_flat, 0.0, out=bg_flat)

        return bg_flat.reshape(frame.shape)

    def _radial_background_flat(self, frame_flat, valid_flat, bin_idx_flat, n_bins):
        """1D background with iterative Poisson truncation on flattened arrays.

        Returns per-bin mean values (1D array of length n_bins).
        """
        selected = valid_flat
        sel_frame = frame_flat[selected]
        sel_bins = bin_idx_flat[selected]

        # Initial mean per bin
        bin_sums = np.bincount(sel_bins, weights=sel_frame, minlength=n_bins).astype(np.float64)
        bin_counts = np.bincount(sel_bins, minlength=n_bins).astype(np.float64)
        with np.errstate(divide="ignore", invalid="ignore"):
            bin_mean = np.where(bin_counts > 0, bin_sums / bin_counts, 0.0)

        # Iterative Poisson truncation
        for _ in range(self.n_poisson_iterations):
            pixel_mean = bin_mean[sel_bins]
            threshold = pixel_mean + 5.0 * np.sqrt(np.maximum(pixel_mean, 0.1))
            keep = sel_frame <= threshold

            sel_frame_k = sel_frame[keep]
            sel_bins_k = sel_bins[keep]
            bin_sums = np.bincount(sel_bins_k, weights=sel_frame_k, minlength=n_bins).astype(np.float64)
            bin_counts = np.bincount(sel_bins_k, minlength=n_bins).astype(np.float64)
            with np.errstate(divide="ignore", invalid="ignore"):
                bin_mean = np.where(bin_counts > 0, bin_sums / bin_counts, bin_mean)

        return bin_mean

    def _merge_scales_fast(self, bg_coarse_bins, bg_fine_bins, bg_2d_bins):
        """Merge multi-scale backgrounds into a single flat background array."""
        n_pixels = self._q_idx_coarse.size

        # Map bin values to pixels
        bg_coarse = bg_coarse_bins[self._q_idx_coarse.ravel()].astype(np.float32)
        bg_fine = bg_fine_bins[self._q_idx_fine.ravel()].astype(np.float32)
        bg_2d = bg_2d_bins[self._idx_2d_full].astype(np.float32)

        # Start from coarse, overlay fine where sufficient
        # (simplified: always use fine if non-zero, since fine bins are computed)
        background = np.where(bg_fine > 0, bg_fine, bg_coarse)

        # Apply azimuthal modulation from 2D model
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = np.where(bg_fine > 0.1, bg_2d / bg_fine, 1.0)
        np.clip(ratio, 0.5, 2.0, out=ratio)
        background *= ratio

        return background

    def _add_ring_component_fast(self, frame_flat, valid_flat, bg_flat,
                                  active_q=None):
        """Vectorized ring component fitting using precomputed pixel indices.

        Args:
            frame_flat: flattened frame array
            valid_flat: flattened validity mask
            bg_flat: flattened background array (modified in-place)
            active_q: set of q values to process, or None for all rings
        """
        rings_to_process = self.ring_q_centers if active_q is None else [
            q for q in self.ring_q_centers if q in active_q
        ]
        for q_ring in rings_to_process:
            if q_ring not in self._ring_pixel_indices:
                continue

            ring_flat_idx = self._ring_pixel_indices[q_ring]
            ring_psi = self._ring_psi_indices[q_ring]

            # Filter to valid pixels
            ring_valid = valid_flat[ring_flat_idx]
            if ring_valid.sum() < 100:
                continue

            ring_frame = frame_flat[ring_flat_idx]
            ring_bg = bg_flat[ring_flat_idx]

            # Vectorized per-azimuthal-bin median using bincount + sorting
            # Instead of looping over bins, use a grouped approach
            valid_idx = ring_valid.nonzero()[0]
            valid_psi = ring_psi[valid_idx]
            valid_frame = ring_frame[valid_idx]
            valid_bg = ring_bg[valid_idx]

            ring_profile = np.zeros(self.n_azimuthal, dtype=np.float32)
            bin_counts = np.bincount(valid_psi, minlength=self.n_azimuthal)

            # Sort by psi bin for grouped operations
            sort_order = np.argsort(valid_psi)
            sorted_psi = valid_psi[sort_order]
            sorted_vals = valid_frame[sort_order]
            sorted_bg = valid_bg[sort_order]

            # Process each bin using cumulative counts (no inner Python loop on data)
            offsets = np.zeros(self.n_azimuthal + 1, dtype=int)
            np.cumsum(bin_counts, out=offsets[1:])

            for b in range(self.n_azimuthal):
                n = bin_counts[b]
                if n < 5:
                    continue
                start, end = offsets[b], offsets[b + 1]
                med_val = np.median(sorted_vals[start:end])
                mean_bg = sorted_bg[start:end].mean()
                ring_profile[b] = max(0.0, med_val - mean_bg)

            # Fill empty bins with neighbor average
            empty = bin_counts < 5
            if empty.any() and not empty.all():
                filled = ~empty
                filled_idx = np.where(filled)[0]
                for b in np.where(empty)[0]:
                    dists = np.abs(
                        np.minimum(
                            np.abs(filled_idx - b),
                            self.n_azimuthal - np.abs(filled_idx - b)
                        )
                    )
                    nearest = filled_idx[np.argmin(dists)]
                    ring_profile[b] = ring_profile[nearest]

            # Apply ring profile to all ring pixels (vectorized lookup)
            bg_flat[ring_flat_idx] += ring_profile[ring_psi]

        return bg_flat
