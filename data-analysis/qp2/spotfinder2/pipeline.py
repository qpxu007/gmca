"""SpotFinderPipeline: orchestrates the full 9-stage spot-finding pipeline.

Wires together background estimation, detection, filtering, and refinement
with configuration management and timing.
"""

import time
import numpy as np
from dataclasses import dataclass, field
from typing import Optional, Dict, Callable

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class SpotFinderConfig:
    """All pipeline parameters with sensible defaults."""

    # Background estimation
    n_radial_coarse: int = 50
    n_radial_fine: int = 250
    n_azimuthal: int = 60
    min_pixels_per_bin: int = 50
    n_poisson_iterations: int = 2  # 2 iterations sufficient for convergence
    ring_width_q: float = 0.005

    # Detection
    p_false_alarm: float = 1e-5
    box_size: int = 3
    nms_size: int = 5

    # CCL + dispersion filter
    min_pixels: int = 2
    max_pixels: int = 200
    n_sigma_b: float = 6.0
    n_sigma_s: float = 3.0
    dispersion_kernel: int = 7

    # Shape filter
    max_aspect_ratio: float = 3.0

    # Ice spot filter
    enable_ice_filter: bool = True
    ice_tolerance: float = 0.02
    ice_min_matches: int = 3

    # MLE refinement
    enable_mle_refinement: bool = True
    psf_sigma: float = 1.0
    mle_cutout_radius: int = 3

    # TDS fitting
    enable_tds_fitting: bool = False
    tds_sigma: float = 4.0
    tds_cutout_radius: int = 5

    # Resolution limits (Angstroms)
    low_resolution_A: float = 50.0
    high_resolution_A: float = 1.5

    # Mask
    mask_values: Optional[set] = None
    masked_circles: Optional[list] = None
    masked_rectangles: Optional[list] = None

    # GPU
    force_cpu: bool = False

    # Crystal count estimation
    estimate_n_crystals: bool = False
    unit_cell: Optional[tuple] = None  # (a, b, c, alpha, beta, gamma) for Level 2

    # Protein diffraction classification
    enable_protein_filter: bool = False
    protein_min_cell_A: float = 20.0
    protein_check_dmin_A: float = 4.0
    protein_check_dmax_A: float = 15.0
    protein_score_threshold: float = 0.5


class SpotFinderPipeline:
    """Orchestrates the full 9-stage spot-finding pipeline.

    Usage:
        from qp2.xio.hdf5_manager import HDF5Reader
        reader = HDF5Reader(master_file, start_timer=False)
        params = reader.get_parameters()

        pipeline = SpotFinderPipeline(params)
        frame = reader.get_frame(0)
        spots = pipeline.find_spots(frame)

        # Or process multiple frames
        results = pipeline.process_dataset(master_file, frame_range=(0, 100))
    """

    def __init__(self, params: dict, config: Optional[SpotFinderConfig] = None):
        """
        Args:
            params: dict from HDF5Reader.get_parameters()
            config: pipeline configuration (uses defaults if None)
        """
        from .backend import get_backend
        from .detector import DetectorGeometry, build_mask
        from .background import BackgroundModel
        from .threshold import ThresholdTable
        from .filtering import IceSpotFilter

        self.config = config or SpotFinderConfig()
        self.params = params
        self.backend = get_backend(force_cpu=self.config.force_cpu)
        self.geometry = DetectorGeometry(params)
        self.bg_model = BackgroundModel(
            self.geometry, self.backend,
            n_radial_coarse=self.config.n_radial_coarse,
            n_radial_fine=self.config.n_radial_fine,
            n_azimuthal=self.config.n_azimuthal,
            min_pixels_per_bin=self.config.min_pixels_per_bin,
            n_poisson_iterations=self.config.n_poisson_iterations,
            ring_width_q=self.config.ring_width_q,
        )
        self.threshold_table = ThresholdTable(p_false_alarm=self.config.p_false_alarm)
        self.ice_filter = (
            IceSpotFilter(self.geometry, tolerance=self.config.ice_tolerance,
                          min_matches=self.config.ice_min_matches)
            if self.config.enable_ice_filter else None
        )

        # Protein diffraction classifier (optional, runs after ice filter)
        self.protein_classifier = None
        if self.config.enable_protein_filter:
            from .filtering import ProteinDiffractionClassifier
            self.protein_classifier = ProteinDiffractionClassifier(
                min_cell_A=self.config.protein_min_cell_A,
                check_dmin_A=self.config.protein_check_dmin_A,
                check_dmax_A=self.config.protein_check_dmax_A,
                score_threshold=self.config.protein_score_threshold,
            )

        self._mask_template = None  # cached mask
        self._resolution_mask = None  # cached resolution mask

    def find_spots(self, frame: np.ndarray, mask: Optional[np.ndarray] = None):
        """Run the full pipeline on a single frame.

        Args:
            frame: 2D detector image (ny, nx), int16/int32/float
            mask: boolean mask (True=masked). If None, auto-computed from frame.

        Returns:
            SpotList with all per-spot fields populated.
        """
        from .detector import build_mask
        from .detection import detect_spots
        from .filtering import filter_by_shape
        from .refinement import refine_centroids, integrate_with_tds
        from .spot_list import SpotList

        t0 = time.time()
        timings = {}

        # Stage 0: Build mask
        if mask is None:
            mask = self._get_mask(frame)
        mask = mask | self._get_resolution_mask()

        # Stage 1: Estimate background (multi-scale + ring-aware)
        t1 = time.time()
        background = self.bg_model.estimate(frame, mask)
        timings["background"] = time.time() - t1

        # Stage 2-4: Detection (box-sum + NMS + CCL + dispersion filter)
        t1 = time.time()
        labels, n_spots, props = detect_spots(
            frame, background, mask, self.geometry, self.backend,
            self.threshold_table,
            box_size=self.config.box_size,
            nms_size=self.config.nms_size,
            min_pixels=self.config.min_pixels,
            max_pixels=self.config.max_pixels,
            n_sigma_b=self.config.n_sigma_b,
            n_sigma_s=self.config.n_sigma_s,
            dispersion_kernel=self.config.dispersion_kernel,
        )
        timings["detection"] = time.time() - t1

        if n_spots == 0:
            logger.debug("No spots detected")
            return SpotList()

        # Build initial SpotList from detection properties
        spots = SpotList.from_arrays(**props)

        # Stage 5: Shape filter
        t1 = time.time()
        labels, n_remaining, aspect_ratios = filter_by_shape(
            labels, frame,
            max_aspect_ratio=self.config.max_aspect_ratio,
            min_pixels=self.config.min_pixels,
            max_pixels=self.config.max_pixels,
        )
        if n_remaining < n_spots:
            # Recompute properties for surviving components
            from .detection import _compute_properties_fast
            props = _compute_properties_fast(
                frame.astype(np.float32), background, labels, n_remaining, self.geometry
            )
            spots = SpotList.from_arrays(**props)
        timings["shape_filter"] = time.time() - t1

        # Stage 6: Ice spot filter (auto-detects active rings from spots)
        if self.ice_filter and spots.count > 0:
            t1 = time.time()
            spots, ice_mask, active_rings = self.ice_filter.filter(spots)
            spots.metadata["ice_rings_detected"] = len(active_rings)
            spots.metadata["ice_rings_d_spacings"] = active_rings
            timings["ice_filter"] = time.time() - t1

        # Stage 6b: Estimate number of crystals
        if self.config.estimate_n_crystals and spots.count > 0:
            t1 = time.time()
            from .crystal_count import estimate_n_crystals
            n_crystals, confidence, crystal_details = estimate_n_crystals(
                spots, self.geometry,
                unit_cell=self.config.unit_cell,
            )
            spots.metadata["n_crystals"] = n_crystals
            spots.metadata["n_crystals_confidence"] = confidence
            spots.metadata["n_crystals_method"] = crystal_details.get("method", "")
            timings["crystal_count"] = time.time() - t1

        # Stage 6c: Protein diffraction classification
        if self.protein_classifier and spots.count > 0:
            t1 = time.time()
            protein_result = self.protein_classifier.classify(spots)
            spots.metadata["protein_score"] = protein_result["protein_score"]
            spots.metadata["is_likely_protein"] = protein_result["is_likely_protein"]
            spots.metadata["protein_heuristics"] = protein_result["heuristics"]
            spots.metadata["n_spots_in_check_shell"] = protein_result["n_spots_in_shell"]
            spots.metadata["n_occupied_bins"] = protein_result["n_occupied_bins"]
            cell_est = protein_result["cell_estimate"]
            spots.metadata["estimated_min_cell_A"] = cell_est["min_cell_A"]
            spots.metadata["estimated_max_cell_A"] = cell_est["max_cell_A"]
            spots.metadata["cell_candidates"] = cell_est["candidates"]
            timings["protein_filter"] = time.time() - t1

        # Stage 7: MLE position refinement
        if self.config.enable_mle_refinement and spots.count > 0:
            t1 = time.time()
            spots = refine_centroids(
                frame, background, spots,
                psf_sigma=self.config.psf_sigma,
                cutout_radius=self.config.mle_cutout_radius,
            )
            timings["mle_refinement"] = time.time() - t1

        # Stage 8: TDS-aware integration
        if self.config.enable_tds_fitting and spots.count > 0:
            t1 = time.time()
            spots = integrate_with_tds(
                frame, background, spots,
                psf_sigma=self.config.psf_sigma,
                tds_sigma=self.config.tds_sigma,
                cutout_radius=self.config.tds_cutout_radius,
            )
            timings["tds_fitting"] = time.time() - t1

        total_time = time.time() - t0
        timing_str = ", ".join(f"{k}={v:.3f}s" for k, v in timings.items())
        logger.info(
            f"find_spots: {spots.count} spots in {total_time:.3f}s ({timing_str})"
        )

        # Store background and labels for visualization
        self._last_background = background
        self._last_labels = labels

        return spots

    def process_dataset(
        self, master_file: str,
        frame_range: Optional[tuple] = None,
        callback: Optional[Callable] = None,
        n_workers: int = 1,
    ) -> Dict[int, "SpotList"]:
        """Process multiple frames from an HDF5 dataset.

        Automatically selects the execution strategy:
        - GPU available: sequential on GPU (avoids multi-context overhead)
        - CPU, n_workers=1: sequential on CPU
        - CPU, n_workers>1: multiprocessing pool for parallel CPU execution

        Uses HDF5Reader if available (Qt environment), falls back to
        direct h5py access otherwise.

        Args:
            master_file: path to HDF5 master file
            frame_range: (start, end) tuple, or None for all frames
            callback: optional callback(frame_idx, spots) for progress
            n_workers: number of parallel workers (CPU only; ignored on GPU)

        Returns:
            dict mapping frame_index -> SpotList
        """
        # Determine total frames and build frame loader
        reader, total = _open_dataset(master_file, self.params)

        if frame_range:
            start, end = frame_range
            end = min(end, total)
        else:
            start, end = 0, total

        frame_indices = list(range(start, end))

        if self.backend.has_gpu or n_workers <= 1:
            mode = "GPU" if self.backend.has_gpu else "CPU"
            logger.info(
                f"process_dataset: {mode} — sequential ({len(frame_indices)} frames)"
            )
            results = self._process_sequential(reader, frame_indices, callback)
        else:
            logger.info(
                f"process_dataset: CPU — {n_workers} workers "
                f"({len(frame_indices)} frames)"
            )
            results = self._process_parallel(
                master_file, frame_indices, n_workers, callback
            )

        if hasattr(reader, "close"):
            reader.close()

        n_processed = len(results)
        total_spots = sum(s.count for s in results.values())
        logger.info(
            f"process_dataset: {n_processed} frames, {total_spots} total spots"
        )
        return results

    def _process_sequential(self, reader, frame_indices, callback=None):
        """Process frames sequentially (GPU or single-core CPU)."""
        results = {}
        mask = None

        for idx in frame_indices:
            frame = _read_frame(reader, idx)
            if frame is None:
                logger.warning(f"Frame {idx}: could not read, skipping")
                continue

            if mask is None:
                mask = self._get_mask(frame)
                mask = mask | self._get_resolution_mask()

            spots = self.find_spots(frame, mask=mask)
            results[idx] = spots

            if callback:
                callback(idx, spots)

        return results

    def _process_parallel(self, master_file, frame_indices, n_workers, callback=None):
        """Process frames in parallel using multiprocessing.

        Each worker opens its own HDF5Reader and SpotFinderPipeline
        (stateless per-frame after mask init).
        """
        import multiprocessing as mp
        from functools import partial

        # Serialize config to dict for pickling across processes
        config_dict = {
            field: getattr(self.config, field)
            for field in self.config.__dataclass_fields__
        }

        # Split frame indices into chunks for workers
        chunks = _split_into_chunks(frame_indices, n_workers)

        logger.info(
            f"Launching {n_workers} workers: "
            + ", ".join(f"worker{i}={len(c)} frames" for i, c in enumerate(chunks))
        )

        with mp.Pool(processes=n_workers) as pool:
            worker_results = pool.map(
                partial(
                    _worker_process_frames,
                    master_file=master_file,
                    params_dict=self.params,
                    config_dict=config_dict,
                ),
                chunks,
            )

        # Merge results from all workers
        results = {}
        for worker_result in worker_results:
            results.update(worker_result)

        # Fire callbacks in order (for progress reporting)
        if callback:
            for idx in sorted(results.keys()):
                callback(idx, results[idx])

        return results

    def get_n_workers_auto(self) -> int:
        """Return recommended number of workers based on hardware.

        GPU: returns 1 (sequential is optimal).
        CPU: returns min(cpu_count/2, 8) — leave cores for OS/other tasks.
        """
        if self.backend.has_gpu:
            return 1
        import os
        n_cpus = os.cpu_count() or 1
        return max(1, min(n_cpus // 2, 8))

    def _get_mask(self, frame):
        """Build or return cached mask."""
        if self._mask_template is not None:
            # Apply per-frame value masking on top of cached geometric mask
            from .detector import build_mask
            value_mask = build_mask(
                frame, self.geometry,
                mask_values=self.config.mask_values,
            )
            return self._mask_template | value_mask

        from .detector import build_mask
        self._mask_template = build_mask(
            frame, self.geometry,
            mask_values=self.config.mask_values,
            masked_circles=self.config.masked_circles,
            masked_rectangles=self.config.masked_rectangles,
        )
        return self._mask_template

    def _get_resolution_mask(self):
        """Mask pixels outside resolution range (cached)."""
        if self._resolution_mask is not None:
            return self._resolution_mask

        mask = np.zeros(self.geometry.resolution_map.shape, dtype=bool)

        if self.config.low_resolution_A < np.inf:
            r_inner = self.geometry.res_to_radius(self.config.low_resolution_A)
            mask |= (self.geometry.radius_map < r_inner)

        if self.config.high_resolution_A > 0:
            r_outer = self.geometry.res_to_radius(self.config.high_resolution_A)
            mask |= (self.geometry.radius_map > r_outer)

        self._resolution_mask = mask
        return mask


# ---- Module-level helpers for HDF5 access and multiprocessing ----

def _open_dataset(master_file, params):
    """Open dataset, returning (reader, total_frames).

    Tries HDF5Reader first (Qt environment), falls back to direct h5py.
    """
    try:
        from qp2.xio.hdf5_manager import HDF5Reader
        reader = HDF5Reader(master_file, start_timer=False)
        return reader, reader.total_frames
    except (ImportError, RuntimeError):
        # Fallback: direct h5py access
        return _H5pyReader(master_file, params), params.get("nimages", 0)


def _read_frame(reader, idx):
    """Read a single frame from any reader type."""
    if hasattr(reader, "get_frame"):
        return reader.get_frame(idx)
    elif isinstance(reader, _H5pyReader):
        return reader.get_frame(idx)
    return None


class _H5pyReader:
    """Minimal HDF5 frame reader using h5py directly (no Qt dependency)."""

    def __init__(self, master_file, params):
        import os
        self.master_dir = os.path.dirname(master_file)
        self.prefix = os.path.basename(master_file).replace("_master.h5", "")
        self.images_per_hdf = params.get("images_per_hdf", 1)
        self._dset_paths = ["/entry/data/data", "/entry/data/raw_data"]
        self._open_handles = {}

    def get_frame(self, idx):
        import h5py
        import os
        file_num = idx // self.images_per_hdf + 1
        local_idx = idx % self.images_per_hdf
        data_file = os.path.join(
            self.master_dir, f"{self.prefix}_data_{file_num:06d}.h5"
        )
        try:
            if data_file not in self._open_handles:
                self._open_handles[data_file] = h5py.File(data_file, "r")
            f = self._open_handles[data_file]
            for dset_path in self._dset_paths:
                if dset_path in f:
                    return f[dset_path][local_idx]
        except Exception:
            return None
        return None

    def close(self):
        for f in self._open_handles.values():
            try:
                f.close()
            except Exception:
                pass
        self._open_handles.clear()

def _split_into_chunks(items, n_chunks):
    """Split a list into n roughly equal chunks."""
    n_chunks = min(n_chunks, len(items))
    if n_chunks <= 0:
        return [items]
    chunk_size = len(items) // n_chunks
    remainder = len(items) % n_chunks
    chunks = []
    start = 0
    for i in range(n_chunks):
        end = start + chunk_size + (1 if i < remainder else 0)
        chunks.append(items[start:end])
        start = end
    return chunks


def _worker_process_frames(frame_indices, master_file, params_dict, config_dict):
    """Worker function for multiprocessing.Pool.

    Each worker creates its own HDF5Reader and SpotFinderPipeline,
    processes its assigned frames, and returns {frame_idx: SpotList.to_dict()}.

    Results are serialized as dicts (not SpotList objects) because
    structured numpy arrays with custom dtype don't pickle cleanly
    across processes.
    """
    import h5py
    import os
    from .spot_list import SpotList

    # Reconstruct config from dict (avoid pickling dataclass with Optional fields)
    config = SpotFinderConfig(**{
        k: v for k, v in config_dict.items()
        if k in SpotFinderConfig.__dataclass_fields__
    })
    # Force CPU in workers (GPU contexts can't be shared across processes)
    config.force_cpu = True

    pipeline = SpotFinderPipeline(params_dict, config)

    # Open HDF5 files directly (avoid HDF5Reader Qt dependency in subprocesses)
    # Build frame_map from master file
    master_dir = os.path.dirname(master_file)
    prefix = os.path.basename(master_file).replace("_master.h5", "")
    images_per_hdf = params_dict.get("images_per_hdf", 1)

    results = {}
    mask = None

    for idx in frame_indices:
        # Determine which data file and local index
        file_num = idx // images_per_hdf + 1
        local_idx = idx % images_per_hdf
        data_file = os.path.join(master_dir, f"{prefix}_data_{file_num:06d}.h5")

        try:
            with h5py.File(data_file, "r") as f:
                for dset_path in ["/entry/data/data", "/entry/data/raw_data"]:
                    if dset_path in f:
                        frame = f[dset_path][local_idx]
                        break
                else:
                    continue
        except Exception:
            continue

        if mask is None:
            mask = pipeline._get_mask(frame)
            mask = mask | pipeline._get_resolution_mask()

        spots = pipeline.find_spots(frame, mask=mask)
        # Serialize to dict for cross-process transfer
        results[idx] = spots.to_dict()

    # Convert back to SpotList on return
    return {idx: SpotList.from_dict(d) for idx, d in results.items()}
