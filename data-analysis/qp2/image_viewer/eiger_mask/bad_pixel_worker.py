# qp2/image_viewer/eiger_mask/bad_pixel_worker.py

import numpy as np
from collections import Counter

from PyQt5.QtCore import QRunnable, QObject, pyqtSignal, pyqtSlot
from skimage.feature import peak_local_max
from skimage.morphology import disk

from qp2.xio.hdf5_manager import HDF5Reader
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class BadPixelSignals(QObject):
    """Defines signals for the BadPixelWorker."""

    finished = pyqtSignal(dict)  # Emits dictionary of results
    error = pyqtSignal(str)
    progress = pyqtSignal(str)


class BadPixelWorker(QRunnable):
    """
    Worker to perform optimized statistical analysis on a set of frames
    to find unreliable pixels using peak detection and persistence analysis.
    """

    def __init__(
        self,
        reader: HDF5Reader,
        frame_indices: list,
        detector_mask: np.ndarray,
        params: dict,
        max_results=100,
        peak_persistence_threshold=0.80,  # Must be peak in >80% of frames
        top_candidates=500,  # Number of top candidates to analyze in detail
    ):
        super().__init__()
        self.reader = reader
        self.frame_indices = frame_indices
        self.detector_mask = detector_mask
        self.params = params
        self.stack = None
        self.max_results = max_results
        self.peak_persistence_threshold = peak_persistence_threshold
        self.top_candidates = top_candidates
        self.signals = BadPixelSignals()

    @pyqtSlot()
    def run(self):
        try:
            # --- 1. Load Frames ---
            self.signals.progress.emit(
                f"Reading {len(self.frame_indices)} random frames..."
            )

            frames = []
            for idx in self.frame_indices:
                f = self.reader.get_frame(idx)
                if f is not None:
                    frames.append(f.astype(np.float32, copy=False))

            if len(frames) < 3:
                self.signals.error.emit("Could not read enough frames for analysis.")
                return

            num_frames = len(frames)
            stack = np.stack(frames, axis=0)
            self.stack = stack  # Keep for potential later analysis

            # Get valid pixel mask
            valid = (
                ~self.detector_mask
                if self.detector_mask is not None
                else np.ones(stack.shape[1:], dtype=bool)
            )

            # --- 2. Run peak_local_max on Each Frame (OPTIMIZED) ---
            self.signals.progress.emit("Finding local maxima in each frame...")

            # Calculate a reasonable threshold once (95th percentile of all data)
            # This is much faster than per-frame percentile
            all_valid_values = stack[:, valid].flatten()
            global_threshold = np.percentile(all_valid_values, 95)
            min_distance = 1
            del all_valid_values  # Free memory
            logger.info(f"global threshold: {global_threshold}")
            peak_coords_list = []
            for i, frame in enumerate(frames):
                # Apply mask by zeroing invalid pixels
                masked_frame = frame.copy()
                masked_frame[~valid] = 0

                # Find peaks with optimized parameters
                coords = peak_local_max(
                    masked_frame,
                    min_distance=1,  # Single pixel hot spots
                    num_peaks=self.top_candidates,
                    threshold_abs=global_threshold,
                    footprint=disk(max(1, min_distance // 2)),
                    exclude_border=False,
                )

                # Convert to tuples for hashing
                peak_coords_list.extend(map(tuple, coords))
                logger.debug(f"frame {i} out of {num_frames} done")

            # --- 3. Get Common Pixels (Persistent Peaks) ---
            self.signals.progress.emit("Identifying persistent peaks...")

            # Count how many times each pixel appeared as a peak
            peak_counts = Counter(peak_coords_list)

            if not peak_counts:
                self.signals.finished.emit(
                    {
                        "bad_pixel_coords": np.array([]),
                        "bad_pixel_reasons": [],
                        "bad_pixel_scores": [],
                        "warning": "No persistent peaks found.",
                    }
                )
                return

            # Filter to persistent peaks only
            min_peak_count = int(num_frames * self.peak_persistence_threshold)
            persistent_pixels = [
                (p, count)
                for p, count in peak_counts.items()
                if count >= min_peak_count
            ]

            if not persistent_pixels:
                self.signals.finished.emit(
                    {
                        "bad_pixel_coords": np.array([]),
                        "bad_pixel_reasons": [],
                        "bad_pixel_scores": [],
                        "warning": f"No pixels persisted in >{self.peak_persistence_threshold*100}% of frames.",
                    }
                )
                return

            # Sort by frequency (most persistent first)
            persistent_pixels.sort(key=lambda x: x[1], reverse=True)

            # --- 4. Pick Top N Candidates and Calculate Statistics ---
            self.signals.progress.emit(
                f"Analyzing top {min(self.top_candidates, len(persistent_pixels))} candidates..."
            )

            # Limit to top_candidates for detailed analysis
            candidates_to_analyze = persistent_pixels[: self.top_candidates]

            # Extract coordinates
            candidate_coords = np.array([p[0] for p in candidates_to_analyze])
            candidate_counts = np.array([p[1] for p in candidates_to_analyze])

            # Prepare normalization for stuck-high detection
            saturation = self.params.get("saturation_value")
            if saturation is None or saturation <= 0:
                saturation = 2 ** self.params.get("bit_depth", 16) - 1
            maxv = float(saturation)

            # Calculate detailed statistics for each candidate
            results_list = []

            for idx, (coord, peak_count) in enumerate(candidates_to_analyze):
                r, c = coord

                # Extract pixel time series
                pixel_series = stack[:, r, c]

                # Normalized series for stuck-high detection
                normalized_series = (
                    np.clip(pixel_series / maxv, 0.0, 1.0) if maxv > 0 else pixel_series
                )

                # Calculate statistics
                median_val = np.median(pixel_series)
                mad_val = np.median(np.abs(pixel_series - median_val))
                mean_val = np.mean(pixel_series)
                std_val = np.std(pixel_series)
                min_val = np.min(pixel_series)
                max_val = np.max(pixel_series)

                # Persistence score (0-1): fraction of frames where this is a peak
                persistence_score = peak_count / num_frames

                # --- Check Criteria ---
                reasons = []
                score_components = []

                # 1. Stuck High: >99.5% of saturation in >95% of frames
                stuck_high_threshold = 0.995
                stuck_high_fraction = 0.95
                is_stuck_high = (
                    np.mean(normalized_series > stuck_high_threshold)
                    > stuck_high_fraction
                )

                if is_stuck_high:
                    reasons.append("Stuck High")
                    score_components.append(2.0)  # High priority

                # 2. Persistent Peak: Already filtered for this
                reasons.append(f"Persistent Peak ({persistence_score:.1%})")
                score_components.append(persistence_score * 1.5)

                # 3. Low Variance (Stuck): std very low relative to mean
                if std_val < 1e-6 or (mean_val > 0 and std_val / mean_val < 0.001):
                    reasons.append("Low Variance/Stuck")
                    score_components.append(1.5)

                # 4. High Outlier Score: consistent deviation from local neighborhood median
                # Calculate robust z-score
                robust_sigma = 1.4826 * mad_val + 1e-12
                z_scores = np.abs(pixel_series - median_val) / robust_sigma
                outlier_fraction = np.mean(z_scores > 6.0)

                if outlier_fraction > 0.5:
                    reasons.append(f"Noisy Outlier ({outlier_fraction:.1%})")
                    score_components.append(outlier_fraction * 1.0)

                # 5. Always at maximum: all values at or near saturation
                if max_val >= saturation * 0.99 and min_val >= saturation * 0.95:
                    reasons.append("Always Saturated")
                    score_components.append(2.5)  # Highest priority

                # Calculate composite score
                composite_score = sum(score_components)

                results_list.append(
                    {
                        "coord": (r, c),
                        "score": composite_score,
                        "persistence": persistence_score,
                        "reasons": ", ".join(reasons),
                        "median": median_val,
                        "mad": mad_val,
                        "std": std_val,
                        "min": min_val,
                        "max": max_val,
                        "peak_count": peak_count,
                    }
                )

            # --- 5. Sort by Score (Most Likely to Least Likely) ---
            self.signals.progress.emit("Ranking results by likelihood...")

            results_list.sort(key=lambda x: x["score"], reverse=True)

            # Truncate to max_results
            if len(results_list) > self.max_results:
                warning_message = (
                    f"Found {len(results_list)} persistent peak candidates. "
                    f"Displaying the top {self.max_results}."
                )
                results_list = results_list[: self.max_results]
            else:
                warning_message = ""

            # --- 6. Format Output ---
            final_coords = np.array([r["coord"] for r in results_list])
            final_reasons = [r["reasons"] for r in results_list]
            final_scores = [r["score"] for r in results_list]

            results = {
                "bad_pixel_coords": final_coords,
                "bad_pixel_reasons": final_reasons,
                "bad_pixel_scores": final_scores,
                "warning": warning_message,
                "detailed_stats": results_list,  # Optional: for further analysis
            }

            self.signals.finished.emit(results)

        except Exception as e:
            logger.error(f"Error in BadPixelWorker: {e}", exc_info=True)
            self.signals.error.emit(str(e))
