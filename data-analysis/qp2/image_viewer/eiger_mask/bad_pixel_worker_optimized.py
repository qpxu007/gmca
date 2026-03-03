# qp2/image_viewer/eiger_mask/bad_pixel_worker.py

import numpy as np
from PyQt5.QtCore import QRunnable, QObject, pyqtSignal, pyqtSlot
from skimage.feature import peak_local_max
from collections import Counter
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
    Worker to perform robust statistical analysis on a set of frames
    to find unreliable pixels using an optimized peak-based approach.
    
    Optimized workflow:
    1. Run peak_local_max on each frame
    2. Find common pixels across frames
    3. Pick top N candidates and calculate detailed statistics
    4. Check whether each is a hot/stuck pixel
    5. Output results sorted by likelihood (most to least likely)
    """

    def __init__(
        self,
        reader: HDF5Reader,
        frame_indices: list,
        detector_mask: np.ndarray,
        params: dict,
        max_results=100,
    ):
        super().__init__()
        self.reader = reader
        self.frame_indices = frame_indices
        self.detector_mask = detector_mask
        self.params = params
        self.max_results = max_results
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
            
            stack = np.stack(frames, axis=0)
            num_frames = len(frames)
            
            # Get saturation value for normalization
            saturation = self.params.get("saturation_value")
            if saturation is None or saturation <= 0:
                saturation = 2 ** self.params.get("bit_depth", 16) - 1
            
            # Get valid mask (unmasked pixels)
            valid_mask = (
                ~self.detector_mask
                if self.detector_mask is not None
                else np.ones(stack.shape[1:], dtype=bool)
            )
            
            # --- 2. Run peak_local_max on each frame and collect candidates ---
            self.signals.progress.emit("Finding local peaks in all frames...")
            
            peak_coords_list = []
            for i, frame in enumerate(frames):
                # Apply mask to frame
                masked_frame = frame.copy()
                masked_frame[~valid_mask] = 0
                
                # Use adaptive threshold based on frame statistics
                threshold = np.percentile(masked_frame[valid_mask], 95)
                
                # Find local peaks
                coords = peak_local_max(
                    masked_frame,
                    min_distance=2,
                    threshold_abs=threshold
                )
                
                # Convert to list of tuples for counting
                peak_coords_list.extend(map(tuple, coords))
            
            # --- 3. Find common pixels (persistent peaks) ---
            self.signals.progress.emit("Identifying persistent peaks...")
            
            # Count how many times each pixel appeared as a peak
            peak_counts = Counter(peak_coords_list)
            
            if not peak_counts:
                self.signals.error.emit("No peaks found in any frame.")
                return
            
            # Calculate persistence score (fraction of frames where pixel is a peak)
            peak_persistence = {
                coord: count / num_frames 
                for coord, count in peak_counts.items()
            }
            
            # --- 4. Select top N candidates for detailed analysis ---
            # Sort by persistence score (most persistent first)
            sorted_candidates = sorted(
                peak_persistence.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            # Limit to top candidates for detailed analysis (efficiency)
            analysis_limit = min(self.max_results * 3, len(sorted_candidates))
            top_candidates = sorted_candidates[:analysis_limit]
            
            if not top_candidates:
                self.signals.error.emit("No persistent peaks found.")
                return
            
            self.signals.progress.emit(
                f"Analyzing top {len(top_candidates)} candidates..."
            )
            
            # --- 5. Calculate detailed statistics for each candidate ---
            candidate_data = []
            
            for (r, c), persistence in top_candidates:
                # Extract pixel time series
                pixel_series = stack[:, r, c]
                
                # Normalize to [0, 1] range
                normalized_series = np.clip(pixel_series / saturation, 0.0, 1.0)
                
                # Calculate statistics
                median_val = np.median(normalized_series)
                mad_val = np.median(np.abs(normalized_series - median_val))
                std_val = np.std(normalized_series)
                min_val = np.min(normalized_series)
                max_val = np.max(normalized_series)
                mean_val = np.mean(normalized_series)
                
                # Check for stuck high condition
                STUCK_HIGH_THRESHOLD = 0.995
                STUCK_HIGH_FRACTION = 0.95
                fraction_high = (normalized_series > STUCK_HIGH_THRESHOLD).mean()
                is_stuck_high = fraction_high > STUCK_HIGH_FRACTION
                
                # Check for low variance (stuck pixel)
                is_stuck_low_variance = std_val < 1e-6
                
                # Check for consistently high values (hot pixel)
                is_hot = median_val > 0.9 and persistence > 0.7
                
                # Calculate outlier score using MAD
                robust_sigma = 1.4826 * (mad_val + 1e-12)
                
                # Global median for comparison
                global_median = np.median(stack[:, valid_mask])
                z_score = abs(median_val - global_median / saturation) / (robust_sigma + 1e-12)
                
                # --- 6. Compute composite likelihood score ---
                # Higher score = more likely to be a bad pixel
                likelihood_score = 0.0
                reasons = []
                
                if is_stuck_high:
                    likelihood_score += 100.0
                    reasons.append("Stuck High")
                
                if is_stuck_low_variance:
                    likelihood_score += 80.0
                    reasons.append("Stuck/Zero Variance")
                
                if is_hot:
                    likelihood_score += 70.0
                    reasons.append("Hot Pixel")
                
                # Persistence weight (more persistent = more likely bad)
                likelihood_score += persistence * 50.0
                if persistence > 0.8:
                    reasons.append(f"Persistent Peak ({persistence:.1%})")
                
                # High median value weight
                if median_val > 0.8:
                    likelihood_score += 30.0
                
                # Z-score weight (statistical outlier)
                if z_score > 6.0:
                    likelihood_score += 20.0
                    reasons.append("Statistical Outlier")
                
                # Store candidate data
                candidate_data.append({
                    'coord': (r, c),
                    'likelihood_score': likelihood_score,
                    'persistence': persistence,
                    'median': median_val,
                    'std': std_val,
                    'max': max_val,
                    'reasons': reasons if reasons else ["Low Confidence"]
                })
            
            # --- 7. Sort by likelihood score (most likely to least likely) ---
            candidate_data.sort(key=lambda x: x['likelihood_score'], reverse=True)
            
            # --- 8. Prepare final results ---
            final_count = min(self.max_results, len(candidate_data))
            final_candidates = candidate_data[:final_count]
            
            final_coords = np.array([c['coord'] for c in final_candidates])
            final_reasons = [
                f"{', '.join(c['reasons'])} (score: {c['likelihood_score']:.1f}, persistence: {c['persistence']:.1%})"
                for c in final_candidates
            ]
            
            warning_message = ""
            if len(candidate_data) > self.max_results:
                warning_message = (
                    f"Found {len(candidate_data)} candidates. "
                    f"Displaying top {final_count} by likelihood."
                )
            
            results = {
                "bad_pixel_coords": final_coords,
                "bad_pixel_reasons": final_reasons,
                "warning": warning_message,
            }
            
            self.signals.finished.emit(results)
            
        except Exception as e:
            logger.error(f"Error in BadPixelWorker: {e}", exc_info=True)
            self.signals.error.emit(str(e))
