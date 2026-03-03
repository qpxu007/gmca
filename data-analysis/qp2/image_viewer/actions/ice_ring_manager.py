import logging
import numpy as np
from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import pyqtSlot

from qp2.image_viewer.utils.ring_math import angstrom_to_pixels

logger = logging.getLogger(__name__)

class IceRingManager(QtCore.QObject):
    """
    Manages Ice Ring Analysis and De-icing operations.
    """

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.ui_manager = main_window.ui_manager
        # self.graphics_manager deferred to property to avoid init order issues
        
        # State
        self._ice_ring_dialog = None
        
    @property
    def graphics_manager(self):
        return self.main_window.graphics_manager

    def run_ice_ring_analysis(self):
        """Triggers the ice ring analysis on the current image."""
        if self.main_window._original_image is None:
            self.ui_manager.show_status_message(
                "No image loaded for ice ring analysis.", 3000
            )
            return

        required_params = ["beam_x", "beam_y", "wavelength", "det_dist", "pixel_size"]
        params = self.main_window.params
        if not all(p in params for p in required_params):
            self.ui_manager.show_warning_message(
                "Missing Parameters",
                "Cannot run ice ring analysis without full detector geometry (beam center, wavelength, distance, pixel size).",
            )
            return

        self.ui_manager.show_status_message("Running ice ring analysis...", 0)
        from qp2.image_viewer.workers.ice_ring_analyzer import IceRingWorker

        worker = IceRingWorker(
            image=self.main_window._original_image,
            detector_mask=self.main_window.detector_mask,
            params=params,
        )
        worker.signals.finished.connect(self._handle_ice_ring_result)
        # Assuming _handle_ice_ring_error does not exist in extracted code (removed/replaced?) 
        # In original code: worker.signals.error.connect(self._handle_ice_ring_error)
        # But I didn't see _handle_ice_ring_error in my quick scan? 
        # Wait, I saw it in line 1680: worker.signals.error.connect(self._handle_ice_ring_error)
        # I need to implement that too or use a generic one.
        # I'll re-check image_viewer.py for _handle_ice_ring_error or just log it.
        # Actually I saw worker.signals.error.connect(lambda err_msg, w=worker: self._on_worker_finished(w))
        # The original code has _handle_ice_ring_error AND lambda.
        
        # For now I will just log error in a lambda or simple method here.
        worker.signals.error.connect(self._handle_ice_ring_error)
        
        # Also connect to main_window._on_worker_finished for tracking
        worker.signals.finished.connect(
            lambda result, w=worker: self.main_window._on_worker_finished(w)
        )
        worker.signals.error.connect(
            lambda err_msg, w=worker: self.main_window._on_worker_finished(w)
        )
        self.main_window.active_workers.add(worker)

        self.main_window.threadpool.start(worker)

    def _handle_ice_ring_error(self, error_msg):
        logger.error(f"Ice Ring Analysis Error: {error_msg}")
        self.ui_manager.show_warning_message("Ice Ring Analysis Failed", error_msg)

    @pyqtSlot(dict)
    def _handle_ice_ring_result(self, result: dict):
        """Displays the results of the ice ring analysis."""
        self.ui_manager.clear_status_message_if("Running ice ring analysis")

        rings_found = result.get("ice_rings_found", [])
        profile_data = result.get("radial_profile")
        
        # Initial feedback
        if rings_found:
            rings_str = ", ".join([f"{r}Å" for r in rings_found])
            self.ui_manager.show_status_message(
                f"Ice rings detected near: {rings_str}", 6000
            )
        else:
            self.ui_manager.show_status_message(
                "No significant ice rings were detected.", 4000
            )

        if profile_data:
            from qp2.image_viewer.ui.ice_ring_dialog import IceRingResultsDialog
            
            # Create and show the interactive dialog
            # Note: We store it as an attribute to prevent garbage collection
            # Pass full result to allow access to multiple profiles
            self._ice_ring_dialog = IceRingResultsDialog(result, self.main_window.params, parent=self.main_window)
            self._ice_ring_dialog.apply_rings.connect(self._apply_ice_rings_to_view)
            self._ice_ring_dialog.deice_request.connect(self._handle_deice_request)
            self._ice_ring_dialog.show()

    def _handle_deice_request(self, profile_data, rings_details, params=None):
        """
        Subtracts the radial profile from the image within detected ice rings to remove them.
        Uses a linear baseline subtraction to preserve the underlying continuum.
        """
        if self.main_window._original_image is None or profile_data is None:
            return

        self.ui_manager.show_status_message("De-icing image (selective)...", 0)
        
        try:
            bin_centers, radial_profile = profile_data
            # Convert to numpy arrays for masking operations
            bin_centers = np.array(bin_centers)
            radial_profile = np.array(radial_profile)
            
            # Use provided params (from analysis time) if available to ensure consistency
            use_params = params if params is not None else self.main_window.params
            
            # Get geometry
            beam_x = use_params.get("beam_x")
            beam_y = use_params.get("beam_y")
            wl = use_params.get("wavelength")
            det_dist = use_params.get("det_dist")
            px_size = use_params.get("pixel_size")
            
            logger.info(f"De-icing with Beam Center: ({beam_x:.2f}, {beam_y:.2f})")
            
            if beam_x is None or beam_y is None:
                logger.error("De-icing failed: Missing beam center coordinates.")
                return

            # Calculate radius for every pixel using the consistent beam center
            y, x = np.indices(self.main_window._original_image.shape)
            radii = np.sqrt((x - beam_x) ** 2 + (y - beam_y) ** 2)
            
            # Interpolate background (this is the full radial profile projected to 2D)
            background = np.interp(radii, bin_centers, radial_profile, left=0, right=0)
            
            # Start with original, converted to float to handle subtraction
            deiced = self.main_window._original_image.astype(np.float32)
            
            processed_rings_count = 0
            
            logger.info(f"Starting de-ice for {len(rings_details)} rings.")

            for i, ring in enumerate(rings_details):
                r = ring.get("radius_pixels", 0)
                w = ring.get("width_pixels", 0)
                res = ring.get("resolution", 0)
                r_type = ring.get("type", "Unknown")
                
                if r > 0 and w > 0:
                    # --- ADAPTIVE WIDTH LIMITING ---
                    effective_w = w  # Preserve natural width by default
                    limit_px = 50.0  # Safe fallback
                    
                    # Calculate pixel width corresponding to 0.1 Angstrom at this resolution
                    if res > 0 and all(v is not None for v in [wl, det_dist, px_size]):
                        try:
                            # Width = delta_R for delta_Res = 0.1A (centered at ring)
                            r_outer = angstrom_to_pixels(max(0.1, res - 0.05), wl, det_dist, px_size)
                            r_inner = angstrom_to_pixels(res + 0.05, wl, det_dist, px_size)
                            limit_px = abs(r_outer - r_inner)
                        except Exception as e:
                            logger.warning(f"Could not calculate Angstrom width limit: {e}")
                    
                    # Only cap if the detected width is "too wide" (> 0.1A)
                    if effective_w > limit_px:
                        effective_w = limit_px
                        
                    half_width = effective_w / 2.0
                    r_min, r_max = r - half_width, r + half_width
                    
                    logger.info(f"De-ice Ring {i+1} [{r_type}]: Res={res}A, R={r:.1f}px, W_orig={w:.1f}px, W_used={effective_w:.1f}px")

                    # Find indices in profile corresponding to edges
                    # Use searchsorted to find nearest bin indices
                    idx_min = np.searchsorted(bin_centers, r_min)
                    idx_max = np.searchsorted(bin_centers, r_max)
                    
                    # Ensure indices are within bounds
                    idx_min = max(0, min(idx_min, len(radial_profile) - 1))
                    idx_max = max(0, min(idx_max, len(radial_profile) - 1))
                    
                    # STABILIZED BASELINE: Average 3 bins at edges to avoid noise
                    def get_smooth_val(idx, profile, radius=1):
                        i0 = max(0, idx - radius)
                        i1 = min(len(profile), idx + radius + 1)
                        return np.mean(profile[i0:i1])

                    val_start = get_smooth_val(idx_min, radial_profile)
                    val_end = get_smooth_val(idx_max, radial_profile)
                    
                    # Calculate linear baseline parameters: I = slope * r + intercept
                    if r_max != r_min:
                        slope = (val_end - val_start) / (r_max - r_min)
                    else:
                        slope = 0
                    
                    # Intercept at r=0 (extrapolated) for the line equation y - y1 = m(x - x1)
                    # y = m*x - m*x1 + y1  => intercept = y1 - m*x1
                    intercept = val_start - slope * r_min
                    
                    logger.info(f"  Baseline: StartI={val_start:.1f}, EndI={val_end:.1f}, Slope={slope:.4f}, Intercept={intercept:.1f}")

                    # Create mask for this specific ring annulus
                    ring_mask = (radii >= r_min) & (radii <= r_max)
                    pixel_count = np.sum(ring_mask)
                    
                    if pixel_count == 0:
                        logger.warning(f"  Ring {i+1} mask is empty. Skipping.")
                        continue
                        
                    # Get radii and background values for pixels in this ring
                    r_in_region = radii[ring_mask]
                    bg_in_region = background[ring_mask]
                    
                    # Calculate the linear continuum (baseline) at these radii
                    baseline_values = slope * r_in_region + intercept
                    
                    # Calculate excess: The "bump" above the linear baseline
                    excess = bg_in_region - baseline_values
                    
                    logger.info(f"  Excess Stats: Min={np.min(excess):.1f}, Max={np.max(excess):.1f}, Mean={np.mean(excess):.1f}")

                    # Clip excess to 0. We only want to remove positive bumps (ice), not dips.
                    excess = np.maximum(excess, 0)
                    
                    # Soft Subtraction: Multiply excess by 0.95 to leave a faint trace
                    # This bridges the gap visually and makes the subtraction look more natural
                    excess *= 0.95
                    
                    # Subtract the excess from the image
                    deiced[ring_mask] -= excess
                    processed_rings_count += 1

            # Final clip to ensure no negative intensities (fixing visual artifacts)
            deiced = np.maximum(deiced, 0)

            # Store as filtered image
            # Note: This modifies main_window state!
            self.main_window._filtered_image = deiced
            self.main_window.image_filter_enabled = True
            
            # Update display
            # We call the internal method of main_window used for this
            # Or better, replicate it if it's small, or expose it.
            # _display_final_image is internal. Let's assume it's exposed or we access private.
            self.main_window._display_final_image(self.main_window._filtered_image)
            
            self.graphics_manager.show_filter_label("De-iced (Selective)")
            self.ui_manager.show_status_message(f"De-iced {processed_rings_count} rings.", 4000)
            
        except Exception as e:
            logger.error(f"Error during de-icing: {e}", exc_info=True)
            self.ui_manager.show_warning_message("De-icing Failed", str(e))

    def _apply_ice_rings_to_view(self, rings_px_list, summary_text):
        """Callback to draw rings on the main image."""
        beam_x = self.main_window.params.get("beam_x")
        beam_y = self.main_window.params.get("beam_y")
        
        if beam_x is not None and beam_y is not None:
            self.graphics_manager.display_ice_rings(beam_x, beam_y, rings_px_list)
            self.graphics_manager.display_ice_ring_summary(summary_text)
            self.ui_manager.show_status_message(f"Applied ice rings: {summary_text}", 3000)
