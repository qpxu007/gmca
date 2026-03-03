# qp2/image_viewer/workers/ice_ring_analyzer.py

import numpy as np
from PyQt5.QtCore import QRunnable, QObject, pyqtSignal
from scipy import signal as sp_signal

from qp2.image_viewer.utils.ring_math import radius_to_resolution
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


# --- Signals for the Worker ---


class IceRingSignals(QObject):
    """Defines the signals available from the IceRingWorker."""

    finished = pyqtSignal(dict)  # Emits a dictionary with results
    error = pyqtSignal(str)  # Emits an error message string


# --- The Worker Class ---


class IceRingWorker(QRunnable):
    """
    A QRunnable worker that performs ice ring analysis in a separate thread.
    """

    def __init__(self, image: np.ndarray, detector_mask: np.ndarray, params: dict, sensitivity: float = 1.0):
        super().__init__()
        self.image = image
        self.detector_mask = detector_mask
        self.params = params
        self.sensitivity = sensitivity
        self.signals = IceRingSignals()

    def run(self):
        """The main execution method of the worker."""
        try:
            results = analyze_for_ice_rings(self.image, self.detector_mask, self.params, self.sensitivity)
            self.signals.finished.emit(results)
        except Exception as e:
            logger.error(f"IceRingWorker failed: {e}", exc_info=True)
            self.signals.error.emit(str(e))


# --- Core Computational Function ---


def calculate_radial_profile(image: np.ndarray, detector_mask: np.ndarray, params: dict):
    """
    Calculates the 1D radial profile (Mean) from the 2D image.
    Returns: (bin_centers, profile_mean)
    """
    beam_x = params.get("beam_x")
    beam_y = params.get("beam_y")
    if beam_x is None or beam_y is None:
        raise ValueError("Beam center (beam_x, beam_y) not found in parameters.")

    # 1. Calculate radial profile
    y, x = np.indices(image.shape)
    radii = np.sqrt((x - beam_x) ** 2 + (y - beam_y) ** 2)

    # Apply detector mask
    valid_pixels = (
        ~detector_mask if detector_mask is not None else np.ones_like(image, dtype=bool)
    )

    # Flatten arrays for binning
    radii_flat = radii[valid_pixels]
    intensities_flat = image[valid_pixels]

    # Bin the data
    r_max = np.max(radii_flat)
    bin_size = 1.0  # 1 pixel bin width
    bins = np.arange(0, r_max + bin_size, bin_size)

    # --- Mean Profile (Fast) ---
    intensity_sum, _ = np.histogram(radii_flat, bins=bins, weights=intensities_flat)
    pixel_counts, _ = np.histogram(radii_flat, bins=bins)

    # Avoid division by zero
    valid_bins = pixel_counts > 0
    bin_centers = (bins[:-1] + bins[1:]) / 2
    radial_profile = np.zeros_like(bin_centers)
    radial_profile[valid_bins] = intensity_sum[valid_bins] / pixel_counts[valid_bins]
    
    return bin_centers, radial_profile


def find_and_classify_ice_rings(
    bin_centers: np.ndarray, 
    radial_profile: np.ndarray, 
    params: dict, 
    sensitivity: float = 1.0, 
    max_d_spacing: float = 4.0
) -> dict:
    """
    Finds peaks in the radial profile and classifies them as ice rings.
    
    Args:
        ...
        max_d_spacing: Peaks with resolution (d-spacing) greater than this value 
                       (i.e., lower resolution) will be ignored. Default 4.0 A.
    """
    # 2. Find peaks in the 1D profile
    # Sensitivity controls prominence: Higher sensitivity -> Lower prominence threshold
    # Base prominence is roughly the standard deviation
    base_prominence = np.std(radial_profile)
    # Avoid division by zero or negative sensitivity
    safe_sensitivity = max(0.1, sensitivity)
    prominence_threshold = base_prominence / safe_sensitivity

    peaks, _ = sp_signal.find_peaks(
        radial_profile, prominence=prominence_threshold, width=2
    )
    
    if peaks.size == 0:
        return {
            "ice_rings_found": [],
            "rings_details": [],
            "inference": "Clean",
            "radial_profile": (bin_centers.tolist(), radial_profile.tolist()),
            "message": "No prominent rings found.",
        }

    # Calculate peak widths (at 95% height)
    widths_px, _, _, _ = sp_signal.peak_widths(radial_profile, peaks, rel_height=0.95)
    
    peak_radii_px = bin_centers[peaks]

    # 3. Convert peak radii to resolution and identify ice rings
    # Reference values for Ice Rings (Angstroms)
    # Hexagonal Ice (Ih) - Common in cryo-EM/MX
    hex_ice_rings = [
        3.897,  # Very strong
        3.669,  # Very strong
        3.441,  # Very strong
        2.671,  # Medium
        2.249,  # Very strong
        2.072,  # Strong
        1.948,  # Weak
        1.918,  # Strong
        1.883,  # Weak
        1.721,  # Weak
        1.525,  # Medium
        1.473,  # Medium
        1.445,  # Medium
        1.367,  # Medium
        1.299,  # Medium
        1.261,  # Medium
        1.225,  # Weak
    ]

    # Cubic Ice (Ic) - Metastable
    cubic_ice_rings = [
        3.670,  # Very strong
        2.246,  # Strong
        1.916,  # Medium
        1.834,  # Very weak
        1.592,  # Very weak
        1.458,  # Medium
        1.297,  # Medium
        1.223,  # Weak
    ]

    tolerance = 0.05  # Resolution tolerance in Angstroms

    found_rings_info = []
    hex_matches_count = 0
    cubic_matches_count = 0

    for i, r_px in enumerate(peak_radii_px):
        try:
            res_a = radius_to_resolution(
                params["wavelength"], params["det_dist"], r_px, params["pixel_size"]
            )
            
            # Filter low resolution peaks (high d-spacing)
            if res_a > max_d_spacing:
                continue

            # Calculate width in Angstroms
            # Approximate by taking resolution at +/- half width
            w_px = widths_px[i]
            r_inner = max(0.1, r_px - w_px / 2) # Safety against negative radius
            r_outer = r_px + w_px / 2
            
            res_inner = radius_to_resolution(
                params["wavelength"], params["det_dist"], r_inner, params["pixel_size"]
            )
            res_outer = radius_to_resolution(
                params["wavelength"], params["det_dist"], r_outer, params["pixel_size"]
            )
            width_a = abs(res_inner - res_outer)

            # Identify ring type
            is_hex = False
            is_cubic = False
            matched_refs = []

            # Check Hex
            for ref in hex_ice_rings:
                if abs(res_a - ref) < tolerance:
                    is_hex = True
                    matched_refs.append(f"Hex({ref})")
                    break # Count once per type
            
            # Check Cubic
            for ref in cubic_ice_rings:
                if abs(res_a - ref) < tolerance:
                    is_cubic = True
                    matched_refs.append(f"Cubic({ref})")
                    break

            ring_type = "Unknown"
            if is_hex and is_cubic:
                ring_type = "Both/Ambiguous"
                hex_matches_count += 1
                cubic_matches_count += 1
            elif is_hex:
                ring_type = "Hexagonal Ice"
                hex_matches_count += 1
            elif is_cubic:
                ring_type = "Cubic Ice"
                cubic_matches_count += 1
            
            found_rings_info.append({
                "resolution": round(res_a, 3),
                "radius_pixels": round(r_px, 1),
                "width_pixels": round(w_px, 1),
                "width_angstrom": round(width_a, 3),
                "type": ring_type,
                "matched_reference": ", ".join(matched_refs) if matched_refs else None
            })

        except (KeyError, ZeroDivisionError) as e:
            logger.warning(f"Could not convert radius {r_px} to resolution: {e}")
            continue

    # Infer overall nature
    inference = "Clean"
    found_resolutions = [r["resolution"] for r in found_rings_info]
    
    if found_rings_info:
        if hex_matches_count > 0 and cubic_matches_count == 0:
            inference = "Hexagonal Ice Detected"
        elif cubic_matches_count > 0 and hex_matches_count == 0:
            inference = "Cubic Ice Detected"
        elif hex_matches_count > 0 and cubic_matches_count > 0:
            # Check for distinguishing peaks
            # Hex specific strong peaks: 3.897, 3.441
            has_strong_hex_specific = any(
                any(abs(r["resolution"] - ref) < tolerance for ref in [3.897, 3.441]) 
                for r in found_rings_info
            )
            if has_strong_hex_specific:
                inference = "Predominantly Hexagonal Ice (with overlapping Cubic peaks)"
            else:
                inference = "Mixed/Ambiguous Ice (Hexagonal + Cubic)"
        else:
            inference = "Unknown Rings Detected"

    message = f"{inference}. Found {len(found_rings_info)} ring(s)."

    return {
        "ice_rings_found": sorted(list(set(found_resolutions))),  # Simple list for backward compatibility
        "rings_details": found_rings_info, # Detailed info
        "inference": inference,
        "radial_profile": (bin_centers.tolist(), radial_profile.tolist()),
        "message": message,
    }


def analyze_for_ice_rings(
        image: np.ndarray, detector_mask: np.ndarray, params: dict, sensitivity: float = 1.0, max_d_spacing: float = 4.0
) -> dict:
    """
    Orchestrates the ice ring analysis.
    """
    bin_centers, radial_profile = calculate_radial_profile(image, detector_mask, params)
    return find_and_classify_ice_rings(bin_centers, radial_profile, params, sensitivity, max_d_spacing)
