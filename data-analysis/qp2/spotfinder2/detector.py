"""Detector geometry, q-maps, and mask handling for spotfinder2.

Computes per-pixel reciprocal-space coordinates, resolution, and azimuthal
angle from HDF5 detector parameters.
"""

import numpy as np
from typing import Optional, Dict, Any

from qp2.image_viewer.utils.ring_math import resolution_to_radius, radius_to_resolution
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class DetectorGeometry:
    """Immutable detector geometry computed from HDF5 parameters.

    Precomputes 2D maps for radius, q-vector magnitude, d-spacing,
    and azimuthal angle. These are used by background estimation,
    detection, and resolution assignment.

    Attributes:
        nx, ny: detector dimensions (pixels)
        beam_x, beam_y: beam center (pixels)
        wavelength: X-ray wavelength (Angstroms)
        det_dist: sample-to-detector distance (mm)
        pixel_size: pixel size (mm)
        radius_map: 2D distance from beam center (pixels)
        q_map: 2D |q| = 2*sin(theta)/wavelength (Å⁻¹)
        resolution_map: 2D d-spacing (Å)
        azimuth_map: 2D azimuthal angle [0, 2*pi)
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Args:
            params: dict from HDF5Reader.get_parameters() containing
                    wavelength, det_dist, pixel_size, beam_x, beam_y, nx, ny,
                    saturation_value, underload_value.
        """
        self.nx = int(params.get("nx", 1028))
        self.ny = int(params.get("ny", 1062))
        self.beam_x = float(params.get("beam_x", self.nx / 2))
        self.beam_y = float(params.get("beam_y", self.ny / 2))
        self.wavelength = float(params.get("wavelength", 1.0))
        self.det_dist = float(params.get("det_dist", 100.0))
        self.pixel_size = float(params.get("pixel_size", 0.075))
        self.saturation_value = params.get("saturation_value", 60000)
        self.underload_value = params.get("underload_value", -1)

        # Precompute coordinate grids
        yy, xx = np.ogrid[:self.ny, :self.nx]
        dx = (xx - self.beam_x).astype(np.float32)
        dy = (yy - self.beam_y).astype(np.float32)

        # Radius map (pixels from beam center)
        self.radius_map = np.sqrt(dx**2 + dy**2)

        # q-map: |q| = 2*sin(theta)/lambda where tan(2*theta) = R*px/D
        radius_mm = self.radius_map * self.pixel_size
        two_theta = np.arctan2(radius_mm, self.det_dist)
        self.q_map = (2.0 / self.wavelength) * np.sin(two_theta / 2.0)

        # Resolution map (d-spacing in Å), avoid division by zero
        with np.errstate(divide="ignore", invalid="ignore"):
            self.resolution_map = np.where(
                self.q_map > 1e-10, 1.0 / self.q_map, np.inf
            )

        # Azimuth map [0, 2*pi)
        self.azimuth_map = np.arctan2(dy, dx).astype(np.float32) % (2 * np.pi)

        logger.info(
            f"DetectorGeometry: {self.nx}x{self.ny}, beam=({self.beam_x:.1f}, {self.beam_y:.1f}), "
            f"dist={self.det_dist:.1f}mm, wl={self.wavelength:.5f}Å, "
            f"q_range=[{self.q_map[self.q_map > 0].min():.4f}, {self.q_map.max():.4f}] Å⁻¹"
        )

    def res_to_radius(self, d_spacing: float) -> float:
        """Convert d-spacing (Å) to radius (pixels)."""
        return resolution_to_radius(d_spacing, self.wavelength,
                                    self.det_dist, self.pixel_size)

    def radius_to_res(self, radius_px: float) -> float:
        """Convert radius (pixels) to d-spacing (Å)."""
        return radius_to_resolution(self.wavelength, self.det_dist,
                                    radius_px, self.pixel_size)

    def q_to_radius(self, q: float) -> float:
        """Convert |q| (Å⁻¹) to radius (pixels)."""
        d = 1.0 / q if q > 0 else np.inf
        return self.res_to_radius(d)

    def make_annular_mask(self, d_low: float, d_high: float) -> np.ndarray:
        """Boolean mask for resolution range [d_high, d_low] (d_high < d_low).

        Note: lower d-spacing = higher resolution = larger radius.
        Returns True for pixels IN the annulus.
        """
        r_inner = self.res_to_radius(d_low) if np.isfinite(d_low) else 0.0
        r_outer = self.res_to_radius(d_high) if np.isfinite(d_high) else self.radius_map.max()
        return (self.radius_map >= r_inner) & (self.radius_map <= r_outer)


def build_mask(
    image: np.ndarray,
    geometry: DetectorGeometry,
    mask_values: Optional[set] = None,
    masked_circles: Optional[list] = None,
    masked_rectangles: Optional[list] = None,
) -> np.ndarray:
    """Build boolean detector mask (True = masked/invalid pixel).

    Adapted from qp2.image_viewer.utils.mask_computation pattern.

    Args:
        image: 2D detector image
        geometry: DetectorGeometry instance
        mask_values: set of pixel values to mask (e.g. {-1, -2, 65535})
        masked_circles: list of (cx, cy, radius) tuples
        masked_rectangles: list of (x0, y0, x1, y1) tuples

    Returns:
        Boolean mask, True = masked pixel.
    """
    ny, nx = image.shape
    mask = np.zeros((ny, nx), dtype=bool)

    # Mask by pixel value
    if mask_values:
        for val in mask_values:
            mask |= (image == val)

    # Mask by saturation
    if geometry.saturation_value is not None:
        mask |= (image >= geometry.saturation_value)

    # Mask by underload
    if geometry.underload_value is not None:
        mask |= (image <= geometry.underload_value)

    # Mask circles
    if masked_circles:
        yy, xx = np.ogrid[:ny, :nx]
        for cx, cy, r in masked_circles:
            dist_sq = (xx - cx)**2 + (yy - cy)**2
            mask |= (dist_sq <= r**2)

    # Mask rectangles
    if masked_rectangles:
        for x0, y0, x1, y1 in masked_rectangles:
            x0, x1 = int(max(0, min(x0, x1))), int(min(nx, max(x0, x1)))
            y0, y1 = int(max(0, min(y0, y1))), int(min(ny, max(y0, y1)))
            mask[y0:y1, x0:x1] = True

    return mask
