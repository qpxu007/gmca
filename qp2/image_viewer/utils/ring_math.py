# qp2/image_viewer/utils/ring_math.py
"""
Utility functions for detector ring calculations (resolution, radius, energy, etc).
"""
import numpy as np

"""
1. calculate the resolution d at a given radius R on a detector in crystallography
D is sample to detector distance, R is radius of from beamcenter to signal
d = λ / (2 * sin(0.5 * arctan(R/D)))

2. given resolution, calculate R
R = D * tan(2 * arcsin(λ / (2d)))
"""

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def resolution_to_radius(resolution, wavelength, distance, pixel_size=None):
    """Convert d-spacing in Angstroms to detector radius, in pixels if pixel size is provided."""
    R = distance * np.tan(2 * np.arcsin(wavelength / (2.0 * resolution)))
    if pixel_size:
        R /= pixel_size
    return R


def radius_to_resolution(wavelength, distance, radius, pixel_size, round=2):
    """Convert detector radius (pixels) to d-spacing (Angstroms)."""
    if pixel_size:
        radius = radius * pixel_size
    theta = 0.5 * np.arctan(radius / distance)
    d = wavelength / (2 * np.sin(theta))
    return np.round(d, round)


def resolution_to_distance(resolution, wavelength, radius, pixel_size, round=5):
    """Given resolution, wavelength, and ring radius, calculate detector distance (mm)."""
    if pixel_size:
        radius = radius * pixel_size
    two_theta = 2 * np.arcsin(wavelength / (2 * resolution))
    dist = radius / np.tan(two_theta)
    return np.round(dist, round)


def resolution_to_energy(resolution, distance, radius, pixel_size, round=4):
    """Given resolution, distance, and ring radius, calculate energy (eV)."""
    if pixel_size:
        radius = radius * pixel_size
    two_theta = np.arctan(radius / distance)
    d = resolution
    wavelength = 2 * d * np.sin(two_theta / 2)
    energy = 12398.4 / wavelength if wavelength != 0 else 0
    return np.round(energy, round)


angstrom_to_pixels = resolution_to_radius
