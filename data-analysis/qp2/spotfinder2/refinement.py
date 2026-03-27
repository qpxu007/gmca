"""MLE position refinement and TDS-aware intensity integration.

Provides sub-pixel centroid accuracy via Poisson maximum-likelihood estimation
(Cash statistic) and optional two-component profile fitting to separate
Bragg peak intensity from thermal diffuse scattering.
"""

import numpy as np
from scipy.optimize import minimize

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def _gaussian_2d(yy, xx, y0, x0, sigma):
    """Normalized 2D Gaussian PSF."""
    r2 = (xx - x0)**2 + (yy - y0)**2
    return np.exp(-0.5 * r2 / sigma**2) / (2 * np.pi * sigma**2)


def _cash_statistic(observed, model):
    """Cash C-statistic: sum(model - observed * log(model)).

    Proper Poisson negative log-likelihood (up to a constant).
    Handles zero-count pixels correctly.
    """
    model_safe = np.maximum(model, 1e-10)
    return np.sum(model_safe - observed * np.log(model_safe))


def refine_centroids(
    frame, background, spots, psf_sigma=1.0, cutout_radius=3, max_iterations=10,
):
    """MLE position refinement using Poisson likelihood (Cash statistic).

    For each spot, fits a 2D Gaussian PSF + background model to a local
    cutout, optimizing for (x0, y0, amplitude).

    Args:
        frame: 2D detector image
        background: 2D background estimate
        spots: SpotList with initial integer-pixel centroids
        psf_sigma: PSF sigma in pixels (default 1.0)
        cutout_radius: half-size of cutout window (default 3 → 7x7)
        max_iterations: L-BFGS-B iteration limit per spot

    Returns:
        Updated SpotList with sub-pixel positions and refined intensities.
    """
    from .spot_list import SpotList

    if spots.count == 0:
        return spots

    ny, nx = frame.shape
    frame_f = frame.astype(np.float64)
    bg_f = background.astype(np.float64)

    new_x = spots.x.copy()
    new_y = spots.y.copy()
    new_intensity = spots.intensity.copy()

    r = cutout_radius
    size = 2 * r + 1

    # Coordinate grid for cutout (reused across spots)
    yy_local, xx_local = np.mgrid[:size, :size]

    for i in range(spots.count):
        cx = int(round(spots.x[i]))
        cy = int(round(spots.y[i]))

        # Bounds check
        if cy - r < 0 or cy + r + 1 > ny or cx - r < 0 or cx + r + 1 > nx:
            continue

        # Extract cutout
        observed = frame_f[cy - r:cy + r + 1, cx - r:cx + r + 1]
        bg_cutout = bg_f[cy - r:cy + r + 1, cx - r:cx + r + 1]

        # Initial guess
        amplitude_guess = float(np.maximum(observed.max() - bg_cutout.mean(), 1.0))
        x0_guess = float(r)  # center of cutout
        y0_guess = float(r)

        def neg_log_likelihood(params):
            y0, x0, amp = params
            if amp < 0:
                return 1e20
            psf = _gaussian_2d(yy_local, xx_local, y0, x0, psf_sigma)
            model = bg_cutout + amp * psf
            return _cash_statistic(observed, model)

        try:
            result = minimize(
                neg_log_likelihood,
                x0=[y0_guess, x0_guess, amplitude_guess],
                method="L-BFGS-B",
                bounds=[(0, size - 1), (0, size - 1), (0, None)],
                options={"maxiter": max_iterations, "ftol": 1e-6},
            )
            if result.success or result.fun < neg_log_likelihood([y0_guess, x0_guess, amplitude_guess]):
                new_y[i] = cy - r + result.x[0]
                new_x[i] = cx - r + result.x[1]
                new_intensity[i] = result.x[2]
        except Exception:
            # Keep original position on failure
            pass

    # Build updated SpotList, preserving metadata from input
    refined = SpotList.from_arrays(
        x=new_x, y=new_y,
        intensity=new_intensity,
        background=spots.background,
        snr=spots.snr,
        resolution=spots.resolution,
        size=spots.size,
        aspect_ratio=spots.aspect_ratio,
        tds_intensity=spots.tds_intensity,
        flags=spots.flags,
    )
    refined.metadata = spots.metadata.copy()
    return refined


def integrate_with_tds(
    frame, background, spots, psf_sigma=1.0, tds_sigma=4.0, cutout_radius=5,
):
    """Two-component profile fitting: sharp Bragg + broad TDS Gaussian.

    Model: pixel = bg + A_bragg * G(sigma=psf_sigma) + A_tds * G(sigma=tds_sigma)

    Args:
        frame: 2D detector image
        background: 2D background estimate
        spots: SpotList with refined positions
        psf_sigma: Bragg PSF sigma (pixels)
        tds_sigma: TDS envelope sigma (pixels)
        cutout_radius: half-size of cutout (default 5 → 11x11)

    Returns:
        Updated SpotList with bragg_intensity and tds_intensity fields.
    """
    from .spot_list import SpotList, FLAG_TDS_FITTED

    if spots.count == 0:
        return spots

    ny, nx = frame.shape
    frame_f = frame.astype(np.float64)
    bg_f = background.astype(np.float64)

    new_intensity = spots.intensity.copy()
    new_tds = spots.tds_intensity.copy()
    new_flags = spots.flags.copy()

    r = cutout_radius
    size = 2 * r + 1
    yy_local, xx_local = np.mgrid[:size, :size]

    for i in range(spots.count):
        cx = int(round(spots.x[i]))
        cy = int(round(spots.y[i]))

        if cy - r < 0 or cy + r + 1 > ny or cx - r < 0 or cx + r + 1 > nx:
            continue

        observed = frame_f[cy - r:cy + r + 1, cx - r:cx + r + 1]
        bg_cutout = bg_f[cy - r:cy + r + 1, cx - r:cx + r + 1]

        # Sub-pixel position relative to cutout
        local_x = spots.x[i] - (cx - r)
        local_y = spots.y[i] - (cy - r)

        a_bragg_guess = float(max(spots.intensity[i], 1.0))
        a_tds_guess = a_bragg_guess * 0.1

        def neg_log_likelihood(params):
            a_bragg, a_tds = params
            if a_bragg < 0 or a_tds < 0:
                return 1e20
            psf_sharp = _gaussian_2d(yy_local, xx_local, local_y, local_x, psf_sigma)
            psf_broad = _gaussian_2d(yy_local, xx_local, local_y, local_x, tds_sigma)
            model = bg_cutout + a_bragg * psf_sharp + a_tds * psf_broad
            return _cash_statistic(observed, model)

        try:
            result = minimize(
                neg_log_likelihood,
                x0=[a_bragg_guess, a_tds_guess],
                method="L-BFGS-B",
                bounds=[(0, None), (0, None)],
                options={"maxiter": 20, "ftol": 1e-6},
            )
            if result.success:
                new_intensity[i] = result.x[0]
                new_tds[i] = result.x[1]
                new_flags[i] |= FLAG_TDS_FITTED
        except Exception:
            pass

    result_spots = SpotList.from_arrays(
        x=spots.x, y=spots.y,
        intensity=new_intensity,
        background=spots.background,
        snr=spots.snr,
        resolution=spots.resolution,
        size=spots.size,
        aspect_ratio=spots.aspect_ratio,
        tds_intensity=new_tds,
        flags=new_flags,
    )
    result_spots.metadata = spots.metadata.copy()
    return result_spots
