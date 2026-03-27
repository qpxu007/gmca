"""Poisson CDF threshold tables for spotfinder2.

Precomputes lookup tables mapping expected background count to detection
threshold, using exact Poisson CDF for low counts and Gaussian approximation
for high counts.
"""

import numpy as np
from scipy import stats

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class ThresholdTable:
    """Precomputed Poisson CDF threshold lookup.

    For expected value mu, threshold is the smallest k such that
    P(X >= k | mu) < p_false_alarm.

    For mu > gaussian_crossover, uses Gaussian approximation:
        threshold = mu + z_alpha * sqrt(mu)

    Usage:
        table = ThresholdTable(p_false_alarm=1e-5)
        thresholds = table(background_array)          # per-pixel threshold
        box_thresh = table.for_box_sum(bg, box_size)  # box-sum threshold
    """

    def __init__(
        self,
        p_false_alarm: float = 1e-5,
        max_mu: int = 1000,
        mu_step: float = 0.1,
        gaussian_crossover: int = 100,
    ):
        self.p_false_alarm = p_false_alarm
        self.gaussian_crossover = gaussian_crossover
        self.mu_step = mu_step

        # Gaussian z-score for the given false alarm rate
        self.z_alpha = stats.norm.isf(p_false_alarm)

        # Build Poisson lookup table — fully vectorized
        n_entries = int(max_mu / mu_step) + 1
        self._mu_values = np.arange(n_entries) * mu_step
        self._thresholds = self._build_table(self._mu_values, p_false_alarm,
                                              gaussian_crossover, self.z_alpha)

        # Box-sum table (up to max_mu * 5x5 box)
        max_box_mu = max_mu * 25
        n_box = int(max_box_mu / mu_step) + 1
        self._box_mu_values = np.arange(n_box) * mu_step
        self._box_thresholds = self._build_table(self._box_mu_values, p_false_alarm,
                                                  gaussian_crossover, self.z_alpha)

        logger.info(
            f"ThresholdTable: p={p_false_alarm}, z_alpha={self.z_alpha:.2f}, "
            f"{n_entries} Poisson entries, {n_box} box-sum entries"
        )

    @staticmethod
    def _build_table(mu_values, p_false_alarm, gaussian_crossover, z_alpha):
        """Build threshold table from mu values — vectorized."""
        thresholds = np.empty(len(mu_values), dtype=np.float32)

        # Near-zero: any count is significant
        near_zero = mu_values < 0.01
        thresholds[near_zero] = 1.0

        # Poisson regime: vectorized isf call
        poisson_mask = (mu_values >= 0.01) & (mu_values < gaussian_crossover)
        if poisson_mask.any():
            thresholds[poisson_mask] = stats.poisson.isf(
                p_false_alarm, mu_values[poisson_mask]
            )

        # Gaussian regime: mu + z * sqrt(mu)
        gauss_mask = mu_values >= gaussian_crossover
        if gauss_mask.any():
            thresholds[gauss_mask] = (
                mu_values[gauss_mask] + z_alpha * np.sqrt(mu_values[gauss_mask])
            )

        return thresholds

    def __call__(self, background: np.ndarray) -> np.ndarray:
        """Vectorized per-pixel threshold lookup.

        Args:
            background: array of expected per-pixel background counts.

        Returns:
            Array of thresholds, same shape as background.
        """
        # Quantize background to table indices
        idx = np.clip(
            (background / self.mu_step).astype(np.int64),
            0, len(self._thresholds) - 1
        )
        return self._thresholds[idx]

    def for_box_sum(self, background_per_pixel: np.ndarray, box_size: int) -> np.ndarray:
        """Threshold for box-sum detection.

        The expected sum over box_size^2 pixels = box_size^2 * background_per_pixel.

        Args:
            background_per_pixel: per-pixel background estimate.
            box_size: side length of integration box (e.g. 3 for 3x3).

        Returns:
            Threshold array for the box sum.
        """
        expected_sum = background_per_pixel * (box_size ** 2)
        idx = np.clip(
            (expected_sum / self.mu_step).astype(np.int64),
            0, len(self._box_thresholds) - 1
        )
        return self._box_thresholds[idx]
