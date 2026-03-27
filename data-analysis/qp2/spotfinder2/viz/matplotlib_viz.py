"""Matplotlib-based visualization for spotfinder2.

Provides static and interactive plots for spot-finding results,
suitable for CLI scripts, Jupyter notebooks, and report generation.
"""

import numpy as np
from typing import Optional

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

# Known ice ring d-spacings for annotation
ICE_RINGS_D = [3.67, 3.44, 2.67, 2.25, 2.07, 1.95, 1.92, 1.88]


class SpotFinderPlot:
    """Matplotlib visualizations for spot-finding results."""

    def __init__(self, figsize=(14, 10)):
        self.figsize = figsize
        self._fig = None

    def plot_image_with_spots(
        self, frame, spots, geometry=None, background=None,
        vmin=None, vmax=None, color_by="snr", cmap="viridis",
        show_rings=True, title=None, save_path=None, ax=None,
    ):
        """Show detector image with spot overlay.

        Args:
            frame: 2D detector image
            spots: SpotList
            geometry: DetectorGeometry (for resolution rings)
            background: 2D background array (shown as contours if provided)
            color_by: field to color spots by ('snr', 'resolution', 'intensity')
            show_rings: overlay resolution rings and ice ring markers
            save_path: save figure to this path (PNG/PDF)
            ax: matplotlib Axes to draw on (creates new figure if None)
        """
        import matplotlib.pyplot as plt
        from matplotlib.patches import Circle
        from matplotlib.collections import PatchCollection
        import matplotlib.colors as mcolors

        if ax is None:
            fig, ax = plt.subplots(1, 1, figsize=self.figsize)
            self._fig = fig
        else:
            fig = ax.get_figure()

        # Display image with log scaling
        display_data = frame.astype(np.float32).copy()
        display_data[display_data <= 0] = 0.1
        if vmin is None:
            valid = display_data[display_data > 0.1]
            vmin = np.percentile(valid, 1) if valid.size > 0 else 0.1
        if vmax is None:
            valid = display_data[display_data > 0.1]
            vmax = np.percentile(valid, 99.5) if valid.size > 0 else 100

        im = ax.imshow(
            display_data, cmap="gray_r", norm=mcolors.LogNorm(vmin=max(vmin, 0.1), vmax=vmax),
            origin="upper", interpolation="nearest",
        )

        # Overlay spots
        if spots.count > 0:
            color_values = spots[color_by] if color_by in ["snr", "resolution", "intensity"] else spots.snr
            sizes = np.sqrt(spots.size.astype(float)) * 4 + 3

            sc = ax.scatter(
                spots.x, spots.y, s=sizes**2,
                c=color_values, cmap=cmap, edgecolors="red",
                linewidths=0.8, alpha=0.8, zorder=5,
            )
            cbar = fig.colorbar(sc, ax=ax, shrink=0.6, pad=0.02)
            cbar.set_label(color_by.upper(), fontsize=10)

        # Resolution rings
        if show_rings and geometry is not None:
            self._draw_resolution_rings(ax, geometry)

        ax.set_title(
            title or f"Spots: {spots.count}",
            fontsize=12,
        )
        ax.set_xlabel("x (pixels)")
        ax.set_ylabel("y (pixels)")

        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
            logger.info(f"Saved plot to {save_path}")

        return fig, ax

    def plot_radial_background(
        self, frame, background, mask, geometry, ax=None, save_path=None,
    ):
        """Radial profile: observed vs background model.

        Args:
            frame: 2D detector image
            background: 2D background estimate
            mask: boolean mask
            geometry: DetectorGeometry
        """
        import matplotlib.pyplot as plt

        if ax is None:
            fig, ax = plt.subplots(1, 1, figsize=(10, 5))
            self._fig = fig
        else:
            fig = ax.get_figure()

        # Compute radial profiles
        valid = ~mask & (frame >= 0)
        q_flat = geometry.q_map[valid]
        frame_flat = frame[valid].astype(np.float64)
        bg_flat = background[valid].astype(np.float64)

        n_bins = 200
        q_bins = np.linspace(q_flat.min(), q_flat.max(), n_bins + 1)
        q_idx = np.clip(np.digitize(q_flat, q_bins) - 1, 0, n_bins - 1)

        obs_mean = np.zeros(n_bins)
        bg_mean = np.zeros(n_bins)
        counts = np.zeros(n_bins)

        for b in range(n_bins):
            in_bin = q_idx == b
            if in_bin.sum() > 0:
                obs_mean[b] = frame_flat[in_bin].mean()
                bg_mean[b] = bg_flat[in_bin].mean()
                counts[b] = in_bin.sum()

        q_centers = 0.5 * (q_bins[:-1] + q_bins[1:])
        # Convert q to d-spacing for x-axis
        with np.errstate(divide="ignore"):
            d_spacing = np.where(q_centers > 0, 1.0 / q_centers, np.inf)

        nonzero = counts > 0
        ax.plot(d_spacing[nonzero], obs_mean[nonzero], "b-", alpha=0.5, label="Observed mean", linewidth=0.8)
        ax.plot(d_spacing[nonzero], bg_mean[nonzero], "r-", label="Background model", linewidth=1.5)

        # Shade threshold region
        threshold_upper = bg_mean + 5 * np.sqrt(np.maximum(bg_mean, 0.1))
        ax.fill_between(
            d_spacing[nonzero], bg_mean[nonzero], threshold_upper[nonzero],
            alpha=0.15, color="red", label="5σ threshold",
        )

        # Ice ring markers
        for d_ice in ICE_RINGS_D:
            if d_spacing[nonzero].min() < d_ice < d_spacing[nonzero].max():
                ax.axvline(d_ice, color="cyan", linestyle="--", alpha=0.5, linewidth=0.8)

        ax.set_xlabel("d-spacing (Å)")
        ax.set_ylabel("Intensity (counts)")
        ax.set_title("Radial Background Profile")
        ax.legend(fontsize=9)
        ax.invert_xaxis()
        ax.set_xlim(right=max(1.0, d_spacing[nonzero].min()))

        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")

        return fig, ax

    def plot_resolution_histogram(self, spots, geometry=None, ax=None, save_path=None):
        """Resolution histogram of detected spots."""
        import matplotlib.pyplot as plt

        if ax is None:
            fig, ax = plt.subplots(1, 1, figsize=(8, 4))
            self._fig = fig
        else:
            fig = ax.get_figure()

        valid = (spots.resolution > 0) & (spots.resolution < 100)
        if valid.sum() == 0:
            ax.text(0.5, 0.5, "No spots", ha="center", va="center", transform=ax.transAxes)
            return fig, ax

        d_vals = spots.resolution[valid]
        ax.hist(d_vals, bins=50, color="steelblue", edgecolor="navy", alpha=0.7)

        # Ice ring markers
        for d_ice in ICE_RINGS_D:
            if d_vals.min() < d_ice < d_vals.max():
                ax.axvline(d_ice, color="red", linestyle="--", alpha=0.6, linewidth=1)

        ax.set_xlabel("d-spacing (Å)")
        ax.set_ylabel("Spot count")
        ax.set_title(f"Resolution Distribution ({valid.sum()} spots)")
        ax.invert_xaxis()

        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")

        return fig, ax

    def plot_summary(
        self, frame, spots, background, geometry, mask=None, title=None, save_path=None,
    ):
        """Multi-panel summary figure (2x2).

        Panel 1: Image with spots
        Panel 2: Radial background profile
        Panel 3: Resolution histogram
        Panel 4: SNR vs resolution scatter
        """
        import matplotlib.pyplot as plt

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        self._fig = fig

        if title:
            fig.suptitle(title, fontsize=14, fontweight="bold")

        # Panel 1: Image with spots
        self.plot_image_with_spots(frame, spots, geometry, ax=axes[0, 0])

        # Panel 2: Radial background
        if mask is None:
            mask = np.zeros(frame.shape, dtype=bool)
        self.plot_radial_background(frame, background, mask, geometry, ax=axes[0, 1])

        # Panel 3: Resolution histogram
        self.plot_resolution_histogram(spots, geometry, ax=axes[1, 0])

        # Panel 4: SNR vs resolution
        if spots.count > 0:
            valid = (spots.resolution > 0) & (spots.resolution < 100)
            if valid.sum() > 0:
                axes[1, 1].scatter(
                    spots.resolution[valid], spots.snr[valid],
                    c=spots.intensity[valid], cmap="plasma",
                    s=15, alpha=0.7, edgecolors="none",
                )
                axes[1, 1].set_xlabel("d-spacing (Å)")
                axes[1, 1].set_ylabel("SNR")
                axes[1, 1].set_title("SNR vs Resolution")
                axes[1, 1].invert_xaxis()

                # Ice ring markers
                for d_ice in ICE_RINGS_D:
                    axes[1, 1].axvline(d_ice, color="red", linestyle="--", alpha=0.4)
            else:
                axes[1, 1].text(0.5, 0.5, "No valid spots", ha="center", va="center",
                                transform=axes[1, 1].transAxes)
        else:
            axes[1, 1].text(0.5, 0.5, "No spots", ha="center", va="center",
                            transform=axes[1, 1].transAxes)

        fig.tight_layout()

        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
            logger.info(f"Saved summary plot to {save_path}")

        return fig

    def _draw_resolution_rings(self, ax, geometry):
        """Draw resolution rings and ice ring markers."""
        from matplotlib.patches import Circle

        # Standard resolution rings
        for d in [10, 5, 3, 2, 1.5]:
            try:
                r = geometry.res_to_radius(d)
                if 0 < r < max(geometry.nx, geometry.ny):
                    circle = Circle(
                        (geometry.beam_x, geometry.beam_y), r,
                        fill=False, color="green", linestyle=":", linewidth=0.5, alpha=0.4,
                    )
                    ax.add_patch(circle)
                    ax.text(
                        geometry.beam_x + r * 0.707, geometry.beam_y - r * 0.707,
                        f"{d}Å", fontsize=7, color="green", alpha=0.6,
                    )
            except Exception:
                pass

        # Ice rings (thicker, red)
        for d in ICE_RINGS_D:
            try:
                r = geometry.res_to_radius(d)
                if 0 < r < max(geometry.nx, geometry.ny):
                    circle = Circle(
                        (geometry.beam_x, geometry.beam_y), r,
                        fill=False, color="cyan", linestyle="--", linewidth=0.8, alpha=0.4,
                    )
                    ax.add_patch(circle)
            except Exception:
                pass

    def show(self):
        """Display the current figure (calls plt.show)."""
        import matplotlib.pyplot as plt
        plt.show()

    def close(self):
        """Close the current figure."""
        import matplotlib.pyplot as plt
        if self._fig is not None:
            plt.close(self._fig)
            self._fig = None
