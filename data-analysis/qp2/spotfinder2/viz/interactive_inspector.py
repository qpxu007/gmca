"""Interactive spot inspector using matplotlib's native zoom/pan/click.

Launch with:
    python -m qp2.spotfinder2.viz.interactive_inspector master.h5 [frame_idx] [--unit-cell A B C AL BE GA]

Or from Python:
    from qp2.spotfinder2.viz.interactive_inspector import SpotInspector
    inspector = SpotInspector(frame, spots, geometry, background)
    inspector.show()

Features:
    - Zoomable image with spot overlays (scroll wheel or toolbar)
    - Click a spot to see its properties in the info panel
    - Toggle spot overlay on/off with 's' key
    - Toggle log/linear scale with 'l' key
    - Resolution rings with 'r' key
    - Navigate frames with left/right arrow keys (dataset mode)
"""

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.patches import Circle
import matplotlib.colors as mcolors


class SpotInspector:
    """Interactive matplotlib-based spot inspector with zoom and click."""

    def __init__(self, frame, spots, geometry, background=None,
                 title=None, crystal_details=None):
        """
        Args:
            frame: 2D detector image
            spots: SpotList
            geometry: DetectorGeometry
            background: 2D background estimate (optional)
            title: figure title
            crystal_details: dict from estimate_n_crystals (optional)
        """
        self.frame = frame.astype(np.float32)
        self.spots = spots
        self.geometry = geometry
        self.background = background
        self.crystal_details = crystal_details
        self._show_spots = True
        self._show_rings = True
        self._log_scale = True
        self._selected_idx = None

        # Setup figure with gridspec for main image + side panels
        self.fig = plt.figure(figsize=(18, 10))
        gs = self.fig.add_gridspec(2, 3, width_ratios=[3, 1, 1],
                                    hspace=0.3, wspace=0.3)
        self.ax_img = self.fig.add_subplot(gs[:, 0])
        self.ax_info = self.fig.add_subplot(gs[0, 1])
        self.ax_hist = self.fig.add_subplot(gs[1, 1])
        self.ax_cutout = self.fig.add_subplot(gs[0, 2])
        self.ax_profile = self.fig.add_subplot(gs[1, 2])

        self.fig.suptitle(
            title or f"{spots.count} spots detected",
            fontsize=13, fontweight="bold",
        )

        # Draw image
        self._draw_image()
        self._draw_spots()
        self._draw_rings()
        self._draw_resolution_hist()
        self._draw_info_panel()

        # Connect events
        self.fig.canvas.mpl_connect("button_press_event", self._on_click)
        self.fig.canvas.mpl_connect("key_press_event", self._on_key)

        # Instructions
        self.ax_info.text(
            0.5, 0.02,
            "Click spot to inspect | s=toggle spots | r=rings | l=log/linear",
            ha="center", va="bottom", fontsize=8, color="gray",
            transform=self.ax_info.transAxes,
        )

    def _draw_image(self):
        ax = self.ax_img
        ax.clear()
        display = self.frame.copy()
        display[display <= 0] = 0.1
        valid = display[display > 0.1]
        vmin = max(np.percentile(valid, 1), 0.1) if valid.size > 0 else 0.1
        vmax = np.percentile(valid, 99.5) if valid.size > 0 else 100

        if self._log_scale:
            norm = mcolors.LogNorm(vmin=vmin, vmax=vmax)
        else:
            norm = mcolors.Normalize(vmin=0, vmax=vmax)

        self._img_artist = ax.imshow(
            display, cmap="gray_r", norm=norm,
            origin="upper", interpolation="nearest",
        )
        ax.set_xlabel("x (pixels)")
        ax.set_ylabel("y (pixels)")

    def _draw_spots(self):
        # Remove old spot artists
        for attr in ("_spot_scatter", "_spot_labels"):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                try:
                    getattr(self, attr).remove()
                except Exception:
                    pass

        self._spot_scatter = None
        self._spot_labels = None

        if not self._show_spots or self.spots.count == 0:
            return

        sizes = np.sqrt(self.spots.size.astype(float)) * 5 + 4
        self._spot_scatter = self.ax_img.scatter(
            self.spots.x, self.spots.y, s=sizes**2,
            c=self.spots.snr, cmap="plasma",
            edgecolors="red", linewidths=0.8, alpha=0.8, zorder=5,
        )

    def _draw_rings(self):
        # Remove old ring artists
        if hasattr(self, "_ring_artists"):
            for a in self._ring_artists:
                try:
                    a.remove()
                except Exception:
                    pass
        self._ring_artists = []

        if not self._show_rings:
            return

        geo = self.geometry
        # Resolution rings
        for d in [20, 10, 5, 3, 2, 1.5]:
            try:
                r = geo.res_to_radius(d)
                if 0 < r < max(geo.nx, geo.ny):
                    c = Circle((geo.beam_x, geo.beam_y), r,
                               fill=False, color="green", linestyle=":",
                               linewidth=0.5, alpha=0.4)
                    self.ax_img.add_patch(c)
                    self._ring_artists.append(c)
                    t = self.ax_img.text(
                        geo.beam_x + r * 0.707, geo.beam_y - r * 0.707,
                        f"{d}Å", fontsize=7, color="green", alpha=0.6,
                    )
                    self._ring_artists.append(t)
            except Exception:
                pass

        # Ice rings
        for d in [3.67, 3.44, 2.67, 2.25]:
            try:
                r = geo.res_to_radius(d)
                if 0 < r < max(geo.nx, geo.ny):
                    c = Circle((geo.beam_x, geo.beam_y), r,
                               fill=False, color="cyan", linestyle="--",
                               linewidth=0.8, alpha=0.4)
                    self.ax_img.add_patch(c)
                    self._ring_artists.append(c)
            except Exception:
                pass

    def _draw_resolution_hist(self):
        ax = self.ax_hist
        ax.clear()
        if self.spots.count == 0:
            ax.text(0.5, 0.5, "No spots", ha="center", va="center",
                    transform=ax.transAxes)
            return

        valid = (self.spots.resolution > 0) & (self.spots.resolution < 100)
        if valid.sum() == 0:
            return

        ax.hist(self.spots.resolution[valid], bins=30,
                color="steelblue", edgecolor="navy", alpha=0.7)
        for d in [3.67, 3.44, 2.67]:
            ax.axvline(d, color="red", linestyle="--", alpha=0.4)
        ax.set_xlabel("d-spacing (Å)")
        ax.set_ylabel("Count")
        ax.set_title("Resolution", fontsize=10)
        ax.invert_xaxis()

    def _draw_info_panel(self, spot_idx=None):
        ax = self.ax_info
        ax.clear()
        ax.axis("off")

        if spot_idx is not None and 0 <= spot_idx < self.spots.count:
            s = self.spots
            lines = [
                f"Spot #{spot_idx + 1}",
                f"",
                f"Position:   ({s.x[spot_idx]:.2f}, {s.y[spot_idx]:.2f})",
                f"Intensity:  {s.intensity[spot_idx]:.1f}",
                f"Background: {s.background[spot_idx]:.2f}",
                f"SNR:        {s.snr[spot_idx]:.2f}",
                f"Resolution: {s.resolution[spot_idx]:.2f} Å",
                f"Size:       {s.size[spot_idx]} pixels",
                f"Aspect:     {s.aspect_ratio[spot_idx]:.2f}",
                f"Flags:      0x{s.flags[spot_idx]:04x}",
            ]
            if s.tds_intensity[spot_idx] > 0:
                lines.append(f"TDS I:      {s.tds_intensity[spot_idx]:.1f}")
        else:
            lines = [
                f"Total spots: {self.spots.count}",
                f"",
            ]
            if self.spots.count > 0:
                lines.extend([
                    f"SNR range:  {self.spots.snr.min():.1f} – {self.spots.snr.max():.1f}",
                    f"I range:    {self.spots.intensity.min():.0f} – {self.spots.intensity.max():.0f}",
                ])
                valid_res = self.spots.resolution[self.spots.resolution > 0]
                if len(valid_res) > 0:
                    lines.append(f"Resolution: {valid_res.min():.1f} – {valid_res.max():.1f} Å")

            if self.crystal_details:
                lines.append(f"")
                d = self.crystal_details
                lines.append(f"Crystals: {d.get('n_crystals', '?')}")
                lines.append(f"Method: {d.get('method', '?')}")
                if "random_match_rate" in d:
                    lines.append(f"Random match: {d['random_match_rate']:.0%}")
                if "peakiness" in d:
                    lines.append(f"Peakiness: {d['peakiness']:.3f}")

        text = "\n".join(lines)
        ax.text(0.05, 0.95, text, transform=ax.transAxes,
                fontsize=10, verticalalignment="top", fontfamily="monospace",
                bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8))
        ax.set_title("Spot Info", fontsize=10)

    def _draw_cutout(self, spot_idx):
        """Draw zoomed cutout around a selected spot."""
        ax = self.ax_cutout
        ax.clear()

        if spot_idx is None or spot_idx >= self.spots.count:
            ax.axis("off")
            ax.set_title("Click a spot", fontsize=10)
            return

        cx = int(round(self.spots.x[spot_idx]))
        cy = int(round(self.spots.y[spot_idx]))
        r = 15  # cutout half-size

        ny, nx = self.frame.shape
        y0, y1 = max(0, cy - r), min(ny, cy + r + 1)
        x0, x1 = max(0, cx - r), min(nx, cx + r + 1)

        cutout = self.frame[y0:y1, x0:x1].copy()
        cutout[cutout <= 0] = 0.1

        if self._log_scale:
            norm = mcolors.LogNorm(vmin=max(cutout.min(), 0.1), vmax=cutout.max())
        else:
            norm = mcolors.Normalize(vmin=0, vmax=cutout.max())

        ax.imshow(cutout, cmap="viridis", norm=norm, origin="upper",
                  extent=[x0, x1, y1, y0], interpolation="nearest")

        # Mark spot center
        ax.plot(self.spots.x[spot_idx], self.spots.y[spot_idx],
                "r+", markersize=15, markeredgewidth=2)

        # Show background level as contour if available
        if self.background is not None:
            bg_cutout = self.background[y0:y1, x0:x1]
            ax.contour(
                np.arange(x0, x1), np.arange(y0, y1), bg_cutout,
                levels=[self.spots.background[spot_idx] * 2],
                colors="white", linewidths=0.5, linestyles="--",
            )

        ax.set_title(f"Spot #{spot_idx + 1} (31×31)", fontsize=10)

    def _draw_radial_profile(self, spot_idx):
        """Draw radial profile around a selected spot."""
        ax = self.ax_profile
        ax.clear()

        if spot_idx is None or spot_idx >= self.spots.count:
            ax.axis("off")
            return

        cx = int(round(self.spots.x[spot_idx]))
        cy = int(round(self.spots.y[spot_idx]))
        r_max = 12

        ny, nx = self.frame.shape
        yy, xx = np.ogrid[max(0, cy-r_max):min(ny, cy+r_max+1),
                          max(0, cx-r_max):min(nx, cx+r_max+1)]
        rr = np.sqrt((xx - cx)**2 + (yy - cy)**2)
        vals = self.frame[max(0, cy-r_max):min(ny, cy+r_max+1),
                          max(0, cx-r_max):min(nx, cx+r_max+1)]

        r_bins = np.arange(0, r_max + 1)
        r_idx = np.clip(rr.astype(int), 0, r_max)
        profile = np.zeros(r_max + 1)
        counts = np.zeros(r_max + 1)
        for ri in range(r_max + 1):
            mask = r_idx == ri
            if mask.sum() > 0:
                profile[ri] = vals[mask].mean()
                counts[ri] = mask.sum()

        ax.bar(r_bins, profile, color="steelblue", edgecolor="navy", alpha=0.7)

        # Background level
        bg = self.spots.background[spot_idx]
        ax.axhline(bg, color="red", linestyle="--", linewidth=1, label=f"bg={bg:.1f}")

        ax.set_xlabel("Radius (pixels)")
        ax.set_ylabel("Mean intensity")
        ax.set_title("Radial profile", fontsize=10)
        ax.legend(fontsize=8)

    def _on_click(self, event):
        """Handle mouse click — find nearest spot."""
        if event.inaxes != self.ax_img or self.spots.count == 0:
            return

        cx, cy = event.xdata, event.ydata
        dists = (self.spots.x - cx)**2 + (self.spots.y - cy)**2
        idx = int(np.argmin(dists))

        # Only select if click is within ~20 pixels of a spot
        if np.sqrt(dists[idx]) > 20:
            return

        self._selected_idx = idx
        self._draw_info_panel(idx)
        self._draw_cutout(idx)
        self._draw_radial_profile(idx)

        # Highlight selected spot on image
        if hasattr(self, "_highlight") and self._highlight is not None:
            try:
                self._highlight.remove()
            except Exception:
                pass
        self._highlight = self.ax_img.scatter(
            [self.spots.x[idx]], [self.spots.y[idx]],
            s=400, facecolors="none", edgecolors="lime",
            linewidths=3, zorder=10,
        )
        self.fig.canvas.draw_idle()

    def _on_key(self, event):
        """Handle keyboard shortcuts."""
        if event.key == "s":
            self._show_spots = not self._show_spots
            self._draw_spots()
            self.fig.canvas.draw_idle()
        elif event.key == "r":
            self._show_rings = not self._show_rings
            self._draw_rings()
            self.fig.canvas.draw_idle()
        elif event.key == "l":
            self._log_scale = not self._log_scale
            self._draw_image()
            self._draw_spots()
            self._draw_rings()
            if self._selected_idx is not None:
                self._draw_cutout(self._selected_idx)
            self.fig.canvas.draw_idle()

    def show(self):
        """Display the interactive inspector."""
        plt.show()


def main():
    """CLI entry point for the interactive inspector."""
    import argparse
    import sys
    import os
    import h5py

    parser = argparse.ArgumentParser(
        description="Interactive spot inspector with zoom and click-to-inspect"
    )
    parser.add_argument("master_file", help="HDF5 master file")
    parser.add_argument("frame", type=int, nargs="?", default=0,
                        help="Frame index (default: 0)")
    parser.add_argument("--unit-cell", type=float, nargs=6,
                        metavar=("A", "B", "C", "AL", "BE", "GA"),
                        help="Unit cell for crystal count")
    parser.add_argument("--no-mle", action="store_true",
                        help="Skip MLE refinement (faster)")
    args = parser.parse_args()

    # Read frame directly with h5py
    with h5py.File(args.master_file, "r") as f:
        det = f["/entry/instrument/detector"]
        spec = det["detectorSpecific"]
        params = {
            "nx": int(spec["x_pixels_in_detector"][()]),
            "ny": int(spec["y_pixels_in_detector"][()]),
            "beam_x": float(det["beam_center_x"][()]),
            "beam_y": float(det["beam_center_y"][()]),
            "wavelength": float(f["/entry/instrument/beam/incident_wavelength"][()]),
            "det_dist": float(det["detector_distance"][()]) * 1000,
            "pixel_size": float(det["x_pixel_size"][()]) * 1000,
            "saturation_value": 60000,
            "underload_value": -1,
            "images_per_hdf": 1,
            "nimages": int(spec["nimages"][()]) * int(spec["ntrigger"][()]),
        }

    # Determine data file
    master_dir = os.path.dirname(args.master_file)
    prefix = os.path.basename(args.master_file).replace("_master.h5", "")

    # Try to read images_per_hdf from first data file
    first_data = os.path.join(master_dir, f"{prefix}_data_000001.h5")
    if os.path.exists(first_data):
        with h5py.File(first_data, "r") as f:
            for dpath in ["/entry/data/data", "/entry/data/raw_data"]:
                if dpath in f:
                    params["images_per_hdf"] = f[dpath].shape[0]
                    break

    iph = params["images_per_hdf"]
    file_num = args.frame // iph + 1
    local_idx = args.frame % iph
    data_file = os.path.join(master_dir, f"{prefix}_data_{file_num:06d}.h5")

    print(f"Reading frame {args.frame} from {os.path.basename(data_file)}[{local_idx}]")
    with h5py.File(data_file, "r") as f:
        for dpath in ["/entry/data/data", "/entry/data/raw_data"]:
            if dpath in f:
                frame = f[dpath][local_idx]
                break

    # Run pipeline
    from qp2.spotfinder2 import SpotFinderPipeline, SpotFinderConfig

    config = SpotFinderConfig(
        force_cpu=True,
        enable_mle_refinement=not args.no_mle,
        enable_tds_fitting=False,
        enable_ice_filter=True,
    )
    pipeline = SpotFinderPipeline(params, config)

    print("Finding spots...")
    spots = pipeline.find_spots(frame.astype(np.float32))
    bg = pipeline._last_background
    print(f"Found {spots.count} spots")

    # Crystal count
    crystal_details = None
    if spots.count >= 5:
        from qp2.spotfinder2.crystal_count import estimate_n_crystals
        uc = tuple(args.unit_cell) if args.unit_cell else None
        n_cryst, conf, crystal_details = estimate_n_crystals(
            spots, pipeline.geometry, unit_cell=uc,
        )
        crystal_details["n_crystals"] = n_cryst
        crystal_details["confidence"] = conf
        method = "L2" if uc else "L1"
        print(f"Crystal count ({method}): {n_cryst} (confidence={conf:.2f})")

    # Launch inspector
    title = (
        f"Frame {args.frame} — {spots.count} spots"
        + (f" — {crystal_details['n_crystals']} crystal(s)"
           if crystal_details else "")
    )
    inspector = SpotInspector(
        frame, spots, pipeline.geometry, bg,
        title=title, crystal_details=crystal_details,
    )
    inspector.show()


if __name__ == "__main__":
    main()
