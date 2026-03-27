"""Fast interactive spot inspector using PyQtGraph.

Much faster than matplotlib for large images (Eiger 16M).
Supports smooth zoom/pan, click-to-inspect, and real-time overlays.

Launch:
    module load py313
    python -m qp2.spotfinder2.viz.pyqt_inspector master.h5 [frame] [--unit-cell A B C AL BE GA]
"""

import sys
import os
import numpy as np

import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtWidgets, QtGui


class SpotInspectorWindow(QtWidgets.QMainWindow):
    """PyQtGraph-based interactive spot inspector."""

    def __init__(self, frame, spots, geometry, background=None,
                 title=None, crystal_details=None):
        super().__init__()
        self.frame = frame.astype(np.float32)
        self.spots = spots
        self.geometry = geometry
        self.background = background
        self.crystal_details = crystal_details
        self._show_spots = True
        self._show_rings = True

        self.setWindowTitle(title or f"SpotInspector — {spots.count} spots")
        self.resize(1600, 900)

        # Central widget with splitter
        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        self.setCentralWidget(splitter)

        # Left: image view
        self.image_widget = pg.GraphicsLayoutWidget()
        self.plot = self.image_widget.addPlot(title="Detector Image")
        self.plot.setAspectLocked(True)
        self.plot.invertY(True)

        # Image item
        self.img_item = pg.ImageItem()
        self.plot.addItem(self.img_item)

        # Colormap
        self.img_item.setColorMap(pg.colormap.get("hot", source="matplotlib"))

        # Set image data (linear, 50–99% percentile, excluding masked pixels)
        self._set_image_linear()

        # Spot overlay
        self.spot_scatter = pg.ScatterPlotItem()
        self.plot.addItem(self.spot_scatter)
        self._update_spot_overlay()

        # Predicted reflections overlay (drawn after right panel is built)
        self._show_predictions = True
        self.pred_scatter = pg.ScatterPlotItem()
        self.plot.addItem(self.pred_scatter)

        # Ring overlays
        self.ring_items = []
        self._draw_rings()

        # Crosshair
        self.vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen("y", width=0.5))
        self.hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen("y", width=0.5))
        self.plot.addItem(self.vline, ignoreBounds=True)
        self.plot.addItem(self.hline, ignoreBounds=True)
        self.vline.setVisible(False)
        self.hline.setVisible(False)

        splitter.addWidget(self.image_widget)

        # Right panel
        right_widget = QtWidgets.QWidget()
        right_layout = QtWidgets.QVBoxLayout(right_widget)
        right_widget.setMaximumWidth(500)

        # Summary label
        summary_lines = [f"<b>Spots: {spots.count}</b>"]
        if spots.count > 0:
            summary_lines.append(
                f"SNR: {spots.snr.min():.1f} – {spots.snr.max():.1f}"
            )
            valid_res = spots.resolution[spots.resolution > 0]
            if len(valid_res) > 0:
                summary_lines.append(
                    f"Resolution: {valid_res.min():.1f} – {valid_res.max():.1f} Å"
                )
        if crystal_details:
            summary_lines.append(
                f"Crystals: {crystal_details.get('n_crystals', '?')} "
                f"(conf={crystal_details.get('confidence', '?')})"
            )
        self.summary_label = QtWidgets.QLabel("<br>".join(summary_lines))
        self.summary_label.setStyleSheet("padding: 8px; font-size: 13px;")
        right_layout.addWidget(self.summary_label)

        # Cutout view
        self.cutout_widget = pg.GraphicsLayoutWidget()
        self.cutout_plot = self.cutout_widget.addPlot(title="Spot Cutout")
        self.cutout_plot.setAspectLocked(True)
        self.cutout_img = pg.ImageItem()
        self.cutout_plot.addItem(self.cutout_img)
        self.cutout_crosshair = pg.ScatterPlotItem(
            size=15, symbol="+", pen=pg.mkPen("r", width=2),
            brush=pg.mkBrush(None),
        )
        self.cutout_plot.addItem(self.cutout_crosshair)
        self.cutout_widget.setMinimumHeight(250)
        right_layout.addWidget(self.cutout_widget)

        # Spot info table
        self.info_table = QtWidgets.QTableWidget(10, 2)
        self.info_table.setHorizontalHeaderLabels(["Property", "Value"])
        self.info_table.horizontalHeader().setStretchLastSection(True)
        self.info_table.verticalHeader().setVisible(False)
        self.info_table.setMinimumHeight(200)
        right_layout.addWidget(self.info_table)
        self._fill_info_table(None)

        # Radial profile plot
        self.profile_widget = pg.PlotWidget(title="Radial Profile")
        self.profile_widget.setLabel("bottom", "Radius", units="pixels")
        self.profile_widget.setLabel("left", "Intensity")
        self.profile_widget.setMinimumHeight(180)
        right_layout.addWidget(self.profile_widget)

        # Spot list table
        self.spot_table = QtWidgets.QTableWidget(min(spots.count, 500), 5)
        self.spot_table.setHorizontalHeaderLabels(["X", "Y", "I", "SNR", "d(Å)"])
        self.spot_table.horizontalHeader().setStretchLastSection(True)
        self.spot_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.spot_table.setMaximumHeight(200)
        for i in range(min(spots.count, 500)):
            self.spot_table.setItem(i, 0, QtWidgets.QTableWidgetItem(f"{spots.x[i]:.1f}"))
            self.spot_table.setItem(i, 1, QtWidgets.QTableWidgetItem(f"{spots.y[i]:.1f}"))
            self.spot_table.setItem(i, 2, QtWidgets.QTableWidgetItem(f"{spots.intensity[i]:.0f}"))
            self.spot_table.setItem(i, 3, QtWidgets.QTableWidgetItem(f"{spots.snr[i]:.1f}"))
            self.spot_table.setItem(i, 4, QtWidgets.QTableWidgetItem(f"{spots.resolution[i]:.1f}"))
        self.spot_table.cellClicked.connect(self._on_table_click)
        right_layout.addWidget(self.spot_table)

        # Keyboard shortcut labels
        shortcuts_label = QtWidgets.QLabel(
            "Keys: <b>S</b>=spots  <b>P</b>=predictions  <b>R</b>=rings  <b>L</b>=log/linear  <b>C</b>=crosshair"
        )
        shortcuts_label.setStyleSheet("padding: 4px; color: gray; font-size: 11px;")
        right_layout.addWidget(shortcuts_label)

        splitter.addWidget(right_widget)
        splitter.setSizes([1100, 500])

        # Connect click on image
        self.img_item.scene().sigMouseClicked.connect(self._on_image_click)

        # Highlight marker for selected spot
        self.highlight_scatter = pg.ScatterPlotItem(
            size=25, symbol="o",
            pen=pg.mkPen("lime", width=3),
            brush=pg.mkBrush(None),
        )
        self.plot.addItem(self.highlight_scatter)

        # Draw predicted reflections (must be after summary_label exists)
        self._draw_predicted_reflections()

    def keyPressEvent(self, event):
        key = event.key()
        if key == QtCore.Qt.Key_S:
            self._show_spots = not self._show_spots
            self.spot_scatter.setVisible(self._show_spots)
        elif key == QtCore.Qt.Key_R:
            self._show_rings = not self._show_rings
            for item in self.ring_items:
                item.setVisible(self._show_rings)
        elif key == QtCore.Qt.Key_L:
            self._log_scale = not self._log_scale
            if self._log_scale:
                self._set_image_log()
            else:
                self._set_image_linear()
        elif key == QtCore.Qt.Key_C:
            vis = not self.vline.isVisible()
            self.vline.setVisible(vis)
            self.hline.setVisible(vis)
        elif key == QtCore.Qt.Key_P:
            self._show_predictions = not self._show_predictions
            self.pred_scatter.setVisible(self._show_predictions)
        else:
            super().keyPressEvent(event)

    def _get_valid_pixels(self):
        """Get pixel values excluding masked pixels (underload/saturation/overflow).

        For uint32, values like 4294967295 (2^32-1) are overflow markers.
        """
        geo = self.geometry
        f = self.frame.astype(np.float64)
        valid_mask = f >= 0
        if geo.underload_value is not None:
            valid_mask &= (f > geo.underload_value)
        if geo.saturation_value is not None:
            valid_mask &= (f < geo.saturation_value)
        return f[valid_mask]

    def _get_contrast_levels(self, p_low=50, p_high=99.999):
        """Compute contrast levels from valid non-zero pixels.

        For photon-counting detectors, most pixels are zero. We use:
          vmin = 0 (always show zero as black)
          vmax = P(p_high) of non-zero valid pixels (so spots are visible)
        Fallback: if too few non-zero pixels, use overall percentiles.
        """
        valid = self._get_valid_pixels()
        nonzero = valid[valid > 0]

        if nonzero.size > 100:
            vmax = max(np.percentile(nonzero, min(p_high, 99.999)), 1)
        elif valid.size > 0:
            vmax = max(valid.max(), 1)
        else:
            vmax = 10
        return 0, vmax

    def _set_image_log(self):
        """Set image display to log scale."""
        display = self.frame.astype(np.float32).copy()
        display[display <= 0] = 0
        vmin, vmax = self._get_contrast_levels()
        log_img = np.log10(display + 1)
        log_min = np.log10(vmin + 1)
        log_max = np.log10(vmax + 1)
        self.img_item.setImage(log_img.T, levels=(log_min, log_max))
        self._log_scale = True

    def _set_image_linear(self):
        """Set image display to linear scale."""
        display = self.frame.astype(np.float32).copy()
        vmin, vmax = self._get_contrast_levels()
        self.img_item.setImage(display.T, levels=(vmin, vmax))
        self._log_scale = False

    def _update_spot_overlay(self):
        if self.spots.count == 0:
            return

        # Color by SNR
        snr = self.spots.snr
        snr_norm = (snr - snr.min()) / max(snr.max() - snr.min(), 1)

        spots_data = []
        for i in range(self.spots.count):
            r = int(255 * snr_norm[i])
            g = int(100 * (1 - snr_norm[i]))
            b = int(255 * (1 - snr_norm[i]))
            size = max(6, np.sqrt(self.spots.size[i]) * 3 + 4)
            spots_data.append({
                "pos": (self.spots.x[i], self.spots.y[i]),
                "size": size,
                "pen": pg.mkPen(color=(r, g, b), width=1.5),
                "brush": pg.mkBrush(None),
                "symbol": "o",
            })
        self.spot_scatter.setData(spots_data)

    def _draw_predicted_reflections(self):
        """Overlay predicted reflection positions from the crystal orientation.

        Uses the R matrix from crystal_count Level 2 to rotate HKL vectors
        and project them back to detector (x, y) coordinates.
        """
        if not self.crystal_details:
            return
        crystals = self.crystal_details.get("crystals", [])
        q_hkl = self.crystal_details.get("q_hkl")
        if not crystals or q_hkl is None:
            return

        # Don't show predictions if orientation is unreliable
        # (match rate not significantly above random)
        random_rate = self.crystal_details.get("random_match_rate", 0)
        best_frac = crystals[0].get("fraction_matched", 0) if crystals else 0
        if best_frac < random_rate * 2:
            self.summary_label.setText(
                self.summary_label.text() +
                "<br><i>Predictions disabled: orientation unreliable "
                f"(match={best_frac:.0%} vs random={random_rate:.0%})</i>"
            )
            return

        geo = self.geometry
        pred_data = []
        colors = [
            (0, 255, 255, 120),    # cyan — crystal 1
            (255, 165, 0, 120),    # orange — crystal 2
            (0, 255, 0, 120),      # green — crystal 3
            (255, 0, 255, 120),    # magenta — crystal 4
        ]

        for ci, cryst in enumerate(crystals):
            R = np.array(cryst.get("R_matrix"))
            if R is None or R.shape != (3, 3):
                continue

            color = colors[ci % len(colors)]

            # Rotate HKL vectors by R: q_lab = R @ q_crystal
            q_lab = (R @ q_hkl.T).T  # (n_hkl, 3)

            # Filter by Ewald sphere intersection:
            # A reflection is visible in a still when its reciprocal lattice
            # point sits ON the Ewald sphere. The Ewald sphere has radius 1/λ
            # centered at (0, 0, -1/λ) in lab frame.
            # Excitation error = | |q + s0| - 1/λ | where s0 = (0,0,1/λ)
            wl = geo.wavelength
            D = geo.det_dist
            inv_wl = 1.0 / wl

            # s = q + s0, where s0 = (0, 0, inv_wl) (incident beam along +z)
            sx = q_lab[:, 0]
            sy = q_lab[:, 1]
            sz = q_lab[:, 2] + inv_wl  # add Ewald sphere center offset

            s_mag = np.sqrt(sx**2 + sy**2 + sz**2)
            excitation_error = np.abs(s_mag - inv_wl)

            # Only show reflections within mosaicity bandwidth of the sphere
            # Typical mosaicity ~0.3° → excitation error < 0.005 Å⁻¹
            mosaicity_bandwidth = 0.005  # Å⁻¹
            on_sphere = excitation_error < mosaicity_bandwidth

            # Project to detector: s direction → detector position
            # detector at distance D along z: x = D * sx/sz, y = D * sy/sz
            valid = on_sphere & (sz > 0.01)
            px_x = np.full(len(q_lab), np.nan)
            px_y = np.full(len(q_lab), np.nan)
            px_x[valid] = D * sx[valid] / sz[valid] / geo.pixel_size + geo.beam_x
            px_y[valid] = D * sy[valid] / sz[valid] / geo.pixel_size + geo.beam_y

            # Filter to on-detector
            on_det = (
                valid &
                (px_x >= 0) & (px_x < geo.nx) &
                (px_y >= 0) & (px_y < geo.ny)
            )

            for j in np.where(on_det)[0]:
                pred_data.append({
                    "pos": (float(px_x[j]), float(px_y[j])),
                    "size": 10,
                    "pen": pg.mkPen(color=color, width=1),
                    "brush": pg.mkBrush(None),
                    "symbol": "x",
                })

        if pred_data:
            self.pred_scatter.setData(pred_data)
            # Count
            n_pred = len(pred_data)
            n_cryst = len(crystals)
            self.summary_label.setText(
                self.summary_label.text() +
                f"<br>Predicted: {n_pred} reflections ({n_cryst} lattice(s))"
            )

    def _draw_rings(self):
        geo = self.geometry
        # Resolution rings
        for d in [20, 10, 5, 3, 2, 1.5]:
            try:
                r = geo.res_to_radius(d)
                if 0 < r < max(geo.nx, geo.ny):
                    circle = pg.CircleROI(
                        [geo.beam_x - r, geo.beam_y - r], [2*r, 2*r],
                        movable=False, resizable=False,
                        pen=pg.mkPen("g", width=0.5, style=QtCore.Qt.DotLine),
                    )
                    circle.removeHandle(0)
                    self.plot.addItem(circle)
                    self.ring_items.append(circle)

                    text = pg.TextItem(f"{d}Å", color="g", anchor=(0, 1))
                    text.setPos(geo.beam_x + r * 0.707, geo.beam_y - r * 0.707)
                    text.setFont(QtGui.QFont("", 8))
                    self.plot.addItem(text)
                    self.ring_items.append(text)
            except Exception:
                pass

        # Ice rings
        for d in [3.67, 3.44, 2.67, 2.25]:
            try:
                r = geo.res_to_radius(d)
                if 0 < r < max(geo.nx, geo.ny):
                    circle = pg.CircleROI(
                        [geo.beam_x - r, geo.beam_y - r], [2*r, 2*r],
                        movable=False, resizable=False,
                        pen=pg.mkPen("c", width=0.8, style=QtCore.Qt.DashLine),
                    )
                    circle.removeHandle(0)
                    self.plot.addItem(circle)
                    self.ring_items.append(circle)
            except Exception:
                pass

    def _on_image_click(self, event):
        if self.spots.count == 0:
            return

        pos = event.scenePos()
        mouse_point = self.plot.vb.mapSceneToView(pos)
        cx, cy = mouse_point.x(), mouse_point.y()

        # Update crosshair
        self.vline.setPos(cx)
        self.hline.setPos(cy)

        # Find nearest spot
        dists = (self.spots.x - cx)**2 + (self.spots.y - cy)**2
        idx = int(np.argmin(dists))

        if np.sqrt(dists[idx]) > 30:
            return

        self._select_spot(idx)

    def _on_table_click(self, row, col):
        if row < self.spots.count:
            self._select_spot(row)
            # Center view on spot
            self.plot.setXRange(
                self.spots.x[row] - 200, self.spots.x[row] + 200,
            )
            self.plot.setYRange(
                self.spots.y[row] - 200, self.spots.y[row] + 200,
            )

    def _select_spot(self, idx):
        # Highlight circle
        self.highlight_scatter.setData(
            [{"pos": (self.spots.x[idx], self.spots.y[idx]),
              "size": 25, "symbol": "o",
              "pen": pg.mkPen("lime", width=3),
              "brush": pg.mkBrush(None)}]
        )

        # Bounding box around spot (based on size)
        self._draw_spot_bbox(idx)

        # Size + SNR text label near the spot
        self._draw_spot_label(idx)

        # Info table
        self._fill_info_table(idx)

        # Cutout
        self._show_cutout(idx)

        # Radial profile
        self._show_radial_profile(idx)

        # Select in table
        self.spot_table.selectRow(idx)

    def _draw_spot_bbox(self, idx):
        """Draw a bounding box around the selected spot on the main image."""
        # Remove previous bbox
        if hasattr(self, "_bbox_item") and self._bbox_item is not None:
            try:
                self.plot.removeItem(self._bbox_item)
            except Exception:
                pass

        cx = self.spots.x[idx]
        cy = self.spots.y[idx]
        sz = max(self.spots.size[idx], 1)
        half = max(3, np.sqrt(sz) * 1.5 + 1)

        rect = QtCore.QRectF(cx - half, cy - half, 2 * half, 2 * half)
        self._bbox_item = pg.RectROI(
            [cx - half, cy - half], [2 * half, 2 * half],
            movable=False, resizable=False,
            pen=pg.mkPen("lime", width=2),
        )
        # Remove the resize handle
        for h in self._bbox_item.getHandles():
            self._bbox_item.removeHandle(h)
        self.plot.addItem(self._bbox_item)

    def _draw_spot_label(self, idx):
        """Draw size and SNR text label near the selected spot."""
        # Remove previous label
        if hasattr(self, "_spot_label") and self._spot_label is not None:
            try:
                self.plot.removeItem(self._spot_label)
            except Exception:
                pass

        s = self.spots
        label_text = (
            f"#{idx+1}  {s.size[idx]}px  "
            f"SNR={s.snr[idx]:.1f}  "
            f"I={s.intensity[idx]:.0f}  "
            f"d={s.resolution[idx]:.1f}Å"
        )
        self._spot_label = pg.TextItem(
            label_text, color="lime", anchor=(0, 1),
            border=pg.mkPen("lime", width=1),
            fill=pg.mkBrush(0, 0, 0, 160),
        )
        self._spot_label.setFont(QtGui.QFont("", 10))
        # Position label above the spot
        offset_y = max(5, np.sqrt(s.size[idx]) * 1.5 + 3)
        self._spot_label.setPos(s.x[idx], s.y[idx] - offset_y)
        self.plot.addItem(self._spot_label)

    def _fill_info_table(self, idx):
        if idx is None or idx >= self.spots.count:
            fields = [
                ("Spots", str(self.spots.count)),
                ("", "Click a spot to inspect"),
            ]
        else:
            s = self.spots
            fields = [
                ("Spot #", str(idx + 1)),
                ("Position", f"({s.x[idx]:.2f}, {s.y[idx]:.2f})"),
                ("Intensity", f"{s.intensity[idx]:.1f}"),
                ("Background", f"{s.background[idx]:.2f}"),
                ("SNR", f"{s.snr[idx]:.2f}"),
                ("Resolution", f"{s.resolution[idx]:.2f} Å"),
                ("Size", f"{s.size[idx]} px"),
                ("Aspect", f"{s.aspect_ratio[idx]:.2f}"),
                ("TDS I", f"{s.tds_intensity[idx]:.1f}"),
                ("Flags", f"0x{s.flags[idx]:04x}"),
            ]

        self.info_table.setRowCount(len(fields))
        for row, (name, value) in enumerate(fields):
            self.info_table.setItem(row, 0, QtWidgets.QTableWidgetItem(name))
            self.info_table.setItem(row, 1, QtWidgets.QTableWidgetItem(value))

    def _show_cutout(self, idx):
        cx = int(round(self.spots.x[idx]))
        cy = int(round(self.spots.y[idx]))
        r = 15
        ny, nx = self.frame.shape
        y0, y1 = max(0, cy - r), min(ny, cy + r + 1)
        x0, x1 = max(0, cx - r), min(nx, cx + r + 1)

        cutout = self.frame[y0:y1, x0:x1].copy()
        cutout[cutout <= 0] = 0.1

        self.cutout_img.setImage(np.log10(cutout + 1).T)
        self.cutout_plot.setTitle(f"Spot #{idx+1} ({y1-y0}×{x1-x0})")

        # Center crosshair on spot
        local_x = self.spots.x[idx] - x0
        local_y = self.spots.y[idx] - y0
        self.cutout_crosshair.setData([{"pos": (local_x, local_y)}])

    def _show_radial_profile(self, idx):
        cx = int(round(self.spots.x[idx]))
        cy = int(round(self.spots.y[idx]))
        r_max = 12
        ny, nx = self.frame.shape

        y0, y1 = max(0, cy - r_max), min(ny, cy + r_max + 1)
        x0, x1 = max(0, cx - r_max), min(nx, cx + r_max + 1)
        yy, xx = np.mgrid[y0:y1, x0:x1]
        rr = np.sqrt((xx - cx).astype(float)**2 + (yy - cy).astype(float)**2)
        vals = self.frame[y0:y1, x0:x1]

        profile = np.zeros(r_max + 1)
        for ri in range(r_max + 1):
            mask = (rr >= ri) & (rr < ri + 1)
            if mask.sum() > 0:
                profile[ri] = vals[mask].mean()

        self.profile_widget.clear()
        self.profile_widget.plot(np.arange(r_max + 1), profile,
                                 pen=pg.mkPen("w", width=2),
                                 symbol="o", symbolSize=5)

        # Background level
        bg = self.spots.background[idx]
        self.profile_widget.addLine(y=bg, pen=pg.mkPen("r", width=1, style=QtCore.Qt.DashLine))
        self.profile_widget.setTitle(f"Radial Profile (bg={bg:.1f})")


def main():
    import argparse
    import h5py

    parser = argparse.ArgumentParser(
        description="Fast interactive spot inspector (PyQtGraph)"
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

    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication(sys.argv)

    # Read parameters from master
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

    # Read frame
    master_dir = os.path.dirname(args.master_file)
    prefix = os.path.basename(args.master_file).replace("_master.h5", "")
    first_data = os.path.join(master_dir, f"{prefix}_data_000001.h5")
    if os.path.exists(first_data):
        with h5py.File(first_data, "r") as f:
            for dp in ["/entry/data/data", "/entry/data/raw_data"]:
                if dp in f:
                    params["images_per_hdf"] = f[dp].shape[0]
                    break

    iph = params["images_per_hdf"]
    file_num = args.frame // iph + 1
    local_idx = args.frame % iph
    data_file = os.path.join(master_dir, f"{prefix}_data_{file_num:06d}.h5")

    print(f"Reading frame {args.frame} from {os.path.basename(data_file)}[{local_idx}]")
    with h5py.File(data_file, "r") as f:
        for dp in ["/entry/data/data", "/entry/data/raw_data"]:
            if dp in f:
                frame = f[dp][local_idx]
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
        print(f"Crystal count: {n_cryst} (confidence={conf:.2f})")

        # Generate full HKL set for predicted reflection overlay
        if uc and crystal_details.get("crystals"):
            from qp2.spotfinder2.crystal_count import (
                _unit_cell_to_reciprocal, _generate_hkl_list,
            )
            rlatt = _unit_cell_to_reciprocal(uc)
            q_full, _ = _generate_hkl_list(rlatt, q_max=0.6)
            # Cap at 20K for rendering performance
            if len(q_full) > 20000:
                rng = np.random.default_rng(42)
                q_full = q_full[rng.choice(len(q_full), 20000, replace=False)]
            crystal_details["q_hkl"] = q_full
            print(f"Prediction overlay: {len(q_full)} HKL reflections")

    # Launch window
    title = f"Frame {args.frame} — {spots.count} spots"
    if crystal_details:
        title += f" — {crystal_details['n_crystals']} crystal(s)"

    win = SpotInspectorWindow(
        frame, spots, pipeline.geometry, bg,
        title=title, crystal_details=crystal_details,
    )
    win.show()

    print("Inspector ready — zoom with scroll, click spots to inspect")
    app.exec_()


if __name__ == "__main__":
    main()
