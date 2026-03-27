"""PyQtGraph-based visualization widget for spotfinder2.

Provides interactive spot overlay and inspection for integration
into the qp2 image viewer.
"""

import numpy as np
from typing import Optional

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

try:
    import pyqtgraph as pg
    from pyqtgraph.Qt import QtCore, QtWidgets, QtGui

    HAS_PYQTGRAPH = True
except ImportError:
    HAS_PYQTGRAPH = False
    logger.debug("PyQtGraph not available — SpotFinderWidget disabled")


if HAS_PYQTGRAPH:

    class SpotOverlayItem(pg.ScatterPlotItem):
        """PyQtGraph scatter plot item for spot markers on an image.

        Displays spots as open circles, color-coded by SNR.
        Clicking a spot emits spot_clicked with the spot index.
        """

        spot_clicked = QtCore.Signal(int)

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._spot_data = None
            self.sigClicked.connect(self._on_click)

        def update_spots(self, spots, color_by="snr", cmap="viridis"):
            """Update displayed spots from a SpotList.

            Args:
                spots: SpotList
                color_by: field to color by ('snr', 'intensity', 'resolution')
                cmap: matplotlib colormap name
            """
            from matplotlib import colormaps

            self._spot_data = spots

            if spots.count == 0:
                self.setData([], [])
                return

            x = spots.x.astype(float)
            y = spots.y.astype(float)
            sizes = np.sqrt(spots.size.astype(float)) * 3 + 5

            # Color mapping
            values = getattr(spots, color_by, spots.snr).astype(float)
            if values.max() > values.min():
                norm_vals = (values - values.min()) / (values.max() - values.min())
            else:
                norm_vals = np.ones_like(values) * 0.5

            cm = colormaps.get_cmap(cmap)
            colors = [pg.mkColor(*[int(c * 255) for c in cm(v)[:3]]) for v in norm_vals]
            brushes = [pg.mkBrush(color=c) for c in colors]
            pens = [pg.mkPen(color="r", width=1.5)] * len(x)

            self.setData(
                x=x, y=y, size=sizes,
                brush=brushes, pen=pens,
                symbol="o",
            )

        def _on_click(self, plot, points):
            """Handle spot click."""
            if points and self._spot_data is not None:
                # Find nearest spot
                click_pos = points[0].pos()
                cx, cy = click_pos.x(), click_pos.y()
                dists = (self._spot_data.x - cx)**2 + (self._spot_data.y - cy)**2
                idx = int(np.argmin(dists))
                self.spot_clicked.emit(idx)

    class SpotInfoPanel(QtWidgets.QWidget):
        """Panel showing details of a selected spot."""

        def __init__(self, parent=None):
            super().__init__(parent)
            layout = QtWidgets.QVBoxLayout(self)
            layout.setContentsMargins(5, 5, 5, 5)

            self.label = QtWidgets.QLabel("Click a spot to inspect")
            self.label.setWordWrap(True)
            layout.addWidget(self.label)

            self.table = QtWidgets.QTableWidget(0, 2)
            self.table.setHorizontalHeaderLabels(["Field", "Value"])
            self.table.horizontalHeader().setStretchLastSection(True)
            self.table.setMaximumHeight(300)
            layout.addWidget(self.table)

        def show_spot(self, spots, idx):
            """Display details of spot at index idx."""
            if idx < 0 or idx >= spots.count:
                return

            fields = [
                ("X", f"{spots.x[idx]:.2f}"),
                ("Y", f"{spots.y[idx]:.2f}"),
                ("Intensity", f"{spots.intensity[idx]:.1f}"),
                ("Background", f"{spots.background[idx]:.2f}"),
                ("SNR", f"{spots.snr[idx]:.2f}"),
                ("Resolution", f"{spots.resolution[idx]:.2f} Å"),
                ("Size", f"{spots.size[idx]} px"),
                ("Aspect Ratio", f"{spots.aspect_ratio[idx]:.2f}"),
                ("TDS Intensity", f"{spots.tds_intensity[idx]:.1f}"),
                ("Flags", f"0x{spots.flags[idx]:04x}"),
            ]

            self.table.setRowCount(len(fields))
            for row, (name, value) in enumerate(fields):
                self.table.setItem(row, 0, QtWidgets.QTableWidgetItem(name))
                self.table.setItem(row, 1, QtWidgets.QTableWidgetItem(value))

            self.label.setText(f"Spot #{idx + 1}")

    class SpotFinderWidget(QtWidgets.QWidget):
        """Integration-ready widget for the qp2 image viewer.

        Contains:
        - SpotOverlayItem for image overlay
        - Side panel with spot table and info
        - Status bar showing spot count and timing
        """

        spots_found = QtCore.Signal(object)  # emits SpotList

        def __init__(self, parent=None):
            super().__init__(parent)
            self._pipeline = None
            self._spots = None

            layout = QtWidgets.QHBoxLayout(self)

            # Left: image view
            self.image_view = pg.ImageView()
            self.spot_overlay = SpotOverlayItem()
            self.image_view.getView().addItem(self.spot_overlay)
            layout.addWidget(self.image_view, stretch=3)

            # Right: info panel
            right_panel = QtWidgets.QVBoxLayout()

            # Status
            self.status_label = QtWidgets.QLabel("Ready")
            self.status_label.setStyleSheet("font-weight: bold; padding: 5px;")
            right_panel.addWidget(self.status_label)

            # Spot info
            self.spot_info = SpotInfoPanel()
            right_panel.addWidget(self.spot_info)

            # Spot table
            self.spot_table = QtWidgets.QTableWidget(0, 5)
            self.spot_table.setHorizontalHeaderLabels(
                ["X", "Y", "I", "SNR", "d(Å)"]
            )
            self.spot_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
            self.spot_table.cellClicked.connect(self._on_table_click)
            right_panel.addWidget(self.spot_table)

            # Run button
            self.run_button = QtWidgets.QPushButton("Find Spots")
            self.run_button.clicked.connect(self._on_run_clicked)
            right_panel.addWidget(self.run_button)

            right_widget = QtWidgets.QWidget()
            right_widget.setLayout(right_panel)
            right_widget.setMaximumWidth(350)
            layout.addWidget(right_widget)

            # Connect spot click
            self.spot_overlay.spot_clicked.connect(self._on_spot_clicked)

        def set_pipeline(self, pipeline):
            """Connect to a SpotFinderPipeline instance."""
            self._pipeline = pipeline

        def set_image(self, frame, autoLevels=True):
            """Set the current image."""
            self._current_frame = frame
            self.image_view.setImage(frame.T, autoLevels=autoLevels)

        def run_on_frame(self, frame=None, mask=None):
            """Trigger spot finding and update display."""
            if frame is not None:
                self._current_frame = frame

            if self._pipeline is None or self._current_frame is None:
                return

            self.status_label.setText("Finding spots...")
            QtWidgets.QApplication.processEvents()

            import time
            t0 = time.time()
            spots = self._pipeline.find_spots(self._current_frame, mask=mask)
            dt = time.time() - t0

            self._spots = spots
            self.spot_overlay.update_spots(spots)
            self._update_table(spots)
            self.status_label.setText(f"{spots.count} spots found in {dt:.3f}s")
            self.spots_found.emit(spots)

        def get_overlay_item(self):
            """Return the overlay item for adding to an external ImageView."""
            return self.spot_overlay

        def _update_table(self, spots):
            """Populate the spot table."""
            self.spot_table.setRowCount(spots.count)
            for i in range(spots.count):
                self.spot_table.setItem(i, 0, QtWidgets.QTableWidgetItem(f"{spots.x[i]:.1f}"))
                self.spot_table.setItem(i, 1, QtWidgets.QTableWidgetItem(f"{spots.y[i]:.1f}"))
                self.spot_table.setItem(i, 2, QtWidgets.QTableWidgetItem(f"{spots.intensity[i]:.0f}"))
                self.spot_table.setItem(i, 3, QtWidgets.QTableWidgetItem(f"{spots.snr[i]:.1f}"))
                self.spot_table.setItem(i, 4, QtWidgets.QTableWidgetItem(f"{spots.resolution[i]:.2f}"))

        def _on_spot_clicked(self, idx):
            """Handle spot click on image."""
            if self._spots is not None:
                self.spot_info.show_spot(self._spots, idx)
                self.spot_table.selectRow(idx)

        def _on_table_click(self, row, col):
            """Handle table row click."""
            if self._spots is not None and row < self._spots.count:
                self.spot_info.show_spot(self._spots, row)

        def _on_run_clicked(self):
            """Handle run button click."""
            self.run_on_frame()
