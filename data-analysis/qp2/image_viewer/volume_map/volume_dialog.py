# Create new file: qp2/image_viewer/ui/volume_dialog.py

import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtWidgets, QtCore

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class VolumeDialog(QtWidgets.QDialog):
    """
    A dialog for displaying a 3D volume as interactive 2D slices,
    listing detected hotspots with comprehensive metrics.
    """
    # Emitted when a peak/hotspot in the table is clicked
    peak_selected = QtCore.pyqtSignal(dict)
    # Emitted when the user clicks the "Find" button
    find_hotspots_requested = QtCore.pyqtSignal(float, int)
    refresh_requested = QtCore.pyqtSignal()
    show_3d_requested = QtCore.pyqtSignal()

    def __init__(self, title, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setMinimumSize(1200, 700)

        # --- Data Storage ---
        self.peaks_data = []
        self.volume_data = None  # Store a reference to the 3D volume array

        # --- Main Layout ---
        layout = QtWidgets.QVBoxLayout(self)
        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        layout.addWidget(splitter)

        # --- Left Panel: The ImageView for slicing ---
        self.image_view = pg.ImageView()
        splitter.addWidget(self.image_view)

        # 1. Create the text item for displaying coordinates
        self.mouse_coord_text = pg.TextItem(
            color='c',  # Cyan color for visibility
            anchor=(0, 1),
            fill=pg.mkBrush(0, 0, 0, 150)  # Dark background
        )
        self.mouse_coord_text.setZValue(100)  # Ensure it's on top
        self.image_view.addItem(self.mouse_coord_text, ignoreBounds=True)

        # 2. Create a proxy to connect to the mouse move signal of the view's scene
        self.mouse_proxy = pg.SignalProxy(
            self.image_view.scene.sigMouseMoved,
            rateLimit=60,  # Limit updates to 60 Hz
            slot=self._on_mouse_moved
        )

        # --- Right Panel: Controls and Peak List ---
        right_panel = QtWidgets.QWidget()
        right_layout = QtWidgets.QVBoxLayout(right_panel)
        splitter.addWidget(right_panel)

        # --- Hotspot Finding Controls ---
        hotspot_controls_group = QtWidgets.QGroupBox("Find 3D Hotspots")
        hotspot_controls_layout = QtWidgets.QHBoxLayout(hotspot_controls_group)

        hotspot_controls_layout.addWidget(QtWidgets.QLabel("Threshold:"))
        self.percentile_spinbox = QtWidgets.QDoubleSpinBox()
        self.percentile_spinbox.setSuffix("%")
        self.percentile_spinbox.setRange(90, 100)
        self.percentile_spinbox.setValue(98)
        self.percentile_spinbox.setSingleStep(0.5)
        self.percentile_spinbox.setToolTip("Find regions in the top (100 - value)% of data")
        hotspot_controls_layout.addWidget(self.percentile_spinbox)

        find_button = QtWidgets.QPushButton("Find")
        find_button.clicked.connect(
            lambda: self.find_hotspots_requested.emit(self.percentile_spinbox.value(), 5)
        )
        hotspot_controls_layout.addWidget(find_button)

        show_3d_button = QtWidgets.QPushButton("Show 3D View")
        show_3d_button.setToolTip("Visualize the detected hotspots in a 3D view")
        show_3d_button.clicked.connect(self.show_3d_requested.emit)
        hotspot_controls_layout.addWidget(show_3d_button)

        refresh_button = QtWidgets.QPushButton("Refresh Data")
        refresh_button.setToolTip("Re-fetch all analysis data from Redis for both scans")
        refresh_button.clicked.connect(self.refresh_requested.emit)
        hotspot_controls_layout.addWidget(refresh_button)
        right_layout.addWidget(hotspot_controls_group)

        # --- Peak List Table ---
        right_layout.addWidget(QtWidgets.QLabel("<b>Detected Hotspots</b>"))
        self.peaks_table = QtWidgets.QTableWidget()
        self.peaks_table.setColumnCount(9)
        self.peaks_table.setHorizontalHeaderLabels([
            "Peak #", "X", "Y", "Z", "Value",
            "Dimensions (L,W,H)", "Voxels", "Intensity", "Angles to X (°)"
        ])
        self.peaks_table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.peaks_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.peaks_table.itemClicked.connect(self._on_peak_item_clicked)
        right_layout.addWidget(self.peaks_table)

        splitter.setSizes([800, 400])

    def update_data(self, volume: np.ndarray, peaks: list):
        """
        Displays the 3D volume in the ImageView and populates the peak list.
        This is typically called once when the data is first loaded.
        """
        if volume is None:
            return

        self.volume_data = volume

        # ImageView expects (time, y, x), so our (z, y, x) shape is perfect.
        # It automatically creates a slider for the first axis (z).
        self.image_view.setImage(volume, xvals=np.arange(volume.shape[2]))
        self.image_view.ui.histogram.setHistogramRange(np.nanmin(volume), np.nanmax(volume))

        # Clean up the ImageView UI for a better look
        self.image_view.ui.roiBtn.hide()
        self.image_view.ui.menuBtn.hide()

        plot_item = self.image_view.view.parent()
        if plot_item:
            plot_item.setLabel('bottom', 'Frame (X-axis)')
            plot_item.setLabel('left', 'Y-axis Row')

        self.image_view.timeLine.setToolTip("Z-axis Slice")

        # Populate the table with the initial set of peaks
        self.update_peak_list(peaks)

    def update_peak_list(self, peaks: list):
        """
        Populates the peak list table with new data. This can be called
        repeatedly after re-running the hotspot analysis.
        """
        self.peaks_data = peaks
        self.peaks_table.setRowCount(len(peaks))
        for i, peak in enumerate(peaks):
            coords = peak.get('coords', (0, 0, 0))
            value = peak.get('value', 0.0)
            dims = peak.get('dimensions', (0, 0, 0))
            voxels = peak.get('voxel_count', 0)
            intensity = peak.get('integrated_intensity', 0.0)
            angles = peak.get('angles_to_x', (0, 0, 0))

            self.peaks_table.setItem(i, 0, QtWidgets.QTableWidgetItem(str(i + 1)))
            self.peaks_table.setItem(i, 1, QtWidgets.QTableWidgetItem(str(coords[0] + 1)))
            self.peaks_table.setItem(i, 2, QtWidgets.QTableWidgetItem(str(coords[1] + 1)))
            self.peaks_table.setItem(i, 3, QtWidgets.QTableWidgetItem(str(coords[2] + 1)))
            self.peaks_table.setItem(i, 4, QtWidgets.QTableWidgetItem(f"{value:.3f}"))

            dims_str = f"{dims[0]:.1f}, {dims[1]:.1f}, {dims[2]:.1f}"
            self.peaks_table.setItem(i, 5, QtWidgets.QTableWidgetItem(dims_str))

            self.peaks_table.setItem(i, 6, QtWidgets.QTableWidgetItem(str(voxels)))
            self.peaks_table.setItem(i, 7, QtWidgets.QTableWidgetItem(f"{intensity:.2f}"))

            angles_str = f"{angles[0]:.1f}, {angles[1]:.1f}, {angles[2]:.1f}"
            self.peaks_table.setItem(i, 8, QtWidgets.QTableWidgetItem(angles_str))

        self.peaks_table.resizeColumnsToContents()

    @QtCore.pyqtSlot(QtWidgets.QTableWidgetItem)
    def _on_peak_item_clicked(self, item):
        """When a cell is clicked, find the corresponding peak data and emit it."""
        if item is not None:
            row = item.row()
            if 0 <= row < len(self.peaks_data):
                peak_info = self.peaks_data[row]
                self.peak_selected.emit(peak_info)

    def _on_mouse_moved(self, event):
        """Handles mouse movement over the ImageView to display 3D coordinates and values."""
        pos = event[0]  # The event is a tuple containing the position

        # Check if the mouse is within the plot area and if we have volume data
        if self.image_view.view.sceneBoundingRect().contains(pos) and self.volume_data is not None:
            # Map the scene position to the image's coordinate system
            mouse_point = self.image_view.view.mapSceneToView(pos)

            # Floor the coordinates to get the integer indices
            x = int(mouse_point.x())
            y = int(mouse_point.y())

            # Get the current slice index from the ImageView's slider
            z = self.image_view.currentIndex

            # Check if the calculated indices are within the bounds of our 3D data array
            # The shape is (z, y, x)
            if 0 <= z < self.volume_data.shape[0] and \
                    0 <= y < self.volume_data.shape[1] and \
                    0 <= x < self.volume_data.shape[2]:

                value = self.volume_data[z, y, x]
                # Update the text item with the full 3D coordinate and value
                self.mouse_coord_text.setText(f"X:{x + 1}, Y:{y + 1}, Z:{z + 1}, Val:{value:.3f}")
                self.mouse_coord_text.setPos(mouse_point)
                self.mouse_coord_text.setVisible(True)
            else:
                self.mouse_coord_text.setVisible(False)
        else:
            self.mouse_coord_text.setVisible(False)
