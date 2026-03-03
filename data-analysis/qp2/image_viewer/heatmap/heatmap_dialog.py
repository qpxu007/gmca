# qp2/image_viewer/heatmap/heatmap_dialog.py

import pyqtgraph as pg
import numpy as np
from pyqtgraph.Qt import QtWidgets, QtCore, QtGui

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class HeatmapDialog(QtWidgets.QDialog):
    """A dialog for displaying raster scan results as a 2D heatmap."""

    cell_clicked = QtCore.pyqtSignal(
        int, int
    )  # matrix_row (0-based), matrix_col (0-based)
    find_hotspots_requested = QtCore.pyqtSignal(
        str, float, int
    )  # find_mode, percentile, min_size
    refresh_requested = QtCore.pyqtSignal()

    def __init__(self, run_prefix, default_metric: str = None, scan_mode: str = "row_wise", parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Grid Heatmap: {run_prefix}")
        self.setMinimumSize(800, 600)
        self.default_metric = default_metric
        self.data_matrices = {}
        self.current_metric = ""
        self.scan_mode = scan_mode

        layout = QtWidgets.QVBoxLayout(self)

        control_layout = QtWidgets.QHBoxLayout()
        control_layout.addWidget(QtWidgets.QLabel("Metric to Display:"))
        self.metric_selector = QtWidgets.QComboBox()
        self.metric_selector.currentIndexChanged.connect(self._on_metric_changed)
        control_layout.addWidget(self.metric_selector, 1)
        layout.addLayout(control_layout)

        control_layout.addWidget(QtWidgets.QLabel("  |  Mode:"))
        self.find_mode_selector = QtWidgets.QComboBox()
        self.find_mode_selector.addItems(
            ["Find Peaks (Higher is Better)", "Find Valleys (Lower is Better)"]
        )
        self.find_mode_selector.setToolTip(
            "Choose whether to find high-value peaks or low-value valleys"
        )
        self.find_mode_selector.currentIndexChanged.connect(self._on_find_mode_changed)
        control_layout.addWidget(self.find_mode_selector)

        control_layout.addWidget(QtWidgets.QLabel("  |  Threshold:"))
        self.percentile_spinbox = QtWidgets.QDoubleSpinBox()
        self.percentile_spinbox.setSuffix("%")
        self.percentile_spinbox.setSingleStep(1)
        # Initial state is for "Peaks"
        self._on_find_mode_changed(0)  # Call it to set initial state
        control_layout.addWidget(self.percentile_spinbox)

        control_layout.addWidget(QtWidgets.QLabel("  |  Min Size:"))
        self.min_size_spinbox = QtWidgets.QSpinBox()
        self.min_size_spinbox.setRange(1, 100)
        self.min_size_spinbox.setValue(3)
        self.min_size_spinbox.setToolTip(
            "Minimum number of cells to be considered a hotspot"
        )
        control_layout.addWidget(self.min_size_spinbox)

        # --- ADDED: Shape selector ---
        control_layout.addWidget(QtWidgets.QLabel("  |  Shape:"))
        self.shape_selector = QtWidgets.QComboBox()
        self.shape_selector.addItems(["Rectangle", "Ellipse"])
        control_layout.addWidget(self.shape_selector)
        control_layout.addStretch(1)

        self.find_button = QtWidgets.QPushButton("Find")
        self.find_button.clicked.connect(self._on_find_hotspots)
        control_layout.addWidget(self.find_button)

        self.clear_button = QtWidgets.QPushButton("Clear Markers")
        self.clear_button.clicked.connect(self.clear_hotspot_markers)
        control_layout.addWidget(self.clear_button)

        self.refresh_button = QtWidgets.QPushButton("Refresh Data")
        self.refresh_button.setToolTip("Re-fetch all analysis data from Redis")
        self.refresh_button.clicked.connect(self.refresh_requested.emit)
        control_layout.addWidget(self.refresh_button)

        self.export_console_button = QtWidgets.QPushButton("Export to Console")
        self.export_console_button.setToolTip("Print current data statistics and matrix to the terminal/console")
        self.export_console_button.clicked.connect(self._on_export_to_console)
        control_layout.addWidget(self.export_console_button)

        self.graphics_widget = pg.GraphicsLayoutWidget()
        layout.addWidget(self.graphics_widget)

        self.hist_lut = pg.HistogramLUTItem()
        cmap = pg.colormap.get("hot", source="matplotlib")
        self.hist_lut.gradient.setColorMap(cmap)
        self.graphics_widget.addItem(self.hist_lut, row=0, col=1)

        self.plot = self.graphics_widget.addPlot(row=0, col=0)
        self.plot.setLabel("bottom", "Frame Number")
        self.plot.setLabel("left", "Row Number")

        self.image_item = pg.ImageItem()
        self.plot.addItem(self.image_item)
        self.hist_lut.setImageItem(self.image_item)

        # Keep track of all created graphical items for easy clearing
        self.hotspot_ellipse_items = []
        self.hotspot_label_items = []

        # Create a single ScatterPlotItem to efficiently handle all center markers
        self.hotspot_center_markers = pg.ScatterPlotItem(
            size=10, pen=pg.mkPen("y", width=2), brush=None, symbol="x"
        )
        self.plot.addItem(self.hotspot_center_markers)
        # ADDED: Connect the mouse click signal for interactivity
        self.mouse_coord_text = pg.TextItem(color="g", anchor=(0, 1))
        self.mouse_coord_text.setZValue(100)  # Ensure it's on top
        self.plot.addItem(self.mouse_coord_text, ignoreBounds=True)

        # --- ADDED: Proxy for tracking mouse movement ---
        self.mouse_proxy = pg.SignalProxy(
            self.plot.scene().sigMouseMoved, rateLimit=60, slot=self._on_mouse_moved
        )
        self.plot.scene().sigMouseClicked.connect(self._on_plot_clicked)

        self.selection_rect_item = None

    def populate_metrics(self, data_matrices: dict):
        """Populates the metric selector and stores the data."""
        self.data_matrices = data_matrices
        self.metric_selector.blockSignals(True)
        self.metric_selector.clear()
        available_metrics = sorted(self.data_matrices.keys())
        self.metric_selector.addItems(available_metrics)
        if self.default_metric and self.default_metric in available_metrics:
            index = self.metric_selector.findText(self.default_metric)
            if index != -1:
                self.metric_selector.setCurrentIndex(index)
        self.metric_selector.blockSignals(False)

        if self.metric_selector.count() > 0:
            self._on_metric_changed()

    def _on_metric_changed(self):
        """Called when the user selects a new metric from the dropdown."""
        metric = self.metric_selector.currentText()
        if not metric or metric not in self.data_matrices:
            return

        self.current_metric = metric
        self._clear_selection_highlight()
        self.update_heatmap()

    def update_heatmap(self):
        """Updates the ImageItem with the correct orientation and data."""
        if self.current_metric not in self.data_matrices:
            self.image_item.clear()
            return

        data = self.data_matrices[self.current_metric]
        self.image_item.setImage(data)

        # --- START: MODIFIED ORIENTATION & LABELS ---
        is_column_scan = "column" in self.scan_mode

        if is_column_scan:
            self.plot.setLabel("bottom", "Column Number")
            self.plot.setLabel("left", "Frame Number")
            self.plot.invertY(False)  # Frames start at top (y=0)
            self.plot.invertX(False) # Columns start at left (x=0)

            num_frames, num_cols = data.shape
            x_tick_interval = max(1, num_cols // 15 if num_cols > 0 else 1)
            x_ticks = [(i, str(i + 1)) for i in range(num_cols) if (i) % x_tick_interval == 0]
            y_tick_interval = max(1, num_frames // 5 if num_frames > 0 else 1)
            y_ticks = [(i, str(i + 1)) for i in range(num_frames) if (i) % y_tick_interval == 0]

            self.plot.setRange(
                xRange=(-0.5, num_cols - 0.5), yRange=(-0.5, num_frames - 0.5), padding=0
            )
        else:  # Row scan
            self.plot.setLabel("bottom", "Frame Number")
            self.plot.setLabel("left", "Row Number")
            self.plot.invertY(True)  # Rows start at top
            self.plot.invertX(False)

            num_rows, num_cols = data.shape
            x_tick_interval = max(1, num_cols // 15 if num_cols > 0 else 1)
            x_ticks = [(i, str(i + 1)) for i in range(num_cols) if (i) % x_tick_interval == 0]
            y_tick_interval = max(1, num_rows // 5 if num_rows > 0 else 1)
            y_ticks = [(i, str(i + 1)) for i in range(num_rows) if (i) % y_tick_interval == 0]

            self.plot.setRange(
                xRange=(-0.5, num_cols - 0.5), yRange=(-0.5, num_rows - 0.5), padding=0
            )

        self.plot.getAxis("bottom").setTicks([x_ticks])
        self.plot.getAxis("left").setTicks([y_ticks])
        # --- END: MODIFIED ORIENTATION & LABELS ---

        self.plot.setTitle(f"Heatmap of '{self.current_metric}'")

    def _clear_selection_highlight(self):
        """Removes the selection rectangle from the plot if it exists."""
        if self.selection_rect_item:
            self.plot.removeItem(self.selection_rect_item)
            self.selection_rect_item = None

    def _on_plot_clicked(self, event):
        """Handles mouse clicks on the plot to identify and highlight the clicked cell."""
        if event.button() == QtCore.Qt.LeftButton and self.image_item.image is not None:
            pos = self.image_item.mapFromScene(event.scenePos())
            matrix_col = int(pos.x())
            matrix_row = int(pos.y())
            num_rows, num_cols = self.image_item.image.shape

            if 0 <= matrix_row < num_rows and 0 <= matrix_col < num_cols:
                logger.info(
                    f"Heatmap cell clicked: Matrix Coords (Row, Col)=({matrix_row}, {matrix_col})"
                )

                self._clear_selection_highlight() 

                ### MODIFIED: The rectangle's top-left corner is the integer coordinate.
                self.selection_rect_item = QtWidgets.QGraphicsRectItem(
                    matrix_col, matrix_row, 1, 1
                )
                
                pen = pg.mkPen('lime', width=3)
                self.selection_rect_item.setPen(pen)
                self.selection_rect_item.setZValue(50) 
                self.plot.addItem(self.selection_rect_item)

                self.cell_clicked.emit(matrix_row, matrix_col)

    def _on_find_hotspots(self):
        """Emits a signal requesting hotspot analysis with all parameters."""
        percentile = self.percentile_spinbox.value()
        min_size = self.min_size_spinbox.value()
        mode_text = self.find_mode_selector.currentText()
        find_mode = "valleys" if "Valleys" in mode_text else "peaks"
        self.find_hotspots_requested.emit(find_mode, percentile, min_size)

    def _on_export_to_console(self):
        """Exports statistics and data of the currently displayed heatmap to the embedded console."""
        if not self.current_metric or self.current_metric not in self.data_matrices:
            logger.warning("No data to export.")
            return

        data = self.data_matrices[self.current_metric]
        if data is None:
            return
            
        # Calculate stats
        count_gt_0 = np.sum(data > 0)
        # Use simple equality for valid pixel counts (often integers)
        count_eq_1 = np.sum(data == 1)
        
        stats = {
            "min": np.nanmin(data),
            "max": np.nanmax(data),
            "mean": np.nanmean(data),
            "count_gt_0": count_gt_0,
            "count_eq_1": count_eq_1,
            "shape": data.shape
        }

        mw = self.parent()
        if hasattr(mw, 'console_widget'):
            console = mw.console_widget
            if console:
                vars_to_push = {
                    'heatmap_data': data,
                    'heatmap_stats': stats
                }
                console.push_vars(vars_to_push)
                
                # Ensure console is visible
                if hasattr(mw, 'console_dock'):
                    mw.console_dock.show()
                    mw.console_dock.raise_()
                
                msg = f"Exported '{self.current_metric}' to console as 'heatmap_data' and 'heatmap_stats'."
                mw.ui_manager.show_status_message(msg, 5000)
                
                # Print confirmation to the console output
                info_str = f"\n# --- Heatmap Export: {self.current_metric} ---\n# Stats: {stats}\n# Data available in 'heatmap_data'\n"
                
                # Handle AdvancedConsoleWidget (IPython)
                if hasattr(console, 'console_widget') and hasattr(console.console_widget, 'append_stream'):
                     console.console_widget.append_stream(info_str)
                # Handle PythonConsoleWidget (Standard)
                elif hasattr(console, 'output') and hasattr(console.output, 'appendPlainText'):
                     console.output.appendPlainText(info_str)
            else:
                mw.ui_manager.show_warning_message("Export Failed", "Console initialization failed.")
        else:
             logger.error("HeatmapDialog parent does not appear to be the main window (no console_widget).")

    def show_hotspot_markers(self, hotspots: list):
        """
        Displays shapes, center markers, and size labels for each hotspot.
        """
        self.clear_hotspot_markers()

        shape_mode = self.shape_selector.currentText()
        centers_for_scatter = []
        label_font = QtGui.QFont("Arial", 8)

        for hotspot in hotspots:
            center_x, center_y = hotspot["center"]
            width, height = hotspot["width"], hotspot["height"]
            angle = hotspot["angle"]

            if "Ellipse" in shape_mode:
                shape_item = QtWidgets.QGraphicsEllipseItem(
                    -width / 2, -height / 2, width, height
                )
            else:  # Rectangle
                shape_item = QtWidgets.QGraphicsRectItem(
                    -width / 2, -height / 2, width, height
                )

            shape_item.setPen(pg.mkPen("c", width=2, style=QtCore.Qt.DotLine))
            transform = QtGui.QTransform()
            transform.translate(center_x, center_y)
            transform.rotate(angle)
            shape_item.setTransform(transform)

            self.plot.addItem(shape_item)
            self.hotspot_ellipse_items.append(shape_item)

            centers_for_scatter.append({"pos": (center_x, center_y)})

            label_text = f"{max(width, height):.1f}x{min(width, height):.1f}"
            text_item = pg.TextItem(text=label_text, color="cyan", anchor=(0.0, 1.0))
            text_item.setFont(label_font)
            text_item.setPos(center_x + 1.0, center_y - 1.0)
            self.plot.addItem(text_item)
            self.hotspot_label_items.append(text_item)

        self.hotspot_center_markers.setData(centers_for_scatter)

    def clear_hotspot_markers(self):
        """Removes all hotspot-related graphics from the plot."""
        for item in self.hotspot_ellipse_items:
            self.plot.removeItem(item)
        self.hotspot_ellipse_items = []

        for item in self.hotspot_label_items:
            self.plot.removeItem(item)
        self.hotspot_label_items = []

        self.hotspot_center_markers.clear()

    def _on_find_mode_changed(self, index):
        if index == 0:  # Peaks
            self.percentile_spinbox.setRange(50, 100)
            self.percentile_spinbox.setValue(95)
            self.percentile_spinbox.setToolTip(
                "Find regions in the top (100 - value)% of data"
            )
        else:  # Valleys
            self.percentile_spinbox.setRange(0, 50)
            self.percentile_spinbox.setValue(5)
            self.percentile_spinbox.setToolTip(
                "Find regions in the bottom value% of data"
            )

    def _on_mouse_moved(self, event):
        pos = event[0]
        if (
                self.plot.sceneBoundingRect().contains(pos)
                and self.image_item.image is not None
        ):
            mouse_point = self.plot.getViewBox().mapSceneToView(pos)

            x = int(mouse_point.x())
            y = int(mouse_point.y())

            num_rows, num_cols = self.image_item.image.shape
            is_column_scan = "column" in self.scan_mode
            if 0 <= y < num_rows and 0 <= x < num_cols:
                value = self.image_item.image[y, x]
                if is_column_scan:
                    coord_text = f"Frame: {y + 1}, Col: {x + 1}, Value: {value:.3f}"
                else:
                    coord_text = f"Row: {y + 1}, Frame: {x + 1}, Value: {value:.3f}"
                
                self.mouse_coord_text.setText(coord_text)
                self.mouse_coord_text.setPos(mouse_point)
                self.mouse_coord_text.setVisible(True)
            else:
                self.mouse_coord_text.setVisible(False)
        else:
            self.mouse_coord_text.setVisible(False)
