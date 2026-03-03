import numpy as np
import pyqtgraph as pg
import pyqtgraph.opengl as gl
from PyQt5 import QtCore, QtWidgets, QtGui

class TwoDProfileDialog(QtWidgets.QDialog):
    def __init__(self, data, pixel_size=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("2D Profile Analysis")
        self.resize(1200, 900)
        
        self.pixel_size = pixel_size
        self.intensity_scale = 1.0
        self.mask_signal = None 
        
        # Init dummy data for setup_ui if needed, but we will call set_data immediately
        self.data = data
        
        # Statistics placeholders
        self.mean_val = 0
        self.std_val = 0
        self.min_val = 0
        self.max_val = 0
        self.threshold = 0

        # Initial Camera Settings (for reset)
        self.initial_cam_dist = max(self.data.shape) * 2
        self.initial_cam_elev = 45
        self.initial_cam_azim = 45

        # Initial Axis Sizes (for reset of axes and scale of axis item)
        self.initial_axis_x_size = self.data.shape[1]
        self.initial_axis_y_size = self.data.shape[0]
        self.initial_axis_z_size = 1

        self.setup_ui()
        self.set_data(data, pixel_size)

    def set_data(self, data, pixel_size=None):
        """Updates the dialog with new data without destroying the GL context."""
        self.data = data
        self.pixel_size = pixel_size
        
        # Statistics
        self.mean_val = np.mean(data)
        self.std_val = np.std(data)
        self.min_val = np.min(data)
        self.max_val = np.max(data)
        
        # Initial Threshold
        self.threshold = self.mean_val + 3.0 * self.std_val
        if self.threshold > self.max_val: self.threshold = self.max_val
        if self.threshold < self.min_val: self.threshold = self.mean_val

        # Update Thresh Line
        # Block signals to avoid triggering unnecessary updates
        self.thresh_line.blockSignals(True)
        self.thresh_line.setValue(self.threshold)
        self.thresh_line.blockSignals(False)

        # Update initial parameters for reset
        self.initial_axis_x_size = self.data.shape[1]
        self.initial_axis_y_size = self.data.shape[0]
        self.initial_axis_z_size = self.max_val if self.max_val > 0 else 1

        # Update Surface Plot Geometry (x, y)
        # Note: GLSurfacePlotItem doesn't easily support changing x/y grid shape 
        # without setData with x and y.
        try:
            self.surface_plot.setData(
                x=np.arange(self.data.shape[1]),
                y=np.arange(self.data.shape[0]),
                z=self.data.T * self.intensity_scale
            )
        except Exception:
            # If shapes are totally incompatible, might need to recreate item, 
            # but usually setData works if arguments are correct.
            pass

        # Update Analysis (re-calc mask, stats, histogram, visuals)
        self.update_analysis()

    def setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)
        
        # Splitter: 3D View (Top) / Histogram (Bottom)
        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        layout.addWidget(splitter, stretch=1)

        # --- 3D View Container ---
        view_container = QtWidgets.QWidget()
        view_layout = QtWidgets.QVBoxLayout(view_container)
        view_layout.setContentsMargins(0,0,0,0)
        
        self.gl_view = gl.GLViewWidget()
        self.gl_view.setCameraPosition(distance=max(self.data.shape)*2, elevation=45, azimuth=45)
        
        # Grids
        g = gl.GLGridItem()
        g.scale(10, 10, 1)
        self.gl_view.addItem(g)

        # Add axes for orientation
        self.axis_item = gl.GLAxisItem()
        # Scale the axis item to match the data dimensions (unscaled Z for now)
        self.axis_item.setSize(
            x=self.initial_axis_x_size,
            y=self.initial_axis_y_size,
            z=self.initial_axis_z_size
        )
        self.gl_view.addItem(self.axis_item)

        # Surface Plot
        # z array needs to be transposed to shape (len(x), len(y)) i.e. (cols, rows)
        self.surface_plot = gl.GLSurfacePlotItem(
            x=np.arange(self.data.shape[1]),
            y=np.arange(self.data.shape[0]),
            z=self.data.T,
            computeNormals=False,
            smooth=False
        )
        self.gl_view.addItem(self.surface_plot)
        
        view_layout.addWidget(self.gl_view)
        splitter.addWidget(view_container)

        # --- Histogram ---
        self.hist_widget = pg.PlotWidget(title="Intensity Histogram")
        self.hist_widget.setFixedHeight(200)
        self.hist_widget.setLabel('bottom', 'Intensity')
        self.hist_widget.setLabel('left', 'Count')
        self.hist_plot_item = self.hist_widget.plot(stepMode=True, fillLevel=0, brush=(0, 0, 255, 150))
        
        # Threshold Line on Histogram
        self.thresh_line = pg.InfiniteLine(pos=self.threshold, movable=True, angle=90, pen='y', label='T={value:.1f}', labelOpts={'position':0.1, 'color': (200,200,0), 'movable': True, 'fill': (0, 0, 200, 100)})
        self.thresh_line.sigPositionChangeFinished.connect(self._on_thresh_line_moved)
        self.hist_widget.addItem(self.thresh_line)
        
        splitter.addWidget(self.hist_widget)

        # --- Controls Layout ---
        controls_group = QtWidgets.QGroupBox("Controls")
        controls_layout = QtWidgets.QGridLayout(controls_group)
        layout.addWidget(controls_group)

        # Row 0: View Mode & Colormap
        controls_layout.addWidget(QtWidgets.QLabel("View Mode:"), 0, 0)
        self.combo_view_mode = QtWidgets.QComboBox()
        self.combo_view_mode.addItems(["Surface", "Wireframe"])
        self.combo_view_mode.currentIndexChanged.connect(lambda: self.update_visuals())
        controls_layout.addWidget(self.combo_view_mode, 0, 1)

        controls_layout.addWidget(QtWidgets.QLabel("Colormap:"), 0, 2)
        self.combo_cmap = QtWidgets.QComboBox()
        self.combo_cmap.addItems(["Binary (Red/White)", "viridis", "plasma", "inferno", "magma", "bipolar", "thermal", "grey"])
        self.combo_cmap.setCurrentText("plasma") # Set default to plasma
        self.combo_cmap.currentIndexChanged.connect(lambda: self.update_visuals())
        controls_layout.addWidget(self.combo_cmap, 0, 3)

        # Row 1: Z-Scale Slider
        controls_layout.addWidget(QtWidgets.QLabel("Z-Scale:"), 1, 0)
        self.scale_slider = QtWidgets.QSlider(QtCore.Qt.Horizontal)
        self.scale_slider.setRange(1, 200)
        self.scale_slider.setValue(10)
        self.scale_slider.valueChanged.connect(self._on_scale_changed)
        controls_layout.addWidget(self.scale_slider, 1, 1, 1, 2)
        self.scale_label = QtWidgets.QLabel("1.0x")
        controls_layout.addWidget(self.scale_label, 1, 3)

        # Reset View Button
        self.btn_reset_view = QtWidgets.QPushButton("Reset 3D View")
        self.btn_reset_view.clicked.connect(self._reset_3d_view)
        controls_layout.addWidget(self.btn_reset_view, 2, 0, 1, 4) # Span all columns

        # --- Statistics ---
        stats_group = QtWidgets.QGroupBox("Statistics")
        stats_layout = QtWidgets.QGridLayout(stats_group)
        layout.addWidget(stats_group)
        
        self.lbl_sig_pixels = QtWidgets.QLabel("0")
        self.lbl_bg_pixels = QtWidgets.QLabel("0")
        self.lbl_avg_bg = QtWidgets.QLabel("0.0")
        self.lbl_total_signal = QtWidgets.QLabel("0.0")
        self.lbl_max_intensity = QtWidgets.QLabel("0.0")
        
        stats_layout.addWidget(QtWidgets.QLabel("Signal Pixels:"), 0, 0); stats_layout.addWidget(self.lbl_sig_pixels, 0, 1)
        stats_layout.addWidget(QtWidgets.QLabel("Background Pixels:"), 0, 2); stats_layout.addWidget(self.lbl_bg_pixels, 0, 3)
        stats_layout.addWidget(QtWidgets.QLabel("Avg Bg Intensity:"), 1, 0); stats_layout.addWidget(self.lbl_avg_bg, 1, 1)
        stats_layout.addWidget(QtWidgets.QLabel("Net Signal:"), 1, 2); stats_layout.addWidget(self.lbl_total_signal, 1, 3)
        stats_layout.addWidget(QtWidgets.QLabel("Max Intensity:"), 2, 0); stats_layout.addWidget(self.lbl_max_intensity, 2, 1)
        
        # Dimensions info
        dim_info = f"{self.data.shape[1]} x {self.data.shape[0]} px (Grid 10x10)"
        stats_layout.addWidget(QtWidgets.QLabel("Region Size:"), 2, 2)
        stats_layout.addWidget(QtWidgets.QLabel(dim_info), 2, 3)

        # Buttons
        button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Close)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _on_thresh_line_moved(self):
        self.threshold = self.thresh_line.value()
        self.update_analysis()

    def _on_scale_changed(self, value):
        self.intensity_scale = value / 10.0
        self.scale_label.setText(f"{self.intensity_scale:.1f}x")
        self.update_visuals()

    def _reset_3d_view(self):
        self.gl_view.setCameraPosition(
            distance=self.initial_cam_dist,
            elevation=self.initial_cam_elev,
            azimuth=self.initial_cam_azim
        )

    def update_analysis(self):
        # 1. Classification
        self.mask_signal = self.data > self.threshold
        mask_bg = ~self.mask_signal

        num_sig = np.sum(self.mask_signal)
        num_bg = np.sum(mask_bg)

        # 2. Stats
        avg_bg = np.mean(self.data[mask_bg]) if num_bg > 0 else 0.0
        sum_sig = np.sum(self.data[self.mask_signal]) if num_sig > 0 else 0.0
        net_signal = sum_sig - (num_sig * avg_bg)
        max_intensity = np.max(self.data)

        # 3. Update Labels
        self.lbl_sig_pixels.setText(f"{num_sig}")
        self.lbl_bg_pixels.setText(f"{num_bg}")
        self.lbl_avg_bg.setText(f"{avg_bg:.2f}")
        self.lbl_total_signal.setText(f"{net_signal:.2f}")
        self.lbl_max_intensity.setText(f"{max_intensity:.1f}")

        # 4. Update Visuals
        self.update_visuals()
        
        # 5. Update Histogram
        self.update_histogram()

    def update_visuals(self):
        # View Mode
        mode = self.combo_view_mode.currentText()
        if mode == "Wireframe":
            self.surface_plot.opts['drawEdges'] = True
            self.surface_plot.opts['drawFaces'] = False
        else: # Surface
            self.surface_plot.opts['drawEdges'] = False
            self.surface_plot.opts['drawFaces'] = True
            
        # Colormap
        cmap_name = self.combo_cmap.currentText()
        
        if cmap_name.startswith("Binary"):
            colors = np.ones((self.data.shape[0], self.data.shape[1], 4), dtype=np.float32)
            if self.mask_signal is not None:
                colors[self.mask_signal] = (1, 0, 0, 1)
        else:
            # Gradient
            # Normalize Z
            rng = self.max_val - self.min_val
            if rng == 0: rng = 1
            z_norm = (self.data - self.min_val) / rng
            
            # Get colormap
            try:
                cmap = pg.colormap.get(cmap_name)
                colors = cmap.map(z_norm, mode='float')
                
                # Apply Threshold Visuals: Reduce alpha for background
                if self.mask_signal is not None:
                    # Make background pixels semi-transparent
                    colors[~self.mask_signal, 3] = 0.3
            except Exception:
                # Fallback if map fails or name invalid
                colors = np.ones((self.data.shape[0], self.data.shape[1], 4), dtype=np.float32)
        
        # Transpose and Flatten Colors
        colors_T = colors.transpose(1, 0, 2)
        
        # Update Data
        self.surface_plot.setData(z=self.data.T * self.intensity_scale, colors=colors_T.reshape(-1, 4))
        
        # Update Axis Item Z scale
        self.axis_item.setSize(
            x=self.initial_axis_x_size,
            y=self.initial_axis_y_size,
            z=self.initial_axis_z_size * self.intensity_scale
        )
        self.surface_plot.update() # Force redraw

    def update_histogram(self):
        # Calculate histogram
        y, x = np.histogram(self.data, bins=50)
        self.hist_plot_item.setData(x, y)