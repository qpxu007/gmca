
import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtWidgets, QtGui

from qp2.image_viewer.utils.ring_math import angstrom_to_pixels
from qp2.image_viewer.workers.ice_ring_analyzer import find_and_classify_ice_rings
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

class IceRingResultsDialog(QtWidgets.QDialog):
    """
    A dialog to display radial profile and interactively adjust ice ring detection sensitivity.
    """
    apply_rings = QtCore.pyqtSignal(list, str) # Emits (list_of_rings_pixels, summary_text)
    deice_request = QtCore.pyqtSignal(object, list) # Emits (profile_data, rings_details)

    def __init__(self, result_dict, params, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Ice Ring Analysis")
        self.resize(800, 600)
        
        self.result_dict = result_dict
        # Worker now returns single profile in "radial_profile"
        self.profile_data = result_dict.get("radial_profile")
        
        self.params = params
        self.sensitivity = 1.0
        
        self.current_results = result_dict # Start with initial results

        self._setup_ui()
        # self._update_analysis() # Initial results already passed in, just plot them? 
        # Actually, let's plot initial state. 
        self._update_plot()
        self._update_summary()

    def _setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)

        # --- Controls ---
        controls_layout = QtWidgets.QHBoxLayout()
        
        controls_layout.addWidget(QtWidgets.QLabel("Sensitivity:"))
        self.sensitivity_spin = QtWidgets.QDoubleSpinBox()
        self.sensitivity_spin.setRange(0.1, 5.0)
        self.sensitivity_spin.setSingleStep(0.1)
        self.sensitivity_spin.setValue(self.sensitivity)
        self.sensitivity_spin.valueChanged.connect(self._on_sensitivity_changed)
        controls_layout.addWidget(self.sensitivity_spin)
        
        controls_layout.addWidget(QtWidgets.QLabel("Ignore Resol > (Å):"))
        self.res_cutoff_spin = QtWidgets.QDoubleSpinBox()
        self.res_cutoff_spin.setRange(1.0, 50.0) # Reasonable range
        self.res_cutoff_spin.setSingleStep(0.5)
        self.res_cutoff_spin.setValue(4.0) # Default
        self.res_cutoff_spin.valueChanged.connect(self._on_sensitivity_changed) # Reuse handler
        controls_layout.addWidget(self.res_cutoff_spin)
        
        self.apply_btn = QtWidgets.QPushButton("Apply to Image")
        self.apply_btn.clicked.connect(self._on_apply)
        controls_layout.addWidget(self.apply_btn)
        
        self.deice_btn = QtWidgets.QPushButton("De-ice (Subtract Bg)")
        self.deice_btn.setToolTip("Subtract the radial profile from the image to remove rings while preserving spots.")
        self.deice_btn.clicked.connect(self._on_deice)
        controls_layout.addWidget(self.deice_btn)
        
        layout.addLayout(controls_layout)

        # --- Plot ---
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground('w')
        self.plot_widget.setTitle("Radial Profile", color="black")
        self.plot_widget.setLabel("left", "Average Intensity", color="black")
        self.plot_widget.setLabel("bottom", "Radius (pixels)", color="black")
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        
        # Style axes
        plot_item = self.plot_widget.getPlotItem()
        axis_pen = pg.mkPen("black")
        for axis_name in ["left", "bottom"]:
            ax = plot_item.getAxis(axis_name)
            ax.setPen(axis_pen)
            ax.setTextPen("black")

        layout.addWidget(self.plot_widget)

        # --- Summary Text ---
        self.summary_label = QtWidgets.QLabel("Ready.")
        self.summary_label.setStyleSheet("font-weight: bold; color: #333;")
        layout.addWidget(self.summary_label)

    def _on_sensitivity_changed(self, val):
        self.sensitivity = self.sensitivity_spin.value() # Update from spinbox
        self._update_analysis()

    def _update_analysis(self):
        if not self.profile_data:
            return

        bin_centers, radial_profile = self.profile_data
        
        # Run detection logic
        bins_np = np.array(bin_centers)
        prof_np = np.array(radial_profile)
        
        try:
            self.current_results = find_and_classify_ice_rings(
                bins_np, 
                prof_np, 
                self.params, 
                sensitivity=self.sensitivity,
                max_d_spacing=self.res_cutoff_spin.value()
            )
            self._update_plot()
            self._update_summary()
        except Exception as e:
            logger.error(f"Error updating ice ring analysis: {e}", exc_info=True)
            self.summary_label.setText(f"Error: {e}")

    def _update_plot(self):
        self.plot_widget.clear()
        
        if not self.profile_data:
            return

        bin_centers, radial_profile = self.profile_data
        
        # Plot profile
        self.plot_widget.plot(
            bin_centers, 
            radial_profile, 
            pen=pg.mkPen('k', width=1.5)
        )

        # Plot detected rings
        rings_details = self.current_results.get("rings_details", [])
        
        for i, ring in enumerate(rings_details):
            r_px = ring["radius_pixels"]
            res = ring["resolution"]
            w_px = ring.get("width_pixels", 0)
            w_ang = ring.get("width_angstrom", 0)
            rtype = ring.get("type", "Unknown")
            
            # Color coding
            color = "r" # Default red
            if "Hexagonal" in rtype:
                color = "b" # Blue for Hex
            elif "Cubic" in rtype:
                color = "m" # Magenta for Cubic
            elif "Both" in rtype:
                color = "purple"

            # Format width string, handling small values
            w_ang_str = f"{w_ang:.3f}" if w_ang >= 0.001 else f"{w_ang:.4f}"
            label_text = f"{res}Å (W:{w_px}px/{w_ang_str}Å)"
            tooltip = f"Resolution: {res}Å\nType: {rtype}\nWidth: {w_px}px (~{w_ang}Å)"

            # Alternate label positions to reduce overlap
            pos_y = 0.8 if i % 2 == 0 else 0.6

            line = pg.InfiniteLine(
                pos=r_px, 
                angle=90, 
                pen=pg.mkPen(color, style=QtCore.Qt.DashLine),
                label=label_text,
                labelOpts={"color": color, "position": pos_y, "movable": True}
            )
            line.setToolTip(tooltip)
            self.plot_widget.addItem(line)

    def _update_summary(self):
        inference = self.current_results.get("inference", "Unknown")
        count = len(self.current_results.get("ice_rings_found", []))
        self.summary_label.setText(f"Detection: {inference} ({count} rings)")

    def _on_apply(self):
        rings_found = self.current_results.get("ice_rings_found", [])
        inference = self.current_results.get("inference", "")
        
        # Convert resolutions back to pixels for the main viewer
        rings_px = []
        for res in rings_found:
            try:
                r_px = angstrom_to_pixels(
                    res,
                    self.params["wavelength"],
                    self.params["det_dist"],
                    self.params["pixel_size"],
                )
                rings_px.append((r_px, res))
            except Exception:
                pass
        
        self.apply_rings.emit(rings_px, inference)

    def _on_deice(self):
        if self.profile_data:
            rings_details = self.current_results.get("rings_details", [])
            self.deice_request.emit(self.profile_data, rings_details)
