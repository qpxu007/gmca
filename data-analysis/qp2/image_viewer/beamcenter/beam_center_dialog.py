from PyQt5 import QtWidgets, QtCore, QtGui
import numpy as np
from qp2.image_viewer.beamcenter.auto_center import (
    optimize_beam_center,
    optimize_beam_center_iterative,
    remove_spots,
    estimate_beamstop_radius,
    calculate_center_of_mass,
)
from qp2.image_viewer.beamcenter.beam_center_updater import (
    update_beam_center_in_master_file,
)
from qp2.log.logging_config import get_logger
import os

logger = get_logger(__name__)


class BeamCenterWorker(QtCore.QThread):
    finished = QtCore.pyqtSignal(object, str)  # result (tuple or None), error_message

    def __init__(self, image, start_guess, mask, method, limit, min_radius, max_radius):
        super().__init__()
        self.image = image
        self.start_guess = start_guess
        self.mask = mask
        self.method = method
        self.limit = limit
        self.min_radius = min_radius
        self.max_radius = max_radius

    def run(self):
        try:
            # Run spot removal if not already done?
            # Ideally the mask passed in should already handle spots or we do it here.
            # Doing it here might be safer to keep UI thread clean.
            # But 'remove_spots' modifies mask.

            # Let's assume mask is basic saturation mask, we refine it here.
            refined_mask = remove_spots(self.image, self.mask)
            
            # Calculate Center of Mass (intensity centroid)
            com_x, com_y = calculate_center_of_mass(self.image, refined_mask)

            # Use the iterative optimization strategy which combines beamstop estimation and center refinement
            center, time_taken = optimize_beam_center_iterative(
                self.image,
                self.start_guess,
                refined_mask,
                method=self.method,
                verbose=True,
                limit=self.limit,
                min_radius=self.min_radius,
            )
            
            result = {
                "optimized_center": center,
                "center_of_mass": (com_x, com_y),
                "time_taken": time_taken
            }
            self.finished.emit(result, "")
        except Exception as e:
            logger.error(f"Beam center optimization failed: {e}", exc_info=True)
            self.finished.emit(None, str(e))


class BeamCenterDialog(QtWidgets.QDialog):
    beam_center_updated = QtCore.pyqtSignal(
        float, float
    )  # Signal when update is successful

    def __init__(self, parent=None, image=None, params=None):
        super().__init__(parent)
        self.setWindowTitle("Master File Geometry Update")
        self.resize(450, 550)

        self.image = image
        self.params = params or {}
        self.calculated_center = None
        self.current_center = (
            self.params.get("beam_x", 0),
            self.params.get("beam_y", 0),
        )

        self.worker = None

        self._init_ui()

    def _init_ui(self):
        layout = QtWidgets.QVBoxLayout(self)

        # --- 1. Main Geometry Editing Section ---
        grp_geometry = QtWidgets.QGroupBox("Geometry Parameters")
        form_geometry = QtWidgets.QFormLayout(grp_geometry)

        # X
        self.spin_new_x = QtWidgets.QDoubleSpinBox()
        self.spin_new_x.setRange(0, 10000)
        self.spin_new_x.setDecimals(2)
        self.spin_new_x.setButtonSymbols(QtWidgets.QAbstractSpinBox.NoButtons)
        self.spin_new_x.setValue(self.current_center[0])
        self.spin_new_x.valueChanged.connect(self._on_values_changed)
        form_geometry.addRow("Beam Center X (px):", self.spin_new_x)

        # Y
        self.spin_new_y = QtWidgets.QDoubleSpinBox()
        self.spin_new_y.setRange(0, 10000)
        self.spin_new_y.setDecimals(2)
        self.spin_new_y.setButtonSymbols(QtWidgets.QAbstractSpinBox.NoButtons)
        self.spin_new_y.setValue(self.current_center[1])
        self.spin_new_y.valueChanged.connect(self._on_values_changed)
        form_geometry.addRow("Beam Center Y (px):", self.spin_new_y)
        
        # Wavelength
        self.spin_wavelength = QtWidgets.QDoubleSpinBox()
        self.spin_wavelength.setRange(0.0001, 100.0) 
        self.spin_wavelength.setDecimals(4)
        self.spin_wavelength.setButtonSymbols(QtWidgets.QAbstractSpinBox.NoButtons)
        self.spin_wavelength.setValue(self.params.get("wavelength", 1.0))
        self.spin_wavelength.setToolTip("Incident Wavelength in Angstroms")
        self.spin_wavelength.valueChanged.connect(self._on_values_changed)
        form_geometry.addRow("Wavelength (Å):", self.spin_wavelength)

        # Detector Distance
        self.spin_det_dist = QtWidgets.QDoubleSpinBox()
        self.spin_det_dist.setRange(0.0, 10000.0) 
        self.spin_det_dist.setDecimals(2)
        self.spin_det_dist.setButtonSymbols(QtWidgets.QAbstractSpinBox.NoButtons)
        self.spin_det_dist.setValue(self.params.get("det_dist", 100.0))
        self.spin_det_dist.setToolTip("Detector Distance in mm")
        self.spin_det_dist.valueChanged.connect(self._on_values_changed)
        form_geometry.addRow("Detector Distance (mm):", self.spin_det_dist)

        layout.addWidget(grp_geometry)

        # --- 2. Auxiliary Refinement Section ---
        self.grp_refinement = QtWidgets.QGroupBox("Estimate Beam Center w Diffuse Background")
        self.grp_refinement.setCheckable(True)
        self.grp_refinement.setChecked(False) # Default to off/collapsed
        
        refine_layout = QtWidgets.QVBoxLayout(self.grp_refinement)
        
        # Settings Form inside
        form_settings = QtWidgets.QFormLayout()
        
        self.combo_method = QtWidgets.QComboBox()
        self.combo_method.addItems(["Robust (Radial Symmetry)", "Variance (Sharpest Spots)"])
        self.combo_method.setToolTip("Robust ignores spots (better for powder/background). Variance needs spots.")
        form_settings.addRow("Method:", self.combo_method)

        self.spin_limit = QtWidgets.QDoubleSpinBox()
        self.spin_limit.setRange(1, 500)
        self.spin_limit.setValue(50)
        self.spin_limit.setSuffix(" px")
        self.spin_limit.setToolTip("Maximum distance from current center to search")
        form_settings.addRow("Search Limit:", self.spin_limit)

        # Estimate beamstop
        estimated_min_radius = 100
        if self.image is not None:
            try:
                # Prepare mask (saturation only for estimation)
                sat_val = self.params.get("saturation_value", 2**32 - 1)
                if sat_val is None:
                    sat_val = 2**32 - 1
                mask = self.image >= sat_val

                est_r = estimate_beamstop_radius(
                    self.current_center, self.image, mask=mask
                )
                if est_r > 10:  # Sanity check
                    estimated_min_radius = int(est_r * 1.1)  # Add slight buffer
                    logger.info(
                        f"Estimated beamstop radius: {est_r:.1f} px -> Default min_radius: {estimated_min_radius}"
                    )
            except Exception as e:
                logger.warning(f"Failed to estimate beamstop radius: {e}")

        self.spin_min_radius = QtWidgets.QSpinBox()
        self.spin_min_radius.setRange(0, 2000)
        self.spin_min_radius.setValue(estimated_min_radius)
        self.spin_min_radius.setSuffix(" px")
        self.spin_min_radius.setToolTip("Inner radius to exclude (beamstop)")
        form_settings.addRow("Min Radius:", self.spin_min_radius)
        
        # CoM Result (informational)
        self.lbl_com = QtWidgets.QLabel("N/A")
        self.lbl_com.setToolTip("Intensity-weighted center of mass (ignoring spots/mask)")
        form_settings.addRow("Center of Mass:", self.lbl_com)
        
        refine_layout.addLayout(form_settings)

        # Calculate Button
        self.btn_calculate = QtWidgets.QPushButton("Estimate")
        self.btn_calculate.clicked.connect(self.start_calculation)
        self.btn_calculate.setStyleSheet("padding: 5px;")
        refine_layout.addWidget(self.btn_calculate)
        
        layout.addWidget(self.grp_refinement)

        # --- 3. Update Options & Action ---
        grp_actions = QtWidgets.QGroupBox("Save Options")
        action_layout = QtWidgets.QVBoxLayout(grp_actions)

        self.chk_save_nexus = QtWidgets.QCheckBox("Save Nexus compatible copy")
        self.chk_save_nexus.setChecked(False)
        self.chk_save_nexus.setToolTip(
            "If checked, creates a new file (e.g., _nexus.h5) with Nexus compliance instead of overwriting."
        )
        action_layout.addWidget(self.chk_save_nexus)
        
        self.chk_remove_correction = QtWidgets.QCheckBox("Remove Flatfield")
        self.chk_remove_correction.setChecked(True)
        self.chk_remove_correction.setToolTip(
            "If checked, removes 'flatfield' from detectorSpecific to save space. 'pixel_mask' is preserved."
        )
        action_layout.addWidget(self.chk_remove_correction)
        
        layout.addWidget(grp_actions)

        self.btn_update = QtWidgets.QPushButton("Update Master File")
        self.btn_update.clicked.connect(self.update_master_file)
        self.btn_update.setStyleSheet("font-weight: bold; font-size: 14px; padding: 5px;")
        layout.addWidget(self.btn_update)
        
        # Global Status
        self.lbl_status = QtWidgets.QLabel("")
        layout.addWidget(self.lbl_status)

        self.progress_bar = QtWidgets.QProgressBar()
        self.progress_bar.setRange(0, 0)  # Indeterminate
        self.progress_bar.hide()
        layout.addWidget(self.progress_bar)

    def start_calculation(self):
        if self.image is None:
            self.lbl_status.setText("Error: No image loaded.")
            return

        method_key = "robust" if self.combo_method.currentIndex() == 0 else "variance"
        limit = self.spin_limit.value()
        min_r = self.spin_min_radius.value()

        # Determine Max Radius from image size
        h, w = self.image.shape
        max_r = max(h, w)  # Heuristic

        # Prepare mask (saturation)
        sat_val = self.params.get("saturation_value", 2**32 - 1)
        if sat_val is None:
            sat_val = 2**32 - 1
        mask = self.image >= sat_val

        self.btn_calculate.setEnabled(False)
        self.progress_bar.show()
        self.lbl_status.setText("Calculating...")

        self.worker = BeamCenterWorker(
            self.image, list(self.current_center), mask, method_key, limit, min_r, max_r
        )
        self.worker.finished.connect(self.on_calculation_finished)
        self.worker.start()

    def on_calculation_finished(self, result, error):
        self.progress_bar.hide()
        self.btn_calculate.setEnabled(True)

        if error:
            self.lbl_status.setText(f"Error: {error}")
            return

        if result:
            # Handle result dictionary (new) or tuple (old/fallback)
            if isinstance(result, dict):
                center = result.get("optimized_center")
                com = result.get("center_of_mass")
                if com:
                    self.lbl_com.setText(f"({com[0]:.2f}, {com[1]:.2f})")
            else:
                center = result
            
            self.calculated_center = center
            # Update the editable fields
            self.spin_new_x.setValue(center[0])
            self.spin_new_y.setValue(center[1])

            self.lbl_status.setText("Calculation complete.")
            self.btn_update.setEnabled(True)

            # Visual feedback on image
            if self.parent() and hasattr(self.parent(), "graphics_manager"):
                self.parent().graphics_manager.display_proposed_beam_center(
                    center[0], center[1]
                )

    def _on_values_changed(self):
        # Update the proposed marker live as user edits
        x = self.spin_new_x.value()
        y = self.spin_new_y.value()
        if self.parent() and hasattr(self.parent(), "graphics_manager"):
            self.parent().graphics_manager.display_proposed_beam_center(x, y)
        # Re-enable update button if it was disabled (e.g. initial state)
        # But we only want to enable if a valid calculation or manual edit happened
        self.btn_update.setEnabled(True)

    def closeEvent(self, event):
        if self.parent() and hasattr(self.parent(), "graphics_manager"):
            self.parent().graphics_manager.clear_proposed_beam_center()
        super().closeEvent(event)

    def update_dataset(self, image, params):
        """Updates the dialog with a new image and parameters from a switched dataset."""
        self.image = image
        self.params = params or {}

        self.current_center = (
            self.params.get("beam_x", 0),
            self.params.get("beam_y", 0),
        )

        # Update UI elements without triggering signals initially
        self.spin_new_x.blockSignals(True)
        self.spin_new_y.blockSignals(True)
        self.spin_wavelength.blockSignals(True)
        self.spin_det_dist.blockSignals(True)

        self.spin_new_x.setValue(self.current_center[0])
        self.spin_new_y.setValue(self.current_center[1])
        self.spin_wavelength.setValue(self.params.get("wavelength", 1.0))
        self.spin_det_dist.setValue(self.params.get("det_dist", 100.0))

        self.spin_new_x.blockSignals(False)
        self.spin_new_y.blockSignals(False)
        self.spin_wavelength.blockSignals(False)
        self.spin_det_dist.blockSignals(False)

        # Reset state
        self.calculated_center = None
        self.lbl_status.setText("Dataset switched.")
        self.lbl_com.setText("N/A")
        self.btn_update.setEnabled(True)

        # Re-estimate beamstop radius
        if self.image is not None:
            try:
                sat_val = self.params.get("saturation_value", 2**32 - 1)
                if sat_val is None:
                    sat_val = 2**32 - 1
                mask = self.image >= sat_val

                est_r = estimate_beamstop_radius(
                    self.current_center, self.image, mask=mask
                )
                if est_r > 10:
                    self.spin_min_radius.setValue(int(est_r * 1.1))
            except Exception as e:
                logger.warning(f"Failed to re-estimate beamstop radius on update: {e}")

    def update_master_file(self):
        # Use the values from the spinboxes, not the cached calculation
        new_x = self.spin_new_x.value()
        new_y = self.spin_new_y.value()
        new_wavelength = self.spin_wavelength.value()
        new_det_dist = self.spin_det_dist.value()
        remove_correction = self.chk_remove_correction.isChecked()

        master_path = self.params.get("master_file")
        if not master_path:
            QtWidgets.QMessageBox.warning(
                self, "Error", "Master file path not found in parameters."
            )
            return

        save_nexus = self.chk_save_nexus.isChecked()

        if save_nexus:
            msg = f"Save Nexus compatible copy with center ({new_x:.2f}, {new_y:.2f})?\n\nWavelength: {new_wavelength} A\nDistance: {new_det_dist} mm\n\nOriginal: {os.path.basename(master_path)}\nNew: {os.path.basename(master_path).split('.')[0]}.nxs"
        else:
            msg = f"Update master file?\nCenter: ({new_x:.2f}, {new_y:.2f})\nWavelength: {new_wavelength} A\nDistance: {new_det_dist} mm\n\nThis will modify: {master_path}"

        reply = QtWidgets.QMessageBox.question(
            self,
            "Confirm Update",
            msg,
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
        )

        if reply == QtWidgets.QMessageBox.Yes:
            # 1. Close the file in the main application to release the lock
            # Only strictly needed if we are modifying the file we have open
            # If saving to new nexus file, technically we could keep reading original, but simpler to just release.
            main_window = self.parent()
            if main_window and hasattr(main_window, "dataset_manager"):
                logger.info(f"Closing file {master_path} before update...")
                main_window.dataset_manager.remove_single_dataset(master_path)

            # 2. Perform the update
            success, result_path = update_beam_center_in_master_file(
                master_path, new_x, new_y, 
                new_wavelength=new_wavelength,
                new_det_dist=new_det_dist,
                remove_correction=remove_correction,
                save_nexus=save_nexus
            )

            if success:
                self.lbl_status.setText("File updated successfully!")
                self.lbl_status.setStyleSheet("color: green")
                self.btn_update.setEnabled(False)  # Prevent double update
                self.beam_center_updated.emit(new_x, new_y)

                QtWidgets.QMessageBox.information(
                    self,
                    "Success",
                    f"Parameters updated in:\n{result_path}\n\nReloading...",
                )

                # 3. Reload the file (load the NEW file if nexus was saved)
                if main_window and hasattr(main_window, "file_io_manager"):
                    main_window.file_io_manager.load_file(result_path)
            else:
                self.lbl_status.setText("Failed to update file.")
                self.lbl_status.setStyleSheet("color: red")
                QtWidgets.QMessageBox.critical(
                    self, "Error", "Failed to update/create file. Check logs."
                )

                # Try to reload original even if failed, to restore state
                if main_window and hasattr(main_window, "file_io_manager"):
                    main_window.file_io_manager.load_file(master_path)