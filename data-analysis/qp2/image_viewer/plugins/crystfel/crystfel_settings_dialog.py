# qp2/image_viewer/ui/crystfel_settings_dialog.py
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import pyqtSignal

from qp2.image_viewer.ui.singleton_dialog import SingletonDialog
from qp2.image_viewer.utils.model_file_handler import handle_model_file_update
import logging


class CheckableComboBox(QtWidgets.QComboBox):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.view().viewport().installEventFilter(self)
        self.setModel(QtGui.QStandardItemModel(self))
        self.model().dataChanged.connect(self.update)

    def eventFilter(self, widget, event):
        if (
            event.type() == QtCore.QEvent.MouseButtonPress
            and widget is self.view().viewport()
        ):
            index = self.view().indexAt(event.pos())
            item = self.model().itemFromIndex(index)
            if item:
                new_state = QtCore.Qt.Unchecked if item.checkState() == QtCore.Qt.Checked else QtCore.Qt.Checked
                item.setCheckState(new_state)
            return True
        return super().eventFilter(widget, event)

    def paintEvent(self, event):
        painter = QtWidgets.QStylePainter(self)
        painter.setPen(self.palette().color(QtGui.QPalette.Text))
        
        opt = QtWidgets.QStyleOptionComboBox()
        self.initStyleOption(opt)
        
        items = self.checked_items()
        if not items:
            opt.currentText = "(None selected)"
        else:
            opt.currentText = ", ".join(items)
            
        painter.drawComplexControl(QtWidgets.QStyle.CC_ComboBox, opt)
        painter.drawControl(QtWidgets.QStyle.CE_ComboBoxLabel, opt)

    def addItems(self, texts):
        for text in texts:
            item = QtGui.QStandardItem(text)
            item.setCheckable(True)
            item.setCheckState(QtCore.Qt.Unchecked)
            self.model().appendRow(item)

    def checked_items(self):
        checked = []
        for i in range(self.model().rowCount()):
            item = self.model().item(i)
            if item.checkState() == QtCore.Qt.Checked:
                checked.append(item.text())
        return checked

    def set_checked_items(self, items):
        self.model().blockSignals(True)
        for i in range(self.model().rowCount()):
            item = self.model().item(i)
            if item.text() in items:
                item.setCheckState(QtCore.Qt.Checked)
            else:
                item.setCheckState(QtCore.Qt.Unchecked)
        self.model().blockSignals(False)
        self.update()


class CrystfelSettingsDialog(SingletonDialog):
    settings_changed = pyqtSignal(dict)

    def __init__(self, current_settings: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("CrystFEL Settings")
        self.setModal(False)
        self.new_settings = current_settings.copy()

        layout = QtWidgets.QVBoxLayout(self)

        # --- Top Section: Peak Algorithm ---
        top_form = QtWidgets.QFormLayout()
        
        self.peak_method_combo = QtWidgets.QComboBox()
        self.peak_method_combo.addItems(["peakfinder9", "peakfinder8"])
        self.peak_method_combo.setCurrentText(self.new_settings.get("crystfel_peaks_method", "peakfinder8"))
        
        self.test_peaks_btn = QtWidgets.QPushButton("Test Peaks")
        self.test_peaks_btn.setToolTip("Run peak finding on current image with current parameters (no indexing).")
        self.test_peaks_btn.setStyleSheet("background-color: #d0f0c0; font-weight: bold; padding: 5px;")
        self.test_peaks_btn.clicked.connect(self._test_peaks)
        
        peak_algo_layout = QtWidgets.QHBoxLayout()
        peak_algo_layout.addWidget(self.peak_method_combo)
        peak_algo_layout.addWidget(self.test_peaks_btn)
        
        top_form.addRow("Peak Algorithm:", peak_algo_layout)
        layout.addLayout(top_form)

        # --- Middle Section: 2 Columns ---
        columns_layout = QtWidgets.QHBoxLayout()
        left_vbox = QtWidgets.QVBoxLayout()
        right_vbox = QtWidgets.QVBoxLayout()
        
        # === Left Column: SNR & Speed ===

        # --- SNR && Peak Finding Settings ---
        snr_group = QtWidgets.QGroupBox("SNR && Peak Finding Settings")
        snr_layout = QtWidgets.QFormLayout(snr_group)
        
        self.min_snr_spinbox = QtWidgets.QDoubleSpinBox()
        self.min_snr_spinbox.setRange(1.0, 20.0)
        self.min_snr_spinbox.setDecimals(1)
        self.min_snr_spinbox.setValue(self.new_settings.get("crystfel_min_snr", 5.0))
        snr_layout.addRow("Min SNR (overall):", self.min_snr_spinbox)

        self.bg_radius_spinbox = QtWidgets.QSpinBox()
        self.bg_radius_spinbox.setRange(1, 10)
        self.bg_radius_spinbox.setValue(
            self.new_settings.get("crystfel_local_bg_radius", 3)
        )
        snr_layout.addRow("Local BG Radius (px):", self.bg_radius_spinbox)

        # peakfinder8 specific section
        snr_layout.addRow(QtWidgets.QLabel("<i>-- peakfinder8 specific --</i>"))
        
        self.pf8_threshold_spin = QtWidgets.QDoubleSpinBox()
        self.pf8_threshold_spin.setRange(0.0, 100000.0)
        self.pf8_threshold_spin.setDecimals(1)
        self.pf8_threshold_spin.setValue(self.new_settings.get("crystfel_peakfinder8_threshold", 20.0))
        self.pf8_threshold_spin.setToolTip("Threshold for peakfinder8")
        
        pf8_thresh_layout = QtWidgets.QHBoxLayout()
        pf8_thresh_layout.addWidget(self.pf8_threshold_spin)
        
        pf8_thresh_layout.addWidget(self.pf8_threshold_spin)
        
        self.auto_thresh_chk = QtWidgets.QCheckBox("Auto (MAD)")
        self.auto_thresh_chk.setToolTip("Auto-calculate threshold using Robust Statistics (Median + 10*MAD + 1).\nUncheck to set manually.")
        self.auto_thresh_chk.setChecked(self.new_settings.get("crystfel_peakfinder8_auto_threshold", True))
        self.auto_thresh_chk.toggled.connect(self._on_auto_thresh_toggled)
        pf8_thresh_layout.addWidget(self.auto_thresh_chk)
        
        snr_layout.addRow("  Threshold:", pf8_thresh_layout)

        self.pf8_min_pix_spin = QtWidgets.QSpinBox()
        self.pf8_min_pix_spin.setRange(1, 100)
        self.pf8_min_pix_spin.setValue(self.new_settings.get("crystfel_peakfinder8_min_pix_count", 2))
        self.pf8_min_pix_spin.setToolTip("Min pixel count for peakfinder8")
        snr_layout.addRow("  Min Pixel Count:", self.pf8_min_pix_spin)

        self.pf8_max_pix_spin = QtWidgets.QSpinBox()
        self.pf8_max_pix_spin.setRange(1, 1000)
        self.pf8_max_pix_spin.setValue(self.new_settings.get("crystfel_peakfinder8_max_pix_count", 200))
        self.pf8_max_pix_spin.setToolTip("Max pixel count for peakfinder8")
        snr_layout.addRow("  Max Pixel Count:", self.pf8_max_pix_spin)

        # peakfinder9 specific section
        snr_layout.addRow(QtWidgets.QLabel("<i>-- peakfinder9 specific --</i>"))
        
        self.min_snr_biggest_pix_spinbox = QtWidgets.QDoubleSpinBox()
        self.min_snr_biggest_pix_spinbox.setRange(1.0, 20.0)
        self.min_snr_biggest_pix_spinbox.setDecimals(1)
        self.min_snr_biggest_pix_spinbox.setValue(
            self.new_settings.get("crystfel_min_snr_biggest_pix", 7.0)
        )
        snr_layout.addRow("  Min SNR (biggest pixel):", self.min_snr_biggest_pix_spinbox)

        self.min_snr_peak_pix_spinbox = QtWidgets.QDoubleSpinBox()
        self.min_snr_peak_pix_spinbox.setRange(1.0, 20.0)
        self.min_snr_peak_pix_spinbox.setDecimals(1)
        self.min_snr_peak_pix_spinbox.setValue(
            self.new_settings.get("crystfel_min_snr_peak_pix", 6.0)
        )
        snr_layout.addRow("  Min SNR (peak pixels):", self.min_snr_peak_pix_spinbox)

        self.min_sig_spinbox = QtWidgets.QDoubleSpinBox()
        self.min_sig_spinbox.setRange(1.0, 20.0)
        self.min_sig_spinbox.setDecimals(1)
        self.min_sig_spinbox.setValue(self.new_settings.get("crystfel_min_sig", 11.0))
        snr_layout.addRow("  Min Significance:", self.min_sig_spinbox)
        
        left_vbox.addWidget(snr_group)

        # --- Speed && Optimization ---
        speed_group = QtWidgets.QGroupBox("Speed && Optimization")
        speed_layout = QtWidgets.QFormLayout(speed_group)
        
        self.min_peaks_spinbox = QtWidgets.QSpinBox()
        self.min_peaks_spinbox.setRange(1, 1000)
        self.min_peaks_spinbox.setValue(self.new_settings.get("crystfel_min_peaks", 15))
        speed_layout.addRow("Min Peaks for Hit:", self.min_peaks_spinbox)
        
        self.no_non_hits_chk = QtWidgets.QCheckBox("Only Save Hits in Stream")
        self.no_non_hits_chk.setChecked(self.new_settings.get("crystfel_no_non_hits", True))
        self.no_non_hits_chk.setToolTip("Pass --no-non-hits-in-stream")
        speed_layout.addRow(self.no_non_hits_chk)

        self.peakfinder8_fast_chk = QtWidgets.QCheckBox("Use peakfinder8-fast")
        self.peakfinder8_fast_chk.setChecked(self.new_settings.get("crystfel_peakfinder8_fast", True))
        speed_layout.addRow(self.peakfinder8_fast_chk)

        self.asdf_fast_chk = QtWidgets.QCheckBox("Use asdf-fast (3x speedup)")
        self.asdf_fast_chk.setChecked(self.new_settings.get("crystfel_asdf_fast", True))
        speed_layout.addRow(self.asdf_fast_chk)

        self.no_retry_chk = QtWidgets.QCheckBox("Disable retry on failure")
        self.no_retry_chk.setChecked(self.new_settings.get("crystfel_no_retry", True))
        speed_layout.addRow(self.no_retry_chk)

        self.no_multi_chk = QtWidgets.QCheckBox("Disable multi-indexing")
        self.no_multi_chk.setChecked(self.new_settings.get("crystfel_no_multi", True))
        speed_layout.addRow(self.no_multi_chk)
        
        self.include_mask_chk = QtWidgets.QCheckBox("Include Bad Pixel Mask")
        self.include_mask_chk.setToolTip("Generates bad regions from image header. May be slower.")
        self.include_mask_chk.setChecked(self.new_settings.get("crystfel_include_mask", False))
        speed_layout.addRow(self.include_mask_chk)

        self.delete_workdir_chk = QtWidgets.QCheckBox("Delete Working Directory")
        self.delete_workdir_chk.setToolTip("Cleanup temporary files after run.")
        self.delete_workdir_chk.setChecked(self.new_settings.get("crystfel_delete_workdir", True))
        speed_layout.addRow(self.delete_workdir_chk)

        self.push_res_spinbox = QtWidgets.QDoubleSpinBox()
        self.push_res_spinbox.setRange(0.0, 10.0)
        self.push_res_spinbox.setSingleStep(0.1)
        self.push_res_spinbox.setValue(self.new_settings.get("crystfel_push_res", 0.0))
        self.push_res_spinbox.setSpecialValueText("Disabled")
        speed_layout.addRow("Integration Res Limit (nm⁻¹):", self.push_res_spinbox)

        self.int_radius_input = QtWidgets.QLineEdit(
            self.new_settings.get("crystfel_int_radius", "3,4,5")
        )
        self.int_radius_input.setPlaceholderText("inner,middle,outer (e.g. 3,4,5)")
        self.int_radius_input.setToolTip("Integration radii: inner, middle, outer. Default: 3,4,5")
        speed_layout.addRow("Integration Radii (px):", self.int_radius_input)

        self.integration_combo = QtWidgets.QComboBox()
        self.integration_combo.addItems(["Standard", "None (No Intensity)", "Cell Only (No Prediction)"])
        integration_mode = self.new_settings.get("crystfel_integration_mode", "Standard")
        self.integration_combo.setCurrentText(integration_mode)
        speed_layout.addRow("Integration Mode:", self.integration_combo)

        left_vbox.addWidget(speed_group)
        left_vbox.addStretch() # Push up

        # === Right Column: Indexing ===

        # --- Indexing ---
        indexing_group = QtWidgets.QGroupBox("Indexing")
        indexing_layout = QtWidgets.QFormLayout(indexing_group)
        
        self.indexing_methods_combo = CheckableComboBox()
        available_methods = [
            "xgandalf", "mosflm", "asdf", "dirax", 
            "taketwo", "smallcell", "xds", "pinkindexer", 
            "ffbidx", "felix"
        ]
        self.indexing_methods_combo.addItems(available_methods)
        
        current_methods_str = self.new_settings.get("crystfel_indexing_methods", "xgandalf")
        current_methods = [m.strip() for m in current_methods_str.split(",") if m.strip()]
        self.indexing_methods_combo.set_checked_items(current_methods)
        self.indexing_methods_combo.setToolTip("Select indexing methods to try.")
        
        idx_method_layout = QtWidgets.QHBoxLayout()
        idx_method_layout.addWidget(self.indexing_methods_combo)
        
        self.test_indexing_btn = QtWidgets.QPushButton("Test Indexing")
        self.test_indexing_btn.setToolTip("Run full indexing on current image settings.")
        self.test_indexing_btn.setStyleSheet("background-color: #d0f0c0; font-weight: bold; padding: 5px;")
        self.test_indexing_btn.clicked.connect(self._test_indexing)
        idx_method_layout.addWidget(self.test_indexing_btn)

        indexing_layout.addRow("Indexing Methods:", idx_method_layout)
        
        # XGANDALF Specific Group (Nested)
        self.xgandalf_group = QtWidgets.QGroupBox("XGANDALF Advanced Settings")
        self.xgandalf_group.setCheckable(True)
        self.xgandalf_group.setChecked(False) # Collapsed by default
        xg_layout = QtWidgets.QFormLayout(self.xgandalf_group)

        self.xgandalf_sampling_spin = QtWidgets.QSpinBox()
        self.xgandalf_sampling_spin.setRange(0, 7)
        self.xgandalf_sampling_spin.setValue(self.new_settings.get("crystfel_xgandalf_sampling_pitch", 6))
        self.xgandalf_sampling_spin.setToolTip("0=loosest, 7=most dense. Default: 6")
        xg_layout.addRow("Sampling Pitch:", self.xgandalf_sampling_spin)

        self.xgandalf_grad_iter_spin = QtWidgets.QSpinBox()
        self.xgandalf_grad_iter_spin.setRange(0, 10)
        self.xgandalf_grad_iter_spin.setValue(self.new_settings.get("crystfel_xgandalf_grad_desc_iterations", 4))
        self.xgandalf_grad_iter_spin.setToolTip("Gradient descent iterations. Default: 4")
        xg_layout.addRow("Grad Desc Iterations:", self.xgandalf_grad_iter_spin)

        self.xgandalf_tolerance_spin = QtWidgets.QDoubleSpinBox()
        self.xgandalf_tolerance_spin.setRange(0.001, 1.0)
        self.xgandalf_tolerance_spin.setSingleStep(0.01)
        self.xgandalf_tolerance_spin.setDecimals(3)
        self.xgandalf_tolerance_spin.setValue(self.new_settings.get("crystfel_xgandalf_tolerance", 0.02))
        xg_layout.addRow("Tolerance:", self.xgandalf_tolerance_spin)

        self.xgandalf_no_dev_chk = QtWidgets.QCheckBox("No Deviation from Cell")
        self.xgandalf_no_dev_chk.setChecked(self.new_settings.get("crystfel_xgandalf_no_deviation", False))
        xg_layout.addRow(self.xgandalf_no_dev_chk)

        self.xgandalf_min_lat_spin = QtWidgets.QDoubleSpinBox()
        self.xgandalf_min_lat_spin.setRange(1.0, 1000.0)
        self.xgandalf_min_lat_spin.setValue(self.new_settings.get("crystfel_xgandalf_min_lattice", 30.0))
        xg_layout.addRow("Min Lattice (Å):", self.xgandalf_min_lat_spin)

        self.xgandalf_max_lat_spin = QtWidgets.QDoubleSpinBox()
        self.xgandalf_max_lat_spin.setRange(1.0, 2000.0)
        self.xgandalf_max_lat_spin.setValue(self.new_settings.get("crystfel_xgandalf_max_lattice", 250.0))
        xg_layout.addRow("Max Lattice (Å):", self.xgandalf_max_lat_spin)

        self.xgandalf_max_peaks_spin = QtWidgets.QSpinBox()
        self.xgandalf_max_peaks_spin.setRange(10, 5000)
        self.xgandalf_max_peaks_spin.setValue(self.new_settings.get("crystfel_xgandalf_max_peaks", 250))
        xg_layout.addRow("Max Peaks:", self.xgandalf_max_peaks_spin)

        indexing_layout.addRow(self.xgandalf_group)

        self.xgandalf_fast_chk = QtWidgets.QCheckBox("Use xgandalf-fast (Overrides Pitch/Iter)")
        self.xgandalf_fast_chk.setChecked(self.new_settings.get("crystfel_xgandalf_fast", True))
        self.xgandalf_fast_chk.toggled.connect(self._update_xgandalf_ui_state)
        indexing_layout.addRow(self.xgandalf_fast_chk)
        
        self._update_xgandalf_ui_state(self.xgandalf_fast_chk.isChecked())

        self.no_refine_chk = QtWidgets.QCheckBox("Disable refinement")
        self.no_refine_chk.setChecked(self.new_settings.get("crystfel_no_refine", False))
        self.no_refine_chk.setToolTip("Recommended for wide-bandwidth (pink beam) data.")
        indexing_layout.addRow(self.no_refine_chk)

        self.no_check_peaks_chk = QtWidgets.QCheckBox("Disable peak checking")
        self.no_check_peaks_chk.setChecked(self.new_settings.get("crystfel_no_check_peaks", False))
        self.no_check_peaks_chk.setToolTip("Recommended for wide-bandwidth (pink beam) data.")
        indexing_layout.addRow(self.no_check_peaks_chk)

        right_vbox.addWidget(indexing_group)
        right_vbox.addStretch()

        columns_layout.addLayout(left_vbox)
        columns_layout.addLayout(right_vbox, stretch=1)
        layout.addLayout(columns_layout)

        # --- Bottom Section: Inputs & Buttons ---
        form_layout = QtWidgets.QFormLayout()
        
        # --- PDB/Cell File ---
        pdb_layout = QtWidgets.QHBoxLayout()
        self.pdb_path_label = QtWidgets.QLineEdit(
            self.new_settings.get("crystfel_pdb_file", "")
        )
        self.pdb_path_label.setPlaceholderText("Optional: Path to .pdb or .cell file")
        self.pdb_path_label.editingFinished.connect(
            lambda: self._update_from_model_file(self.pdb_path_label.text())
        )
        self.pdb_browse_button = QtWidgets.QPushButton("Browse...")
        self.pdb_browse_button.clicked.connect(self._browse_for_pdb)
        pdb_layout.addWidget(self.pdb_path_label)
        pdb_layout.addWidget(self.pdb_browse_button)
        form_layout.addRow("Initial Model (PDB/Cell):", pdb_layout)

        # --- Parallelism ---
        self.nproc_spinbox = QtWidgets.QSpinBox()
        self.nproc_spinbox.setRange(1, 128)
        self.nproc_spinbox.setValue(self.new_settings.get("crystfel_nproc", 32))
        form_layout.addRow("Parallel Processors (nproc):", self.nproc_spinbox)

        form_layout.addRow(QtWidgets.QFrame(frameShape=QtWidgets.QFrame.HLine))

        self.extra_options_input = QtWidgets.QLineEdit(
            self.new_settings.get("crystfel_extra_options", "")
        )
        self.extra_options_input.setPlaceholderText("--option1=value --flag2 ...")
        self.extra_options_input.setToolTip(
            "Enter any additional command-line options for indexamajig."
        )
        form_layout.addRow("Additional Options:", self.extra_options_input)

        layout.addLayout(form_layout)

        button_box = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
        )
        
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
        
        # Trigger initial UI state update
        QtCore.QTimer.singleShot(0, lambda: self._on_auto_thresh_toggled(self.auto_thresh_chk.isChecked()))

    @staticmethod
    def _sanitize_int_radius(raw: str, default: str = "3,4,5") -> tuple[str, bool]:
        """
        Normalise an integration-radii string.
        Strips whitespace, checks that exactly 3 positive integers are given,
        and that inner < middle < outer.
        Returns (sanitised_string, is_valid).
        """
        # Remove all whitespace then split on comma
        parts = [p.strip() for p in raw.strip().split(",")]
        try:
            if len(parts) != 3:
                raise ValueError(f"Expected 3 values, got {len(parts)}")
            vals = [int(p) for p in parts]
            if any(v <= 0 for v in vals):
                raise ValueError("All radii must be positive integers")
            if not (vals[0] < vals[1] < vals[2]):
                raise ValueError("Radii must satisfy inner < middle < outer")
            return ",".join(str(v) for v in vals), True
        except (ValueError, AttributeError):
            return default, False

    def _gather_settings_from_ui(self):
        """Helper to construct settings dict from current UI state."""
        settings = self.new_settings.copy() # Start with defaults/existing
        settings["crystfel_peaks_method"] = self.peak_method_combo.currentText()
        settings["crystfel_min_snr"] = self.min_snr_spinbox.value()
        settings["crystfel_min_snr_biggest_pix"] = self.min_snr_biggest_pix_spinbox.value()
        settings["crystfel_min_snr_peak_pix"] = self.min_snr_peak_pix_spinbox.value()
        settings["crystfel_min_peaks"] = self.min_peaks_spinbox.value()
        settings["crystfel_indexing_methods"] = "none" # Force none for testing peaks

        # PF8
        settings["crystfel_peakfinder8_threshold"] = self.pf8_threshold_spin.value()
        settings["crystfel_peakfinder8_min_pix_count"] = self.pf8_min_pix_spin.value()
        settings["crystfel_peakfinder8_max_pix_count"] = self.pf8_max_pix_spin.value()

        settings["crystfel_no_check_peaks"] = self.no_check_peaks_chk.isChecked()
        settings["crystfel_no_refine"] = True # No indexing means no refinement needed
        settings["crystfel_pdb_file"] = self.pdb_path_label.text()
        settings["crystfel_xgandalf_sampling_pitch"] = self.xgandalf_sampling_spin.value()
        settings["crystfel_xgandalf_grad_desc_iterations"] = self.xgandalf_grad_iter_spin.value()
        settings["crystfel_xgandalf_tolerance"] = self.xgandalf_tolerance_spin.value()
        settings["crystfel_xgandalf_no_deviation"] = self.xgandalf_no_dev_chk.isChecked()
        settings["crystfel_xgandalf_min_lattice"] = self.xgandalf_min_lat_spin.value()
        settings["crystfel_xgandalf_max_lattice"] = self.xgandalf_max_lat_spin.value()
        settings["crystfel_xgandalf_max_peaks"] = self.xgandalf_max_peaks_spin.value()

        settings["crystfel_include_mask"] = self.include_mask_chk.isChecked()
        settings["crystfel_delete_workdir"] = self.delete_workdir_chk.isChecked()
        return settings

    def _test_peaks(self):
        """Runs peak finding on the current image."""
        main_window = self.parent()
        if not main_window or not hasattr(main_window, "current_master_file"):
             # Fallback if parent isn't main window (depends on how it was opened)
             # Try finding top level widget
             logging.warning("Test Peaks: Could not verify parent is Main Window.")
             return

        if not main_window.current_master_file:
            QtWidgets.QMessageBox.warning(self, "No Image", "Please open a dataset first.")
            return

        # Prepare params
        params = self._gather_settings_from_ui()
        
        # Prepare mapping
        current_frame_idx = getattr(main_window, "current_frame_index", 0)
        # Use 1-based index for mapping convention
        mapping = {main_window.current_master_file: [current_frame_idx + 1]}
        
        # Run in thread
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        self.test_peaks_btn.setEnabled(False)
        self.test_peaks_btn.setText("Finding Peaks...")
        
        # Local worker class to avoid circular imports or complex deps
        from qp2.image_viewer.plugins.crystfel.run_crystfel_strategy import run_crystfel_strategy
        import shutil
        import tempfile
        
        class TestPeaksWorker(QtCore.QThread):
            finished_sig = pyqtSignal(dict)
            error_sig = pyqtSignal(str)

            def __init__(self, mapping, params):
                super().__init__()
                self.mapping = mapping
                self.params = params

            def run(self):
                try:
                    # Create temp dir
                    workdir = tempfile.mkdtemp(prefix="crystfel_test_peaks_")
                    try:
                        result = run_crystfel_strategy(self.mapping, workdir, self.params)
                        self.finished_sig.emit(result)
                    finally:
                        if self.params.get("crystfel_delete_workdir", False):
                             shutil.rmtree(workdir, ignore_errors=True)
                        else:
                             logging.info(f"Test Peaks Workdir kept at: {workdir}")
                except Exception as e:
                    self.error_sig.emit(str(e))

        self.worker = TestPeaksWorker(mapping, params)
        self.worker.finished_sig.connect(lambda res: self._on_test_peaks_finished(res, main_window))
        self.worker.error_sig.connect(self._on_test_peaks_error)
        self.worker.finished.connect(lambda: self.test_peaks_btn.setEnabled(True))
        self.worker.finished.connect(lambda: self.test_peaks_btn.setText("Test Peaks"))
        self.worker.finished.connect(lambda: QtWidgets.QApplication.restoreOverrideCursor())
        self.worker.start()

    def _test_indexing(self):
        """Runs full indexing on the current image with current (unsaved) settings."""
        main_window = self.parent()
        if not main_window or not hasattr(main_window, "current_master_file"):
             logging.warning("Test Indexing: Could not verify parent is Main Window.")
             return

        if not main_window.current_master_file:
            QtWidgets.QMessageBox.warning(self, "No Image", "Please open a dataset first.")
            return

        # Prepare params from current UI
        params = self._gather_settings_from_ui()
        
        # Override specific params that might be forced in gather but needed for indexing
        # _gather_settings_from_ui sets "indexing_methods" to "none" because it was designed for testing peaks!
        # Wait, I need to check _gather_settings_from_ui. 
        # It DOES force indexing="none"!
        # I need a clean gather or update it. 
        
        # Let's fix params manually:
        selected_methods = self.indexing_methods_combo.checked_items()
        params["crystfel_indexing_methods"] = ",".join(selected_methods)
        params["crystfel_no_refine"] = self.no_refine_chk.isChecked() # Was forced True
        
        # Also need logic to ensure settings like 'crystfel_pdb' are mapped correctly if names differ
        # StrategyManager expects 'crystfel_peaks_method', 'crystfel_indexing_methods' etc.
        # It also maps settings_manager 'crystfel_cell_file' to 'crystfel_pdb'.
        # My gather uses 'crystfel_pdb_file'.
        # I should map it to match what StrategyManager usually expects or what `run_crystfel_strategy` expects.
        # `run_crystfel_strategy` uses `crystfel_pdb`.
        # StrategyManager maps 'crystfel_cell_file' -> 'crystfel_pdb'.
        # My gather sets 'crystfel_pdb_file'. 
        
        if "crystfel_pdb_file" in params:
             params["crystfel_pdb"] = params["crystfel_pdb_file"]

        # Prepare mapping
        current_frame_idx = getattr(main_window, "current_frame_index", 0)
        mapping = {main_window.current_master_file: [current_frame_idx + 1]}
        
        # Use StrategyManager to run it properly
        if hasattr(main_window, "strategy_manager"):
             main_window.strategy_manager.run_strategy("crystfel", mapping, override_params=params)

    def _on_test_peaks_finished(self, result, main_window):
        if not result or "spots_by_master_crystfel" not in result:
             QtWidgets.QMessageBox.warning(self, "Failed", "Peak finding returned no valid data.")
             return
        
        # Extract spots
        # result structure: { "spots_by_master_crystfel": { master: { frame: { "spots_crystfel": [...] } } } }
        # Flatten extraction
        all_spots = []
        for master in result["spots_by_master_crystfel"]:
            for frame in result["spots_by_master_crystfel"][master]:
                spots = result["spots_by_master_crystfel"][master][frame].get("spots_crystfel", [])
                if spots:
                    all_spots.extend(spots)
        
        import numpy as np
        if all_spots:
            spots_arr = np.array(all_spots)
            # Flip coordinates from (x, y) to (y, x) for display
            spots_arr = np.fliplr(spots_arr)
            main_window.graphics_manager.clear_spots() # Clear old
            main_window.graphics_manager.display_spots(spots_arr)
            main_window.ui_manager.show_status_message(f"Test Peaks: Found {len(all_spots)} spots.", 5000)
        else:
             main_window.graphics_manager.clear_spots()
             main_window.ui_manager.show_status_message("Test Peaks: No spots found.", 5000)

    def _on_test_peaks_error(self, err_msg):
        QtWidgets.QMessageBox.critical(self, "Error", f"Peak finding failed:\n{err_msg}")

    def _on_auto_thresh_toggled(self, checked):
        """Disables input for auto mode and optionally triggers calculation."""
        self.pf8_threshold_spin.setEnabled(not checked)
        if checked:
             self._estimate_threshold_auto()

    def _estimate_threshold_auto(self):
        """Calculates Robust MAD threshold using shared utility logic."""
        main_window = self.parent()
        if not main_window or not hasattr(main_window, "graphics_manager"):
            return

        from qp2.image_viewer.plugins.crystfel.utils import calculate_robust_threshold_mad
        
        image_item = getattr(main_window.graphics_manager, "img_item", None)
        if image_item is None or image_item.image is None:
             if self.sender() == self.auto_thresh_chk and self.auto_thresh_chk.isChecked():
                  # Only warn if explicitly toggled on by user just now, otherwise silent
                  # Actually, better to just log warning for auto
                  pass 
             return
             
        data = image_item.image
        mask = None
        if hasattr(main_window, "detector_mask_manager"):
             main_window.detector_mask_manager.ensure_mask_up_to_date()
             mask = main_window.detector_mask_manager.mask

        threshold = calculate_robust_threshold_mad(data, mask)

        if threshold is not None:
             self.pf8_threshold_spin.setValue(float(threshold))
             if self.sender() == self.auto_thresh_chk or self.isVisible():
                  main_window.ui_manager.show_status_message(f"Auto-Threshold (MAD > 0): {threshold:.1f}", 3000)
        else:
             if self.sender() == self.auto_thresh_chk:
                  logging.warning("Auto-Threshold: Could not calculate (no valid data).")

    def _update_xgandalf_ui_state(self, is_fast_checked):
        """Disable detailed XGANDALF pitch/iter settings if fast execution is selected."""
        self.xgandalf_sampling_spin.setEnabled(not is_fast_checked)
        self.xgandalf_grad_iter_spin.setEnabled(not is_fast_checked)

    def _browse_for_pdb(self):
        if getattr(self, "_pdb_just_downloaded", False):
            self._pdb_just_downloaded = False
            return

        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Select Model File", "", "Model Files (*.pdb *.cell);;All Files (*)"
        )
        if file_path:
            self.pdb_path_label.setText(file_path)
            self._update_from_model_file(file_path)

    def _update_from_model_file(self, file_path):
        self._pdb_just_downloaded = handle_model_file_update(
            file_path_input=self.pdb_path_label,
            space_group_input=None,
            unit_cell_input=None,
            download_dir_input=None
        )

    def accept(self):
        # --- Validate inputs before closing ---
        raw_int_radius = self.int_radius_input.text()
        sanitised, valid = self._sanitize_int_radius(raw_int_radius)
        if not valid:
            QtWidgets.QMessageBox.warning(
                self,
                "Invalid Integration Radii",
                f'"{raw_int_radius}" is not a valid int-radius string.\n\n'
                "Format must be three ascending positive integers separated by commas,\n"
                'e.g. "3,4,5".',
            )
            self.int_radius_input.setFocus()
            self.int_radius_input.selectAll()
            return  # Keep dialog open

        self.new_settings["crystfel_peaks_method"] = self.peak_method_combo.currentText()
        self.new_settings["crystfel_min_snr"] = self.min_snr_spinbox.value()
        self.new_settings["crystfel_min_snr_biggest_pix"] = (
            self.min_snr_biggest_pix_spinbox.value()
        )
        self.new_settings["crystfel_min_snr_peak_pix"] = (
            self.min_snr_peak_pix_spinbox.value()
        )
        # PF8
        self.new_settings["crystfel_peakfinder8_threshold"] = self.pf8_threshold_spin.value()
        self.new_settings["crystfel_peakfinder8_auto_threshold"] = self.auto_thresh_chk.isChecked()
        self.new_settings["crystfel_peakfinder8_min_pix_count"] = self.pf8_min_pix_spin.value()
        self.new_settings["crystfel_peakfinder8_max_pix_count"] = self.pf8_max_pix_spin.value()

        self.new_settings["crystfel_min_peaks"] = self.min_peaks_spinbox.value()
        self.new_settings["crystfel_no_non_hits"] = self.no_non_hits_chk.isChecked()

        selected_methods = self.indexing_methods_combo.checked_items()
        self.new_settings["crystfel_indexing_methods"] = ",".join(selected_methods)

        self.new_settings["crystfel_xgandalf_fast"] = self.xgandalf_fast_chk.isChecked()
        self.new_settings["crystfel_no_refine"] = self.no_refine_chk.isChecked()
        self.new_settings["crystfel_no_check_peaks"] = self.no_check_peaks_chk.isChecked()

        self.new_settings["crystfel_peakfinder8_fast"] = self.peakfinder8_fast_chk.isChecked()
        self.new_settings["crystfel_asdf_fast"] = self.asdf_fast_chk.isChecked()
        self.new_settings["crystfel_no_retry"] = self.no_retry_chk.isChecked()
        self.new_settings["crystfel_no_multi"] = self.no_multi_chk.isChecked()
        self.new_settings["crystfel_include_mask"] = self.include_mask_chk.isChecked()
        self.new_settings["crystfel_delete_workdir"] = self.delete_workdir_chk.isChecked()
        self.new_settings["crystfel_push_res"] = self.push_res_spinbox.value()
        self.new_settings["crystfel_integration_mode"] = self.integration_combo.currentText()
        self.int_radius_input.setText(sanitised)
        self.new_settings["crystfel_int_radius"] = sanitised

        self.new_settings["crystfel_min_sig"] = self.min_sig_spinbox.value()
        self.new_settings["crystfel_local_bg_radius"] = self.bg_radius_spinbox.value()
        self.new_settings["crystfel_pdb_file"] = self.pdb_path_label.text()
        self.new_settings["crystfel_nproc"] = self.nproc_spinbox.value()
        self.new_settings["crystfel_extra_options"] = self.extra_options_input.text()

        self.new_settings["crystfel_xgandalf_sampling_pitch"] = self.xgandalf_sampling_spin.value()
        self.new_settings["crystfel_xgandalf_grad_desc_iterations"] = self.xgandalf_grad_iter_spin.value()
        self.new_settings["crystfel_xgandalf_tolerance"] = self.xgandalf_tolerance_spin.value()
        self.new_settings["crystfel_xgandalf_no_deviation"] = self.xgandalf_no_dev_chk.isChecked()
        self.new_settings["crystfel_xgandalf_min_lattice"] = self.xgandalf_min_lat_spin.value()
        self.new_settings["crystfel_xgandalf_max_lattice"] = self.xgandalf_max_lat_spin.value()
        self.new_settings["crystfel_xgandalf_max_peaks"] = self.xgandalf_max_peaks_spin.value()

        self.settings_changed.emit(self.new_settings)
        super().accept()
