import os
from PyQt5.QtCore import QTimer, Qt, pyqtSignal
from pyqtgraph.Qt import QtWidgets

from qp2.image_viewer.ui.singleton_dialog import SingletonDialog
from qp2.log.logging_config import get_logger
from qp2.image_viewer.utils.model_file_handler import handle_model_file_update
from qp2.utils.auxillary import sanitize_space_group

logger = get_logger(__name__)


class SettingsDialog(SingletonDialog):
    """Dialog for editing application settings."""

    settings_changed = pyqtSignal(dict)  # Emits the updated settings dict on change
    request_spreadsheet_update = pyqtSignal()

    def __init__(self, current_settings: dict, parent=None):
        super().__init__(parent)
        self.settings_manager = parent.settings_manager
        
        self.setWindowTitle("Application Settings")
        self.setModal(False)  # Non-modal dialog

        self.current_settings = current_settings  # Store reference
        self.new_settings = current_settings.copy()  # Work on a copy

        main_layout = QtWidgets.QVBoxLayout(self)
        
        # Horizontal layout for two columns
        columns_layout = QtWidgets.QHBoxLayout()
        
        # Left column container
        left_column_container = QtWidgets.QWidget()
        left_column = QtWidgets.QVBoxLayout(left_column_container)
        left_column.setContentsMargins(0, 0, 0, 0)

        # Right column container
        right_column_container = QtWidgets.QWidget()
        right_column = QtWidgets.QVBoxLayout(right_column_container)
        right_column.setContentsMargins(0, 0, 0, 0)

        # Ensure balanced minimum widths
        left_column_container.setMinimumWidth(300)
        right_column_container.setMinimumWidth(400)

        # --- Left Column: Contrast Settings ---
        contrast_group = QtWidgets.QGroupBox("Contrast Percentiles")
        contrast_layout = QtWidgets.QFormLayout(contrast_group)
        self.low_perc_spinbox = QtWidgets.QDoubleSpinBox()
        self.low_perc_spinbox.setRange(0.0, 99.0)
        self.low_perc_spinbox.setDecimals(1)
        self.low_perc_spinbox.setSingleStep(0.5)
        self.low_perc_spinbox.setValue(
            self.current_settings.get("contrast_low_percentile", 5.0)
        )
        contrast_layout.addRow("Low (%):", self.low_perc_spinbox)

        self.high_perc_spinbox = QtWidgets.QDoubleSpinBox()
        self.high_perc_spinbox.setRange(1.0, 100.0)
        self.high_perc_spinbox.setDecimals(1)
        self.high_perc_spinbox.setSingleStep(0.5)
        self.high_perc_spinbox.setValue(
            self.current_settings.get("contrast_high_percentile", 95.0)
        )
        contrast_layout.addRow("High (%):", self.high_perc_spinbox)
        left_column.addWidget(contrast_group)

        # --- Left Column: Resolution Rings ---
        rings_group = QtWidgets.QGroupBox("Resolution Rings (Å)")
        rings_layout = QtWidgets.QFormLayout(rings_group)
        current_rings_str = ", ".join(
            map(str, self.current_settings.get("resolution_rings", []))
        )
        self.rings_input = QtWidgets.QLineEdit(current_rings_str)
        self.rings_input.setPlaceholderText("e.g., 3.67, 3.03, 2.25")
        rings_layout.addRow("Rings (comma-sep):", self.rings_input)
        left_column.addWidget(rings_group)

        # --- Left Column: Grid Scan Mode ---
        scan_mode_group = QtWidgets.QGroupBox("Grid Scan Mode")
        scan_mode_layout = QtWidgets.QFormLayout(scan_mode_group)

        self.scan_mode_combo = QtWidgets.QComboBox()
        self.scan_mode_combo.setToolTip(
            "Define the geometry and direction of grid scans."
        )
        scan_options = {
            "row_wise": "Row-wise (Left to Right)",
            "column_wise": "Column-wise (Top to Bottom)",
            "row_wise_serpentine": "Row-wise Serpentine",
            "column_wise_serpentine": "Column-wise Serpentine",
        }
        for actual_value, display_text in scan_options.items():
            self.scan_mode_combo.addItem(display_text, actual_value)

        current_scan_mode = self.current_settings.get("scan_mode", "row_wise")
        index = self.scan_mode_combo.findData(current_scan_mode)
        if index != -1:
            self.scan_mode_combo.setCurrentIndex(index)

        scan_mode_layout.addRow("Mode:", self.scan_mode_combo)
        left_column.addWidget(scan_mode_group)

        # --- Left Column: Playback ---
        playback_group = QtWidgets.QGroupBox("Playback")
        playback_layout = QtWidgets.QFormLayout(playback_group)

        self.adaptive_playback_checkbox = QtWidgets.QCheckBox("Enabled (Follows exposure)")
        self.adaptive_playback_checkbox.setToolTip(
            "Automatically adjust playback speed and skip based on data collection rate (exposure time)."
        )
        self.adaptive_playback_checkbox.setChecked(
            self.current_settings.get("adaptive_live_playback", True)
        )
        playback_layout.addRow("Adaptive Mode:", self.adaptive_playback_checkbox)

        self.playback_skip_spinbox = QtWidgets.QSpinBox()
        self.playback_skip_spinbox.setRange(
            1, 1000
        )  # Min skip is 1, Max is arbitrary (e.g., 1000)
        self.playback_skip_spinbox.setSingleStep(1)
        self.playback_skip_spinbox.setValue(
            self.current_settings.get("playback_skip", 1)
        )  # Use get() with default
        playback_layout.addRow("Frames to Skip:", self.playback_skip_spinbox)

        self.playback_interval_spinbox = QtWidgets.QSpinBox()
        self.playback_interval_spinbox.setRange(10, 10000)
        self.playback_interval_spinbox.setSingleStep(10)
        self.playback_interval_spinbox.setSuffix(" ms")
        self.playback_interval_spinbox.setValue(
            self.current_settings.get("playback_interval_ms", 100)
        )
        playback_layout.addRow("Interval:", self.playback_interval_spinbox)

        left_column.addWidget(playback_group)
        left_column.addStretch()

        # --- Right Column: Common Processing Parameters ---
        common_proc_group = QtWidgets.QGroupBox("Common Data Processing Parameters")
        common_proc_layout = QtWidgets.QFormLayout(common_proc_group)

        common_info_label = QtWidgets.QLabel("<i>* Note: These parameters are shared with the Data Processing Server.</i>")
        common_info_label.setStyleSheet("color: gray;")
        common_proc_layout.addRow(common_info_label)

        self.common_mode_combo = QtWidgets.QComboBox()
        self.common_mode_combo.addItems(["Manual", "Spreadsheet"])
        mode = self.current_settings.get("processing_common_mode", "manual")
        index = self.common_mode_combo.findText(mode.capitalize())
        if index != -1:
            self.common_mode_combo.setCurrentIndex(index)
        self.common_mode_combo.currentTextChanged.connect(self._on_common_mode_changed)
        common_proc_layout.addRow("Parameter Source:", self.common_mode_combo)

        self.common_sg_input = QtWidgets.QLineEdit(
            self.current_settings.get("processing_common_space_group", "")
        )
        self.common_sg_input.setPlaceholderText("e.g. P212121")
        common_proc_layout.addRow("Space Group:", self.common_sg_input)

        self.common_uc_input = QtWidgets.QLineEdit(
            self.current_settings.get("processing_common_unit_cell", "")
        )
        self.common_uc_input.setPlaceholderText("e.g. 10 20 30 90 90 90")
        common_proc_layout.addRow("Unit Cell:", self.common_uc_input)

        self.common_model_input = self._create_file_input(
            self.current_settings.get("processing_common_model_file", ""),
            "PDB Files (*.pdb *.cif);;All Files (*)"
        )
        self.common_model_input.line_edit.setPlaceholderText("Path to .pdb or .cif")
        self.common_model_input.line_edit.editingFinished.connect(
            lambda: self._update_from_model_file(self.common_model_input.line_edit.text())
        )
        common_proc_layout.addRow("Model File:", self.common_model_input)

        self.common_ref_hkl_input = self._create_file_input(
            self.current_settings.get("processing_common_reference_reflection_file", ""),
            "Reflection Files (*.hkl *.mtz *.refl);;All Files (*)"
        )
        self.common_ref_hkl_input.line_edit.setPlaceholderText("Path to .hkl or .mtz")
        common_proc_layout.addRow("Reference HKL:", self.common_ref_hkl_input)

        self.common_proc_root_input = self._create_dir_input(
            self.current_settings.get("processing_common_proc_dir_root", "")
        )
        self.common_proc_root_input.line_edit.setPlaceholderText("Optional: Root directory for processing outputs")
        self.common_proc_root_input.line_edit.textChanged.connect(self._on_param_changed)
        common_proc_layout.addRow("Processing Root:", self.common_proc_root_input)

        self.common_res_low_spinbox = QtWidgets.QDoubleSpinBox()
        self.common_res_low_spinbox.setRange(0.0, 1000.0)
        # Handle None value for display (use 0 or default if None)
        val_low = self.current_settings.get("processing_common_res_cutoff_low")
        self.common_res_low_spinbox.setValue(val_low if val_low is not None else 0.0)
        common_proc_layout.addRow("Res Cutoff Low (Å):", self.common_res_low_spinbox)

        self.common_res_high_spinbox = QtWidgets.QDoubleSpinBox()
        self.common_res_high_spinbox.setRange(0.0, 100.0)
        val_high = self.current_settings.get("processing_common_res_cutoff_high")
        self.common_res_high_spinbox.setValue(val_high if val_high is not None else 0.0)
        common_proc_layout.addRow("Res Cutoff High (Å):", self.common_res_high_spinbox)

        self.common_native_checkbox = QtWidgets.QCheckBox("Process Native Data")
        self.common_native_checkbox.setChecked(self.current_settings.get("processing_common_native", True))
        self.common_native_checkbox.setToolTip("If checked, treat data as native (FRIEDEL'S_LAW=TRUE, no anomalous).")
        common_proc_layout.addRow(self.common_native_checkbox)

        self.refresh_spreadsheet_btn = QtWidgets.QPushButton("Reload from Spreadsheet")
        self.refresh_spreadsheet_btn.setToolTip("Fetch crystal parameters from the spreadsheet info in Redis.")
        self.refresh_spreadsheet_btn.clicked.connect(self.request_spreadsheet_update.emit)
        common_proc_layout.addRow(self.refresh_spreadsheet_btn)

        right_column.addWidget(common_proc_group)

        # --- Right Column: Pipelines by Mode ---
        pipelines_group = QtWidgets.QGroupBox("Pipelines by Collection Mode")
        pipelines_layout = QtWidgets.QGridLayout(pipelines_group)
        self.pipeline_checkboxes = {}
        
        mode_rows = [
            ("Std/Vec/Site", ["STANDARD", "VECTOR", "SINGLE", "SITE"], ["dozor", "xds", "xia2", "autoproc"]),
            ("Raster", ["RASTER"], ["dozor", "nxds", "xia2_ssx", "crystfel"]),
            ("Strategy", ["STRATEGY"], ["dozor", "xds_strategy", "mosflm_strategy"]),
        ]
        
        from qp2.image_viewer.config import DEFAULT_SETTINGS
        current_pipelines = self.current_settings.get("pipelines_by_mode") or DEFAULT_SETTINGS.get("pipelines_by_mode", {})
        
        info_label = QtWidgets.QLabel("<i>* Note: These settings are ONLY for the Data Processing Server.</i>")
        info_label.setStyleSheet("color: gray;")
        pipelines_layout.addWidget(info_label, 0, 0, 1, 6)
        
        row_idx = 1
        for label, modes, available_pipes in mode_rows:
            pipelines_layout.addWidget(QtWidgets.QLabel(f"<b>{label}:</b>"), row_idx, 0)
            col_idx = 1
            for pipe in available_pipes:
                cb = QtWidgets.QCheckBox(pipe)
                # Take state from first mode in group
                is_checked = pipe in current_pipelines.get(modes[0], [])
                cb.setChecked(is_checked)
                cb.stateChanged.connect(self._on_param_changed)
                pipelines_layout.addWidget(cb, row_idx, col_idx)
                
                for m in modes:
                    if m not in self.pipeline_checkboxes:
                        self.pipeline_checkboxes[m] = {}
                    self.pipeline_checkboxes[m][pipe] = cb
                col_idx += 1
            row_idx += 1
            
        right_column.addWidget(pipelines_group)

        right_column.addStretch()

        columns_layout.addWidget(left_column_container, 1)
        columns_layout.addWidget(right_column_container, 1)
        main_layout.addLayout(columns_layout)

        # --- Bottom Buttons ---
        restore_button = QtWidgets.QPushButton("Restore Defaults")
        restore_button.clicked.connect(self._restore_defaults)
        main_layout.addWidget(restore_button)

        # Connect all widgets to the debounced change handler
        self.low_perc_spinbox.valueChanged.connect(self._on_param_changed)
        self.high_perc_spinbox.valueChanged.connect(self._on_param_changed)
        self.rings_input.editingFinished.connect(self._on_param_changed)
        self.scan_mode_combo.currentIndexChanged.connect(self._on_param_changed)
        self.adaptive_playback_checkbox.stateChanged.connect(self._on_param_changed)
        self.playback_skip_spinbox.valueChanged.connect(self._on_param_changed)
        self.playback_interval_spinbox.valueChanged.connect(self._on_param_changed)
        
        self.common_sg_input.editingFinished.connect(self._on_param_changed)
        self.common_uc_input.editingFinished.connect(self._on_param_changed)
        self.common_model_input.line_edit.textChanged.connect(self._on_param_changed)
        self.common_ref_hkl_input.line_edit.textChanged.connect(self._on_param_changed)
        self.common_res_low_spinbox.valueChanged.connect(self._on_param_changed)
        self.common_res_high_spinbox.valueChanged.connect(self._on_param_changed)
        self.common_native_checkbox.stateChanged.connect(self._on_param_changed)

        # Debounce timer for parameter changes
        self._debounce_timer = QTimer(self)
        self._debounce_timer.setSingleShot(True)
        self._debounce_timer.timeout.connect(self._emit_settings_changed)
        
        self.settings_manager.settings_changed.connect(self.refresh_widgets)
        
        self.resize(850, 600)

    def refresh_widgets(self, new_settings: dict, changed_keys: list = None):
        """Updates the dialog's widgets from the settings dict."""
        self.current_settings = new_settings.copy()
        self.new_settings = new_settings.copy()
        
        # Block signals to avoid triggering _on_param_changed loops
        self.low_perc_spinbox.blockSignals(True)
        self.high_perc_spinbox.blockSignals(True)
        self.rings_input.blockSignals(True)
        self.scan_mode_combo.blockSignals(True)
        self.adaptive_playback_checkbox.blockSignals(True)
        self.playback_skip_spinbox.blockSignals(True)
        self.playback_interval_spinbox.blockSignals(True)
        self.common_sg_input.blockSignals(True)
        self.common_uc_input.blockSignals(True)
        self.common_model_input.line_edit.blockSignals(True)
        self.common_ref_hkl_input.line_edit.blockSignals(True)
        self.common_proc_root_input.line_edit.blockSignals(True)
        self.common_res_low_spinbox.blockSignals(True)
        self.common_res_high_spinbox.blockSignals(True)
        self.common_native_checkbox.blockSignals(True)
        
        for m_checks in self.pipeline_checkboxes.values():
             for cb in m_checks.values():
                 cb.blockSignals(True)

        try:
            self.low_perc_spinbox.setValue(new_settings.get("contrast_low_percentile", 5.0))
            self.high_perc_spinbox.setValue(new_settings.get("contrast_high_percentile", 95.0))
            
            rings_str = ", ".join(map(str, new_settings.get("resolution_rings", [])))
            self.rings_input.setText(rings_str)
            
            index = self.scan_mode_combo.findData(new_settings.get("scan_mode", "row_wise"))
            if index != -1:
                self.scan_mode_combo.setCurrentIndex(index)
                
            self.adaptive_playback_checkbox.setChecked(new_settings.get("adaptive_live_playback", True))
            self.playback_skip_spinbox.setValue(new_settings.get("playback_skip", 1))
            self.playback_interval_spinbox.setValue(new_settings.get("playback_interval_ms", 100))
            
            self.common_sg_input.setText(new_settings.get("processing_common_space_group", ""))
            self.common_uc_input.setText(new_settings.get("processing_common_unit_cell", ""))
            self.common_model_input.line_edit.setText(new_settings.get("processing_common_model_file", ""))
            self.common_ref_hkl_input.line_edit.setText(new_settings.get("processing_common_reference_reflection_file", ""))
            self.common_proc_root_input.line_edit.setText(new_settings.get("processing_common_proc_dir_root", ""))
            
            val_low = new_settings.get("processing_common_res_cutoff_low")
            self.common_res_low_spinbox.setValue(val_low if val_low is not None else 0.0)
            
            val_high = new_settings.get("processing_common_res_cutoff_high")
            self.common_res_high_spinbox.setValue(val_high if val_high is not None else 0.0)
            
            self.common_native_checkbox.setChecked(new_settings.get("processing_common_native", True))
            
            from qp2.image_viewer.config import DEFAULT_SETTINGS
            pipelines = new_settings.get("pipelines_by_mode") or DEFAULT_SETTINGS.get("pipelines_by_mode", {})
            for mode, checks in self.pipeline_checkboxes.items():
                active = pipelines.get(mode, [])
                for pipe, chkbox in checks.items():
                    chkbox.setChecked(pipe in active)
        finally:
            self.low_perc_spinbox.blockSignals(False)
            self.high_perc_spinbox.blockSignals(False)
            self.rings_input.blockSignals(False)
            self.scan_mode_combo.blockSignals(False)
            self.adaptive_playback_checkbox.blockSignals(False)
            self.playback_skip_spinbox.blockSignals(False)
            self.playback_interval_spinbox.blockSignals(False)
            self.common_sg_input.blockSignals(False)
            self.common_uc_input.blockSignals(False)
            self.common_model_input.line_edit.blockSignals(False)
            self.common_ref_hkl_input.line_edit.blockSignals(False)
            self.common_proc_root_input.line_edit.blockSignals(False)
            self.common_res_low_spinbox.blockSignals(False)
            self.common_res_high_spinbox.blockSignals(False)
            self.common_native_checkbox.blockSignals(False)
            
            for m_checks in self.pipeline_checkboxes.values():
                 for cb in m_checks.values():
                     cb.blockSignals(False)

    def _create_file_input(self, initial_text, file_filter):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        line_edit = QtWidgets.QLineEdit(initial_text)
        browse_button = QtWidgets.QPushButton("...")
        browse_button.setFixedSize(30, 22)
        browse_button.clicked.connect(
            lambda: self._browse_for_file(line_edit, file_filter)
        )
        layout.addWidget(line_edit)
        layout.addWidget(browse_button)
        widget.line_edit = line_edit
        return widget

    def _create_dir_input(self, initial_text):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        line_edit = QtWidgets.QLineEdit(initial_text)
        browse_button = QtWidgets.QPushButton("...")
        browse_button.setFixedSize(30, 22)
        browse_button.clicked.connect(
            lambda: self._browse_for_dir(line_edit)
        )
        layout.addWidget(line_edit)
        layout.addWidget(browse_button)
        widget.line_edit = line_edit
        return widget

    def _browse_for_file(self, line_edit, file_filter):
        start_dir = (
            os.path.dirname(line_edit.text())
            if line_edit.text()
            else os.path.expanduser("~")
        )
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Select File", start_dir, file_filter
        )
        if path:
            line_edit.setText(path)
            # Auto-populate if this is the model file
            if line_edit == self.common_model_input.line_edit:
                self._update_from_model_file(path)

    def _browse_for_dir(self, line_edit):
        start_dir = (
            line_edit.text()
            if line_edit.text() and os.path.isdir(line_edit.text())
            else os.path.expanduser("~")
        )
        path = QtWidgets.QFileDialog.getExistingDirectory(
            self, "Select Directory", start_dir
        )
        if path:
            line_edit.setText(path)

    def _update_from_model_file(self, file_path):
        handle_model_file_update(
            file_path_input=self.common_model_input.line_edit,
            space_group_input=self.common_sg_input,
            unit_cell_input=self.common_uc_input,
            download_dir_input=self.common_proc_root_input.line_edit
        )

    def _on_common_mode_changed(self, text):
        # Force immediate update of settings to ensure the new mode is saved
        self._emit_settings_changed()
        if text == "Spreadsheet":
            self.request_spreadsheet_update.emit()

    def _on_param_changed(self, *args):
        # Start or restart the debounce timer (e.g., 200ms delay)
        self._debounce_timer.start(200)

    def _emit_settings_changed(self):
        # Update new_settings dict with current widget values
        self.new_settings["contrast_low_percentile"] = self.low_perc_spinbox.value()
        self.new_settings["contrast_high_percentile"] = self.high_perc_spinbox.value()

        # Parse resolution rings
        rings_str = self.rings_input.text()
        try:
            rings = [float(r.strip()) for r in rings_str.split(",") if r.strip()]
        except Exception:
            rings = []
        self.new_settings["resolution_rings"] = rings
        self.new_settings["scan_mode"] = str(self.scan_mode_combo.currentData())

        self.new_settings["adaptive_live_playback"] = self.adaptive_playback_checkbox.isChecked()
        self.new_settings["playback_skip"] = self.playback_skip_spinbox.value()
        self.new_settings["playback_interval_ms"] = (
            self.playback_interval_spinbox.value()
        )
        
        self.new_settings["processing_common_space_group"] = sanitize_space_group(self.common_sg_input.text()) or ""
        self.new_settings["processing_common_unit_cell"] = self.common_uc_input.text().strip()
        self.new_settings["processing_common_model_file"] = self.common_model_input.line_edit.text().strip()
        self.new_settings["processing_common_reference_reflection_file"] = self.common_ref_hkl_input.line_edit.text().strip()
        self.new_settings["processing_common_proc_dir_root"] = self.common_proc_root_input.line_edit.text().strip()
        
        low_res = self.common_res_low_spinbox.value()
        self.new_settings["processing_common_res_cutoff_low"] = low_res if low_res > 0 else None
        
        high_res = self.common_res_high_spinbox.value()
        self.new_settings["processing_common_res_cutoff_high"] = high_res if high_res > 0 else None

        self.new_settings["processing_common_native"] = self.common_native_checkbox.isChecked()
        
        self.new_settings["pipelines_by_mode"] = {}
        for mode, checks in self.pipeline_checkboxes.items():
            active = [pipe for pipe, cb in checks.items() if cb.isChecked()]
            self.new_settings["pipelines_by_mode"][mode] = active

        # self.settings_changed.emit(self.new_settings.copy())
        # self.settings_manager.update_from_dict(self.new_settings.copy())
        changed_keys = {}
        for key, new_value in self.new_settings.items():
            old_value = self.current_settings.get(key)
            if old_value != new_value:
                changed_keys[key] = (old_value, new_value)

        if changed_keys:
            # This log is much more helpful for debugging
            logger.debug(f"Settings changed: {list(changed_keys.keys())}")
            self.settings_manager.update_from_dict(self.new_settings.copy())

    def _restore_defaults(self):
        from qp2.image_viewer.config import DEFAULT_SETTINGS

        # Set all widgets to their default values
        self.low_perc_spinbox.setValue(DEFAULT_SETTINGS["contrast_low_percentile"])
        self.high_perc_spinbox.setValue(DEFAULT_SETTINGS["contrast_high_percentile"])
        # Resolution rings
        rings_str = ", ".join(map(str, DEFAULT_SETTINGS["resolution_rings"]))
        self.rings_input.setText(rings_str)
        # Scan mode
        index = self.scan_mode_combo.findData(
            DEFAULT_SETTINGS.get("scan_mode", "row_wise")
        )
        if index != -1:
            self.scan_mode_combo.setCurrentIndex(index)

        self.adaptive_playback_checkbox.setChecked(DEFAULT_SETTINGS.get("adaptive_live_playback", True))
        self.playback_skip_spinbox.setValue(DEFAULT_SETTINGS["playback_skip"])
        self.playback_interval_spinbox.setValue(
            DEFAULT_SETTINGS["playback_interval_ms"]
        )
        
        self.common_sg_input.setText(DEFAULT_SETTINGS.get("processing_common_space_group", ""))
        self.common_uc_input.setText(DEFAULT_SETTINGS.get("processing_common_unit_cell", ""))
        self.common_model_input.line_edit.setText(DEFAULT_SETTINGS.get("processing_common_model_file", ""))
        self.common_ref_hkl_input.line_edit.setText(DEFAULT_SETTINGS.get("processing_common_reference_reflection_file", ""))
        self.common_proc_root_input.line_edit.setText(DEFAULT_SETTINGS.get("processing_common_proc_dir_root", ""))
        
        low_res = DEFAULT_SETTINGS.get("processing_common_res_cutoff_low")
        self.common_res_low_spinbox.setValue(low_res if low_res is not None else 0.0)
        
        high_res = DEFAULT_SETTINGS.get("processing_common_res_cutoff_high")
        self.common_res_high_spinbox.setValue(high_res if high_res is not None else 0.0)

        self.common_native_checkbox.setChecked(DEFAULT_SETTINGS.get("processing_common_native", True))
        
        pipelines = DEFAULT_SETTINGS.get("pipelines_by_mode", {})
        for mode, checks in self.pipeline_checkboxes.items():
            active = pipelines.get(mode, [])
            for pipe, chkbox in checks.items():
                chkbox.setChecked(pipe in active)

        self._emit_settings_changed()

    def accept(self):
        """Validate and store new settings on OK."""
        try:
            # Validate contrast
            low_p = self.low_perc_spinbox.value()
            high_p = self.high_perc_spinbox.value()
            if low_p >= high_p:
                raise ValueError("Low contrast percentile must be less than High.")
            self.new_settings["contrast_low_percentile"] = low_p
            self.new_settings["contrast_high_percentile"] = high_p

            # Validate resolution rings
            rings_str = self.rings_input.text()
            try:
                rings = [float(r.strip()) for r in rings_str.split(",") if r.strip()]
            except Exception:
                rings = []
            self.new_settings["resolution_rings"] = rings
            self.new_settings["scan_mode"] = self.scan_mode_combo.currentData()
            self.new_settings["adaptive_live_playback"] = self.adaptive_playback_checkbox.isChecked()
            self.new_settings["playback_skip"] = self.playback_skip_spinbox.value()
            self.new_settings["playback_interval_ms"] = (
                self.playback_interval_spinbox.value()
            )
            
            self.new_settings["processing_common_space_group"] = sanitize_space_group(self.common_sg_input.text()) or ""
            self.new_settings["processing_common_unit_cell"] = self.common_uc_input.text().strip()
            self.new_settings["processing_common_model_file"] = self.common_model_input.line_edit.text().strip()
            self.new_settings["processing_common_reference_reflection_file"] = self.common_ref_hkl_input.line_edit.text().strip()
            self.new_settings["processing_common_proc_dir_root"] = self.common_proc_root_input.line_edit.text().strip()
            
            low_res = self.common_res_low_spinbox.value()
            self.new_settings["processing_common_res_cutoff_low"] = low_res if low_res > 0 else None
            
            high_res = self.common_res_high_spinbox.value()
            self.new_settings["processing_common_res_cutoff_high"] = high_res if high_res > 0 else None

            self.new_settings["processing_common_native"] = self.common_native_checkbox.isChecked()
            
            self.new_settings["pipelines_by_mode"] = {}
            for mode, checks in self.pipeline_checkboxes.items():
                active = [pipe for pipe, cb in checks.items() if cb.isChecked()]
                self.new_settings["pipelines_by_mode"][mode] = active

            self.settings_changed.emit(self.new_settings.copy())
            super().accept()
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Settings Error", str(e))

    def get_updated_settings(self) -> dict:
        """Returns the validated new settings."""
        return self.new_settings

    def showEvent(self, event):
        super().showEvent(event)

    def keyPressEvent(self, event):
        """Override Enter to commit current widget value without closing."""
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            # Manually trigger the parameter change handler
            self._on_param_changed()
            event.accept()
        else:
            super().keyPressEvent(event)
