# qp2/image_viewer/plugins/nxds/nxds_settings_dialog.py
import os

from PyQt5.QtCore import pyqtSignal
from pyqtgraph.Qt import QtWidgets

from qp2.image_viewer.ui.singleton_dialog import SingletonDialog
from qp2.image_viewer.utils.model_file_handler import handle_model_file_update
from qp2.utils.auxillary import sanitize_space_group


class NXDSSettingsDialog(SingletonDialog):
    settings_changed = pyqtSignal(dict)

    def __init__(self, current_settings: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("nXDS Settings")
        self.setModal(False)
        self.new_settings = current_settings.copy()

        layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()

        # --- Indexing && Scaling ---
        indexing_group = QtWidgets.QGroupBox("Indexing && Scaling")
        indexing_layout = QtWidgets.QFormLayout(indexing_group)
        self.space_group_input = QtWidgets.QLineEdit(
            self.new_settings.get("nxds_space_group", "")
        )
        self.space_group_input.setPlaceholderText("e.g., P43212 or 96")
        indexing_layout.addRow("Space Group:", self.space_group_input)

        self.unit_cell_input = QtWidgets.QLineEdit(
            self.new_settings.get("nxds_unit_cell", "")
        )
        self.unit_cell_input.setPlaceholderText("e.g., a b c alpha beta gamma")
        indexing_layout.addRow("Unit Cell:", self.unit_cell_input)

        self.resolution_spinbox = QtWidgets.QDoubleSpinBox()
        self.resolution_spinbox.setRange(0.0, 50.0)
        self.resolution_spinbox.setDecimals(2)
        self.resolution_spinbox.setValue(self.new_settings.get("nxds_resolution") or 0.0)
        self.resolution_spinbox.setSpecialValueText("Auto")
        indexing_layout.addRow("High Resolution Cutoff (Å):", self.resolution_spinbox)

        self.nxds_native = QtWidgets.QCheckBox("Process Native Data")
        self.nxds_native.setChecked(self.new_settings.get("nxds_native", True))
        indexing_layout.addRow(self.nxds_native)

        self.powder_checkbox = QtWidgets.QCheckBox(
            "Enable POWDER step to generate pseudo powder pattern"
        )
        self.powder_checkbox.setChecked(self.new_settings.get("nxds_powder", False))
        indexing_layout.addRow(self.powder_checkbox)
        form_layout.addRow(indexing_group)

        # --- Extra Parameters ---
        extra_group = QtWidgets.QGroupBox("Extra XDS.INP Parameters")
        extra_layout = QtWidgets.QVBoxLayout(extra_group)
        self.extra_params_input = QtWidgets.QPlainTextEdit(
            self.new_settings.get("nxds_extra_params", "")
        )
        self.extra_params_input.setPlaceholderText("KEY=VALUE\nKEY=VALUE")
        self.extra_params_input.setToolTip("Add extra XDS.INP parameters, one per line.\nExample:\nEXCLUDE_RESOLUTION_RANGE= 3.93 3.87")
        # Set a reasonable height
        self.extra_params_input.setFixedHeight(100)
        extra_layout.addWidget(self.extra_params_input)
        form_layout.addRow(extra_group)

        # --- Reference Files ---
        ref_group = QtWidgets.QGroupBox("Reference Files (Optional)")
        ref_layout = QtWidgets.QFormLayout(ref_group)
        self.ref_hkl_input = self._create_file_input(
            self.new_settings.get("nxds_reference_hkl", ""),
            "HKL Files (*.hkl *.ahkl)",
        )
        ref_layout.addRow("Reference HKL:", self.ref_hkl_input)

        self.pdb_input = self._create_file_input(
            self.new_settings.get("nxds_pdb_file", ""), "PDB Files (*.pdb)"
        )
        self.pdb_input.line_edit.editingFinished.connect(
            lambda: self._update_from_model_file(self.pdb_input.line_edit.text())
        )
        ref_layout.addRow("Reference PDB:", self.pdb_input)
        form_layout.addRow(ref_group)

        job_group = QtWidgets.QGroupBox("Job Control")
        job_layout = QtWidgets.QFormLayout(job_group)
        self.nproc_spinbox = QtWidgets.QSpinBox()
        self.nproc_spinbox.setRange(1, 32)
        self.nproc_spinbox.setValue(self.new_settings.get("nxds_nproc", 16))
        job_layout.addRow("Processors per Node (nproc):", self.nproc_spinbox)

        self.njobs_spinbox = QtWidgets.QSpinBox()
        self.njobs_spinbox.setRange(1, 15)
        self.njobs_spinbox.setValue(self.new_settings.get("nxds_njobs", 4))
        job_layout.addRow("Parallel Nodes (njobs):", self.njobs_spinbox)

        self.auto_merge_checkbox = QtWidgets.QCheckBox(
            "Auto-merge when all datasets are processed"
        )
        self.auto_merge_checkbox.setChecked(
            self.new_settings.get("nxds_auto_merge", False)
        )
        self.auto_merge_checkbox.setToolTip(
            "Requires Space Group and Unit Cell to be set. Will trigger merging\n"
            "for all successfully processed datasets currently in the viewer."
        )
        job_layout.addRow(self.auto_merge_checkbox)

        # Batch Merge Settings (Horizontal Layout)
        batch_layout = QtWidgets.QHBoxLayout()
        
        self.min_merge_size_spin = QtWidgets.QSpinBox()
        self.min_merge_size_spin.setRange(1, 1000)
        self.min_merge_size_spin.setValue(self.new_settings.get("nxds_min_merge_size", 20))
        self.min_merge_size_spin.setToolTip("Minimum number of datasets required to trigger the FIRST merge.")
        
        self.merge_step_size_spin = QtWidgets.QSpinBox()
        self.merge_step_size_spin.setRange(1, 1000)
        self.merge_step_size_spin.setValue(self.new_settings.get("nxds_merge_step_size", 20))
        self.merge_step_size_spin.setToolTip("Wait for this many NEW datasets before re-merging.")

        batch_layout.addWidget(QtWidgets.QLabel("Min Batch Size:"))
        batch_layout.addWidget(self.min_merge_size_spin)
        batch_layout.addWidget(QtWidgets.QLabel("Step Size:"))
        batch_layout.addWidget(self.merge_step_size_spin)
        
        # Add to form layout
        job_layout.addRow("Auto-Merge Control:", batch_layout)

        form_layout.addRow(job_group)

        layout.addLayout(form_layout)
        button_box = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _create_file_input(self, initial_text, file_filter):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        line_edit = QtWidgets.QLineEdit(initial_text)
        browse_button = QtWidgets.QPushButton("Browse...")
        browse_button.clicked.connect(
            lambda: self._browse_for_file(line_edit, file_filter)
        )
        layout.addWidget(line_edit)
        layout.addWidget(browse_button)
        widget.line_edit = line_edit
        return widget

    def _browse_for_file(self, line_edit, file_filter):
        if getattr(self, "_pdb_just_downloaded", False):
            self._pdb_just_downloaded = False
            return

        start_dir = (
            os.path.dirname(line_edit.text())
            if line_edit.text()
            else os.path.expanduser("~")
        )
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Select File", start_dir, file_filter
        )
        if file_path:
            line_edit.setText(file_path)
            if line_edit == self.pdb_input.line_edit:
                self._update_from_model_file(file_path)

    def _update_from_model_file(self, file_path):
        self._pdb_just_downloaded = handle_model_file_update(
            file_path_input=self.pdb_input.line_edit,
            space_group_input=self.space_group_input,
            unit_cell_input=self.unit_cell_input,
            download_dir_input=None,
            ref_hkl_input=self.ref_hkl_input.line_edit
        )

    def accept(self):
        self.new_settings["nxds_space_group"] = sanitize_space_group(self.space_group_input.text()) or ""

        unit_cell_str = self.unit_cell_input.text().strip()
        if unit_cell_str:
            try:
                sanitized_str = unit_cell_str.replace(",", " ")
                cell_params = [float(p) for p in sanitized_str.split()]
                if len(cell_params) != 6:
                    raise ValueError("Unit cell must have 6 parameters.")
                self.new_settings["nxds_unit_cell"] = " ".join(map(str, cell_params))
            except (ValueError, TypeError):
                QtWidgets.QMessageBox.warning(
                    self,
                    "Invalid Unit Cell",
                    "Unit Cell must be empty or contain 6 numbers separated by spaces/commas.",
                )
                return
        else:
            self.new_settings["nxds_unit_cell"] = ""

        res_val = self.resolution_spinbox.value()
        self.new_settings["nxds_resolution"] = res_val if res_val > 0.0 else None

        self.new_settings["nxds_native"] = self.nxds_native.isChecked()
        self.new_settings["nxds_powder"] = self.powder_checkbox.isChecked()
        self.new_settings["nxds_reference_hkl"] = (
            self.ref_hkl_input.line_edit.text().strip()
        )
        self.new_settings["nxds_pdb_file"] = self.pdb_input.line_edit.text().strip()
        self.new_settings["nxds_nproc"] = self.nproc_spinbox.value()
        self.new_settings["nxds_njobs"] = self.njobs_spinbox.value()
        self.new_settings["nxds_auto_merge"] = self.auto_merge_checkbox.isChecked()
        self.new_settings["nxds_min_merge_size"] = self.min_merge_size_spin.value()
        self.new_settings["nxds_merge_step_size"] = self.merge_step_size_spin.value()
        self.new_settings["nxds_extra_params"] = self.extra_params_input.toPlainText().strip()

        self.settings_changed.emit(self.new_settings)
        super().accept()
