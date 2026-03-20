# qp2/image_viewer/plugins/xds/xds_settings_dialog.py
import os

from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QFormLayout, QLineEdit, QCheckBox, QSpinBox, QPlainTextEdit,
    QDoubleSpinBox, QGroupBox, QDialogButtonBox, QHBoxLayout, QPushButton, QFileDialog, QMessageBox
)

from qp2.image_viewer.ui.singleton_dialog import SingletonDialog
from qp2.image_viewer.utils.model_file_handler import handle_model_file_update
from qp2.utils.auxillary import sanitize_space_group


class XDSSettingsDialog(SingletonDialog):
    settings_changed = pyqtSignal(dict)

    def __init__(self, current_settings: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("XDS Settings")
        self.setModal(False)
        self.new_settings = current_settings.copy()

        layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        # --- Indexing & Scaling ---
        indexing_group = QGroupBox("Indexing & Scaling")
        indexing_layout = QFormLayout(indexing_group)
        self.xds_space_group = QLineEdit(self.new_settings.get("xds_space_group", ""))
        indexing_layout.addRow("Space Group:", self.xds_space_group)

        self.xds_unit_cell = QLineEdit(self.new_settings.get("xds_unit_cell", ""))
        indexing_layout.addRow("Unit Cell:", self.xds_unit_cell)

        self.xds_resolution = QDoubleSpinBox(minimum=0.0, maximum=50.0, decimals=2,
                                             value=self.new_settings.get("xds_resolution") or 0.0)
        self.xds_resolution.setSpecialValueText("Auto")
        indexing_layout.addRow("High Resolution Cutoff (Å):", self.xds_resolution)

        self.xds_native = QCheckBox("Process Native Data")
        self.xds_native.setChecked(self.new_settings.get("xds_native", True))
        indexing_layout.addRow(self.xds_native)
        form_layout.addRow(indexing_group)

        # --- Extra Parameters ---
        extra_group = QGroupBox("Extra XDS.INP Parameters")
        extra_layout = QVBoxLayout(extra_group)
        self.extra_params_input = QPlainTextEdit(
            self.new_settings.get("xds_extra_params", "")
        )
        self.extra_params_input.setPlaceholderText("KEY=VALUE\nKEY=VALUE")
        self.extra_params_input.setToolTip("Add extra XDS.INP parameters, one per line.\nExample:\nEXCLUDE_RESOLUTION_RANGE= 3.93 3.87")
        self.extra_params_input.setFixedHeight(100)
        extra_layout.addWidget(self.extra_params_input)
        form_layout.addRow(extra_group)

        # --- Reference & Model Files ---
        ref_group = QGroupBox("Reference & Model Files (Optional)")
        ref_layout = QFormLayout(ref_group)
        self.xds_reference_hkl = self._create_file_input("HKL Files (*.hkl *.ahkl)")
        self.xds_reference_hkl.line_edit.setText(self.new_settings.get("xds_reference_hkl", ""))
        ref_layout.addRow("Reference HKL:", self.xds_reference_hkl)

        self.xds_model_pdb = self._create_file_input("PDB Files (*.pdb)")
        self.xds_model_pdb.line_edit.setText(self.new_settings.get("xds_model_pdb", ""))
        self.xds_model_pdb.line_edit.editingFinished.connect(
            lambda: self._update_from_model_file(self.xds_model_pdb.line_edit.text())
        )
        ref_layout.addRow("Model for Dimple (PDB):", self.xds_model_pdb)
        form_layout.addRow(ref_group)

        # --- Job Control & Advanced ---
        job_group = QGroupBox("Job Control & Advanced")
        job_layout = QFormLayout(job_group)
        self.xds_nproc = QSpinBox(minimum=1, maximum=128, value=self.new_settings.get("xds_nproc", 32))
        job_layout.addRow("Processors per Node (--nproc):", self.xds_nproc)
        self.xds_njobs = QSpinBox(minimum=1, maximum=32, value=self.new_settings.get("xds_njobs", 6))
        job_layout.addRow("Parallel Nodes (--njobs):", self.xds_njobs)
        default_proc_root = os.path.join(os.path.expanduser("~"), "xds_runs")
        self.xds_proc_dir_root = QLineEdit(self.new_settings.get("xds_proc_dir_root", default_proc_root))
        job_layout.addRow("Output Directory Root:", self.xds_proc_dir_root)
        form_layout.addRow(job_group)

        layout.addLayout(form_layout)

        # Apply common setting fallbacks
        self._apply_common_fallback(
            self.xds_space_group,
            self.new_settings.get("xds_space_group", ""),
            self.new_settings.get("processing_common_space_group", ""),
        )
        self._apply_common_fallback(
            self.xds_unit_cell,
            self.new_settings.get("xds_unit_cell", ""),
            self.new_settings.get("processing_common_unit_cell", ""),
        )
        self._apply_common_fallback(
            self.xds_model_pdb.line_edit,
            self.new_settings.get("xds_model_pdb", ""),
            self.new_settings.get("processing_common_model_file", ""),
        )
        self._apply_common_fallback(
            self.xds_reference_hkl.line_edit,
            self.new_settings.get("xds_reference_hkl", ""),
            self.new_settings.get("processing_common_reference_reflection_file", ""),
        )
        self._apply_common_spinbox_fallback(
            self.xds_resolution,
            self.new_settings.get("xds_resolution"),
            self.new_settings.get("processing_common_res_cutoff_high"),
        )

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _create_file_input(self, file_filter):
        widget = QWidget()
        layout = QHBoxLayout(widget);
        layout.setContentsMargins(0, 0, 0, 0)
        line_edit = QLineEdit()
        browse_button = QPushButton("...");
        browse_button.setFixedSize(30, 22)
        browse_button.clicked.connect(lambda: self._browse_for_file(line_edit, file_filter))
        layout.addWidget(line_edit);
        layout.addWidget(browse_button)
        widget.line_edit = line_edit
        return widget

    def _browse_for_file(self, line_edit, file_filter):
        if getattr(self, "_pdb_just_downloaded", False):
            self._pdb_just_downloaded = False
            return

        path, _ = QFileDialog.getOpenFileName(self, "Select File", os.path.expanduser("~"), file_filter)
        if path:
            line_edit.setText(path)
            if line_edit == self.xds_model_pdb.line_edit:
                self._update_from_model_file(path)

    def _update_from_model_file(self, file_path):
        self._pdb_just_downloaded = handle_model_file_update(
            file_path_input=self.xds_model_pdb.line_edit,
            space_group_input=self.xds_space_group,
            unit_cell_input=self.xds_unit_cell,
            download_dir_input=self.xds_proc_dir_root,
            ref_hkl_input=self.xds_reference_hkl.line_edit
        )

    def accept(self):
        self.new_settings["xds_space_group"] = sanitize_space_group(self.xds_space_group.text()) or ""

        unit_cell_str = self.xds_unit_cell.text().strip()
        if unit_cell_str:
            try:
                # Normalize input: replace commas with spaces, split and rejoin
                parts = unit_cell_str.replace(",", " ").split()
                if len(parts) != 6:
                    raise ValueError("Unit cell must have 6 parameters.")
                # Validate they are numbers
                [float(p) for p in parts]
                self.new_settings["xds_unit_cell"] = " ".join(parts)
            except ValueError:
                QMessageBox.warning(
                    self,
                    "Invalid Unit Cell",
                    "Unit Cell must be empty or contain 6 numbers separated by spaces or commas.",
                )
                return
        else:
            self.new_settings["xds_unit_cell"] = ""

        res_val = self.xds_resolution.value()
        self.new_settings["xds_resolution"] = res_val if res_val > 0.0 else None
        self.new_settings["xds_native"] = self.xds_native.isChecked()
        self.new_settings["xds_reference_hkl"] = self.xds_reference_hkl.line_edit.text().strip()
        self.new_settings["xds_model_pdb"] = self.xds_model_pdb.line_edit.text().strip()
        self.new_settings["xds_nproc"] = self.xds_nproc.value()
        self.new_settings["xds_njobs"] = self.xds_njobs.value()
        self.new_settings["xds_proc_dir_root"] = self.xds_proc_dir_root.text().strip()
        self.new_settings["xds_extra_params"] = self.extra_params_input.toPlainText().strip()

        self.settings_changed.emit(self.new_settings)
        super().accept()
