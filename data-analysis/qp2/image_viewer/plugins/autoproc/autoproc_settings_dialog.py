# qp2/image_viewer/plugins/autoproc/autoproc_settings_dialog.py
import os
from PyQt5.QtCore import pyqtSignal
from pyqtgraph.Qt import QtWidgets
from qp2.image_viewer.ui.singleton_dialog import SingletonDialog
from qp2.image_viewer.utils.model_file_handler import handle_model_file_update
from qp2.utils.auxillary import sanitize_space_group


class AutoPROCSettingsDialog(SingletonDialog):
    settings_changed = pyqtSignal(dict)

    def __init__(self, current_settings: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("autoPROC Settings")
        self.setModal(False)
        self.new_settings = current_settings.copy()

        layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()

        # --- Processing Parameters ---
        proc_group = QtWidgets.QGroupBox("Processing Parameters")
        proc_layout = QtWidgets.QFormLayout(proc_group)

        self.space_group = QtWidgets.QLineEdit(
            self.new_settings.get("autoproc_space_group", "")
        )
        proc_layout.addRow("Space Group:", self.space_group)

        self.unit_cell = QtWidgets.QLineEdit(
            self.new_settings.get("autoproc_unit_cell", "")
        )
        self.unit_cell.setPlaceholderText("a b c alpha beta gamma")
        proc_layout.addRow("Unit Cell:", self.unit_cell)

        self.highres = QtWidgets.QDoubleSpinBox(
            minimum=0.3,
            maximum=7.0,
            decimals=2,
            value=self.new_settings.get("autoproc_highres", 0.0),
        )
        self.highres.setSpecialValueText("Auto (CC1/2)")
        proc_layout.addRow("High Resolution (Å):", self.highres)

        self.native_checkbox = QtWidgets.QCheckBox("Process Native Data")
        self.native_checkbox.setChecked(self.new_settings.get("autoproc_native", True))
        proc_layout.addRow(self.native_checkbox)

        form_layout.addRow(proc_group)

        # --- Post-Processing ---
        post_proc_group = QtWidgets.QGroupBox("Post-Processing (Optional)")
        post_proc_layout = QtWidgets.QFormLayout(post_proc_group)
        self.model_pdb = self._create_file_input(
            self.new_settings.get("autoproc_model", ""), "PDB Files (*.pdb)"
        )
        # Connect PDB auto-download/update logic
        self.model_pdb.line_edit.editingFinished.connect(
            lambda: self._update_from_model_file(self.model_pdb.line_edit.text())
        )
        post_proc_layout.addRow("Model for Dimple (PDB):", self.model_pdb)
        form_layout.addRow(post_proc_group)

        # --- Job Control ---
        job_group = QtWidgets.QGroupBox("Job Control")
        job_layout = QtWidgets.QFormLayout(job_group)
        self.nproc = QtWidgets.QSpinBox(
            minimum=1, maximum=32, value=self.new_settings.get("autoproc_nproc", 4)
        )
        job_layout.addRow("Processors per Node:", self.nproc)
        self.njobs = QtWidgets.QSpinBox(
            minimum=1, maximum=32, value=self.new_settings.get("autoproc_njobs", 8)
        )
        job_layout.addRow("Parallel Nodes (XDS jobs):", self.njobs)
        self.fast_mode = QtWidgets.QCheckBox("Enable Fast Mode")
        self.fast_mode.setChecked(self.new_settings.get("autoproc_fast", True))
        job_layout.addRow(self.fast_mode)
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

        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Select File", os.path.expanduser("~"), file_filter
        )
        if path:
            line_edit.setText(path)
            if line_edit == self.model_pdb.line_edit:
                self._update_from_model_file(path)

    def _update_from_model_file(self, file_path):
        self._pdb_just_downloaded = handle_model_file_update(
            file_path_input=self.model_pdb.line_edit,
            space_group_input=self.space_group,
            unit_cell_input=self.unit_cell,
            download_dir_input=None  # Fallback to default directories
        )

    def accept(self):
        self.new_settings["autoproc_space_group"] = sanitize_space_group(self.space_group.text()) or ""

        unit_cell_str = self.unit_cell.text().strip()
        if unit_cell_str:
            try:
                # Normalize input: replace commas with spaces, split, and rejoin
                parts = unit_cell_str.replace(",", " ").split()
                if len(parts) != 6:
                    raise ValueError("Unit cell must have 6 parameters.")
                # Validate they are numbers
                [float(p) for p in parts]
                self.new_settings["autoproc_unit_cell"] = " ".join(parts)
            except ValueError:
                QtWidgets.QMessageBox.warning(
                    self,
                    "Invalid Unit Cell",
                    "Unit Cell must be empty or contain 6 numbers separated by spaces or commas.",
                )
                return
        else:
            self.new_settings["autoproc_unit_cell"] = ""

        self.new_settings["autoproc_highres"] = (
            self.highres.value() if self.highres.value() > 0 else None
        )
        self.new_settings["autoproc_native"] = self.native_checkbox.isChecked()
        self.new_settings["autoproc_model"] = self.model_pdb.line_edit.text().strip()
        self.new_settings["autoproc_nproc"] = self.nproc.value()
        self.new_settings["autoproc_njobs"] = self.njobs.value()
        self.new_settings["autoproc_fast"] = self.fast_mode.isChecked()

        self.settings_changed.emit(self.new_settings)
        super().accept()
