# qp2/image_viewer/plugins/dials/dials_settings_dialog.py
import os

from PyQt5.QtCore import pyqtSignal
from pyqtgraph.Qt import QtWidgets

from qp2.image_viewer.ui.singleton_dialog import SingletonDialog
from qp2.image_viewer.utils.model_file_handler import handle_model_file_update
from qp2.utils.auxillary import sanitize_space_group


class DialsSettingsDialog(SingletonDialog):
    settings_changed = pyqtSignal(dict)

    def __init__(self, current_settings: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("DIALS SSX Settings")
        self.setModal(False)
        self.new_settings = current_settings.copy()

        layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()

        # --- Spot Finding Settings ---
        spot_group = QtWidgets.QGroupBox("Spot Finding")
        spot_layout = QtWidgets.QFormLayout(spot_group)

        self.d_min_spinbox = QtWidgets.QDoubleSpinBox()
        self.d_min_spinbox.setRange(0.5, 50.0)
        self.d_min_spinbox.setDecimals(2)
        self.d_min_spinbox.setValue(self.new_settings.get("dials_d_min", 0.5))
        spot_layout.addRow("High Resolution Limit (d_min, Å):", self.d_min_spinbox)
        form_layout.addRow(spot_group)

        # --- Indexing Settings ---
        indexing_group = QtWidgets.QGroupBox("Indexing")
        indexing_layout = QtWidgets.QFormLayout(indexing_group)
        self.space_group_input = QtWidgets.QLineEdit(
            self.new_settings.get("dials_space_group", "")
        )
        self.space_group_input.setPlaceholderText("Optional: P43212 or 96")
        indexing_layout.addRow("Space Group:", self.space_group_input)

        self.unit_cell_input = QtWidgets.QLineEdit(
            self.new_settings.get("dials_unit_cell", "")
        )
        self.unit_cell_input.setPlaceholderText("Optional: a,b,c,α,β,γ")
        indexing_layout.addRow("Unit Cell:", self.unit_cell_input)
        form_layout.addRow(indexing_group)

        # --- Reference Files ---
        ref_group = QtWidgets.QGroupBox("Reference Files (Optional)")
        ref_layout = QtWidgets.QFormLayout(ref_group)
        self.ref_refl_input = self._create_file_input(
            self.new_settings.get("dials_reference_reflections", ""),
            "Reflection files (*.refl *.expt, *.mtz);;All Files (*)",
        )
        ref_layout.addRow("Reference Reflections/Expt:", self.ref_refl_input)

        self.model_pdb_input = self._create_file_input(
            self.new_settings.get("dials_model_pdb", ""), "PDB Files (*.pdb)"
        )
        self.model_pdb_input.line_edit.editingFinished.connect(
            lambda: self._update_from_model_file(self.model_pdb_input.line_edit.text())
        )
        ref_layout.addRow("Model:", self.model_pdb_input)
        form_layout.addRow(ref_group)

        # --- Parallelism && Advanced ---
        job_group = QtWidgets.QGroupBox("Job Control && Advanced")
        job_layout = QtWidgets.QFormLayout(job_group)
        self.nproc_spinbox = QtWidgets.QSpinBox()
        self.nproc_spinbox.setRange(1, 128)
        self.nproc_spinbox.setValue(self.new_settings.get("dials_nproc", 8))
        job_layout.addRow("Parallel Processors (nproc):", self.nproc_spinbox)

        self.extra_options_input = QtWidgets.QLineEdit(
            self.new_settings.get("dials_extra_options", "")
        )
        self.extra_options_input.setPlaceholderText(
            "e.g., indexing.method=real_space_grid_search"
        )
        self.extra_options_input.setToolTip(
            "Enter any additional command-line options for dials.ssx_index.\n"
            "Options will be passed directly to the command.\n"
            "Example: 'indexing.method=fft1d integration.algorithm=stills'"
        )
        job_layout.addRow("Additional Options:", self.extra_options_input)
        form_layout.addRow(job_group)

        layout.addLayout(form_layout)

        # --- Buttons ---
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
            if line_edit == self.model_pdb_input.line_edit:
                self._update_from_model_file(file_path)

    def _update_from_model_file(self, file_path):
        self._pdb_just_downloaded = handle_model_file_update(
            file_path_input=self.model_pdb_input.line_edit,
            space_group_input=self.space_group_input,
            unit_cell_input=self.unit_cell_input,
            download_dir_input=None
        )

    def accept(self):
        # self.new_settings["dials_min_spot_size"] = self.min_spot_size_spinbox.value()
        self.new_settings["dials_d_min"] = self.d_min_spinbox.value()
        self.new_settings["dials_space_group"] = sanitize_space_group(self.space_group_input.text()) or ""

        unit_cell_str = self.unit_cell_input.text().strip()
        if unit_cell_str:
            try:
                # Normalize input: replace commas with spaces, split, and rejoin
                parts = unit_cell_str.replace(",", " ").split()
                if len(parts) != 6:
                    raise ValueError("Unit cell must have 6 parameters.")
                # Validate they are numbers
                [float(p) for p in parts]
                self.new_settings["dials_unit_cell"] = " ".join(parts)
            except ValueError:
                QtWidgets.QMessageBox.warning(
                    self,
                    "Invalid Unit Cell",
                    "Unit Cell must be empty or contain 6 numbers separated by spaces or commas.",
                )
                return
        else:
            self.new_settings["dials_unit_cell"] = ""

        self.new_settings["dials_reference_reflections"] = (
            self.ref_refl_input.line_edit.text().strip()
        )
        self.new_settings["dials_model_pdb"] = (
            self.model_pdb_input.line_edit.text().strip()
        )
        self.new_settings["dials_nproc"] = self.nproc_spinbox.value()
        self.new_settings["dials_extra_options"] = (
            self.extra_options_input.text().strip()
        )

        self.settings_changed.emit(self.new_settings)
        super().accept()
