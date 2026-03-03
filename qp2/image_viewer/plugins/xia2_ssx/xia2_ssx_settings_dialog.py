# qp2/image_viewer/plugins/xia2_ssx/xia2_ssx_settings_dialog.py
import os
from PyQt5.QtCore import pyqtSignal
from pyqtgraph.Qt import QtWidgets
from qp2.image_viewer.ui.singleton_dialog import SingletonDialog
from qp2.image_viewer.utils.model_file_handler import handle_model_file_update
from qp2.utils.auxillary import sanitize_space_group


class Xia2SSXSettingsDialog(SingletonDialog):
    settings_changed = pyqtSignal(dict)

    def __init__(self, current_settings: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("xia2.ssx Settings")
        self.setModal(False)
        self.new_settings = current_settings.copy()

        layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()

        # --- Processing Parameters ---
        proc_group = QtWidgets.QGroupBox("Processing Parameters")
        proc_layout = QtWidgets.QFormLayout(proc_group)
        self.space_group = QtWidgets.QLineEdit(
            self.new_settings.get("xia2_ssx_space_group", "")
        )
        proc_layout.addRow("Space Group:", self.space_group)
        self.unit_cell = QtWidgets.QLineEdit(
            self.new_settings.get("xia2_ssx_unit_cell", "")
        )
        self.unit_cell.setPlaceholderText("a b c alpha beta gamma")
        proc_layout.addRow("Unit Cell:", self.unit_cell)

        self.d_min = QtWidgets.QLineEdit(
            str(self.new_settings.get("xia2_ssx_d_min", ""))
        )
        self.d_min.setPlaceholderText("High Resolution Cutoff (Angstroms)")
        proc_layout.addRow("Resolution Cutoff (d_min):", self.d_min)
        form_layout.addRow(proc_group)
        # --- Indexing Options ---
        indexing_group = QtWidgets.QGroupBox("Indexing Options")
        indexing_layout = QtWidgets.QFormLayout(indexing_group)
        
        self.max_lattices = QtWidgets.QSpinBox()
        self.max_lattices.setRange(1, 100)
        self.max_lattices.setValue(self.new_settings.get("xia2_ssx_max_lattices", 3))
        self.max_lattices.setToolTip("Maximum number of lattices to search for, per image")
        indexing_layout.addRow("Max Lattices:", self.max_lattices)

        self.min_spots = QtWidgets.QSpinBox()
        self.min_spots.setRange(1, 1000)
        self.min_spots.setValue(self.new_settings.get("xia2_ssx_min_spots", 10))
        self.min_spots.setToolTip("Attempt indexing on images with at least this number of strong spots")
        indexing_layout.addRow("Min Spots:", self.min_spots)
        
        form_layout.addRow(indexing_group)

        # --- Post-Processing / Reference ---
        ref_group = QtWidgets.QGroupBox("Reference Model")
        ref_layout = QtWidgets.QFormLayout(ref_group)
        self.model_pdb = self._create_file_input(
            self.new_settings.get("xia2_ssx_model", ""), "PDB/MTZ Files (*.pdb *.mtz *.hkl)"
        )
        self.model_pdb.line_edit.editingFinished.connect(
            lambda: self._update_from_model_file(self.model_pdb.line_edit.text())
        )
        ref_layout.addRow("Reference (PDB):", self.model_pdb)
        self.reference_hkl = self._create_file_input(
            self.new_settings.get("xia2_ssx_reference_hkl", ""), "Reference Files (*.mtz *.pdb *.cif);;All Files (*)"
        )
        ref_layout.addRow("Reference (HKL):", self.reference_hkl)
        form_layout.addRow(ref_group)

        # --- Job Control ---
        job_group = QtWidgets.QGroupBox("Job Control")
        job_layout = QtWidgets.QFormLayout(job_group)
        self.nproc = QtWidgets.QSpinBox(
            minimum=1, maximum=64, value=self.new_settings.get("xia2_ssx_nproc", 32)
        )
        job_layout.addRow("Processors:", self.nproc)
        self.njobs = QtWidgets.QSpinBox(
            minimum=1, maximum=16, value=self.new_settings.get("xia2_ssx_njobs", 1)
        )
        job_layout.addRow("Parallel Jobs:", self.njobs)
        
        self.incremental_merging = QtWidgets.QCheckBox("Incremental Merging (25%, 50%, 95%)")
        self.incremental_merging.setChecked(self.new_settings.get("xia2_ssx_incremental_merging", False))
        job_layout.addRow("", self.incremental_merging)

        self.force_reprocessing = QtWidgets.QCheckBox("Force Reprocessing")
        self.force_reprocessing.setChecked(self.new_settings.get("xia2_ssx_force_reprocessing", False))
        self.force_reprocessing.setToolTip("If unchecked, existing successful integration results will be reused.")
        job_layout.addRow("", self.force_reprocessing)

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
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Select File", start_dir, file_filter
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
            download_dir_input=None
        )

    def accept(self):
        self.new_settings["xia2_ssx_space_group"] = sanitize_space_group(self.space_group.text()) or ""

        unit_cell_str = self.unit_cell.text().strip()
        if unit_cell_str:
            try:
                # Normalize input: replace commas with spaces, split, and rejoin
                parts = unit_cell_str.replace(",", " ").split()
                if len(parts) != 6:
                    raise ValueError("Unit cell must have 6 parameters.")
                # Validate they are numbers
                [float(p) for p in parts]
                self.new_settings["xia2_ssx_unit_cell"] = " ".join(parts)
            except ValueError:
                QtWidgets.QMessageBox.warning(
                    self,
                    "Invalid Unit Cell",
                    "Unit Cell must be empty or contain 6 numbers separated by spaces or commas.",
                )
                return
        else:
            self.new_settings["xia2_ssx_unit_cell"] = ""

        d_min_str = self.d_min.text().strip()
        if d_min_str:
            try:
                self.new_settings["xia2_ssx_d_min"] = float(d_min_str)
            except ValueError:
                QtWidgets.QMessageBox.warning(
                    self,
                    "Invalid d_min",
                    "Resolution Cutoff (d_min) must be a number.",
                )
                return
        else:
             self.new_settings["xia2_ssx_d_min"] = ""
        
        self.new_settings["xia2_ssx_max_lattices"] = self.max_lattices.value()
        self.new_settings["xia2_ssx_min_spots"] = self.min_spots.value()

        self.new_settings["xia2_ssx_model"] = self.model_pdb.line_edit.text().strip()
        self.new_settings["xia2_ssx_reference_hkl"] = self.reference_hkl.line_edit.text().strip()
        self.new_settings["xia2_ssx_nproc"] = self.nproc.value()
        self.new_settings["xia2_ssx_njobs"] = self.njobs.value()
        self.new_settings["xia2_ssx_incremental_merging"] = self.incremental_merging.isChecked()
        self.new_settings["xia2_ssx_force_reprocessing"] = self.force_reprocessing.isChecked()
        self.settings_changed.emit(self.new_settings)
        super().accept()
