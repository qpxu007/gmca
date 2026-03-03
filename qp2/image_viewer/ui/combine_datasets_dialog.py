# qp2/image_viewer/ui/combine_datasets_dialog.py

import os
from pathlib import Path
from PyQt5 import QtWidgets, QtCore
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, 
    QPushButton, QFileDialog, QComboBox, QSpinBox, QFormLayout, QGroupBox, QCheckBox
)
from qp2.xio.proc_utils import determine_proc_base_dir

class CombineDatasetsDialog(QDialog):
    """
    Dialog to gather parameters for combining selected HDF5 datasets.
    Supports both static frame selection and Redis-based metric filtering.
    """

    def __init__(self, dataset_paths, parent=None):
        super().__init__(parent)
        self.dataset_paths = sorted(list(dataset_paths))
        self.setWindowTitle(f"Combine {len(self.dataset_paths)} Datasets")
        self.setMinimumWidth(500)
        
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Warning about metadata
        warning_label = QLabel("Warning: Expected metadata should be the same between datasets.")
        warning_label.setStyleSheet("color: red; font-weight: bold;")
        layout.addWidget(warning_label)

        # --- Output Settings ---
        output_group = QGroupBox("Output Settings")
        output_form = QFormLayout(output_group)
        
        self.prefix_edit = QLineEdit("combined_dataset")
        output_form.addRow("Output Prefix:", self.prefix_edit)
        
        # Calculate intelligent default directory
        default_dir = os.path.expanduser("~")
        if self.dataset_paths:
            # Try to determine based on the first dataset
            first_ds = self.dataset_paths[0]
            try:
                proc_base = determine_proc_base_dir(None, first_ds)
                if proc_base and os.access(proc_base, os.W_OK):
                    default_dir = str(proc_base)
            except Exception:
                pass # Fallback to home

        dir_layout = QHBoxLayout()
        self.dir_edit = QLineEdit(default_dir)
        self.browse_btn = QPushButton("Browse...")
        self.browse_btn.clicked.connect(self._browse_dir)
        dir_layout.addWidget(self.dir_edit)
        dir_layout.addWidget(self.browse_btn)
        output_form.addRow("Output Directory:", dir_layout)
        
        self.images_per_file_spin = QSpinBox()
        self.images_per_file_spin.setRange(1, 10000)
        self.images_per_file_spin.setValue(1000)
        output_form.addRow("Max Images per File:", self.images_per_file_spin)
        
        layout.addWidget(output_group)

        # --- Job Submission Settings ---
        submission_group = QGroupBox("Job Submission")
        submission_form = QFormLayout(submission_group)

        self.submit_chk = QCheckBox("Submit to Cluster (Slurm)")
        self.submit_chk.setChecked(True)
        self.submit_chk.toggled.connect(self._toggle_submission_fields)
        submission_form.addRow(self.submit_chk)

        self.walltime_edit = QLineEdit("02:00:00")
        submission_form.addRow("Walltime:", self.walltime_edit)

        self.memory_edit = QLineEdit("32gb")
        submission_form.addRow("Memory:", self.memory_edit)

        self.nproc_spin = QSpinBox()
        self.nproc_spin.setRange(1, 128)
        self.nproc_spin.setValue(8)
        submission_form.addRow("Parallel Processes:", self.nproc_spin)

        layout.addWidget(submission_group)

        # --- Selection Mode ---
        selection_group = QGroupBox("Frame Selection Mode")
        selection_layout = QVBoxLayout(selection_group)
        
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["Static (First Frame Only)", "Metric Filter (Redis Scan)"])
        self.mode_combo.setCurrentIndex(1)
        self.mode_combo.currentIndexChanged.connect(self._on_mode_changed)
        selection_layout.addWidget(self.mode_combo)
        
        # --- Redis Scan Sub-section ---
        self.redis_widget = QtWidgets.QWidget()
        redis_form = QFormLayout(self.redis_widget)
        
        self.plugin_combo = QComboBox()
        self.plugin_combo.addItems(["dozor", "spotfinder", "dials"])
        redis_form.addRow("Plugin:", self.plugin_combo)
        
        self.metric_edit = QLineEdit("Main Score")
        redis_form.addRow("Metric Name:", self.metric_edit)
        
        self.condition_edit = QLineEdit("> 10")
        self.condition_edit.setPlaceholderText("e.g. > 10, <= 0.5")
        redis_form.addRow("Condition:", self.condition_edit)
        
        self.redis_host_edit = QLineEdit("")
        self.redis_host_edit.setPlaceholderText("Optional host override")
        redis_form.addRow("Redis Host:", self.redis_host_edit)
        
        selection_layout.addWidget(self.redis_widget)
        self.redis_widget.setVisible(self.mode_combo.currentIndex() == 1)
        
        layout.addWidget(selection_group)

        # --- Dialog Buttons ---
        btn_layout = QHBoxLayout()
        self.combine_btn = QPushButton("Combine Datasets")
        self.combine_btn.setDefault(True)
        self.combine_btn.clicked.connect(self.accept)
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.reject)
        
        btn_layout.addStretch()
        btn_layout.addWidget(self.cancel_btn)
        btn_layout.addWidget(self.combine_btn)
        layout.addLayout(btn_layout)

    def _browse_dir(self):
        dir_path = QFileDialog.getExistingDirectory(self, "Select Output Directory", self.dir_edit.text())
        if dir_path:
            self.dir_edit.setText(dir_path)

    def _on_mode_changed(self, index):
        self.redis_widget.setVisible(index == 1)

    def _toggle_submission_fields(self, checked):
        self.walltime_edit.setEnabled(checked)
        self.memory_edit.setEnabled(checked)
        self.nproc_spin.setEnabled(checked)

    def get_params(self):
        params = {
            "prefix": self.prefix_edit.text(),
            "outdir": self.dir_edit.text(),
            "n": self.images_per_file_spin.value(),
            "mode": "redis" if self.mode_combo.currentIndex() == 1 else "static",
            "submit": self.submit_chk.isChecked(),
            "time": self.walltime_edit.text(),
            "mem": self.memory_edit.text(),
            "nproc": self.nproc_spin.value(),
        }
        
        if params["mode"] == "redis":
            params.update({
                "plugin": self.plugin_combo.currentText(),
                "metric": self.metric_edit.text(),
                "condition": self.condition_edit.text(),
                "redis_host": self.redis_host_edit.text() or None
            })
        else:
            # Default static mapping for the selected datasets: frame 1
            params["mapping"] = {path: [1] for path in self.dataset_paths}
            
        return params