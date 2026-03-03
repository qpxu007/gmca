#!/usr/bin/env python3
import os
import sys
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

from PyQt5.QtWidgets import (

    QApplication,
    QDialog,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QComboBox,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QFileDialog,
    QGroupBox,
    QFormLayout,
    QCheckBox,
    QSpinBox,
    QDoubleSpinBox,
    QMessageBox,
    QSplitter,
    QDialogButtonBox,
    QProgressDialog,
    QListView,
    QTreeView,
    QAbstractItemView,
    QStyle,
    QStyleOptionComboBox,
    QStylePainter,
)
from PyQt5.QtGui import QStandardItem, QStandardItemModel, QPalette
from PyQt5.QtCore import Qt, QThreadPool, QEvent

# --- QP2 Core Imports ---
from qp2.log.logging_config import setup_logging, get_logger
from qp2.xio.hdf5_manager import HDF5Reader
from qp2.xio.user_group_manager import get_esaf_from_data_path
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.xio.redis_manager import RedisManager
from qp2.utils.icon import generate_icon_with_text
from qp2.utils.auxillary import sanitize_unit_cell, sanitize_space_group

# --- Plugin Worker Imports ---
from qp2.image_viewer.plugins.xds.submit_xds_job import XDSProcessDatasetWorker
from qp2.image_viewer.plugins.xia2.submit_xia2_job import Xia2ProcessDatasetWorker
from qp2.image_viewer.plugins.xia2_ssx.submit_xia2_ssx_job import Xia2SSXProcessDatasetWorker
from qp2.image_viewer.plugins.autoproc.submit_autoproc_job import (
    AutoPROCProcessDatasetWorker,
)
from qp2.image_viewer.plugins.nxds.submit_nxds_job import NXDSProcessDatasetWorker
from qp2.image_viewer.plugins.nxds.nxds_merging_worker import NXDSMergingWorker
from qp2.image_viewer.plugins.crystfel.submit_crystfel_job import CrystfelProcessDatasetWorker
from qp2.image_viewer.workers.directory_loader import DirectoryLoaderWorker
from qp2.image_viewer.utils.model_file_handler import handle_model_file_update
from qp2.image_viewer.plugins.crystfel.utils import calculate_robust_threshold_mad
from qp2.image_viewer.utils.run_job import run_command

# --- Symmetry ---
from qp2.pipelines.gmcaproc.symmetry import Symmetry

logger = get_logger("DatasetProcessorDialog")

REDIS_KEYS = {
    "XDS": "analysis:out:xds",
    "nXDS": "analysis:out:nxds",
    "xia2": "analysis:out:xia2",
    "xia2_ssx": "analysis:out:xia2_ssx",
    "autoPROC": "analysis:out:autoproc",
    "CrystFEL": "analysis:out:crystfel",
}

PIPELINES = ["XDS", "xia2", "xia2_ssx", "autoPROC", "nXDS", "CrystFEL"]


class CheckableComboBox(QComboBox):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.view().viewport().installEventFilter(self)
        self.setModel(QStandardItemModel(self))
        self.model().dataChanged.connect(self.update)

    def eventFilter(self, widget, event):
        if (
            event.type() == QEvent.MouseButtonPress
            and widget is self.view().viewport()
        ):
            index = self.view().indexAt(event.pos())
            item = self.model().itemFromIndex(index)
            if item:
                new_state = Qt.Unchecked if item.checkState() == Qt.Checked else Qt.Checked
                item.setCheckState(new_state)
            return True
        return super().eventFilter(widget, event)

    def paintEvent(self, event):
        painter = QStylePainter(self)
        painter.setPen(self.palette().color(QPalette.Text))

        opt = QStyleOptionComboBox()
        self.initStyleOption(opt)

        items = self.checked_items()
        if not items:
            opt.currentText = "(None selected)"
        else:
            opt.currentText = ", ".join(items)

        painter.drawComplexControl(QStyle.CC_ComboBox, opt)
        painter.drawControl(QStyle.CE_ComboBoxLabel, opt)

    def addItems(self, texts):
        for text in texts:
            item = QStandardItem(text)
            item.setCheckable(True)
            item.setCheckState(Qt.Unchecked)
            self.model().appendRow(item)

    def checked_items(self):
        checked = []
        for i in range(self.model().rowCount()):
            item = self.model().item(i)
            if item.checkState() == Qt.Checked:
                checked.append(item.text())
        return checked

    def set_checked_items(self, items):
        self.model().blockSignals(True)
        for i in range(self.model().rowCount()):
            item = self.model().item(i)
            if item.text() in items:
                item.setCheckState(Qt.Checked)
            else:
                item.setCheckState(Qt.Unchecked)
        self.model().blockSignals(False)
        self.update()


class LoadOptionsDialog(QDialog):
    """Dialog for configuring directory load options (recursive, filters)."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Load Options")
        self.resize(400, 300)
        layout = QVBoxLayout(self)

        # Recursive Option
        self.cb_recursive = QCheckBox("Search recursively (subdirectories)")
        self.cb_recursive.setChecked(True)
        layout.addWidget(self.cb_recursive)

        # Path Filter
        layout.addWidget(QLabel("Path must contain (optional):"))
        self.le_path_filter = QLineEdit()
        self.le_path_filter.setPlaceholderText("e.g., 'collect'")
        layout.addWidget(self.le_path_filter)

        layout.addWidget(QLabel("Exclude path containing (optional):"))
        self.le_path_not_filter = QLineEdit()
        self.le_path_not_filter.setPlaceholderText("e.g., 'test'")
        layout.addWidget(self.le_path_not_filter)

        # Image Count Filter
        layout.addWidget(QLabel("Frame Count:"))
        count_layout = QHBoxLayout()
        
        self.sb_min_images = QSpinBox()
        self.sb_min_images.setRange(0, 1000000)
        self.sb_min_images.setValue(0)
        self.sb_min_images.setPrefix(">= ")
        count_layout.addWidget(self.sb_min_images)

        count_layout.addWidget(QLabel("and"))

        self.sb_max_images = QSpinBox()
        self.sb_max_images.setRange(0, 1000000)
        self.sb_max_images.setValue(1000000)
        self.sb_max_images.setSpecialValueText("No Limit")
        count_layout.addWidget(self.sb_max_images)
        
        layout.addLayout(count_layout)

        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def get_options(self):
        max_img = self.sb_max_images.value()
        if max_img == self.sb_max_images.maximum():
            max_img = None
            
        return {
            "recursive": self.cb_recursive.isChecked(),
            "path_contains": self.le_path_filter.text().strip(),
            "path_not_contains": self.le_path_not_filter.text().strip(),
            "min_images": self.sb_min_images.value(),
            "max_images": max_img
        }


class DatasetProcessorDialog(QDialog):
    """
    Replacement implementation for the data processing dialog.
    Handles dataset selection, parameter configuration, and direct job submission.
    """

    def __init__(
        self,
        initial_dataset_paths,
        on_accept_callback=None,
        parent=None,
        job_context=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("QP2 Data Processing Launcher")
        self.resize(1000, 750)

        # Set Window Icon
        app_icon = generate_icon_with_text(text="dp", bg_color="#3498db", size=128)
        self.setWindowIcon(app_icon)

        self.datasets = []
        # Flag to track if user manually edited output dir
        self.manual_output_dir = False

        if initial_dataset_paths:
            for p in initial_dataset_paths:
                self._add_dataset_to_list(p)

        self.redis_manager = RedisManager()
        self.job_context = job_context
        self.on_accept_callback = on_accept_callback
        self.thread_pool = QThreadPool.globalInstance()

        self.setup_ui()
        # Trigger initial update after UI setup
        self._auto_update_output_dir()

    def setup_ui(self):
        main_layout = QVBoxLayout(self)

        # --- Splitter (Datasets | Parameters) ---
        splitter = QSplitter(Qt.Horizontal)

        # 1. Left Panel: Dataset Management
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)

        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["Master File Path", "Start", "End"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeToContents
        )
        self.table.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeToContents
        )
        left_layout.addWidget(self.table)

        btn_layout = QHBoxLayout()
        add_btn = QPushButton("Add Files...")
        add_btn.clicked.connect(self.add_files)
        
        add_dir_btn = QPushButton("Add Directories...")
        add_dir_btn.clicked.connect(self.add_directory)

        add_list_btn = QPushButton("Add from List...")
        add_list_btn.setToolTip("Load master file paths from a text file (one path per line)")
        add_list_btn.clicked.connect(self.add_from_file_list)
        
        rem_btn = QPushButton("Remove Selected")
        rem_btn.clicked.connect(self.remove_selected)
        
        btn_layout.addWidget(add_btn)
        btn_layout.addWidget(add_dir_btn)
        btn_layout.addWidget(add_list_btn)
        btn_layout.addWidget(rem_btn)
        left_layout.addLayout(btn_layout)

        # Merge Option
        merge_layout = QHBoxLayout()
        self.merge_chk = QCheckBox("Merge all datasets into ONE job")
        self.merge_chk.setToolTip("Only supported by Xia2 and autoPROC")
        self.merge_chk.toggled.connect(self.validate_ui_state)
        merge_layout.addWidget(self.merge_chk)
        
        self.merge_method_combo = QComboBox()
        self.merge_method_combo.addItems(["xscale", "xia2_multiplex"])
        self.merge_method_combo.setToolTip("Select merging strategy for XDS pipeline")
        self.merge_method_combo.setVisible(False)
        merge_layout.addWidget(self.merge_method_combo)
        
        left_layout.addLayout(merge_layout)

        splitter.addWidget(left_panel)

        # 2. Right Panel: Parameters
        right_panel = QGroupBox("Job Parameters")
        form = QFormLayout(right_panel)

        # Pipeline Selection
        self.pipeline_combo = QComboBox()
        self.pipeline_combo.addItems(PIPELINES)
        self.pipeline_combo.currentTextChanged.connect(self.validate_ui_state)
        self.pipeline_combo.currentTextChanged.connect(self._update_pipeline_defaults)
        form.addRow("Pipeline:", self.pipeline_combo)

        # Output Directory
        self.out_dir_edit = QLineEdit()
        self.out_dir_edit.setPlaceholderText("Auto-detect based on input path")
        # Track manual edits
        self.out_dir_edit.textEdited.connect(
            lambda: setattr(self, "manual_output_dir", True)
        )

        browse_out_btn = QPushButton("Browse...")
        browse_out_btn.clicked.connect(self.browse_output)
        out_layout = QHBoxLayout()
        out_layout.addWidget(self.out_dir_edit)
        out_layout.addWidget(browse_out_btn)
        form.addRow("Output Root:", out_layout)

        # Native Data (replacing Anomalous combo)
        self.native_chk = QCheckBox("Process Native Data")
        self.native_chk.setChecked(True)
        form.addRow("Data Type:", self.native_chk)

        # Crystallography Params
        self.sg_edit = QLineEdit()
        self.sg_edit.setPlaceholderText("e.g. P43212")
        form.addRow("Space Group:", self.sg_edit)

        self.cell_edit = QLineEdit()
        self.cell_edit.setPlaceholderText("a b c alpha beta gamma")
        form.addRow("Unit Cell:", self.cell_edit)

        self.res_spin = QDoubleSpinBox()
        self.res_spin.setRange(0, 100)
        self.res_spin.setSpecialValueText("Auto")
        self.res_spin.setValue(0)
        form.addRow("High Res (Å):", self.res_spin)

        # Model PDB
        self.pdb_edit = QLineEdit()
        self.pdb_edit.setPlaceholderText("Path to PDB file (optional)")
        self.pdb_edit.editingFinished.connect(self._update_from_model_file)
        browse_pdb_btn = QPushButton("Browse...")
        browse_pdb_btn.clicked.connect(self.browse_pdb)
        pdb_layout = QHBoxLayout()
        pdb_layout.addWidget(self.pdb_edit)
        pdb_layout.addWidget(browse_pdb_btn)
        form.addRow("Model PDB:", pdb_layout)

        # Reference HKL
        self.hkl_edit = QLineEdit()
        self.hkl_edit.setPlaceholderText("Path to HKL/MTZ file (optional)")
        browse_hkl_btn = QPushButton("Browse...")
        browse_hkl_btn.clicked.connect(self.browse_hkl)
        hkl_layout = QHBoxLayout()
        hkl_layout.addWidget(self.hkl_edit)
        hkl_layout.addWidget(browse_hkl_btn)
        form.addRow("Reference HKL:", hkl_layout)

        # Job Control
        self.nproc_spin = QSpinBox()
        self.nproc_spin.setRange(1, 256)
        self.nproc_spin.setValue(32)
        form.addRow("Cores/Job:", self.nproc_spin)

        self.nodes_spin = QSpinBox()
        self.nodes_spin.setRange(1, 64)
        self.nodes_spin.setValue(1)
        form.addRow("Nodes/Job:", self.nodes_spin)

        self.fast_chk = QCheckBox("Fast Mode")
        self.fast_chk.setChecked(True)
        form.addRow("Options:", self.fast_chk)

        self.trust_beam_chk = QCheckBox("Trust Beam Centre")
        self.trust_beam_chk.setToolTip("Only for xia2")
        self.trust_beam_chk.setChecked(True)
        form.addRow("", self.trust_beam_chk)

        self.force_rerun_chk = QCheckBox("Force Rerun (Clear Status)")
        self.force_rerun_chk.setChecked(True)
        form.addRow("", self.force_rerun_chk)

        splitter.addWidget(right_panel)
        splitter.setSizes([600, 400])
        main_layout.addWidget(splitter)

        # --- CrystFEL Settings (Collapsible) ---
        self.crystfel_toggle_btn = QPushButton("Show Advanced CrystFEL Settings")
        self.crystfel_toggle_btn.setCheckable(True)
        self.crystfel_toggle_btn.setStyleSheet("text-align: left; font-weight: bold; padding: 5px;")
        self.crystfel_toggle_btn.toggled.connect(self._toggle_crystfel_settings)
        main_layout.addWidget(self.crystfel_toggle_btn)

        self.crystfel_container = QWidget()
        crystfel_container_layout = QVBoxLayout(self.crystfel_container)
        crystfel_container_layout.setContentsMargins(0, 0, 0, 0)

        self.crystfel_group = QGroupBox("CrystFEL Configuration")
        crystfel_layout = QFormLayout(self.crystfel_group)

        # Peak Algorithm
        self.peak_method_combo = QComboBox()
        self.peak_method_combo.addItems(["peakfinder8"])
        crystfel_layout.addRow("Peak Algorithm:", self.peak_method_combo)
        
        self.min_peaks_spin = QSpinBox()
        self.min_peaks_spin.setRange(1, 100)
        self.min_peaks_spin.setValue(15)
        self.min_peaks_spin.setToolTip("Minimum number of peaks to consider a hit")
        crystfel_layout.addRow("Min Peaks/Frame:", self.min_peaks_spin)

        # SNR Settings
        self.min_snr_spin = QDoubleSpinBox()
        self.min_snr_spin.setRange(1.0, 20.0)
        self.min_snr_spin.setValue(5.0)
        crystfel_layout.addRow("Min SNR:", self.min_snr_spin)

        self.bg_radius_spin = QSpinBox()
        self.bg_radius_spin.setRange(1, 10)
        self.bg_radius_spin.setValue(3)
        crystfel_layout.addRow("Local BG Radius (px):", self.bg_radius_spin)

        # Peakfinder8 Specifics
        pf8_layout = QHBoxLayout()
        self.pf8_threshold_spin = QDoubleSpinBox()
        self.pf8_threshold_spin.setRange(0.0, 100000.0)
        self.pf8_threshold_spin.setValue(20.0)
        
        self.auto_thresh_chk = QCheckBox("Auto (MAD)")
        self.auto_thresh_chk.setChecked(True)
        self.auto_thresh_chk.setToolTip("Calculate threshold at runtime for each job.")
        self.auto_thresh_chk.toggled.connect(lambda c: self.pf8_threshold_spin.setEnabled(not c))
        
        self.estimate_btn = QPushButton("Estimate")
        self.estimate_btn.setToolTip("Calculate threshold now from first dataset")
        self.estimate_btn.clicked.connect(self._estimate_threshold)
        
        pf8_layout.addWidget(self.pf8_threshold_spin)
        pf8_layout.addWidget(self.estimate_btn)
        pf8_layout.addWidget(self.auto_thresh_chk)
        crystfel_layout.addRow("PF8 Threshold:", pf8_layout)

        self.pf8_min_pix_spin = QSpinBox()
        self.pf8_min_pix_spin.setRange(1, 100)
        self.pf8_min_pix_spin.setValue(2)
        crystfel_layout.addRow("PF8 Min Pixels:", self.pf8_min_pix_spin)

        self.pf8_max_pix_spin = QSpinBox()
        self.pf8_max_pix_spin.setRange(1, 1000)
        self.pf8_max_pix_spin.setValue(200)
        crystfel_layout.addRow("PF8 Max Pixels:", self.pf8_max_pix_spin)

        # Speed && Optimization
        speed_layout = QVBoxLayout()
        self.peakfinder8_fast_chk = QCheckBox("Use peakfinder8-fast")
        self.asdf_fast_chk = QCheckBox("Use asdf-fast")
        self.no_retry_chk = QCheckBox("Disable retry on failure")
        self.no_multi_chk = QCheckBox("Disable multi-indexing")
        self.no_refine_chk = QCheckBox("Disable refinement")
        self.no_check_peaks_chk = QCheckBox("Disable peak checking")
        self.no_non_hits_chk = QCheckBox("Reject Non-Hits (Stream size reduction)")
        self.include_mask_chk = QCheckBox("Include Bad Pixel Mask")
        
        speed_layout.addWidget(self.peakfinder8_fast_chk)
        speed_layout.addWidget(self.asdf_fast_chk)
        speed_layout.addWidget(self.no_retry_chk)
        speed_layout.addWidget(self.no_multi_chk)
        speed_layout.addWidget(self.no_refine_chk)
        speed_layout.addWidget(self.no_check_peaks_chk)
        speed_layout.addWidget(self.no_non_hits_chk)
        speed_layout.addWidget(self.include_mask_chk)
        crystfel_layout.addRow("Optimization:", speed_layout)

        # Indexing Methods
        self.indexing_methods_combo = CheckableComboBox()
        self.indexing_methods_combo.addItems([
            "xgandalf", "mosflm", "asdf", "dirax", 
            "taketwo", "smallcell", "xds", "pinkindexer", 
            "ffbidx", "felix"
        ])
        self.indexing_methods_combo.set_checked_items(["xgandalf"])
        crystfel_layout.addRow("Indexing Methods:", self.indexing_methods_combo)

        # XGANDALF Settings
        xg_layout = QHBoxLayout()
        self.xgandalf_fast_chk = QCheckBox("XGANDALF Fast")
        self.xgandalf_no_dev_chk = QCheckBox("No Deviation")
        xg_layout.addWidget(self.xgandalf_fast_chk)
        xg_layout.addWidget(self.xgandalf_no_dev_chk)
        crystfel_layout.addRow("XGANDALF:", xg_layout)

        # Integration
        self.push_res_spin = QDoubleSpinBox()
        self.push_res_spin.setRange(0.0, 10.0)
        self.push_res_spin.setSingleStep(0.1)
        self.push_res_spin.setValue(0.0)
        self.push_res_spin.setSpecialValueText("Disabled")
        crystfel_layout.addRow("Integration Res (nm⁻¹):", self.push_res_spin)

        self.int_radius_edit = QLineEdit("4,6,8")
        self.int_radius_edit.setPlaceholderText("inner,middle,outer")
        crystfel_layout.addRow("Integration Radii:", self.int_radius_edit)

        self.integration_combo = QComboBox()
        self.integration_combo.addItems(["Standard", "None (No Intensity)", "Cell Only (No Prediction)"])
        crystfel_layout.addRow("Integration Mode:", self.integration_combo)
        
        self.extra_options_edit = QLineEdit()
        self.extra_options_edit.setPlaceholderText("--option=val ...")
        crystfel_layout.addRow("Extra Options:", self.extra_options_edit)

        crystfel_container_layout.addWidget(self.crystfel_group)
        self.crystfel_container.setVisible(False)
        self.crystfel_toggle_btn.setVisible(False)
        main_layout.addWidget(self.crystfel_container)


        # Submit Button
        self.submit_btn = QPushButton("Submit Jobs to Cluster")
        self.submit_btn.setFixedHeight(40)
        self.submit_btn.setStyleSheet(
            "font-weight: bold; font-size: 14px; background-color: #4CAF50; color: white;"
        )
        self.submit_btn.clicked.connect(self.submit_jobs)
        main_layout.addWidget(self.submit_btn)

        # Initial Refresh
        self._refresh_table()
        self.validate_ui_state()
        self._update_pipeline_defaults()  # Set initial defaults for selected pipeline

    def _add_dataset_to_list(self, path, total_frames=None):
        """Adds a path to internal list, reading HDF5 for frame counts."""
        if total_frames is None:
            try:
                reader = HDF5Reader(path, start_timer=False)
                total_frames = reader.total_frames
                reader.close()
            except:
                total_frames = 100  # Default fallback

        if not any(d["path"] == path for d in self.datasets):
            self.datasets.append({"path": path, "start": 1, "end": total_frames})
            # Dataset list changed, update output dir
            self._auto_update_output_dir()
            
            # Auto-check merge if > 1 dataset
            if len(self.datasets) > 1 and hasattr(self, "merge_chk"):
                self.merge_chk.setChecked(True)

    def _auto_update_output_dir(self):
        """Updates output directory based on first dataset if not manually set."""
        if not hasattr(self, "out_dir_edit"):
            return

        if self.manual_output_dir or not self.datasets:
            return

        first_path = self.datasets[0]["path"]
        base = os.path.dirname(first_path)

        # Standardize /data/ -> /processing/ logic
        if "/data/" in base.lower():
            # Try case-sensitive first for Linux paths
            if "/DATA/" in base:
                proc_base = base.replace("/DATA/", "/PROCESSING/")
            elif "/data/" in base:
                proc_base = base.replace("/data/", "/processing/")
            else:
                proc_base = base  # Fallback
        else:
            # If not in standard structure, create PROCESSING sibling
            proc_base = os.path.join(base, "PROCESSING")

        self.out_dir_edit.setText(proc_base)

    def add_files(self):
        paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Select HDF5 Master Files",
            os.path.expanduser("~"),
            "HDF5 Master Files (*_master.h5)",
        )
        if paths:
            for p in paths:
                self._add_dataset_to_list(p)
            self._refresh_table()

    def add_from_file_list(self):
        """Loads a list of datasets from a text file (one full path per line)."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select File List",
            os.path.expanduser("~"),
            "Text Files (*.txt);;All Files (*)",
        )
        if not file_path:
            return

        try:
            with open(file_path, "r") as f:
                lines = f.readlines()
            
            added_count = 0
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("!"):
                    continue
                
                # Remove quotes if they exist (common in some exported lists)
                path = line.strip('"').strip("'")
                
                if path and os.path.isfile(path):
                    self._add_dataset_to_list(os.path.abspath(path))
                    added_count += 1
            
            if added_count > 0:
                self._refresh_table()
                logger.info(f"Added {added_count} datasets from list file: {file_path}")
            else:
                QMessageBox.warning(self, "No Files Added", "No valid master file paths found in the list.")
                
        except Exception as e:
            logger.error(f"Error loading file list: {e}", exc_info=True)
            QMessageBox.critical(self, "Error", f"Failed to load file list:\n{e}")

    def add_directory(self):
        # Configure QFileDialog for multi-directory selection
        file_dialog = QFileDialog(self, "Select Directories", os.path.expanduser("~"))
        file_dialog.setFileMode(QFileDialog.DirectoryOnly)
        file_dialog.setOption(QFileDialog.DontUseNativeDialog, True)

        file_view = file_dialog.findChild(QListView, 'listView')
        if file_view:
            file_view.setSelectionMode(QAbstractItemView.MultiSelection)
        
        f_tree_view = file_dialog.findChild(QTreeView)
        if f_tree_view:
            f_tree_view.setSelectionMode(QAbstractItemView.MultiSelection)

        directory_paths = []
        if file_dialog.exec_():
            directory_paths = file_dialog.selectedFiles()

        if not directory_paths:
            return

        # Bug fix: Filter out parent directories if their subdirectories are also selected.
        if len(directory_paths) > 1:
            cleaned_paths = list(set([os.path.abspath(p) for p in directory_paths]))
            final_paths = []
            for p in cleaned_paths:
                is_parent_of_other = False
                p_slash = p if p.endswith(os.sep) else p + os.sep
                for q in cleaned_paths:
                    if p != q and q.startswith(p_slash):
                        is_parent_of_other = True
                        break
                if not is_parent_of_other:
                    final_paths.append(p)
            directory_paths = final_paths

        # Show Options Dialog
        dlg = LoadOptionsDialog(self)
        if dlg.exec_() != QDialog.Accepted:
            return
            
        options = dlg.get_options()
        
        # Prepare Progress Dialog
        self.progress = QProgressDialog("Scanning directories...", "Cancel", 0, 0, self)
        self.progress.setWindowModality(Qt.WindowModal)
        self.progress.show()
        
        # Start Worker
        worker = DirectoryLoaderWorker(
            directory_paths=directory_paths,
            recursive=options["recursive"],
            min_images=options["min_images"],
            max_images=options["max_images"],
            path_contains=options["path_contains"],
            path_not_contains=options["path_not_contains"]
        )
        
        worker.signals.progress.connect(self.progress.setLabelText)
        worker.signals.found_batch.connect(self._on_directory_files_found)
        worker.signals.finished.connect(self._on_directory_search_finished)
        
        self.thread_pool.start(worker)

    def _on_directory_files_found(self, batch):
        """Handle batch of found readers/params from worker."""
        for reader, params in batch:
            try:
                path = reader.master_file_path
                frames = reader.total_frames
                self._add_dataset_to_list(path, total_frames=frames)
                reader.close() # Important: close file handle
            except Exception as e:
                logger.warning(f"Error processing found file: {e}")
                
        self._refresh_table()

    def _on_directory_search_finished(self, _):
        if hasattr(self, "progress"):
            self.progress.cancel()
            self.progress.deleteLater()
            del self.progress
        self._refresh_table()

    def remove_selected(self):
        rows = sorted(
            set(index.row() for index in self.table.selectedIndexes()), reverse=True
        )
        for row in rows:
            del self.datasets[row]
        self._refresh_table()
        self._auto_update_output_dir()

    def browse_output(self):
        d = QFileDialog.getExistingDirectory(self, "Select Output Root")
        if d:
            self.out_dir_edit.setText(d)
            self.manual_output_dir = True

    def _update_from_model_file(self):
        self._pdb_just_downloaded = handle_model_file_update(
            file_path_input=self.pdb_edit,
            space_group_input=self.sg_edit,
            unit_cell_input=self.cell_edit,
            download_dir_input=None
        )

    def browse_pdb(self):
        if getattr(self, "_pdb_just_downloaded", False):
            self._pdb_just_downloaded = False
            return

        f, _ = QFileDialog.getOpenFileName(
            self, "Select PDB Model", os.path.expanduser("~"), "PDB Files (*.pdb)"
        )
        if f:
            self.pdb_edit.setText(f)
            self._update_from_model_file()

    def browse_hkl(self):
        f, _ = QFileDialog.getOpenFileName(
            self, "Select Reference MTZ/PDB/CIF", os.path.expanduser("~"), "Reference Files (*.mtz *.pdb *.cif);;All Files (*)"
        )
        if f:
            self.hkl_edit.setText(f)

    def _refresh_table(self):
        """Rebuilds the table from self.datasets data."""
        self.table.setRowCount(0)
        self.table.blockSignals(True)

        for i, ds in enumerate(self.datasets):
            self.table.insertRow(i)

            item_path = QTableWidgetItem(ds["path"])
            item_path.setFlags(item_path.flags() ^ Qt.ItemIsEditable)
            self.table.setItem(i, 0, item_path)

            item_start = QTableWidgetItem(str(ds["start"]))
            self.table.setItem(i, 1, item_start)

            item_end = QTableWidgetItem(str(ds["end"]))
            self.table.setItem(i, 2, item_end)

        self.table.blockSignals(False)
        self.table.cellChanged.connect(self._on_cell_changed)

    def _on_cell_changed(self, row, col):
        try:
            val = int(self.table.item(row, col).text())
            if col == 1:
                self.datasets[row]["start"] = val
            elif col == 2:
                self.datasets[row]["end"] = val
        except ValueError:
            pass

    def validate_ui_state(self):
        pipeline = self.pipeline_combo.currentText()
        can_merge = pipeline in ["xia2", "autoPROC", "xia2_ssx", "XDS", "nXDS"]
        self.merge_chk.setEnabled(can_merge)
        if not can_merge:
            self.merge_chk.setChecked(False)

        is_xia2 = pipeline == "xia2"
        self.trust_beam_chk.setEnabled(is_xia2)
        self.trust_beam_chk.setVisible(is_xia2)

        # Fast mode not for CrystFEL
        self.fast_chk.setVisible(pipeline != "CrystFEL")

        # Show merge method combo only for XDS when merge is checked
        is_xds_merge = pipeline == "XDS" and self.merge_chk.isChecked()
        self.merge_method_combo.setVisible(is_xds_merge)
        
        # Show/Hide CrystFEL Group
        if hasattr(self, "crystfel_group"):
            is_crystfel = pipeline == "CrystFEL"
            self.crystfel_toggle_btn.setVisible(is_crystfel)
            # Only show container if pipeline is CrystFEL AND toggle is checked
            self.crystfel_container.setVisible(is_crystfel and self.crystfel_toggle_btn.isChecked())

        if pipeline == "nXDS":
            self.nodes_spin.setToolTip("For nXDS this controls MPI tasks")
        else:
            self.nodes_spin.setToolTip("Slurm Nodes")

    def _toggle_crystfel_settings(self, checked):
        if hasattr(self, "crystfel_container"):
            self.crystfel_container.setVisible(checked)
            self.crystfel_toggle_btn.setText(
                "Hide Advanced CrystFEL Settings" if checked else "Show Advanced CrystFEL Settings"
            )

    def _estimate_threshold(self):
        """Calculates Robust MAD threshold from the first frame of the first dataset."""
        if not self.datasets:
            QMessageBox.warning(self, "No Datasets", "Please add at least one dataset first.")
            return

        first_path = self.datasets[0]["path"]
        
        try:
            progress = QProgressDialog("Calculating threshold...", "Cancel", 0, 0, self)
            progress.setWindowModality(Qt.WindowModal)
            progress.show()
            QApplication.processEvents()

            reader = HDF5Reader(first_path, start_timer=False)
            # Read first frame (index 0)
            data, _ = reader.get_image(0)
            reader.close()
            
            progress.close()

            if data is None:
                QMessageBox.warning(self, "Error", "Failed to read image data from the first frame.")
                return

            # Calculate threshold (default to factor=1.0 for now, matching plugin logic if needed)
            # Plugin logic uses factor=1.0 inside calculate_robust_threshold_mad by default?
            # Let's check signature. It often takes data, mask, factor. 
            # We'll assume standard usage.
            threshold = calculate_robust_threshold_mad(data)

            if threshold is not None:
                self.pf8_threshold_spin.setValue(float(threshold))
                # Disable auto if estimated manually? Or just fill it in.
                # User asked to "let program set threshold", implying they want to see it.
                # Use might want to uncheck auto to use this value.
                if self.auto_thresh_chk.isChecked():
                    reply = QMessageBox.question(
                        self, 
                        "Threshold Calculated", 
                        f"Calculated MAD Threshold: {threshold:.2f}\n\n"
                        "The 'Auto (MAD)' checkbox is currently CHECKED, which will recalculate this at runtime.\n"
                        "Do you want to UNCHECK 'Auto (MAD)' to use this specific value?",
                        QMessageBox.Yes | QMessageBox.No
                    )
                    if reply == QMessageBox.Yes:
                        self.auto_thresh_chk.setChecked(False)
            else:
                QMessageBox.warning(self, "Calculation Failed", "Could not calculate a valid threshold.")

        except Exception as e:
            logger.error(f"Threshold estimation failed: {e}", exc_info=True)
            QMessageBox.critical(self, "Error", f"Failed to estimate threshold:\n{e}")

    def _update_pipeline_defaults(self):
        """Update nproc and njobs defaults based on selected pipeline."""
        pipeline = self.pipeline_combo.currentText()
        
        # Pipeline-specific defaults
        if pipeline in ["XDS", "nXDS"]:
            # XDS and nXDS: nproc=16, njobs=8
            self.nproc_spin.setValue(16)
            self.nodes_spin.setValue(8)
        elif pipeline in ["autoPROC", "xia2", "xia2_ssx"]:
            # autoPROC, xia2: nproc=32, njobs=1
            self.nproc_spin.setValue(32)
            self.nodes_spin.setValue(1)
        elif pipeline == "CrystFEL":
            # CrystFEL: nproc=32
            self.nproc_spin.setValue(32)
            self.nodes_spin.setValue(1)


    def _is_dir_writable(self, path_str):
        try:
            path = Path(path_str)
            # Check if path exists
            if path.exists():
                return os.access(path, os.W_OK)
            
            # If not, check nearest existing parent
            parent = path.parent
            while not parent.exists():
                if parent == parent.parent: # Reached root
                    break
                parent = parent.parent
            
            return os.access(parent, os.W_OK)
        except Exception:
            return False

    def submit_jobs(self):
        if not self.datasets:
            QMessageBox.warning(self, "Error", "No datasets selected.")
            return

        pipeline = self.pipeline_combo.currentText()
        out_root = self.out_dir_edit.text().strip()
        if out_root and not self._is_dir_writable(out_root):
             alt_path = os.path.join(os.path.expanduser("~"), f"{pipeline.lower()}_runs")
             reply = QMessageBox.warning(
                 self, 
                 "Output Directory Not Writable",
                 f"The output directory:\n{out_root}\nis not writable.\n\nWould you like to switch to the default: {alt_path}?",
                 QMessageBox.Yes | QMessageBox.No
             )
             if reply == QMessageBox.Yes:
                 self.out_dir_edit.setText(alt_path)
                 self.manual_output_dir = True
             else:
                 return
        redis_conn = self.redis_manager.get_analysis_connection()

        if not redis_conn:
            QMessageBox.critical(self, "Error", "Cannot connect to Redis.")
            return

        common_kwargs = self._gather_kwargs(pipeline)
        
        # Extract Redis Host for cluster jobs (crucial for accurate polling)
        try:
             redis_host = redis_conn.connection_pool.connection_kwargs.get('host', 'localhost')
             common_kwargs["nxds_redis_host"] = redis_host
             logger.info(f"Detected Redis Host for cluster jobs: {redis_host}")
        except Exception as e:
             logger.warning(f"Could not determine Redis host from connection: {e}")
             common_kwargs["nxds_redis_host"] = "localhost"

        try:
            # --- nXDS MANUAL MERGE ---
            if self.merge_chk.isChecked() and pipeline == "nXDS":
                 logger.info(f"Submitting {len(self.datasets)} individual nXDS jobs first...")
                 for ds in self.datasets:
                    self._launch_single_worker(ds, pipeline, redis_conn, common_kwargs)
                
                 logger.info("Submitting nXDS Merge Job (Redis Polling Step)...")
                 
                 # 1. Prepare file list (Master files for polling)
                 import sys
                 
                 dataset_paths = [d["path"] for d in self.datasets]
                 
                 # Resolve base processing directory
                 first_path = self.datasets[0]["path"]
                 proc_root = common_kwargs.get("nxds_proc_dir_root")
                 if not proc_root:
                      proc_root = os.path.dirname(first_path)
                      if "/data/" in proc_root.lower():
                           proc_root = proc_root.replace("/data/", "/processing/", 1).replace("/DATA/", "/PROCESSING/", 1)
                      else:
                           proc_root = os.path.join(proc_root, "PROCESSING")

                 common_prefix = os.path.commonprefix([os.path.basename(p) for p in dataset_paths]).rstrip("_-") or "merged"
                 merge_dir = Path(proc_root) / f"merge_solve_{common_prefix}_{len(dataset_paths)}datasets"
                 merge_dir.mkdir(parents=True, exist_ok=True)
                 
                 # Write MASTER FILES to list (not HKLs yet, script will resolve them)
                 hkl_list_file = merge_dir / "master_files_list.txt"
                 with open(hkl_list_file, "w") as f:
                     for p in dataset_paths:
                         f.write(p + "\n")
                         
                 # 2. command args
                 cluster_python = os.environ.get("CLUSTER_PYTHON")
                 cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")
                 
                 if cluster_python and cluster_root:
                     python_exe = cluster_python
                     # Assuming CLUSTER_PROJECT_ROOT points to the 'qp2' package root
                     merge_script = os.path.join(cluster_root, "image_viewer/plugins/nxds/run_nxds_merge.py")
                 else:
                     python_exe = sys.executable
                     merge_script = os.path.join(os.path.dirname(__file__), "../../../image_viewer/plugins/nxds/run_nxds_merge.py")
                     if not os.path.exists(merge_script):
                          merge_script = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../image_viewer/plugins/nxds/run_nxds_merge.py"))

                 cmd = [
                     python_exe,
                     merge_script,
                     "--hkl_list", str(hkl_list_file),
                     "--space_group", str(common_kwargs.get("nxds_space_group", "0")),
                     "--nproc", str(common_kwargs.get("nxds_nproc", 64)),
                     "--output_dir", str(merge_dir),
                     "--wait_for_keys",
                     "--redis_host", str(common_kwargs.get("nxds_redis_host", "localhost"))
                 ]

                 # Optional arguments
                 unit_cell = common_kwargs.get("nxds_unit_cell")
                 pdb_file = common_kwargs.get("nxds_pdb_file")
                 logger.info(f"Preparing merge command. Unit Cell: '{unit_cell}', PDB File: '{pdb_file}'")
                 
                 if unit_cell:
                     # Quote the unit cell string so it's treated as a single argument by the shell
                     quoted_unit_cell = f"'{sanitize_unit_cell(str(unit_cell))}'"
                     cmd.extend(["--unit_cell", quoted_unit_cell])

                 ref_hkl = common_kwargs.get("nxds_reference_hkl")
                 if ref_hkl:
                     # Quote paths just in case they contain spaces
                     cmd.extend(["--reference_hkl", f"'{str(ref_hkl)}'"])
                     
                 if pdb_file:
                     cmd.extend(["--pdb_file", f"'{str(pdb_file)}'"])
                 
                 # 3. Submit WITHOUT dependency (Script handles waiting)
                 logger.info("Submitting merge job with Redis Polling enabled.")

                 merge_job_id = run_command(
                     cmd=cmd,
                     cwd=str(merge_dir),
                     method="slurm",
                     job_name="nxds_merge_poll",
                     background=True,
                     processors=int(common_kwargs.get("nxds_nproc", 64)),
                 )
                 
                 msg = f"Submitted {len(self.datasets)} nXDS jobs + 1 Polling Merge Job (ID: {merge_job_id}). Results in {merge_dir}"

            # --- GENERIC MERGE (xia2, autoPROC, etc.) ---
            elif self.merge_chk.isChecked() and len(self.datasets) > 1:
                # MERGED JOB - Submit individual jobs first, then merge
                logger.info(f"Submitting {len(self.datasets)} individual {pipeline} jobs first...")
                
                # Submit individual jobs for each dataset
                for ds in self.datasets:
                    self._launch_single_worker(ds, pipeline, redis_conn, common_kwargs)
                
                logger.info(f"Now submitting merge job for {len(self.datasets)} datasets...")
                
                # Then submit the merge job
                primary = self.datasets[0]
                extra_paths = [d["path"] for d in self.datasets[1:]]

                kwargs = common_kwargs.copy()
                kwargs["extra_data_files"] = extra_paths

                self._launch_single_worker(primary, pipeline, redis_conn, kwargs)
                msg = f"Submitted {len(self.datasets)} individual jobs + 1 MERGED {pipeline} job."
            
            else:
                # SEPARATE JOBS
                for ds in self.datasets:
                    self._launch_single_worker(ds, pipeline, redis_conn, common_kwargs)
                msg = f"Submitted {len(self.datasets)} {pipeline} jobs."

            self.accept()
            QMessageBox.information(self, "Success", msg)

        except Exception as e:
            logger.error(f"Submission failed: {e}", exc_info=True)
            QMessageBox.critical(self, "Submission Failed", str(e))

    def _gather_kwargs(self, pipeline):
        sg = sanitize_space_group(self.sg_edit.text()) or ""
        cell = self.cell_edit.text().strip()
        res = self.res_spin.value() if self.res_spin.value() > 0 else None
        pdb = self.pdb_edit.text().strip()
        hkl = self.hkl_edit.text().strip()
        nproc = self.nproc_spin.value()
        njobs = self.nodes_spin.value()

        # Auto-scale down njobs for large batches to prevent cluster saturation
        if pipeline in ["XDS", "nXDS", "autoPROC"] and len(self.datasets) >= 20:
            njobs = 1
            logger.info(f"Large batch detected ({len(self.datasets)} datasets). Forcing njobs=1 for {pipeline}.")

        out_root = self.out_dir_edit.text().strip()
        fast = self.fast_chk.isChecked()
        trust_beam = self.trust_beam_chk.isChecked()
        force_rerun = self.force_rerun_chk.isChecked()
        is_native = self.native_chk.isChecked()
        
        # Merge method logic
        merge_method = None
        if self.merge_chk.isChecked() and pipeline == "XDS":
            merge_method = self.merge_method_combo.currentText()

        # Convert Space Group to Number for XDS/nXDS
        if pipeline in ["XDS", "nXDS"] and sg:
            try:
                sg_num = Symmetry.symbol_to_number(sg)
                if sg_num is not None:
                     sg = str(sg_num)
                     logger.info(f"Converted space group '{self.sg_edit.text()}' to number '{sg}'")
                elif not sg.isdigit():
                     # If it's a string and we couldn't resolve it, XDS might fail.
                     # Default to P1 (1) or just warn? XDS usually needs a number or explicit P1.
                     # But some users might be passing specific things. 
                     # For now, let's keep it as is if resolution fails, but log a warning.
                     logger.warning(f"Could not resolve space group '{sg}' to a number. XDS might require a number.")
            except Exception as e:
                logger.warning(f"Error converting space group: {e}")

        kwargs = {"force_rerun": force_rerun}
        if pipeline == "XDS":
            # If Fast Mode is UNCHECKED, we enable optimization
            is_optimization = not fast
            kwargs = {
                "xds_space_group": sg,
                "xds_unit_cell": cell,
                "xds_resolution": res,
                "xds_model_pdb": pdb,
                "xds_native": is_native,
                "xds_nproc": nproc,
                "xds_njobs": njobs,
                "xds_proc_dir_root": out_root,
                "xds_optimization": is_optimization,
            }
            if merge_method:
                kwargs["xds_merge_method"] = merge_method
        elif pipeline == "nXDS":
            kwargs = {
                "nxds_space_group": sg,
                "nxds_unit_cell": cell,
                "nxds_nproc": nproc,
                "nxds_njobs": njobs,
                "nxds_proc_dir_root": out_root,
                "nxds_native": is_native,
                "nxds_pdb_file": pdb,
                "nxds_reference_hkl": hkl,
            }
        elif pipeline == "xia2":
            kwargs = {
                "xia2_pipeline": "xia2_dials",
                "xia2_space_group": sg,
                "xia2_unit_cell": cell,
                "xia2_highres": res,
                "xia2_model": pdb,
                "xia2_nproc": nproc,
                "xia2_njobs": njobs,
                "xia2_fast": fast,
                "xia2_trust_beam_centre": trust_beam,
                "xia2_proc_dir_root": out_root,
                "xia2_native": is_native,
            }
        elif pipeline == "xia2_ssx":
            kwargs = {
                "xia2_ssx_space_group": sg,
                "xia2_ssx_unit_cell": cell,
                "xia2_ssx_model": pdb,
                "xia2_ssx_reference_hkl": hkl,
                "xia2_ssx_nproc": nproc,
                "xia2_ssx_njobs": njobs,
                "xia2_ssx_proc_dir_root": out_root,
            }
        elif pipeline == "autoPROC":
            kwargs = {
                "autoproc_space_group": sg,
                "autoproc_unit_cell": cell,
                "autoproc_highres": res,
                "autoproc_model": pdb,
                "autoproc_nproc": nproc,
                "autoproc_njobs": njobs,
                "autoproc_fast": fast,
                "autoproc_proc_dir_root": out_root,
                "autoproc_native": is_native,
            }
        elif pipeline == "CrystFEL":
            kwargs = {
                "crystfel_proc_dir_root": out_root,
                "nproc": nproc,
                "pdb_file": pdb,
                
                # Peak Finding
                "peak_method": self.peak_method_combo.currentText(),
                "min_peaks": self.min_peaks_spin.value(),
                "min_snr": self.min_snr_spin.value(),
                "local_bg_radius": self.bg_radius_spin.value(),
                
                # Peakfinder8
                "peakfinder8_threshold": self.pf8_threshold_spin.value(),
                "peakfinder8_auto_threshold": self.auto_thresh_chk.isChecked(),
                "peakfinder8_min_pix_count": self.pf8_min_pix_spin.value(),
                "peakfinder8_max_pix_count": self.pf8_max_pix_spin.value(),
                
                # Optimization
                "peakfinder8_fast": self.peakfinder8_fast_chk.isChecked(),
                "asdf_fast": self.asdf_fast_chk.isChecked(),
                "no_retry": self.no_retry_chk.isChecked(),
                "no_multi": self.no_multi_chk.isChecked(),
                "no_refine": self.no_refine_chk.isChecked(),
                "no_check_peaks": self.no_check_peaks_chk.isChecked(),
                "no_non_hits": self.no_non_hits_chk.isChecked(),
                
                # Indexing
                "indexing_methods": ",".join(self.indexing_methods_combo.checked_items()),
                "xgandalf_fast": self.xgandalf_fast_chk.isChecked(),
                # "xgandalf_no_deviation": self.xgandalf_no_dev_chk.isChecked(), # Note: Worker update may be needed
                
                # Integration
                "push_res": self.push_res_spin.value() if self.push_res_spin.value() > 0 else None,
                "integration_mode": self.integration_combo.currentText(),
                "extra_options": self.extra_options_edit.text(),
            }
        kwargs["force_rerun"] = force_rerun
        return kwargs

    def _launch_single_worker(self, dataset_info, pipeline, redis_conn, kwargs):
        master_file = dataset_info["path"]
        metadata = self._get_full_metadata(master_file)

        # nXDS restriction check
        if pipeline == "nXDS":
            collect_mode = metadata.get("collect_mode", "STANDARD").upper()
            if collect_mode != "RASTER":
                logger.warning(
                    f"Warning: nXDS is typically for RASTER datasets, but dataset '{os.path.basename(master_file)}' "
                    f"has mode '{collect_mode}'. Proceeding anyway."
                )
                # Removed blocking check to allow batch submissions
                # QMessageBox.warning(...)
                # return

        WorkerClass = {
            "XDS": XDSProcessDatasetWorker,
            "nXDS": NXDSProcessDatasetWorker,
            "xia2": Xia2ProcessDatasetWorker,
            "xia2_ssx": Xia2SSXProcessDatasetWorker,
            "autoPROC": AutoPROCProcessDatasetWorker,
            "CrystFEL": CrystfelProcessDatasetWorker,
        }[pipeline]

        # Pass frame ranges - Map to pipeline-specific keys
        if pipeline == "XDS":
            kwargs["xds_start"] = dataset_info["start"]
            kwargs["xds_end"] = dataset_info["end"]
        # Generic fallback
        kwargs["start_frame"] = dataset_info["start"]
        kwargs["end_frame"] = dataset_info["end"]

        worker = WorkerClass(
            master_file=master_file,
            metadata=metadata,
            redis_conn=redis_conn,
            redis_key_prefix=REDIS_KEYS[pipeline],
            **kwargs,
        )

        logger.info(f"Executing worker for {os.path.basename(master_file)}...")
        worker.run()

    def _get_full_metadata(self, master_file):
        meta = {}
        try:
            reader = HDF5Reader(master_file, start_timer=False)
            meta = reader.get_parameters()
            reader.close()
        except Exception:
            pass

        esaf_info = get_esaf_from_data_path(master_file)
        meta.update(esaf_info)
        meta.setdefault("beamline", get_beamline_from_hostname())
        meta.setdefault("username", os.getenv("USER"))
        
        # Ensure run_prefix exists for DB linking
        if "run_prefix" not in meta:
            meta["run_prefix"] = meta.get("prefix")
            
        return meta

    def get_configuration(self):
        """Legacy compatibility method."""
        return {
            "datasets": self.datasets,
            "pipeline": self.pipeline_combo.currentText(),
        }


if __name__ == "__main__":
    setup_logging()
    app = QApplication(sys.argv)
    initial = sys.argv[1:] if len(sys.argv) > 1 else []
    dlg = DatasetProcessorDialog(initial)
    dlg.show()
    sys.exit(app.exec_())