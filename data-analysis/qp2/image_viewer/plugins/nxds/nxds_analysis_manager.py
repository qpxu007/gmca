# qp2/image_viewer/plugins/nxds/nxds_analysis_manager.py
import os
import sys
import subprocess
import tempfile
import time
import redis
import json

from PyQt5 import QtWidgets, QtCore, QtGui

from qp2.image_viewer.plugins.nxds.cluster_analysis_dialog import ClusterAnalysisDialog
from qp2.image_viewer.plugins.nxds.nxds_merging_worker import NXDSMergingWorker
from qp2.image_viewer.ui.singleton_dialog import SingletonDialog
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

class LPViewerDialog(SingletonDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("nXSCALE Log File")
        self.setMinimumSize(800, 600)
        self.setModal(False)

        layout = QtWidgets.QVBoxLayout(self)
        self.text_edit = QtWidgets.QTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setFont(QtGui.QFont("Monospace", 9))
        layout.addWidget(self.text_edit)

        button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Close)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def set_content(self, content: str):
        self.text_edit.setPlainText(content)
        self.show()
        self.raise_()
        self.activateWindow()


class NXDSMergingDialog(QtWidgets.QDialog):
    def __init__(self, num_datasets: int, current_settings: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle(
            f"nXDS Merging & Solving Pipeline for {num_datasets} Datasets"
        )
        self.setModal(True)
        self.settings = {}
        self.current_settings = current_settings

        layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()

        self.space_group_input = QtWidgets.QLineEdit(
            self.current_settings.get("nxds_space_group", "")
        )
        self.space_group_input.setPlaceholderText("e.g., P43212 or 181")
        form_layout.addRow("Space Group:*", self.space_group_input)

        self.unit_cell_input = QtWidgets.QLineEdit(
            self.current_settings.get("nxds_unit_cell", "")
        )
        self.unit_cell_input.setPlaceholderText("e.g., 106 106 99 90 90 120")
        form_layout.addRow("Unit Cell:*", self.unit_cell_input)

        self.ref_hkl_input = self._create_file_input(
            self.current_settings.get("nxds_reference_hkl", ""),
            "HKL Files (*.hkl *.ahkl)",
        )
        form_layout.addRow("Reference HKL (Optional):", self.ref_hkl_input)

        self.pdb_input = self._create_file_input(
            self.current_settings.get("nxds_pdb_file", ""), "PDB Files (*.pdb)"
        )
        form_layout.addRow("PDB for Dimple (Optional):", self.pdb_input)

        self.nproc_spinbox = QtWidgets.QSpinBox()
        self.nproc_spinbox.setRange(1, 256)
        self.nproc_spinbox.setValue(24)
        form_layout.addRow("Processors:", self.nproc_spinbox)

        self.resolution_low_input = QtWidgets.QDoubleSpinBox()
        self.resolution_low_input.setRange(0.0, 100.0)
        self.resolution_low_input.setValue(50.0)
        self.resolution_low_input.setDecimals(1)
        form_layout.addRow("Low Resolution (Å):", self.resolution_low_input)

        self.resolution_high_input = QtWidgets.QDoubleSpinBox()
        self.resolution_high_input.setRange(0.1, 100.0)
        self.resolution_high_input.setValue(1.5)
        self.resolution_high_input.setDecimals(2)
        form_layout.addRow("High Resolution (Å):", self.resolution_high_input)

        layout.addLayout(form_layout)

        info_label = QtWidgets.QLabel("*Space Group and Unit Cell are required.")
        info_label.setStyleSheet("font-style: italic; color: grey;")
        layout.addWidget(info_label)

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

    def accept(self):
        sg = self.space_group_input.text().strip()
        cell = self.unit_cell_input.text().strip()

        if not sg or not cell:
            QtWidgets.QMessageBox.warning(
                self, "Input Required", "Space Group and Unit Cell cannot be empty."
            )
            return

        try:
            # Sanitize input (commas, multiple spaces) and parse
            sanitized_str = cell.replace(",", " ")
            cell_params = [float(p) for p in sanitized_str.split()]
            if len(cell_params) != 6:
                raise ValueError("Unit cell must contain 6 parameters.")
        except (ValueError, TypeError):
            QtWidgets.QMessageBox.warning(
                self,
                "Invalid Input",
                "Unit Cell must contain 6 numbers separated by spaces or commas.",
            )
            return

        self.settings["space_group"] = sg
        # Reformat to the canonical space-separated string before storing
        self.settings["unit_cell"] = " ".join(map(str, cell_params))
        self.settings["reference_hkl"] = self.ref_hkl_input.line_edit.text().strip()
        self.settings["pdb_file"] = self.pdb_input.line_edit.text().strip()
        self.settings["nproc"] = self.nproc_spinbox.value()
        self.settings["res_low"] = self.resolution_low_input.value()
        self.settings["res_high"] = self.resolution_high_input.value()

        super().accept()

    def get_settings(self):
        return self.settings


class NXDSAnalysisManager:
    def __init__(self, main_window):
        self.main_window = main_window
        self.ui_manager = main_window.ui_manager
        self.threadpool = main_window.threadpool
        self.redis_conn = main_window.redis_output_server
        self.dataset_manager = main_window.dataset_manager
        self.lp_viewer = None
        # Track last merged count per sample group to prevent re-merging same data
        # Key: sample_name, Value: integer count
        self.last_merged_counts = {}

    def check_auto_merge_conditions(self):
        """
        Checks if auto-merge should be triggered for any sample group.
        Logic:
        1. Group datasets by Sample.
        2. For each sample, check if ALL jobs are finished.
        3. Check batch size thresholds (Min Size, Step Size, or Quiet Catch-all).
        4. Trigger merge if conditions met.
        """
        settings = self.main_window.settings_manager

        # 1. Global On/Off Switch
        if not settings.get("nxds_auto_merge", False):
            return

        # 2. Check Prerequisites (SG/Cell)
        space_group = settings.get("nxds_space_group", "").strip()
        unit_cell = settings.get("nxds_unit_cell", "").strip()
        if not space_group or not unit_cell:
            return

        # 3. Get Batch Settings
        min_merge_size = settings.get("nxds_min_merge_size", 20)
        merge_step_size = settings.get("nxds_merge_step_size", 20)

        # 4. Group Datasets by Sample
        # We use the dataset manager's structure which is already grouped by Sample -> Run
        all_samples = self.dataset_manager.get_all_data()
        
        for sample_name, sample_data in all_samples.items():
            sample_datasets = []
            
            # Flatten runs for this sample
            for run_name, run_info in sample_data.get("runs", {}).items():
                for reader in run_info["datasets"]:
                     sample_datasets.append(reader.master_file_path)

            if not sample_datasets:
                continue

            # 5. Check Processing Status for this Group
            # If ANY job in this sample is running, we wait.
            running_jobs = 0
            completed_datasets = []
            
            for master_file in sample_datasets:
                redis_key = f"analysis:out:nxds:{master_file}"
                status_key = f"{redis_key}:status"
                try:
                    status_json = self.redis_conn.get(status_key)
                    if status_json:
                        status_data = json.loads(status_json)
                        status = status_data.get("status")
                        if status in ["RUNNING", "SUBMITTED"]:
                            running_jobs += 1
                        elif status == "COMPLETED":
                            completed_datasets.append(master_file)
                except Exception:
                    pass # Treat errors as non-running

            current_count = len(completed_datasets)
            last_count = self.last_merged_counts.get(sample_name, 0)
            
            # 6. Evaluation Logic
            should_merge = False
            
            # A. Safety: Must have 0 running jobs to merge
            if running_jobs > 0:
                continue # Wait for quiet period

            # B. Must have new data
            if current_count <= last_count:
                continue
            
            # C. Threshold Logic
            # Condition 1: Initial Batch
            if last_count == 0 and current_count >= min_merge_size:
                should_merge = True
                logger.info(f"[Auto-Merge] Triggering INITIAL merge for {sample_name} (Count: {current_count} >= {min_merge_size})")

            # Condition 2: Step Increment
            elif (current_count - last_count) >= merge_step_size:
                should_merge = True
                logger.info(f"[Auto-Merge] Triggering STEP merge for {sample_name} (Count: {current_count}, Δ: {current_count - last_count})")

            # Condition 3: Quiet Period Catch-all
            # If we have new data (current > last) AND no running jobs, 
            # we merge to ensure we don't leave data stranded just because it didn't hit the exact step size.
            # (Note: running_jobs == 0 is already checked above)
            elif current_count > last_count:
                 should_merge = True
                 logger.info(f"[Auto-Merge] Triggering CATCH-ALL merge for {sample_name} (Count: {current_count}, Idle)")

            if should_merge:
                self.last_merged_counts[sample_name] = current_count
                
                # Prepare Settings
                merge_settings = {
                    "space_group": space_group,
                    "unit_cell": unit_cell,
                    "reference_hkl": settings.get("nxds_reference_hkl", ""),
                    "pdb_file": settings.get("nxds_pdb_file", ""),
                    "nproc": settings.get("nxds_nproc", 24),
                    "res_low": settings.get("processing_common_res_cutoff_low", 50.0),
                    "res_high": settings.get("processing_common_res_cutoff_high", 1.5),
                }

                logger.info(f"[Auto-Merge] Launching pipeline for {sample_name} with {current_count} datasets.")
                self.ui_manager.show_status_message(f"Auto-Merging {sample_name} ({current_count} datasets)...", 5000)
                
                # Launch Non-Interactive Merge
                self.run_merging_non_interactive(completed_datasets, merge_settings)

    def run_cluster_analysis(self, dataset_paths):
        if not self.redis_conn:
            self.ui_manager.show_warning_message(
                "Redis Error", "Analysis Redis connection is not available."
            )
            return

        dialog = ClusterAnalysisDialog(dataset_paths, self.redis_conn, self.main_window)
        dialog.exec_()

    def run_orientation_analysis(self, dataset_paths):
        """Run crystal orientation analysis on the selected datasets.

        Resolves nXDS processing directories from Redis, runs
        nxds_orientation_analysis.py as a subprocess in a background thread,
        and displays the resulting PNG plot when done.
        """
        if not self.redis_conn:
            self.ui_manager.show_warning_message(
                "Redis Error", "Analysis Redis connection is not available."
            )
            return

        # Resolve processing directories from Redis
        proc_dirs = []
        for path in dataset_paths:
            redis_key = f"analysis:out:nxds:{path}"
            proc_dir = self.redis_conn.hget(redis_key, "_proc_dir")
            if proc_dir:
                if isinstance(proc_dir, bytes):
                    proc_dir = proc_dir.decode("utf-8")
                if os.path.isdir(proc_dir):
                    proc_dirs.append(proc_dir)

        if not proc_dirs:
            self.ui_manager.show_warning_message(
                "No Data",
                "No nXDS processing directories found for the selected datasets.\n"
                "Ensure the datasets have been processed with nXDS first.",
            )
            return

        # Determine output path (temp directory)
        output_dir = tempfile.mkdtemp(prefix="orientation_analysis_")
        output_plot = os.path.join(output_dir, "orientation_analysis.png")

        # Build the command
        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "nxds", "orientation_clustering", "nxds_orientation_analysis.py",
        )
        cmd = [sys.executable, script_path]
        cmd.extend(proc_dirs)
        cmd.extend(["-o", output_plot])

        logger.info(f"Orientation Analysis Command: {' '.join(cmd)}")
        logger.info(f"Analyzing {len(proc_dirs)} directories: {proc_dirs}")

        self.ui_manager.show_status_message(
            f"Running orientation analysis on {len(proc_dirs)} directories...", 0
        )

        worker = _OrientationAnalysisWorker(cmd, output_plot)
        worker.signals.finished.connect(self._on_orientation_finished)
        worker.signals.error.connect(self._on_orientation_error)
        self.threadpool.start(worker)

    def _on_orientation_finished(self, png_path):
        self.ui_manager.show_status_message("Orientation analysis complete.", 5000)
        dialog = _OrientationResultDialog(png_path, self.main_window)
        dialog.show()

    def _on_orientation_error(self, err_msg):
        self.ui_manager.show_status_message("Orientation analysis failed.", 5000)
        self.ui_manager.show_critical_message(
            "Orientation Analysis Error", err_msg
        )


    def run_merging(self, dataset_paths):
        if not self.redis_conn:
            self.ui_manager.show_warning_message(
                "Redis Error", "Analysis Redis connection is not available."
            )
            return

        saturation_value = None
        if dataset_paths:
            reader = self.dataset_manager.get_reader(dataset_paths[0])
            if reader:
                params = reader.get_parameters()
                saturation_value = params.get("saturation_value")
                sensor_thickness = params.get("sensor_thickness")
            
        if saturation_value is None:
            self.ui_manager.show_warning_message(
                "Parameter Warning",
                "Could not retrieve 'saturation_value' for the selected datasets. Using a default value.",
            )
            saturation_value = 60000

        dialog = NXDSMergingDialog(
            num_datasets=len(dataset_paths),
            current_settings=self.main_window.settings_manager.as_dict(),
            parent=self.main_window,
        )
        if dialog.exec_() == QtWidgets.QDialog.Accepted:
            QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)

            settings = dialog.get_settings()
            settings["saturation_value"] = int(saturation_value)
            
            if sensor_thickness is not None:
                # Convert meters to mm if it looks like meters (XDS expects mm)
                # Usually HDF5 is in meters (e.g. 0.00045) -> 0.45 mm
                # If it's already > 0.1, assume mm? 
                # Better to be consistent with run_nxds_merge.py: Assume HDF5 is meters.
                settings["sensor_thickness"] = float(sensor_thickness) * 1000.0

            worker = NXDSMergingWorker(dataset_paths, self.redis_conn, settings)
            worker.signals.progress.connect(
                lambda msg: self.ui_manager.show_status_message(f"Pipeline: {msg}", 0)
            )
            worker.signals.finished.connect(self._on_merging_finished)
            worker.signals.error.connect(self._on_merging_error)
            worker.signals.show_lp_file.connect(self._show_lp_viewer)
            self.threadpool.start(worker)

    def run_merging_non_interactive(self, dataset_paths: list, settings: dict):
        """
        Runs the merging pipeline without showing a user dialog.
        Used for automated workflows like auto-merging.
        """
        if not self.redis_conn:
            self.ui_manager.show_warning_message(
                "Redis Error",
                "Analysis Redis connection is not available for auto-merge.",
            )
            return

        saturation_value = None
        sensor_thickness = None
        if dataset_paths:
            reader = self.dataset_manager.get_reader(dataset_paths[0])
            if reader:
                params = reader.get_parameters()
                saturation_value = params.get("saturation_value")
                sensor_thickness = params.get("sensor_thickness")

        if saturation_value is None:
            # In an auto-run, just log a warning and proceed with a default
            logger.warning(
                "Could not retrieve 'saturation_value' for auto-merge. Using default."
            )
            saturation_value = 60000

        # We already have the settings, so we can skip the dialog.
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)

        # Add the determined saturation value to the settings dictionary
        settings["saturation_value"] = int(saturation_value)
        if sensor_thickness is not None:
             settings["sensor_thickness"] = float(sensor_thickness) * 1000.0

        worker = NXDSMergingWorker(dataset_paths, self.redis_conn, settings)
        worker.signals.progress.connect(
            lambda msg: self.ui_manager.show_status_message(
                f"Auto-Merge Pipeline: {msg}", 0
            )
        )
        worker.signals.finished.connect(self._on_merging_finished)
        worker.signals.error.connect(self._on_merging_error)
        worker.signals.show_lp_file.connect(self._show_lp_viewer)
        self.threadpool.start(worker)

    def _on_merging_finished(self, msg):
        QtWidgets.QApplication.restoreOverrideCursor()
        self.ui_manager.show_information_message("Pipeline Complete", msg)

    def _on_merging_error(self, err):
        QtWidgets.QApplication.restoreOverrideCursor()
        self.ui_manager.show_critical_message("Pipeline Error", err)

    def _show_lp_viewer(self, content):
        if self.lp_viewer is None:
            self.lp_viewer = LPViewerDialog(self.main_window)
        self.lp_viewer.set_content(content)


class _OrientationWorkerSignals(QtCore.QObject):
    finished = QtCore.pyqtSignal(str)   # png_path
    error = QtCore.pyqtSignal(str)      # error message


class _OrientationAnalysisWorker(QtCore.QRunnable):
    """Runs nxds_orientation_analysis.py as a subprocess in a background thread."""

    def __init__(self, cmd, output_plot_path):
        super().__init__()
        self.cmd = cmd
        self.output_plot_path = output_plot_path
        self.signals = _OrientationWorkerSignals()

    def run(self):
        try:
            start_time = time.time()
            logger.info("Starting orientation analysis subprocess...")
            
            result = subprocess.run(
                self.cmd,
                capture_output=True,
                text=True,
                timeout=14400, # 4 hours
            )
            
            duration = time.time() - start_time
            logger.info(f"Orientation analysis subprocess finished in {duration:.2f}s")
            
            if result.returncode != 0:
                logger.error(f"Orientation analysis failed (code {result.returncode})")
                logger.error(f"STDOUT:\n{result.stdout}")
                logger.error(f"STDERR:\n{result.stderr}")
                
                self.signals.error.emit(
                    f"Script exited with code {result.returncode}.\n"
                    f"See log for full output.\n"
                    f"stderr tail:\n{result.stderr[-2000:] if result.stderr else '(empty)'}"
                )
                return

            if not os.path.isfile(self.output_plot_path):
                logger.error("Orientation analysis output plot not found.")
                logger.error(f"STDOUT:\n{result.stdout}")
                logger.error(f"STDERR:\n{result.stderr}")
                
                self.signals.error.emit(
                    f"Analysis completed but output plot was not created.\n\n"
                    f"stdout tail:\n{result.stdout[-2000:] if result.stdout else '(empty)'}"
                )
                return

            self.signals.finished.emit(self.output_plot_path)

        except subprocess.TimeoutExpired:
            logger.error("Orientation analysis timed out (>4 hours)")
            self.signals.error.emit("Orientation analysis timed out (>4 hours).")
        except Exception as e:
            logger.error(f"Orientation analysis unexpected error: {e}", exc_info=True)
            self.signals.error.emit(f"Unexpected error: {e}")


class _OrientationResultDialog(QtWidgets.QDialog):
    """Dialog to display the orientation analysis PNG result."""

    def __init__(self, png_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Crystal Orientation Analysis")
        self.setMinimumSize(900, 700)
        self.setAttribute(QtCore.Qt.WA_DeleteOnClose)

        layout = QtWidgets.QVBoxLayout(self)

        # Scrollable image display
        scroll_area = QtWidgets.QScrollArea()
        scroll_area.setWidgetResizable(True)
        label = QtWidgets.QLabel()
        pixmap = QtGui.QPixmap(png_path)
        if not pixmap.isNull():
            label.setPixmap(pixmap)
        else:
            label.setText(f"Could not load image: {png_path}")
        label.setAlignment(QtCore.Qt.AlignCenter)
        scroll_area.setWidget(label)
        layout.addWidget(scroll_area)

        # Info label
        info_label = QtWidgets.QLabel(f"Plot: {png_path}")
        info_label.setStyleSheet("color: grey; font-size: 10px;")
        layout.addWidget(info_label)

        button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Close)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

