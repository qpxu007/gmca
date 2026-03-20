# qp2/image_viewer/plugins/xia2_ssx/xia2_ssx_manager.py
import json
import os
import webbrowser
from pathlib import Path
from typing import Optional
from PyQt5 import QtCore, QtWidgets
from qp2.image_viewer.plugins.generic_plot_manager import GenericPlotManager
from qp2.image_viewer.plugins.xia2_ssx.submit_xia2_ssx_job import Xia2SSXProcessDatasetWorker
from qp2.image_viewer.plugins.xia2_ssx.xia2_ssx_settings_dialog import Xia2SSXSettingsDialog
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

class Xia2SSXManager(GenericPlotManager):
    def __init__(self, parent):
        config = {
            "worker_class": Xia2SSXProcessDatasetWorker,
            "redis_connection": parent.redis_output_server,
            "redis_key_template": "analysis:out:xia2_ssx:{master_file}",
            "spot_field_key": None,  # No per-frame data for xia2
            "default_source_type": "redis",
            "status_key_type": "string",
        }
        super().__init__(parent=parent, name="xia2_ssx", config=config)
        self.processed_datasets = set()

    def _setup_ui(self):
        super()._setup_ui()
        # Hide plot-related widgets as they are not used by this manager
        self.plot_widget.hide()
        self.metric_combobox.hide()
        control_bar_layout = self.container_widget.layout().itemAt(0).layout()
        for i in range(control_bar_layout.count()):
            widget = control_bar_layout.itemAt(i).widget()
            if isinstance(widget, QtWidgets.QLabel) and widget.text() == "Y-Axis:":
                widget.hide()
                break

        # Add a settings button for xia2-specific parameters
        self.settings_button = QtWidgets.QPushButton("⚙️ Settings")
        self.settings_button.setToolTip("Open xia2.ssx Settings")
        self.settings_button.clicked.connect(self._open_settings_dialog)

        # Create a tool button for viewing different types of reports
        self.results_button = QtWidgets.QToolButton()
        self.results_button.setText("📊 View Report")
        self.results_button.setToolTip("View processing results")
        self.results_button.setPopupMode(QtWidgets.QToolButton.InstantPopup)
        self.results_button.setEnabled(False)

        # Create the dropdown menu for the results button
        results_menu = QtWidgets.QMenu(self.results_button)
        self.html_report_action = results_menu.addAction("Open HTML Report")
        self.text_summary_action = results_menu.addAction("View Text Summary")
        self.html_report_action.triggered.connect(self._show_html_report)
        self.text_summary_action.triggered.connect(self._show_text_summary)
        self.results_button.setMenu(results_menu)

        # Insert all custom buttons into the control bar
        control_bar = self.container_widget.layout().itemAt(0).layout()
        actions_index = control_bar.indexOf(self.actions_button)
        control_bar.insertWidget(actions_index, self.settings_button)
        control_bar.insertWidget(2, self.results_button)

    def _open_settings_dialog(self):
        """Opens the settings dialog for xia2.ssx."""
        dialog = Xia2SSXSettingsDialog(
            self.main_window.settings_manager.as_dict(), self.main_window
        )
        dialog.settings_changed.connect(
            self.main_window.settings_manager.update_from_dict
        )
        dialog.show()

    def _prepare_worker_kwargs(self) -> dict:
        """Gathers all xia2-specific settings to pass to the worker."""
        settings = self.main_window.settings_manager
        kwargs = {
            key: settings.get(key)
            for key in settings.as_dict()
            if key.startswith("xia2_ssx_")
        }
        
        # Fallback to common settings
        if not kwargs.get("xia2_ssx_space_group"):
            kwargs["xia2_ssx_space_group"] = settings.get("processing_common_space_group", "")
            
        if not kwargs.get("xia2_ssx_unit_cell"):
            kwargs["xia2_ssx_unit_cell"] = settings.get("processing_common_unit_cell", "")
            
        if not kwargs.get("xia2_ssx_model"):
            kwargs["xia2_ssx_model"] = settings.get("processing_common_model_file", "")
            
        if not kwargs.get("xia2_ssx_reference_hkl"):
            kwargs["xia2_ssx_reference_hkl"] = settings.get("processing_common_reference_reflection_file", "")
            
        if not kwargs.get("xia2_ssx_d_min") and settings.get("processing_common_res_cutoff_high"):
            kwargs["xia2_ssx_d_min"] = settings.get("processing_common_res_cutoff_high")

        if not kwargs.get("xia2_ssx_d_max") and settings.get("processing_common_res_cutoff_low"):
            kwargs["xia2_ssx_d_max"] = settings.get("processing_common_res_cutoff_low")

        if "xia2_ssx_native" not in kwargs:
            kwargs["xia2_ssx_native"] = settings.get("processing_common_native", True)
            
        kwargs["processing_common_proc_dir_root"] = settings.get("processing_common_proc_dir_root", "")

        return kwargs

    def _fetch_and_prepare_data(self) -> bool:
        """Checks for the existence of result files to enable/disable UI buttons."""
        if not self.redis_connection or not self.current_master_file:
            self.results_button.setEnabled(False)
            return False

        key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(key, "_proc_dir")

        html_found, text_summary_found = False, False
        if proc_dir_str:
            proc_dir = Path(proc_dir_str)
            if (proc_dir / "xia2.html").exists():
                html_found = True

            # Check for typical log files
            if (proc_dir / "xia2.txt").exists():
                text_summary_found = True
            elif (proc_dir / "xia2.ssx.log").exists():
                text_summary_found = True
            # Check for slurm output log as fallback
            elif list(proc_dir.glob("xia2_ssx_*.out")):
                text_summary_found = True

        self.results_button.setEnabled(html_found or text_summary_found)
        self.html_report_action.setEnabled(html_found)
        self.text_summary_action.setEnabled(text_summary_found)
        return html_found or text_summary_found

    def _show_html_report(self):
        """Opens the main xia2.html report in a web browser."""
        key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(key, "_proc_dir")
        if not proc_dir_str:
            return
        html_path = Path(proc_dir_str) / "xia2.html"
        if html_path.exists():
            webbrowser.open(html_path.as_uri())
        else:
            self.main_window.ui_manager.show_warning_message(
                "Not Found", "xia2.html not found."
            )

    def _show_text_summary(self):
        """Opens a dialog and displays the raw content of xia2.txt or log."""
        key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(key, "_proc_dir")
        if not proc_dir_str:
            return

        proc_dir = Path(proc_dir_str)
        summary_path = proc_dir / "xia2.txt"
        if not summary_path.exists():
            summary_path = proc_dir / "xia2.ssx.log"
        
        if not summary_path.exists():
             # Try finding any .out file
            logs = list(proc_dir.glob("xia2_ssx_*.out"))
            if logs:
                summary_path = logs[0]

        if not summary_path.exists():
            self.main_window.ui_manager.show_warning_message(
                "Not Found", f"Log file not found in {proc_dir}"
            )
            return

        try:
            with open(summary_path, "r") as f:
                summary_text = f.read()

            dialog = QtWidgets.QDialog(self.main_window)
            dialog.setWindowTitle(f"Log: {summary_path.name}")
            dialog.setMinimumSize(800, 600)
            layout = QtWidgets.QVBoxLayout(dialog)
            text_edit = QtWidgets.QTextEdit()
            text_edit.setReadOnly(True)
            text_edit.setFontFamily("Monospace")

            text_edit.setPlainText(summary_text)

            layout.addWidget(text_edit)
            dialog.exec_()
        except Exception as e:
            self.main_window.ui_manager.show_critical_message(
                "Error", f"Could not display summary file: {e}"
            )

    def update_source(self, new_reader, new_master_file):
        """Overrides GenericPlotManager to connect to the series_completed signal for per-dataset jobs."""
        if self.reader and hasattr(self.reader, "series_completed"):
            try:
                self.reader.series_completed.disconnect(self.handle_dataset_completed)
            except (TypeError, RuntimeError):
                pass

        super().update_source(new_reader, new_master_file)

        if self.reader and hasattr(self.reader, "series_completed"):
            self.reader.series_completed.connect(self.handle_dataset_completed)
            if self.reader.series_completion_signal_emitted and self.main_window.is_live_mode:
                self.handle_dataset_completed(
                    self.reader.master_file_path,
                    self.reader.total_frames,
                    self.reader.get_parameters(),
                )

    @QtCore.pyqtSlot(str, int, dict)
    def handle_dataset_completed(self, master_file_path, total_frames, metadata):
        """Launches the single processing job for the whole dataset when it's ready."""
        worker_class = self.config.get("worker_class")
        if not worker_class or not self.run_processing_enabled:
            return
        if master_file_path in self.processed_datasets:
            return

        self.processed_datasets.add(master_file_path)
        worker_kwargs = self._prepare_worker_kwargs()
        worker = worker_class(
            master_file=master_file_path,
            metadata=metadata,
            redis_conn=self.redis_connection,
            redis_key_prefix=self.config.get("redis_key_template", "").split(
                ":{master_file}"
            )[0],
            **worker_kwargs,
        )
        worker.signals.error.connect(self._handle_worker_error)
        worker.signals.result.connect(
            lambda status, msg, path: self._handle_worker_result(path, status, msg)
        )
        self.request_main_threadpool.emit(worker)

    @QtCore.pyqtSlot()
    def _rerun_analysis(self, reader=None, master_file=None):
        """Triggers a re-run for the currently loaded dataset."""
        # Use passed args or fallback to current state
        target_reader = reader if reader else self.reader
        # target_master_file = master_file if master_file else self.current_master_file

        if not target_reader:
            return
        
        self.status_update.emit(f"[{self.name}] Re-running analysis...", 3000)
        
        if target_reader.master_file_path in self.processed_datasets:
            self.processed_datasets.remove(target_reader.master_file_path)
            
        self.handle_dataset_completed(
            target_reader.master_file_path,
            target_reader.total_frames,
            target_reader.get_parameters(),
        )

    @QtCore.pyqtSlot(list)
    def handle_data_files_ready(self, files_batch: list):
        """Override to do nothing, preventing the base class from running per-file jobs."""
        pass
