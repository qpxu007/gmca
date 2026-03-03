# qp2/image_viewer/plugins/xia2/xia2_manager.py
import json
import os
import webbrowser
from pathlib import Path
from typing import Optional
from PyQt5 import QtCore, QtWidgets
from qp2.image_viewer.plugins.generic_plot_manager import GenericPlotManager
from qp2.image_viewer.plugins.xia2.submit_xia2_job import Xia2ProcessDatasetWorker
from qp2.image_viewer.plugins.xia2.xia2_settings_dialog import Xia2SettingsDialog
from qp2.config.programs import ProgramConfig


class Xia2Manager(GenericPlotManager):
    def __init__(self, parent):
        config = {
            "worker_class": Xia2ProcessDatasetWorker,
            "redis_connection": parent.redis_output_server,
            "redis_key_template": "analysis:out:xia2:{master_file}",
            "spot_field_key": None,  # No per-frame data for xia2
            "default_source_type": "redis",
            "status_key_type": "string",
        }
        super().__init__(parent=parent, name="xia2", config=config)
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
        self.settings_button.setToolTip("Open xia2 Settings")
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

        # Add a button to launch Coot with post-processing results
        self.coot_button = QtWidgets.QPushButton("Launch Coot")
        self.coot_button.setToolTip("Launch Coot with Dimple results (if available)")
        self.coot_button.clicked.connect(self._launch_coot)
        self.coot_button.setEnabled(False)

        # Insert all custom buttons into the control bar
        control_bar = self.container_widget.layout().itemAt(0).layout()
        actions_index = control_bar.indexOf(self.actions_button)
        control_bar.insertWidget(actions_index, self.settings_button)
        control_bar.insertWidget(2, self.results_button)
        control_bar.insertWidget(3, self.coot_button)

    def _open_settings_dialog(self):
        """Opens the settings dialog for xia2."""
        dialog = Xia2SettingsDialog(
            self.main_window.settings_manager.as_dict(), self.main_window
        )
        dialog.settings_changed.connect(
            self.main_window.settings_manager.update_from_dict
        )
        dialog.show()

    def _prepare_worker_kwargs(self) -> dict:
        """Gathers all xia2-specific settings to pass to the worker."""
        settings = self.main_window.settings_manager
        
        # 1. Gather xia2 specific settings
        kwargs = {
            key: settings.get(key)
            for key in settings.as_dict()
            if key.startswith("xia2_")
        }
        
        # 2. Implement fallback logic
        if not kwargs.get("xia2_space_group"):
            kwargs["xia2_space_group"] = settings.get("processing_common_space_group", "")
        
        if not kwargs.get("xia2_unit_cell"):
            kwargs["xia2_unit_cell"] = settings.get("processing_common_unit_cell", "")
            
        if not kwargs.get("xia2_model"):
             kwargs["xia2_model"] = settings.get("processing_common_model_file", "")
             
        if not kwargs.get("xia2_highres"):
            common_high = settings.get("processing_common_res_cutoff_high")
            if common_high is not None:
                kwargs["xia2_highres"] = common_high

        kwargs["processing_common_proc_dir_root"] = settings.get("processing_common_proc_dir_root", "")

        return kwargs

    def _fetch_and_prepare_data(self) -> bool:
        """Checks for the existence of result files to enable/disable UI buttons."""
        if not self.redis_connection or not self.current_master_file:
            self.results_button.setEnabled(False)
            self.coot_button.setEnabled(False)
            return False

        key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(key, "_proc_dir")

        html_found, text_summary_found, coot_found = False, False, False
        if proc_dir_str:
            proc_dir = Path(proc_dir_str)
            if (proc_dir / "xia2.html").exists():
                html_found = True

            if (proc_dir / "xia2.txt").exists():
                text_summary_found = True

            json_path = self._find_results_json(proc_dir)
            if json_path and json_path.exists():
                try:
                    with open(json_path, "r") as f:
                        data = json.load(f)
                    pdb, mtz = data.get("dimple_pdb"), data.get("dimple_mtz")
                    if pdb and mtz and os.path.exists(pdb) and os.path.exists(mtz):
                        coot_found = True
                except (json.JSONDecodeError, IOError):
                    pass

        self.results_button.setEnabled(html_found or text_summary_found)
        self.html_report_action.setEnabled(html_found)
        self.text_summary_action.setEnabled(text_summary_found)
        self.coot_button.setEnabled(coot_found)
        return html_found or text_summary_found or coot_found

    def _find_results_json(self, proc_dir: Path) -> Optional[Path]:
        """Finds any of the possible xia2 results JSON files (needed for Coot)."""
        possible_names = [
            "xia2_dials_results.json",
            "xia2_dials_aimless_results.json",
            "xia2_xds_results.json",
        ]
        for name in possible_names:
            path = proc_dir / name
            if path.exists():
                return path
        return None

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
        """Opens a dialog and displays the raw content of xia2.txt."""
        key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(key, "_proc_dir")
        if not proc_dir_str:
            return

        summary_path = Path(proc_dir_str) / "xia2.txt"

        if not summary_path.exists():
            self.main_window.ui_manager.show_warning_message(
                "Not Found", f"{summary_path.name} not found."
            )
            return

        try:
            with open(summary_path, "r") as f:
                summary_text = f.read()

            dialog = QtWidgets.QDialog(self.main_window)
            dialog.setWindowTitle("xia2 Log Summary")
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

    def _launch_coot(self):
        """Launches Coot with the final PDB and MTZ from the Dimple run."""
        key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(key, "_proc_dir")
        if not proc_dir_str:
            return

        json_path = self._find_results_json(Path(proc_dir_str))
        if not json_path:
            self.main_window.ui_manager.show_warning_message(
                "File Not Found", "Cannot launch Coot without results JSON."
            )
            return

        try:
            with open(json_path, "r") as f:
                data = json.load(f)
            pdb, mtz = data.get("dimple_pdb"), data.get("dimple_mtz")
            if not (pdb and mtz and os.path.exists(pdb) and os.path.exists(mtz)):
                raise FileNotFoundError(
                    "final.pdb or final.mtz not found in results or on disk."
                )

            from qp2.image_viewer.utils.run_job import run_command

            self.main_window.ui_manager.show_status_message("Launching Coot...", 0)
            run_command(
                cmd=["coot", "--pdb", pdb, "--auto", mtz],
                cwd=proc_dir_str,
                method="shell",
                job_name="coot_launcher",
                background=True,
                pre_command=ProgramConfig.get_setup_command('ccp4'),
            )
        except (json.JSONDecodeError, FileNotFoundError, Exception) as e:
            self.main_window.ui_manager.show_critical_message(
                "Coot Launch Failed", f"An error occurred:\n{e}"
            )
        finally:
            self.main_window.ui_manager.clear_status_message_if("Launching Coot")

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
        if metadata.get("collect_mode", "STANDARD").upper() in ("RASTER", "STRATEGY"):
            return
        if master_file_path in self.processed_datasets:
            return

        self.processed_datasets.add(master_file_path)
        worker_kwargs = self._prepare_worker_kwargs()
        print("=x=x=", worker_kwargs)
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
        worker.signals.result.connect(self._handle_worker_result)
        self.request_main_threadpool.emit(worker)

    @QtCore.pyqtSlot()
    def _rerun_analysis(self, reader=None, master_file=None):
        """Triggers a re-run for the currently loaded dataset."""
        # Use passed args or fallback to current state
        target_reader = reader if reader else self.reader
        # target_master_file = master_file if master_file else self.current_master_file (not needed explicitly if reader has it)

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
