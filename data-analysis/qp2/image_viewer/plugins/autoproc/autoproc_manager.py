# qp2/image_viewer/plugins/autoproc/autoproc_manager.py
import json
import os
import webbrowser

from pathlib import Path
from PyQt5 import QtCore, QtWidgets
from qp2.image_viewer.plugins.generic_plot_manager import GenericPlotManager
from qp2.image_viewer.plugins.autoproc.submit_autoproc_job import (
    AutoPROCProcessDatasetWorker,
)
from qp2.image_viewer.plugins.autoproc.autoproc_settings_dialog import (
    AutoPROCSettingsDialog,
)
from qp2.image_viewer.utils.run_job import run_command
from qp2.config.programs import ProgramConfig


class AutoPROCManager(GenericPlotManager):
    def __init__(self, parent):
        config = {
            "worker_class": AutoPROCProcessDatasetWorker,
            "redis_connection": parent.redis_output_server,
            "redis_key_template": "analysis:out:autoproc:{master_file}",
            "spot_field_key": None,  # No per-frame spots
            "default_source_type": "redis",
            "status_key_type": "string",
        }
        super().__init__(parent=parent, name="autoPROC", config=config)
        self.processed_datasets = set()

    def _setup_ui(self):
        super()._setup_ui()
        # Hide plot-related widgets as they are not used
        self.plot_widget.hide()
        self.metric_combobox.hide()
        self.container_widget.layout().itemAt(0).layout().itemAt(
            3
        ).widget().hide()  # Hide Y-Axis label

        # Add a settings button
        self.settings_button = QtWidgets.QPushButton("⚙️ Settings")
        self.settings_button.setToolTip("Open autoPROC Settings")
        self.settings_button.clicked.connect(self._open_settings_dialog)

        self.results_button = QtWidgets.QToolButton()
        self.results_button.setText("📊 View Report")
        self.results_button.setToolTip("View processing results")
        self.results_button.setPopupMode(QtWidgets.QToolButton.InstantPopup)
        self.results_button.setEnabled(False)

        # Create the menu and actions
        results_menu = QtWidgets.QMenu(self.results_button)
        self.html_report_action = results_menu.addAction("Open HTML Report")
        self.text_summary_action = results_menu.addAction("View Text Summary")

        # Connect actions
        self.html_report_action.triggered.connect(self._show_html_report)
        self.text_summary_action.triggered.connect(self._show_text_summary)

        self.results_button.setMenu(results_menu)

        self.coot_button = QtWidgets.QPushButton("Launch Coot")
        self.coot_button.setToolTip("Launch Coot with Dimple results (if available)")
        self.coot_button.clicked.connect(self._launch_coot)
        self.coot_button.setEnabled(False)  # Disabled by default

        # Insert buttons into the control bar
        control_bar = self.container_widget.layout().itemAt(0).layout()
        actions_index = control_bar.indexOf(self.actions_button)
        control_bar.insertWidget(actions_index, self.settings_button)
        control_bar.insertWidget(2, self.results_button)
        control_bar.insertWidget(3, self.coot_button)

    def _open_settings_dialog(self):
        dialog = AutoPROCSettingsDialog(
            self.main_window.settings_manager.as_dict(), self.main_window
        )
        dialog.settings_changed.connect(
            self.main_window.settings_manager.update_from_dict
        )
        dialog.show()

    def _prepare_worker_kwargs(self) -> dict:
        settings = self.main_window.settings_manager
        kwargs = {
            key: settings.get(key)
            for key in settings.as_dict()
            if key.startswith("autoproc_")
        }
        
        # Fallback to common settings
        if not kwargs.get("autoproc_space_group"):
            kwargs["autoproc_space_group"] = settings.get("processing_common_space_group", "")
            
        if not kwargs.get("autoproc_unit_cell"):
            kwargs["autoproc_unit_cell"] = settings.get("processing_common_unit_cell", "")
            
        if not kwargs.get("autoproc_model"):
            kwargs["autoproc_model"] = settings.get("processing_common_model_file", "")
            
        if not kwargs.get("autoproc_highres"):
            common_high = settings.get("processing_common_res_cutoff_high")
            if common_high is not None:
                kwargs["autoproc_highres"] = common_high

        kwargs["processing_common_proc_dir_root"] = settings.get("processing_common_proc_dir_root", "")

        return kwargs

    def _fetch_and_prepare_data(self) -> bool:
        """
        Check if results files exist to enable the results and Coot buttons.
        """
        if not self.redis_connection or not self.current_master_file:
            self.results_button.setEnabled(False)
            self.coot_button.setEnabled(False)  # +++ ADDED
            return False

        key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(key, "_proc_dir")

        html_report_found = False
        json_summary_found = False
        coot_files_found = False

        if proc_dir_str:
            proc_dir = Path(proc_dir_str)
            # Check for HTML report
            html_report_path = proc_dir / "summary.html"
            if html_report_path.exists():
                html_report_found = True

            json_summary_path = proc_dir / "autoPROC_results.json"
            if json_summary_path.exists():
                json_summary_found = True
                try:
                    with open(json_summary_path, "r") as f:
                        results_data = json.load(f)
                    final_pdb = results_data.get("dimple_pdb")
                    final_mtz = results_data.get("dimple_mtz")
                    if (
                        final_pdb
                        and final_mtz
                        and os.path.exists(final_pdb)
                        and os.path.exists(final_mtz)
                    ):
                        coot_files_found = True
                except (json.JSONDecodeError, IOError):
                    pass  # Ignore errors, button will just remain disabled

        self.results_button.setEnabled(html_report_found or json_summary_found)
        self.html_report_action.setEnabled(html_report_found)
        self.text_summary_action.setEnabled(json_summary_found)
        self.coot_button.setEnabled(coot_files_found)

        # Return True if either button state might have changed
        return html_report_found or json_summary_found or coot_files_found

    def _show_html_report(self):
        """
        MODIFIED: Opens the summary.html file in the default web browser.
        """
        key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(key, "_proc_dir")
        if not proc_dir_str:
            return

        html_report_path = Path(proc_dir_str) / "summary.html"
        if not html_report_path.exists():
            self.main_window.ui_manager.show_warning_message(
                "File Not Found", f"Could not find summary.html at:\n{html_report_path}"
            )
            return

        # Use webbrowser to open the file
        try:
            webbrowser.open(
                html_report_path.as_uri()
            )  # as_uri() creates a file:/// URL
            self.main_window.ui_manager.show_status_message(
                "Opening HTML report in browser...", 3000
            )
        except Exception as e:
            self.main_window.ui_manager.show_critical_message(
                "Error Opening Report", f"Failed to open report:\n{e}"
            )

    def _launch_coot(self):
        """Launches Coot with the final PDB and MTZ from the Dimple run."""
        key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(key, "_proc_dir")
        if not proc_dir_str:
            return

        json_summary_path = Path(proc_dir_str) / "autoPROC_results.json"
        if not json_summary_path.exists():
            self.main_window.ui_manager.show_warning_message(
                "File Not Found", "Cannot launch Coot without results summary."
            )
            return

        try:
            with open(json_summary_path, "r") as f:
                results_data = json.load(f)

            final_pdb = results_data.get("dimple_pdb")
            final_mtz = results_data.get("dimple_mtz")

            if not (
                final_pdb
                and final_mtz
                and os.path.exists(final_pdb)
                and os.path.exists(final_mtz)
            ):
                raise FileNotFoundError(
                    "final.pdb or final.mtz not found in results or on disk."
                )

            self.main_window.ui_manager.show_status_message("Launching Coot...", 0)

            pre_command_str = ProgramConfig.get_setup_command('ccp4')

            # The command needs to run from the processing directory for Coot to find the files
            run_command(
                cmd=["coot", "--pdb", final_pdb, "--auto", final_mtz],
                cwd=proc_dir_str,
                method="shell",
                job_name="coot_launcher",
                background=True,  # Launch and detach
                pre_command=pre_command_str
            )
        except (json.JSONDecodeError, FileNotFoundError, Exception) as e:
            self.main_window.ui_manager.show_critical_message(
                "Coot Launch Failed", f"An error occurred:\n{e}"
            )
        finally:
            self.main_window.ui_manager.clear_status_message_if("Launching Coot")

    def _show_text_summary(self):
        """Opens a dialog with a formatted text summary from the JSON results."""
        key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(key, "_proc_dir")
        if not proc_dir_str:
            return

        json_summary_path = Path(proc_dir_str) / "autoPROC_results.json"
        if not json_summary_path.exists():
            self.main_window.ui_manager.show_warning_message(
                "File Not Found",
                "Results summary file (autoPROC_results.json) not found.",
            )
            return

        try:
            with open(json_summary_path, "r") as f:
                data = json.load(f)

            # Format the data into a readable string
            summary_text = self._format_summary_text(data)

            # Display in a dialog
            dialog = QtWidgets.QDialog(self.main_window)
            dialog.setWindowTitle("autoPROC Text Summary")
            dialog.setMinimumSize(700, 600)
            layout = QtWidgets.QVBoxLayout(dialog)
            text_edit = QtWidgets.QTextEdit()
            text_edit.setReadOnly(True)
            text_edit.setFontFamily("Monospace")
            text_edit.setText(summary_text)
            layout.addWidget(text_edit)
            dialog.exec_()

        except (json.JSONDecodeError, Exception) as e:
            self.main_window.ui_manager.show_critical_message(
                "Error", f"Failed to read or parse results file:\n{e}"
            )

    def _format_summary_text(self, data: dict) -> str:
        """Formats the JSON data into a human-readable text block."""
        lines = []
        lines.append(f"--- autoPROC Summary ---")
        lines.append(f"Space Group: {data.get('spacegroup', 'N/A')}")
        lines.append(f"Unit Cell:   {data.get('unitcell', 'N/A')}")
        lines.append("-" * 25)

        lines.append(f"{'Parameter':<25} {'Overall':>10} {'Inner':>10} {'Outer':>10}")
        lines.append(f"{'-'*25:<25} {'-'*10:>10} {'-'*10:>10} {'-'*10:>10}")

        param_map = {
            "Resolution (Å)": ("highresolution", "lowresolution"),
            "Rmerge": "rmerge",
            "Rmeas": "rmeas",
            "Rpim": "rpim",
            "I/sig(I)": "isigmai",
            "Completeness (%)": "completeness",
            "Multiplicity": "multiplicity",
            "CC(1/2)": "cchalf",
        }

        for name, key_or_tuple in param_map.items():
            if isinstance(key_or_tuple, tuple):  # Special case for resolution range
                val = f"{data.get(key_or_tuple[1], ''):>10} - {data.get(key_or_tuple[0], ''):<10}"
                lines.append(f"{name:<25} {val}")
            else:
                key = key_or_tuple
                overall = data.get(key, " ")
                inner = data.get(f"{key}_inner", " ")
                outer = data.get(f"{key}_outer", " ")
                lines.append(f"{name:<25} {overall:>10} {inner:>10} {outer:>10}")

        lines.append("\n--- Dimple Post-Processing ---")
        if data.get("dimple_pdb"):
            lines.append(f"Final PDB:  {data.get('dimple_pdb')}")
            lines.append(f"Final MTZ:  {data.get('dimple_mtz')}")
            lines.append(f"Final R-free: {data.get('dimple_r_free', 'N/A')}")
        else:
            lines.append("Dimple was not run or did not succeed.")

        return "\n".join(lines)

    @QtCore.pyqtSlot(str, int, dict)
    def handle_dataset_completed(self, master_file_path, total_frames, metadata):
        """Launches the single processing job for the whole dataset."""
        worker_class = self.config.get("worker_class")
        if not worker_class or not self.run_processing_enabled:
            return
        if metadata.get("collect_mode", "STANDARD").upper() in ("RASTER", "STRATEGY"):
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

    def update_source(self, new_reader, new_master_file):
        """Overrides GenericPlotManager to connect to series_completed signal."""
        if self.reader and hasattr(self.reader, "series_completed"):
            try:
                self.reader.series_completed.disconnect(self.handle_dataset_completed)
            except (TypeError, RuntimeError):
                pass

        # Call the base class method to handle the basic setup
        super().update_source(new_reader, new_master_file)

        if self.reader and hasattr(self.reader, "series_completed"):
            self.reader.series_completed.connect(self.handle_dataset_completed)
            if self.reader.series_completion_signal_emitted and self.main_window.is_live_mode:
                self.handle_dataset_completed(
                    self.reader.master_file_path,
                    self.reader.total_frames,
                    self.reader.get_parameters(),
                )

    @QtCore.pyqtSlot()
    def _rerun_analysis(self, reader=None, master_file=None):
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
        """Override to do nothing, as this is a per-dataset plugin."""
        pass
