# qp2/image_viewer/plugins/xds/xds_results_dialog.py
import json
import os
import subprocess
from pathlib import Path
import webbrowser

from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QDialog,
    QTabWidget,
    QWidget,
    QVBoxLayout,
    QTextEdit,
    QPushButton,
    QMessageBox,
    QScrollArea,
    QMenu,
    QHBoxLayout,
)

from qp2.config.programs import ProgramConfig


class LogViewerDialog(QDialog):
    """A simple dialog to display the content of a log file."""

    def __init__(self, file_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle(os.path.basename(file_path))
        self.setMinimumSize(1100, 700)

        layout = QVBoxLayout(self)
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setFont(QFont("Monospace", 9))

        try:
            with open(file_path, "r", errors="ignore") as f:
                text_edit.setText(f.read())
        except IOError as e:
            text_edit.setText(f"Error reading file:\n{e}")

        layout.addWidget(text_edit)


class XDSResultsDialog(QDialog):
    def __init__(self, stats_files, parent=None):
        super().__init__(parent)
        self.setWindowTitle("XDS Overall Statistics")
        self.setMinimumSize(1100, 500)

        self.layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        self.layout.addWidget(self.tabs)

        for file_path in stats_files:
            self._add_stats_tab(file_path)

    def format_results(self, stats_data, proc_dir):
        table1_text = stats_data.get("table1_text", "Not available.")
        index_table = "".join(stats_data.get("index_table_text", []))
        index_table_header = " ".join(stats_data.get("index_table_header", []))

        proc_dir_line = f"Processing Directory: {proc_dir}\n\n"

        return (
            f"{proc_dir_line}"
            f"===== Scaling Statistics (from CORRECT.LP) =====\n{table1_text}\n\n"
            f"===== Indexing Solutions (from IDXREF.LP) =====\n{index_table_header}\n{index_table}"
        )

    def _add_stats_tab(self, file_path):
        try:
            with open(file_path, "r") as f:
                stats_data = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            error_text = f"Error reading {os.path.basename(file_path)}:\n\n{e}"
            error_widget = QTextEdit(error_text)
            error_widget.setReadOnly(True)
            self.tabs.addTab(error_widget, os.path.basename(file_path))
            return

        tab_widget = QWidget()
        tab_layout = QVBoxLayout(tab_widget)

        # Use a scroll area for potentially long text
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)

        stats_text_edit = QTextEdit()
        stats_text_edit.setReadOnly(True)
        stats_text_edit.setFont(QFont("Monospace", 9))

        proc_dir = stats_data.get("proc_dir")

        formatted_results = self.format_results(stats_data, proc_dir)
        stats_text_edit.setText(formatted_results)
        scroll_area.setWidget(stats_text_edit)
        tab_layout.addWidget(scroll_area)

        if proc_dir and os.path.isdir(proc_dir):
            button_layout = QHBoxLayout()  # Create a horizontal layout for buttons
            button_layout.addStretch()  # Push buttons to the right

            master_file = stats_data.get("dataset", "")
            if master_file:
                prefix = Path(master_file).stem.replace("master", "")
                html_report_path = Path(proc_dir) / f"{prefix}.html"
                if html_report_path.exists():
                    report_button = QPushButton("View HTML Report")
                    report_button.clicked.connect(
                        lambda: self._view_html_report(html_report_path)
                    )
                    button_layout.addWidget(report_button)

            pdb_file = stats_data.get("final_pdb")
            mtz_file = stats_data.get("final_mtz")
            if (
                pdb_file
                and mtz_file
                and os.path.exists(pdb_file)
                and os.path.exists(mtz_file)
            ):
                coot_button = QPushButton("Launch Coot")
                coot_button.clicked.connect(
                    lambda: self._launch_coot(pdb_file, mtz_file)
                )
                button_layout.addWidget(coot_button)

            xdsgui_button = QPushButton("Launch xdsgui")
            xdsgui_button.clicked.connect(lambda: self._launch_xdsgui(proc_dir))

            view_log_button = QPushButton("View Log File...")
            view_log_button.clicked.connect(
                lambda: self._show_log_menu(view_log_button, proc_dir)
            )

            button_layout.addWidget(view_log_button)
            button_layout.addWidget(xdsgui_button)

            tab_layout.addLayout(
                button_layout
            )  # Add the button layout to the main tab layout

        self.tabs.addTab(tab_widget, os.path.basename(proc_dir or file_path))

    def _launch_coot(self, pdb_file, mtz_file):
        try:
            # Command to load CCP4 environment and then run coot.
            # This is more robust for systems using environment modules.
            command = f"{ProgramConfig.get_setup_command('ccp4')} && coot --pdb {pdb_file} --auto {mtz_file}"
            subprocess.Popen(
                ["bash", "-c", command],
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            QMessageBox.information(
                self, "Launched", f"Launched Coot with:\n{pdb_file}\n{mtz_file}"
            )
        except Exception as e:
            QMessageBox.critical(self, "Launch Error", f"Failed to launch Coot:\n{e}")

    def _view_html_report(self, report_path):
        try:
            webbrowser.open(report_path.as_uri())
        except Exception as e:
            QMessageBox.critical(
                self, "Error Opening Report", f"Could not open HTML report:\n{e}"
            )

    def _launch_xdsgui(self, proc_dir):
        try:
            # We use Popen to launch xdsgui as a detached process
            command = f"cd {proc_dir} && xdsgui"
            subprocess.Popen(command, shell=True, start_new_session=True)
            QMessageBox.information(
                self, "Launched", f"Launched 'xdsgui' in:\n{proc_dir}"
            )
        except Exception as e:
            QMessageBox.critical(self, "Launch Error", f"Failed to launch xdsgui:\n{e}")

    def _show_log_menu(self, button, proc_dir):
        """Creates and shows a dropdown menu with available log files."""
        log_menu = QMenu(self)
        proc_path = Path(proc_dir)

        log_files_to_check = ["IDXREF.LP", "INTEGRATE.LP", "CORRECT.LP"]
        found_logs = False

        for log_name in log_files_to_check:
            log_path = proc_path / log_name
            if log_path.exists():
                action = log_menu.addAction(log_name)
                action.triggered.connect(
                    lambda checked=False, p=log_path: self._view_lp_log(p)
                )
                found_logs = True

        if not found_logs:
            QMessageBox.information(
                self,
                "No Logs Found",
                f"Could not find any standard XDS log files in:\n{proc_dir}",
            )
            return

        # Show the menu below the button
        log_menu.exec_(button.mapToGlobal(button.rect().bottomLeft()))

    def _view_lp_log(self, file_path):
        """Opens a new dialog to show the content of the selected log file."""
        dialog = LogViewerDialog(file_path, self)
        dialog.exec_()