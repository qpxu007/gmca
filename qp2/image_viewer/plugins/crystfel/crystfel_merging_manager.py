# crystfel_merging_manager.py

import os
import subprocess

from PyQt5.QtCore import QObject, pyqtSlot, QTimer
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import QDialog, QVBoxLayout, QTextEdit

from qp2.image_viewer.plugins.crystfel.crystfel_merging_dialog import MergingDialog
from qp2.image_viewer.plugins.crystfel.crystfel_stream_manager import StreamManager
from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.log.logging_config import get_logger
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)

CRYSTFEL_ENV_COMMAND = ProgramConfig.get_setup_command('crystfel')


class MergingManager(QObject):
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.stream_manager = StreamManager(main_window)
        self.merging_dialog = None

        self.active_job_id = None
        self.active_job_method = None
        self.output_file_path = None
        self.last_file_pos = 0
        self.log_poll_timer = QTimer(self)
        self.log_poll_timer.setInterval(2000)
        self.log_poll_timer.timeout.connect(self._poll_log_file)

    @pyqtSlot(list)
    def launch_merging_tool(self, datasets: list = None):
        if not self.merging_dialog:
            self.merging_dialog = MergingDialog(None, parent=self.main_window)
            self.stream_manager.stream_updated.connect(self._on_stream_updated)
            self.merging_dialog.run_command_requested.connect(
                self._on_run_command_requested
            )
            self.merging_dialog.view_stream_requested.connect(
                self._on_view_stream_requested
            )
            self.merging_dialog.output_dir_changed.connect(self._on_output_dir_changed)
            self.merging_dialog.finished.connect(self._on_dialog_closed)

        initial_dir = self.merging_dialog.output_dir_input.text()
        self._on_output_dir_changed(initial_dir, datasets_to_monitor=datasets)

        self.merging_dialog.show()
        self.merging_dialog.raise_()
        self.merging_dialog.activateWindow()

    def _on_dialog_closed(self, result):
        self.stream_manager.stop_monitoring()
        self.log_poll_timer.stop()
        self.merging_dialog = None

    @pyqtSlot(int, int)
    def _on_stream_updated(self, total_chunks, new_chunks):
        if self.merging_dialog:
            self.merging_dialog.setWindowTitle(
                f"CrystFEL Merging Tools ({total_chunks} Segments)"
            )
            self.merging_dialog.update_stream_file_display(
                self.stream_manager.get_stream_file_path()
            )

    @pyqtSlot(str)
    def _on_output_dir_changed(self, new_dir: str, datasets_to_monitor: list = None):
        if datasets_to_monitor is None:
            datasets_to_monitor = self.stream_manager.monitored_datasets

        if self.stream_manager.set_stream_file_path(new_dir):
            self.merging_dialog.update_stream_file_display(
                self.stream_manager.get_stream_file_path()
            )
            self.stream_manager.start_monitoring(datasets_to_monitor)
        else:
            self.stream_manager.stop_monitoring()
            self.merging_dialog.update_stream_file_display("Invalid Output Directory")

    @pyqtSlot(str, list, str)
    def _on_run_command_requested(self, program: str, args: list, job_name: str):
        if self.log_poll_timer.isActive():
            self.main_window.ui_manager.show_warning_message(
                "Process Busy", "A job is already being monitored."
            )
            return

        output_dir = self.merging_dialog.output_dir_input.text()
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        command_to_run = f"{program} {' '.join(args)}"
        is_local_command = program in ["cell_explorer", "crystfel"]

        try:
            if is_local_command:
                self.merging_dialog.clear_log()
                self.merging_dialog.append_log(
                    f"Running local command: {command_to_run}"
                )
                self.main_window.ui_manager.show_status_message(
                    f"Launching {program}...", 0
                )

                is_background = True
                result = run_command(
                    cmd=command_to_run,
                    cwd=output_dir,
                    job_name=job_name,
                    background=is_background,
                    method="shell",
                    pre_command=CRYSTFEL_ENV_COMMAND,
                )
                if not is_background and result:
                    self.merging_dialog.append_log(result.stdout)
                    self.merging_dialog.append_log(
                        f"\n--- {program} finished with exit code {result.returncode} ---"
                    )
                self.main_window.ui_manager.clear_status_message_if(
                    f"Launching {program}..."
                )
            else:
                self.active_job_name = job_name
                self.output_file_path = os.path.join(output_dir, f"{job_name}.out")
                self.last_file_pos = 0
                if os.path.exists(self.output_file_path):
                    os.remove(self.output_file_path)
                self.active_job_method = "slurm" if is_sbatch_available() else "shell"
                self.merging_dialog.clear_log()
                self.merging_dialog.append_log(
                    f"Starting job: {job_name} via {self.active_job_method}\nOutput will be streamed from: {self.output_file_path}\n"
                )

                self.active_job_id = run_command(
                    cmd=command_to_run,
                    cwd=output_dir,
                    job_name=job_name,
                    background=True,
                    pre_command=CRYSTFEL_ENV_COMMAND,
                    method=self.active_job_method,
                )
                if not self.active_job_id:
                    raise RuntimeError(
                        "Job submission failed to return a valid ID or process."
                    )
                self.log_poll_timer.start()
                self.merging_dialog.set_process_running(True)
        except Exception as e:
            self.merging_dialog.append_log(f"\n--- FAILED TO LAUNCH JOB: {e} ---")
            if not is_local_command:
                self.merging_dialog.set_process_running(False)
                self.active_job_id = None

    def _poll_log_file(self):
        job_is_running = False
        if self.active_job_id:
            if self.active_job_method == "slurm":
                try:
                    result = subprocess.run(
                        ["squeue", "-j", str(self.active_job_id)],
                        capture_output=True,
                        text=True,
                    )
                    if str(self.active_job_id) in result.stdout:
                        job_is_running = True
                except FileNotFoundError:
                    job_is_running = True
            else:
                if self.active_job_id.poll() is None:
                    job_is_running = True

        if self.output_file_path and os.path.exists(self.output_file_path):
            try:
                with open(self.output_file_path, "r") as f:
                    f.seek(self.last_file_pos)
                    new_text = f.read()
                    self.last_file_pos = f.tell()
                if new_text and self.merging_dialog:
                    self.merging_dialog.append_log(new_text)
            except Exception as e:
                logger.error(f"Error polling log file: {e}")
                self._on_stop_process_requested()

        if not job_is_running:
            if self.merging_dialog:
                self.merging_dialog.append_log("\n--- Job finished. ---")
            self._on_stop_process_requested()

    def _on_stop_process_requested(self):
        if self.log_poll_timer.isActive():
            self.log_poll_timer.stop()
            if self.merging_dialog:
                self.merging_dialog.set_process_running(False)
            self.active_job_id = None
            self.active_job_method = None
            self.output_file_path = None

    def _on_view_stream_requested(self):
        try:
            stream_path = self.stream_manager.get_stream_file_path()
            if not os.path.exists(stream_path):
                self.main_window.ui_manager.show_warning_message(
                    "File Not Found", "Stream file has not been generated yet."
                )
                return
            with open(stream_path, "r") as f:
                header_text = "".join(f.readline() for _ in range(1000))
            dialog = QDialog(self.main_window)
            dialog.setWindowTitle(f"Header: {os.path.basename(stream_path)}")
            dialog.setMinimumSize(700, 500)
            layout = QVBoxLayout(dialog)
            text_edit = QTextEdit()
            text_edit.setReadOnly(True)
            text_edit.setFont(QFont("Monospace", 9))
            text_edit.setPlainText(
                header_text if header_text else "[File is empty or contains no text]"
            )
            layout.addWidget(text_edit)
            dialog.show()
        except Exception as e:
            self.main_window.ui_manager.show_warning_message(
                "Error", f"Could not open stream file:\n{e}"
            )
