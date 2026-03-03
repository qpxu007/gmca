# qp2/image_viewer/beamcenter/calibration_dialog.py

import datetime
import getpass
import os
import socket
import subprocess
from typing import Optional, Tuple

from PyQt5 import QtWidgets, QtGui, QtCore

from qp2.image_viewer.beamcenter.find_rings import prepare_calibration_message
from qp2.log.logging_config import get_logger
from qp2.xio.user_group_manager import find_esaf_directory
from qp2.image_viewer.ui.busy_cursor import BusyCursor

logger = get_logger(__name__)


class CalibrationResultsDialog(QtWidgets.QDialog):
    """
    A dialog to display calibration results and provide actions like
    exporting site files or updating EPICS values.
    """

    def __init__(
            self,
            result: dict,
            params: dict,
            calibration_ring_resolution: float,
            parent=None,
    ):
        """
        Initializes the dialog with calibration data.

        Args:
            result (dict): The dictionary of results from the CalibrationWorker.
            params (dict): The HDF5 parameters for the current dataset.
            calibration_ring_resolution (float): The resolution used for calibration.
            parent (QWidget, optional): The parent widget.
        """
        super().__init__(parent)

        # Store essential data
        self.params = params
        self.refined_circle = result.get("refined_circle")
        self.refine_circle_radii = result.get("refine_circle_radii")
        self.calibration_ring_resolution = calibration_ring_resolution

        # Pre-calculate values needed by multiple methods
        self.r_cx, self.r_cy, self.r_r = (
            self.refined_circle if self.refined_circle else (None, None, None)
        )
        self.xbeam_mm, self.ybeam_mm = self._calculate_denzo_values()

        # Initialize UI components
        self._setup_ui()
        self._connect_signals()

    def _setup_ui(self):
        """Creates and arranges all the widgets in the dialog."""
        self.setWindowTitle("Calibration Results")
        self.setModal(False)
        self.setMinimumSize(600, 450)

        layout = QtWidgets.QVBoxLayout(self)

        # --- Text Display Area ---
        self.text_edit = QtWidgets.QTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setFont(QtGui.QFont("Consolas", 10))
        text_block = prepare_calibration_message(
            self.refined_circle,
            self.params,
            self.calibration_ring_resolution,
            self.refine_circle_radii,
        )
        self.text_edit.setPlainText(text_block)
        layout.addWidget(self.text_edit)

        # --- Action Buttons ---
        self.export_button = QtWidgets.QPushButton("Save HKL def.site")
        self.export_button.setToolTip("Generate and save a def.site file for HKL-2000.")
        button_font_metrics = QtGui.QFontMetrics(self.export_button.font())
        text_width = button_font_metrics.horizontalAdvance(self.export_button.text())
        self.export_button.setMaximumWidth(text_width + 40)

        self.update_beam_button = QtWidgets.QPushButton(
            "Update Beam Center in EPICS && Save HKL def.site"
        )
        self.update_beam_button.setToolTip(
            "Update the beamline's EPICS PV and save def.site files to the data and home directories."
        )

        button_layout = QtWidgets.QHBoxLayout()
        button_layout.addWidget(self.update_beam_button)

        button_layout.addWidget(self.export_button)
        layout.addLayout(button_layout)

        # --- Close Button ---
        self.close_button = QtWidgets.QPushButton("Close")
        layout.addWidget(self.close_button, 0, QtCore.Qt.AlignmentFlag.AlignRight)

        # Disable buttons if calibration failed
        if self.refined_circle is None:
            self.export_button.setEnabled(False)
            self.update_beam_button.setEnabled(False)

    def _connect_signals(self):
        """Connects widget signals to their handler slots."""
        self.export_button.clicked.connect(self._handle_export_clicked)
        self.update_beam_button.clicked.connect(self._handle_update_beam_center_clicked)
        self.close_button.clicked.connect(self.accept)

    def _calculate_denzo_values(self) -> Tuple[Optional[float], Optional[float]]:
        """Calculates beam center coordinates in mm for denzo/HKL-2000."""
        pixel_size = self.params.get("pixel_size")
        if self.r_cx is not None and self.r_cy is not None and pixel_size is not None:
            try:
                xbeam_mm = round(float(self.r_cx) * float(pixel_size), 2)
                ybeam_mm = round(float(self.r_cy) * float(pixel_size), 2)
                return xbeam_mm, ybeam_mm
            except (ValueError, TypeError):
                pass
        return None, None

    def _generate_site_content(self) -> Optional[str]:
        """Generates the string content for a def.site file."""
        if self.xbeam_mm is None or self.ybeam_mm is None:
            return None

        now = datetime.datetime.now().strftime("%H:%M:%S %b %d, %Y")
        user = getpass.getuser()
        detector = self.params.get("detector", "CCD Eiger16m")

        if self.params.get("nx") == 4150 and self.params.get("ny") == 4371:
            hkl_detector = "CCD Eiger16m"
        elif self.params.get("nx") == 4148 and self.params.get("ny") == 4362:
            hkl_detector = "CCD Eiger2 16m"
        else:
            hkl_detector = f"CCD {detector}"

        # HKL-2000 expects xbeam and ybeam to be swapped relative to ADXV
        xbeam_str = f"{self.ybeam_mm:.2f}"
        ybeam_str = f"{self.xbeam_mm:.2f}"

        return (
            "HKLSuite0.95SITE\n"
            f"{{detec}} {{{hkl_detector}}}\n"
            f"{{last_saved,date}} {{{now}}}\n"
            f"{{last_saved,user}} {{{user}}}\n"
            "{{rotation_axis} {Phi}\n"
            f"{{xbeam}} {{{xbeam_str}}}\n"
            f"{{ybeam}} {{{ybeam_str}}}\n"
        )

    def _export_site_file(self, save_path: str) -> Tuple[bool, str]:
        """
        Writes the def.site content to the specified path.

        Returns:
            A tuple (success_boolean, message_string).
        """
        content = self._generate_site_content()
        if content is None:
            return False, "Could not generate site file content (missing parameters)."

        try:
            with open(save_path, "w") as f:
                f.write(content)
            return True, f"Successfully saved to:\n{save_path}"
        except Exception as e:
            error_msg = f"Error saving def.site file to {save_path}: {e}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg

    def _handle_export_clicked(self):
        """Handles the 'Export HKL def.site...' button click."""
        home = os.path.expanduser("~")
        options = QtWidgets.QFileDialog.Options()
        path_to_save, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Export def.site",
            os.path.join(home, "def.site"),
            "Site files (*.site);;All Files (*)",
            options=options,
        )

        if not path_to_save:
            return  # User cancelled

        success, message = self._export_site_file(path_to_save)
        if success:
            QtWidgets.QMessageBox.information(self, "Export Successful", message)
        else:
            QtWidgets.QMessageBox.warning(self, "Export Failed", message)

    def _handle_update_beam_center_clicked(self):
        """Handles the combined 'Update EPICS and Save' button click."""
        # 1. Determine EPICS PV
        hostname = socket.gethostname()
        if hostname.startswith("bl1"):
            pv = "23i:bi:beam_xy"
        elif hostname.startswith("bl2"):
            pv = "23o:bi:beam_xy"
        else:
            QtWidgets.QMessageBox.warning(
                self,
                "Hostname Error",
                f"Unrecognized hostname: {hostname}\nCannot determine EPICS PV.",
            )
            return

        # 2. Prepare for confirmation
        try:
            refined_x = int(round(self.r_cx))
            refined_y = int(round(self.r_cy))
        except (ValueError, TypeError):
            QtWidgets.QMessageBox.warning(
                self, "Beam Center Error", "Could not parse refined beam center."
            )
            return

        master_file_path = self.parent().current_master_file
        esaf_dir = find_esaf_directory(master_file_path) if master_file_path else None

        # 3. Build and show confirmation dialog
        confirm_message = (
            f"This will perform the following actions:\n\n"
            f"1. Set EPICS PV '{pv}' to '{refined_x}, {refined_y}'.\n"
        )
        if esaf_dir:
            confirm_message += (
                f"2. Save 'def.site' to data directory:\n   '{esaf_dir}'.\n"
            )
        else:
            confirm_message += (
                "2. WARNING: Could not find ESAF data directory to save 'def.site'.\n"
            )

        confirm_message += "3. Save a backup copy to your home directory.\n\n"
        confirm_message += "Are you sure you want to continue?"

        reply = QtWidgets.QMessageBox.question(
            self,
            "Confirm Update and Save",
            confirm_message,
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
        )
        if reply != QtWidgets.QMessageBox.Yes:
            return

        # 4. Execute actions
        with BusyCursor():
            # Action 1: Run caput command
            try:
                result = subprocess.run(
                    ["caput", pv, f"{refined_x}, {refined_y}"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode != 0:
                    QtWidgets.QMessageBox.critical(
                        self, "EPICS Update Failed", f"caput failed:\n{result.stderr}"
                    )
                    return  # Stop if EPICS update fails
            except Exception as e:
                QtWidgets.QMessageBox.critical(
                    self, "EPICS Update Error", f"Error running caput: {e}"
                )
                return

            # Actions 2 & 3: Save files
            save_messages = ["EPICS update successful."]
            if esaf_dir:
                _, message = self._export_site_file(os.path.join(esaf_dir, "def.site"))
                save_messages.append(message)

            home = os.path.expanduser("~")
            date_tag = datetime.datetime.now().strftime("%Y%m%d")
            home_path = os.path.join(home, f"def.site_{date_tag}")
            _, message = self._export_site_file(home_path)
            save_messages.append(message)

        # 5. Final report
        QtWidgets.QMessageBox.information(
            self, "Actions Completed", "\n\n".join(save_messages)
        )
