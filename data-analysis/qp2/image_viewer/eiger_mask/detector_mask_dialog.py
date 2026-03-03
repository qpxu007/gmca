# qp2/image_viewer/eiger_mask/detector_mask_dialog.py

import socket
from pyqtgraph.Qt import QtCore, QtWidgets, QtGui
import numpy as np

from qp2.image_viewer.eiger_mask.eiger_api_manager import EigerAPIManager


class DetectorMaskDialog(QtWidgets.QDialog):
    """Dialog to manage and upload a hardware pixel mask to an EIGER detector."""

    def __init__(self, initial_pixels_to_add, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Update Detector Hardware Mask")
        self.setMinimumSize(600, 400)

        self.initial_pixels = initial_pixels_to_add
        self.current_mask = None

        # --- Layout ---
        layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()

        # --- API Settings ---
        hostname = socket.gethostname()
        if hostname.lower().startswith("bl1"):
            eiger_dcu = "bl1dcu"
        elif hostname.lower().startswith("bl2"):
            eiger_dcu = "bl2dcu"
        else:
            eiger_dcu = ""

        self.ip_input = QtWidgets.QLineEdit(f"{eiger_dcu}")
        self.port_input = QtWidgets.QLineEdit("80")
        self.api_version_input = QtWidgets.QLineEdit("1.8.0")

        form_layout.addRow("Detector IP:", self.ip_input)
        form_layout.addRow("Port:", self.port_input)
        form_layout.addRow("API Version:", self.api_version_input)

        # --- Pixel List ---
        self.pixel_list_widget = QtWidgets.QListWidget()
        self.pixel_list_widget.setSelectionMode(
            QtWidgets.QAbstractItemView.ExtendedSelection
        )
        self.add_pixel_input = QtWidgets.QLineEdit()
        self.add_pixel_button = QtWidgets.QPushButton("Add Pixel (x, y)")
        self.remove_pixel_button = QtWidgets.QPushButton("Remove Selected")

        pixel_button_layout = QtWidgets.QHBoxLayout()
        pixel_button_layout.addWidget(self.add_pixel_input)
        pixel_button_layout.addWidget(self.add_pixel_button)
        pixel_button_layout.addWidget(self.remove_pixel_button)

        # --- Action Buttons ---
        self.fetch_button = QtWidgets.QPushButton("1. Fetch Current Mask")
        self.upload_button = QtWidgets.QPushButton("2. Upload New Mask")
        self.verify_button = QtWidgets.QPushButton("3. Verify Upload")
        self.upload_button.setEnabled(False)
        self.verify_button.setEnabled(False)

        action_layout = QtWidgets.QHBoxLayout()
        action_layout.addWidget(self.fetch_button)
        action_layout.addWidget(self.upload_button)
        action_layout.addWidget(self.verify_button)

        # --- Status Label ---
        self.status_label = QtWidgets.QLabel("Status: Idle")
        font = self.status_label.font()
        font.setBold(True)
        self.status_label.setFont(font)

        # --- Assembly ---
        layout.addLayout(form_layout)
        layout.addWidget(QtWidgets.QLabel("Pixels to Mask:"))
        layout.addWidget(self.pixel_list_widget)
        layout.addLayout(pixel_button_layout)
        layout.addStretch()
        layout.addLayout(action_layout)
        layout.addWidget(self.status_label)

        # --- Connections ---
        self.add_pixel_button.clicked.connect(self.add_pixel)
        self.remove_pixel_button.clicked.connect(self.remove_selected_pixels)
        self.fetch_button.clicked.connect(self.fetch_mask)
        self.upload_button.clicked.connect(self.upload_mask)
        self.verify_button.clicked.connect(self.verify_mask)

        # Populate initial list
        for r, c in self.initial_pixels:
            self.pixel_list_widget.addItem(f"{c}, {r}")

    def get_api_manager(self):
        return EigerAPIManager(
            self.ip_input.text(), self.port_input.text(), self.api_version_input.text()
        )

    def add_pixel(self):
        text = self.add_pixel_input.text()
        try:
            x_str, y_str = text.replace("(", "").replace(")", "").split(",")
            x, y = int(x_str.strip()), int(y_str.strip())
            self.pixel_list_widget.addItem(f"{x}, {y}")
            self.add_pixel_input.clear()
        except ValueError:
            self.set_status("Invalid format. Use 'x, y'.", "red")

    def remove_selected_pixels(self):
        for item in self.pixel_list_widget.selectedItems():
            self.pixel_list_widget.takeItem(self.pixel_list_widget.row(item))

    def set_status(self, text, color="black"):
        self.status_label.setText(f"Status: {text}")
        self.status_label.setStyleSheet(f"color: {color};")
        QtWidgets.QApplication.processEvents()

    def fetch_mask(self):
        self.set_status("Fetching mask from detector...", "orange")
        try:
            api = self.get_api_manager()
            self.current_mask = api.get_pixel_mask()
            self.set_status(
                f"Success! Fetched mask with shape {self.current_mask.shape}.", "green"
            )
            self.upload_button.setEnabled(True)
        except Exception as e:
            self.set_status(f"Error: {e}", "red")

    def upload_mask(self):
        if self.current_mask is None:
            self.set_status("Error: Must fetch current mask first.", "red")
            return

        new_mask = self.current_mask.copy()

        # Add new pixels from the list
        pixels_added = 0
        for i in range(self.pixel_list_widget.count()):
            text = self.pixel_list_widget.item(i).text()
            try:
                x, y = map(int, text.split(","))
                # Eiger mask convention might be different.
                # Common is setting to 1 (bad) or 2 (hot). Let's use 2.
                new_mask[y, x] = 2
                pixels_added += 1
            except (ValueError, IndexError):
                continue

        reply = QtWidgets.QMessageBox.question(
            self,
            "Confirm Upload",
            f"You are about to upload a new mask to the detector at {self.ip_input.text()}.\n\n"
            f"This will add {pixels_added} pixels to the existing mask.\n\n"
            "This is a hardware operation. Are you sure you want to proceed?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No,
        )
        if reply == QtWidgets.QMessageBox.No:
            return

        self.set_status(
            f"Uploading new mask with {pixels_added} additional bad pixels...", "orange"
        )
        try:
            api = self.get_api_manager()
            api.set_pixel_mask(new_mask)
            self.set_status("Upload successful!", "green")
            self.verify_button.setEnabled(True)
        except Exception as e:
            self.set_status(f"Upload failed: {e}", "red")

    def verify_mask(self):
        self.set_status("Verifying mask on detector...", "orange")
        try:
            api = self.get_api_manager()
            remote_mask = api.get_pixel_mask()

            # Check a few of the newly added pixels
            verified_count = 0
            for i in range(min(self.pixel_list_widget.count(), 10)):  # Check up to 10
                text = self.pixel_list_widget.item(i).text()
                x, y = map(int, text.split(","))
                if remote_mask[y, x] != 0:
                    verified_count += 1

            if verified_count > 0:
                self.set_status(
                    f"Verification successful! Confirmed {verified_count} pixels are masked.",
                    "green",
                )
            else:
                self.set_status(
                    "Verification failed. Mask on detector does not match.", "red"
                )

        except Exception as e:
            self.set_status(f"Verification failed: {e}", "red")
