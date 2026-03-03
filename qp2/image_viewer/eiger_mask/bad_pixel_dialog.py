# qp2/image_viewer/eiger_mask/bad_pixel_dialog.py

import socket
import numpy as np
from pyqtgraph.Qt import QtCore, QtGui, QtWidgets


class BadPixelDialog(QtWidgets.QDialog):
    """
    Dialog to display bad pixel candidates, allowing user interaction.
    """

    zoom_requested = QtCore.pyqtSignal(int, int)  # row, col
    apply_to_mask_requested = QtCore.pyqtSignal(list)  # list of [r,c] coords
    analyze_pixel_requested = QtCore.pyqtSignal()  # To trigger the manager
    update_hardware_mask_requested = QtCore.pyqtSignal(list)

    def __init__(self, bad_pixel_coords, bad_pixel_reasons, parent=None):
        super().__init__(parent)
        self.bad_pixel_coords = bad_pixel_coords

        self.setWindowTitle("Bad Pixel Candidates")
        self.setMinimumSize(400, 500)

        # --- Layout ---
        layout = QtWidgets.QVBoxLayout(self)

        # --- Table Widget ---
        self.table_widget = QtWidgets.QTableWidget()
        self.table_widget.setColumnCount(3)
        self.table_widget.setHorizontalHeaderLabels(["X (col)", "Y (row)", "Reason"])
        self.table_widget.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table_widget.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table_widget.setSelectionMode(
            QtWidgets.QAbstractItemView.ExtendedSelection
        )
        self.populate_table(bad_pixel_coords, bad_pixel_reasons)
        layout.addWidget(self.table_widget)

        # --- Buttons ---
        button_layout = QtWidgets.QHBoxLayout()
        self.analyze_button = QtWidgets.QPushButton("Analyze Pixel...")
        self.update_hw_button = QtWidgets.QPushButton("Update Hardware Mask...")
        self.update_hw_button.setToolTip(
            "To update detector mask, run the procedure on epu"
        )
        self.apply_button = QtWidgets.QPushButton("Apply Selected to Session")
        self.close_button = QtWidgets.QPushButton("Close")

        button_layout.addWidget(self.analyze_button)
        button_layout.addWidget(self.update_hw_button)
        button_layout.addStretch()
        button_layout.addWidget(self.apply_button)
        button_layout.addWidget(self.close_button)
        layout.addLayout(button_layout)

        # --- Connections ---
        self.table_widget.itemSelectionChanged.connect(self.on_selection_changed)
        self.close_button.clicked.connect(self.accept)

        self.update_hw_button.clicked.connect(self.on_update_hw_clicked)
        self.update_hw_button.setEnabled(
            "epu" in socket.gethostname()
        )  # only enable hw on epu machines
        self.apply_button.clicked.connect(self.on_apply_clicked)
        self.analyze_button.clicked.connect(self.analyze_pixel_requested.emit)

    def on_update_hw_clicked(self):
        """Emits a signal with ALL bad pixels found to populate the hardware dialog."""
        all_coords_rc = []
        for i in range(self.table_widget.rowCount()):
            c = int(self.table_widget.item(i, 0).text())
            r = int(self.table_widget.item(i, 1).text())
            all_coords_rc.append([r, c])

        if not all_coords_rc:
            QtWidgets.QMessageBox.information(
                self, "No Pixels", "No bad pixel candidates to send."
            )
            return

        self.update_hardware_mask_requested.emit(all_coords_rc)

    def populate_table(self, coords, reasons):
        self.table_widget.setRowCount(len(coords))
        for i, (coord, reason) in enumerate(zip(coords, reasons)):
            r, c = coord
            self.table_widget.setItem(
                i, 0, QtWidgets.QTableWidgetItem(str(c))
            )  # X (col)
            self.table_widget.setItem(
                i, 1, QtWidgets.QTableWidgetItem(str(r))
            )  # Y (row)
            self.table_widget.setItem(i, 2, QtWidgets.QTableWidgetItem(reason))
        self.table_widget.resizeColumnsToContents()

    def on_selection_changed(self):
        selected_items = self.table_widget.selectedItems()
        if not selected_items:
            return
        # Get the first selected row
        selected_row_index = selected_items[0].row()
        c = int(self.table_widget.item(selected_row_index, 0).text())  # X (col)
        r = int(self.table_widget.item(selected_row_index, 1).text())  # Y (row)
        self.zoom_requested.emit(r, c)

    def on_apply_clicked(self):
        selected_rows = sorted(
            list(set(index.row() for index in self.table_widget.selectedIndexes()))
        )
        if not selected_rows:
            QtWidgets.QMessageBox.warning(
                self, "No Selection", "Please select pixels to apply to the mask."
            )
            return

        reply = QtWidgets.QMessageBox.question(
            self,
            "Confirm Update",
            f"Are you sure you want to add {len(selected_rows)} selected pixel(s) to the detector mask for this session?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No,
        )

        if reply == QtWidgets.QMessageBox.Yes:
            coords_to_add = []
            for row_idx in selected_rows:
                c = int(self.table_widget.item(row_idx, 0).text())  # X (col)
                r = int(self.table_widget.item(row_idx, 1).text())  # Y (row)
                coords_to_add.append([r, c])
            self.apply_to_mask_requested.emit(coords_to_add)
            self.accept()  # Close dialog after applying
