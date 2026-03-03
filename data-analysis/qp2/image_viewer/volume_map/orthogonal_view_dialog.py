# In qp2/image_viewer/ui/orthogonal_view_dialog.py
import re
from pyqtgraph.Qt import QtWidgets, QtCore

from qp2.xio.hdf5_manager import HDF5Reader  # Import HDF5Reader for type hinting


class OrthogonalViewDialog(QtWidgets.QDialog):
    """
    A simple dialog that acts as a remote control to display one of two
    orthogonal images in the main application window.
    """

    # Signal emits the reader and the 0-based frame index to display
    view_image_requested = QtCore.pyqtSignal(HDF5Reader, int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Orthogonal Image Selector")
        self.setMinimumWidth(400)

        self.reader_xy = None
        self.reader_xz = None
        self.frame_index_xy = -1
        self.frame_index_xz = -1

        # --- Main Layout ---
        self.layout = QtWidgets.QVBoxLayout(self)

        # --- UI Elements ---
        self.info_label = QtWidgets.QLabel(
            "Click a button to load the corresponding image in the main viewer."
        )
        self.info_label.setWordWrap(True)

        self.btn_view_xy = QtWidgets.QPushButton("View XY Scan Image")
        self.btn_view_xz = QtWidgets.QPushButton("View XZ Scan Image")

        self.layout.addWidget(self.info_label)
        self.layout.addWidget(self.btn_view_xy)
        self.layout.addWidget(self.btn_view_xz)

        # --- Connections ---
        self.btn_view_xy.clicked.connect(self._on_view_xy_clicked)
        self.btn_view_xz.clicked.connect(self._on_view_xz_clicked)

    def set_data_sources(
            self, reader_xy: HDF5Reader, reader_xz: HDF5Reader, frame_index_xy: int, frame_index_xz: int
    ):
        """Stores the necessary information to load the images."""
        self.reader_xy = reader_xy
        self.reader_xz = reader_xz
        self.frame_index_xy = frame_index_xy
        self.frame_index_xz = frame_index_xz

        # Update button text to be more informative
        if reader_xy:
            self.btn_view_xy.setText(f"View XY Image (Row {self._get_row(reader_xy)}, Frame {self.frame_index_xy + 1})")
            self.btn_view_xy.setEnabled(True)
        else:
            self.btn_view_xy.setText("XY Image Not Found")
            self.btn_view_xy.setEnabled(False)

        if reader_xz:
            self.btn_view_xz.setText(f"View XZ Image (Row {self._get_row(reader_xz)}, Frame {self.frame_index_xz + 1})")
            self.btn_view_xz.setEnabled(True)
        else:
            self.btn_view_xz.setText("XZ Image Not Found")
            self.btn_view_xz.setEnabled(False)

    def _get_row(self, reader: HDF5Reader) -> int:
        """Helper to parse row number from a reader's filepath."""
        match = re.search(r"_(?:R|C)(\d+)", reader.master_file_path, re.IGNORECASE)
        return int(match.group(1)) if match else 0

    def _on_view_xy_clicked(self):
        if self.reader_xy and self.frame_index_xy != -1:
            self.view_image_requested.emit(self.reader_xy, self.frame_index_xy)

    def _on_view_xz_clicked(self):
        if self.reader_xz and self.frame_index_xz != -1:
            self.view_image_requested.emit(self.reader_xz, self.frame_index_xz)
