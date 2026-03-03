from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QDialog, QVBoxLayout
from pyqtgraph.Qt import QtWidgets


class PeelablePlotDialog(QDialog):
    """A dialog to host the 'peeled' Dozor plot container."""

    # Signal to indicate the plot should be re-docked, passing back the container
    request_redock = pyqtSignal(QtWidgets.QWidget)

    def __init__(self, plot_container_widget: QtWidgets.QWidget, parent=None):
        super().__init__(parent)
        self.plot_container_widget = plot_container_widget
        self.setWindowTitle("Detached Dozor Plot")
        self.setMinimumSize(400, 300)  # Set a reasonable minimum size

        layout = QVBoxLayout(self)
        layout.addWidget(self.plot_container_widget)
        self.setLayout(layout)
        self.plot_container_widget.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Expanding,
        )

    def closeEvent(self, event):
        """Emit a signal when the dialog is closed by any means."""
        self.request_redock.emit(self.plot_container_widget)
        super().closeEvent(event)
