from pyqtgraph.Qt import QtCore, QtWidgets


class BusyCursor:
    """Context manager to show a busy cursor during slow UI-blocking operations."""

    def __enter__(self):
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        QtWidgets.QApplication.processEvents()  # Ensure cursor changes immediately

    def __exit__(self, exc_type, exc_val, exc_tb):
        QtWidgets.QApplication.restoreOverrideCursor()
