from PyQt5 import QtWidgets, QtCore
import numpy as np

try:
    from qtconsole.rich_jupyter_widget import RichJupyterWidget
    from qtconsole.inprocess import QtInProcessKernelManager
    HAS_QTCONSOLE = True
except ImportError:
    HAS_QTCONSOLE = False
    class RichJupyterWidget(QtWidgets.QWidget): pass # Dummy class

class AdvancedConsoleWidget(QtWidgets.QWidget):
    def __init__(self, namespace=None, parent=None):
        super().__init__(parent)
        self.namespace = namespace or {}
        
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        if HAS_QTCONSOLE:
            self.kernel_manager = QtInProcessKernelManager()
            self.kernel_manager.start_kernel()
            self.kernel = self.kernel_manager.kernel
            self.kernel.gui = 'qt'

            self.kernel_client = self.kernel_manager.client()
            self.kernel_client.start_channels()

            self.console_widget = RichJupyterWidget()
            self.console_widget.kernel_manager = self.kernel_manager
            self.console_widget.kernel_client = self.kernel_client
            
            # Push variables to the IPython namespace
            self.push_vars(self.namespace)
            
            layout.addWidget(self.console_widget)
            
            # Print welcome message
            self.console_widget.append_stream("Advanced IPython Console initialized.\n")
            self.console_widget.append_stream("Variables available: " + ", ".join(self.namespace.keys()) + "\n")
            
        else:
            label = QtWidgets.QLabel("Advanced Console requires 'qtconsole'.\nPlease install it: pip install qtconsole")
            label.setAlignment(QtCore.Qt.AlignCenter)
            layout.addWidget(label)

    def push_vars(self, variables):
        """Push variables to the IPython kernel namespace."""
        self.namespace.update(variables)
        if HAS_QTCONSOLE and self.kernel:
            self.kernel.shell.push(variables)

    @property
    def localNamespace(self):
        """Expose the namespace dictionary to match PythonConsoleWidget API."""
        return self.namespace

    def shutdown(self):
        if HAS_QTCONSOLE:
            self.kernel_client.stop_channels()
            self.kernel_manager.shutdown_kernel()
