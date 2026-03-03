from PyQt5.QtCore import pyqtSignal, QTimer
from PyQt5.QtWidgets import QVBoxLayout, QFormLayout, QSpinBox, QLabel

from qp2.image_viewer.ui.singleton_dialog import SingletonDialog


class SumSettingsDialog(SingletonDialog):
    sum_params_changed = pyqtSignal(int)  # num_frames

    def __init__(self, current_sum_count=1, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Frame Summation Settings")
        self.setModal(False)
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.sum_count_spinbox = QSpinBox()
        self.sum_count_spinbox.setRange(1, 1000)
        self.sum_count_spinbox.setValue(current_sum_count)
        form.addRow(QLabel("Number of Frames to Sum:"), self.sum_count_spinbox)

        layout.addLayout(form)
        self.setLayout(layout)

        self.sum_count_spinbox.valueChanged.connect(self._on_param_changed)

        # Debounce timer for parameter changes
        self._debounce_timer = QTimer(self)
        self._debounce_timer.setSingleShot(True)
        self._debounce_timer.timeout.connect(self._emit_params_changed)

    def _on_param_changed(self, *args):
        self._debounce_timer.start(200)

    def _emit_params_changed(self):
        self.sum_params_changed.emit(self.sum_count_spinbox.value())

    def set_sum_count(self, count):
        self.sum_count_spinbox.setValue(count)
