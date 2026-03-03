# qp2/image_viewer/beamcenter/calibration_settings.py
from PyQt5.QtCore import QTimer
from pyqtgraph.Qt import QtWidgets

from qp2.image_viewer.ui.singleton_dialog import SingletonDialog
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class CalibrationSettingsDialog(SingletonDialog):
    """A dialog for configuring and launching beam center calibration."""

    def __init__(self, settings_manager, parent=None):
        super().__init__(parent)
        self.settings_manager = settings_manager

        self.setWindowTitle("Beam Center Calibration Settings")
        # This is a modal dialog: user must interact with it before continuing.
        self.setModal(True)

        layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()

        # --- Calibration Settings ---
        self.calibration_mode_combo = QtWidgets.QComboBox()
        self.calibration_mode_combo.setToolTip(
            "'Refine': Adjusts a close beam center (recommended).\n"
            "'StartScratch': Finds beam center from scratch using Hough transform.\n"
            "'ManualFit': Allows manually selecting points on a ring."
        )
        calibration_options = {
            "Refine": "Refine from Metadata",
            "StartScratch": "Find from Scratch",
            "ManualFit": "Manual Point Selection",
        }
        for actual_value, display_text in calibration_options.items():
            self.calibration_mode_combo.addItem(display_text, actual_value)
        current_mode = self.settings_manager.get("calibration_mode")
        index = self.calibration_mode_combo.findData(current_mode)
        if index != -1:
            self.calibration_mode_combo.setCurrentIndex(index)
        form_layout.addRow("Calibration Mode:", self.calibration_mode_combo)

        self.calibration_ring_spinbox = QtWidgets.QDoubleSpinBox()
        self.calibration_ring_spinbox.setRange(1.0, 10.0)
        self.calibration_ring_spinbox.setDecimals(4)
        self.calibration_ring_spinbox.setSingleStep(0.001)
        self.calibration_ring_spinbox.setValue(
            self.settings_manager.get("calibration_ring_resolution")
        )
        self.calibration_ring_spinbox.setToolTip(
            "Enter the calibration ring resolution in Ångstroms (e.g., 3.022 Å for burn paper)."
        )
        self.calibration_ring_spinbox.setSuffix(" Å")
        form_layout.addRow("Calibration Ring:", self.calibration_ring_spinbox)

        self.calibration_band_width_spinbox = QtWidgets.QSpinBox()
        self.calibration_band_width_spinbox.setRange(1, 200)
        self.calibration_band_width_spinbox.setSingleStep(1)
        self.calibration_band_width_spinbox.setValue(
            self.settings_manager.get("calibration_band_width")
        )
        self.calibration_band_width_spinbox.setSuffix(" px")
        self.calibration_band_width_spinbox.setToolTip(
            "Width of the annular band (in pixels) used to find strong pixels for fitting."
        )
        form_layout.addRow("Band Width:", self.calibration_band_width_spinbox)

        layout.addLayout(form_layout)

        # --- OK / Cancel Buttons ---
        button_box = QtWidgets.QDialogButtonBox()
        run_button = button_box.addButton(
            "Run Calibration", QtWidgets.QDialogButtonBox.AcceptRole
        )
        button_box.addButton(QtWidgets.QDialogButtonBox.Cancel)
        layout.addWidget(button_box)

        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)

        # Connect widgets to live-update settings
        self.calibration_mode_combo.currentIndexChanged.connect(self._on_param_changed)
        self.calibration_ring_spinbox.valueChanged.connect(self._on_param_changed)
        self.calibration_band_width_spinbox.valueChanged.connect(self._on_param_changed)

        # Debounce timer for saving changes
        self._debounce_timer = QTimer(self)
        self._debounce_timer.setSingleShot(True)
        self._debounce_timer.timeout.connect(self._save_settings)

    def _on_param_changed(self, *args):
        """Starts a timer to save settings after a short delay."""
        self._debounce_timer.start(200)

    def _save_settings(self):
        """Saves the current UI values to the settings manager."""
        new_settings = {
            "calibration_mode": self.calibration_mode_combo.currentData(),
            "calibration_ring_resolution": self.calibration_ring_spinbox.value(),
            "calibration_band_width": self.calibration_band_width_spinbox.value(),
        }
        self.settings_manager.update_from_dict(new_settings)
        logger.debug(f"Calibration settings updated: {new_settings}")

    def accept(self):
        """Ensures settings are saved before closing."""
        if self._debounce_timer.isActive():
            self._debounce_timer.stop()
            self._save_settings()
        super().accept()
