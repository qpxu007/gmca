# qp2/image_viewer/plugins/spot_finder/spot_finder_settings.py
from PyQt5.QtCore import pyqtSignal, QTimer
from pyqtgraph.Qt import QtWidgets

from qp2.image_viewer.ui.singleton_dialog import SingletonDialog
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class SpotFinderSettingsDialog(SingletonDialog):
    """Dialog for editing live spot finding settings."""

    # Emits the updated settings dict on change
    peak_params_changed = pyqtSignal(dict)

    def __init__(self, settings_manager, parent=None):
        super().__init__(parent)
        self.settings_manager = settings_manager
        self.setWindowTitle("Live Spot Finder Settings")
        self.setModal(False)

        layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()

        # --- Peak Finding Settings ---
        peak_group = QtWidgets.QGroupBox("Peak Finding Parameters")
        peak_layout = QtWidgets.QFormLayout(peak_group)

        # Low Resolution Limit
        self.peak_low_res_spinbox = QtWidgets.QDoubleSpinBox()
        self.peak_low_res_spinbox.setRange(5.0, 100.0)
        self.peak_low_res_spinbox.setDecimals(1)
        self.peak_low_res_spinbox.setSuffix(" Å")
        self.peak_low_res_spinbox.setValue(
            self.settings_manager.get("peak_finding_low_resolution_A")
        )
        peak_layout.addRow("Low-Res Limit (Inner):", self.peak_low_res_spinbox)

        # High Resolution Limit
        self.peak_high_res_spinbox = QtWidgets.QDoubleSpinBox()
        self.peak_high_res_spinbox.setRange(0.5, 50.0)
        self.peak_high_res_spinbox.setDecimals(2)
        self.peak_high_res_spinbox.setSuffix(" Å")
        self.peak_high_res_spinbox.setValue(
            self.settings_manager.get("peak_finding_high_resolution_A")
        )
        peak_layout.addRow("High-Res Limit (Outer):", self.peak_high_res_spinbox)

        # Z-Score Cutoff
        self.peak_zscore_spinbox = QtWidgets.QDoubleSpinBox()
        self.peak_zscore_spinbox.setRange(0.0, 20.0)
        self.peak_zscore_spinbox.setDecimals(1)
        self.peak_zscore_spinbox.setSingleStep(0.5)
        self.peak_zscore_spinbox.setValue(
            self.settings_manager.get("peak_finding_zscore_cutoff")
        )
        peak_layout.addRow("Z-Score Cutoff (SNR):", self.peak_zscore_spinbox)

        # Max Number of Peaks
        self.peak_num_spinbox = QtWidgets.QSpinBox()
        self.peak_num_spinbox.setRange(10, 1000)
        self.peak_num_spinbox.setSingleStep(10)
        self.peak_num_spinbox.setValue(
            self.settings_manager.get("peak_finding_num_peaks")
        )
        peak_layout.addRow("Max Number of Peaks:", self.peak_num_spinbox)

        # Min Peak Distance
        self.peak_dist_spinbox = QtWidgets.QSpinBox()
        self.peak_dist_spinbox.setRange(1, 50)
        self.peak_dist_spinbox.setSingleStep(1)
        self.peak_dist_spinbox.setSuffix(" px")
        self.peak_dist_spinbox.setValue(
            self.settings_manager.get("peak_finding_min_distance")
        )
        peak_layout.addRow("Min Peak Distance:", self.peak_dist_spinbox)

        # Min Pixels per Peak
        self.peak_min_pixels_spinbox = QtWidgets.QSpinBox()
        self.peak_min_pixels_spinbox.setRange(1, 30)
        self.peak_min_pixels_spinbox.setSingleStep(1)
        self.peak_min_pixels_spinbox.setSuffix(" px")
        self.peak_min_pixels_spinbox.setValue(
            self.settings_manager.get("peak_finding_min_pixels")
        )
        peak_layout.addRow("Min Pixels per Peak:", self.peak_min_pixels_spinbox)

        # Min Peak Intensity
        self.peak_imin_spinbox = QtWidgets.QSpinBox()
        self.peak_imin_spinbox.setRange(1, 1000)
        self.peak_imin_spinbox.setSingleStep(1)
        self.peak_imin_spinbox.setSuffix(" photons")
        self.peak_imin_spinbox.setValue(
            self.settings_manager.get("peak_finding_min_intensity")
        )
        peak_layout.addRow("Min Peak Intensity:", self.peak_imin_spinbox)

        # Median Filter Size
        self.peak_median_filter_combobox = QtWidgets.QComboBox()
        current_filter_size = self.settings_manager.get(
            "peak_finding_median_filter_size"
        )
        self.peak_median_filter_combobox.addItem("None", None)
        self.peak_median_filter_combobox.addItem("3", 3)
        self.peak_median_filter_combobox.addItem("5", 5)
        index = self.peak_median_filter_combobox.findData(current_filter_size)
        if index == -1:
            index = 0
        self.peak_median_filter_combobox.setCurrentIndex(index)
        peak_layout.addRow("Median Filter Size:", self.peak_median_filter_combobox)

        # Ice Ring Filter Params
        peak_layout.addRow(QtWidgets.QLabel("<b>Ice Ring / False Spot Filtering:</b>"))
        
        self.peak_bin1_max_res_spinbox = QtWidgets.QDoubleSpinBox()
        self.peak_bin1_max_res_spinbox.setRange(1.0, 20.0)
        self.peak_bin1_max_res_spinbox.setDecimals(1)
        self.peak_bin1_max_res_spinbox.setSuffix(" Å")
        self.peak_bin1_max_res_spinbox.setValue(
            self.settings_manager.get("peak_finding_bin1_max_res", 5.5)
        )
        self.peak_bin1_max_res_spinbox.setToolTip("Upper bound of the first resolution bin (e.g. 20.0 - 5.5 Å)")
        peak_layout.addRow("First Bin Max Res:", self.peak_bin1_max_res_spinbox)

        self.peak_bin1_min_count_spinbox = QtWidgets.QSpinBox()
        self.peak_bin1_min_count_spinbox.setRange(0, 100)
        self.peak_bin1_min_count_spinbox.setValue(
            self.settings_manager.get("peak_finding_bin1_min_count", 2)
        )
        self.peak_bin1_min_count_spinbox.setToolTip("Minimum number of spots required in the first bin to accept results")
        peak_layout.addRow("First Bin Min Spots:", self.peak_bin1_min_count_spinbox)

        form_layout.addRow(peak_group)
        layout.addLayout(form_layout)

        # Connect signals
        self.peak_low_res_spinbox.valueChanged.connect(self._on_param_changed)
        self.peak_high_res_spinbox.valueChanged.connect(self._on_param_changed)
        self.peak_zscore_spinbox.valueChanged.connect(self._on_param_changed)
        self.peak_num_spinbox.valueChanged.connect(self._on_param_changed)
        self.peak_dist_spinbox.valueChanged.connect(self._on_param_changed)
        self.peak_min_pixels_spinbox.valueChanged.connect(self._on_param_changed)
        self.peak_imin_spinbox.valueChanged.connect(self._on_param_changed)
        self.peak_median_filter_combobox.currentIndexChanged.connect(
            self._on_param_changed
        )
        self.peak_bin1_max_res_spinbox.valueChanged.connect(self._on_param_changed)
        self.peak_bin1_min_count_spinbox.valueChanged.connect(self._on_param_changed)

        # Debounce timer
        self._debounce_timer = QTimer(self)
        self._debounce_timer.setSingleShot(True)
        self._debounce_timer.timeout.connect(self._emit_settings_changed)

    def _on_param_changed(self, *args):
        self._debounce_timer.start(200)

    def _emit_settings_changed(self):
        new_settings = {
            "peak_finding_low_resolution_A": self.peak_low_res_spinbox.value(),
            "peak_finding_high_resolution_A": self.peak_high_res_spinbox.value(),
            "peak_finding_zscore_cutoff": self.peak_zscore_spinbox.value(),
            "peak_finding_num_peaks": self.peak_num_spinbox.value(),
            "peak_finding_min_distance": self.peak_dist_spinbox.value(),
            "peak_finding_min_pixels": self.peak_min_pixels_spinbox.value(),
            "peak_finding_min_intensity": self.peak_imin_spinbox.value(),
            "peak_finding_median_filter_size": self.peak_median_filter_combobox.currentData(),
            "peak_finding_bin1_max_res": self.peak_bin1_max_res_spinbox.value(),
            "peak_finding_bin1_min_count": self.peak_bin1_min_count_spinbox.value(),
        }
        self.peak_params_changed.emit(new_settings)
