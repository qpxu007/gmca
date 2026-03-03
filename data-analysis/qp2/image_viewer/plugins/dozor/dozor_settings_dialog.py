# qp2/image_viewer/plugins/dozor/dozor_settings_dialog.py

from PyQt5.QtCore import pyqtSignal
from pyqtgraph.Qt import QtWidgets

from qp2.image_viewer.ui.singleton_dialog import SingletonDialog


class DozorSettingsDialog(SingletonDialog):
    """
    A dialog for editing Dozor-specific settings.
    """

    settings_changed = pyqtSignal(dict)

    def __init__(self, current_settings: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Dozor Settings")
        self.setModal(False)
        self.new_settings = current_settings.copy()

        layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()

        # --- Beamstop Size Setting ---
        self.beamstop_size_spinbox = QtWidgets.QSpinBox()
        self.beamstop_size_spinbox.setRange(10, 500)
        self.beamstop_size_spinbox.setSingleStep(10)
        self.beamstop_size_spinbox.setSuffix(" px")
        self.beamstop_size_spinbox.setValue(
            self.new_settings.get("dozor_beamstop_size", 100)
        )
        self.beamstop_size_spinbox.setToolTip(
            "Size of the square mask around the beam center to exclude from spot finding."
        )
        form_layout.addRow("Beamstop Size:", self.beamstop_size_spinbox)

        # --- Spot Size Setting ---
        self.spot_size_spinbox = QtWidgets.QSpinBox()
        self.spot_size_spinbox.setRange(1, 10)
        self.spot_size_spinbox.setValue(self.new_settings.get("dozor_spot_size", 3))
        self.spot_size_spinbox.setToolTip(
            "Expected spot size in pixels for Dozor's algorithm."
        )
        form_layout.addRow("Spot Size:", self.spot_size_spinbox)

        self.spot_level_spinbox = QtWidgets.QSpinBox()
        self.spot_level_spinbox.setRange(1, 20)
        self.spot_level_spinbox.setValue(self.new_settings.get("dozor_spot_level", 6))
        self.spot_level_spinbox.setToolTip(
            "Expected spot level in sigma for Dozor's algorithm."
        )
        form_layout.addRow("Spot level:", self.spot_level_spinbox)

        # --- Batch Size Multipliers ---
        # Live Mode
        self.live_batch_mult_spinbox = QtWidgets.QDoubleSpinBox()
        self.live_batch_mult_spinbox.setRange(1, 100.0)
        self.live_batch_mult_spinbox.setSingleStep(0.5)
        self.live_batch_mult_spinbox.setValue(
            self.new_settings.get("dozor_live_batch_multiplier", 5.0)
        )
        self.live_batch_mult_spinbox.setToolTip(
            "In Live Mode, batch size = Multiplier * (Images per HDF5 file)."
        )
        form_layout.addRow("Live Batch Multiplier:", self.live_batch_mult_spinbox)

        # Rerun Mode
        self.rerun_batch_mult_spinbox = QtWidgets.QDoubleSpinBox()
        self.rerun_batch_mult_spinbox.setRange(1, 100.0)
        self.rerun_batch_mult_spinbox.setSingleStep(0.5)
        self.rerun_batch_mult_spinbox.setValue(
            self.new_settings.get("dozor_rerun_batch_multiplier", 10.0)
        )
        self.rerun_batch_mult_spinbox.setToolTip(
            "For Reruns, batch size = Multiplier * (Images per HDF5 file)."
        )
        form_layout.addRow("Rerun Batch Multiplier:", self.rerun_batch_mult_spinbox)

        # --- New Dozor Parameters ---
        # Dist Cutoff
        self.dist_cutoff_spinbox = QtWidgets.QDoubleSpinBox()
        self.dist_cutoff_spinbox.setRange(0.0, 1000.0)
        self.dist_cutoff_spinbox.setValue(self.new_settings.get("dozor_dist_cutoff", 20.0))
        self.dist_cutoff_spinbox.setToolTip("Distance cutoff in pixels.")
        form_layout.addRow("Dist Cutoff (pixels):", self.dist_cutoff_spinbox)

        # Res Cutoff Low
        self.res_cutoff_low_spinbox = QtWidgets.QDoubleSpinBox()
        self.res_cutoff_low_spinbox.setRange(0.0, 100.0)
        self.res_cutoff_low_spinbox.setValue(self.new_settings.get("dozor_res_cutoff_low", 20.0))
        self.res_cutoff_low_spinbox.setToolTip("Low resolution cutoff in Angstroms.")
        form_layout.addRow("Res Cutoff Low (Å):", self.res_cutoff_low_spinbox)

        # Res Cutoff High
        self.res_cutoff_high_spinbox = QtWidgets.QDoubleSpinBox()
        self.res_cutoff_high_spinbox.setRange(0.2, 100.0)
        self.res_cutoff_high_spinbox.setValue(self.new_settings.get("dozor_res_cutoff_high", 2.))
        self.res_cutoff_high_spinbox.setToolTip("High resolution cutoff in Angstroms.")
        form_layout.addRow("Res Cutoff High (Å):", self.res_cutoff_high_spinbox)

        # Check Ice Rings
        self.check_ice_rings_checkbox = QtWidgets.QCheckBox()
        self.check_ice_rings_checkbox.setChecked(self.new_settings.get("dozor_check_ice_rings", "T") == "T")
        self.check_ice_rings_checkbox.setToolTip("Enable/disable ice ring checking.")
        form_layout.addRow("Check Ice Rings:", self.check_ice_rings_checkbox)

        # Exclude Resolution Ranges
        self.exclude_res_ranges_textedit = QtWidgets.QPlainTextEdit()
        ranges = self.new_settings.get("dozor_exclude_resolution_ranges", [])
        ranges_str = "\n".join([f"{r[0]} {r[1]}" for r in ranges if len(r) == 2])
        self.exclude_res_ranges_textedit.setPlainText(ranges_str)
        self.exclude_res_ranges_textedit.setPlaceholderText("e.g.\n2.25 2.30\n3.85 3.95")
        self.exclude_res_ranges_textedit.setToolTip("Resolution ranges to exclude, one per line (e.g., '2.25 2.30').")
        form_layout.addRow("Exclude Res Ranges:", self.exclude_res_ranges_textedit)

        # --- New Spot Filtering Parameters ---
        self.min_spot_range_low_spinbox = QtWidgets.QDoubleSpinBox()
        self.min_spot_range_low_spinbox.setRange(0.0, 1000.0)
        self.min_spot_range_low_spinbox.setValue(self.new_settings.get("dozor_min_spot_range_low", 15.0))
        self.min_spot_range_low_spinbox.setToolTip("Low resolution limit for spot counting (e.g. 15.0).")
        form_layout.addRow("Min Spot Range Low (Å):", self.min_spot_range_low_spinbox)

        self.min_spot_range_high_spinbox = QtWidgets.QDoubleSpinBox()
        self.min_spot_range_high_spinbox.setRange(0.0, 100.0)
        self.min_spot_range_high_spinbox.setValue(self.new_settings.get("dozor_min_spot_range_high", 4.0))
        self.min_spot_range_high_spinbox.setToolTip("High resolution limit for spot counting (e.g. 4.0).")
        form_layout.addRow("Min Spot Range High (Å):", self.min_spot_range_high_spinbox)

        self.min_spot_count_spinbox = QtWidgets.QSpinBox()
        self.min_spot_count_spinbox.setRange(0, 1000)
        self.min_spot_count_spinbox.setValue(self.new_settings.get("dozor_min_spot_count", 2))
        self.min_spot_count_spinbox.setToolTip("Minimum number of spots required within the range.")
        form_layout.addRow("Min Spot Count:", self.min_spot_count_spinbox)

        # Debug Mode
        self.debug_checkbox = QtWidgets.QCheckBox()
        self.debug_checkbox.setChecked(self.new_settings.get("dozor_debug", False))
        self.debug_checkbox.setToolTip("Enable debug mode (keeps temporary files).")
        form_layout.addRow("Debug:", self.debug_checkbox)

        layout.addLayout(form_layout)

        # --- Buttons ---
        button_box = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def accept(self):
        """Called when the OK button is clicked."""
        self.new_settings["dozor_beamstop_size"] = self.beamstop_size_spinbox.value()
        self.new_settings["dozor_spot_size"] = self.spot_size_spinbox.value()
        self.new_settings["dozor_spot_level"] = self.spot_level_spinbox.value()
        self.new_settings["dozor_live_batch_multiplier"] = self.live_batch_mult_spinbox.value()
        self.new_settings["dozor_rerun_batch_multiplier"] = self.rerun_batch_mult_spinbox.value()

        # Save new Dozor parameters
        self.new_settings["dozor_dist_cutoff"] = self.dist_cutoff_spinbox.value()
        self.new_settings["dozor_res_cutoff_low"] = self.res_cutoff_low_spinbox.value()
        self.new_settings["dozor_res_cutoff_high"] = self.res_cutoff_high_spinbox.value()
        self.new_settings["dozor_check_ice_rings"] = "T" if self.check_ice_rings_checkbox.isChecked() else "F"
        self.new_settings["dozor_min_spot_range_low"] = self.min_spot_range_low_spinbox.value()
        self.new_settings["dozor_min_spot_range_high"] = self.min_spot_range_high_spinbox.value()
        self.new_settings["dozor_min_spot_count"] = self.min_spot_count_spinbox.value()
        self.new_settings["dozor_debug"] = self.debug_checkbox.isChecked()

        ranges_text = self.exclude_res_ranges_textedit.toPlainText()
        ranges = []
        for line in ranges_text.splitlines():
            parts = line.split()
            if len(parts) == 2:
                try:
                    ranges.append([float(parts[0]), float(parts[1])])
                except ValueError:
                    pass
        self.new_settings["dozor_exclude_resolution_ranges"] = ranges

        # Emit the changes to be picked up by the main SettingsManager
        self.settings_changed.emit(self.new_settings)
        super().accept()
