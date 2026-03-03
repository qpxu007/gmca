from PyQt5.QtCore import pyqtSignal, QTimer
from PyQt5.QtWidgets import QVBoxLayout, QFormLayout, QComboBox, QSpinBox, QLabel

from qp2.image_viewer.ui.singleton_dialog import SingletonDialog


class FilterSettingsDialog(SingletonDialog):
    filter_params_changed = pyqtSignal(str, int)  # filter_type, se_size

    def __init__(self, current_filter_type, current_se_size, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Image Filter Settings")
        self.setModal(False)
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.filter_combo = QComboBox()
        # Only include fast and medium-speed filters
        # Removed very slow filters: Radial Background Removal, Radial Spot Enhancement, Radial Top-hat
        # Removed slow filters: Matched Filter, Beam Center Correction
        filters = [
            "Poisson Threshold",  # Very Fast
            "Radial Poisson Threshold",
            "Local Background Subtraction",  # Medium
            "Laplacian of Gaussian",  # Medium
            "Difference of Gaussians",  # Medium
            "Radial Background Removal",  # slow

            "Maximum", "Smooth", "Gaussian Smooth", "Median",  # Fast
            "Dilation", "Erosion", "Opening", "Closing",  # Fast
            "Spot Enhancement", "Spot Detection",  # Medium
            "Spot Sharpening", "Spot Contrast",  # Medium
            "Top-hat Filter",
            "Bandpass Filter",  # Medium
            "CLAHE Enhancement",  # Medium (does NOT use SE Size)
            "Visual Spot Enhancement",  # Custom visual filter
            "Radial Spot Enhancement",
            "Beam Center Correction",
            "Radial Top-hat",
            "Experimental",  # Niblack threshold filter
            "Cut-off Filter",
            # "Label",  # Removed filter for labeling
        ]
        self.filter_combo.addItems(filters)
        self.filter_combo.setCurrentText(current_filter_type)
        form.addRow(QLabel("Filter Type:"), self.filter_combo)

        self.se_size_spinbox = QSpinBox()
        self.se_size_spinbox.setRange(1, 49)
        self.se_size_spinbox.setSingleStep(2)  # Enforce upper bound of 49
        self.se_size_spinbox.setValue(current_se_size)
        self.param_label = QLabel("SE Size:") # Keep reference to update text
        form.addRow(self.param_label, self.se_size_spinbox)

        layout.addLayout(form)
        self.setLayout(layout)

        self.filter_combo.currentTextChanged.connect(self._on_filter_type_changed)
        self.se_size_spinbox.valueChanged.connect(self._on_param_changed)

        # Debounce timer for parameter changes
        self._debounce_timer = QTimer(self)
        self._debounce_timer.setSingleShot(True)
        self._debounce_timer.timeout.connect(self._emit_params_changed)

        # Track if user has manually changed se_size
        self._user_changed_se_size = False

        # Set initial state
        self._on_filter_type_changed(self.filter_combo.currentText())

    def _on_param_changed(self, *args):
        self._user_changed_se_size = True
        # Start or restart the debounce timer (e.g., 200ms delay)
        self._debounce_timer.start(200)

    def _emit_params_changed(self):
        self.filter_params_changed.emit(self.filter_combo.currentText(), self.se_size_spinbox.value())

    def set_params(self, filter_type, se_size):
        self.filter_combo.setCurrentText(filter_type)
        self.se_size_spinbox.setValue(se_size)
        self._user_changed_se_size = False
        self._on_filter_type_changed(filter_type)

    def _on_filter_type_changed(self, filter_type):
        # Disable SE Size for CLAHE Enhancement, enable otherwise
        if filter_type == "CLAHE Enhancement":
            self.se_size_spinbox.setEnabled(False)
        else:
            self.se_size_spinbox.setEnabled(True)

        # Handle Cut-off Filter specific UI changes
        if filter_type == "Cut-off Filter":
            self.param_label.setText("Cutoff Value:")
            self.se_size_spinbox.setRange(0, 1000000) # Allow large range
            self.se_size_spinbox.setSingleStep(10)
        else:
            self.param_label.setText("SE Size:")
            self.se_size_spinbox.setRange(1, 49)
            self.se_size_spinbox.setSingleStep(2)
        
        # Set filter-dependent default se_size if user hasn't changed it
        default_se_size = 5
        if filter_type == "Maximum":
            default_se_size = 20
        elif filter_type == "Cut-off Filter":
            default_se_size = 10 # Default cutoff

        # Clamp to upper bound
        if default_se_size > self.se_size_spinbox.maximum():
            default_se_size = self.se_size_spinbox.maximum()
        # If user hasn't changed se_size, set to default for this filter
        if not self._user_changed_se_size:
            self.se_size_spinbox.setValue(default_se_size)
        # If switching away from Maximum and user hasn't changed se_size, restore to overall default
        elif self._user_changed_se_size and filter_type != "Maximum" and filter_type != "Cut-off Filter":
            # If previous filter was Maximum and se_size is 20, reset to 5
            if self.se_size_spinbox.value() == 20:
                self.se_size_spinbox.setValue(5)
            # If previous was Cut-off and value is presumably large/different, maybe reset? 
            # Ideally we only reset if it looks like a left-over value from the special filter
            if self.se_size_spinbox.value() > 49:
                self.se_size_spinbox.setValue(5)

        # If user set a very large value (e.g. from Cutoff), clamp it when switching back
        if self.se_size_spinbox.value() > self.se_size_spinbox.maximum():
            self.se_size_spinbox.setValue(self.se_size_spinbox.maximum())
            
        self._emit_params_changed()
