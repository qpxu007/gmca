# qp2/image_viewer/plugins/spot_finder/spot_finder_manager.py

from PyQt5 import QtCore, QtWidgets

from qp2.image_viewer.config import DOZOR_PLOT_REFRESH_INTERVAL
from qp2.image_viewer.plugins.generic_plot_manager import GenericPlotManager
from qp2.image_viewer.plugins.spot_finder.find_spots_worker import PeakFinderDataFileWorker

REDIS_SPOTFINDER_KEY_PREFIX = "analysis:out:spots:spotfinder"


class SpotFinderManager(GenericPlotManager):
    def __init__(self, parent):
        spot_finder_config = {
            "worker_class": PeakFinderDataFileWorker,
            "redis_connection": parent.redis_output_server,
            "redis_key_template": f"{REDIS_SPOTFINDER_KEY_PREFIX}:{{master_file}}",
            "spot_field_key": "spots",
            "x_axis_key": "img_num",
            "default_y_axis": "num_spots",
            "refresh_interval_ms": DOZOR_PLOT_REFRESH_INTERVAL,
            "default_source_type": "redis",
            "status_key_type": "hash",
        }
        super().__init__(parent=parent, name="Spot Finder", config=spot_finder_config)

    def _setup_ui(self):
        """Overrides the base method to add a settings button."""
        super()._setup_ui()  # Call the parent's setup first

        # Create the settings button
        self.settings_button = QtWidgets.QPushButton("⚙️ Settings")
        self.settings_button.setToolTip("Open Live Spot Finder Settings")
        # self.settings_button.setFixedSize(QtCore.QSize(30, 25))

        # Find the layout of the control bar (it's the first item in the container's layout)
        control_bar_layout = self.container_widget.layout().itemAt(0).layout()

        # Insert the button before the "Actions" button for consistency
        actions_button_index = control_bar_layout.indexOf(self.actions_button)
        control_bar_layout.insertWidget(actions_button_index, self.settings_button)

        # Connect the button's clicked signal
        self.settings_button.clicked.connect(self._open_spot_finder_settings)

    def _open_spot_finder_settings(self):
        """Asks the LivePeakFindingManager to open its settings dialog."""
        self.main_window.live_peak_finding_manager.open_settings_dialog()

    def _prepare_worker_kwargs(self) -> dict:
        """
        Overrides the base method to provide the specific keyword arguments
        required by the PeakFinderDataFileWorker.
        """
        # This manager's specific duty is to call the main window's
        # centralized helper function for getting peak finding parameters.
        return self.main_window.get_peak_finder_kwargs()
