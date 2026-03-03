# qp2/image_viewer/plugins/generic_plot_manager.py
import hashlib
import json
import os
import shutil
from typing import Set, Dict, Any, Optional
import time

import numpy as np
import pyqtgraph as pg
import redis
from PyQt5.QtCore import (
    pyqtSlot,
    pyqtSignal,
)
from pyqtgraph.Qt import QtCore, QtWidgets

from qp2.image_viewer.plugins.peelable_plot_dialog import PeelablePlotDialog
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class GenericPlotManager(QtCore.QObject):
    """
    A generic base class to manage a plot that monitors data from Redis or a file,
    triggers processing workers, and allows user interaction. This class is designed
    to be subclassed by specialized managers (e.g., DozorManager, SpotFinderManager).
    """

    # --- Signals to communicate back to the main window ---
    status_update = pyqtSignal(str, int)  # message, timeout
    frame_selected = pyqtSignal(int)  # 0-based frame index
    request_spots_display = pyqtSignal(object)  # np.ndarray of spot coordinates
    request_main_threadpool = pyqtSignal(
        object
    )  # Pass a QRunnable to the main threadpool
    data_source_changed = pyqtSignal(
        str, str
    )  # source_type (redis/file), path/key_template

    def __init__(self, parent, name: str, config: Dict[str, Any]):
        """
        Initializes the GenericPlotManager.

        Args:
            parent: The parent object, typically the main window.
            name: The name of this manager instance (e.g., "Dozor").
            config: A dictionary containing specific configurations for the plugin.
        """
        super().__init__(parent)
        self.main_window = parent
        self.name = name
        self.config = config

        # --- Data Sources & State ---
        self.reader = None
        self.current_master_file = None
        self.redis_connection = self.config.get("redis_connection")
        self.processed_segments: Set = set()
        self.run_processing_enabled = True

        # Current active data source configuration
        self._current_data_source_type = self.config.get("default_source_type", "redis")
        self._current_data_source_path_or_key = self.config.get(
            "redis_key_template", ""
        ).format(master_file="<master_file>")

        # --- Plot Data and State ---
        self.plot_data = []
        self.plot_data_item = None
        self.numeric_fields = []
        self.processed_data_len = 0  # Used for Redis to track list length
        self.plot_y_axis_key = self.config.get("default_y_axis", "")
        self.plot_x_manual_range = False

        # --- UI Components and Timer ---
        self._setup_ui()
        self.plot_timer = QtCore.QTimer()
        self.plot_timer.setInterval(self.config.get("refresh_interval_ms", 2000))
        self.plot_timer.timeout.connect(self._refresh_data_and_plot)
        self._connect_internal_signals()

        self.main_ui_splitter = self.main_window.ui_manager.right_panel_splitter

    def _configure_plot_style(self):
        """
        Sets the visual style for the plot widget, including axes.
        This method can be overridden by subclasses for custom styling.
        """
        if not self.plot_widget:
            return

        # This code is restored from the original DozorManager
        plot_item = self.plot_widget.getPlotItem()
        if not plot_item:
            return

        # Set a consistent black color for the axis lines and labels
        axis_pen = pg.mkPen(color="k", width=1)

        for axis_name in ["left", "bottom"]:
            axis = plot_item.getAxis(axis_name)
            if axis:
                axis.setPen(axis_pen)
                axis.setTextPen(axis_pen)

        # Optional: Add some padding to the right of the plot to prevent
        # the last tick label from being cut off.
        view_box = plot_item.getViewBox()
        if view_box:  # and hasattr(view_box, "setPixelPadding"):
            # view_box.setPixelPadding(right=15)
            view_box.setDefaultPadding(0.02)

    def _setup_ui(self):
        """Creates a single, compact control bar for the manager's widget."""
        self.container_widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(self.container_widget)
        # Use smaller margins for a tighter look
        layout.setContentsMargins(2, 2, 2, 2)
        layout.setSpacing(4)  # Reduce spacing between control bar and plot

        # --- THE NEW SINGLE CONTROL BAR ---
        control_bar_layout = QtWidgets.QHBoxLayout()

        # 1. The Title Label (Name of the plugin)
        self.title_label = QtWidgets.QLabel(f"<b>{self.name}</b>")
        control_bar_layout.addWidget(self.title_label)

        # job status
        self.status_label = QtWidgets.QLabel("Status: Idle")
        self.status_label.setMinimumWidth(200)
        self.status_label.setStyleSheet("padding: 2px; border-radius: 3px;")
        self._set_status_label("Idle")  # Set initial style
        control_bar_layout.addWidget(self.status_label)

        # 2. Add a small spacer
        control_bar_layout.addSpacing(10)

        # 3. Y-Axis Label and Combobox
        y_axis_label = QtWidgets.QLabel("Y-Axis:")
        self.metric_combobox = QtWidgets.QComboBox()
        self.metric_combobox.setToolTip("Select the metric to display on the Y-Axis")
        # Make the combobox expand to fill available space
        self.metric_combobox.setSizePolicy(
            QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Preferred
        )
        control_bar_layout.addWidget(y_axis_label)
        control_bar_layout.addWidget(self.metric_combobox)

        # 4. Add a stretch to push buttons to the right
        control_bar_layout.addStretch(1)

        # 5. The Buttons (use shorter text or icons)
        self.peel_button = QtWidgets.QPushButton("Peel")
        self.peel_button.setToolTip("Detach/Dock Plot")
        self.peel_button.setFixedSize(QtCore.QSize(45, 25))

        self.reset_button = QtWidgets.QPushButton("Reset")
        self.reset_button.setToolTip("Reset Plot View")
        self.reset_button.setFixedSize(QtCore.QSize(50, 25))

        control_bar_layout.addWidget(self.peel_button)
        control_bar_layout.addWidget(self.reset_button)

        self.actions_button = QtWidgets.QPushButton("Actions ▾")
        self.actions_button.setToolTip("Show analysis actions")
        self.actions_button.setFixedSize(QtCore.QSize(80, 25))

        # Create the QMenu that will be the popup
        actions_menu = QtWidgets.QMenu(self.actions_button)

        # Create the actions
        refresh_action = actions_menu.addAction("Refresh Plot")
        actions_menu.addSeparator()
        clear_action = actions_menu.addAction("Clear Redis Results")
        rerun_action = actions_menu.addAction("Clear and Re-run")

        # Connect the actions' triggered signals to our existing slots
        refresh_action.triggered.connect(self._manual_refresh_triggered)
        clear_action.triggered.connect(self._confirm_and_clear_redis_results)
        rerun_action.triggered.connect(
            lambda: self._confirm_and_clear_redis_results(and_rerun=True)
        )

        # Tell the button to show the menu when clicked
        self.actions_button.setMenu(actions_menu)

        # Add a custom signal to the menu to check before showing
        actions_menu.aboutToShow.connect(
            lambda: self._update_actions_menu_state(clear_action, rerun_action)
        )

        control_bar_layout.addWidget(self.actions_button)

        # Add the new control bar to the main layout
        layout.addLayout(control_bar_layout)

        # The Plot Widget remains the same
        self.plot_widget = pg.PlotWidget(background="w")
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.plot_widget.setLabel("bottom", "Frame Number")
        layout.addWidget(self.plot_widget, stretch=1)  # Give it all the stretch

        self._configure_plot_style()  # Keep this for styling axes

        (
            self.peeled_dialog,
            self.original_splitter_index,
            self.original_splitter_sizes,
        ) = (None, 1, [])

    def _update_actions_menu_state(self, clear_action, rerun_action):
        """Called just before the actions menu is shown to enable/disable items."""
        is_redis_mode = (
            self._current_data_source_type == "redis"
            and self.redis_connection is not None
        )
        clear_action.setEnabled(is_redis_mode)
        rerun_action.setEnabled(is_redis_mode)

    @pyqtSlot()
    def _manual_refresh_triggered(self):
        """Slot for the manual refresh action."""
        self._refresh_data_and_plot()
        self.status_update.emit(f"[{self.name}] Plot refreshed.", 2000)

    def _connect_internal_signals(self):
        """Connects signals for internal UI components."""
        self.metric_combobox.currentIndexChanged.connect(self._update_plot_display)
        self.peel_button.clicked.connect(self.toggle_peel_plot)
        self.reset_button.clicked.connect(self.reset_plot_view)

        view_box = self.plot_widget.getViewBox()

        view_box.sigRangeChangedManually.connect(
            lambda: setattr(self, "plot_x_manual_range", True)
        )

    def get_widget(self) -> QtWidgets.QWidget:
        """Returns the main container widget for this manager to be placed in the UI."""
        return self.container_widget

    def update_crystal_parameters(self, params: Dict[str, Any]):
        """
        Receives crystallographic parameters and updates the manager's state.
        Subclasses should override this to update their specific settings UI/logic.
        """
        logger.info(f"[{self.name}] Received crystal parameters: {params}")
        # Base implementation does nothing, subclasses should override.

    def update_source(self, new_reader, new_master_file):
        """Updates the manager with a new data source (HDF5Reader)."""
        if self.reader and hasattr(self.reader, "data_files_ready_batch"):
            try:
                self.reader.data_files_ready_batch.disconnect(
                    self.handle_data_files_ready
                )
            except (TypeError, RuntimeError):
                pass  # Already disconnected or never connected

        self.reader = new_reader
        self.current_master_file = new_master_file

        if self._current_data_source_type == "redis" and self.current_master_file:
            # Update the Redis key now that we know the master file
            self._current_data_source_path_or_key = self.config[
                "redis_key_template"
            ].format(master_file=self.current_master_file)
            self.data_source_changed.emit(
                self._current_data_source_type, self._current_data_source_path_or_key
            )

        self.init_plot_for_new_source()

        # Connect to the signal for any FUTURE data files
        if self.reader and hasattr(self.reader, "data_files_ready_batch"):
            self.reader.data_files_ready_batch.connect(self.handle_data_files_ready)

        # After connecting, check if data is already available and process it.
        # This handles the case where the file was loaded before the plugin was selected.
        # Only do this in live mode to prevent auto-running on old datasets.
        if self.reader and self.reader.total_frames > 0 and self.main_window.is_live_mode:
            self.process_existing_data()

        if self.current_master_file:
            self.main_window._fetch_and_apply_crystal_data(self.current_master_file)

    def process_existing_data(self):
        """
        Checks for already-available data files from the reader and triggers
        the 'handle_data_files_ready' slot manually.
        """
        if not self.reader:
            return

        # The HDF5Reader builds a 'frame_map' of all expected data files.
        # We can use this map to create the 'files_batch' payload.

        all_available_files_batch = []

        # Check which of the expected files actually exist on disk right now.
        for start_idx, end_idx, fpath, _ in self.reader.frame_map:
            if os.path.exists(fpath):
                file_info = {
                    "file_path": fpath,
                    "start_frame": start_idx,
                    "end_frame": end_idx - 1,  # The handler expects inclusive end frame
                    "metadata": self.reader.get_parameters(),  # Pass the reader's full metadata
                }
                all_available_files_batch.append(file_info)

        if all_available_files_batch:
            self.status_update.emit(
                f"[{self.name}] Found {len(all_available_files_batch)} existing data segments. Starting processing...",
                4000,
            )
            # Manually call the slot with the batch of existing files.
            # We use a singleShot timer to ensure this happens in the next event loop cycle,
            # which is slightly safer than a direct call right after initialization.
            QtCore.QTimer.singleShot(
                50, lambda: self.handle_data_files_ready(all_available_files_batch)
            )

    def init_plot_for_new_source(self):
        """Resets and prepares the plot for a new data source."""
        self.processed_segments.clear()
        self.plot_timer.stop()
        self.plot_y_axis_key = self.config.get("default_y_axis", "")
        self.plot_data, self.numeric_fields, self.processed_data_len = [], [], 0
        self.plot_x_manual_range = False

        if self.plot_data_item:
            self.plot_widget.removeItem(self.plot_data_item)
            self.plot_data_item = None

        self.plot_widget.setTitle(f"{self.name} Analysis")
        self._update_metric_combobox()

        total_frames = self.reader.total_frames if self.reader else 0
        self.plot_widget.getPlotItem().setLimits(
            xMin=0, xMax=total_frames + 1 if total_frames > 0 else 100
        )
        self.plot_widget.setXRange(
            0, total_frames + 1 if total_frames > 1 else 2, padding=0
        )

        self._refresh_data_and_plot()
        self.plot_timer.start()

    def _refresh_data_and_plot(self):
        """Periodic task to fetch new data and update the plot if data has changed."""
        self._update_job_status_display()

        if self._fetch_and_prepare_data():
            self._update_plot_display()

    def _set_data_source(self, source_type: str, path_or_key: str):
        """Sets the active data source and triggers a full refresh."""
        if (
            self._current_data_source_type == source_type
            and self._current_data_source_path_or_key == path_or_key
        ):
            return

        self._current_data_source_type = source_type
        self._current_data_source_path_or_key = path_or_key

        self.plot_data, self.numeric_fields, self.processed_data_len = [], [], 0
        self._update_metric_combobox()

        # --- ADDED: Tell the main window to clear any spots from the old source ---
        self.request_spots_display.emit(None)

        self.status_update.emit(
            f"[{self.name}] Data source set to: {source_type.capitalize()} ({os.path.basename(path_or_key)})",
            3000,
        )
        self.data_source_changed.emit(source_type, path_or_key)

        self._refresh_data_and_plot()
        self._update_source_button_text()

    def _prepare_worker_kwargs(self) -> dict:
        """
        Gathers worker-specific keyword arguments.
        Subclasses that need to pass special parameters to their workers
        (like spot finding settings) should override this method.

        Returns:
            A dictionary of keyword arguments to be passed to the worker.
        """
        # The base implementation returns an empty dictionary, which is safe for
        # workers like DozorWorker that don't need extra parameters.
        return {}

    @pyqtSlot(list)
    def handle_data_files_ready(self, files_batch: list):
        """Receives new data files and starts processing workers if configured."""
        worker_class = self.config.get("worker_class")
        if not worker_class or not self.run_processing_enabled:
            return

        worker_kwargs = self._prepare_worker_kwargs()
        if worker_kwargs is None:
            logger.error(
                f"[{self.name}] Aborting batch processing, could not get valid peak finder parameters."
            )
            return

        for file_info in files_batch:
            segment_id = (
                file_info["metadata"].get("master_file"),
                file_info["start_frame"],
                file_info["end_frame"],
            )
            if segment_id in self.processed_segments:
                continue

            self.processed_segments.add(segment_id)

            # file_info["metadata"]["hdf5_reader_instance"] = self.main_window.reader
            file_info["metadata"]["params"] = self.main_window.params
            file_info["metadata"]["detector_mask"] = self.main_window.detector_mask

            worker = worker_class(
                file_path=file_info["file_path"],
                start_frame=file_info["start_frame"],
                end_frame=file_info["end_frame"],
                metadata=file_info["metadata"],
                redis_conn=self.redis_connection,
                redis_key_prefix=self.config.get("redis_key_template", "").split(
                    ":{master_file}"
                )[0],
                **worker_kwargs,
            )

            if hasattr(worker, "signals"):
                if hasattr(worker.signals, "error"):
                    worker.signals.error.connect(self._handle_worker_error)
                if hasattr(worker.signals, "result"):
                    worker.signals.result.connect(self._handle_worker_result)

            self.request_main_threadpool.emit(worker)

    def _handle_worker_result(self, file_path, status_code, message):
        self.status_update.emit(
            f"{self.name} worker for {os.path.basename(file_path)}: {message}", 4000
        )

    def _handle_worker_error(self, file_path, error_message):
        self.status_update.emit(
            f"ERROR in {self.name} worker for {os.path.basename(file_path)}: {error_message}",
            8000,
        )

    def _prompt_data_source_selection(self):
        """Shows a dialog for the user to choose between Redis and local file."""
        dialog = QtWidgets.QDialog(self.main_window)
        dialog.setWindowTitle(f"Select {self.name} Data Source")
        layout = QtWidgets.QVBoxLayout(dialog)

        radio_redis = QtWidgets.QRadioButton("Live from Redis Stream")
        radio_file = QtWidgets.QRadioButton("From Local JSON File")

        if self._current_data_source_type == "redis":
            radio_redis.setChecked(True)
        else:
            radio_file.setChecked(True)

        layout.addWidget(radio_redis)
        layout.addWidget(radio_file)

        file_path_layout = QtWidgets.QHBoxLayout()
        self.file_path_label = QtWidgets.QLineEdit(
            self._current_data_source_path_or_key
            if self._current_data_source_type == "file"
            else ""
        )
        self.file_path_label.setReadOnly(True)
        self.browse_file_button = QtWidgets.QPushButton("Browse...")
        file_path_layout.addWidget(self.file_path_label)
        file_path_layout.addWidget(self.browse_file_button)
        layout.addLayout(file_path_layout)

        def toggle_file_widgets_visibility():
            is_file_selected = radio_file.isChecked()
            self.file_path_label.setVisible(is_file_selected)
            self.browse_file_button.setVisible(is_file_selected)

        radio_redis.toggled.connect(toggle_file_widgets_visibility)
        toggle_file_widgets_visibility()
        self.browse_file_button.clicked.connect(self._open_file_dialog)

        buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel,
            QtCore.Qt.Horizontal,
            dialog,
        )
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        if dialog.exec_() == QtWidgets.QDialog.Accepted:
            if radio_redis.isChecked():
                key = self.config["redis_key_template"].format(
                    master_file=(
                        self.current_master_file
                        if self.current_master_file
                        else "<master_file>"
                    )
                )
                self._set_data_source("redis", key)
            else:
                chosen_path = self.file_path_label.text()
                if chosen_path and os.path.exists(chosen_path):
                    self._set_data_source("file", chosen_path)
                else:
                    self.status_update.emit(
                        f"[{self.name}] Invalid file path. Source not changed.", 3000
                    )

        self._update_source_button_text()

    def _open_file_dialog(self):
        """Helper to open a file dialog for selecting a JSON data source."""
        last_dir = (
            os.path.dirname(self.file_path_label.text())
            if self.file_path_label.text()
            else os.getcwd()
        )
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self.main_window,
            f"Select {self.name} Data JSON File",
            last_dir,
            "JSON Files (*.json);;All Files (*)",
        )
        if file_path:
            self.file_path_label.setText(file_path)

    def _update_available_metrics(self):
        """Updates the list of numeric fields available for plotting from the current data."""
        if not self.plot_data:
            if self.numeric_fields:
                self.numeric_fields = []
                self._update_metric_combobox()
            return

        first_item = self.plot_data[0]
        x_key = self.config.get("x_axis_key", "img_num")
        potential_fields = sorted(
            [
                k
                for k, v in first_item.items()
                if isinstance(v, (int, float)) and k != x_key
            ]
        )

        if potential_fields != self.numeric_fields:
            self.numeric_fields = potential_fields
            self._update_metric_combobox()

    def _update_metric_combobox(self):
        """Populates the metric selection combobox."""
        self.metric_combobox.blockSignals(True)
        self.metric_combobox.clear()
        if not self.numeric_fields:
            self.metric_combobox.addItem("No numeric data")
            self.metric_combobox.setEnabled(False)
        else:
            default_y = self.config.get("default_y_axis")
            if default_y and default_y in self.numeric_fields:
                self.metric_combobox.addItem(default_y)
                for field in self.numeric_fields:
                    if field != default_y:
                        self.metric_combobox.addItem(field)
            else:
                self.metric_combobox.addItems(self.numeric_fields)
            self.metric_combobox.setEnabled(True)
        self.metric_combobox.blockSignals(False)

    def _update_plot_display(self):
        """Updates the plot with the current data and selected metric."""
        selected_metric = self.metric_combobox.currentText()
        self.plot_y_axis_key = selected_metric

        if (
            not self.plot_data
            or not selected_metric
            or selected_metric == "No numeric data"
        ):
            if self.plot_data_item:
                self.plot_data_item.clear()
            return

        x_key = self.config.get("x_axis_key", "img_num")
        x_vals = []
        y_vals = []

        logger.debug(f"[{self.name}] Preparing plot data for metric: {selected_metric} using x-axis: {x_key}")

        for i, item in enumerate(self.plot_data):
            raw_x = item.get(x_key)
            raw_y = item.get(selected_metric)

            if raw_x is None or raw_y is None:
                continue

            try:
                val_x = float(raw_x)
                val_y = float(raw_y)
                if np.isfinite(val_x) and np.isfinite(val_y):
                    x_vals.append(val_x)
                    y_vals.append(val_y)
                else:
                    logger.debug(f"[{self.name}] Skipping non-finite data at index {i}: x={val_x}, y={val_y}")
            except (ValueError, TypeError) as e:
                logger.debug(f"[{self.name}] Skipping invalid data at index {i}: x={raw_x}, y={raw_y}, error={e}")
                continue

        logger.debug(f"[{self.name}] Extracted {len(x_vals)} valid points for plotting.")

        if not x_vals:
            if self.plot_data_item:
                self.plot_data_item.setData([], [])
            return

        if self.plot_data_item is None:
            self.plot_data_item = self.plot_widget.plot(
                pen={"color": "b", "width": 1.5},
                symbol="o",
                symbolSize=8,
                symbolBrush="orange",
                symbolPen="red",
                hoverable=True,
                autoDownsample=True,  # Enable automatic, dynamic downsampling
                downsampleMethod="peak",  # Crucial for not losing important data features
                clipToView=True,  # Essential performance boost: only process visible data
            )
            self.plot_data_item.sigPointsClicked.connect(self._on_plot_point_clicked)

        self.plot_data_item.setData(x=x_vals, y=y_vals, name=selected_metric)
        self.plot_widget.setLabel("left", selected_metric)

        view_box = self.plot_widget.getViewBox()
        logger.debug(f"[{self.name}] Updating plot view for {len(x_vals)} points.")

        if (
            not self.plot_x_manual_range
            and self.reader
            and self.reader.total_frames > 0
        ):
            view_box.enableAutoRange(axis=pg.ViewBox.XAxis, enable=False)
            self.plot_widget.setXRange(1, self.reader.total_frames + 1, padding=1)

        # For multiple points, auto-ranging the Y-axis is best.
        view_box.enableAutoRange(axis=pg.ViewBox.YAxis, enable=True)

    def _on_plot_point_clicked(self, plot_item, points):
        """Handles clicks on data points in the plot."""
        if points is None:
            return

        point = points[0]
        x_val = point.pos().x()
        frame_index = int(x_val) - 1
        self.frame_selected.emit(frame_index)

        spot_key = self.config.get("spot_field_key")
        if not spot_key:
            return

        x_key = self.config.get("x_axis_key", "img_num")
        clicked_frame_data = next(
            (item for item in self.plot_data if item.get(x_key) == x_val), None
        )
        if clicked_frame_data:
            spots_raw = clicked_frame_data.get(spot_key, [])

            spot_coords_yx = self._parse_spot_data(spots_raw)
            # The graphics manager expects (y, x) and then plots them as (x, y)
            self.request_spots_display.emit(spot_coords_yx)

    def reset_plot_view(self):
        """Resets the plot's zoom/pan to the default full view."""
        self.plot_x_manual_range = False
        if self.reader and self.reader.total_frames > 0:
            self.plot_widget.setXRange(1, self.reader.total_frames, padding=0)
        else:
            self.plot_widget.setXRange(1, 100, padding=0)
        self.plot_widget.getViewBox().enableAutoRange(
            axis=pg.ViewBox.YAxis, enable=True
        )
        self.status_update.emit(f"{self.name} plot view reset.", 2000)

    def toggle_peel_plot(self):
        """Detaches or re-docks the Dozor plot from/to the main window."""
        if self.peeled_dialog is None:
            # If there's no dialog, we must be docked, so detach.
            self._detach_plot()
        else:
            # If the dialog exists, closing it will trigger the re-docking process.
            self.peeled_dialog.close()

    def _detach_plot(self):
        """Detaches the Dozor plot into a separate dialog window."""
        # Use the stored reference, which is always valid.
        splitter = self.main_ui_splitter
        if not splitter:
            logger.error(
                f"[{self.name}] Cannot detach plot: Main UI splitter not found."
            )
            return

        # Store the current state before detaching
        self.original_splitter_index = splitter.indexOf(self.container_widget)
        self.original_splitter_sizes = splitter.sizes()

        # Reparent the widget to None to remove it from the splitter
        self.container_widget.setParent(None)

        # Create and show the new dialog
        self.peeled_dialog = PeelablePlotDialog(
            self.container_widget, parent=self.main_window
        )
        self.peeled_dialog.request_redock.connect(self._redock_plot)
        self.peeled_dialog.finished.connect(self._on_peeled_dialog_finished)
        self.peeled_dialog.show()

        # Update button and title text
        self.peel_button.setText("Dock")
        self.title_label.setText(f"{self.name} Plot (Detached)")

    def _redock_plot(self, plot_container):
        """Re-inserts the Dozor plot container back into the main UI."""
        # Use the stored reference, which is always valid.
        splitter = self.main_ui_splitter
        if not splitter:
            logger.error(
                f"[{self.name}] Cannot redock plot: Main UI splitter not found."
            )

            if self.peeled_dialog:
                self.peeled_dialog = None
            return

        # Reparent again to ensure it's removed from the dialog's layout
        plot_container.setParent(None)

        # Insert the widget back into the splitter at its original position
        splitter.insertWidget(self.original_splitter_index, plot_container)

        # Restore the splitter sizes if possible
        if len(self.original_splitter_sizes) == splitter.count():
            splitter.setSizes(self.original_splitter_sizes)

        # Cleanup reference to the dialog
        if self.peeled_dialog:
            # Disconnect to prevent re-entry loops
            try:
                self.peeled_dialog.request_redock.disconnect(self._redock_plot)
                self.peeled_dialog.finished.disconnect(self._on_peeled_dialog_finished)
            except TypeError:
                pass
            self.peeled_dialog = None

        # Update button and title text
        self.peel_button.setText("Peel")
        self.title_label.setText(f"{self.name} Analysis Plot")

    # This method is fine as a safety net
    def _on_peeled_dialog_finished(self):
        """Ensures state is cleaned up when the peeled dialog is closed."""
        if self.peeled_dialog:
            container = self.peeled_dialog.plot_container_widget
            if self.main_ui_splitter.indexOf(container) == -1:
                self._redock_plot(container)
            else:
                self.peeled_dialog = None

    def _fetch_and_prepare_data(self) -> bool:
        """
        Fetches data from the source. Now reads from a Redis HASH.
        Returns True if the data was changed, False otherwise.
        """
        if self._current_data_source_type == "redis":
            if not self.redis_connection or not self.current_master_file:
                return False
            key = self._current_data_source_path_or_key
            try:
                # --- MODIFICATION: Read from a HASH ---
                redis_data_dict = self.redis_connection.hgetall(key)
                logger.debug(f"redis_data_dict: {key} len={len(redis_data_dict)}")

                # --- FIX: Filter out non-JSON metadata fields ---
                newly_parsed = []
                for field, value in redis_data_dict.items():
                    # By convention, metadata fields start with '_'
                    if field.startswith("_"):
                        continue
                    try:
                        # Also check for empty strings before parsing
                        if value:
                            newly_parsed.append(json.loads(value))
                    except json.JSONDecodeError:
                        logger.warning(
                            f"Could not parse JSON for field '{field}' in key '{key}'. Value: '{value}'"
                        )
                        continue

                # Re-sort every time to ensure order
                x_key = self.config.get("x_axis_key", "img_num")

                def robust_sort_key(item):
                    val = item.get(x_key)
                    if val is None:
                        return float("inf")
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return float("inf")

                newly_parsed.sort(key=robust_sort_key)

                if newly_parsed != self.plot_data:
                    self.plot_data = newly_parsed
                    self._update_available_metrics()
                    return True

            except redis.RedisError as e:
                logger.error(f"[{self.name}] Redis error: {e}")
                return False

        elif self._current_data_source_type == "file":
            file_path = self._current_data_source_path_or_key
            if not file_path or not os.path.exists(file_path):
                return False
            try:
                with open(file_path, "r") as f:
                    new_data = json.load(f)
                if not isinstance(new_data, list):
                    logger.warning(
                        f"[{self.name}] File content is not a JSON list: {file_path}"
                    )
                    return False
                if new_data != self.plot_data:
                    self.plot_data = new_data
                    self._update_available_metrics()
                    return True
            except (IOError, json.JSONDecodeError) as e:
                logger.warning(
                    f"[{self.name}] Could not read or parse file: {file_path} - {e}"
                )
                return False

        return False

    def _parse_spot_data(self, spots_raw: list) -> Optional[np.ndarray]:
        """
        Parses the raw spot data from a result dictionary into a NumPy array of (y, x) coordinates.
        This base implementation assumes the input is a list of (x, y) pairs.
        Subclasses should override this method for their specific data format.

        Args:
            spots_raw: The raw 'spots' value from the result dictionary.

        Returns:
            A NumPy array of shape (N, 2) with (y, x) coordinates, or None.
        """
        if not spots_raw or not isinstance(spots_raw, list):
            return None

        try:
            # This handles the SpotFinder case: [[x1, y1], [x2, y2], ...]
            # We swap to (y, x) for internal consistency, as pyqtgraph's ImageItem
            # is in (row, col) i.e. (y, x) order.
            return np.array([[s[1], s[0]] for s in spots_raw])
        except (IndexError, TypeError) as e:
            logger.error(
                f"[{self.name}] Failed to parse spot data with default parser: {e}"
            )
            return None

    def cleanup(self):
        """
        Safely cleans up resources used by the manager, such as timers
        and any detached dialogs.
        """
        logger.debug(f"[{self.name}] Cleaning up manager...")
        # Stop the timer to prevent it from firing after cleanup
        self.plot_timer.stop()

        # If a peeled-off dialog exists, close it.
        # This will trigger its own cleanup and redocking logic, but in a safe order.
        if self.peeled_dialog:
            # Disconnect the signal first to prevent re-entry during shutdown
            try:
                self.peeled_dialog.request_redock.disconnect(self._redock_plot)
            except (TypeError, RuntimeError):
                pass  # Already disconnected

            self.peeled_dialog.close()
            self.peeled_dialog = None

    def _handle_plot_mouse_click(self, event):
        """
        Handles mouse clicks on the plot scene, showing a custom context menu
        on right-click.
        """
        logger.debug(f"[{self.name}] Plot mouse click event: {event}")
        if event.button() == QtCore.Qt.RightButton:
            context_menu = QtWidgets.QMenu()

            refresh_action = context_menu.addAction("Refresh Plot from Source")
            refresh_action.setToolTip(
                "Manually re-fetch the latest data from Redis or the local file and update the plot."
            )
            context_menu.addSeparator()

            clear_action = context_menu.addAction("Clear Analysis Results")
            clear_action.setToolTip(f"Deletes all results for this dataset from Redis.")

            # 2. Create the new "Clear and Re-run" action
            rerun_action = context_menu.addAction("Clear and Re-run Analysis")
            rerun_action.setToolTip(
                "Deletes all results from Redis and immediately starts a new processing job."
            )

            # 3. Disable both actions if the source is not Redis
            is_redis_mode = (
                self._current_data_source_type == "redis" and self.redis_connection
            )
            if not is_redis_mode:
                clear_action.setEnabled(False)
                clear_action.setText("Clear Results (Redis not in use)")
                rerun_action.setEnabled(False)
                rerun_action.setText("Re-run Analysis (Redis not in use)")
            # --- END MODIFICATION ---

            # Show the menu and get the selected action
            selected_action = context_menu.exec_(event.screenPos())

            # Trigger the appropriate method based on the selection
            if selected_action == refresh_action:
                # Call the existing refresh method directly.
                self._refresh_data_and_plot()
                self.status_update.emit(f"[{self.name}] Plot refreshed.", 2000)
            elif selected_action == clear_action:
                self._confirm_and_clear_redis_results()
            elif selected_action == rerun_action:
                self._confirm_and_clear_redis_results(and_rerun=True)

    def _show_context_menu(self, position):
        """
        Shows a custom context menu when right-clicking on the plot.
        """
        logger.debug(f"[{self.name}] Plot context menu requested at {position}")
        context_menu = QtWidgets.QMenu(self.plot_widget)

        refresh_action = context_menu.addAction("Refresh Plot from Source")
        refresh_action.setToolTip(
            "Manually re-fetch the latest data from Redis or the local file and update the plot."
        )

        context_menu.addSeparator()

        clear_action = context_menu.addAction("Clear Analysis Results")
        clear_action.setToolTip(f"Deletes all results for this dataset from Redis.")

        rerun_action = context_menu.addAction("Clear and Re-run Analysis")
        rerun_action.setToolTip(
            "Deletes all results from Redis and immediately starts a new processing job."
        )

        global_pos = self.plot_widget.mapToGlobal(position)
        selected_action = context_menu.exec_(global_pos)

        # Disable both actions if the source is not Redis
        is_redis_mode = (
            self._current_data_source_type == "redis" and self.redis_connection
        )

        if not is_redis_mode:
            clear_action.setEnabled(False)
            clear_action.setText("Clear Results (Redis not in use)")
            rerun_action.setEnabled(False)
            rerun_action.setText("Re-run Analysis (Redis not in use)")

        # Show the menu at the cursor position
        global_pos = self.plot_widget.mapToGlobal(position)
        selected_action = context_menu.exec_(global_pos)

        # Handle the selected action
        if selected_action == refresh_action:
            self._refresh_data_and_plot()
            self.status_update.emit(f"[{self.name}] Plot refreshed.", 2000)
        elif selected_action == clear_action:
            self._confirm_and_clear_redis_results()
        elif selected_action == rerun_action:
            self._confirm_and_clear_redis_results(and_rerun=True)

    def _confirm_and_clear_redis_results(self, and_rerun: bool = False):
        """
        Shows a confirmation dialog and, if confirmed, clears the Redis results.
        If 'and_rerun' is True, it will also trigger a new analysis.
        """
        if self._current_data_source_type != "redis" or not self.redis_connection:
            return

        # Customize the message based on the action
        title = "Re-run Analysis?" if and_rerun else "Delete Results?"
        main_text = (
            "This will first delete all existing results and then start a new processing job."
            if and_rerun
            else "This will permanently delete all analysis results for the current dataset."
        )

        msg_box = QtWidgets.QMessageBox()
        msg_box.setIcon(QtWidgets.QMessageBox.Warning)
        msg_box.setText(title)
        msg_box.setInformativeText(
            f"{main_text}\n\n"
            f"Redis key affected:\n{self._current_data_source_path_or_key}\n\n"
            "Are you sure you want to continue?"
        )
        msg_box.setStandardButtons(QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        msg_box.setDefaultButton(QtWidgets.QMessageBox.No)

        ret = msg_box.exec_()

        if ret == QtWidgets.QMessageBox.Yes:
            # First, clear the results
            self._clear_redis_results()
            # Then, if requested, trigger the re-run
            if and_rerun:
                # Use a QTimer to ensure the re-run happens in the next event loop cycle,
                # giving the UI time to process the plot clearing first.
                QtCore.QTimer.singleShot(50, self._rerun_analysis)

    @pyqtSlot()
    def _rerun_analysis(self, reader=None, master_file=None):
        """
        Triggers a new analysis run for the current dataset.
        Subclasses for per-dataset workers (like XDS, DIALS) MUST override this method.
        """
        # Use passed args or fallback to current state
        target_reader = reader if reader else self.reader
        # target_master_file = master_file if master_file else self.current_master_file

        # The default implementation is for per-file workers like CrystFEL.
        if hasattr(self, "handle_dataset_completed"):
            logger.warning(
                f"[{self.name}] _rerun_analysis was called but not overridden for a per-dataset plugin. This may not work as intended."
            )

        if not target_reader or target_reader.total_frames == 0:
            self.status_update.emit(
                f"[{self.name}] Cannot re-run: No data loaded.", 3000
            )
            return

        self.status_update.emit(f"[{self.name}] Re-running analysis...", 3000)

        # This part is only correct for per-file workers.
        # It finds all available data files and triggers the handler.
        # Note: process_existing_data relies on self.reader, so we might need to ensure self.reader is correct if we are in a batch loop
        # For now, we assume if reader is passed, it's because we are in a batch loop where self.reader might be changing.
        # But process_existing_data uses self.reader. 
        # To support batch execution properly for per-file workers, process_existing_data needs refactoring or self.reader needs to be set.
        # However, most per-file workers don't use this batch loop logic yet (CrystFEL). 
        # The main issue is for XDS/xia2 which are per-dataset and override this method anyway.
        # So updating the signature here mainly fixes the crash when the base method is called (or if subclass calls super).
        
        self.process_existing_data()

    @pyqtSlot()
    def _clear_redis_results(self):
        """
        Deletes ALL Redis keys and physical files associated with the current analysis.
        """
        if (
            self._current_data_source_type != "redis"
            or not self.redis_connection
            or not self.current_master_file
        ):
            self.status_update.emit(
                f"[{self.name}] Cannot clear results: Not connected to Redis or no master file loaded.",
                3000,
            )
            return

        # 1. Define all related Redis keys
        results_key = self._current_data_source_path_or_key
        status_key = f"{results_key}:status"
        header_key = f"{results_key}:header"
        segments_key = f"{results_key}:segments"

        keys_to_delete = [results_key, status_key, header_key, segments_key]

        logger.info(
            f"[{self.name}] Preparing to clear all artifacts for {os.path.basename(self.current_master_file)}"
        )

        try:
            # 2. (Optional but Recommended) Clean up physical stream segment files
            segment_dir_base = self.config.get("stream_segment_dir")
            if segment_dir_base:
                master_file_hash = hashlib.sha1(
                    self.current_master_file.encode()
                ).hexdigest()
                segment_dir_to_delete = segment_dir_base / master_file_hash
                if segment_dir_to_delete.exists():
                    logger.info(
                        f"[{self.name}] Deleting physical stream segment directory: {segment_dir_to_delete}"
                    )
                    shutil.rmtree(segment_dir_to_delete, ignore_errors=True)

            # 3. Delete all Redis keys
            num_deleted = self.redis_connection.delete(*keys_to_delete)

            if num_deleted > 0:
                msg = f"[{self.name}] Successfully deleted {num_deleted} Redis key(s)."
                logger.info(msg)
                self.status_update.emit(msg, 4000)
            else:
                msg = f"[{self.name}] No Redis keys found to delete."
                logger.warning(msg)
                self.status_update.emit(msg, 4000)

            # 4. Reset the local state (as before)
            self.plot_data = []
            self.numeric_fields = []
            self.processed_data_len = 0
            self.processed_segments.clear()
            self.request_spots_display.emit(None)
            self._update_plot_display()
            self._update_metric_combobox()

        except redis.RedisError as e:
            msg = f"[{self.name}] Redis error while deleting keys: {e}"
            logger.error(msg, exc_info=True)
            self.status_update.emit(msg, 5000)
        except Exception as e:
            msg = f"[{self.name}] Filesystem error while deleting stream segments: {e}"
            logger.error(msg, exc_info=True)
            self.status_update.emit(msg, 5000)

    def clear_and_rerun_without_prompt(self):
        """
        Public method to programmatically trigger a clear and re-run.
        Bypasses the user confirmation dialog. Used for batch operations.
        """
        if self._current_data_source_type != "redis" or not self.redis_connection:
            logger.warning(f"[{self.name}] Cannot re-run: Not in Redis mode.")
            return

        # Capture current state explicitly for the delayed callback
        current_reader = self.reader
        current_master_file = self.current_master_file

        logger.info(
            f"[{self.name}] Programmatically clearing and re-running for {self.current_master_file}"
        )
        # First, clear the results
        self._clear_redis_results()
        
        # Then, trigger the re-run in the next event loop cycle using the captured state.
        # This is critical for batch loops where self.reader might change before the timer fires.
        if current_reader and current_master_file:
            QtCore.QTimer.singleShot(
                50, lambda: self._rerun_analysis(reader=current_reader, master_file=current_master_file)
            )
        else:
            # Fallback if state is somehow missing (shouldn't happen in valid flow)
            QtCore.QTimer.singleShot(50, self._rerun_analysis)

    def _set_status_label(self, status: str, tooltip: str = ""):
        """Sets the text and color of the status label based on the status string."""
        status_map = {
            "COMPLETED": ("#27ae60", "white"),  # Green
            "SUBMITTED": ("#f39c12", "black"),  # Orange
            "RUNNING": ("#f39c12", "black"),  # Orange
            "FAILED": ("#c0392b", "white"),  # Red
            "Idle": ("#bdc3c7", "black"),  # Gray
            "File Mode": ("#95a5a6", "white"),  # Darker Gray
        }

        display_text = f"Status: {status}"
        base_style = "padding: 2px; border-radius: 3px;"

        for key, (bg_color, text_color) in status_map.items():
            if status.startswith(key):
                self.status_label.setStyleSheet(
                    f"{base_style} background-color: {bg_color}; color: {text_color};"
                )
                self.status_label.setText(display_text)
                self.status_label.setToolTip(tooltip)
                return

        # Default for progress or unknown statuses
        self.status_label.setStyleSheet(
            f"{base_style} background-color: #3498db; color: white;"
        )  # Blue for progress
        self.status_label.setText(display_text)
        self.status_label.setToolTip(tooltip)

    def _update_job_status_display(self):
        """Fetches the job status from Redis and updates the UI label."""
        if (
            self._current_data_source_type != "redis"
            or not self.redis_connection
            or not self.current_master_file
        ):
            self._set_status_label(
                "File Mode" if self._current_data_source_type == "file" else "Idle"
            )
            return

        key = self._current_data_source_path_or_key
        status_key = f"{key}:status"
        status_key_type = self.config.get("status_key_type", "string")

        try:
            if status_key_type == "string":
                # For per-dataset jobs like nXDS, XDS, DIALS
                status_json = self.redis_connection.get(status_key)
                if status_json:
                    status_data = json.loads(status_json)
                    status = status_data.get("status", "Unknown")
                    tooltip = f"Status updated at: {time.strftime('%H:%M:%S', time.localtime(status_data.get('timestamp', 0)))}\n"
                    if status == "FAILED":
                        tooltip += f"Error: {status_data.get('error', 'N/A')}"
                    self._set_status_label(status, tooltip)
                else:
                    self._set_status_label("Not Started")

            elif status_key_type == "hash":
                # For per-file/segment jobs like CrystFEL
                status_hash = self.redis_connection.hgetall(status_key)
                if not status_hash:
                    self._set_status_label("Not Started")
                    return

                total_segments = len(status_hash)
                completed_count = 0
                failed_count = 0
                running_count = 0

                for field, status_json in status_hash.items():
                    status_data = json.loads(status_json)
                    status = status_data.get("status")
                    if status == "COMPLETED":
                        completed_count += 1
                    elif status == "FAILED":
                        failed_count += 1
                    elif status in ["RUNNING", "SUBMITTED"]:
                        running_count += 1

                if completed_count == total_segments and total_segments > 0:
                    self._set_status_label(
                        "COMPLETED",
                        f"All {total_segments} segments processed successfully.",
                    )
                elif failed_count > 0:
                    progress_text = f"FAILED ({failed_count}/{total_segments} failed)"
                    self._set_status_label(
                        progress_text,
                        f"{completed_count} completed, {running_count} running.",
                    )
                elif running_count > 0:
                    progress_text = f"RUNNING ({completed_count}/{total_segments})"
                    self._set_status_label(
                        progress_text, f"{running_count} segments are active."
                    )
                else:
                    self._set_status_label(
                        "SUBMITTED", f"{total_segments} segments queued."
                    )

        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.warning(f"[{self.name}] Could not update job status: {e}")
            self._set_status_label("Error")
