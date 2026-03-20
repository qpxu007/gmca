# ui_manager.py
import datetime
import getpass
import os
import re

import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui, QtWidgets

from qp2.image_viewer.config import (
    IMAGE_COLORMAP,
    SETTINGS_ORGANIZATION,
    SETTINGS_APPLICATION,
    SETTINGS_LAST_DIR_KEY,
)
from qp2.log.logging_config import get_logger
from qp2.xio.user_group_manager import get_current_bluice_user
from qp2.image_viewer.beamcenter.calibration_dialog import CalibrationResultsDialog
from qp2.utils.icon import generate_icon_with_text
from qp2.image_viewer.utils.sort_files import natural_sort_key

logger = get_logger(__name__)


class UIManager:
    """Handles the creation, layout, and state updates of UI widgets."""

    def __init__(self, main_window):
        self.main_window = main_window  # Reference to the main window instance
        self.reader = main_window.reader  # Access reader via main_window
        # Initialize widget attributes to None
        self.frame_slider = None
        self.frame_label = None
        self.prev_button = None
        self.next_button = None
        self.play_button = None
        self.frame_input = None
        self.find_peaks_button = None
        self.pixel_coord_label = None
        self.rel_coord_label = None
        self.radius_label = None
        self.two_theta_label = None
        self.resolution_label = None
        self.intensity_label = None
        self.dataset_tree_widget = None
        self.graphics_widget = None  # Graphics layout widget itself
        self.view_box = None  # The ViewBox within the graphics widget
        self.hist_lut = None  # The HistogramLUTItem
        self.launch_batch_proc_button = None
        self.launch_sview_button = None
        self.launch_data_viewer_button = None

    def create_actions(self):
        """Creates QAction objects and assigns them to the main window."""
        mw = self.main_window
        # Analysis Actions
        mw.auto_peak_find_action = QtGui.QAction(
            "&Auto Find Peaks on Change", mw, checkable=True
        )
        mw.beam_center_calibration_action = QtGui.QAction(
            "&Beam Center Calibration (Rings)...", mw
        )
        
        mw.calculate_new_beam_center_action = QtGui.QAction(
            "Update Master File (to correct geometry)", mw
        )
        mw.calculate_new_beam_center_action.setStatusTip(
            "Estimate beam center by optimizing radial symmetry of the background."
        )

        # Display Actions
        mw.auto_contrast_action = QtGui.QAction(
            "Auto-Contrast on Zoom/Pan/Frame change", mw, checkable=True
        )
        mw.lock_contrast_action = QtGui.QAction(
            "Lock Contrast/Colormap", mw, checkable=True
        )
        mw.lock_contrast_action.setStatusTip(
            "Prevent contrast from changing when loading new datasets"
        )

        mw.enhance_contrast_action = QtGui.QAction("Focus on 10Å–3.5Å", mw)
        mw.toggle_rings_action = QtGui.QAction("Toggle Resolution Rings", mw)
        # Image Processing Actions
        mw.image_filter_action = QtGui.QAction(
            "Apply Image &Filter", mw, checkable=False
        )

        # ADDED: Create the QAction for clearing visuals
        mw.clear_visuals_action = QtGui.QAction("Clear All Visuals", mw)
        mw.clear_visuals_action.setStatusTip(
            "Clear all spots, rings, and peaks from the image view."
        )
        mw.clear_visuals_action.setShortcut("Ctrl+D")  # "D" for de-clutter, for example

        # --- NEW: Image Stats Overlay Action ---
        mw.image_stats_overlay_action = QtGui.QAction(
            "Show Image Stats Overlay", mw, checkable=True
        )

        # --- NEW: Update Detector Mask Action ---
        mw.locate_bad_pixels_action = QtGui.QAction("Find &Stuck Hot Pixels...", mw)
        mw.locate_bad_pixels_action.setStatusTip(
            "Analyze a random set of frames to find stuck or noisy pixels"
        )
        mw.update_detector_mask_action = QtGui.QAction("Update Detector Mask", mw)
        mw.update_detector_mask_action.setStatusTip(
            "Recompute the detector mask for the current dataset"
        )

        mw.radial_sum_action = QtGui.QAction("&Radial Sum Analysis...", mw)
        mw.analyze_ice_rings_action = QtGui.QAction("Analyze &Ice Rings...", mw)
        mw.analyze_ice_rings_action.setStatusTip(
            "Analyze the image for common ice ring patterns."
        )
        # Strategy Actions
        mw.strategy_xds_action = QtGui.QAction("Strategy (XDS)...", mw)
        mw.strategy_mosflm_action = QtGui.QAction("Strategy (MOSFLM)...", mw)
        mw.strategy_crystfel_action = QtGui.QAction("Index (CrystFEL)...", mw)
        mw.strategy_both_action = QtGui.QAction("Strategy (Both)...", mw)

        mw.sum_frames_action = QtGui.QAction("&Sum/Slab Frames", mw, checkable=False)
        # File Actions
        mw.open_master_action = QtGui.QAction("&Open Master File(s)...", mw)
        mw.open_directory_action = QtGui.QAction("Open &Directories...", mw)
        mw.open_directory_action.setStatusTip(
            "Open all master files from selected directories"
        )
        mw.load_list_action = QtGui.QAction("Load from &List File...", mw)
        mw.load_list_action.setStatusTip(
            "Load a list of datasets from a text file (one path per line)"
        )
        mw.load_latest_redis_action = QtGui.QAction("Load Latest via Redis", mw)
        mw.recent_datasets_action = QtGui.QAction("Recent Datasets...", mw)
        mw.recent_datasets_action.setStatusTip(
            "Show a list of recent datasets from Redis to open"
        )

        # Settings Action
        mw.settings_action = QtGui.QAction("&Settings...", mw)

        # Set properties (status tips, shortcuts, initial state)
        mw.auto_peak_find_action.setChecked(mw.auto_peak_finding_enabled)
        mw.auto_peak_find_action.setStatusTip(
            "Automatically run peak finding whenever the frame changes (pauses playback)"
        )
        mw.beam_center_calibration_action.setStatusTip(
            "Run ring detection to calibrate beam center"
        )
        mw.auto_contrast_action.setChecked(mw.auto_contrast_on_zoom)
        mw.auto_contrast_action.setStatusTip(
            "Adjust image contrast based on the visible area or full image"
        )
        mw.toggle_rings_action.setStatusTip("Toggle display of resolution rings")
        mw.toggle_rings_action.setShortcut("Ctrl+R")
        mw.image_filter_action.setStatusTip("Apply an image filter to the image")
        # mw.sum_frames_action.setChecked(mw.sum_frames_enabled)  # No longer checkable
        mw.sum_frames_action.setEnabled(True)
        playback_skip = mw.settings_manager.get("playback_skip")
        if playback_skip is None:
            playback_skip = 1
        mw.sum_frames_action.setStatusTip(
            f"Sum current frame and next {playback_skip - 1} frames"
        )
        mw.open_master_action.setStatusTip("Open a different master HDF5 file")
        mw.open_master_action.setShortcut("Ctrl+O")
        mw.load_latest_redis_action.setStatusTip(
            "Load the latest image found via Redis query (xrevrange)"
        )
        mw.load_latest_redis_action.setEnabled(mw.redis_manager is not None)
        mw.recent_datasets_action.setEnabled(mw.redis_manager is not None)

        mw.settings_action.setStatusTip("Configure application settings")

        mw.show_metadata_action = QtGui.QAction("&Show HDF5 Metadata...", mw)
        mw.show_metadata_action.setStatusTip(
            "Display metadata from the current HDF5 master file"
        )

        mw.about_action = QtGui.QAction("&About...", mw)
        mw.about_action.setStatusTip("Show application information")

        # --- Create and Connect Measure Distance Action ---
        # This would typically be part of UIManager.setup_actions() or UIManager.setup_menus()
        mw.measure_distance_action = QtWidgets.QAction(
            "Measure Distance", mw, checkable=True
        )
        mw.measure_distance_action.setStatusTip(
            "Select two points to measure distance in pixels and Angstroms"
        )
        mw.calculate_line_profile_action = QtWidgets.QAction(
            "Calculate Line Profile", mw, checkable=True
        )
        mw.calculate_line_profile_action.setStatusTip(
            "Select two points to calculate intensity profile along the line"
        )
        
        mw.show_2d_profile_action = QtWidgets.QAction(
            "Show 2D Profile", mw, checkable=True
        )
        mw.show_2d_profile_action.setStatusTip(
            "Ctrl + Drag to view 3D profile and signal statistics"
        )

        mw.toggle_mask_overlay_action = QtGui.QAction(
            "Toggle Mask Overlay", mw, checkable=True
        )
        mw.toggle_mask_overlay_action.setStatusTip("Show/hide detector mask overlay")

        mw.toggle_console_action = QtGui.QAction("Python Console", mw, checkable=True)
        mw.toggle_console_action.setStatusTip("Show/Hide the Python Console")
        mw.toggle_console_action.setShortcut("Ctrl+`")

        mw.toggle_ai_action = QtGui.QAction("AI Assistant", mw, checkable=True)
        mw.toggle_ai_action.setStatusTip("Show/Hide the AI Assistant")
        mw.toggle_ai_action.setShortcut("Ctrl+G")

        # toggle follow redis or unfollow
        mw.toggle_redis_follow_action = QtGui.QAction(mw)
        initial_label = (
            "Unfollow detector redis" if mw.is_live_mode else "Follow detector redis"
        )
        mw.toggle_redis_follow_action.setText(initial_label)
        # Gray out if Redis stream is not available (no RedisManager)
        mw.toggle_redis_follow_action.setEnabled(mw.redis_manager is not None)

    def setup_ui(self):
        """Initializes the UI widgets and layout."""
        mw = self.main_window
        self.create_actions()  # Create actions first

        # --- Menu Bar ---
        menu_bar = mw.menuBar()

        # --- FILE MENU ---
        file_menu = menu_bar.addMenu("&File")
        file_menu.addAction(mw.open_master_action)
        file_menu.addAction(mw.open_directory_action)
        file_menu.addAction(mw.load_list_action)
        file_menu.addAction(mw.load_latest_redis_action)
        file_menu.addAction(mw.recent_datasets_action)
        file_menu.addAction(mw.toggle_redis_follow_action)
        file_menu.addSeparator()
        file_menu.addAction(mw.show_metadata_action)
        file_menu.addSeparator()
        file_menu.addAction(mw.settings_action)
        file_menu.addAction(QtGui.QAction("E&xit", mw, triggered=mw.close))

        # --- NEW: VIEW MENU ---
        view_menu = menu_bar.addMenu("&View")
        view_menu.addAction(mw.lock_contrast_action)
        view_menu.addAction(mw.auto_contrast_action)
        view_menu.addAction(mw.enhance_contrast_action)
        view_menu.addSeparator()
        view_menu.addAction(mw.toggle_rings_action)
        view_menu.addAction(mw.toggle_mask_overlay_action)
        view_menu.addAction(mw.image_stats_overlay_action)
        view_menu.addSeparator()
        view_menu.addAction(mw.clear_visuals_action)

        # --- RESTRUCTURED: ANALYSIS MENU (replaces old Actions menu) ---
        analysis_menu = menu_bar.addMenu("A&nalysis")

        # Peak Finding Sub-group
        analysis_menu.addAction(mw.auto_peak_find_action)
        analysis_menu.addSeparator()

        # Image Processing Sub-group
        analysis_menu.addAction(mw.image_filter_action)
        analysis_menu.addAction(mw.sum_frames_action)
        analysis_menu.addSeparator()

        # Specific Analyses Sub-group
        analysis_menu.addAction(mw.radial_sum_action)
        analysis_menu.addAction(mw.analyze_ice_rings_action)
        analysis_menu.addSeparator()

        # Strategy Sub-menu
        strategy_menu = analysis_menu.addMenu("Index/Strategy")
        strategy_menu.addAction(mw.strategy_xds_action)
        strategy_menu.addAction(mw.strategy_mosflm_action)
        strategy_menu.addAction(mw.strategy_both_action)
        strategy_menu.addSeparator()
        strategy_menu.addAction(mw.strategy_crystfel_action)

        # --- NEW: TOOLS MENU ---
        tools_menu = menu_bar.addMenu("&Tools")
        tools_menu.addAction(mw.measure_distance_action)
        tools_menu.addAction(mw.calculate_line_profile_action)
        tools_menu.addAction(mw.show_2d_profile_action)
        tools_menu.addAction(mw.beam_center_calibration_action)
        tools_menu.addAction(mw.calculate_new_beam_center_action) # Added new action here
        tools_menu.addSeparator()

        # Masking Sub-menu
        masking_menu = tools_menu.addMenu("Detector Masking")
        masking_menu.addAction(mw.locate_bad_pixels_action)
        masking_menu.addAction(mw.update_detector_mask_action)
        tools_menu.addSeparator()

        tools_menu.addAction(mw.toggle_console_action)


        # --- HELP MENU ---
        help_menu = menu_bar.addMenu("&Help")
        help_menu.addAction(mw.toggle_ai_action)
        help_menu.addSeparator()
        help_menu.addAction(mw.about_action)

        # --- Main Layout ---
        central_widget = QtWidgets.QWidget()
        mw.setCentralWidget(central_widget)
        main_layout = QtWidgets.QHBoxLayout(central_widget)

        # --- Central Widget ---
        # The central widget now ONLY contains the right-side graphics view.
        # The left-side items will be moved into Dockable Widgets.
        self.right_panel_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        main_layout.addWidget(self.right_panel_splitter, stretch=3)
        main_layout.setContentsMargins(0, 0, 0, 0)
        
        # --- Left Control Panel (Now a Dock Widget) ---
        self.left_panel = QtWidgets.QWidget()
        left_layout = QtWidgets.QVBoxLayout(self.left_panel)
        left_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignTop)
        self.left_panel.setMaximumWidth(250)
        
        self.controls_dock = QtWidgets.QDockWidget("Viewer Controls", mw)
        self.controls_dock.setWidget(self.left_panel)
        self.controls_dock.setFeatures(QtWidgets.QDockWidget.DockWidgetFloatable | QtWidgets.QDockWidget.DockWidgetMovable)
        mw.addDockWidget(QtCore.Qt.LeftDockWidgetArea, self.controls_dock)

        # Frame Slider & Label
        total_frames = mw.reader.total_frames if mw.reader else 0
        self.frame_slider = QtWidgets.QSlider(QtCore.Qt.Orientation.Horizontal)
        self.frame_slider.setRange(0, max(0, total_frames - 1))
        self.frame_slider.setValue(mw.current_frame_index)
        self.frame_slider.setTickInterval(max(1, total_frames // 10))
        self.frame_slider.setTickPosition(QtWidgets.QSlider.TickPosition.TicksBelow)
        left_layout.addWidget(QtWidgets.QLabel("Frame:"))
        left_layout.addWidget(self.frame_slider)

        frame_label_layout = QtWidgets.QHBoxLayout()
        self.frame_label = QtWidgets.QLabel(
            f"Frame {mw.current_frame_index + 1} / {total_frames}"
        )
        self.frame_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        frame_label_layout.addWidget(self.frame_label, 1)
        left_layout.addLayout(frame_label_layout)

        # Navigation Buttons
        nav_layout = QtWidgets.QHBoxLayout()
        self.prev_button = QtWidgets.QPushButton("<< Prev")
        self.next_button = QtWidgets.QPushButton("Next >>")
        self.play_button = QtWidgets.QPushButton("▶ Play")
        self.prev_button.setFixedWidth(70)
        self.next_button.setFixedWidth(70)
        self.play_button.setFixedWidth(70)
        nav_layout.addWidget(self.prev_button)
        nav_layout.addWidget(self.play_button)
        nav_layout.addWidget(self.next_button)
        left_layout.addLayout(nav_layout)

        # Go To Frame Input
        goto_layout = QtWidgets.QHBoxLayout()
        goto_layout.addWidget(QtWidgets.QLabel("Go to Frame:"))
        self.frame_input = QtWidgets.QLineEdit(str(mw.current_frame_index + 1))
        validator_max = total_frames if total_frames > 0 else 1
        self.frame_input.setValidator(QtGui.QIntValidator(1, max(1, validator_max)))
        self.frame_input.setMaximumWidth(80)
        goto_layout.addWidget(self.frame_input)
        goto_layout.addStretch(1)
        self.find_peaks_button = QtWidgets.QPushButton("Peaks")
        self.peak_settings_button = QtWidgets.QPushButton("⚙️")
        self.peak_settings_button.setToolTip("Open Live Spot Finder Settings")
        button_size = self.find_peaks_button.sizeHint().height()
        self.peak_settings_button.setFixedSize(button_size, button_size)
        goto_layout.addWidget(self.find_peaks_button)
        goto_layout.addWidget(self.peak_settings_button)

        left_layout.addLayout(goto_layout)

        left_layout.addSpacing(15)

        # Pixel Information Panel
        info_group = QtWidgets.QGroupBox("Pixel Information")
        info_layout = QtWidgets.QFormLayout(info_group)
        self.pixel_coord_label = QtWidgets.QLabel("Pixel: (-, -)")
        self.rel_coord_label = QtWidgets.QLabel("Rel. Beam: (-, -) mm")
        self.radius_label = QtWidgets.QLabel("Radius: - pix (- mm)")
        self.two_theta_label = QtWidgets.QLabel("2θ: - °")
        self.resolution_label = QtWidgets.QLabel("Resolution: - Å")
        self.intensity_label = QtWidgets.QLabel("Intensity: -")
        info_layout.addRow("Pixel:", self.pixel_coord_label)
        info_layout.addRow("Rel. Beam:", self.rel_coord_label)
        info_layout.addRow("Radius:", self.radius_label)
        info_layout.addRow("2θ:", self.two_theta_label)
        info_layout.addRow("Resolution:", self.resolution_label)
        info_layout.addRow("Intensity:", self.intensity_label)
        left_layout.addWidget(info_group)

        # Dataset History Tree (Now a Dock Widget)
        # We wrap the QTreeWidget in a normal widget instead of a QGroupBox because 
        # the QDockWidget already provides a titled frame border.
        dataset_widget = QtWidgets.QWidget()
        dataset_layout = QtWidgets.QVBoxLayout(dataset_widget)
        dataset_layout.setContentsMargins(0, 0, 0, 0) # Remove padding to maximize space
        
        self.dataset_tree_widget = QtWidgets.QTreeWidget()
        self.dataset_tree_widget.setHeaderLabels(["Name", "Frames", "Path"])
        
        # Optimize performance by setting header resize modes instead of calculating contents iteratively
        header = self.dataset_tree_widget.header()
        # Allow user to adjust the width of the Name column
        header.setSectionResizeMode(0, QtWidgets.QHeaderView.Interactive)
        self.dataset_tree_widget.setColumnWidth(0, 250) # Set a reasonable default width
        header.setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QtWidgets.QHeaderView.Interactive)
        self.dataset_tree_widget.setColumnWidth(2, 150)
        
        self.dataset_tree_widget.setToolTip(
            "Double-click a dataset to load. Right-click for options."
        )
        self.dataset_tree_widget.setSelectionMode(
            QtWidgets.QAbstractItemView.ExtendedSelection
        )
        self.dataset_tree_widget.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)

        sizePolicyTree = QtWidgets.QSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Expanding,
        )
        self.dataset_tree_widget.setSizePolicy(sizePolicyTree)
        dataset_layout.addWidget(self.dataset_tree_widget)
        
        self.dataset_dock = QtWidgets.QDockWidget("Runs & Datasets", mw)
        self.dataset_dock.setWidget(dataset_widget)
        self.dataset_dock.setFeatures(QtWidgets.QDockWidget.DockWidgetFloatable | QtWidgets.QDockWidget.DockWidgetMovable)
        mw.addDockWidget(QtCore.Qt.LeftDockWidgetArea, self.dataset_dock)

        # Organize the Docks Vertically
        mw.splitDockWidget(self.controls_dock, self.dataset_dock, QtCore.Qt.Vertical)
        mw.resizeDocks([self.controls_dock, self.dataset_dock], [10, 1000], QtCore.Qt.Vertical)

        # --- Right Panel (Graphics and Dozor Plot) ---
        self.right_panel_splitter.setStyleSheet(
            """
            QSplitter::handle:vertical {
                background-color: #888888; /* A noticeable medium gray */
                border: 1px solid #555555; /* Optional darker border for definition */
                height: 7px;              /* Make it thicker */
            }
            QSplitter::handle:vertical:hover {
                background-color: #999999; /* Slightly lighter on hover */
            }
            QSplitter::handle:vertical:pressed {
                background-color: #777777; /* Slightly darker when pressed */
            }
        """
        )

        # --- Right Graphics Panel (GraphicsLayoutWidget created here) ---
        self.graphics_widget = pg.GraphicsLayoutWidget()
        # self.graphics_widget.setMinimumHeight(600)
        self.right_panel_splitter.addWidget(self.graphics_widget)

        # ViewBox created here, reference stored
        self.view_box = self.graphics_widget.addViewBox(row=0, col=0)
        self.view_box.setAspectLocked(True)
        self.view_box.invertY(True)

        # Histogram/LUT created here, reference stored
        # ImageItem is managed by GraphicsManager, but LUT needs a reference initially
        # We pass None initially, GraphicsManager will set it later
        self.hist_lut = pg.HistogramLUTItem(image=None)
        self.hist_lut.setFixedWidth(75)

        cmap = pg.colormap.get(IMAGE_COLORMAP)
        self.hist_lut.gradient.setColorMap(cmap)

        self.graphics_widget.addItem(self.hist_lut, row=0, col=1)

        # --- Generic Analysis Plot Container ---
        # Create the group box WITHOUT a title. We'll make our own.
        self.analysis_plot_container = QtWidgets.QGroupBox()
        analysis_container_layout = QtWidgets.QVBoxLayout(self.analysis_plot_container)
        analysis_container_layout.setContentsMargins(5, 5, 5, 5)

        # --- NEW: Create a custom title bar using a horizontal layout ---
        analysis_title_bar_layout = QtWidgets.QHBoxLayout()

        # 1. Add the title label
        analysis_title_label = QtWidgets.QLabel("Analysis Plugin")
        analysis_title_label.setStyleSheet("font-weight: bold;")
        analysis_title_bar_layout.addWidget(analysis_title_label)

        # 2. Add a stretch to push the combobox to the right
        analysis_title_bar_layout.addStretch(1)

        # 3.1 Add the ComboBox for selecting the analysis plugin
        self.analysis_selector_combo = QtWidgets.QComboBox()
        self.analysis_selector_combo.setToolTip("Select which analysis to display")
        self.analysis_selector_combo.setMinimumWidth(
            150
        )  # Give it a reasonable minimum size
        analysis_title_bar_layout.addWidget(self.analysis_selector_combo)

        # 3.2 Add the Data Processing icon button to the right of the combo
        self.launch_batch_proc_button = QtWidgets.QPushButton()
        self.launch_batch_proc_button.setToolTip("Data Processing...")
        dp_icon = generate_icon_with_text(text="dp", bg_color="#e74c3c", size=64)
        self.launch_batch_proc_button.setIcon(dp_icon)
        self.launch_batch_proc_button.setIconSize(QtCore.QSize(20, 20))
        button_height = self.analysis_selector_combo.sizeHint().height()
        self.launch_batch_proc_button.setFixedSize(button_height, button_height)
        self.launch_batch_proc_button.setText("")
        analysis_title_bar_layout.addWidget(self.launch_batch_proc_button)

        # 3.3 Add the data viewer icon button
        self.launch_data_viewer_button = QtWidgets.QPushButton()
        self.launch_data_viewer_button.setToolTip("Launch Data Viewer")
        dv_icon = generate_icon_with_text(text="dv", bg_color="#e74c3c", size=64)
        self.launch_data_viewer_button.setIcon(dv_icon)
        self.launch_data_viewer_button.setIconSize(QtCore.QSize(20, 20))
        self.launch_data_viewer_button.setFixedSize(button_height, button_height)
        self.launch_data_viewer_button.setText("")
        analysis_title_bar_layout.addWidget(self.launch_data_viewer_button)

        # 3.4 Add the sview icon button
        self.launch_sview_button = QtWidgets.QPushButton()
        self.launch_sview_button.setToolTip("check cluster status using sview")
        self.launch_sview_button.setIcon(
            self.main_window.style().standardIcon(
                QtWidgets.QStyle.SP_FileDialogInfoView
            )
        )
        self.launch_sview_button.setIconSize(QtCore.QSize(20, 20))
        self.launch_sview_button.setFixedSize(button_height, button_height)
        self.launch_sview_button.setText("")
        analysis_title_bar_layout.addWidget(self.launch_sview_button)

        # 3.5 Add the Job Status icon button
        self.launch_job_status_button = QtWidgets.QPushButton()
        self.launch_job_status_button.setToolTip("Display Processing Job Status")
        self.launch_job_status_button.setIcon(
            self.main_window.style().standardIcon(
                QtWidgets.QStyle.SP_ComputerIcon
            )
        )
        self.launch_job_status_button.setIconSize(QtCore.QSize(20, 20))
        self.launch_job_status_button.setFixedSize(button_height, button_height)
        self.launch_job_status_button.setText("")
        analysis_title_bar_layout.addWidget(self.launch_job_status_button)

        # Add the custom title bar layout to the top of the group box
        analysis_container_layout.addLayout(analysis_title_bar_layout)

        self.analysis_widget_layout = QtWidgets.QVBoxLayout()
        analysis_container_layout.addLayout(self.analysis_widget_layout)
        self.right_panel_splitter.addWidget(self.analysis_plot_container)

        # --- Call to set initial splitter sizes ---
        # Use QTimer.singleShot to ensure layout has settled
        QtCore.QTimer.singleShot(
            0, lambda: self._set_initial_splitter_sizes(self.right_panel_splitter)
        )

        # --- Status Bar (Access via main_window) ---
        mw.statusBar().showMessage("Ready")

    def populate_analysis_selector(self, plugin_names: list):
        self.analysis_selector_combo.blockSignals(True)
        self.analysis_selector_combo.clear()
        self.analysis_selector_combo.addItem("None")
        self.analysis_selector_combo.addItems(plugin_names)
        self.analysis_selector_combo.blockSignals(False)

    def connect_signals(self):
        """Connect signals from UI widgets to main window slots."""
        mw = self.main_window

        # Frame Navigation
        # self.frame_slider.valueChanged.connect(mw.slider_changed)
        self.frame_slider.sliderPressed.connect(
            lambda: setattr(mw, "is_slider_dragging", True)
        )
        self.frame_slider.sliderMoved.connect(mw.slider_dragged)
        self.frame_slider.sliderReleased.connect(mw.slider_released)
        # valueChanged is still needed for programmatic changes (playback) and simple clicks
        self.frame_slider.valueChanged.connect(mw.slider_changed_by_user)

        self.prev_button.clicked.connect(mw.prev_frame)
        self.next_button.clicked.connect(mw.next_frame)
        self.frame_input.returnPressed.connect(mw.go_to_frame)
        self.play_button.clicked.connect(mw.toggle_playback)
        self.find_peaks_button.clicked.connect(mw.focus_and_find_peaks)
        self.peak_settings_button.clicked.connect(
            mw.live_peak_finding_manager.open_settings_dialog
        )

        # File Actions
        mw.open_master_action.triggered.connect(mw.file_io_manager.open_file_dialog)
        mw.open_directory_action.triggered.connect(self.open_directory_dialog)
        mw.load_list_action.triggered.connect(mw.file_io_manager.load_from_list_file)
        mw.load_latest_redis_action.triggered.connect(
            mw.file_io_manager.load_latest_from_redis
        )
        mw.recent_datasets_action.triggered.connect(
            mw.file_io_manager.show_recent_datasets_dialog
        )
        mw.toggle_redis_follow_action.triggered.connect(mw._toggle_redis_follow)

        self.launch_batch_proc_button.clicked.connect(mw._launch_batch_processor)
        self.launch_sview_button.clicked.connect(mw._launch_sview)
        self.launch_job_status_button.clicked.connect(mw._show_job_status_dialog_for_current)
        self.launch_data_viewer_button.clicked.connect(mw._launch_data_viewer)
        mw.settings_action.triggered.connect(mw._open_settings_dialog)
        mw.show_metadata_action.triggered.connect(mw._show_hdf5_metadata)

        # Main Actions
        mw.auto_peak_find_action.toggled.connect(
            mw.live_peak_finding_manager.set_auto_mode
        )

        mw.beam_center_calibration_action.triggered.connect(mw._launch_calibration)
        mw.calculate_new_beam_center_action.triggered.connect(mw._launch_beam_center_calculation) # Connected here
        mw.image_filter_action.triggered.connect(mw._launch_image_filter_dialog)

        mw.sum_frames_action.triggered.connect(
            mw.summation_manager.open_settings_dialog
        )
        mw.radial_sum_action.triggered.connect(mw.run_radial_sum_analysis)
        mw.analyze_ice_rings_action.triggered.connect(mw.ice_ring_manager.run_ice_ring_analysis)
        mw.update_detector_mask_action.triggered.connect(mw.detector_mask_manager.update_detector_mask)
        mw.strategy_xds_action.triggered.connect(
            lambda: mw._run_strategy_for_current_view("xds")
        )
        mw.strategy_mosflm_action.triggered.connect(
            lambda: mw._run_strategy_for_current_view("mosflm")
        )
        mw.strategy_crystfel_action.triggered.connect(
            lambda: mw._run_strategy_for_current_view("crystfel")
        )
        mw.strategy_both_action.triggered.connect(mw._run_strategy_both)

        # Display/Tool Actions
        mw.clear_visuals_action.triggered.connect(mw.clear_all_visuals)
        mw.lock_contrast_action.toggled.connect(mw._toggle_contrast_lock)
        mw.auto_contrast_action.toggled.connect(mw._toggle_auto_contrast_action)
        mw.locate_bad_pixels_action.triggered.connect(mw._launch_bad_pixel_detection)
        mw.image_stats_overlay_action.toggled.connect(mw._toggle_image_stats_overlay)
        mw.toggle_mask_overlay_action.toggled.connect(mw.detector_mask_manager.toggle_mask_overlay)
        mw.toggle_rings_action.triggered.connect(mw.toggle_resolution_rings)
        
        mw.toggle_console_action.toggled.connect(mw.console_dock.setVisible)
        mw.console_dock.visibilityChanged.connect(mw.toggle_console_action.setChecked)

        mw.toggle_ai_action.toggled.connect(
            lambda checked: mw.get_ai_assistant_window().show() if checked else mw.get_ai_assistant_window().hide()
        )

        mw.enhance_contrast_action.triggered.connect(
            lambda: mw.focus_on_region_based_on_resolution(3.5)
        )
        mw.measure_distance_action.triggered.connect(
            mw.measurement_manager.toggle_distance_measurement_mode
        )
        mw.calculate_line_profile_action.triggered.connect(
            mw.measurement_manager.toggle_line_profile_mode
        )
        mw.show_2d_profile_action.triggered.connect(
            mw.measurement_manager.toggle_2d_profile_mode
        )

        # Help Action
        mw.about_action.triggered.connect(mw._show_about_dialog)

        # Dataset History Tree
        if self.dataset_tree_widget is not None:
            self.dataset_tree_widget.itemActivated.connect(
                self.on_dataset_item_activated
            )
            self.dataset_tree_widget.customContextMenuRequested.connect(
                mw._show_dataset_context_menu
            )

        # Analysis Plugin Selector
        self.analysis_selector_combo.currentIndexChanged.connect(
            self.main_window._on_analysis_plugin_selected
        )

    def on_dataset_item_activated(self, item, column):
        """
        Handles activation (double-click) of an item in the tree.
        Only loads data if a leaf item (a dataset) is activated.
        A dataset is identified by having no children.
        """
        if item and item.childCount() == 0:
            self.main_window.file_io_manager.on_dataset_selected_from_history(item)

    def update_frame_elements(self, frame_index, total_frames, latest_available_index):
        """Updates slider, labels, buttons based on frame indices."""
        mw = self.main_window
        # Update Slider
        self.frame_slider.blockSignals(True)
        self.frame_slider.setRange(0, max(0, total_frames - 1))
        self.frame_slider.setValue(frame_index)
        self.frame_slider.blockSignals(False)

        # Update Label
        self.frame_label.setText(f"Frame {frame_index + 1} / {total_frames}")

        # Update Validator
        validator_max = total_frames if total_frames > 0 else 1
        self.frame_input.setValidator(QtGui.QIntValidator(1, max(1, validator_max)))
        self.frame_input.setText(str(frame_index + 1))

        # Update Buttons
        self.prev_button.setEnabled(frame_index > 0)
        self.next_button.setEnabled(frame_index < latest_available_index)

        # Update Status Bar (use main window's method)
        mw.update_status_bar_frame_info(
            frame_index
        )  # Example: Add a dedicated method in main window

    def update_playback_button(self, state: str):
        """Updates the Play button text ('Play', 'Pause', 'Wait...')."""
        if state == "play":
            self.play_button.setText("▶ Play")
        elif state == "pause":
            self.play_button.setText("❚❚ Pause")
        elif state == "wait":
            self.play_button.setText("↺ Wait...")
        else:
            self.play_button.setText("?")  # Unknown state

    # NEW METHOD: Provides a clean interface to check the UI state.
    def is_play_button_in_wait_state(self) -> bool:
        """Checks if the play button is currently in the 'Wait' state."""
        return self.play_button.text() == "↺ Wait..."

    def update_pixel_info_panel(self, info: dict):
        """Updates the labels in the Pixel Information panel."""
        self.pixel_coord_label.setText(info.get("coords", "Pixel: (-, -)"))
        self.intensity_label.setText(info.get("intensity", "Intensity: -"))
        self.rel_coord_label.setText(info.get("relative", "Rel. Beam: (-, -) mm"))
        self.radius_label.setText(info.get("radius", "Radius: - pix (- mm)"))
        self.two_theta_label.setText(info.get("two_theta", "2θ: - °"))
        self.resolution_label.setText(info.get("resolution", "Resolution: - Å"))

    def clear_pixel_info_panel(self):
        """Clears the pixel information panel labels."""
        self.update_pixel_info_panel({})  # Call update with empty dict

    def update_sum_action_state(self, enabled: bool, skip_count: int):
        """Updates the enabled state and tooltip of the Sum Frames action."""
        self.main_window.sum_frames_action.setEnabled(True)
        self.main_window.sum_frames_action.setStatusTip(
            f"Sum current frame and next {skip_count - 1} frames"
        )

    def reset_ui_on_load_error(self, error_state_text="Load Error"):
        """Resets UI elements to an empty state after a file loading error."""
        mw = self.main_window
        self.update_frame_elements(0, 0, -1)
        self.clear_pixel_info_panel()

        self.update_playback_button("play")
        mw.setWindowTitle(f"Viewer - {error_state_text}")

    def reset_ui_to_empty(self):
        """Resets UI elements to an empty state cleanly."""
        mw = self.main_window
        self.update_frame_elements(0, 0, -1)
        self.clear_pixel_info_panel()
        self.update_playback_button("play")
        mw.setWindowTitle("Viewer")

    def show_status_message(self, message: str, timeout: int = 0):
        """Shows a message in the main window's status bar."""
        self.main_window.statusBar().showMessage(message, timeout)

    def clear_status_message_if(self, prefix: str):
        """Clears status bar if message starts with prefix."""
        if self.main_window.statusBar().currentMessage().startswith(prefix):
            self.main_window.statusBar().clearMessage()

    def show_warning_message(self, title: str, message: str):
        QtWidgets.QMessageBox.warning(self.main_window, title, message)

    def show_critical_message(self, title: str, message: str):
        QtWidgets.QMessageBox.critical(self.main_window, title, message)

    def show_information_message(self, title: str, message: str):
        QtWidgets.QMessageBox.information(self.main_window, title, message)

    def get_file_dialog(self) -> list:
        """
        Shows open file dialog and returns selected paths. The starting
        directory is intelligently determined for the best user experience.
        """
        options = QtWidgets.QFileDialog.Options()
        settings = QtCore.QSettings(SETTINGS_ORGANIZATION, SETTINGS_APPLICATION)

        esaf = get_current_bluice_user()
        logger.info(f"Current bluice user: {esaf}")

        preferred_default_paths = [
            (os.path.join("/mnt/beegfs/DATA", esaf) if esaf else None),
            os.path.expanduser("~/DATA"),
            "/mnt/beegfs/DATA",
            os.path.expanduser("~"),
        ]

        best_default_dir = "."  # The ultimate fallback
        for path in preferred_default_paths:
            if path and os.path.isdir(path) and os.access(path, os.R_OK):
                best_default_dir = path
                break
        else:
            logger.warning(
                f"No preferred default paths were accessible. Falling back to '{best_default_dir}'."
            )

        logger.info(f"Determined best fallback directory: {best_default_dir}")

        start_dir = settings.value(SETTINGS_LAST_DIR_KEY, best_default_dir, type=str)

        if not os.path.isdir(start_dir):
            logger.warning(
                f"Last-used directory '{start_dir}' is no longer valid. "
                f"Using fallback '{best_default_dir}' instead."
            )
            start_dir = best_default_dir

        file_paths, _ = QtWidgets.QFileDialog.getOpenFileNames(
            self.main_window,
            "Select Master HDF5 File(s)",
            start_dir,  # Use the validated start_dir
            "HDF5 Files (*_master.h5 *_master.hdf5 *.nxs);;All Files (*)",
            options=options,
        )

        if file_paths:
            settings.setValue(SETTINGS_LAST_DIR_KEY, os.path.dirname(file_paths[0]))
            return file_paths
        return []

    def _set_initial_splitter_sizes(self, splitter: QtWidgets.QSplitter):
        """
        Sets initial sizes for the splitter after the layout has had a chance to settle.
        This method should be part of the UIManager class.
        """
        if not splitter:
            logger.warning(
                "_set_initial_splitter_sizes: Splitter not available for setting initial sizes."
            )
            return

        available_height = splitter.height()
        if available_height <= 0:  # Wait if height not yet determined
            QtCore.QTimer.singleShot(
                10, lambda: self._set_initial_splitter_sizes(splitter)
            )
            return

        # Define desired height for the Dozor plot container
        has_plugin = self.analysis_widget_layout.count() > 0
        if has_plugin:
            dozor_plot_height = 200  # Desired initial height for the bottom widget
        else:
            # Just enough to show the title bar and controls when no plugin is selected
            dozor_plot_height = self.analysis_plot_container.sizeHint().height()

        # Calculate height for the graphics_widget (image view)
        # Ensure graphics_widget has some minimum sensible height
        graphics_widget_height = max(100, available_height - dozor_plot_height)

        if available_height > (
            dozor_plot_height + 50
        ):  # Only set specific sizes if there's enough total space
            logger.debug(
                f"_set_initial_splitter_sizes: Setting splitter sizes to [{graphics_widget_height}, {dozor_plot_height}] with total height {available_height}"
            )
            splitter.setSizes([graphics_widget_height, dozor_plot_height])
        else:
            # Fallback: if not enough space, divide proportionally (e.g., 70/30 or based on current children hints)
            # This might happen if the window starts very small.
            current_sizes = splitter.sizes()
            if sum(current_sizes) > 0:  # If splitter has some default sizes
                logger.debug(
                    f"_set_initial_splitter_sizes: Splitter too small ({available_height}px), keeping current sizes: {current_sizes}"
                )
            else:  # Fallback to a proportional split if no current sizes
                proportional_graphics_height = int(available_height * 0.7)
                proportional_dozor_height = (
                    available_height - proportional_graphics_height
                )
                logger.debug(
                    f"_set_initial_splitter_sizes: Splitter too small ({available_height}px), setting proportional sizes: [{proportional_graphics_height}, {proportional_dozor_height}]"
                )
                splitter.setSizes(
                    [proportional_graphics_height, proportional_dozor_height]
                )

    # NEW METHOD: Handles the creation and display of the HDF5 metadata dialog.
    def show_hdf5_metadata(self):
        """Creates and shows a dialog with HDF5 metadata."""
        mw = self.main_window
        if not mw.reader or not mw.params:
            self.show_warning_message(
                "Metadata", "No HDF5 file with parameters is open."
            )
            return

        # Calculate denzo values
        beam_x = mw.params.get("beam_x", None)
        beam_y = mw.params.get("beam_y", None)
        pixel_size = mw.params.get("pixel_size", None)
        xbeam_mm = ybeam_mm = None
        if beam_x is not None and beam_y is not None and pixel_size is not None:
            try:
                xbeam_mm = round(float(beam_x) * float(pixel_size), 2)
                ybeam_mm = round(float(beam_y) * float(pixel_size), 2)
            except Exception:
                xbeam_mm = ybeam_mm = None

        metadata_lines = [
            f"<b>HDF5 Parameters for:</b> <span style='color:#007'>{os.path.basename(mw.current_master_file)}</span>",
            "<hr>",
        ]
        for key in sorted(mw.params.keys()):
            value = mw.params[key]
            if isinstance(value, np.ndarray):
                value_str = np.array2string(
                    value, threshold=10, edgeitems=2, max_line_width=80
                )
            elif isinstance(value, (list, tuple, dict)):
                import pprint

                value_str = pprint.pformat(value, width=80, compact=True)
            else:
                value_str = str(value)
            metadata_lines.append(
                f"<b>{key}</b>: <span style='color:#333'>{value_str}</span>"
            )
        # Add derived denzo values
        if xbeam_mm is not None and ybeam_mm is not None:
            metadata_lines.append("<hr>")
            metadata_lines.append(
                f"<b>beam_x (denzo)</b>: <span style='color:#333'>{xbeam_mm} mm</span>"
            )
            metadata_lines.append(
                f"<b>beam_y (denzo)</b>: <span style='color:#333'>{ybeam_mm} mm</span>"
            )
        metadata_str = "<br>\n".join(metadata_lines)

        dialog = QtWidgets.QDialog(mw)
        dialog.setWindowTitle(
            f"HDF5 Parameters - {os.path.basename(mw.current_master_file)}"
        )
        dialog.setMinimumSize(800, 600)
        layout = QtWidgets.QVBoxLayout(dialog)
        text_edit = QtWidgets.QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setFont(QtGui.QFont("Consolas", 10))
        text_edit.setHtml(metadata_str)
        layout.addWidget(text_edit)

        # 1. Create a horizontal layout for the action buttons
        button_layout = QtWidgets.QHBoxLayout()
        export_button = QtWidgets.QPushButton("Export HKL def.site")

        # 2. Create the new "Generate CrystFEL Geom" button
        generate_geom_button = QtWidgets.QPushButton("Generate CrystFEL Geom File...")
        generate_geom_button.setToolTip(
            "Create a .geom and bad pixel map .h5 file for CrystFEL."
        )

        # Add buttons to the layout
        button_layout.addWidget(export_button)
        button_layout.addWidget(generate_geom_button)

        # Add the button layout to the main dialog layout
        layout.addLayout(button_layout)

        def export_site_file():
            now = datetime.datetime.now().strftime("%H:%M:%S %b %d, %Y")
            user = getpass.getuser()
            detector = mw.params.get("detector", "CCD Eiger16m")
            if mw.params.get("nx") == 4150 and mw.params.get("ny") == 4371:
                hkl_detector = "CCD Eiger16m"
            elif mw.params.get("nx") == 4148 and mw.params.get("ny") == 4362:
                hkl_detector = "CCD Eiger2 16m"
            else:
                hkl_detector = f"CCD {detector}"

            xbeam_str = f"{xbeam_mm:.2f}" if xbeam_mm is not None else ""
            ybeam_str = f"{ybeam_mm:.2f}" if ybeam_mm is not None else ""
            content = (
                "HKLSuite0.95SITE\n"
                f"{{detec}} {{{hkl_detector}}}\n"
                f"{{last_saved,date}} {{{now}}}\n"
                f"{{last_saved,user}} {{{user}}}\n"
                "{rotation_axis} {Phi}\n"
                f"{{xbeam}} {{{ybeam_str}}}\n"
                f"{{ybeam}} {{{xbeam_str}}}\n"
            )
            # Auto-save to home directory with date only
            home = os.path.expanduser("~")
            date_tag = datetime.datetime.now().strftime("%Y%m%d")
            auto_path = os.path.join(home, f"def.site_{date_tag}")
            with open(auto_path, "w") as f:
                f.write(content)
            # Prompt user for save location
            options = QtWidgets.QFileDialog.Options()
            file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
                dialog,
                "Export def.site",
                f"{home}/def.site",
                "Site files (*.site);;All Files (*)",
                options=options,
            )
            if file_path:
                with open(file_path, "w") as f:
                    f.write(content)
                QtWidgets.QMessageBox.information(
                    dialog,
                    "Export",
                    f"File '{os.path.basename(file_path)}' has been exported.",
                )

        def generate_geom():
            """Handles the file dialogs and calls the geometry generation utility."""

            # 1. Ask the user for options FIRST using a custom dialog
            include_gaps = False
            include_mask = False
            
            try:
                logger.debug("Creating geometry options dialog...")
                opts_dialog = QtWidgets.QDialog(dialog)
                opts_dialog.setWindowTitle("Geometry Generation Options")
                opts_dialog.setMinimumWidth(400)
                
                opts_layout = QtWidgets.QVBoxLayout()
                opts_dialog.setLayout(opts_layout)
                
                cb_gaps = QtWidgets.QCheckBox("Include Panel Gaps (bad_v/bad_h regions)")
                cb_gaps.setToolTip("Automatically detect and write panel gaps into the geometry file.")
                cb_gaps.setChecked(False) 
                opts_layout.addWidget(cb_gaps)

                cb_mask = QtWidgets.QCheckBox("Link External Mask File")
                cb_mask.setToolTip("Add a reference to the generated mask file (will be saved as *_bad_pixels.h5) in the geometry.")
                cb_mask.setChecked(False)
                opts_layout.addWidget(cb_mask)

                opts_buttons = QtWidgets.QDialogButtonBox(
                    QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
                )
                opts_buttons.accepted.connect(opts_dialog.accept)
                opts_buttons.rejected.connect(opts_dialog.reject)
                opts_layout.addWidget(opts_buttons)

                logger.debug("Executing options dialog...")
                # Use exec_() for compatibility; if it returns 1 (Accepted), proceed.
                if opts_dialog.exec_() != 1: # 1 is QDialog.Accepted
                    logger.debug("Options dialog cancelled.")
                    return

                include_gaps = cb_gaps.isChecked()
                include_mask = cb_mask.isChecked()
                logger.info(f"Geometry options selected: include_gaps={include_gaps}, include_mask={include_mask}")

            except Exception as e:
                logger.error(f"Error showing options dialog: {e}", exc_info=True)
                return

            # 2. Get a single save-as filename from the user.
            default_dir = os.path.dirname(mw.current_master_file)
            default_name = (
                f"{os.path.splitext(os.path.basename(mw.current_master_file))[0]}.geom"
            )

            geom_path, _ = QtWidgets.QFileDialog.getSaveFileName(
                dialog,
                "Save CrystFEL Geometry File",
                os.path.join(default_dir, default_name),
                "CrystFEL Geometry Files (*.geom);;All Files (*)",
            )

            # If the user cancels, do nothing.
            if not geom_path:
                return

            # 3. Automatically derive the bad pixel map path
            pixel_map_path = f"{os.path.splitext(geom_path)[0]}_bad_pixels.h5"

            # 4. Call the utility function with all the collected info
            try:
                self.show_status_message("Generating CrystFEL files...", 0)
                QtWidgets.QApplication.processEvents()  # Force status message to show
                from qp2.image_viewer.plugins.crystfel.crystfel_geometry import (
                    generate_crystfel_geometry_file,
                )

                generated_geom, _ = generate_crystfel_geometry_file(
                    master_file_path=mw.current_master_file,
                    output_geom_path=geom_path,
                    bad_pixels_file_path=pixel_map_path,
                    include_gaps=include_gaps,
                    include_mask=include_mask,
                    save_mask_to_redis=False,  # We are creating local files
                )

                self.show_information_message(
                    "Success",
                    f"Successfully generated files:\n\nGeometry: {generated_geom}\nMask: {pixel_map_path}",
                )
            except Exception as e:
                logger.error(
                    f"Failed to generate CrystFEL geometry from UI: {e}", exc_info=True
                )
                self.show_critical_message(
                    "Generation Failed", f"An error occurred:\n\n{e}"
                )
            finally:
                self.clear_status_message_if("Generating")

        export_button.clicked.connect(export_site_file)
        generate_geom_button.clicked.connect(generate_geom)

        dialog.show()

    # NEW METHOD: Handles the creation and display of the About dialog.
    def show_about_dialog(self):
        """Shows the application's About box."""
        about_text = (
            f"<h2>GMCA/APS HDF5 Diffraction Viewer</h2><p>Version: 1.0.0</p>"
            "<p>Developed at GM/CA @ APS, Argonne National Laboratory.</p><br>"
            "<p><b>Developers:</b><ul><li>qxu</li></ul></p>"
            "<p>&copy; 2025 GM/CA @ APS. All rights reserved.</p>"
        )
        QtWidgets.QMessageBox.about(self.main_window, "About Viewer", about_text)

    def show_calibration_results_dialog(
        self, result, params, calibration_ring_resolution
    ):
        """
        Creates and shows the dedicated dialog for calibration results.
        """
        # The complex logic is now entirely self-contained within the dialog class.
        dialog = CalibrationResultsDialog(
            result,
            params,
            calibration_ring_resolution,
            self.main_window,  # Pass the main window as the parent
        )
        # Use show() for a non-modal dialog that allows interacting with the main window
        dialog.show()

    def update_dataset_tree(self, samples_data):
        """
        Updates the dataset tree widget differentially based on the two-level
        sample -> run -> dataset structure, avoiding complete clears to
        improve UI responsiveness in live mode.
        """
        if self.dataset_tree_widget is None:
            return

        def find_child(parent, text, column=0, data_role=None):
            for i in range(parent.childCount()):
                child = parent.child(i)
                if data_role is not None:
                    if child.data(column, data_role) == text:
                        return child
                else:
                    if child.text(column) == text:
                        return child
            return None

        root = self.dataset_tree_widget.invisibleRootItem()

        # Check if any samples are currently expanded
        expanded_samples = set()
        for i in range(root.childCount()):
            sample_item = root.child(i)
            if sample_item.isExpanded():
                expanded_samples.add(sample_item.text(0))

        # 1. Gather keys to remove obsolete samples
        current_samples = set(samples_data.keys())
        for i in reversed(range(root.childCount())):
            sample_item = root.child(i)
            if sample_item.text(0) not in current_samples:
                root.removeChild(sample_item)

        # 2. Sort sample prefixes by their creation time (newest first)
        sorted_sample_prefixes = sorted(
            samples_data.keys(),
            key=lambda sp: samples_data[sp].get("creation_time", 0),
            reverse=True,
        )

        for sample_idx, sample_prefix in enumerate(sorted_sample_prefixes):
            sample_data = samples_data[sample_prefix]
            runs_data = sample_data.get("runs", {})

            # Find or create sample item
            sample_item = find_child(root, sample_prefix)
            if not sample_item:
                sample_item = QtWidgets.QTreeWidgetItem()
                sample_item.setText(0, sample_prefix)
                font = sample_item.font(0)
                font.setBold(True)
                sample_item.setFont(0, font)
                sample_item.setIcon(
                    0,
                    self.dataset_tree_widget.style().standardIcon(
                        QtWidgets.QStyle.SP_DirIcon
                    ),
                )
                root.insertChild(sample_idx, sample_item)
                
                # Expand the newest sample by default if no samples are currently expanded
                if sample_idx == 0 and not expanded_samples:
                    sample_item.setExpanded(True)
            else:
                # Ensure it's at the correct index to preserve sorting
                if root.indexOfChild(sample_item) != sample_idx:
                    is_expanded = sample_item.isExpanded()
                    root.removeChild(sample_item)
                    root.insertChild(sample_idx, sample_item)
                    if is_expanded:
                        sample_item.setExpanded(True)

            # Gather keys to remove obsolete runs
            current_runs = set(runs_data.keys())
            for i in reversed(range(sample_item.childCount())):
                run_item = sample_item.child(i)
                if run_item.data(0, QtCore.Qt.ItemDataRole.UserRole) not in current_runs:
                    sample_item.removeChild(run_item)

            sorted_run_prefixes = sorted(runs_data.keys(), key=natural_sort_key)

            for run_idx, run_prefix in enumerate(sorted_run_prefixes):
                run_info = runs_data[run_prefix]
                
                # Find or create run item
                run_item = find_child(sample_item, run_prefix, data_role=QtCore.Qt.ItemDataRole.UserRole)
                if not run_item:
                    run_item = QtWidgets.QTreeWidgetItem()
                    display_run_name = run_prefix.replace(f"{sample_prefix}_", "")
                    run_item.setText(0, display_run_name)
                    run_item.setData(0, QtCore.Qt.ItemDataRole.UserRole, run_prefix)
                    font = sample_item.font(0)
                    run_item.setFont(0, font)
                    sample_item.insertChild(run_idx, run_item)
                else:
                    if sample_item.indexOfChild(run_item) != run_idx:
                        is_expanded = run_item.isExpanded()
                        sample_item.removeChild(run_item)
                        sample_item.insertChild(run_idx, run_item)
                        if is_expanded:
                            run_item.setExpanded(True)

                # Sort datasets within a run
                sorted_datasets = sorted(
                    run_info["datasets"],
                    key=lambda r: natural_sort_key(r.master_file_path),
                )
                current_dataset_paths = set(r.master_file_path for r in sorted_datasets)
                
                # Remove obsolete datasets
                for i in reversed(range(run_item.childCount())):
                    ds_item = run_item.child(i)
                    if ds_item.data(0, QtCore.Qt.ItemDataRole.UserRole) not in current_dataset_paths:
                        run_item.removeChild(ds_item)

                for ds_idx, reader in enumerate(sorted_datasets):
                    master_path = reader.master_file_path
                    base_name = (
                        os.path.basename(master_path)
                        .replace(f"{run_prefix}_", "")
                        .replace("_master.h5", "")
                    )

                    dataset_item = find_child(run_item, master_path, data_role=QtCore.Qt.ItemDataRole.UserRole)
                    if not dataset_item:
                        dataset_item = QtWidgets.QTreeWidgetItem()
                        dataset_item.setText(0, base_name)
                        dataset_item.setText(1, str(reader.total_frames))
                        dataset_item.setText(2, os.path.dirname(master_path))
                        dataset_item.setToolTip(0, master_path)
                        dataset_item.setData(
                            0, QtCore.Qt.ItemDataRole.UserRole, master_path
                        )
                        run_item.insertChild(ds_idx, dataset_item)
                    else:
                        # Update frames if changed
                        if dataset_item.text(1) != str(reader.total_frames):
                            dataset_item.setText(1, str(reader.total_frames))
                        
                        # Preserve sorting position
                        if run_item.indexOfChild(dataset_item) != ds_idx:
                            is_expanded = dataset_item.isExpanded()
                            is_selected = dataset_item.isSelected()
                            run_item.removeChild(dataset_item)
                            run_item.insertChild(ds_idx, dataset_item)
                            if is_expanded:
                                dataset_item.setExpanded(True)
                            if is_selected:
                                dataset_item.setSelected(True)

        # Fallback: if no selection but main window has a file open, prioritize that
        selected_items = self.dataset_tree_widget.selectedItems()
        if not selected_items and getattr(self.main_window, "current_master_file", None):
             self.select_dataset_in_tree(self.main_window.current_master_file)

    def select_dataset_in_tree(self, master_file_path: str):
        """
        Finds and highlights the specific dataset item in the QTreeWidget by
        iterating and checking the stored file path data.
        """
        logger.debug(f"Selecting dataset in tree: {master_file_path}")
        if not master_file_path or not self.dataset_tree_widget:
            return

        target_item = None

        # --- START: ROBUST ITERATIVE SEARCH ---
        # Get the invisible root item to start the iteration
        root = self.dataset_tree_widget.invisibleRootItem()
        for i in range(root.childCount()):  # Iterate through samples
            sample_item = root.child(i)
            for j in range(sample_item.childCount()):  # Iterate through runs
                run_item = sample_item.child(j)
                for k in range(run_item.childCount()):  # Iterate through datasets
                    dataset_item = run_item.child(k)

                    item_data = dataset_item.data(0, QtCore.Qt.ItemDataRole.UserRole)
                    if item_data and os.path.samefile(item_data, master_file_path):
                        target_item = dataset_item
                        break  # Found it, exit inner loop
                if target_item:
                    break  # Exit middle loop
            if target_item:
                break  # Exit outer loop
        # --- END: ROBUST ITERATIVE SEARCH ---

        logger.debug(f"Target item found: {target_item is not None}")

        if target_item:
            self.dataset_tree_widget.clearSelection()
            self.dataset_tree_widget.setCurrentItem(target_item)

            parent = target_item.parent()
            while parent:
                parent.setExpanded(True)
                parent = parent.parent()

            self.dataset_tree_widget.scrollToItem(target_item)

    def open_directory_dialog(self):
        """Shows a dialog to select a directory, configure filters, and initiates loading."""
        settings = QtCore.QSettings(SETTINGS_ORGANIZATION, SETTINGS_APPLICATION)
        start_dir = settings.value(
            SETTINGS_LAST_DIR_KEY, os.path.expanduser("~"), type=str
        )

        file_dialog = QtWidgets.QFileDialog(self.main_window, "Select Directories Containing Master Files", start_dir)
        file_dialog.setFileMode(QtWidgets.QFileDialog.DirectoryOnly)
        file_dialog.setOption(QtWidgets.QFileDialog.DontUseNativeDialog, True)

        file_view = file_dialog.findChild(QtWidgets.QListView, 'listView')
        if file_view:
            file_view.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        
        f_tree_view = file_dialog.findChild(QtWidgets.QTreeView)
        if f_tree_view:
            f_tree_view.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)

        directory_paths = []
        if file_dialog.exec_():
            directory_paths = file_dialog.selectedFiles()

        if directory_paths:
            # Bug fix: Filter out parent directories if their subdirectories are also selected.
            # This happens with QFileDialog multi-selection in DirectoryOnly mode.
            if len(directory_paths) > 1:
                cleaned_paths = list(set([os.path.abspath(p) for p in directory_paths]))
                final_paths = []
                for p in cleaned_paths:
                    is_parent_of_other = False
                    p_slash = p if p.endswith(os.sep) else p + os.sep
                    for q in cleaned_paths:
                        if p != q and q.startswith(p_slash):
                            is_parent_of_other = True
                            break
                    if not is_parent_of_other:
                        final_paths.append(p)
                directory_paths = final_paths

            if directory_paths:
                settings.setValue(SETTINGS_LAST_DIR_KEY, directory_paths[0])
            else:
                 # Should not happen if original list was not empty, but safety check
                 pass

            # --- Create a configuration dialog ---
            dialog = QtWidgets.QDialog(self.main_window)
            dialog.setWindowTitle("Load Options")
            layout = QtWidgets.QVBoxLayout(dialog)

            # Recursive Option
            cb_recursive = QtWidgets.QCheckBox("Search recursively (subdirectories)")
            cb_recursive.setChecked(True)
            layout.addWidget(cb_recursive)

            # Keep Existing Option
            cb_keep_existing = QtWidgets.QCheckBox("Keep Existing Datasets")
            cb_keep_existing.setChecked(True) # Default to keeping for better UX
            layout.addWidget(cb_keep_existing)

            # Path Filter
            layout.addWidget(QtWidgets.QLabel("Path must contain (optional):"))
            le_path_filter = QtWidgets.QLineEdit()
            le_path_filter.setPlaceholderText("e.g., 'collect'")
            layout.addWidget(le_path_filter)

            layout.addWidget(QtWidgets.QLabel("Exclude path containing (optional):"))
            le_path_not_filter = QtWidgets.QLineEdit()
            le_path_not_filter.setPlaceholderText("e.g., 'test'")
            layout.addWidget(le_path_not_filter)

            # Image Count Filter
            layout.addWidget(QtWidgets.QLabel("Frame Count:"))
            count_layout = QtWidgets.QHBoxLayout()
            
            sb_min_images = QtWidgets.QSpinBox()
            sb_min_images.setRange(0, 1000000)
            sb_min_images.setValue(0)
            sb_min_images.setPrefix(">= ")
            count_layout.addWidget(sb_min_images)

            count_layout.addWidget(QtWidgets.QLabel("and"))

            sb_max_images = QtWidgets.QSpinBox()
            sb_max_images.setRange(0, 1000000)
            sb_max_images.setValue(1000000)
            sb_max_images.setPrefix("<= ")
            sb_max_images.setSpecialValueText("No Limit")
            count_layout.addWidget(sb_max_images)
            
            layout.addLayout(count_layout)

            # Buttons
            button_box = QtWidgets.QDialogButtonBox(
                QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
            )
            button_box.accepted.connect(dialog.accept)
            button_box.rejected.connect(dialog.reject)
            layout.addWidget(button_box)

            if dialog.exec_() == QtWidgets.QDialog.Accepted:
                # Handle max_images: if it's the max value, treat as None
                max_img = sb_max_images.value()
                if max_img == sb_max_images.maximum():
                    max_img = None

                # Delegate to main window with all collected parameters
                self.main_window.load_files_from_directory(
                    directory_paths,
                    recursive=cb_recursive.isChecked(),
                    min_images=sb_min_images.value(),
                    max_images=max_img,
                    path_contains=le_path_filter.text().strip(),
                    path_not_contains=le_path_not_filter.text().strip(),
                    keep_existing=cb_keep_existing.isChecked()
                )
