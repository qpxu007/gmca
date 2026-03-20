# qp2/image_viewer/ui/image_viewer.py

import importlib
import json
import os
import glob
from typing import List, Optional

import numpy as np
import pyqtgraph as pg
from PyQt5.QtCore import (
    QThreadPool,
    pyqtSlot,
    QTimer,
    QPointF,
    QPoint,
)
from pyqtgraph.GraphicsScene.mouseEvents import MouseClickEvent
from pyqtgraph.Qt import QtCore, QtGui, QtWidgets

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

# --- Local Imports ---
from qp2.xio.hdf5_manager import HDF5Reader
from qp2.xio.redis_manager import RedisManager
from qp2.image_viewer.actions.dataset_context_manager import DatasetContextMenuManager
from qp2.image_viewer.ui.busy_cursor import BusyCursor
from qp2.image_viewer.actions.settings_manager import SettingsManager
from qp2.image_viewer.actions.playback_manager import PlaybackManager, PlaybackState
from qp2.image_viewer.actions.file_io_manager import FileIOManager
from qp2.image_viewer.dataset.dataset_manager import DatasetManager
from qp2.image_viewer.strategy.run_strategy import StrategyWorker
from qp2.image_viewer.workers.directory_loader import DirectoryLoaderWorker
from qp2.image_viewer.beamcenter.beam_center_dialog import BeamCenterDialog

# DEFERRED: from qp2.image_viewer.ui.settings import SettingsDialog
from qp2.utils.icon import generate_icon_with_text
from qp2.image_viewer.utils.ring_math import angstrom_to_pixels
from qp2.image_viewer.utils.sort_files import natural_sort_key
from qp2.xio.db_manager import get_beamline_from_hostname

from qp2.image_viewer.config import (
    MASKED_CIRCLES,
    MASKED_RECTANGLES,
    DEFAULT_SETTINGS,
    ZOOM_CONTRAST_DEBOUNCE_MS,
    PIXEL_TEXT_UPDATE_DEBOUNCE_MS,
    PIXEL_TEXT_ZOOM_THRESHOLD,
    PLAYBACK_LAG_THRESHOLD,
    PLAYBACK_JUMP_OFFSET,
)
from qp2.image_viewer.utils.mask_computation import (
    compute_detector_mask,
    compute_annular_mask,
    compute_valid_pixel_mask,
)
from qp2.image_viewer.utils.contrast_utils import (
    calculate_contrast_levels,
    calculate_histogram_range,
    extract_view_subset,
    calculate_zoom_ratio,
)
from qp2.image_viewer.utils.pixel_utils import calculate_pixel_info, calculate_distance
from qp2.image_viewer.utils.image_stats import (
    calculate_image_statistics,
    format_statistics_text,
)
from qp2.image_viewer.utils.validation_utils import (
    extract_valid_parameters,
)

from qp2.image_viewer.plugins.spot_finder.live_peak_finding_manager import (
    LivePeakFindingManager,
)
from qp2.image_viewer.actions.summation_manager import SummationManager


# Lazy import: HeatmapManager, VolumeManager, and MergingManager will be imported on first use
# from qp2.image_viewer.heatmap.heatmap_manager import HeatmapManager
# from qp2.image_viewer.volume_map.volume_manager import VolumeManager
# from qp2.image_viewer.plugins.crystfel.crystfel_merging_manager import MergingManager
# DEFERRED: from qp2.image_viewer.workers.radial_sum import RadialSumManager
# DEFERRED: from qp2.image_viewer.actions.image_filter_manager import ImageFilterManager
# DEFERRED: from qp2.image_viewer.actions.calibration_manager import CalibrationManager
# DEFERRED: from qp2.image_viewer.plugins.dozor.dozor_manager import DozorManager
# DEFERRED: from qp2.image_viewer.plugins.spot_finder.spot_finder_manager import SpotFinderManager
# DEFERRED: from qp2.image_viewer.workers.ice_ring_analyzer import IceRingWorker
# DEFERRED: from qp2.image_viewer.plugins.crystfel.crystfel_manager import CrystfelManager
# DEFERRED: from qp2.image_viewer.plugins.dials_ssx.dials_manager import DialsManager
# DEFERRED: from qp2.image_viewer.plugins.nxds.nxds_manager import NXDSManager
# DEFERRED: from qp2.image_viewer.plugins.xds.xds_manager import XDSManager


class DiffractionViewerWindow(QtWidgets.QMainWindow):
    """
    Main window coordinating managers, state, and background workers.
    """

    def __init__(
        self,
        initial_file_path: Optional[str],
        all_file_paths: List[str],
        live_mode: bool = False,
        query_redis_for_initial_file: bool = False,
    ):
        super().__init__()

        logger.info("DiffractionViewerWindow: Initializing main window.")

        # --- Store initial loading parameters ---
        self._initial_file_path = initial_file_path
        self._all_file_paths = all_file_paths
        self._query_redis_for_initial_file = query_redis_for_initial_file

        self._is_live_mode = live_mode

        try:
            self.redis_manager = RedisManager()
        except Exception as e:
            logger.error(
                f"A critical error occurred while creating RedisManager: {e}",
                exc_info=True,
            )
            self.redis_manager = None

        # --- Settings Manager ---
        self.settings_manager = SettingsManager()
        self.settings_manager.settings_changed.connect(self._on_settings_changed)

        # --- Playback Manager ---
        self.playback_manager = PlaybackManager(self)
        self.playback_manager.frame_changed.connect(self._on_frame_changed)
        self.playback_manager.state_changed.connect(self._on_playback_state_changed)

        # Timer for debouncing slider ---
        self.slider_update_timer = QTimer()
        self.slider_update_timer.setSingleShot(True)
        self.slider_update_timer.setInterval(50)  # 50ms delay
        self.slider_update_timer.timeout.connect(self._on_slider_timer_timeout)
        self.is_slider_dragging = False

        # --- Summation State ---
        self.summation_active = False
        self.contrast_locked = False  # lock contrast

        # --- Core Data and Managers (Initialize to empty/None state) ---
        self.reader = None
        self.dataset_manager = DatasetManager()
        self.params = {}
        self.threadpool = QThreadPool.globalInstance()

        # --- Application State ---
        self.current_frame_index = 0
        self.latest_available_frame_index = -1
        self.current_master_file = None
        self.resolution_rings_visible = False
        self.sum_frames_enabled = False
        self.waiting_for_sum_worker = False
        self.directory_loader_worker = None
        self.image_filter_enabled = False
        self.auto_peak_finding_enabled = False
        self.auto_contrast_on_zoom = False
        self.waiting_for_peaks = False
        self.dataset_contrast_set = False
        self.is_zoomed_for_text = False
        self.last_mouse_pos_img = None
        self.image_stats_overlay_enabled = False
        # detector_mask, display_mask, and mask_overlay_visible are now properties 
        # delegating to detector_mask_manager.
        self.strategy_dialogs = {}

        try:
            if self.redis_manager:
                self.redis_output_server = self.redis_manager.get_analysis_connection()
            else:
                self.redis_output_server = None
        except Exception as e:
            logger.error(
                f"Error getting analysis Redis connection in __init__: {e}",
                exc_info=True,
            )
            self.redis_output_server = None

        # --- Image Data Storage ---
        self._original_image = None
        self._original_image_before_sum = None
        self._filtered_image = None

        # --- Measurement Mode State ---
        self.is_manual_calibration_mode = False
        self.manual_calibration_dialog = None

        # --- Mask Values ---
        self.mask_values = set()
        self._update_mask_values()

        # --- Instantiate Managers (imports deferred until now) ---
        from qp2.image_viewer.ui.ui_manager import UIManager
        from qp2.image_viewer.ui.graphics_manager import GraphicsManager
        from qp2.image_viewer.ui.python_console import PythonConsoleWidget
        from qp2.image_viewer.ai.assistant import AIAssistantWidget, AIAssistantWindow
        from qp2.image_viewer.utils.redis_cache import save_numpy_array_to_redis
        from qp2.image_viewer.actions.measurement_manager import MeasurementManager
        from qp2.image_viewer.actions.ice_ring_manager import IceRingManager
        from qp2.image_viewer.actions.detector_mask_manager import DetectorMaskManager
        from qp2.image_viewer.actions.strategy_manager import StrategyManager
        from qp2.image_viewer.actions.analysis_plugin_manager import AnalysisPluginManager
        from qp2.image_viewer.ui.dataset_tree_manager import DatasetTreeManager
        
        self.ui_manager = UIManager(self)
        self.measurement_manager = MeasurementManager(self)
        self.ice_ring_manager = IceRingManager(self)
        self.detector_mask_manager = DetectorMaskManager(self)

        self.ui_manager.setup_ui()
        
        self.file_io_manager = FileIOManager(self)
        self.live_peak_finding_manager = LivePeakFindingManager(self)
        self.summation_manager = SummationManager(self)
        # Lazy managers: instantiate on first use via properties below
        self._heatmap_manager = None
        self._volume_manager = None
        self._merging_manager = None
        self.dataset_context_manager = DatasetContextMenuManager(self)
        self.image_filter_manager = None
        self.calibration_manager = None
        self.radial_sum_manager = None
        self._bad_pixel_manager = None
        self.graphics_manager = GraphicsManager(
            self.ui_manager.view_box, self.ui_manager.hist_lut, self
        )
        self.strategy_manager = StrategyManager(self)
        self.analysis_plugin_manager = AnalysisPluginManager(self)
        self.dataset_tree_manager = DatasetTreeManager(self)

        self.file_io_manager.data_loaded.connect(self._on_data_loaded)
        self.file_io_manager.load_failed.connect(self._on_load_failed)
        self.summation_manager.summation_complete.connect(self._on_summation_complete)
        self.summation_manager.summation_error.connect(self._on_summation_error)
        self.summation_manager.summation_stopped.connect(self._on_summation_stopped)

        # --- Timers ---
        self.zoom_contrast_timer = QTimer()
        self.zoom_contrast_timer.setSingleShot(True)
        self.zoom_contrast_timer.setInterval(ZOOM_CONTRAST_DEBOUNCE_MS)
        self.pixel_text_update_timer = QTimer()
        self.pixel_text_update_timer.setSingleShot(True)
        self.pixel_text_update_timer.setInterval(PIXEL_TEXT_UPDATE_DEBOUNCE_MS)

        # --- Window Setup ---
        self.update_window_title()
        self.app_icon = generate_icon_with_text(text="iv", bg_color="#e74c3c", size=128)
        self.setWindowIcon(self.app_icon)
        
        # Set the application-wide icon to ensure taskbars pick it up reliably on Linux Desktop Environments
        app_instance = QtWidgets.QApplication.instance()
        if app_instance:
            app_instance.setWindowIcon(self.app_icon)
            # Wayland window managers heavily rely on desktopFileName for matching icons
            app_instance.setDesktopFileName("qp2_image_viewer")

        # Calculate starting window size to make the image view roughly square.
        # Fixed elements: left panel (~250px width), plugin area (~50px height), window chrome (~60px)
        target_image_size = 850
        window_width = target_image_size + 250 + 15  # Includes left panel & splitter/margin width
        window_height = target_image_size + 50 + 60  # Includes plugin title bar & menu/status bars
        self.setGeometry(100, 100, window_width, window_height)

        # Dataset tree filtering now handled by DatasetTreeManager

        # Modules loaded on first use via AnalysisPluginManager

        self.active_workers = set()

        # --- Lazy-loaded Widgets (initialized to None) ---
        self._console_widget = None
        self._console_dock = None
        self._ai_assistant_window = None

        # --- Connect Signals ---
        self._connect_signals()
        self._connect_manager_signals()
        self.dataset_manager.runs_changed.connect(self._update_dataset_tree_widget)
        
        # Resolve group_name for scoping Redis processing override keys
        self._group_name = self._resolve_group_name()

        # Pull live processing overrides from Redis so we start perfectly in sync
        self._pull_redis_processing_overrides()
        
        # Schedule initial data loading and live mode startup
        QtCore.QTimer.singleShot(10, self._deferred_initial_load)

    @property
    def detector_mask(self):
        return self.detector_mask_manager.mask

    @detector_mask.setter
    def detector_mask(self, value):
        # Backward compatibility setter
        self.detector_mask_manager._mask = value
        
    @property
    def display_mask(self):
        return self.detector_mask_manager.display_mask
        
    @display_mask.setter
    def display_mask(self, value):
        self.detector_mask_manager._display_mask = value
    
    @property
    def mask_overlay_visible(self):
        return self.detector_mask_manager.mask_overlay_visible

    @mask_overlay_visible.setter
    def mask_overlay_visible(self, value):
        self.detector_mask_manager.mask_overlay_visible = value

    def update_window_title(self):
        """Updates the window title based on the live mode status."""
        title = "GMCA/APS HDF5 Diffraction Viewer"
        if self._is_live_mode:
            title += " [LIVE MODE]"
        else:
            title += " [REVIEW MODE]"
        
        if self.current_master_file:
            title += f" - {os.path.basename(self.current_master_file)}"

        self.setWindowTitle(title)

    @property
    def is_live_mode(self):
        return self._is_live_mode

    @is_live_mode.setter
    def is_live_mode(self, value):
        self._is_live_mode = value
        self.update_window_title()

    @property
    def console_widget(self):
        if self._console_widget is None:
            # Try Advanced Console first
            from qp2.image_viewer.ui.advanced_console import AdvancedConsoleWidget, HAS_QTCONSOLE
            if HAS_QTCONSOLE:
                self._console_widget = AdvancedConsoleWidget(namespace={'viewer': self, 'np': np})
                logger.info("Initialized Advanced Python Console (IPython).")
            else:
                # Fallback to Standard Console
                from qp2.image_viewer.ui.python_console import PythonConsoleWidget
                self._console_widget = PythonConsoleWidget(namespace={'viewer': self, 'np': np})
                logger.info("Initialized Standard Python Console (Fallback).")
        return self._console_widget

    @property
    def console_dock(self):
        if self._console_dock is None:
            # Ensure widget exists
            widget = self.console_widget
            self._console_dock = QtWidgets.QDockWidget("Python Console", self)
            self._console_dock.setWidget(widget)
            self.addDockWidget(QtCore.Qt.DockWidgetArea.BottomDockWidgetArea, self._console_dock)
            self._console_dock.hide()
        return self._console_dock

    def get_ai_assistant_window(self):
        if self._ai_assistant_window is None:
            from qp2.image_viewer.ai.assistant import AIAssistantWindow
            self._ai_assistant_window = AIAssistantWindow(
                namespace_provider=self._get_ai_namespace,
                parent=self
            )
            # Connect signal here for lazy loading
            self._ai_assistant_window.closed.connect(lambda: self.toggle_ai_action.setChecked(False))
        return self._ai_assistant_window

    def _get_ai_namespace(self):
        """
        Constructs the namespace for the AI Assistant, ensuring key variables
        and aliases are present for easier coding.
        """
        # Start with the console's current namespace (which tracks updates)
        # Use property to ensure it exists if AI is called
        ns = self.console_widget.localNamespace.copy()
        
        # Ensure 'viewer' and 'np' are always present
        if 'viewer' not in ns:
            ns['viewer'] = self
        if 'np' not in ns:
            ns['np'] = np
        
        # Add pyqtgraph (pg) for direct access
        if 'pg' not in ns:
            ns['pg'] = pg
            
        # Add aliases for parameters if they exist in 'params'
        if 'params' in ns and isinstance(ns['params'], dict):
            p = ns['params']
            
            # Debug logging
            logger.info(f"AI Assistant Namespace Params Keys: {list(p.keys())}")

            # Map 'det_dist' or 'detector_distance' to 'distance'
            if 'distance' not in p:
                if 'det_dist' in p:
                    p['distance'] = p['det_dist']
                elif 'detector_distance' in p:
                    p['distance'] = p['detector_distance']
                elif 'detector_dist_m' in p: # Common in your code
                    p['distance'] = p['detector_dist_m']

            # Handle Wavelength / Energy
            if 'wavelength' not in p:
                if 'energy' in p:
                    # Approximation: E (keV) = 12.398 / lambda (A) => lambda = 12.398 / E
                    # Check units! Assuming eV or keV.
                    pass # Logic too complex for simple alias without knowing units
                elif 'energy_eV' in p:
                     try:
                         p['wavelength'] = 12398.4 / float(p['energy_eV'])
                     except (ValueError, TypeError):
                         pass

            # Expose top-level variables for convenience
            for key in ['wavelength', 'pixel_size', 'beam_x', 'beam_y', 'distance', 'det_dist', 'detector_dist_m']:
                if key in p:
                    ns[key] = p[key]

        # Expose masks
        if hasattr(self, 'detector_mask'):
            ns['detector_mask'] = self.detector_mask
        if hasattr(self, 'display_mask'):
            ns['display_mask'] = self.display_mask
        if hasattr(self, 'mask_values'):
            ns['mask_values'] = self.mask_values
                    
        return ns
    @property
    def heatmap_manager(self):
        """Lazy-instantiate and return HeatmapManager."""
        if self._heatmap_manager is None:
            try:
                module = importlib.import_module(
                    "qp2.image_viewer.heatmap.heatmap_manager"
                )
                ManagerClass = getattr(module, "HeatmapManager")
                self._heatmap_manager = ManagerClass(self)
                logger.info("HeatmapManager initialized on first use.")
            except Exception as e:
                logger.error(f"Failed to initialize HeatmapManager: {e}", exc_info=True)
                raise
        return self._heatmap_manager

    @property
    def volume_manager(self):
        """Lazy-instantiate and return VolumeManager."""
        if self._volume_manager is None:
            try:
                module = importlib.import_module(
                    "qp2.image_viewer.volume_map.volume_manager"
                )
                ManagerClass = getattr(module, "VolumeManager")
                self._volume_manager = ManagerClass(self)
                logger.info("VolumeManager initialized on first use.")
            except Exception as e:
                logger.error(f"Failed to initialize VolumeManager: {e}", exc_info=True)
                raise
        return self._volume_manager

    @property
    def merging_manager(self):
        """Lazy-instantiate and return MergingManager."""
        if self._merging_manager is None:
            try:
                module = importlib.import_module(
                    "qp2.image_viewer.plugins.crystfel.crystfel_merging_manager"
                )
                ManagerClass = getattr(module, "MergingManager")
                self._merging_manager = ManagerClass(self)
                logger.info("MergingManager initialized on first use.")
            except Exception as e:
                logger.error(f"Failed to initialize MergingManager: {e}", exc_info=True)
                raise
        return self._merging_manager

    def _deferred_initial_load(self):
        """
        This method is called once, right after the UI becomes visible.
        It handles the initial (potentially slow) data loading operations.
        """
        file_to_load = self._initial_file_path

        # If we were told to query redis, do it now.
        if self._query_redis_for_initial_file and self.redis_manager:
            logger.info(
                "No master file provided; querying Redis for the latest to start..."
            )
            redis_file = self.redis_manager.get_latest_dataset_path()
            if redis_file:
                logger.info(f"Found latest master file via Redis: {redis_file}")
                file_to_load = redis_file
                # Add it to the list if it's not already there
                if redis_file not in self._all_file_paths:
                    self._all_file_paths.insert(0, redis_file)
            else:
                logger.info(
                    "No file found via Redis. Starting with a blank viewer in live mode."
                )

        # Now, load the determined file (if any)
        if file_to_load:
            # The FileIOManager handles loading and emits signals on success/failure
            self.file_io_manager.load_file(file_to_load)
        else:
            # No file to load, start with a blank screen
            self.ui_manager.update_frame_elements(0, 0, -1)
            self.ui_manager.show_status_message(
                "No data loaded. Please load a dataset.", 3000
            )
            self.graphics_manager.display_blank_image()

        # If there are other files, load them in the background
        if len(self._all_file_paths) > 1:
            # We already loaded the first one, so start from the second
            other_files = [f for f in self._all_file_paths if f != file_to_load]
            if other_files:
                # Use another timer to avoid blocking the UI thread right after the first load
                QTimer.singleShot(
                    100, lambda: self.load_datasets_parallel(other_files)
                )

        # Start Redis monitoring if in live mode
        if self.is_live_mode:
            logger.info("Live mode enabled, starting Redis monitoring.")
            if self.redis_manager:
                self.redis_manager.start_monitoring()
            # If we started with a file in live mode, start playing
            if file_to_load and self.reader and self.reader.total_frames > 0:
                logger.info("Live mode with initial data, auto-starting playback.")
                QTimer.singleShot(100, self.toggle_playback)

    def load_datasets_parallel(self, file_paths: List[str]):
        """
        Loads additional datasets in the background using parallel threads
        to minimize UI freezing and total load time.
        """
        import concurrent.futures

        logger.info(f"Loading {len(file_paths)} additional datasets in the background (Parallel).")
        
        # Helper function to read a single file's metadata
        def _read_file_metadata(path):
            try:
                # Open with start_timer=False to avoid overhead
                reader = HDF5Reader(path, start_timer=False)
                # Force read of parameters now, in the worker thread
                params = reader.get_parameters()
                return (reader, params)
            except Exception as e:
                logger.error(f"Failed to load additional file {path}: {e}")
                return None

        # Use ThreadPoolExecutor to parallelize I/O and metadata reading
        # max_workers=4 is usually a sweet spot for I/O bound tasks without overwhelming the GIL
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            # Create a map of future -> file_path for debugging/tracking if needed
            future_to_path = {executor.submit(_read_file_metadata, path): path for path in file_paths}
            
            for future in concurrent.futures.as_completed(future_to_path):
                res = future.result()
                if res:
                    results.append(res)
        
        # Batch add all successfully loaded datasets to the manager
        # This triggers only ONE 'runs_changed' signal, refreshing the UI once.
        if results:
            self.dataset_manager.add_datasets(results)
            logger.info(f"Successfully batch-loaded {len(results)} datasets.")

    @pyqtSlot(object)
    def _on_worker_finished(self, worker_instance):
        """
        Removes a worker from the active set to allow garbage collection.
        This slot is connected to the finished/error signals of workers.
        """
        if worker_instance in self.active_workers:
            self.active_workers.remove(worker_instance)

    def _connect_signals(self):
        """Connect signals from timers, viewbox, LUT, etc."""
        self.ui_manager.connect_signals()
        QtCore.QTimer.singleShot(50, self._connect_mouse_move)

        self.ui_manager.hist_lut.sigLevelsChanged.connect(
            self._update_image_levels_from_lut
        )
        self.ui_manager.hist_lut.sigLookupTableChanged.connect(
            self._update_image_lut_from_lut
        )
        self.ui_manager.view_box.sigRangeChanged.connect(
            self._handle_range_change_debounced
        )

        self.zoom_contrast_timer.timeout.connect(self._update_contrast_from_view)
        self.pixel_text_update_timer.timeout.connect(self._trigger_pixel_text_update)

        self.dataset_manager.runs_changed.connect(self._update_dataset_tree_widget)

        if self.ui_manager.view_box and self.ui_manager.view_box.scene():
            self.ui_manager.view_box.scene().sigMouseClicked.connect(
                self.measurement_manager.handle_mouse_click
            )
        else:
            QtCore.QTimer.singleShot(
                100, self._connect_mouse_click_for_distance_measurement
            )

        self.ui_manager.populate_analysis_selector(
            list(self.analysis_plugin_manager.available_plugins.keys())
        )

    def _connect_mouse_click_for_distance_measurement(self):
        """Helper to retry connecting the mouse click signal."""
        if self.ui_manager.view_box and self.ui_manager.view_box.scene():
            self.ui_manager.view_box.scene().sigMouseClicked.connect(
                self.measurement_manager.handle_mouse_click
            )

    def _connect_manager_signals(self):
        """Connect signals from RedisManager."""
        if self.redis_manager:
            # new_master_file_stream is handled by FileIOManager

            self.redis_manager.run_started.connect(self._on_run_started)
            self.redis_manager.run_completed.connect(self._on_run_completed)
            self.redis_manager.status_update.connect(
                lambda msg: self.ui_manager.show_status_message(msg, 3000)
            )
            self.redis_manager.connection_error.connect(
                lambda msg: self.ui_manager.show_critical_message(
                    "Redis Connection Error", msg
                )
            )
            # if self.enable_dataset_autoupdate:
            #     self.redis_manager.start_monitoring()

    def update_frame_display(self, frame_index: int, is_playback: bool = False):
        """
        Loads and displays the specified frame. Delegates processing to managers.
        """
        self.graphics_manager.clear_peaks()
        if not is_playback:
            self.graphics_manager.clear_spots()
            self.graphics_manager.clear_indexed_reflections()
            self.graphics_manager.clear_plugin_info_text()

        if not self.reader or not (0 <= frame_index < self.reader.total_frames):
            logger.warning(f"Frame index {frame_index} is out of bounds.")
            if self.playback_manager.state == PlaybackState.PLAYING:
                self.toggle_playback()
            return

        if self.summation_manager.is_active:
            self.summation_manager.trigger_summation()
            return

        frame_data = self.reader.get_frame(frame_index)
        if frame_data is None:
            if self.playback_manager.state == PlaybackState.PLAYING:
                self.playback_manager.pause(is_user_request=False)
            return

        self._original_image = frame_data.copy()
        
        vars_to_push = {
            'image': self._original_image, 
            'frame_index': frame_index,
            'detector_mask': self.detector_mask,
            'display_mask': self.display_mask,
            'beam_center_x': self.params.get('beam_x'),
            'beam_center_y': self.params.get('beam_y'),
            'wavelength': self.params.get('wavelength'),
            'pixel_size': self.params.get('pixel_size'),
            'det_dist_m': self.params.get('det_dist_m'),
            'energy_eV': self.params.get('energy_eV'),
        }
        
        if self._console_widget:
            self._console_widget.push_vars(vars_to_push)

        # Check and update detector mask if needed
        self.detector_mask_manager.ensure_mask_up_to_date()

        if self.image_filter_manager and self.image_filter_manager.is_active:
            self.image_filter_manager.apply_filter(self._original_image)
            return

        self._display_final_image(self._original_image)

    def slider_changed_by_user(self, value):
        """
        This slot is connected to valueChanged. It handles programmatic updates
        and simple user clicks, but ignores rapid drag events.
        """
        if self.is_slider_dragging:
            # If the user is dragging, do nothing here. The debouncing logic will handle it.
            return

        # This will handle playback updates or if the user just clicks on the slider bar.
        self.playback_manager.go_to_frame(value)

    def slider_dragged(self, value):
        """
        This slot is connected to sliderMoved. It restarts the debounce timer
        on every move, preventing image updates until the user pauses.
        """
        self.slider_update_timer.start()

    def slider_released(self):
        """
        This slot is connected to sliderReleased. It immediately triggers
        the final frame update.
        """
        self.is_slider_dragging = False
        # Stop any pending timer and immediately update to the final position.
        if self.slider_update_timer.isActive():
            self.slider_update_timer.stop()
        self._on_slider_timer_timeout()

    def _on_slider_timer_timeout(self):
        """
        Called 50ms after the user stops dragging. This is where the
        expensive image update happens.
        """
        # The playback_manager will handle pausing and emitting frame_changed
        self.playback_manager.go_to_frame(self.ui_manager.frame_slider.value())

    def prev_frame(self):
        self.playback_manager.prev_frame()

    def next_frame(self):
        self.playback_manager.next_frame()

    def go_to_frame(self):
        try:
            target_frame_one_based = int(self.ui_manager.frame_input.text())
            target_frame_zero_based = target_frame_one_based - 1
            if not self.reader or not (
                0 <= target_frame_zero_based < self.reader.total_frames
            ):
                raise ValueError("Frame number out of range.")
            if target_frame_zero_based > self.latest_available_frame_index:
                target_frame_zero_based = self.latest_available_frame_index
            self.playback_manager.go_to_frame(target_frame_zero_based)
        except ValueError as e:
            logger.warning(f"go_to_frame: Invalid frame input: {e}")
            self.ui_manager.show_warning_message("Invalid Input", str(e))
            self.ui_manager.frame_input.setText(str(self.current_frame_index + 1))

    def focus_and_find_peaks(self):
        self.activateWindow()
        self.live_peak_finding_manager.run()

    def enhance_region_based_on_resolution(
        self, inner_resolution=10.0, outer_resolution=3.5
    ):
        # Validate parameters and image
        if (
            not self.graphics_manager.img_item
            or self.graphics_manager.img_item.image is None
        ):
            return

        required_params = ["wavelength", "det_dist", "pixel_size", "beam_x", "beam_y"]
        param_values = extract_valid_parameters(self.params, required_params)
        if param_values is None:
            return

        wl, det_dist, px_size, beam_x, beam_y = param_values
        img = self.graphics_manager.img_item.image
        try:
            r_outer = angstrom_to_pixels(outer_resolution, wl, det_dist, px_size)
            r_inner = angstrom_to_pixels(inner_resolution, wl, det_dist, px_size)
        except (ValueError, ZeroDivisionError) as e:
            logger.warning(
                f"enhance_region_based_on_resolution: Error in angstrom_to_pixels: {e}"
            )
            return

        # Use utility functions for mask computation
        annulus_mask = compute_annular_mask(img, beam_x, beam_y, r_inner, r_outer)
        valid_mask = compute_valid_pixel_mask(img, self.detector_mask)

        final_mask = annulus_mask & valid_mask
        annulus_pixels = img[final_mask]
        if annulus_pixels.size == 0:
            return

        # Use utility function for contrast calculation
        low, high = calculate_contrast_levels(
            annulus_pixels,
            self.settings_manager.get("contrast_low_percentile"),
            self.settings_manager.get("contrast_high_percentile"),
        )
        self.graphics_manager.hist_lut.setLevels(low, high)
        self.focus_on_region_based_on_resolution(outer_resolution=outer_resolution)

    def focus_on_region_based_on_resolution(self, outer_resolution=3.5):
        # Validate parameters and image
        if (
            not self.graphics_manager.img_item
            or self.graphics_manager.img_item.image is None
        ):
            return

        required_params = ["wavelength", "det_dist", "pixel_size", "beam_x", "beam_y"]
        param_values = extract_valid_parameters(self.params, required_params)
        if param_values is None:
            return

        wl, det_dist, px_size, beam_x, beam_y = param_values
        try:
            r_outer = angstrom_to_pixels(outer_resolution, wl, det_dist, px_size)
        except (ValueError, ZeroDivisionError) as e:
            logger.warning(
                f"focus_on_region_based_on_resolution: Error in angstrom_to_pixels: {e}"
            )
            return
        padding = 1.1
        radius_padded = r_outer * padding
        x0, x1 = float(beam_x) - radius_padded, float(beam_x) + radius_padded
        y0, y1 = float(beam_y) - radius_padded, float(beam_y) + radius_padded
        view_rect = QtCore.QRectF(x0, y0, (x1 - x0), (y1 - y0))
        self.graphics_manager.view_box.setRange(rect=view_rect, padding=0)

    def toggle_playback(self):
        self.playback_manager.toggle_playback()

    @QtCore.pyqtSlot(PlaybackState)
    def _on_playback_state_changed(self, state: PlaybackState):
        """Updates the UI based on the new playback state."""
        if state == PlaybackState.PLAYING:
            self.ui_manager.update_playback_button("pause")
            self.ui_manager.show_status_message("Playback started.", 2000)
        elif state == PlaybackState.WAITING:
            self.ui_manager.update_playback_button("wait")
        else:  # STOPPED or PAUSED
            self.ui_manager.update_playback_button("play")
            if self.waiting_for_peaks:
                self.waiting_for_peaks = False
            if state == PlaybackState.STOPPED:
                self.ui_manager.show_status_message("Playback finished.", 3000)
            elif state == PlaybackState.PAUSED:
                self.ui_manager.show_status_message("Playback paused.", 2000)

    @QtCore.pyqtSlot(int, int)
    def _update_ui_after_file_change(
        self, latest_available_index: int, total_frames: int
    ):
        prev_latest = self.latest_available_frame_index
        self.latest_available_frame_index = latest_available_index
        was_waiting = self.playback_manager.state == PlaybackState.WAITING
        data_incomplete = latest_available_index < total_frames - 1

        # Log when all frames become available (data_incomplete flips False)
        prev_incomplete = prev_latest < total_frames - 1
        if prev_incomplete and not data_incomplete:
            logger.info(
                f"All frames now on disk: {total_frames} total. "
                f"IV is at frame {self.current_frame_index + 1} "
                f"({total_frames - 1 - self.current_frame_index} frames behind). "
                f"Anti-lag jump is now DISABLED — relying on adaptive playback to catch up."
            )

        lag_to_latest = latest_available_index - self.current_frame_index

        self.ui_manager.update_frame_elements(
            self.current_frame_index, total_frames, latest_available_index
        )
        status_msg = (
            f"Data updated. Available: {latest_available_index + 1}/{total_frames}"
        )
        current_status = self.statusBar().currentMessage()
        if not any(
            current_status.startswith(p)
            for p in ["Waiting", "Running", "Applying", "Summing"]
        ):
            self.ui_manager.show_status_message(status_msg, 4000)

        if was_waiting:
            self.ui_manager.show_status_message(
                f"Data available. Resuming playback.", 3000
            )
            self.playback_manager.play()

    def _continue_toggle_image_filter(self, checked):
        self.image_filter_manager.toggle(checked)

    def _update_sum_action_state(self):
        playback_skip = self.settings_manager.get("playback_skip")
        if playback_skip is None:
            playback_skip = 1
        can_sum = playback_skip > 1
        self.ui_manager.update_sum_action_state(can_sum, playback_skip)
        if not can_sum and self.sum_frames_enabled:
            self.sum_frames_action.blockSignals(True)
            self.sum_frames_action.setChecked(False)
            self.sum_frames_action.blockSignals(False)
            self.sum_frames_enabled = False

    def _apply_contrast(self, data_subset=None):
        if data_subset is not None:
            self._calculate_and_set_contrast(data_subset)
        elif self.auto_contrast_on_zoom:
            self._update_contrast_from_view()
        else:
            current_image = (
                self.graphics_manager.img_item.image if self.graphics_manager else None
            )
            self._calculate_and_set_contrast(current_image)

    def _calculate_and_set_contrast(self, data_for_levels):
        vmin, vmax = calculate_contrast_levels(
            data_for_levels,
            self.settings_manager.get("contrast_low_percentile"),
            self.settings_manager.get("contrast_high_percentile"),
            self.detector_mask,
        )
        self.graphics_manager.set_contrast_levels(vmin, vmax)
        hist_min, hist_max = calculate_histogram_range(vmin, vmax)
        self.graphics_manager.set_histogram_range(hist_min, hist_max)

    def _toggle_auto_contrast_action(self, checked):
        self.auto_contrast_on_zoom = checked
        self.ui_manager.show_status_message(
            f"Auto-Contrast: {'Enabled' if checked else 'Disabled'}", 2000
        )
        self._apply_contrast()

    def _handle_range_change_debounced(self):
        if not self.ui_manager.view_box or not self.graphics_manager.img_item:
            return
        if self.auto_contrast_on_zoom:
            self.zoom_contrast_timer.start()
        try:
            sx, sy = self.ui_manager.view_box.viewPixelSize()
            zoom_ratio = calculate_zoom_ratio((sx, sy))
            is_zoomed_now = zoom_ratio >= PIXEL_TEXT_ZOOM_THRESHOLD
        except Exception as e:
            logger.warning(
                f"_handle_range_change_debounced: Error calculating zoom ratio: {e}"
            )
            is_zoomed_now = False
        if self.is_zoomed_for_text != is_zoomed_now:
            self.is_zoomed_for_text = is_zoomed_now
            if not self.is_zoomed_for_text:
                self.graphics_manager.hide_all_pixel_text()
            elif self.last_mouse_pos_img:
                self.pixel_text_update_timer.start()
        if self.is_zoomed_for_text and self.last_mouse_pos_img:
            if self.ui_manager.view_box.viewRect().contains(self.last_mouse_pos_img):
                self.pixel_text_update_timer.start()
            else:
                self.graphics_manager.hide_all_pixel_text()

    def _update_contrast_from_view(self):
        if (
            not self.auto_contrast_on_zoom
            or not self.graphics_manager
            or self.graphics_manager.img_item is None
            or self.graphics_manager.img_item.image is None
        ):
            return
        full_image = self.graphics_manager.img_item.image
        view_range = self.ui_manager.view_box.viewRange()

        logger.debug(
            f"_update_contrast_from_view: full_image shape={full_image.shape}, view_range={view_range}"
        )

        try:
            subset = extract_view_subset(full_image, view_range)
            if subset is not None:
                logger.debug(
                    f"_update_contrast_from_view: subset shape={subset.shape}, subset range=[{subset.min():.2f}, {subset.max():.2f}]"
                )

                # Extract corresponding subset of detector mask if it exists
                subset_mask = None
                if (
                    self.detector_mask is not None
                    and self.detector_mask.shape == full_image.shape
                ):
                    subset_mask = extract_view_subset(self.detector_mask, view_range)

                # Calculate contrast with the subset mask
                vmin, vmax = calculate_contrast_levels(
                    subset,
                    self.settings_manager.get("contrast_low_percentile"),
                    self.settings_manager.get("contrast_high_percentile"),
                    subset_mask,
                )
                self.graphics_manager.set_contrast_levels(vmin, vmax)
                hist_min, hist_max = calculate_histogram_range(vmin, vmax)
                self.graphics_manager.set_histogram_range(hist_min, hist_max)
            else:
                logger.debug(
                    "_update_contrast_from_view: No valid subset extracted, using full image"
                )
                self._calculate_and_set_contrast(full_image)
        except Exception as e:
            logger.error(
                f"_update_contrast_from_view: Error updating contrast: {e}",
                exc_info=True,
            )
            # Fallback to full image
            self._calculate_and_set_contrast(full_image)

    def _update_image_levels_from_lut(self):
        levels = self.ui_manager.hist_lut.getLevels()
        if levels and self.graphics_manager.img_item:
            self.graphics_manager.img_item.setLevels(
                (float(levels[0]), float(levels[1])), update=True
            )

    def _update_image_lut_from_lut(self):
        cmap = self.ui_manager.hist_lut.gradient.colorMap()
        if cmap and self.graphics_manager.img_item:
            self.graphics_manager.img_item.setLookupTable(
                cmap.getLookupTable(nPts=512), update=True
            )

    def _connect_mouse_move(self):
        if self.ui_manager.view_box and self.ui_manager.view_box.scene():
            try:
                self.ui_manager.view_box.scene().sigMouseMoved.connect(self.mouse_moved)
            except Exception:
                QTimer.singleShot(100, self._connect_mouse_move)
        else:
            QTimer.singleShot(100, self._connect_mouse_move)

    def mouse_moved(self, pos):
        if not self.ui_manager.view_box or not self.graphics_manager.img_item:
            return
        try:
            img_coords = self.ui_manager.view_box.mapSceneToView(pos)
            self.last_mouse_pos_img = img_coords
            if self.ui_manager.view_box.sceneBoundingRect().contains(pos):
                self.graphics_manager.update_crosshairs(img_coords)
                self.ui_manager.update_pixel_info_panel(
                    calculate_pixel_info(
                        img_coords.x(),
                        img_coords.y(),
                        self.graphics_manager.img_item.image,
                        self.params,
                    )
                )
                if self.is_zoomed_for_text:
                    self.pixel_text_update_timer.start()
            else:
                self.graphics_manager.update_crosshairs(None)
                self.ui_manager.clear_pixel_info_panel()
                if self.is_zoomed_for_text:
                    self.graphics_manager.hide_all_pixel_text()
        except Exception:
            self._cleanup_mouse_move_error()

    def _cleanup_mouse_move_error(self):
        self.graphics_manager.update_crosshairs(None)
        self.ui_manager.clear_pixel_info_panel()
        if self.is_zoomed_for_text:
            self.graphics_manager.hide_all_pixel_text()

    def _get_pixel_info_dict(self, x, y) -> dict:
        return calculate_pixel_info(
            x, y, self.graphics_manager.img_item.image, self.params
        )

    def _trigger_pixel_text_update(self):
        if (
            self.is_zoomed_for_text
            and self.last_mouse_pos_img
            and self.graphics_manager.img_item
            and self.graphics_manager.img_item.image is not None
        ):
            self.graphics_manager.update_pixel_text(
                self.graphics_manager.img_item.image,
                self.last_mouse_pos_img,
                detector_mask=self.detector_mask,
            )
        else:
            self.graphics_manager.hide_all_pixel_text()

    def get_peak_finder_kwargs(self) -> Optional[dict]:
        """
        Gathers all necessary parameters for peak finding from the current
        settings and detector parameters. Converts resolutions to pixel radii.

        Returns:
            A dictionary of keyword arguments for find_peaks_in_annulus,
            or None if essential parameters are missing.
        """
        if not self.params:
            logger.warning(
                "get_peak_finder_kwargs: Cannot get parameters, no data loaded."
            )
            return None

        # 1. Check for essential geometry parameters AND ensure they are not None.
        required_params = {
            "wavelength": "Wavelength",
            "det_dist": "Detector Distance",
            "pixel_size": "Pixel Size",
        }

        missing_or_invalid = []
        for key, name in required_params.items():
            value = self.params.get(key)
            # Check if the key is missing OR if its value is None or not a finite number
            if value is None or not np.isfinite(value):
                missing_or_invalid.append(name)

        if missing_or_invalid:
            msg = f"Missing or invalid geometry for peak finding: {', '.join(missing_or_invalid)}."
            self.ui_manager.show_status_message(msg, 5000)
            logger.error(msg)
            return None

        # 2. Gather algorithm settings from the SettingsManager
        bin1_max_res = self.settings_manager.get("peak_finding_bin1_max_res", 5.5)
        
        kwargs = {
            "num_peaks": self.settings_manager.get("peak_finding_num_peaks"),
            "min_distance": self.settings_manager.get("peak_finding_min_distance"),
            "min_pixels_per_peak": self.settings_manager.get("peak_finding_min_pixels"),
            "threshold_abs": self.settings_manager.get("peak_finding_min_intensity"),
            "median_filter_size": self.settings_manager.get(
                "peak_finding_median_filter_size"
            ),
            "resolutions_bins_in_pixels": self.resolutions_to_pixels(
                [20, bin1_max_res, 3.93, 3.87, 3.7, 3.64, 3.0, 2.0]
            ),
            "zscore_cutoff": self.settings_manager.get("peak_finding_zscore_cutoff"),
            "bin1_min_count": self.settings_manager.get("peak_finding_bin1_min_count", 2),
        }

        # 3. Calculate pixel radii from resolution settings
        try:
            # Low resolution (e.g., 20 Å) corresponds to a SMALL pixel radius.
            # High resolution (e.g., 3 Å) corresponds to a LARGE pixel radius.
            low_res_A = self.settings_manager.get(
                "peak_finding_low_resolution_A",
                DEFAULT_SETTINGS["peak_finding_low_resolution_A"],
            )
            high_res_A = self.settings_manager.get(
                "peak_finding_high_resolution_A",
                DEFAULT_SETTINGS["peak_finding_high_resolution_A"],
            )

            inner_radius_px = angstrom_to_pixels(
                low_res_A,
                self.params["wavelength"],
                self.params["det_dist"],
                self.params["pixel_size"],
            )
            outer_radius_px = angstrom_to_pixels(
                high_res_A,
                self.params["wavelength"],
                self.params["det_dist"],
                self.params["pixel_size"],
            )
            kwargs["r1"] = min(outer_radius_px, inner_radius_px)
            kwargs["r2"] = max(outer_radius_px, inner_radius_px)
        except Exception as e:  # Catch any other unexpected math errors
            logger.error(
                f"Could not calculate pixel radii from settings: {e}", exc_info=True
            )
            self.ui_manager.show_status_message(f"Error calculating radii: {e}", 4000)
            return None

        return kwargs

    def toggle_resolution_rings(self):
        self.resolution_rings_visible = not self.resolution_rings_visible
        self.graphics_manager.update_resolution_rings(
            self.resolution_rings_visible,
            self.params,
            self.settings_manager.get("resolution_rings"),
        )
        self.ui_manager.show_status_message(
            f"Resolution rings {'shown' if self.resolution_rings_visible else 'hidden'}.",
            2000,
        )

    def _clear_calibration_visuals(self):
        self.graphics_manager.clear_calibration_visuals()
        self.ui_manager.show_status_message("Calibration visuals cleared.", 2000)

    def _update_dataset_tree_widget(self):
        self.dataset_tree_manager.update_tree()

    def _update_beam_center_marker(self):
        self.graphics_manager.update_beam_center_marker(self.params)

    def _update_mask_values(self):
        """Determines the set of pixel values to be masked."""
        # Start with an empty set
        self.mask_values = set()
        if self.params:
            # 1. Add mask value based on bit depth from the data file
            bit_depth = self.params.get("bit_depth")
            if bit_depth is not None and bit_depth > 0:
                # The primary value for masked/saturated pixels from Eiger detectors
                self.mask_values.add(2 ** int(bit_depth) - 1)
            else:
                # Fallback for older files or missing metadata
                self.mask_values.update({2**32 - 1, 2**16 - 1})

        logger.info(f"mask value: {self.mask_values}")

    def _open_settings_dialog(self):
        # Sync local dictionary with whatever is currently live in Redis
        # so multiple image viewer instances stay in parity
        self._pull_redis_processing_overrides()

        # Lazy import to avoid importing matplotlib at startup via SettingsDialog
        from qp2.image_viewer.ui.settings import SettingsDialog

        # Create the dialog instance (singleton will handle reuse)
        dialog = SettingsDialog(self.settings_manager.as_dict(), self)
        
        # Connect signal to reload from spreadsheet
        try:
            dialog.request_spreadsheet_update.disconnect()
        except (TypeError, RuntimeError):
            pass
        dialog.request_spreadsheet_update.connect(
            lambda: self._fetch_and_apply_crystal_data(self.current_master_file, force=True)
        )
        
        # Show the dialog (if it's already visible, this will just bring it to front)
        dialog.show()

    def _on_settings_changed(self, new_settings, changed_keys):
        # Only respond to what actually changed
        peak_params = {
            "peak_finding_zscore_cutoff",
            "peak_finding_low_resolution_A",
            "peak_finding_high_resolution_A",
            "peak_finding_num_peaks",
            "peak_finding_min_distance",
            "peak_finding_min_pixels",
            "peak_finding_min_intensity",
            "peak_finding_median_filter_size",
        }
        contrast_params = {"contrast_low_percentile", "contrast_high_percentile"}
        common_proc_params = {
            "processing_common_space_group",
            "processing_common_unit_cell",
            "processing_common_model_file",
            "processing_common_res_cutoff_low",
            "processing_common_res_cutoff_high",
            "processing_common_native",
            "processing_common_proc_dir_root",
            "pipelines_by_mode",
        }

        if peak_params.intersection(changed_keys):
            logger.info("Peak finding parameters changed, running peak finder.")
            self.live_peak_finding_manager.run()

        if contrast_params.intersection(changed_keys):
            self._apply_contrast()

        if common_proc_params.intersection(changed_keys):
            if not getattr(self, "_is_pulling_redis", False):
                self._update_redis_processing_overrides(new_settings)

        if "resolution_rings" in changed_keys:
            if not self.resolution_rings_visible:
                self.resolution_rings_visible = True
            self.graphics_manager.update_resolution_rings(
                self.resolution_rings_visible,
                self.params,
                self.settings_manager.get("resolution_rings"),
            )

        if "playback_interval_ms" in changed_keys:
            new_interval = self.settings_manager.get("playback_interval_ms")
            # Safety check, although the settings dialog should prevent invalid values.
            if new_interval and new_interval > 0:
                # Update the timer's interval, which will take effect on the next timeout.
                self.playback_manager.play_timer.setInterval(new_interval)

        self.statusBar().showMessage("Settings updated.", 3000)

    def _resolve_group_name(self) -> str:
        """Resolves the group_name (ESAF or 'staff') for scoping Redis keys."""
        try:
            from qp2.xio.user_group_manager import get_esaf_from_data_path
            initial_path = self._initial_file_path or ""
            esaf_info = get_esaf_from_data_path(initial_path)
            group_name = esaf_info.get("group_name") or esaf_info.get("primary_group", "staff")
            logger.info(f"Resolved group_name for Redis key scoping: '{group_name}'")
            return group_name
        except Exception as e:
            logger.warning(f"Failed to resolve group_name, falling back to 'staff': {e}")
            return "staff"

    def _pull_redis_processing_overrides(self):
        """Pulls processing settings from Redis and updates local settings_manager."""
        if not self.redis_manager:
            return
        analysis_conn = self.redis_manager.get_analysis_connection()
        if not analysis_conn:
            return

        try:
            from qp2.config.redis_keys import AnalysisRedisKeys
            import json

            def _safe_decode(val):
                return val.decode('utf-8') if isinstance(val, bytes) else str(val)

            all_updates = {}

            # Fetch common overrides (group-scoped key, fallback to global)
            key = AnalysisRedisKeys.scoped_processing_overrides(self._group_name)
            if not analysis_conn.exists(key):
                key = AnalysisRedisKeys.KEY_PROCESSING_OVERRIDES
            if analysis_conn.exists(key):
                overrides = analysis_conn.hgetall(key)
                if overrides:
                    updates = {}
                    if overrides.get("space_group"):
                        updates["processing_common_space_group"] = overrides["space_group"]
                    if overrides.get("unit_cell"):
                        updates["processing_common_unit_cell"] = overrides["unit_cell"]
                    if overrides.get("model_pdb"):
                        updates["processing_common_model_file"] = overrides["model_pdb"]
                    if overrides.get("proc_dir_root"):
                        updates["processing_common_proc_dir_root"] = overrides["proc_dir_root"]

                    if "res_cutoff_low" in overrides:
                        val = overrides["res_cutoff_low"]
                        updates["processing_common_res_cutoff_low"] = float(val) if val else None
                    if "res_cutoff_high" in overrides:
                        val = overrides["res_cutoff_high"]
                        updates["processing_common_res_cutoff_high"] = float(val) if val else None

                    if "native" in overrides:
                        updates["processing_common_native"] = (overrides["native"].lower() == 'true')

                    if updates:
                        all_updates.update(updates)

            # Fetch pipelines by mode (group-scoped key, fallback to global)
            key_by_mode = AnalysisRedisKeys.scoped_pipelines_by_mode(self._group_name)
            modes_str = analysis_conn.get(key_by_mode)
            if not modes_str:
                key_by_mode = AnalysisRedisKeys.KEY_PROCESSING_OVERRIDES_BY_MODE
                modes_str = analysis_conn.get(key_by_mode)
            if modes_str:
                try:
                    modes_str = _safe_decode(modes_str)
                    pipelines = json.loads(modes_str)
                    all_updates["pipelines_by_mode"] = pipelines
                except json.JSONDecodeError:
                    logger.warning("Failed to decode pipelines_by_mode from Redis")
                    
            if all_updates:
                try:
                    self._is_pulling_redis = True
                    self.settings_manager.update_from_dict(all_updates)
                    logger.debug(f"Pulled processing overrides from Redis: {list(all_updates.keys())}")
                finally:
                    self._is_pulling_redis = False
                    
        except Exception as e:
            logger.error(f"Failed to pull redis processing overrides: {e}")

    def _update_redis_processing_overrides(self, new_settings):
        """Pushes user-defined processing settings overrides to Redis (group-scoped)."""
        if not self.redis_manager:
            return
        analysis_conn = self.redis_manager.get_analysis_connection()
        if not analysis_conn:
            return

        try:
            from qp2.config.redis_keys import AnalysisRedisKeys
            key = AnalysisRedisKeys.scoped_processing_overrides(self._group_name)

            overrides = {
                "space_group": new_settings.get("processing_common_space_group", ""),
                "unit_cell": new_settings.get("processing_common_unit_cell", ""),
                "model_pdb": new_settings.get("processing_common_model_file", ""),
                "res_cutoff_low": str(new_settings.get("processing_common_res_cutoff_low") or ""),
                "res_cutoff_high": str(new_settings.get("processing_common_res_cutoff_high") or ""),
                "native": str(new_settings.get("processing_common_native", True)),
                "proc_dir_root": new_settings.get("processing_common_proc_dir_root", ""),
            }

            for field, value in overrides.items():
                if value:
                    analysis_conn.hset(key, field, value)
                else:
                    analysis_conn.hdel(key, field)

            analysis_conn.expire(key, 86400)

            logger.debug(f"Pushed processing overrides to Redis key '{key}': {overrides}")

            # Push pipelines_by_mode (group-scoped)
            pipelines_by_mode = new_settings.get("pipelines_by_mode")
            if pipelines_by_mode:
                try:
                    import json
                    key_by_mode = AnalysisRedisKeys.scoped_pipelines_by_mode(self._group_name)
                    pipelines_json = json.dumps(pipelines_by_mode)
                    analysis_conn.set(key_by_mode, pipelines_json)
                    analysis_conn.expire(key_by_mode, 86400)
                    logger.debug(f"Pushed pipelines_by_mode to Redis key '{key_by_mode}'")
                except Exception as e:
                    logger.error(f"Failed to push pipelines_by_mode to Redis: {e}")

        except Exception as e:
            logger.error(f"Failed to update redis processing overrides: {e}")

    def closeEvent(self, event):
        self.playback_manager.play_timer.stop()
        self.zoom_contrast_timer.stop()
        self.pixel_text_update_timer.stop()
        
        # Shutdown console if it supports it (AdvancedConsoleWidget)
        if self._console_widget and hasattr(self._console_widget, 'shutdown'):
            self._console_widget.shutdown()
            
        self.analysis_plugin_manager.clear_active_plugin()
        if self.redis_manager.is_monitoring_active:
            self.redis_manager.stop_monitoring()
        self.file_io_manager.close()
        self.summation_manager.stop()
        if self.image_filter_manager:
            self.image_filter_manager.stop()
        if self.reader:
            self.reader.close()
        self.dataset_manager.clear()  # Close all managed readers
        event.accept()

    def update_status_bar_frame_info(self, frame_index):
        current_status = self.statusBar().currentMessage()
        if not any(
            current_status.startswith(p)
            for p in ["Waiting", "Running", "Applying", "Summing"]
        ):
            self.ui_manager.show_status_message(
                f"Displayed frame {frame_index + 1}", 2000
            )

    @pyqtSlot()
    def _show_job_status_dialog_for_current(self):
        """Shows the job status dialog for the currently loaded dataset."""
        if not self.current_master_file:
            self.ui_manager.show_warning_message("Job Status", "No dataset loaded.")
            return
        
        # Reuse the logic in the context manager
        self.dataset_context_manager._show_job_status_dialog([self.current_master_file])

    @pyqtSlot()
    def _show_hdf5_metadata(self):
        # MODIFICATION: Responsibility moved to UIManager.
        # The main window's role is to simply trigger the action.
        self.ui_manager.show_hdf5_metadata()

    @pyqtSlot()
    def _show_about_dialog(self):
        # MODIFICATION: Responsibility moved to UIManager.
        self.ui_manager.show_about_dialog()

    @pyqtSlot()
    def clear_all_visuals(self):
        self.graphics_manager.clear_all_visuals()




    def _launch_dataset_processor_dialog(self, dataset_paths: list):
        from qp2.data_proc.client.dataset_processor_dialog import DatasetProcessorDialog

        dialog = DatasetProcessorDialog(dataset_paths, parent=self)
        dialog.exec_()

    def _handle_processing_progress(self, master_file_basename: str, message: str):
        self.ui_manager.show_status_message(
            f"Processing {master_file_basename}: {message}", 5000
        )

    def _handle_processing_finished(
        self, master_file_basename: str, status: str, result_data: dict
    ):
        message = result_data.get("message", "Processing finished.")
        final_message = f"Processing for {master_file_basename} {status}. {message}"
        if result_data.get("redis_key"):
            final_message += f" (Redis: {result_data['redis_key']})"
        self.ui_manager.show_status_message(final_message, 10000)

    def _handle_processing_error(self, master_file_basename: str, error_msg: str):
        self.ui_manager.show_critical_message(
            f"Processing Error for {master_file_basename}", error_msg
        )

    def _query_processing_results(self, dataset_paths: list):
        if not self.redis_output_server:
            self.ui_manager.show_warning_message(
                "Redis Error", "Analysis Redis connection not available."
            )
            return
        results_text = []
        for path in dataset_paths:
            basename = os.path.basename(path)
            key = f"analysis:out:data:gmcaproc:{path}"
            try:
                raw_data = self.redis_output_server.lrange(key, 0, -1)
                current_text = f"--- Results for: {basename} ---\nRedis Key: {key}\n"
                if raw_data:
                    for item in raw_data:
                        data = json.loads(item)
                        current_text += f"  Status: {data.get('status', 'N/A')}, Message: {data.get('message', 'N/A')}\n"
                else:
                    current_text += "  No data found in Redis for this key.\n"
                results_text.append(current_text)
            except Exception as e:
                results_text.append(
                    f"--- Error querying for {basename} ---\n  Error: {e}\n"
                )

        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle("Processing Results")
        dialog.setMinimumSize(700, 500)
        layout = QtWidgets.QVBoxLayout(dialog)
        text_edit = QtWidgets.QTextEdit("\n".join(results_text))
        text_edit.setReadOnly(True)
        text_edit.setFont(QtGui.QFont("Monospace", 9))
        layout.addWidget(text_edit)
        button_box = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok
        )
        button_box.accepted.connect(dialog.accept)
        layout.addWidget(button_box)
        dialog.exec_()

    @pyqtSlot(QPoint)
    def _show_dataset_context_menu(self, pos: QPoint):
        """Delegates context menu handling to the dedicated manager."""
        self.dataset_context_manager.show_context_menu(pos)

    def _apply_dataset_history_filter(
        self, filter_mode="show_containing_text", filter_text=None
    ):
        self.dataset_tree_manager.apply_filter(filter_mode, filter_text)

    def _clear_dataset_history_filter(self):
        self.dataset_tree_manager.clear_filter()

    @QtCore.pyqtSlot(str, int, int, list, list)
    def _on_run_started(
        self,
        run_prefix: str,
        accumulated_frames: int,
        total_frames: int,
        masterfiles: list,
        metadata_list: list,
    ):
        self.ui_manager.show_status_message(
            f"Run '{run_prefix}' started. Series: {metadata_list[0].get('prefix', 'N/A')}",
            7000,
        )

    @QtCore.pyqtSlot(str, int, int, list, list)
    def _on_run_completed(
        self,
        run_prefix: str,
        accumulated_frames: int,
        total_frames: int,
        masterfiles: list,
        metadata_list: list,
    ):
        self.ui_manager.show_status_message(
            f"Run '{run_prefix}' completed. Total series: {len(masterfiles)}.", 10000
        )

    def get_mask(self):
        if self.detector_mask is not None:
            self.mask_bad = self.detector_mask.copy()
            self.mask_good = ~self.detector_mask

    def get_analysis_image(self):
        return self._original_image

    def get_params(self):
        return self.params

    @pyqtSlot()
    def run_radial_sum_analysis(self):
        """Triggers the radial sum analysis on the current image."""
        # --- MODIFICATION: This is now a launcher method ---
        if self.radial_sum_manager is None:
            logger.info("First use: Initializing RadialSumManager.")
            from qp2.image_viewer.workers.radial_sum import RadialSumManager

            self.radial_sum_manager = RadialSumManager(self, self.ui_manager)

        if self.get_analysis_image() is None:
            self.ui_manager.show_status_message(
                "No image loaded for radial sum analysis.", 3000
            )
            return

        # The manager will use the state access methods (get_analysis_image, get_params)
        self.radial_sum_manager.calculate_radial_sum()



    def _toggle_image_stats_overlay(self, checked):
        self.image_stats_overlay_enabled = checked
        if checked:
            self._update_image_stats_overlay()
            self.ui_manager.show_status_message("Image Stats Overlay Enabled", 2000)
        else:
            self.graphics_manager.clear_image_stats_overlay()
            self.ui_manager.show_status_message("Image Stats Overlay Disabled", 2000)

    def _update_image_stats_overlay(self):
        # Get the current displayed image
        image = (
            self._filtered_image
            if self.image_filter_enabled and self._filtered_image is not None
            else self._original_image
        )
        if image is None:
            return

        # Calculate statistics using utility function
        stats = calculate_image_statistics(image, self.detector_mask)
        stats_text = format_statistics_text(stats)
        self.graphics_manager.display_image_stats_overlay(
            stats_text, stats["max_x"], stats["max_y"]
        )





    def _on_frame_changed(self, frame_index):
        self.current_frame_index = frame_index
        if self.summation_manager.is_active:
            self.summation_manager.trigger_summation()
            # Update UI elements even during summation
            if self.reader:
                self.ui_manager.update_frame_elements(
                    self.current_frame_index,
                    self.reader.total_frames,
                    self.latest_available_frame_index,
                )
        else:
            is_playing = self.playback_manager.state == PlaybackState.PLAYING
            self.update_frame_display(frame_index, is_playback=is_playing)



    @pyqtSlot(int)
    def _on_analysis_plugin_selected(self, index: int):
        selected_name = self.ui_manager.analysis_selector_combo.itemText(index)
        self.analysis_plugin_manager.select_plugin(selected_name)







    def _fetch_and_apply_crystal_data(self, master_file: str, force: bool = False):
        """
        Fetches crystallographic data from Redis and applies it to the
        Common Processing Parameters in SettingsManager.
        
        Args:
            master_file: Path to master file.
            force: If True, ignore the 'processing_common_mode' setting and force update.
        """
        if not self.redis_manager or not master_file:
            return

        mode = self.settings_manager.get("processing_common_mode", "manual")
        if not force and mode != "spreadsheet":
            logger.debug("Crystal data fetch skipped (Manual mode active).")
            return

        try:
            redis_conn = self.redis_manager.get_analysis_connection()
            if not redis_conn:
                return

            redis_key = f"dataset:info:{master_file}"
            crystal_data_bytes = redis_conn.hgetall(redis_key)
            
            # Prepare data dict, default to empty strings if nothing found (to clear fields)
            crystal_data = {}
            if crystal_data_bytes:
                crystal_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in crystal_data_bytes.items()}
            
            # Map to Common Settings - overwriting everything
            update_dict = {}
            
            # Helper to safely get value
            def get_val(k): return crystal_data.get(k, "")
            
            update_dict["processing_common_space_group"] = get_val("space_group")
            update_dict["processing_common_model_file"] = get_val("model_pdb")
            update_dict["processing_common_reference_reflection_file"] = get_val("reference_hkl")

            # Validate Unit Cell
            uc_str = get_val("unit_cell")
            final_uc = ""
            if uc_str:
                try:
                    parts = uc_str.replace(",", " ").split()
                    if len(parts) == 6:
                        [float(x) for x in parts] # Check if numbers
                        final_uc = " ".join(parts)
                    else:
                        logger.warning(f"Invalid unit cell from spreadsheet (expected 6 numbers): {uc_str}")
                except ValueError:
                    logger.warning(f"Invalid unit cell values from spreadsheet: {uc_str}")
            
            update_dict["processing_common_unit_cell"] = final_uc

            # Log and update
            # We log at debug to avoid spam if called automatically
            logger.info(f"Updating common settings from spreadsheet: {update_dict}")
            self.settings_manager.update_from_dict(update_dict)
            
            # Only show status if forced (manual click) or if data actually changed? 
            # Showing status on auto-load is fine.
            self.ui_manager.show_status_message("Common parameters updated from spreadsheet.", 3000)

        except Exception as e:
            logger.error(f"Failed to fetch or apply crystal data: {e}", exc_info=True)

    @pyqtSlot(object, dict, str)
    def _on_data_loaded(self, reader, params, file_path):
        """Handles a successful file load."""
        self.reader = reader
        self.params = params
        self.current_master_file = file_path

        # Auto-fetch crystal data from spreadsheet info in Redis if available
        self._fetch_and_apply_crystal_data(file_path)

        self.dataset_manager.add_dataset(reader, params)

        self.current_frame_index = 0
        
        # Use the reader's tracked availability state.
        # Fallback to -1 if the reader doesn't have the attribute yet (e.g., older version or mock).
        self.latest_available_frame_index = getattr(self.reader, "last_known_available_index", -1)

        if not self.contrast_locked:
            self.dataset_contrast_set = False

        self.image_filter_action.setChecked(False)
        self.sum_frames_action.setChecked(False)
        if self.image_filter_manager:
            self.image_filter_manager.stop()
        self.summation_manager.stop()
        self.graphics_manager.clear_all_visuals()
        
        # Restore resolution rings if they were visible
        if self.resolution_rings_visible:
            self.graphics_manager.update_resolution_rings(
                True,
                self.params,
                self.settings_manager.get("resolution_rings"),
            )
            
        self.graphics_manager.update_beam_center_marker(self.params)
        self._update_mask_values()

        # Update Console
        vars_to_push = {
            'reader': self.reader,
            'params': self.params,
            'master_file': self.current_master_file,
            'beam_center_x': self.params.get('beam_x'),
            'beam_center_y': self.params.get('beam_y'),
            'wavelength': self.params.get('wavelength'),
            'pixel_size': self.params.get('pixel_size'),
            'det_dist_m': self.params.get('det_dist_m'),
            'energy_eV': self.params.get('energy_eV'),
        }
        
        if self._console_widget:
            self._console_widget.push_vars(vars_to_push)

        if self.analysis_plugin_manager:
            self.analysis_plugin_manager.update_source(
                self.reader, self.current_master_file
            )

        self.reader.frames_updated.connect(self._update_ui_after_file_change)

        if self.reader.total_frames == 0:
            self.ui_manager.show_status_message(
                f"Waiting for data in {os.path.basename(file_path)}...", 0
            )
            self._update_ui_after_file_change(-1, 0)
            self.graphics_manager.clear_image()
        else:
            self._update_ui_after_file_change(
                self.latest_available_frame_index, self.reader.total_frames
            )
            self.update_frame_display(0)

        self.update_window_title()
        if self.is_live_mode and self.reader.total_frames > 0:
            if self.playback_manager.state != PlaybackState.PLAYING:
                QTimer.singleShot(50, self.playback_manager.play)

        self._fetch_and_apply_crystal_data(self.current_master_file)

        # Update BeamCenterDialog if it's open, so it points to the new dataset
        if "beam_center" in self.strategy_dialogs:
            dialog = self.strategy_dialogs["beam_center"]
            if dialog and dialog.isVisible():
                dialog.update_dataset(self._original_image, self.params)

    @pyqtSlot(str, bool, int, str, bool)
    def load_files_from_directory(
        self,
        directory_paths,
        recursive=False,
        min_images=0,
        max_images=None,
        path_contains="",
        path_not_contains="",
        keep_existing=True,
    ):
        """
        Starts the DirectoryLoaderWorker to scan directories for master files.
        """
        if self.directory_loader_worker:
            self.ui_manager.show_status_message(
                "Directory scan already in progress.", 3000
            )
            return

        if isinstance(directory_paths, str):
            directory_paths = [directory_paths]

        self.ui_manager.show_status_message(
            f"Scanning {len(directory_paths)} directories...", 0
        )
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)

        if not keep_existing:
            self.dataset_manager.clear()
            self._reset_view_state()

        self.directory_loader_worker = DirectoryLoaderWorker(
            directory_paths,
            recursive,
            min_images,
            max_images,
            path_contains,
            path_not_contains,
        )
        self.directory_loader_worker.signals.found_batch.connect(
            self._on_directory_files_found_batch
        )
        self.directory_loader_worker.signals.finished.connect(
            self._on_directory_load_finished
        )
        self.directory_loader_worker.signals.error.connect(
            self._on_directory_load_error
        )

        self.threadpool.start(self.directory_loader_worker)

    @pyqtSlot(list)
    def _on_directory_files_found_batch(self, batch_data):
        """
        Called when the loader finds a batch of valid files.
        batch_data: List of (HDF5Reader, metadata_dict) tuples.
        The readers are already open and initialized from the worker thread.
        """
        if batch_data:
            self.dataset_manager.add_datasets(batch_data)

    @pyqtSlot(str)
    def _on_directory_file_found(self, file_path):
        # Kept for backward compatibility if needed, but unused now
        pass

    @pyqtSlot()
    def _on_directory_load_finished(self):
        """Called when the directory scan is complete."""
        self.directory_loader_worker = None
        QtWidgets.QApplication.restoreOverrideCursor()
        self.ui_manager.show_status_message("Directory scan complete.", 5000)

        # If no data was previously loaded, load the first one found
        if self.reader is None:
            # Find the most recent one added to the manager
            all_data = self.dataset_manager.get_all_data()
            # (Simple heuristic: get the first one. A better one would be latest creation time)
            found_any = False
            for sample in all_data.values():
                for run in sample['runs'].values():
                    if run['datasets']:
                        first_reader = run['datasets'][0]
                        self.file_io_manager.load_file(first_reader.master_file_path)
                        found_any = True
                        break
                if found_any: break

    @pyqtSlot(str)
    def _on_directory_load_error(self, err_msg):
        """Called if the directory loader encounters a fatal error."""
        self.directory_loader_worker = None
        QtWidgets.QApplication.restoreOverrideCursor()
        self.ui_manager.show_critical_message("Directory Load Error", err_msg)

    @pyqtSlot()
    def _on_load_failed(self):
        """Resets the application to a 'no data' state."""
        self._reset_view_state(is_error=True)

    def _reset_view_state(self, is_error=False, error_text="Load Error"):
        """Resets the view and internal state to a 'no data' state."""
        self.reader = None
        self.params = {}
        self.current_master_file = None
        self.current_frame_index = 0
        self.latest_available_frame_index = -1
        self.dataset_contrast_set = False
        self._original_image = self._filtered_image = None
        self.image_filter_enabled = self.sum_frames_enabled = False

        self.image_filter_action.setChecked(False)
        self.sum_frames_action.setChecked(False)

        if is_error:
            self.ui_manager.reset_ui_on_load_error(error_text)
        else:
            self.ui_manager.reset_ui_to_empty()

        self.graphics_manager.clear_image()
        self.graphics_manager.update_beam_center_marker({})
        self.graphics_manager.clear_all_visuals()

        if self.playback_manager.state != PlaybackState.STOPPED:
            self.playback_manager.stop()
        self.waiting_for_peaks = self.waiting_for_sum_worker = False

    @pyqtSlot(list)
    def _on_datasets_updated(self, datasets):
        pass

    def _on_summation_complete(self, summed_image, start_frame, end_frame):
        """Handles the display of a successfully summed image."""

        QtWidgets.QApplication.restoreOverrideCursor()
        self._original_image = summed_image

        self.graphics_manager.show_sum_label(start_frame, end_frame)
        self.ui_manager.show_status_message(
            f"Displayed sum of {end_frame - start_frame + 1} frames.", 3000
        )

        if self.image_filter_manager and self.image_filter_manager.is_active:
            self.image_filter_manager.apply_filter(self._original_image)
            return

        self._display_final_image(self._original_image)

    @pyqtSlot(str)
    def _on_summation_error(self, error_msg):
        """Handles a failure in the summation worker."""
        QtWidgets.QApplication.restoreOverrideCursor()
        if self._original_image_before_sum is not None:
            self._original_image = self._original_image_before_sum
            self._original_image_before_sum = None
            self.graphics_manager.display_image(self._original_image)
            self._apply_contrast()
        else:
            self.update_frame_display(self.current_frame_index)

        self.ui_manager.clear_status_message_if("Summing")

    @pyqtSlot()
    def _on_summation_stopped(self):
        """Called when the summation dialog is closed to refresh the view."""
        QtWidgets.QApplication.restoreOverrideCursor()
        self.summation_active = False  # Update state
        self.update_frame_display(self.current_frame_index)
        QTimer.singleShot(10, self._apply_contrast)

    @pyqtSlot(np.ndarray, str)
    def _on_filter_applied(self, filtered_image: np.ndarray, filter_type: str):
        QtWidgets.QApplication.restoreOverrideCursor()
        self._filtered_image = filtered_image
        self.graphics_manager.show_filter_label(filter_type)
        self._display_final_image(self._filtered_image)

    @pyqtSlot(str)
    def _on_filter_error(self, error_msg: str):
        QtWidgets.QApplication.restoreOverrideCursor()
        self.ui_manager.show_warning_message("Image Filter Error", error_msg)
        self.graphics_manager.hide_filter_label()
        if self._original_image is not None:
            self._display_final_image(self._original_image)

    @pyqtSlot()
    def _on_filter_stopped(self):
        QtWidgets.QApplication.restoreOverrideCursor()
        self.graphics_manager.hide_filter_label()
        if self._original_image is not None:
            self._display_final_image(self._original_image)
            QTimer.singleShot(10, self._apply_contrast)

    def _display_final_image(self, image_data: np.ndarray):
        """Centralized method to display an image and run all post-display actions."""

        mask = self.display_mask
        if mask is not None and mask.shape == image_data.shape:
            disp = image_data.copy(order="K")
            np.putmask(disp, mask, 0)
        else:
            disp = image_data
        self.graphics_manager.display_image(disp)

        if not self.contrast_locked:
            if self.auto_contrast_on_zoom:
                self._update_contrast_from_view()
            elif not self.dataset_contrast_set:
                self._apply_contrast(image_data)
                self.dataset_contrast_set = True

        if self.reader:
            self.ui_manager.update_frame_elements(
                self.current_frame_index,
                self.reader.total_frames,
                self.latest_available_frame_index,
            )

        if self.is_zoomed_for_text:
            self._trigger_pixel_text_update()

        if self.auto_peak_finding_enabled:
            self.live_peak_finding_manager.run()

        if self.image_stats_overlay_enabled:
            self._update_image_stats_overlay()

    @pyqtSlot(HDF5Reader, int, dict)
    def display_image_with_overlays(
        self, reader: HDF5Reader, frame_index: int, overlays: dict
    ):
        """
        A dedicated method to load a specific image and overlay various graphics,
        such as spots and reflections.
        """
        self.file_io_manager.load_file(reader.master_file_path)

        def display_action():
            """This function will be called after a short delay."""
            self.playback_manager.go_to_frame(frame_index)

            self.graphics_manager.clear_spots()
            self.graphics_manager.clear_indexed_reflections()

            if "spots" in overlays and overlays["spots"] is not None:
                self.graphics_manager.display_spots(overlays["spots"])

            if "reflections" in overlays and overlays["reflections"]:
                self.graphics_manager.display_indexed_reflections(
                    overlays["reflections"]
                )

            indexing_info = overlays.get("indexing_info")
            if indexing_info:
                cell_a, cell_b, cell_c, cell_alpha, cell_beta, cell_gamma = (
                    indexing_info["unit_cell"]
                )

                info_html = f"""
                <div style='color: #FFFFD0; font-family: Consolas, "Courier New", monospace; font-size: 9pt;'>
                <b>Indexer:</b> {indexing_info['indexer']}<br>
                <b>Lattice:</b> {indexing_info['lattice_type']} <b>Centering:</b> {indexing_info['centering']}<br>
                <b>Cell:</b>
                {cell_a:.2f}, {cell_b:.2f}, {cell_c:.2f}<br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                {cell_alpha:.1f}, {cell_beta:.1f}, {cell_gamma:.1f}
                </div>
                """
                self.graphics_manager.display_plugin_info_text(info_html)

            self.raise_()
            self.activateWindow()

        QTimer.singleShot(100, display_action)

    @pyqtSlot()
    def _launch_batch_processor(self):
        """Launches the integrated Data Processing launcher."""
        dataset_paths = []
        if self.current_master_file:
            dataset_paths = [self.current_master_file]
        
        self._launch_dataset_processor_dialog(dataset_paths)

    def _toggle_redis_follow(self):
        """Toggle following the detector Redis stream and update action label."""
        if not self.redis_manager:
            return

        try:
            if self.is_live_mode:
                self.is_live_mode = False
                if self.redis_manager.is_monitoring_active:
                    self.redis_manager.stop_monitoring()
                if hasattr(self, "toggle_redis_follow_action"):
                    self.toggle_redis_follow_action.setText("Follow detector redis")
                self.statusBar().showMessage(
                    "Detector Redis unfollowed. Live mode off.", 5000
                )
            else:
                self.is_live_mode = True
                self.redis_manager.start_monitoring()
                
                # Silently fetch recent datasets to bridge the gap in the UI tree
                self.load_recent_datasets_in_background()
                
                if hasattr(self, "toggle_redis_follow_action"):
                    self.toggle_redis_follow_action.setText("Unfollow detector redis")
                self.statusBar().showMessage(
                    "Following detector Redis. Live mode on.", 5000
                )
                if self.reader and self.reader.total_frames > 0:
                    QtCore.QTimer.singleShot(100, self.toggle_playback)
        except Exception as e:
            if hasattr(self, "toggle_redis_follow_action"):
                self.toggle_redis_follow_action.setText(
                    "Unfollow detector redis"
                    if self.is_live_mode
                    else "Follow detector redis"
                )
            self.statusBar().showMessage(f"Redis toggle error: {e}", 7000)

    def load_recent_datasets_in_background(self):
        """
        Background load of recent datasets to fill out the UI history tree 
        silently, bridging any gap created by being 'unfollowed'.
        """
        if not self.redis_manager:
            return
            
        from qp2.image_viewer.workers.recent_datasets_loader import RecentDatasetsLoaderWorker
        
        # We fetch up to 40 recent missed datasets quietly
        worker = RecentDatasetsLoaderWorker(self.redis_manager, count=40)
        
        # The worker emits lists of (reader, metadata) which we feed to dataset_manager 
        worker.signals.found_batch.connect(self.dataset_manager.add_datasets)
        
        # Cleanup when finished
        worker.signals.finished.connect(lambda: self._on_worker_finished(worker))
        worker.signals.error.connect(lambda err: logger.warning(f"Backfill worker error: {err}"))
        
        self.active_workers.add(worker)
        self.thread_pool.start(worker)

    @pyqtSlot()
    def _launch_sview(self):
        """Launches the sview application in a detached process."""
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        QtWidgets.QApplication.processEvents()

        from qp2.image_viewer.utils.run_job import run_command

        run_command(
            cmd="sview",
            cwd=os.path.expanduser("~"),
            method="shell",
            job_name="sview_launcher",
            background=True,
        )

        QTimer.singleShot(2000, QtWidgets.QApplication.restoreOverrideCursor)

    def _toggle_contrast_lock(self, checked: bool):
        """Slot to handle the locking/unlocking of the histogram."""
        self.contrast_locked = checked
        status = "LOCKED" if checked else "UNLOCKED"
        self.ui_manager.show_status_message(
            f"Contrast/Histogram is now {status}.", 3000
        )

    # deferred loading
    @pyqtSlot()
    def _launch_image_filter_dialog(self):
        """Creates the ImageFilterManager on first use and shows its dialog."""
        if self.image_filter_manager is None:
            logger.info("First use: Initializing ImageFilterManager.")
            from qp2.image_viewer.actions.image_filter_manager import ImageFilterManager

            self.image_filter_manager = ImageFilterManager(self)

            # Connect signals that were previously in __init__
            self.image_filter_manager.filter_applied.connect(self._on_filter_applied)
            self.image_filter_manager.filter_error.connect(self._on_filter_error)
            self.image_filter_manager.filter_stopped.connect(self._on_filter_stopped)

        self.image_filter_manager.open_settings_dialog()

    @pyqtSlot()
    def _launch_beam_center_calculation(self):
        """Launches the dialog to calculate and optionally update beam center."""
        if self._original_image is None or not self.params:
            self.ui_manager.show_warning_message("Error", "No image loaded.")
            return

        dialog = BeamCenterDialog(self, self._original_image, self.params)
        dialog.beam_center_updated.connect(self._on_beam_center_updated)
        dialog.show()
        self.strategy_dialogs['beam_center'] = dialog

    @pyqtSlot(float, float)
    def _on_beam_center_updated(self, new_x, new_y):
        """Called when the beam center has been updated in the master file."""
        # Update local params to reflect change immediately?
        self.params['beam_x'] = new_x
        self.params['beam_y'] = new_y
        self._update_beam_center_marker()
        
        # Reloading might be cleaner to ensure everything (reader, etc) is in sync
        # But for now, updating params helps visual feedback.
        # User is instructed to reload by the dialog.

    @pyqtSlot()
    def _launch_calibration(self):
        """Creates the CalibrationManager on first use and runs calibration."""
        if self.calibration_manager is None:
            logger.info("First use: Initializing CalibrationManager.")
            from qp2.image_viewer.beamcenter.calibration_manager import (
                CalibrationManager,
            )

            self.calibration_manager = CalibrationManager(self)

        self.calibration_manager.run()

    def resolutions_to_pixels(self, res_list):
        wl = self.params["wavelength"]
        dist = self.params["det_dist"]
        px = self.params["pixel_size"]
        return [int(angstrom_to_pixels(d, wl, dist, px)) for d in res_list]

    def _run_strategy_for_current_view(self, program: str):
        self.strategy_manager.run_strategy_for_current_view(program)

    def _run_strategy_both(self):
        self.strategy_manager.run_strategy_both()

    def _run_strategy(self, programs: list or str, mapping: dict):
        self.strategy_manager.run_strategy(programs, mapping)



    @property
    def bad_pixel_manager(self):
        """Lazy-instantiate and return BadPixelManager."""
        if self._bad_pixel_manager is None:
            try:
                module = importlib.import_module(
                    "qp2.image_viewer.eiger_mask.bad_pixel_manager"
                )
                ManagerClass = getattr(module, "BadPixelManager")
                self._bad_pixel_manager = ManagerClass(self)
                logger.info("BadPixelManager initialized on first use.")
            except Exception as e:
                logger.error(
                    f"Failed to initialize BadPixelManager: {e}", exc_info=True
                )
                raise
        return self._bad_pixel_manager

    # --- NEW METHODS ---
    @pyqtSlot()
    def _launch_bad_pixel_detection(self):
        """Entry point to start the bad pixel detection process."""
        self.bad_pixel_manager.run_detection()

    @pyqtSlot(int, int)
    def zoom_to_pixel(self, row, col, padding=3):
        """Sets the viewbox range to focus on a specific pixel."""
        x0, x1 = float(col - padding), float(col + padding)
        y0, y1 = float(row - padding), float(row + padding)
        view_rect = QtCore.QRectF(x0, y0, (x1 - x0), (y1 - y0))
        self.ui_manager.view_box.setRange(rect=view_rect, padding=0)

    @pyqtSlot(list)
    def update_detector_mask_with_new_pixels(self, coords_to_add):
        """Updates the in-memory detector mask with new bad pixels."""
        if self.detector_mask is None:
            self.update_detector_mask()  # Ensure mask is computed if it doesn't exist

        if self.detector_mask is None:
            self.ui_manager.show_critical_message(
                "Mask Error", "Could not compute an initial detector mask."
            )
            return

        coords_array = np.array(coords_to_add)
        rows, cols = coords_array[:, 0], coords_array[:, 1]

        # Update the boolean mask (True means masked)
        self.detector_mask[rows, cols] = True

        self.ui_manager.show_status_message(
            f"Added {len(coords_to_add)} pixels to the detector mask.", 4000
        )

        # Refresh visuals if the mask overlay is visible
        if self.mask_overlay_visible:
            self.graphics_manager.show_mask_overlay(self.detector_mask)

        # We need to re-display the image to apply the new mask for analysis/display
        self._display_final_image(self.get_analysis_image())

    def set_manual_calibration_mode(self, enabled: bool, dialog=None):
        """Enables or disables the mode for selecting points for manual calibration."""
        self.is_manual_calibration_mode = enabled
        self.manual_calibration_dialog = dialog
        if enabled:
            # Clear any previous measurement visuals to avoid confusion
            self.graphics_manager.clear_measure_visuals()
            self.ui_manager.show_status_message(
                "Manual Calibration: Click on the image to select points on a ring.", 0
            )
        else:
            # Clear the temporary points when the mode is disabled
            self.graphics_manager.clear_measure_visuals()
            self.ui_manager.clear_status_message_if("Manual Calibration:")

    @pyqtSlot()
    def _launch_data_viewer(self):
        """Launches the Data Viewer application in a detached process."""
        with BusyCursor():
            from qp2.utils.project_root import find_qp2_parent

            project_root = find_qp2_parent(__file__)
            script_path = os.path.join(project_root, "qp2", "bin", "dv")
            from qp2.image_viewer.utils.run_job import run_command

            run_command(
                cmd=script_path,
                cwd=os.path.expanduser("~"),
                method="shell",
                job_name="qp2_data_viewer_launcher",
                background=True,
            )
