import os
import numpy as np
import logging
from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import pyqtSlot, QObject

from qp2.xio.user_group_manager import get_esaf_from_data_path
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.image_viewer.strategy.run_strategy import StrategyWorker
from qp2.image_viewer.plugins.crystfel.utils import calculate_robust_threshold_mad
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

class StrategyManager(QObject):
    """
    Manages the running of strategy calculations (XDS/MOSFLM) and visualization of results.
    """
    
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        # Access commonly used components from main_window
        self.ui_manager = main_window.ui_manager
        self.graphics_manager = main_window.graphics_manager
        self.threadpool = main_window.threadpool
        self.settings_manager = main_window.settings_manager
        # Mapping to keep track of open dialogs
        self.strategy_dialogs = {}

    def run_strategy_for_current_view(self, program: str):
        """Triggers strategy for the currently displayed master file and frame."""
        if not self.main_window.current_master_file or self.main_window.reader is None:
            self.ui_manager.show_warning_message(
                "No Data", "Please load a dataset before running a strategy."
            )
            return

        mapping = {self.main_window.current_master_file: [self.main_window.current_frame_index + 1]}
        self.run_strategy(program, mapping)

    def run_strategy_both(self):
        """Triggers both XDS and MOSFLM strategy in parallel."""
        if not self.main_window.current_master_file or self.main_window.reader is None:
            self.ui_manager.show_warning_message(
                "No Data", "Please load a dataset before running a strategy."
            )
            return

        mapping = {self.main_window.current_master_file: [self.main_window.current_frame_index + 1]}
        self.run_strategy(["mosflm", "xds"], mapping)

    def run_strategy(self, programs: list or str, mapping: dict, override_params: dict = None):
        """
        Creates and starts the StrategyWorker.
        
        Args:
            programs: Single string or list of program names (e.g., "crystfel", "xds").
            mapping: Metadata mapping for the dataset.
            override_params: Optional dict of parameters to use instead of querying SettingsManager.
                             Useful for testing new settings without saving them first.
        """
        program_list = [programs] if isinstance(programs, str) else programs
        program_names = " & ".join([p.upper() for p in program_list])

        self.ui_manager.show_status_message(f"Starting {program_names} strategy...", 0)
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        QtWidgets.QApplication.processEvents()

        # Safely get the first master file path
        first_master_file = next(iter(mapping), None)
        esaf_info = (
            get_esaf_from_data_path(first_master_file) if first_master_file else {}
        )

        pipeline_params = {
            "beamline": get_beamline_from_hostname(),
            "username": self.settings_manager.get("username", os.getenv("USER")),
            "primary_group": esaf_info.get("primary_group"),
            "esaf_id": esaf_info.get("esaf_id"),
            "pi_badge": esaf_info.get("pi_badge"),
        }

        if "crystfel" in programs or programs == "crystfel":
            pipeline_params["crystfel_peaks_method"] = (
                self.settings_manager.get("crystfel_peaks_method")
            )
            pipeline_params["crystfel_min_snr"] = self.settings_manager.get(
                "crystfel_min_snr"
            )
            pipeline_params["crystfel_min_peaks"] = self.settings_manager.get(
                "crystfel_min_peaks"
            )
            pipeline_params["crystfel_indexing_methods"] = (
                self.settings_manager.get("crystfel_indexing_methods")
            )
            pipeline_params["crystfel_pdb"] = self.settings_manager.get(
                "crystfel_cell_file"
            )
            # Detailed peak finding params
            pipeline_params["crystfel_min_snr_biggest_pix"] = self.settings_manager.get("crystfel_min_snr_biggest_pix")
            pipeline_params["crystfel_min_snr_peak_pix"] = self.settings_manager.get("crystfel_min_snr_peak_pix")
            pipeline_params["crystfel_min_sig"] = self.settings_manager.get("crystfel_min_sig")
            pipeline_params["crystfel_local_bg_radius"] = self.settings_manager.get("crystfel_local_bg_radius")
            # PF8
            pipeline_params["crystfel_peakfinder8_threshold"] = self.settings_manager.get("crystfel_peakfinder8_threshold")
            pipeline_params["crystfel_peakfinder8_min_pix_count"] = self.settings_manager.get("crystfel_peakfinder8_min_pix_count")
            pipeline_params["crystfel_peakfinder8_max_pix_count"] = self.settings_manager.get("crystfel_peakfinder8_max_pix_count")
            
            # Speed/Optimization booleans
            pipeline_params["crystfel_no_check_peaks"] = self.settings_manager.get("crystfel_no_check_peaks")
            pipeline_params["crystfel_no_refine"] = self.settings_manager.get("crystfel_no_refine")
            pipeline_params["crystfel_no_non_hits"] = self.settings_manager.get("crystfel_no_non_hits")
            pipeline_params["crystfel_peakfinder8_fast"] = self.settings_manager.get("crystfel_peakfinder8_fast")
            pipeline_params["crystfel_asdf_fast"] = self.settings_manager.get("crystfel_asdf_fast")
            pipeline_params["crystfel_no_retry"] = self.settings_manager.get("crystfel_no_retry")
            pipeline_params["crystfel_no_multi"] = self.settings_manager.get("crystfel_no_multi")
            pipeline_params["crystfel_include_mask"] = self.settings_manager.get("crystfel_include_mask")
            pipeline_params["crystfel_delete_workdir"] = self.settings_manager.get("crystfel_delete_workdir")
            
            # Integration
            pipeline_params["crystfel_push_res"] = self.settings_manager.get("crystfel_push_res")
            pipeline_params["crystfel_integration_mode"] = self.settings_manager.get("crystfel_integration_mode")
            pipeline_params["crystfel_int_radius"] = self.settings_manager.get("crystfel_int_radius")
            
            # XGANDALF
            pipeline_params["crystfel_xgandalf_fast"] = self.settings_manager.get("crystfel_xgandalf_fast")
            pipeline_params["crystfel_xgandalf_sampling_pitch"] = self.settings_manager.get("crystfel_xgandalf_sampling_pitch")
            pipeline_params["crystfel_xgandalf_grad_desc_iterations"] = self.settings_manager.get("crystfel_xgandalf_grad_desc_iterations")
            pipeline_params["crystfel_xgandalf_tolerance"] = self.settings_manager.get("crystfel_xgandalf_tolerance")
            pipeline_params["crystfel_xgandalf_no_deviation"] = self.settings_manager.get("crystfel_xgandalf_no_deviation")
            pipeline_params["crystfel_xgandalf_min_lattice"] = self.settings_manager.get("crystfel_xgandalf_min_lattice")
            pipeline_params["crystfel_xgandalf_max_lattice"] = self.settings_manager.get("crystfel_xgandalf_max_lattice")
            pipeline_params["crystfel_xgandalf_max_peaks"] = self.settings_manager.get("crystfel_xgandalf_max_peaks")

            # Misc
            pipeline_params["crystfel_extra_options"] = self.settings_manager.get("crystfel_extra_options")

            # Auto-Threshold Logic
            # Only if PF8 is selected (or default) AND auto is enabled
            peaks_method = pipeline_params.get("crystfel_peaks_method", "peakfinder8")
            auto_thresh = self.settings_manager.get("crystfel_peakfinder8_auto_threshold", True)
            
            # Check if override disabled auto (unlikely but possible if passed explicitly)
            if override_params and "crystfel_peakfinder8_auto_threshold" in override_params:
                 auto_thresh = override_params["crystfel_peakfinder8_auto_threshold"]

            if peaks_method == "peakfinder8" and auto_thresh:
                 # Check if we have image data
                 # We need the displayed image if we are running on current view
                 try:
                      # StrategyManager is usually run on current view -> self.graphics_manager.img_item
                      img_item = getattr(self.graphics_manager, "img_item", None)
                      if img_item and img_item.image is not None:
                           mask = None
                           if hasattr(self.main_window, "detector_mask_manager"):
                                self.main_window.detector_mask_manager.ensure_mask_up_to_date()
                                mask = self.main_window.detector_mask_manager.mask

                           calc_thresh = calculate_robust_threshold_mad(img_item.image, mask)
                           if calc_thresh is not None:
                                logger.info(f"Auto-calculated CrystFEL PF8 Threshold: {calc_thresh:.1f}")
                                pipeline_params["crystfel_peakfinder8_threshold"] = calc_thresh
                           else:
                                logger.warning("Could not auto-calculate threshold (no valid data?), using static default.")
                      else:
                           logger.warning("No image displayed to auto-calculate threshold. Using static default.")
                 except Exception as e:
                      logger.error(f"Failed to auto-calculate threshold: {e}")

        # Apply overrides if provided (this superseeds settings manager values)
        if override_params:
             pipeline_params.update(override_params)

        # Filter out any None values
        pipeline_params = {k: v for k, v in pipeline_params.items() if v is not None}

        delete_workdir = False
        # Check if crystfel is in use and use its specific setting
        is_crystfel = "crystfel" in programs or programs == "crystfel"
        if is_crystfel:
             # Check split logic: if passed in params, use it; otherwise check settings
             if override_params and "crystfel_delete_workdir" in override_params:
                  delete_workdir = override_params["crystfel_delete_workdir"]
             else:
                  delete_workdir = self.settings_manager.get("crystfel_delete_workdir", False)
        
        worker = StrategyWorker(
            program_list, mapping, pipeline_params=pipeline_params, delete_workdir=delete_workdir
        )
        worker.signals.finished.connect(self._on_strategy_finished)
        worker.signals.error.connect(self._on_strategy_error)
        worker.signals.all_done.connect(self._on_all_strategies_done)
        self.threadpool.start(worker)

    @pyqtSlot()
    def _on_all_strategies_done(self):
        """Called when all tasks in the StrategyWorker have completed."""
        QtWidgets.QApplication.restoreOverrideCursor()
        self.ui_manager.clear_status_message_if("Starting")
        self.ui_manager.show_status_message("All strategy calculations complete.", 5000)

    @pyqtSlot(str, object, dict)
    def _on_strategy_finished(self, program: str, result_data: dict, mapping: dict):
        """Handles successful completion of the strategy worker."""

        self.ui_manager.clear_status_message_if(f"Starting {program.upper()}")
        self.ui_manager.show_status_message(
            f"{program.upper()} strategy finished.", 4000
        )

        from qp2.image_viewer.strategy.strategy_results_dialog import (
            StrategyResultsDialog,
        )

        if program in self.strategy_dialogs:
            try:
                self.strategy_dialogs[program].close()
            except Exception:
                pass

        dialog = StrategyResultsDialog(result_data, program, mapping, self.main_window)
        self.strategy_dialogs[program] = dialog

        dialog.request_show_spots.connect(self._show_strategy_spots)
        dialog.request_hide_spots.connect(self.graphics_manager.clear_spots)
        dialog.request_show_reflections.connect(self._show_strategy_reflections)
        dialog.request_hide_reflections.connect(
            self.graphics_manager.clear_indexed_reflections
        )
        
        # Connect to self._display_frame_from_strategy_dialog
        dialog.request_frame_display.connect(self._display_frame_from_strategy_dialog)

        dialog.show()

    def _on_strategy_error(self, program: str, error_msg: str):
        self.ui_manager.show_critical_message(
            f"{program.upper()} Strategy Failed", error_msg
        )

    def _show_strategy_spots(self, spots_yx: np.ndarray):
        self.graphics_manager.display_spots(spots_yx)

    def _show_strategy_reflections(self, reflections: list):
        self.graphics_manager.display_indexed_reflections(reflections)

    @pyqtSlot(str, int)
    def _display_frame_from_strategy_dialog(self, master_path: str, frame_index: int):
        """Switches the main view to the specified dataset and frame."""
        if self.main_window.current_master_file != master_path:
            # This will trigger a file load and automatically display the first frame
            self.main_window.file_io_manager.load_file(master_path)
        # Go to the specific frame
        self.main_window.playback_manager.go_to_frame(frame_index)
