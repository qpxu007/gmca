# crystfel_manager.py
import os
import json
from typing import Optional, Dict, Any

import numpy as np
import pyqtgraph as pg
from PyQt5 import QtCore, QtWidgets

from qp2.image_viewer.plugins.crystfel.crystfel_settings_dialog import (
    CrystfelSettingsDialog,
)
from qp2.image_viewer.plugins.crystfel.submit_crystfel_job import (
    CrystfelProcessDatasetWorker,
)
from qp2.image_viewer.plugins.generic_plot_manager import GenericPlotManager
from qp2.log.logging_config import get_logger
from qp2.xio.proc_utils import determine_proc_base_dir

logger = get_logger(__name__)

REDIS_CRYSTFEL_KEY_PREFIX = "analysis:out:crystfel"


class CrystfelManager(GenericPlotManager):
    def __init__(self, parent):
        crystfel_config = {
            "worker_class": CrystfelProcessDatasetWorker,
            "redis_connection": parent.redis_output_server,
            "redis_key_template": f"{REDIS_CRYSTFEL_KEY_PREFIX}:{{master_file}}",
            "spot_field_key": "spots_crystfel",
            "x_axis_key": "img_num",
            "default_y_axis": "num_spots_crystfel",
            "refresh_interval_ms": 5000,
            "default_source_type": "redis",
            "status_key_type": "string",
        }
        super().__init__(parent=parent, name="CrystFEL", config=crystfel_config)
        self.processed_datasets = set()

    def update_source(self, new_reader, new_master_file):
        """Overrides GenericPlotManager to connect to series_completed signal."""
        if self.reader and hasattr(self.reader, "series_completed"):
            try:
                self.reader.series_completed.disconnect(self.handle_dataset_completed)
            except (TypeError, RuntimeError):
                pass

        super().update_source(new_reader, new_master_file)

        if self.reader and hasattr(self.reader, "series_completed"):
            self.reader.series_completed.connect(self.handle_dataset_completed)
            # Auto-run if live mode
            if self.reader.series_completion_signal_emitted and self.main_window.is_live_mode:
                self.handle_dataset_completed(
                    self.reader.master_file_path,
                    self.reader.total_frames,
                    self.reader.get_parameters(),
                )

    def handle_data_files_ready(self, files_batch: list):
        """
        CrystFEL is now dataset-based (series). 
        We ignore per-file updates to avoid launching redundant jobs.
        """
        pass

    @QtCore.pyqtSlot()
    def _rerun_analysis(self, reader=None, master_file=None):
        """
        Overrides GenericPlotManager to handle dataset-level re-run.
        """
        target_reader = reader if reader else self.reader
        if not target_reader:
            return

        # Clear local tracking to allow re-triggering
        if target_reader.master_file_path in self.processed_datasets:
            self.processed_datasets.remove(target_reader.master_file_path)

        self.handle_dataset_completed(
            target_reader.master_file_path,
            target_reader.total_frames,
            target_reader.get_parameters(),
            force_rerun=True
        )

    @QtCore.pyqtSlot(str, int, dict)
    def handle_dataset_completed(self, master_file_path, total_frames, metadata, force_rerun=False):
        """Launches dataset-level CrystFEL processing."""
        worker_class = self.config.get("worker_class")
        if not worker_class or not self.run_processing_enabled:
            return
        
        if not force_rerun and master_file_path in self.processed_datasets:
            return
            
        self.processed_datasets.add(master_file_path)

        worker_kwargs = self._prepare_worker_kwargs()
        if force_rerun:
            worker_kwargs["force_rerun"] = True

        worker = worker_class(
            master_file=master_file_path,
            metadata=metadata,
            redis_conn=self.redis_connection,
            redis_key_prefix=REDIS_CRYSTFEL_KEY_PREFIX,
            **worker_kwargs,
        )
        worker.signals.error.connect(self._handle_worker_error)
        worker.signals.result.connect(self._handle_worker_result)
        self.request_main_threadpool.emit(worker)

    def _fetch_and_prepare_data(self) -> bool:
        """
        Loads results from JSON file if available in Redis.
        """
        if (
            self._current_data_source_type != "redis"
            or not self.redis_connection
            or not self.current_master_file
        ):
            return False

        key = self._current_data_source_path_or_key
        status_key = f"{key}:status"
        
        json_path = None
        try:
            # 1. Check for results_json in Redis hash (Main Key)
            json_path = self.redis_connection.hget(key, "results_json")
            
            # 2. Fallback: Check the status key (which is a JSON string)
            if not json_path:
                status_raw = self.redis_connection.get(status_key)
                if status_raw:
                    status_data = json.loads(status_raw)
                    json_path = status_data.get("results_json")

            if not json_path or not os.path.exists(json_path):
                # Fallback to base class polling (hash of frames) if JSON not ready
                # This is important for "live" updates while the job is still running
                # and hasn't written the final JSON yet.
                return super()._fetch_and_prepare_data()

            # Load the JSON file
            with open(json_path, "r") as f:
                raw_data = json.load(f)
                
                # Support both dictionary format (new) and list format (old)
                if isinstance(raw_data, dict):
                    new_data = list(raw_data.values())
                elif isinstance(raw_data, list):
                    new_data = raw_data
                else:
                    logger.warning(f"[{self.name}] Unexpected data type in results JSON: {type(raw_data)}")
                    return False
                
                # Fallback: Ensure every item has img_num for plotting
                for i, item in enumerate(new_data):
                    if "img_num" not in item:
                        # Try serial number or event_num, otherwise use index
                        val = item.get("image_serial_number") or item.get("event_num")
                        if val is not None:
                            item["img_num"] = val + 1
                        else:
                            item["img_num"] = i + 1
                    
                    if "num_spots_crystfel" not in item and "spots_crystfel" in item:
                        item["num_spots_crystfel"] = len(item["spots_crystfel"])

                # JSON is now a list of frame results, sort by img_num
                new_data.sort(key=lambda x: x.get("img_num", 0))

            if new_data != self.plot_data:
                self.plot_data = new_data
                self._update_available_metrics()
                return True
        except Exception as e:
            logger.error(f"[{self.name}] Error loading results from JSON ({json_path}): {e}")
        
        return False

    def _setup_ui(self):
        # First, call the parent method to build the standard control bar
        super()._setup_ui()

        # --- ADDED: Add a new Settings button to the control bar ---
        self.crystfel_settings_button = QtWidgets.QPushButton("⚙️ Settings")  # Use an icon/symbol
        self.crystfel_settings_button.setToolTip("Open CrystFEL-specific settings")
        # self.crystfel_settings_button.setFixedSize(QtCore.QSize(30, 25))

        # Insert it before the "Actions" dropdown
        actions_button_index = (
            self.container_widget.layout()
            .itemAt(0)
            .layout()
            .indexOf(self.actions_button)
        )
        self.container_widget.layout().itemAt(0).layout().insertWidget(
            actions_button_index, self.crystfel_settings_button
        )

        # Connect the button's clicked signal
        self.crystfel_settings_button.clicked.connect(self._open_crystfel_settings)

    def _open_crystfel_settings(self):
        """Opens the dialog to configure CrystFEL parameters."""
        dialog = CrystfelSettingsDialog(
            current_settings=self.main_window.settings_manager.as_dict(),
            parent=self.main_window,
        )
        # Connect the dialog's signal to the main settings manager
        dialog.settings_changed.connect(
            self.main_window.settings_manager.update_from_dict
        )
        dialog.show()

    def _prepare_worker_kwargs(self) -> dict:
        """
        Overrides the base method to pass all user-configured CrystFEL
        settings to the worker.
        """
        settings = self.main_window.settings_manager
        
        user_root = settings.get("processing_common_proc_dir_root", "")
        data_path = self.reader.master_file_path if self.reader else ""
        proc_base = determine_proc_base_dir(user_root, data_path)
        
        # Calculate the dataset-specific processing directory
        master_basename = os.path.splitext(os.path.basename(data_path))[0]
        output_proc_dir = proc_base / "crystfel" / master_basename

        # Fallback for PDB file
        pdb_file = settings.get("crystfel_pdb_file")
        if not pdb_file:
            pdb_file = settings.get("processing_common_model_file", "")

        # Gather all the keys from the settings manager
        return {
            "nproc": settings.get("crystfel_nproc"),
            "peak_method": settings.get("crystfel_peaks_method"),
            "min_snr": settings.get("crystfel_min_snr"),
            "min_snr_biggest_pix": settings.get("crystfel_min_snr_biggest_pix"),
            "min_snr_peak_pix": settings.get("crystfel_min_snr_peak_pix"),
            "min_peaks": settings.get("crystfel_min_peaks", 15),
            "no_non_hits": settings.get("crystfel_no_non_hits", True),
            "indexing_methods": settings.get("crystfel_indexing_methods", "xgandalf"),
            "xgandalf_fast": settings.get("crystfel_xgandalf_fast", True),
            "no_refine": settings.get("crystfel_no_refine", False),
            "no_check_peaks": settings.get("crystfel_no_check_peaks", False),
            "peakfinder8_fast": settings.get("crystfel_peakfinder8_fast", True),
            "asdf_fast": settings.get("crystfel_asdf_fast", True),
            "no_retry": settings.get("crystfel_no_retry", True),
            "no_multi": settings.get("crystfel_no_multi", True),
            "push_res": settings.get("crystfel_push_res", 0.0),
            "integration_mode": settings.get("crystfel_integration_mode", "Standard"),
            "debug": settings.get("crystfel_debug", False),
            "min_sig": settings.get("crystfel_min_sig"),
            "local_bg_radius": settings.get("crystfel_local_bg_radius"),
            "pdb_file": pdb_file,
            "extra_options": settings.get("crystfel_extra_options", ""),
            "int_radius": settings.get("crystfel_int_radius", "3,4,5"),
            "output_proc_dir": str(output_proc_dir),
            "processing_common_proc_dir_root": user_root,
        }

    def _parse_spot_data(self, spots_raw: list) -> Optional[np.ndarray]:
        """
        Handles CrystFEL's (fs, ss) spot format. 
        CrystFEL provides (x, y). GraphicsManager expects (y, x) for internal 
        consistency with numpy/pyqtgraph row-major indexing.
        """
        if not spots_raw or not isinstance(spots_raw, list):
            return None
        try:
            # CrystFEL: spots_raw is [(fs, ss), ...] -> (x, y)
            # GraphicsManager.display_spots expects yx_coords[:, 1] to be x 
            # and yx_coords[:, 0] to be y.
            # So we return [[y, x], ...]
            return np.array([[float(s[1]), float(s[0])] for s in spots_raw])
        except (IndexError, TypeError, ValueError):
            return None

    def _update_plot_display(self):
        """
        Overrides the base method to add conditional coloring for indexed frames.
        This version is self-contained and does not call super().
        """
        selected_metric = self.metric_combobox.currentText()
        self.plot_y_axis_key = selected_metric

        # 1. Clear existing plot item if data is invalid
        if (
            not self.plot_data
            or not selected_metric
            or selected_metric == "No numeric data"
        ):
            if self.plot_data_item:
                self.plot_widget.removeItem(self.plot_data_item)
                self.plot_data_item = None
            return

        # 2. Prepare point data
        indexed_points = []
        unindexed_points = []
        x_key = self.config.get("x_axis_key", "img_num")

        # Separate data points into two lists based on indexing status
        for i, item in enumerate(self.plot_data):
            if item.get(x_key) is not None and item.get(selected_metric) is not None:
                point = {"pos": (item[x_key], item[selected_metric]), "data": i}
                if item.get("crystfel_indexed_by", "none") != "none":
                    indexed_points.append(point)
                else:
                    unindexed_points.append(point)

        # 3. Create or clear the plot item
        if not isinstance(self.plot_data_item, pg.ScatterPlotItem):
            if self.plot_data_item:
                self.plot_widget.removeItem(self.plot_data_item)
            self.plot_data_item = pg.ScatterPlotItem(
                hoverable=False,
                selectionBrush=None,
                selectionPen=None,
            )
            self.plot_widget.addItem(self.plot_data_item)
            self.plot_data_item.sigClicked.connect(self._on_plot_point_clicked)
        else:
            self.plot_data_item.clear()

        # 4. Add the points with their respective styles
        self.plot_data_item.addPoints(
            unindexed_points,
            symbol="o",
            size=8,
            brush=pg.mkBrush(255, 165, 0, 200),  # Orange
            pen=None,
        )
        self.plot_data_item.addPoints(
            indexed_points,
            symbol="o",
            size=10,
            brush=pg.mkBrush(0, 255, 255, 255),  # Cyan
            pen=pg.mkPen("w"),  # White border
        )

        # 5. Update axes and view range
        self.plot_widget.setLabel("left", selected_metric)
        view_box = self.plot_widget.getViewBox()

        if (
            not self.plot_x_manual_range
            and self.reader
            and self.reader.total_frames > 0
        ):
            view_box.enableAutoRange(axis=pg.ViewBox.XAxis, enable=False)
            self.plot_widget.setXRange(1, self.reader.total_frames + 1, padding=0.05)

        view_box.enableAutoRange(axis=pg.ViewBox.YAxis, enable=True)

    def _on_plot_point_clicked(self, plot_item, points):
        """
        Overrides base method to display un-indexed spots, and if available,
        overlay indexed reflections, their labels, and indexing info.
        """
        if points is None:
            return

        point = points[0]
        point_index = point.data()
        clicked_frame_data = self.plot_data[point_index]
        frame_num_1_based = clicked_frame_data["img_num"]
        frame_index_0_based = frame_num_1_based - 1

        # 1. Signal to main window to load the frame
        self.frame_selected.emit(frame_index_0_based)

        # 2. Clear all previous plugin visuals for a clean slate
        gfx = self.main_window.graphics_manager
        gfx.clear_spots()
        gfx.clear_indexed_reflections()
        gfx.clear_plugin_info_text()

        # 3. ALWAYS display the raw, un-indexed spots first.
        spots_raw = clicked_frame_data.get(self.config["spot_field_key"], [])
        if spots_raw:
            spot_coords_yx = self._parse_spot_data(spots_raw)
            if spot_coords_yx is not None:
                gfx.display_spots(spot_coords_yx)

        # 4. If the image was indexed, OVERLAY the indexed reflections and info.
        unit_cell_params = clicked_frame_data.get("unit_cell_crystfel")

        logger.debug(
            f"Checking for indexing. 'unit_cell_crystfel' content: {unit_cell_params}"
        )

        # Check if the data is a list with exactly 6 elements
        if isinstance(unit_cell_params, list) and len(unit_cell_params) == 6:
            logger.debug("Frame is indexed. Proceeding to display info.")

            # a) Display the indexed reflections
            reflections_raw = clicked_frame_data.get("reflections_crystfel", [])
            if reflections_raw:
                parsed_reflections = []
                for r in reflections_raw:
                    try:
                        # Correct indices for fs/px and ss/px in a stream file
                        parsed_reflections.append(
                            {
                                "h": int(r[0]),
                                "k": int(r[1]),
                                "l": int(r[2]),
                                "x": float(r[7]),
                                "y": float(r[8]),
                            }
                        )
                    except (ValueError, IndexError):
                        continue
                gfx.display_indexed_reflections(parsed_reflections)

            # b) Display the indexing information text box
            # Now it is safe to unpack
            cell_a, cell_b, cell_c, cell_alpha, cell_beta, cell_gamma = unit_cell_params
            lattice_type = clicked_frame_data.get("crystfel_lattice", "N/A")
            lattice_centering = clicked_frame_data.get("crystfel_centering", "N/A")
            indexer = clicked_frame_data.get("crystfel_indexed_by", "N/A")

            info_html = f"""
            <div style='color: #FFFFD0; font-family: Consolas, "Courier New", monospace; font-size: 9pt;'>
            <b>Indexer:</b> {indexer}<br>
            <b>Lattice:</b> {lattice_type} <b>Centering:</b> {lattice_centering}<br>
            <b>Cell:</b>
            {cell_a:.2f}, {cell_b:.2f}, {cell_c:.2f}<br>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            {cell_alpha:.1f}, {cell_beta:.1f}, {cell_gamma:.1f}
            </div>
            """
            logger.debug("Generated valid HTML. Calling graphics manager to display.")
            gfx.display_plugin_info_text(info_html)
        else:
            logger.debug(
                "Frame is not indexed or unit cell data is malformed. No info box will be shown."
            )
