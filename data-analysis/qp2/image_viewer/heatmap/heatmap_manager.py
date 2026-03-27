# qp2/image_viewer/heatmap/heatmap_manager.py

from PyQt5.QtCore import QObject, pyqtSlot
# --- ADD THIS IMPORT ---
from pyqtgraph.Qt import QtWidgets, QtCore

from qp2.image_viewer.heatmap.heatmap_data_from_file_worker import HeatmapDataFromFileWorker
from qp2.image_viewer.heatmap.heatmap_data_worker import HeatmapDataWorker
from qp2.image_viewer.heatmap.heatmap_dialog import HeatmapDialog
from qp2.image_viewer.heatmap.heatmap_utils import find_heatmap_hotspots
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class HeatmapManager(QObject):
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.active_dialog = None
        self.scan_map = {}
        self.raw_data_map = {}
        self.was_live_mode = False
        self.scan_mode = "row_wise"

    @pyqtSlot(str)
    def launch_viewer(self, run_prefix: str):
        mw = self.main_window

        if mw.is_live_mode:
            self.was_live_mode = True
            mw.is_live_mode = False
            mw.ui_manager.show_status_message("Live mode paused for heatmap analysis.", 3000)

        if self.active_dialog:
            self.active_dialog.raise_()
            self.active_dialog.activateWindow()
            return

        active_plugin = mw.analysis_plugin_manager.active_plugin
        if not active_plugin or not mw.redis_manager:
            mw.ui_manager.show_warning_message(
                "Error", "An active analysis plugin and Redis connection are required."
            )
            return

        run_datasets = mw.dataset_manager.get_datasets_for_run(run_prefix)
        if not run_datasets:
            mw.ui_manager.show_warning_message(
                "Error", "No datasets found for this run."
            )
            return

        self.scan_mode = self.main_window.settings_manager.get("scan_mode", "auto")
        if self.scan_mode == "auto":
            self.scan_mode = self._resolve_auto_scan_mode(run_prefix, run_datasets)

        default_metric = active_plugin.config.get("default_y_axis")
        self.active_dialog = HeatmapDialog(
            run_prefix,
            default_metric=default_metric,
            scan_mode=self.scan_mode,
            parent=mw
        )
        # Connect the new signal to its handler
        self.active_dialog.cell_clicked.connect(self._on_cell_clicked)
        self.active_dialog.find_hotspots_requested.connect(
            self._on_find_hotspots_requested
        )
        self.active_dialog.refresh_requested.connect(self._on_refresh_requested)

        self.active_dialog.show()
        self.active_dialog.finished.connect(self._on_dialog_closed)

        mw.ui_manager.show_status_message(
            f"Fetching heatmap data for run '{run_prefix}'...", 0
        )
        
        # --- ADD THIS LINE ---
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)

        plugin_name = self.main_window.analysis_plugin_manager.active_plugin.name
        plugins_using_file_storage = ["nXDS", "DIALS SSX"]
        if plugin_name in plugins_using_file_storage:
            WorkerClass = HeatmapDataFromFileWorker
        else:  # Default to the original Redis HASH worker
            WorkerClass = HeatmapDataWorker

        worker = WorkerClass(
            datasets=run_datasets,
            redis_conn=mw.redis_output_server,
            plugin_config=mw.analysis_plugin_manager.active_plugin.config,
            scan_mode=self.scan_mode,
        )
        worker.signals.finished.connect(self._on_data_fetched)
        worker.signals.error.connect(self._on_fetch_error)
        mw.threadpool.start(worker)

    @pyqtSlot(dict)
    def _on_data_fetched(self, final_result: dict):
        # --- ADD THIS LINE ---
        QtWidgets.QApplication.restoreOverrideCursor()
        
        self.main_window.ui_manager.clear_status_message_if("Fetching heatmap data")

        data_matrices = final_result.get("data_matrices", {})
        self.scan_map = final_result.get("scan_map", {})
        self.raw_data_map = final_result.get("raw_data_map", {})
        if self.active_dialog:
            self.active_dialog.populate_metrics(data_matrices)

    @pyqtSlot(str)
    def _on_fetch_error(self, error_msg: str):
        # --- ADD THIS LINE ---
        QtWidgets.QApplication.restoreOverrideCursor()

        self.main_window.ui_manager.clear_status_message_if("Fetching heatmap data")
        self.main_window.ui_manager.show_warning_message(
            "Heatmap Error", f"Failed to fetch data: {error_msg}"
        )
        if self.active_dialog:
            self.active_dialog.close()

    def _on_dialog_closed(self):
        # Good practice to ensure cursor is restored if dialog is closed while worker is running
        QtWidgets.QApplication.restoreOverrideCursor()

        if self.was_live_mode:
            self.main_window.is_live_mode = True
            self.main_window.ui_manager.show_status_message("Live mode resumed.", 3000)
            self.was_live_mode = False
        self.active_dialog = None
        self.scan_map = {}
        self.raw_data_map = {}

    @pyqtSlot(str, float, int)
    def _on_find_hotspots_requested(
            self, find_mode: str, percentile: float, min_size: int
    ):
        """Processes the current heatmap data to find and display hotspots."""
        if not self.active_dialog or not self.active_dialog.current_metric:
            return

        current_metric = self.active_dialog.current_metric
        data_matrix = self.active_dialog.data_matrices.get(current_metric)

        if data_matrix is None:
            return

        self.main_window.ui_manager.show_status_message(
            f"Finding hotspots in '{current_metric}' data...", 0
        )

        # This runs in the main thread, but it's very fast.
        # For huge heatmaps, this could be moved to a worker.
        hotspots = find_heatmap_hotspots(data_matrix, percentile, find_mode, min_size)

        self.main_window.ui_manager.show_status_message(
            f"Found {len(hotspots)} hotspots.", 3000
        )

        if self.active_dialog:
            self.active_dialog.show_hotspot_markers(hotspots)

    @pyqtSlot(int, int)
    def _on_cell_clicked(self, matrix_row: int, matrix_col: int):
        mw = self.main_window

        # --- START: MODIFIED LOGIC FOR SCAN MODE ---
        is_column_scan = "column" in self.scan_mode

        if is_column_scan:
            scan_index = matrix_col
            target_reader = self.scan_map.get(scan_index)
            frame_data = self.raw_data_map.get((matrix_row, matrix_col), {})
        else:  # Row scan
            scan_index = matrix_row
            target_reader = self.scan_map.get(scan_index)
            frame_data = self.raw_data_map.get((matrix_row, matrix_col), {})
        # --- END: MODIFIED LOGIC FOR SCAN MODE ---

        if target_reader and frame_data:
            mw.ui_manager.select_dataset_in_tree(target_reader.master_file_path)
            
            x_axis_key = mw.analysis_plugin_manager.active_plugin.config.get("x_axis_key")
            original_frame_index = frame_data.get(x_axis_key, 1) - 1

            # 2. Get the raw spot list using the active plugin's config.
            spot_key = mw.analysis_plugin_manager.active_plugin.config.get("spot_field_key")
            refl_key = "reflections_crystfel"

            spots_raw = frame_data.get(spot_key, [])
            refls_raw = frame_data.get(refl_key, [])

            # 3. Use the plugin's own parser to convert the raw data to (y, x) coordinates.
            spots_coords = mw.analysis_plugin_manager.active_plugin._parse_spot_data(spots_raw)

            parsed_reflections = []
            if refls_raw:
                try:
                    for r in refls_raw:
                        parsed_reflections.append({
                            "h": int(r[0]), "k": int(r[1]), "l": int(r[2]),
                            "x": float(r[7]), "y": float(r[8]),
                        })
                except (ValueError, IndexError):
                    pass  # Ignore malformed reflection data

            indexing_info = None
            unit_cell = frame_data.get("unit_cell_crystfel")
            if isinstance(unit_cell, list) and len(unit_cell) == 6:
                indexing_info = {
                    "unit_cell": unit_cell,
                    "indexer": frame_data.get("crystfel_indexed_by", "N/A"),
                    "lattice_type": frame_data.get("crystfel_lattice", "N/A"),
                    "centering": frame_data.get("crystfel_centering", "N/A"),
                }

            # Create the overlays dictionary
            overlays = {
                'spots': spots_coords,
                'reflections': parsed_reflections,
                'indexing_info': indexing_info
            }

            # 4. Call the new method on the main window.
            mw.display_image_with_overlays(target_reader, original_frame_index, overlays)
        else:
            scan_type = "column" if is_column_scan else "row"
            mw.ui_manager.show_warning_message(
                "Not Found", f"Could not find the dataset for {scan_type} {scan_index + 1}."
            )

    @pyqtSlot()
    def _on_refresh_requested(self):
        """Re-runs the data fetching worker to get the latest results."""
        if not self.active_dialog:
            return

        mw = self.main_window
        run_prefix = self.active_dialog.windowTitle().replace("Grid Heatmap: ", "")

        mw.ui_manager.show_status_message(f"Refreshing heatmap data for '{run_prefix}'...", 0)

        # --- ADD THIS LINE ---
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)

        self.scan_mode = self.main_window.settings_manager.get("scan_mode", "auto")
        if self.scan_mode == "auto":
            run_datasets = mw.dataset_manager.get_datasets_for_run(run_prefix)
            self.scan_mode = self._resolve_auto_scan_mode(run_prefix, run_datasets)

        # --- START MODIFICATION ---
        # This re-uses the exact same worker logic as the initial launch
        # Ensure we select the correct worker based on the active plugin
        plugin_name = self.main_window.analysis_plugin_manager.active_plugin.name
        plugins_using_file_storage = ["nXDS", "DIALS SSX"]
        if plugin_name in plugins_using_file_storage:
            WorkerClass = HeatmapDataFromFileWorker
        else:
            WorkerClass = HeatmapDataWorker

        worker = WorkerClass(
            datasets=mw.dataset_manager.get_datasets_for_run(run_prefix),
            redis_conn=mw.redis_output_server,
            plugin_config=mw.analysis_plugin_manager.active_plugin.config,
            scan_mode=self.scan_mode,
        )
        # --- END MODIFICATION ---
        
        # The worker will emit its 'finished' signal, which is already connected
        # to _on_data_fetched, so the UI will update automatically.
        worker.signals.finished.connect(self._on_data_fetched)
        worker.signals.error.connect(self._on_fetch_error)
        mw.threadpool.start(worker)

    def _resolve_auto_scan_mode(self, run_prefix: str, datasets) -> str:
        """Resolve scan mode from analysis Redis or filename pattern."""
        from qp2.pipelines.raster_3d.scan_mode import resolve_auto_scan_mode

        mw = self.main_window
        master_files = [d.master_file_path for d in datasets] if datasets else []
        group_name = ""
        if master_files:
            try:
                from qp2.xio.user_group_manager import get_esaf_from_data_path
                info = get_esaf_from_data_path(master_files[0])
                group_name = info.get("group_name") or info.get("primary_group", "")
            except Exception:
                pass
        return resolve_auto_scan_mode(
            run_prefix, master_files,
            analysis_conn=mw.redis_output_server,
            group_name=group_name,
        )
