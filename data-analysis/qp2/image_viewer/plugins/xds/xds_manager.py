# qp2/image_viewer/plugins/xds/xds_manager.py
import json
import os
from pathlib import Path

import pyqtgraph as pg
from PyQt5 import QtCore, QtWidgets

from qp2.image_viewer.plugins.generic_plot_manager import GenericPlotManager
from qp2.image_viewer.plugins.xds.submit_xds_job import XDSProcessDatasetWorker
from qp2.image_viewer.plugins.xds.xds_results_dialog import XDSResultsDialog
from qp2.image_viewer.plugins.xds.xds_settings_dialog import XDSSettingsDialog
from qp2.log.logging_config import get_logger
from qp2.xio.db_manager import get_beamline_from_hostname

logger = get_logger(__name__)
REDIS_XDS_KEY_PREFIX = "analysis:out:xds"


class XDSManager(GenericPlotManager):
    def __init__(self, parent):
        xds_config = {
            "worker_class": XDSProcessDatasetWorker,
            "redis_connection": parent.redis_output_server,
            "redis_key_template": f"{REDIS_XDS_KEY_PREFIX}:{{master_file}}",
            "spot_field_key": None,
            "x_axis_key": "img_num",
            "default_y_axis": "num_strong_refl",
            "refresh_interval_ms": 10000,
            "default_source_type": "redis",
            "status_key_type": "string",
        }
        super().__init__(parent=parent, name="XDS", config=xds_config)
        self.processed_datasets = set()

        self.overall_stats = {}
        self.xds_settings = self._load_initial_xds_settings()

    def _load_initial_xds_settings(self) -> dict:
        """Loads only XDS-related keys from the main settings manager."""
        settings_mgr = self.main_window.settings_manager
        all_settings = settings_mgr.as_dict()
        return {k: v for k, v in all_settings.items() if k.startswith("xds_")}

    @QtCore.pyqtSlot(dict)
    def _on_xds_settings_changed(self, new_settings: dict):
        """
        Receives updated settings from the dialog, updates the local copy,
        and propagates them to the main settings manager.
        """
        self.xds_settings.update(new_settings)
        self.main_window.settings_manager.update_from_dict(self.xds_settings)
        logger.info(
            f"[{self.name}] Settings updated. New proc_dir_root: {self.xds_settings.get('xds_proc_dir_root')}"
        )

    def _setup_ui(self):
        super()._setup_ui()
        self.xds_settings_button = QtWidgets.QPushButton("⚙️ Settings")
        self.xds_settings_button.setToolTip("Open XDS specific settings")
        # self.xds_settings_button.setFixedSize(QtCore.QSize(30, 25))

        self.xds_results_button = QtWidgets.QToolButton()
        self.xds_results_button.setText("📊 View Report")
        self.xds_results_button.setToolTip("Show overall processing statistics")
        # self.xds_results_button.setFixedSize(QtCore.QSize(30, 25))

        # Insert buttons into the control bar
        control_bar = self.container_widget.layout().itemAt(0).layout()
        
        # 1. Settings button before Actions
        actions_button_index = control_bar.indexOf(self.actions_button)
        control_bar.insertWidget(actions_button_index, self.xds_settings_button)
        
        # 2. View Report button before Peel (which is after Y-axis and stretch)
        peel_button_index = control_bar.indexOf(self.peel_button)
        control_bar.insertWidget(peel_button_index, self.xds_results_button)

        self.xds_settings_button.clicked.connect(self._open_xds_settings)
        self.xds_results_button.clicked.connect(self._show_results_dialog)

    def _open_xds_settings(self):
        dialog = XDSSettingsDialog(
            current_settings=self.xds_settings,
            parent=self.main_window,
        )
        dialog.settings_changed.connect(self._on_xds_settings_changed)
        dialog.show()

    def _show_results_dialog(self):
        if not self.current_master_file:
            QtWidgets.QMessageBox.warning(
                self.main_window, "No Data", "No dataset is currently loaded."
            )
            return

        redis_key = self.config["redis_key_template"].format(
            master_file=self.current_master_file
        )
        proc_dir_str = self.redis_connection.hget(redis_key, "_proc_dir")

        if not proc_dir_str or not os.path.isdir(proc_dir_str):
            QtWidgets.QMessageBox.information(
                self.main_window, "No Results", "Processing directory not found."
            )
            return

        proc_dir = Path(proc_dir_str)
        stats_files = list(proc_dir.glob("**/XDS_stats.json"))

        if not stats_files:
            QtWidgets.QMessageBox.information(
                self.main_window,
                "No Results",
                "Could not find any XDS_stats.json files in the processing directory.",
            )
            return

        dialog = XDSResultsDialog(stats_files, self.main_window)
        dialog.exec_()

    def _prepare_worker_kwargs(self) -> dict:
        # Start with a copy of the specific XDS settings
        kwargs = self.xds_settings.copy()
        
        settings = self.main_window.settings_manager

        # Implement fallback logic
        if not kwargs.get("xds_space_group"):
            kwargs["xds_space_group"] = settings.get("processing_common_space_group", "")
        
        if not kwargs.get("xds_unit_cell"):
            kwargs["xds_unit_cell"] = settings.get("processing_common_unit_cell", "")
            
        if not kwargs.get("xds_model_pdb"):
             kwargs["xds_model_pdb"] = settings.get("processing_common_model_file", "")
             
        if not kwargs.get("xds_reference_hkl"):
             kwargs["xds_reference_hkl"] = settings.get("processing_common_reference_reflection_file", "")

        if not kwargs.get("xds_resolution"):
            common_high = settings.get("processing_common_res_cutoff_high")
            if common_high is not None:
                kwargs["xds_resolution"] = common_high

        kwargs["processing_common_proc_dir_root"] = settings.get("processing_common_proc_dir_root", "")

        logger.debug(
            f"[{self.name}] Preparing worker with settings (including common fallbacks): {kwargs}"
        )
        return kwargs

    def update_source(self, new_reader, new_master_file):
        # This method is correct and does not need changes
        if self.reader and hasattr(self.reader, "series_completed"):
            try:
                self.reader.series_completed.disconnect(self.handle_dataset_completed)
            except (TypeError, RuntimeError):
                pass
        self.reader = new_reader
        self.current_master_file = new_master_file
        self._current_data_source_path_or_key = self.config[
            "redis_key_template"
        ].format(master_file=self.current_master_file)
        self.init_plot_for_new_source()
        if self.reader and hasattr(self.reader, "series_completed"):
            self.reader.series_completed.connect(self.handle_dataset_completed)
            if self.reader.series_completion_signal_emitted and self.main_window.is_live_mode:
                self.handle_dataset_completed(
                    self.reader.master_file_path,
                    self.reader.total_frames,
                    self.reader.get_parameters(),
                )

    def init_plot_for_new_source(self):
        """Resets plot and also clears stored overall stats."""
        self.overall_stats = {}  # Clear overall stats for the new dataset
        super().init_plot_for_new_source()

    @QtCore.pyqtSlot(str, int, dict)
    def handle_dataset_completed(self, master_file_path, total_frames, metadata):
        worker_class = self.config.get("worker_class")
        if not worker_class or not self.run_processing_enabled:
            return
        if metadata.get("collect_mode", "STANDARD").upper() in ("RASTER", "STRATEGY"):
            logger.info(f"[{self.name}] Skipping XDS for {metadata.get('collect_mode')} dataset: {master_file_path}")
            return
        if master_file_path in self.processed_datasets:
            return
        self.processed_datasets.add(master_file_path)
        worker_kwargs = self._prepare_worker_kwargs()
        worker_kwargs["beamline"] = get_beamline_from_hostname()  # pass along beamline to write to the right db
        worker = worker_class(
            master_file=master_file_path,
            metadata=metadata,
            redis_conn=self.redis_connection,
            redis_key_prefix=self.config.get("redis_key_template", "").split(
                ":{master_file}"
            )[0],
            **worker_kwargs,
        )
        worker.signals.error.connect(self._handle_worker_error)
        worker.signals.result.connect(self._handle_worker_result)
        self.request_main_threadpool.emit(worker)

    @QtCore.pyqtSlot()
    def _rerun_analysis(self, reader=None, master_file=None):
        # Use passed args or fallback to current state
        target_reader = reader if reader else self.reader
        # target_master_file = master_file if master_file else self.current_master_file

        if not target_reader:
            return
        
        self.status_update.emit(f"[{self.name}] Re-running analysis...", 3000)
        
        if target_reader.master_file_path in self.processed_datasets:
            self.processed_datasets.remove(target_reader.master_file_path)
            
        self.handle_dataset_completed(
            target_reader.master_file_path,
            target_reader.total_frames,
            target_reader.get_parameters(),
        )

    def _fetch_and_prepare_data(self) -> bool:
        """
        MODIFIED: Fetches from two separate files and stores them separately.
        """
        if not self.redis_connection or not self.current_master_file:
            return False
        key = self._current_data_source_path_or_key
        try:
            proc_dir_str = self.redis_connection.hget(key, "_proc_dir")
            logger.debug(f"retrive data from {proc_dir_str} using redis key {key}")
            if not proc_dir_str:
                return False

            proc_dir = Path(proc_dir_str)
            per_frame_json_path = proc_dir / "XDS.json"
            overall_stats_json_path = proc_dir / "XDS_stats.json"

            if not per_frame_json_path.exists() or not overall_stats_json_path.exists():
                return False

            # 1. Load per-frame data (this becomes self.plot_data)
            with open(per_frame_json_path, "r") as f:
                per_frame_dict = json.load(f)
            new_plot_data = []
            for frame_num_str, frame_data in per_frame_dict.items():
                try:
                    frame_data["img_num"] = int(frame_num_str)
                    new_plot_data.append(frame_data)
                except (ValueError, TypeError):
                    continue
            new_plot_data.sort(key=lambda x: x["img_num"])

            # 2. Load overall stats (this is stored separately)
            with open(overall_stats_json_path, "r") as f:
                new_overall_stats = json.load(f)

            # 3. Check if either data source has changed
            if (
                    new_plot_data != self.plot_data
                    or new_overall_stats != self.overall_stats
            ):
                self.plot_data = new_plot_data
                self.overall_stats = new_overall_stats
                self._update_available_metrics()
                return True

        except Exception as e:
            logger.error(
                f"[{self.name}] Error fetching XDS results from files: {e}",
                exc_info=True,
            )
        return False

    def _update_plot_display(self):
        """
        Custom plot update method for XDS, using ScatterPlotItem directly.
        This avoids the problematic PlotDataItem used by the generic manager.
        """
        selected_metric = self.metric_combobox.currentText()
        self.plot_y_axis_key = selected_metric

        if (
                not self.plot_data
                or not selected_metric
                or selected_metric == "No numeric data"
        ):
            if self.plot_data_item:
                # self.plot_data_item here is a ScatterPlotItem, clear it
                self.plot_data_item.clear()
            return

        # Prepare points for the scatter plot
        points_to_plot = []
        x_key = self.config.get("x_axis_key", "img_num")
        for i, item in enumerate(self.plot_data):
            x_val = item.get(x_key)
            y_val = item.get(selected_metric)
            if x_val is not None and y_val is not None:
                # The 'data' field stores the original index for the click handler
                points_to_plot.append({"pos": (x_val, y_val), "data": i})

        # Ensure we have a ScatterPlotItem, not a PlotDataItem
        if not isinstance(self.plot_data_item, pg.ScatterPlotItem):
            if self.plot_data_item:
                self.plot_widget.removeItem(self.plot_data_item)
            self.plot_data_item = pg.ScatterPlotItem(
                symbol="o",
                size=8,
                brush=pg.mkBrush(255, 165, 0, 200),  # Orange
                pen=pg.mkPen("w"),
                hoverable=True
            )
            self.plot_widget.addItem(self.plot_data_item)
            # Use sigClicked for ScatterPlotItem, which has a different signature
            self.plot_data_item.sigClicked.connect(self._on_plot_point_clicked)

        # Set the data on the scatter plot item
        self.plot_data_item.setData(points_to_plot)

        # Update axes and view ranges
        self.plot_widget.setLabel("left", selected_metric)
        if (
                not self.plot_x_manual_range
                and self.reader
                and self.reader.total_frames > 0
        ):
            self.plot_widget.setXRange(1, self.reader.total_frames + 1, padding=0.05)
        self.plot_widget.getViewBox().enableAutoRange(
            axis=pg.ViewBox.YAxis, enable=True
        )

    def _on_plot_point_clicked(self, plot_item, points):
        """
        MODIFIED: Displays both per-frame stats for the clicked point and
        the overall dataset stats for context.
        """
        if points is None:
            return

        point_index = points[0].data()
        if point_index is None or point_index >= len(self.plot_data):
            logger.debug(f"point index = {point_index}, points[0]: {points[0]}, total spots: {len(self.plot_data)}")
            return  # Safety check

        clicked_frame_data = self.plot_data[point_index]
        frame_index_0_based = clicked_frame_data.get("img_num", 1) - 1

        # Action 1: Select the frame in the main viewer
        self.frame_selected.emit(frame_index_0_based)

        # Action 2: Display detailed, contextual information in the info box
        gfx = self.main_window.graphics_manager
        gfx.clear_spots()
        gfx.clear_indexed_reflections()
        gfx.clear_plugin_info_text()

        # --- Part A: Display stats for the CLICKED FRAME ---
        frame_num = clicked_frame_data.get("img_num", "N/A")
        strong_refl = clicked_frame_data.get("num_strong_refl", "N/A")
        overloads = clicked_frame_data.get("num_overloaded_refl", "N/A")
        beam_div = clicked_frame_data.get("beam_divergence_esd", "N/A")
        mosaicity = clicked_frame_data.get("mosaicity_esd", "N/A")

        frame_html = f"""
        <b style='color: #A0D0FF;'>Frame #{frame_num} Stats</b><br>
        <b>Strong Reflections:</b> {strong_refl}<br>
        <b>Overloaded:</b> {overloads}<br>
        <b>Beam Div. (ESD):</b> {beam_div:.3f}<br>
        <b>Mosaicity (ESD):</b> {mosaicity:.3f}<br>
        <hr style='border-color: #555;'>
        """

        # --- Part B: Display OVERALL dataset stats from the separate storage ---
        stats = self.overall_stats
        overall_html = "<b style='color: #FFFFD0;'>Overall Dataset Stats</b><br>"
        if stats:
            sg = stats.get("SPACE_GROUP_NUMBER", "N/A")
            # Ensure cell constants are formatted correctly from list or string
            cell = stats.get("UNIT_CELL_CONSTANTS", [])
            cell_str = " ".join(map(str, cell)) if isinstance(cell, list) else str(cell)

            isa = stats.get("ISa")
            b_factor = stats.get("B")
            res_cchalf = stats.get("resolution_based_on_cchalf")

            table_total = stats.get("table1_total", [])
            r_obs, i_sigma, completeness = "N/A", "N/A", "N/A"
            # The 'total' line from CORRECT.LP has a specific structure
            if len(table_total) > 8:
                # Indices based on the header in xds_parsers.py, accounting for 'total' label
                completeness = table_total[4]
                r_obs = table_total[5]
                i_sigma = table_total[8]

            overall_html += f"""
            <b>Space Group:</b> {sg}<br>
            <b>Unit Cell:</b> {cell_str}<br>
            <b>ISa:</b> {isa:.2f} &nbsp; <b>Wilson B:</b> {b_factor:.2f}<br>
            <b>R-factor (obs):</b> {r_obs} &nbsp; <b>I/sig(I):</b> {i_sigma}<br>
            <b>Completeness:</b> {completeness}<br>
            <b>Res (CC1/2):</b> {res_cchalf:.2f} Å
            """
        else:
            overall_html += "<i>Not yet available.</i>"

        # --- Combine and display ---
        full_html = f"""
        <div style='color: #E0E0E0; font-family: Consolas, "Courier New", monospace; font-size: 9pt;'>
        {frame_html}
        {overall_html}
        </div>
        """
        gfx.display_plugin_info_text(full_html)
