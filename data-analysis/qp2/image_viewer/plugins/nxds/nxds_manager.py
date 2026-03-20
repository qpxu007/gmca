# qp2/image_viewer/plugins/nxds/nxds_manager.py
import json
import os
import redis

import pyqtgraph as pg
from PyQt5 import QtCore, QtWidgets

from qp2.image_viewer.plugins.generic_plot_manager import GenericPlotManager
from qp2.image_viewer.plugins.nxds.nxds_settings_dialog import NXDSSettingsDialog
from qp2.image_viewer.plugins.nxds.submit_nxds_job import NXDSProcessDatasetWorker
from .nxds_analysis_manager import NXDSAnalysisManager
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)
REDIS_NXDS_KEY_PREFIX = "analysis:out:nxds"


class NXDSManager(GenericPlotManager):
    def __init__(self, parent):
        nxds_config = {
            "worker_class": NXDSProcessDatasetWorker,
            "redis_connection": parent.redis_output_server,
            "redis_key_template": f"{REDIS_NXDS_KEY_PREFIX}:{{master_file}}",
            "spot_field_key": "spots_nxds",
            "x_axis_key": "img_num",
            "default_y_axis": "nspots",
            "refresh_interval_ms": 10000,
            "default_source_type": "redis",
            "status_key_type": "string",
        }
        super().__init__(parent=parent, name="nXDS", config=nxds_config)
        self.processed_datasets = set()
        self._analysis_manager = None

    @property
    def analysis_manager(self):
        """Lazy-instantiate and return NXDSAnalysisManager."""
        if self._analysis_manager is None:
            self._analysis_manager = NXDSAnalysisManager(self.main_window)
        return self._analysis_manager

    def _handle_worker_result(self, file_path, status_code, message):
        """Overrides the base method to add the auto-merge check."""
        super()._handle_worker_result(file_path, status_code, message)
        
        # Trigger the new robust auto-merge check in AnalysisManager
        if self.analysis_manager:
            self.analysis_manager.check_auto_merge_conditions()

    def _setup_ui(self):
        super()._setup_ui()
        self.nxds_settings_button = QtWidgets.QPushButton("⚙️ Settings")
        self.nxds_settings_button.setToolTip("Open nXDS specific settings")
        # self.nxds_settings_button.setFixedSize(QtCore.QSize(30, 25))

        self.nxds_results_button = QtWidgets.QPushButton("📊")

        actions_button_index = (
            self.container_widget.layout()
            .itemAt(0)
            .layout()
            .indexOf(self.actions_button)
        )
        self.container_widget.layout().itemAt(0).layout().insertWidget(
            actions_button_index, self.nxds_settings_button
        )

        self.nxds_settings_button.clicked.connect(self._open_nxds_settings)

    def _open_nxds_settings(self):
        dialog = NXDSSettingsDialog(
            current_settings=self.main_window.settings_manager.as_dict(),
            parent=self.main_window,
        )
        dialog.settings_changed.connect(
            self.main_window.settings_manager.update_from_dict
        )
        dialog.show()

    def _prepare_worker_kwargs(self) -> dict:
        settings = self.main_window.settings_manager
        kwargs = {
            key: settings.get(key)
            for key in settings.as_dict()
            if key.startswith("nxds_")
        }

        # Implement fallback to Common Processing Parameters
        if not kwargs.get("nxds_space_group"):
            kwargs["nxds_space_group"] = settings.get("processing_common_space_group", "")
        
        if not kwargs.get("nxds_unit_cell"):
            kwargs["nxds_unit_cell"] = settings.get("processing_common_unit_cell", "")
        
        if not kwargs.get("nxds_pdb_file"):
            kwargs["nxds_pdb_file"] = settings.get("processing_common_model_file", "")
            
        if not kwargs.get("nxds_reference_hkl"):
            kwargs["nxds_reference_hkl"] = settings.get("processing_common_reference_reflection_file", "")

        kwargs["processing_common_proc_dir_root"] = settings.get("processing_common_proc_dir_root", "")

        return kwargs

    def update_source(self, new_reader, new_master_file):
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
            # Only auto-run if we are in live mode. Otherwise wait for manual action.
            if self.reader.series_completion_signal_emitted and self.main_window.is_live_mode:
                self.handle_dataset_completed(
                    self.reader.master_file_path,
                    self.reader.total_frames,
                    self.reader.get_parameters(),
                )

    @QtCore.pyqtSlot(str, int, dict)
    def handle_dataset_completed(self, master_file_path, total_frames, metadata):
        worker_class = self.config.get("worker_class")
        if not worker_class or not self.run_processing_enabled:
            return
        if master_file_path in self.processed_datasets:
            return
        self.processed_datasets.add(master_file_path)

        worker_kwargs = self._prepare_worker_kwargs()

        # Dynamic Batch Optimization:
        # If multiple datasets are loaded, force njobs=1 to prevent cluster node oversubscription.
        # This ensures that we rely on the scheduler (Slurm) for parallelism across datasets,
        # rather than trying to parallelize within each dataset (which requires >1 node per job).
        all_datasets = self.main_window.dataset_manager.get_all_datasets()
        if len(all_datasets) >= 20:
            logger.info(
                f"Batch mode detected ({len(all_datasets)} datasets). Forcing nxds_njobs=1 for '{os.path.basename(master_file_path)}' to optimize cluster throughput."
            )
            worker_kwargs["nxds_njobs"] = 1

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
        worker.signals.result.connect(
            lambda status, msg, path: self._handle_worker_result(path, status, msg)
        )
        self.request_main_threadpool.emit(worker)

    @QtCore.pyqtSlot()
    def _clear_redis_results(self):
        """
        Overrides the base method to also clear physical processing directory
        and reset the internal processed_datasets set for the current file.
        """
        if (
            self._current_data_source_type != "redis"
            or not self.redis_connection
            or not self.current_master_file
        ):
            return

        # 1. Get the physical directory to delete from Redis
        results_key = self._current_data_source_path_or_key
        proc_dir = self.redis_connection.hget(results_key, "_proc_dir")

        # 2. Call the base class method to delete all standard Redis keys
        super()._clear_redis_results()

        # 3. Delete the physical directory if it exists
        if proc_dir and os.path.isdir(proc_dir):
            try:
                import shutil

                shutil.rmtree(proc_dir)
                logger.info(f"[{self.name}] Deleted physical directory: {proc_dir}")
            except Exception as e:
                logger.error(
                    f"[{self.name}] Failed to delete directory {proc_dir}: {e}"
                )

        # 4. Crucially, remove the current dataset from the processed set
        if self.current_master_file in self.processed_datasets:
            self.processed_datasets.remove(self.current_master_file)

    @QtCore.pyqtSlot()
    def _rerun_analysis(self, reader=None, master_file=None):
        """
        Triggers a new analysis run.
        Can be passed explicit reader/file to support batch processing where self.reader might change.
        """
        # Use passed args or fall back to current state
        active_reader = reader if reader else self.reader
        active_master_file = master_file if master_file else (active_reader.master_file_path if active_reader else None)

        if not active_reader:
            logger.warning(f"[{self.name}] Re-run requested but no reader loaded/provided.")
            return
            
        logger.info(f"[{self.name}] Re-running analysis for {os.path.basename(active_master_file)}...")
        self.status_update.emit(f"[{self.name}] Re-running analysis...", 3000)
        
        if active_master_file in self.processed_datasets:
            self.processed_datasets.remove(active_master_file)
            
        self.handle_dataset_completed(
            active_master_file,
            active_reader.total_frames,
            active_reader.get_parameters(),
        )

    def _fetch_and_prepare_data(self) -> bool:
        if (
            self._current_data_source_type != "redis"
            or not self.redis_connection
            or not self.current_master_file
        ):
            return False

        key = self._current_data_source_path_or_key
        try:
            json_path = self.redis_connection.hget(key, "_results_json_path")
            if not json_path or not os.path.exists(json_path):
                return False

            with open(json_path, "r") as f:
                data_from_json_dict = json.load(f)
                new_data = []
                # Convert dict {"1":{}, "2":{}} to list [{}, {}]
                for frame_num_str, frame_data in data_from_json_dict.items():
                    try:
                        frame_data["img_num"] = int(frame_num_str)
                        new_data.append(frame_data)
                    except (ValueError, TypeError):
                        continue
                new_data.sort(key=lambda x: x["img_num"])

            if new_data != self.plot_data:
                self.plot_data = new_data
                self._update_available_metrics()
                return True
        except Exception as e:
            logger.error(
                f"[{self.name}] Error fetching nXDS results from file: {e}",
                exc_info=True,
            )
        return False

    def _update_plot_display(self):
        selected_metric = self.metric_combobox.currentText()
        if (
            not self.plot_data
            or not selected_metric
            or selected_metric == "No numeric data"
        ):
            if self.plot_data_item:
                self.plot_widget.removeItem(self.plot_data_item)
                self.plot_data_item = None
            return

        indexed_points, unindexed_points = [], []
        x_key = self.config.get("x_axis_key", "img_num")
        for i, item in enumerate(self.plot_data):
            if item.get(x_key) is not None and isinstance(
                item.get(selected_metric), (int, float)
            ):
                point = {"pos": (item[x_key], item[selected_metric]), "data": i}
                if item.get("accepted", False):
                    indexed_points.append(point)
                else:
                    unindexed_points.append(point)

        if not isinstance(self.plot_data_item, pg.ScatterPlotItem):
            if self.plot_data_item:
                self.plot_widget.removeItem(self.plot_data_item)
            self.plot_data_item = pg.ScatterPlotItem(
                hoverable=False,
            )

            self.plot_widget.addItem(self.plot_data_item)
            self.plot_data_item.sigClicked.connect(self._on_plot_point_clicked)
        else:
            self.plot_data_item.clear()

        self.plot_data_item.addPoints(
            unindexed_points, symbol="o", size=8, brush=pg.mkBrush(255, 165, 0, 200)
        )  # Orange
        self.plot_data_item.addPoints(
            indexed_points,
            symbol="o",
            size=10,
            brush=pg.mkBrush(0, 255, 255, 255),
            pen=pg.mkPen("w"),
        )  # Cyan

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
        if points is None:
            return
        point_index = points[0].data()
        clicked_frame_data = self.plot_data[point_index]
        frame_index_0_based = clicked_frame_data.get("img_num", 1) - 1
        self.frame_selected.emit(frame_index_0_based)

        gfx = self.main_window.graphics_manager
        gfx.clear_spots()
        gfx.clear_indexed_reflections()
        gfx.clear_plugin_info_text()

        spots_raw = clicked_frame_data.get(self.config["spot_field_key"], [])
        if spots_raw:
            # spots_raw is [[x, y, is_indexed_flag], ...]
            spot_coords_yx = self._parse_spot_data([s[:2] for s in spots_raw])
            if spot_coords_yx is not None:
                gfx.display_spots(spot_coords_yx)

        reflections_raw = clicked_frame_data.get("reflections_nxds", [])
        if reflections_raw:
            # reflections_raw is [[h, k, l, x, y], ...]
            parsed_reflections = [
                {"h": r[0], "k": r[1], "l": r[2], "x": r[3], "y": r[4]}
                for r in reflections_raw
            ]
            gfx.display_indexed_reflections(parsed_reflections)

        # --- MODIFIED: Info box logic with fallback ---
        info_html = ""
        # Prioritize the final, refined unit cell
        unit_cell = clicked_frame_data.get("unit_cell_parameters")
        if unit_cell and isinstance(unit_cell, list) and len(unit_cell) == 6:
            sg = clicked_frame_data.get("space_group_nxds", "N/A")
            a, b, c, al, be, ga = unit_cell
            info_html = f"""
            <div style='color: #FFFFD0; font-size: 9pt;'>
            <b>nXDS Indexing (Accepted)</b><br>
            <b>Space Group:</b> {sg}<br>
            <b>Refined Cell:</b> {a:.2f}, {b:.2f}, {c:.2f}<br>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            {al:.1f}, {be:.1f}, {ga:.1f}
            </div>
            """
        # Fallback to the reduced cell if the refined one isn't present
        elif clicked_frame_data.get("reduced_cell"):
            reduced_cell_str = clicked_frame_data.get("reduced_cell")
            try:
                # The reduced_cell is a string, so we parse it
                rc_params = [float(p) for p in reduced_cell_str.split()]
                if len(rc_params) == 6:
                    a, b, c, al, be, ga = rc_params
                    status = (
                        "Accepted" if clicked_frame_data.get("accepted") else "Rejected"
                    )
                    info_html = f"""
                    <div style='color: #FFFFD0; font-size: 9pt;'>
                    <b>nXDS Indexing ({status})</b><br>
                    <b>Reduced Cell:</b> {a:.2f}, {b:.2f}, {c:.2f}<br>
                    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                    {al:.1f}, {be:.1f}, {ga:.1f}
                    </div>
                    """
            except (ValueError, TypeError):
                pass  # Could not parse reduced_cell string

        if info_html:
            gfx.display_plugin_info_text(info_html)
