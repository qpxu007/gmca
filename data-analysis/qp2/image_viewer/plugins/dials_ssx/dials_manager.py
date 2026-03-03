# qp2/image_viewer/plugins/dials/dials_manager.py
import os

import pyqtgraph as pg
from PyQt5 import QtCore, QtWidgets

from qp2.image_viewer.plugins.dials_ssx.dials_settings_dialog import DialsSettingsDialog
from qp2.image_viewer.plugins.dials_ssx.submit_dials_dataset_job import (
    DialsProcessDatasetWorker,
)
from qp2.image_viewer.plugins.generic_plot_manager import GenericPlotManager
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

REDIS_DIALS_KEY_PREFIX = "analysis:out:dials:ssx"


class DialsManager(GenericPlotManager):
    def __init__(self, parent):
        dials_config = {
            "worker_class": DialsProcessDatasetWorker,
            "redis_connection": parent.redis_output_server,
            "redis_key_template": f"{REDIS_DIALS_KEY_PREFIX}:{{master_file}}",
            "spot_field_key": "spots_dials",
            "x_axis_key": "img_num",
            "default_y_axis": "num_spots_dials",
            "refresh_interval_ms": 10000,  # Check less frequently
            "default_source_type": "redis",
            "status_key_type": "string",
        }
        super().__init__(parent=parent, name="DIALS SSX", config=dials_config)
        self.processed_datasets = set()  # Track processed datasets

    def _setup_ui(self):
        super()._setup_ui()
        self.dials_settings_button = QtWidgets.QPushButton("⚙️ Settings")
        self.dials_settings_button.setToolTip("Open DIALS SSX specific settings")
        # self.dials_settings_button.setFixedSize(QtCore.QSize(30, 25))
        actions_button_index = (
            self.container_widget.layout()
            .itemAt(0)
            .layout()
            .indexOf(self.actions_button)
        )
        self.container_widget.layout().itemAt(0).layout().insertWidget(
            actions_button_index, self.dials_settings_button
        )
        self.dials_settings_button.clicked.connect(self._open_dials_settings)

    def _open_dials_settings(self):
        dialog = DialsSettingsDialog(
            current_settings=self.main_window.settings_manager.as_dict(),
            parent=self.main_window,
        )
        dialog.settings_changed.connect(
            self.main_window.settings_manager.update_from_dict
        )
        dialog.show()

    def _prepare_worker_kwargs(self) -> dict:
        settings = self.main_window.settings_manager
        # Prefix settings with 'dials_' to avoid conflicts
        kwargs = {
            key: settings.get(key)
            for key in settings.as_dict()
            if key.startswith("dials_")
        }
        
        # Fallback to common settings
        if not kwargs.get("dials_space_group"):
            kwargs["dials_space_group"] = settings.get("processing_common_space_group", "")
            
        if not kwargs.get("dials_unit_cell"):
            kwargs["dials_unit_cell"] = settings.get("processing_common_unit_cell", "")
            
        if not kwargs.get("dials_model_pdb"):
            kwargs["dials_model_pdb"] = settings.get("processing_common_model_file", "")
            
        if not kwargs.get("dials_reference_reflections"):
            kwargs["dials_reference_reflections"] = settings.get("processing_common_reference_reflection_file", "")
            
        if not kwargs.get("dials_d_min") or kwargs.get("dials_d_min") == 0.5:
            common_high = settings.get("processing_common_res_cutoff_high")
            if common_high is not None:
                kwargs["dials_d_min"] = common_high

        kwargs["processing_common_proc_dir_root"] = settings.get("processing_common_proc_dir_root", "")

        return kwargs

    def update_source(self, new_reader, new_master_file):
        """
        MODIFICATION: Overrides base method to connect to the series_completed signal.
        """
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
            # Manually trigger if the series was already complete
            if self.reader.series_completion_signal_emitted and self.main_window.is_live_mode:
                self.handle_dataset_completed(
                    self.reader.master_file_path,
                    self.reader.total_frames,
                    self.reader.get_parameters(),
                )

    @QtCore.pyqtSlot(str, int, dict)
    def handle_dataset_completed(self, master_file_path, total_frames, metadata):
        """
        MODIFICATION: Launches the single processing job for the whole dataset.
        """
        worker_class = self.config.get("worker_class")
        if not worker_class or not self.run_processing_enabled:
            return

        if master_file_path in self.processed_datasets:
            return
        self.processed_datasets.add(master_file_path)

        worker_kwargs = self._prepare_worker_kwargs()

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

    # Visualization logic remains largely the same
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
            # The _proc_dir entry is not for plotting, so we skip it.
            if item.get(x_key) is None or not isinstance(
                    item.get(selected_metric), (int, float)
            ):
                continue

            point = {"pos": (item[x_key], item[selected_metric]), "data": i}
            if item.get("dials_indexed", False):
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

        # --- MODIFICATION START ---
        # Get the key for the spot data from the config
        spot_key = self.config.get("spot_field_key")

        # Get the raw spot data from the clicked frame's dictionary
        spots_raw = clicked_frame_data.get(spot_key, [])

        if spots_raw:
            # The base GenericPlotManager._parse_spot_data expects a list of [x, y]
            # and converts it to a numpy array of [y, x]. This is exactly what we need.
            spot_coords_yx = self._parse_spot_data(spots_raw)
            if spot_coords_yx is not None:
                gfx.display_spots(spot_coords_yx)
        # --- MODIFICATION END ---

        # Display indexing info if available
        unit_cell = clicked_frame_data.get("unit_cell_dials")
        if isinstance(unit_cell, list) and len(unit_cell) == 6:
            sg = clicked_frame_data.get("space_group_dials", "N/A")
            a, b, c, al, be, ga = unit_cell
            rmsd = clicked_frame_data.get("rmsd")
            rmsd_str = f"<b>RMSD:</b> {rmsd:.3f}<br>" if rmsd is not None else ""
            info_html = f"""
            <div style='color: #FFFFD0; font-size: 9pt;'>
            <b>DIALS Indexing</b><br>
            <b>Space Group:</b> {sg}<br>
            {rmsd_str}
            <b>Cell:</b> {a:.2f}, {b:.2f}, {c:.2f}, {al:.1f}, {be:.1f}, {ga:.1f}
            </div>
            """
            gfx.display_plugin_info_text(info_html)

    @QtCore.pyqtSlot()
    def _rerun_analysis(self, reader=None, master_file=None):
        """
        Overrides the base method to correctly trigger a per-dataset job.
        """
        # Use passed args or fallback to current state
        target_reader = reader if reader else self.reader
        # target_master_file = master_file if master_file else self.current_master_file

        if not target_reader:
            self.status_update.emit(
                f"[{self.name}] Cannot re-run: No data loaded.", 3000
            )
            return

        self.status_update.emit(
            f"[{self.name}] Re-running analysis for {os.path.basename(target_reader.master_file_path)}...",
            3000,
        )

        # We must clear the dataset from our session's "processed" set to allow it to run again.
        if target_reader.master_file_path in self.processed_datasets:
            self.processed_datasets.remove(target_reader.master_file_path)

        # Call the per-dataset handler directly with the current reader's info.
        self.handle_dataset_completed(
            target_reader.master_file_path,
            target_reader.total_frames,
            target_reader.get_parameters(),
        )
