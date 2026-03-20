# Create new file: qp2/image_viewer/volume_map/volume_manager.py

import os
import re

from PyQt5.QtCore import QObject, pyqtSlot
from PyQt5.QtWidgets import QInputDialog

from qp2.image_viewer.volume_map.orthogonal_view_dialog import OrthogonalViewDialog
from qp2.image_viewer.volume_map.volume_3d_dialog import Volume3dDialog
from qp2.image_viewer.volume_map.volume_data_worker import VolumeDataWorker
from qp2.image_viewer.volume_map.volume_dialog import VolumeDialog
from qp2.image_viewer.volume_map.volume_utils import find_3d_hotspots
from qp2.log.logging_config import get_logger
from qp2.xio.hdf5_manager import HDF5Reader

logger = get_logger(__name__)


class VolumeManager(QObject):
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.active_dialog = None
        self.active_3d_dialog = None
        self.ortho_view_dialog = None
        self.xy_datasets = []
        self.xz_datasets = []
        self.current_volume_data = None
        self.raw_data_xy = {}
        self.raw_data_xz = {}
        self.was_live_mode = False
        self.last_used_shift = 0.0
        self.scan_mode = "row_wise"

    @staticmethod
    def _detect_scan_pattern(datasets) -> str:
        """Auto-detect scan index pattern (_R or _C) from filenames."""
        for candidate in [r"_R(\d+)", r"_C(\d+)"]:
            if any(re.search(candidate, r.master_file_path, re.IGNORECASE)
                   for r in datasets):
                return candidate
        return r"_R(\d+)"  # fallback

    @pyqtSlot(str, str)
    def launch_viewer(self, run_xy_prefix: str, run_xz_prefix: str):
        mw = self.main_window
        if mw.is_live_mode:
            self.was_live_mode = True
            mw.is_live_mode = False
            mw.ui_manager.show_status_message("Live mode paused for 3D analysis.", 3000)

        self.xy_datasets = mw.dataset_manager.get_datasets_for_run(run_xy_prefix)
        self.xz_datasets = mw.dataset_manager.get_datasets_for_run(run_xz_prefix)

        if not self.xy_datasets or not self.xz_datasets:
            mw.ui_manager.show_warning_message(
                "Error", "One or both selected runs have no datasets."
            )
            return
        
        self.scan_mode = mw.settings_manager.get("scan_mode", "row_wise")

        # --- START: MODIFIED SHIFT LOGIC ---
        n_frames_xy = self.xy_datasets[0].total_frames
        n_frames_xz = self.xz_datasets[0].total_frames
        shift = 0.0

        if n_frames_xy != n_frames_xz:
            # Only prompt if the number of frames/columns is different
            shift_val, ok = QInputDialog.getDouble(
                mw,
                "Enter Scan Shift",
                "The number of frames in the scans do not match.\n"
                f"({n_frames_xy} vs {n_frames_xz}).\n\n"
                "Enter the shift of the XZ scan relative to the XY scan:",
                value=0.0,
                decimals=3,
            )
            if not ok:
                return  # User cancelled
            shift = shift_val
        else:
            mw.ui_manager.show_status_message(
                "Scan dimensions match. Using shift=0.", 2000
            )

        self.last_used_shift = shift
        # --- END: MODIFIED SHIFT LOGIC ---

        common_prefix = os.path.commonprefix([run_xy_prefix, run_xz_prefix]).rsplit(
            "_", 1
        )[0]
        title = f"3D Volume: {common_prefix}" if common_prefix else "3D Volume"

        self.active_dialog = VolumeDialog(title, parent=mw)
        self.active_dialog.peak_selected.connect(self._on_peak_selected)
        self.active_dialog.find_hotspots_requested.connect(
            self._on_find_3d_hotspots_requested
        )
        self.active_dialog.refresh_requested.connect(self._on_refresh_requested)
        self.active_dialog.show_3d_requested.connect(self._on_show_3d_requested)
        self.active_dialog.show()
        self.active_dialog.finished.connect(self._on_dialog_closed)

        mw.ui_manager.show_status_message(
            f"Constructing 3D volume for '{common_prefix}'...", 0
        )

        worker = VolumeDataWorker(
            run_xy=self.xy_datasets,
            run_xz=self.xz_datasets,
            shift=shift,  # Pass the shift to the worker
            redis_conn=mw.redis_output_server,
            plugin_config=mw.analysis_plugin_manager.active_plugin.config,
            scan_mode=self.scan_mode,
        )
        worker.signals.finished.connect(self._on_data_fetched)
        worker.signals.error.connect(self._on_fetch_error)
        worker.signals.progress.connect(
            lambda msg: mw.ui_manager.show_status_message(msg, 0)
        )
        mw.threadpool.start(worker)

    @pyqtSlot(dict)
    def _on_data_fetched(self, result: dict):
        self.main_window.ui_manager.clear_status_message_if("Constructing 3D volume")

        self.current_volume_data = result.get("volume")
        self.raw_data_xy = result.get("raw_data_xy", {})
        self.raw_data_xz = result.get("raw_data_xz", {})
        metric_name = result.get("metric")

        if self.active_dialog:
            self.active_dialog.setWindowTitle(f"3D Volume: {metric_name}")

            self.main_window.ui_manager.show_status_message(
                "Finding initial 3D hotspots...", 0
            )
            initial_peaks = find_3d_hotspots(
                self.current_volume_data,
                percentile_threshold=98,
                min_size=5,
            )
            self.main_window.ui_manager.show_status_message(
                f"Found {len(initial_peaks)} initial hotspots.", 3000
            )

            self.active_dialog.update_data(self.current_volume_data, initial_peaks)

    @pyqtSlot(str)
    def _on_fetch_error(self, error_msg: str):
        self.main_window.ui_manager.show_critical_message(
            "3D Construction Error", error_msg
        )
        if self.active_dialog:
            self.active_dialog.close()

    def _on_dialog_closed(self):
        if self.was_live_mode:
            self.main_window.is_live_mode = True
            self.main_window.ui_manager.show_status_message("Live mode resumed.", 3000)
            self.was_live_mode = False  # Reset the flag

        self.active_dialog = None
        self.current_volume_data = None
        self.xy_datasets = []
        self.xz_datasets = []
        self.raw_data_xy = {}  # <-- Clear on close
        self.raw_data_xz = {}  # <-- Clear on close
        if self.ortho_view_dialog:
            self.ortho_view_dialog.close()
            self.ortho_view_dialog = None
        if self.active_3d_dialog:
            self.active_3d_dialog.close()
            self.active_3d_dialog = None

    @pyqtSlot(float, int)
    def _on_find_3d_hotspots_requested(self, percentile: float, min_size: int):
        """Re-runs the hotspot analysis on the already-loaded volume data."""
        if not self.active_dialog or self.current_volume_data is None:
            return

        self.main_window.ui_manager.show_status_message(
            "Re-calculating 3D hotspots...", 0
        )

        peaks = find_3d_hotspots(
            self.current_volume_data, percentile_threshold=percentile, min_size=min_size
        )

        self.main_window.ui_manager.show_status_message(
            f"Found {len(peaks)} 3D hotspots.", 3000
        )

        if self.active_dialog:
            self.active_dialog.update_peak_list(peaks)

        if self.active_3d_dialog:
            self.active_3d_dialog.add_hotspots(peaks, self.current_volume_data.shape)

    @pyqtSlot()
    def _on_show_3d_requested(self):
        """Creates and shows the 3D visualization dialog."""
        if self.current_volume_data is None or self.active_dialog is None:
            self.main_window.ui_manager.show_warning_message(
                "3D View Error", "No volume data is available to display."
            )
            return

        current_peaks = self.active_dialog.peaks_data
        volume_shape = self.current_volume_data.shape

        # Reuse or create 3D dialog
        if self.active_3d_dialog:
            try:
                # Reuse existing dialog
                # Check if C++ object is valid
                if self.active_3d_dialog.isVisible() or True: 
                    # accessing attribute will fail if deleted
                    _ = self.active_3d_dialog.windowTitle() 
                    
                    self.active_3d_dialog.add_hotspots(current_peaks, volume_shape)
                    self.active_3d_dialog.show()
                    self.active_3d_dialog.raise_()
                    self.active_3d_dialog.activateWindow()
                    return
            except RuntimeError:
                # Dialog might have been deleted externally
                self.active_3d_dialog = None

        # Create new dialog if reuse failed or didn't exist
        self.active_3d_dialog = Volume3dDialog(parent=self.main_window)
        # Note: we are keeping the reference, so we don't clear it on finished to allow reuse
        
        self.active_3d_dialog.add_hotspots(current_peaks, volume_shape)
        self.active_3d_dialog.show()

    @pyqtSlot(dict)
    def _on_peak_selected(self, peak_info: dict):
        """
        Receives peak info, finds the readers, and opens the remote control dialog.
        """
        coords = peak_info.get("coords")
        if not coords:
            return
        reconstructed_x, y_coord, z_coord = coords
        shift = self.last_used_shift

        n_frames_xy = self.xy_datasets[0].total_frames if self.xy_datasets else 0
        n_frames_xz = self.xz_datasets[0].total_frames if self.xz_datasets else 0

        # --- START: FINAL CORRECTED SHIFT LOGIC ---
        # The reconstructed_x coordinate is in the coordinate system of the NARROWER scan.
        # To find the coordinate in the WIDER scan, we must apply the shift.
        if (
                n_frames_xy <= n_frames_xz
        ):  # XY is narrower or equal (reference), XZ is wider.
            x_in_xy_scan = reconstructed_x
            x_in_xz_scan = int(round(reconstructed_x - shift))
        else:  # XZ is narrower (reference), XY is wider.
            x_in_xz_scan = reconstructed_x
            x_in_xy_scan = int(round(reconstructed_x + shift))
        # --- END: FINAL CORRECTED SHIFT LOGIC ---

        # --- START: NEW SERPENTINE CORRECTION ---
        is_serpentine = "serpentine" in self.scan_mode

        original_frame_xy = x_in_xy_scan
        if is_serpentine and y_coord % 2 == 1:
            original_frame_xy = (n_frames_xy - 1) - x_in_xy_scan

        original_frame_xz = x_in_xz_scan
        if is_serpentine and z_coord % 2 == 1:
            original_frame_xz = (n_frames_xz - 1) - x_in_xz_scan
        # --- END: NEW SERPENTINE CORRECTION ---

        # --- START: NEW SCAN MODE READER FINDING ---
        is_column_scan = "column" in self.scan_mode
        scan_idx_pattern = self._detect_scan_pattern(self.xy_datasets + self.xz_datasets)

        # Find the reader for the Y coordinate (XY scan)
        reader_xy = next(
            (
                r
                for r in self.xy_datasets
                if (
                    int(re.search(scan_idx_pattern, r.master_file_path, re.IGNORECASE).group(1)) - 1
                   ) == y_coord
            ),
            None,
        )
        # --- END: NEW SCAN MODE READER FINDING ---

        # Validate the calculated frame index for the XY scan
        if reader_xy and not (0 <= original_frame_xy < reader_xy.total_frames):
            logger.warning(
                f"Calculated XY frame {original_frame_xy} is out of bounds for reader with {reader_xy.total_frames} frames. Disabling view."
            )
            reader_xy = None

        # Find the reader for the Z coordinate (XZ scan)
        reader_xz = next(
            (
                r
                for r in self.xz_datasets
                if (
                    int(re.search(scan_idx_pattern, r.master_file_path, re.IGNORECASE).group(1)) - 1
                   ) == z_coord
            ),
            None,
        )

        # Validate the calculated frame index for the XZ scan
        if reader_xz and not (0 <= original_frame_xz < reader_xz.total_frames):
            logger.warning(
                f"Calculated XZ frame {original_frame_xz} is out of bounds for reader with {reader_xz.total_frames} frames. Disabling view."
            )
            reader_xz = None

        if not self.ortho_view_dialog:
            self.ortho_view_dialog = OrthogonalViewDialog(parent=self.main_window)
            self.ortho_view_dialog.view_image_requested.connect(
                self._on_view_image_requested
            )

        self.ortho_view_dialog.set_data_sources(
            reader_xy, reader_xz, original_frame_xy, original_frame_xz
        )
        self.ortho_view_dialog.setWindowTitle(
            f"Image Selector for Peak at (X,Y,Z): ({reconstructed_x + 1}, {y_coord + 1}, {z_coord + 1})"
        )

        self.ortho_view_dialog.show()
        self.ortho_view_dialog.raise_()
        self.ortho_view_dialog.activateWindow()

    @pyqtSlot(HDF5Reader, int)
    def _on_view_image_requested(self, reader: HDF5Reader, frame_index: int):
        mw = self.main_window
        mw.ui_manager.select_dataset_in_tree(reader.master_file_path)
        plugin_cfg = mw.analysis_plugin_manager.active_plugin.config
        spot_key = plugin_cfg.get("spot_field_key")
        refl_key = "reflections_crystfel"  # Crystfel specific

        overlays = {"spots": None, "reflections": []}

        is_xy_reader = any(
            r.master_file_path == reader.master_file_path for r in self.xy_datasets
        )
        data_map_to_use = self.raw_data_xy if is_xy_reader else self.raw_data_xz

        # The raw_data_map is keyed by (scan_idx, original_frame_idx).
        is_column_scan = "column" in self.scan_mode
        all_datasets = self.xy_datasets + self.xz_datasets
        scan_idx_pattern = self._detect_scan_pattern(all_datasets)

        match = re.search(scan_idx_pattern, reader.master_file_path, re.IGNORECASE)
        if match:
            scan_idx = int(match.group(1)) - 1
            frame_data = data_map_to_use.get((scan_idx, frame_index), {})

            spots_raw = frame_data.get(spot_key, [])
            overlays["spots"] = mw.analysis_plugin_manager.active_plugin._parse_spot_data(spots_raw)

            refls_raw = frame_data.get(refl_key, [])
            if refls_raw:
                try:
                    for r in refls_raw:
                        overlays["reflections"].append(
                            {
                                "h": int(r[0]),
                                "k": int(r[1]),
                                "l": int(r[2]),
                                "x": float(r[7]),
                                "y": float(r[8]),
                            }
                        )
                except (ValueError, IndexError):
                    pass

            unit_cell = frame_data.get("unit_cell_crystfel")
            if isinstance(unit_cell, list) and len(unit_cell) == 6:
                overlays["indexing_info"] = {
                    "unit_cell": unit_cell,
                    "indexer": frame_data.get("crystfel_indexed_by", "N/A"),
                    "lattice_type": frame_data.get("crystfel_lattice", "N/A"),
                    "centering": frame_data.get("crystfel_centering", "N/A"),
                }

        mw.display_image_with_overlays(reader, frame_index, overlays)

    @pyqtSlot()
    def _on_refresh_requested(self):
        """Re-runs the data fetching worker for both orthogonal scans."""
        if not self.active_dialog or not self.xy_datasets or not self.xz_datasets:
            return

        mw = self.main_window
        self.scan_mode = mw.settings_manager.get("scan_mode", "row_wise")

        # --- Apply same skip-dialog logic on refresh ---
        n_frames_xy = self.xy_datasets[0].total_frames
        n_frames_xz = self.xz_datasets[0].total_frames
        shift = 0.0

        if n_frames_xy != n_frames_xz:
            shift_val, ok = QInputDialog.getDouble(
                mw,
                "Enter Scan Shift",
                "The number of frames in the scans do not match.\n"
                f"({n_frames_xy} vs {n_frames_xz}).\n\n"
                "Enter the shift of the XZ scan relative to the XY scan:",
                value=self.last_used_shift,  # Default to the last used shift
                decimals=3,
            )
            if not ok:
                return
            shift = shift_val

        self.last_used_shift = shift
        # --- End logic ---

        title = self.active_dialog.windowTitle()
        mw.ui_manager.show_status_message(
            f"Refreshing 3D volume data for '{title}'...", 0
        )

        worker = VolumeDataWorker(
            run_xy=self.xy_datasets,
            run_xz=self.xz_datasets,
            shift=shift,  # Pass the new shift value
            redis_conn=mw.redis_output_server,
            plugin_config=mw.analysis_plugin_manager.active_plugin.config,
            scan_mode=self.scan_mode,
        )
        worker.signals.finished.connect(self._on_data_fetched)
        worker.signals.error.connect(self._on_fetch_error)
        worker.signals.progress.connect(
            lambda msg: mw.ui_manager.show_status_message(msg, 0)
        )
        mw.threadpool.start(worker)
