# qp2/image_viewer/strategy/strategy_results_dialog.py

from pyqtgraph.Qt import QtCore, QtGui, QtWidgets
import numpy as np
import requests

from qp2.data_viewer.utils import get_rpc_url, send_strategy_to_redis
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class StrategyResultsDialog(QtWidgets.QDialog):
    """A dialog to display strategy results and control overlays."""

    # Signals to control overlays in the main window's GraphicsManager
    request_show_spots = QtCore.pyqtSignal(object)
    request_hide_spots = QtCore.pyqtSignal()
    request_show_reflections = QtCore.pyqtSignal(object)
    request_hide_reflections = QtCore.pyqtSignal()
    request_frame_display = QtCore.pyqtSignal(str, int)  # master_path, frame_index

    def __init__(self, result_data, program, mapping, parent=None):
        super().__init__(parent)
        self.result_data = result_data
        self.program = program
        self.mapping = mapping
        self.spots_data = None
        self.reflections_data = None
        self.main_window = parent  # Reference to DiffractionViewerWindow

        self.setWindowTitle(f"Strategy Results ({program.upper()})")
        self.setMinimumSize(600, 700)

        layout = QtWidgets.QVBoxLayout(self)

        # Main summary text
        self.summary_text_edit = QtWidgets.QTextEdit()
        self.summary_text_edit.setReadOnly(True)
        self.summary_text_edit.setFont(QtGui.QFont("Monospace", 9))
        layout.addWidget(self.summary_text_edit)

        # Collapsible GroupBox for Indexing Table
        self.index_table_group = QtWidgets.QGroupBox("Indexing Table (Click to Show)")
        self.index_table_group.setCheckable(True)
        self.index_table_group.setChecked(False)
        index_layout = QtWidgets.QVBoxLayout(self.index_table_group)
        index_layout.setContentsMargins(5, 5, 5, 5)
        self.index_table_text_edit = QtWidgets.QTextEdit()
        self.index_table_text_edit.setReadOnly(True)
        self.index_table_text_edit.setFont(QtGui.QFont("Monospace", 9))
        index_layout.addWidget(self.index_table_text_edit)
        layout.addWidget(self.index_table_group)

        self.index_table_text_edit.setVisible(False)

        # --- Connect the toggled signal to show/hide the content ---
        self.index_table_group.toggled.connect(self.index_table_text_edit.setVisible)
        self.index_table_group.toggled.connect(self._update_group_title)

        # Overlay Controls Group
        overlay_group = QtWidgets.QGroupBox("Overlay Controls")
        overlay_layout = QtWidgets.QVBoxLayout(overlay_group)
        self.dataset_selector = QtWidgets.QComboBox()
        self.dataset_selector.addItems(self.mapping.keys())
        self.dataset_selector.setVisible(len(self.mapping) > 1)
        overlay_layout.addWidget(self.dataset_selector)
        button_layout = QtWidgets.QHBoxLayout()
        self.spots_button = QtWidgets.QPushButton("Toggle Spots Overlay")
        self.spots_button.setCheckable(True)
        self.reflections_button = QtWidgets.QPushButton("Toggle Reflections Overlay")
        self.reflections_button.setCheckable(True)
        button_layout.addWidget(self.spots_button)
        button_layout.addWidget(self.reflections_button)
        overlay_layout.addLayout(button_layout)

        action_button_layout = QtWidgets.QHBoxLayout()
        self.export_button = QtWidgets.QPushButton("Export Strategy to Blu-Ice")
        action_button_layout.addWidget(self.export_button)
        overlay_layout.addLayout(action_button_layout)
        layout.addWidget(overlay_group)

        # --- Disable export button if strategy failed ---
        strategy_succeeded = False
        if self.program == "xds" and self.result_data.get("xplan"):
            strategy_succeeded = True
        elif self.program == "mosflm" and self.result_data.get("final"):
            strategy_succeeded = True
        elif self.program == "crystfel" and self.result_data.get("crystfel"):
            strategy_succeeded = True

        if self.program == "crystfel":
             self.export_button.setEnabled(False)
             self.export_button.setToolTip("Export to Blu-Ice is not applicable for CrystFEL strategy.")
        elif not strategy_succeeded:
            self.export_button.setEnabled(False)
            self.export_button.setToolTip(
                "Strategy calculation did not produce a valid result."
            )

        self._format_results_text()
        self._update_overlay_data()

        # Connect signals
        self.dataset_selector.currentIndexChanged.connect(self._on_dataset_selected)
        self.spots_button.toggled.connect(self._toggle_spots_display)
        self.reflections_button.toggled.connect(self._toggle_reflections_display)
        self.export_button.clicked.connect(self.on_export_strategy)

    def _update_group_title(self, checked):
        """Update the group box title to guide the user."""
        if checked:
            self.index_table_group.setTitle("Indexing Table (Click to Hide)")
        else:
            self.index_table_group.setTitle("Indexing Table (Click to Show)")

    def _on_dataset_selected(self):
        """Called when the user selects a different dataset from the dropdown."""
        self.spots_button.setChecked(False)
        self.reflections_button.setChecked(False)
        self._update_overlay_data()
        selected_master = self.dataset_selector.currentText()
        frame_num_one_based = self.mapping[selected_master][0]
        self.request_frame_display.emit(selected_master, frame_num_one_based - 1)

    def _format_results_text(self):
        """Creates a formatted string from the strategy results dictionary."""
        summary_text = f"--- Strategy Summary ({self.program.upper()}) ---\n"
        index_table_text = "Not available."

        if self.program == "xds":
            idxref = self.result_data.get("idxref", {})
            xplan = self.result_data.get("xplan", {})
            matthews = self.result_data.get("matthews", {})
            start = xplan.get("xplan_starting_angle")
            rot = xplan.get("xplan_total_rotation")

            osc_end = None
            if start and rot:
                osc_end = start + rot

            # Format unit cell list into a string
            uc_list = idxref.get("auto_index_unitcell", [])
            unitcell_str = " ".join(map(str, uc_list)) if uc_list else "N/A"

            summary_text += (
                f"Space Group: {idxref.get('auto_index_spacegroup', 'N/A')}\n"
            )
            summary_text += f"Unit Cell: {unitcell_str}\n"
            summary_text += f"Mosaicity: {idxref.get('mosaicity', 'N/A')} deg\n"
            summary_text += (
                f"Completeness: {xplan.get('xplan_completeness', 'N/A')} %\n"
            )
            summary_text += (
                f"Oscillation Start: {xplan.get('xplan_starting_angle', 'N/A')} deg\n"
            )
            summary_text += f"Oscillation End: {osc_end if osc_end else 'N/A'} deg\n"
            summary_text += (
                f"Oscillation Delta: {idxref.get('max_osc_range', 'N/A')} deg\n"
            )
            summary_text += f"Detector Distance: {self.result_data.get('detectordistance', 'N/A')} mm\n"
            summary_text += (
                f"Screen Score: {self.result_data.get('screen_score', 'N/A')}\n"
            )
            summary_text += f"Resolution (from spots): {self.result_data.get('spot_res', 'N/A')} Å\n"
            summary_text += (
                f"Number of Spots: {self.result_data.get('n_spots', 'N/A')}\n"
            )
            summary_text += f"Solvent Content: {matthews.get('solvent', 'N/A')} %\n"
            summary_text += (
                f"ASU Content (Residues): {matthews.get('asu_content', 'N/A')}\n"
            )

            table_rows = idxref.get("index_table_candidates")
            if table_rows and isinstance(table_rows, list):
                index_table_text = "\n".join(
                    " ".join(map(str, row)) for row in table_rows
                )

        elif self.program == "mosflm":
            final = self.result_data.get("final", {})
            matthews = self.result_data.get("matthews", {})
            spot_stats = self.result_data.get("spot", {})

            summary_text += f"Space Group: {final.get('spacegroup', 'N/A')}\n"
            summary_text += f"Unit Cell: {final.get('unitcell', 'N/A')}\n"
            summary_text += f"Mosaicity: {final.get('mosaic', 'N/A')} deg\n"
            summary_text += (
                f"Native Completeness: {final.get('nativeCompleteness', 'N/A')} %\n"
            )
            summary_text += f"Anomalous Completeness: {final.get('anomalousCompletenes', 'N/A')} %\n"
            summary_text += f"Oscillation Start: {final.get('startAngle', 'N/A')} deg\n"
            summary_text += f"Oscillation End: {final.get('endAngle', 'N/A')} deg\n"
            summary_text += f"Oscillation Delta: {final.get('osc', 'N/A')} deg\n"
            summary_text += f"Detector Distance: {final.get('distance', 'N/A')} mm\n"
            summary_text += f"Screen Score: {final.get('score', 'N/A')}\n"
            summary_text += f"Resolution (from spots): {spot_stats.get('resolution_from_spots', 'N/A')} Å\n"
            summary_text += f"Number of Spots: {spot_stats.get('n_spots', 'N/A')}\n"
            summary_text += f"Solvent Content: {matthews.get('solvent', 'N/A')} %\n"
            summary_text += (
                f"ASU Content (Residues): {matthews.get('asu_content', 'N/A')}\n"
            )

            index_table_text = self.result_data.get("autoindex", {}).get(
                "index_table", "Not available."
            )
        
        elif self.program == "crystfel":
            cf_data = self.result_data.get("crystfel", {})
            summary_text += f"Indexed By: {cf_data.get('indexed_by', 'N/A')}\n"
            summary_text += f"Lattice Type: {cf_data.get('lattice_type', 'N/A')}\n"
            summary_text += f"Centering: {cf_data.get('centering', 'N/A')}\n"
            
            uc = cf_data.get("unit_cell")
            if uc:
                 # uc is likely a list [a, b, c, al, be, ga]
                 uc_str = ", ".join([f"{x:.2f}" for x in uc])
                 summary_text += f"Unit Cell: {uc_str}\n"
            else:
                 summary_text += "Unit Cell: N/A\n"
            
            summary_text += f"Number of Spots (Peak Search): {cf_data.get('num_spots', 'N/A')}\n"
            summary_text += f"Number of Indexed Reflections: {cf_data.get('num_reflections', 'N/A')}\n"

        self.summary_text_edit.setText(summary_text)
        self.index_table_text_edit.setText(index_table_text)

    def on_export_strategy(self):
        """Handles the logic for exporting a strategy when the button is clicked."""
        if not self.main_window:
            QtWidgets.QMessageBox.critical(
                self, "Error", "Cannot access main application window."
            )
            return

        # 1. Gather all required options from the results data
        user = self.main_window.settings_manager.get("username", "unknown")
        beamline = self.main_window.params.get("beamline", "unknown")

        osc_start, osc_end, osc_delta, distance = None, None, None, None

        if self.program == "xds":
            idxref = self.result_data.get("idxref", {})
            xplan = self.result_data.get("xplan", {})
            osc_start = xplan.get("xplan_starting_angle")
            total_rot = xplan.get("xplan_total_rotation")
            if osc_start is not None and total_rot is not None:
                osc_end = float(osc_start) + float(total_rot)
            osc_delta = idxref.get("max_osc_range")
            distance = self.result_data.get("detectordistance")
        elif self.program == "mosflm":
            final = self.result_data.get("final", {})
            osc_start = final.get("startAngle")
            osc_end = final.get("endAngle")
            osc_delta = final.get("osc")
            distance = final.get("distance")

        prefix = self._get_common_prefix()
        opt = {
            "id": prefix,
            "pipeline": f"strategy_{self.program}",
            "username": user,
            "beamline": beamline,
            "osc_start": osc_start,
            "osc_end": osc_end,
            "osc_delta": osc_delta,
            "distance": distance,
        }

        # 2. Send strategy to Redis
        if not send_strategy_to_redis(beamline, opt):
            QtWidgets.QMessageBox.warning(
                self, "Redis Error", "Failed to send strategy data to Redis."
            )

        # 3. Get the RPC URL from the database via helper
        rpc_url = get_rpc_url()
        if not rpc_url:
            QtWidgets.QMessageBox.critical(
                self, "DB Error", "Failed to retrieve RPC URL from database."
            )
            return

        # 4. Construct POST data and send the request
        post_data = {
            "module": "run_create",
            "frame_deg_start": opt["osc_start"],
            "frame_deg_end": opt["osc_end"],
            "delta_deg": opt["osc_delta"],
            "det_z_mm": opt["distance"],
            "atten_factors": "",
            "mode": "",
            "expTime_sec": "",
            "energy1_keV": "",
        }

        # Filter out any None values before sending
        post_data = {k: v for k, v in post_data.items() if v is not None}

        try:
            self.main_window.ui_manager.show_status_message("Sending export request...")
            resp = requests.post(rpc_url, data=post_data, timeout=10)
            resp.raise_for_status()

            QtWidgets.QMessageBox.information(
                self,
                "Export Successful",
                f"Request sent to Blu-Ice successfully.\n\nResponse:\n{resp.content.decode('utf-8')}",
            )
        except requests.exceptions.RequestException as e:
            QtWidgets.QMessageBox.critical(
                self, "Request Error", f"Failed to post request to {rpc_url}:\n{e}"
            )
        finally:
            self.main_window.ui_manager.clear_status_message_if("Sending")

    def _update_overlay_data(self):
        """Extracts spot and reflection data for the currently selected dataset."""
        # Reset internal data and button states
        self.spots_data = None
        self.reflections_data = None
        self.spots_button.setEnabled(False)
        self.reflections_button.setEnabled(False)

        master = self.dataset_selector.currentText()
        if not master:
            return

        frame_num = self.mapping[master][0]

        key = "spots_by_master"
        spots_key = "spots_xds"
        refls_key = "reflections_xds"
        
        if self.program == "mosflm":
            key = "spots_by_master_mosflm"
            spots_key = "spots_mosflm"
            refls_key = "reflections_mosflm"
        elif self.program == "crystfel":
            key = "spots_by_master_crystfel"
            spots_key = "spots_crystfel"
            refls_key = "reflections_crystfel"

        frame_data = self.result_data.get(key, {}).get(master, {}).get(frame_num, {})

        if spots := frame_data.get(spots_key):
            # Data from strategy backends is [x, y, indexed_flag]
            spots_xy = np.array(spots)[:, :2]

            # GraphicsManager.display_spots historically expects a (y, x) format.
            # Based on empirical evidence:
            # - XDS strategy results need to be flipped from (x,y) to (y,x).
            # - MOSFLM strategy results appear to be correct as (y,x) already from the predictor.
            # - CrystFEL: returns (fs, ss) which corresponds to (x, y) coordinates on the detector.
            #   However, pyqtgraph/numpy usually expect (row, col) which is (y, x).
            #   So we likely need to flip CrystFEL results too if they are (x, y).

            if self.program == "xds":
                self.spots_data = np.fliplr(spots_xy)  # Flip (x, y) -> (y, x)
            elif self.program == "crystfel":
                self.spots_data = np.fliplr(spots_xy) # Flip (x, y) -> (y, x)
            else:  # mosflm
                self.spots_data = spots_xy  # Assume MOSFLM provides (y, x) directly

            self.spots_button.setEnabled(True)

        if refls := frame_data.get(refls_key):
            # Data from both backends is consistently [h, k, l, x, y] in the results dict.
            # The display_indexed_reflections function expects a dict with 'x' and 'y' keys.
            if self.program == "mosflm":
                self.reflections_data = [
                    {"h": r[0], "k": r[1], "l": r[2], "x": r[4], "y": r[3]}
                    for r in refls
                ]
            elif self.program == "crystfel":
                 # CrystFEL stream: h k l I sigma peak bg fs/px ss/px panel
                 # But our parser may return it split. 
                 # run_crystfel_strategy uses parser.all_results["reflections_crystfel"] which is a list of lists.
                 # Need to check parser output format for indices.
                 # In stream_utils.py:
                 # miller_indices = np.array([[int(r[0]), int(r[1]), int(r[2])] for r in reflns])
                 # The 'reflns' is list of splits. 
                 # The parser does NOT filter or re-order columns if using _parse_chunk directly?
                 # Let's check stream_utils.py again.
                 # It returns reflns = [p.split() for p in chunk...]
                 # So r[0]=h, r[1]=k, r[2]=l.
                 # r[7]=fs/px (x), r[8]=ss/px (y). 
                 # NOTE: CrystFEL usually outputs fs/px then ss/px.
                 # Let's assume indices 7 and 8.
                 self.reflections_data = []
                 for r in refls:
                     try:
                         self.reflections_data.append({
                             "h": int(r[0]), "k": int(r[1]), "l": int(r[2]),
                             "x": float(r[7]), "y": float(r[8])
                         })
                     except (IndexError, ValueError):
                         continue
            else:  # xds
                self.reflections_data = [
                    {"h": r[0], "k": r[1], "l": r[2], "x": r[3], "y": r[4]}
                    for r in refls
                ]
            self.reflections_button.setEnabled(True)

    def _toggle_spots_display(self, checked):
        if checked and self.spots_data is not None:
            self.request_show_spots.emit(self.spots_data)
        else:
            self.request_hide_spots.emit()

    def _toggle_reflections_display(self, checked):
        if checked and self.reflections_data is not None:
            self.request_show_reflections.emit(self.reflections_data)
        else:
            self.request_hide_reflections.emit()

    def _get_common_prefix(self) -> str:
        """Determines a common prefix from the master files in the mapping."""
        import os
        from pathlib import Path

        if not self.mapping:
            return "strategy_run"

        master_paths = list(self.mapping.keys())
        if not master_paths:
            return "strategy_run"

        # Get stems without "_master.h5" and the .h5 extension
        stems = [Path(p).stem.replace("_master", "") for p in master_paths]

        if len(stems) == 1:
            return stems[0]

        common = os.path.commonprefix(stems)
        # Clean up trailing characters that are often separators
        common = common.rstrip("_-")

        # If the common prefix is empty after stripping, fall back to the first stem.
        return common or stems[0]

    def closeEvent(self, event):
        """Cleanup overlays and data when the dialog is closed."""
        # Uncheck buttons to trigger hide signals (if connected)
        # But we also explicitely emit hide signals to be sure
        self.request_hide_spots.emit()
        self.request_hide_reflections.emit()
        
        # Clear internal data to free memory
        self.spots_data = None
        self.reflections_data = None
        
        # Reset UI state (optional, as dialog is closing, but good practice)
        self.spots_button.setChecked(False)
        self.reflections_button.setChecked(False)
        
        super().closeEvent(event)
