import logging
import numpy as np
import pyqtgraph as pg
from scipy.signal import find_peaks
from scipy.ndimage import map_coordinates

from PyQt5 import QtCore, QtWidgets
from pyqtgraph.GraphicsScene.mouseEvents import MouseClickEvent

from qp2.image_viewer.utils.pixel_utils import calculate_distance
from qp2.image_viewer.utils.ring_math import radius_to_resolution
from qp2.image_viewer.ui.two_d_profile_dialog import TwoDProfileDialog

logger = logging.getLogger(__name__)

class MeasurementManager(QtCore.QObject):
    """
    Manages interactive measurement tools:
    - Distance (point-to-point)
    - Line Profile (intensity plot)
    - 2D Profile (ROI selection)
    """

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.ui_manager = main_window.ui_manager
        # self.graphics_manager deferred to property

        # State
        self.measure_distance_mode = False
        self.line_profile_mode = False
        self.is_2d_profile_mode = False
        self.distance_points = []
        
        # Windows/Dialogs
        self._line_profile_win = None

    @property
    def graphics_manager(self):
        return self.main_window.graphics_manager
        
    def _get_params(self):
        return self.main_window.params

    def toggle_distance_measurement_mode(self, checked: bool):
        self.measure_distance_mode = checked
        if checked:
            if self.line_profile_mode:
                self.line_profile_mode = False
                self.main_window.calculate_line_profile_action.setChecked(False)

            self.distance_points = []
            self.graphics_manager.clear_measure_visuals()
            self.ui_manager.show_status_message(
                "Distance Mode: Click the first point.", 0
            )
        else:
            if not self.line_profile_mode:
                 self.graphics_manager.clear_measure_visuals()
            self.ui_manager.show_status_message("Distance Mode Deactivated.", 2000)
            self.distance_points = []
            if self.main_window.measure_distance_action.isChecked() != checked:
                self.main_window.measure_distance_action.setChecked(checked)

    def toggle_line_profile_mode(self, checked: bool):
        self.line_profile_mode = checked
        if checked:
            if self.measure_distance_mode:
                self.measure_distance_mode = False
                self.main_window.measure_distance_action.setChecked(False)
            if self.is_2d_profile_mode:
                self.is_2d_profile_mode = False
                self.main_window.show_2d_profile_action.setChecked(False)

            self.distance_points = []
            self.graphics_manager.clear_measure_visuals()
            self.ui_manager.show_status_message(
                "Line Profile: Click first point on image.", 0
            )
        else:
            if not self.measure_distance_mode:
                self.graphics_manager.clear_measure_visuals()
            self.ui_manager.show_status_message("Line Profile Mode Deactivated.", 2000)
            self.distance_points = []
            if self.main_window.calculate_line_profile_action.isChecked() != checked:
                self.main_window.calculate_line_profile_action.setChecked(checked)

    def toggle_2d_profile_mode(self, checked: bool):
        self.is_2d_profile_mode = checked
        if checked:
            # Turn off other modes
            if self.measure_distance_mode:
                self.measure_distance_mode = False
                self.main_window.measure_distance_action.setChecked(False)
            if self.line_profile_mode:
                self.line_profile_mode = False
                self.main_window.calculate_line_profile_action.setChecked(False)
            
            self.distance_points = []
            self.graphics_manager.clear_measure_visuals()
            self.ui_manager.show_status_message(
                "2D Profile: Ctrl + Drag to select a rectangular region.", 0
            )
            self.graphics_manager.enable_roi_selection(self.on_roi_selected)
        else:
            self.graphics_manager.disable_roi_selection()
            self.ui_manager.show_status_message("2D Profile Mode Deactivated.", 2000)

    def on_roi_selected(self, roi_item):
        """Callback when an ROI is selected via the overlay."""
        if not roi_item:
            return

        img_item = self.graphics_manager.img_item
        img = img_item.image
        if img is None:
            self.ui_manager.view_box.removeItem(roi_item)
            return
        
        try:
            # Extract raw data using integer slicing (avoids interpolation)
            roi_pos = roi_item.pos()
            roi_size = roi_item.size()
            
            x = int(round(roi_pos.x()))
            y = int(round(roi_pos.y()))
            w = int(round(roi_size.x()))
            h = int(round(roi_size.y()))
            
            img_h, img_w = img.shape[:2]
            x1 = max(0, x)
            y1 = max(0, y)
            x2 = min(img_w, x + w)
            y2 = min(img_h, y + h)
            
            if x2 > x1 and y2 > y1:
                data_slice = img[y1:y2, x1:x2]
            else:
                data_slice = None
            
            if data_slice is not None and data_slice.size > 0:
                # Turn off mode
                self.is_2d_profile_mode = False
                self.main_window.show_2d_profile_action.setChecked(False)
                self.graphics_manager.disable_roi_selection()
                self.ui_manager.show_status_message("2D Profile calculated.", 3000)

                # Reuse or create dialog
                dialog = self.main_window.strategy_dialogs.get('2d_profile')

                # Check if dialog is valid (not deleted c++ object)
                is_valid = False
                if dialog is not None:
                     try:
                         # Accessing a deleted C++ object will raise RuntimeError
                         if isinstance(dialog, TwoDProfileDialog):
                             is_valid = True
                     except RuntimeError:
                         dialog = None

                if not is_valid:
                    # Create new dialog if none exists or invalid
                    dialog = TwoDProfileDialog(data_slice, parent=self.main_window)
                    self.main_window.strategy_dialogs['2d_profile'] = dialog
                else:
                    # Reuse existing dialog
                    # If we are switching ROIs while dialog is open, we must remove the previous ROI manually
                    if hasattr(dialog, 'current_roi_item') and dialog.current_roi_item:
                         try:
                             if dialog.current_roi_item.scene() is not None:
                                self.ui_manager.view_box.removeItem(dialog.current_roi_item)
                         except Exception:
                             pass

                    try:
                        # Disconnect previous cleanup signals to prevent accumulation
                        dialog.finished.disconnect()
                    except TypeError:
                        pass # No signals connected
                    
                    dialog.set_data(data_slice)
                
                # Store reference to current ROI for future cleanup
                dialog.current_roi_item = roi_item

                # Connect cleanup for this specific ROI
                dialog.finished.connect(lambda result, r=roi_item: self.ui_manager.view_box.removeItem(r))
                
                dialog.show()
                dialog.raise_()
                dialog.activateWindow() 
            else:
                self.ui_manager.show_warning_message("2D Profile", "Empty region selected.")
                self.ui_manager.view_box.removeItem(roi_item)
                    
        except Exception as e:
            logger.error(f"Error extracting 2D profile region: {e}", exc_info=True)
            self.ui_manager.view_box.removeItem(roi_item)

    def handle_mouse_click(self, event: MouseClickEvent):
        # calibration select pixels
        if self.main_window.is_manual_calibration_mode and self.main_window.manual_calibration_dialog:
            if event.button() != QtCore.Qt.MouseButton.LeftButton:
                return
            img_coords = self.ui_manager.view_box.mapSceneToView(event.scenePos())
            if (
                self.graphics_manager.img_item is None
                or not self.ui_manager.view_box.viewRect().contains(img_coords)
            ):
                return
            self.main_window.manual_calibration_dialog.add_point(img_coords)
            # Use the measurement point visual for temporary feedback
            self.graphics_manager.draw_measure_point(img_coords.x(), img_coords.y())
            return  # End here, don't proceed to distance measurement

        # Check if any measurement mode is active
        if (
            not (self.measure_distance_mode or self.line_profile_mode)
            or event.button() != QtCore.Qt.MouseButton.LeftButton
        ):
            return

        img_coords = self.ui_manager.view_box.mapSceneToView(event.scenePos())
        if (
            self.graphics_manager.img_item is None
            or not self.ui_manager.view_box.viewRect().contains(img_coords)
        ):
            return

        if self.measure_distance_mode:
            self._handle_distance_click(img_coords)
        elif self.line_profile_mode:
            self._handle_line_profile_click(img_coords)

    def _handle_distance_click(self, img_coords):
        self.distance_points.append(img_coords)
        if len(self.distance_points) == 1:
            self.graphics_manager.clear_measure_visuals()
            self.graphics_manager.draw_measure_point(img_coords.x(), img_coords.y())
            self.ui_manager.show_status_message(
                "Distance Mode: Click the second point.", 0
            )
        elif len(self.distance_points) == 2:
            p1, p2 = self.distance_points
            label = self._calculate_and_display_distance(p1, p2)
            self.graphics_manager.draw_measure_point(p2.x(), p2.y())
            self.graphics_manager.draw_measure_line(p1, p2, label)
            self.distance_points = []
            self.ui_manager.show_status_message(
                "Distance Mode: Click first point for next measurement.", 0
            )

    def _handle_line_profile_click(self, img_coords):
        self.distance_points.append(img_coords)
        if len(self.distance_points) == 1:
            self.graphics_manager.clear_measure_visuals()
            self.graphics_manager.draw_measure_point(img_coords.x(), img_coords.y())
            self.ui_manager.show_status_message(
                "Line Profile: Click second point.", 0
            )
        elif len(self.distance_points) == 2:
            p1, p2 = self.distance_points
            # Default height H = 100
            H = 100
            self._calculate_and_display_line_profile(p1, p2, height=H)
            
            # Calculate distance label for the image
            distance_info = calculate_distance(
                (p1.x(), p1.y()), (p2.x(), p2.y()), self._get_params()
            )
            label = distance_info["label"]

            self.graphics_manager.draw_measure_point(p2.x(), p2.y())
            self.graphics_manager.draw_measure_line(p1, p2, label)
            self.distance_points = []
            self.ui_manager.show_status_message(
                "Line Profile: Click first point for new profile.", 0
            )

    def _calculate_and_display_distance(self, p1: QtCore.QPointF, p2: QtCore.QPointF):
        distance_info = calculate_distance(
            (p1.x(), p1.y()), (p2.x(), p2.y()), self._get_params()
        )
        return distance_info["label"]

    def _calculate_and_display_line_profile(self, p1, p2, height=100):
        if (
            not self.graphics_manager.img_item
            or self.graphics_manager.img_item.image is None
        ):
            logger.warning("Line Profile: No image item found.")
            return

        img = self.graphics_manager.img_item.image

        try:
            scene_p1 = self.ui_manager.view_box.mapViewToScene(p1)
            scene_p2 = self.ui_manager.view_box.mapViewToScene(p2)
            pix_p1 = self.graphics_manager.img_item.mapFromScene(scene_p1)
            pix_p2 = self.graphics_manager.img_item.mapFromScene(scene_p2)
        except Exception as e:
            logger.error(f"Line Profile: Coordinate mapping failed: {e}")
            return

        x1, y1 = pix_p1.x(), pix_p1.y()
        x2, y2 = pix_p2.x(), pix_p2.y()

        logger.info(f"Line Profile: P1({x1:.2f}, {y1:.2f}) -> P2({x2:.2f}, {y2:.2f})")

        dx = x2 - x1
        dy = y2 - y1
        length_px = np.hypot(dx, dy)
        N = int(length_px)
        
        if N < 2:
            logger.info("Line Profile: Line too short ( < 2 pixels).")
            return

        t = np.linspace(0, 1, N)
        xs = x1 + dx * t
        ys = y1 + dy * t

        if length_px == 0:
            return
        px = -dy / length_px
        py = dx / length_px

        h2 = height / 2.0

        try:
            K = int(height)
            if K < 1:
                K = 1
            u = np.linspace(-h2, h2, K)
            
            xs_band = xs[:, None] + px * u[None, :]
            ys_band = ys[:, None] + py * u[None, :]

            coords = np.vstack([ys_band.ravel(), xs_band.ravel()])
            band_vals = map_coordinates(
                img, coords, order=1, mode="nearest", output=float
            ).reshape(N, K)
            
            profile = band_vals.mean(axis=1)

        except Exception as e:
            logger.error(f"Line Profile: Sampling failed: {e}", exc_info=True)
            return

        min_val = np.min(profile)
        max_val = np.max(profile)
        
        prominence = (max_val - min_val) * 0.05 if max_val > min_val else None
        peak_indices, _ = find_peaks(profile, prominence=prominence)
        
        distances = np.linspace(0, length_px, N)

        peak_distances = np.diff(distances[peak_indices]) if len(peak_indices) > 1 else np.array([])
        
        logger.info(f"Line Profile: Found {len(peak_indices)} peaks. Max val: {max_val:.2f}")

        self._show_line_profile_plot(distances, profile, peak_indices, peak_distances)

    def _show_line_profile_plot(self, distances, profile, peak_indices, peak_distances):
        win = getattr(self, "_line_profile_win", None)
        if win is None:
            win = pg.GraphicsLayoutWidget(title="Line Profile")
            win.setWindowTitle("Line Profile")
            self._line_profile_plot = win.addPlot(row=0, col=0)
            
            self._line_profile_text = QtWidgets.QTextEdit()
            self._line_profile_text.setReadOnly(True)
            self._line_profile_text.setStyleSheet("QTextEdit { color : white; background-color: #333; }")
            self._line_profile_text.setMaximumHeight(100)
            
            proxy = QtWidgets.QGraphicsProxyWidget()
            proxy.setWidget(self._line_profile_text)
            win.addItem(proxy, row=1, col=0)
            
            self._line_profile_win = win
        else:
            self._line_profile_plot.clear()

        p = self._line_profile_plot
        p.setLabel("bottom", "Distance", units="px")
        p.setLabel("left", "Intensity")
        p.setTitle("Line Profile")
        p.showGrid(x=True, y=True)

        p.plot(distances, profile, pen="y")

        if len(peak_indices) > 0:
            peak_x = distances[peak_indices]
            peak_y = profile[peak_indices]
            p.plot(
                peak_x,
                peak_y,
                pen=None,
                symbol="o",
                symbolBrush="r",
                symbolSize=8,
            )

            for i in range(len(peak_indices) - 1):
                idx1 = peak_indices[i]
                idx2 = peak_indices[i + 1]
                
                x1, x2 = distances[idx1], distances[idx2]
                y1, y2 = profile[idx1], profile[idx2]
                
                dist_px = x2 - x1
                mid_x = (x1 + x2) / 2
                mid_y = (y1 + y2) / 2
                
                # Calculate Angstrom
                dist_ang_str = ""
                # Use helper in this file directly
                
                def get_param_local(keys):
                    params = self._get_params()
                    if not params: return None
                    for k in keys:
                        val = params.get(k)
                        if val is not None: return float(val)
                    return None

                wl_l = get_param_local(["wavelength", "lambda", "lam"])
                dd_l = get_param_local(["det_dist", "detector_distance", "distance", "det_dist_m"])
                ps_l = get_param_local(["pixel_size", "pixel_x"])
                
                if all(v is not None and v > 0 for v in [wl_l, dd_l, ps_l]):
                    try:
                        d_ang = radius_to_resolution(wl_l, dd_l, dist_px, ps_l)
                        dist_ang_str = f"\n{d_ang:.2f} Å"
                    except Exception:
                        pass
                
                label_text = f"{dist_px:.1f} px{dist_ang_str}"
                
                ti = pg.TextItem(text=label_text, color=(200, 200, 200), anchor=(0.5, 1))
                ti.setPos(mid_x, mid_y)
                p.addItem(ti)

        if peak_distances.size > 0:
            mean_dist = np.mean(peak_distances)
            std_dist = np.std(peak_distances)
            
            mean_ang_str = ""
            indiv_ang_strs = []
            
            def get_param(keys):
                params = self._get_params()
                if not params: return None
                for k in keys:
                    val = params.get(k)
                    if val is not None: return float(val)
                return None

            wl = get_param(["wavelength", "lambda", "lam"])
            dd = get_param(["det_dist", "detector_distance", "distance", "det_dist_m"])
            ps = get_param(["pixel_size", "pixel_x"])
            
            if all(v is not None and v > 0 for v in [wl, dd, ps]):
                try:
                    mean_ang = radius_to_resolution(wl, dd, mean_dist, ps)
                    mean_ang_str = f" | {mean_ang:.2f} Å"
                    
                    for d in peak_distances:
                         ang = radius_to_resolution(wl, dd, d, ps)
                         indiv_ang_strs.append(f"{d:.1f} px ({ang:.2f} Å)")
                except Exception as e:
                    logger.error(f"LineProfile conversion error: {e}", exc_info=True)
            
            if not indiv_ang_strs:
                indiv_ang_strs = [f"{d:.1f}" for d in peak_distances]

            txt = f"<b>Mean Distance:</b> {mean_dist:.2f} px +/- {std_dist:.2f}{mean_ang_str}<br>"
            txt += "<b>Individual Distances:</b><br>"
            txt += ", ".join(indiv_ang_strs)
        else:
            txt = "No adjacent peaks found."

        self._line_profile_text.setHtml(txt)

        self._line_profile_win.show()
        self._line_profile_win.raise_()
        self._line_profile_win.activateWindow()
