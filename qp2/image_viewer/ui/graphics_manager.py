# graphics_manager.py
from typing import Optional, List

import numpy as np
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui, QtWidgets

# Import necessary config constants or helper functions
from qp2.image_viewer.config import (
    PEAK_LABEL_FONT_SIZE,
    PEAK_LABEL_COLOR,
    PEAK_LABEL_OFFSET,
    NUM_PEAKS_TO_LABEL,
    NUM_REFLECTION_LABELS,
    PIXEL_TEXT_GRID_RADIUS,
    PIXEL_TEXT_COLOR,
    MAX_DISPLAYED_REFLECTIONS,
    MAX_DISPLAYED_SPOTS,
)
from qp2.image_viewer.utils.ring_math import angstrom_to_pixels, radius_to_resolution
from qp2.log.logging_config import get_logger
from qp2.image_viewer.ui.roi_overlay import ROISelectionOverlay

logger = get_logger(__name__)


class GraphicsManager:
    """Manages pyqtgraph graphics items within the ViewBox."""

    def __init__(
            self, view_box: pg.ViewBox, hist_lut: pg.HistogramLUTItem, main_window
    ):
        self.view_box = view_box
        self.hist_lut = hist_lut  # Reference to the LUT created by UIManager
        self.main_window = main_window  # For accessing params, state
        
        self.roi_overlay = None

        # --- Initialize Graphics Items ---
        self.img_item = pg.ImageItem(border="w")
        self.img_item.setAutoDownsample(True)
        self.img_item.setOpts(downsampleMethod='peak')  # or 'peak' or 'subsample'
        self.view_box.addItem(self.img_item)
        # Connect image item to LUT *after* img_item is created
        self.hist_lut.setImageItem(self.img_item)

        # peaks button & live spots on playback
        self.peak_scatter_item = pg.ScatterPlotItem(
            size=12,
            pen=None,  # No outline
            brush=pg.mkBrush(color='r'),  # Solid red color
            symbol="+",  # Use '+' symbol
            hoverable=True,
            hoverPen=pg.mkPen("w", width=2),  # White hover pen to match plugins
        )

        self.view_box.addItem(self.peak_scatter_item)

        self.beam_center_marker = None  # Initialized as None

        self.v_line = pg.InfiniteLine(
            angle=90, movable=False, pen=pg.mkPen("y", style=QtCore.Qt.PenStyle.DotLine)
        )
        self.h_line = pg.InfiniteLine(
            angle=0, movable=False, pen=pg.mkPen("y", style=QtCore.Qt.PenStyle.DotLine)
        )
        self.view_box.addItem(self.v_line, ignoreBounds=True)
        self.view_box.addItem(self.h_line, ignoreBounds=True)
        self.v_line.hide()
        self.h_line.hide()

        # --- Item Storage Lists/Dicts ---
        self.peak_label_items = []
        self.resolution_ring_items = {}
        self.resolution_ring_labels = {}
        self.calibration_ring_items = []
        self.calibration_points_item = None
        self.calibration_center_marker = None
        self.calibration_center_label = None
        self.pixel_text_items = []

        # --- Measurement Visuals ---
        self.measure_point_items = []
        self.measure_line_item = None
        self.measure_text_item = None

        # --- PLUGIN VISUALS START ---
        self.bad_pixel_overlay_item = pg.ScatterPlotItem(
            size=10,
            pen=pg.mkPen("y", width=2), # Yellow outline
            brush=None, # No fill
            symbol="+",
            hoverable=True,
            hoverPen=pg.mkPen("w", width=2),
        )
        self.bad_pixel_overlay_item.setZValue(30) # Draw on top of most things
        self.view_box.addItem(self.bad_pixel_overlay_item)
        # Generic spots (used by SpotFinder, Dozor, and for CrystFEL's raw spots)

        self.spot_scatter_item = pg.ScatterPlotItem(
            size=12,
            pen=None,  # CRITICAL: No outline pen
            brush=pg.mkBrush(color='r'),  # CRITICAL: Use a solid red brush
            symbol="+",  # The '+' symbol
            hoverable=True,
            hoverPen=pg.mkPen("w", width=2),
        )
        self.view_box.addItem(self.spot_scatter_item)

        # CrystFEL Indexed Reflections
        self.indexed_reflections_scatter_item = pg.ScatterPlotItem(
            size=8,  # Squares can be a bit smaller
            pen=pg.mkPen(color="g", width=1),  # Thin green outline
            brush=None,  # No fill for a hollow square
            symbol="s",  # 's' for square
            hoverable=True,
            hoverPen=pg.mkPen("w", width=2),
        )
        self.indexed_reflections_scatter_item.setZValue(20)  # Draw on top
        self.view_box.addItem(self.indexed_reflections_scatter_item)

        # CrystFEL Reflection Labels (h,k,l)
        self.reflection_label_items: List[pg.TextItem] = []

        # Generic Text Box for plugin info (e.g., CrystFEL cell params)

        self.plugin_info_text_item = pg.TextItem(
            html="",  # Use html for rich text
            border=pg.mkPen("#CCCCCC", width=1),
            fill=pg.mkBrush(0, 0, 0, 180),
            anchor=(0.0, 0.0),
        )
        self.plugin_info_text_item.setParentItem(self.view_box)
        self.plugin_info_text_item.setZValue(100)
        self.plugin_info_text_item.hide()
        self.view_box.addItem(self.plugin_info_text_item)

        # Dynamic Hover Tooltip for Reflections
        self.hover_tooltip_item = pg.TextItem(
            text="",
            color="yellow",
            anchor=(0.0, 1.0),
            border=pg.mkPen("yellow", width=1),
            fill=pg.mkBrush(0, 0, 0, 200),
        )
        self.hover_tooltip_item.setZValue(101)  # Top-most
        self.hover_tooltip_item.hide()
        self.view_box.addItem(self.hover_tooltip_item)

        # Connect hover signal for reflections
        self.indexed_reflections_scatter_item.sigHovered.connect(self._on_reflection_hover)

        # Add filter label
        self.filter_label = pg.TextItem(
            text="",
            color="yellow",
            anchor=(0.0, 0.0),
            border=pg.mkPen("#FFD700", width=2),
            fill=pg.mkBrush(0, 0, 0, 200),
        )
        self.filter_label.hide()
        self.view_box.addItem(self.filter_label)

        # Add summed image label
        self.sum_label = pg.TextItem(
            text="",
            color="cyan",
            anchor=(0.0, 0.0),
            border=pg.mkPen("#00FFFF", width=2),
            fill=pg.mkBrush(0, 0, 0, 200),
        )
        self.sum_label.hide()
        self.view_box.addItem(self.sum_label)

        # Add calibration label
        self.calibration_label = pg.TextItem(
            text="",
            color="magenta",
            anchor=(0.0, 0.0),
            border=pg.mkPen("#FF00FF", width=2),
            fill=pg.mkBrush(0, 0, 0, 200),
        )
        self.calibration_label.hide()
        self.view_box.addItem(self.calibration_label)

        self.mask_overlay_item = None
        self.ice_ring_items = []
        self.ice_ring_summary_label = None
        self.proposed_center_marker = None

    # --- Image Display ---
    def display_image(self, image_data):
        """Displays the given image data."""
        if image_data is None:
            self.img_item.clear()
        else:
            try:
                # autoLevels=False because contrast is handled by hist_lut
                self.img_item.setImage(image_data, autoLevels=False)
            except Exception as e:
                logger.error(f"display_image: Error setting image: {e}", exc_info=True)
                self.img_item.clear()

    def clear_image(self):
        self.img_item.clear()

    # --- Contrast / LUT ---
    def set_contrast_levels(self, vmin, vmax):
        """Sets the contrast levels on the HistogramLUTItem."""
        try:
            # if not getattr(self.main_window, "contrast_locked", False):
            #     # Define a custom gradient with 2 ticks (white and black)
            #     state = {
            #         "mode": "rgb",
            #         "ticks": [
            #             (0.0, (255, 255, 255, 255)),  # white
            #             (1.0, (0, 0, 0, 255)),  # black
            #         ],
            #     }
            #     self.hist_lut.gradient.restoreState(state)

            self.hist_lut.setLevels(float(vmin), float(vmax))
        except Exception as e:
            logger.error(
                f"set_contrast_levels: Error setting LUT levels: {e}", exc_info=True
            )

    def set_histogram_range(self, rmin, rmax):
        """Sets the histogram range on the HistogramLUTItem."""
        try:
            if np.isfinite(rmin) and np.isfinite(rmax) and rmax > rmin:
                self.hist_lut.setHistogramRange(rmin, rmax)
        except Exception as e:
            logger.error(
                f"set_histogram_range: Error setting histogram range: {e}",
                exc_info=True,
            )

    # --- Peak Visuals ---
    def update_peaks(self, peaks: Optional[np.ndarray]):
        """Updates the peak scatter plot and rank labels."""
        self._clear_peak_labels()  # Clear previous labels first
        if peaks is None or len(peaks) == 0:
            self.peak_scatter_item.setData([], [])
            return

        y_coords, x_coords = peaks[:, 0], peaks[:, 1]
        self.peak_scatter_item.setData(x_coords, y_coords)

        # Add Rank Labels
        n_to_label = min(NUM_PEAKS_TO_LABEL, len(peaks))
        if n_to_label > 0:
            label_font = QtGui.QFont()
            label_font.setPixelSize(PEAK_LABEL_FONT_SIZE)
            label_dx, label_dy = PEAK_LABEL_OFFSET
            try:
                for i in range(n_to_label):
                    peak_x, peak_y = x_coords[i], y_coords[i]
                    label_text = f"{i + 1}"
                    label_item = pg.TextItem(
                        text=label_text, color=PEAK_LABEL_COLOR, anchor=(0.5, 1.0)
                    )
                    label_item.setPos(peak_x + label_dx, peak_y + label_dy)
                    label_item.setFont(label_font)
                    self.view_box.addItem(label_item)
                    self.peak_label_items.append(label_item)
            except (IndexError, ValueError) as e:
                logger.error(
                    f"update_peaks: Error creating peak labels: {e}", exc_info=True
                )
                self._clear_peak_labels()  # Clear partial labels on error

    def clear_peaks(self):
        """Clears peak scatter plot and labels."""
        if self.peak_scatter_item:
            self.peak_scatter_item.setData([], [])
        self._clear_peak_labels()

    def _clear_peak_labels(self):
        """Removes peak label TextItems from the scene."""
        for item in self.peak_label_items:
            if item.scene():
                try:
                    self.view_box.removeItem(item)
                except Exception:
                    pass
        self.peak_label_items = []

    # --- Beam Center Marker ---
    def update_beam_center_marker(self, params):
        """Updates the position of the beam center marker."""
        # Remove existing
        if self.beam_center_marker and self.beam_center_marker.scene():
            try:
                self.view_box.removeItem(self.beam_center_marker)
            except Exception:
                pass
        self.beam_center_marker = None

        # Add new if params valid
        beam_x = params.get("beam_x")
        beam_y = params.get("beam_y")
        if (
                beam_x is not None
                and beam_y is not None
                and np.isfinite(beam_x)
                and np.isfinite(beam_y)
        ):
            try:
                self.beam_center_marker = pg.ScatterPlotItem(
                    pos=[(float(beam_x), float(beam_y))],
                    symbol="x",
                    size=20,
                    pen=pg.mkPen("g", width=2),
                )
                self.view_box.addItem(self.beam_center_marker)
            except Exception as e:
                logger.error(
                    f"update_beam_center_marker: Error creating beam center marker: {e}",
                    exc_info=True,
                )
                self.beam_center_marker = None

    def display_proposed_beam_center(self, x, y):
        """Displays a marker for a proposed beam center (e.g. from calculation)."""
        self.clear_proposed_beam_center()
        try:
            self.proposed_center_marker = pg.ScatterPlotItem(
                pos=[(float(x), float(y))],
                symbol="+",
                size=25,
                pen=pg.mkPen("m", width=3),  # Magenta, thicker
                brush=pg.mkBrush("m"),
            )
            self.view_box.addItem(self.proposed_center_marker)
        except Exception as e:
            logger.error(f"display_proposed_beam_center: {e}", exc_info=True)

    def clear_proposed_beam_center(self):
        if self.proposed_center_marker and self.proposed_center_marker.scene():
            try:
                self.view_box.removeItem(self.proposed_center_marker)
            except Exception:
                pass
        self.proposed_center_marker = None

    # --- Resolution Rings ---
    def update_resolution_rings(self, visible, params, ring_list):
        """Draws or hides resolution rings."""
        self._hide_resolution_rings()  # Clear existing first
        if not visible:
            return

        # Check params needed for calculation
        beam_x = params.get("beam_x")
        beam_y = params.get("beam_y")
        wl = params.get("wavelength")
        dist = params.get("det_dist")
        px_size = params.get("pixel_size")

        if (
                not all(
                    isinstance(p, (int, float, np.number))
                    and np.isfinite(p)
                    and p is not None
                    for p in [beam_x, beam_y, wl, dist, px_size]
                )
                or wl <= 0
                or dist <= 0
                or px_size <= 0
        ):
            logger.debug(
                "update_resolution_rings: Cannot draw rings due to invalid params."
            )
            return

        try:
            ring_color_map = {3.67: "y", 3.03: "r", 2.25: "c"}
            for d_spacing in ring_list:
                if d_spacing <= 0:
                    continue
                try:
                    radius_pixels = angstrom_to_pixels(d_spacing, wl, dist, px_size)
                except Exception as e:
                    logger.error(
                        f"update_resolution_rings: Error calculating radius for d={d_spacing}Å: {e}",
                        exc_info=True,
                    )
                    continue
                if not np.isfinite(radius_pixels) or radius_pixels <= 0:
                    continue

                # Create Ellipse
                x, y = float(beam_x - radius_pixels), float(beam_y - radius_pixels)
                diameter = 2 * float(radius_pixels)
                ring_item = QtWidgets.QGraphicsEllipseItem(x, y, diameter, diameter)
                color = ring_color_map.get(d_spacing, "g")
                ring_item.setPen(pg.mkPen(color, width=1.5))
                self.view_box.addItem(ring_item)
                self.resolution_ring_items[d_spacing] = ring_item

                # Create Label
                label_angle = np.pi / 4
                label_radius_offset = 10
                label_x = float(beam_x) + (
                        radius_pixels + label_radius_offset
                ) * np.cos(label_angle)
                label_y = float(beam_y) + (
                        radius_pixels + label_radius_offset
                ) * np.sin(label_angle)
                label_html = f'<span style="color: {color}; font-size: 10pt;">{d_spacing:.2f}Å</span>'
                label_item = pg.TextItem(
                    html=label_html,
                    anchor=(0.5, 0.5),
                    fill=pg.mkBrush(255, 255, 255, 220),
                )
                label_item.setPos(label_x, label_y)
                self.view_box.addItem(label_item)
                self.resolution_ring_labels[d_spacing] = label_item
        except Exception as e:
            logger.error(
                f"update_resolution_rings: Error drawing resolution rings: {e}",
                exc_info=True,
            )
            self._hide_resolution_rings()  # Clean up

    def _hide_resolution_rings(self):
        """Removes all resolution ring graphics items."""
        for item in self.resolution_ring_items.values():
            if item and item.scene():
                try:
                    self.view_box.removeItem(item)
                except Exception:
                    pass
        self.resolution_ring_items.clear()
        for item in self.resolution_ring_labels.values():
            if item and item.scene():
                try:
                    self.view_box.removeItem(item)
                except Exception:
                    pass
        self.resolution_ring_labels.clear()

    # --- Calibration Visuals ---
    def display_calibration_results(self, result_dict):
        """Displays circles, points, and center from calibration worker results."""
        self.clear_calibration_visuals()  # Clear previous
        if not result_dict:
            return

        refined_circle = result_dict.get("refined_circle")
        refine_circle_radii = result_dict.get("refine_circle_radii")
        hough_circle = result_dict.get("hough_circle")
        strong_pixels = result_dict.get("strong_pixels")
        selected_peak_indices = result_dict.get("selected_peak_indices", [])

        # Draw Hough Circle (optional)
        if hough_circle:
            try:
                h_cx, h_cy, h_r = hough_circle
                if h_r > 0:
                    h_ellipse = QtWidgets.QGraphicsEllipseItem(
                        h_cx - h_r, h_cy - h_r, 2 * h_r, 2 * h_r
                    )
                    h_ellipse.setPen(
                        pg.mkPen("orange", width=2, style=QtCore.Qt.PenStyle.DashLine)
                    )
                    self.view_box.addItem(h_ellipse)
                    self.calibration_ring_items.append(h_ellipse)
            except Exception as e:
                logger.error(
                    f"display_calibration_results: Error drawing Hough circle: {e}",
                    exc_info=True,
                )

        # Draw Refined Circle/Ellipse and Center
        if refined_circle:
            try:
                r_cx, r_cy, r_r = refined_circle
                # Use ellipse if radii are provided and valid
                if (
                        refine_circle_radii is not None
                        and len(refine_circle_radii) == 2
                        and all(x > 0 for x in refine_circle_radii)
                ):
                    rx, ry = refine_circle_radii
                    ellipse_item = QtWidgets.QGraphicsEllipseItem(
                        r_cx - rx, r_cy - ry, 2 * rx, 2 * ry
                    )
                    ellipse_item.setPen(pg.mkPen("lime", width=4))
                    self.view_box.addItem(ellipse_item)
                    self.calibration_ring_items.append(ellipse_item)
                elif r_r > 0:
                    r_ellipse = QtWidgets.QGraphicsEllipseItem(
                        r_cx - r_r, r_cy - r_r, 2 * r_r, 2 * r_r
                    )
                    r_ellipse.setPen(pg.mkPen("lime", width=4))
                    self.view_box.addItem(r_ellipse)
                    self.calibration_ring_items.append(r_ellipse)

                # Additional rings from selected_peak_indices (yellow, thinner)
                for radius in selected_peak_indices:
                    ring = QtWidgets.QGraphicsEllipseItem(
                        r_cx - radius, r_cy - radius, 2 * radius, 2 * radius
                    )
                    # Yellow, thinner line
                    ring.setPen(pg.mkPen(color="y", width=1))
                    self.view_box.addItem(ring)
                    self.calibration_ring_items.append(ring)

                # Center Marker (+)
                self.calibration_center_marker = pg.ScatterPlotItem(
                    pos=[(r_cx, r_cy)],
                    symbol="+",
                    size=25,
                    pen=pg.mkPen("lime", width=2),
                )
                self.view_box.addItem(self.calibration_center_marker)

                # # Center Label
                label_text = f"Refined Center:\n({r_cx:.1f}, {r_cy:.1f})"
                self.calibration_center_label = pg.TextItem(
                    text=label_text, color="lime", anchor=(0.0, 1.0)
                )
                self.calibration_center_label.setPos(r_cx + 5, r_cy - 5)
                self.view_box.addItem(self.calibration_center_label)

                # --- Add resolution label ---
                params = getattr(self.main_window, "params", None)
                if params:
                    wl = params.get("wavelength")
                    dist = params.get("det_dist")
                    px_size = params.get("pixel_size")
                    if (
                            all(
                                isinstance(p, (int, float, np.number))
                                and np.isfinite(p)
                                and p is not None
                                for p in [wl, dist, px_size]
                            )
                            and wl > 0
                            and dist > 0
                            and px_size > 0
                    ):
                        resolution = radius_to_resolution(
                            wl, dist, r_r, px_size, round=4
                        )
                        label_angle = np.pi / 4
                        label_radius_offset = 15
                        label_x = float(r_cx) + (r_r + label_radius_offset) * np.cos(
                            label_angle
                        )
                        label_y = float(r_cy) + (r_r + label_radius_offset) * np.sin(
                            label_angle
                        )
                        label_html = f'<span style="color: lime; font-size: 12pt;">{resolution:.4f}Å</span>'
                        res_label_item = pg.TextItem(
                            html=label_html,
                            anchor=(0.5, 0.5),
                            fill=pg.mkBrush(0, 0, 0, 180),
                        )
                        res_label_item.setPos(label_x, label_y)
                        self.view_box.addItem(res_label_item)
                        self.calibration_ring_items.append(res_label_item)
            except Exception as e:
                logger.error(
                    f"display_calibration_results: Error drawing refined circle/center: {e}",
                    exc_info=True,
                )

        # Show Strong Pixels
        if strong_pixels is not None and len(strong_pixels) > 0:
            try:
                self.calibration_points_item = pg.ScatterPlotItem(
                    x=strong_pixels[:, 0],
                    y=strong_pixels[:, 1],
                    symbol="o",
                    size=5,
                    pen=None,
                    brush=pg.mkBrush(255, 0, 0, 150),
                )
                self.view_box.addItem(self.calibration_points_item)
            except Exception as e:
                logger.error(
                    f"display_calibration_results: Error displaying strong pixels: {e}",
                    exc_info=True,
                )

    def clear_calibration_visuals(self):
        """Removes calibration-related graphics items."""
        for item in self.calibration_ring_items:
            if item and item.scene():
                try:
                    self.view_box.removeItem(item)
                except:
                    pass
        self.calibration_ring_items = []

        if self.calibration_points_item and self.calibration_points_item.scene():
            try:
                self.view_box.removeItem(self.calibration_points_item)
            except:
                pass
        self.calibration_points_item = None

        if self.calibration_center_marker and self.calibration_center_marker.scene():
            try:
                self.view_box.removeItem(self.calibration_center_marker)
            except:
                pass
        self.calibration_center_marker = None

        if self.calibration_center_label and self.calibration_center_label.scene():
            try:
                self.view_box.removeItem(self.calibration_center_label)
            except:
                pass
        self.calibration_center_label = None

    # --- Crosshairs ---
    def update_crosshairs(self, pos: Optional[QtCore.QPointF]):
        """Updates crosshair position or hides them."""
        if pos:
            self.v_line.setPos(pos.x())
            self.h_line.setPos(pos.y())
            self.v_line.show()
            self.h_line.show()
        else:
            self.v_line.hide()
            self.h_line.hide()

    # --- Pixel Text ---
    def update_pixel_text(
            self,
            image_data,
            center_pos: QtCore.QPointF,
            mask_values: set = None,
            detector_mask: np.ndarray = None,
    ):
        """Updates the grid of pixel value text items."""
        logger = get_logger(__name__)
        if image_data is None:
            self.hide_all_pixel_text()
            logger.debug(
                "update_pixel_text: image_data is None, hiding all pixel text."
            )
            return

        img_h, img_w = image_data.shape[:2]
        radius = PIXEL_TEXT_GRID_RADIUS
        center_x = int(round(center_pos.x()))
        center_y = int(round(center_pos.y()))

        if not (0 <= center_x < img_w and 0 <= center_y < img_h):
            self.hide_all_pixel_text()
            return

        y_start = max(0, center_y - radius)
        y_end = min(img_h - 1, center_y + radius)
        x_start = max(0, center_x - radius)
        x_end = min(img_w - 1, center_x + radius)
        needed_items = (y_end - y_start + 1) * (x_end - x_start + 1)

        # Manage Pool
        while len(self.pixel_text_items) < needed_items:
            text_item = pg.TextItem(
                color=PIXEL_TEXT_COLOR, anchor=(0.5, 0.5), fill=pg.mkBrush(0, 0, 0, 150)
            )
            text_item.setZValue(15)  # Higher than mask overlay (10)
            self.pixel_text_items.append(text_item)
            self.view_box.addItem(text_item)

        # Update Items
        item_index = 0
        for r in range(y_start, y_end + 1):
            for c in range(x_start, x_end + 1):
                if item_index >= len(self.pixel_text_items):
                    break
                text_item = self.pixel_text_items[item_index]
                try:
                    intensity = image_data[r, c]
                    # Format
                    if np.issubdtype(image_data.dtype, np.integer):
                        text_str = f"{intensity:.0f}"
                    elif np.issubdtype(image_data.dtype, np.floating):
                        text_str = f"{intensity:.1f}"
                    else:
                        text_str = str(intensity)
                    # Check Mask
                    is_masked = False
                    if (
                            detector_mask is not None
                            and detector_mask.shape == image_data.shape
                    ):
                        # Use precomputed detector mask
                        is_masked = detector_mask[r, c]
                    elif mask_values is not None:
                        # Use mask_values set (backward compatibility)
                        intensity_val = (
                            intensity.item()
                            if hasattr(intensity, "item")
                            else intensity
                        )
                        try:
                            if intensity_val in mask_values:
                                is_masked = True
                        except TypeError:
                            for mv in mask_values:
                                try:
                                    if isinstance(intensity_val, float) and isinstance(
                                            mv, (float, int)
                                    ):
                                        if np.isclose(intensity_val, float(mv)):
                                            is_masked = True
                                            break
                                    elif intensity_val == mv:
                                        is_masked = True
                                        break
                                except Exception:
                                    pass
                    if is_masked:
                        text_str += " M"
                    # Update Item
                    text_item.setText(text_str)
                    text_item.setPos(c + 0.5, r + 0.5)
                    text_item.setVisible(True)
                except Exception as e:
                    text_item.setVisible(False)
                    logger.error(
                        f"update_pixel_text: Error at ({r},{c}): {e}", exc_info=True
                    )
                item_index += 1
            if item_index >= len(self.pixel_text_items):
                break

        # Hide unused
        self._hide_pixel_text_range(item_index, len(self.pixel_text_items))
        logger.debug(
            f"update_pixel_text: {item_index} items set visible, {len(self.pixel_text_items) - item_index} hidden."
        )

    def _hide_pixel_text_range(self, start_index, end_index):
        """Hides TextItems in the pool within the specified index range."""
        safe_end = min(end_index, len(self.pixel_text_items))
        for i in range(start_index, safe_end):
            if self.pixel_text_items[i].isVisible():
                self.pixel_text_items[i].setVisible(False)

    def hide_all_pixel_text(self):
        """Makes all TextItems in the pixel text pool invisible."""
        self._hide_pixel_text_range(0, len(self.pixel_text_items))

    def clear_all_visuals(self):
        """Clears all potentially persistent visuals (peaks, rings, calibration)."""
        self.clear_peaks()
        self._hide_resolution_rings()
        self.clear_calibration_visuals()
        self.clear_proposed_beam_center()
        # Don't clear beam center marker here, it's updated based on params
        self.clear_spots()
        self.clear_measure_visuals()
        self.clear_indexed_reflections()
        self.clear_plugin_info_text()
        self.clear_bad_pixels_overlay()
        self.clear_ice_rings()

    def display_spots(self, spots_yx_coords: Optional[np.ndarray]):
        """Displays raw spots on the image."""
        if spots_yx_coords is None or len(spots_yx_coords) == 0:
            self.spot_scatter_item.setData([], [])
            return
        # Assuming spots_yx_coords is an array of (y, x)
        # Limit the number of spots to display for performance
        if len(spots_yx_coords) > MAX_DISPLAYED_SPOTS:
            logger.debug(f"Limiting displayed spots from {len(spots_yx_coords)} to {MAX_DISPLAYED_SPOTS}")
            spots_yx_coords = spots_yx_coords[:MAX_DISPLAYED_SPOTS]
            
        self.spot_scatter_item.setData(x=spots_yx_coords[:, 1], y=spots_yx_coords[:, 0])
        
        # Optimize performance for large number of spots
        if len(spots_yx_coords) > 2000:
             self.spot_scatter_item.setAcceptHoverEvents(False)
        else:
             self.spot_scatter_item.setAcceptHoverEvents(True)

    def clear_spots(self):
        """Clears Dozor spots from the image."""
        if self.spot_scatter_item:
            self.spot_scatter_item.setData([], [])

    def display_indexed_reflections(self, reflections: Optional[List[dict]]):
        """
        Displays indexed reflections and their h,k,l labels for CrystFEL.
        Reflections are a list of dicts, e.g., {'h':h, 'k':k, 'l':l, 'x':x, 'y':y}.
        """
        self.clear_indexed_reflections()  # Clear previous ones first
        if not reflections:
            return

        # Limit the number of reflections to display
        if len(reflections) > MAX_DISPLAYED_REFLECTIONS:
            logger.debug(f"Limiting displayed indexed reflections from {len(reflections)} to {MAX_DISPLAYED_REFLECTIONS}")
            reflections = reflections[:MAX_DISPLAYED_REFLECTIONS]

        try:
            coords_xy = np.array([[r["x"], r["y"]] for r in reflections])
            labels = [f"({r['h']},{r['k']},{r['l']})" for r in reflections]

            # Display the reflections as scatter points
            # Store the label in the 'data' field for hover retrieval
            self.indexed_reflections_scatter_item.setData(
                x=coords_xy[:, 0], 
                y=coords_xy[:, 1],
                data=labels  # Pass labels as per-point data
            )
            
            # Show a few labels statically if configured, but prioritize performance
            num_static_labels = 5 # Hard cap for static labels to ensure responsiveness
            label_font = QtGui.QFont("Arial", 8)
            
            for i in range(min(num_static_labels, len(labels))):
                label_text = labels[i]
                label_item = pg.TextItem(
                    text=label_text, color="cyan", anchor=(0.0, 0.5)
                )
                # Offset slightly to the right of the '+'
                label_item.setPos(coords_xy[i, 0] + 5, coords_xy[i, 1])
                label_item.setFont(label_font)
                label_item.setZValue(21)  # On top of everything
                self.view_box.addItem(label_item)
                self.reflection_label_items.append(label_item)
                
        except (ValueError, IndexError, KeyError) as e:
            logger.error(f"Failed to parse and display indexed reflections: {e}")
            self.clear_indexed_reflections()  # Clean up on failure

    def _on_reflection_hover(self, item, points, ev):
        """Shows a tooltip with HKL indices when hovering over a reflection."""
        # points can be None, empty list, or empty numpy array
        # 'if not points:' fails for numpy arrays
        if points is None or len(points) == 0:
            self.hover_tooltip_item.hide()
            return

        try:
            # Get the top-most point under cursor
            point = points[0]
            label_text = point.data()
            if label_text:
                self.hover_tooltip_item.setText(label_text)
                # Position near the mouse cursor (from event) or point
                # Use point position + offset
                pos = point.pos()
                self.hover_tooltip_item.setPos(pos.x() + 10, pos.y() - 10)
                self.hover_tooltip_item.setVisible(True)
        except Exception:
             self.hover_tooltip_item.hide()

    def clear_indexed_reflections(self):
        """Clears all indexed reflection visuals."""
        if self.indexed_reflections_scatter_item:
            self.indexed_reflections_scatter_item.setData([], [])
        for item in self.reflection_label_items:
            if item.scene():
                self.view_box.removeItem(item)
        self.reflection_label_items = []
        
        # Hide the hover tooltip so it doesn't persist if open
        if self.hover_tooltip_item:
            self.hover_tooltip_item.hide()

    # end of plugin visuals

    # --- Measurement Visuals ---
    def draw_measure_point(self, x: float, y: float):
        """Draws a marker for a measured point."""
        point_item = pg.ScatterPlotItem(
            pos=[(x, y)],
            symbol="o",
            size=6,
            pen=pg.mkPen("cyan", width=1.5, alpha=0.7),
            brush=pg.mkBrush("cyan", alpha=0.7),
        )
        self.view_box.addItem(point_item)
        self.measure_point_items.append(point_item)

    def draw_measure_line(self, p1: QtCore.QPointF, p2: QtCore.QPointF, text: str):
        """Draws a line between two points and displays distance text."""
        if self.measure_line_item and self.measure_line_item.scene():
            self.view_box.removeItem(self.measure_line_item)
        self.measure_line_item = pg.PlotCurveItem(
            x=[p1.x(), p2.x()],
            y=[p1.y(), p2.y()],
            pen=pg.mkPen("cyan", width=2, style=QtCore.Qt.PenStyle.DashLine),
        )
        self.view_box.addItem(self.measure_line_item)

        if self.measure_text_item and self.measure_text_item.scene():
            self.view_box.removeItem(self.measure_text_item)

        mid_x = (p1.x() + p2.x()) / 2
        mid_y = (p1.y() + p2.y()) / 2

        self.measure_text_item = pg.TextItem(
            text, color="cyan", anchor=(0.5, 0), fill=pg.mkBrush(0, 0, 0, 150)
        )
        self.measure_text_item.setPos(mid_x, mid_y)

        font = QtGui.QFont()
        font.setPointSize(12)  # Adjust the font size as needed
        self.measure_text_item.setFont(font)

        self.view_box.addItem(self.measure_text_item)

    def clear_measure_visuals(self):
        """Removes all measurement-related graphics items."""
        for item in self.measure_point_items:
            if item and item.scene():
                try:
                    self.view_box.removeItem(item)
                except Exception:
                    pass
        self.measure_point_items = []

        if self.measure_line_item and self.measure_line_item.scene():
            try:
                self.view_box.removeItem(self.measure_line_item)
            except Exception:
                pass
            self.measure_line_item = None

        if self.measure_text_item and self.measure_text_item.scene():
            try:
                self.view_box.removeItem(self.measure_text_item)
            except Exception:
                pass
            self.measure_text_item = None

    def display_blank_image(self):
        """Display a blank placeholder image when no data is available."""
        try:
            # Create a small blank image
            blank_image = np.zeros((512, 512), dtype=np.float32)

            # Display the blank image
            if self.img_item is None:
                self.img_item = pg.ImageItem()
                self.view_box.addItem(self.img_item)

            self.img_item.setImage(blank_image)
            self.hist_lut.setLevels(0, 1)  # Set reasonable contrast

            logger.debug("display_blank_image: Displayed blank image.")

        except Exception as e:
            logger.error(
                f"display_blank_image: Error displaying blank image: {e}", exc_info=True
            )

    def show_filter_label(self, filter_type: str):
        if filter_type and filter_type != "None":
            self.filter_label.setText(f"Filter: {filter_type}")
            self.filter_label.setPos(10, 10)
            self.filter_label.show()
        else:
            self.hide_filter_label()

    def show_filter_waiting_label(self, filter_type: str):
        if filter_type and filter_type != "None":
            self.filter_label.setText(f"Applying {filter_type}...")
            self.filter_label.setPos(10, 10)
            self.filter_label.show()
        else:
            self.hide_filter_label()

    def hide_filter_label(self):
        self.filter_label.hide()

    def show_sum_label(self, start_frame: int, end_frame: int):
        self.sum_label.setText(
            f"Summed {end_frame - start_frame + 1} frames ({start_frame + 1}-{end_frame + 1})"
        )
        self.sum_label.setPos(10, 40)  # Below filter label
        self.sum_label.show()

    def hide_sum_label(self):
        self.sum_label.hide()

    def show_calibration_label(self):
        self.calibration_label.setText("Calibration...")
        self.calibration_label.setPos(10, 70)  # Below filter/sum labels
        self.calibration_label.show()

    def hide_calibration_label(self):
        self.calibration_label.hide()

    def display_image_stats_overlay(self, stats_text, max_x, max_y):
        # Remove any existing overlay
        self.clear_image_stats_overlay()
        # Create text item for stats
        self._stats_text_item = pg.TextItem(
            text=stats_text,
            color=(255, 255, 255),  # White text
            anchor=(0.0, 0.0),
            border=pg.mkPen(color=(0, 0, 0), width=2),  # Black border
            fill=pg.mkBrush(color=(0, 0, 0, 180)),  # Semi-transparent black background
        )
        self._stats_text_item.setPos(10, 10)  # Top-left corner
        self.view_box.addItem(self._stats_text_item)
        # Create marker for max pixel
        self._max_marker_item = pg.ScatterPlotItem(
            x=[max_x],
            y=[max_y],
            symbol="o",
            size=15,
            pen=pg.mkPen(color=(255, 0, 0), width=2),
            brush=pg.mkBrush(color=(255, 0, 0, 100)),
        )
        self.view_box.addItem(self._max_marker_item)

    def clear_image_stats_overlay(self):
        # Remove overlay items if present
        if hasattr(self, "_stats_text_item") and self._stats_text_item:
            self.view_box.removeItem(self._stats_text_item)
            self._stats_text_item = None
        if hasattr(self, "_max_marker_item") and self._max_marker_item:
            self.view_box.removeItem(self._max_marker_item)
            self._max_marker_item = None

    def show_mask_overlay(self, mask):
        import numpy as np
        import pyqtgraph as pg

        logger = get_logger(__name__)
        if self.mask_overlay_item is not None:
            self.view_box.removeItem(self.mask_overlay_item)
            self.mask_overlay_item = None
        if mask is None:
            logger.debug("show_mask_overlay: mask is None, nothing to show.")
            return
        # Create RGBA image: masked pixels are semi-transparent red, others fully transparent
        h, w = mask.shape
        rgba = np.zeros((h, w, 4), dtype=np.ubyte)
        rgba[mask] = [255, 0, 0, 100]  # Red, alpha=100
        self.mask_overlay_item = pg.ImageItem(rgba, opacity=1.0)
        self.mask_overlay_item.setZValue(10)
        self.mask_overlay_item.setOpts(axisOrder="row-major")
        self.view_box.addItem(self.mask_overlay_item)

    def hide_mask_overlay(self):
        if self.mask_overlay_item is not None:
            self.view_box.removeItem(self.mask_overlay_item)
            self.mask_overlay_item = None

    # --- Plugin Info Text ---
    def display_plugin_info_text(self, text: str):
        """
        Displays a text box in the top-right corner of the view.
        """
        if not text:
            self.clear_plugin_info_text()
            return

        # Get the current visible rectangle of the data
        view_rect = self.view_box.viewRect()
        if not view_rect:
            logger.warning(
                "Cannot display plugin info text: viewRect is not available."
            )
            return

        self.plugin_info_text_item.setHtml(text)
        self.plugin_info_text_item.setPos(10, 10)

        self.plugin_info_text_item.show()

    def clear_plugin_info_text(self):
        """Hides the plugin information text box."""
        self.plugin_info_text_item.hide()

    def show_bad_pixels_overlay(self, coords_rc: np.ndarray):
        """Displays bad pixel candidates as yellow '+' markers."""
        if coords_rc is None or len(coords_rc) == 0:
            self.bad_pixel_overlay_item.setData([], [])
            return
        # Input is (row, col), but scatter plot needs (x, y) which is (col, row)
        self.bad_pixel_overlay_item.setData(x=coords_rc[:, 1], y=coords_rc[:, 0])

    def clear_bad_pixels_overlay(self):
        """Clears the bad pixel overlay."""
        if self.bad_pixel_overlay_item:
            self.bad_pixel_overlay_item.setData([], [])

    def enable_roi_selection(self, callback):
        """Enables the ROI selection overlay for capturing mouse events."""
        if self.roi_overlay:
            self.disable_roi_selection()
        
        self.roi_overlay = ROISelectionOverlay(self.view_box)
        self.view_box.addItem(self.roi_overlay, ignoreBounds=True)
        self.roi_overlay.sigSelectionFinished.connect(callback)

    def disable_roi_selection(self):
        """Disables and removes the ROI selection overlay."""
        if self.roi_overlay:
            self.view_box.removeItem(self.roi_overlay)
            self.roi_overlay = None

    # --- Ice Ring Visuals ---
    def display_ice_rings(self, center_x, center_y, rings_data):
        """
        Displays ice rings on the image.
        rings_data: List of tuples (radius_pixels, resolution_angstrom)
        """
        self.clear_ice_rings()
        
        if not rings_data or center_x is None or center_y is None:
            return

        for r_px, res_ang in rings_data:
            if r_px <= 0: continue
            
            # Draw Circle
            diameter = 2 * float(r_px)
            x = float(center_x - r_px)
            y = float(center_y - r_px)
            
            ring_item = QtWidgets.QGraphicsEllipseItem(x, y, diameter, diameter)
            # Use a distinctive color, e.g., dashed red/magenta
            ring_item.setPen(pg.mkPen("r", width=2, style=QtCore.Qt.DashLine))
            self.view_box.addItem(ring_item)
            self.ice_ring_items.append(ring_item)
            
            # Label
            label_angle = np.pi * 0.75 # Top-leftish
            label_x = float(center_x) + (r_px + 10) * np.cos(label_angle)
            label_y = float(center_y) + (r_px + 10) * np.sin(label_angle)
            
            label_item = pg.TextItem(
                text=f"{res_ang:.2f}Å",
                color="r",
                anchor=(0.5, 0.5)
            )
            label_item.setPos(label_x, label_y)
            self.view_box.addItem(label_item)
            self.ice_ring_items.append(label_item)

    def clear_ice_rings(self):
        """Removes ice ring visuals."""
        for item in self.ice_ring_items:
            if item.scene():
                try:
                    self.view_box.removeItem(item)
                except Exception:
                    pass
        self.ice_ring_items = []
        
        if self.ice_ring_summary_label and self.ice_ring_summary_label.scene():
             try:
                 self.view_box.removeItem(self.ice_ring_summary_label)
             except Exception:
                 pass
        self.ice_ring_summary_label = None

    def display_ice_ring_summary(self, text):
        """Displays a summary text box for ice analysis."""
        if self.ice_ring_summary_label:
             try:
                 self.view_box.removeItem(self.ice_ring_summary_label)
             except Exception:
                 pass
        
        self.ice_ring_summary_label = pg.TextItem(
            text=text,
            color="r",
            anchor=(0, 0),
            border=pg.mkPen("r", width=1),
            fill=pg.mkBrush(0, 0, 0, 200)
        )
        # Position at top-left, slightly below stats if present
        self.ice_ring_summary_label.setPos(10, 40) 
        self.view_box.addItem(self.ice_ring_summary_label)