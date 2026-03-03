from typing import Dict, Tuple, Optional

import numpy as np
from PyQt5.QtCore import QRunnable, QObject, pyqtSignal
from PyQt5.QtWidgets import (
    QVBoxLayout,
    QPushButton,
    QLabel,
    QGroupBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QMessageBox,
)
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg as FigureCanvas,
    NavigationToolbar2QT as NavigationToolbar,
)
from matplotlib.figure import Figure

from qp2.image_viewer.utils.ring_math import angstrom_to_pixels
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class CustomNavigationToolbar(NavigationToolbar):
    """Custom navigation toolbar that shows both pixel and Angstrom coordinates."""

    # Signal to emit coordinate updates
    coordinates_updated = pyqtSignal(str)

    def __init__(self, canvas, parent, params=None):
        super().__init__(canvas, parent)
        self.params = params or {}
        self.plot_data = None  # Store plot data for coordinate lookup
        logger.debug(f"CustomNavigationToolbar: Initialized with params: {self.params}")

    def set_plot_data(self, radii, radial_sum, radial_average):
        """Store plot data for coordinate lookup."""
        self.plot_data = {
            "radii": radii,
            "radial_sum": radial_sum,
            "radial_average": radial_average,
        }

    def _mouse_event_to_message(self, event):
        """Override the method that converts mouse events to coordinate messages."""
        if event.inaxes is None:
            return ""

        # Get the original coordinates
        x, y = event.xdata, event.ydata
        if x is None or y is None:
            return ""

        # Convert to Angstrom if we have the necessary parameters
        angstrom_x = self._pixels_to_angstrom(x)

        # Try to get both y-values for dual y-axis plots
        y_sum = y  # This is the primary y-axis value (radial sum)
        y_avg = None

        # Use stored plot data to get both y-values
        if self.plot_data is not None:
            try:
                radii = self.plot_data["radii"]
                radial_sum = self.plot_data["radial_sum"]
                radial_average = self.plot_data["radial_average"]

                # Find closest radius value
                idx = np.argmin(np.abs(radii - x))
                if idx < len(radial_sum) and idx < len(radial_average):
                    y_sum = radial_sum[idx]
                    y_avg = radial_average[idx]
            except Exception as e:
                logger.debug(
                    f"CustomNavigationToolbar: Could not get plot data values: {e}",
                    exc_info=True,
                )

        # Format the message
        if angstrom_x is not None:
            if y_avg is not None:
                # Show both y-values for dual axis plot
                s = f"x={x:.1f} px ({angstrom_x:.2f} Å), sum={y_sum:.1f}, avg={y_avg:.1f}"
            else:
                # Show single y-value
                s = f"x={x:.1f} px ({angstrom_x:.2f} Å), y={y_sum:.1f}"
        else:
            if y_avg is not None:
                # Show both y-values without Angstrom
                s = f"x={x:.1f}, sum={y_sum:.1f}, avg={y_avg:.1f}"
            else:
                # Show single y-value without Angstrom
                s = f"x={x:.1f}, y={y_sum:.1f}"

        # Emit signal for external display
        self.coordinates_updated.emit(s)
        return s

    def set_message(self, s):
        """Override to show both pixel and Angstrom coordinates."""
        logger.debug(f"Original message: {s}")
        logger.debug(f"Available params: {self.params}")

        if s and "x=" in s and "y=" in s:
            try:
                # Extract x and y values from the original message
                # Format is typically "x=123.4, y=567.8"
                parts = s.split(",")
                x_part = parts[0].strip()
                y_part = parts[1].strip()

                x_val = float(x_part.split("=")[1])
                y_val = float(y_part.split("=")[1])

                logger.debug(
                    f"CustomNavigationToolbar: Extracted values: x={x_val}, y={y_val}"
                )

                # Convert to Angstrom if we have the necessary parameters
                angstrom_x = self._pixels_to_angstrom(x_val)

                if angstrom_x is not None:
                    # Show both pixel and Angstrom coordinates
                    s = f"x={x_val:.1f} px ({angstrom_x:.2f} Å), y={y_val:.1f}"
                    logger.debug(f"CustomNavigationToolbar: Converted message: {s}")
                else:
                    # Fall back to original format
                    s = f"x={x_val:.1f}, y={y_val:.1f}"
                    logger.debug(f"CustomNavigationToolbar: Fallback message: {s}")

            except (ValueError, IndexError, AttributeError) as e:
                # If conversion fails, use original message
                logger.debug(
                    f"CustomNavigationToolbar: Could not convert coordinates: {e}",
                    exc_info=True,
                )
                pass

        super().set_message(s)
        # Emit signal for coordinate updates
        self.coordinates_updated.emit(s)

    def _pixels_to_angstrom(self, radius_pixels):
        """Convert radius in pixels to d-spacing in Angstroms."""
        try:
            wavelength = self.params.get("wavelength", 1.0)
            det_dist = self.params.get("det_dist", 100.0)
            pixel_size = self.params.get("pixel_size", 0.075)

            logger.debug(
                f"CustomNavigationToolbar: Conversion params: wavelength={wavelength}, det_dist={det_dist}, pixel_size={pixel_size}"
            )
            logger.debug(
                f"CustomNavigationToolbar: Input radius_pixels: {radius_pixels}"
            )

            if wavelength <= 0 or det_dist <= 0 or pixel_size <= 0:
                logger.debug(
                    f"CustomNavigationToolbar: Invalid parameters: wavelength={wavelength}, det_dist={det_dist}, pixel_size={pixel_size}"
                )
                return None

            # Convert radius in pixels to mm
            radius_mm = radius_pixels * pixel_size
            logger.debug(f"CustomNavigationToolbar: Radius in mm: {radius_mm}")

            # Calculate scattering angle (2-theta)
            two_theta = np.arctan(radius_mm / det_dist)
            logger.debug(f"CustomNavigationToolbar: 2-theta: {two_theta}")

            # Calculate d-spacing using Bragg's law
            # wavelength = 2 * d * sin(theta)
            # d = wavelength / (2 * sin(theta))
            theta = two_theta / 2
            d_spacing = wavelength / (2 * np.sin(theta))

            logger.debug(f"CustomNavigationToolbar: Calculated d-spacing: {d_spacing}")
            return d_spacing

        except Exception as e:
            logger.debug(
                f"CustomNavigationToolbar: Error converting pixels to Angstrom: {e}",
                exc_info=True,
            )
            return None


def calculate_radial_statistics(
        image: np.ndarray,
        center: Tuple[float, float],
        max_radius: Optional[float] = None,
        detector_mask: Optional[np.ndarray] = None,
) -> Dict:
    """
    Calculate comprehensive radial statistics of an image.

    This function now uses the optimized implementation for better performance.

    Args:
        image: 2D numpy array representing the image
        center: Tuple of (x, y) coordinates for the center point
        max_radius: Maximum radius to calculate
        detector_mask: Boolean mask to mask out pixels
    Returns:
        Dictionary containing radial statistics
    """
    # Import here to avoid circular imports
    from qp2.image_viewer.utils.radial_utils import (
        calculate_radial_statistics_optimized,
    )

    result = calculate_radial_statistics_optimized(
        image, center, max_radius, detector_mask
    )

    # Add the extra fields that the original function returned
    result["max_radius"] = (
        max_radius
        if max_radius is not None
        else result.get("max_radius", len(result["radii"]) - 1)
    )
    result["center"] = center

    return result


class RadialSumSignals(QObject):
    """Signals for radial sum worker."""

    finished = pyqtSignal(dict)  # Radial statistics
    error = pyqtSignal(str)  # Error message


class RadialSumWorker(QRunnable):
    """Worker for calculating radial sum in background thread."""

    def __init__(
            self,
            image: np.ndarray,
            center: Tuple[float, float],
            max_radius: Optional[float] = None,
            detector_mask: Optional[np.ndarray] = None,
    ):
        super().__init__()
        self.image = image
        self.center = center
        self.max_radius = max_radius
        self.detector_mask = detector_mask
        self.signals = RadialSumSignals()

    def run(self):
        """Execute radial sum calculation in background thread."""
        try:
            logger.info(f"Starting radial sum calculation from center: {self.center}")

            # Calculate radial statistics
            stats = calculate_radial_statistics(
                self.image, self.center, self.max_radius, self.detector_mask
            )

            logger.info(f"Radial sum calculation complete.")
            self.signals.finished.emit(stats)

        except Exception as e:
            logger.error(
                f"RadialSumWorker: Error in radial sum calculation: {e}", exc_info=True
            )
            self.signals.error.emit(str(e))


class RadialSumDialog(QDialog):
    """Dialog for displaying radial sum results."""

    def __init__(self, radial_stats: Dict, parent=None, frame_number=None):
        super().__init__(parent)
        self.radial_stats = radial_stats
        self.parent = parent
        self.frame_number = frame_number
        self.setup_ui()

    def setup_ui(self):
        """Setup the dialog UI."""
        self.setWindowTitle("Radial Sum Analysis")
        self.setMinimumSize(800, 600)

        layout = QVBoxLayout()

        # Create matplotlib figure
        self.figure = Figure(figsize=(10, 8))
        self.canvas = FigureCanvas(self.figure)

        # Get detector parameters for coordinate conversion
        params = {}
        if self.parent and hasattr(self.parent, "get_params"):
            params = self.parent.get_params() or {}
            logger.debug(f"RadialSumDialog: Retrieved params from parent: {params}")
        else:
            logger.debug(
                f"RadialSumDialog: Parent has no get_params method or is None: {self.parent}"
            )

        self.toolbar = CustomNavigationToolbar(self.canvas, self, params)
        layout.addWidget(self.toolbar)
        layout.addWidget(self.canvas)

        # Statistics panel
        stats_group = QGroupBox("Statistics")
        stats_layout = QVBoxLayout()

        # Display key statistics
        stats_text = f"""
        Center: ({self.radial_stats['center'][0]:.1f}, {self.radial_stats['center'][1]:.1f})
        Max Radius: {self.radial_stats['max_radius']:.1f} pixels
        Total Intensity: {self.radial_stats['total_intensity']:.2e}
        Mean Intensity: {self.radial_stats['mean_intensity']:.2f}
        Max Intensity: {self.radial_stats['max_intensity']:.2f}
        """

        stats_label = QLabel(stats_text)
        stats_layout.addWidget(stats_label)
        stats_group.setLayout(stats_layout)
        layout.addWidget(stats_group)

        # Add save button
        save_button = QPushButton("Save Profile Data...")
        save_button.clicked.connect(self.save_profile_data)
        layout.addWidget(save_button)

        # Buttons
        button_box = QDialogButtonBox(QDialogButtonBox.Ok)
        button_box.accepted.connect(self.accept)
        layout.addWidget(button_box)

        self.setLayout(layout)

        # Plot the radial sum
        self.plot_radial_sum()

    def plot_radial_sum(self):
        """Plot the radial sum and average data with dual y-axes."""
        self.figure.clear()

        # Create subplots
        gs = self.figure.add_gridspec(2, 1, height_ratios=[3, 1])

        # Main plot with dual y-axes
        ax1 = self.figure.add_subplot(gs[0])
        radii = self.radial_stats["radii"]
        radial_sum = self.radial_stats["radial_sum"]
        radial_average = self.radial_stats["radial_average"]

        # Find the meaningful range (up to max_radius)
        max_radius = self.radial_stats["max_radius"]
        meaningful_mask = radii <= max_radius

        # Plot only the meaningful range
        meaningful_radii = radii[meaningful_mask]
        meaningful_radial_sum = radial_sum[meaningful_mask]
        meaningful_radial_average = radial_average[meaningful_mask]

        # Plot radial sum on left y-axis (primary)
        line1 = ax1.plot(
            meaningful_radii,
            meaningful_radial_sum,
            "b-",
            linewidth=2,
            label="Radial Sum",
        )
        ax1.set_xlabel("Radius (pixels)")
        ax1.set_ylabel("Intensity Sum", color="b")
        ax1.tick_params(axis="y", labelcolor="b")

        # Create right y-axis for radial average
        ax2 = ax1.twinx()
        line2 = ax2.plot(
            meaningful_radii,
            meaningful_radial_average,
            "r-",
            linewidth=2,
            label="Radial Average",
        )
        ax2.set_ylabel("Intensity Average", color="r")
        ax2.tick_params(axis="y", labelcolor="r")

        # Set title and limits
        if self.frame_number is not None:
            ax1.set_title(
                f"Radial Intensity Profile (Frame {self.frame_number}, Max Radius: {max_radius:.0f} pixels)"
            )
        else:
            ax1.set_title(
                f"Radial Intensity Profile (Max Radius: {max_radius:.0f} pixels)"
            )
        ax1.set_xlim(0, max_radius)
        ax1.grid(True, alpha=0.3)

        # Add hash marks at x-axis for specific d-spacings (angstroms)
        try:
            params = self.parent.get_params()
            wavelength = params.get("wavelength", 1.0)
            det_dist = params.get("det_dist", 100.0)
            pixel_size = params.get("pixel_size", 0.075)
            d_spacings = [20, 3.7, 2.5, 2.0, 1.8]
            tick_positions = []
            tick_labels = []
            for d in d_spacings:
                try:
                    px = angstrom_to_pixels(d, wavelength, det_dist, pixel_size)
                    if 0 < px <= max_radius:
                        tick_positions.append(px)
                        tick_labels.append(f"{d}")
                except Exception:
                    continue
            if tick_positions:
                # Draw red hash lines and rotated labels above the axis
                ylim = ax1.get_ylim()
                for px, label in zip(tick_positions, tick_labels):
                    ax1.axvline(
                        px, color="red", linestyle="--", alpha=0.8, linewidth=1.5
                    )
                    # Place label above the hash line, slightly above the bottom of the plot
                    offset = 15  # pixels to the right
                    ax1.text(
                        px + offset,
                        ylim[0] + 0.05 * (ylim[1] - ylim[0]),
                        f"{label} \u00c5",
                        color="red",
                        fontsize=10,
                        ha="left",
                        va="bottom",
                        rotation=90,
                        clip_on=True,
                    )
        except Exception as e:
            logger.warning(
                f"RadialSumDialog: Could not add angstrom ticks: {e}", exc_info=True
            )
        # Combine legends
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc="upper right")

        # Log scale plot with dual y-axes for both radial sum and average
        ax3 = self.figure.add_subplot(gs[1])
        valid_mask = (radial_sum > 0) & meaningful_mask
        valid_avg_mask = (radial_average > 0) & meaningful_mask

        if np.any(valid_mask):
            # Plot radial sum on left y-axis (primary)
            line3 = ax3.semilogy(
                radii[valid_mask],
                radial_sum[valid_mask],
                "g-",
                linewidth=1,
                label="Radial Sum",
            )
            ax3.set_xlabel("Radius (pixels)")
            ax3.set_ylabel("Intensity Sum (log scale)", color="g")
            ax3.tick_params(axis="y", labelcolor="g")

            # Create right y-axis for radial average
            ax4 = ax3.twinx()
            if np.any(valid_avg_mask):
                line4 = ax4.semilogy(
                    radii[valid_avg_mask],
                    radial_average[valid_avg_mask],
                    "m-",
                    linewidth=1,
                    label="Radial Average",
                )
                ax4.set_ylabel("Intensity Average (log scale)", color="m")
                ax4.tick_params(axis="y", labelcolor="m")

                # Combine legends for log scale plot
                lines_log = line3 + line4
                labels_log = [l.get_label() for l in lines_log]
                ax3.legend(lines_log, labels_log, loc="upper right")
            else:
                ax3.legend(line3, ["Radial Sum"], loc="upper right")

            ax3.set_xlim(0, max_radius)
            ax3.grid(True, alpha=0.3)

        self.figure.tight_layout()
        self.canvas.draw()

        # Pass plot data to toolbar for coordinate lookup
        if hasattr(self, "toolbar") and self.toolbar:
            radii = self.radial_stats["radii"]
            radial_sum = self.radial_stats["radial_sum"]
            radial_average = self.radial_stats["radial_average"]
            self.toolbar.set_plot_data(radii, radial_sum, radial_average)

    def save_profile_data(self):
        """Save the radial profile data to a file."""
        try:
            # Get file path from user
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Radial Profile Data",
                f"radial_profile_{self.radial_stats['center'][0]:.0f}_{self.radial_stats['center'][1]:.0f}.txt",
                "Text Files (*.txt);;CSV Files (*.csv);;All Files (*)",
            )

            if not file_path:
                return

            # Prepare data for saving
            radii = self.radial_stats["radii"]
            radial_sum = self.radial_stats["radial_sum"]
            max_radius = self.radial_stats["max_radius"]

            # Filter data to meaningful range
            meaningful_mask = radii <= max_radius
            meaningful_radii = radii[meaningful_mask]
            meaningful_radial_sum = radial_sum[meaningful_mask]

            # Write data to file
            with open(file_path, "w") as f:
                # Write header with metadata
                f.write(f"# Radial Profile Data\n")
                f.write(
                    f"# Center: ({self.radial_stats['center'][0]:.1f}, {self.radial_stats['center'][1]:.1f})\n"
                )
                f.write(f"# Max Radius: {self.radial_stats['max_radius']:.1f} pixels\n")
                f.write(
                    f"# Total Intensity: {self.radial_stats['total_intensity']:.2e}\n"
                )
                f.write(
                    f"# Mean Intensity: {self.radial_stats['mean_intensity']:.2f}\n"
                )
                f.write(f"# Max Intensity: {self.radial_stats['max_intensity']:.2f}\n")

                f.write(f"#\n")
                f.write(
                    f"# Radius(pixels)\tIntensity_Sum\tIntensity_Average\tPixel_Count\n"
                )

                # Write data points
                meaningful_radial_average = self.radial_stats["radial_average"][
                    meaningful_mask
                ]
                meaningful_pixel_counts = self.radial_stats["pixel_counts"][
                    meaningful_mask
                ]
                for radius, intensity_sum, intensity_avg, pixel_count in zip(
                        meaningful_radii,
                        meaningful_radial_sum,
                        meaningful_radial_average,
                        meaningful_pixel_counts,
                ):
                    f.write(
                        f"{radius}\t{intensity_sum}\t{intensity_avg}\t{pixel_count}\n"
                    )

            # Show success message
            QMessageBox.information(
                self, "Save Successful", f"Radial profile data saved to:\n{file_path}"
            )

        except Exception as e:
            logger.error(
                f"RadialSumDialog: Error saving radial profile data: {e}", exc_info=True
            )
            QMessageBox.critical(
                self, "Save Error", f"Failed to save radial profile data:\n{str(e)}"
            )


class RadialSumManager:
    """Manager for radial sum calculations and display."""

    def __init__(self, mw, ui_manager):
        self.mw = mw
        self.ui_manager = ui_manager
        self.current_dialog = None

    def calculate_radial_sum(self, center: Optional[Tuple[float, float]] = None):
        """Calculate and display radial sum of current image."""
        # Get current image
        image = self.mw.get_analysis_image()
        if image is None:
            self.ui_manager.show_status_message(
                "No image available for radial sum analysis", 3000
            )
            return

        # Get center point (use beam center if available, otherwise image center)
        if center is None or center is False:
            params = self.mw.get_params()
            logger.debug(f"RadialSumManager: params = {params}")
            if params and "beam_x" in params and "beam_y" in params:
                beam_x = params["beam_x"]
                beam_y = params["beam_y"]
                # Check if beam coordinates are valid numbers (not bool)
                if (
                        isinstance(beam_x, (int, float))
                        and isinstance(beam_y, (int, float))
                        and not isinstance(beam_x, bool)
                        and not isinstance(beam_y, bool)
                ):
                    center = (float(beam_x), float(beam_y))
                    logger.debug(f"RadialSumManager: Using beam center: {center}")
                else:
                    center = (image.shape[1] / 2, image.shape[0] / 2)
                    logger.debug(f"RadialSumManager: Using image center: {center}")
            else:
                center = (image.shape[1] / 2, image.shape[0] / 2)
                logger.debug(f"RadialSumManager: Using image center: {center}")
        else:
            logger.debug(f"RadialSumManager: Using provided center: {center}")

        # Get the precomputed detector mask from the main window
        detector_mask = getattr(self.mw, "detector_mask", None)
        logger.debug(f"RadialSumManager: detector_mask = {detector_mask}")

        # Calculate max_radius that fits a complete circle within the image bounds
        h, w = image.shape
        cx, cy = center
        dist_to_left = cx
        dist_to_right = w - 1 - cx
        dist_to_top = cy
        dist_to_bottom = h - 1 - cy
        max_radius = min(dist_to_left, dist_to_right, dist_to_top, dist_to_bottom)
        max_radius = max(1, int(max_radius))

        logger.debug(f"RadialSumManager: Calculated max_radius: {max_radius}")

        # Create and start worker
        worker = RadialSumWorker(
            image, center, max_radius=max_radius, detector_mask=detector_mask
        )
        worker.signals.finished.connect(self._handle_radial_sum_complete)
        worker.signals.error.connect(self._handle_radial_sum_error)

        self.ui_manager.show_status_message("Calculating radial sum...", 0)
        self.mw.threadpool.start(worker)

    def _handle_radial_sum_complete(self, stats: Dict):
        """Handle completion of radial sum calculation."""
        self.ui_manager.clear_status_message_if("Calculating radial sum")
        self.ui_manager.show_status_message("Radial sum calculation complete", 3000)

        # Create and show dialog
        # if self.current_dialog:
        #     self.current_dialog.close()

        frame_number = getattr(self.mw, "current_frame_index", None)
        if frame_number is not None:
            frame_number = frame_number + 1  # 1-based for user display
        dialog = RadialSumDialog(
            stats, self.ui_manager.main_window, frame_number=frame_number
        )
        dialog.show()

    def _handle_radial_sum_error(self, error_msg: str):
        """Handle error in radial sum calculation."""
        self.ui_manager.clear_status_message_if("Calculating radial sum")
        self.ui_manager.show_warning_message("Radial Sum Error", error_msg)
