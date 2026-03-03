# -*- coding: utf-8 -*-

import numpy as np
from PyQt5.QtCore import QRunnable, QObject, pyqtSignal, pyqtSlot

# Import improved utilities
from qp2.log.logging_config import get_logger
from qp2.xio.hdf5_manager import HDF5Reader

logger = get_logger(__name__)


class FrameSumSignals(QObject):
    """Defines signals available from the FrameSumWorker thread."""

    finished = pyqtSignal(np.ndarray)  # Emits the summed image data
    error = pyqtSignal(str)  # Emits error message string
    progress = pyqtSignal(
        int, int
    )  # Emits (current_frame_processed, total_frames_to_sum)


class FrameSumWorker(QRunnable):
    """
    Worker thread for summing multiple frames.
    Handles mask values by setting them to 0 before summing,
    and then restoring the original masked values from the first frame.
    """

    def __init__(
            self, reader: HDF5Reader, start_index: int, num_frames: int, detector_mask: np.ndarray = None
    ):
        super().__init__()
        self.reader = reader
        self.start_index = start_index
        self.num_frames = num_frames  # Total frames including the first one
        self.detector_mask = detector_mask
        self.signals = FrameSumSignals()

    @pyqtSlot()
    def run(self):
        """Execute the frame summation."""
        if self.num_frames < 1:
            self.signals.error.emit("Number of frames to sum must be at least 1.")
            return

        if not self.reader:
            self.signals.error.emit("HDF5 reader is not available.")
            return

        boolean_mask = self.detector_mask
        first_frame_data = None
        sum_image = None

        try:
            # --- 1. Read the first frame ---
            logger.debug(f"FrameSumWorker: Reading frame {self.start_index}...")
            self.signals.progress.emit(1, self.num_frames)
            first_frame_data = self.reader.get_frame(self.start_index)
            if first_frame_data is None:
                self.signals.error.emit(
                    f"Failed to read starting frame {self.start_index}."
                )
                return

            # --- 3. Initialize sum image (use float64 for summation) ---
            sum_image = first_frame_data.astype(np.float64, copy=True)

            # Set masked areas to 0 in the initial sum image before summing others
            if boolean_mask is not None and boolean_mask.any():
                sum_image[boolean_mask] = 0.0
                logger.debug(
                    f"FrameSumWorker: Zeroed {np.sum(boolean_mask)} masked pixels in initial sum image."
                )

            # --- 4. Loop through subsequent frames ---
            for i in range(1, self.num_frames):
                current_index = self.start_index + i
                logger.debug(f"FrameSumWorker: Reading frame {current_index}...")
                self.signals.progress.emit(i + 1, self.num_frames)

                frame_data = self.reader.get_frame(current_index)
                if frame_data is None:
                    # Handle missing frame - stop summation? Or skip? Let's stop and report error.
                    self.signals.error.emit(
                        f"Failed to read frame {current_index}. Summation stopped."
                    )
                    return  # Stop processing

                # Convert current frame to float64 for summation
                frame_data_float = frame_data.astype(np.float64, copy=False)

                # Apply mask (set to 0) to current frame before adding
                if boolean_mask is not None and boolean_mask.any():
                    # Apply the mask derived from the *first* frame
                    frame_data_float[boolean_mask] = 0.0

                # Add to the sum
                sum_image += frame_data_float

            # --- 5. Restore original masked values (from first frame) ---
            if boolean_mask is not None and boolean_mask.any():
                logger.debug(
                    f"FrameSumWorker: Restoring original values for {np.sum(boolean_mask)} masked pixels..."
                )
                try:
                    # Use the boolean mask derived from the first frame
                    # Place values from the first frame back into the summed image
                    sum_image[boolean_mask] = first_frame_data[boolean_mask]
                except Exception as restore_err:
                    logger.warning(
                        f"FrameSumWorker: Error during mask restoration: {restore_err}", exc_info=True
                    )

            # --- 6. Emit final result ---
            logger.info("FrameSumWorker: Frame summation complete.")
            self.signals.finished.emit(sum_image)

        except Exception as e:
            logger.error(f"FrameSumWorker: Error during frame summation: {e}", exc_info=True)
            self.signals.error.emit(f"Error during frame summation: {e}")
