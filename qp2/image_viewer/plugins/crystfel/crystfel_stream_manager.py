# Create new file: qp2/image_viewer/workers/crystfel_stream_manager.py

import os

from PyQt5.QtCore import QObject, pyqtSignal, QTimer

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

REDIS_CRYSTFEL_KEY_PREFIX = "analysis:out:crystfel"


class StreamManager(QObject):
    """
    Manages the assembly of a CrystFEL .stream file from pre-computed
    stream segments whose locations are indexed in Redis.
    """

    stream_updated = pyqtSignal(int, int)  # total_chunks, new_chunks

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.redis_conn = main_window.redis_output_server

        self.is_monitoring = False
        self.monitored_datasets = []
        self.written_segments = set()  # Stores (master_file, start_frame_str)

        self.stream_file_path = None  # Set externally by the MergingManager
        self.scan_timer = QTimer(self)
        self.scan_timer.setInterval(10000)  # Scan for new segments every 10 seconds
        self.scan_timer.timeout.connect(self.update_stream_file)

    def set_stream_file_path(self, directory: str, filename: str = "qp2_merged.stream"):
        """Sets the full path for the output stream file."""
        if directory and os.path.isdir(directory):
            self.stream_file_path = os.path.join(directory, filename)
            return True
        logger.error(f"Invalid directory provided for stream file: {directory}")
        self.stream_file_path = None
        return False

    def get_stream_file_path(self):
        return self.stream_file_path

    def update_stream_file(self):
        """
        Checks Redis for new stream segment files and appends them to the main stream.
        """
        if not self.is_monitoring or not self.redis_conn:
            return

        new_chunks_this_scan = 0

        for reader in self.monitored_datasets:
            master_file = reader.master_file_path
            segments_key = f"{REDIS_CRYSTFEL_KEY_PREFIX}:{master_file}:segments"

            try:
                segment_paths = self.redis_conn.hgetall(segments_key)
                if not segment_paths:
                    continue

                sorted_frames = sorted(segment_paths.keys(), key=int)

                new_segments_found = False
                with open(self.stream_file_path, "a") as main_stream_file:
                    for frame_str in sorted_frames:
                        if (master_file, frame_str) not in self.written_segments:
                            new_segments_found = True
                            segment_path = segment_paths[frame_str]
                            if os.path.exists(segment_path):
                                with open(segment_path, "r") as segment_file:
                                    content = segment_file.read()
                                    main_stream_file.write(content)
                                    new_chunks_this_scan += content.count(
                                        "----- Begin chunk -----"
                                    )
                                self.written_segments.add((master_file, frame_str))
                            else:
                                logger.warning(
                                    f"Stream segment file in Redis manifest not found on disk: {segment_path}"
                                )

                if new_segments_found and new_chunks_this_scan > 0:
                    total_chunks = sum(
                        1 for v in self.written_segments
                    )  # This is segments, but a good proxy
                    logger.info(
                        f"Appended {new_chunks_this_scan} new chunks from new segments to {self.stream_file_path}."
                    )
                    self.stream_updated.emit(
                        len(self.written_segments), new_chunks_this_scan
                    )

            except Exception as e:
                logger.error(
                    f"Error scanning Redis for segments from {master_file}: {e}"
                )

    def start_monitoring(self, datasets: list):
        """
        Initializes the stream file with a header from the first available
        segment file and starts periodic updates.
        """
        self.stop_monitoring()
        if not self.stream_file_path:
            logger.error(
                "StreamManager Error: Stream file path must be set before starting monitoring."
            )
            return

        logger.info(f"Starting stream monitoring. Output: {self.stream_file_path}")
        self.monitored_datasets = datasets
        self.written_segments.clear()

        if os.path.exists(self.stream_file_path):
            os.remove(self.stream_file_path)

        # Find the VERY FIRST stream segment file available across all monitored datasets.
        first_segment_path = None
        for reader in self.monitored_datasets:
            master_file = reader.master_file_path
            segments_key = f"{REDIS_CRYSTFEL_KEY_PREFIX}:{master_file}:segments"
            # Get all start frames, sort them numerically, and take the first one
            all_frames = self.redis_conn.hkeys(segments_key)
            if all_frames:
                first_frame = sorted(all_frames, key=int)[0]
                first_segment_path = self.redis_conn.hget(segments_key, first_frame)
                break  # Found the first one, no need to check other datasets

        if first_segment_path and os.path.exists(first_segment_path):
            try:
                with open(first_segment_path, "r") as f_segment:
                    header_lines = []
                    for line in f_segment:
                        if line.startswith("----- Begin chunk -----"):
                            break
                        header_lines.append(line)
                    header_content = "".join(header_lines)

                with open(self.stream_file_path, "w") as f_stream:
                    f_stream.write(header_content)
                logger.info(
                    f"Wrote stream header from segment file: {first_segment_path}"
                )

            except Exception as e:
                logger.error(
                    f"Failed to read header from segment file: {e}", exc_info=True
                )
        else:
            logger.warning(
                "Could not find any stream segment files to extract a header from. Stream file will be built without one."
            )

        self.is_monitoring = True
        self.scan_timer.start()
        self.update_stream_file()

    def stop_monitoring(self):
        """Stops the periodic scanning timer and resets the state."""
        if self.is_monitoring:
            logger.info("Stopping CrystFEL stream manager.")
            self.scan_timer.stop()
            self.is_monitoring = False
