import argparse
import glob
import os
import sys

os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"

from qp2.log.logging_config import get_logger, setup_logging

logger = get_logger(__name__)


def load_files_from_pattern(pattern, recursive=False):
    logger.debug(f"load_files_from_pattern: pattern: {pattern}, recursive: {recursive}")
    if not pattern:
        return []

    def is_valid_master(fpath):
        # Must end in .nxs OR (_master.h5 / _master.hdf5)
        if fpath.endswith(".nxs"):
            return True
        if fpath.endswith("_master.h5") or fpath.endswith("_master.hdf5"):
            return True
        return False

    # Case 1: Pattern is a file (relative or absolute)
    if os.path.isfile(pattern):
        # Sub-case 1a: Valid master file
        if is_valid_master(pattern):
            if os.access(pattern, os.R_OK):  # Check if readable
                return [os.path.abspath(pattern)]
            else:
                logger.warning(f"File exists but is not readable: {pattern}")
                return []
        
        # Sub-case 1b: Text file containing list of files
        # We try to read it as a text file list.
        try:
            # Check size to avoid reading massive binaries into memory inadvertently
            if os.path.getsize(pattern) > 10 * 1024 * 1024: # 10MB limit for list files
                logger.warning(f"File {pattern} is too large to be a file list. Skipping.")
                return []

            with open(pattern, 'r') as f:
                # Read first chunk to check for binary characters
                first_chunk = f.read(1024)
                if "\0" in first_chunk:
                    logger.warning(f"Invalid file type: {pattern}. Binary file detected (not a valid master file or text list).")
                    return []
                
                # Rewind and read lines
                f.seek(0)
                lines = f.readlines()
            
            logger.info(f"Parsing file list: {pattern}")
            found_files = []
            
            for line in lines:
                line = line.strip()
                # Remove inline comments
                if "#" in line:
                    line = line.split("#")[0].strip()
                
                if not line:
                    continue
                
                # Recursive call to handle the file path/pattern in the list
                # Paths in list are treated relative to CWD (standard CLI behavior)
                found = load_files_from_pattern(line, recursive=recursive)
                found_files.extend(found)
            
            return found_files

        except UnicodeDecodeError:
            logger.warning(f"Invalid file type: {pattern}. Not a text file.")
            return []
        except Exception as e:
            logger.warning(f"Error reading potential file list {pattern}: {e}")
            return []

    # Case 2: Pattern is a directory (relative or absolute)
    if (
        os.path.isdir(pattern)
        and "*" not in pattern
        and "?" not in pattern
        and "[" not in pattern
    ):
        files = []
        # We search for everything and filter later, or search specifically
        patterns_to_search = ["*.nxs", "*_master.h5", "*_master.hdf5"]
        
        for p in patterns_to_search:
            if recursive:
                search_pattern = os.path.join(pattern, "**", p)
                found = glob.glob(search_pattern, recursive=True)
            else:
                search_pattern = os.path.join(pattern, p)
                found = glob.glob(search_pattern)
            files.extend(found)
    else:
        # Case 3: Pattern has wildcards, treat as glob pattern anywhere
        files = glob.glob(pattern)
        # Filter strictly
        files = [f for f in files if is_valid_master(f)]

    readable_files = [
        os.path.abspath(f) for f in files if os.path.isfile(f) and os.access(f, os.R_OK)
    ]
    
    # Extra safety: filter out data files if logic above missed them (though is_valid_master should catch them)
    readable_files = [f for f in readable_files if "_data_" not in os.path.basename(f)]
    
    logger.debug(f"files: {readable_files}")
    return sorted(readable_files)


def main():
    # Lazy import Qt and pyqtgraph inside main() to reduce import-time footprint
    from pyqtgraph.Qt import QtWidgets
    import pyqtgraph as pg

    parser = argparse.ArgumentParser(
        description="View HDF5 diffraction images with live update option."
    )
    parser.add_argument(
        "master_files",  # Renamed for clarity
        nargs="*",  # '*' means 0 or more arguments
        help="One or more master_files or directories containing master_files. If wild cards are used, double quotes are required. If omitted, tries Redis.",
    )
    parser.add_argument(
        "--log-level",
        default="DEBUG",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: DEBUG)",
    )

    parser.add_argument("--version", action="version", version="%(prog)s 1.0")
    parser.add_argument("--live", action="store_true", help="Connect to Redis stream on startup (live mode)")
    parser.add_argument("--nolive", action="store_true", help="Start in offline mode (default; kept for backward compatibility)")
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Search for master files recursively when a directory is provided.",
    )
    args = parser.parse_args()

    setup_logging(root_name="qp2", log_level=args.log_level)

    try:
        from qp2.config.servers import ServerConfig
        ServerConfig.log_all_configs()
    except Exception as e:
        logger.warning(f"Failed to log server configurations: {e}")

    pg.setConfigOptions(imageAxisOrder="row-major")  # Crucial for numpy arrays
    pg.setConfigOption("background", "d")  # Dark background 'd' or 'k'
    pg.setConfigOption("foreground", "w")  # Light foreground 'w'

    all_master_files = []
    if args.master_files:
        for pattern in args.master_files:
            all_master_files.extend(
                load_files_from_pattern(pattern, recursive=args.recursive)
            )

    # Remove duplicates and sort
    all_master_files = sorted(list(set(all_master_files)))

    # Determine initial file path but DO NOT load it yet
    initial_master_file = all_master_files[0] if all_master_files else None

    # Determine if we should start in live mode.
    # Live mode requires --live to be explicitly requested; offline is the default.
    start_in_live_mode = args.live and not args.nolive and initial_master_file is None

    # We no longer query Redis here. We'll pass a flag to the main window to do it after it's visible.
    query_redis_for_initial_file = start_in_live_mode and not initial_master_file

    app = QtWidgets.QApplication(sys.argv)

    try:
        # Lazy import: only import the viewer when actually starting the application.
        from qp2.image_viewer.ui.image_viewer import DiffractionViewerWindow

        main_window = DiffractionViewerWindow(
            initial_file_path=initial_master_file,
            all_file_paths=all_master_files,
            live_mode=start_in_live_mode,
            query_redis_for_initial_file=query_redis_for_initial_file,
        )
        main_window.show()
        sys.exit(app.exec_())

    except Exception as e:
        # Catch-all for unexpected errors during setup
        logger.error(
            f"Unhandled exception during application startup: {e}", exc_info=True
        )
        import traceback

        traceback.print_exc()
        QtWidgets.QMessageBox.critical(
            None, "Application Startup Error", f"An unexpected error occurred:\n{e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
