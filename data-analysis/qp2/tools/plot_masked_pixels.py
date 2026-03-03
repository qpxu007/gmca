import sys
import os
import argparse
import numpy as np
import h5py
import multiprocessing
from PyQt5 import QtWidgets, QtCore
import pyqtgraph as pg
from qp2.xio.hdf5_manager import HDF5Reader

# Default maximum number of CPUs to use
DEFAULT_MAX_CPUS = 16

def process_file_chunk(args):
    """
    Worker function to process a single HDF5 data file.
    Args: (file_path, dset_path, mask_value, expected_count)
    Returns: List of pixel counts (length = expected_count). -1 for errors.
    """
    file_path, dset_path, mask_value, expected_count = args
    
    # Default result (error/missing)
    results = [-1] * expected_count

    if not os.path.exists(file_path):
        return results

    try:
        with h5py.File(file_path, 'r') as f:
            if dset_path in f:
                dset = f[dset_path]
                # Check shape and Read all frames in this file at once (vectorized)
                if dset.ndim == 3:
                    # Shape: (N, H, W)
                    data = dset[()]
                    # Count masked pixels along image dimensions (axis 1 and 2)
                    # This is significantly faster than iterating in python
                    counts = np.count_nonzero(data == mask_value, axis=(1, 2))
                    chunk_results = counts.tolist()
                    
                    # Handle case where file has fewer/more frames than expected
                    if len(chunk_results) < expected_count:
                        chunk_results.extend([-1] * (expected_count - len(chunk_results)))
                    elif len(chunk_results) > expected_count:
                        chunk_results = chunk_results[:expected_count]
                    
                    results = chunk_results
                
                elif dset.ndim == 2 and expected_count == 1:
                    # Single frame case
                    data = dset[()]
                    count = np.count_nonzero(data == mask_value)
                    results = [count]
    except Exception:
        # Fail silently for speed/simplicity in worker
        pass
        
    return results

def main():
    parser = argparse.ArgumentParser(description="Analyze and plot masked pixels in HDF5 detector data.")
    parser.add_argument("master_file", help="Path to the HDF5 master file")
    parser.add_argument("-q", "--quit", action="store_true", 
                        help="Automatically quit after displaying the plot (no GUI interaction)")
    parser.add_argument("-t", "--threshold", type=int, default=100,
                        help="Threshold for highlighting large differences (default: 100)")
    parser.add_argument("-s", "--skip-threshold", type=int, default=10,
                        help="Skip displaying plot if max absolute difference is less than this value (default: 10). Set to 0 to disable skipping.")
    parser.add_argument("-n", "--no-display", action="store_true",
                        help="Disable GUI display. Only output the full path of master files with max absolute difference >= skip-threshold.")
    parser.add_argument("-j", "--jobs", type=int, default=DEFAULT_MAX_CPUS,
                        help=f"Maximum number of parallel processes to use (default: {DEFAULT_MAX_CPUS})")
    args = parser.parse_args()

    master_file = args.master_file
    auto_quit = args.quit
    diff_threshold = args.threshold
    skip_threshold = args.skip_threshold
    no_display = args.no_display
    max_jobs = args.jobs

    if not os.path.exists(master_file):
        print(f"Error: File {master_file} not found.", file=sys.stderr)
        sys.exit(1)

    # Get the absolute path of the master file
    master_file_abs = os.path.abspath(master_file)

    # Only create QApplication if we need the GUI
    if not no_display:
        # Create QApplication (Needed for HDF5Reader and Plotting)
        # Note: Created before Pool, but worker does not touch QT, so fork is generally safe on Linux.
        app = QtWidgets.QApplication(sys.argv)
    else:
        app = None

    if not no_display:
        print(f"Loading metadata from {master_file}...")
    try:
        reader = HDF5Reader(master_file, start_timer=False)
    except Exception as e:
        print(f"Failed to initialize reader: {e}", file=sys.stderr)
        sys.exit(1)

    params = reader.get_parameters()
    total_frames = reader.total_frames
    
    # Determine Mask Value
    bit_depth = params.get('bit_depth', 32)
    if not bit_depth: bit_depth = 32
    mask_value = (2 ** bit_depth) - 1
    
    exposure_time = params.get('exposure', 0)
    
    if not no_display:
        print(f"Bit Depth: {bit_depth}, Mask Value: {mask_value}")
        print(f"Total Frames: {total_frames}")

    if total_frames == 0:
        if not no_display:
            print("No frames found.")
        sys.exit(0)

    # Prepare tasks from the reader's frame map
    # reader.frame_map is [(start, end, file_path, dset_path), ...]
    tasks = []
    for start_idx, end_idx, fpath, dpath in reader.frame_map:
        num_frames = end_idx - start_idx
        tasks.append((fpath, dpath, mask_value, num_frames))

    # We are done with the reader for the processing phase
    reader.close()

    # Determine number of CPUs to use (limit to max_jobs or available CPUs, whichever is smaller)
    available_cpus = multiprocessing.cpu_count()
    cpu_count = min(max_jobs, available_cpus)
    
    if not no_display:
        print(f"Starting parallel processing of {len(tasks)} file chunks using {cpu_count} CPUs...")
    
    frame_numbers = []
    masked_pixel_counts = []

    # Run Multiprocessing
    with multiprocessing.Pool(processes=cpu_count) as pool:
        # map maintains order corresponding to tasks
        chunk_results_list = pool.map(process_file_chunk, tasks)

    # Aggregate results
    global_frame_idx = 0
    for chunk_res in chunk_results_list:
        for count in chunk_res:
            global_frame_idx += 1
            if count >= 0:
                frame_numbers.append(global_frame_idx)
                masked_pixel_counts.append(count)

    if not no_display:
        print("Processing complete.")

    if not frame_numbers:
        if not no_display:
            print("No valid frame data extracted.")
        sys.exit(0)

    # --- Calculate differences between consecutive frames ---
    frame_numbers_arr = np.array(frame_numbers)
    masked_pixel_counts_arr = np.array(masked_pixel_counts)
    
    # Calculate differences (diff[i] = counts[i+1] - counts[i])
    differences = np.diff(masked_pixel_counts_arr)
    
    # Find max and min differences
    if len(differences) > 0:
        max_diff = np.max(differences)
        min_diff = np.min(differences)
        max_abs_diff = max(abs(max_diff), abs(min_diff))
        max_diff_idx = np.argmax(differences)
        min_diff_idx = np.argmin(differences)
        max_diff_frame = frame_numbers[max_diff_idx + 1]  # +1 because diff is between i and i+1
        min_diff_frame = frame_numbers[min_diff_idx + 1]
    else:
        max_diff = 0
        min_diff = 0
        max_abs_diff = 0
        max_diff_frame = 0
        min_diff_frame = 0

    # Find frames with large differences (above threshold)
    large_diff_indices = np.where(np.abs(differences) >= diff_threshold)[0]
    # The frame with large diff is at index i+1 (the frame that changed significantly)
    highlight_frame_numbers = []
    highlight_counts = []
    highlight_diffs = []
    for idx in large_diff_indices:
        # Highlight the frame after the change
        highlight_frame_numbers.append(frame_numbers[idx + 1])
        highlight_counts.append(masked_pixel_counts[idx + 1])
        highlight_diffs.append(differences[idx])

    if not no_display:
        print(f"Max difference: {max_diff} (at frame {max_diff_frame})")
        print(f"Min difference: {min_diff} (at frame {min_diff_frame})")
        print(f"Max absolute difference: {max_abs_diff}")
        print(f"Frames with |difference| >= {diff_threshold}: {len(highlight_frame_numbers)}")

    # --- Handle no-display mode ---
    if no_display:
        # Output the full path only if max_abs_diff >= skip_threshold
        if skip_threshold <= 0 or max_abs_diff >= skip_threshold:
            print(master_file_abs)
            # Print frame numbers above threshold with their differences
            if highlight_frame_numbers:
                print(f"  Frames above threshold ({diff_threshold}):")
                for frame_num, diff_val in zip(highlight_frame_numbers, highlight_diffs):
                    print(f"    Frame {frame_num}: diff = {int(diff_val)}")
        sys.exit(0)

    # --- Check if we should skip displaying the plot ---
    if skip_threshold > 0 and max_abs_diff < skip_threshold:
        print(f"Skipping display: max absolute difference ({max_abs_diff}) is less than skip threshold ({skip_threshold})")
        sys.exit(0)

    # --- Plotting ---
    win = pg.GraphicsLayoutWidget(show=True, title="Masked Pixel Analysis")
    win.resize(800, 600)
    win.setWindowTitle(f"Masked Pixels - {os.path.basename(master_file)}")

    plot = win.addPlot(title="Masked Pixels per Frame")
    plot.setLabel('bottom', "Frame Number")
    plot.setLabel('left', "Count of Masked Pixels")
    plot.showGrid(x=True, y=True)

    # Plot all points in blue
    plot_item = plot.plot(frame_numbers, masked_pixel_counts, pen=None, symbol='o', symbolBrush='b', symbolSize=5)
    
    # Overlay highlighted points (large differences) in red
    if highlight_frame_numbers:
        highlight_plot = plot.plot(
            highlight_frame_numbers, 
            highlight_counts, 
            pen=None, 
            symbol='o', 
            symbolBrush='r', 
            symbolSize=8
        )

    # Add markers for max and min difference frames
    if len(differences) > 0:
        # Mark max diff frame with a green triangle
        plot.plot(
            [max_diff_frame], 
            [masked_pixel_counts[max_diff_idx + 1]], 
            pen=None, 
            symbol='t', 
            symbolBrush='g', 
            symbolSize=12
        )
        # Mark min diff frame with a yellow triangle (pointing down)
        plot.plot(
            [min_diff_frame], 
            [masked_pixel_counts[min_diff_idx + 1]], 
            pen=None, 
            symbol='t1', 
            symbolBrush='y', 
            symbolSize=12
        )

    # Click handler
    def on_point_clicked(points):
        if points:
            clicked_point = points[0]
            frame_num = int(clicked_point.pos().x())
            count = int(clicked_point.pos().y())
            # Find the difference for this frame if available
            if frame_num in frame_numbers:
                idx = frame_numbers.index(frame_num)
                if idx > 0:
                    diff_from_prev = masked_pixel_counts[idx] - masked_pixel_counts[idx - 1]
                    diff_info = f"\nDiff from previous: {diff_from_prev}"
                else:
                    diff_info = "\n(First frame)"
            else:
                diff_info = ""
            QtWidgets.QMessageBox.information(
                win, 
                "Point Clicked", 
                f"Frame Number: {frame_num}\nMasked Pixels: {count}{diff_info}"
            )

    plot_item.sigPointsClicked.connect(lambda _, points: on_point_clicked(points))

    # Build info text with statistics
    text_lines = [
        f"Mean Exposure Time: {exposure_time:.4f} s",
        f"Max Y Diff: {max_diff} (frame {max_diff_frame})",
        f"Min Y Diff: {min_diff} (frame {min_diff_frame})",
        f"Highlight Threshold: {diff_threshold}",
        f"Highlighted Points: {len(highlight_frame_numbers)} (red)",
    ]
    text = "  |  ".join(text_lines)
    
    win.nextRow()
    label_layout = win.addLabel(text, row=1, col=0)
    label_layout.setText(text, size='10pt', color='#FFFFFF')

    # Add a legend
    legend = plot.addLegend()
    legend.addItem(pg.PlotDataItem(pen=None, symbol='o', symbolBrush='b', symbolSize=5), "Normal")
    legend.addItem(pg.PlotDataItem(pen=None, symbol='o', symbolBrush='r', symbolSize=8), f"Large Diff (>={diff_threshold})")
    legend.addItem(pg.PlotDataItem(pen=None, symbol='t', symbolBrush='g', symbolSize=12), "Max Diff")
    legend.addItem(pg.PlotDataItem(pen=None, symbol='t1', symbolBrush='y', symbolSize=12), "Min Diff")

    if auto_quit:
        # Show briefly then quit
        QtCore.QTimer.singleShot(100, app.quit)

    sys.exit(app.exec_())

if __name__ == "__main__":
    # Enforce spawn or fork? Default is fork on Linux.
    # Since we create QApp before Pool, fork might be risky if QApp initializes certain FDs.
    # But typically QApp just connects to X11.
    # 'fork' is fastest. 'spawn' is safest but slower startup.
    # Let's stick to default (fork on Linux) as it usually works for pure compute workers.
    main()
