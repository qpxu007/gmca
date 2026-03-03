import numpy as np
import matplotlib.pyplot as plt
import time
import os
import sys
from qp2.xio.hdf5_manager import HDF5Reader
from qp2.image_viewer.beamcenter.auto_center import (
    optimize_beam_center,
    remove_spots,
    calculate_robust_radial_score,
    calculate_variance_score,
    generate_asymmetric_image
)

def process_dataset_list(list_file_path):
    if not os.path.exists(list_file_path):
        print(f"Error: File list {list_file_path} not found.")
        return

    output_file = list_file_path + ".optimized_centers.txt"
    print(f"Processing datasets from: {list_file_path}")
    print(f"Writing results to: {output_file}")

    with open(list_file_path, 'r') as f:
        file_paths = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    results = []
    
    # Radial range limits
    R1 = 150 # Inner radius to ignore

    for file_path in file_paths:
        print(f"\nProcessing: {file_path}")
        try:
            reader = HDF5Reader(file_path, start_timer=False)
            meta = reader.get_parameters()
            true_x = meta.get('beam_x', 0)
            true_y = meta.get('beam_y', 0)
            nx = meta.get('nx', 0)
            ny = meta.get('ny', 0)
            
            # Calculate R2
            if nx > 0 and ny > 0:
                corners = [(0, 0), (nx, 0), (0, ny), (nx, ny)]
                R2 = max(np.sqrt((cx - true_x)**2 + (cy - true_y)**2) for cx, cy in corners)
            else:
                R2 = None

            print(f"  Metadata Center: ({true_x:.2f}, {true_y:.2f})")
            
            # Use frame 0 for optimization
            image = reader.get_frame(0)
            if image is None:
                print("  Failed to read frame 0")
                results.append(f"{file_path}\tFAILED_READ\n")
                continue

            mask = image >= meta.get('saturation_value', 2**32-1)
            mask = remove_spots(image, mask)
            initial_guess = [true_x, true_y]

            # Robust Method
            est_robust, _ = optimize_beam_center(image, initial_guess, mask, method='robust', verbose=False, limit=50, min_radius=R1, max_radius=R2)
            
            # Variance Method
            est_var, _ = optimize_beam_center(image, initial_guess, mask, method='variance', verbose=False, limit=50, min_radius=R1, max_radius=R2)

            print(f"  Robust Est: ({est_robust[0]:.2f}, {est_robust[1]:.2f})")
            print(f"  Variance Est: ({est_var[0]:.2f}, {est_var[1]:.2f})")

            results.append(f"{file_path}\t{true_x:.2f},{true_y:.2f}\t{est_robust[0]:.2f},{est_robust[1]:.2f}\t{est_var[0]:.2f},{est_var[1]:.2f}\n")
            
            # Write immediately to disk
            with open(output_file, 'w') as f_out:
                f_out.write("# FilePath\tMetadata(X,Y)\tRobust(X,Y)\tVariance(X,Y)\n")
                f_out.writelines(results)

        except Exception as e:
            print(f"  Error processing {file_path}: {e}")
            results.append(f"{file_path}\tERROR: {e}\n")

    print("\nBatch processing complete.")

def main():
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if os.path.isfile(arg) and arg.endswith(".txt"):
            process_dataset_list(arg)
            return
        elif os.path.isfile(arg) and (arg.endswith(".h5") or arg.endswith(".cbf")):
             file_path = arg
        else:
             file_path = "/home/qxu/test-data/A16_run2_master.h5"
    else:
        file_path = "/home/qxu/test-data/A16_run2_master.h5"

    print(f"=== Beam Center Estimation Comparison ===")
    print(f"File: {file_path}")
    
    try:
        reader = HDF5Reader(file_path, start_timer=False)
    except Exception as e:
        print(f"Failed to open file: {e}")
        return

    meta = reader.get_parameters()
    true_x = meta.get('beam_x', 0)
    true_y = meta.get('beam_y', 0)
    nx = meta.get('nx', 0)
    ny = meta.get('ny', 0)
    
    # Calculate R2 as distance to the furthest corner
    if nx > 0 and ny > 0:
        corners = [(0, 0), (nx, 0), (0, ny), (nx, ny)]
        R2 = max(np.sqrt((cx - true_x)**2 + (cy - true_y)**2) for cx, cy in corners)
    else:
        R2 = None

    print(f"Metadata (True) Center: ({true_x:.2f}, {true_y:.2f})")
    print(f"Detector Dimensions: {nx} x {ny}")
    if R2:
        print(f"Radial Range: R1=150, R2={R2:.1f}")
    else:
        print(f"Radial Range: R1=150, R2=None")
    print("-" * 110)
    print(f"{'Frame':<6} | {'Guess Offset':<15} | {'Robust Est':<18} | {'Err(px)':<8} | {'Time(s)':<8} | {'Variance Est':<18} | {'Err(px)':<8} | {'Time(s)':<8}")
    print("-" * 110)

    # Reduced test set to avoid timeout
    test_frames = [0]
    guess_offsets = [(-20, 20)]
    
    # Radial range limits
    R1 = 150 # Inner radius to ignore

    for frame_idx in test_frames:
        try:
            image = reader.get_frame(frame_idx)
            if image is None: continue
            
            mask = image >= meta.get('saturation_value', 2**32-1)
            mask = remove_spots(image, mask)
            
            for ox, oy in guess_offsets:
                initial_guess = [true_x + ox, true_y + oy]
                
                # Robust Method
                est_robust, time_robust = optimize_beam_center(image, initial_guess, mask, method='robust', verbose=False, limit=50, min_radius=R1, max_radius=R2)
                err_robust = np.sqrt((true_x - est_robust[0])**2 + (true_y - est_robust[1])**2)
                
                # Variance Method
                est_var, time_var = optimize_beam_center(image, initial_guess, mask, method='variance', verbose=False, limit=50, min_radius=R1, max_radius=R2)
                err_var = np.sqrt((true_x - est_var[0])**2 + (true_y - est_var[1])**2)
                
                guess_str = f"({ox:+d}, {oy:+d})"
                rob_str = f"({est_robust[0]:.1f}, {est_robust[1]:.1f})"
                var_str = f"({est_var[0]:.1f}, {est_var[1]:.1f})"
                
                print(f"{frame_idx:<6} | {guess_str:<15} | {rob_str:<18} | {err_robust:<8.2f} | {time_robust:<8.2f} | {var_str:<18} | {err_var:<8.2f} | {time_var:<8.2f}")

        except Exception as e:
            print(f"Error processing frame {frame_idx}: {e}")

if __name__ == "__main__":
    main()
