# qp2/xio/test_combiner.py

import json
import os
import sys
from pathlib import Path
import h5py

# Ensure qp2 is in the path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from qp2.xio.hdf5_combiner import DatasetCombiner
from qp2.xio.hdf5_manager import HDF5Reader

def test_combination():
    # Use real test data if available, otherwise skip
    # This is based on typical paths seen in logs
    test_data_dir = Path("/home/qxu/test-data/100hz")
    if not test_data_dir.exists():
        print(f"Test data directory {test_data_dir} not found. Skipping real data test.")
        return

    master_files = list(test_data_dir.glob("*_master.h5"))
    if not master_files:
        print("No master files found in test directory.")
        return

    # Create a mapping: 5 frames from the first master, 5 from the second (if exists)
    mapping = {
        str(master_files[0]): [1, 2, 3, 4, 5]
    }
    if len(master_files) > 1:
        mapping[str(master_files[1])] = [1, 10, 20, 30, 40]

    output_dir = "/tmp/combined_test"
    output_prefix = "combined_result"
    
    # Cleanup previous run
    if os.path.exists(output_dir):
        import shutil
        shutil.rmtree(output_dir)

    combiner = DatasetCombiner(output_dir, output_prefix)
    
    print(f"Starting combination of {len(mapping)} datasets...")
    success = combiner.combine(mapping, images_per_file=3) # Use small images_per_file to test chunking
    
    if not success:
        print("Combination failed.")
        return

    print("Combination successful. Verifying output...")
    
    output_master = Path(output_dir) / f"{output_prefix}_master.h5"
    if not output_master.exists():
        print(f"Output master file {output_master} was not created!")
        return

    # Verify using the project's HDF5Reader
    try:
        reader = HDF5Reader(str(output_master), start_timer=False)
        params = reader.get_parameters()
        
        print("Metadata verification:")
        print(f"  Total frames: {params['nimages']} (expected {combiner.total_frames_combined})")
        print(f"  Wavelength:   {params['wavelength']}")
        print(f"  Distance:     {params['det_dist']}")
        
        if params['nimages'] != combiner.total_frames_combined:
            print(f"Error: Frame count mismatch! {params['nimages']} != {combiner.total_frames_combined}")
        
        # Check if we can read frames
        print("Reading first and last frame...")
        f1 = reader.get_frame(0)
        flast = reader.get_frame(params['nimages'] - 1)
        
        if f1 is not None and flast is not None:
            print(f"Successfully read frames. Shapes: {f1.shape}, {flast.shape}")
        else:
            print("Failed to read frames from combined dataset.")
            
        reader.close()
    except Exception as e:
        print(f"Verification failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_combination()
