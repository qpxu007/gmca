
#!/usr/bin/env python3
"""
Systematic Validation of Orientation Analysis against cmpmat.

This script:
1. Takes a GXPARM.nXDS file as input.
2. Runs `nxds_orientation_analysis.py --to_xds` to generate XPARM files.
3. Selects random pairs of images.
4. Runs `cmpmat` (via module load autoproc) on the generated XPARM pairs.
5. Parses `cmpmat` output to get the reference misorientation angle.
6. Calculates misorientation using `nxds_orientation_analysis` module.
7. Compares the results and reports statistics.
"""

import os
import sys
import subprocess
import random
import re
import numpy as np
import argparse
from pathlib import Path
import nxds_orientation_analysis as noa
from scipy.spatial.transform import Rotation

def parse_cmpmat_output(output_str):
    """Extract minimum misorientation angle from cmpmat output."""
    # Look for "ANGLE, AXIS" blocks and the following floating point number
    # Or better, look for the blocks and find the minimum angle reported.
    # The output format is:
    #  ==== SYMOP #   n
    #  MAT = ...
    #  ANGLE, AXIS
    #    45.4139...
    
    angles = []
    lines = output_str.splitlines()
    for i, line in enumerate(lines):
        if "ANGLE, AXIS" in line:
            # Next line should have the angle
            try:
                parts = lines[i+1].split()
                if parts:
                    angle = float(parts[0])
                    angles.append(angle)
            except (IndexError, ValueError):
                pass
                
    if not angles:
        return None
    return min(angles)

def run_cmpmat(file1, file2, space_group):
    """Run cmpmat on two files and return the misorientation angle."""
    # cmd = f"module load autoproc && cmpmat {file1} {file2} {space_group}"
    cmd = f"cmpmat {file1} {file2} {space_group}"
    try:
        # Run in shell to handle module load
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, executable="/bin/bash")
        if result.returncode != 0:
            print(f"Error running cmpmat: {result.stderr}")
            return None
        
        angle = parse_cmpmat_output(result.stdout)
        return angle
    except Exception as e:
        print(f"Exception running cmpmat: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Verify orientation analysis against cmpmat")
    parser.add_argument("gxparm", help="Path to GXPARM.nXDS file")
    parser.add_argument("--img-range", nargs=2, type=int, help="Range of images to test (start end)", default=None)
    parser.add_argument("--num-pairs", type=int, default=10, help="Number of random pairs to test")
    parser.add_argument("--keep-xds", action="store_true", help="Keep generated XPARM files")
    parser.add_argument("--slow", action="store_true",
                        help="Use original Kabsch alignment instead of fast vectorized einsum (default)")
    args = parser.parse_args()

    gxparm_path = Path(args.gxparm)
    if not gxparm_path.exists():
        print(f"Error: {gxparm_path} not found")
        sys.exit(1)
        
    # 1. Generate XPARM files
    print(f"Generating XPARM files from {gxparm_path}...")
    # Call the module function directly or run subproces?
    # Let's run subprocess to test the CLI interface too
    cmd = [sys.executable, "nxds_orientation_analysis.py", "--to_xds", str(gxparm_path)]
    subprocess.check_call(cmd)
    
    # 2. Parse GXPARM to get image data and Space Group
    print("Parsing GXPARM to load internal data...")
    gxparm_data = noa.parse_nxds_xparm(str(gxparm_path))
    images = gxparm_data["images"]
    space_group = gxparm_data["header"]["space_group"]
    print(f"Loaded {len(images)} images. Space Group: {space_group}")
    
    # Map image index to XPARM filename
    # The module names files as GXPARM_XPARM_{image_num}.XDS
    # We need to know the mapping from list index to image number.
    # The module uses image filenames to extract numbers.
    
    xparm_files = []
    valid_indices = []
    
    for idx, img in enumerate(images):
        # Extract number using same logic as module
        fname = Path(img["filename"]).name
        match = re.search(r'(?:_|^)(\d+)(?:\.\w+)?$', fname)
        if match:
            img_num = int(match.group(1))
        else:
            img_num = idx + 1 # Fallback
            
        # Determine prefix from directory header (first line of nXDS)
        dir_line = gxparm_data["header"].get("directory", "")
        prefix = "XPARM"
        if dir_line:
             template_name = Path(dir_line).name
             # Remove _??????.h5 or similar pattern
             prefix = re.sub(r'_[?*#\d]+\.[a-zA-Z0-9]+$', '', template_name)
             if not prefix:
                 prefix = "XPARM"
        
        xparm_name = f"{prefix}_XPARM_{img_num}.XDS"
        xparm_path_full = gxparm_path.parent / xparm_name
        
        if xparm_path_full.exists():
            xparm_files.append(xparm_path_full)
            valid_indices.append(idx)
        else:
            # If explicit XPARM not found (maybe filename parsing changed?)
            # Try just check what files exist?
            pass

    print(f"Found {len(xparm_files)} generated XPARM files.")
    if len(xparm_files) < 2:
        print("Not enough XPARM files to test.")
        sys.exit(1)

    # 3. Select Pairs
    pairs = []
    indices = list(range(len(xparm_files)))
    for _ in range(args.num_pairs):
        i1, i2 = random.sample(indices, 2)
        pairs.append((i1, i2))
        
    # 4. Run Tests
    results = []
    
    # Pre-calculate symmetry operators
    sym_ops = noa.get_point_group_operators(space_group)
    
    print(f"\nTesting {len(pairs)} pairs...")
    print(f"{'Images':<20} | {'cmpmat':<10} | {'Module':<10} | {'Diff':<10} | {'Status'}")
    print("-" * 70)
    
    passed = 0
    
    for i1, i2 in pairs:
        file1 = xparm_files[i1]
        file2 = xparm_files[i2]
        img1_data = images[valid_indices[i1]]
        img2_data = images[valid_indices[i2]]
        
        # Run cmpmat
        angle_cmp = run_cmpmat(file1, file2, space_group)
        if angle_cmp is None:
            print(f"{file1.name} - {file2.name}: cmpmat failed")
            continue
            
        # Run Module Logic
        # Construct A matrices
        a1, b1, c1 = img1_data["a_axis"], img1_data["b_axis"], img1_data["c_axis"]
        a2, b2, c2 = img2_data["a_axis"], img2_data["b_axis"], img2_data["c_axis"]
        
        A1 = np.column_stack([a1, b1, c1])
        A2 = np.column_stack([a2, b2, c2])
        
        # Calculate pair misorientation (using condensed logic single pair)
        # Or just use low-level
        # Reuse pairwise function logic for single pair to test EXACT path
        # But pairwise takes (N, 3, 3).
        A_stack = np.stack([A1, A2])
        # chunk_size=100
        if args.slow:
            dists = noa.pairwise_misorientation_condensed(A_stack, sym_ops=sym_ops)
        else:
            dists = noa.pairwise_misorientation_condensed_fast(A_stack, sym_ops=sym_ops)
        angle_mod = dists[0]
        
        diff = abs(angle_cmp - angle_mod)
        status = "PASS" if diff < 0.1 else "FAIL"
        if status == "PASS": passed += 1
        
        pair_str = f"{file1.stem.split('_')[-1]}-{file2.stem.split('_')[-1]}"
        print(f"{pair_str:<20} | {angle_cmp:<10.4f} | {angle_mod:<10.4f} | {diff:<10.4f} | {status}")
        
    print("-" * 70)
    print(f"Passed: {passed}/{len(pairs)}")
    
    # Cleanup
    if not args.keep_xds:
        print("\nCleaning up XPARM files...")
        for p in xparm_files:
            try:
                p.unlink()
            except:
                pass

if __name__ == "__main__":
    main()
