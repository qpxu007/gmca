#!/usr/bin/env python3
"""
Adjacent Image Orientation Analysis

For images with consecutive numbers (e.g., 100 and 101), assume they come
from the same crystal. Compute the misorientation angle between each
adjacent pair to quantify:
  - How stable the orientation is within one crystal
  - The typical "noise" in orientation determination

Usage:
    python adjacent_orientation_analysis.py /path/to/GXPARM.nXDS
    python adjacent_orientation_analysis.py /path/to/GXPARM.nXDS --no-symmetry
"""

import argparse
import re
import sys
from pathlib import Path
from typing import List, Tuple

import numpy as np

import nxds_orientation_analysis as noa


def extract_image_number(filename: str) -> int:
    """Extract the numeric image number from a filename like 'sample_000123.h5'."""
    fname = Path(filename).name
    match = re.search(r'(?:_|^)(\d+)(?:\.\w+)?$', fname)
    if match:
        return int(match.group(1))
    # Fallback: find last sequence of digits
    nums = re.findall(r'\d+', fname)
    if nums:
        return int(nums[-1])
    return -1


def find_adjacent_pairs(images: list) -> List[Tuple[int, int, int, int]]:
    """Find pairs of images with consecutive image numbers.

    Returns list of (idx1, idx2, img_num1, img_num2) sorted by image number.
    """
    # Extract image numbers and sort
    numbered = []
    for idx, img in enumerate(images):
        num = extract_image_number(img["filename"])
        numbered.append((num, idx))

    numbered.sort(key=lambda x: x[0])

    pairs = []
    for i in range(len(numbered) - 1):
        num1, idx1 = numbered[i]
        num2, idx2 = numbered[i + 1]
        if num2 - num1 == 1:  # Adjacent
            pairs.append((idx1, idx2, num1, num2))

    return pairs


def compute_adjacent_misorientations(
    images: list,
    pairs: List[Tuple[int, int, int, int]],
    sym_ops: np.ndarray,
    slow: bool = False,
) -> List[dict]:
    """Compute misorientation angle for each adjacent pair.

    Uses the same pairwise logic as the main analysis module.
    Default: fast vectorized einsum on rotation matrices.
    """
    results = []

    for idx1, idx2, num1, num2 in pairs:
        img1 = images[idx1]
        img2 = images[idx2]

        a1, b1, c1 = img1["a_axis"], img1["b_axis"], img1["c_axis"]
        a2, b2, c2 = img2["a_axis"], img2["b_axis"], img2["c_axis"]

        A1 = np.column_stack([a1, b1, c1])
        A2 = np.column_stack([a2, b2, c2])

        # Use pairwise function for consistency with main analysis
        A_stack = np.stack([A1, A2])
        if slow:
            dists = noa.pairwise_misorientation_condensed(A_stack, sym_ops=sym_ops)
        else:
            dists = noa.pairwise_misorientation_condensed_fast(A_stack, sym_ops=sym_ops)
        angle = dists[0]

        results.append({
            "img_num1": num1,
            "img_num2": num2,
            "angle_deg": angle,
            "filename1": img1["filename"],
            "filename2": img2["filename"],
        })

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Analyze orientation differences between adjacent images (same crystal)"
    )
    parser.add_argument(
        "path",
        help="Path to GXPARM.nXDS/XPARM.nXDS file or directory"
    )
    parser.add_argument(
        "--no-symmetry", action="store_true",
        help="Ignore crystal symmetry (treat as triclinic)"
    )
    parser.add_argument(
        "--threshold", "-t", type=float, default=1.0,
        help="Angle threshold to flag outliers (default: 1.0 deg)"
    )
    parser.add_argument(
        "--output-plot", "-o", default=None,
        help="Save histogram plot to this path"
    )
    parser.add_argument(
        "--slow", action="store_true",
        help="Use original Kabsch alignment instead of fast vectorized einsum (default)"
    )
    args = parser.parse_args()

    # Find and parse files
    files = noa.find_xparm_files(args.path)
    if not files:
        print(f"No XPARM.nXDS or GXPARM.nXDS files found in: {args.path}")
        sys.exit(1)

    all_images = []
    header = None
    for fp in files:
        data = noa.parse_nxds_xparm(str(fp))
        if header is None:
            header = data["header"]
        all_images.extend(data["images"])
        print(f"Parsed: {fp} ({len(data['images'])} images)")

    print(f"Total images: {len(all_images)}")

    # Get symmetry operators
    sg = 1 if args.no_symmetry else header.get("space_group", 1)
    sym_ops = noa.get_point_group_operators(sg)
    print(f"Space group: {sg} ({len(sym_ops)} symmetry operators)")

    # Find adjacent pairs
    pairs = find_adjacent_pairs(all_images)
    print(f"Adjacent pairs found: {len(pairs)}")

    if not pairs:
        print("No adjacent image pairs found.")
        sys.exit(0)

    # Compute misorientations
    print("\nComputing misorientations for adjacent pairs...")
    results = compute_adjacent_misorientations(all_images, pairs, sym_ops, slow=args.slow)

    # Statistics
    angles = np.array([r["angle_deg"] for r in results])
    outliers = [r for r in results if r["angle_deg"] > args.threshold]

    print("\n" + "=" * 60)
    print("ADJACENT IMAGE ORIENTATION ANALYSIS")
    print("=" * 60)
    print(f"Adjacent pairs analyzed: {len(results)}")
    print(f"\nMisorientation statistics (same-crystal pairs):")
    print(f"  Mean:   {np.mean(angles):.4f}°")
    print(f"  Median: {np.median(angles):.4f}°")
    print(f"  Std:    {np.std(angles):.4f}°")
    print(f"  Min:    {np.min(angles):.4f}°")
    print(f"  Max:    {np.max(angles):.4f}°")

    # Percentiles
    for p in [50, 90, 95, 99]:
        print(f"  P{p:02d}:    {np.percentile(angles, p):.4f}°")

    # Outliers
    print(f"\nOutliers (> {args.threshold}°): {len(outliers)} / {len(results)}")
    if outliers:
        print(f"  These may indicate crystal boundaries (different crystal):")
        for r in sorted(outliers, key=lambda x: -x["angle_deg"])[:20]:
            print(f"    Images {r['img_num1']}-{r['img_num2']}: {r['angle_deg']:.4f}°")
        if len(outliers) > 20:
            print(f"    ... and {len(outliers) - 20} more")

    # Distribution of small angles
    bins_fine = [0, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 180.0]
    hist, _ = np.histogram(angles, bins=bins_fine)
    print(f"\nAngle distribution:")
    for i in range(len(bins_fine) - 1):
        pct = hist[i] / len(angles) * 100
        bar = "█" * int(pct / 2)
        print(f"  {bins_fine[i]:>6.1f}° - {bins_fine[i+1]:>6.1f}°: {hist[i]:>5d} ({pct:>5.1f}%) {bar}")

    # Plot
    if args.output_plot:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
            fig.suptitle("Adjacent Image Orientation Differences (Same Crystal)", fontsize=13)

            # Left: histogram of all angles
            ax1.hist(angles, bins=50, color="#4C72B0", edgecolor="white", alpha=0.8)
            ax1.axvline(np.mean(angles), color="red", linestyle="--", label=f"Mean={np.mean(angles):.3f}°")
            ax1.axvline(np.median(angles), color="orange", linestyle="--", label=f"Median={np.median(angles):.3f}°")
            ax1.set_xlabel("Misorientation angle (°)")
            ax1.set_ylabel("Count")
            ax1.set_title("All adjacent pairs")
            ax1.legend(fontsize=9)

            # Right: scatter plot of angle vs image number
            img_nums = [r["img_num1"] for r in results]
            ax2.scatter(img_nums, angles, s=2, alpha=0.5, color="#4C72B0")
            ax2.axhline(args.threshold, color="red", linestyle="--", alpha=0.5, label=f"Threshold={args.threshold}°")
            ax2.set_xlabel("Image number")
            ax2.set_ylabel("Misorientation angle (°)")
            ax2.set_title("Angle vs image number")
            ax2.legend(fontsize=9)

            plt.tight_layout()
            plt.savefig(args.output_plot, dpi=150, bbox_inches="tight")
            print(f"\nPlot saved to: {args.output_plot}")
            plt.close()
        except ImportError:
            print("matplotlib not available — skipping plot")


if __name__ == "__main__":
    main()
