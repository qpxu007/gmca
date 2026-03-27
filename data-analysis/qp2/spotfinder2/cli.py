"""Command-line interface for spotfinder2.

Usage:
    python -m qp2.spotfinder2.cli /path/to/master.h5 [options]

Examples:
    # Process frames 1-10, generate summary plots
    python -m qp2.spotfinder2.cli master.h5 --frames 1 10 --plot

    # Save results to HDF5
    python -m qp2.spotfinder2.cli master.h5 --output spots.h5

    # Force CPU mode, custom resolution limits
    python -m qp2.spotfinder2.cli master.h5 --no-gpu --low-res 50 --high-res 2.0

    # Full pipeline with TDS fitting
    python -m qp2.spotfinder2.cli master.h5 --tds --plot-dir plots/
"""

import argparse
import sys
import os
import time
import json
import numpy as np


def main():
    parser = argparse.ArgumentParser(
        description="spotfinder2 — Advanced Bragg spot detection for serial crystallography",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("master_file", help="Path to HDF5 master file")
    parser.add_argument("--frames", type=int, nargs=2, metavar=("START", "END"),
                        help="Frame range (default: all)")
    parser.add_argument("--output", "-o", help="Output HDF5 file for results")
    parser.add_argument("--output-json", help="Output JSON file for results")
    parser.add_argument("--gpu", action="store_true", default=False,
                        help="Force GPU mode (requires CuPy)")
    parser.add_argument("--no-gpu", action="store_true", default=False,
                        help="Force CPU mode")
    parser.add_argument("--plot", action="store_true",
                        help="Generate summary plot for each frame")
    parser.add_argument("--plot-dir", default="spotfinder2_plots",
                        help="Directory for plot output (default: spotfinder2_plots/)")
    parser.add_argument("--low-res", type=float, default=50.0,
                        help="Low resolution limit in Angstrom (default: 50)")
    parser.add_argument("--high-res", type=float, default=1.5,
                        help="High resolution limit in Angstrom (default: 1.5)")
    parser.add_argument("--box-size", type=int, default=3,
                        help="Integration box size (default: 3)")
    parser.add_argument("--p-false-alarm", type=float, default=1e-5,
                        help="False alarm probability for threshold (default: 1e-5)")
    parser.add_argument("--mle", action="store_true", default=True,
                        help="Enable MLE position refinement (default: on)")
    parser.add_argument("--no-mle", action="store_true",
                        help="Disable MLE position refinement")
    parser.add_argument("--tds", action="store_true", default=False,
                        help="Enable TDS-aware integration")
    parser.add_argument("--no-ice-filter", action="store_true",
                        help="Disable ice spot filter")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable debug logging")
    parser.add_argument("--estimate-crystals", action="store_true",
                        help="Estimate number of crystal lattices per frame")
    parser.add_argument("--unit-cell", type=float, nargs=6,
                        metavar=("A", "B", "C", "ALPHA", "BETA", "GAMMA"),
                        help="Unit cell for Level 2 crystal count (a b c alpha beta gamma in Å/°)")
    parser.add_argument("--protein-filter", action="store_true",
                        help="Enable protein diffraction classification heuristic")
    parser.add_argument("--protein-min-cell", type=float, default=20.0,
                        help="Assumed minimum protein cell dimension in Angstrom (default: 20)")
    parser.add_argument("--workers", "-j", type=int, default=0,
                        help="Number of parallel workers (0=auto, 1=sequential, N=N workers). "
                             "Ignored when GPU is available.")

    args = parser.parse_args()

    # Setup logging
    import logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(name)s %(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")

    # Ensure Qt application exists (HDF5Reader requires it)
    try:
        from PyQt5.QtCore import QCoreApplication
        app = QCoreApplication.instance()
        if app is None:
            app = QCoreApplication(sys.argv)
    except ImportError:
        pass  # May work without Qt if HDF5Reader doesn't need signals

    # Import pipeline
    from qp2.spotfinder2 import SpotFinderPipeline, SpotFinderConfig
    from qp2.xio.hdf5_manager import HDF5Reader

    print(f"spotfinder2 v0.1.0")
    print(f"Master file: {args.master_file}")

    # Open dataset
    t0 = time.time()
    reader = HDF5Reader(args.master_file, start_timer=False)
    params = reader.get_parameters()
    total_frames = reader.total_frames
    print(f"Dataset: {total_frames} frames, {params.get('nx', '?')}x{params.get('ny', '?')} pixels")
    print(f"Wavelength: {params.get('wavelength', '?')} Å, Distance: {params.get('det_dist', '?')} mm")

    # Configure pipeline
    config = SpotFinderConfig(
        force_cpu=args.no_gpu,
        low_resolution_A=args.low_res,
        high_resolution_A=args.high_res,
        box_size=args.box_size,
        p_false_alarm=args.p_false_alarm,
        enable_mle_refinement=not args.no_mle,
        enable_tds_fitting=args.tds,
        enable_ice_filter=not args.no_ice_filter,
        estimate_n_crystals=args.estimate_crystals,
        unit_cell=tuple(args.unit_cell) if args.unit_cell else None,
        enable_protein_filter=args.protein_filter,
        protein_min_cell_A=args.protein_min_cell,
    )

    pipeline = SpotFinderPipeline(params, config)

    # Determine frame range
    if args.frames:
        start, end = args.frames
        end = min(end, total_frames)
    else:
        start, end = 0, total_frames

    # Determine workers
    n_workers = args.workers
    if n_workers == 0:
        n_workers = pipeline.get_n_workers_auto()
    if pipeline.backend.has_gpu:
        n_workers = 1  # GPU: always sequential

    print(f"Processing frames {start}-{end-1} ({end - start} frames, {n_workers} workers)")
    print(f"Config: box={config.box_size}, p_fa={config.p_false_alarm}, "
          f"MLE={'on' if config.enable_mle_refinement else 'off'}, "
          f"TDS={'on' if config.enable_tds_fitting else 'off'}, "
          f"ice_filter={'on' if config.enable_ice_filter else 'off'}, "
          f"protein_filter={'on' if config.enable_protein_filter else 'off'}")
    print("-" * 60)

    # Process frames (parallel or sequential depending on hardware)
    def progress_callback(idx, spots):
        if spots.count > 0:
            n_cryst_str = ""
            if args.estimate_crystals:
                nc = spots.metadata.get("n_crystals", "?")
                nc_conf = spots.metadata.get("n_crystals_confidence", 0)
                nc_method = spots.metadata.get("n_crystals_method", "?")
                n_cryst_str = f", crystals~{nc} ({nc_method}, conf={nc_conf:.2f})"
            protein_str = ""
            if args.protein_filter:
                ps = spots.metadata.get("protein_score", 0)
                is_prot = spots.metadata.get("is_likely_protein", "?")
                min_c = spots.metadata.get("estimated_min_cell_A", 0)
                max_c = spots.metadata.get("estimated_max_cell_A", 0)
                cands = spots.metadata.get("cell_candidates", [])
                cell_str = ""
                if cands:
                    top_cells = [f"{c['cell_A']:.0f}" for c in cands[:3]]
                    cell_str = f", cells=[{','.join(top_cells)}]A"
                    cell_str += f" range=[{min_c:.0f}-{max_c:.0f}]A"
                protein_str = f", protein={is_prot} (score={ps:.2f}{cell_str})"
            ice_str = ""
            n_ice = spots.metadata.get("ice_rings_detected", 0)
            if n_ice > 0:
                ice_d = spots.metadata.get("ice_rings_d_spacings", [])
                ice_str = f", ice={n_ice} rings [{','.join(f'{d:.2f}' for d in ice_d)}]A"
            print(f"  Frame {idx}: {spots.count} spots "
                  f"(SNR range: {spots.snr.min():.1f}-{spots.snr.max():.1f})"
                  f"{n_cryst_str}{protein_str}{ice_str}")
        else:
            print(f"  Frame {idx}: 0 spots")

    all_results = pipeline.process_dataset(
        args.master_file,
        frame_range=(start, end),
        callback=progress_callback if n_workers == 1 else None,
        n_workers=n_workers,
    )
    total_spots = sum(s.count for s in all_results.values())

    # Print results for parallel mode (callback doesn't fire in workers)
    if n_workers > 1:
        for idx in sorted(all_results.keys()):
            spots = all_results[idx]
            if spots.count > 0:
                print(f"  Frame {idx}: {spots.count} spots "
                      f"(SNR range: {spots.snr.min():.1f}-{spots.snr.max():.1f})")
            else:
                print(f"  Frame {idx}: 0 spots")

    # Generate plots (always sequential — matplotlib is not thread-safe)
    if args.plot:
        os.makedirs(args.plot_dir, exist_ok=True)
        from qp2.spotfinder2.viz.matplotlib_viz import SpotFinderPlot
        plotter = SpotFinderPlot()
        for idx in sorted(all_results.keys()):
            spots = all_results[idx]
            if spots.count > 0:
                frame = reader.get_frame(idx)
                if frame is not None:
                    # Re-run to get background (only for plotting, fast with cached mask)
                    bg = pipeline.bg_model.estimate(
                        frame.astype(np.float32),
                        pipeline._get_mask(frame) | pipeline._get_resolution_mask(),
                    )
                    save_path = os.path.join(args.plot_dir, f"frame_{idx:06d}.png")
                    plotter.plot_summary(
                        frame, spots, bg, pipeline.geometry,
                        title=f"Frame {idx}",
                        save_path=save_path,
                    )
                    plotter.close()

    # Summary
    elapsed = time.time() - t0
    n_processed = len(all_results)
    print("-" * 60)
    print(f"Processed {n_processed} frames in {elapsed:.1f}s "
          f"({elapsed/max(n_processed,1):.3f}s/frame)")
    print(f"Total spots: {total_spots} ({total_spots/max(n_processed,1):.1f} avg/frame)")

    # Save results
    if args.output:
        import h5py
        with h5py.File(args.output, "w") as f:
            f.attrs["master_file"] = args.master_file
            f.attrs["n_frames"] = n_processed
            f.attrs["total_spots"] = total_spots
            for idx, spots in all_results.items():
                grp = f.create_group(f"frame_{idx:06d}")
                spots.to_hdf5(grp)
        print(f"Results saved to {args.output}")

    if args.output_json:
        results_json = {
            "master_file": args.master_file,
            "n_frames": n_processed,
            "total_spots": total_spots,
            "frames": {
                str(idx): spots.to_dict() for idx, spots in all_results.items()
            },
        }
        with open(args.output_json, "w") as f:
            json.dump(results_json, f)
        print(f"Results saved to {args.output_json}")

    reader.close()


if __name__ == "__main__":
    main()
