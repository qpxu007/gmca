
import os
import logging
import subprocess
import shutil
from pathlib import Path

from qp2.image_viewer.plugins.crystfel.crystfel_geometry import (
    generate_crystfel_geometry_file,
)
from qp2.image_viewer.plugins.crystfel.stream_utils import StreamParser
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def run_crystfel_command(cmd, cwd):
    """Helper to run a CrystFEL command and log its output."""
    logger.info(f"Executing command in {cwd}: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)
    if result.stdout:
        logger.debug(f"CrystFEL STDOUT:\n{result.stdout}")
    if result.stderr:
        logger.warning(f"CrystFEL STDERR:\n{result.stderr}")
    if result.returncode != 0:
        raise RuntimeError(
            f"CrystFEL command failed with exit code {result.returncode}:\n{' '.join(cmd)}"
        )
    return result


def run_crystfel_strategy(mapping: dict, workdir: str, pipeline_params: dict) -> dict:
    """
    Runs CrystFEL indexing on a single image.

    Args:
        mapping: Dict mapping master_file_path -> [frame_list].
                 For strategy, we expect one master file and one frame.
        workdir: Directory to run in.
        pipeline_params: Dictionary of pipeline parameters.

    Returns:
        dict: Results dictionary suitable for display in StrategyResultsDialog.
    """
    # 1. Extract inputs
    if not mapping:
        raise ValueError("No data provided in mapping.")

    master_file = next(iter(mapping))
    frames = mapping[master_file]
    if not frames:
        raise ValueError("No frames provided in mapping.")
    
    # CrystFEL uses 0-based indexing internally for some tools, but the Mapping usually has 
    # 1-based frame numbers (from UI). However, check how other strategies handle it.
    # run_strategy.py passes the mapping.
    # indexamajig expects a list file.
    # If the UI passes [1], that's frame 1 (1-based).
    # But usually the UI passes what the user sees.
    # Let's assume 1-based coming in, but we need to verify. 
    # In StrategyManager.run_strategy_for_current_view:
    # mapping = {self.main_window.current_master_file: [self.main_window.current_frame_index + 1]}
    # So it is 1-based.
    
    frame_num_1_based = frames[0]
    frame_index_0_based = frame_num_1_based - 1

    workdir_path = Path(workdir)
    workdir_path.mkdir(parents=True, exist_ok=True)

    # 2. Generate Geometry
    geom_file = workdir_path / "crystfel.geom"
    
    include_mask = pipeline_params.get("crystfel_include_mask", False)
    
    # If explicit masking is requested, we need a path for the h5 mask file
    mask_file = None
    if include_mask:
        mask_file = str(workdir_path / "mask.h5")

    generate_crystfel_geometry_file(
        master_file_path=master_file,
        output_geom_path=str(geom_file),
        bad_pixels_file_path=mask_file,
        include_gaps=include_mask,     # Treat gaps as part of the mask request
        include_mask=include_mask      # Link the mask file in the geom
    )

    # 3. Create List File
    # indexamajig input format: filename //event_number (if needed)
    # HDF5 support in CrystFEL might require event number if it's a multi-image file.
    # For HDF5, usually it's "filename //frame_index" or just "filename" if 1 frame.
    from qp2.xio.hdf5_manager import HDF5Reader

    # Resolve actual data file node from master using HDF5Reader
    # This handles Eiger external links / VDS logic robustly
    try:
        reader = HDF5Reader(master_file, start_timer=False)
        target_file_path = None
        local_index = 0
        
        # HDF5Reader builds a frame_map: [(start, end, filepath, dsetpath), ...]
        for start_idx, end_idx, fpath, dpath in reader.frame_map:
            if start_idx <= frame_index_0_based < end_idx:
                target_file_path = fpath
                local_index = frame_index_0_based - start_idx
                break
        
        reader.close()

        if not target_file_path:
             logger.warning(f"Could not resolve frame {frame_index_0_based} in {master_file} using HDF5Reader. Using master file as fallback.")
             target_file_path = master_file
             local_index = frame_index_0_based

    except Exception as e:
        logger.error(f"Error resolving data file for {master_file}: {e}")
        # Fallback
        target_file_path = master_file
        local_index = frame_index_0_based

    list_file = workdir_path / "images.lst"
    with open(list_file, "w") as f:
        # Use resolved path and local index
        f.write(f"{target_file_path} //{local_index}\n")

    # 4. Run indexamajig
    stream_file = workdir_path / "output.stream"
    
    # Log input parameters for traceability
    logger.info(f"CrystFEL Strategy Parameters:\n{pipeline_params}")
    
    # Parameters from pipeline_params (which come from settings)
    peak_method = pipeline_params.get("crystfel_peaks_method", "peakfinder9")
    min_snr = pipeline_params.get("crystfel_min_snr", 4.0)
    min_peaks = pipeline_params.get("crystfel_min_peaks", 10)
    indexing_methods = pipeline_params.get("crystfel_indexing_methods", "xgandalf")
    pdb_file = pipeline_params.get("crystfel_pdb")

    no_check_peaks = pipeline_params.get("crystfel_no_check_peaks", True)
    no_refine = pipeline_params.get("crystfel_no_refine", True)
    
    extra_options = pipeline_params.get("crystfel_extra_options", "")

    # Default parameters for a quick strategy/indexing check
    cmd = [
        "indexamajig",
        "-i", str(list_file),
        "-g", str(geom_file),
        "-o", str(stream_file),
        "-j", "1", # Single core for single image
        f"--peaks={peak_method}", 
        f"--indexing={indexing_methods}", 
        f"--min-snr={min_snr}",
        f"--min-peaks={min_peaks}",
    ]

    # Peak finding detailed params
    min_snr_biggest = pipeline_params.get("crystfel_min_snr_biggest_pix")
    min_snr_peak = pipeline_params.get("crystfel_min_snr_peak_pix")
    min_sig = pipeline_params.get("crystfel_min_sig")
    bg_radius = pipeline_params.get("crystfel_local_bg_radius")

    if min_snr_biggest is not None:
        cmd.append(f"--min-snr-biggest-pix={min_snr_biggest}")
    if min_snr_peak is not None:
        cmd.append(f"--min-snr-peak-pix={min_snr_peak}")
    if min_sig is not None:
        cmd.append(f"--min-sig={min_sig}")
    if bg_radius is not None:
        cmd.append(f"--local-bg-radius={bg_radius}")
        
    # Peakfinder8 specific flags
    if peak_method == "peakfinder8":
        pf8_thresh = pipeline_params.get("crystfel_peakfinder8_threshold")
        pf8_min_pix = pipeline_params.get("crystfel_peakfinder8_min_pix_count")
        pf8_max_pix = pipeline_params.get("crystfel_peakfinder8_max_pix_count")
        
        if pf8_thresh is not None:
            cmd.append(f"--threshold={pf8_thresh}")
        if pf8_min_pix is not None:
            cmd.append(f"--min-pix-count={pf8_min_pix}")
        if pf8_max_pix is not None:
            cmd.append(f"--max-pix-count={pf8_max_pix}")

    # Speed / Algo options
    if pipeline_params.get("crystfel_peakfinder8_fast", False):
        cmd.append("--peakfinder8-fast")
    if pipeline_params.get("crystfel_asdf_fast", False):
        cmd.append("--asdf-fast")
    if pipeline_params.get("crystfel_no_retry", False):
        cmd.append("--no-retry")
    if pipeline_params.get("crystfel_no_multi", False):
        cmd.append("--no-multi")
    if pipeline_params.get("crystfel_no_non_hits", False):
        cmd.append("--no-non-hits-in-stream")
        
    # Integration
    push_res = pipeline_params.get("crystfel_push_res", 0.0)
    if push_res and push_res > 0:
        cmd.append(f"--push-res={push_res}")
        
    int_radius = pipeline_params.get("crystfel_int_radius")
    if int_radius:
        cmd.append(f"--int-radius={int_radius}")

    int_mode = pipeline_params.get("crystfel_integration_mode", "Standard")
    # Mapping for integration mode if necessary. 
    # Usually indexamajig is --integration=method. 
    # But settings dialog has "Standard", "None (No Intensity)", "Cell Only (No Prediction)".
    # Indexamajig options: --integration=method (rings-grad, rings-nocen-grad, prof2d...)
    # Or specific flags: --cell-parameters-only, --no-refls-in-stream
    
    if int_mode == "Cell Only (No Prediction)":
        cmd.append("--cell-parameters-only")
    elif int_mode == "None (No Intensity)":
        cmd.append("--no-refls-in-stream")
    # else "Standard" let defaults apply (or specify default method if we want)

    if no_check_peaks:
        cmd.append("--no-check-peaks")
    if no_refine:
        cmd.append("--no-refine")
    
    if pdb_file:
        cmd.append(f"-p {pdb_file}")
        
    # --- XGANDALF Specific Options ---
    # Only adding these if xgandalf is one of the indexing methods to avoid clutter/warnings
    if "xgandalf" in indexing_methods:
        xg_fast = pipeline_params.get("crystfel_xgandalf_fast", False)
        
        if xg_fast:
            cmd.append("--xgandalf-fast-execution")
        else:
            # Use specific pitch/iter if not fast
            xg_pitch = pipeline_params.get("crystfel_xgandalf_sampling_pitch", 6)
            xg_iter = pipeline_params.get("crystfel_xgandalf_grad_desc_iterations", 4)
            cmd.append(f"--xgandalf-sampling-pitch={xg_pitch}")
            cmd.append(f"--xgandalf-grad-desc-iterations={xg_iter}")

        xg_tol = pipeline_params.get("crystfel_xgandalf_tolerance", 0.02)
        xg_no_dev = pipeline_params.get("crystfel_xgandalf_no_deviation", False)
        xg_min_lat = pipeline_params.get("crystfel_xgandalf_min_lattice", 30.0)
        xg_max_lat = pipeline_params.get("crystfel_xgandalf_max_lattice", 250.0)
        xg_max_peaks = pipeline_params.get("crystfel_xgandalf_max_peaks", 250)

        cmd.append(f"--xgandalf-tolerance={xg_tol}")
        if xg_no_dev:
            cmd.append("--xgandalf-no-deviation-from-provided-cell")
        cmd.append(f"--xgandalf-min-lattice-vector-length={xg_min_lat}")
        cmd.append(f"--xgandalf-max-lattice-vector-length={xg_max_lat}")
        cmd.append(f"--xgandalf-max-peaks={xg_max_peaks}")

    if extra_options:
         import shlex
         cmd.extend(shlex.split(extra_options))

    # Environment Setup
    from qp2.config.programs import ProgramConfig
    from qp2.image_viewer.utils.run_job import run_command as run_job_command

    setup_cmds = []
    # User requested modules
    setup_cmds.append(ProgramConfig.get_setup_command("ccp4"))
    setup_cmds.append(ProgramConfig.get_setup_command("xds"))
    # Ensure CrystFEL itself is loaded
    setup_cmds.append(ProgramConfig.get_setup_command("crystfel"))
    
    pre_command = "; ".join(setup_cmds)

    logger.info(f"Running indexamajig with environment setup: {pre_command}")
    logger.info(f"Full CrystFEL Command: {' '.join(cmd)}")
    
    # Use qp2 common run_command to handle environment wrapping via shell script
    run_job_command(
        cmd=cmd,
        cwd=str(workdir_path),
        job_name="crystfel_strategy",
        pre_command=pre_command,
        method="shell",
        background=False
    )

    # 5. Parse Output
    parser = StreamParser(str(stream_file))
    results = parser.all_results

    if not results:
        return {"status": "failed", "message": "No results found in stream."}

    # We expect only one result
    res = results[0]
    
    # 6. Format Results for Dialog
    # The dialog expects specific keys.
    # StrategyResultsDialog expects `result_data` to be a dictionary.
    # It checks `result_data.get("final")` for Mosflm or `idxref` for XDS.
    # We will add a "crystfel" key or adapt the structure.
    # Since we are modifying StrategyResultsDialog, we can define our own structure.
    
    # Extract unit cell
    uc = res.get("unit_cell_crystfel", [])
    
    formatted_result = {
        "program": "crystfel",
        "crystfel": {
            "indexed_by": res.get("indexed_by"),
            "lattice_type": res.get("lattice_type"),
            "centering": res.get("centering"),
            "unit_cell": uc,
            "num_spots": res.get("num_peaks"),
            "num_reflections": len(res.get("reflections_crystfel", [])),
        },
        # Add spots/reflections for overlay
        # Structure: {master_file: {frame_1_based: dict_data}}
        "spots_by_master_crystfel": {
            master_file: {
                frame_num_1_based: {
                    "spots_crystfel": res.get("spots_crystfel", []),
                    "reflections_crystfel": res.get("reflections_crystfel", [])
                }
            }
        }
    }

    return formatted_result
