import os
import subprocess
import logging
import tempfile
import shutil
from qp2.config.programs import ProgramConfig

logger = logging.getLogger(__name__)

DOZOR_IN_TEMPLATE = """!DOZOR v2 template
job single
nx {nx}
ny {ny}
pixel {pixel}
pixel_max {cutoff}
fraction_polarization 0.99
detector_distance {distance}
X-ray_wavelength {wavelength}
orgx {orgx}
orgy {orgy}
spot_size {spot_size}
spot_level {spot_level}
exposure {exposure}
oscillation_range {osc}
starting_angle {start_angle}
name_template_image {template}
first_image_number {n_first}
number_images {nimages}
pixel_min 0
ix_min 1
ix_max {ix_max}
iy_min {iy_min}
iy_max {iy_max}
library {mylib}
dist_cutoff {dist_cutoff}
res_cutoff_low {res_cutoff_low}
res_cutoff_high {res_cutoff_high}
check_ice_rings {check_ice_rings}
{extra_params}
"""

def parse_dozor_output(outlog):
    """Parse the main dozor output log."""
    lines = outlog.split("\n")
    # Find start of table
    data_lines = []
    in_table = False
    
    # Dozor output usually has a header line starting with " Image" or similar
    # But the provided parser logic in dozor_process.py skips first 6 lines.
    # We will try to be more robust or copy the existing logic.
    
    # Existing logic from dozor_process.py:
    # lines = outlog.split("\n")[6:-2]
    # Iterates and looks for lines starting with "-" to toggle start.
    
    # Let's assume the standard Dozor output format.
    out = []
    start = False
    for line in lines:
        if line.strip().startswith("---------"):
            start = not start
            continue
        if start:
            parts = line.split("|")
            if len(parts) < 2: 
                continue
                
            # Expected columns:
            # 0: Image
            # 1: Spots (No. Peaks, Int, R-fact, Resol)
            # 2: Powder (Scale, B-fact, Resol, Corr, R-fact)
            # 3: Score (Main, Spot, Resol, Total)
            
            # parts[0] is image number
            # parts[1] contains spot stats
            # parts[3] contains scores
            
            try:
                img_num = int(parts[0].strip())
                
                # Parse scores from the last section
                # parts[3] usually looks like: "   2.1   1.2   0.0   0.0"
                scores_str = parts[3].strip().split()
                if len(scores_str) >= 2:
                    main_score = float(scores_str[0])
                    spot_score = float(scores_str[1])
                else:
                    main_score = 0.0
                    spot_score = 0.0

                # Parse spot count from parts[1]
                # parts[1] looks like: "  152  12.3  12.3   1.2"
                spots_str = parts[1].strip().split()
                if len(spots_str) >= 1:
                    num_spots = int(spots_str[0])
                else:
                    num_spots = 0

                out.append({
                    "frame": img_num,
                    "score": main_score,
                    "num_spots": num_spots
                })

            except ValueError:
                continue
                
    return out

def run_dozor(metadata, work_dir, start_frame, end_frame, 
              spot_level=6, spot_size=3, min_spots=0, min_score=0):
    """
    Runs Dozor on the specified frame range.
    Returns a list of frame numbers that pass the filter criteria.
    """
    dozor_dir = os.path.join(work_dir, "dozor")
    os.makedirs(dozor_dir, exist_ok=True)
    
    logger.info(f"Running Dozor in {dozor_dir} for frames {start_frame}-{end_frame}")

    # Prepare Parameters
    params = {
        "nx": metadata["nx"],
        "ny": metadata["ny"],
        "pixel": metadata["pixel_size"],
        "cutoff": metadata["saturation_value"],
        "distance": metadata["det_dist"],
        "wavelength": metadata["wavelength"],
        "orgx": metadata["beam_x"],
        "orgy": metadata["beam_y"],
        "exposure": metadata["exposure"],
        "osc": metadata["omega_range"],
        "start_angle": metadata["omega_start"],
        "template": metadata["master_file"].replace("master", "??????"),
        "n_first": start_frame,
        "nimages": end_frame - start_frame + 1,
        "spot_size": spot_size,
        "spot_level": spot_level,
        
        # Defaults
        "ix_max": int(metadata["beam_x"] + 100), # Approx beamstop area
        "iy_min": int(metadata["beam_y"] - 100),
        "iy_max": int(metadata["beam_y"] + 100),
        "dist_cutoff": 20.0,
        "res_cutoff_low": 50.0,
        "res_cutoff_high": 2.0,
        "check_ice_rings": "T",
        "extra_params": f"min_spot_count {min_spots}" if min_spots > 0 else ""
    }
    
    # Library
    master_file = metadata["master_file"]
    if master_file.endswith(".cbf"):
        mylib = ProgramConfig.get_library_path("xds-zcbf")
    elif master_file.endswith("_master.h5"):
        mylib = ProgramConfig.get_library_path("dectris-neggia")
    else:
        # Fallback/Guess
        mylib = ProgramConfig.get_library_path("dectris-neggia")
        
    params["mylib"] = mylib
    
    # Write Input
    inp_content = DOZOR_IN_TEMPLATE.format(**params)
    inp_path = os.path.join(dozor_dir, "dozor.in")
    with open(inp_path, "w") as f:
        f.write(inp_content)
        
    # Executable
    setup_cmd = ProgramConfig.get_setup_command("dozor")
    dozor_exe = ProgramConfig.get_program_path("dozor")
    cmd = f"{setup_cmd} && {dozor_exe} -pall -s -p {inp_path}"
    
    try:
        logger.info("Starting Dozor process...")
        # Run and capture output
        res = subprocess.run(
            ["bash", "-c", cmd], 
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=dozor_dir,
            text=True
        )
        
        # Write log for debugging
        with open(os.path.join(dozor_dir, "dozor.log"), "w") as f:
            f.write(res.stdout)
            
        if res.returncode != 0:
            logger.error(f"Dozor failed with return code {res.returncode}")
            return []
            
        # Parse
        results = parse_dozor_output(res.stdout)
        
        # Filter
        valid_frames = []
        for r in results:
            if r["num_spots"] >= min_spots:
                if min_score > 0 and r["score"] < min_score:
                    continue
                valid_frames.append(r["frame"])
                
        logger.info(f"Dozor finished. {len(valid_frames)}/{len(results)} frames passed filter (min_spots={min_spots}, min_score={min_score}).")
        return valid_frames
        
    except Exception as e:
        logger.error(f"Error running Dozor: {e}")
        return []
