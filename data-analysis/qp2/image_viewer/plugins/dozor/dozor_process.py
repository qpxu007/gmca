import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile

import redis
from qp2.config.programs import ProgramConfig

try:
    from qp2.log.logging_config import get_logger

    logger = get_logger(__name__)

except ImportError:
    import logging

    logging.basicConfig(
        level=logging.DEBUG,  # Set the minimum level to show
        format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d - %(message)s",
        stream=sys.stdout,  # Ensure logs go to the console
    )
    logger = logging.getLogger(__name__)


def check_frames_exist_in_redis_hash(redis_conn, redis_key, start_frame, num_frames):
    """
    Checks if all frames in a given range exist as fields in a Redis HASH.

    Args:
        redis_conn: An active Redis connection object.
        redis_key (str): The Redis hash key to check.
        start_frame (int): The 1-based starting frame number.
        num_frames (int): The number of frames in the range.

    Returns:
        bool: True if all frames exist, False otherwise.
    """
    if redis_conn is None or not redis_key:
        return False

    if not redis_conn.exists(redis_key):
        return False

    try:
        requested_frames = {
            str(i) for i in range(start_frame, start_frame + num_frames)
        }

        pipe = redis_conn.pipeline()
        for frame_num in requested_frames:
            pipe.hexists(redis_key, frame_num)

        existence_results = pipe.execute()

        return all(existence_results)

    except redis.RedisError as e:
        logger.error(f"Redis error during job check for key {redis_key}: {e}")
        return False  # Fail safe: better to re-run than to fail silently.


DOZOR_IN = """!DOZOR v2 template
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
pixel_min 0 ! should be 0, otherwise may fail randomly
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


def parse_dozor_sum_int(wdir):
    """Parse dozor_sum_int.dat for intensity statistics."""
    file = os.path.join(wdir, "dozor_sum_int.dat")
    results = []
    if os.path.exists(file):
        with open(file, "r") as f:
            lines = f.readlines()
            keys = ["iseq", "totalInten", "totalBackgr", "relDInten", "relInt/Int(1)"]
            for line in lines:
                if "totalInten" in line or "end" in line:
                    continue
                values = [-1 if "*" in v else v for v in line.split()]
                if len(values) == len(keys):
                    data = dict(zip(keys, map(float, values)))
                    del data["iseq"]  # NB iseq is not img_num, so remove it
                    # data["iseq"] = int(data["iseq"])
                    results.append(data)
    return results


def parse_dozor(outlog):
    """Parse the main dozor output log."""
    lines = outlog.split("\n")[6:-2]
    out = []
    if lines:
        start = False
        for line in lines:
            if line.startswith("-"):
                start = not start
                continue
            if start:
                results = []
                groups = line.split("|")
                results.append(groups[0].strip())
                keys = [
                    "img_num",
                    "No. Peaks",
                    "Spot Avg Int",
                    "Spot R-factor",
                    "Spot Resol",
                    "Scale",
                    "B factor",
                    "Resol Wilson",
                    "Correlation",
                    "R-factor",
                    "Main Score",
                    "Spot Score",
                    "Resol Visible",
                    "TotalAvInt",
                ]
                nfields_per_group = [1, 4, 5, 4]
                for i in range(1, 4):
                    if i < len(groups):  # Protect against missing groups in malformed lines
                        if "no results" in groups[i]:
                            results.extend(["0"] * nfields_per_group[i])
                        else:
                            results.extend(groups[i].split())
                
                # Check for 13 (old) or 14 (new) columns
                if len(results) >= 13:
                    try:
                        # Convert to float/int
                        converted = [float(r) for r in results]
                        converted[0] = int(converted[0])
                        converted[1] = int(converted[1])
                        
                        # Zip will safely truncate 'keys' if 'converted' has fewer items
                        # If converted has 13 items, "TotalAvInt" key is ignored.
                        # If converted has 14 items, "TotalAvInt" is used.
                        out.append(dict(zip(keys, converted)))
                    except (ValueError, TypeError):
                        # Fallback to strings if conversion fails
                        out.append(dict(zip(keys, results)))
    return out if out else outlog


def parse_spot_files(wdir):
    """
    Parse .spot files in a given directory.
    Returns a dictionary: {sequence_number: [(iseq, x, y, intensity, sigi), ...]}
    """
    results_dict = {}
    for spot_file_path in glob.glob(os.path.join(wdir, "*.spot")):
        file_name = os.path.basename(spot_file_path)
        try:
            seq_num = int(os.path.splitext(file_name)[0])
        except ValueError:
            logger.warning(
                f"Could not parse sequence number from filename: {file_name}. Skipping."
            )
            continue
        spot_data_list = []
        try:
            with open(spot_file_path, "r") as f:
                for _ in range(3):
                    next(f)
                for line_num, line in enumerate(f, start=4):
                    parts = line.strip().split()
                    if len(parts) == 5:
                        try:
                            iseq, x, y, intensity, sigi = (
                                int(parts[0]),
                                float(parts[1]),
                                float(parts[2]),
                                float(parts[3]),
                                float(parts[4]),
                            )
                            spot_data_list.append((iseq, x, y, intensity, sigi))
                        except ValueError:
                            logger.warning(
                                f"Could not parse data line in {file_name} at line {line_num}: '{line}'. Skipping line."
                            )
                    elif line.strip():
                        logger.warning(
                            f"Unexpected number of columns in {file_name} at line {line_num}: '{line}'. Expected 5 columns. Skipping line."
                        )
            results_dict[seq_num] = spot_data_list
        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")
    return results_dict


def dozor_job(
    metadata,
    redis_conn=None,
    redis_key_prefix="analysis:out:spots:dozor2",
    start=1,
    nimages=1,
    tempdir_root=None,
    debug=False,
):
    """Run a Dozor job and process outputs."""

    chosen_tempdir_root = None
    if tempdir_root:  # If explicitly provided
        if os.path.isdir(tempdir_root) and os.access(tempdir_root, os.W_OK):
            chosen_tempdir_root = tempdir_root
        else:
            logger.warning(
                f"Provided tempdir_root '{tempdir_root}' not usable. Falling back."
            )

    if not chosen_tempdir_root:  # Try /dev/shm
        if os.path.isdir("/dev/shm") and os.access("/dev/shm", os.W_OK):
            chosen_tempdir_root = "/dev/shm"
        else:
            logger.warning("/dev/shm not usable. Falling back to system default temp.")

    # If chosen_tempdir_root is still None, mkdtemp will use system default (e.g., /tmp)
    wdir = tempfile.mkdtemp(prefix="dozor_tmp_", dir=chosen_tempdir_root)
    logger.debug(f"Using working directory: {wdir}")

    try:
        orgx, orgy = metadata["beam_x"], metadata["beam_y"]
        template = metadata["master_file"]

        beamstop_size_in_pixels = metadata.get("dozor_beamstop_size", 100)
        spot_size = metadata.get("dozor_spot_size", 3)
        spot_level = metadata.get("dozor_spot_level", 6)

        ix_max = int(orgx + beamstop_size_in_pixels)
        iy_min = int(orgy - beamstop_size_in_pixels)
        iy_max = int(orgy + beamstop_size_in_pixels)
        start_angle = metadata["omega_start"]
        osc = metadata["omega_range"]
        wavelength = metadata["wavelength"]
        cutoff = metadata["saturation_value"]
        exposure = metadata["exposure"]
        distance = metadata["det_dist"]
        nx, ny = metadata["nx"], metadata["ny"]
        pixel_size = metadata["pixel_size"]

        # New Dozor parameters
        dist_cutoff = metadata.get("dozor_dist_cutoff", 20.0)
        res_cutoff_low = metadata.get("dozor_res_cutoff_low", 20.0)
        res_cutoff_high = metadata.get("dozor_res_cutoff_high", 2.5)
        check_ice_rings = metadata.get("dozor_check_ice_rings", "T")
        exclude_resolution_ranges = metadata.get("dozor_exclude_resolution_ranges", [])

        min_spot_range_low = metadata.get("dozor_min_spot_range_low", 15.0)
        min_spot_range_high = metadata.get("dozor_min_spot_range_high", 4.0)
        min_spot_count = metadata.get("dozor_min_spot_count", 2)

        exclude_str_list = []
        for r in exclude_resolution_ranges:
            if isinstance(r, (list, tuple)) and len(r) == 2:
                # Sort to ensure [larger_val, smaller_val] (Low Res to High Res)
                r_sorted = sorted(r, reverse=True)
                exclude_str_list.append(f"exclude_resolution_range {r_sorted[0]} {r_sorted[1]}")
        
        # New logic for combining optional parameters
        if min_spot_count > 0:
            # Sort to ensure [larger_val, smaller_val] (Low Res to High Res)
            s_low, s_high = max(min_spot_range_low, min_spot_range_high), min(min_spot_range_low, min_spot_range_high)
            exclude_str_list.append(f"min_spot_range {s_low} {s_high}")
            exclude_str_list.append(f"min_spot_count {min_spot_count}")
        
        extra_params = "\n".join(exclude_str_list)

        if template.endswith(".cbf"):
            mylib = ProgramConfig.get_library_path("xds-zcbf")
        elif template.endswith("_master.h5"):
            mylib = ProgramConfig.get_library_path("dectris-neggia")
        else:
            logger.error("Unknown template file extension.")
            return
        if not os.path.exists(mylib):
            logger.error(f"Image library is not found, please define {mylib}")
            return
        dozor_input = DOZOR_IN.format(
            cutoff=cutoff,
            nx=nx,
            ny=ny,
            pixel=pixel_size,
            distance=distance,
            wavelength=wavelength,
            orgx=orgx,
            orgy=orgy,
            exposure=exposure,
            osc=osc,
            start_angle=start_angle,
            template=template.replace("master", "??????"),
            n_first=start,
            spot_size=spot_size,
            spot_level=spot_level,
            nimages=nimages,
            ix_max=ix_max,
            iy_min=iy_min,
            iy_max=iy_max,
            mylib=mylib,
            dist_cutoff=dist_cutoff,
            res_cutoff_low=res_cutoff_low,
            res_cutoff_high=res_cutoff_high,
            check_ice_rings=check_ice_rings,
            extra_params=extra_params,
        )
        redis_out_queue = f"{redis_key_prefix}:{template}"
        dozor_in = os.path.join(wdir, "dozor.in")
        with open(dozor_in, "w") as fh:
            fh.write(dozor_input.strip())
            
        # Use ProgramConfig to get the setup command for Dozor
        setup_cmd = ProgramConfig.get_setup_command("dozor")
        dozor_exe = ProgramConfig.get_program_path("dozor")
        
        # Construct the full shell command
        full_command = f"{setup_cmd} && {dozor_exe} -pall -s -p {dozor_in}"
        
        dozor_out = subprocess.check_output(
            ["bash", "-c", full_command], 
            stderr=subprocess.STDOUT, 
            cwd=wdir
        )
        d_out = parse_dozor(dozor_out.decode("utf-8", errors="ignore"))
        logger.debug(f"Parsed Dozor output: {d_out}")
        intensity_stat1 = parse_dozor_sum_int(wdir)
        spots = parse_spot_files(wdir)
        if not isinstance(d_out, list) or not d_out:
            return d_out

        redis_out_key = f"{redis_key_prefix}:{metadata['master_file']}"
        logger.debug(f"Parsed Dozor output: {redis_out_key} {redis_conn}")

        if redis_conn:
            try:
                with redis_conn.pipeline() as pipe:
                    for i, d in enumerate(d_out):
                        logger.debug(
                            f"Processing image {i + 1}/{len(d_out)}: {d['img_num']}"
                        )
                        if intensity_stat1 and len(d_out) == len(intensity_stat1):
                            d.update(intensity_stat1[i])
                        d["template"] = metadata["master_file"]
                        img_num = int(d["img_num"])
                        d["spots"] = spots.get(img_num, [])

                        # Set the HASH field to the image number (which is the unique key)
                        # and the value to the JSON string of the result dictionary.
                        logger.debug(
                            f"Writing to Redis HASH: {redis_out_key}, img_num: {img_num}, data: {d}"
                        )
                        pipe.hset(redis_out_key, img_num, json.dumps(d))

                    # Set expiration for the entire hash key (per master_file)
                    # This ensures the Dozor results for this dataset expire after 24 hours (86400 seconds)
                    pipe.expire(redis_out_key, 24 * 3600)  # 24 hours in seconds

                    # Execute all the HSET commands in the pipeline
                    pipe.execute()

            except redis.RedisError as e:
                logger.error(f"Failed to write Dozor results to Redis HASH: {e}")
        else:  # Fallback to writing local files if no redis
            for d in d_out:
                with open(f"{d['img_num']}.json", "w") as f:
                    f.write(json.dumps(d))

    except subprocess.CalledProcessError as e:
        logger.error(f"{full_command} failed @{wdir}")
        logger.error(f"{e.output.decode(errors='ignore')}")
        raise
    finally:
        if not debug:
            shutil.rmtree(wdir)


def check_job_already_run(metadata, redis_conn, redis_key_prefix, start_frame, nimages):
    """
    Checks if all frames in the requested range have already been processed
    by checking for their keys in a Redis HASH.
    """
    if redis_conn is None:
        return False

    redis_key_for_dozor_output = f"{redis_key_prefix}:{metadata['master_file']}"

    if not redis_conn.exists(redis_key_for_dozor_output):
        return False

    try:
        # --- MODIFICATION: More efficient check using HASH ---
        # Get the set of frame numbers we WANT to process (Dozor uses 1-based indexing)
        requested_img_nums = {str(i) for i in range(start_frame, start_frame + nimages)}

        # Use HEXISTS in a pipeline to check for all frames at once.
        # This is much faster than fetching all keys with HKEYS.
        pipe = redis_conn.pipeline()
        for frame_num in requested_img_nums:
            pipe.hexists(redis_key_for_dozor_output, frame_num)

        # The result is a list of booleans [True, False, True, ...]
        existence_results = pipe.execute()

        # If all results are True, then all frames exist and the job can be skipped.
        return all(existence_results)
        # --- END MODIFICATION ---

    except redis.RedisError as e:
        logger.error(f"Redis error during job check for {metadata['master_file']}: {e}")
        return False  # Fail safe: attempt to run the job if Redis check fails.


def main():
    parser = argparse.ArgumentParser(description="Run Dozor job in a separate process.")
    parser.add_argument(
        "--metadata",
        type=json.loads,
        required=True,
        help="JSON string containing metadata",
    )
    parser.add_argument("--start", type=int, default=1, help="Start frame number")
    parser.add_argument("--nimages", type=int, default=1, help="Number of images")
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debugging; keep temporary directory",
    )
    parser.add_argument(
        "--redis_host", type=str, default=None, help="Redis host (optional)"
    )
    parser.add_argument(
        "--redis_port", type=int, default=6379, help="Redis port (optional)"
    )
    parser.add_argument(
        "--redis_key_prefix",
        type=str,
        default="analysis:out:spots:dozor2",
        help="key prefix for storing redis results",
    )

    args = parser.parse_args()

    redis_conn = None
    if args.redis_host:
        try:
            redis_conn = redis.Redis(host=args.redis_host, port=args.redis_port)
            redis_conn.ping()  # Check connection
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Error connecting to Redis: {e}")
            redis_conn = None

    if redis_conn is not None:
        redis_key = f"{args.redis_key_prefix}:{args.metadata['master_file']}"
        # MODIFICATION: Call the new, shared utility function
        job_already_run = check_frames_exist_in_redis_hash(
            redis_conn, redis_key, args.start, args.nimages
        )
        if job_already_run:
            logger.info(
                f"Frames {args.start} to {args.start + args.nimages - 1} found in Redis. Exiting job."
            )
            return  # Exit the job

    try:
        dozor_job(
            args.metadata,
            redis_conn=redis_conn,
            redis_key_prefix=args.redis_key_prefix,
            start=args.start,
            nimages=args.nimages,
            debug=args.debug,
        )
        logger.info("Dozor job completed successfully.")  # Indicate success
    except Exception as e:
        logger.error(f"Dozor job failed: {e}")  # Indicate failure


if __name__ == "__main__":
    main()
