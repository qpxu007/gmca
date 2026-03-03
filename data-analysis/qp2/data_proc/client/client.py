import argparse
import json
import os
import socket
import sys

import requests

# This import might need to be adjusted or removed if this script is run standalone
try:
    from qp2.data_proc.config import DATAPROC_SERVER_HTTP_PORT
except ImportError:
    pass
from qp2.config.servers import ServerConfig

# --- Module-Level Constants and Configuration ---

JOB_ENDPOINT = "/launch_job"

def determine_active_config():
    """Determines the active server URL and beamline based on configuration."""
    return {
        "server_url": ServerConfig.get_dataproc_url(),
        # Keep beamline logic if strictly necessary or move to ServerConfig too?
        # The original code had beamline hardcoded per IP. 
        # ServerConfig.get_dataproc_url() handles the IP/URL part.
        # We can keep the simple beamline default or infer it.
        # For now, let's keep it simple as the original code's beamline 
        # determination was coupled with the IP.
        "beamline": "23b", # Default, can be overridden by job data
    }

ENVIRONMENT_CONFIGS = {} # Deprecated in favor of ServerConfig

DEFAULT_APP_CONFIG = {
    "server_url": ServerConfig.get_dataproc_url(),
    "beamline": "23b",
}

REQUIRED_XPROCESS_KEYS = {"pipeline", "sample_id", "username"}
KNOWN_XPROCESS_KEYS = REQUIRED_XPROCESS_KEYS.union(
    {
        "proc_dir",
        "data_dir",
        "job_tag",
        "program",
        "beamline",
        "prefix",
        "percent",
        "filelist",
        "start",
        "end",
        "images",
        "nproc",
        "njobs",
        "primary_group",
        "groupname", # 'groupname' is often used as an alias
        "esaf_id",
        "pi_id",
        "redis_key",
        "spreadsheet",
        "robot_mounted",
        "runner",
        "highres",
        "symm",
        "space_group",
        "model",
        "model_type",
        "nmol",
        "sequence",
        "unitcell",
        "referencedata",
        "reference_hkl",
        "xds_refhkl",
        "nativedata",
        "native",
        "workdir",
        "imagedir",
        "samplename",
        "sampleName",
        "mounted",
        "datasets",
        "master_file",
        "force_rerun",
        "extra_data_files",
        # Plugin specific keys
        "xds_space_group", "xds_unit_cell", "xds_resolution", "xds_model_pdb", "xds_native", "xds_nproc", "xds_njobs", "xds_proc_dir_root", "xds_start", "xds_end",
        "nxds_space_group", "nxds_unit_cell", "nxds_nproc", "nxds_njobs", "nxds_proc_dir_root", "nxds_powder", "nxds_reference_hkl",
        "xia2_pipeline", "xia2_space_group", "xia2_unit_cell", "xia2_highres", "xia2_model", "xia2_nproc", "xia2_njobs", "xia2_fast", "xia2_trust_beam_centre", "xia2_proc_dir_root",
        "xia2_ssx_space_group", "xia2_ssx_unit_cell", "xia2_ssx_model", "xia2_ssx_reference_hkl", "xia2_ssx_nproc", "xia2_ssx_njobs", "xia2_ssx_proc_dir_root",
        "autoproc_space_group", "autoproc_unit_cell", "autoproc_highres", "autoproc_model", "autoproc_nproc", "autoproc_njobs", "autoproc_fast", "autoproc_proc_dir_root",
        "crystfel_proc_dir_root", "peak_method", "min_snr", "pdb_file", "crystfel_nproc", "crystfel_peaks_method", "crystfel_min_snr", "crystfel_min_snr_biggest_pix", "crystfel_min_snr_peak_pix", "crystfel_min_sig", "crystfel_local_bg_radius", "crystfel_pdb_file", "crystfel_extra_options"
    }
)

# --- Core Functions (Accessible on Import) ---


def determine_active_config():
    """Determines the active server URL and beamline based on the hostname."""
    # Use ServerConfig to get the URL. 
    # For beamline, we can do a quick check here or add it to ServerConfig if reused.
    # The original logic mapped hostname prefix to beamline.
    
    server_url = ServerConfig.get_dataproc_url()
    
    hostname = socket.gethostname()
    beamline = "23b"
    if hostname.startswith("bl1"):
        beamline = "23i"
    elif hostname.startswith("bl2"):
        beamline = "23o"
        
    return {
        "server_url": server_url,
        "beamline": beamline
    }


def validate_job_data(job_data: dict):
    """
    Validates the job_data dictionary.
    Returns: (is_valid: bool, messages: list[str])
    """
    is_valid = True
    messages = []

    missing_keys = REQUIRED_XPROCESS_KEYS - set(job_data.keys())
    if missing_keys:
        messages.append(
            f"Missing required keys: {', '.join(sorted(list(missing_keys)))}"
        )
        is_valid = False

    # Conditional validation for legacy vs plugin jobs
    if "datasets" not in job_data and "master_file" not in job_data:
        # Assume legacy job, which strictly requires proc_dir and data_dir
        if "proc_dir" not in job_data:
            messages.append("Missing required key for legacy job: proc_dir")
            is_valid = False
        if "data_dir" not in job_data:
            messages.append("Missing required key for legacy job: data_dir")
            is_valid = False

    unknown_keys = set(job_data.keys()) - KNOWN_XPROCESS_KEYS
    if unknown_keys:
        # This is treated as a warning, not a validation failure.
        messages.append(
            f"Warning: Unknown keys provided: {', '.join(sorted(list(unknown_keys)))}"
        )

    return is_valid, messages


def send_job(server_url: str, job_data: dict, timeout: float = 30.0):
    """
    Sends a job processing request to the server.
    Returns: A tuple (success: bool, response_text: str)
    """
    full_endpoint_url = server_url.rstrip("/") + JOB_ENDPOINT
    print(f"\nSending job to: {full_endpoint_url}")
    print(f"Job data payload:\n{json.dumps(job_data, indent=2)}")

    try:
        response = requests.post(full_endpoint_url, json=job_data, timeout=timeout)
        response.raise_for_status()
        return True, response.text
    except requests.exceptions.RequestException as e:
        error_message = f"Request failed: {e}"
        print(error_message)
        return False, error_message


def validate_and_submit(job_data_dict: dict, force: bool = True, timeout: float = 30.0):
    """
    Validates and submits a job dictionary. This is the primary entry point for imports.
    Args:
        job_data_dict (dict): The dictionary containing the job data.
        force (bool): If False, will prompt user to continue if unknown keys are found.
        timeout (float): Request timeout in seconds.
    Returns:
        A tuple (success: bool, message: str)
    """
    resolved_config = determine_active_config()
    server_url = resolved_config["server_url"]

    if not isinstance(job_data_dict, dict):
        return False, "Error: job_data must be a dictionary."

    # --- Augment data (same logic as before) ---
    if "beamline" not in job_data_dict:
        job_data_dict["beamline"] = resolved_config["beamline"]
    if "username" not in job_data_dict:
        for e in job_data_dict.get("data_dir", "").split("/"):
            if e.startswith("esaf"):
                job_data_dict["username"] = e
                break
    if "username" not in job_data_dict:
        job_data_dict["username"] = os.getenv("USER")

    # --- Validation ---
    is_valid, messages = validate_job_data(job_data_dict)
    print("--- Job Validation ---")
    for msg in messages:
        print(f"- {msg}")

    if not is_valid:
        error_msg = "Critical validation failed. Cannot submit."
        print(error_msg)
        return False, error_msg

    unknown_keys_present = set(job_data_dict.keys()) - KNOWN_XPROCESS_KEYS
    if unknown_keys_present and not force:
        confirm = (
            input("Warnings present. Proceed with submission? (yes/no): ")
            .strip()
            .lower()
        )
        if confirm != "yes":
            return False, "Submission aborted by user."

    # --- Submission ---
    return send_job(server_url, job_data_dict, timeout)


# --- Command-Line Execution Logic ---


def main():
    """Function to handle command-line argument parsing and execution."""
    # Determine default server URL for help text
    default_server_url = determine_active_config()["server_url"]

    parser = argparse.ArgumentParser(
        description="Client to send jobs to the Data Processing Server."
    )
    parser.add_argument(
        "--server-url",
        type=str,
        default=default_server_url,
        help=f"Base URL of the server (default: {default_server_url}).",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--job-json-file", type=str, help="Path to a JSON file containing the job data."
    )
    group.add_argument(
        "--job-json-string", type=str, help="A JSON string representing the job data."
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Request timeout in seconds (default: 30.0).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force submission despite warnings about unknown keys.",
    )

    args = parser.parse_args()

    job_data_dict = {}
    if args.job_json_file:
        try:
            with open(args.job_json_file, "r") as f:
                job_data_dict = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            print(
                f"Error reading or parsing file {args.job_json_file}: {e}",
                file=sys.stderr,
            )
            sys.exit(1)
    elif args.job_json_string:
        try:
            job_data_dict = json.loads(args.job_json_string)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON string: {e}", file=sys.stderr)
            sys.exit(1)

    # Call the core logic function
    success, message = validate_and_submit(
        job_data_dict, force=args.force, timeout=args.timeout
    )

    if success:
        print("\n--- Final Status: SUCCESS ---")
        print(message)
    else:
        print("\n--- Final Status: FAILED ---")
        print(message)
        sys.exit(1)


if __name__ == "__main__":
    main()
