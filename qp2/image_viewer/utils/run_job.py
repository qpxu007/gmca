import os
import shutil
import stat
import subprocess
import time
from typing import Optional, List, Union

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def is_sbatch_available():
    return shutil.which("sbatch") is not None


def run_command(
        cmd: Union[str, List[str]],
        cwd: Optional[str] = None,
        method: str = "shell",
        run_as_user: Optional[str] = None,
        job_name: str = "job",
        nodes: int = 1,
        processors: int = 1,
        memory: str = "2gb",
        walltime: str = "10:00:00", # Changed default from 12:00:00 to 10:00:00 (600 mins)
        background: bool = False,
        gpu: bool = False,
        pre_command: Optional[str] = None,
        dry_run: bool = False,
        quiet: bool = False,
) -> Optional[str]:
    """
    Run a command via shell or SLURM, saving stdout/stderr to <job_name>.out.

    Args:
        cmd: Command as string or list
        cwd: Working directory (defaults to current)
        method: "shell" or "slurm"
        run_as_user: User to run as
        job_name: Job name (used for output file)
        nodes: SLURM nodes
        processors: SLURM processors per node
        memory: SLURM memory
        walltime: SLURM max runtime
        dry_run: If True, log script and return dummy ID without execution
        quiet: If True, log run details at DEBUG level instead of INFO

    Note:
        For autoproc, xds, nxds the nproc and njobs are only passed into actual programs which handle resource requests.
        For xia2, crystfel, dials, nproc and njobs (usually 1) should be used in resource requests.

    Returns:
        str or None: Job ID for SLURM, None for shell

    Raises:
        ValueError: Invalid method
        PermissionError: CWD not writable
        RuntimeError: Execution fails
    """
    method = method.lower()
    if method not in ["shell", "slurm"]:
        raise ValueError("Method must be 'shell' or 'slurm'")

    # Auto-detect if we are already inside a Slurm job
    # If so, downgrade to "shell" to prevent nested submissions
    if method == "slurm" and "SLURM_JOB_ID" in os.environ:
        # Check if we are truly inside an allocation (sometimes env vars persist in login shells if not careful, 
        # but usually SLURM_JOB_ID implies an active allocation).
        # We can also check SLURM_NODELIST or SLURM_JOB_NAME to be sure.
        job_id = os.environ.get("SLURM_JOB_ID")
        logger.info(f"Detected running inside Slurm job {job_id}. Forcing method='shell' to prevent nested submission.")
        method = "shell"

    effective_cwd = cwd or os.getcwd()

    if not dry_run:
        # Ensure CWD is writable
        if not os.path.exists(effective_cwd):
            os.makedirs(effective_cwd, mode=0o755, exist_ok=True)
        if not os.access(effective_cwd, os.W_OK):
            if not run_as_user:
                os.chmod(effective_cwd, 0o755)
            if not os.access(effective_cwd, os.W_OK):
                raise PermissionError(f"CWD '{effective_cwd}' not writable")

    output_file = os.path.join(os.path.abspath(effective_cwd), f"{job_name}.out")
    script_file_path = os.path.join(os.path.abspath(effective_cwd), f"{job_name}.sh")

    cmd_str = cmd if isinstance(cmd, str) else " ".join(cmd)
    # if isinstance(cmd, list):
    #    cmd_str = ' '.join(shlex.quote(str(c)) for c in cmd)
    # else:
    #    cmd_str = cmd # Assume a pre-formatted string is already safe

    log_func = logger.debug if quiet else logger.info
    log_func(
        f"Running in '{effective_cwd}' via {method}, output to '{output_file}': {cmd_str}"
    )

    # --- SCRIPT GENERATION LOGIC ---
    script_content = ""
    if method == "shell":
        # Create a simple shell script
        script_content = f"#!/bin/bash\n"
        script_content += f"# Job '{job_name}' submitted for local shell execution\n\n"



        # Construct the block to execute
        cmd_block = f"echo \"--- Job started on $(hostname) at $(date) ---\"\n"
        if pre_command:
            cmd_block += f"# --- Pre-command Environment Setup ---\n"
            cmd_block += f"{pre_command}\n\n"
        
        shell_cmd_str = f"sudo -u {run_as_user} {cmd_str}" if run_as_user else cmd_str
        cmd_block += f"# --- Wrapped Command ---\n"
        cmd_block += f"{shell_cmd_str}\n"
        cmd_block += f"echo \"--- Job finished on $(hostname) at $(date) ---\""

        # Wrap in braces for redirection to capture ALL output (including setup errors)
        if background:
            script_content += f"{{\n{cmd_block}\n}} > {output_file} 2>&1 &\n"
        else:
            script_content += f"{{\n{cmd_block}\n}} > {output_file} 2>&1\n"

    else:  # slurm
        # Create a full sbatch script
        script_content = f"#!/bin/bash\n"
        script_content += f"#SBATCH --job-name={job_name}\n"
        script_content += f"#SBATCH --output={output_file}\n"
        
        if nodes:
            script_content += f"#SBATCH --nodes={nodes}\n"
        if processors:
            script_content += f"#SBATCH --cpus-per-task={processors}\n"
        if memory:
            script_content += f"##SBATCH --mem={memory}\n"
            
        script_content += f"##SBATCH --time={walltime}\n"
        script_content += f"#SBATCH --hint=nomultithread\n"
        
        if run_as_user:
            script_content += f"#SBATCH --uid={run_as_user}\n"
            
        if gpu:
            script_content += f"#SBATCH --partition=gpu\n"

        script_content += f"\necho \"--- Job started on $(hostname) at $(date) ---\"\n"



        if pre_command:
            script_content += "\n# --- Pre-command Environment Setup ---\n"
            script_content += f"{pre_command}\n"

        script_content += "\n# --- Wrapped Command ---\n"
        script_content += f"{cmd_str}\n"
        script_content += f"\necho \"--- Job finished on $(hostname) at $(date) ---\"\n"

    if dry_run:
        logger.info(f"--- DRY RUN: Script content for {script_file_path} ---")
        logger.info(script_content)
        logger.info("--- DRY RUN: End of script content ---")
        return "dry_run_job_id"

    try:
        with open(script_file_path, "w") as f:
            f.write(script_content)
            f.flush()
            os.fsync(f.fileno())
        
        # Make the script executable
        os.chmod(script_file_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
        # Add a small delay to ensure file handle is fully released by OS
        time.sleep(0.1) 
        
        logger.info(f"Job script saved to: {script_file_path}")
    except IOError as e:
        logger.error(f"Failed to write job script to {script_file_path}: {e}")
        raise RuntimeError(f"Failed to write job script") from e

    try:
        if method == "shell":
            # Execute the script we just wrote
            # Return the Popen object for background jobs ---
            process = subprocess.Popen(script_file_path, shell=True, cwd=effective_cwd)
            if background:
                return process  # Return the process handle
            else:
                process.wait()  # Block until complete
                return subprocess.CompletedProcess(
                    cmd, process.returncode, process.stdout, process.stderr
                )

        else:  # SLURM
            # Submit the script we just wrote
            slurm_cmd = ["sbatch"]
            if not background:
                slurm_cmd.append("--wait")
            slurm_cmd.append(script_file_path)

            logger.debug(f"slurm cmd = {' '.join(slurm_cmd)}")
            result = subprocess.run(
                slurm_cmd, cwd=effective_cwd, capture_output=True, text=True
            )

            if result.returncode != 0:
                # If sbatch fails, log the error and the content of the script
                logger.error(
                    f"sbatch submission failed for {script_file_path}. Error: {result.stderr.strip()}"
                )
                logger.error(
                    f"--- FAILED SCRIPT CONTENT ---\n{script_content}\n-----------------------------"
                )
                return None

            return (
                result.stdout.split()[-1]
                if "Submitted batch job" in result.stdout
                else None
            )
    except Exception as e:
        logger.error("Failed to execute job script via %s: %s", method, e)
        raise RuntimeError(f"Failed to execute job script: {script_file_path}") from e
