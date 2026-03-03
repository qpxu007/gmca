import asyncio
import datetime
import grp
import os
# For user/group operations (ensure these modules are available)
import pwd
import shutil
import stat
import subprocess
import threading  # Kept for original run_async, but run_async_real uses asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable, List, Dict, Any  # Added List, Dict, Any

from qp2.log.logging_config import get_logger, setup_logging

logger = get_logger(__name__)

# SLURM template from the original script.py
slurm_template = """\
#!/bin/sh
#SBATCH --export=ALL
#SBATCH -o {joblabel}.out
echo job started at `date "+%Y-%m-%d %H:%M:%S"`
echo "host: `hostname -s` (`uname`) user: `whoami`"
echo
echo
{chdir}
{script_text}
echo
echo
echo job finished at `date "+%Y-%m-%d %H:%M:%S"`
"""


def get_primary_group(username: str) -> str:
    """Gets the primary group name for a given username."""
    try:
        pw_record = pwd.getpwnam(username)
        gid = pw_record.pw_gid
        group_name = grp.getgrgid(gid).gr_name
        return group_name
    except KeyError:
        logger.error(
            f"User '{username}' not found when trying to get primary group.")
        raise
    except Exception as e:
        logger.error(f"Error getting primary group for {username}: {e}")
        raise


def recursive_chown(path: str, user: str, group: str) -> None:
    """Recursively changes ownership of a path."""
    try:
        # Ensure user and group are valid before calling shutil.chown
        # shutil.chown can take user/group names directly on POSIX.
        uid = pwd.getpwnam(user).pw_uid
        gid = grp.getgrnam(group).gr_gid

        shutil.chown(path, user=uid, group=gid)  # Use uid/gid for robustness
        for root, dirs, files in os.walk(path):
            for name in dirs:
                shutil.chown(os.path.join(root, name), user=uid, group=gid)
            for name in files:
                shutil.chown(os.path.join(root, name), user=uid, group=gid)
    except KeyError as e:
        logger.error(
            f"User '{user}' or group '{group}' not found for chown: {e}")
        raise  # Re-raise as this is critical
    except OSError as e:
        logger.warning(
            f"OSError changing ownership of {path} to {user}:{group}: {e}. Insufficient privileges?")
        # Do not re-raise if it's a permission issue and we want to proceed cautiously.
        # However, for script execution, this might be critical.
    except Exception as e:
        logger.error(f"Unexpected error in recursive_chown for {path}: {e}")
        raise


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class JobConfig:
    """Configuration for job execution"""
    wdir: str
    script_name: str
    script_text: str  # This is the core command/script content
    nproc: int = 1
    run_as_user: Optional[str] = None  # Changed to Optional
    run_as_group: Optional[str] = None  # Changed to Optional
    runner: str = 'local'  # 'local' or 'slurm'
    timeout: int = 3600  # 1 hour default timeout for run_async_real
    # Add other SBATCH options if needed, e.g., partition, memory, etc.
    sbatch_options: Dict[str, str] = field(default_factory=dict)


class Script:
    def __init__(self, config: JobConfig):
        self.config = self._validate_config(config)
        # For asyncio
        self.process: Optional[asyncio.subprocess.Process] = None
        # For original run_async
        self.legacy_process: Optional[subprocess.Popen] = None
        self.status = JobStatus.PENDING

        self.my_env = os.environ.copy()
        self.my_env.pop('LD_PRELOAD', None)  # Remove problematic env var

        # Determine user/group for chown and sbatch --uid
        self.target_username: Optional[str] = self.config.run_as_user
        self.target_groupname: Optional[str] = self.config.run_as_group
        if self.target_username and not self.target_groupname:
            try:
                self.target_groupname = get_primary_group(self.target_username)
            except Exception:  # If get_primary_group fails
                # Logged in get_primary_group, decide if this is fatal for Script init
                # For now, allow proceeding without group if user is set.
                # Permissions might not be fully set as intended.
                pass

        self._setup_user_permissions()  # Call after target_username/groupname are set

    def _validate_config(self, config: JobConfig) -> JobConfig:
        """Validate job configuration."""
        if not os.path.isabs(config.wdir):
            config.wdir = os.path.abspath(config.wdir)
            logger.info(
                f"Converted working directory to absolute path: {config.wdir}")

        if not config.wdir or not config.script_name:
            raise ValueError("Working directory and script name are required.")

        if not config.script_text:  # The actual commands to run
            raise ValueError("Script text (commands) cannot be empty.")

        return config

    def _setup_user_permissions(self):
        """Sets up permissions for the working directory if run_as_user is specified."""
        if self.target_username and self.target_groupname:
            try:
                if not os.path.exists(self.config.wdir):
                    os.makedirs(self.config.wdir, exist_ok=True)
                    logger.info(
                        f"Created working directory: {self.config.wdir}")
                # This recursive_chown should be privileged if not running as target_user already
                recursive_chown(self.config.wdir,
                                self.target_username, self.target_groupname)
                logger.info(
                    f"Attempted to set ownership of '{self.config.wdir}' to user '{self.target_username}', group '{self.target_groupname}'.")
            except OSError as e:
                logger.warning(
                    f"OSError setting up permissions for '{self.config.wdir}': {e}. Insufficient privileges or non-existent user/group?")
            except ValueError as e:  # From recursive_chown if user/group not found
                logger.error(
                    f"Failed to set up permissions due to invalid user/group: {e}")
                raise  # This is likely critical
            except Exception as e:
                logger.error(
                    f"Unexpected error in _setup_user_permissions for '{self.config.wdir}': {e}")

    def write_script(self, overwrite: bool = True) -> str:
        """
        Writes the script_text into a script file, possibly formatted with slurm_template.
        Returns the path to the written script file.
        """
        if not os.path.exists(self.config.wdir):
            try:
                os.makedirs(self.config.wdir, exist_ok=True)
                logger.info(
                    f"Created working directory for script: {self.config.wdir}")
                # If wdir was created, set ownership if target_user is specified
                if self.target_username and self.target_groupname:
                    recursive_chown(
                        self.config.wdir, self.target_username, self.target_groupname)
            except OSError as e:
                logger.error(
                    f"Failed to create working directory {self.config.wdir} in write_script: {e}")
                raise

        script_file_path = os.path.join(
            self.config.wdir, self.config.script_name)

        if os.path.isfile(script_file_path) and not overwrite:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            backup_name = f"{script_file_path}.{timestamp}"
            os.rename(script_file_path, backup_name)
            logger.info(f"Backed up existing script to {backup_name}")

        # The content of the script file is always the config.script_text,
        # potentially wrapped in the SLURM template.
        # For local execution, it's a simple shell script. For SLURM, it's a SLURM batch script.

        # Default chdir to script's own directory
        # The slurm_template expects `chdir` and `script_text`
        # `script_text` here is self.config.script_text (the actual commands)
        # `joblabel` can be derived from script_name (e.g., without .sh)
        job_label = os.path.splitext(self.config.script_name)[0]

        # Always use the slurm_template for writing the script file itself,
        # as per original script.py. The runner decides *how* to execute this file.
        content_to_write = slurm_template.format(
            joblabel=job_label,
            chdir=f"cd {self.config.wdir}",  # SLURM script will cd to wdir
            script_text=self.config.script_text
        )

        with open(script_file_path, "w") as fh:
            fh.write(content_to_write)
        logger.info(f"Script '{script_file_path}' written successfully.")

        # Set permissions
        try:
            # Make executable by user, readable by user/group/others
            os.chmod(script_file_path, stat.S_IRWXU |
                     stat.S_IRGRP | stat.S_IROTH)  # 744
            if self.target_username and self.target_groupname:
                # shutil.chown can take string names on POSIX
                uid = pwd.getpwnam(self.target_username).pw_uid
                gid = grp.getgrnam(self.target_groupname).gr_gid
                shutil.chown(script_file_path, user=uid, group=gid)
                logger.info(
                    f"Set ownership of script '{script_file_path}' to '{self.target_username}:{self.target_groupname}'.")
        except OSError as e:
            logger.warning(
                f"OSError setting permissions/ownership for '{script_file_path}': {e}. Insufficient privileges?")
        except KeyError as e:  # User/group not found
            logger.error(
                f"User '{self.target_username}' or group '{self.target_groupname}' not found for chown on script: {e}")
            # This might be critical if script cannot be run by intended user.
        except Exception as e:
            logger.error(
                f"Error setting permissions for '{script_file_path}': {e}")

        return script_file_path

    def _build_command(self, script_file_path: str) -> List[str]:
        """Builds the command list for execution based on the runner."""
        cmd_list: List[str] = []

        sbatch_user_args = []
        if self.config.runner == 'slurm':
            if os.getenv("USER") == "root" and self.target_username and self.target_groupname:
                # Ensure numeric UIDs/GIDs for sbatch --uid if possible
                try:
                    uid = pwd.getpwnam(self.target_username).pw_uid
                    # gid = grp.getgrnam(self.target_groupname).gr_gid # Not directly used by --uid, but good to check
                    # 5L seems specific, check SLURM docs
                    sbatch_user_args.extend(
                        [f"--uid={uid}", "--get-user-env=5L"])
                except KeyError:
                    logger.warning(
                        f"Cannot get UID for user {self.target_username} for sbatch --uid. Omitting.")

            cmd_list.append("sbatch")
            cmd_list.extend(sbatch_user_args)
            cmd_list.extend([
                f"--cpus-per-task={self.config.nproc}",
                # Job name from script name
                f"--job-name=j.{os.path.splitext(self.config.script_name)[0]}",
                f"--chdir={self.config.wdir}",  # Working directory for SLURM
                # Export env, set HOME
                "--export=ALL,HOME={}".format(self.config.wdir)
            ])
            # Add custom sbatch options
            for key, value in self.config.sbatch_options.items():
                cmd_list.append(f"--{key}={value}")

            cmd_list.append(script_file_path)  # The script to be submitted

        elif self.config.runner == 'local':
            # For local, the command is just the script file itself.
            # Ensure it's executable and has a shebang. The slurm_template provides #!/bin/sh.
            cmd_list.append(script_file_path)
        else:
            raise ValueError(f"Unsupported runner: {self.config.runner}")

        return cmd_list

    async def run_async_real(self, callback: Optional[Callable[['Script', int], Any]] = None) -> int:
        """
        Writes the script, builds the command, and executes it asynchronously using asyncio.
        Calls the callback with self and return_code upon completion.
        """
        try:
            self.status = JobStatus.RUNNING
            # Writes the actual commands from self.config.script_text into the file
            script_file = self.write_script()

            # _build_command now returns a list of arguments
            cmd_list = self._build_command(script_file)
            # For subprocess.shell=True or logging
            cmd_str = " ".join(cmd_list)

            logger.info(
                f"Executing job '{self.config.script_name}' with command: {cmd_str}")

            # Define stdout/stderr log files
            stdout_log = os.path.join(
                self.config.wdir, f"{os.path.splitext(self.config.script_name)[0]}.out.async")
            stderr_log = os.path.join(
                self.config.wdir, f"{os.path.splitext(self.config.script_name)[0]}.err.async")

            with open(stdout_log, 'w') as out_f, open(stderr_log, 'w') as err_f:
                # For SLURM, sbatch itself is the command. For local, the script is.
                # asyncio.create_subprocess_shell is suitable if cmd_str is a full command string.
                # If cmd_list is [executable, arg1, arg2], use shell=False.
                # Given sbatch and local script execution, shell=True with cmd_str is often simpler.
                # Let's use shell=False with cmd_list for better security and control.

                # If runner is 'local', the script itself is the executable.
                # If runner is 'slurm', 'sbatch' is the executable.
                executable = cmd_list[0]
                args = cmd_list[1:]

                self.process = await asyncio.create_subprocess_exec(
                    executable,
                    *args,  # Unpack arguments
                    cwd=self.config.wdir,  # Working directory
                    env=self.my_env,      # Custom environment
                    stdout=out_f,         # Redirect stdout
                    stderr=err_f          # Redirect stderr
                )

                logger.info(
                    f"Job '{self.config.script_name}' (PID: {self.process.pid}) started. Output: {stdout_log}, Errors: {stderr_log}")

                try:
                    return_code = await asyncio.wait_for(self.process.wait(), timeout=self.config.timeout)
                    self.status = JobStatus.COMPLETED if return_code == 0 else JobStatus.FAILED
                    logger.info(
                        f"Job '{self.config.script_name}' finished with return code: {return_code}")
                except asyncio.TimeoutError:
                    logger.warning(
                        f"Job '{self.config.script_name}' timed out after {self.config.timeout}s. Terminating...")
                    self.process.terminate()
                    try:
                        # Wait a bit for termination
                        await asyncio.wait_for(self.process.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"Job '{self.config.script_name}' did not terminate gracefully after timeout. Killing...")
                        self.process.kill()
                        await self.process.wait()  # Ensure kill is processed
                    self.status = JobStatus.FAILED
                    return_code = -1  # Indicate timeout
                    logger.info(
                        f"Job '{self.config.script_name}' terminated due to timeout.")

                if callback:
                    # If callback is an async function
                    if asyncio.iscoroutinefunction(callback):
                        await callback(self, return_code)
                    else:  # If callback is a regular function
                        callback(self, return_code)

                return return_code

        except Exception as e:
            self.status = JobStatus.FAILED
            logger.error(
                f"Job '{self.config.script_name}' failed during setup or execution: {e}", exc_info=True)
            # If callback exists and expects notification on failure too
            if callback:
                if asyncio.iscoroutinefunction(callback):
                    await callback(self, -1)  # or appropriate error code
                else:
                    callback(self, -1)
            raise  # Re-raise the exception to be handled by the caller

    # Original run_async method (threaded, blocking style) for compatibility or specific use cases
    # This is largely from the original script.py [2]
    def run_async(self, debug: bool = False) -> int:
        """
        Original threaded version of run_async.
        Writes the script and submits it using a separate thread.
        This is largely for backward compatibility or if true asyncio is not desired.
        Note: The 'callback' functionality of run_async_real is not present here.
        """
        logger.info(
            f"Using legacy run_async (threaded) for '{self.config.script_name}'.")

        def target():
            try:
                script_file = self.write_script()
                cmd_list = self._build_command(script_file)
                # subprocess.Popen with shell=True needs a string command
                cmd_str = " ".join(cmd_list)

                logger.info(
                    f"[Threaded] Executing job '{self.config.script_name}' with command: {cmd_str}")

                # Log files for threaded execution
                stdout_log_th = os.path.join(
                    self.config.wdir, f"{os.path.splitext(self.config.script_name)[0]}.out.thread")
                stderr_log_th = os.path.join(
                    self.config.wdir, f"{os.path.splitext(self.config.script_name)[0]}.err.thread")

                with open(stdout_log_th, "w") as fileout_th, open(stderr_log_th, "w") as fileerr_th:
                    # Original used subprocess.DEVNULL for out, but logging to file is better
                    self.legacy_process = subprocess.Popen(
                        cmd_str,  # Popen with shell=True takes a string
                        shell=True,  # Original used shell=True
                        cwd=self.config.wdir,
                        env=self.my_env,
                        stdout=fileout_th,
                        stderr=fileerr_th
                    )

                # .communicate() will block until the process finishes.
                # If this target function is in a thread, this is fine.
                stdout_data, stderr_data = self.legacy_process.communicate()
                return_code = self.legacy_process.returncode

                if return_code == 0:
                    self.status = JobStatus.COMPLETED
                    logger.info(
                        f"[Threaded] Job '{self.config.script_name}' completed successfully (RC: {return_code}).")
                else:
                    self.status = JobStatus.FAILED
                    logger.warning(
                        f"[Threaded] Job '{self.config.script_name}' failed (RC: {return_code}).")
                    # stderr is already redirected to fileerr_th

            except Exception as e:
                self.status = JobStatus.FAILED
                logger.error(
                    f"[Threaded] Job '{self.config.script_name}' execution error: {e}", exc_info=True)

        thread = threading.Thread(target=target)
        thread.daemon = True  # Allow main program to exit even if thread is running
        thread.start()

        # Original script.py had thread.join(30) here, which is problematic.
        # It makes run_async somewhat blocking for up to 30s, or the job finishes.
        # For a true async submission (fire and forget style), don't join here.
        # The caller needs to manage tracking job completion if required.
        # If we want to ensure the job submission part (e.g. sbatch call) is done:
        # For 'slurm', sbatch returns quickly. For 'local', it's the actual job runtime.
        # We'll remove the join to make it more "fire-and-forget" from the caller's perspective.
        logger.info(
            f"[Threaded] Job '{self.config.script_name}' submitted via thread.")
        return 0  # Original returned 0 to indicate submission.


# Example usage:
async def main_async():
    setup_logging()

    # Ensure the working directory exists for the example
    test_wdir = "./test_script_output"
    os.makedirs(test_wdir, exist_ok=True)

    # --- Test local execution ---
    local_config = JobConfig(
        wdir=test_wdir,
        script_name="local_test_script.sh",
        script_text="echo 'Hello from local script!'\nls -l\ndate\nexit 0",
        runner='local',
        timeout=10
    )
    local_script = Script(local_config)

    async def my_callback(script_instance: Script, rc: int):
        logger.info(
            f"Callback received for '{script_instance.config.script_name}': Status={script_instance.status}, RC={rc}")

    logger.info("Running local script with run_async_real...")
    await local_script.run_async_real(callback=my_callback)

    # --- Test SLURM execution (will likely only write script if sbatch is not configured locally) ---
    # This is a mock example; sbatch command might fail if SLURM isn't available
    slurm_config = JobConfig(
        wdir=test_wdir,
        script_name="slurm_test_script.sh",
        script_text="echo 'Hello from SLURM script!'\nsrun hostname\ndate\nexit 0",
        runner='slurm',
        nproc=2,
        sbatch_options={"partition": "debug",
                        "mem": "1G"},  # Example sbatch options
        timeout=60
    )
    slurm_script = Script(slurm_config)
    logger.info("Running SLURM script with run_async_real (mock)...")
    # Note: This will try to run 'sbatch'. If sbatch is not found or fails, it will be logged.
    try:
        await slurm_script.run_async_real(callback=my_callback)
    except FileNotFoundError:
        logger.warning(
            "sbatch command not found. SLURM script was written but not executed.")
    except Exception as e:
        logger.error(f"SLURM script execution failed: {e}")

    # --- Test legacy run_async (threaded) ---
    legacy_local_config = JobConfig(
        wdir=test_wdir,
        script_name="legacy_local_test_script.sh",
        script_text="echo 'Hello from legacy local script!'\nsleep 2\necho 'Legacy done.'\nexit 0",
        runner='local'
    )
    legacy_script = Script(legacy_local_config)
    logger.info("Running local script with legacy run_async (threaded)...")
    legacy_script.run_async()
    # Give the thread some time to complete for demo purposes
    await asyncio.sleep(5)
    logger.info(f"Legacy script final status: {legacy_script.status}")


if __name__ == '__main__':
    asyncio.run(main_async())
