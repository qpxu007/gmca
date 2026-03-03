import logging
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from filelock import FileLock, Timeout  # Import FileLock and Timeout

sys.path.append("../gmcaproc")

from run_job import run_command
from cbfreader import extract_numbers
from symmetry import Symmetry
from param_sweep import ParameterSweepBase, ParameterSweepJob

# Assuming logger is configured elsewhere or configure it here
logger = logging.getLogger(__name__)
# Basic config if not set up
if not logger.hasHandlers():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )


class ShelXProcessor:
    """
    Handles the execution of individual SHELX steps (C, D, E) for a specific
    set of parameters within a defined processing directory.
    """

    DEFAULT_SHELXE_SOLVENT = 0.5
    DEFAULT_SHELXE_CYCLES = 20
    DEFAULT_SHELXC_MAXM = 2  # Max memory for SHELXC
    DEFAULT_SHELXC_NTRY = 0  # Usually 0 when FIND is used

    def __init__(
            self,
            prefix: str,
            proc_dir: Path,
            xds_ascii_hkl_file: Path,
            unit_cell: Optional[str] = None,
            space_group: Optional[str] = None,
            shelxc_params: Optional[Dict[str, Any]] = None,
            shelxe_params: Optional[Dict[str, Any]] = None,
    ):
        """
        Initializes the processor for a single SHELX run.
        Args:
            prefix: Base name for output files (e.g., 'mycrystal_run1').
            proc_dir: Directory where SHELX jobs will run and outputs stored.
            xds_ascii_hkl_file: Path to the input XDS_ASCII.HKL file.
            unit_cell: Unit cell parameters (optional, attempts to read from HKL).
            space_group: Space group symbol (optional, attempts to read from HKL).
            shelxc_params: Additional parameters for SHELXC input.
            shelxe_params: Parameters for SHELXE execution.
        """
        self.prefix = prefix
        self.proc_dir = Path(proc_dir)
        self.xds_ascii_hkl_file = Path(xds_ascii_hkl_file)
        self.unit_cell = unit_cell
        self.space_group = space_group

        # Ensure processing directory exists
        self.proc_dir.mkdir(parents=True, exist_ok=True)

        self.shelxc_inp_file = self.proc_dir / f"{self.prefix}_shelxc.inp"
        self.shelxd_ins_file = self.proc_dir / f"{self.prefix}_fa.ins"
        self.shelxd_res_file = self.proc_dir / f"{self.prefix}_fa.res"
        self.shelxd_lst_file = self.proc_dir / f"{self.prefix}_fa.lst"
        self.shelxd_hkl_file = self.proc_dir / f"{self.prefix}_fa.hkl"
        self.shelxe_lst_file = self.proc_dir / f"{self.prefix}.lst"
        self.shelxe_pdb_file = self.proc_dir / f"{self.prefix}.pdb"

        # Default parameters, can be overridden
        self.shelxc_inp_base: Dict[str, Any] = {
            "SAD": str(self.xds_ascii_hkl_file.resolve()),  # Use absolute path
            "MAXM": self.DEFAULT_SHELXC_MAXM,
            "NTRY": self.DEFAULT_SHELXC_NTRY,
            "FIND": "8",  # Default, can be changed via set_find_parameter
            **(shelxc_params or {}),
        }

        self.shelxe_params: Dict[str, Any] = {
            "solvent": self.DEFAULT_SHELXE_SOLVENT,
            "cycles": self.DEFAULT_SHELXE_CYCLES,
            **(shelxe_params or {}),
        }

        self.results: Dict[str, Any] = {}  # Store results specific to this run

    def _ensure_crystal_parameters(self) -> None:
        """
        Ensures unit cell and space group are available, reading from HKL if needed.
        Raises ValueError if parameters cannot be determined.
        """
        if self.unit_cell and self.space_group:
            logger.debug(f"Using provided Unit Cell: {self.unit_cell}")
            logger.debug(f"Using provided Space Group: {self.space_group}")
            return

        logger.info(
            f"Attempting to derive crystal parameters from {self.xds_ascii_hkl_file}"
        )

        found_cell = self.unit_cell is not None
        found_spg = self.space_group is not None

        try:
            with open(self.xds_ascii_hkl_file, "r") as f:
                for line in f:
                    if line.startswith("!END_OF_HEADER"):
                        break
                    if not found_cell and line.startswith("!UNIT_CELL_CONSTANTS="):
                        self.unit_cell = line.split("=", 1)[1].strip()
                        logger.debug(f"Found Unit Cell: {self.unit_cell}")
                        found_cell = True
                    elif not found_spg and line.startswith("!SPACE_GROUP_NUMBER="):
                        spg_num_str = line.split("=", 1)[1].strip()
                        try:
                            spg_num = int(spg_num_str)
                            self.space_group = Symmetry.number_to_symbol(
                                spg_num, remove_parentheses=True
                            )
                            logger.debug(
                                f"Found Space Group Number {spg_num}, Symbol: {self.space_group}"
                            )
                            found_spg = True
                        except (ValueError, AttributeError) as e:
                            logger.warning(
                                f"Could not convert space group number '{spg_num_str}' or find symbol: {e}"
                            )
                        except (
                                Exception
                        ) as e:  # Catch potential issues in Symmetry class
                            logger.warning(
                                f"Error processing space group number {spg_num_str}: {e}"
                            )

                    if found_cell and found_spg:
                        break
        except FileNotFoundError:
            logger.error(f"XDS ASCII HKL file not found: {self.xds_ascii_hkl_file}")
            raise
        except Exception as e:
            logger.error(f"Error reading {self.xds_ascii_hkl_file}: {e}")
            raise

        if not self.unit_cell:
            raise ValueError(
                "Unit cell parameters could not be determined from HKL file."
            )
        if not self.space_group:
            raise ValueError("Space group could not be determined from HKL file.")

    def _prepare_shelxc_input(self) -> None:
        """Prepare SHELXC input file (_shelxc.inp)."""
        logger.debug(f"Preparing SHELXC input file: {self.shelxc_inp_file}")
        self._ensure_crystal_parameters()  # Make sure we have cell/spg

        shelxc_input_data = {
            **self.shelxc_inp_base,
            "CELL": self.unit_cell,
            "SPAG": self.space_group,
        }

        # Filter out any keys with None values
        shelxc_input_data = {
            k: v for k, v in shelxc_input_data.items() if v is not None
        }

        # Generate input string - ensure values are strings
        inp = "\n".join(
            f"{key} {str(value)}" for key, value in shelxc_input_data.items()
        )

        try:
            with open(self.shelxc_inp_file, "w") as fh:
                fh.write(inp)
            logger.debug(f"SHELXC input file written successfully.")
            logger.debug(f"SHELXC input content:\n{inp}")
        except IOError as e:
            logger.error(
                f"Failed to write SHELXC input file {self.shelxc_inp_file}: {e}"
            )
            raise

    def run_shelxc(self) -> None:
        """Prepare input and run SHELXC."""
        logger.info(f"[{self.prefix}] Starting SHELXC execution in {self.proc_dir}")
        try:
            self._prepare_shelxc_input()
            # SHELXC uses the prefix directly, reads _shelxc.inp
            command = f"shelxc {self.prefix} <{Path.absolute(self.shelxc_inp_file)}"
            run_command(
                command,
                cwd=str(self.proc_dir),  # run_command might expect string path
                job_name=f"shelxc_{self.prefix}",
                method="shell",  # SHELXC is usually fast, run directly
            )

            # Check if output HKL exists as a basic success indicator
            if not self.shelxd_hkl_file.is_file():
                raise FileNotFoundError(
                    f"SHELXC output HKL file not found: {self.shelxd_hkl_file}"
                )

            logger.info(f"[{self.prefix}] SHELXC completed successfully.")
        except Exception as e:
            logger.error(f"[{self.prefix}] SHELXC failed: {e}")
            # Consider cleanup? e.g., remove proc_dir?
            raise RuntimeError(
                f"SHELXC execution failed for prefix {self.prefix}"
            ) from e

    def _prepare_shelxd_input(self, resolution_cutoff: float) -> None:
        """
        Prepare SHELXD input file (_fa.ins) by modifying the one created by SHELXC.
        Adds SHEL instruction with resolution cutoff and comments out NTRY.
        """
        logger.debug(f"Preparing SHELXD input file: {self.shelxd_ins_file}")
        if not self.shelxd_ins_file.is_file():
            raise FileNotFoundError(
                f"SHELXD input file {self.shelxd_ins_file} not found. Did SHELXC run correctly?"
            )

        try:
            with open(self.shelxd_ins_file, "r") as f:
                lines = f.readlines()
        except IOError as e:
            logger.error(
                f"Failed to read SHELXD input file {self.shelxd_ins_file}: {e}"
            )
            raise

        processed_lines = []
        insert_pos = -1
        shel_line_exists = False
        for i, line in enumerate(lines):
            stripped_line = line.strip()
            # Find position to insert SHEL (after CELL/ZERR/LATT/SYMM/SFAC/UNIT)
            if stripped_line.startswith(
                    ("CELL", "ZERR", "LATT", "SYMM", "SFAC", "UNIT")
            ):
                insert_pos = i + 1
                processed_lines.append(line)
                # logger.debug(f"Copied line: {line.strip()}")  # Too verbose
                # Comment out conflicting instructions
            # FIND often added by SHELXC
            elif stripped_line.startswith(("NTRY")):
                processed_lines.append(f"REM {line}")
                logger.debug(f"Commented out line: {line.strip()}")
            # Check if SHEL line already exists (e.g., rerun)
            elif stripped_line.startswith("SHEL"):
                # Replace existing SHEL line
                new_shel_line = f"SHEL 999 {resolution_cutoff:.2f}\n"
                processed_lines.append(new_shel_line)
                logger.debug(
                    f"Replaced existing SHEL line with: {new_shel_line.strip()}"
                )
                shel_line_exists = True
                # Update insert_pos to avoid inserting another SHEL line later
                insert_pos = -1  # Mark as handled
            else:
                processed_lines.append(line)

        # Insert SHEL line if it wasn't found and replaced
        if not shel_line_exists and insert_pos != -1:
            new_shel_line = f"SHEL 999 {resolution_cutoff:.2f}\n"
            processed_lines.insert(insert_pos, new_shel_line)
            logger.debug(f"Inserted SHEL line: {new_shel_line.strip()}")
        elif not shel_line_exists and insert_pos == -1:
            logger.error(
                f"Could not find suitable insertion point for SHEL in {self.shelxd_ins_file}"
            )
            raise ValueError(
                f"Could not process SHELXD input file {self.shelxd_ins_file}"
            )

        try:
            with open(self.shelxd_ins_file, "w") as fh:
                fh.write("".join(processed_lines))
            logger.debug(
                f"SHELXD input file written successfully: {self.shelxd_ins_file}"
            )
        except IOError as e:
            logger.error(
                f"Failed to write modified SHELXD input file {self.shelxd_ins_file}: {e}"
            )
            raise

    def run_shelxd(
            self,
            resolution_cutoff: float = 2.0,
            run_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Prepare input and run SHELXD.
        Args:
            resolution_cutoff: High-resolution cutoff for SHELXD.
            run_config: Dictionary containing execution parameters like
                'method' (shell, slurm), 'nodes', 'processors',
                'omp_threads', 'background'.
        """
        logger.info(f"[{self.prefix}] Starting SHELXD with cutoff {resolution_cutoff}A")
        run_cfg = {  # Defaults
            "method": "slurm",
            "nodes": 1,
            "processors": 8,
            "omp_threads": 2,
            "background": True,
            **(run_config or {}),
        }

        try:
            self._prepare_shelxd_input(resolution_cutoff)
            # SHELXD uses _fa as base for .ins and .hkl
            shelxd_base = f"{self.prefix}_fa"

            # Set OMP_NUM_THREADS if specified
            env_prefix = (
                f"export OMP_NUM_THREADS={run_cfg['omp_threads']}; "
                if run_cfg.get("omp_threads")
                else ""
            )

            command = f"{env_prefix}shelxd {shelxd_base}"

            run_command(
                command,
                cwd=str(self.proc_dir),
                job_name=f"shelxd_{self.prefix}",
                method=run_cfg["method"],
                nodes=run_cfg["nodes"],
                processors=run_cfg["processors"],
                background=run_cfg["background"],
            )

            # If background=True, job is just submitted. Success check happens during monitoring.
            logger.info(
                f"[{self.prefix}] SHELXD job submitted (method: {run_cfg['method']})."
            )
        except Exception as e:
            logger.error(f"[{self.prefix}] SHELXD failed: {e}")
            raise RuntimeError(
                f"SHELXD execution failed for prefix {self.prefix}"
            ) from e

    def run_shelxe(
            self, invert: bool = False, run_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Run SHELXE on the result of SHELXD (_fa.res -> .pdb, .lst).
        Args:
            invert: Whether to run SHELXE with the inversion flag (-i).
            run_config: Dictionary containing execution parameters.
        Returns:
            Standard output from the SHELXE command execution.
        """
        mode = "inverted" if invert else "normal"
        logger.info(f"[{self.prefix}] Starting SHELXE ({mode} hand)")
        run_cfg = {  # Defaults
            "method": "slurm",  # SHELXE can sometimes take time
            "nodes": 1,
            "processors": 1,  # Typically single-threaded
            "background": False,  # Usually run sequentially after SHELXD finishes
            **(run_config or {}),
        }

        # Check prerequisites
        if not self.shelxd_res_file.is_file():
            raise FileNotFoundError(
                f"SHELXD result file {self.shelxd_res_file} not found. Cannot run SHELXE."
            )

        if not self.shelxd_hkl_file.is_file():
            raise FileNotFoundError(
                f"SHELXD HKL file {self.shelxd_hkl_file} not found. Cannot run SHELXE."
            )

        try:
            # SHELXE uses _fa ...
            # Command construction
            cmd_parts = [
                "shelxe",
                self.prefix,
                f"{self.prefix}_fa",
                f"-s{self.shelxe_params['solvent']}",
                f"-m{self.shelxe_params['cycles']}",
                "-h",  # Use HKL file from SHELXD (_fa.hkl)
                "-b",  # Generate PDB output (.pdb)
                # Maybe add -a (auto-tracing), -q (quick trace), -l (line output)? Configurable?
            ]
            if invert:
                cmd_parts.append("-i")
            command = " ".join(cmd_parts)

            job_name_suffix = "_inv" if invert else ""
            output = run_command(
                command,
                cwd=str(self.proc_dir),
                job_name=f"shelxe{job_name_suffix}_{self.prefix}",
                method=run_cfg["method"],
                nodes=run_cfg["nodes"],
                processors=run_cfg["processors"],
                background=run_cfg["background"],
            )

            # Basic check for output files
            if not self.shelxe_pdb_file.is_file() or not self.shelxe_lst_file.is_file():
                logger.warning(
                    f"[{self.prefix}] SHELXE ({mode}) ran but output files (.pdb, .lst) missing."
                )
            else:
                logger.info(f"[{self.prefix}] SHELXE ({mode}) completed successfully.")
            return output if output else ""  # run_command might return None
        except Exception as e:
            logger.error(f"[{self.prefix}] SHELXE ({mode}) failed: {e}")
            raise RuntimeError(
                f"SHELXE ({mode}) execution failed for prefix {self.prefix}"
            ) from e

    def set_find_parameter(self, nsites: int) -> None:
        """Set the FIND parameter for SHELXC."""
        logger.debug(f"[{self.prefix}] Setting FIND parameter to {nsites}")
        self.shelxc_inp_base["FIND"] = str(nsites)

    def check_shelxd_result(self) -> Optional[Tuple[float, float, float]]:
        """
        Checks the SHELXD result file (_fa.res) for CC and CFOM values.
        Returns:
            Tuple (CC, CC_weak, CFOM) if found, else None.
        """
        if self.shelxd_res_file.is_file():
            try:
                with open(self.shelxd_res_file, "r") as f:
                    # Read first few lines, TITL line usually contains CC/CFOM
                    for _ in range(5):  # Check first 5 lines usually suffice
                        line = f.readline()
                        if not line:
                            break  # EOF
                        if line.startswith("REM Best SHELXD solution"):
                            cc, cc_weak, cfom = extract_numbers(line)
                            return cc, cc_weak, cfom
            except Exception as e:
                logger.warning(
                    f"[{self.prefix}] Could not read or parse {self.shelxd_res_file}: {e}"
                )
        return None

    def terminate_shelxd(self) -> None:
        """Creates the .fin file to signal SHELXD to terminate."""
        fin_file = self.proc_dir / f"{self.prefix}_fa.fin"
        logger.info(f"fin file creation: {fin_file}")
        try:
            fin_file.touch()
            logger.info(f"[{self.prefix}] Created termination file: {fin_file}")
        except IOError as e:
            logger.error(
                f"[{self.prefix}] Failed to create termination file {fin_file}: {e}"
            )


class ShelXSweep(ParameterSweepBase):
    def __init__(
            self,
            # SHELX specific inputs
            xds_ascii_hkl_file: Path,
            unit_cell: str,
            # Config for run_command (nodes, proc, etc)
            shelx_run_config: Dict[str, Any],
            target_cc: float,
            target_cc_weak: float,
            # General Sweep inputs
            param_space: Dict[
                str, List[Any]
            ],  # e.g., {"spg": [...], "cutoff": [...], "nsites": [...]}
            base_output_dir: Path,
            base_job_prefix: str = "shelx",
            timeout: float = ParameterSweepBase.DEFAULT_TIMEOUT,
            monitor_interval: int = ParameterSweepBase.DEFAULT_MONITOR_INTERVAL,
            stop_after_n_successes: int = 1,
            shelxc_extra_params: Optional[Dict[str, Any]] = None,
            shelxe_extra_params: Optional[Dict[str, Any]] = None,
    ):
        # SHELX specific attributes
        self.xds_ascii_hkl_file = xds_ascii_hkl_file
        self.unit_cell = unit_cell
        self.shelx_run_config = shelx_run_config
        self.target_cc = target_cc
        self.target_cc_weak = target_cc_weak
        self.shelxc_extra_params = shelxc_extra_params or {}
        self.shelxe_extra_params = shelxe_extra_params or {}

        # Call parent initializer
        super().__init__(
            param_space=param_space,
            base_output_dir=base_output_dir,
            base_job_prefix=base_job_prefix,
            timeout=timeout,
            monitor_interval=monitor_interval,
            stop_after_n_successes=stop_after_n_successes,
            success_metric="CC",  # SHELX success is primarily based on CC
            higher_is_better=True,
        )
        # Shared flag file and lock
        self.success_flag_file = (
                self.base_output_dir / "success_flag.txt"
        )  # Shared flag file
        self.success_flag_lock_file = (
                self.base_output_dir / "success_flag.lock"
        )  # Lock file

    def setup_job(self, job: ParameterSweepJob) -> Any:
        """Create ShelXProcessor instance and prepare."""
        processor = ShelXProcessor(
            prefix=job.id,  # Use job.id as the base prefix for files
            proc_dir=job.output_dir,
            xds_ascii_hkl_file=self.xds_ascii_hkl_file,
            unit_cell=self.unit_cell,
            space_group=job.params["spg"],  # Get from job params
            shelxc_params=self.shelxc_extra_params,
            shelxe_params=self.shelxe_extra_params,
        )

        processor.set_find_parameter(job.params["nsites"])  # Get from job params
        return processor  # Return the processor as context

    def run_job_setup_task(self, job: ParameterSweepJob) -> None:
        """Run SHELXC."""
        processor: ShelXProcessor = job.context
        logger.debug(f"Running SHELXC for job {job.id}")
        # Adjust run_config for SHELXC if needed (usually 'shell')
        shelxc_run_cfg = {
            **self.shelx_run_config,
            "method": "shell",
            "background": False,
        }

        processor.run_shelxc()  # Assuming run_command is handled inside

    def run_job_main_task(self, job: ParameterSweepJob) -> None:
        """Run SHELXD."""
        processor: ShelXProcessor = job.context
        cutoff = job.params["cutoff"]
        logger.debug(f"Running SHELXD for job {job.id} with cutoff {cutoff}")
        # Adjust run_config for SHELXD (usually 'slurm', background=True)
        shelxd_run_cfg = {
            **self.shelx_run_config,
            "method": "slurm",
            "background": True,
        }

        processor.run_shelxd(resolution_cutoff=cutoff, run_config=shelxd_run_cfg)
        job.start_time = (
            time.time()
        )  # Mark start for timeout tracking relative to SHELXD start

    def check_job_progress(
            self, job: ParameterSweepJob
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Check SHELXD .res file and handle global termination."""
        processor: ShelXProcessor = job.context
        result_tuple = processor.check_shelxd_result()

        if result_tuple:
            cc, cc_weak, cfom = result_tuple
            result = {"CC": cc, "CC_weak": cc_weak, "CFOM": cfom}

            if self.check_success_criteria(result):
                job.success_counter = getattr(job, "success_counter", 0) + 1
                logger.info(
                    f"Job {job.id} found a successful solution. Count: {job.success_counter}/{self.stop_after_n_successes}"
                )

                if (
                        self.stop_after_n_successes is not None
                        and job.success_counter >= self.stop_after_n_successes
                ):

                    lock = FileLock(self.success_flag_lock_file, timeout=1)
                    try:
                        with lock:
                            if not self.success_flag_file.exists():
                                try:
                                    with open(self.success_flag_file, "w") as f:
                                        f.write(f"Job {job.id} found success.\n")
                                    logger.info(
                                        f"Job {job.id} found a successful solution. Signalling other jobs to terminate."
                                    )
                                except IOError as e:
                                    logger.error(
                                        f"Could not write success flag file: {e}"
                                    )
                    except Timeout:
                        logger.debug(
                            f"Job {job.id} timed out acquiring lock for success flag, assuming another job handled it."
                        )
                    except Exception as e:
                        logger.error(
                            f"Job {job.id} encountered an error with file lock for success flag: {e}"
                        )

                    job.result = result
                    # save the success to job.result
                    job.result["sweep_success"] = True
                    self._evaluate_result(job)  # Ensure success is recorded
                    # terminate the current shelxd job
                    self.terminate_job(job)
                    return "completed", result
            else:
                return "running", result

        # Check for termination signal before declaring running
        if self.success_flag_file.exists():
            logger.info(f"Job {job.id} detected success signal. Terminating.")
            self.terminate_job(job)
            return "terminated", {"message": "Terminated by success signal"}

        if not processor.shelxd_ins_file.exists():
            return "failed", {
                "error": "SHELXD input file missing, SHELXC likely failed."
            }
        return "running", None

    def terminate_job(self, job: ParameterSweepJob) -> None:
        """Create .fin file for SHELXD."""
        processor: ShelXProcessor = job.context
        try:
            processor.terminate_shelxd()
        except Exception as e:
            logger.warning(f"Failed to signal termination for job {job.id}: {e}")

    def check_success_criteria(self, result: Dict[str, Any]) -> bool:
        """Check if CC and CC_weak meet targets."""
        return (
                result.get("CC", 0) >= self.target_cc
                and result.get("CC_weak", 0) >= self.target_cc_weak
        )

    def run_job_post_task(self, job: ParameterSweepJob) -> None:
        """Run SHELXE (normal and inverted) concurrently if success criteria were met."""
        processor: ShelXProcessor = job.context

        # Check if the job was successful according to sweep_success flag
        if job.result and job.result.get("sweep_success"):
            logger.info(f"Running SHELXE concurrently for successful job {job.id}")

            # Configuration for SHELXE runs.
            # 'background': False here means run_command (called by processor.run_shelxe)
            # will be blocking within each thread. Concurrency is handled by the threads.
            shelxe_run_cfg = {
                **self.shelx_run_config,
                "method": "slurm",  # Or "shell", as per your setup
                "background": False,
            }

            results = {}  # Dictionary to store outputs/errors from threads

            # Define a target function for the threads
            def run_shelxe_in_thread(invert_flag: bool):
                mode = "inverted" if invert_flag else "normal"
                try:
                    # This call is blocking within this thread
                    output = processor.run_shelxe(
                        invert=invert_flag, run_config=shelxe_run_cfg
                    )
                    output_key = f"shelxe_output_{mode}"
                    results[output_key] = output
                except Exception as e:
                    error_key = f"shelxe_error_{mode}"
                    logger.error(f"SHELXE ({mode}) failed for job {job.id}: {e}")
                    results[error_key] = str(e)

            # Create thread for the normal SHELXE run
            thread_normal = threading.Thread(target=run_shelxe_in_thread, args=(False,))

            # Create thread for the inverted SHELXE run
            thread_inverted = threading.Thread(
                target=run_shelxe_in_thread, args=(True,)
            )

            # Start both threads
            thread_normal.start()
            logger.debug(f"Job {job.id}: Started SHELXE (normal) thread.")
            thread_inverted.start()
            logger.debug(f"Job {job.id}: Started SHELXE (inverted) thread.")

            # Wait for both threads to complete
            thread_normal.join()
            logger.debug(f"Job {job.id}: SHELXE (normal) thread completed.")
            thread_inverted.join()
            logger.debug(f"Job {job.id}: SHELXE (inverted) thread completed.")

            # Update job.result with outcomes from the threads
            # Ensure job.result exists (it should if sweep_success is True)
            if job.result is None:
                job.result = {}
            job.result.update(results)
            logger.info(f"Job {job.id}: Both SHELXE post-processing tasks finished.")

        else:
            logger.debug(
                f"Skipping SHELXE for job {job.id} as it did not meet success criteria (sweep_success not set or False)."
            )


if __name__ == "__main__":
    base_dir = Path("./4fd0test")
    hkl_file = Path("4fd0-peak-XDS_ASCII.HKL")
    summary_file = base_dir / "sweep_summary.json"
    if not hkl_file.exists():
        base_dir.mkdir(parents=True, exist_ok=True)
        with open(hkl_file, "w") as f:
            f.write("!Generated dummy HKL file for testing\n")
            f.write("!UNIT_CELL_CONSTANTS= 50.0 60.0 70.0 90.0 90.0 90.0\n")
            f.write("!SPACE_GROUP_NUMBER= 19\n")
            f.write("!END_OF_HEADER\n")
            f.write(" 0 0 1 100.0 10.0\n")
    run_configuration = {
        "nodes": 1,
        "processors": 1,
        "omp_threads": 1,
    }
    shelx_param_space = {
        "spg": ["I 2 2 2"],
        "cutoff": [2.7, 3.2],
        "nsites": [3],
    }
    logger.info("--- Starting Abstract SHELX Sweep Example ---")
    try:
        sweep_manager = ShelXSweep(
            xds_ascii_hkl_file=hkl_file,
            unit_cell="87.343 104.206 154.133 90.000 90.000 90.000",
            shelx_run_config=run_configuration,
            target_cc=35.0,
            target_cc_weak=20.0,
            param_space=shelx_param_space,
            base_output_dir=base_dir / "shelx_runs",
            base_job_prefix="4fd0",
            timeout=600.0,
            monitor_interval=10,
            stop_after_n_successes=1,
        )
        final_results = sweep_manager.run_sweep()
        sweep_manager.save_summary(summary_file)
        logger.info("--- Sweep Results Summary ---")
        if not final_results:
            logger.info("No results were generated.")
        else:
            final_results.sort(
                key=lambda r: (
                    0 if "error" in r or "shelxe_error" in r else 1,
                    r.get("CC", -1),
                ),
                reverse=True,
            )
            for result in final_results:
                params_str = ", ".join(
                    f"{k}={v}" for k, v in result.get("params", {}).items()
                )
                print(f"Job: {result.get('job_id', 'N/A')} ({params_str})")
                print(f" Status: {result.get('status', 'N/A')}")
                if "error" in result:
                    print(f" Error: {result['error']}")
                if "CC" in result:
                    print(
                        f" CC={result['CC']:.2f}, CC_weak={result['CC_weak']:.2f}, CFOM={result['CFOM']:.2f}"
                    )
                if result.get("sweep_success"):
                    print(" ** Met Sweep Success Criteria **")
                if "shelxe_error_normal" in result:
                    print(f" SHELXE Normal Error: {result['shelxe_error_normal']}")
                if "shelxe_error_inverted" in result:
                    print(f" SHELXE Inverted Error: {result['shelxe_error_inverted']}")
                print(f" Output Dir: {result.get('output_dir', 'N/A')}")
                print("-" * 20)
    except Exception as main_e:
        logger.exception(f"An error occurred during the sweep execution: {main_e}")
        print(f"FATAL ERROR: {main_e}", file=sys.stderr)
    logger.info("--- Abstract SHELX Sweep Example Finished ---")
