import argparse
import os
import stat
import subprocess

from qp2.log.logging_config import get_logger, setup_logging
from qp2.utils.tempdirectory import temporary_directory
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)


class MatthewCoef:
    def __init__(
            self,
            spacegroup,
            unitcell,
            molsize=None,
            molw=None,
            moltype=None,
            nmol=None,
            highres=None,
            wdir=".",
            script_name="matthews_coef.sh",
            logfile_name="matthews_coef.log",
    ):
        self.spacegroup = spacegroup
        self.unitcell = unitcell
        self.molsize = molsize
        self.molw = molw
        self.moltype = moltype
        self.nmol = nmol
        self.highres = highres
        self.wdir = os.path.abspath(wdir)
        if not os.path.exists(self.wdir):
            os.makedirs(self.wdir, exist_ok=True)

        self.script_path = os.path.join(self.wdir, script_name)
        self.logfile_path = os.path.join(self.wdir, logfile_name)
        # --- NEW: Define a dedicated input file path ---
        self.command_file_path = os.path.join(self.wdir, "matthews_commands.inp")

        # Output capture files remain the same
        self.stdout_path = os.path.join(self.wdir, script_name + ".out")
        self.stderr_path = os.path.join(self.wdir, script_name + ".err")

    def get_table(self):
        try:
            with open(self.logfile_path, "r") as f:
                lines = f.readlines()
        except FileNotFoundError:
            logger.error(f"Log file not found: {self.logfile_path}")
            return "Log file not found."

        start_line_index = -1
        for i, line in enumerate(lines):
            if (
                    "Nmol/asym" in line
                    and "Matthews Coeff" in line
                    and "%solvent" in line
                    and "P(tot)" in line
            ):
                start_line_index = i
                break

        if start_line_index == -1:
            logger.warning("Table header not found in log file.")
            return "Table header not found in log file."

        table_lines = []
        # The table data starts two lines after the header (to skip the header and the '____' separator)
        for line in lines[start_line_index + 2:]:
            # Stop if we hit the end-of-table separator line
            if "___" in line:
                break
            # Also stop if we hit the end of the HTML preformatted block
            if "</pre>" in line:
                break

            fields = line.split()
            # A valid data line will have exactly 4 columns
            if len(fields) == 4:
                try:
                    # Quick check to ensure all fields are numeric-like
                    [float(f) for f in fields]
                    table_lines.append(line.strip())
                except ValueError:
                    # This line has 4 fields but they aren't numbers, so skip it
                    continue

        if not table_lines:
            logger.warning("Table content is empty after finding header.")
            return "Table content is empty."

        return "\n".join(table_lines)

    def get_estimated_asu_content(self):
        table_str = self.get_table()
        if not isinstance(table_str, str) or any(
                err in table_str for err in ["not found", "is empty"]
        ):
            logger.warning(
                f"Cannot get estimated ASU content due to table error: {table_str}"
            )
            return None

        nmol_at_50solvent = None
        min_diff = float("inf")
        best_line_data = {}

        for line_content in table_str.splitlines():
            fields = line_content.split()
            if len(fields) == 4:
                try:
                    nmol, matthews_coeff, solvent_percent, prob = fields
                    solvent_value = float(solvent_percent)
                    current_diff = abs(solvent_value - 50.0)

                    if current_diff < min_diff:
                        min_diff = current_diff
                        nmol_at_50solvent = nmol
                        best_line_data = {
                            "solvent": solvent_value,
                            "matthews_coef": float(matthews_coeff),
                            "prob": float(prob),
                        }
                except ValueError:
                    logger.debug(f"Skipping line due to parsing error: {line_content}")
                    continue

        if nmol_at_50solvent is not None:
            try:
                return {
                    "asu_content": int(nmol_at_50solvent) * self.molsize,
                    "nmol": int(nmol_at_50solvent),
                    "nres": int(self.molsize),
                    **best_line_data,
                }
            except (ValueError, TypeError):
                logger.error(
                    f"Could not convert nmol '{nmol_at_50solvent}' or molsize '{self.molsize}' to int."
                )
                return None
        else:
            logger.warning("Could not determine nmol at ~50% solvent.")
            return None

    def _generate_script_text(self):
        """
        Generates the matthews_coef input commands and the shell script to run it.
        This is a simplified, corrected, and more robust version.
        """
        # 1. Create the input commands for matthews_coef
        matthews_input_cmds = []
        matthews_input_cmds.append(f"symmetry {self.spacegroup.replace(' ', '')}")
        matthews_input_cmds.append(f"cell {self.unitcell}")
        matthews_input_cmds.append("auto" if not self.nmol else f"nmol {self.nmol}")
        if self.molw:
            matthews_input_cmds.append(f"molw {self.molw}")
        elif self.molsize:
            matthews_input_cmds.append(f"nres {self.molsize}")
        if self.moltype in ["D", "C"]:
            matthews_input_cmds.append(f"mode {self.moltype}")
        if self.highres:
            matthews_input_cmds.append(f"reso {self.highres}")
        matthews_input_cmds.append("END")

        # Write commands to the dedicated input file
        with open(self.command_file_path, "w") as f_cmds:
            f_cmds.write("\n".join(matthews_input_cmds) + "\n")

        # 2. Create the shell script text
        # Inject the setup command for CCP4 environment
        setup_cmd = ProgramConfig.get_setup_command("ccp4")
        
        script_text = (
            f"#!/bin/bash\n"
            f"# This script was auto-generated by MatthewCoef.py\n\n"
            f"{setup_cmd}\n\n"  # Source environment setup
            f'cd "{self.wdir}"\n\n'
            f"# Run matthews_coef, redirecting input from command file and output to log file\n"
            f'matthews_coef < "{os.path.basename(self.command_file_path)}" > "{os.path.basename(self.logfile_path)}"\n'
        )
        return script_text

    def run(self):
        script_text = self._generate_script_text()
        logger.debug(f"Generated script content:\n{script_text}")

        with open(self.script_path, "w") as f:
            f.write(script_text)

        st = os.stat(self.script_path)
        os.chmod(self.script_path, st.st_mode | stat.S_IEXEC)

        # Use subprocess.run for a simpler and more modern approach
        try:
            process = subprocess.run(
                self.script_path,
                shell=True,
                capture_output=True,
                text=True,
                check=False,  # Don't raise exception on non-zero exit code
                cwd=self.wdir,
            )

            with open(self.stdout_path, "w") as f_stdout:
                f_stdout.write(process.stdout)
            with open(self.stderr_path, "w") as f_stderr:
                f_stderr.write(process.stderr)

            logger.info(f"Script completed with exit code: {process.returncode}")
            if process.returncode != 0:
                logger.error(f"Script failed. Stderr: {process.stderr.strip()}")

            return process.returncode

        except FileNotFoundError:
            logger.error(
                f"Command 'matthews_coef' not found. "
                f"Please ensure the CCP4 suite is correctly installed and in your system's PATH."
            )
            return -1  # Return a custom error code


def run_matthews_coef(
        spacegroup,
        unitcell,
        molsize=None,
        molw=None,
        moltype=None,
        nmol=None,
        highres=None,
        debug=False,
):
    """
    Runs the Matthews coefficient calculation in a temporary directory.

    Args:
        spacegroup (str): The spacegroup.
        unitcell (str): The unit cell parameters.
        molsize (int, optional): The molecule size. Defaults to None.
        molw (float, optional): The molecule weight. Defaults to None.
        moltype (str, optional): The molecule type. Defaults to None.
        nmol (int, optional): The number of molecules. Defaults to None.
        highres (float, optional): The high resolution. Defaults to None.
        debug (bool, optional): If True, the temporary directory will not be deleted after the calculation. Defaults to False.

    Returns:
        int: The estimated ASU content, or None if it could not be determined.
    """
    if molw is not None and molsize is None:
        molsize = int(molw / 110)  # assume protein

    if molsize is None:
        molsize = molsize if molsize is not None else 25

    with temporary_directory(delete=not debug) as tmpdir:
        logger.info(f"Created temporary directory: {tmpdir}")
        try:
            matthews = MatthewCoef(
                spacegroup=spacegroup,
                unitcell=unitcell,
                molsize=molsize,
                molw=molw,
                moltype=moltype,
                nmol=nmol,
                highres=highres,
                wdir=tmpdir,
            )
            return_code = matthews.run()
            if return_code != 0:
                logger.error("Matthews coef run failed.")
                return None
            asu_content = matthews.get_estimated_asu_content()

        finally:
            if debug:
                logger.info(
                    f"Debug mode: Temporary directory {tmpdir} will not be deleted.  You can find it at: {tmpdir}"
                )
            else:
                logger.info("Cleaning up temporary directory.")
        return asu_content


if __name__ == "__main__":
    # Set up logging
    setup_logging()

    # Define command-line arguments
    parser = argparse.ArgumentParser(
        description="Run Matthews coefficient calculation in a temporary directory."
    )
    parser.add_argument("--spacegroup", "--symm", help="The spacegroup")
    parser.add_argument(
        "--unitcell",
        "--cell",
        help="The unit cell parameters (e.g., '79.4 79.4 38.1 90 90 90')",
    )
    parser.add_argument(
        "--molsize",
        type=int,
        help="The molecule size (number of residues)",
        default=None,
    )
    parser.add_argument("--molw", type=float, help="The molecule weight", default=None)
    parser.add_argument(
        "--moltype",
        type=str,
        help="The molecule type (D for DNA, C for Protein/DNA complex)",
        default=None,
    )
    parser.add_argument(
        "--nmol",
        type=int,
        help="The number of molecules in the unit cell",
        default=None,
    )
    parser.add_argument(
        "--highres", type=float, help="The high resolution limit", default=None
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode: the temporary directory will not be deleted",
    )

    # Parse command-line arguments
    args = parser.parse_args()

    # Run the Matthews coefficient calculation
    estimated_asu_content = run_matthews_coef(
        spacegroup=args.spacegroup,
        unitcell=args.unitcell,
        molsize=args.molsize,
        molw=args.molw,
        moltype=args.moltype,
        nmol=args.nmol,
        highres=args.highres,
        debug=args.debug,
    )

    if estimated_asu_content is not None:
        logger.info(f"Estimated ASU content: {estimated_asu_content}")
    else:
        logger.error("Failed to estimate ASU content.")
