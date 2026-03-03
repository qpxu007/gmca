from pathlib import Path
from pathlib import Path
from typing import Dict, Optional, Tuple, Any

from symmetry import Symmetry

# Configure logging
logger = get_logger(__name__)

class ShelXProcessor:
    """Handles the execution of SHELX steps (C, D, E)."""

    DEFAULT_SHELXE_SOLVENT = 0.5
    DEFAULT_SHELXE_CYCLES = 20
    DEFAULT_SHELXC_MAXM = 2
    DEFAULT_SHELXC_NTRY = 0

    def __init__(
        self,
        prefix: str,
        proc_dir: Path,
        xds_ascii_hkl_file: Path,
        unit_cell: Optional[str] = None,
        space_group: Optional[str] = None,
        shelxc_params: Optional[Dict[str, Any]] = None,
        shelxe_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initializes the processor for a single SHELX run."""
        self.prefix = prefix
        self.proc_dir = Path(proc_dir)
        self.xds_ascii_hkl_file = Path(xds_ascii_hkl_file)
        self.unit_cell = unit_cell
        self.space_group = space_group
        self.proc_dir.mkdir(parents=True, exist_ok=True)

        # Define file paths
        self.shelxc_inp_file = self.proc_dir / f"{self.prefix}_shelxc.inp"
        self.shelxd_ins_file = self.proc_dir / f"{self.prefix}_fa.ins"
        self.shelxd_res_file = self.proc_dir / f"{self.prefix}_fa.res"
        self.shelxd_hkl_file = self.proc_dir / f"{self.prefix}_fa.hkl"
        self.shelxe_lst_file = self.proc_dir / f"{self.prefix}.lst"
        self.shelxe_pdb_file = self.proc_dir / f"{self.prefix}.pdb"

        # Default parameters
        self.shelxc_params: Dict[str, Any] = {
            "SAD": str(self.xds_ascii_hkl_file.resolve()),
            "MAXM": self.DEFAULT_SHELXC_MAXM,
            "NTRY": self.DEFAULT_SHELXC_NTRY,
            "FIND": "8",
            **(shelxc_params or {}),
        }

        self.shelxe_params: Dict[str, Any] = {
            "solvent": self.DEFAULT_SHELXE_SOLVENT,
            "cycles": self.DEFAULT_SHELXE_CYCLES,
            **(shelxe_params or {}),
        }

        self.results: Dict[str, Any] = {}

    def _extract_crystal_params_from_hkl(self) -> Tuple[Optional[str], Optional[str]]:
        """Extract crystal parameters from the HKL file."""
        unit_cell = None
        space_group = None
        try:
            with open(self.xds_ascii_hkl_file, "r") as f:
                for line in f:
                    if line.startswith("!END_OF_HEADER"):
                        break
                    if line.startswith("!UNIT_CELL_CONSTANTS="):
                        unit_cell = line.split("=", 1)[1].strip()
                    elif line.startswith("!SPACE_GROUP_NUMBER="):
                        spg_num_str = line.split("=", 1)[1].strip()
                        try:
                            spg_num = int(spg_num_str)
                            space_group = Symmetry.number_to_symbol(spg_num, remove_parentheses=True)
                        except (ValueError, AttributeError) as e:
                            logger.warning(f"Could not process space group number '{spg_num_str}': {e}")
        except FileNotFoundError:
            logger.error(f"HKL file not found: {self.xds_ascii_hkl_file}")
            raise
        except Exception as e:
            logger.error(f"Error reading HKL file: {e}")
            raise
        return unit_cell, space_group

    def _ensure_crystal_parameters(self) -> None:
        """Ensures unit cell and space group are available."""
        if self.unit_cell and self.space_group:
            logger.debug(f"Using provided Unit Cell: {self.unit_cell}")
            logger.debug(f"Using provided Space Group: {self.space_group}")
            return

        logger.info(f"Deriving crystal parameters from {self.xds_ascii_hkl_file}")
        try:
            self.unit_cell, self.space_group = self._extract_crystal_params_from_hkl()
            if not self.unit_cell or not self.space_group:
                raise ValueError("Could not determine unit cell or space group from HKL file.")
        except Exception as e:
            logger.error(f"Failed to ensure crystal parameters: {e}")
            raise

    def _prepare_shelxc_input(self) -> None:
        """Prepares SHELXC input file (_shelxc.inp)."""
        logger.debug(f"Preparing SHELXC input file: {self.shelxc_inp_file}")
        self._ensure_crystal_parameters()

        # Update parameters with cell and space group
        shelxc_input_data = {
            **self.shelxc_params,
            "CELL": self.unit_cell,
            "SPAG": self.space_group,
        }

        # Filter out any keys with None values
        shelxc_input_data = {k: v for k, v in shelxc_input_data.items() if v is not None}

        # Generate input string
        inp = "\n".join(f"{key} {value}" for key, value in shelxc_input_data.items())

        try:
            with open(self.shelxc_inp_file, "w") as fh:
                fh.write(inp)
            logger.debug(f"SHELXC input file written successfully.")
        except IOError as e:
            logger.error(f"Failed to write SHELXC input file: {e}")
            raise

    def run_shelxc(self, run_command_func) -> None:
        """Prepares input and runs SHELXC."""
        logger.info(f"[{self.prefix}] Starting SHELXC execution in {self.proc_dir}")
        try:
            self._prepare_shelxc_input()
            command = f"shelxc {self.prefix} < {self.shelxc_inp_file.resolve()}"
            run_command_func(command, cwd=str(self.proc_dir), job_name=f"shelxc_{self.prefix}", method="shell")

            if not self.shelxd_hkl_file.is_file():
                raise FileNotFoundError(f"SHELXC output HKL file not found: {self.shelxd_hkl_file}")

            logger.info(f"[{self.prefix}] SHELXC completed successfully.")
        except Exception as e:
            logger.error(f"[{self.prefix}] SHELXC failed: {e}")
            raise

    def _prepare_shelxd_input(self, resolution_cutoff: float) -> None:
        """Prepares SHELXD input file by modifying the SHELXC output."""
        logger.debug(f"Preparing SHELXD input file: {self.shelxd_ins_file}")

        if not self.shelxd_ins_file.is_file():
            raise FileNotFoundError(
                f"SHELXD input file {self.shelxd_ins_file} not found. Did SHELXC run correctly?"
            )

        try:
            with open(self.shelxd_ins_file, "r") as f:
                lines = f.readlines()
        except IOError as e:
            logger.error(f"Failed to read SHELXD input file: {e}")
            raise

        processed_lines = []
        insert_pos = -1
        shel_line_exists = False

        for i, line in enumerate(lines):
            stripped_line = line.strip()

            if stripped_line.startswith(("CELL", "ZERR", "LATT", "SYMM", "SFAC", "UNIT")):
                insert_pos = i + 1
                processed_lines.append(line)
            elif stripped_line.startswith(("NTRY")):
                processed_lines.append(f"REM {line}")
                logger.debug(f"Commented out line: {line.strip()}")
            elif stripped_line.startswith("SHEL"):
                new_shel_line = f"SHEL 999 {resolution_cutoff:.2f}\n"
                processed_lines.append(new_shel_line)
                shel_line_exists = True
                insert_pos = -1
            else:
                processed_lines.append(line)

        if not shel_line_exists and insert_pos != -1:
            new_shel_line = f"SHEL 999 {resolution_cutoff:.2f}\n"
            processed_lines.insert(insert_pos, new_shel_line)
            logger.debug(f"Inserted SHEL line: {new_shel_line.strip()}")
        elif not shel_line_exists and insert_pos == -1:
            logger.error(f"Could not find suitable insertion point for SHEL in {self.shelxd_ins_file}")
            raise ValueError(f"Could not process SHELXD input file {self.shelxd_ins_file}")

        try:
            with open(self.shelxd_ins_file, "w") as fh:
                fh.write("".join(processed_lines))
            logger.debug(f"SHELXD input file written successfully: {self.shelxd_ins_file}")
        except IOError as e:
            logger.error(f"Failed to write modified SHELXD input file: {e}")
            raise

    def run_shelxd(
        self,
        resolution_cutoff: float,
        run_command_func,
        run_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Prepares input and runs SHELXD."""
        logger.info(f"[{self.prefix}] Starting SHELXD with cutoff {resolution_cutoff}A")

        run_cfg = {
            "method": "slurm",
            "nodes": 1,
            "processors": 8,
            "omp_threads": 8,
            "background": True,
            **(run_config or {}),
        }

        try:
            self._prepare_shelxd_input(resolution_cutoff)
            shelxd_base = f"{self.prefix}_fa"
            env_prefix = (
                f"export OMP_NUM_THREADS={run_cfg['omp_threads']}; "
                if run_cfg.get("omp_threads")
                else ""
            )
            command = f"{env_prefix}shelxd {shelxd_base}"
            run_command_func(
                command,
                cwd=str(self.proc_dir),
                job_name=f"shelxd_{self.prefix}",
                method=run_cfg["method"],
                nodes=run_cfg["nodes"],
                processors=run_cfg["processors"],
                background=run_cfg["background"],
            )

            logger.info(f"[{self.prefix}] SHELXD job submitted (method: {run_cfg['method']}).")
        except Exception as e:
            logger.error(f"[{self.prefix}] SHELXD failed: {e}")
            raise

    def run_shelxe(
        self, run_command_func, invert: bool = False, run_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """Runs SHELXE on the result of SHELXD."""
        mode = "inverted" if invert else "normal"
        logger.info(f"[{self.prefix}] Starting SHELXE ({mode} hand)")

        run_cfg = {
            "method": "slurm",
            "nodes": 1,
            "processors": 1,
            "background": False,
            **(run_config or {}),
        }

        if not self.shelxd_res_file.is_file():
            raise FileNotFoundError(
                f"SHELXD result file {self.shelxd_res_file} not found. Cannot run SHELXE."
            )

        if not self.shelxd_hkl_file.is_file():
            raise FileNotFoundError(
                f"SHELXD HKL file {self.shelxd_hkl_file} not found. Cannot run SHELXE."
            )

        try:
            cmd_parts = [
                "shelxe",
                self.prefix,
                f"{self.prefix}_fa",
                f"-s{self.shelxe_params['solvent']}",
                f"-m{self.shelxe_params['cycles']}",
                "-h",
                "-b",
            ]
            if invert:
                cmd_parts.append("-i")
            command = " ".join(cmd_parts)
            job_name_suffix = "_inv" if invert else ""
            output = run_command_func(
                command,
                cwd=str(self.proc_dir),
                job_name=f"shelxe{job_name_suffix}_{self.prefix}",
                method=run_cfg["method"],
                nodes=run_cfg["nodes"],
                processors=run_cfg["processors"],
                background=run_cfg["background"],
            )

            if not self.shelxe_pdb_file.is_file() or not self.shelxe_lst_file.is_file():
                logger.warning(
                    f"[{self.prefix}] SHELXE ({mode}) ran but output files (.pdb, .lst) missing."
                )
            else:
                logger.info(f"[{self.prefix}] SHELXE ({mode}) completed successfully.")

            return output if output else ""
        except Exception as e:
            logger.error(f"[{self.prefix}] SHELXE ({mode}) failed: {e}")
            raise

    def set_find_parameter(self, nsites: int) -> None:
        """Sets the FIND parameter for SHELXC."""
        logger.debug(f"[{self.prefix}] Setting FIND parameter to {nsites}")
        self.shelxc_params["FIND"] = str(nsites)

    def check_shelxd_result(self) -> Optional[Tuple[float, float, float]]:
        """Checks the SHELXD result file for CC and CFOM values."""
        if not self.shelxd_res_file.is_file():
            logger.warning(f"SHELXD result file not found: {self.shelxd_res_file}")
            return None
        try:
            with open(self.shelxd_res_file, "r") as f:
                for _ in range(5):
                    line = f.readline()
                    if not line:
                        break
                    if line.startswith("REM Best SHELXD solution"):
                        cc, cc_weak, cfom = self._extract_cc_cfom(line)
                        return cc, cc_weak, cfom
        except Exception as e:
            logger.warning(f"Could not read or parse {self.shelxd_res_file}: {e}")
            return None

        return None

    def _extract_cc_cfom(self, line: str) -> Tuple[float, float, float]:
        """Extracts CC, CC_weak, and CFOM values from the SHELXD result line."""
        try:
            parts = line.split()
            cc = float(parts[4])
            cc_weak = float(parts[7])
            cfom = float(parts[9])
            return cc, cc_weak, cfom
        except (IndexError, ValueError) as e:
            logger.error(f"Could not parse CC and CFOM from line: {line.strip()}. Error: {e}")
            raise ValueError("Failed to extract CC and CFOM values") from e

    def terminate_shelxd(self) -> None:
        """Creates the .fin file to signal SHELXD to terminate."""
        fin_file = self.proc_dir / f"{self.prefix}_fa.fin"
        logger.info(f"fin file creation: {fin_file}")
        try:
            fin_file.touch()
            logger.info(f"[{self.prefix}] Created termination file: {fin_file}")
        except IOError as e:
            logger.error(f"[{self.prefix}] Failed to create termination file: {e}")
            raise

