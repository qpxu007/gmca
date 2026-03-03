import csv
import os
import shutil
import subprocess
from collections import OrderedDict
from dataclasses import dataclass, field, fields
from typing import List, Dict, Any

from qp2.log.logging_config import get_logger
from qp2.utils.tempdirectory import temporary_directory
from qp2.utils.matthews_coef import run_matthews_coef
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)

job_template = """#!/bin/sh 

raddose3d_jar="{raddose3d_jar}"

cmd_to_run=""

if [ -f "$raddose3d_jar" ]; then
    cmd_to_run="java -jar $raddose3d_jar"
else
    echo "Error: raddose3d.jar ('$raddose3d_jar') not found." >&2
    exit 1
fi

cat >r3d.inp <<eof

{crystal_block}

{beam_block}

{wedge_blocks}
eof

$cmd_to_run -i r3d.inp
"""


@dataclass
class Sample:
    coef_calc: str = "AVERAGE"
    pdbcode: str = ""
    cell: str = "78 78 39 90 90 90"
    nmon: int = 8  # in space group 1
    nres: int = 129  # no of residues per monomer
    nrna: int = 0
    ndna: int = 0
    ncarb: int = 0
    crystal_size: str = "101 100 100"  # microns
    crystal_shape: str = "Cuboid"
    solvent_fraction: float = 0.5
    angle_p: float = (
        0.0  # angle between crystal y axis & gonio axis, in degrees; in axis view
    )
    angle_l: float = (
        0.0  # angle between crystal y axis & gonio axis, in degrees; view from top
    )
    protein_heavy_atoms: str = ""  # e.g., "ProteinHeavyAtoms Zn 0.333 S 6"
    solvent_heavy_conc: str = (
        ""  # e.g., "P 425, concentration of elements in the solvent in mmol/l"
    )
    pixelspermicron: float = field(init=False)
    calculate_pe_escape: bool = False
    calculate_fl_escape: bool = False
    calculate_surrounding: bool = False
    surrounding_heavy_conc: str = ""

    def __post_init__(self):
        # Calculate pixelspermicron based on beam size and crystal size
        beam_size_val = 20.0  # Default value if beam is not provided
        if "beam" in self.__dict__ and self.beam:
            beam_size_val = min(map(float, self.beam.beam_size.split()))
        self.pixelspermicron = self._calculate_pixelspermicron(
            beam_size_val, self.crystal_size
        )

    def _calculate_pixelspermicron(self, beam_size: float, crystal_size: str) -> float:
        avg_crystal_size = sum(map(float, crystal_size.split())) / 3.0
        pixelspermicron = 10.0 / beam_size

        if avg_crystal_size <= 20.0 and pixelspermicron < 1.0:
            pixelspermicron = 1.0
        elif avg_crystal_size >= 100.0 and pixelspermicron > 0.5:
            pixelspermicron = 0.2

        # slow with large value, also out of memory if value is too big
        if pixelspermicron >= 2.0:
            pixelspermicron = 2
        elif pixelspermicron <= 0.1:
            pixelspermicron = 0.1

        return pixelspermicron


@dataclass
class Beam:
    beam_type: str = "Gaussian"
    flux: float = 1.0e12  # the flux at the sample position after collimation
    energy: float = 12.0  # kev
    beam_size: str = "20 20"  # microns, µm, X and Y for a Gaussian beam
    attenuation_factor: float = 1.0
    collimator_size: str = ""
    collimator_shape: str = "CIRCULAR"  # CIRCULAR or rectangle

    def get_collimator_size(self) -> str:
        if not self.collimator_size:
            return " ".join(str(2.0 * float(x)) for x in self.beam_size.split())
        return self.collimator_size


@dataclass
class Wedge:
    start_angle: float = 0.0
    osc: float = 0.2
    exposure_time_per_image: float = 0.2  # seconds
    angular_resolution: float = 2.0
    translate_per_degree: str = "0 0 0"  # micron, used helical data collection
    start_offset: str = "0 0 0"
    rotaxbeam_offset: float = 0.0
    nimages: int = 1800


def prepare_raddose3d_script(
        sample: Sample, beam: Beam, wedges: List[Wedge], swap_xy: bool = True
) -> str:
    """Prepares the raddose3d input script."""

    if sample.coef_calc.upper() == "AVERAGE" or sample.pdbcode:
        optional = "!"  # no need to provide unit cell etc
    else:
        optional = ""

    coef_calc = "EXP" if sample.pdbcode else sample.coef_calc
    pdbcode = f"PDB {sample.pdbcode}" if sample.pdbcode else ""

    # Swap x, y for crystal size if swap_xy is True
    a_crystal_size = sample.crystal_size.split()
    if len(a_crystal_size) == 3 and swap_xy:
        a_crystal_size[0], a_crystal_size[1] = a_crystal_size[1], a_crystal_size[0]
    crystal_size = " ".join(a_crystal_size)

    protein_heavy_atoms = (
        f"Proteinheavyatoms {sample.protein_heavy_atoms}"
        if sample.protein_heavy_atoms
        else ""
    )
    solvent_heavy_conc = (
        f"Solventheavyconc {sample.solvent_heavy_conc}"
        if sample.solvent_heavy_conc
        else ""
    )
    surrounding_heavy_conc = (
        f"Surrounding_heavy_conc {sample.surrounding_heavy_conc}"
        if sample.surrounding_heavy_conc
        else ""
    )

    crystal_block = f"""
Crystal
Type {sample.crystal_shape.upper()}
Dimension {crystal_size}
AbsCoefCalc {coef_calc}
{pdbcode}
{optional}Unitcell {sample.cell}
{optional}NumMonomers {int(sample.nmon)}
{optional}NumResidues {int(sample.nres)}
{optional}Numdna {int(sample.ndna)}
{optional}Numrna {int(sample.nrna)}
{optional}Numcarb {int(sample.ncarb)}
!Solventfraction {sample.solvent_fraction}
!DDM LEAL
!DECAYPARAM 0.00748 18.1 0.298 !cryo
AngleP {sample.angle_p}
AngleL {sample.angle_l}
Pixelspermicron {sample.pixelspermicron}
{protein_heavy_atoms}
{solvent_heavy_conc}
{'' if not sample.calculate_pe_escape else 'CALCULATEPEESCAPE TRUE'}
{'' if not sample.calculate_fl_escape else 'CALCULATEFLESCAPE TRUE'}
{'' if not sample.calculate_surrounding else 'CALCSURROUNDING TRUE'}
{surrounding_heavy_conc}
#end of crystal
"""

    beam_size = beam.beam_size.split()[::-1]  # swap
    energy_value = beam.energy if beam.energy <= 100.0 else beam.energy / 1000.0
    flux_value = (
        beam.flux / beam.attenuation_factor if beam.attenuation_factor else beam.flux
    )

    collimation_line = (
        f"COLLIMATION {beam.collimator_shape} {beam.get_collimator_size()}"
    )
    beam_block = f"""
Beam
Type {beam.beam_type}
Energy {energy_value}
FWHM {' '.join(beam_size)}
Flux {flux_value}
# this may increase the calculation speed, suggest to be twice the beam
{collimation_line if beam.collimator_shape and beam.collimator_size.strip() else '#COLLIMATION  CIRCULAR 40 40'} 
#end of beam
"""

    wedge_blocks = ""
    for i, w in enumerate(wedges):
        angular_resolution = "ANGULARRESOLUTION 2.0" if w.osc > 0.0001 else ""

        # Swap x and y in translate_per_degree
        translate_per_degree_values = w.translate_per_degree.split()
        translate_per_degree = " ".join(
            translate_per_degree_values[:2][::-1] + translate_per_degree_values[2:]
        )

        wedge = f"""
WEDGE {w.start_angle} {w.start_angle + w.osc * w.nimages}
ExposureTime {w.nimages * w.exposure_time_per_image}
Translateperdegree {translate_per_degree}
{angular_resolution}
Startoffset {w.start_offset}
Rotaxbeamoffset {w.rotaxbeam_offset}
#end of wedge {i + 1}
"""
        wedge_blocks += wedge

    raddose3d_jar_path = ProgramConfig.get_library_path("raddose3d")
    script = job_template.format(
        raddose3d_jar=raddose3d_jar_path,
        crystal_block=crystal_block,
        beam_block=beam_block,
        wedge_blocks=wedge_blocks,
    )
    return script


def run_raddose3d(
        sample: Sample,
        beam: Beam,
        wedges: List[Wedge],
        swap_xy: bool = True,
        debug: bool = False,
        executor: str = "/bin/bash",
) -> List[Dict[str, Any]]:
    """Runs the raddose3d calculation and returns the results."""
    logger.info(f"Starting RADDOSE-3D calculation with {len(wedges)} wedge(s)")
    logger.debug(
        f"Sample parameters: crystal_size={sample.crystal_size}, shape={sample.crystal_shape}"
    )
    logger.debug(
        f"Beam parameters: flux={beam.flux}, energy={beam.energy}, beam_size={beam.beam_size}"
    )
    script = prepare_raddose3d_script(sample, beam, wedges, swap_xy)

    with temporary_directory(delete=not debug) as wdir:
        logger.debug(f"working directory {wdir}, delete: {not debug}")
        script_file = os.path.join(wdir, "run.sh")

        with open(script_file, "w") as fh:
            fh.write(script)

        cmd = [executor, script_file]
        try:
            logger.debug(f"Executing RADDOSE-3D command: {' '.join(cmd)}")
            p = subprocess.check_output(cmd, cwd=wdir, stderr=subprocess.STDOUT)
            output = p.decode()
            logger.debug("RADDOSE-3D execution completed successfully")
        except subprocess.CalledProcessError as e:
            error_message = e.output.decode()
            logger.error(f"Command execution failed with exit code: {e.returncode}")
            logger.error(f"Error output: {error_message}")
            return [{"summary": error_message}]

        summary_file = os.path.join(wdir, "output-Summary.txt")
        try:
            with open(summary_file, "r") as f:
                summary = f.readlines()
            logger.debug(f"Successfully read summary file: {summary_file}")
        except FileNotFoundError:
            logger.warning(f"Summary file not found: {summary_file}")
            summary = ["Summary file not found."]

        summary_dict = parse_summary_to_ordered_dict(summary)

        data = []
        csv_file = os.path.join(wdir, "output-Summary.csv")
        try:
            with open(csv_file, "r") as csvfile:
                reader = csv.DictReader(csvfile, delimiter=",")
                for row in reader:
                    selected_row = {
                        k.strip(): row[k].strip()
                        for k in row.keys()
                                 & {"Wedge Number", " Average DWD", " Max Dose"}
                    }
                    selected_row["log"] = output
                    selected_row["summary"] = summary
                    data.append(selected_row)
        except FileNotFoundError:
            data.append({"summary": "CSV summary file not found.", "log": output})

        shutil.copy(script_file, os.getenv("HOME"))
        logger.info(
            f"RADDOSE-3D calculation completed. Avg DWD: {summary_dict.get('Avg DWD', 'N/A')} MGy"
        )
        return data, summary_dict


def convert_kwargs_to_dataclass(**kwargs):
    # only 1 wedge case
    sample_fields = [f.name for f in fields(Sample)]
    beam_fields = [f.name for f in fields(Beam)]
    wedge_fields = [f.name for f in fields(Wedge)]

    sample = Sample()
    beam = Beam()
    wedge = Wedge()
    sample.beam = beam

    for k in kwargs:
        if k in sample_fields:
            setattr(sample, k, kwargs[k])
        elif k in beam_fields:
            setattr(beam, k, kwargs[k])
        elif k in wedge_fields:
            setattr(wedge, k, kwargs[k])
        elif k in ("symm", "nasu"):
            pass  # ignored for now,
        else:
            logger.warning(f"unknown parameter key: {k}, ignored")
            logger.warning(f"valid keys for Sample: {sample_fields}")
            logger.warning(f"valid keys for : {beam_fields}")
            logger.warning(f"valid keys for Wedge: {wedge_fields}")

    if "cell" not in kwargs:
        logger.error("cell is not provided, cannot continue.")
        raise SystemExit

    # estimate nmon if not given
    if "nmon" not in kwargs:
        if "symm" not in kwargs:
            symm = "P1"
        else:
            symm = kwargs.get("symm", "P1")

        try:
            import gemmi

            nops = len(gemmi.SpaceGroup(symm).operations())
        except:
            logger.error("failed to import gemmi, use nops=1")
            nops = 1

        molsize = kwargs.get("nres") if "nres" in kwargs else None
        if "nasu" in kwargs:
            setattr(sample, "nmon", nops * kwargs.get("nasu", 1))
        else:
            try:

                out = run_matthews_coef(symm, kwargs.get("cell"), molsize=molsize)
                logger.debug("matthews estimation:", out)
                nasu = out.get("nmol")
                nres = out.get("nres")
                solvent_percent = out.get("solvent", 0.5)
                setattr(sample, "nres", nres)
                setattr(sample, "nmon", nops * nasu)
                setattr(sample, "solvent_fraction", float(solvent_percent) / 100.0)
                logger.info(
                    f"estimate 50% solvent, estimated nres = {nres} nmon = {nops * nasu} in symm = {symm}, nops={nops}"
                )

            except Exception as e:
                logger.error(f"failed to estimate nres. {e}")

    sample.__post_init__()
    return sample, beam, wedge


def run_raddose3d_one_wedge(**kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Runs raddose3d with one wedge, converting kwargs to dataclasses."""
    sample, beam, wedge = convert_kwargs_to_dataclass(**kwargs)
    return run_raddose3d(sample, beam, [wedge])


def parse_summary_to_ordered_dict(summary_lines):
    """
    Parses a multi-line string summary into an OrderedDict of key-value pairs,
    considering only lines where key and value are separated by a colon.

    Args:
        summary_text (str): A string containing the summary to parse.

    Returns:
        OrderedDict: An OrderedDict where keys are extracted from the part before
                     the colon and values are from the part after the colon.
                     Returns an empty OrderedDict if no valid key-value pairs are found.
    """
    ordered_data = OrderedDict()

    for line in summary_lines:
        line = line.strip()
        if not line:  # Skip empty lines
            continue

        # Check if the line contains a colon
        if ":" in line:
            key, value = line.split(":", 1)  # Split only at the first occurrence of ":"
            key = key.strip()
            value = value.strip()
            if value:
                ordered_data[key] = value
    ordered_data["Avg DWD"] = float(
        ordered_data["Average Diffraction Weighted Dose"].replace("MGy", "").rstrip()
    )
    ordered_data["Max Dose"] = float(
        ordered_data["Max Dose"].replace("MGy", "").rstrip()
    )
    ordered_data["Last DWD"] = float(
        ordered_data["Last Diffraction Weighted Dose"].replace("MGy", "").rstrip()
    )
    return ordered_data


@dataclass
class Raster:
    box_x: int = 10  # in microns
    box_y: int = 10

    def start_offsets(self, sample: Sample) -> List[str]:
        xyz = [float(x) for x in sample.crystal_size.split()]
        nx = round(xyz[0] / self.box_x / 2.0 - 0.5)
        ny = round(xyz[1] / self.box_y / 2.0 - 0.5)

        pos = []
        for i in range(-nx, nx + 1):
            for j in range(-ny, ny + 1):
                x = i * self.box_x
                y = j * self.box_y
                p = f"{y} {x} 0"
                pos.append(p)
        return pos


def run_raddose_raster() -> None:
    s = Sample(crystal_size="50 10 10")
    b = Beam(beam_size="10 10")
    raster = Raster()

    ws = []
    for offset in raster.start_offsets(s):
        w = Wedge(start_offset=offset, start_angle=0.0, osc=0.0, nimages=1)
        ws.append(w)
    r = run_raddose3d(s, b, ws)
    logger.info(r)
    logger.info([x["Average DWD"] for x in r])
    logger.info([x["Max Dose"] for x in r])


if __name__ == "__main__":
    data, summary_dict = run_raddose3d_one_wedge(
        crystal_size="50 50 50",
        energy=12.0,
        beam_size="20 20",
        translate_per_degree="0 0 0",
        cell="78 78 39 90 90 90",
        # symm=92,
        # nres=128,
        coef_calc="rd3d",
        osc=1.0,
    )

    logger.info(data)
    logger.info(summary_dict)
    # run_raddose_raster()
