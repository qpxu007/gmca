import math
import os
import re
import json
import xml.etree.ElementTree as ET
from collections import OrderedDict
from collections import namedtuple
from typing import Dict, Any
from pathlib import Path
from typing import Sequence, Optional


import numpy as np

from qp2.pipelines.gmcaproc.symmetry import Symmetry
from qp2.pipelines.gmcaproc.xds_config import XdsConfig
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


def parse_xplan_lp(filepath):
    """Parse XPLAN.LP for summary table and detailed completeness data."""
    try:
        with open(filepath, "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return None

    table = []
    in_table = False
    for i, line in enumerate(lines[:-1]):
        if not in_table and "starting at" in line and "spindle_angle" in lines[i + 1]:
            in_table = True
            continue
        if in_table:
            fields = line.strip().split()
            if len(fields) == 4:
                try:
                    table.append([float(f) for f in fields])
                except ValueError:
                    break

    # Select row with completeness >= 90 and smallest total rotation
    eligible = [row for row in table if row[2] >= 90]
    selected = min(eligible, key=lambda x: x[1]) if eligible else None
    if not selected:
        return {"xplan_table": table}

    start, rot, comp, mult = selected
    results = {
        "xplan_table": table,
        "xplan_starting_angle": start,
        "xplan_total_rotation": rot,
        "xplan_completeness": comp,
        "xplan_multiplicity": mult,
    }

    # Extract detailed completeness for matching total rotation and starting angle
    in_section = False
    for i, line in enumerate(lines):
        if (
            " TOTAL ROTATION RANGE =" in line
            and abs(float(line.split()[-2]) - rot) <= 0.01
        ):
            in_section = True
        if (
            in_section
            and "COMPLETENESS OF DATA COLLECTED IN THE OSCILLATION RANGE" in line
        ):
            curr_start = float(line.split()[-2].split("...")[0])
            if abs(curr_start - start) <= 0.01:
                results.update(
                    {
                        "xplan_completeness_existing_percent": float(
                            lines[i + 2].split()[-1].replace("%", "")
                        ),
                        "xplan_completeness_fromnew_percent": float(
                            lines[i + 3].split()[-1].replace("%", "")
                        ),
                        "xplan_completeness_combined_percent": float(
                            lines[i + 4].split()[-1].replace("%", "")
                        ),
                        "xplan_common_reflections_percent": float(
                            lines[i + 5].split()[-1].replace("%", "")
                        ),
                    }
                )
                break
    return results


def extract_mosaicity(text):
    # Returns all found mosaicity values as floats
    return [
        float(x) for x in re.findall(r"CRYSTAL MOSAICITY \(DEGREES\)\s+([0-9.]+)", text)
    ]


def extract_spot_stddev(text):
    # Returns all found standard deviation spot position values as floats
    return [
        float(x)
        for x in re.findall(
            r"STANDARD DEVIATION OF SPOT\s+POSITION \(PIXELS\)\s+([0-9.]+)", text
        )
    ]


def extract_max_osc_range(text):
    # Returns tables of (oscillation range, high res limit) as tuples of floats
    return [
        tuple(map(float, m))
        for m in re.findall(
            r"\s+([0-9.]+)\s+([0-9.]+)",
            re.search(
                r"Maximum oscillation range.*?\(degrees\).*?\(Angstrom\)(.*?)(?:\n\n|\Z)",
                text,
                re.S,
            ).group(1),
        )
    ]


def parse_idxref_lp(file_path, user_space_group=None, parse_symmetry_table=False):
    result_dict: Dict[str, Any] = {
        "user_space_group": user_space_group,
        "user_unit_cell": None,
    }

    with open(file_path, "r") as file:
        lines = file.readlines()

    # Extract the first table (Lattice Character and Bravais Lattice)
    lattice_table_start = None
    lattice_table_end = None
    for i, line in enumerate(lines):
        # if "LATTICE-  BRAVAIS-" in line:
        if (
            "CHARACTER  LATTICE     OF FIT      a      b      c   alpha  beta gamma"
            in line
        ):
            lattice_table_start = i + 1
        if (
            lattice_table_start and "For protein crystals" in line
        ):  # Detect end of table
            lattice_table_end = i
            break

    index_table = (
        lines[lattice_table_start:lattice_table_end]
        if lattice_table_start and lattice_table_end
        else []
    )
    index_table_header = [
        "MARKED",
        "LATTICE_CHARACTER",
        "BRAVAIS_LATTICE",
        "QUALITY_OF_FIT",
        "a",
        "b",
        "c",
        "alpha",
        "beta",
        "gamma",
    ]
    index_table_candidates = []
    index_table_rest = []
    for line in index_table:
        if line.strip():
            parts = re.split(r"\s+", line.strip())
            if len(parts) == 10:
                index_table_candidates.append(parts)
            elif len(parts) == 9:
                index_table_rest.append([""] + parts)

    # create a map for lattice order, the candidates will be sorted in reverse order of lattice, then
    # increasing value of quality of fit
    lattice_order = {key: i for i, key in enumerate(reversed(Symmetry.get_lattices()))}
    index_table_candidates = sorted(
        index_table_candidates, key=lambda x: (lattice_order.get(x[2], -1), float(x[3]))
    )

    if parse_symmetry_table:
        # Extract the second table (Lattice Symmetry Implicated by Space Group Symmetry)
        symmetry_table_start = None
        symmetry_table_end = None
        for i, line in enumerate(lines):
            if "LATTICE SYMMETRY IMPLICATED BY SPACE GROUP SYMMETRY" in line:
                symmetry_table_start = i + 1
            if (
                symmetry_table_start and "Maximum oscillation range" in line
            ):  # Detect end of table
                symmetry_table_end = i
                break

        reference_table = (
            lines[symmetry_table_start:symmetry_table_end]
            if symmetry_table_start and symmetry_table_end
            else []
        )
        symmetry_table = OrderedDict()
        line_pattern = r"(\w+(?:,\w+)*)\s+(?:\[(\d+,[^\]]+)\]\s*)+"
        for line in reference_table:
            matches = re.finditer(line_pattern, line)
            for match in matches:
                key = match.group(1)
                value = re.findall(r"\[(\d+,[^\]]+)\]", match.group(0))
                symmetry_table[key] = [
                    (int(x), y) for pair in value for x, y in [pair.split(",")]
                ]

    user_unit_cell = None
    if user_space_group:
        lattice_type = Symmetry.space_group_to_lattice(user_space_group)
        for alist in index_table_candidates:
            if alist[0] == "*" and alist[2] == lattice_type:
                user_unit_cell = Symmetry.correct_cell_enforced_by_lattice(
                    lattice_type, alist[-6:]
                )
                break

    result_dict["user_unit_cell"] = user_unit_cell
    result_dict["index_table_text"] = index_table
    result_dict["index_table_header"] = index_table_header
    result_dict["index_table_candidates"] = index_table_candidates
    result_dict["index_table_rest"] = index_table_rest

    result_dict["possible_solutions"] = []
    for solution in index_table_candidates:
        alattice = solution[2]
        aspg = Symmetry.get_lowest_spacegroup_number(alattice)
        corrected_cell = Symmetry.correct_cell_enforced_by_lattice(
            alattice, solution[-6:]
        )
        corrected_cell_str = [str(x) for x in corrected_cell]
        result_dict["possible_solutions"].append([alattice, aspg, corrected_cell_str])

    # highest lattice with the best fit
    if result_dict["possible_solutions"]:
        result_dict["auto_index_lattice"] = result_dict["possible_solutions"][0][0]
        result_dict["auto_index_unitcell"] = result_dict["possible_solutions"][0][-1]
        result_dict["auto_index_spacegroup"] = result_dict["possible_solutions"][0][1]
    else:
        logger.warning("No possible indexing solutions found in IDXREF.LP.")

    logger.debug(f"auto index result: {result_dict['index_table_candidates']}")

    return result_dict


def parse_idxref_strategy(file_path):
    with open(file_path, "r") as file:
        log = file.read()

    mosaicities = extract_mosaicity(log)
    spot_stddev = extract_spot_stddev(log)
    max_osc_range = extract_max_osc_range(log)

    mosaicity = mosaicities[-1] if mosaicities else None

    sdxy_pixel = spot_stddev[-1] if spot_stddev else None

    osc_range = min(osc for (osc, r) in max_osc_range) if max_osc_range else None

    logger.debug(
        f"mosaicity: {mosaicity} spot_stddev: {sdxy_pixel} max_osc_range: {osc_range}"
    )
    return {
        "mosaicity": mosaicity,
        "spot_stddev": sdxy_pixel,
        "max_osc_range": osc_range,
    }


def is_decreasing(arr):
    """
    Checks if an array is in non-increasing (decreasing) order.
    Allows for duplicate values (e.g., [10, 8, 8, 5]).
    Returns True for empty or single-element arrays.
    """
    # Compare each element with the one that comes after it.
    # The generator (arr[i] >= arr[i+1] for...) creates a sequence of True/False values.
    # all() returns True only if all values in the sequence are True.
    return all(arr[i] >= arr[i + 1] for i in range(len(arr) - 1))


def is_mostly_decreasing(arr: list, tolerance_percent: float = 0) -> bool:
    """
    Checks if an array is in a non-increasing (mostly decreasing) order,
    allowing for a specified percentage tolerance for increases.

    Args:
        arr: The list of numbers to check.
        tolerance_percent: The percentage by which a value can be greater
                           than its predecessor and still be considered
                           "decreasing". Defaults to 0 (strict non-increasing).

    Returns:
        True if the array is mostly decreasing within the tolerance.
        Returns True for empty or single-element arrays.
    """
    if tolerance_percent < 0:
        raise ValueError("Tolerance percentage cannot be negative.")

    # Calculate the multiplicative factor from the percentage.
    # e.g., a 5% tolerance means a value can be up to 1.05 times the previous one.
    tolerance_factor = 1 + (tolerance_percent / 100.0)

    # Compare each element with the one that comes after it.
    # The condition checks if the next element is within the acceptable tolerance.
    # This handles positive, negative, and zero values correctly.
    return all(arr[i + 1] <= arr[i] * tolerance_factor for i in range(len(arr) - 1))


def interpolate_from_linear_fit(x, y, target_x):
    """
    Performs a linear fit (y = mx + b) on the entire dataset (x, y) and then
    calculates the y value for a given target_x using the best-fit line.

    Args:
        x (list or np.ndarray): List of x values.
        y (list or np.ndarray): List of y values (must be the same length as x).
        target_x (float): The x value for which to calculate y from the fitted line.

    Returns:
        float: The calculated y value from the linear fit equation, or None if a
               fit is not possible (e.g., fewer than 2 data points).
    """
    # --- Input Validation ---
    if len(x) != len(y):
        raise ValueError("x and y must have the same length.")
    if len(x) < 2:
        # A linear fit requires at least two points to define a line.
        return None

    # --- Linear Fit ---
    # np.polyfit with deg=1 performs a linear regression (y = mx + b).
    # It returns the coefficients [m, b] (slope and y-intercept).
    try:
        m, b = np.polyfit(x, y, 1)
    except np.linalg.LinAlgError:
        # This can happen if the data is ill-conditioned (e.g., all x values are the same)
        return None

    # --- Calculation from the Fitted Line ---
    # Use the calculated slope (m) and intercept (b) to find the y value.
    interpolated_y = m * target_x + b

    return np.round(interpolated_y, 2)


def estimate_resolution(x, y, target_x):
    """
    Interpolates the y value for a given target_x. To handle data that is not
    strictly decreasing, it first finds the Longest Decreasing Subsequence (LDS)
    of x, which is equivalent to removing the minimum number of points to
    enforce a decreasing trend. The interpolation is then performed on this
    filtered, well-behaved data.

    Args:
        x (list): List of x values.
        y (list): List of y values (same length as x).
        target_x (float): The x value for which to interpolate y.

    Returns:
        float: Interpolated y value, a specific value if out of bounds, or None.
    """
    if len(x) != len(y):
        raise ValueError("x and y must have the same length")

    if not x:
        return None

    # If all remaining x values are greater than target_x, return the last y value
    if all(a > target_x for a in x):
        return y[-1]

    # If all remaining x values are less than target_x, return -1
    if all(a < target_x for a in x):
        return -1

    # --- Modification Start: Find the Longest Decreasing Subsequence (LDS) ---
    n = len(x)
    # dp[i] will store the length of the LDS ending at index i
    dp = [1] * n
    # parent[i] will store the index of the previous element in the LDS ending at i
    parent = [-1] * n

    for i in range(1, n):
        for j in range(i):
            # If x[i] can extend the subsequence ending at x[j]
            if x[i] < x[j] and dp[j] + 1 > dp[i]:
                dp[i] = dp[j] + 1
                parent[i] = j

    # Find the end of the longest subsequence
    max_len = 0
    end_idx = -1
    for i in range(n):
        if dp[i] > max_len:
            max_len = dp[i]
            end_idx = i

    # Reconstruct the LDS indices by backtracking from the end
    lds_indices = []
    curr_idx = end_idx
    while curr_idx != -1:
        lds_indices.append(curr_idx)
        curr_idx = parent[curr_idx]
    lds_indices.reverse()  # Reverse to get the correct order

    # Create filtered lists based on the LDS indices
    x_filtered = [x[i] for i in lds_indices]
    y_filtered = [y[i] for i in lds_indices]
    # print(x_filtered, y_filtered)
    # --- Modification End ---

    # Check if any data remains after filtering
    if not x_filtered:
        return None

    # Perform interpolation on the filtered data
    for i in range(len(x_filtered) - 1, 0, -1):
        x1, x2 = x_filtered[i - 1], x_filtered[i]
        y1, y2 = y_filtered[i - 1], y_filtered[i]

        if x2 <= target_x <= x1:
            if x2 != x1:
                # Perform linear interpolation using the formula:
                # y = y1 + (y2 - y1) * (x - x1) / (x2 - x1)
                return y1 + (y2 - y1) * (target_x - x1) / (x2 - x1)
            else:
                return y1

    return y_filtered[-1]


def parse_correct_lp(file_path):
    table1_header = [
        "RESOLUTION_LIMIT",
        "NUMBER_OBSERVED",
        "NUMBER_UNIQUE",
        "NUMBER_POSSIBLE",
        "COMPLETENESS",
        "R_FACTOR_OBSERVED",
        "R_FACTOR_EXPECTED",
        "NREFL_COMPARED",
        "I_SIGMA",
        "R_MEAS",
        "CC_HALF",
        "ANOMAL_CORR",
        "SIGANO",
        "NANO",
    ]
    result_dict: Dict[str, Any] = {
        "SPACE_GROUP_NUMBER": None,
        "UNIT_CELL_CONSTANTS": None,
        "ISa": None,
        "B": None,  # Wilson B-factor
        "table1_header": table1_header,
        "table1": [],
        "table1_total": [],
        "table1_text": "",
        "resolution_based_on_isigma": None,
        "resolution_based_on_cchalf": None,
    }

    with open(file_path, "r") as file:
        content = file.read()

    section_start = "SELECTED SPACE GROUP AND UNIT CELL FOR THIS DATA SET"
    section_end = "UNIT_CELL_C-AXIS="
    in_section = False
    lines = content.split("\n")
    for i, line in enumerate(lines):
        if section_start in line:
            in_section = True
            continue
        if in_section:
            key_value = line.strip().split("=")
            if len(key_value) == 2:
                key, value = key_value
                result_dict[key.strip()] = value.strip()
            if section_end in line:
                in_section = False

        if "a        b          ISa" in line and i + 1 < len(lines):
            values_line = lines[i + 1].strip()
            values = values_line.split()
            if len(values) == 3:
                result_dict["ISa"] = float(values[2])

        if "WILSON LINE (using all data)" in line:
            match = re.search(r"B=\s*([0-9]+\.[0-9]+)", line)
            if match:
                result_dict["B"] = float(match.group(1))

    # Parse refined parameters from the "REFINEMENT OF DIFFRACTION PARAMETERS USING ALL IMAGES" section
    refinement_section_start = "REFINEMENT OF DIFFRACTION PARAMETERS USING ALL IMAGES"
    refinement_section_end = "THE DATA COLLECTION STATISTICS REPORTED BELOW ASSUMES" 
    in_refinement = False
    
    for line in lines:
        if refinement_section_start in line:
            in_refinement = True
            continue
        if in_refinement:
            if refinement_section_end in line:
                in_refinement = False
                break
            
            line_strip = line.strip()
            if line_strip.startswith("UNIT CELL PARAMETERS"):
                # UNIT CELL PARAMETERS    232.015   232.015   232.015  90.000  90.000  90.000
                parts = line_strip.split()
                if len(parts) >= 7:
                    # parts[0:3] is "UNIT CELL PARAMETERS"
                    result_dict["REFINED_UNIT_CELL_CONSTANT"] = " ".join(parts[3:9])

            elif line_strip.startswith("CRYSTAL TO DETECTOR DISTANCE (mm)"):
                # CRYSTAL TO DETECTOR DISTANCE (mm)       251.63
                parts = line_strip.split()
                if len(parts) >= 6:
                     # parts[0:5] is "CRYSTAL TO DETECTOR DISTANCE (mm)"
                    try:
                        result_dict["REFINED_DISTANCE"] = float(parts[5])
                    except ValueError:
                        pass

            elif line_strip.startswith("CRYSTAL MOSAICITY (DEGREES)"):
                # CRYSTAL MOSAICITY (DEGREES)     0.117
                parts = line_strip.split()
                if len(parts) >= 4:
                    # parts[0:3] is "CRYSTAL MOSAICITY (DEGREES)"
                    try:
                        result_dict["REFINED_MOSAICITY"] = float(parts[3])
                    except ValueError:
                        pass

            elif line_strip.startswith("DETECTOR COORDINATES (PIXELS) OF DIRECT BEAM"):
                # DETECTOR COORDINATES (PIXELS) OF DIRECT BEAM    2125.81   2225.72
                parts = line_strip.split()
                if len(parts) >= 8:
                    # parts[0:6] is "DETECTOR COORDINATES (PIXELS) OF DIRECT BEAM"
                    try:
                        result_dict["REFINED_ORXY"] = f"{float(parts[6])} {float(parts[7])}"
                    except ValueError:
                        pass

    logger.info(
        f"xds determined spg={result_dict['SPACE_GROUP_NUMBER']}, cell={result_dict['UNIT_CELL_CONSTANTS']}, ISa={result_dict['ISa']}"
    )
    # Find the last occurrence of the table
    table_data = []
    table_start = "SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE"
    table_end = "NUMBER OF REFLECTIONS IN SELECTED SUBSET OF IMAGES"
    last_table_start = content.rfind(table_start)
    if last_table_start != -1:
        table_section = content[last_table_start:]
        table_end_pos = table_section.find(table_end)
        if table_end_pos != -1:
            table_section = table_section[:table_end_pos]
            result_dict["table1_text"] = "\n".join(
                table_section.strip().split("\n")[1:]
            )

            lines_ = table_section.split("\n")
            for line in lines_[1:]:  # Skip the first line (title)
                if re.match(r"\s+[\d.]+", line):
                    values = re.findall(r"\S+", line)
                    if len(values) == len(table1_header):
                        table_data.append(values)
                elif "total" in line:
                    result_dict["table1_total"] = line.split()

    # append to REMOVE.HKL
    remove_hkl = [l for l in lines if "alien" in l]
    logger.info(f"Found {len(remove_hkl)} lines to be rejected.")
    with open(os.path.dirname(file_path) + "/REMOVE.HKL", "a") as f:
        f.write("".join(remove_hkl))

    result_dict["table1"] = table_data

    def interpolated_resolution(header, xds_table1, using_column, target_value):
        index_of_using_column = header.index(using_column)
        x_list = [
            float(l[index_of_using_column].replace("*", "").replace("%", ""))
            for l in xds_table1
        ]

        resolution_list = [float(l[0]) for l in xds_table1]

        resolution_i = estimate_resolution(x_list, resolution_list, target_value)

        # Rounds a number up to the nearest 0.05.
        return math.ceil(resolution_i * 20) / 20

    result_dict["resolution_based_on_cchalf"] = interpolated_resolution(
        table1_header,
        table_data,
        using_column="CC_HALF",
        target_value=XdsConfig.CC_HALF_TARGET,
    )

    result_dict["resolution_based_on_isigma"] = interpolated_resolution(
        table1_header,
        table_data,
        using_column="I_SIGMA",
        target_value=XdsConfig.ISIGMA_TARGET,
    )

    result_dict["resolution_based_on_cc_anom"] = interpolated_resolution(
        table1_header,
        table_data,
        using_column="ANOMAL_CORR",
        target_value=XdsConfig.CC_ANOM_TARGET,
    )

    result_dict["resolution_highres"] = float(result_dict["table1"][-1][0])

    logger.info(
        f"resolution cutoff based on CC1/2>={XdsConfig.CC_HALF_TARGET}: {result_dict['resolution_based_on_cchalf']}"
    )
    table1 = namedtuple("Table1", result_dict["table1_header"])
    table1_total = table1(*result_dict["table1_total"])
    table1_lastshell = table1(*result_dict["table1"][-1])
    logger.info(f"table1 total: {table1_total}")
    logger.info(f"table1 lastshell: {table1_lastshell}")

    return result_dict


def parse_pointless_xml(file_path):
    if not os.path.exists(file_path):
        return None

    try:
        with open(file_path, "r") as f:
            xmldata = f.read()
        root = ET.fromstring(xmldata)
    except (ET.ParseError, FileNotFoundError) as e:
        logger.error(f"Failed to parse pointless XML file {file_path}: {e}")
        return None

    # Extract information from BestCell
    best_cell_element = root.find("BestCell/cell")
    if best_cell_element is None:
        logger.warning(f"No <BestCell> found in {file_path}")
        return None  # Cannot proceed without cell

    cell_data = {
        "a": float(best_cell_element.find("a").text),
        "b": float(best_cell_element.find("b").text),
        "c": float(best_cell_element.find("c").text),
        "alpha": float(best_cell_element.find("alpha").text),
        "beta": float(best_cell_element.find("beta").text),
        "gamma": float(best_cell_element.find("gamma").text),
    }

    # --- MODIFICATION START ---
    # Extract information from BestSolution, which may not exist
    best_solution = root.find("BestSolution")

    # If there is no BestSolution, Pointless could not decide. Return None.
    if best_solution is None:
        logger.warning(
            f"No <BestSolution> tag found in {file_path}. Pointless could not determine a unique solution."
        )
        return None

    result = {
        "pointless_best_solution": {
            "GroupName": best_solution.find("GroupName").text.strip(),
            "SGnumber": best_solution.find("SGnumber").text.strip(),
            "CCP4_SGnumber": best_solution.find("CCP4_SGnumber").text.strip(),
            "ReindexOperator": best_solution.find("ReindexOperator").text.strip(),
            "ReindexMatrix": best_solution.find("ReindexMatrix").text.strip(),
            "LGProb": float(best_solution.find("LGProb").text),
            "SysAbsProb": float(best_solution.find("SysAbsProb").text),
            "Confidence": float(best_solution.find("Confidence").text),
            "LGconfidence": float(best_solution.find("LGconfidence").text),
            "TotalProb": float(best_solution.find("TotalProb").text),
            "UnitCell": " ".join(
                str(cell_data[key]) for key in ["a", "b", "c", "alpha", "beta", "gamma"]
            ),
        }
    }
    # --- MODIFICATION END ---
    return result


def extract_space_groups(text):
    """
    Extracts space group names by splitting columns on multiple spaces
    rather than relying on exact indentation counts.
    """
    # Extract the relevant section
    pattern = re.compile(
        r"Choosing between possible best groups:\s*(.*?)\s*Space group confidence",
        re.DOTALL | re.MULTILINE,
    )
    match = pattern.search(text)
    if not match:
        return []

    section_text = match.group(1)
    space_groups = []

    for line in section_text.splitlines():
        line = line.strip()
        # Skip empty lines and header line
        if not line or line.startswith("Space group"):
            continue

        # Split by 2 or more spaces to separate columns
        parts = re.split(r"\s{2,}", line)
        if parts:
            space_groups.append(parts[0])  # First column is the space group

    return space_groups


def parse_spot_xds(filepath, output_json=None):
    """Parses SPOT.XDS to get detailed spot info for frames that were processed."""
    results_by_frame = {}
    try:
        with open(filepath, "r") as f:
            lines = f.readlines()

        for line in lines:
            line = line.strip()
            if not line:
                continue  # skip empty lines

            # Safely parse only fully numeric lines
            try:
                parts = list(map(float, line.split()))
            except ValueError:
                continue  # skip headers/comments or malformed rows

            if len(parts) == 4:
                x, y, z, intensity = parts
                frame_num = int(z + 0.5)
                frame_dict = results_by_frame.setdefault(
                    frame_num, {"spots_xds": [], "reflections_xds": []}
                )
                frame_dict["spots_xds"].append([x, y, 0])
            elif len(parts) == 7:
                x, y, z, intensity, h, k, l = parts
                frame_num = int(z + 0.5)
                h_i, k_i, l_i = int(h), int(k), int(l)
                is_indexed = not (h_i == 0 and k_i == 0 and l_i == 0)
                frame_dict = results_by_frame.setdefault(
                    frame_num, {"spots_xds": [], "reflections_xds": []}
                )
                frame_dict["spots_xds"].append([x, y, 1 if is_indexed else 0])
                if is_indexed:
                    frame_dict["reflections_xds"].append([h_i, k_i, l_i, x, y])
            else:
                # Ignore lines that don't match expected numeric field counts
                continue

    except FileNotFoundError:
        logger.warning(f"{filepath} not found. Cannot parse spot coordinates.")
    except Exception as e:
        logger.error(f"Error parsing {filepath}: {e}", exc_info=True)

    if output_json:
        with open(output_json, "w") as f:
            json.dump(results_by_frame, f, indent=2)
        logger.info(f"Successfully wrote spot details to {output_json}")

    return results_by_frame


def update_xparm_spacegroup_cell(
    infile: str,
    space_group: int,
    unit_cell: Sequence[float],
    outfile: Optional[str] = None,
    backup: bool = True,
):
    """
    Update the 'space group number and unit cell parameters' line in XPARM.XDS/GXPARM.XDS.
    The line is written in fixed-width fields: I10,6F10.3 as required by XDS.

    Parameters
    ----------
    infile : path to XPARM.XDS (or GXPARM.XDS)
    space_group : integer space-group number (1..230)
    unit_cell : iterable of 6 floats [a, b, c, alpha, beta, gamma]
    outfile : if given, write to this path; otherwise overwrite infile
    backup : if overwriting, create a .bak copy first
    """
    a, b, c, alpha, beta, gamma = unit_cell
    if not (1 <= int(space_group) <= 230):
        raise ValueError("space_group must be an integer in [1, 230]")

    p_in = Path(infile)
    if not p_in.is_file():
        raise FileNotFoundError(f"File not found: {p_in}")

    text = p_in.read_text().splitlines()

    # Find the SG+cell line: exactly 7 tokens, first is int (1..230), next six are floats
    target_idx = None
    for i, line in enumerate(text):
        parts = line.split()
        if len(parts) != 7:
            continue
        try:
            sg_candidate = int(parts[0])
            if not (1 <= sg_candidate <= 230):
                continue
            # ensure remaining tokens are floats
            _ = [float(x) for x in parts[1:]]
        except ValueError:
            continue
        # Optional contextual sanity check: following lines often are 3 floats each (a,b,c axes)
        target_idx = i
        break

    if target_idx is None:
        raise RuntimeError(
            "Could not locate the space-group/unit-cell line in XPARM.XDS"
        )

    # Format per XDS: I10,6F10.3 (fixed-width, no extra spaces)
    new_line = (
        f"{int(space_group):10d}"
        f"{a:10.3f}{b:10.3f}{c:10.3f}{alpha:10.3f}{beta:10.3f}{gamma:10.3f}"
    )

    text[target_idx] = new_line

    # Write output
    p_out = Path(outfile) if outfile else p_in
    if not outfile and backup:
        p_in.rename(p_in.with_suffix(p_in.suffix + ".bak"))
        p_out = p_in  # overwrite original after renaming to .bak

    p_out.write_text("\n".join(text) + "\n")


def parse_pointless_log(file_path):
    try:
        with open(file_path, "r") as file:
            content = file.read()

            # Find the relevant summary section containing "Result:"
            start_marker = "<!--SUMMARY_BEGIN-->\n$TEXT:Result: $$ $$"
            end_marker = "$$ <!--SUMMARY_END-->"
            start_idx = content.find(start_marker)
            if start_idx == -1:
                return "Result summary section not found"
            start_idx += len(start_marker)
            end_idx = content.find(end_marker, start_idx)
            if end_idx == -1:
                return "End of result summary section not found"

            # Extract the summary section
            summary = content[start_idx:end_idx].strip()

            # Process line by line
            lines = summary.split("\n")

            parsed_data = {}
            for line in lines:
                line = line.strip()
                # Extract unit cell
                if line.startswith("Unit cell:"):
                    parsed_data["UnitCell"] = line.split(":", 1)[1].strip()
                # Extract space group confidence
                elif line.startswith("Space group confidence:"):
                    parsed_data["SpaceGroup Confidence"] = line.split(":", 1)[1].strip()
                elif line.startswith("Laue group confidence"):
                    parsed_data["LaueGroup Confidence"] = line.rsplit()[-1].strip()
                # Extract space group
                elif line.startswith("Best Solution:"):
                    parsed_data["GroupName"] = line.split("group", 1)[1].strip()
                elif line.startswith("Systematic absence probability:"):
                    parsed_data["SysAbsProb"] = line.rsplit()[-1].strip()
                elif line.startswith("Total probability:"):
                    parsed_data["TotalProb"] = line.rsplit()[-1].strip()
                elif line.startswith("Laue group probability:"):
                    parsed_data["LGProb"] = line.rsplit()[-1].strip()

        ccp4_sg_number = Symmetry.symbol_to_number(parsed_data["GroupName"])
        sg_number = ccp4_sg_number
        lg_spg_confidence = parsed_data.get("LaueGroup Confidence", 0.0)
        possible_sgs = extract_space_groups(content)
        group_name = parsed_data["GroupName"]
        spg_confidence = parsed_data.get("SpaceGroup Confidence", 0.0)
        unit_cell = parsed_data["UnitCell"]

        result = {
            "pointless_best_solution": {
                "GroupName": group_name,
                "SGnumber": str(sg_number),
                "CCP4_SGnumber": str(ccp4_sg_number),
                "PossibleSPGs": possible_sgs,
                "LGProb": parsed_data.get("LGProb", 0.0),
                "SysAbsProb": parsed_data.get("SysAbsProb", 0.0),
                "Confidence": spg_confidence,
                "LGconfidence": lg_spg_confidence,
                "TotalProb": parsed_data.get("SysAbsProb", 0.0),
                "UnitCell": " ".join(unit_cell.split()),
            }
        }

        return result

    except FileNotFoundError:
        return "File not found"
    except Exception as e:
        return f"Error parsing file: {str(e)}"


def parse_integrate_lp(integrate_lp_path):
    """find update beam and reflection parameters used for refinement"""
    dict_integ_suggestions = {}

    try:
        with open(integrate_lp_path, "r") as f:
            lines = f.readlines()[:-100]

        found_header = False

        # Search for the specific suggestion block
        for line_idx, line_content in enumerate(lines):
            if (
                "SUGGESTED VALUES FOR INPUT PARAMETERS" in line_content
            ):  # More specific header
                found_header = True

                # Parameters usually start 2 lines after this header
                param_lines_start_idx = line_idx + 1
                for param_line_idx in range(param_lines_start_idx, len(lines)):
                    current_param_line = lines[param_line_idx].strip()
                    if not current_param_line:  # Stop at empty line
                        break

                    elements = re.split(r"\s*=\s*|\s+", current_param_line.strip())

                    # Pair up elements as key-value pairs
                    dict_integ_suggestions.update(
                        dict(zip(elements[::2], elements[1::2]))
                    )

                logger.info(f"Found suggested parameter: {dict_integ_suggestions}")
                break  # Found suggestions, exit loop

        else:
            logger.info(
                "No new suggestions found in INTEGRATE.LP or section not found."
            )

    except FileNotFoundError:  # Should have been caught by os.path.exists
        logger.error(
            f"Error: {integrate_lp_path} not found during optimization param update."
        )
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while parsing INTEGRATE.LP for suggestions: {e}"
        )

    return dict_integ_suggestions


def parse_integrate_lp_per_frame(integrate_lp_path: str) -> dict:
    """
    Parses INTEGRATE.LP to extract statistics for each integrated frame
    from the main processing summary table(s). This version correctly handles
    multiple processing blocks in the file.

    Args:
        integrate_lp_path: The full path to the INTEGRATE.LP file.

    Returns:
        A dictionary where keys are frame numbers (int) and values are
        dictionaries of statistics for that frame.
    """
    per_frame_results = {}
    if not os.path.exists(integrate_lp_path):
        return per_frame_results

    try:
        with open(integrate_lp_path, "r") as f:
            content = f.read()
    except Exception as e:
        logger.error(f"Could not read {integrate_lp_path}: {e}")
        return per_frame_results

    processing_blocks = re.split(r"\s\*{10,}\s+PROCESSING OF IMAGES.*?\n", content)[1:]

    if not processing_blocks:
        logger.warning(
            f"Could not find any 'PROCESSING OF IMAGES' blocks in {integrate_lp_path}"
        )
        return per_frame_results

    header_keys = [
        "IMAGE",
        "IER",
        "SCALE",
        "NBKG",
        "NOVL",
        "NEWALD",
        "NSTRONG",
        "NREJ",
        "SIGMAB",
        "SIGMAR",
    ]

    for block in processing_blocks:
        in_table_section = False
        for line in block.split("\n"):
            # The table starts after the DEFINITION OF SYMBOLS and the header line
            if "IMAGE IER    SCALE     NBKG" in line:
                in_table_section = True
                continue

            if not in_table_section:
                continue

            # Stop parsing this block if we hit a summary or empty line
            if (
                not line.strip()
                or line.strip().startswith("SUBSET")
                or line.strip().startswith("STANDARD DEVIATION")
            ):
                in_table_section = False
                break

            parts = line.strip().split()
            if len(parts) == len(header_keys):
                try:
                    frame_num = int(parts[0])
                    frame_stats = {
                        "ier": int(parts[1]),
                        "scale": float(parts[2]),
                        "num_bg_pixels": int(parts[3]),
                        "num_overloaded_refl": int(parts[4]),
                        "num_ewald_refl": int(parts[5]),
                        "num_strong_refl": int(parts[6]),
                        "num_rejected_refl": int(parts[7]),
                        "beam_divergence_esd": float(parts[8]),
                        "mosaicity_esd": float(parts[9]),
                    }
                    per_frame_results[frame_num] = frame_stats
                except (ValueError, IndexError) as e:
                    logger.warning(
                        f"Could not parse data line in INTEGRATE.LP table: '{line}'. Error: {e}"
                    )
                    continue

    return per_frame_results


if __name__ == "__main__":
    # Example usage:
    # result = parse_summary('path_to_your_file.txt')
    # print(result)

    # r = parse_pointless_log("pointless.out")
    # print(r)
    x = [98.1, 94.5, 91.9, 79.2, 88.8, 83.8, 69.3, 51.5, 32.8]
    y = [7.6, 5.44, 4.46, 3.87, 3.46, 3.16, 2.93, 2.74, 2.59]
    print(estimate_resolution(x, y, 50))
