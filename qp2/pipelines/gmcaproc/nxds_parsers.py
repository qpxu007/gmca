# qp2/gmcaproc/nxds_parsers.py
import json
import logging
import re
from pathlib import Path
from typing import Dict, Any, Optional, List
import math

from qp2.utils.merge_dicts import merge_dicts

try:
    from qp2.pipelines.gmcaproc.xds_config import XdsConfig

    CC_HALF_TARGET = getattr(XdsConfig, "CC_HALF_TARGET", 30)
    ISIGMA_TARGET = getattr(XdsConfig, "ISIGMA_TARGET", 0.5)
except Exception:
    CC_HALF_TARGET = 30
    ISIGMA_TARGET = 0.5


logger = logging.getLogger(__name__)


def parse_colspot_lp(filepath, output_json=None):
    """Parses COLSPOT.LP to get the number of spots per frame."""
    spot_counts = {}
    try:
        with open(filepath, "r") as f:
            content = f.read()

        table_match = re.search(
            r"FRAME #\s+NBKG\s+NSTRONG\s+NSPOT\n(.*?)\n\s*\n", content, re.DOTALL
        )
        if not table_match:
            return {}

        table_content = table_match.group(1)
        for line in table_content.strip().split("\n"):
            parts = line.split()
            if len(parts) == 4:
                try:
                    frame_num = int(parts[0])
                    nspot = int(parts[3])
                    spot_counts[frame_num] = {"nspots": nspot if nspot >= 0 else 0}
                except ValueError:
                    continue
    except FileNotFoundError:
        logger.warning(f"{filepath} not found. Cannot parse spot counts.")
    except Exception as e:
        logger.error(f"Error parsing {filepath}: {e}")

    if output_json:
        with open(output_json, "w") as f:
            json.dump(spot_counts, f, indent=2)
        logger.info(f"Successfully wrote spot counts to {output_json}")
    return spot_counts


def parse_spot_nxds(filepath, output_json=None):
    """Parses SPOT.nXDS to get detailed spot info for frames that were processed."""
    results_by_frame = {}
    try:
        with open(filepath, "r") as f:
            lines = f.readlines()

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if re.match(r"^\d+$", line):
                try:
                    frame_num = int(line)
                    i += 1
                    summary_line = lines[i].strip()
                    nspot = int(summary_line.split()[-1])

                    frame_results = {
                        "spots_nxds": [],
                        "reflections_nxds": [],
                    }
                    for j in range(nspot):
                        i += 1
                        parts = [float(p) for p in lines[i].strip().split()]

                        if len(parts) == 7:
                            z, x, y, intensity, h, k, l = parts
                            is_indexed = not (h == 0 and k == 0 and l == 0)
                            frame_results["spots_nxds"].append(
                                [x, y, 1 if is_indexed else 0]
                            )
                            if is_indexed:
                                frame_results["reflections_nxds"].append(
                                    [int(h), int(k), int(l), x, y]
                                )
                    results_by_frame[frame_num] = frame_results
                except (ValueError, IndexError):
                    logger.warning(
                        f"Could not parse spot block starting at line {i} in {filepath}"
                    )
                    break
            i += 1
    except FileNotFoundError:
        logger.warning(f"{filepath} not found. Cannot parse spot coordinates.")
    except Exception as e:
        logger.error(f"Error parsing {filepath}: {e}", exc_info=True)

    if output_json:
        with open(output_json, "w") as f:
            json.dump(results_by_frame, f, indent=2)
        logger.info(f"Successfully wrote spot details to {output_json}")

    return results_by_frame


def parse_nxds_idxref_log(nxds_idxref_log: str, output_json=None) -> dict:
    """
    Parses the IDXREF.LP.txt log file, correctly handling the final image block.

    Args:
        log_content: A string containing the content of the IDXREF.LP.txt file.

    Returns:
        A list of dictionaries with the parsed information for each image.
    """
    with open(nxds_idxref_log, "r") as file:
        log_content = file.read()

    #print(log_content[:1000])
    parsed_data = {}

    # Find the start of all image sections and their numbers
    headers = list(
        re.finditer(r"\s\*{10,}\s+SPOT INDEXING FOR IMAGE\s+(\d+)", log_content)
    )

    for i, header_match in enumerate(headers):
        image_number = int(header_match.group(1))

        # Define the start and end position for the current section
        start_pos = header_match.end()
        end_pos = headers[i + 1].start() if i + 1 < len(headers) else len(log_content)

        section = log_content[start_pos:end_pos]

        image_data = {}

        # Extract REDUCED CELL
        reduced_cell_match = re.search(r"REDUCED CELL\s+([\d.\s]+)", section)
        image_data["reduced_cell"] = (
            reduced_cell_match.group(1).strip() if reduced_cell_match else None
        )

        # Extract ACCEPTING/EXCLUDING status
        status_match = re.search(r"(ACCEPTING|EXCLUDING) IMAGE\s+\d+", section)
        image_data["accepted"] = (
            status_match.group(1) == "ACCEPTING" if status_match else None
        )

        # Extract candidate lattices table
        candidate_lattices_match = re.search(
            r"DETERMINATION OF LATTICE CHARACTER AND BRAVAIS LATTICE.*?(LATTICE-.*?gamma\n\n(.*?))(?=\n\n\s*NUMBER OF OBSERVED SPOTS|\Z)",
            section,
            re.DOTALL,
        )
        if candidate_lattices_match:
            lattices_str = candidate_lattices_match.group(2).strip()
            image_data["candidate_lattices"] = [
                line.strip() for line in lattices_str.split("\n") if line.strip()
            ]
        else:
            image_data["candidate_lattices"] = []

        # Look for the refined parameters section
        refined_params_section = re.search(
            r"REFINED DIFFRACTION PARAMETERS\s\*{5}(.*?)\*{5}\sDETERMINATION OF LATTICE CHARACTER",
            section,
            re.DOTALL,
        )

        if refined_params_section:
            refined_section_content = refined_params_section.group(1)
            # Extract UNIT CELL PARAMETERS
            unit_cell_match = re.search(
                r"UNIT CELL PARAMETERS\s+([\d.\s]+)", refined_section_content
            )
            image_data["unit_cell_parameters"] = (
                unit_cell_match.group(1).strip() if unit_cell_match else None
            )
            # Extract number of indexed and observed spots
            indexed_spots_match = re.search(
                r"NUMBER OF INDEXED\s+SPOTS\s+(\d+)", refined_section_content
            )
            observed_spots_match = re.search(
                r"NUMBER OF OBSERVED SPOTS\s+(\d+)", refined_section_content
            )
            if indexed_spots_match and observed_spots_match:
                indexed = int(indexed_spots_match.group(1))
                observed = int(observed_spots_match.group(1))
                image_data["percentage_indexed"] = (
                    round((indexed / observed) * 100, 2) if observed > 0 else 0.0
                )
                image_data["num_indexed_spots"] = indexed
                image_data["num_observed_spots"] = observed

            else:
                image_data["percentage_indexed"] = None
                image_data["num_indexed_spots"] = None
                image_data["num_observed_spots"] = None

        else:
            image_data["unit_cell_parameters"] = None
            indexed_spots_match = re.search(r"NUMBER OF INDEXED SPOTS\s+(\d+)", section)
            observed_spots_match = re.search(
                r"NUMBER OF OBSERVED SPOTS\s+(\d+)", section
            )
            if indexed_spots_match and observed_spots_match:
                indexed = int(indexed_spots_match.group(1))
                observed = int(observed_spots_match.group(1))
                image_data["percentage_indexed"] = (
                    round((indexed / observed) * 100, 2) if observed > 0 else 0.0
                )
                image_data["num_indexed_spots"] = indexed
                image_data["num_observed_spots"] = observed
            else:
                image_data["percentage_indexed"] = 0.0
                image_data["num_indexed_spots"] = 0
                image_data["num_observed_spots"] = 0

        parsed_data[image_number] = image_data

    if output_json:
        with open(output_json, "w") as f:
            json.dump(parsed_data, f, indent=2)

    return parsed_data


def parse_nxscale_or_ncorrect_lp(lp_path):
    table1_header = "RESLIM  OBSERVED  UNIQUE  POSSIBLE COMPLETE  I/SIGMA  CHI-test Rmrgd-F  CC(1/2)   NFREE    Zano     N-    N+"
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

    def _safe_float(x: str) -> Optional[float]:
        try:
            return float(x.replace("*", "").replace(",", ""))
        except Exception:
            return None

    def _safe_int(x: str) -> Optional[int]:
        try:
            return int(x.replace(",", ""))
        except Exception:
            return None

    def _parse_unit_cell_constants(text: str) -> Optional[List[float]]:
        m = re.search(r"\bMEDIAN CELL CONSTANTS\s+([0-9.\s]+)", text)
        if m:
            vals = [v for v in m.group(1).strip().split() if re.match(r"^[0-9.]+$", v)]
            if len(vals) >= 6:
                return [float(v) for v in vals[:6]]
        m = re.search(r"Mean conventional cell constants\s+([0-9.\s]+)", text)
        if m:
            vals = [v for v in m.group(1).strip().split() if re.match(r"^[0-9.]+$", v)]
            if len(vals) >= 6:
                return [float(v) for v in vals[:6]]
        m = re.search(r"\bUNIT_CELL_CONSTANTS=\s+([0-9.\s]+)", text)
        if m:
            vals = [v for v in m.group(1).strip().split() if re.match(r"^[0-9.]+$", v)]
            if len(vals) >= 6:
                return [float(v) for v in vals[:6]]
        return None

    def _parse_ISa(text: str) -> Optional[float]:
        m = re.search(r"\bISa\s*=\s*([0-9.+\-Ee]+)", text)
        if m:
            val = _safe_float(m.group(1))
            if val is not None:
                return val
        block = re.search(
            r"CORRECTION PARAMETERS FOR THE STANDARD ERROR OF REFLECTION INTENSITIES.*?\n\s*a\s+b\s*\n\s*([0-9.+\-Ee]+)\s+([0-9.+\-Ee]+)",
            text,
            flags=re.DOTALL,
        )
        if block:
            a_val = _safe_float(block.group(1))
            b_val = _safe_float(block.group(2))
            if a_val and b_val and a_val > 0.0 and b_val > 0.0:
                try:
                    return 1.0 / math.sqrt(a_val * b_val)
                except Exception:
                    return None
        return None

    def _parse_wilson_B(text: str) -> Optional[float]:
        patterns = [
            r"(?:WILSON|Wilson)[^\n]*?\bB(?:-?\s*factor)?\s*=\s*([0-9.+\-Ee]+)",
            r"\bWilson\s+B-?factor\s*[:=]\s*([0-9.+\-Ee]+)",
            r"\bB-?factor\s*\(Wilson\)\s*[:=]\s*([0-9.+\-Ee]+)",
        ]
        for pat in patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                val = _safe_float(m.group(1))
                if val is not None:
                    return val
        m = re.search(
            r"WILSON[^\n]*\n(?:.*\n){0,5}.*\bB\s*=\s*([0-9.+\-Ee]+)",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            val = _safe_float(m.group(1))
            if val is not None:
                return val
        return None

    def _parse_summary_table(text: str) -> Dict[str, Any]:
        out = {"rows": [], "total": None, "text": ""}
        header_re = re.compile(
            r"^\s*RESLIM\s+OBSERVED\s+UNIQUE\s+POSSIBLE\s+COMPLETE\s+I/SIGMA\s+CHI-test\s+Rmrgd-F\s+CC\(1/2\)\s+NFREE\s+Zano\s+N-\s+N\+\s*$"
        )
        lines = text.splitlines()
        start_idx = None
        for i, line in enumerate(lines):
            if header_re.match(line):
                start_idx = i
                break
        if start_idx is None:
            return out
        data_lines: List[str] = []
        j = start_idx + 1
        while j < len(lines):
            ln = lines[j]
            if not ln.strip():
                break
            if re.match(r"^\s*-{3,}\s*$", ln):
                break
            if re.match(r"^\s*(?:total|TOTAL|\d|\.)", ln):
                data_lines.append(ln.rstrip())
            else:
                break
            j += 1
        out["text"] = "\n".join([lines[start_idx]] + data_lines)
        cols = [
            "RESLIM",
            "OBSERVED",
            "UNIQUE",
            "POSSIBLE",
            "COMPLETE",
            "I/SIGMA",
            "CHI-test",
            "Rmrgd-F",
            "CC(1/2)",
            "NFREE",
            "Zano",
            "N-",
            "N+",
        ]
        for raw in data_lines:
            parts = raw.split()
            if not parts:
                continue
            is_total = parts[0].lower() == "total"
            if is_total:
                values = parts[1:]
                row = {"label": "total"}
                for k, v in zip(cols[1:], values):
                    if k in {"OBSERVED", "UNIQUE", "POSSIBLE", "NFREE", "N-", "N+"}:
                        row[k] = _safe_int(v)
                    else:
                        row[k] = _safe_float(v)
                out["total"] = row
                continue
            if len(parts) < 13:
                continue
            row = {}
            for k, v in zip(cols, parts[:13]):
                if k in {"OBSERVED", "UNIQUE", "POSSIBLE", "NFREE", "N-", "N+"}:
                    row[k] = _safe_int(v)
                else:
                    row[k] = _safe_float(v)
            out["rows"].append(row)
        return out

    # Methods replicated from xds_parsers.py for consistent cutoff estimation
    def _estimate_resolution(
        x: List[float], y: List[float], target_x: float
    ) -> Optional[float]:
        #print(x, y, target_x)
        if len(x) != len(y):
            raise ValueError("x and y must have the same length.")
        if not x:
            return None
        if all(a > target_x for a in x):
            return y[-1]
        if all(a < target_x for a in x):
            return -1
        n = len(x)
        dp = [1] * n
        parent = [-1] * n
        for i in range(1, n):
            for j in range(i):
                if x[i] < x[j] and dp[j] + 1 > dp[i]:
                    dp[i] = dp[j] + 1
                    parent[i] = j
        max_len = 0
        end_idx = -1
        for i in range(n):
            if dp[i] > max_len:
                max_len = dp[i]
                end_idx = i
        lds_indices = []
        curr_idx = end_idx
        while curr_idx != -1:
            lds_indices.append(curr_idx)
            curr_idx = parent[curr_idx]
        lds_indices.reverse()
        x_filtered = [x[i] for i in lds_indices]
        y_filtered = [y[i] for i in lds_indices]
        if not x_filtered:
            return None
        for i in range(len(x_filtered) - 1, 0, -1):
            x1, x2 = x_filtered[i - 1], x_filtered[i]
            y1, y2 = y_filtered[i - 1], y_filtered[i]
            if x2 <= target_x <= x1:
                if x2 != x1:
                    return y1 + (y2 - y1) * (target_x - x1) / (x2 - x1)
                else:
                    return y1
        return y_filtered[-1]

    def _ceil_to_0p05(val: Optional[float]) -> Optional[float]:
        if val is None or isinstance(val, bool):
            return None
        return math.ceil(float(val) * 20) / 20.0

    def _interpolated_resolution_from_rows(
        rows: List[Dict[str, Any]], using_key: str, target_value: float
    ) -> Optional[float]:
        if not rows:
            return None
        # Use table order as printed (low-res shells first, high-res last)
        x_vals: List[float] = []
        res_vals: List[float] = []
        for r in rows:
            x = r.get(using_key)
            res = r.get("RESLIM")
            if isinstance(x, (int, float)) and isinstance(res, (int, float)):
                x_vals.append(float(x))
                res_vals.append(float(res))
        if len(x_vals) < 2:
            return None
        est = _estimate_resolution(x_vals, res_vals, target_value)
        return _ceil_to_0p05(est) if est is not None else None

    # Read file
    try:
        text = Path(lp_path).read_text()
    except FileNotFoundError:
        logger.warning(f"{lp_path} not found. Cannot parse.")
        return result_dict
    except Exception as e:
        logger.error(f"Error reading {lp_path}: {e}")
        return result_dict

    # SPACE_GROUP_NUMBER
    m = re.search(r"\bSPACE_GROUP_NUMBER=\s*(\d+)", text)
    if m:
        result_dict["SPACE_GROUP_NUMBER"] = int(m.group(1))

    # UNIT_CELL_CONSTANTS
    result_dict["UNIT_CELL_CONSTANTS"] = _parse_unit_cell_constants(text)

    # ISa
    result_dict["ISa"] = _parse_ISa(text)

    # Wilson B-factor
    result_dict["B"] = _parse_wilson_B(text)

    # SUMMARY OF DATA REDUCTION table
    tbl = _parse_summary_table(text)
    result_dict["table1_text"] = tbl.get("text", "")
    result_dict["table1"] = tbl.get("rows", [])

    if tbl.get("total"):
        total = tbl["total"]
        ordered = []
        for k in [
            "OBSERVED",
            "UNIQUE",
            "POSSIBLE",
            "COMPLETE",
            "I/SIGMA",
            "CHI-test",
            "Rmrgd-F",
            "CC(1/2)",
            "NFREE",
            "Zano",
            "N-",
            "N+",
        ]:
            ordered.append(total.get(k))
        result_dict["table1_total"] = ordered

    # Resolution cutoffs using the same interpolation + LDS + rounding as xds_parsers.py
    result_dict["resolution_based_on_isigma"] = _interpolated_resolution_from_rows(
        result_dict["table1"], using_key="I/SIGMA", target_value=ISIGMA_TARGET
    )
    result_dict["resolution_based_on_cchalf"] = _interpolated_resolution_from_rows(
        result_dict["table1"], using_key="CC(1/2)", target_value=CC_HALF_TARGET
    )

    return result_dict


def main():
    wdir = Path(
        "/qp2/image_viewer/plugins/nxds/B1_ras_run1_R7_master"
    )
    spot_counts = parse_colspot_lp(wdir / "COLSPOT.LP", output_json="spot_counts.json")
    spot_details = parse_spot_nxds(wdir / "SPOT.nXDS", output_json="spot_details.json")

    indexing_results = parse_nxds_idxref_log(
        wdir / "IDXREF.LP", output_json="nxds_idxref_log.json"
    )

    combined_data = merge_dicts(spot_counts, spot_details)
    combined_data = merge_dicts(combined_data, indexing_results)

    with open("combined_data.json", "w") as f:
        json.dump(combined_data, f, indent=2)


if __name__ == "__main__":
    main()
