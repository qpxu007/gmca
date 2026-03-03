# mosflm_parsers.py
# Standalone, single-pass parsers for MOSFLM logs.

from __future__ import annotations

import math
import os
import re
from collections import namedtuple
from typing import Dict, List, Tuple
from typing import Sequence, Optional
import numpy as np

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

LATTICE = {
    "aP": (1, "Primitive Triclinic", [1], [1]),
    "mP": (2, "Primitive Monoclinic", [3], [3, 4]),
    "mC": (3, "C Centered Monoclinic", [5], [5]),
    "mI": (3, "C Centered Monoclinic", [5], [5]),
    "oP": (4, "Primitive Orthorhombic", [16], [16, 17, 18, 19]),
    "oC": (5, "C Centered Orthorhombic", [21], [21, 20]),
    "oF": (6, "F Centered Orthorhombic", [22], [22]),
    "oI": (7, "I Centered Orthorhombic", [23], [23, 24]),
    "tP": (
        8,
        "Primitive Tetragonal",
        [89, 75],
        [75, 76, 77, 78, 89, 90, 91, 92, 93, 94, 95, 96],
    ),
    "tI": (9, "I Centered Tetragonal", [97, 79], [79, 80, 97, 98]),
    "hP": (
        10,
        "Primitive Hexagonal",
        [177, 168, 150, 149, 143],
        [
            143,
            144,
            145,
            149,
            150,
            151,
            152,
            153,
            154,
            168,
            169,
            170,
            171,
            172,
            173,
            177,
            178,
            179,
            180,
            181,
            182,
        ],
    ),
    "hR": (11, "Primitive Rhombohedral", [155, 146], [146, 155]),
    "cP": (12, "Primitive Cubic", [207, 195], [195, 198, 207, 208, 212, 213]),
    "cF": (13, "F Centered Cubic", [209, 196], [196, 209, 210]),
    "cI": (14, "I Centered Cubic", [211, 197], [197, 199, 211, 214]),
}


def similar_cell(cell1, cell2, epsilon=0.015):
    if len(cell1) != 6 or len(cell2) != 6:
        return False

    cell1 = [float(e) for e in cell1]
    cell2 = [float(e) for e in cell2]
    return all(
        abs(cell1[i] - cell2[i]) / min(cell1[i], cell2[i]) <= epsilon for i in range(6)
    )


# Exported lightweight container compatible with existing code expectations
MosflmSolution = namedtuple(
    "MosflmSolution", "SolutionNo Penalty sdxy Spacegroup Refined_cell"
)

# ---------- Utilities ----------

_number_rx = re.compile(r"[-+]?\d*\.\d+|[-+]?\d+")


def get_numbers(text_line: str) -> List[float]:
    if isinstance(text_line, (int, float)):
        return [float(text_line)]
    if not isinstance(text_line, str):
        return []
    return [float(x) for x in _number_rx.findall(text_line)]


def _calculate_edge_and_corner_res(params: dict) -> dict:
    """
    Compute resolutions at the detector boundary:
      - corner_res: using the farthest corner from the beam center (best achievable on the panel)
      - edge_res: using the farthest edge point along x/y (i.e., to the detector edges, excluding diagonals)

    Expected units:
      det_dist in mm, pixel_size in mm, wavelength in Å, beam_x/beam_y in pixels, nx/ny in pixels.
    """
    dist_mm = float(params.get("det_dist", 100.0))
    wl_a = float(params.get("wavelength", 1.0))  # Å (not meters)
    px_size_mm = float(params.get("pixel_size", 0.075))  # mm
    nx = int(params.get("nx", 1024))
    ny = int(params.get("ny", 1024))
    beam_x_px = float(params.get("beam_x", nx / 2))
    beam_y_px = float(params.get("beam_y", ny / 2))

    if dist_mm <= 0:
        return {"corner_res": 100.0, "edge_res": 100.0}

    # Helper: d-spacing from a given panel radius (mm)
    def d_from_radius(radius_mm: float) -> float:
        if radius_mm <= 0:
            return 100.0
        two_theta = math.atan(radius_mm / dist_mm)
        denom = math.sin(two_theta / 2.0)
        return wl_a / (2.0 * denom) if denom != 0 else 100.0

    # 1) Corner radius (pixels): max distance from beam center to any corner
    corners = [(0, 0), (nx, 0), (0, ny), (nx, ny)]
    r_corner_px_sq = max(
        (x - beam_x_px) ** 2 + (y - beam_y_px) ** 2 for x, y in corners
    )
    r_corner_mm = math.sqrt(r_corner_px_sq) * px_size_mm

    # 2) Edge radius (pixels): farthest point on the rectangle boundary along x/y (exclude diagonals)
    #    This is the min distance from the beam center to the nearest of each pair of opposite edges.
    #    Horizontal reach to edges (left/right), vertical reach to edges (bottom/top).
    reach_x_px = max(
        beam_x_px, nx - beam_x_px
    )  # farthest horizontal distance to an edge
    reach_y_px = max(beam_y_px, ny - beam_y_px)  # farthest vertical distance to an edge
    r_edge_px = max(reach_x_px, reach_y_px)
    r_edge_mm = r_edge_px * px_size_mm

    corner_res = d_from_radius(r_corner_mm)
    edge_res = d_from_radius(r_edge_mm)

    return {"corner_res": corner_res, "edge_res": edge_res}


def _calculate_distance_for_res(
    params: dict, max_res: float, default_distance: float = 350.0, use_edge=True
) -> float:
    wl_a = float(params.get("wavelength", 1.0e-10))  # m -> Å
    px_size_mm = float(params.get("pixel_size", 0.075))
    nx = int(params.get("nx", 1024))
    ny = int(params.get("ny", 1024))
    beam_x_px = float(params.get("beam_x", nx / 2))
    beam_y_px = float(params.get("beam_y", ny / 2))
    radius_mm = None
    if use_edge:
        # recommend distance based on edge, not corner
        reach_x_px = max(
            beam_x_px, nx - beam_x_px
        )  # farthest horizontal distance to an edge
        reach_y_px = max(
            beam_y_px, ny - beam_y_px
        )  # farthest vertical distance to an edge
        r_edge_px = max(reach_x_px, reach_y_px)
        r_edge_mm = r_edge_px * px_size_mm
        radius_mm = r_edge_mm

    else:
        corners = [(0, 0), (nx, 0), (0, ny), (nx, ny)]
        max_dist_px_sq = max(
            (x - beam_x_px) ** 2 + (y - beam_y_px) ** 2 for x, y in corners
        )
        radius_mm = math.sqrt(max_dist_px_sq) * px_size_mm

    try:
        theta = math.asin(wl_a / (2.0 * max_res))
        return round(radius_mm / math.tan(2.0 * theta), 0)
    except (ValueError, ZeroDivisionError):
        return default_distance


def _screening_score(resol, rms, mosaic, mosaic_penalty=1.0):
    # score as calculated in the webice
    try:
        score = (
            1.0
            - 0.7 * math.exp(-4.0 / float(resol))
            - 1.5 * float(rms)
            - 0.2 * float(mosaic) * float(mosaic_penalty)
        )
        score = round(score, 3)
    except Exception:
        logger.warning("Failed to calculate score screening score.")
        score = 0

    return score


def _calculate_phi(params: dict, image_number: int) -> float:
    start_phi = float(params.get("omega_start", 0.0))
    osc_range = float(params.get("omega_range", 0.1))
    return start_phi + (image_number - 1) * osc_range


def _get_osc(params: dict) -> float:
    return float(params.get("omega_range", 0.1))


def _median(vals):
    a = sorted(vals)
    n = len(a)
    if n == 0:
        return float("nan")
    m = n // 2
    return a[m] if n % 2 else 0.5 * (a[m - 1] + a[m])


def _mad(vals, center=None):
    if not vals:
        return 0.0
    c = _median(vals) if center is None else center
    return _median([abs(v - c) for v in vals])


def determine_penalty_cutoff(
    values: Sequence[float], z: float = 3.0, eps: float = 1e-12
) -> Optional[float]:
    """
    Return a cutoff between two adjacent values that maximizes group separation without
    explicit size constraints. Accepts ascending or descending input.
    """
    n = len(values)
    if n == 0:
        return None
    if n == 1:
        return float(values[0])

    # Work ascending for stable gap computation
    asc = sorted(values)
    diffs = [asc[i + 1] - asc[i] for i in range(n - 1)]  # >=0

    # If all gaps ~0, split in the middle
    if max(diffs) <= 0:
        k = (n // 2) - 1
        return 0.5 * (asc[k] + asc[k + 1])

    # Robust candidate set
    med = _median(diffs)
    mad = _mad(diffs, center=med)
    if mad <= eps:
        # Degenerate: use all indices achieving the maximum gap (ties possible)
        maxgap = max(diffs)
        candidates = [i for i, d in enumerate(diffs) if abs(d - maxgap) <= eps]
    else:
        T = (0.0 if math.isnan(med) else med) + z * mad
        candidates = [i for i, d in enumerate(diffs) if d >= T]
        if not candidates:
            maxgap = max(diffs)
            candidates = [i for i, d in enumerate(diffs) if abs(d - maxgap) <= eps]

    # Tie-break by maximizing between-group variance (Otsu/Fisher)
    best_k, best_J = None, -1.0
    prefix_sum = [0.0]
    for v in asc:
        prefix_sum.append(prefix_sum[-1] + v)
    total_sum = prefix_sum[-1]

    for k in candidates:
        nL, nR = k + 1, n - (k + 1)
        if nL == 0 or nR == 0:
            continue
        muL = prefix_sum[k + 1] / nL
        muR = (total_sum - prefix_sum[k + 1]) / nR
        J = nL * nR * (muL - muR) * (muL - muR)
        # Prefer larger J; if tied, prefer the rightmost split to separate long low cluster
        if J > best_J or (abs(J - best_J) <= eps and (best_k is None or k > best_k)):
            best_J, best_k = J, k

    if best_k is None:
        # Fallback: rightmost index with max gap
        maxgap = max(diffs)
        best_k = max(i for i, d in enumerate(diffs) if abs(d - maxgap) <= eps)

    return 0.5 * (asc[best_k] + asc[best_k + 1])


def iter_lines(path: str):
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            yield line.rstrip("\n")


# ---------- Findspots (single-pass) ----------


def parse_findspots_log(log_path: str) -> Dict:
    """
    Single-pass aggregation of per-image spot statistics from a combined findspots log.
    Returns a dict with totals and weighted resolution for downstream use.
    """
    spot_stat_raw: Dict[str, Dict] = {}
    current_key: Optional[str] = None

    for line in iter_lines(log_path):
        if "image FILENAME:" in line:
            # Example tail token is the filename; robustly take the last whitespace token
            current_key = os.path.split(line.strip())[-1]
            if current_key not in spot_stat_raw:
                spot_stat_raw[current_key] = dict(
                    nspots=None,
                    resol1=None,
                    resol2=None,
                    nspots_ice=None,
                    spotsize=None,
                    threshold_autoindex=None,
                )
        elif current_key:
            if "highest resolution is" in line:
                nums = get_numbers(line)
                spot_stat_raw[current_key]["resol1"] = nums[-1] if nums else None
            elif "99% have resolution less than" in line:
                nums = get_numbers(line)
                spot_stat_raw[current_key]["resol2"] = nums[-1] if nums else None
            elif "Number of spots excluded from possible ice rings:" in line:
                nums = get_numbers(line)
                spot_stat_raw[current_key]["nspots_ice"] = (
                    int(nums[-1]) if nums else None
                )
            elif "Based on a spot size of" in line:
                nums = get_numbers(line)
                spot_stat_raw[current_key]["spotsize"] = (
                    "x".join(map(str, nums)) if nums else None
                )
            elif "I/sig(I) threshold for autoindexing set to:" in line:
                nums = get_numbers(line)
                spot_stat_raw[current_key]["threshold_autoindex"] = (
                    nums[-1] if nums else None
                )
            elif "spots were written to file" in line:
                nums = get_numbers(line)
                spot_stat_raw[current_key]["nspots"] = int(nums[-1]) if nums else 0
                current_key = None
            elif "NO SPOTS have been found on image" in line:
                spot_stat_raw[current_key]["nspots"] = 0
                current_key = None

    logger.debug(f"raw spots stat: {spot_stat_raw}")
    images_keys = list(spot_stat_raw.keys())
    out = dict(software="mosflm", state="SPOT", n_ice_rings="NA")

    nspots_list, nspots_ice_list = [], []
    sum1 = sum2 = 0.0
    for k in images_keys:
        s = spot_stat_raw[k]
        if s.get("nspots") is not None:
            nspots_list.append(int(s["nspots"]))
            if s.get("resol2") is not None:
                try:
                    sum1 += int(s["nspots"]) * float(s["resol2"])
                    sum2 += int(s["nspots"])
                except Exception:
                    pass
        if s.get("nspots_ice") is not None:
            nspots_ice_list.append(int(s["nspots_ice"]))
    out["n_spots"] = sum(nspots_list) if nspots_list else 0
    out["n_spots_ice"] = sum(nspots_ice_list) if nspots_ice_list else 0
    out["resolution_from_spots"] = round(sum1 / sum2, 2) if sum2 > 0 else "NA"
    out["details"] = spot_stat_raw
    return out


def parse_testgen(log_path: str) -> Optional[Dict[str, float]]:
    """
    Parse the 'Phi start / Phi end / no of images / oscillation angle / %age overlaps / %age fulls'
    table and return a dict with those fields if found, else None.
    Robust to spacing and alignment differences.
    """
    header_seen = False
    header_rx = re.compile(
        r"phi\s+start\s+phi\s+end\s+no\s+of\s+images\s+oscillation\s+angle\s+%age\s+overlaps\s+%age\s+fulls",
        re.I,
    )

    for line in iter_lines(log_path):
        if not header_seen and header_rx.search(line):
            header_seen = True
            continue
        if header_seen:
            if not line.strip():
                # end of section without data
                return None
            nums = get_numbers(line)
            # Expect at least 6 numbers in order shown in header
            if len(nums) >= 6:
                return dict(
                    phi_start=float(nums[0]),
                    phi_end=float(nums[1]),
                    n_images=int(round(nums[2])),
                    osc_angle=float(nums[3]),
                    pct_overlaps=float(nums[4]),
                    pct_fulls=float(nums[5]),
                )
            # If a non-numeric line occurs after header, keep scanning until blank
    return None


def _parse_spt_file(spt_path: str) -> Optional[np.ndarray]:
    """
    Efficiently parses a MOSFLM .spt file by reading lines, removing the
    header and footer, and then using np.loadtxt on the clean data.
    Returns an array of (x_mm, y_mm) coordinates.
    """    
    if not os.path.exists(spt_path):
        return None
    try:
        # Skip header (3 lines) and footer (2 lines)
        # Columns are: X, Y, ?, ?, Intensity, Sigma
        coords = np.loadtxt(spt_path, skiprows=3, usecols=(0, 1))
        # The file might be empty except for header/footer
        if coords.ndim == 1 and coords.size == 2:  # single spot case
            coords = coords.reshape(1, 2)
        return coords[:-2] # skipp last two lines
    except Exception as e:
        logger.error(f"Failed to parse .spt file {spt_path}: {e}")
        return None


def parse_autoindex_and_strategy(
    log_path: str,
    penalty_cutoff: float = 100.0,
    sd_cutoff: float = 0.25,
    score_cutoff: float = 100.0,
    dedup: bool = True,
) -> Tuple[str, List[List[str]], Optional[MosflmSolution], Dict]:
    header_pat = re.compile(r"\bNo\s+PENALTY\b.*\bgamma\b", re.I)
    anom_head_pat = re.compile(
        r"\bOptimum rotation gives\s+([-\d\.]+)\s*% of anomalous pairs", re.I
    )
    anom_stats_pat = re.compile(
        r"Completeness of anomalous pairs is\s+([-\d\.]+)\s*%", re.I
    )
    native_stats_pat = re.compile(
        r"This is\s+([-\d\.]+)\s+percent of the unique data for this spacegroup", re.I
    )

    suggest_pat = re.compile(r"Suggested Solution:\s*#?\s*(\d+)\s+([A-Za-z0-9]+)")
    penalty_line_pat = re.compile(r"^\s*penalty:\s*([-\d\.]+)", re.I)
    cell_line_pat = re.compile(r"^\s*cell:\s*(.+)$", re.I)
    reg_cell_line_pat = re.compile(r"^\s*regularized\s+cell:\s*(.+)$", re.I)
    refine_pat = re.compile(
        r"Refining solution\s+#?\s*(\d+).+?\swith\s+([A-Za-z0-9]+)\s+symmetry", re.I
    )
    final_cell_pat = re.compile(r"Final cell \(after refinement\) is", re.I)
    sdxy_pat = re.compile(r"final sd in spot positions is", re.I)
    optimum_pat = re.compile(r"Optimum rotation gives", re.I)
    run_pat = re.compile(r"^\s*Run number", re.I)
    mult_pat = re.compile(r"^\s*Mean Multiplicity", re.I)
    from_deg_pat = re.compile(r"^\s*From .*degrees$", re.I)

    raw_table_lines: List[str] = []
    in_table = False
    candidates_rows: List[List[str]] = []

    slnNum_str: Optional[str] = None
    spg_str: Optional[str] = None
    penalty_val: Optional[str] = None
    refined_cell_str: str = "N/A"
    sdxy_val: str = "N/A"
    initial_cell_str: Optional[str] = None
    regularized_cell_str: Optional[str] = None

    strategy_mode = "unknown"
    anom_completeness = "NA"
    native_completeness = "NA"
    start_angle = "NA"
    end_angle = "NA"
    multiplicity_val = "NA"
    nrun_val = 0
    mosaic_val = None
    matrix_file = None
    optimum_window_remaining = -1
    native_completeness = "NA"

    seen_suggest_block = False

    for line in iter_lines(log_path):
        stripped = line.strip()

        # Table capture
        if not in_table and header_pat.search(line):
            in_table = True
            raw_table_lines.append(line)
            continue
        if in_table:
            if stripped.startswith("Refining solution") or stripped.startswith(
                "Suggested Solution:"
            ):
                in_table = False
            else:
                raw_table_lines.append(line)
                if "unrefined" not in line and "PENALTY" not in line:
                    fields = line.split()
                    if fields:
                        if len(fields) == 1 and candidates_rows:
                            # handling overflow, append additional spg candidates to the end of last row
                            candidates_rows[-1][-1] = (
                                candidates_rows[-1][-1] + "," + fields[0]
                            )
                        else:
                            candidates_rows.append(fields)

        # extract completeness: native, this is to handle when the mode is anomalous
        m = native_stats_pat.search(line)
        if m:
            try:
                native_completeness = float(m.group(1))
            except Exception:
                pass

        # redundant: anom_completeness
        m = anom_head_pat.search(line)
        if m:
            try:
                anom_completeness = float(m.group(1))
            except Exception:
                pass

        m = anom_stats_pat.search(line)
        if m:
            try:
                # Prefer explicit stats line over header if both present
                anom_completeness = float(m.group(1))
            except Exception:
                pass

        # Suggested solution block (capture number, spg, penalty, cells)
        m = suggest_pat.search(line)
        if m:
            slnNum_str, spg_str = m.group(1), m.group(2)
            seen_suggest_block = True
            continue
        if seen_suggest_block:
            p = penalty_line_pat.search(line)
            if p and penalty_val is None:
                penalty_val = p.group(1)
            c = cell_line_pat.search(line)
            if c and initial_cell_str is None:
                nums = get_numbers(c.group(1))
                if nums:
                    initial_cell_str = " ".join(str(round(float(n), 3)) for n in nums)
            r = reg_cell_line_pat.search(line)
            if r and regularized_cell_str is None:
                nums = get_numbers(r.group(1))
                if nums:
                    regularized_cell_str = " ".join(
                        str(round(float(n), 3)) for n in nums
                    )
            # End of suggest block when a blank or a non-indented section appears
            if stripped == "" or stripped.startswith("Mosflm has chosen solution"):
                seen_suggest_block = False

        # Refinement lines
        m = refine_pat.search(line)
        if m and (slnNum_str is None or spg_str is None):
            slnNum_str, spg_str = m.group(1), m.group(2)
        if final_cell_pat.search(line):
            nums = get_numbers(line)
            if nums:
                refined_cell_str = " ".join(str(round(float(n), 2)) for n in nums)
        if sdxy_pat.search(line):
            nums = get_numbers(line)
            if nums:
                sdxy_val = str(nums[0])

        # Strategy window
        if optimum_pat.search(line):
            if "anomalous" in line.lower():
                strategy_mode = "anomalous"
                vals = get_numbers(line)
                if vals:
                    anom_completeness = vals[0]
            elif "unique data" in line.lower():
                strategy_mode = "notanom"
                vals = get_numbers(line)
                if vals:
                    native_completeness = vals[0]
            optimum_window_remaining = 50
        if optimum_window_remaining > 0:
            optimum_window_remaining -= 1
            if run_pat.search(line):
                nrun_val += 1
            elif mult_pat.search(line):
                vals = get_numbers(line)
                if vals:
                    multiplicity_val = vals[0]
            elif from_deg_pat.search(line):
                vals = get_numbers(line)
                if len(vals) >= 2:
                    start_angle, end_angle = vals[0], vals[1]
            elif stripped == "" or stripped.startswith("===>"):
                optimum_window_remaining = 0

        # Mosaic and matrix
        if "The mosaicity has been estimated as" in line:
            vals = get_numbers(line)
            if vals:
                mosaic_val = vals[-1]
        if "===> matrix " in line or "===> newmat " in line:
            toks = stripped.split()
            if toks:
                matrix_file = toks[-1]

    penalties = []
    candidates_rows = [r for r in candidates_rows if len(r) == 12]

    for r in candidates_rows:
        try:
            penalties.append(float(r[1]))
        except Exception:
            continue

    cut = determine_penalty_cutoff(penalties)
    logger.info(f"suggested penalty cutoff: {cut}")

    def _row_is_candidate(fields: List[str]) -> bool:
        try:
            pen = float(fields[1])
            if len(fields) >= 11 and "unrefined" not in fields:
                sdcell = float(fields[2])
                # Keep if refined quality OR under robust penalty cutoff
                return (sdcell <= sd_cutoff) or (pen <= cut)
            # Unrefined rows rely on penalty only
            return pen <= cut
        except Exception:
            return False

    filtered = [r for r in candidates_rows if _row_is_candidate(r)]

    if dedup and filtered:
        lattice_idx = -7
        try:
            order = LATTICE
            filtered.sort(
                key=lambda r: (
                    -order.get(r[lattice_idx], (0,))[0],
                    float(r[1]) if len(r) > 1 else 1e9,
                )
            )
        except Exception:
            pass
        unique: List[List[str]] = []
        for r in filtered:
            try:
                lat = r[lattice_idx]
                c1 = [float(x) for x in r[-7:-1]]
            except Exception as e:
                logger.exception(e)
                unique.append(r)
                continue
            dup = False
            for u in unique:
                try:
                    same_lat = u[lattice_idx] == lat
                    c2 = [float(x) for x in u[-7:-1]]
                    similar = similar_cell(c1, c2)
                except Exception:
                    similar = False
                if same_lat and similar:
                    dup = True
                    break
            if not dup:
                unique.append(r)
        filtered = unique

    raw_table_text = "\n".join(raw_table_lines).rstrip("\n")
    # Prefer refined cell for the solution, but include parsed penalty
    penalty_out = penalty_val if penalty_val is not None else "N/A"
    solution = (
        MosflmSolution(
            slnNum_str if slnNum_str is not None else "NA",
            penalty_out,
            sdxy_val,
            spg_str if spg_str is not None else "NA",
            refined_cell_str,
        )
        if (slnNum_str and spg_str)
        else None
    )

    strategy = dict(
        anomalous_type=strategy_mode,
        startAngle=start_angle,
        endAngle=end_angle,
        anomalousCompletenes=anom_completeness,
        nativeCompleteness=native_completeness,
        nRun=nrun_val,
        mosaic=(mosaic_val if mosaic_val is not None else "NA"),
        multiplicity=multiplicity_val,
        matrix=(matrix_file or "autoindex.mat"),
        solution=solution,
    )
    # Optional: expose initial/regularized cells for QA
    if initial_cell_str:
        strategy["initialCell"] = initial_cell_str
    if regularized_cell_str:
        strategy["regularizedCell"] = regularized_cell_str
    logger.info(f"chosen solution: {solution}")
    return raw_table_text, filtered, solution, strategy


if __name__ == "__main__":
    log1 = "/tmp/mosflm_strategy/mosflm_findspots_E8_scr_90_master.log"
    # print(parse_findspots_log(log1))

    log1 = "/tmp/mosflm_strategy/mosflm_findspots_E8_scr_00_master.log"
    # print(parse_findspots_log(log1))

    log3 = "/tmp/mosflm_strategy/mosflm_testgen_sln9.log"

    # print(parse_testgen(log3))

    log2 = "/tmp/mosflm_strategy/mosflm_autoindex.log"
    parse_autoindex_and_strategy(log2)
    # pprint(parse_autoindex_and_strategy(log2))
