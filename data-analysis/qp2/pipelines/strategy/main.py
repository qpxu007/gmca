#!/usr/bin/env python3

from __future__ import annotations

import sys
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Union, Literal, Any

# Assumes these two modules are importable in the environment
from qp2.pipelines.strategy.mosflm.mosflm_strategy import (
    run_strategy as run_mosflm_strategy,
)  # returns final strategy dict
from qp2.pipelines.strategy.xds.xds_strategy import (
    run_xds_strategy,
)  # returns results dict including idxref/xplan
from qp2.xio.proc_utils import determine_proc_base_dir, extract_master_prefix

Program = Literal["mosflm", "xds"]


@dataclass
class UnifiedStrategyResult:
    software: str
    spacegroup_symbol: Optional[str]
    spacegroup_number: Optional[Union[int, str]]
    unitcell: Optional[Union[str, List[float]]]
    osc_start: Optional[float]
    osc_end: Optional[float]
    osc_delta: Optional[float]
    completeness_native: Optional[float]
    completeness_anomalous: Optional[float]
    mosaicity: Optional[float]
    distance: Optional[float]
    score: Optional[float]
    resolution_from_spots: Optional[float]
    n_spots: Optional[int]
    index_table: Optional[str]
    workdir: str
    solvent_content: Optional[float]
    estimated_asu_content_aa: Optional[int]
    raw: dict  # original payload

    def to_dict(self) -> dict:
        return asdict(self)


def _map_from_mosflm(strategy: dict, workdir: str) -> UnifiedStrategyResult:
    # MOSFLM final strategy dict fields:
    # "startAngle","endAngle","osc","mosaic","nativeCompleteness","anomalousCompletenes",
    # "spacegroup","unitcell","distance","score","edge_resol", etc.
    osc_start = _coerce_float(strategy.get("startAngle"))
    osc_end = _coerce_float(strategy.get("endAngle"))
    osc_delta = _coerce_float(strategy.get("osc"))
    completeness_native = _coerce_float(strategy.get("nativeCompleteness"))
    completeness_anomalous = _coerce_float(strategy.get("anomalousCompletenes"))
    mosaicity = _coerce_float(strategy.get("mosaic"))
    distance = _coerce_float(strategy.get("distance"))
    score = _coerce_float(strategy.get("score"))
    solvent_content = _coerce_float(strategy.get("solvent_content"))
    asu_content = _coerce_int(strategy.get("estimated_asu_content_aa"))

    return UnifiedStrategyResult(
        software="MOSFLM",
        spacegroup_symbol=(
            str(strategy.get("spacegroup"))
            if strategy.get("spacegroup") is not None
            else None
        ),
        spacegroup_number=None,
        unitcell=(
            str(strategy.get("unitcell"))
            if strategy.get("unitcell") is not None
            else None
        ),
        osc_start=osc_start,
        osc_end=osc_end,
        osc_delta=osc_delta,
        completeness_native=completeness_native,
        completeness_anomalous=completeness_anomalous,
        mosaicity=mosaicity,
        distance=distance,
        score=score,
        resolution_from_spots=None,  # not present in final return
        n_spots=None,  # not present in final return
        index_table=None,  # not present in final return
        solvent_content=solvent_content,
        estimated_asu_content_aa=asu_content,
        workdir=workdir,
        raw=dict(strategy or {}),
    )


def _map_from_xds(results: dict, workdir: str) -> UnifiedStrategyResult:
    idxref = results.get("idxref", {}) or {}
    xplan = results.get("xplan", {}) or {}
    matthews = results.get("matthews", {}) or {}

    osc_start = _coerce_float(xplan.get("xplan_starting_angle"))
    total_rot = _coerce_float(xplan.get("xplan_total_rotation"))
    osc_end = (
        (osc_start + total_rot)
        if (osc_start is not None and total_rot is not None)
        else None
    )
    osc_delta = _coerce_float(idxref.get("max_osc_range"))
    completeness_native = _coerce_float(xplan.get("xplan_completeness"))

    mosaicity = _coerce_float(idxref.get("mosaicity"))
    spacegroup_number = idxref.get("auto_index_spacegroup")
    unitcell = idxref.get("auto_index_unitcell")
    score = _coerce_float(results.get("screen_score"))

    res_from_spots = _coerce_float(results.get("spot_res"))
    distance = _coerce_float(results.get("detectordistance", 300))

    n_spots = _coerce_int(results.get("n_spots"))
    solvent_content = _coerce_float(matthews.get("solvent"))
    asu_content = _coerce_int(matthews.get("asu_content"))

    # If parse_idxref_strategy appended a human-readable table, it should be in idxref or results
    index_table = None
    if isinstance(idxref.get("index_table_candidates"), list):
        try:
            rows = idxref.get("index_table_candidates") or []
            index_table = "\n".join(
                " ".join(map(str, r)) for r in rows if isinstance(r, (list, tuple))
            )
        except Exception:
            index_table = None

    return UnifiedStrategyResult(
        software="XDS",
        spacegroup_symbol=None,  # symbol derivation not exposed in results without extra mapping
        spacegroup_number=spacegroup_number,
        unitcell=unitcell,
        osc_start=osc_start,
        osc_end=osc_end,
        osc_delta=osc_delta,
        completeness_native=completeness_native,
        completeness_anomalous=None,
        mosaicity=mosaicity,
        distance=distance,
        score=score,
        resolution_from_spots=res_from_spots,
        n_spots=n_spots,
        index_table=index_table,
        solvent_content=solvent_content,
        estimated_asu_content_aa=asu_content,
        workdir=workdir,
        raw=dict(results or {}),
    )


def _coerce_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        xf = float(x)
        if xf != xf:  # NaN
            return None
        return xf
    except Exception:
        return None


def _coerce_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def run_strategy(
        program: Program,
        multi_master_map: Dict[str, List[int]],
        workdir: Optional[str] = None,
        molsize: Optional[int] = None,
        pipeline_params: Optional[dict] = None,
) -> UnifiedStrategyResult:
    """
    Run MOSFLM or XDS strategy behind a single interface and return a normalized result.
    """
    if workdir is None or workdir == ".":
        master_file = next(iter(multi_master_map.keys()))
        prefix = extract_master_prefix(master_file)
        user_root = (
            pipeline_params.get("processing_common_proc_dir_root")
            if pipeline_params
            else None
        )
        base = determine_proc_base_dir(user_root, master_file)
        workdir = str(base / f"{program}_strategy" / prefix)

    if program == "mosflm":
        final = run_mosflm_strategy(
            multi_master_map,
            workdir=workdir,
            molsize=molsize,
            pipeline_params=pipeline_params,
        )
        return _map_from_mosflm(final or {}, workdir)
    elif program == "xds":
        results = run_xds_strategy(
            multi_master_map,
            workdir=workdir,
            molsize=molsize,
            pipeline_params=pipeline_params,
        )
        return _map_from_xds(results or {}, workdir)
    else:
        raise ValueError("program must be 'mosflm' or 'xds'")


def _get_image_set_string(run_map: dict[str, list[int]]) -> str:
    """Creates a descriptive imageSet string like 'prefix1:1,91' or 'prefix1:1-prefix2:1'."""
    from pathlib import Path

    parts = []
    for master, frames in run_map.items():
        prefix = Path(master).stem.replace("_master", "")
        if frames:
            frame_str = ",".join(map(str, frames))
            parts.append(f"{frame_str}:{prefix}")

    if not parts:
        return "N/A"

    # Use a different joiner for single vs multiple datasets for clarity
    joiner = "-" if len(parts) > 1 else ""
    return joiner.join(parts)


def main():
    import argparse
    import json
    import os
    from pathlib import Path
    import concurrent.futures
    import hashlib

    def _coerce_to_mapping(multi_master_input):
        """
        Accepts:
          - str: single master file -> {abs_path: [1]}
          - list[str]: list of master files -> {abs_path: [1], ...}
          - dict[str, list[int]]: pass-through after validation; [] becomes [1]
        """
        if isinstance(multi_master_input, str):
            return {str(Path(multi_master_input).resolve()): [1]}
        if isinstance(multi_master_input, (list, tuple)):
            return {str(Path(p).resolve()): [1] for p in multi_master_input}
        if isinstance(multi_master_input, dict):
            norm = {}
            for k, v in multi_master_input.items():
                if not isinstance(k, str) or not isinstance(v, (list, tuple)):
                    raise ValueError("Mapping must be dict[str, list[int]].")
                vv = [1] if len(v) == 0 else v
                if not all(isinstance(x, int) and x >= 1 for x in vv):
                    raise ValueError(f"Invalid image numbers for {k}: {v}")
                norm[str(Path(k).resolve())] = list(vv)
            return norm
        raise ValueError("Input must be a str, list[str], or dict[str, list[int]].")

    def _stem(p: str) -> str:
        # Basename plus short hash of absolute path to avoid collisions
        s = Path(p).stem
        h = hashlib.sha1(str(Path(p).resolve()).encode()).hexdigest()[:6]
        return f"{s}__{h}"

    def _solo_dir(root: str, stem: str) -> str:
        return os.path.join(root, "solo", stem)

    def _combined_dir(root: str, stems: list[str]) -> str:
        safe = "__".join(sorted(stems))
        return os.path.join(root, "combined", safe)

    ap = argparse.ArgumentParser(
        description="Unified strategy CLI for MOSFLM or XDS with flexible inputs"
    )
    ap.add_argument(
        "mapping",
        nargs="?",
        help='JSON like: {"path/to/master1.h5":[1], "path/to/master2.h5":[1,91]} (omit when using --masters)',
    )
    ap.add_argument(
        "--masters",
        nargs="+",
        help="One or more HDF5 master files; defaults to using image 1 for each.",
    )
    ap.add_argument(
        "--frames",
        nargs="+",
        action="append",
        metavar="MASTER:FRAMES",
        help="Specify frames for a master file, e.g., --frames file1.h5:1,91 --frames file2.h5:20-30. "
             "This overrides --masters and mapping JSON. Can be specified multiple times.",
    )

    ap.add_argument(
        "--program",
        choices=["mosflm", "xds"],
        required=True,
        help="Which backend to run.",
    )
    ap.add_argument(
        "--workdir",
        default=None,
        help="Root work directory; defaults to /PROCESSING/.../strategy if not provided.",
    )
    # Execution plan controls
    ap.add_argument(
        "--enable_single",
        action="store_true",
        help="Run per-dataset strategies in subdirs and, if >1 dataset, a combined run too.",
    )
    ap.add_argument(
        "--parallel",
        action="store_true",
        help="Run planned scopes concurrently (solo and combined) in separate subdirs.",
    )

    # Optional metadata
    ap.add_argument("--username", default=os.getenv("USER"))
    ap.add_argument("--sampleName")
    ap.add_argument("--esaf_id", type=int)
    ap.add_argument("--pi_id", type=int)
    ap.add_argument("--primary_group")
    ap.add_argument(
        "--molsize",
        type=int,
        default=None,
        help="Molecule size (residues) for Matthews coefficient.",
    )
    args = ap.parse_args()

    mapping = None
    if args.frames:
        mapping = {}
        # Flatten the list of lists from action="append"
        frame_specs = [item for sublist in args.frames for item in sublist]
        for spec in frame_specs:
            parts = spec.split(":", 1)
            if len(parts) != 2:
                print(
                    f"Error: Invalid --frames format. Expected MASTER:FRAMES, got '{spec}'",
                    file=sys.stderr,
                )
                sys.exit(2)

            master_file, frame_str = parts
            frames = []
            try:
                # Parse comma-separated numbers and ranges (e.g., "1,5,10-15")
                for part in frame_str.split(","):
                    if "-" in part:
                        start, end = map(int, part.split("-"))
                        frames.extend(range(start, end + 1))
                    else:
                        frames.append(int(part))
                mapping[master_file] = sorted(
                    list(set(frames))
                )  # Ensure unique, sorted frames
            except ValueError:
                print(
                    f"Error: Could not parse frame numbers from '{frame_str}'",
                    file=sys.stderr,
                )
                sys.exit(2)
        # Coerce to final validated format (absolute paths, etc.)
        mapping = _coerce_to_mapping(mapping)

    elif args.masters:
        mapping = _coerce_to_mapping(args.masters)

    elif args.mapping:
        try:
            maybe = json.loads(args.mapping)
            mapping = _coerce_to_mapping(maybe)
        except Exception:
            mapping = _coerce_to_mapping(args.mapping)
    else:
        print("Error: provide mapping JSON or --masters.", file=sys.stderr)
        sys.exit(2)

    # Resolve default workdir and create it
    if not args.workdir:
        master_file = next(iter(mapping.keys()))
        prefix = extract_master_prefix(master_file)
        base = determine_proc_base_dir(None, master_file)
        args.workdir = str(base / f"{args.program}_strategy" / prefix)
    os.makedirs(args.workdir, exist_ok=True)

    # Pipeline metadata
    pipeline_params = {
        "username": args.username,
        "sampleName": args.sampleName,
        "esaf_id": args.esaf_id,
        "pi_id": args.pi_id,
        "primary_group": args.primary_group,
        "workdir": args.workdir,
    }
    pipeline_params = {k: v for k, v in pipeline_params.items() if v is not None}

    # Build execution plans
    masters = list(mapping.keys())
    stems = [_stem(m) for m in masters]

    plans = []  # (label, run_map, subdir)
    combined_run_dir = _combined_dir(args.workdir, stems)

    if args.enable_single:
        # If enabled, add a plan for each individual master file
        for i, m in enumerate(masters):
            plans.append(("single", {m: mapping[m]}, _solo_dir(args.workdir, stems[i])))

        # If there's more than one master, also add the combined plan
        if len(masters) > 1:
            plans.append(("combined", mapping, combined_run_dir))
    else:
        # If not enabled, the *only* plan is the run with all masters in the combined directory
        plans.append(("combined", mapping, combined_run_dir))

    executions = []

    def _run_plan(label, run_map, subdir):
        os.makedirs(subdir, exist_ok=True)
        pp = dict(pipeline_params)
        pp["workdir"] = subdir
        pp["imageSet"] = _get_image_set_string(run_map)
        res = run_strategy(
            program=args.program,
            multi_master_map=run_map,
            workdir=subdir,
            molsize=args.molsize,
            pipeline_params=pp,
        )
        # res is a dataclass; use its to_dict or asdict fallback
        return {
            "scope": label,
            "program": args.program,
            "workdir": subdir,
            "mapping": run_map,
            "result": res.to_dict() if hasattr(res, "to_dict") else asdict(res),
        }

    max_workers = min(len(plans), 8)

    if args.parallel and len(plans) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_run_plan, *p) for p in plans]
            for fut in concurrent.futures.as_completed(futures):
                executions.append(fut.result())
    else:
        for p in plans:
            executions.append(_run_plan(*p))

    print(json.dumps({"executions": executions}, indent=2))


if __name__ == "__main__":
    main()
