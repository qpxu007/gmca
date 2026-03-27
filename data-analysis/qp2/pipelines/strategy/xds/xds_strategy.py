import json
import os
from pathlib import Path
import shutil
from typing import Dict, List, Tuple, Optional

import numpy as np

from qp2.data_viewer.models import ScreenStrategyResults
from qp2.pipelines.gmcaproc.hdf5reader import HDF5Reader  # dataset object expected by XDS
from qp2.pipelines.utils.pipeline_tracker import PipelineTracker
from qp2.pipelines.gmcaproc.symmetry import Symmetry
from qp2.pipelines.gmcaproc.xds2 import XDS
from qp2.pipelines.utils.image_set import get_image_set_string
from qp2.pipelines.gmcaproc.xds_parsers import (
    parse_idxref_lp,
    parse_idxref_strategy,
    parse_xplan_lp,
    update_xparm_spacegroup_cell,
)
from qp2.image_viewer.utils.ring_math import radius_to_resolution
from qp2.log.logging_config import get_logger, setup_logging
from qp2.pipelines.strategy.mosflm.mosflm_parsers import (
    _calculate_edge_and_corner_res,
    _screening_score,
    _calculate_distance_for_res,
)
from qp2.utils.matthews_coef import run_matthews_coef
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.config.servers import ServerConfig
from qp2.xio.proc_utils import determine_proc_base_dir, extract_master_prefix

logger = get_logger(__name__)
setup_logging()


def _normalize_mapping(multi_master_map: Dict[str, List[int]]) -> Dict[str, List[int]]:
    """Ensure mapping is absolute paths and non-empty image lists (default to [1])."""
    normalized: Dict[str, List[int]] = {}
    for mpath, frames in multi_master_map.items():
        abs_mpath = str(Path(mpath).resolve())
        frames_list = list(frames) if frames else [1]
        if not all(isinstance(n, int) and n >= 1 for n in frames_list):
            raise ValueError(f"Invalid image numbers for {mpath}: {frames_list}")
        normalized[abs_mpath] = sorted(frames_list)
    return normalized


def _to_contiguous_ranges(frames: List[int]) -> List[str]:
    """Convert a sorted list of image numbers into contiguous SPOT_RANGE strings 'start end'."""
    if not frames:
        return []
    ranges: List[Tuple[int, int]] = []
    start = frames[0]
    prev = frames[0]
    for f in frames[1:]:
        if f == prev + 1:
            prev = f
        else:
            ranges.append((start, prev))
            start = f
            prev = f
    ranges.append((start, prev))
    return [f"{s} {e}" for s, e in ranges]


class XdsStrategy:
    """
    XDS multi-dataset strategy:
      1) First dataset: run JOB=XYCORR INIT COLSPOT; rename SPOT.XDS -> <master>.XDS and COLSPOT.LP -> <master>.LP
      2) Subsequent datasets: run JOB=COLSPOT; same renaming
      3) Merge all renamed *.XDS into SPOT.XDS
      4) Run JOB=IDXREF and parse IDXREF.LP
      5) Update XDS.INP with suggested SG and cell; run JOB=XPLAN and parse XPLAN.LP
    """

    def __init__(
        self,
        multi_master_map: Dict[str, List[int]],
        workdir: Optional[str] = None,
        use_slurm: bool = True,
        nproc: int = 1,
        njobs: int = 1,
        molsize: Optional[int] = None,
        pipeline_params: Optional[dict] = None,
        pipeline_status_id: Optional[int] = None,
    ):
        self.map = _normalize_mapping(multi_master_map)
        self.use_slurm = use_slurm
        self.nproc = nproc
        self.njobs = njobs
        self.molsize = molsize
        self.default_template = next(iter(self.map.keys()))

        if workdir is None or workdir == ".":
            # Intelligent determination
            master_basename = extract_master_prefix(self.default_template)
            user_root = (
                pipeline_params.get("processing_common_proc_dir_root")
                if pipeline_params
                else None
            )
            proc_base = determine_proc_base_dir(user_root, self.default_template)
            self.workdir = str(proc_base / "xds_strategy" / master_basename)
        else:
            self.workdir = os.path.abspath(workdir)

        os.makedirs(self.workdir, exist_ok=True)

        self._per_dataset_spot_files: List[str] = []
        self.idxref_lp_path = os.path.join(self.workdir, "IDXREF.LP")
        self.xplan_lp_path = os.path.join(self.workdir, "XPLAN.LP")
        self.combined_spot_path = os.path.join(self.workdir, "SPOT.XDS")
        self.combined_spot_path_bk = os.path.join(self.workdir, "SPOT.XDS.bk")
        self.summary_json_path = os.path.join(self.workdir, "xds_strategy.json")
        self.xparm_path = os.path.join(self.workdir, "XPARM.XDS")

        self.results = {
            "spot_files": [],
            "idxref": {},
            "xplan": {},
        }
        self._edge_res_cutoff = None
        self._corner_res_cutoff = None
        self._h5_params = {}
        self._spot_entries: List[Dict] = (
            []
        )  # each: {master, spot_file, omega_start, omega_range}
        self.spots = []
        self.spots_by_master: Dict[str, Dict[int, Dict[str, List]]] = {}
        self._z_map_to_master: List[Tuple[float, float, str, float]] = (
            []
        )  # (min_z, max_z, master_file, offset)
        self.screen_score = None

        if pipeline_params is None:
            pipeline_params = {}

        self._bluice_redis_key = pipeline_params.get("redis_key")
        self._redis_manager = pipeline_params.get("redis_manager")
        if self._redis_manager and self._bluice_redis_key:
            self._bluice_redis = self._redis_manager.get_bluice_connection()
        else:
            self._bluice_redis = (
                ServerConfig.get_bluice_redis_connection(pipeline_params.get("beamline"))
                if self._bluice_redis_key else None
            )

        # Derive sample name from master file stem if not provided
        sample_name = (
            pipeline_params.get("sampleName")
            or Path(self.default_template).stem.rsplit("_master", 1)[0]
        )

        pipeline_params.setdefault("sampleName", sample_name)
        pipeline_params.setdefault("imagedir", os.path.dirname(self.default_template))
        pipeline_params.setdefault("workdir", self.workdir)
        pipeline_params.setdefault(
            "logfile", os.path.join(self.workdir, "xds_strategy.log")
        )
        pipeline_params.setdefault("beamline", get_beamline_from_hostname())
        pipeline_params.setdefault("imageSet", get_image_set_string(self.map))
        pipeline_params.setdefault("datasets", json.dumps(list(self.map.keys())))

        # Get Redis host from central config
        redis_host = ServerConfig.get_redis_hosts().get("analysis_results", "10.20.103.67")

        self.tracker = PipelineTracker(
            pipeline_name="xds_strategy",
            run_identifier=self.default_template,
            initial_params=pipeline_params,
            result_mapper=self._get_sql_mapped_results,
            redis_config={"host": redis_host, "db": 0},
            existing_pipeline_status_id=pipeline_status_id,
            results_model=ScreenStrategyResults,
        )

    def _get_sql_mapped_results(self, results_dict: dict) -> dict:
        """
        Map internal XDS results to ScreenStrategyResults fields, providing sensible defaults.
        """
        idxref_info = results_dict.get("idxref", {})
        xplan_info = results_dict.get("xplan", {})
        matthews_info = results_dict.get("matthews", {})

        # Best lattice candidate and unit cell
        best_candidate = idxref_info.get("index_table_candidates", [])
        if best_candidate:
            bravais_lattice = best_candidate[0][2] if len(best_candidate[0]) > 2 else ""
            unitcell_list = best_candidate[0][-6:] if len(best_candidate[0]) >= 6 else []
            unitcell = " ".join(map(str, unitcell_list))
        else:
            bravais_lattice = ""
            unitcell = ""

        # Directory and images
        directory = os.path.dirname(self.default_template)
        default_images = self.map.get(self.default_template, [1])
        images = ",".join(str(i) for i in default_images) if default_images else ""

        # Detector distance
        det_dist = ""
        try:
            params = self._h5_params.get(self.default_template, {})
            if params and params.get("det_dist") is not None:
                det_dist = str(params.get("det_dist"))
        except Exception:
            det_dist = ""

        # State
        state = "STRATEGY" if xplan_info else "SPOT"

        # Spacegroup name and display text
        logger.debug(f"bravais lattice: {bravais_lattice}")
        try:
            spg_name = str(
                Symmetry.number_to_symbol(
                    Symmetry.get_lowest_spacegroup_number(bravais_lattice)
                )
                or ""
            )
        except Exception:
            spg_name = ""
        displaytext = f"{spg_name} {unitcell}".strip()

        # Strategy serialized
        try:
            strategy_text = json.dumps(xplan_info, default=str)
        except Exception:
            strategy_text = ""

        mapped = {
            # Identifiers / context
            "sampleName": self.tracker.initial_params.get("sampleName", ""),
            "directory": directory,
            "images": images,
            "software": "XDS",
            "state": state,
            "workdir": self.workdir,
            # Table content
            "index_table": "\n".join(
                " ".join(row) for row in idxref_info.get("index_table_candidates", [])
            )
            or "",
            "unitcell": unitcell,
            "bravais_lattice": bravais_lattice,
            "rmsd": str(idxref_info.get("spot_stddev", "")),
            "ice_rings": "",
            "resolution_from_spots": str(results_dict.get("spot_res", "")),
            "n_spots": str(results_dict.get("n_spots", "")),
            "n_spots_ice": "",
            "n_ice_rings": "",
            "avg_spotsize": "",
            "spacegroup": spg_name,
            "solution_number": "",
            "penalty": "",
            "mosaicity": str(idxref_info.get("mosaicity", "")),
            "score": str(results_dict.get("screen_score", "")),
            "resolution_from_integ": "",
            "warning": "",
            # Strategy details
            "anomalous": 0,
            "osc_start": str(xplan_info.get("xplan_starting_angle", "")),
            "osc_end": (
                str(
                    float(xplan_info.get("xplan_starting_angle", 0.0))
                    + float(xplan_info.get("xplan_total_rotation", 0.0))
                )
                if xplan_info
                else ""
            ),
            "osc_delta": str(idxref_info.get("max_osc_range", "")),
            "completeness_native": str(xplan_info.get("xplan_completeness", "")),
            "completeness_anomalous": "",
            "completeness_referencedata": "",
            "detectorwarning": "",
            "detectordistance": (
                str(results_dict.get("detectordistance", "")) or det_dist
                if det_dist
                else "350.0"
            ),
            "referencedata": "",
            "displaytext": displaytext,
            "xplanlog": "",
            "strategy": strategy_text,
            # Run meta
            "reprocess": 0,
            "solvent_content": str(matthews_info.get("solvent", "")),  # NEW
            "estimated_asu_content_aa": str(
                matthews_info.get("asu_content", "")
            ),  # NEW
            "export2run": 0,
            "pointgroup_choices": "",
        }
        return mapped

    def _make_xds(self, master_file: str) -> XDS:
        """Construct an XDS instance bound to the shared workdir for a given master file."""
        dataset = HDF5Reader(master_file)
        if master_file not in self._h5_params:
            self._h5_params[master_file] = dataset.get_parameters()

        if self._edge_res_cutoff is None:
            edge_corner_res = _calculate_edge_and_corner_res(
                self._h5_params[master_file]
            )
            self._edge_res_cutoff = edge_corner_res.get("edge_res")
            self._corner_res_cutoff = edge_corner_res.get("corner_res")
            logger.debug(
                f"edge resolution: {self._edge_res_cutoff}, res @corner {self._corner_res_cutoff}"
            )

        xds = XDS(
            dataset=dataset,
            proc_dir=self.workdir,
            use_slurm=self.use_slurm,
            nproc=self.nproc,
            njobs=self.njobs,
            strategy=False,
        )
        if self._edge_res_cutoff:
            xds.xds_inp["INCLUDE_RESOLUTION_RANGE"] = (
                f"50.0 {self._edge_res_cutoff:.2f}"
            )
        return xds

    def _rename_outputs(self, master_file: str):
        """Rename SPOT.XDS and COLSPOT.LP to <master>.XDS and <master>.LP, and record omega params from self._h5_params."""
        stem = Path(master_file).stem
        src_spot = os.path.join(self.workdir, "SPOT.XDS")
        src_lp = os.path.join(self.workdir, "COLSPOT.LP")
        dst_spot = os.path.join(self.workdir, f"{stem}.XDS")
        dst_lp = os.path.join(self.workdir, f"{stem}.LP")

        if os.path.exists(src_spot):
            try:
                if os.path.exists(dst_spot):
                    os.remove(dst_spot)
                os.rename(src_spot, dst_spot)
                self._per_dataset_spot_files.append(dst_spot)
                self.results["spot_files"].append(dst_spot)

                params = self._h5_params.get(master_file, {}) or {}
                omega_start = float(params.get("omega_start", 0.0))
                omega_range = float(params.get("omega_range", 0.0)) or 0.0
                self._spot_entries.append(
                    {
                        "master": master_file,
                        "spot_file": dst_spot,
                        "omega_start": omega_start,
                        "omega_range": omega_range if omega_range > 1e-4 else 1e-4,
                    }
                )

                logger.info(f"Saved per-dataset spot file: {dst_spot}")
            except Exception as e:
                logger.error(f"Failed to rename {src_spot} -> {dst_spot}: {e}")
        else:
            logger.warning(f"{src_spot} not found after COLSPOT for {stem}")

        if os.path.exists(src_lp):
            try:
                if os.path.exists(dst_lp):
                    os.remove(dst_lp)
                os.rename(src_lp, dst_lp)
                logger.info(f"Saved per-dataset COLSPOT log: {dst_lp}")
            except Exception as e:
                logger.error(f"Failed to rename {src_lp} -> {dst_lp}: {e}")
        else:
            logger.warning(f"{src_lp} not found after COLSPOT for {stem}")

    def _run_colspot_for(self, master_file: str, frames: List[int], first: bool):
        """Run XDS for COLSPOT (with XYCORR/INIT only for the first dataset)."""
        xds = self._make_xds(master_file)
        spot_ranges = _to_contiguous_ranges(frames)
        job_def = "XYCORR INIT COLSPOT" if first else "COLSPOT"
        xds.xds_inp.update({"JOB": job_def})
        if spot_ranges:
            xds.xds_inp["SPOT_RANGE"] = spot_ranges
        else:
            xds.xds_inp["SPOT_RANGE"] = ["1 1"]

        if self._edge_res_cutoff:
            xds.xds_inp["INCLUDE_RESOLUTION_RANGE"] = (
                f"50.0 {self._edge_res_cutoff:.2f}"
            )

        xds.generate_xds_inp()
        logger.info(
            f"Running XDS '{job_def}' for {Path(master_file).name} with SPOT_RANGE={spot_ranges}"
        )
        xds.run()
        self._rename_outputs(master_file)

    def _merge_spot_xds(self):
        """Merge all per-dataset *.XDS into SPOT.XDS, correcting Z by omega offset.
        Also builds a map of Z-value ranges to their source master file for later correlation.
        """
        if not self._spot_entries:
            logger.error("No per-dataset spot files found to merge.")
            return False

        valid = [e for e in self._spot_entries if e["omega_range"] not in (None, 0.0)]
        if not valid:
            logger.error("No valid entries with non-zero omega_range for merging.")
            return False

        valid.sort(key=lambda e: e["omega_start"])
        base_start = valid[0]["omega_start"]

        self._z_map_to_master.clear()

        reset_frame = len(self._spot_entries) > 1
        try:
            with open(self.combined_spot_path, "w") as fout:
                for entry in valid:
                    sf = entry["spot_file"]
                    master_file = entry["master"]
                    if not os.path.exists(sf):
                        logger.warning(f"Spot file missing, skipping: {sf}")
                        continue

                    offset = (entry["omega_start"] - base_start) / entry["omega_range"]

                    # Get the number of frames for this master to calculate the max z-value
                    frames_in_master = self.map.get(master_file, [1])
                    max_frame_num = max(frames_in_master) if frames_in_master else 1

                    # Define the Z-range for this master file in the merged file
                    # A small buffer is added to handle floating point comparisons
                    min_z = offset - 0.01
                    max_z = offset + max_frame_num + 0.01
                    self._z_map_to_master.append((min_z, max_z, master_file, offset))
                    logger.info(
                        f"Mapping Z-range [{min_z:.2f}, {max_z:.2f}] to {Path(master_file).name}"
                    )

                    # The _shift_spot_file method is now simplified
                    for line in self._shift_spot_file(
                        sf, offset, reset_frame=reset_frame
                    ):
                        fout.write(line)
                logger.info(
                    f"Merged SPOT.XDS written with Z-offset correction: {self.combined_spot_path}"
                )
            shutil.copy(self.combined_spot_path, self.combined_spot_path_bk)
            return True
        except Exception as e:
            logger.error(
                f"Failed to merge/correct spot files into {self.combined_spot_path}: {e}"
            )
            return False

    def _shift_spot_file(
        self, spot_path: str, offset: float, reset_frame=True
    ) -> List[str]:
        """
        Add `offset` to the 3rd numeric column (Z) of a SPOT.XDS-like file.
        Preserves non-numeric lines.
        Uses z=0.5 intentionally to treat all frames in a wedge as one block.
        """
        out_lines: List[str] = []
        try:
            with open(spot_path, "r") as fin:
                for raw in fin:
                    line = raw.rstrip("\n")
                    parts = line.split()
                    if len(parts) >= 3:
                        # Add to simple list for resolution estimation
                        self.spots.append([float(parts[0]), float(parts[1])])
                        try:
                            if reset_frame:
                                # INTENTIONAL: Use 0.5 to represent the block of frames
                                z = 0.5
                            else:
                                # Use the actual frame number from the file
                                z = float(parts[2])

                            parts[2] = f"{z + offset:.3f}"
                            out_lines.append(" ".join(parts) + "\n")
                            continue
                        except ValueError:
                            pass
                    out_lines.append(raw)
        except Exception as e:
            logger.warning(f"Failed to read/shift {spot_path}: {e}")
        return out_lines

    def _run_idxref_and_parse(self, master_file: str):
        """Run IDXREF and parse IDXREF.LP."""
        xds = self._make_xds(master_file)
        xds.xds_inp["JOB"] = "IDXREF"
        if "SPOT_RANGE" in xds.xds_inp:
            xds.xds_inp.pop("SPOT_RANGE", None)

        xds.generate_xds_inp()
        logger.info("Running XDS 'IDXREF' on merged SPOT.XDS")
        xds.run()

        if os.path.exists(self.idxref_lp_path):
            idxref_res = parse_idxref_lp(self.idxref_lp_path)
            self.results["idxref"] = idxref_res or {}
            logger.info("Parsed IDXREF.LP for auto-index results.")
            return idxref_res or {}
        else:
            logger.error(f"{self.idxref_lp_path} not found after IDXREF.")
            return {}

    def _parse_and_map_indexed_spots(self):
        """
        Parses the final, indexed SPOT.XDS file.
        Uses the pre-built Z-range map to assign all spots and indexed
        reflections back to their original master file and frame.
        """
        logger.info("Parsing and mapping indexed spots from final SPOT.XDS...")
        indexed_spot_path = os.path.join(self.workdir, "SPOT.XDS")
        if not os.path.exists(indexed_spot_path):
            logger.warning("Indexed SPOT.XDS not found. Cannot parse and map spots.")
            return

        unmapped_count = 0
        total_spots = 0

        try:
            with open(indexed_spot_path, "r") as f:
                for line in f:
                    parts = line.strip().split()
                    if not parts:
                        continue

                    try:
                        # Try to parse as a spot/reflection line
                        x, y, z = float(parts[0]), float(parts[1]), float(parts[2])
                        total_spots += 1

                        # Find the source master file using the Z-value
                        source_master = None
                        source_offset = 0.0
                        for min_z, max_z, master_file, offset in self._z_map_to_master:
                            if min_z <= z < max_z:
                                source_master = master_file
                                source_offset = offset
                                break

                        if not source_master:
                            unmapped_count += 1
                            continue

                        # Calculate original frame number. Since z=0.5 was used, this will always be 1.
                        original_frame_num = int((z - source_offset) + 0.5)

                        master_entry = self.spots_by_master.setdefault(
                            source_master, {}
                        )
                        frame_dict = master_entry.setdefault(
                            original_frame_num, {"spots_xds": [], "reflections_xds": []}
                        )

                        is_indexed = False
                        if len(parts) == 7:
                            h, k, l = int(parts[4]), int(parts[5]), int(parts[6])
                            is_indexed = not (h == 0 and k == 0 and l == 0)
                            if is_indexed:
                                frame_dict["reflections_xds"].append([h, k, l, x, y])

                        frame_dict["spots_xds"].append([x, y, 1 if is_indexed else 0])

                    except (ValueError, IndexError):
                        # Not a valid numeric spot line, ignore
                        continue

        except Exception as e:
            logger.error(
                f"Error during final spot parsing and mapping: {e}", exc_info=True
            )

        logger.info(
            f"Finished mapping spots. Total spots processed: {total_spots}. Unmapped: {unmapped_count}."
        )
        if unmapped_count > 0:
            logger.warning(
                f"{unmapped_count} spots could not be mapped back to a source master file."
            )

    def _run_xplan_with_suggested(self, master_file: str, idxref_res: Dict):
        """Update XDS.INP with suggested SG and cell; run XPLAN and parse."""
        auto_sg = idxref_res.get("auto_index_spacegroup", 0)
        auto_cell = idxref_res.get("auto_index_unitcell", [1, 1, 1, 90, 90, 90])

        if not auto_sg or not auto_cell:
            logger.warning(
                "Auto-index SG or cell not found; running XPLAN without update."
            )
        update_xparm_spacegroup_cell(
            self.xparm_path, int(auto_sg), [float(x) for x in auto_cell]
        )

        # spot_details = parse_spot_xds(self.combined_spot_path)
        # self.results.update(spot_details)

        xds = self._make_xds(master_file)
        omega_start = min(
            (float(v.get("omega_start", 0.0)) for v in self._h5_params.values())
        )
        xds.xds_inp["STARTING_ANGLE"] = omega_start
        if self._edge_res_cutoff:
            xds.xds_inp["INCLUDE_RESOLUTION_RANGE"] = (
                f"50.0 {self._edge_res_cutoff:.2f}"
            )
            logger.debug(f"edge resolution {self._edge_res_cutoff}")

        # restore original spot.xds
        shutil.copy(self.combined_spot_path_bk, self.combined_spot_path)
        shutil.copy(
            self.idxref_lp_path, f"{self.idxref_lp_path}.bk"
        )  # make a copy of idxref.lp
        xds.xds_inp["JOB"] = "DEFPIX XPLAN"

        if auto_sg and auto_cell:
            xds.xds_inp["SPACE_GROUP_NUMBER"] = auto_sg
            xds.xds_inp["UNIT_CELL_CONSTANTS"] = " ".join(map(str, auto_cell))

        xds.generate_xds_inp()
        logger.info(f"Running XDS 'XPLAN' with SG={auto_sg} cell={auto_cell}")
        xds.run()

        if os.path.exists(self.xplan_lp_path):
            xplan_res = parse_xplan_lp(self.xplan_lp_path)
            self.results["xplan"] = xplan_res or {}
            logger.info("Parsed XPLAN.LP for strategy results.")
        else:
            logger.error(f"{self.xplan_lp_path} not found after XPLAN.")

    def _run_xplan_with_suggested_alternative(self, master_file: str, idxref_res: Dict):
        """Update XDS.INP with suggested SG and cell; run XPLAN and parse."""
        auto_sg = idxref_res.get("auto_index_spacegroup")
        auto_cell = idxref_res.get("auto_index_unitcell")

        if not auto_sg or not auto_cell:
            logger.warning(
                "Auto-index SG or cell not found; running XPLAN without update."
            )
        shutil.copy(
            self.idxref_lp_path, f"{self.idxref_lp_path}.bk"
        )  # make a copy of idxref.lp

        for auto_lattice, auto_sg, auto_cell in idxref_res.get(
            "possible_solutions", []
        ):
            logger.info(f"rerun xds in space group: {auto_sg}, cell: {auto_cell}")

            xds = self._make_xds(master_file)
            logger.debug(f"edge resolution {self._edge_res_cutoff}")
            if self._edge_res_cutoff:
                xds.xds_inp["INCLUDE_RESOLUTION_RANGE"] = (
                    f"50.0 {self._edge_res_cutoff:.2f}"
                )

            # restore original spot.xds
            shutil.copy(self.combined_spot_path_bk, self.combined_spot_path)
            xds.xds_inp["JOB"] = "IDXREF DEFPIX XPLAN"

            if auto_sg and auto_cell:
                xds.xds_inp["SPACE_GROUP_NUMBER"] = auto_sg
                xds.xds_inp["UNIT_CELL_CONSTANTS"] = " ".join(map(str, auto_cell))

            xds.generate_xds_inp()
            logger.info(f"Running XDS 'XPLAN' with SG={auto_sg} cell={auto_cell}")
            xds.run()

            if os.path.exists(self.xplan_lp_path):
                xplan_res = parse_xplan_lp(self.xplan_lp_path)
                self.results["xplan"] = xplan_res or {}
                self.results["idxref"]["auto_index_lattice"] = auto_lattice
                self.results["idxref"]["auto_index_unitcell"] = auto_cell
                self.results["idxref"]["auto_index_spacegroup"] = auto_sg
                logger.info("Parsed XPLAN.LP for strategy results.")
                return
            else:
                logger.warning(f"{self.xplan_lp_path} not found after XPLAN.")

    def _publish_to_bluice_redis(self, success: bool):
        """Write strategy results to the Bluice Redis key for pybluice GUI display."""
        if not self._bluice_redis or not self._bluice_redis_key:
            return
        try:
            mapped = self._get_sql_mapped_results(self.results)
            mapped["software"] = "XDS"
            if success:
                mapped["status"] = "1"
            else:
                existing_status = self._bluice_redis.hget(self._bluice_redis_key, "status")
                if existing_status == "1":
                    logger.info("Bluice Redis key already has status=1; skipping failed update.")
                    return
                mapped["status"] = "0"
            self._bluice_redis.hset(self._bluice_redis_key, mapping=mapped)
            self._bluice_redis.incr("bluice:sample:strategy_ver__s")
            logger.info(f"Published XDS strategy results (status={mapped['status']}) to {self._bluice_redis_key}")
        except Exception as e:
            logger.warning(f"Failed to publish to Bluice Redis: {e}")

    def run(self) -> Dict:
        """Execute the full strategy workflow and return results dict."""
        self.tracker.start()

        try:
            masters = list(self.map.keys())
            if not masters:
                raise ValueError("Empty dataset map provided.")

            # 1) First dataset: XYCORR INIT COLSPOT + rename outputs
            first_master = masters[0]
            self._run_colspot_for(first_master, self.map[first_master], first=True)

            # 2) Subsequent datasets: COLSPOT + rename outputs
            for m in masters[1:]:
                self._run_colspot_for(m, self.map[m], first=False)

            self.tracker.update_progress("SPOT", self.results)

            # 3) Merge spot files into SPOT.XDS
            if not self._merge_spot_xds():
                raise RuntimeError("Failed to merge spot files into SPOT.XDS")





            # 4) Run IDXREF and parse results
            idxref_res = self._run_idxref_and_parse(first_master)
            if not idxref_res:
                raise RuntimeError("IDXREF ran but failed to find any valid indexing solutions.")

            self.tracker.update_progress("INDEX", self.results)

            if not idxref_res.get("possible_solutions"):
                raise RuntimeError("IDXREF failed to find any possible solutions.")

            self._parse_and_map_indexed_spots()

            # 5) Update XDS.INP with suggested SG/cell, run XPLAN, parse results
            self._run_xplan_with_suggested(first_master, idxref_res)
            self.tracker.update_progress("STRATEGY", self.results)

            self.results["spots_by_master"] = self.spots_by_master

            # Finalize results
            self.results["idxref"].update(parse_idxref_strategy(self.idxref_lp_path))
            self.results["n_spots"] = len(self.spots)
            self.results["spot_res"] = self.estimate_spot_resolution()

            resol = self.results.get("spot_res")
            mosaicity = self.results.get("idxref", {}).get("mosaicity")
            sdxy = self.results.get("idxref", {}).get("spot_stddev")

            if resol:
                estimated_processed_resol = max(float(resol) - 0.5, 0.5)
                distance_val = int(
                    round(
                        _calculate_distance_for_res(
                            self._h5_params.get(first_master, {}),
                            estimated_processed_resol,
                        )
                    )
                )
                distance_val = max(distance_val, 125)
                self.results["detectordistance"] = str(distance_val)

            if sdxy and mosaicity and resol:
                pixel_size = self._h5_params.get(first_master, {}).get(
                    "pixel_size", 0.075
                )
                self.results["screen_score"] = _screening_score(
                    resol=resol,
                    rms=float(sdxy) * float(pixel_size),
                    mosaic=float(mosaicity) * 2.0,  # Mosflm definition is different
                )
                logger.info(
                    f"Screen score: {self.results['screen_score']}, xds mosaicity={mosaicity}, spot stddev={sdxy}, spot res={resol}"
                )

            # matthew_coef calculation
            sg_num = idxref_res.get("auto_index_spacegroup")
            unit_cell_list = idxref_res.get("auto_index_unitcell")
            if sg_num and unit_cell_list:
                unit_cell_str = " ".join(map(str, unit_cell_list))
                logger.info(
                    f"Running Matthews coefficient for SG={sg_num}, Cell='{unit_cell_str}', Molsize={self.molsize}"
                )
                matthews_results = run_matthews_coef(
                    spacegroup=str(sg_num),
                    unitcell=unit_cell_str,
                    molsize=self.molsize,
                    debug=False,
                )
                if matthews_results:
                    logger.info(f"Matthews results: {matthews_results}")
                    self.results["matthews"] = matthews_results
                else:
                    logger.warning("Matthews coefficient calculation failed.")
                    self.results["matthews"] = {}
            else:
                logger.warning(
                    "Skipping Matthews: SG or cell not found in IDXREF results."
                )

            # Persist summary JSON
            try:
                with open(self.summary_json_path, "w") as f:
                    json.dump(self.results, f, indent=2)
                logger.info(f"Summary written: {self.summary_json_path}")
            except Exception as e:
                logger.warning(f"Failed to write summary JSON: {e}")

            self._publish_to_bluice_redis(True)
            self.tracker.succeed(self.results)
            return self.results

        except Exception as e:
            error_message = f"XDS strategy failed: {e}"
            logger.error(error_message, exc_info=True)
            self._publish_to_bluice_redis(False)
            self.tracker.fail(error_message, self.results)
            return None
        return self.results

    def estimate_spot_resolution(self) -> Optional[float]:
        """
        Estimate the 95th percentile resolution (Å) from detected spots using NumPy.
        """
        masterfile = next(iter(self.map.keys()), None)
        params = self._h5_params.get(masterfile) or {}
        spots = getattr(self, "spots", None)
        if not params or not spots:
            logger.warning("Missing detector parameters or no spots available.")
            return None

        try:
            px_mm = float(params.get("pixel_size", 0.075))
            nx = int(params.get("nx", 1024))
            ny = int(params.get("ny", 1024))
            beam_x = float(params.get("beam_x", nx / 2.0))
            beam_y = float(params.get("beam_y", ny / 2.0))
            wavelength = float(params.get("wavelength", 1.0))
            distance = float(params.get("det_dist", 300))
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid or missing required parameters: {e}")
            return None

        try:
            spot_arr = np.asarray(spots, dtype=float)
            if spot_arr.ndim != 2 or spot_arr.shape[1] != 2 or spot_arr.size == 0:
                return None
            dx = spot_arr[:, 0] - beam_x
            dy = spot_arr[:, 1] - beam_y
            r_px = np.hypot(dx, dy)
        except Exception as e:
            logger.warning(f"Failed computing radii: {e}")
            return None

        try:
            vec = np.vectorize(
                lambda rr: radius_to_resolution(
                    wavelength,
                    distance,
                    float(rr) * px_mm,
                    1.0,  # use px_mm=1.0 since we pass radius in mm
                ),
                otypes=[float],
            )
            res = vec(r_px)
            res = np.asarray(res, dtype=float)
            res = res[np.isfinite(res) & (res > 0)]
        except Exception as e:
            logger.warning(f"Failed converting radius to resolution: {e}")
            return None

        if res.size == 0:
            return None

        p5 = float(np.percentile(res, 5))
        return round(p5, 2)


def run_xds_strategy(
    multi_master_map: Dict[str, List[int]],
    workdir: Optional[str] = None,
    molsize: Optional[int] = None,
    use_slurm: bool = False,
    nproc: int = 1,
    njobs: int = 1,
    pipeline_params: Optional[dict] = None,
) -> Dict:
    """
    Convenience function to run the XDS strategy pipeline.
    """
    return XdsStrategy(
        multi_master_map,
        workdir=workdir,
        molsize=molsize,
        use_slurm=use_slurm,
        nproc=nproc,
        njobs=njobs,
        pipeline_params=pipeline_params,
    ).run()


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Run XDS multi-dataset strategy (per-master COLSPOT -> merge -> IDXREF -> XPLAN)."
    )
    ap.add_argument(
        "mapping",
        nargs="?",
        help='JSON like: {"path/to/master1.h5":[1], "path/to/master2.h5":[1,91]}',
    )
    ap.add_argument(
        "--workdir", default="/tmp/xds_strategy", help="Working directory for outputs"
    )
    ap.add_argument("--use_slurm", action="store_true", help="Run with SLURM")
    ap.add_argument("--nproc", type=int, default=8, help="Number of processors")
    ap.add_argument(
        "--njobs", type=int, default=1, help="Number of jobs (nodes for SLURM)"
    )
    # --- ADDED: Args for PipelineTracker ---
    ap.add_argument(
        "--username", default=os.getenv("USER"), help="Username for job attribution"
    )
    ap.add_argument("--esaf_id", type=int, help="ESAF ID for job attribution")
    ap.add_argument("--pi_id", type=int, help="PI ID for job attribution")
    ap.add_argument("--primary_group", help="Primary group for job attribution")
    ap.add_argument("--run_prefix", help="Run prefix for linking to DatasetRun")
    ap.add_argument("--sampleName", help="Sample Name for job attribution")
    ap.add_argument(
        "--molsize",
        type=int,
        default=None,
        help="Molecule size (residues) for Matthews coefficient.",
    )
    args = ap.parse_args()

    ## testing
    if not args.mapping:
        args.mapping = json.dumps(
            {
                "/mnt/beegfs/qxu/data-analysis/qp2/strategy/esaf281988-E8-sceen/E8_scr_00_master.h5": [1],
                "/mnt/beegfs/qxu/data-analysis/qp2/strategy/esaf281988-E8-sceen/E8_scr_90_master.h5": [1]
            }
        )

    # Convert mapping string back to dict
    try:
        mapping = json.loads(args.mapping)
    except Exception as e:
        logger.error(f"Failed to parse mapping JSON: {e}")
        sys.exit(1)

    # Prepare tracker params
    pipeline_params = {
        "username": args.username,
        "esaf_id": args.esaf_id,
        "pi_id": args.pi_id,
        "primary_group": args.primary_group,
        "run_prefix": args.run_prefix,
        "sampleName": args.sampleName,
    }
    # Filter out None values
    pipeline_params = {k: v for k, v in pipeline_params.items() if v is not None}

    res = run_xds_strategy(
        mapping,
        workdir=args.workdir,
        molsize=args.molsize,
        use_slurm=args.use_slurm,
        nproc=args.nproc,
        njobs=args.njobs,
        pipeline_params=pipeline_params,
    )
    print(json.dumps(res, indent=2))
