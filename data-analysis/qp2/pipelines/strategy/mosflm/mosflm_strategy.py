#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MOSFLM multi-dataset driver (per-template processes for findspots).

Input:
  - A dict mapping HDF5 master files to image numbers, e.g.:
    { "/path/to/E8_scr_00_master.h5": [1], "/path/to/E8_scr_90_master.h5": [] }
  - Empty list defaults to [1].

Behavior:
  1) For each master, run a separate ipmosflm process with directory/template and findspots on the specified images, writing a per-master .spt file.
  2) Merge all .spt files into merged.spt.
  3) Run a single ipmosflm process for autoindex + strategy using merged.spt.
  4) Run testgen, estimate oscillation and detector distance from spot-resolution.
  5) Persist results to mosflm_results.json and the database via PipelineTracker.

"""

import datetime
import json
import os
import subprocess
from collections import namedtuple, OrderedDict
from pathlib import Path
from typing import Optional, Dict, List, Union

from qp2.data_viewer.models import ScreenStrategyResults

from qp2.pipelines.utils.image_set import get_image_set_string

from qp2.pipelines.utils.pipeline_tracker import PipelineTracker
from qp2.pipelines.gmcaproc.symmetry import Symmetry
from qp2.log.logging_config import get_logger, setup_logging
from qp2.utils.matthews_coef import run_matthews_coef
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.xio.hdf5_manager import HDF5Reader
from qp2.config.servers import ServerConfig
from qp2.config.programs import ProgramConfig
from .mosflm_predictor import MosflmPredictor

logger = get_logger(__name__)
setup_logging()

from .mosflm_parsers import (
    _calculate_distance_for_res,
    _calculate_phi,
    _calculate_edge_and_corner_res,
    _get_osc,
    _screening_score,
    _parse_spt_file,
    parse_findspots_log,
    parse_autoindex_and_strategy,
    parse_testgen,
)
from qp2.xio.proc_utils import determine_proc_base_dir, extract_master_prefix

MIN_OSC_VALUE = 0.001
MosflmSolution = namedtuple(
    "MosflmSolution", "SolutionNo Penalty sdxy Spacegroup Refined_cell"
)


class MosflmStrategy:
    def __init__(
        self,
        multi_master_map: Dict[str, List[int]],
        workdir: Optional[str] = None,
        rmin: float = 5.0,
        highres: Optional[float] = None,
        native: bool = False,
        space_group: Optional[str] = None,
        unit_cell: Optional[str] = None,
        best: bool = True,
        sample_name: Optional[str] = None,
        sample_number: Union[str, int] = "1",
        molsize: Optional[int] = None,
        pipeline_params: Optional[dict] = None,
        pipeline_status_id: Optional[int] = None,
    ):
        if not isinstance(multi_master_map, dict) or not multi_master_map:
            raise ValueError(
                "multi_master_map must be a non-empty dict of {master.h5: [image_numbers]}"
            )

        # Normalize frames; default to [1] if empty/missing
        normalized: dict[str, list[int]] = {}
        for mpath, frames in multi_master_map.items():
            frames_list = list(frames) if frames else [1]
            if not all(isinstance(n, int) and n >= 1 for n in frames_list):
                raise ValueError(f"Invalid image numbers for {mpath}: {frames_list}")
            normalized[str(Path(mpath).resolve())] = frames_list

        self.multi_map = normalized
        self.default_template = next(iter(self.multi_map.keys()))
        self.default_image_number = self.multi_map.get(self.default_template, [1])[0]

        if workdir is None or workdir == ".":
            # Intelligent determination
            master_basename = extract_master_prefix(self.default_template)
            user_root = (
                pipeline_params.get("processing_common_proc_dir_root")
                if pipeline_params
                else None
            )
            proc_base = determine_proc_base_dir(user_root, self.default_template)
            self.workdir = str(proc_base / "mosflm_strategy" / master_basename)
        else:
            self.workdir = os.path.abspath(workdir)

        os.makedirs(self.workdir, exist_ok=True)

        self.rmin = rmin
        self.highres = highres
        self.native = native
        self.space_group = space_group
        self.unit_cell = unit_cell
        self.best = best

        self.sample_number = str(sample_number)
        self.molsize = molsize

        # Read HDF5 parameters once per master
        self._h5_params: dict[str, dict] = {}
        for mpath in self.multi_map:
            reader = None
            try:
                reader = HDF5Reader(mpath, start_timer=False)
                p = reader.get_parameters() or {}
                # Normalize expected keys and units
                self._h5_params[mpath] = {
                    "wavelength": float(p.get("wavelength", 1.0e-10)),  # meters
                    "det_dist": float(p.get("det_dist", 100.0)),  # mm
                    "pixel_size": float(p.get("pixel_size", 0.075)),  # mm
                    "beam_x": float(p.get("beam_x", 512.0)),  # px
                    "beam_y": float(p.get("beam_y", 512.0)),  # px
                    "nx": int(p.get("nx", 1024)),
                    "ny": int(p.get("ny", 1024)),
                    "omega_start": float(p.get("omega_start", 0.0)),
                    "omega_range": float(p.get("omega_range", 0.1)),
                }
                
                # Check units for wavelength and convert to Angstroms if necessary
                wl = self._h5_params[mpath]["wavelength"]
                if wl < 0.1:
                    # likely in meters
                    self._h5_params[mpath]["wavelength"] = wl * 1e10
                    logger.info(f"[{mpath}] Converted wavelength {wl:.3e} m to {self._h5_params[mpath]['wavelength']:.4f} A")
                else:
                    logger.info(f"[{mpath}] Using wavelength {wl:.4f} A (assumed Angstroms)")
            finally:
                if reader:
                    try:
                        reader.close()
                    except Exception:
                        pass

        # sort selt.muti_map by omega_start
        def _omega(params: dict) -> float:
            # HDF5Reader returns 'omegastart'; support 'omega_start' if present
            return float(params.get("omegastart", params.get("omega_start", 0.0)))

        # Reorder the mapping by omega start
        self.multi_map = OrderedDict(
            sorted(
                self.multi_map.items(),
                key=lambda kv: _omega(self._h5_params.get(kv[0], {})),
            )
        )

        # Recompute attributes that depend on mapping order
        self.default_template = next(iter(self.multi_map))
        self.default_image_number = self.multi_map[self.default_template][0]

        # Files and state
        self.spot_files = [
            os.path.join(self.workdir, Path(p).name.replace(".h5", ".spt"))
            for p in self.multi_map
        ]
        self._result_json: dict = {}
        self._log_lines: list[str] = []
        self._autoindex_table_data = None
        self._raw_index_table_text = None
        self.strategy = None
        self.spot_stat = None
        self.nref20s = 0
        self.nref10s = 0
        self.n_merged_refs = 0
        self.dps_threshold = 5

        # Output files per MOSFLM run
        self.spotod = None
        self.summary = None
        self.coords = None
        self.log_file_path = None
        self.err_file_path = None
        edge_corner_res = _calculate_edge_and_corner_res(
            self._h5_params.get(self.default_template, {})
        )
        self.edge_res = edge_corner_res.get("edge_res")
        self.corner_res = edge_corner_res.get("corner_res")

        logger.debug(f"meta data: {self._h5_params[self.default_template]}")
        logger.debug(
            f"Resolution @edge of the detector is: {self.edge_res} @corner of the detector is: {self.corner_res}"
        )

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

        if not sample_name:
            sample_name = (
                pipeline_params.get("sampleName")
                or Path(self.default_template).stem.rsplit("_master", 1)[0]
            )
        self.sample_name = str(sample_name)

        # Populate essential params if not provided
        pipeline_params.setdefault("sampleName", self.sample_name)
        pipeline_params.setdefault("imagedir", os.path.dirname(self.default_template))
        pipeline_params.setdefault("workdir", self.workdir)
        pipeline_params.setdefault(
            "logfile", os.path.join(self.workdir, "mosflm_strategy.log")
        )
        pipeline_params.setdefault("beamline", get_beamline_from_hostname())
        pipeline_params.setdefault("imageSet", get_image_set_string(self.multi_map))
        pipeline_params.setdefault("datasets", json.dumps(list(self.multi_map.keys())))

        # Get Redis host from central config
        redis_host = ServerConfig.get_redis_hosts().get("analysis_results", "10.20.103.67")

        self.tracker = PipelineTracker(
            pipeline_name="mosflm_strategy",
            run_identifier=self.default_template,
            initial_params=pipeline_params,
            result_mapper=self._get_sql_mapped_results,
            redis_config={"host": redis_host, "db": 0},
            existing_pipeline_status_id=pipeline_status_id,
            results_model=ScreenStrategyResults,
        )

    # ------------------------
    # MOSFLM command builders
    # ------------------------
    def _cmd_init(self):
        cmd = []
        cmd.append(f"PNAME {self.sample_name}")
        cmd.append(f"DNAME {self.sample_number}")
        cmd.append(f"TEMPLATE {self.default_template}")
        if self.best:
            cmd.append("best on")
        if self.space_group:
            cmd.append(f"symm {self.space_group}")
        if self.unit_cell:
            cmd.append(f"cell {self.unit_cell}")
        if self.highres:
            cmd.append(f"resolution {self.highres}")
        else:
            self.highres = self.edge_res
            cmd.append(f"resolution {self.edge_res}")
        
        logger.info(f"Using strategy resolution limit: {self.highres:.4f} A")
        cmd.append("head brief")
        cmd.append("go")
        return cmd

    def _cmd_findspots_for(self, mpath: str, imgs: list[int]):
        params = self._h5_params.get(mpath, {})
        mdir = str(Path(mpath).parent)
        templ = Path(mpath).name
        spt_file = os.path.join(self.workdir, Path(mpath).name.replace(".h5", ".spt"))
        cmd = []
        cmd.append(f"directory {mdir}")
        cmd.append(f"template {templ}")
        cmd.append(f"findspots rmin {self.rmin} AUTORESLN AUTORING LOCAL ICE")
        for imgnum in imgs:
            osc = float(_get_osc(params))
            phi_start = _calculate_phi(params, imgnum)
            phi_end = phi_start + float(_get_osc(params))

            if osc <= MIN_OSC_VALUE:
                cmd.append(
                    f"findspots find {imgnum} phi {phi_start} {phi_start} file {spt_file}"
                )
            else:
                cmd.append(
                    f"findspots find {imgnum} phi {phi_start} {phi_end} file {spt_file}"
                )
        cmd.append("go")
        return cmd

    def _cmd_autoindex(self, refine=True):
        cmd = []
        imgs = " "
        merged_spotfile = os.path.join(self.workdir, "merged.spt")
        if os.path.isfile(merged_spotfile):
            imgs += f" thresh {self.dps_threshold} file {merged_spotfile}"
        if refine:
            cmd.append(f"autoindex dps image {imgs} refine save")
        else:
            cmd.append(f"autoindex dps image {imgs} save")
        cmd.append("newmat autoindex.mat")
        cmd.append(f"mosaic estimate images {self.default_image_number}")
        cmd.append("go")
        if self.native:
            cmd.append("strategy auto notanom")
        else:
            cmd.append("strategy auto anomalous")
        cmd.append("go")
        cmd.append("stats")
        return cmd

    def _cmd_testgen(
        self,
        matrix_name="autoindex.mat",
        mosaic_val=None,
        start=0,
        end=180,
        maxosc=0.2,
        overlap=1.0,
    ):
        cmd = []
        params1 = next(iter(self._h5_params.items()))[-1]
        start_angle = params1.get("omega_start", 0.0)
        osc = params1.get("omega_range", 0.0001)
        cmd.append(
            f"image {self.default_image_number} phi {start_angle} {start_angle + osc}"
        )
        cmd.append(f"!image {self.default_image_number}")

        cmd.append("go")
        if self.strategy and isinstance(self.strategy, dict):
            start = self.strategy.get("startAngle", start)
            end = self.strategy.get("endAngle", end)
            matrix_name = self.strategy.get("matrix", matrix_name)
            cmd.append(f"matrix {matrix_name}")
            mosaic_from_strategy = self.strategy.get("mosaic")
            if mosaic_from_strategy and mosaic_from_strategy != "NA":
                cmd.append(f"mosaic {mosaic_from_strategy}")
            else:
                cmd.append(f"mosaic estimate images {self.default_image_number}")
                cmd.append("go")
        elif matrix_name:
            cmd.append(f"matrix {matrix_name}")
        cmd.append(f"testgen start {start} end {end} maxosc {maxosc} overlap {overlap}")
        cmd.append("go")
        # integration
        cmd.append(f"!process {self.default_image_number} {self.default_image_number}")
        cmd.append("!go")
        return cmd

    # ------------------------
    # MOSFLM execution
    # ------------------------
    def run_mosflm(self, cmds, tag=None):
        if cmds is None:
            logger.error("No commands provided for MOSFLM.")
            return None
        _tag = tag if tag else datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        # Output file paths for this run
        self.spotod = os.path.join(self.workdir, f"mosflm_{_tag}.spotod")
        self.summary = os.path.join(self.workdir, f"mosflm_{_tag}.sum")
        self.coords = os.path.join(self.workdir, f"mosflm_{_tag}.coords")
        self.log_file_path = os.path.join(self.workdir, f"mosflm_{_tag}.log")
        self.err_file_path = os.path.join(self.workdir, f"mosflm_{_tag}.err")

        # Use ProgramConfig to get the setup command for CCP4
        setup_cmd = ProgramConfig.get_setup_command("ccp4")
        
        # Construct the full shell command string for ipmosflm
        ipmosflm_cmd = (
            f"ipmosflm SPOTOD {self.spotod} COORDS {self.coords} SUMMARY {self.summary}"
        )
        
        # Combine setup and execution
        full_command = f"{setup_cmd} && {ipmosflm_cmd}"

        input_lines = []
        input_lines.extend(self._cmd_init())
        input_lines.extend(cmds)
        input_lines.append("exit")
        input_str = "\n".join(input_lines) + "\n"

        env = os.environ.copy()
        env.pop("LD_PRELOAD", None)

        try:
            # Execute via bash -c to allow environment setup (source modules)
            proc = subprocess.Popen(
                ["bash", "-c", full_command],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.workdir,
                text=True,
                env=env,
            )
        except FileNotFoundError:
            with open(self.log_file_path, "w") as f:
                f.write("Error: 'ipmosflm' not found in PATH.\n")
            logger.error("'ipmosflm' not found in PATH.")
            return None

        try:
            stdout_data, stderr_data = proc.communicate(input=input_str, timeout=300)
        except subprocess.TimeoutExpired:
            logger.warning(f"MOSFLM task '{_tag}' timed out; terminating.")
            proc.terminate()
            try:
                stdout_data, stderr_data = proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout_data, stderr_data = "", "Killed after timeout."
        rc = proc.returncode if proc and proc.returncode is not None else -1

        with open(self.log_file_path, "w") as f_log:
            f_log.write(stdout_data or "")
        if stderr_data:
            with open(self.err_file_path, "w") as f_err:
                f_err.write(stderr_data)

        if rc != 0:
            logger.error(f"MOSFLM task '{_tag}' failed with return code {rc}")
        else:
            logger.info(f"MOSFLM task '{_tag}' completed.")

        self._read_log(self.log_file_path)
        return self.log_file_path

    # ------------------------
    # Spot merging and parsing
    # ------------------------
    def _merge_spot_files(self):
        logger.debug("Merging spot files...")
        merge_spots = []
        header = footer = ""
        for i, file_spt in enumerate(self.spot_files):
            if os.path.isfile(file_spt):
                with open(file_spt, "r") as f_spt:
                    lines = f_spt.readlines()
                if not lines:
                    continue
                if i == 0:
                    header, footer = lines[:3], lines[-2:]
                    merge_spots.extend(lines[3:-2])
                else:
                    merge_spots.extend(lines[3:-2])

        self.n_merged_refs = len(merge_spots)
        self.nref10s = self.nref20s = 0
        for spot_line in merge_spots:
            fields = spot_line.split()
            if len(fields) == 6:
                try:
                    intensity, sigma_i = float(fields[-2]), float(fields[-1])
                    if sigma_i > 0:
                        ios = intensity / sigma_i
                        if ios >= 20.0:
                            self.nref20s += 1
                        if ios >= 10.0:
                            self.nref10s += 1
                except ValueError as e:
                    logger.exception(e)
                    continue
        logger.info(f"n > 20 sigma {self.nref20s},  n > 10 sigma {self.nref10s}")
        if self.nref20s >= 100:
            self.dps_threshold = 20
        elif self.nref10s >= 100:
            self.dps_threshold = 10
        else:
            self.dps_threshold = 5
        logger.info(f"select threshold for index: {self.dps_threshold}")
        if self.n_merged_refs > 0 and header and footer:
            with open(os.path.join(self.workdir, "merged.spt"), "w") as f_out:
                f_out.writelines(header + merge_spots + footer)

    def _read_log(self, path):
        self._log_lines = []
        if path and os.path.exists(path):
            try:
                with open(path, "r") as fh:
                    self._log_lines = fh.readlines()
            except Exception as e:
                logger.error(f"Error reading log file {path}: {e}")

    def _append_to_combined_log(self, src_log, combined_path):
        if src_log and os.path.exists(src_log):
            with open(src_log, "r") as f_in, open(combined_path, "a") as f_out:
                f_out.write(f_in.read())

    def _get_sql_mapped_results(self, results_dict: dict) -> dict:
        """
        Map internal results to ScreenStrategyResults fields with robust defaults.
        Populates required schema columns: directory, images, software, solution_number,
        penalty, ice/spot stats, and strategy summary.
        """
        # Extract stage dictionaries
        spot_info = results_dict.get("spot", {})
        autoindex_info = results_dict.get("autoindex", {})
        strategy_info = results_dict.get("strategy", {})
        testgen_info = results_dict.get("testgen", {})
        final_info = results_dict.get("final", {})
        solution = autoindex_info.get("solution", {})
        matthews_info = results_dict.get("matthews", {})

        # Derive bravais lattice from spacegroup
        bravais_lattice = ""
        spacegroup = final_info.get("spacegroup", "")
        if spacegroup:
            try:
                bravais_lattice = Symmetry.space_group_to_lattice(spacegroup)
            except Exception as e:
                logger.warning(
                    f"Could not determine Bravais lattice for {spacegroup}: {e}"
                )
                bravais_lattice = ""

        # Directory and images for default template
        directory = os.path.dirname(self.default_template)
        default_images = self.multi_map.get(self.default_template, [1])
        images = ",".join(str(i) for i in default_images) if default_images else ""

        # Spot stats: totals and ice
        n_spots = str(spot_info.get("n_spots", ""))
        n_spots_ice = ""
        n_ice_rings = ""
        ice_rings = ""
        avg_spotsize = ""
        try:
            # Some parsers provide details per image; compute aggregates if available
            details = spot_info.get("details", {})
            if isinstance(details, dict) and details:
                # Sum nspots_ice across entries
                n_spots_ice_val = 0
                ice_ring_vals = []
                spot_sizes = []
                for _, d in details.items():
                    if isinstance(d, dict):
                        if d.get("nspots_ice") is not None:
                            n_spots_ice_val += int(d.get("nspots_ice", 0))
                        # ice_rings not explicitly provided; keep placeholder
                        # spotsize like "2.0x2.0" -> take first as numeric for avg
                        ss = d.get("spotsize")
                        if isinstance(ss, str) and "x" in ss:
                            try:
                                sx = float(ss.split("x")[0])
                                spot_sizes.append(sx)
                            except Exception:
                                pass
                if n_spots_ice_val:
                    n_spots_ice = str(n_spots_ice_val)
                if spot_sizes:
                    avg_spotsize = f"{sum(spot_sizes) / len(spot_sizes):.2f}"
            # Fallbacks
            if not n_spots_ice:
                n_spots_ice = str(spot_info.get("n_spots_ice", ""))
            if not n_ice_rings:
                n_ice_rings = str(spot_info.get("n_ice_rings", ""))
            if not ice_rings:
                ice_rings = ""  # not provided by parser
        except Exception:
            pass

        # Solution fields
        solution_number = str(solution.get("number", ""))
        penalty = str(solution.get("penalty", ""))
        rmsd = str(solution.get("sdxy", ""))

        # Strategy serializations / display
        strategy_text = ""
        try:
            strategy_text = json.dumps(strategy_info, default=str)
        except Exception:
            strategy_text = ""
        displaytext = ""
        if spacegroup or final_info.get("unitcell"):
            displaytext = f"{spacegroup} {final_info.get('unitcell', '')}".strip()

        mapped = {
            # Identifiers / context
            "sampleName": self.sample_name,
            "directory": directory,
            "images": images,
            "software": "MOSFLM",
            "state": "SPOT" if not strategy_info else "STRATEGY",
            "workdir": self.workdir,
            # Table content
            "index_table": str(autoindex_info.get("index_table", "")),
            "unitcell": str(final_info.get("unitcell", "")),
            "bravais_lattice": bravais_lattice,
            "rmsd": rmsd,
            "ice_rings": ice_rings,
            "resolution_from_spots": str(spot_info.get("resolution_from_spots", "")),
            "n_spots": n_spots,
            "n_spots_ice": n_spots_ice,
            "n_ice_rings": n_ice_rings,
            "avg_spotsize": avg_spotsize,
            "spacegroup": spacegroup,
            "solution_number": solution_number,
            "penalty": penalty,
            "mosaicity": str(strategy_info.get("mosaic", "")),
            "score": str(final_info.get("score", "")),
            "resolution_from_integ": "",
            "warning": "",
            "anomalous": 0,
            "osc_start": str(strategy_info.get("startAngle", "")),
            "osc_end": str(strategy_info.get("endAngle", "")),
            "osc_delta": str(testgen_info.get("osc_delta", "")),
            "completeness_native": str(strategy_info.get("nativeCompleteness", "")),
            "completeness_anomalous": str(
                strategy_info.get("anomalousCompletenes", "")
            ),
            "completeness_referencedata": "",
            "detectorwarning": "",
            "detectordistance": str(testgen_info.get("distance", "")),
            "referencedata": "",
            "displaytext": displaytext,
            "xplanlog": "",
            "strategy": strategy_text,
            "reprocess": 0,
            "estimated_asu_content_aa": str(matthews_info.get("asu_content", "")),
            "solvent_content": str(matthews_info.get("solvent", "")),
            "export2run": 0,
            "pointgroup_choices": "",
        }
        # Return the full mapped dict; DB model defaults will handle empty strings where needed
        return mapped

    def _publish_to_bluice_redis(self, success: bool):
        """Write strategy results to the Bluice Redis key for pybluice GUI display."""
        if not self._bluice_redis or not self._bluice_redis_key:
            return
        try:
            mapped = self._get_sql_mapped_results(self._result_json)
            mapped["software"] = "MOSFLM"
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
            logger.info(f"Published MOSFLM strategy results (status={mapped['status']}) to {self._bluice_redis_key}")
        except Exception as e:
            logger.warning(f"Failed to publish to Bluice Redis: {e}")

    # ------------------------
    # Public pipeline
    # ------------------------
    def findspots(self):
        # Clean combined findspots log
        combined_log = os.path.join(self.workdir, "mosflm_findspots.log")
        if os.path.exists(combined_log):
            os.remove(combined_log)

        # Run a separate MOSFLM process per master/template
        for mpath, imgs in self.multi_map.items():
            tag = f"findspots_{Path(mpath).stem}"
            cmds = self._cmd_findspots_for(mpath, imgs)
            run_log = self.run_mosflm(cmds, tag=tag)
            # Append to combined log for unified parsing
            self._append_to_combined_log(run_log, combined_log)

        # Read combined log for downstream parsing
        self.log_file_path = combined_log
        self._read_log(combined_log)
        # Merge all produced .spt files
        self._merge_spot_files()

    def autoindex(self, tag="autoindex"):
        self.run_mosflm(self._cmd_autoindex(), tag=tag)

    def testgen(self, tag="testgen", matrix_name="autoindex.mat", mosaic_val=None):
        self.run_mosflm(
            self._cmd_testgen(matrix_name=matrix_name, mosaic_val=mosaic_val), tag=tag
        )

    def _write_results_json(self):
        try:
            outp = os.path.join(self.workdir, "mosflm_results.json")
            with open(outp, "w") as f:
                json.dump(self._result_json, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed writing mosflm_results.json: {e}")

    def standard_strategy(self):
        self.tracker.start()

        try:
            # Step 1: Findspots (per-template processes)
            merged_spt = os.path.join(self.workdir, "merged.spt")
            combined_log = os.path.join(self.workdir, "mosflm_findspots.log")
            if not (os.path.exists(combined_log) and os.path.exists(merged_spt)):
                self.findspots()
            else:
                self._read_log(combined_log)
                try:
                    with open(merged_spt, "r") as f_spt_m:
                        self.n_merged_refs = max(0, len(f_spt_m.readlines()) - 5)
                except Exception:
                    self.n_merged_refs = 0
                logger.info("Using existing combined findspots log and merged.spt.")

            self.spot_stat = parse_findspots_log(combined_log)
            if not self.spot_stat:
                raise RuntimeError("Spot stats parsing failed. Aborting.")

            self._result_json["spot"] = dict(self.spot_stat or {})
            self._result_json["spot"].update(
                dict(
                    nref_gt20=self.nref20s,
                    nref_gt10=self.nref10s,
                    n_merged=self.n_merged_refs,
                    dps_thresh=self.dps_threshold,
                )
            )
            self._write_results_json()
            # --- NEW: Parse raw spots from .spt files and store them ---
            raw_spots_data = (
                {}
            )  # Temporary storage: {master_path: {img_num: [[x, y, 0], ...]}}
            for mpath, imgs in self.multi_map.items():
                params = self._h5_params.get(mpath, {})
                pixel_size_mm = params.get("pixel_size")
                spt_file_path = os.path.join(
                    self.workdir, Path(mpath).name.replace(".h5", ".spt")
                )
                raw_spots_xy = _parse_spt_file(spt_file_path)
                if raw_spots_xy is not None and pixel_size_mm and pixel_size_mm > 0:
                    # Convert coordinates from mm (in .spt) to pixels for display
                    raw_spots_in_pixels = raw_spots_xy / pixel_size_mm

                    master_entry = raw_spots_data.setdefault(mpath, {})
                    # The spots belong to all images processed for this master file
                    for img_num in imgs:
                        frame_dict = master_entry.setdefault(
                            img_num, {"spots_mosflm": []}
                        )
                        # Add the raw spots with an "unindexed" flag (0)
                        for x, y in raw_spots_in_pixels:
                            frame_dict["spots_mosflm"].append([x, y, 0])
                elif raw_spots_xy is not None:
                    logger.warning(
                        f"Could not find valid pixel size for {mpath}. Cannot convert raw spot coordinates."
                    )

            self.tracker.update_progress("SPOT", self._result_json)

            if self.n_merged_refs < 15:
                raise RuntimeError(
                    f"Need >= 15 merged spots (found {self.n_merged_refs}). Aborted."
                )

            # Step 2: Autoindex + Strategy (single MOSFLM process)
            self.autoindex(tag="autoindex")
            auto_log = os.path.join(self.workdir, "mosflm_autoindex.log")
            raw_table_text, candidates, solution, strategy = (
                parse_autoindex_and_strategy(auto_log)
            )
            self._autoindex_table_data = candidates
            self._raw_index_table_text = raw_table_text
            self.strategy = strategy
            if not solution or not self.strategy:
                raise RuntimeError("Autoindex/strategy parsing failed.")

            self._result_json["autoindex"] = dict(
                solution=dict(
                    number=solution.SolutionNo,
                    penalty=solution.Penalty,
                    sdxy=solution.sdxy,
                    spacegroup=solution.Spacegroup,
                    refined_cell=solution.Refined_cell,
                ),
                index_table=self._raw_index_table_text,
                possible_groups="",
            )
            self._result_json["strategy"] = dict(self.strategy or {})
            self._write_results_json()
            self.tracker.update_progress("INDEX", self._result_json)

            # =================================================================
            # === NEW: RUN MOSFLM PREDICTION AFTER SUCCESSFUL AUTOINDEXING ===
            # =================================================================
            logger.info(
                "Autoindexing successful. Proceeding to Mosflm prediction step..."
            )

            matrix_file_for_predict = self.strategy.get("matrix")
            mosaicity_for_predict = self.strategy.get("mosaic")

            if (
                matrix_file_for_predict
                and mosaicity_for_predict
                and str(mosaicity_for_predict).upper() != "NA"
            ):
                predictor = MosflmPredictor(
                    executable_path="ipmosflm", workdir=self.workdir
                )

                # This will store the results in a structure similar to XDS
                self.spots_by_master = {}

                for master_path, image_nums in self.multi_map.items():
                    master_entry = self.spots_by_master.setdefault(master_path, {})
                    params = self._h5_params.get(master_path, {})

                    for img_num in image_nums:
                        phi = _calculate_phi(params, img_num)
                        osc = _get_osc(params)

                        logger.info(
                            f"Running Mosflm prediction for {Path(master_path).name}, image {img_num}..."
                        )

                        predictions = predictor.run(
                            template=master_path,
                            image_num=img_num,
                            phi=phi,
                            osc=osc,
                            matrix_file=matrix_file_for_predict,
                            mosaicity=float(mosaicity_for_predict),
                            resolution=float(self.highres),
                        )

                        if predictions:
                            frame_dict = master_entry.setdefault(
                                img_num, {"spots_mosflm": [], "reflections_mosflm": []}
                            )

                            if (
                                raw_spots := raw_spots_data.get(master_path, {})
                                .get(img_num, {})
                                .get("spots_mosflm")
                            ):
                                frame_dict["spots_mosflm"].extend(raw_spots)

                            # All predicted spots are stored in 'spots_mosflm'
                            # 'reflections_mosflm' will contain only those that are indexed (which is all of them)
                            all_predictions = predictions.get(
                                "fulls", []
                            ) + predictions.get("partials", [])
                            for p in all_predictions:
                                frame_dict["reflections_mosflm"].append(
                                    [p["h"], p["k"], p["l"], p["x"], p["y"]]
                                )

                            logger.info(
                                f"  -> Found {len(frame_dict['spots_mosflm'])} raw spots and {len(all_predictions)} predictions for image {img_num}."
                            )
                        else:
                            logger.warning(
                                f"  -> Prediction failed for image {img_num}."
                            )

                # Add the collected prediction data to the main results dictionary
                self._result_json["spots_by_master_mosflm"] = self.spots_by_master

            else:
                logger.warning(
                    "Skipping Mosflm prediction step: Matrix or mosaicity not found after autoindexing."
                )
            # =================================================================
            # === END OF NEW PREDICTION LOGIC =================================
            # =================================================================

            # Step 3: Testgen + distance estimate
            matrix_for_testgen = (
                self.strategy.get("matrix", "autoindex.mat")
                if self.strategy
                else "autoindex.mat"
            )
            mosaic_for_testgen = self.strategy.get("mosaic") if self.strategy else None
            self.testgen(
                tag=f"testgen_sln{solution.SolutionNo}",
                matrix_name=matrix_for_testgen,
                mosaic_val=mosaic_for_testgen,
            )

            testgen_log = os.path.join(
                self.workdir, f"mosflm_testgen_sln{solution.SolutionNo}.log"
            )
            test_row = parse_testgen(testgen_log)
            osc_val = None
            if isinstance(test_row, dict):
                osc_val = test_row.get("osc_angle")
            elif isinstance(test_row, (int, float)):
                osc_val = test_row

            distance_val = 350
            spot_reso = self.spot_stat.get("resolution_from_spots", "")

            score = None
            if spot_reso and mosaic_for_testgen and solution.sdxy != "N/A":
                try:
                    # --- robust conversion guards (mosaic may be "NA" as string) ---
                    if str(mosaic_for_testgen).upper() != "NA":
                        score = _screening_score(
                            resol=float(spot_reso),
                            rms=float(solution.sdxy),
                            mosaic=float(mosaic_for_testgen),
                        )
                except Exception as e:
                    logger.warning(f"Failed to compute screening score: {e}")

            spot_res_str = str(spot_reso)
            if spot_res_str and spot_res_str != "NA":
                try:
                    first_master = next(iter(self.multi_map.keys()))
                    estimated_processed_resol = max(float(spot_reso) - 0.5, 0.5)
                    distance_val = int(
                        round(
                            _calculate_distance_for_res(
                                self._h5_params.get(first_master, {}),
                                estimated_processed_resol,
                            )
                        )
                    )
                    distance_val = max(distance_val, 125)
                    logger.info(
                        f"Recommend Distance: {distance_val} mm based on spot resolution {estimated_processed_resol}."
                    )
                except (ValueError, TypeError):
                    logger.warning("Distance estimate failed; using default 350 mm.")
            else:
                logger.warning("Spot resolution NA; using default distance 350 mm.")

            self._result_json["testgen"] = dict(
                osc_delta=osc_val, distance=distance_val
            )

            self._write_results_json()
            self.tracker.update_progress("TESTGEN", self._result_json)

            if solution.Spacegroup != "NA" and solution.Refined_cell != "N/A":
                logger.info(
                    f"Running Matthews coefficient for SG={solution.Spacegroup}, Cell='{solution.Refined_cell}', Molsize={self.molsize}"
                )
                matthews_results = run_matthews_coef(
                    spacegroup=solution.Spacegroup,
                    unitcell=solution.Refined_cell,
                    molsize=self.molsize,
                    debug=False,  # Set to True for debugging matthews itself
                )

                if matthews_results:
                    logger.info(f"Matthews results: {matthews_results}")
                    self._result_json["matthews"] = matthews_results
                    self.strategy.update(
                        {
                            "estimated_asu_content_aa": matthews_results.get(
                                "asu_content"
                            ),
                            "solvent_content": matthews_results.get("solvent"),
                        }
                    )
                else:
                    logger.warning(
                        "Matthews coefficient calculation failed or returned no results."
                    )
                    self._result_json["matthews"] = {}

            # Finalize and return strategy
            if self.strategy:
                self.strategy.update(
                    {
                        "osc": osc_val if osc_val is not None else "NA",
                        "solutionNo": solution.SolutionNo,
                        "spacegroup": solution.Spacegroup,
                        "unitcell": solution.Refined_cell,
                        "edge_resol": self.highres,
                        "distance": distance_val,
                        "score": score,
                    }
                )
                self._result_json["final"] = dict(self.strategy or {})
                self._write_results_json()
                self.tracker.update_progress("STRATEGY", self._result_json)
                self._publish_to_bluice_redis(True)
                self.tracker.succeed(self._result_json)
                return self._result_json

            # This part should not be reached if strategy is populated
            raise RuntimeError("Final strategy was not populated.")

        except Exception as e:
            error_message = f"MOSFLM strategy failed: {e}"
            logger.error(error_message, exc_info=True)
            self._publish_to_bluice_redis(False)
            self.tracker.fail(error_message, self._result_json)
            return None


def _coerce_to_mapping(multi_master_input) -> dict[str, list[int]]:
    """
    Accepts:
      - str: single master file -> {abs_path: [1]}
      - list[str]: list of master files -> {abs_path: [1], ...}
      - dict[str, list[int]]: pass-through after basic validation
    Returns a normalized mapping dict.
    """
    from pathlib import Path

    if isinstance(multi_master_input, str):
        return {str(Path(multi_master_input).resolve()): [1]}
    if isinstance(multi_master_input, (list, tuple)):
        return {str(Path(p).resolve()): [1] for p in multi_master_input}
    if isinstance(multi_master_input, dict):
        # Basic validation
        for k, v in multi_master_input.items():
            if not isinstance(k, str) or not isinstance(v, (list, tuple)):
                raise ValueError("Mapping must be dict[str, list[int]].")
            if not all(isinstance(x, int) and x >= 1 for x in v):
                raise ValueError(f"Invalid image numbers for {k}: {v}")
        return {str(Path(k).resolve()): list(v) for k, v in multi_master_input.items()}
    raise ValueError(
        "multi_master_input must be a str, list[str], or dict[str, list[int]]."
    )


def run_strategy(
    multi_master_input: Union[str, List[str], Dict[str, List[int]]],
    workdir: str = ".",
    molsize: Optional[int] = None,
    pipeline_params: Optional[dict] = None,
):
    """
    Accepts a single master file (str), a list of master files (list[str]),
    or the original dict mapping {master: [images]}.
    """
    mapping = _coerce_to_mapping(multi_master_input)
    return MosflmStrategy(
        mapping, workdir=workdir, molsize=molsize, pipeline_params=pipeline_params
    ).standard_strategy()


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Run MOSFLM multi-dataset strategy (per-template findspots)."
    )
    ap.add_argument(
        "mapping",
        nargs="?",
        help='JSON like: {"path/to/master1.h5":[1], "path/to/master2.h5":[1,91]}',
    )
    ap.add_argument(
        "--masters",
        nargs="+",
        help="One or more HDF5 master files; defaults to using image 1 for each.",
    )
    ap.add_argument(
        "--workdir",
        default="/tmp/mosflm_strategy",
        help="Working directory for logs and outputs",
    )
    ap.add_argument(
        "--molsize",
        type=int,
        default=None,
        help="Molecule size (number of residues) for Matthews coefficient calculation.",
    )
    ap.add_argument(
        "--username", default=os.getenv("USER"), help="Username for job attribution"
    )
    ap.add_argument("--esaf_id", type=int, help="ESAF ID for job attribution")
    ap.add_argument("--pi_id", type=int, help="PI ID for job attribution")
    ap.add_argument("--primary_group", help="Primary group for job attribution")
    ap.add_argument("--run_prefix", help="Run prefix for linking to DatasetRun")
    ap.add_argument(
        "--beamline",
        default=get_beamline_from_hostname(),
        help="beamline for db destination and job attribution",
    )

    args = ap.parse_args()

    # Build mapping from either JSON or --masters
    mapping = None
    if args.mapping:
        try:
            mapping = json.loads(args.mapping)
            if not isinstance(mapping, dict):
                raise ValueError("Mapping must be a dict.")
        except Exception as e:
            print(f"Failed to parse mapping JSON: {e}")
            exit(3)
    elif args.masters:
        mapping = {str(Path(p).resolve()): [1] for p in args.masters}
    else:
        logger.error(f"no input given, run with a test data")
        mapping = {
            "/mnt/beegfs/qxu/data-analysis/qp2/strategy/esaf281988-E8-sceen/E8_scr_00_master.h5": [
                1
            ],
            "/mnt/beegfs/qxu/data-analysis/qp2/strategy/esaf281988-E8-sceen/E8_scr_90_master.h5": [
                1
            ],
        }

    pipeline_params = {
        "username": args.username,
        "esaf_id": args.esaf_id,
        "pi_id": args.pi_id,
        "primary_group": args.primary_group,
        "run_prefix": args.run_prefix,
    }
    # Filter out any None values so they don't override defaults
    pipeline_params = {k: v for k, v in pipeline_params.items() if v is not None}

    strat = run_strategy(
        mapping,
        workdir=args.workdir,
        molsize=args.molsize,
        pipeline_params=pipeline_params,
    )
    print(json.dumps({"strategy": strat}, indent=2))
