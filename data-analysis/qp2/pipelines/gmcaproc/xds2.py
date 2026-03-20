#!/mnt/beegfs/.software_bl2/px/miniconda3/envs/data-analysis/bin/python
# -*- coding: utf-8 -*-
"""
QX Created on Mon Oct 23 10:00:00 2023
"""

import argparse
import json
import os
import re
import subprocess
import shutil
import sys
from enum import Enum
from multiprocessing import Process
from typing import Dict, Optional, Any

from qp2.pipelines.gmcaproc.cbfreader import CbfReader
from qp2.pipelines.gmcaproc.hdf5reader import HDF5Reader
from qp2.pipelines.gmcaproc.nxds_parsers import (
    parse_colspot_lp,
    parse_nxds_idxref_log,
    parse_spot_nxds,
    parse_nxscale_or_ncorrect_lp
)
from qp2.pipelines.utils.pipeline_tracker import PipelineTracker

from qp2.pipelines.gmcaproc.rcsb import RCSB
from qp2.pipelines.gmcaproc.symmetry import Symmetry
from qp2.pipelines.gmcaproc.xds_config import Filenames, XdsConfig, get_detector_gaps
from qp2.pipelines.gmcaproc.xds_parsers import (
    parse_idxref_lp,
    parse_xplan_lp,
    parse_correct_lp,
    parse_pointless_xml,
    parse_integrate_lp,
    parse_integrate_lp_per_frame,
)
from qp2.image_viewer.utils.run_job import run_command
from qp2.log.logging_config import (
    get_multiprocessing_queue,
    start_queue_listener,
    setup_logging,
    get_logger,
)
from qp2.utils.merge_dicts import merge_dicts
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.config.servers import ServerConfig
from qp2.config.programs import ProgramConfig
from .mtz_utils import get_cell_symm_from_mtz
from .xds_report import XDSReportGenerator

logger = get_logger(__name__)


class MetaData(Enum):
    DETECTOR = "detector"
    NIMAGES = "nimages"
    X_PIXELS = "x_pixels_in_detector"
    Y_PIXELS = "y_pixels_in_detector"
    OVERLOAD = "count_cutoff"
    SENSOR_THICKNESS = "sensor_thickness"
    X_PIXEL_SIZE = "x_pixel_size"
    Y_PIXEL_SIZE = "y_pixel_size"
    BEAM_CENTER_X = "beam_center_x"
    BEAM_CENTER_Y = "beam_center_y"
    DETECTOR_DISTANCE = "detector_distance"
    WAVELENGTH = "incident_wavelength"
    OMEGA_RANGE = "omega_range_average"
    STARTING_OMEGA = "omega"
    TEMPLATE = "template"
    MASTER_FILE = "master_file"
    PREFIX = "prefix"
    START = "start"
    END = "end"
    DATA_FILES = "datafiles"


from qp2.pipelines.utils.image_set import get_image_set_string


class XDS:

    def __init__(
            self,
            dataset,
            optimization=False,
            user_space_group=None,
            user_unit_cell=None,
            user_resolution_cutoff=None,
            user_native=True,
            user_start=None,
            user_end=None,
            user_percentage=None,
            proc_dir=None,
            strategy=False,
            njobs=None,
            nproc=None,
            use_slurm=True,
            reference_hkl=None,
            user_model=None,
            beamstop_radius=XdsConfig.DEFAULT_BEAMSTOP_RADIUS,
            initial_results=None,
            use_redis=False,
            pipeline_status_id=None,
            pipeline_params: Optional[dict] = None,
            extra_xds_inp_params: Optional[Dict[str, Any]] = None,
    ):
        self.dataset = dataset
        self.metadata = self.dataset.get_metadata()
        self.extra_xds_inp_params = extra_xds_inp_params or {}
        self.optimization = optimization
        self.proc_dir = proc_dir
        self.nproc = nproc
        self.njobs = njobs
        self.friedellaw = "TRUE" if user_native else "FALSE"
        self.use_slurm = use_slurm
        self.reference_hkl = reference_hkl
        self.strategy = strategy
        self.user_model = user_model
        self.use_redis = use_redis

        self.user_space_group = user_space_group
        if self.user_space_group:
            self.user_space_group_number = Symmetry.symbol_to_number(
                self.user_space_group
            )
        else:
            self.user_space_group_number = None
        self.user_unit_cell = user_unit_cell

        logger.info(
            f"User space group: {self.user_space_group} {self.user_space_group_number} unit cell: {self.user_unit_cell}"
        )

        self.user_resolution_cutoff = user_resolution_cutoff

        self.master_file = self._get_metadata("master_file")
        self.prefix = self._get_metadata("prefix")
        self.template = self._get_metadata("template")
        self.nimages = self._get_metadata(MetaData.NIMAGES.value)

        self.user_start = self._get_metadata("start", 1)
        if user_start:
            self.user_start = user_start

        if user_percentage:
            if user_percentage <= 1:
                user_percentage = user_percentage * 100
            user_end = (
                    int(float(user_percentage) * (self.nimages - self.user_start + 1) / 100)
                    + self.user_start
                    - 1
            )

        if user_end:
            self.user_end = user_end
        else:
            self.user_end = self._get_metadata("end")

        nimages_used = self.user_end - self.user_start + 1
        if self.njobs is None:
            self.njobs = self.njobs or int(max(4, nimages_used // 1000 + 1))
        if self.nproc is None:
            self.nproc = self.nproc or int(min(32, 8 * (nimages_used // 500 + 1)))

        self._setup_paths()
        self._validate_metadata()

        # --- PipelineTracker Integration ---
        # The tracker will manage DB and Redis connections and state IDs
        # Get default from central config
        redis_host = ServerConfig.get_redis_hosts().get("analysis_results", "127.0.0.1")
        redis_config = {"host": redis_host, "db": 0} if self.use_redis else None

        dataset_paths_json = json.dumps([self.master_file])
        
        # Standardize imageSet string using the common utility
        # We pass start and end images; the utility detects it as a contiguous range
        run_map = {self.master_file: [self.user_start, self.user_end]}
        self.image_set_str = get_image_set_string(run_map)

        # If pipeline_params are not provided, create a minimal dictionary for backward compatibility
        if pipeline_params is None:
            pipeline_params = {
                "sampleName": self.prefix,
                "imagedir": os.path.dirname(self.master_file),
                "logfile": os.path.join(self.proc_dir, "xds.log"),
                "command": " ".join(sys.argv),
                "workdir": self.proc_dir,
                "beamline": get_beamline_from_hostname(),
                "imageSet": self.image_set_str,
                "datasets": dataset_paths_json,
            }
            logger.warning(
                "No pipeline_params provided to XDS; using minimal defaults. This may limit functionality."
            )

        if "imageSet" not in pipeline_params:
            pipeline_params["imageSet"] = self.image_set_str

        if "datasets" not in pipeline_params:
            pipeline_params["datasets"] = dataset_paths_json

        self.logfile = pipeline_params.get(
            "logfile", os.path.join(self.proc_dir, "xds.log")
        )

        self.tracker = PipelineTracker(
            pipeline_name="xds",
            run_identifier=self.master_file,
            initial_params=pipeline_params,
            result_mapper=self._get_sql_mapped_results,
            redis_config=redis_config,
            existing_pipeline_status_id=pipeline_status_id,
        )

        # The following block is the same as the original file
        nx = self._get_metadata(MetaData.X_PIXELS.value)
        ny = self._get_metadata(MetaData.Y_PIXELS.value)
        gaps = get_detector_gaps(nx, ny)

        silicon = None
        detector_name = self._get_metadata(MetaData.DETECTOR.value, "").upper()
        if "CDTE" in detector_name:
            from .signal_CdTe import cal_silicon_CdTe_NIST

            silicon = round(
                cal_silicon_CdTe_NIST(self._get_metadata(MetaData.WAVELENGTH.value))
            )

        orgx = self._get_metadata(MetaData.BEAM_CENTER_X.value)
        orgy = self._get_metadata(MetaData.BEAM_CENTER_Y.value)
        osc = self._get_metadata(MetaData.OMEGA_RANGE.value)
        osc = osc if osc > 1e-4 else 1e-4
        beamstop = f"{int(orgx - beamstop_radius)} {int(orgx + beamstop_radius)} {int(orgy - beamstop_radius)} {int(orgy + beamstop_radius)}"
        beamstop_arm = (
            f"{0} {int(orgx)} {int(orgy - beamstop_radius)} {int(orgy + beamstop_radius)}"
        )
        gaps.append(beamstop_arm)

        self.xds_inp = {
            "DETECTOR": "EIGER",
            "MINIMUM_VALID_PIXEL_VALUE": 0,
            "OVERLOAD": self._get_metadata(MetaData.OVERLOAD.value),
            "SENSOR_THICKNESS": self._get_metadata(MetaData.SENSOR_THICKNESS.value),
            "SILICON": silicon,
            "QX": self._get_metadata(MetaData.X_PIXEL_SIZE.value),
            "QY": self._get_metadata(MetaData.Y_PIXEL_SIZE.value),
            "NX": nx,
            "NY": ny,
            "UNTRUSTED_RECTANGLE": gaps,
            "UNTRUSTED_ELLIPSE": [beamstop],
            "UNTRUSTED_QUADRILATERAL": [],
            "EXCLUDE_RESOLUTION_RANGE": [],
            "TRUSTED_REGION": "0.0 1.41",
            "DIRECTION_OF_DETECTOR_X-AXIS": "1 0 0",
            "DIRECTION_OF_DETECTOR_Y-AXIS": "0 1 0",
            "ORGX": self._get_metadata(MetaData.BEAM_CENTER_X.value),
            "ORGY": self._get_metadata(MetaData.BEAM_CENTER_Y.value),
            "DETECTOR_DISTANCE": self._get_metadata(MetaData.DETECTOR_DISTANCE.value),
            "GAIN": 1.0,
            "ROTATION_AXIS": "1.0 0.0 0.0",
            "OSCILLATION_RANGE": osc,
            "X-RAY_WAVELENGTH": self._get_metadata(MetaData.WAVELENGTH.value),
            "INCIDENT_BEAM_DIRECTION": "0.0 0.0 1.0",
            "FRACTION_OF_POLARIZATION": 0.99,
            "POLARIZATION_PLANE_NORMAL": "0.0 1.0 0.0",
            "SPACE_GROUP_NUMBER": (
                self.user_space_group_number
                if self.user_space_group_number and self.user_unit_cell
                else 0
            ),
            "UNIT_CELL_CONSTANTS": (
                self.user_unit_cell
                if self.user_space_group_number and self.user_unit_cell
                else "1 1 1 90 90 90"
            ),
            "NAME_TEMPLATE_OF_DATA_FRAMES": self.template,
            "DATA_RANGE": f"{self.user_start} {self.user_end}",
            "BACKGROUND_RANGE": f"1 {min(5, self.user_end - self.user_start + 1)}",
            "SPOT_RANGE": [
                f"{self.user_start} {int((self.user_end - self.user_start + 1) // 2) or self.user_start}"
            ],
            "EXCLUDE_DATA_RANGE": [],
            "REFINE(IDXREF)": "POSITION BEAM ORIENTATION CELL AXIS",
            "REFINE(INTEGRATE)": " POSITION BEAM ORIENTATION CELL",
            "REFINE(CORRECT)": "POSITION BEAM ORIENTATION CELL AXIS",
            "INCLUDE_RESOLUTION_RANGE": "50 0.3",
            "STARTING_ANGLE": self._get_metadata(MetaData.STARTING_OMEGA.value),
            "STARTING_ANGLES_OF_SPINDLE_ROTATION": "0.0 180.0 10.0",
            "TOTAL_SPINDLE_ROTATION_RANGES": "10.0 180.0 10.0",
            "NUMBER_OF_PROFILE_GRID_POINTS_ALONG_ALPHA/BETA": 9,
            "NUMBER_OF_PROFILE_GRID_POINTS_ALONG_GAMMA": 9,
            "SEPMIN": 4.0,
            "MINIMUM_NUMBER_OF_PIXELS_IN_A_SPOT": 3,
            "MINIMUM_FRACTION_OF_INDEXED_SPOTS": 0.5,
            "MAXIMUM_ERROR_OF_SPOT_POSITION": 2.0,
            "FRIEDEL'S_LAW": self.friedellaw,
            "VALUE_RANGE_FOR_TRUSTED_DETECTOR_PIXELS": "6000 30000",
            "CLUSTER_RADIUS": 2,
            "RELRAD": 5,
            "LIB": XdsConfig.LIB_PATH,
            "MAXIMUM_NUMBER_OF_JOBS": self.njobs,
            "MAXIMUM_NUMBER_OF_PROCESSORS": self.nproc,
            "JOB": "ALL !XYCORR INIT COLSPOT IDXREF DEFPIX XPLAN INTEGRATE CORRECT",
            "REFERENCE_DATA_SET": self.reference_hkl,
        }
        if "EIGER" in detector_name:
            self.xds_inp["DETECTOR"] = "EIGER"
        elif "PILATUS" in detector_name:
            self.xds_inp["DETECTOR"] = "PILATUS"

        if self.extra_xds_inp_params:
            self.xds_inp.update(self.extra_xds_inp_params)
            logger.info(f"Applied extra XDS.INP parameters: {self.extra_xds_inp_params}")

        self.xdsconv_inp = {
            "INPUT_FILE": f"{self.xds_ascii_hkl_file}",
            "OUTPUT_FILE": f"{self.processed_hkl_file} CCP4_I+F ! or CCP4_I or CCP4_F or SHELX or CNS",
            "FRIEDEL'S_LAW": f"FALSE ! self.friedellaw use FALSE regardless suggested by Kay",
            "GENERATE_FRACTION_OF_TEST_REFLECTIONS": "0.05",
            "!SPACE_GROUP_NUMBER": "96",
            "!UNIT_CELL_CONSTANTS": "30 30 70 90 90 90",
        }

        self.results = {}
        if initial_results:
            self.results.update(initial_results)

    def _setup_paths(self):
        """Defines standard file paths within the processing directory."""
        self.xds_inp_file = os.path.join(self.proc_dir, Filenames.XDS_INPUT)
        self.xdsconv_inp_file = os.path.join(self.proc_dir, Filenames.XDSCONV_INPUT)
        self.f2mtz_inp_file = os.path.join(self.proc_dir, Filenames.F2MTZ_INPUT)
        self.idxref_lp_file = os.path.join(self.proc_dir, Filenames.IDXREF_LP)
        self.xplan_lp_file = os.path.join(self.proc_dir, Filenames.XPLAN_LP)
        self.correct_lp_file = os.path.join(self.proc_dir, Filenames.CORRECT_LP)
        self.spot_nxds_file = os.path.join(self.proc_dir, Filenames.NXDS_SPOT)
        self.colspot_lp_file = os.path.join(self.proc_dir, Filenames.COLSPOT_LP)
        self.integrate_lp_file = os.path.join(self.proc_dir, Filenames.INTEGRATE_LP)
        self.xds_ascii_hkl_file = os.path.join(self.proc_dir, Filenames.XDS_ASCII_HKL)
        self.pointless_xml_file = os.path.join(self.proc_dir, Filenames.POINTLESS_XML)
        self.pointless_log_file = os.path.join(self.proc_dir, Filenames.POINTLESS_LOG)
        self.gxparm_file = os.path.join(self.proc_dir, Filenames.GXPARM)
        self.xparm_file = os.path.join(self.proc_dir, Filenames.XPARM)
        self.processed_hkl_file = os.path.join(self.proc_dir, f"{self.prefix}.hkl")
        self.processed_mtz_file = os.path.join(self.proc_dir, f"{self.prefix}.mtz")
        self.nxds_json_path = os.path.abspath(
            os.path.join(self.proc_dir, Filenames.NXDS_JSON)
        )
        self.xds_json_path = os.path.abspath(
            os.path.join(self.proc_dir, Filenames.XDS_JSON)
        )
        self.xds_json_stats_path = os.path.abspath(
            os.path.join(self.proc_dir, Filenames.XDS_STATS_JSON)
        )
        self.html_report_path = os.path.abspath(
            os.path.join(self.proc_dir, f"{self.prefix}.html")
        )

        self.Rcsb = RCSB(self.proc_dir)
        if not os.path.isdir(self.proc_dir):
            try:
                os.makedirs(self.proc_dir, exist_ok=True)
                logger.info("Created processing directory: %s", self.proc_dir)
            except OSError as e:
                logger.error(
                    "Failed to create processing directory %s: %s", self.proc_dir, e
                )
                raise

    def _get_metadata(
            self, key: str, default: Any = None, required: bool = False
    ) -> Any:
        value = self.metadata.get(key, default)
        if value is default and required:
            logger.error(f"Missing required metadata key: {key}")
            raise ValueError(f"Missing required metadata key: {key}")
        if value is default and not required:
            logger.debug(f"Optional metadata key '{key}' not found, using default: {default}")
        return value

    def _validate_metadata(self):
        logger.info("Validating required metadata keys...")
        for field in MetaData:
            self._get_metadata(field.value, required=False)
        logger.info(
            "Metadata validation complete (all keys treated as optional for now)."
        )

    def check_job_status(self, directory=None, last_step="CORRECT", job_steps=None):
        if directory is None:
            directory = self.proc_dir
        if job_steps is None:
            job_steps = XdsConfig.JOB_STEPS
        try:
            idx = job_steps.index(last_step.upper())
        except ValueError:
            logger.error(f"Invalid last_step provided: {last_step}")
            return last_step
        for step in job_steps[: idx + 1]:
            if (
                    last_step != "XPLAN"
                    and step == "XPLAN"
                    and "XPLAN" not in self.xds_inp.get("JOB", "")
            ):
                continue
            if (
                    last_step != "POWDER"
                    and step == "POWDER"
                    and "POWDER" not in self.xds_inp.get("JOB", "")
            ):
                continue
            success = False
            log_file = os.path.join(directory, f"{step}.LP")
            if not os.path.exists(log_file):
                logger.warning(
                    f"Log file not found for step {step}: {log_file}. Assuming failure if step was intended."
                )
                if step in self.xds_inp.get(
                        "JOB", ""
                ).split() or "ALL" in self.xds_inp.get("JOB", ""):
                    return step
                else:
                    continue
            try:
                with open(log_file, "r") as f:
                    lines = f.readlines()
                    # Check for errors in the last 20 lines
                    for line in lines[-20:]:
                        if "!!! ERROR" in line:
                            if "INSUFFICIENT PERCENTAGE" in line:
                                logger.warning(
                                    f"Ignored error in {step}: {line.strip()}. Continuing processing as suggested by XDS."
                                )
                                success = True
                            else:
                                success = False
                                break # Found a real error, stop checking
                        else:
                             # If no error in this line, keep success assumption (initially False? No, loop logic needs care)
                             pass
                    
                    # Logic fix:
                    # We want to detect if there is ANY unhandled error.
                    # Let's refine:
                    error_found = False
                    for line in lines[-20:]:
                        if "!!! ERROR" in line:
                            if "INSUFFICIENT PERCENTAGE" in line:
                                logger.warning(
                                    f"Ignored non-fatal error in {step}: {line.strip()}"
                                )
                            else:
                                error_found = True
                                break
                    
                    if not error_found:
                        success = True
                        
            except IOError as e:
                logger.error(f"Could not read log file {log_file}: {e}")
                if step in self.xds_inp.get(
                        "JOB", ""
                ).split() or "ALL" in self.xds_inp.get("JOB", ""):
                    return step
                else:
                    continue
            if not success:
                logger.info(f"XDS step {step} failed or log indicates error.")
                return step
        logger.info(
            f"All XDS steps up to {last_step} completed successfully or were skipped."
        )
        return None

    def generate_xds_inp(self):
        lines = []
        for key, value in self.xds_inp.items():
            if value is None or (isinstance(value, (list, tuple)) and not value):
                continue
            if isinstance(value, (list, tuple)):
                lines.extend(f"{key}= {element}" for element in value)
            else:
                lines.append(f"{key}= {value}")
        output = "\n".join(lines) + "\n"
        try:
            with open(self.xds_inp_file, "w") as file:
                file.write(output)
            logger.info(f"Generated {self.xds_inp_file}")
        except IOError as e:
            logger.error(f"Failed to write to file {self.xds_inp_file}: {e}")
            raise IOError(f"Failed to write to file {self.xds_inp_file}: {e}")
        return output

    def pointless_rerun_if_needed(self, pipeline_status_id):
        setup_logging(is_multiprocess_worker=True)

        if not os.path.exists(self.correct_lp_file):
            logger.warning(
                f"{self.correct_lp_file} not found, skipping pointless rerun."
            )
            return

        logger.info("start to run pointless...")
        self.run_pointless()

        if "SPACE_GROUP_NUMBER" not in self.results:
            logger.error(
                "XDS space group number not found in results. Cannot compare with Pointless."
            )
            return

        if (
                "pointless_best_solution" not in self.results
                or not self.results["pointless_best_solution"]
        ):
            logger.error("Pointless results not found. Cannot compare.")
            return

        try:
            xds_sg = int(self.results["SPACE_GROUP_NUMBER"])
            pointless_solution = self.results["pointless_best_solution"]
            pointless_sg = int(pointless_solution.get("CCP4_SGnumber"))
            pointless_cell = pointless_solution.get("UnitCell")
        except (ValueError, TypeError, AttributeError) as e:
            logger.error(f"Could not parse pointless/XDS results for comparison: {e}")
            return

        if pointless_cell is None:
            logger.error("Pointless UnitCell not found.")
            return

        logger.info(
            f"Pointless suggested space group: {pointless_sg}, XDS solution: {xds_sg}."
        )
        if Symmetry.same_point_group(xds_sg, pointless_sg):
            logger.info("XDS and Pointless agree on the point group.")
            return

        proc_dir = os.path.join(self.proc_dir, f"spg{pointless_sg}")
        os.makedirs(proc_dir, exist_ok=True)
        logger.info(f"Rerunning XDS in space group: {pointless_sg} in {proc_dir}")

        new_xds = XDS(
            self.dataset,
            proc_dir=proc_dir,
            use_slurm=self.use_slurm,
            njobs=self.njobs,
            nproc=self.nproc,
            user_space_group=str(pointless_sg),
            user_unit_cell=pointless_cell,
            user_start=self.user_start,
            user_end=self.user_end,
            user_native=(self.friedellaw == "TRUE"),
            user_resolution_cutoff=self.user_resolution_cutoff,
            optimization=self.optimization,
            reference_hkl=self.reference_hkl,
            strategy=self.strategy,
            user_model=self.user_model,
            initial_results=self.results,
            use_redis=self.use_redis,
            pipeline_status_id=pipeline_status_id,  # Pass the ID to attach the tracker
        )
        new_xds.process()
        logger.info(
            f"Completed pointless-triggered rerun in {proc_dir} for space group: {pointless_sg}"
        )

    def xds_init(self):
        self.xds_inp.update({"JOB": "XYCORR INIT"})
        self.generate_xds_inp()
        bg_range_str = self.xds_inp.get("BACKGROUND_RANGE")
        if bg_range_str:
            if hasattr(self.dataset, "wait_for_datafiles") and callable(
                    getattr(self.dataset, "wait_for_datafiles")
            ):
                missing_files = self.dataset.wait_for_datafiles(
                    [bg_range_str], wait=True
                )
                if missing_files:
                    logger.error(
                        f"Missing data files for background range, cannot proceed with INIT: {missing_files}"
                    )
                    return "INIT"
            else:
                logger.warning(
                    "Dataset object does not support wait_for_datafiles. Proceeding without check."
                )
        else:
            logger.warning("BACKGROUND_RANGE not defined in XDS.INP for INIT step.")
        self.run()
        err = self.check_job_status(last_step="INIT")
        return err

    def alternative_index1(self):
        self.xds_inp.update({"JOB": "COLSPOT IDXREF"})
        spot_end = self.user_start + (self.user_end - self.user_start + 1) // 2 - 1
        if spot_end < self.user_start:
            spot_end = self.user_start
        spot_range_val = f"{self.user_start} {spot_end}"
        self.xds_inp.update({"SPOT_RANGE": [spot_range_val]})
        self.generate_xds_inp()
        if hasattr(self.dataset, "wait_for_datafiles") and callable(
                getattr(self.dataset, "wait_for_datafiles")
        ):
            missing_files = self.dataset.wait_for_datafiles(
                self.xds_inp["SPOT_RANGE"], wait=True
            )
            if missing_files:
                logger.error(
                    f"Missing data files for SPOT_RANGE (alt1), cannot proceed with IDXREF: {missing_files}"
                )
                return "IDXREF"
        else:
            logger.warning(
                "Dataset object does not support wait_for_datafiles for SPOT_RANGE (alt1)."
            )
        self.run()
        err = self.check_job_status(last_step="IDXREF")
        return err

    def alternative_index2(self):
        self.xds_inp.update({"SPOT_RANGE": [f"{self.user_start} {self.user_end}"]})
        self.xds_inp.update({"JOB": "COLSPOT IDXREF"})
        self.generate_xds_inp()
        if hasattr(self.dataset, "wait_for_datafiles") and callable(
                getattr(self.dataset, "wait_for_datafiles")
        ):
            missing_files = self.dataset.wait_for_datafiles(
                self.xds_inp["SPOT_RANGE"], wait=True
            )
            if missing_files:
                logger.error(
                    f"Missing data files for SPOT_RANGE (alt2), cannot proceed with IDXREF: {missing_files}"
                )
                return "IDXREF"
        else:
            logger.warning(
                "Dataset object does not support wait_for_datafiles for SPOT_RANGE (alt2)."
            )
        self.run()
        err = self.check_job_status(last_step="IDXREF")
        return err

    def fast_index(self):
        self.set_fastdp_spot_ranges()
        self.xds_inp.update({"JOB": "COLSPOT IDXREF"})
        self.generate_xds_inp()
        if hasattr(self.dataset, "wait_for_datafiles") and callable(
                getattr(self.dataset, "wait_for_datafiles")
        ):
            missing_files = self.dataset.wait_for_datafiles(
                self.xds_inp["SPOT_RANGE"], wait=True
            )
            if missing_files:
                logger.error(
                    f"Missing data files for fast_index SPOT_RANGE, cannot proceed with IDXREF: {missing_files}"
                )
                return "IDXREF"
        else:
            logger.warning(
                "Dataset object does not support wait_for_datafiles for fast_index SPOT_RANGE."
            )
        self.run()
        err = self.check_job_status(last_step="IDXREF")
        return err

    def xplan(self):
        if not self.user_space_group_number or not self.user_unit_cell:
            auto_index_spacegroup = self.results.get("auto_index_spacegroup", None)
            auto_index_unitcell_list = self.results.get("auto_index_unitcell", None)
            if auto_index_spacegroup and auto_index_unitcell_list:
                if isinstance(auto_index_unitcell_list, list):
                    auto_index_unitcell_str = " ".join(
                        map(str, auto_index_unitcell_list)
                    )
                else:
                    auto_index_unitcell_str = str(auto_index_unitcell_list)
                self.xds_inp.update(
                    {
                        "SPACE_GROUP_NUMBER": auto_index_spacegroup,
                        "UNIT_CELL_CONSTANTS": auto_index_unitcell_str,
                        "JOB": "IDXREF",
                    }
                )
                logger.info(
                    f"Rerunning IDXREF with auto-determined SG: {auto_index_spacegroup} and Cell: {auto_index_unitcell_str}"
                )
                self.generate_xds_inp()
                self.run()
                err_idxref_rerun = self.check_job_status(last_step="IDXREF")
                if err_idxref_rerun:
                    logger.error(f"IDXREF rerun for XPLAN failed: {err_idxref_rerun}")
                    return err_idxref_rerun
                idxref_results_rerun = parse_idxref_lp(
                    self.idxref_lp_file,
                    user_space_group=Symmetry.number_to_symbol(auto_index_spacegroup),
                )
                self.results.update(idxref_results_rerun)
        self.xds_inp.update(
            {"INCLUDE_RESOLUTION_RANGE": "50 2.0", "JOB": "DEFPIX XPLAN"}
        )
        self.generate_xds_inp()
        logger.info("Running XPLAN...")
        self.run()
        err = self.check_job_status(last_step="XPLAN")
        if err:
            logger.error(f"XPLAN failed: {err}")
        else:
            logger.info("XPLAN completed successfully.")
        return err

    def _run_xds_steps(
            self, job_definition: str, last_step_to_check: str
    ) -> Optional[str]:
        self.xds_inp["JOB"] = job_definition
        self.generate_xds_inp()
        logger.info(f"Running XDS job(s): {job_definition}")
        self.run()
        error_step = self.check_job_status(last_step=last_step_to_check)
        if error_step:
            logger.error(f"XDS failed during '{job_definition}' at step: {error_step}")
        return error_step

    def _handle_error(self, step: str, message: str, detail: Optional[str] = None):
        logger.error(f"Processing failed at step '{step}': {message}")
        self.results["error_step"] = step
        self.results["error_message"] = message
        if detail:
            self.results["error_detail"] = detail
        self.results["status"] = "FAILED"

    def _run_initialization(self) -> bool:
        if self.xds_init():
            self._handle_error("INIT", "XDS initialization (XYCORR, INIT) failed.")
            return False
        logger.info("XDS initialization completed successfully.")
        return True

    def _run_indexing(self) -> bool:
        indexing_methods = [
            self.fast_index,
            self.alternative_index1,
            self.alternative_index2,
        ]
        
        if self.optimization:
            # If optimization is requested, we MUST use the full data range for spot finding
            # so that SPOT.XDS contains enough information for later frame rejection logic.
            logger.info("Optimization enabled: switching to full-range indexing (alternative_index2).")
            indexing_methods = [self.alternative_index2]

        for method in indexing_methods:
            logger.info(f"Attempting indexing with: {method.__name__}")
            if method() is None:
                logger.info(f"Indexing successful with {method.__name__}.")
                if not os.path.exists(self.idxref_lp_file):
                    self._handle_error(
                        "IDXREF_PARSE",
                        f"{self.idxref_lp_file} not found after successful indexing.",
                    )
                    return False
                idxref_results = parse_idxref_lp(
                    self.idxref_lp_file, user_space_group=self.user_space_group
                )
                if not idxref_results:
                    self._handle_error(
                        "IDXREF_PARSE", "Failed to parse IDXREF.LP after indexing."
                    )
                    return False
                self.results.update(idxref_results)
                return True
        self._handle_error("IDXREF", "All available auto-indexing methods failed.")
        return False

    def _run_strategy_if_needed(self) -> bool:
        is_few_images = (self.user_end - self.user_start + 1) <= 5
        if not (self.strategy or is_few_images):
            return True
        logger.info("Running XPLAN for strategy determination.")
        if self.xplan() is not None:
            self._handle_error("XPLAN", "XPLAN failed during strategy determination.")
            return False
        if os.path.exists(self.xplan_lp_file):
            xplan_results = parse_xplan_lp(self.xplan_lp_file)
            if xplan_results:
                self.results.update(xplan_results)
        else:
            logger.warning(f"{self.xplan_lp_file} not found after XPLAN run.")
        return True

    def _refine_indexing_with_user_input(self) -> bool:
        cell_from_idxref = self.results.get("user_unit_cell")
        if self.user_space_group and self.user_unit_cell is None and cell_from_idxref:
            self.user_unit_cell = (
                " ".join(map(str, cell_from_idxref))
                if isinstance(cell_from_idxref, list)
                else str(cell_from_idxref)
            )
            logger.info(
                f"Re-running IDXREF with user-supplied SG {self.user_space_group} and auto-determined cell {self.user_unit_cell}"
            )
            self.set_user_space_group()
            if not os.path.exists(self.idxref_lp_file):
                self._handle_error(
                    "IDXREF_REPARSE",
                    f"{self.idxref_lp_file} not found after space group update.",
                )
                return False
            idxref_results_updated = parse_idxref_lp(
                self.idxref_lp_file, user_space_group=self.user_space_group
            )
            if not idxref_results_updated:
                self._handle_error(
                    "IDXREF_REPARSE",
                    "Failed to re-parse IDXREF.LP after setting user space group.",
                )
                return False
            self.results.update(idxref_results_updated)
        return True

    def _run_integration_and_scaling(self) -> bool:
        data_range_str = self.xds_inp.get("DATA_RANGE")
        if hasattr(self.dataset, "wait_for_datafiles") and callable(
                getattr(self.dataset, "wait_for_datafiles")
        ):
            missing_files = self.dataset.wait_for_datafiles([data_range_str], wait=True)
            if missing_files:
                self._handle_error(
                    "DATA_WAIT",
                    f"Missing required data files for integration: {missing_files}",
                )
                return False
        self.set_resolution_cutoff()
        error_step = self._run_xds_steps("DEFPIX INTEGRATE CORRECT", "CORRECT")
        if error_step:
            self._handle_error(
                error_step, f"Main processing failed at step {error_step}."
            )
            return False
        if not os.path.exists(self.correct_lp_file):
            self._handle_error(
                "CORRECT_PARSE", f"{self.correct_lp_file} not found after processing."
            )
            return False
        correct_results = parse_correct_lp(self.correct_lp_file)
        if not correct_results:
            self._handle_error(
                "CORRECT_PARSE", f"Failed to parse {self.correct_lp_file}."
            )
            return False
        self.results.update(correct_results)
        return True

    def _search_rcsb(self):
        current_unit_cell = self.results.get("UNIT_CELL_CONSTANTS")
        current_sg_number = self.results.get("SPACE_GROUP_NUMBER")
        if current_unit_cell and current_sg_number:
            try:
                sg_num_int = int(current_sg_number)
                pdbfile = self.Rcsb.search_with_unit_cell_and_spg(
                    current_unit_cell,
                    sg_num_int,
                    edge_err=XdsConfig.UNITCELL_EDGE_ERR_TOLERANCE,
                    angle_err=XdsConfig.UNITCELL_ANGLE_ERR_TOLERANCE,
                )
                self.results.update({"pdbfile": pdbfile})
            except (ValueError, TypeError) as e:
                logger.error(
                    f"Could not perform RCSB search due to invalid space group number: {current_sg_number} - {e}"
                )
            except Exception as e:
                logger.error(f"An unexpected error occurred during RCSB search: {e}")
        else:
            logger.warning(
                "Unit cell or space group not available. Skipping RCSB search."
            )

    def _refine_resolution_iteratively(self):
        if self.user_resolution_cutoff is not None:
            logger.info(
                "Skipping iterative resolution refinement as a user-defined cutoff was provided."
            )
            return

        CORRECT_NROUNDS = 2
        TERMINATION_THRESHOLD = 0.1
        current_res_cutoff = self.results.get("resolution_based_on_cchalf")

        for nround in range(CORRECT_NROUNDS):
            if not current_res_cutoff:
                logger.info(
                    "No CC1/2-based resolution found. Stopping iterative refinement."
                )
                break

            logger.info(
                f"Resolution refinement round {nround + 1}, new cutoff: {current_res_cutoff:.2f} Å"
            )
            self.set_resolution_cutoff(current_res_cutoff)
            
            # --- Modification: Update Space Group and Unit Cell if available ---
            current_sg = self.results.get(
                "SPACE_GROUP_NUMBER", self.xds_inp.get("SPACE_GROUP_NUMBER")
            )
            # Prefer refined unit cell if available
            current_cell = self.results.get(
                "REFINED_UNIT_CELL_CONSTANT", 
                self.results.get(
                    "UNIT_CELL_CONSTANTS", self.xds_inp.get("UNIT_CELL_CONSTANTS")
                )
            )
            
            if current_sg:
                logger.info(f"Updating SPACE_GROUP_NUMBER for refinement: {current_sg}")
                self.xds_inp["SPACE_GROUP_NUMBER"] = current_sg
            if current_cell:
                # If current_cell is a list, convert to string
                if isinstance(current_cell, list):
                    current_cell_str = " ".join(map(str, current_cell))
                else:
                    current_cell_str = str(current_cell)
                    
                logger.info(
                    f"Updating UNIT_CELL_CONSTANTS for refinement: {current_cell_str}"
                )
                self.xds_inp["UNIT_CELL_CONSTANTS"] = current_cell_str
            # ----------------------------------------------------------------


            if self._run_xds_steps("CORRECT", "CORRECT") is not None:
                logger.error(
                    "CORRECT step failed during iterative refinement. Halting refinement."
                )
                break

            rerun_results = (
                parse_correct_lp(self.correct_lp_file)
                if os.path.exists(self.correct_lp_file)
                else None
            )
            if not rerun_results:
                logger.warning(
                    "Failed to parse CORRECT.LP after refinement run. Halting refinement."
                )
                break

            self.results.update(rerun_results)
            new_res_cutoff = rerun_results.get("resolution_based_on_cchalf")

            if (
                    not new_res_cutoff
                    or abs(current_res_cutoff - new_res_cutoff) < TERMINATION_THRESHOLD
            ):
                logger.info("Resolution refinement has converged.")
                break
            current_res_cutoff = new_res_cutoff

    def _get_sql_mapped_results(self, results_dict: dict) -> dict:
        """
        Returns a dictionary of results with keys mapped to the SQL column names.
        This function is now used as the `result_mapper` for PipelineTracker.
        """
        # Extract overall stats from the 'total' line of the summary table
        total_stats = {}
        if results_dict.get("table1_total"):
            # Header has 14 items, but the 'total' line often has 13 (missing resolution)
            header = results_dict.get("table1_header", [])
            total_values = results_dict["table1_total"]
            # Safely create a dictionary
            total_stats = {
                header[i + 1].lower(): total_values[i + 1]
                for i in range(len(total_values) - 1)
            }

        spgn = results_dict.get("SPACE_GROUP_NUMBER", "")
        spg_symbol = spgn
        if spgn:
            try:
                spg_symbol = Symmetry.number_to_symbol(int(spgn)).replace(" ", "")
                logger.info(
                    f"convert space group from xds {spgn} to symbol: {spg_symbol}"
                )
            except ValueError:
                logger.warning(
                    "Unable to look up space group symbol for spg number: {spgn}"
                )

        unit_cell_value = results_dict.get("UNIT_CELL_CONSTANTS", [])
        unit_cell_for_db = ""
        if isinstance(unit_cell_value, str):
            # If it's already a string, just normalize the whitespace and use it.
            unit_cell_for_db = " ".join(unit_cell_value.split())
        elif isinstance(unit_cell_value, (list, tuple)):
            # If it's a list/tuple (the originally expected format), join it.
            unit_cell_for_db = " ".join(map(str, unit_cell_value))

        def to_fraction(val_str):
            if not val_str:
                return ""
            try:
                # Remove common non-numeric chars and convert to float
                clean_val = str(val_str).replace("%", "").replace("*", "")
                # Divide by 100 to convert percentage to fraction
                return str(float(clean_val) / 100.0)
            except ValueError:
                return str(val_str)

        def format_wavelength(val):
            if not val:
                return ""
            try:
                # Round to at most 5 decimal places
                return str(round(float(val), 5))
            except ValueError:
                return str(val)

        mapped = {
            "sampleName": self.prefix,
            "imageSet": self.image_set_str,
            "firstFrame": str(self.user_start),
            "start": str(self.user_start),
            "end": str(self.user_end),
            "prefix": self.prefix,
            "workdir": self.proc_dir,
            "imagedir": os.path.dirname(self.master_file),
            "highresolution": str(
                results_dict.get("resolution_highres")
                or self.user_resolution_cutoff
                or results_dict.get("resolution_based_on_cchalf")
                or ""
            ),
            "spacegroup": str(spg_symbol),
            "unitcell": unit_cell_for_db,
            "wavelength": format_wavelength(
                results_dict.get("X-RAY_WAVELENGTH")
                or self.xds_inp.get("X-RAY_WAVELENGTH")
            ),
            "rmerge": to_fraction(total_stats.get("r_factor_observed", "")),
            "rmeas": to_fraction(total_stats.get("r_meas", "")),
            "rpim": str(
                results_dict.get("R-pim") or ""
            ),  # This is not in the XDS table
            "isigmai": str(total_stats.get("i_sigma", "")),
            "multiplicity": str(
                round(
                    float(total_stats.get("number_observed", 0))
                    / float(total_stats.get("number_unique", 1)),
                    2,
                )
            ),
            "completeness": str(total_stats.get("completeness", "")).replace("%", ""),
            "anom_completeness": str(results_dict.get("anomalous_completeness") or ""),
            "table1": results_dict.get("table1_text", ""),
            "scale_log": self.correct_lp_file,
            "truncate_mtz": self.processed_mtz_file,
            "run_stats": json.dumps(results_dict, default=str),
            "solve": str(results_dict.get("final_pdb") or ""),
            "isa": str(results_dict.get("ISa") or ""),
            "cchalf": to_fraction(total_stats.get("cc_half", "")),
            "nobs": str(total_stats.get("number_observed", "")),
            "nuniq": str(total_stats.get("number_unique", "")),
            "report_url": str(results_dict.get("report_url") or ""),
        }
        # Filter out None values to allow model defaults to take over
        return {k: v for k, v in mapped.items() if v is not None}

    def process(self):
        """
        Orchestrates the full XDS processing workflow using PipelineTracker.
        """
        self.tracker.start()

        if self.tracker.pipeline_status_id is None:
            logger.error(
                "Tracker failed to initialize and get a pipeline status ID. Aborting."
            )
            return

        log_queue = get_multiprocessing_queue()
        listener = start_queue_listener(log_queue)
        logger.info("Multiprocessing logging listener started.")
        pointless_process = None

        try:
            self.results["status"] = "INITIALIZING"
            self.tracker.update_progress("INITIALIZING", self.results)
            if not self._run_initialization():
                raise RuntimeError("Initialization failed")

            self.results["status"] = "INDEXING"
            self.tracker.update_progress("INDEXING", self.results)
            if not self._run_indexing():
                raise RuntimeError("Indexing failed")

            if not self._run_strategy_if_needed():
                if self.strategy:
                    logger.info("Strategy-only run complete.")
                    self.results["status"] = "DONE"
                    self.tracker.succeed(self.results)
                    return
                raise RuntimeError("Strategy determination failed")

            if not self._refine_indexing_with_user_input():
                raise RuntimeError("Refining indexing failed")

            self.results["status"] = "SCALING"
            self.tracker.update_progress("SCALING", self.results)
            if not self._run_integration_and_scaling():
                raise RuntimeError("Integration and scaling failed")

            self.results["status"] = "SCALED"
            self.tracker.update_progress("SCALED", self.results)

            if os.path.exists(self.xds_ascii_hkl_file):
                logger.info("Starting Pointless analysis in the background.")
                pointless_args = (self.tracker.pipeline_status_id,)
                pointless_process = Process(
                    target=self.pointless_rerun_if_needed, args=pointless_args
                )
                pointless_process.start()
            else:
                logger.warning(
                    f"{self.xds_ascii_hkl_file} not found, skipping Pointless."
                )

            self.results["status"] = "REFINING"
            self.tracker.update_progress("REFINING", self.results)
            self._search_rcsb()
            self._refine_resolution_iteratively()

            if self.optimization:
                logger.info("Starting parameter optimization...")
                self.results["status"] = "OPTIMIZING"
                self.tracker.update_progress("OPTIMIZING", self.results)
                self.optimize_with_revert()

            logger.info("Main processing tasks finished. Starting post-processing.")
            self.results["status"] = "POST-PROCESSING"
            self.tracker.update_progress("POST-PROCESSING", self.results)
            self.post_processing()

            self.create_summary()
            self.results["status"] = "DONE"
            self.tracker.succeed(self.results)

        except Exception as e:
            error_message = f"Processing failed: {e}"
            logger.error(error_message, exc_info=True)
            self._handle_error("FAILED", error_message)
            self.tracker.fail(error_message, self.results)
        finally:
            if pointless_process and pointless_process.is_alive():
                logger.info("Waiting for Pointless process to complete...")
                pointless_process.join(timeout=300)
                if pointless_process.is_alive():
                    logger.warning(
                        "Pointless process timed out and will be terminated."
                    )
                    pointless_process.terminate()

            logger.info("Stopping multiprocessing logging listener.")
            if "listener" in locals():
                listener.stop()

        logger.info(f"End of XDS processing run for {self.proc_dir}")

    def post_processing(self):
        self.run_xdsconv()
        self.run_dimple()
        self.run_shelx()

    def run_shelx(self):
        pass

    def optimize(
            self,
            xds_directory=None,
            max_iterations: int = XdsConfig.MAX_OPTIMIZE_ITERATIONS,
    ):
        if xds_directory is None:
            xds_directory = self.proc_dir
        source_xparm = None
        if os.path.exists(os.path.join(xds_directory, Filenames.GXPARM)):
            source_xparm = os.path.join(xds_directory, Filenames.GXPARM)
        elif os.path.exists(os.path.join(xds_directory, Filenames.XPARM)):
            source_xparm = os.path.join(xds_directory, Filenames.XPARM)
        if not source_xparm:
            logger.error(
                f"Neither {Filenames.GXPARM} nor {Filenames.XPARM} found in {xds_directory}. Cannot start optimization."
            )
            return
        target_xparm = os.path.join(xds_directory, Filenames.XPARM)
        if source_xparm == os.path.join(xds_directory, Filenames.GXPARM):
            try:
                with open(source_xparm, "r") as src, open(target_xparm, "w") as dst:
                    dst.write(src.read())
                logger.info(
                    f"Copied {source_xparm} to {target_xparm} for optimization."
                )
            except IOError as e:
                logger.error(f"Error copying {source_xparm} to {target_xparm}: {e}")
                return
        initial_ISa = self.results.get("ISa")
        if initial_ISa is None:
            logger.error("Initial ISa not found in results. Cannot start optimization.")
            if os.path.exists(self.correct_lp_file):
                correct_results_temp = parse_correct_lp(self.correct_lp_file)
                if correct_results_temp and "ISa" in correct_results_temp:
                    initial_ISa = correct_results_temp["ISa"]
                    self.results["ISa"] = initial_ISa
                    logger.info(
                        f"Fetched ISa={initial_ISa} from {self.correct_lp_file} for optimization."
                    )
                else:
                    logger.error(
                        f"Still no ISa after parsing {self.correct_lp_file} . Optimization aborted."
                    )
                    return
            else:
                logger.error(f"{self.correct_lp_file} not found. Optimization aborted.")
                return
        current_ISa = initial_ISa
        iteration = 0
        while iteration < max_iterations:
            iteration += 1
            logger.info(f"Optimization iteration: {iteration}")
            integrate_lp_path = os.path.join(xds_directory, Filenames.INTEGRATE_LP)
            if not os.path.exists(integrate_lp_path):
                logger.warning(
                    f"{integrate_lp_path} not found. Cannot get suggested params. Skipping update."
                )
            else:
                dict_integ_suggestions = parse_integrate_lp(integrate_lp_path)
                if dict_integ_suggestions:
                    self.xds_inp.update(dict_integ_suggestions)
                    logger.info(
                        f"XDS.INP updated with suggestions from INTEGRATE.LP: {dict_integ_suggestions}"
                    )
                else:
                    logger.info(
                        "No new suggestions found in INTEGRATE.LP or section not found."
                    )
            
            # --- Modification: Add IDXREF in first cycle if space group not provided ---
            if iteration == 1 and not self.user_space_group:
                 self.xds_inp.update({"JOB": "IDXREF INTEGRATE CORRECT"})
            else:
                 self.xds_inp.update({"JOB": "INTEGRATE CORRECT"})
            # -------------------------------------------------------------------------

            self.xds_inp.update(
                {
                    "NUMBER_OF_PROFILE_GRID_POINTS_ALONG_ALPHA/BETA": 19,
                    "NUMBER_OF_PROFILE_GRID_POINTS_ALONG_GAMMA": 19,
                }
            )
            
            # --- Modification: Use Refined Parameters for Optimization ---
            if "REFINED_UNIT_CELL_CONSTANT" in self.results:
                self.xds_inp["UNIT_CELL_CONSTANTS"] = self.results["REFINED_UNIT_CELL_CONSTANT"]
            if "REFINED_DISTANCE" in self.results:
                self.xds_inp["DETECTOR_DISTANCE"] = self.results["REFINED_DISTANCE"]
            if "REFINED_MOSAICITY" in self.results:
                self.xds_inp["REFLECTING_RANGE_E.S.D."] = self.results["REFINED_MOSAICITY"]
            if "REFINED_ORXY" in self.results:
                refined_orxy = self.results["REFINED_ORXY"].split()
                if len(refined_orxy) == 2:
                    self.xds_inp["ORGX"] = refined_orxy[0]
                    self.xds_inp["ORGY"] = refined_orxy[1]
            # -----------------------------------------------------------

            res_cchalf = self.results.get("resolution_based_on_cchalf")
            if res_cchalf:
                self.set_resolution_cutoff(res_cchalf)
            else:
                self.set_resolution_cutoff(self.user_resolution_cutoff)
            self.generate_xds_inp()
            self.run(job_name=f"xds_opt_{iteration}")
            opt_job_status = self.check_job_status(last_step="CORRECT")
            if opt_job_status is not None:
                logger.error(
                    f"Optimization cycle failed at {opt_job_status}. Stopping optimization."
                )
                break
            correct_lp_path_opt = os.path.join(xds_directory, Filenames.CORRECT_LP)
            if not os.path.exists(correct_lp_path_opt):
                logger.error(
                    f"{correct_lp_path_opt} not found after optimization cycle. Cannot assess improvement."
                )
                break
            correct_result_opt = parse_correct_lp(correct_lp_path_opt)
            if not correct_result_opt or "ISa" not in correct_result_opt:
                logger.error(
                    "Failed to parse CORRECT.LP or ISa missing after optimization cycle."
                )
                break
            self.results.update(correct_result_opt)
            ISa_new = correct_result_opt["ISa"]
            logger.info(
                f"Optimization iteration {iteration}: ISa {current_ISa} --> {ISa_new}"
            )
            try:
                ISa_new_float = float(ISa_new)
                current_ISa_float = float(current_ISa)
                
                # --- Modification: Tolerant check for first iteration ---
                isa_diff = ISa_new_float - current_ISa_float
                threshold = XdsConfig.OPTIMIZE_ISA_THRESHOLD
                if iteration == 1:
                    threshold = XdsConfig.OPTIMIZE_ISA_TOLERANCE_INITIAL

                if isa_diff <= threshold:
                    logger.info(
                        f"ISa improvement ({isa_diff:.3f}) is below threshold ({threshold}). Stopping optimization."
                    )
                    break
                else:
                    current_ISa = ISa_new_float
                # --------------------------------------------------------
            except ValueError:
                logger.error(
                    f"Could not convert ISa values to float for comparison ({ISa_new}, {current_ISa}). Stopping optimization."
                )
                break
            gxparm_opt_path = os.path.join(xds_directory, Filenames.GXPARM)
            if os.path.exists(gxparm_opt_path):
                try:
                    with open(gxparm_opt_path, "r") as src, open(
                            target_xparm, "w"
                    ) as dst:
                        dst.write(src.read())
                    logger.info(
                        f"Copied {gxparm_opt_path} to {target_xparm} for next optimization iteration."
                    )
                except IOError as e:
                    logger.error(
                        f"Error copying {gxparm_opt_path} to {target_xparm} in optimization: {e}"
                    )
                    break
            else:
                logger.warning(
                    f"{gxparm_opt_path} not found after INTEGRATE/CORRECT in optimization. Using existing XPARM.XDS for next iteration."
                )
        if iteration >= max_iterations:
            logger.info(f"Reached maximum optimization iterations ({max_iterations}).")
        logger.info("Optimization finished.")

    def _identify_low_signal_ranges(self, min_spots=10):
        """
        Parses SPOT.XDS (or COLSPOT.LP if SPOT.XDS missing) to find frame ranges
        with very low spot counts.
        Returns a list of strings "start end" for EXCLUDE_DATA_RANGE.
        """
        spot_file = os.path.join(self.proc_dir, "SPOT.XDS")
        if not os.path.exists(spot_file):
            return []

        # Parse SPOT.XDS to get spots per frame
        # Format: x y z intensity
        # z corresponds to frame number (0-based or 1-based depending on indexing, usually float)
        # We'll bin by integer frame number.
        spots_per_frame = {}
        try:
            with open(spot_file, 'r') as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 3:
                        z = float(parts[2])
                        frame = int(z + 0.5) # Round to nearest integer frame
                        spots_per_frame[frame] = spots_per_frame.get(frame, 0) + 1
        except Exception as e:
            logger.warning(f"Failed to parse SPOT.XDS for low signal exclusion: {e}")
            return []

        # Identify ranges
        exclude_ranges = []
        current_start = None
        
        # We iterate through the user-defined data range
        for frame in range(self.user_start, self.user_end + 1):
            count = spots_per_frame.get(frame, 0)
            if count < min_spots:
                if current_start is None:
                    current_start = frame
            else:
                if current_start is not None:
                    exclude_ranges.append(f"{current_start} {frame - 1}")
                    current_start = None
        
        # Close any open range at the end
        if current_start is not None:
            exclude_ranges.append(f"{current_start} {self.user_end}")

        if exclude_ranges:
            logger.info(f"[Spot Analysis] Detected {len(exclude_ranges)} weak ranges (<{min_spots} spots): {exclude_ranges}")

        return exclude_ranges

    def _identify_weak_integration_ranges(self, min_reflections=10):
        """
        Parses INTEGRATE.LP to find frame ranges where the number of strong
        reflections is below a threshold.
        Returns a list of strings "start end" for EXCLUDE_DATA_RANGE.
        """
        if not os.path.exists(self.integrate_lp_file):
            return []

        try:
            per_frame_stats = parse_integrate_lp_per_frame(self.integrate_lp_file)
        except Exception as e:
            logger.warning(f"Failed to parse INTEGRATE.LP for weak signal identification: {e}")
            return []

        if not per_frame_stats:
            return []

        exclude_ranges = []
        current_start = None

        # Iterate through the data range
        for frame in range(self.user_start, self.user_end + 1):
            stats = per_frame_stats.get(frame)
            # If frame is missing from INTEGRATE.LP or reflections below threshold
            is_weak = (stats is None) or (stats.get("num_strong_refl", 0) < min_reflections)
            
            if is_weak:
                if current_start is None:
                    current_start = frame
            else:
                if current_start is not None:
                    exclude_ranges.append(f"{current_start} {frame - 1}")
                    current_start = None

        # Close any open range
        if current_start is not None:
            exclude_ranges.append(f"{current_start} {self.user_end}")

        if exclude_ranges:
            logger.info(f"[Integration Analysis] Detected {len(exclude_ranges)} weak ranges (<{min_reflections} reflections): {exclude_ranges}")

        return exclude_ranges

    def _update_exclude_ranges(self, new_ranges):
        """
        Merges new exclusion ranges with existing ones in self.xds_inp.
        consolidates overlapping or adjacent ranges.
        """
        existing_ranges = self.xds_inp.get("EXCLUDE_DATA_RANGE", [])
        if isinstance(existing_ranges, str):
            existing_ranges = [existing_ranges]
        
        # Parse all ranges into (start, end) tuples
        all_ranges = []
        for r_str in existing_ranges + new_ranges:
            try:
                s, e = map(int, r_str.split())
                all_ranges.append((s, e))
            except ValueError:
                continue
        
        if not all_ranges:
            return

        # Sort by start frame
        all_ranges.sort(key=lambda x: x[0])

        # Merge
        merged = []
        if all_ranges:
            curr_s, curr_e = all_ranges[0]
            for next_s, next_e in all_ranges[1:]:
                if next_s <= curr_e + 1: # Overlap or adjacent
                    curr_e = max(curr_e, next_e)
                else:
                    merged.append((curr_s, curr_e))
                    curr_s, curr_e = next_s, next_e
            merged.append((curr_s, curr_e))

        # Update XDS.INP
        self.xds_inp["EXCLUDE_DATA_RANGE"] = [f"{s} {e}" for s, e in merged]
        logger.info(f"Final EXCLUDE_DATA_RANGE (merged): {self.xds_inp['EXCLUDE_DATA_RANGE']}")

    def optimize_with_revert(
            self,
            xds_directory=None,
            max_iterations: int = XdsConfig.MAX_OPTIMIZE_ITERATIONS,
    ):
        if xds_directory is None:
            xds_directory = self.proc_dir

        def run_shell_command(command, cwd):
            full_command = f"bash -c 'set -e; {command}'"
            try:
                subprocess.run(
                    full_command,
                    shell=True,
                    cwd=cwd,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                logger.info(f"Successfully executed: {command}")
            except subprocess.CalledProcessError as e:
                if command.strip().startswith("cp") and (
                        "No such file or directory" in e.stderr or "cannot stat" in e.stderr
                ):
                    logger.warning(
                        f"Backup command '{command}' matched no files to copy. Continuing."
                    )
                else:
                    logger.error(f"Failed to execute command: '{command}' in '{cwd}'.")
                    logger.error(f"STDERR: {e.stderr}")
                    raise RuntimeError(
                        f"A critical shell command failed, stopping optimization: {command}"
                    ) from e

        # 1. Expand Spot Range for Optimization
        logger.info("Setting SPOT_RANGE to full data range for optimization.")
        self.xds_inp["SPOT_RANGE"] = [f"{self.user_start} {self.user_end}"]

        source_xparm = None
        if os.path.exists(os.path.join(xds_directory, Filenames.GXPARM)):
            source_xparm = os.path.join(xds_directory, Filenames.GXPARM)
        elif os.path.exists(os.path.join(xds_directory, Filenames.XPARM)):
            source_xparm = os.path.join(xds_directory, Filenames.XPARM)
        if not source_xparm:
            logger.error(
                f"Neither {Filenames.GXPARM} nor {Filenames.XPARM} found. Cannot start optimization."
            )
            return
        target_xparm = os.path.join(xds_directory, Filenames.XPARM)
        with open(source_xparm, "r") as src, open(target_xparm, "w") as dst:
            dst.write(src.read())
        initial_ISa = self.results.get("ISa")
        if initial_ISa is None:
            logger.error("Initial ISa not found in results. Cannot start optimization.")
            return
        logger.info(f"Starting optimization with initial ISa: {initial_ISa}")
        best_results_dict = self.results.copy()
        best_ISa = float(initial_ISa)
        best_dir_name = "best"
        initial_dir_name = "initial_state"
        prepare_backup_dir_cmd = f"rm -rf {best_dir_name} {initial_dir_name}; mkdir {best_dir_name} {initial_dir_name}"
        backup_files_cmd = f"cp -p [A-Z]*[A-Z].{{LP,HKL,XDS,INP,cbf}} {best_dir_name}/"
        backup_initial_cmd = f"cp -p [A-Z]*[A-Z].{{LP,HKL,XDS,INP,cbf}} {initial_dir_name}/"
        restore_cmd = f"cp -p {best_dir_name}/* ."
        restore_initial_cmd = f"cp -p {initial_dir_name}/* ."
        cleanup_cmd = f"rm -rf {best_dir_name} {initial_dir_name}"
        logger.info(f"Backing up initial state to './{best_dir_name}' and './{initial_dir_name}' directory...")
        run_shell_command(prepare_backup_dir_cmd, cwd=xds_directory)
        run_shell_command(backup_files_cmd, cwd=xds_directory)
        run_shell_command(backup_initial_cmd, cwd=xds_directory)
        iteration = 0
        revert_to_previous = False
        res_cchalf = self.results.get("resolution_based_on_cchalf")
        
        while iteration < max_iterations:
            iteration += 1
            logger.info(f"Optimization iteration: {iteration}")
            
            # 2. Check for low signal ranges from BOTH Spot Finding and Integration
            low_signal_ranges = self._identify_low_signal_ranges(min_spots=10)
            weak_integration_ranges = self._identify_weak_integration_ranges(min_reflections=10)
            
            combined_new_exclusions = list(set(low_signal_ranges + weak_integration_ranges))
            
            if combined_new_exclusions:
                logger.info(f"Identified weak data ranges for exclusion: {combined_new_exclusions}")
                self._update_exclude_ranges(combined_new_exclusions)

            dict_integ_suggestions = parse_integrate_lp(
                os.path.join(xds_directory, Filenames.INTEGRATE_LP)
            )
            if dict_integ_suggestions:
                self.xds_inp.update(dict_integ_suggestions)

            # --- Modification: Add IDXREF in first cycle if space group not provided ---
            if iteration == 1 and not self.user_space_group:
                 self.xds_inp.update({"JOB": "IDXREF INTEGRATE CORRECT"})
            else:
                 self.xds_inp.update({"JOB": "INTEGRATE CORRECT"})
            # -------------------------------------------------------------------------
            
            self.xds_inp.update(
                {
                    "NUMBER_OF_PROFILE_GRID_POINTS_ALONG_ALPHA/BETA": 19,
                    "NUMBER_OF_PROFILE_GRID_POINTS_ALONG_GAMMA": 19,
                    "RELRAD": 7,
                }
            )
            
            # --- Modification: Use Refined Parameters for Optimization ---
            if "REFINED_UNIT_CELL_CONSTANT" in self.results:
                self.xds_inp["UNIT_CELL_CONSTANTS"] = self.results["REFINED_UNIT_CELL_CONSTANT"]
            if "REFINED_DISTANCE" in self.results:
                self.xds_inp["DETECTOR_DISTANCE"] = self.results["REFINED_DISTANCE"]
            if "REFINED_MOSAICITY" in self.results:
                self.xds_inp["REFLECTING_RANGE_E.S.D."] = self.results["REFINED_MOSAICITY"]
            if "REFINED_ORXY" in self.results:
                refined_orxy = self.results["REFINED_ORXY"].split()
                if len(refined_orxy) == 2:
                    self.xds_inp["ORGX"] = refined_orxy[0]
                    self.xds_inp["ORGY"] = refined_orxy[1]
            # -----------------------------------------------------------

            if self.user_resolution_cutoff is None:
                self.set_resolution_cutoff(res_cchalf)
            self.generate_xds_inp()
            self.run(job_name=f"xds_opt_{iteration}")
            opt_job_status = self.check_job_status(last_step="CORRECT")
            if opt_job_status is not None:
                logger.error(
                    f"Optimization cycle failed at {opt_job_status}. Reverting to previous best results."
                )
                revert_to_previous = True
                break
            correct_result_opt = parse_correct_lp(
                os.path.join(xds_directory, Filenames.CORRECT_LP)
            )
            if not correct_result_opt or "ISa" not in correct_result_opt:
                logger.error("Failed to parse CORRECT.LP or ISa missing. Reverting.")
                revert_to_previous = True
                break
            ISa_new = float(correct_result_opt["ISa"])
            logger.info(
                f"Optimization iteration {iteration}: Previous best ISa {best_ISa:.3f} --> New ISa {ISa_new:.3f}"
            )
            res_cchalf = correct_result_opt.get("resolution_based_on_cchalf")
            
            # --- Modification: Tolerant check for first iteration ---
            isa_diff = ISa_new - best_ISa
            threshold = XdsConfig.OPTIMIZE_ISA_THRESHOLD
            if iteration == 1:
                threshold = XdsConfig.OPTIMIZE_ISA_TOLERANCE_INITIAL
                
            if isa_diff > threshold:
                logger.info(f"ISa improvement ({isa_diff:.3f}) > threshold ({threshold}). Updating best state.")
            # --------------------------------------------------------
                best_ISa = ISa_new
                self.results.update(correct_result_opt)
                best_results_dict = self.results.copy()
                run_shell_command(backup_files_cmd, cwd=xds_directory)
                if os.path.exists(os.path.join(xds_directory, Filenames.GXPARM)):
                    with open(
                            os.path.join(xds_directory, Filenames.GXPARM), "r"
                    ) as src, open(target_xparm, "w") as dst:
                        dst.write(src.read())
            else:
                logger.info(
                    "ISa improvement not significant. Reverting to previous best state."
                )
                revert_to_previous = True
                break
        if revert_to_previous:
            logger.info(f"Restoring state from the './{best_dir_name}' directory.")
            self.results = best_results_dict
            run_shell_command(restore_cmd, cwd=xds_directory)
        
        # --- Modification: Final Check - Ensure we didn't end up worse than initial state ---
        if best_ISa < float(initial_ISa):
            logger.warning(
                f"Optimization resulted in lower ISa ({best_ISa:.3f}) than initial ({float(initial_ISa):.3f}). "
                f"Reverting to initial state."
            )
            # We assume results dictionary needs to be reset too - but we might have lost initial results dict if we didn't copy it.
            # Fortunately self.results was copied to best_results_dict at start.
            # Wait, best_results_dict is updated as we go.
            # We need to re-parse CORRECT.LP from initial state or use initial_ISa? 
            # Actually, we should rely on the file restore.
            run_shell_command(restore_initial_cmd, cwd=xds_directory)
            
            # Re-parse initial CORRECT.LP to reset self.results
            initial_correct_lp = os.path.join(xds_directory, Filenames.CORRECT_LP)
            if os.path.exists(initial_correct_lp):
                initial_results = parse_correct_lp(initial_correct_lp)
                if initial_results:
                     self.results.update(initial_results)
        # ------------------------------------------------------------------------------------

        logger.info(f"Cleaning up backup directories.")
        run_shell_command(cleanup_cmd, cwd=xds_directory)
        logger.info("Optimization finished.")

    def parse_xds_inp(self, file_path):
        parsed_inp = {}
        if not os.path.exists(file_path):
            logger.error(f"XDS.INP file not found at {file_path}")
            return parsed_inp
        param_pattern = re.compile(
            r"^\s*([A-Z/()'-]+)\s*=\s*(.*?)(?:\s*!.*)?$", re.MULTILINE
        )
        try:
            with open(file_path, "r") as file:
                content = file.read()
                matches = param_pattern.finditer(content)
                for match in matches:
                    key = match.group(1).strip()
                    value_str = match.group(2).strip()
                    try:
                        if "." in value_str:
                            value = float(value_str)
                        else:
                            value = int(value_str)
                    except ValueError:
                        value = value_str
                    if key in parsed_inp:
                        if isinstance(parsed_inp[key], list):
                            parsed_inp[key].append(value)
                        else:
                            parsed_inp[key] = [parsed_inp[key], value]
                    else:
                        parsed_inp[key] = value
        except IOError as e:
            logger.error(f"Error reading XDS.INP file {file_path}: {e}")
            return {}
        self.xds_inp = parsed_inp

    def _calculate_smart_resolution_cutoff(self) -> Optional[float]:
        """
        Calculates a 'smart' default resolution cutoff based on detector geometry.
        The cutoff is set to the resolution corresponding to the average of the
        distance to the nearest edge and the farthest corner.
        """
        try:
            det_dist = self._get_metadata(MetaData.DETECTOR_DISTANCE.value)
            pix_size_x = self._get_metadata(MetaData.X_PIXEL_SIZE.value)
            pix_size_y = self._get_metadata(MetaData.Y_PIXEL_SIZE.value)
            nx = self._get_metadata(MetaData.X_PIXELS.value)
            ny = self._get_metadata(MetaData.Y_PIXELS.value)
            beam_x = self._get_metadata(MetaData.BEAM_CENTER_X.value)
            beam_y = self._get_metadata(MetaData.BEAM_CENTER_Y.value)
            wavelength = self._get_metadata(MetaData.WAVELENGTH.value)

            if any(v is None for v in [det_dist, pix_size_x, pix_size_y, nx, ny, beam_x, beam_y, wavelength]):
                return None

            # Calculate distances to all 4 corners in pixels
            corners = [
                (0, 0),
                (nx, 0),
                (0, ny),
                (nx, ny)
            ]
            
            # Max radius (farthest corner)
            max_r_sq = 0
            for cx, cy in corners:
                dist_sq = (cx - beam_x)**2 + (cy - beam_y)**2
                if dist_sq > max_r_sq:
                    max_r_sq = dist_sq
            max_r_px = max_r_sq**0.5

            # Min radius (nearest edge)
            # Distance to left, right, top, bottom edges
            dist_left = beam_x
            dist_right = nx - beam_x
            dist_top = beam_y
            dist_bottom = ny - beam_y
            min_r_px = min(dist_left, dist_right, dist_top, dist_bottom)

            # Average radius in pixels
            # We assume square pixels or take average size for simplicity, or use specific dimension
            # Let's use average pixel size
            avg_pix_size = (pix_size_x + pix_size_y) / 2.0
            
            smart_r_px = (min_r_px + max_r_px) / 2.0
            smart_r_mm = smart_r_px * avg_pix_size

            # Calculate resolution
            # theta = 0.5 * atan(r / D)
            # d = lambda / (2 * sin(theta))
            
            import math
            theta = 0.5 * math.atan(smart_r_mm / det_dist)
            resolution = wavelength / (2 * math.sin(theta))
            
            return resolution

        except Exception as e:
            logger.warning(f"Could not calculate smart resolution cutoff: {e}")
            return None

    def set_resolution_cutoff(self, resolution_cutoff=None):
        cutoff_to_use = (
            resolution_cutoff
            if resolution_cutoff is not None
            else self.user_resolution_cutoff
        )
        
        if cutoff_to_use is None:
            # Try to calculate smart cutoff based on geometry
            smart_cutoff = self._calculate_smart_resolution_cutoff()
            if smart_cutoff:
                cutoff_to_use = smart_cutoff
                logger.info(f"Using smart resolution cutoff based on detector geometry: {smart_cutoff:.2f} Å")
            else:
                cutoff_to_use = XdsConfig.DEFAULT_RESOLUTION_CUTOFF
                logger.info(f"Using default resolution cutoff: {cutoff_to_use} Å")

        try:
            cutoff_float = float(cutoff_to_use)
            self.xds_inp["INCLUDE_RESOLUTION_RANGE"] = f"50.0 {cutoff_float:.2f}"
            logger.info(f"Resolution cutoff set in XDS.INP: 50 {cutoff_float:.2f}")
        except (ValueError, TypeError):
            logger.error(
                f"Invalid resolution cutoff value provided: {cutoff_to_use}. Using default."
            )
            self.xds_inp["INCLUDE_RESOLUTION_RANGE"] = (
                f"50.0 {XdsConfig.DEFAULT_RESOLUTION_CUTOFF:.2f}"
            )

    def set_user_space_group(self):
        if self.user_space_group and self.user_unit_cell:
            sg_number = (
                Symmetry.symbol_to_number(self.user_space_group)
                if isinstance(self.user_space_group, str)
                   and not self.user_space_group.isdigit()
                else self.user_space_group
            )
            self.xds_inp["SPACE_GROUP_NUMBER"] = sg_number
            self.xds_inp["UNIT_CELL_CONSTANTS"] = self.user_unit_cell
            self.xds_inp["JOB"] = "IDXREF"
            logger.info(
                f"Setting user-defined space group to {sg_number} and unit cell to {self.user_unit_cell}. Rerunning IDXREF."
            )
            self.generate_xds_inp()
            self.run(job_name="xds_user_sg")
            idxref_rerun_status = self.check_job_status(last_step="IDXREF")
            if idxref_rerun_status is not None:
                logger.error(
                    f"IDXREF rerun with user-defined parameters failed at step: {idxref_rerun_status}"
                )
            else:
                logger.info("IDXREF rerun with user parameters completed successfully.")
        else:
            logger.warning(
                "User space group or unit cell not fully provided. Cannot set and rerun IDXREF."
            )

    def set_fastdp_spot_ranges(self):
        start_frame, end_frame = self.user_start, self.user_end
        osc_range = self._get_metadata(MetaData.OMEGA_RANGE.value, 0.1)
        wedge_size = int(90.0 / osc_range) if osc_range > 1e-4 else 900
        wedge_size = min(wedge_size, end_frame - start_frame + 1)
        spot_range = []
        spot_range.append(f"{start_frame} {start_frame + wedge_size - 1}")
        middle_start = max(
            start_frame, (start_frame + end_frame) // 2 - wedge_size // 2
        )
        spot_range.append(f"{middle_start} {middle_start + wedge_size - 1}")
        end_start = max(start_frame, end_frame - wedge_size + 1)
        spot_range.append(f"{end_start} {end_frame}")
        unique_spot_ranges = []
        for r in spot_range:
            s, e = map(int, r.split())
            s = max(start_frame, s)
            e = min(end_frame, e)
            if s <= e and f"{s} {e}" not in unique_spot_ranges:
                unique_spot_ranges.append(f"{s} {e}")
        self.xds_inp["SPOT_RANGE"] = unique_spot_ranges
        logger.info(f"Set SPOT_RANGE using fast_dp strategy: {unique_spot_ranges}")

    def run(self, job_name=None):
        cmd = XdsConfig.XDS_EXECUTABLE
        if job_name is None:
            job_name = f"run_xds_{self.prefix}"
        if self.use_slurm:
            run_command(
                cmd,
                cwd=self.proc_dir,
                method="slurm",
                job_name=job_name,
                processors=self.nproc,
                nodes=self.njobs,
                pre_command=ProgramConfig.get_setup_command("xds"),
            )
        else:
            run_command(
                cmd,
                cwd=self.proc_dir,
                method="shell",
                job_name=job_name,
                processors=self.nproc,
                pre_command=ProgramConfig.get_setup_command("xds"),
            )

    def run_pointless(self):
        if not os.path.exists(self.xds_ascii_hkl_file):
            logger.warning(
                f"{self.xds_ascii_hkl_file} not found. Cannot run pointless."
            )
            return
        
        # Use ProgramConfig to get the setup command
        setup_cmd = ProgramConfig.get_setup_command("ccp4")
        
        # Construct the command for pointless
        # Note: We use the executable name from config, assuming it is in path after module load
        # or it is an absolute path.
        pointless_cmd = f"{XdsConfig.POINTLESS_EXECUTABLE} {self.xds_ascii_hkl_file} xmlout {self.pointless_xml_file}"
        
        try:
            # Run using the shared run_command utility
            # Use a unique job_name to avoid same-file conflict with self.pointless_log_file (pointless.out)
            run_command(
                pointless_cmd,
                cwd=self.proc_dir,
                method="shell",
                job_name="run_pointless",
                pre_command=setup_cmd
            )
            
            # The output of run_command is saved to <job_name>.out
            pointless_out = os.path.join(self.proc_dir, "run_pointless.out")
            if os.path.exists(pointless_out):
                shutil.copy(pointless_out, self.pointless_log_file)
            
            logger.info("Pointless executed successfully.")
        except Exception as e:
            logger.error(f"Pointless execution failed: {e}")
            # If it failed, we still try to copy whatever output was generated
            pointless_out = os.path.join(self.proc_dir, "run_pointless.out")
            if os.path.exists(pointless_out):
                shutil.copy(pointless_out, self.pointless_log_file)
            return

        if os.path.exists(self.pointless_xml_file):
            pointless_results = parse_pointless_xml(self.pointless_xml_file)
            if pointless_results:
                self.results.update(pointless_results)
                logger.info(
                    f"Pointless results parsed: {pointless_results.get('pointless_best_solution')}"
                )
            else:
                logger.error(f"Failed to parse {self.pointless_xml_file}.")
        else:
            logger.error(
                f"{self.pointless_xml_file} not found after running pointless."
            )

    def run_xdsconv(self):
        if not os.path.exists(self.xds_ascii_hkl_file):
            logger.warning(f"{self.xds_ascii_hkl_file} not found. Cannot run XDSCONV.")
            return

        sg_from_correct = self.results.get("SPACE_GROUP_NUMBER")
        uc_from_correct = self.results.get("UNIT_CELL_CONSTANTS")

        if sg_from_correct and uc_from_correct:
            self.xdsconv_inp["!SPACE_GROUP_NUMBER"] = sg_from_correct
            self.xdsconv_inp["!UNIT_CELL_CONSTANTS"] = (
                " ".join(map(str, uc_from_correct))
                if isinstance(uc_from_correct, list)
                else str(uc_from_correct)
            )
        else:
            logger.warning(
                "Space group or unit cell not found in results. XDSCONV might use defaults."
            )

        try:
            with open(self.xdsconv_inp_file, "w") as f:
                for key, value in self.xdsconv_inp.items():
                    f.write(f"{key}= {value}\n")
            logger.info(f"Generated {self.xdsconv_inp_file}")
        except IOError as e:
            logger.error(f"Failed to write {self.xdsconv_inp_file}: {e}")
            return

        try:
            # --- STEP 1: Run XDSCONV ---
            logger.info("Running XDSCONV...")
            run_command(
                XdsConfig.XDSCONV_EXECUTABLE, 
                cwd=self.proc_dir, 
                method="shell",
                job_name="xdsconv",
                pre_command=ProgramConfig.get_setup_command("xds")
            )
            logger.info("XDSCONV executed successfully.")

            # --- STEP 2: Run F2MTZ if the necessary input files exist ---
            if not os.path.exists(self.processed_hkl_file):
                logger.error(
                    f"{self.processed_hkl_file} was not created by XDSCONV. Cannot run f2mtz."
                )
                return
            if not os.path.exists(self.f2mtz_inp_file):
                logger.error(
                    f"{self.f2mtz_inp_file} was not created by XDSCONV. Cannot run f2mtz."
                )
                return

            logger.info(f"Running f2mtz to create {self.processed_mtz_file}")

            # Construct the f2mtz command. The input is redirected from F2MTZ.INP by the shell.
            f2mtz_command = (
                f"{XdsConfig.F2MTZ_EXECUTABLE} "
                f"hklout {self.processed_mtz_file} < {self.f2mtz_inp_file}"
            )

            # Use run_command with method='shell' to handle the input redirection (<)
            # f2mtz requires CCP4 environment
            run_command(
                f2mtz_command, 
                cwd=self.proc_dir, 
                method="shell",
                job_name="f2mtz",
                pre_command=ProgramConfig.get_setup_command("ccp4")
            )

            if os.path.exists(self.processed_mtz_file):
                logger.info(f"Successfully created {self.processed_mtz_file}")
                self.results["truncate_mtz"] = self.processed_mtz_file
            else:
                logger.error(f"f2mtz failed to create {self.processed_mtz_file}")

        except RuntimeError as e:
            logger.error(f"XDSCONV or F2MTZ execution failed: {e}")

    def run_dimple(self):
        if not os.path.exists(self.processed_mtz_file):
            logger.warning(f"{self.processed_mtz_file} not found. Cannot run Dimple.")
            return
        pdb_model = self.user_model or self.results.get("pdbfile")
        if not pdb_model or not os.path.exists(pdb_model):
            logger.warning(f"PDB model ({pdb_model}) not found. Cannot run Dimple.")
            return
        dimple_dir = os.path.join(self.proc_dir, "dimple")
        os.makedirs(dimple_dir, exist_ok=True)
        cmd = ["dimple", self.processed_mtz_file, pdb_model, dimple_dir]
        try:
            run_command(
                cmd, 
                cwd=self.proc_dir, 
                method="shell", 
                job_name="dimple",
                pre_command=ProgramConfig.get_setup_command("ccp4")
            )
            logger.info(f"Dimple job started in {dimple_dir}.")
            final_pdb = os.path.join(dimple_dir, "final.pdb")
            final_mtz = os.path.join(dimple_dir, "final.mtz")
            reindex_mtz = os.path.join(dimple_dir, "reindex.mtz")
            if os.path.exists(final_pdb) and os.path.exists(final_mtz):
                self.results["final_pdb"] = final_pdb
                self.results["final_mtz"] = final_mtz
                logger.info("Dimple finished successfully.")

                new_spg_num, new_cell = get_cell_symm_from_mtz(final_mtz)
                if new_spg_num and new_cell:
                    self.results["SPACE_GROUP_NUMBER"] = str(new_spg_num)
                    self.results["UNIT_CELL_CONSTANTS"] = new_cell
                    self.processed_mtz_file = (
                        reindex_mtz if os.path.exists(reindex_mtz) else final_mtz
                    )
                    logger.info(
                        "Updated main results with Dimple's space group, unit cell, and MTZ file."
                    )
            else:
                logger.error("Dimple did not produce final.pdb and final.mtz.")

        except RuntimeError as e:
            logger.error(f"Dimple execution failed: {e}")

    def create_summary(self):
        """
        Consolidates results from processing steps into a machine-readable
        XDS.json file. Also logs a high-level summary.
        """
        logger.info("--- XDS Processing Summary ---")
        if "error_step" in self.results:
            logger.error(f"Status: FAILED at step '{self.results['error_step']}'")
            logger.error(f"Message: {self.results.get('error_message')}")
        else:
            logger.info("Status: SUCCESS (based on process flow completion)")

        logger.info(f"Consolidating detailed results into {self.xds_json_path}...")

        # Get per-frame stats from INTEGRATE.LP
        per_frame_stats = {}
        if os.path.exists(self.integrate_lp_file):
            per_frame_stats = parse_integrate_lp_per_frame(self.integrate_lp_file)
        else:
            logger.warning(
                f"{self.integrate_lp_file} not found. Per-frame stats will be missing."
            )

        if per_frame_stats:
            with open(self.xds_json_path, "w") as f:
                json.dump(per_frame_stats, f, indent=4, default=str)
            logger.info(
                f"Successfully generated per frame results file: {self.xds_json_path}"
            )

        # The self.results dictionary already contains the overall stats
        overall_stats = self.results.copy()
        overall_stats["dataset"] = self.master_file
        overall_stats["proc_dir"] = self.proc_dir
        overall_stats["xds_inp"] = self.xds_inp

        with open(self.xds_json_stats_path, "w") as f:
            json.dump(overall_stats, f, indent=4, default=str)
        logger.info(
            f"Successfully generated overall results file: {self.xds_json_stats_path}"
        )
        self.results["xds_json_path"] = self.xds_json_path

        logger.info(f"Generating HTML report at {self.html_report_path}")
        try:
            report_generator = XDSReportGenerator(
                work_dir=self.proc_dir, tag_name=self.prefix
            )
            report_generator.create_report(
                output_filename=os.path.basename(self.html_report_path)
            )

            # Add the report URL to the results dictionary to be saved.
            if os.path.exists(self.html_report_path):
                self.results["report_url"] = self.html_report_path
                logger.info(f"Report URL added to results: {self.html_report_path}")
            else:
                logger.warning(
                    f"HTML report was not generated at {self.html_report_path}"
                )
        except Exception as e:
            logger.error(f"Failed to generate HTML report: {e}", exc_info=True)
            self.results["report_url"] = None


# The nXDS class and test functions remain unchanged.
class nXDS(XDS):
    """
    Handles the nXDS processing workflow by inheriting from XDS and
    overriding nXDS-specific configurations and processes.
    """

    def __init__(self, dataset, proc_dir=None, **kwargs):
        """
        Initializes the nXDS run, setting up nXDS-specific filenames,
        job definitions, and file paths.
        """

        # --- Handle nXDS-specific keyword arguments ---
        self.powder = kwargs.pop("powder", False)
        self.run_correct = kwargs.pop("run_correct", False)
        logger.info(
            f"Re-configuring for nXDS run. POWDER step is {'enabled' if self.powder else 'disabled'}. "
            f"CORRECT step is {'enabled' if self.run_correct else 'disabled'}."
        )

        # --- Correctly pass all arguments to the parent constructor ---
        # This ensures all parent class attributes are initialized properly.
        super().__init__(dataset, proc_dir=proc_dir, **kwargs)

        # Apply user-defined resolution cutoff for nXDS specifically
        if self.user_resolution_cutoff is not None:
            self.set_resolution_cutoff(self.user_resolution_cutoff)

        # --- Override file paths for nXDS ---
        # Input file is specific to nXDS.
        self.xds_inp_file = os.path.join(self.proc_dir, Filenames.NXDS_INPUT)

        # Log files (.LP) often keep their original names even when run by a wrapper.
        # We will assume they are not prefixed unless specified otherwise.
        self.idxref_lp_file = os.path.join(self.proc_dir, Filenames.IDXREF_LP)
        self.correct_lp_file = os.path.join(self.proc_dir, Filenames.CORRECT_LP)
        self.integrate_lp_file = os.path.join(self.proc_dir, Filenames.INTEGRATE_LP)

        # Key output files (.HKL, .XDS) are prefixed with 'n' for nXDS.
        # We construct these paths manually for clarity and correctness.
        self.xds_ascii_hkl_file = os.path.join(
            self.proc_dir, f"n{Filenames.XDS_ASCII_HKL}"
        )
        self.gxparm_file = os.path.join(self.proc_dir, f"n{Filenames.GXPARM}")
        self.xparm_file = os.path.join(self.proc_dir, f"n{Filenames.XPARM}")
        self.processed_hkl_file = os.path.join(self.proc_dir, f"n{self.prefix}.hkl")
        self.processed_mtz_file = os.path.join(self.proc_dir, f"n{self.prefix}.mtz")

        # --- Set and override parameters for nXDS in one block ---
        nxds_params = {
            "MINIMUM_NUMBER_OF_SPOTS": 10,
            "BACKGROUND_PIXEL": 2.0,
            "SIGNAL_PIXEL": 2.8,
            "MINIMUM_NUMBER_OF_PIXELS_IN_A_SPOT": 3,
            "TRUSTED_REGION": "0 1.2",
            "SEPMIN": 4.0,
            "CLUSTER_RADIUS": 2.0,
            "INDEX_ERROR": 0.1, # default 0.05
            "INDEX_MAGNITUDE": 15, # default 8
            "INDEX_QUALITY": 0.7, # default 0.8
            "MAXIMUM_ERROR_OF_SPOT_POSITION": 4.0, # default 3.0
            "MINIMUM_FRACTION_OF_INDEXED_SPOTS": 0.25, # default 0.3
            "MINPK": self._get_minpk(self._get_metadata(MetaData.OMEGA_RANGE.value, 0.0)),
            "MINIMUM_ZETA": 0.0002 if self._get_metadata(MetaData.OMEGA_RANGE.value, 0.0) < 0.01 else 0.10,
            "MINIMUM_EWALD_OFFSET_CORRECTION": 0.4,
            "REFERENCE_DATA_SET": self.reference_hkl,
            "REFINE(IDXREF)": "BEAM ORIENTATION CELL !POSITION",
            "POSTREFINE": "SKALA  ! B-FACTOR POSITION BEAM ORIENTATION CELL MOSAICITY SEGMENT",
        }
        self.xds_inp.update(nxds_params)

        # --- Remove XDS-specific keywords not used by nXDS ---
        keys_to_remove = [
            "SPOT_RANGE",
            "REFINE(INTEGRATE)",
            "REFINE(CORRECT)",
            "STARTING_ANGLE",
            "RELRAD",
            "NUMBER_OF_PROFILE_GRID_POINTS_ALONG_GAMMA",
            "VALUE_RANGE_FOR_TRUSTED_DETECTOR_PIXELS",
            "STARTING_ANGLES_OF_SPINDLE_ROTATION",
            "TOTAL_SPINDLE_ROTATION_RANGES",
            "FRIEDEL'S_LAW",
        ]
        for key in keys_to_remove:
            self.xds_inp.pop(key, None)  # Use pop with a default to avoid errors

        if self.extra_xds_inp_params:
            self.xds_inp.update(self.extra_xds_inp_params)
            logger.info(f"Re-applied extra XDS.INP parameters for nXDS: {self.extra_xds_inp_params}")

    def _get_minpk(self, omega_range: float) -> float:
        """
        Returns recommended MINPK based on oscillation range.
        For stills (omega_range == 0), use a slightly reduced value to
        accommodate fully partial reflections. Otherwise use the XDS default.
        """
        if omega_range <= 0.001:
            return 70.0
        return 75.0

    def run(self):
        """Runs the nxds_par program."""
        if not os.path.exists(self.xds_inp_file):
            logger.error(f"{self.xds_inp_file} not found. Cannot run nXDS.")
            raise FileNotFoundError(f"{self.xds_inp_file} not found.")

        job_name = f"run_nxds_{self.prefix}"
        logger.info(
            f"Submitting nXDS job '{job_name}' with JOB={self.xds_inp.get('JOB')}"
        )

        try:
            run_command(
                [XdsConfig.NXDS_EXECUTABLE],
                cwd=self.proc_dir,
                job_name=job_name,
                nodes=self.njobs,
                processors=self.nproc,
                method="slurm" if self.use_slurm else "shell",
                background=False,
                pre_command=ProgramConfig.get_setup_command("nxds"),
            )
            logger.info("nXDS execution initiated.")
        except Exception as e:
            logger.error(f"Failed to run nXDS: {e}", exc_info=True)
            raise

    def create_summary(self):
        """
        Creates a summary for the nXDS run.
        This reuses the parent's summary method, as the final CORRECT.LP
        parsing and structure are assumed to be the same.
        """
        logger.info("Creating summary for nXDS run using base XDS summary logic.")
        pass

    def _run_step(self, job_definition: str, last_step: str) -> Optional[str]:
        """Helper method to run a specific job step and check its status."""
        self.xds_inp["JOB"] = job_definition
        self.generate_xds_inp()
        self.run()
        return self.check_job_status(
            last_step=last_step, job_steps=XdsConfig.NXDS_JOB_STEPS
        )

    def process(self):
        """
        Orchestrates the nXDS processing workflow step-by-step,
        with error handling at each stage.
        """
        logger.info(f"Starting nXDS processing workflow for {self.proc_dir}")

        # Step 1: Initialization
        if self._run_step("XYCORR FILTER INIT", "INIT"):
            self._handle_error(
                "INIT", "nXDS initialization (XYCORR, FILTER, INIT) failed."
            )
            return

        # Step 2: Indexing
        powder_step = "POWDER" if self.powder else ""
        indexing_job = f"COLSPOT {powder_step} IDXREF".strip().replace("  ", " ")
        if self._run_step(indexing_job, "IDXREF"):
            self._handle_error("IDXREF", "nXDS indexing (COLSPOT, IDXREF) failed.")
            return

        spots_per_frame = parse_colspot_lp(self.colspot_lp_file)
        spots_details = parse_spot_nxds(self.spot_nxds_file)
        index_results = parse_nxds_idxref_log(self.idxref_lp_file)
        combines_results = merge_dicts(spots_per_frame, spots_details)
        combines_results = merge_dicts(combines_results, index_results)
        with open(self.nxds_json_path, "w") as f:
            json.dump(combines_results, f, indent=4)
        self.results["nxds_json_path"] = self.nxds_json_path

        # Step 3: Integration
        if self._run_step("INTEGRATE", "INTEGRATE"):
            self._handle_error("INTEGRATE", "nXDS integration failed.")
            return

        # Step 4: Correction (optional, off by default)
        if self.run_correct:
            if self._run_step("CORRECT", "CORRECT"):
                self._handle_error("CORRECT", "nXDS scaling (CORRECT) failed.")
                return
        else:
            logger.info(
                f"End of nXDS processing run for {self.proc_dir}. CORRECT step was not requested."
            )
            return

        logger.info("nXDS main processing completed successfully.")

        # Parse results from CORRECT.LP
        if os.path.exists(self.correct_lp_file):
            correct_results = parse_nxscale_or_ncorrect_lp(self.correct_lp_file)
            if correct_results:
                self.results.update(correct_results)
            else:
                logger.warning(f"Failed to parse {self.correct_lp_file}")
        else:
            logger.error(f"{self.correct_lp_file} not found. Cannot get final results.")

        # Create and log the final summary
        self.create_summary()
        # self.save_to_redis() # This can be re-enabled if desired

        logger.info(f"End of nXDS processing run for {self.proc_dir}")


def run_xds_test(
        test_number: Optional[int] = None,
        base_dir: str = "/mnt/beegfs/qxu",
        dataset=None,  # Assuming metadata is defined elsewhere
        njobs: int = 2,
):
    """
    Run XDS processing tests.

    Args:
        test_number: Specific test to run (1-4), or None to run all tests
        base_dir: Base directory for output
        dataset: dataset object (HDF5Reader or CbfReader)
        njobs: Number of jobs to use for processing
    """
    if dataset is None:
        logger.error("Dataset object must be provided to run_xds_test.")
        raise ValueError("Dataset object is required.")

    # Define the test configurations
    tests = [
        {
            "name": "Test 1: strategy only",
            "dir": os.path.join(base_dir, "tmp0strategy"),
            "params": {
                "user_end": 1,
                "strategy": True,
            },  # Needs user_start if not using full range
        },
        {
            "name": "Test 2: Basic processing with percentage",
            "dir": os.path.join(base_dir, "tmp0"),
            "params": {"user_percentage": 0.50},  # Use 0.0-1.0 for percentage
        },
        {
            "name": "Test 3: With user_end parameter",
            "dir": os.path.join(base_dir, "tmp1"),
            "params": {"user_end": 400},
        },
        {
            "name": "Test 4: With space group (symbol or number)",
            "dir": os.path.join(base_dir, "tmp2"),
            # Example symbol
            "params": {"user_end": 400, "user_space_group": "P43212"},
        },
        {
            "name": "Test 5: With space group and unit cell, and optimization",
            "dir": os.path.join(base_dir, "tmp3"),
            "params": {
                "user_space_group": 196,  # Example number
                "user_unit_cell": "231.1 231.1 231.1 90 90 90",
                "optimization": True,
            },
        },
    ]

    # Determine which tests to run
    if test_number is not None:
        if 1 <= test_number <= len(tests):
            tests_to_run = [tests[test_number - 1]]
        else:
            logger.error(f"Test number must be between 1 and {len(tests)}")
            raise ValueError(f"Test number must be between 1 and {len(tests)}")
    else:
        tests_to_run = tests

    # Run the selected tests
    for test_config in tests_to_run:
        logger.info(f"Running {test_config['name']}")
        # Ensure proc_dir is absolute
        proc_directory = os.path.abspath(test_config["dir"])

        # Merge common params with test-specific params
        current_params = test_config["params"].copy()
        current_params["njobs"] = njobs  # Add/override njobs from function arg
        # Add other common params if needed, e.g., nproc, use_slurm

        # Create XDS instance with dataset, proc_dir, and unpacked params
        xds_instance = XDS(dataset, proc_dir=proc_directory, **current_params)
        try:
            xds_instance.process()
            # Optionally, save results to JSON or Redis here if needed for each test
            # logger.info(f"Results for {test_config['name']}:\n{xds_instance.to_json()}")
        except Exception as e:
            logger.error(
                f"Error during processing for {test_config['name']}: {e}", exc_info=True
            )

        logger.info(f"Completed {test_config['name']}\n")


def main():
    parser = argparse.ArgumentParser(description="XDS processing script")
    parser.add_argument("master_file", help="Path to the HDF5 master file")
    parser.add_argument(
        "--proc_dir", help="Processing directory", default=os.path.abspath("")
    )
    parser.add_argument("--nproc", type=int, help="Number of processors", default=8)
    parser.add_argument("--njobs", type=int, help="Number of jobs", default=1)
    parser.add_argument(
        "--no-slurm",
        action="store_true",
        help="Do not use SLURM, run on the local machine",
    )
    parser.add_argument("--space_group", help="Space group symbol or number")
    parser.add_argument("--unit_cell", help="Unit cell parameters")
    parser.add_argument("--native", action="store_true", default=True, help="Native data (FRIEDEL'S_LAW=TRUE)")
    parser.add_argument("--resolution", type=float, help="High resolution cutoff")
    parser.add_argument("--start", type=int, help="Starting image number")
    parser.add_argument("--end", type=int, help="Ending image number")
    parser.add_argument("--percentage", type=float, help="Percentage of images to use")
    parser.add_argument(
        "--strategy", action="store_true", help="Run strategy determination only"
    )
    parser.add_argument(
        "--optimization",
        action="store_true",
        help="Enable iterative optimization of processing parameters.",
    )
    parser.add_argument("--reference_hkl", help="Reference HKL file for scaling")
    parser.add_argument("--model", help="PDB model for Dimple")
    parser.add_argument(
        "--beamstop_radius",
        type=int,
        default=XdsConfig.DEFAULT_BEAMSTOP_RADIUS,
        help="Radius of the beamstop in pixels",
    )
    parser.add_argument(
        "--use-redis",
        action="store_true",
        help="Save progress and results to Redis server",
    )
    parser.add_argument(
        "--xds_param",
        action="append",
        help="Additional XDS.INP parameters in KEY=VALUE format (can be used multiple times).",
    )

    parser.add_argument("--username", default=os.getenv("USER"), help="Username for job attribution")
    parser.add_argument(
        "--primary_group", help="Primary group (ESAF) for job attribution"
    )
    parser.add_argument("--run_prefix", help="Run prefix for linking to DatasetRun")
    parser.add_argument("--esaf_id", default=0, type=int, help="ESAF ID for job attribution")
    parser.add_argument(
        "--pi_id", type=int, default=0, help="PI ID for job attribution"
    )

    args = parser.parse_args()

    # Parse xds_param into a dictionary
    extra_xds_params = {}
    if args.xds_param:
        for param in args.xds_param:
            if "=" in param:
                key, value = param.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key in extra_xds_params:
                    if isinstance(extra_xds_params[key], list):
                        extra_xds_params[key].append(value)
                    else:
                        extra_xds_params[key] = [extra_xds_params[key], value]
                else:
                    extra_xds_params[key] = value
            else:
                logger.warning(f"Ignoring invalid parameter format: {param}. Expected KEY=VALUE.")

    pipeline_params = {
        "username": args.username,
        "primary_group": args.primary_group,
        "run_prefix": args.run_prefix,
        "esaf_id": args.esaf_id,
        "pi_id": args.pi_id,
    }
    # Filter out any None values so they don't override defaults in the models
    pipeline_params = {k: v for k, v in pipeline_params.items() if v is not None}

    if args.master_file.endswith((".h5", ".hdf5")):
        dataset_reader = HDF5Reader(args.master_file)
    elif args.master_file.endswith(".cbf"):
        dataset_reader = CbfReader(args.master_file)
    else:
        logger.error(f"Unsupported file type: {args.master_file}")
        sys.exit(1)

    xds_proc = XDS(
        dataset=dataset_reader,
        proc_dir=args.proc_dir,
        nproc=args.nproc,
        njobs=args.njobs,
        use_slurm=not args.no_slurm,
        user_space_group=args.space_group,
        user_unit_cell=args.unit_cell,
        user_native=args.native,
        user_resolution_cutoff=args.resolution,
        user_start=args.start,
        user_end=args.end,
        user_percentage=args.percentage,
        strategy=args.strategy,
        optimization=args.optimization,
        reference_hkl=args.reference_hkl,
        user_model=args.model,
        beamstop_radius=args.beamstop_radius,
        use_redis=args.use_redis,
        pipeline_params=pipeline_params,
        extra_xds_inp_params=extra_xds_params,
    )

    xds_proc.process()


if __name__ == "__main__":
    main()
