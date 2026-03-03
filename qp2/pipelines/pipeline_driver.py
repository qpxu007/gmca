#!/usr/bin/env python
"""
QP2 Pipeline Driver - Unified interface for running crystallographic data processing pipelines.

This module provides a standardized interface for executing different pipelines
(autoproc, xia2, gmcaproc, strategy) with consistent input validation, status tracking,
and database integration.
"""

import argparse
import json
import logging
import os
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

from qp2.pipelines.utils.pipeline_tracker import PipelineTracker
from qp2.data_viewer.models import DataProcessResults, ScreenStrategyResults
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.log.logging_config import get_logger, setup_logging
from qp2.config.servers import ServerConfig
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)


class PipelineType(Enum):
    """Supported pipeline types."""

    AUTOPROC = "autoproc"
    XIA2 = "xia2"
    XIA2_SSX = "xia2_ssx"
    GMCAPROC = "gmcaproc"
    STRATEGY = "strategy"


class JobStatus(Enum):
    """Job execution status."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class DatasetSpec:
    """Specification for a dataset to be processed."""

    master_file: str
    frame_range: Optional[List[int]] = None

    def __post_init__(self):
        self.master_file = str(Path(self.master_file).resolve())
        if not os.path.exists(self.master_file):
            raise FileNotFoundError(f"Dataset file not found: {self.master_file}")


@dataclass
class PipelineConfig:
    pipeline: str
    data_paths: List[str]
    work_dir: str = "."
    space_group: Optional[str] = None
    unit_cell: Optional[str] = None
    highres: Optional[float] = None
    native: bool = True
    model: Optional[str] = None
    nproc: int = 4
    njobs: int = 8
    runner: str = "slurm"
    fast: bool = False
    trust_beam_centre: bool = True
    wavelength: Optional[float] = None
    reference_hkl: Optional[str] = None
    reference_refl: Optional[str] = None
    extra_options: Optional[str] = None
    sample_name: Optional[str] = None
    username: str = field(default_factory=lambda: os.getenv("USER", "unknown"))
    beamline: str = field(default_factory=get_beamline_from_hostname)
    primary_group: Optional[str] = None
    pi_id: Optional[int] = None
    esaf_id: Optional[int] = None


@dataclass
class PipelineResult:
    """Standardized pipeline execution result."""

    pipeline_type: str
    job_status: JobStatus
    work_dir: str
    start_time: float
    end_time: Optional[float] = None

    # Processing results
    results: Dict[str, Any] = None
    error_message: Optional[str] = None

    # Output files
    log_files: List[str] = None
    output_files: List[str] = None

    # Database tracking
    pipeline_status_id: Optional[int] = None
    result_pk_value: Optional[int] = None

    def __post_init__(self):
        if self.results is None:
            self.results = {}
        if self.log_files is None:
            self.log_files = []
        if self.output_files is None:
            self.output_files = []

    @property
    def elapsed_time(self) -> float:
        """Calculate elapsed time in seconds."""
        end = self.end_time or time.time()
        return end - self.start_time

    @property
    def success(self) -> bool:
        """Check if pipeline succeeded."""
        return self.job_status == JobStatus.SUCCESS

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["job_status"] = self.job_status.value
        data["elapsed_time"] = self.elapsed_time
        data["success"] = self.success
        return data


class BasePipeline(ABC):
    """Abstract base class for pipeline implementations."""

    def __init__(
        self,
        pipeline_type: PipelineType,
        datasets: List[DatasetSpec],
        config: PipelineConfig,
    ):
        self.pipeline_type = pipeline_type
        self.datasets = datasets
        self.config = config
        self.tracker: Optional[PipelineTracker] = None

        # Create work directory
        os.makedirs(config.work_dir, exist_ok=True)

        # Set up logging
        log_file = os.path.join(config.work_dir, f"{pipeline_type.value}.log")
        self._setup_logging(log_file)

    def _setup_logging(self, log_file: str):
        """Configure pipeline-specific logging."""
        handler = logging.FileHandler(log_file)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    def _create_tracker(
        self, result_mapper, results_model=DataProcessResults
    ) -> PipelineTracker:
        """Create and configure pipeline tracker."""
        run_identifier = self._get_run_identifier()

        initial_params = {
            "sampleName": self._get_sample_name(),
            "username": self.config.username,
            "beamline": self.config.beamline,
            "workdir": self.config.work_dir,
            "datasets": json.dumps([ds.master_file for ds in self.datasets]),
            "imagedir": (
                os.path.dirname(self.datasets[0].master_file) if self.datasets else ""
            ),
            "primary_group": self.config.primary_group,
            "pi_id": self.config.pi_id,
            "esaf_id": self.config.esaf_id,
            "command": " ".join(sys.argv),
        }

        # Get default from central config
        redis_host = ServerConfig.get_redis_hosts().get("analysis_results", "127.0.0.1")
        redis_config = {"host": redis_host, "db": 0}

        return PipelineTracker(
            pipeline_name=self.pipeline_type.value,
            run_identifier=run_identifier,
            initial_params=initial_params,
            result_mapper=result_mapper,
            redis_config=redis_config,
            results_model=results_model,
        )

    def _get_run_identifier(self) -> str:
        """Generate unique run identifier."""
        if self.datasets:
            return os.path.basename(self.datasets[0].master_file)
        return f"{self.pipeline_type.value}_{int(time.time())}"

    def _get_sample_name(self) -> str:
        """Determine sample name for tracking."""
        if self.config.sample_name:
            return self.config.sample_name

        if self.datasets:
            master_file = self.datasets[0].master_file
            return (
                os.path.basename(master_file)
                .replace("_master.h5", "")
                .replace(".h5", "")
                .replace(".cbf", "")
            )

        return "unknown_sample"

    @abstractmethod
    def _validate_inputs(self) -> bool:
        """Validate pipeline-specific inputs."""
        pass

    @abstractmethod
    def _construct_command(self) -> str:
        """Construct the command to execute the pipeline."""
        pass

    @abstractmethod
    def _parse_results(self) -> Dict[str, Any]:
        """Parse pipeline output and extract results."""
        pass

    @abstractmethod
    def _get_result_mapper(self):
        """Get function to map results to database fields."""
        pass

    def run(self) -> PipelineResult:
        """Execute the pipeline with full tracking and error handling."""
        start_time = time.time()
        result = PipelineResult(
            pipeline_type=self.pipeline_type.value,
            job_status=JobStatus.PENDING,
            work_dir=self.config.work_dir,
            start_time=start_time,
        )

        try:
            # Input validation
            if not self._validate_inputs():
                raise ValueError("Pipeline input validation failed")

            # Initialize tracker
            self.tracker = self._create_tracker(self._get_result_mapper())
            self.tracker.start()
            result.pipeline_status_id = self.tracker.pipeline_status_id

            logger.info(f"Starting {self.pipeline_type.value} pipeline")
            logger.info(f"Work directory: {self.config.work_dir}")
            logger.info(f"Datasets: {[ds.master_file for ds in self.datasets]}")

            # Construct and log command
            command = self._construct_command()
            logger.info(f"Command: {command}")

            result.job_status = JobStatus.RUNNING
            self.tracker.update_progress("RUNNING", {"command": command})

            # Execute pipeline
            self._execute_command(command)

            # Parse results
            logger.info("Parsing pipeline results...")
            self.tracker.update_progress("PARSING", {})

            parsed_results = self._parse_results()
            if not parsed_results:
                raise RuntimeError("Failed to parse any results from pipeline output")

            result.results = parsed_results
            result.job_status = JobStatus.SUCCESS
            result.end_time = time.time()

            # Save results summary
            self._save_results_summary(result)

            # Update tracker with success
            self.tracker.succeed(parsed_results)
            result.result_pk_value = self.tracker.result_pk_value

            logger.info(f"{self.pipeline_type.value} pipeline completed successfully")

        except Exception as e:
            error_msg = f"Pipeline failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            result.job_status = JobStatus.FAILED
            result.error_message = error_msg
            result.end_time = time.time()

            if self.tracker:
                self.tracker.fail(error_msg, result.results)

        return result

    def _execute_command(self, command: str):
        """Execute the pipeline command."""
        from qp2.image_viewer.utils.run_job import run_command

        run_command(
            command,
            cwd=self.config.work_dir,
            method=self.config.runner,
            nodes=self.config.njobs,
            processors=self.config.nproc,
            job_name=f"{self.pipeline_type.value}_{os.path.basename(self.config.work_dir)}",
        )

    def _save_results_summary(self, result: PipelineResult):
        """Save pipeline results to JSON file."""
        summary_file = os.path.join(self.config.work_dir, "pipeline_summary.json")

        with open(summary_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2, default=str)

        result.output_files.append(summary_file)
        logger.info(f"Results summary saved to: {summary_file}")


class AutoPROCPipeline(BasePipeline):
    """AutoPROC pipeline implementation."""

    def __init__(self, datasets: List[DatasetSpec], config: PipelineConfig):
        super().__init__(PipelineType.AUTOPROC, datasets, config)

    def _validate_inputs(self) -> bool:
        """Validate AutoPROC-specific inputs."""
        if not self.datasets:
            logger.error("No datasets provided for AutoPROC")
            return False

        for ds in self.datasets:
            if not ds.master_file.endswith((".h5", ".hdf5")):
                logger.error(f"AutoPROC requires HDF5 files: {ds.master_file}")
                return False

        return True

    def _construct_command(self) -> str:
        """Construct AutoPROC command."""
        setup_cmd = [ProgramConfig.get_setup_command('autoproc')]

        process_cmd = ["process -d ."]

        # Add datasets
        for i, ds in enumerate(self.datasets):
            if ds.frame_range and len(ds.frame_range) == 2:
                start_frame, end_frame = ds.frame_range
                base_id = (
                    os.path.basename(ds.master_file)
                    .replace("_master.h5", "")
                    .replace(".h5", "")
                )
                sweep_id = f"sweep{i + 1}_{base_id}"
                image_dir = os.path.dirname(ds.master_file)
                template = ds.master_file
                id_string = (
                    f'"{sweep_id},{image_dir},{template},{start_frame},{end_frame}"'
                )
                process_cmd.append(f"-Id {id_string}")
            else:
                process_cmd.append(f"-h5 {ds.master_file}")

        # Add processing parameters
        if self.config.highres and self.config.lowres:
            process_cmd.append(f"-R {self.config.lowres} {self.config.highres}")
        elif self.config.highres:
            process_cmd.append(f"-R 45.0 {self.config.highres}")
        else:
            process_cmd.append("-M HighResCutOnCChalf")

        if self.config.space_group:
            process_cmd.append(f'symm="{self.config.space_group}"')
        if self.config.unit_cell:
            process_cmd.append(f'cell="{self.config.unit_cell}"')

        # Model for molecular replacement
        if self.config.model:
            process_cmd.append(f"-M MR")
            process_cmd.append(f'MR_MODEL="{self.config.model}"')

        # Anomalous data processing
        if not self.config.native:
            process_cmd.append("-ANO")

        # Friedel pairs handling
        if not self.config.friedel_pairs:
            process_cmd.append("-M TruncateNoFriedel")

        # Wavelength
        if self.config.wavelength:
            process_cmd.append(f'WAVELENGTH="{self.config.wavelength}"')

        # Beam center
        if self.config.beam_center:
            beam_x, beam_y = self.config.beam_center
            process_cmd.append(f'BEAM_CENTER="{beam_x} {beam_y}"')

        # Detector distance
        if self.config.detector_distance:
            process_cmd.append(f'DETECTOR_DISTANCE="{self.config.detector_distance}"')

        # Job control parameters
        if self.config.njobs > 1:
            process_cmd.append(
                f"autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_JOBS={self.config.njobs}"
            )

        if self.config.nproc > 1:
            process_cmd.append(
                f"autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_PROCESSORS={self.config.nproc}"
            )
            process_cmd.append(f"-nthreads {self.config.nproc}")

        if self.config.fast_mode:
            process_cmd.append("-M fast")

        return "\n".join(setup_cmd) + "\n" + " \\\n  ".join(process_cmd)

    def _parse_results(self) -> Dict[str, Any]:
        """Parse AutoPROC results."""
        from qp2.pipelines.autoproc_xia2.autoproc_xml_parser import AutoPROCXmlParser
        from qp2.pipelines.autoproc_xia2.aimless_parser import AimlessParser

        # Try XML files first
        xml_files = ["autoPROC_staraniso.xml", "autoPROC.xml"]
        for xml_file in xml_files:
            xml_path = os.path.join(self.config.work_dir, xml_file)
            if os.path.exists(xml_path):
                try:
                    parser = AutoPROCXmlParser(
                        wdir=self.config.work_dir, filename=xml_file
                    )
                    results = parser.summarize()
                    if results:
                        results["sampleName"] = self._get_sample_name()
                        return results
                except Exception as e:
                    logger.warning(f"Failed to parse {xml_file}: {e}")

        # Fallback to aimless.log
        for root, _, files in os.walk(self.config.work_dir):
            if "aimless.log" in files:
                try:
                    parser = AimlessParser(wdir=root)
                    results = parser.summarize()
                    results["sampleName"] = self._get_sample_name()
                    results["report_url"] = os.path.join(
                        self.config.work_dir, "summary.html"
                    )
                    return results
                except Exception as e:
                    logger.warning(f"Failed to parse aimless.log: {e}")

        return {}

    def _get_result_mapper(self):
        """Get result mapper for AutoPROC."""

        def mapper(results: Dict[str, Any]) -> Dict[str, str]:
            mapped = {
                "sampleName": results.get("sampleName") or results.get("prefix"),
                "workdir": self.config.work_dir,
                "highresolution": results.get("highresolution"),
                "spacegroup": results.get("spacegroup"),
                "unitcell": results.get("unitcell"),
                "rmerge": results.get("rmerge"),
                "rmeas": results.get("rmeas"),
                "rpim": results.get("rpim"),
                "isigmai": results.get("isigmai"),
                "multiplicity": results.get("multiplicity"),
                "completeness": results.get("completeness"),
                "anom_completeness": results.get("anom_completeness"),
                "table1": results.get("table1"),
                "cchalf": results.get("cchalf"),
                "nobs": results.get("Nobs"),
                "nuniq": results.get("Nuniq"),
                "report_url": results.get("report_url"),
                "truncate_mtz": results.get("truncate_mtz"),
                "scale_log": results.get("scale_log"),
                "run_stats": json.dumps(results, default=str),
            }
            return {k: str(v) for k, v in mapped.items() if v is not None}

        return mapper


class Xia2Pipeline(BasePipeline):
    """Xia2 pipeline implementation."""

    def __init__(self, datasets: List[DatasetSpec], config: PipelineConfig):
        super().__init__(PipelineType.XIA2, datasets, config)
        self.project = config.pipeline_options.get("project", "xia2_project")
        self.crystal = self._get_crystal_name()

    def _validate_inputs(self) -> bool:
        """Validate Xia2-specific inputs."""
        if not self.datasets:
            logger.error("No datasets provided for Xia2")
            return False

        for ds in self.datasets:
            if not ds.master_file.endswith((".h5", ".hdf5")):
                logger.error(f"Xia2 requires HDF5 files: {ds.master_file}")
                return False

        return True

    def _get_crystal_name(self) -> str:
        """Generate crystal name for xia2."""
        sample_name = self._get_sample_name()
        crystal_name = sample_name

        if crystal_name[0].isdigit():
            crystal_name = f"p_{crystal_name}"
        crystal_name = crystal_name.replace("-", "_").replace(".", "_")

        return crystal_name

    def _construct_command(self) -> str:
        """Construct Xia2 command."""
        njobs = min(len(self.datasets), 4)

        dials_setup = self.config.pipeline_options.get(
            "dials_setup", ProgramConfig.get_setup_command('dials')
        )
        fast_mode = "dials.fast_mode=True" if self.config.fast_mode else ""

        cmd = [
            f"#SBATCH --ntasks-per-node={self.config.nproc * njobs}",
            "#SBATCH --nodes=1",
            "unset HDF5_PLUGIN_PATH",
            dials_setup,
            "",
            f"xia2 failover=True read_all_image_headers=False trust_beam_centre=True {fast_mode}",
        ]

        # Add datasets
        for ds in self.datasets:
            if ds.frame_range and len(ds.frame_range) == 2:
                cmd.append(
                    f"image={ds.master_file}:{ds.frame_range[0]}:{ds.frame_range[1]}"
                )
            else:
                cmd.append(f"image={ds.master_file}")

        # Add processing parameters
        if self.config.highres:
            cmd.append(f"xia2.settings.resolution.d_min={self.config.highres}")
        if self.config.lowres:
            cmd.append(f"xia2.settings.resolution.d_max={self.config.lowres}")

        if self.config.space_group:
            cmd.append(f"xia2.settings.space_group='{self.config.space_group}'")
            if self.config.unit_cell:
                cmd.append(f'xia2.settings.unit_cell="{self.config.unit_cell}"')

        # Model for molecular replacement
        if self.config.model:
            cmd.append(f'reference_reflection_file="{self.config.model}"')

        # Anomalous data processing
        if not self.config.native:
            cmd.append("xia2.settings.small_molecule=False")
            cmd.append("xia2.settings.anomalous=True")

        # Friedel pairs
        if not self.config.friedel_pairs:
            cmd.append("xia2.settings.merge_anomalous=True")

        # Beam center override
        if self.config.beam_center:
            beam_x, beam_y = self.config.beam_center
            cmd.append(f'xia2.settings.beam.centre="{beam_x},{beam_y}"')

        # Detector distance
        if self.config.detector_distance:
            cmd.append(f"xia2.settings.distance={self.config.detector_distance}")

        # Wavelength
        if self.config.wavelength:
            cmd.append(f"xia2.settings.wavelength={self.config.wavelength}")

        # Parallelization
        cmd.append("multiprocessing.mode=parallel")
        cmd.append(f"multiprocessing.njob={njobs}")
        cmd.append(f"multiprocessing.nproc={self.config.nproc}")

        # Pipeline type
        pipeline_type = self.config.pipeline_options.get("pipeline_type", "dials")
        if pipeline_type == "dials-aimless":
            cmd.append("pipeline=dials-aimless")
        elif pipeline_type == "dials":
            cmd.append("pipeline=dials")
        elif pipeline_type == "xds":
            cmd.append("pipeline=3d")

        cmd.append(f"project={self.project}")
        cmd.append(f"crystal={self.crystal}")

        return " ".join(cmd)

    def _parse_results(self) -> Dict[str, Any]:
        """Parse Xia2 results."""
        from qp2.pipelines.autoproc_xia2.xia2_parser import Xia2Parser
        from qp2.pipelines.autoproc_xia2.aimless_parser import AimlessParser

        results = {}

        # Try xia2.txt first
        xia2_txt = os.path.join(self.config.work_dir, "xia2.txt")
        if os.path.exists(xia2_txt):
            try:
                parser = Xia2Parser(wdir=self.config.work_dir, filename="xia2.txt")
                results = parser.summarize() or {}
            except Exception as e:
                logger.warning(f"Failed to parse xia2.txt: {e}")

        # Try aimless.log as fallback
        if not results:
            for root, _, files in os.walk(self.config.work_dir):
                if "aimless.log" in files:
                    try:
                        parser = AimlessParser(wdir=root, filename="aimless.log")
                        results = parser.summarize()
                        break
                    except Exception as e:
                        logger.warning(f"Failed to parse aimless.log: {e}")

        if results:
            results["sampleName"] = self._get_sample_name()
            results.setdefault(
                "report_url", os.path.join(self.config.work_dir, "xia2.html")
            )
            results.setdefault("logfile", xia2_txt)

            # Find output MTZ
            if "truncate_mtz" not in results:
                mtz_path = os.path.join(
                    self.config.work_dir,
                    "DataFiles",
                    f"{self.project}_{self.crystal}_free.mtz",
                )
                if os.path.exists(mtz_path):
                    results["truncate_mtz"] = mtz_path

        return results

    def _get_result_mapper(self):
        """Get result mapper for Xia2."""
        return self._get_default_result_mapper()

    def _get_default_result_mapper(self):
        """Default result mapper for data processing pipelines."""

        def mapper(results: Dict[str, Any]) -> Dict[str, str]:
            mapped = {
                "sampleName": results.get("sampleName") or results.get("prefix"),
                "workdir": self.config.work_dir,
                "highresolution": results.get("highresolution"),
                "spacegroup": results.get("spacegroup"),
                "unitcell": results.get("unitcell"),
                "rmerge": results.get("rmerge"),
                "rmeas": results.get("rmeas"),
                "rpim": results.get("rpim"),
                "isigmai": results.get("isigmai"),
                "multiplicity": results.get("multiplicity"),
                "completeness": results.get("completeness"),
                "anom_completeness": results.get("anom_completeness"),
                "table1": results.get("table1"),
                "cchalf": results.get("cchalf"),
                "nobs": results.get("Nobs"),
                "nuniq": results.get("Nuniq"),
                "report_url": results.get("report_url"),
                "truncate_mtz": results.get("truncate_mtz"),
                "scale_log": results.get("scale_log"),
                "run_stats": json.dumps(results, default=str),
            }
            return {k: str(v) for k, v in mapped.items() if v is not None}

        return mapper


class Xia2SSXPipeline(BasePipeline):
    """Xia2 SSX pipeline implementation."""

    def __init__(self, datasets: List[DatasetSpec], config: PipelineConfig):
        super().__init__(PipelineType.XIA2_SSX, datasets, config)

    def _validate_inputs(self) -> bool:
        """Validate Xia2 SSX inputs."""
        if not self.datasets:
            logger.error("No datasets provided for Xia2 SSX")
            return False

        for ds in self.datasets:
            # xia2.ssx accepts directories or image files (h5, cbf, etc)
            if not os.path.exists(ds.master_file):
                logger.error(f"Input path does not exist: {ds.master_file}")
                return False
        return True

    def _construct_command(self) -> str:
        """Construct Xia2 SSX command."""
        # Setup dials environment
        # User specified "module dials"
        setup_cmd = [ProgramConfig.get_setup_command('dials')]

        cmd = ["xia2.ssx"]

        # Add inputs
        for ds in self.datasets:
            if os.path.isdir(ds.master_file):
                cmd.append(f"directory={ds.master_file}")
            else:
                # Assuming file input is treated as 'image='
                cmd.append(f"image={ds.master_file}")

        # Add processing parameters
        if self.config.space_group:
            cmd.append(f"space_group={self.config.space_group}")

        if self.config.unit_cell:
            cmd.append(f"unit_cell={self.config.unit_cell}")

        # Reference/Model
        reference = self.config.model or self.config.scaling_reference
        if reference:
            cmd.append(f"reference={reference}")

        # Parallelization
        if self.config.nproc:
            cmd.append(f"nproc={self.config.nproc}")
        if self.config.njobs:
            cmd.append(f"njobs={self.config.njobs}")

        # Add any other pipeline options passed as key=value
        if self.config.pipeline_options:
            for key, value in self.config.pipeline_options.items():
                # Skip internal options or handled ones if necessary
                if key not in ["program", "molsize", "pipeline_type", "reference_dataset", "powder", "variant"]:
                    cmd.append(f"{key}={value}")

        full_cmd = "\n".join(setup_cmd) + "\n" + " ".join(cmd)
        return full_cmd

    def _parse_results(self) -> Dict[str, Any]:
        """Parse Xia2 SSX results."""
        from qp2.pipelines.autoproc_xia2.xia2_parser import Xia2Parser
        from qp2.pipelines.autoproc_xia2.aimless_parser import AimlessParser

        results = {}

        # Check for xia2.ssx specific logs or standard xia2 output
        # xia2.ssx might produce xia2.txt or similar.
        # It often produces 'xia2.ssx.log' or standard output captured in a log file.
        
        # Try standard Xia2 parsers first as fallback or primary if structure is similar
        xia2_txt = os.path.join(self.config.work_dir, "xia2.txt")
        if os.path.exists(xia2_txt):
            try:
                parser = Xia2Parser(wdir=self.config.work_dir, filename="xia2.txt")
                results = parser.summarize() or {}
            except Exception as e:
                logger.warning(f"Failed to parse xia2.txt: {e}")
        
        # Try aimless.log
        if not results:
            for root, _, files in os.walk(self.config.work_dir):
                if "aimless.log" in files:
                    try:
                        parser = AimlessParser(wdir=root, filename="aimless.log")
                        results = parser.summarize()
                        break
                    except Exception as e:
                        logger.warning(f"Failed to parse aimless.log: {e}")

        # If we still have no results, maybe parse the main log file (xia2_ssx.log)
        # defined in BasePipeline as f"{pipeline_type.value}.log" -> xia2_ssx.log
        if not results:
             log_file = os.path.join(self.config.work_dir, "xia2_ssx.log")
             if os.path.exists(log_file):
                 # TODO: Implement specific log parsing for xia2.ssx if needed
                 pass

        if results:
            results["sampleName"] = self._get_sample_name()
            # xia2.ssx output html report?
            results.setdefault(
                "report_url", os.path.join(self.config.work_dir, "xia2.html")
            )
            
        return results

    def _get_result_mapper(self):
        """Get result mapper for Xia2 SSX."""
        return self._get_default_result_mapper()

    def _get_default_result_mapper(self):
        """Default result mapper for data processing pipelines."""

        def mapper(results: Dict[str, Any]) -> Dict[str, str]:
            mapped = {
                "sampleName": results.get("sampleName") or results.get("prefix"),
                "workdir": self.config.work_dir,
                "highresolution": results.get("highresolution"),
                "spacegroup": results.get("spacegroup"),
                "unitcell": results.get("unitcell"),
                "rmerge": results.get("rmerge"),
                "rmeas": results.get("rmeas"),
                "rpim": results.get("rpim"),
                "isigmai": results.get("isigmai"),
                "multiplicity": results.get("multiplicity"),
                "completeness": results.get("completeness"),
                "anom_completeness": results.get("anom_completeness"),
                "table1": results.get("table1"),
                "cchalf": results.get("cchalf"),
                "nobs": results.get("Nobs"),
                "nuniq": results.get("Nuniq"),
                "report_url": results.get("report_url"),
                "truncate_mtz": results.get("truncate_mtz"),
                "scale_log": results.get("scale_log"),
                "run_stats": json.dumps(results, default=str),
            }
            return {k: str(v) for k, v in mapped.items() if v is not None}

        return mapper


class GMCAProcPipeline(BasePipeline):
    """GMCA XDS/nXDS processing pipeline implementation."""

    def __init__(self, datasets: List[DatasetSpec], config: PipelineConfig):
        super().__init__(PipelineType.GMCAPROC, datasets, config)

        # Determine pipeline variant: XDS or nXDS
        self.pipeline_variant = config.pipeline_options.get("variant", "nxds").lower()
        if self.pipeline_variant not in ["xds", "nxds"]:
            self.pipeline_variant = "nxds"  # Default to nXDS

    def _validate_inputs(self) -> bool:
        """Validate GMCA XDS/nXDS inputs."""
        if not self.datasets:
            logger.error("No datasets provided for GMCA processing")
            return False

        for ds in self.datasets:
            if not (ds.master_file.endswith((".h5", ".hdf5", ".cbf"))):
                logger.error(
                    f"GMCA processing requires HDF5 or CBF files: {ds.master_file}"
                )
                return False

        return True

    def _construct_command(self) -> str:
        """Construct GMCA XDS/nXDS command using xds2.py."""
        if self.pipeline_variant == "nxds":
            return self._construct_nxds_command()
        else:
            return self._construct_xds_command()

    def _construct_nxds_command(self) -> str:
        """Construct nXDS command using nxds_proc.py wrapper."""
        cmd_parts = [
            "python",
            os.path.join(os.path.dirname(__file__), "gmcaproc", "nxds_proc.py"),
            f"--proc_dir_root {self.config.work_dir}",
            f"--nproc {self.config.nproc}",
            f"--njobs {self.config.njobs}",
        ]

        # Add processing parameters
        if self.config.space_group:
            cmd_parts.append(f"--symm '{self.config.space_group}'")

        if self.config.unit_cell:
            cmd_parts.append(f"--unitcell '{self.config.unit_cell}'")

        # Reference dataset for scaling
        reference = (
            self.config.pipeline_options.get("reference_dataset")
            or self.config.scaling_reference
        )
        if reference:
            cmd_parts.append(f"--reference_dataset '{reference}'")

        # Wavelength override
        if self.config.wavelength:
            cmd_parts.append(f"--xds_param 'X-RAY_WAVELENGTH={self.config.wavelength}'")

        # Enable powder for ice ring detection
        if self.config.pipeline_options.get("powder", False):
            cmd_parts.append("--powder")

        # Add datasets (nxds_proc handles multiple datasets)
        for ds in self.datasets:
            cmd_parts.append(f"--data {ds.master_file}")

        return " ".join(cmd_parts)

    def _construct_xds_command(self) -> str:
        """Construct XDS command using xds2.py or xscale_process_dataset.py."""
        if len(self.datasets) > 1:
            # Merged XDS workflow
            script_path = os.path.join(
                os.path.dirname(__file__), "..", "image_viewer", "plugins", "xds", "xscale_process_dataset.py"
            )
            cmd_parts = [
                "python",
                script_path,
                f"--proc_dir {self.config.work_dir}",
                f"--nproc {self.config.nproc}",
                f"--njobs {self.config.njobs}",
            ]
            
            for ds in self.datasets:
                cmd_parts.append(f"--master_file {ds.master_file}")
                
            if self.config.space_group:
                cmd_parts.append(f"--space_group '{self.config.space_group}'")
            if self.config.unit_cell:
                cmd_parts.append(f"--unit_cell '{self.config.unit_cell}'")
            if self.config.highres:
                cmd_parts.append(f"--resolution {self.config.highres}")
            if self.config.native:
                cmd_parts.append("--native")
            if self.config.wavelength:
                cmd_parts.append(f"--xds_param 'X-RAY_WAVELENGTH={self.config.wavelength}'")
            
            reference = (
                self.config.pipeline_options.get("reference_dataset")
                or self.config.scaling_reference
            )
            if reference:
                cmd_parts.append(f"--reference_hkl '{reference}'")
                
            # Redis integration for tracking (optional, but good for uniformity)
            # pipeline_driver usually doesn't pass redis info from CLI, but xscale_process needs it for keys.
            # We can use placeholder keys as xscale_process_dataset handles its own tracking.
            cmd_parts.extend([
                "--redis_key 'pipeline:merged:xds'",
                "--status_key 'pipeline:merged:xds:status'"
            ])
            
            return " ".join(cmd_parts)

        # For single dataset, use original individual processing
        commands = []
        for ds in self.datasets:
            dataset_name = os.path.splitext(os.path.basename(ds.master_file))[0]
            dataset_dir = os.path.join(self.config.work_dir, dataset_name)

            cmd_parts = [
                "python",
                os.path.join(os.path.dirname(__file__), "gmcaproc", "xds2.py"),
                f"--data {ds.master_file}",
                f"--proc_dir {dataset_dir}",
                f"--nproc {self.config.nproc}",
                f"--njobs {self.config.njobs}",
            ]

            # Add processing parameters
            if self.config.space_group:
                cmd_parts.append(f"--symm '{self.config.space_group}'")

            if self.config.unit_cell:
                cmd_parts.append(f"--unitcell '{self.config.unit_cell}'")

            if self.config.highres:
                cmd_parts.append(f"--highres {self.config.highres}")

            if self.config.native:
                cmd_parts.append("--native")

            if self.config.wavelength:
                cmd_parts.append(f"--xds_param 'X-RAY_WAVELENGTH={self.config.wavelength}'")

            # Reference dataset
            reference = (
                self.config.pipeline_options.get("reference_dataset")
                or self.config.scaling_reference
            )
            if reference:
                cmd_parts.append(f"--reference_hkl '{reference}'")

            # Strategy mode for few images
            if ds.frame_range and len(ds.frame_range) == 2:
                start, end = ds.frame_range
                if (end - start + 1) <= 5:
                    cmd_parts.append("--strategy")
                cmd_parts.append(f"--user_start {start}")
                cmd_parts.append(f"--user_end {end}")

            # Use SLURM if specified
            if self.config.runner == "slurm":
                cmd_parts.append("--use_slurm")

            commands.append(" ".join(cmd_parts))

        # Join commands with && to run sequentially
        return " && ".join(commands)

    def _parse_results(self) -> Dict[str, Any]:
        """Parse GMCA XDS/nXDS results."""
        results = {"datasets": [], "pipeline_variant": self.pipeline_variant}

        # Parse results for each dataset
        for ds in self.datasets:
            dataset_name = os.path.splitext(os.path.basename(ds.master_file))[0]
            dataset_dir = os.path.join(self.config.work_dir, dataset_name)

            dataset_results = self._parse_dataset_results(
                dataset_dir, dataset_name, ds.master_file
            )
            if dataset_results:
                results["datasets"].append(dataset_results)

        # If only one dataset, promote its results to top level
        if len(results["datasets"]) == 1:
            dataset_results = results["datasets"][0]
            results.update(dataset_results)

        results["sampleName"] = self._get_sample_name()
        results["pipeline_type"] = f"gmcaproc_{self.pipeline_variant}"

        return results

    def _parse_dataset_results(
        self, dataset_dir: str, dataset_name: str, master_file: str
    ) -> Dict[str, Any]:
        """Parse results for a single dataset using appropriate parser."""
        if not os.path.exists(dataset_dir):
            return {}

        results = {
            "dataset_name": dataset_name,
            "dataset_dir": dataset_dir,
            "master_file": master_file,
        }

        # Parse based on pipeline variant
        if self.pipeline_variant == "nxds":
            results.update(self._parse_nxds_results(dataset_dir))
        else:
            results.update(self._parse_xds_results(dataset_dir))

        return results

    def _parse_nxds_results(self, dataset_dir: str) -> Dict[str, Any]:
        """Parse nXDS-specific results."""
        from qp2.pipelines.gmcaproc.nxds_parsers import (
            parse_nxds_idxref_log,
            parse_nxscale_or_ncorrect_lp,
            parse_spot_nxds,
        )

        results = {}

        # Parse nXDS IDXREF log
        idxref_log = os.path.join(dataset_dir, "nxds_idxref.log")
        if os.path.exists(idxref_log):
            try:
                nxds_results = parse_nxds_idxref_log(idxref_log)
                results.update(nxds_results)
            except Exception as e:
                logger.warning(f"Failed to parse nXDS IDXREF log: {e}")

        # Parse nXDS scaling results
        ncorrect_lp = os.path.join(dataset_dir, "NCORRECT.LP")
        nxscale_lp = os.path.join(dataset_dir, "NXSCALE.LP")

        for scale_file in [ncorrect_lp, nxscale_lp]:
            if os.path.exists(scale_file):
                try:
                    scale_results = parse_nxscale_or_ncorrect_lp(scale_file)
                    results.update(scale_results)
                    break
                except Exception as e:
                    logger.warning(f"Failed to parse {scale_file}: {e}")

        # Parse nXDS spot file
        spot_file = os.path.join(dataset_dir, "SPOT.NXDS")
        if os.path.exists(spot_file):
            try:
                spot_results = parse_spot_nxds(spot_file)
                results.update(spot_results)
            except Exception as e:
                logger.warning(f"Failed to parse SPOT.NXDS: {e}")

        # Look for output files
        hkl_file = os.path.join(dataset_dir, "XDS_ASCII.HKL")
        if os.path.exists(hkl_file):
            results["hkl_file"] = hkl_file

        mtz_file = os.path.join(dataset_dir, f"{os.path.basename(dataset_dir)}.mtz")
        if os.path.exists(mtz_file):
            results["mtz_file"] = mtz_file

        return results

    def _parse_xds_results(self, dataset_dir: str) -> Dict[str, Any]:
        """Parse XDS results."""
        from qp2.pipelines.gmcaproc.xds_parsers import parse_correct_lp, parse_idxref_lp

        results = {}

        # Parse XDS CORRECT.LP
        correct_lp = os.path.join(dataset_dir, "CORRECT.LP")
        if os.path.exists(correct_lp):
            try:
                xds_results = parse_correct_lp(correct_lp)
                results.update(xds_results)
            except Exception as e:
                logger.warning(f"Failed to parse CORRECT.LP: {e}")

        # Parse XDS IDXREF.LP
        idxref_lp = os.path.join(dataset_dir, "IDXREF.LP")
        if os.path.exists(idxref_lp):
            try:
                idxref_results = parse_idxref_lp(idxref_lp)
                results.update(idxref_results)
            except Exception as e:
                logger.warning(f"Failed to parse IDXREF.LP: {e}")

        # Look for output files
        hkl_file = os.path.join(dataset_dir, "XDS_ASCII.HKL")
        if os.path.exists(hkl_file):
            results["hkl_file"] = hkl_file

        return results

    def _get_result_mapper(self):
        """Get result mapper for GMCA nXDS."""
        return self._get_default_result_mapper()


class StrategyPipeline(BasePipeline):
    """Strategy calculation pipeline implementation."""

    def __init__(self, datasets: List[DatasetSpec], config: PipelineConfig):
        super().__init__(PipelineType.STRATEGY, datasets, config)
        self._create_tracker_strategy = lambda: self._create_tracker(
            self._get_result_mapper(), ScreenStrategyResults
        )

    def _validate_inputs(self) -> bool:
        """Validate strategy inputs."""
        if not self.datasets:
            logger.error("No datasets provided for strategy calculation")
            return False

        program = self.config.pipeline_options.get("program", "mosflm")
        if program not in ["mosflm", "xds"]:
            logger.error(f"Invalid strategy program: {program}")
            return False

        return True

    def _create_tracker(self, result_mapper, results_model=ScreenStrategyResults):
        """Override to use strategy-specific model."""
        return super()._create_tracker(result_mapper, results_model)

    def _construct_command(self) -> str:
        """Construct strategy command."""
        program = self.config.pipeline_options.get("program", "mosflm")
        molsize = self.config.pipeline_options.get("molsize")

        # Prepare dataset mapping
        mapping = {}
        for ds in self.datasets:
            if ds.frame_range:
                mapping[ds.master_file] = ds.frame_range
            else:
                mapping[ds.master_file] = [1]  # Default to first frame

        # Use the strategy main module
        cmd_parts = [
            "python",
            os.path.join(os.path.dirname(__file__), "strategy", "main.py"),
            f"--program {program}",
            f"--workdir {self.config.work_dir}",
            f'"{json.dumps(mapping)}"',
        ]

        if molsize:
            cmd_parts.append(f"--molsize {molsize}")

        if self.config.username:
            cmd_parts.append(f"--username {self.config.username}")

        if self._get_sample_name():
            cmd_parts.append(f"--sampleName {self._get_sample_name()}")

        if self.config.esaf_id:
            cmd_parts.append(f"--esaf_id {self.config.esaf_id}")

        if self.config.pi_id:
            cmd_parts.append(f"--pi_id {self.config.pi_id}")

        if self.config.primary_group:
            cmd_parts.append(f"--primary_group {self.config.primary_group}")

        return " ".join(cmd_parts)

    def _parse_results(self) -> Dict[str, Any]:
        """Parse strategy results."""
        # Strategy should output JSON results
        strategy_json = os.path.join(self.config.work_dir, "strategy_results.json")
        if os.path.exists(strategy_json):
            try:
                with open(strategy_json, "r") as f:
                    results = json.load(f)
                    results["sampleName"] = self._get_sample_name()
                    return results
            except Exception as e:
                logger.warning(f"Failed to parse strategy JSON: {e}")

        # Fallback: look for specific result files
        results = {"sampleName": self._get_sample_name()}

        # Look for MOSFLM or XDS output
        program = self.config.pipeline_options.get("program", "mosflm")
        if program == "mosflm":
            results.update(self._parse_mosflm_results())
        else:
            results.update(self._parse_xds_results())

        return results

    def _parse_mosflm_results(self) -> Dict[str, Any]:
        """Parse MOSFLM strategy results."""
        results = {}

        mosflm_log = os.path.join(self.config.work_dir, "mosflm.lp")
        if os.path.exists(mosflm_log):
            try:
                from qp2.pipelines.strategy.mosflm.mosflm_parsers import (
                    parse_strategy_log,
                )

                results = parse_strategy_log(mosflm_log)
            except Exception as e:
                logger.warning(f"Failed to parse MOSFLM log: {e}")

        return results

    def _parse_xds_results(self) -> Dict[str, Any]:
        """Parse XDS strategy results."""
        results = {}

        xplan_lp = os.path.join(self.config.work_dir, "XPLAN.LP")
        if os.path.exists(xplan_lp):
            try:
                from qp2.pipelines.gmcaproc.xds_parsers import parse_xplan_lp

                results = parse_xplan_lp(xplan_lp)
            except Exception as e:
                logger.warning(f"Failed to parse XPLAN.LP: {e}")

        return results

    def _get_result_mapper(self):
        """Get result mapper for strategy."""

        def mapper(results: Dict[str, Any]) -> Dict[str, str]:
            mapped = {
                "sampleName": results.get("sampleName"),
                "workdir": self.config.work_dir,
                "spacegroup": results.get("spacegroup_symbol")
                or results.get("spacegroup_number"),
                "unitcell": results.get("unitcell"),
                "osc_start": results.get("osc_start"),
                "osc_end": results.get("osc_end"),
                "osc_delta": results.get("osc_delta"),
                "completeness_native": results.get("completeness_native"),
                "completeness_anomalous": results.get("completeness_anomalous"),
                "mosaicity": results.get("mosaicity"),
                "distance": results.get("distance"),
                "resolution_from_spots": results.get("resolution_from_spots"),
                "n_spots": results.get("n_spots"),
                "solvent_content": results.get("solvent_content"),
                "estimated_asu_content_aa": results.get("estimated_asu_content_aa"),
                "score": results.get("score"),
                "software": results.get("software"),
                "raw_results": json.dumps(results, default=str),
            }
            return {k: str(v) for k, v in mapped.items() if v is not None}

        return mapper


class PipelineFactory:
    """Factory for creating pipeline instances."""

    _pipeline_classes = {
        PipelineType.AUTOPROC: AutoPROCPipeline,
        PipelineType.XIA2: Xia2Pipeline,
        PipelineType.XIA2_SSX: Xia2SSXPipeline,
        PipelineType.GMCAPROC: GMCAProcPipeline,
        PipelineType.STRATEGY: StrategyPipeline,
    }

    @classmethod
    def create_pipeline(
        cls,
        pipeline_type: Union[str, PipelineType],
        datasets: List[DatasetSpec],
        config: PipelineConfig,
    ) -> BasePipeline:
        """Create a pipeline instance of the specified type."""
        if isinstance(pipeline_type, str):
            try:
                pipeline_type = PipelineType(pipeline_type.lower())
            except ValueError:
                raise ValueError(f"Unknown pipeline type: {pipeline_type}")

        if pipeline_type not in cls._pipeline_classes:
            raise ValueError(f"No implementation for pipeline type: {pipeline_type}")

        pipeline_class = cls._pipeline_classes[pipeline_type]
        return pipeline_class(datasets, config)

    @classmethod
    def get_supported_pipelines(cls) -> List[str]:
        """Get list of supported pipeline types."""
        return [pt.value for pt in cls._pipeline_classes.keys()]


class PipelineDriver:
    """Main driver for executing crystallographic data processing pipelines."""

    def __init__(self):
        self.results_history: List[PipelineResult] = []

    def run_pipeline(
        self,
        pipeline_type: Union[str, PipelineType],
        datasets: List[Union[str, DatasetSpec]],
        config: PipelineConfig,
    ) -> PipelineResult:
        """
        Run a pipeline with the specified configuration.

        Args:
            pipeline_type: Type of pipeline to run
            datasets: List of dataset specifications or file paths
            config: Pipeline configuration

        Returns:
            Pipeline execution result
        """
        # Normalize datasets
        normalized_datasets = []
        for ds in datasets:
            if isinstance(ds, str):
                normalized_datasets.append(DatasetSpec(ds))
            elif isinstance(ds, DatasetSpec):
                normalized_datasets.append(ds)
            else:
                raise ValueError(f"Invalid dataset specification: {ds}")

        # Create and run pipeline
        pipeline = PipelineFactory.create_pipeline(
            pipeline_type, normalized_datasets, config
        )
        result = pipeline.run()

        # Store result
        self.results_history.append(result)

        return result

    def run_multiple_pipelines(
        self, pipeline_configs: List[Dict[str, Any]], parallel: bool = False
    ) -> List[PipelineResult]:
        """
        Run multiple pipelines with different configurations.

        Args:
            pipeline_configs: List of pipeline configuration dictionaries
            parallel: Whether to run pipelines in parallel

        Returns:
            List of pipeline results
        """
        results = []

        if parallel:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            with ThreadPoolExecutor(
                max_workers=min(len(pipeline_configs), 4)
            ) as executor:
                futures = []

                for config_dict in pipeline_configs:
                    future = executor.submit(self._run_pipeline_from_dict, config_dict)
                    futures.append(future)

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Pipeline execution failed: {e}")
                        # Create failed result
                        failed_result = PipelineResult(
                            pipeline_type="unknown",
                            job_status=JobStatus.FAILED,
                            work_dir="unknown",
                            start_time=time.time(),
                            end_time=time.time(),
                            error_message=str(e),
                        )
                        results.append(failed_result)
        else:
            for config_dict in pipeline_configs:
                try:
                    result = self._run_pipeline_from_dict(config_dict)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Pipeline execution failed: {e}")
                    failed_result = PipelineResult(
                        pipeline_type=config_dict.get("pipeline_type", "unknown"),
                        job_status=JobStatus.FAILED,
                        work_dir=config_dict.get("work_dir", "unknown"),
                        start_time=time.time(),
                        end_time=time.time(),
                        error_message=str(e),
                    )
                    results.append(failed_result)

        return results

    def _run_pipeline_from_dict(self, config_dict: Dict[str, Any]) -> PipelineResult:
        """Run pipeline from configuration dictionary."""
        pipeline_type = config_dict.pop("pipeline_type")
        datasets = config_dict.pop("datasets")

        config = PipelineConfig(**config_dict)
        return self.run_pipeline(pipeline_type, datasets, config)

    def get_pipeline_status(self, work_dir: str) -> Optional[PipelineResult]:
        """Get status of pipeline by work directory."""
        for result in self.results_history:
            if result.work_dir == work_dir:
                return result
        return None

    def get_results_summary(self) -> Dict[str, Any]:
        """Get summary of all pipeline executions."""
        total_runs = len(self.results_history)
        successful_runs = sum(1 for r in self.results_history if r.success)
        failed_runs = total_runs - successful_runs

        pipeline_counts = {}
        for result in self.results_history:
            pipeline_counts[result.pipeline_type] = (
                pipeline_counts.get(result.pipeline_type, 0) + 1
            )

        return {
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "success_rate": successful_runs / total_runs if total_runs > 0 else 0.0,
            "pipeline_counts": pipeline_counts,
            "recent_runs": [r.to_dict() for r in self.results_history[-5:]],
        }


def parse_dataset_argument(data_arg: str) -> DatasetSpec:
    """Parse dataset argument in format: path/to/file.h5 or path/to/file.h5:start:end"""
    parts = data_arg.split(":")
    master_file = parts[0]

    if len(parts) == 1:
        return DatasetSpec(master_file)
    elif len(parts) == 3:
        try:
            start_frame = int(parts[1])
            end_frame = int(parts[2])
            return DatasetSpec(master_file, [start_frame, end_frame])
        except ValueError:
            raise ValueError(f"Invalid frame range in dataset argument: {data_arg}")
    else:
        raise ValueError(f"Invalid dataset argument format: {data_arg}")


def main():
    """Command-line interface for the pipeline driver."""
    setup_logging()

    parser = argparse.ArgumentParser(
        description="QP2 Pipeline Driver - Unified interface for crystallographic data processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run AutoPROC on single dataset
  %(prog)s autoproc --data /path/to/master.h5 --work_dir ./autoproc_run

  # Run AutoPROC with molecular replacement
  %(prog)s autoproc --data /path/to/master.h5 --work_dir ./autoproc_mr \\
      --model /path/to/search.pdb --highres 2.0 --anomalous

  # Run Xia2 with frame range and resolution limits
  %(prog)s xia2 --data /path/to/master.h5:1:100 --work_dir ./xia2_run \\
      --fast --highres 1.8 --lowres 50.0 --beam_center 1024 1024

  # Run Xia2 for anomalous data
  %(prog)s xia2 --data /path/to/master.h5 --work_dir ./anomalous_run \\
      --anomalous --no_friedel_pairs --wavelength 1.5418

  # Run strategy calculation
  %(prog)s strategy --data /path/to/master.h5 --work_dir ./strategy_run \\
      --program mosflm --molsize 300

  # Run GMCA nXDS with comprehensive parameters
  %(prog)s gmcaproc --data /path/to/master.h5 --work_dir ./nxds_run \\
      --scaling_reference /path/to/ref.hkl --powder --variant nxds
  
  # Run GMCA XDS (traditional) processing
  %(prog)s gmcaproc --data /path/to/master.h5 --work_dir ./xds_run \\
      --variant xds --space_group P212121 --detector_distance 300
  
  # Run with multiple datasets
  %(prog)s autoproc --data file1.h5:1:50 --data file2.h5:51:100 \\
      --work_dir ./multi_run --space_group P212121
        """,
    )

    # Required arguments
    parser.add_argument(
        "pipeline_type",
        choices=PipelineFactory.get_supported_pipelines(),
        help="Type of pipeline to run",
    )

    parser.add_argument(
        "--data",
        required=True,
        action="append",
        help="Dataset to process. Format: /path/to/master.h5 or /path/to/master.h5:start:end. Can be specified multiple times.",
    )

    parser.add_argument(
        "--work_dir", required=True, help="Working directory for pipeline execution"
    )

    # Execution parameters
    exec_group = parser.add_argument_group("Execution Parameters")
    exec_group.add_argument(
        "--runner",
        choices=["slurm", "shell"],
        default="slurm",
        help="Job execution method",
    )
    exec_group.add_argument(
        "--nproc", type=int, default=8, help="Number of processors per job"
    )
    exec_group.add_argument(
        "--njobs", type=int, default=1, help="Number of parallel jobs"
    )

    # Processing parameters
    proc_group = parser.add_argument_group("Processing Parameters")
    proc_group.add_argument(
        "--highres", type=float, help="High resolution cutoff (Angstroms)"
    )
    proc_group.add_argument(
        "--lowres", type=float, help="Low resolution cutoff (Angstroms)"
    )
    proc_group.add_argument("--space_group", help="Space group symbol or number")
    proc_group.add_argument(
        "--unit_cell", help="Unit cell parameters: 'a b c alpha beta gamma'"
    )
    proc_group.add_argument(
        "--fast", action="store_true", help="Enable fast processing mode"
    )
    proc_group.add_argument("--model", help="PDB model file for molecular replacement")
    proc_group.add_argument("--sequence", help="FASTA sequence file")
    parser.add_argument(
        "--native", action="store_true", default=True, help="Process data as native"
    )
    proc_group.add_argument(
        "--no_friedel_pairs", action="store_true", help="Do not merge Friedel pairs"
    )
    proc_group.add_argument("--scaling_reference", help="Reference dataset for scaling")

    # Data collection parameters
    data_group = parser.add_argument_group("Data Collection Parameters")
    data_group.add_argument(
        "--wavelength", type=float, help="X-ray wavelength (Angstroms)"
    )
    data_group.add_argument(
        "--beam_center",
        nargs=2,
        type=float,
        metavar=("X", "Y"),
        help="Beam center coordinates in pixels (X Y)",
    )
    data_group.add_argument(
        "--detector_distance", type=float, help="Detector distance (mm)"
    )

    # Output options
    output_group = parser.add_argument_group("Output Options")
    output_group.add_argument(
        "--output_format",
        choices=["mtz", "sca", "xds_ascii"],
        help="Output file format",
    )
    output_group.add_argument(
        "--no_merge", action="store_true", help="Do not merge multiple datasets"
    )
    output_group.add_argument(
        "--no_report", action="store_true", help="Skip HTML report generation"
    )

    # Metadata parameters
    meta_group = parser.add_argument_group("Metadata and Tracking")
    meta_group.add_argument("--sample_name", help="Sample name for tracking")
    meta_group.add_argument(
        "--username", default=os.getenv("USER"), help="Username for job attribution"
    )
    meta_group.add_argument("--beamline", help="Beamline identifier")
    meta_group.add_argument(
        "--esaf_id", type=int, help="ESAF ID for experiment tracking"
    )
    meta_group.add_argument("--pi_id", type=int, help="Principal investigator ID")
    meta_group.add_argument("--primary_group", help="Primary group for experiment")

    # Pipeline-specific options
    pipeline_group = parser.add_argument_group("Pipeline-Specific Options")
    pipeline_group.add_argument(
        "--program", help="Strategy program (mosflm/xds) for strategy pipeline"
    )
    pipeline_group.add_argument(
        "--molsize", type=int, help="Molecule size (residues) for strategy calculations"
    )
    pipeline_group.add_argument(
        "--pipeline_type_variant",
        help="Pipeline variant (e.g., 'dials', 'dials-aimless', 'xds' for xia2)",
    )
    pipeline_group.add_argument(
        "--reference_dataset", help="Reference dataset path for scaling (GMCA XDS/nXDS)"
    )
    pipeline_group.add_argument(
        "--powder", action="store_true", help="Enable powder processing (GMCA nXDS)"
    )
    pipeline_group.add_argument(
        "--variant",
        choices=["xds", "nxds"],
        help="GMCA pipeline variant: 'xds' for traditional XDS, 'nxds' for serial nXDS (default: nxds)",
    )

    args = parser.parse_args()

    try:
        # Parse datasets
        datasets = []
        for data_arg in args.data:
            datasets.append(parse_dataset_argument(data_arg))

        # Build pipeline options
        pipeline_options = {}
        if args.program:
            pipeline_options["program"] = args.program
        if args.molsize:
            pipeline_options["molsize"] = args.molsize
        if args.pipeline_type_variant:
            pipeline_options["pipeline_type"] = args.pipeline_type_variant
        if args.reference_dataset:
            pipeline_options["reference_dataset"] = args.reference_dataset
        if args.powder:
            pipeline_options["powder"] = args.powder
        if args.variant:
            pipeline_options["variant"] = args.variant

        # Create configuration
        config = PipelineConfig(
            work_dir=args.work_dir,
            runner=args.runner,
            nproc=args.nproc,
            njobs=args.njobs,
            highres=args.highres,
            lowres=args.lowres,
            space_group=args.space_group,
            unit_cell=args.unit_cell,
            fast_mode=args.fast,
            model=args.model,
            sequence=args.sequence,
            wavelength=args.wavelength,
            beam_center=args.beam_center,
            detector_distance=args.detector_distance,
            native=args.native,
            friedel_pairs=not args.no_friedel_pairs,
            scaling_reference=args.scaling_reference,
            output_format=args.output_format,
            merge_data=not args.no_merge,
            generate_report=not args.no_report,
            sample_name=args.sample_name,
            username=args.username,
            beamline=args.beamline,
            esaf_id=args.esaf_id,
            pi_id=args.pi_id,
            primary_group=args.primary_group,
            pipeline_options=pipeline_options,
        )

        # Run pipeline
        driver = PipelineDriver()
        logger.info(
            f"Starting {args.pipeline_type} pipeline with {len(datasets)} dataset(s)"
        )

        result = driver.run_pipeline(args.pipeline_type, datasets, config)

        # Report results
        if result.success:
            logger.info("Pipeline completed successfully!")
            print(f"Results saved to: {result.work_dir}")
            print(f"Elapsed time: {result.elapsed_time:.1f} seconds")
            if result.pipeline_status_id:
                print(f"Database tracking ID: {result.pipeline_status_id}")
        else:
            logger.error("Pipeline failed!")
            print(f"Error: {result.error_message}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
