import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from qp2.data_proc.server.script import Script, JobConfig
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class ProcessingConfig:
    """Configuration for processing jobs"""

    quickprocess_dir: str = field(default_factory=lambda: _get_quickprocess_dir())
    python_path: str = (
        "/mnt/software/px/miniconda3/envs/data-analysis/bin"  # Example path
    )
    bluice_path: str = "/mnt/software/bluice/"  # Example path

    default_nproc: int = 32
    default_njobs: int = 1  # As seen in original xprocess for program 'process'


def _get_quickprocess_dir() -> str:
    """Get the quickprocess directory path (example implementation)"""
    # This needs to correctly locate 'data-analysis' relative to this file.
    # The original xprocess.py has:
    # curr_dir = os.path.abspath(__file__)
    # QUICKPROCESS_DIR = os.path.join(curr_dir[:curr_dir.find('data-analysis')], "data-analysis")
    # Adjust as necessary for your project structure.
    # For robustness, this might come from an environment variable or a more reliable relative path.
    try:
        # More robust for module context
        curr_dir = os.path.abspath(os.path.dirname(__file__))
        # Assuming this script is in image_viewer/data_proc/server/
        # And data-analysis is a sibling to image_viewer/
        base_dir = Path(curr_dir).parent.parent
        quickprocess_path = base_dir / "data-analysis"
        if not quickprocess_path.exists():  # Fallback if structure is different
            # Original logic
            return os.path.join(
                curr_dir[: curr_dir.find("data-analysis")], "data-analysis"
            )
        return str(quickprocess_path)

    except Exception:  # Fallback if find fails or pathing is unexpected
        return "/mnt/beegfs/qxu/data-analysis"  # Provide a default or raise error


class ProcessingJobBuilder:
    """Builder for processing job commands"""

    PIPELINE_CONFIGS = {
        # True if it always implies optimize or specific condition
        "gmcaproc": {},
        "autoproc": {},
        "xia2_dials": {},
        "mosflm_strategy": {"program": "strategy"},
        "xds_strategy": {"program": "strategy"},
        "dials_strategy": {"program": "strategy"},
        "labelit_strategy": {"program": "strategy"},
    }

    def __init__(self, config: Optional[ProcessingConfig] = None):
        self.config = config or ProcessingConfig()

    def build_job(self, opt: Dict) -> Script:
        """Build a processing job from options"""
        self._validate_options(opt)

        workdir = self._get_workdir(opt)
        # Ensure workdir is absolute for Script class if it expects that
        if workdir:
            workdir = os.path.abspath(workdir)

        cmd_script_text = self._build_command_script_text(opt, workdir)
        script_name = self._get_script_name(opt)

        username = opt.get("username")
        # groupname = opt.get('groupname') # script.py's Script class can derive group if not provided

        config = JobConfig(
            script_name=script_name,
            wdir=workdir,
            # Allow override from opt
            nproc=opt.get("nproc", self.config.default_nproc),
            run_as_user=username,
            # run_as_group=groupname, # Pass if Script class uses it directly
            script_text=cmd_script_text,
            # Allow runner to be specified in opt
            runner=opt.get("runner", "slurm"),
        )
        return Script(config=config)

    def _validate_options(self, opt: Dict) -> None:
        """Validate processing options"""
        required_fields = ["proc_dir"]  # Or workdir, handled by _get_workdir
        if not (opt.get("proc_dir") or opt.get("workdir")):
            raise ValueError(f"Missing required option: proc_dir or workdir")

        pipeline = opt.get("pipeline")
        if pipeline and pipeline not in self.PIPELINE_CONFIGS:
            # Allow pipelines not in PIPELINE_CONFIGS but log a warning
            logger.warning(f"Pipeline '{pipeline}' not in known PIPELINE_CONFIGS.")

    def _get_workdir(self, opt: Dict) -> str:
        """Get working directory from options"""
        workdir = opt.get("workdir") or opt.get("proc_dir")
        if not workdir:
            raise ValueError(
                "Working directory (workdir or proc_dir) must be specified."
            )
        return workdir

    def _get_script_name(self, opt: Dict) -> str:
        """Generate script name from options"""
        job_tag = opt.get("job_tag", "auto_process")  # Default job_tag
        pipeline_name = opt.get("pipeline", "default_pipeline")
        return f"{job_tag}_{pipeline_name}.sh"

    def _get_environment_setup(self, workdir: str) -> str:
        """Get environment setup commands"""
        # Use paths from self.config
        return f"""
cd {workdir}
export MYPYTHON={self.config.python_path}
export BLUICE={self.config.bluice_path}
export PATH=$MYPYTHON:$PATH
export PYTHONPATH={self.config.quickprocess_dir}:$PYTHONPATH
export EPICS_CA_AUTO_ADDR_LIST=YES # Uncomment if needed
"""

    def _build_command_script_text(self, opt: Dict, workdir: str) -> str:
        """Build the full script text including environment setup and the command."""
        program = opt.get("program", "process")

        # Base command setup
        cmd_parts = [f"python -m quickProcess.{program} --log2db"]

        # Add arguments
        self._add_directory_args(cmd_parts, opt, workdir)
        self._add_pipeline_args(cmd_parts, opt)
        self._add_processing_args(cmd_parts, opt)
        self._add_user_and_meta_args(cmd_parts, opt)
        self._add_job_control_args(cmd_parts, opt, program)

        env_setup = self._get_environment_setup(workdir)
        return env_setup + "\n" + " ".join(cmd_parts)

    def _add_directory_args(
        self, cmd_parts: List[str], opt: Dict, workdir: str
    ) -> None:
        """Add directory-related arguments"""
        # workdir is already determined and used for 'cd' and --proc_dir
        cmd_parts.append(f"--proc_dir={workdir}")

        imagedir = opt.get("imagedir") or opt.get("data_dir")
        if imagedir:
            # Ensure absolute path
            cmd_parts.append(f"--data_dir={os.path.abspath(imagedir)}")

    def _add_pipeline_args(self, cmd_parts: List[str], opt: Dict) -> None:
        """Add pipeline-specific arguments"""
        pipeline = opt.get("pipeline")
        if not pipeline:
            return

        pipeline_config = self.PIPELINE_CONFIGS.get(pipeline, {})
        percent_str = opt.get("percent")  # Keep as string for comparison

        if pipeline == "gmcaproc" and percent_str not in ["25%", "50%"]:
            cmd_parts.append(f"--pipeline={pipeline} --gmca_optimize")
        else:
            cmd_parts.append(f"--pipeline={pipeline}")

    def _add_processing_args(self, cmd_parts: List[str], opt: Dict) -> None:
        """Add various processing-related arguments"""
        if opt.get("beamline"):
            cmd_parts.append(f"--beamline={opt['beamline']}")
        if opt.get("start"):  # Allow 0
            cmd_parts.append(f"--start={opt['start']}")
        if opt.get("end"):  # Allow 0
            cmd_parts.append(f"--end={opt['end']}")

        images_str = opt.get("images")
        if images_str:
            for f_image in re.split(r"[;, ]\s*", images_str):
                if f_image:
                    cmd_parts.append(f"--image={f_image}")

        filelist = opt.get("filelist")
        if filelist:
            # Assuming filelist is a path string
            cmd_parts.append(f"--filelist={filelist}")

        sample_name = (
            opt.get("sample_id") or opt.get("samplename") or opt.get("sampleName")
        )
        if sample_name:
            cmd_parts.append(f"--sampleName={sample_name}")

        if opt.get("prefix"):
            cmd_parts.append(f"--prefix={opt['prefix']}")
        elif opt.get("run_prefix"):
            cmd_parts.append(f"--prefix={opt['run_prefix']}")
        if opt.get("percent"):
            cmd_parts.append(f"--percent={opt['percent']}")

        if opt.get("nativedata") or opt.get("native", False):
            cmd_parts.append("--native")
        if opt.get("highres"):
            cmd_parts.append(f"--highres={opt['highres']}")
        if opt.get("symm"):  # Original uses "symm" for "space_group"
            cmd_parts.append(f"--space_group={opt['symm']}")
        if opt.get("model"):
            cmd_parts.append(f"--model={opt['model']}")
        if opt.get("model_type"):
            cmd_parts.append(f"--model_type={opt['model_type']}")
        if opt.get("nmol"):
            cmd_parts.append(f"--nmol={opt['nmol']}")
        if opt.get("sequence"):
            cmd_parts.append(f"--sequence={opt['sequence']}")
        if opt.get("unitcell"):
            # Quotes as in original
            cmd_parts.append(f'--unit_cell="{opt["unitcell"]}"')
        if opt.get("referencedata"):  # Original uses "referencedata" for "xds_refhkl"
            cmd_parts.append(f"--xds_refhkl={opt['referencedata']}")

        mounted_crystal = opt.get("robot_mounted", None) or opt.get("mounted", None)
        if mounted_crystal:  # Check explicitly for None if boolean False is valid
            cmd_parts.append(f"--mountedCrystal={mounted_crystal}")

        xls_file = opt.get("spreadsheet")
        if xls_file:
            # Ensure xls_file path is absolute if it's a relative path from somewhere else
            abs_xls_file = os.path.abspath(xls_file)
            if os.path.exists(abs_xls_file) and os.path.isfile(abs_xls_file):
                # Quotes and abs path
                cmd_parts.append(f'--xlsFile="{abs_xls_file}"')
            else:
                logger.warning(
                    f"Spreadsheet file not found or not a file: {abs_xls_file}"
                )

    def _add_user_and_meta_args(self, cmd_parts: List[str], opt: Dict) -> None:
        """Add user, esaf, group, pi, and redis key arguments"""
        if opt.get("username"):
            cmd_parts.append(f"--username={opt['username']}")
        if opt.get("esaf_id"):
            cmd_parts.append(f"--esaf_id={opt['esaf_id']}")
        # The reprocessing dialog sends 'primary_group', but original xprocess used 'groupname'. Let's handle both.
        group = opt.get("primary_group") or opt.get("groupname")
        if group:
            cmd_parts.append(f"--primary_group={group}")
        if opt.get("pi_id"):
            cmd_parts.append(f"--pi_id={opt['pi_id']}")
        if opt.get("redis_key"):
            cmd_parts.append(f"--redis_key={opt['redis_key']}")

    def _add_job_control_args(
        self, cmd_parts: List[str], opt: Dict, program: str
    ) -> None:
        """Add njobs and nproc arguments"""
        if program == "process":
            njobs = opt.get("njobs", self.config.default_njobs)
            nproc = opt.get("nproc", self.config.default_nproc)
            cmd_parts.append(f"--njobs={njobs}")
            cmd_parts.append(f"--nproc={nproc}")


# Default job_tag from original was "reprocess"
def xprocess(opt: Dict, job_tag: str = "auto_process") -> int:
    """
    Create and submit a processing job using the ProcessingJobBuilder.
    'opt' dictionary contains all necessary parameters.
    'job_tag' is a prefix for the script name.
    """
    try:
        # Update opt with the job_tag if it's not already set or to override
        opt["job_tag"] = opt.get("job_tag", job_tag)

        # DEBUG logging of input parameters
        logger.debug(f"xprocess: Submitting job '{opt['job_tag']}' with options: {opt}")

        builder = ProcessingJobBuilder()  # Uses default ProcessingConfig
        job_script_obj = builder.build_job(opt)

        # DEBUG logging of constructed script
        logger.debug(f"xprocess: Constructed script text for '{opt['job_tag']}':\n{job_script_obj.config.script_text}")

        if opt.get("dry_run"):
            logger.info(f"DRY RUN: Job '{opt.get('job_tag')}' would be submitted.")
            logger.info(f"DRY RUN: Script Name: {job_script_obj.config.script_name}")
            logger.info(f"DRY RUN: Working Directory: {job_script_obj.config.wdir}")
            logger.info(f"DRY RUN: Script Content:\n{job_script_obj.config.script_text}")
            return 0

        # The run_async method is from the Script class
        # Ensure it matches the refactored Script class's signature
        # (e.g., if it became truly async and needs await or specific callback handling)
        # For now, assuming it's the run_async from original script.py [2] which is blocking in a thread.
        # Or run_async_real if using the fully async version
        return_code = job_script_obj.run_async()

        # If using the fully async version that returns a Task or similar:
        # task = await job_script_obj.run_async_real()
        # return task # Or handle appropriately

        return return_code  # Original run_async returns 0 after starting thread

    except ValueError as ve:  # Catch validation errors from builder
        logger.error(
            f"Configuration error for processing job '{opt.get('job_tag')}': {ve}"
        )
        raise  # Re-raise to indicate failure
    except Exception as e:
        logger.error(
            f"Failed to create or submit processing job '{opt.get('job_tag')}': {e}",
            exc_info=True
        )
        # import traceback
        # logger.error(traceback.format_exc()) # For more detailed debugging
        raise


# Example Usage (similar to original xprocess.py's __main__ or data_processing_server.py's submit_job)
if __name__ == "__main__":
    # Example opt dictionary, mirroring what data_processing_server might provide
    # or what was used in original xprocess.py tests.
    sample_opt_completion_strategy = {
        "program": "strategy",  # From data_processing_server.py, process_completion
        "pipeline": "dials_strategy",
        "proc_dir": "./test_processing_output/strategy_run",  # Unique processing dir
        "data_dir": "./test_cbf_data",  # cbf_data_dir from convert_hdf5_to_cbf_for_strategy
        "filelist": "./test_cbf_data/filelist.txt",  # cbf_file_list
        "username": "testuser",
        "beamline": "23id",
        "sample_id": "my_sample_strategy",
        "job_tag": "strategy_complete",
        # Add other relevant keys from get_opt in data_processing_server.py
    }

    sample_opt_completion_standard = {
        "program": "process",  # Default for standard
        "pipeline": "gmcaproc",
        "proc_dir": "./test_processing_output/standard_run_gmca",
        "data_dir": "/path/to/raw/h5_data",  # Original data_dir
        # "filelist": None, # Explicitly None or omitted for standard pipelines
        "username": "testuser",
        "beamline": "23id",
        "prefix": "dataset_prefix",  # Usually from metadata
        "sample_id": "my_sample_standard",
        "job_tag": "standard_complete_gmca",
        "nproc": 16,  # Example override
        # Add other relevant keys
    }

    sample_opt_milestone = {
        "program": "process",
        "pipeline": "gmcaproc",  # For milestones like 25%, 50%
        "percent": "25",  # Milestone percentage
        "proc_dir": "./test_processing_output/milestone_run",
        "data_dir": "/path/to/raw/h5_data",
        "username": "testuser",
        "prefix": "dataset_prefix_milestone",
        "sample_id": "my_sample_milestone",
        "job_tag": "milestone_25pct",
    }

    try:
        # Ensure directories for proc_dir exist for Script to write into, or Script handles creation
        os.makedirs("./test_processing_output/strategy_run", exist_ok=True)
        # Create a dummy filelist if needed for testing
        # with open("./test_cbf_data/filelist.txt", "w") as f: f.write("dummy_image.cbf\n")

        logger.info(
            f"Submitting strategy job with opts: {sample_opt_completion_strategy}"
        )
        xprocess(sample_opt_completion_strategy, job_tag="cli_test_strat")

        os.makedirs("./test_processing_output/standard_run_gmca", exist_ok=True)
        logger.info(
            f"Submitting standard processing job with opts: {sample_opt_completion_standard}"
        )
        xprocess(sample_opt_completion_standard, job_tag="cli_test_std")

        os.makedirs("./test_processing_output/milestone_run", exist_ok=True)
        logger.info(f"Submitting milestone job with opts: {sample_opt_milestone}")
        xprocess(sample_opt_milestone, job_tag="cli_test_milestone")

    except Exception as e:
        logger.error(f"Error in example usage: {e}")
        # import traceback
        # logger.error(traceback.format_exc())
