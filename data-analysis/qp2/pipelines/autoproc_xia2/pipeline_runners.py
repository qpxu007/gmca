# FILE: pipeline_runners.py
import json
import logging
import os
import re
import sys
from typing import Dict, Any, Optional, List
import h5py

from qp2.pipelines.utils.pipeline_tracker import PipelineTracker
from qp2.image_viewer.utils.run_job import run_command
from qp2.pipelines.autoproc_xia2.autoproc_xml_parser import AutoPROCXmlParser
from qp2.pipelines.autoproc_xia2.aimless_parser import AimlessParser
from qp2.pipelines.autoproc_xia2.xia2_parser import Xia2Parser
from qp2.config.servers import ServerConfig
from qp2.config.programs import ProgramConfig

logger = logging.getLogger(__name__)


from qp2.pipelines.utils.image_set import get_image_set_string


class BaseRunner:
    """
    Base class for running and tracking data processing pipelines like autoPROC and xia2.
    """

    def __init__(
        self,
        pipeline_name: str,
        datasets: Dict[str, Optional[List[int]]],
        work_dir: str,
        pipeline_params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.pipeline_name = pipeline_name
        self.datasets = datasets
        self.work_dir = os.path.abspath(work_dir)
        self.kwargs = kwargs

        os.makedirs(self.work_dir, exist_ok=True)

        # --- PipelineTracker Integration ---
        run_identifier = (
            list(self.datasets.keys())[0] if self.datasets else "unknown_dataset"
        )

        if pipeline_params is None:
            pipeline_params = {}

        # Enrich pipeline_params with runner-specific info
        pipeline_params.setdefault("command", " ".join(sys.argv))
        pipeline_params.setdefault("workdir", self.work_dir)
        pipeline_params.setdefault(
            "logfile", os.path.join(self.work_dir, f"{self.pipeline_name}.log")
        )
        
        # Standardize imageSet string using common utility
        # If it was already set by the caller (main.py), we don't overwrite it
        if "imageSet" not in pipeline_params:
            pipeline_params["imageSet"] = get_image_set_string(self.datasets)

        # Get default from central config
        redis_host = ServerConfig.get_redis_hosts().get("analysis_results", "127.0.0.1")

        self.tracker = PipelineTracker(
            pipeline_name=self.pipeline_name,
            run_identifier=run_identifier,
            initial_params=pipeline_params,
            redis_config={"host": redis_host, "db": 0},
            result_mapper=self._get_sql_mapped_results,
        )
        self.results: Dict[str, Any] = {}

    def _get_sql_mapped_results(self, results_dict: Dict[str, Any]) -> Dict[str, str]:
        """Maps the parser output dictionary to the database model fields."""
        mapped = {
            "sampleName": results_dict.get("sampleName") or results_dict.get("prefix"),
            "imageSet": results_dict.get("imageSet") or self.tracker.initial_params.get("imageSet"),
            "workdir": self.work_dir,
            "highresolution": results_dict.get("highresolution"),
            "spacegroup": results_dict.get("spacegroup"),
            "unitcell": results_dict.get("unitcell"),
            "rmerge": results_dict.get("rmerge"),
            "rmeas": results_dict.get("rmeas"),
            "rpim": results_dict.get("rpim"),
            "isigmai": results_dict.get("isigmai"),
            "multiplicity": results_dict.get("multiplicity"),
            "completeness": results_dict.get("completeness"),
            "anom_completeness": results_dict.get("anom_completeness"),
            "table1": results_dict.get("table1"),
            "cchalf": results_dict.get("cchalf"),
            "nobs": results_dict.get("Nobs"),
            "nuniq": results_dict.get("Nuniq"),
            "report_url": results_dict.get("report_url"),
            "truncate_mtz": results_dict.get("truncate_mtz"),
            "scale_log": results_dict.get("scale_log"),
            "run_stats": json.dumps(results_dict, default=str),
            "wavelength": results_dict.get("wavelength"),
        }
        return {k: str(v) for k, v in mapped.items() if v is not None}

    def _construct_command(self) -> str:
        raise NotImplementedError

    def _parse_results(self) -> Dict[str, Any]:
        raise NotImplementedError

    def run(self) -> Dict[str, Any]:
        self.tracker.start()
        try:
            command = self._construct_command()
            logger.info(f"Constructed command:\n{command}")
            self.tracker.update_progress("RUNNING", {"command": command})

            runner_type = self.kwargs.get("runner", "slurm")
            nproc = self.kwargs.get("nproc", 8)
            njobs = self.kwargs.get("njobs", 1)

            # --- Corrected SLURM resource allocation ---
            slurm_nodes = njobs
            slurm_processors = nproc
            if self.pipeline_name == "autoPROC":
                # autoPROC handles its own resource requests (submits jobs)
                slurm_nodes = 1
                slurm_processors = nproc
            elif self.pipeline_name.startswith("xia2"):
                # Xia2 runs on a single node but uses nproc*njobs internally for parallelism
                slurm_nodes = 1
                slurm_processors = njobs * nproc

            run_command(
                command,
                cwd=self.work_dir,
                method=runner_type,
                nodes=slurm_nodes,
                processors=slurm_processors,
                job_name=f"run_{self.pipeline_name}_{os.path.basename(self.work_dir)}",
            )

            logger.info("Command finished. Parsing results...")
            self.tracker.update_progress("PARSING", {})

            self.results = self._parse_results()
            if not self.results:
                raise RuntimeError("Failed to parse processing results.")

            # Save local JSON copy
            json_path = os.path.join(
                self.work_dir, f"{self.pipeline_name}_results.json"
            )
            with open(json_path, "w") as f:
                json.dump(self.results, f, indent=4, default=str)
            logger.info(f"Saved local results summary to {json_path}")
            self.results["json_summary"] = json_path

            self.tracker.succeed(self.results)
            logger.info(f"{self.pipeline_name} processing completed successfully.")

        except Exception as e:
            error_message = f"Processing failed: {e}"
            logger.error(error_message, exc_info=True)
            self.tracker.fail(error_message, self.results)

        return self.results


class AutoPROCRunner(BaseRunner):
    """
    A runner for the autoPROC pipeline. Handles both '-Id' and '-h5' input formats.
    """

    def __init__(self, datasets, work_dir, **kwargs):
        super().__init__("autoPROC", datasets, work_dir, **kwargs)

    def _construct_command(self) -> str:
        setup_cmd = [ProgramConfig.get_setup_command('autoproc')]

        process_cmd = ["process -d ."]

        for i, (master_file, data_range) in enumerate(self.datasets.items()):
            if data_range:
                start_frame, end_frame = data_range
                base_id = (
                    os.path.basename(master_file)
                    .replace("_master.h5", "")
                    .replace(".h5", "")
                )
                sweep_id = f"sweep{i + 1}_{base_id}"
                image_dir = os.path.dirname(master_file)
                template = master_file
                id_string = (
                    f'"{sweep_id},{image_dir},{template},{start_frame},{end_frame}"'
                )
                process_cmd.append(f"-Id {id_string}")
            else:
                process_cmd.append(f"-h5 {master_file}")

        if self.kwargs.get("highres"):
            process_cmd.append(f"-R 45.0 {self.kwargs['highres']}")
        else:
            process_cmd.append("-M HighResCutOnCChalf")

        if self.kwargs.get("space_group"):
            process_cmd.append(f'symm="{self.kwargs["space_group"]}"')
        if self.kwargs.get("unit_cell"):
            process_cmd.append(f'cell="{self.kwargs["unit_cell"]}"')

        # Anomalous data processing
        if self.kwargs.get("native", True):
            process_cmd.append("-noANO")
        else:
            # default to anomalous if native is False or not defined
            process_cmd.append("-ANO")

        njobs = self.kwargs.get("njobs", 1)
        if njobs > 1:
            process_cmd.append(f"autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_JOBS={njobs}")

        nproc = self.kwargs.get("nproc", 1)
        if nproc > 1:
            process_cmd.append(
                f"autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_PROCESSORS={nproc}"
            )
            process_cmd.append(f"-nthreads {nproc}")

        if self.kwargs.get("fast", False):
            process_cmd.append("-M fast")

        return "\n".join(setup_cmd) + "\n" + " \\\n  ".join(process_cmd)

    def _parse_results(self) -> Dict[str, Any]:
        # +++ MODIFICATION: Prioritize autoPROC.xml +++
        xml_path = os.path.join(self.work_dir, "autoPROC.xml")
        staraniso_xml_path = os.path.join(self.work_dir, "autoPROC_staraniso.xml")
        results = {}

        # Prefer the staraniso XML if it exists, as it's often more complete
        if os.path.exists(staraniso_xml_path):
            xml_to_parse = staraniso_xml_path
        elif os.path.exists(xml_path):
            xml_to_parse = xml_path
        else:
            xml_to_parse = None

        if xml_to_parse:
            logging.info(f"Parsing results from XML: {xml_to_parse}")
            try:
                parser = AutoPROCXmlParser(
                    wdir=self.work_dir, filename=os.path.basename(xml_to_parse)
                )
                results = parser.summarize()
            except Exception as e:
                logging.error(f"Failed to parse {xml_to_parse}: {e}", exc_info=True)
                # Fallback to aimless.log if XML parsing fails
                results = self._parse_from_aimless_log()
        else:
            # Fallback to aimless.log if no XML is found
            results = self._parse_from_aimless_log()

        if not results:
            logging.error(
                "Failed to parse any valid results files (XML or aimless.log)."
            )
            return {}

        results["sampleName"] = self.kwargs.get("sampleName")
        return results

    def _parse_from_aimless_log(self) -> Dict[str, Any]:
        """Fallback parser for aimless.log if XML is missing or corrupt."""
        aimless_log_path = None
        for root, _, files in os.walk(self.work_dir):
            if "aimless.log" in files:
                aimless_log_path = os.path.join(root, "aimless.log")
                break

        if aimless_log_path:
            logging.warning(
                f"Falling back to parsing aimless.log at: {aimless_log_path}"
            )
            parser = AimlessParser(wdir=os.path.dirname(aimless_log_path))
            results = parser.summarize()
            results["report_url"] = os.path.join(self.work_dir, "summary.html")
            results["scale_log"] = aimless_log_path

            # Find the MTZ file separately
            for root, _, files in os.walk(self.work_dir):
                for f in files:
                    if f.endswith(".mtz") and (
                        "truncate-unique" in f or "staraniso" in f
                    ):
                        results["truncate_mtz"] = os.path.join(root, f)
                        break
                if "truncate_mtz" in results:
                    break
            return results

        return {}


class Xia2Runner(BaseRunner):
    """A runner for the xia2 pipeline."""

    def __init__(self, datasets: Dict[str, Any], work_dir: str, **kwargs):
        pipeline = kwargs.get("pipeline", "xia2_dials")
        super().__init__(pipeline, datasets, work_dir, **kwargs)
        self.project = self.pipeline_name
        self.crystal = self._get_crystal_name()

    def _get_crystal_name(self) -> str:
        """Determines a suitable crystal name for xia2."""
        sample_name = self.kwargs.get("sampleName")
        if not sample_name:
            first_master = list(self.datasets.keys())[0]
            base = os.path.basename(first_master)
            sample_name = os.path.splitext(base)[0].split("_", 1)[0]

        crystal_name = sample_name
        if crystal_name[0].isdigit():
            crystal_name = f"p_{crystal_name}"
        crystal_name = crystal_name.replace("-", "_").replace(".", "_")
        return crystal_name

    def _construct_command(self) -> str:
        nproc = self.kwargs.get("nproc", 1)
        njobs = min(len(self.datasets), 4)

        dials_setup = self.kwargs.get(
            "dials_setup", ProgramConfig.get_setup_command('dials')
        )
        fast_mode = "dials.fast_mode=True" if self.kwargs.get("fast") else ""
        trust_beam = self.kwargs.get("trust_beam_centre", True)
        
        # Preamble: SBATCH directives and environment setup
        preamble = [
            f"#SBATCH --ntasks-per-node={nproc*njobs}",
            "#SBATCH --nodes=1",
            "unset HDF5_PLUGIN_PATH",
            dials_setup,
            ""
        ]
        
        # Main Command: xia2 execution
        cmd = [
            f"xia2 failover=True read_all_image_headers=False trust_beam_centre={str(trust_beam).lower()} {fast_mode}",
        ]

        for master_file, data_range in self.datasets.items():
            if data_range and len(data_range) == 2:
                cmd.append(f"image={master_file}:{data_range[0]}:{data_range[1]}")
            else:
                cmd.append(f"image={master_file}")

        if self.kwargs.get("highres"):
            cmd.append(f'xia2.settings.resolution.d_min={self.kwargs["highres"]}')

        if self.kwargs.get("lowres"):
            cmd.append(f'xia2.settings.resolution.d_max={self.kwargs["lowres"]}')

        if self.kwargs.get("space_group"):
            cmd.append(f"xia2.settings.space_group='{self.kwargs['space_group']}'")
            if self.kwargs.get("unit_cell"):
                cmd.append(f'xia2.settings.unit_cell="{self.kwargs["unit_cell"]}"')

        # Anomalous data processing
        if not self.kwargs.get("native", True):
            cmd.append("xia2.settings.input.anomalous=True")
        else:
            cmd.append("xia2.settings.input.anomalous=False")

        cmd.append("multiprocessing.mode=parallel")
        cmd.append(f"multiprocessing.njob={njobs}")
        cmd.append(f"multiprocessing.nproc={nproc}")

        pipeline_type = self.kwargs.get("pipeline", "xia2_dials")
        if pipeline_type == "xia2_dials_aimless":
            cmd.append("pipeline=dials-aimless")
        elif pipeline_type == "xia2_dials":
            cmd.append("pipeline=dials")
        elif pipeline_type == "xia2_xds":
            cmd.append("pipeline=3d")

        cmd.append(f"project={self.project}")
        cmd.append(f"crystal={self.crystal}")
        
        return "\n".join(preamble) + "\n" + " \\\n  ".join(cmd)

    def _parse_results(self) -> Dict[str, Any]:
        """Parses xia2 output, trying xia2.txt first then falling back to aimless.log."""
        xia2_txt_path = os.path.join(self.work_dir, "xia2.txt")
        results: Dict[str, Any] = {}

        if os.path.exists(xia2_txt_path):
            try:
                parser = Xia2Parser(wdir=self.work_dir, filename="xia2.txt")
                results = parser.summarize() or {}
            except Exception as e:
                logger.warning(
                    f"Could not parse xia2.txt: {e}. Will check for aimless.log."
                )

        if not results or self.pipeline_name == "xia2_dials_aimless":
            aimless_log_path = None
            for root, _, files in os.walk(self.work_dir):
                if "aimless.log" in files:
                    aimless_log_path = os.path.join(root, "aimless.log")
                    break
            if aimless_log_path:
                logger.info(f"Parsing results from aimless.log: {aimless_log_path}")
                parser = AimlessParser(
                    wdir=os.path.dirname(aimless_log_path), filename="aimless.log"
                )
                aimless_results = parser.summarize()
                results.update(aimless_results)

        if not results:
            logger.error(
                "Failed to find and parse any valid results files (xia2.txt or aimless.log)."
            )
            return {}

        results.setdefault("report_url", os.path.join(self.work_dir, "xia2.html"))
        results.setdefault("logfile", xia2_txt_path)
        if "truncate_mtz" not in results or not os.path.exists(
            results.get("truncate_mtz", "")
        ):
            mtz_path = os.path.join(
                self.work_dir, "DataFiles", f"{self.project}_{self.crystal}_free.mtz"
            )
            if os.path.exists(mtz_path):
                results["truncate_mtz"] = mtz_path

        return results


class Xia2SSXRunner(BaseRunner):
    """A runner for the xia2.ssx pipeline."""

    def __init__(self, datasets: Dict[str, Any], work_dir: str, **kwargs):
        super().__init__("xia2_ssx", datasets, work_dir, **kwargs)

    def _construct_command(self) -> str:
        setup_cmd = [ProgramConfig.get_setup_command('dials')]

        cmd = ["xia2.ssx"]

        import_phil_lines = []
        warned_about_oscillation = False

        # Add inputs
        for master_file in self.datasets.keys():
            if os.path.isdir(master_file):
                cmd.append(f"directory={master_file}")
            else:
                cmd.append(f"image={master_file}")

                # Check for oscillation if it's an HDF5 file and we haven't already handled it
                if not warned_about_oscillation and (master_file.endswith(".h5") or master_file.endswith(".nxs")) and os.path.isfile(master_file):
                    try:
                        with h5py.File(master_file, "r") as f:
                            # Check typical paths for oscillation/omega range
                            osc_range = None
                            if "/entry/sample/goniometer/omega_range_average" in f:
                                osc_range = f["/entry/sample/goniometer/omega_range_average"][()]

                            if osc_range is not None:
                                # Handle scalar/0-d array
                                if hasattr(osc_range, "item"):
                                    osc_range = osc_range.item()

                                # Check if oscillation is significantly non-zero
                                if abs(osc_range) > 1e-6:
                                    logger.warning(
                                        f"Detected non-zero oscillation ({osc_range}) in {master_file} for xia2.ssx pipeline. "
                                        "Applying workaround: forcing geometry.scan.oscillation=0.0,0.0."
                                    )
                                    warned_about_oscillation = True
                                    import_phil_lines.append("geometry.scan.oscillation=0.0,0.0")

                    except Exception as e:
                        logger.warning(f"Failed to check oscillation in {master_file}: {e}")

        # Beam centre / distance override
        beam_x = self.kwargs.get("beam_x")
        beam_y = self.kwargs.get("beam_y")
        distance = self.kwargs.get("distance")
        if beam_x and beam_y:
            import_phil_lines += [
                "geometry {",
                "  detector {",
                f'    fast_slow_beam_centre = "{beam_x},{beam_y}"',
            ]
            if distance:
                import_phil_lines.append(f"    distance = {distance}")
            import_phil_lines += ["  }", "}"]
            logger.info(f"Applying detector geometry override: beam_centre={beam_x},{beam_y}, distance={distance}")

        # Write import.phil once and append to cmd
        if import_phil_lines:
            import_phil_path = os.path.join(self.work_dir, "import.phil")
            try:
                with open(import_phil_path, "w") as phil_f:
                    phil_f.write("\n".join(import_phil_lines) + "\n")
                cmd.append(f"dials_import.phil={import_phil_path}")
            except Exception as e:
                logger.error(f"Failed to write {import_phil_path}: {e}")

        # Processing parameters
        if self.kwargs.get("space_group"):
            cmd.append(f"space_group={self.kwargs['space_group']}")

        if self.kwargs.get("unit_cell"):
            cell = self.kwargs['unit_cell']  
            cmd.append(f"unit_cell='{cell}'")

        if self.kwargs.get("model"):
            cmd.append(f"reference={self.kwargs['model']}")

        if self.kwargs.get("reference_hkl"):
            cmd.append(f"reference={self.kwargs['reference_hkl']}")

        if self.kwargs.get("highres"):
            cmd.append(f"d_min={self.kwargs['highres']}")

        if self.kwargs.get("lowres"):
            cmd.append(f"d_max={self.kwargs['lowres']}")

        # Anomalous data processing
        if not self.kwargs.get("native", True):
            cmd.append("anomalous=True")
        else:
            cmd.append("anomalous=False")

        if self.kwargs.get("steps"):
            cmd.append(f"steps={self.kwargs['steps']}")

        if self.kwargs.get("max_lattices"):
             cmd.append(f"indexing.max_lattices={self.kwargs['max_lattices']}")
        
        if self.kwargs.get("min_spots"):
             cmd.append(f"indexing.min_spots={self.kwargs['min_spots']}")

        # Parallelization
        nproc = self.kwargs.get("nproc", 1)
        njobs = self.kwargs.get("njobs", 1)
        cmd.append(f"nproc={nproc}")
        cmd.append(f"njobs={njobs}")

        return "\n".join(setup_cmd) + "\n" + " \\\n  ".join(cmd)

    def _parse_results(self) -> Dict[str, Any]:
        """Parses xia2.ssx output. Skipping for now as requested."""
        logger.info("Skipping Xia2SSX results parsing as requested.")
        return {"parse_skipped": True}


class DimpleRunner:
    """A simple, non-tracking runner for the Dimple pipeline."""

    def __init__(self, mtz_file: str, pdb_file: str, work_dir: str, **kwargs):
        self.mtz_file = mtz_file
        self.pdb_file = pdb_file
        self.work_dir = os.path.abspath(work_dir)
        self.kwargs = kwargs
        self.dimple_dir = os.path.join(self.work_dir, "dimple_run")
        self.results: Dict[str, Any] = {}

    def _construct_command(self) -> str:
        """Constructs the command to run Dimple."""
        # Using newline is safer for scripts than &&
        cmd = [
            "dimple",
            self.mtz_file,
            self.pdb_file,
            self.dimple_dir,
        ]
        return " ".join(cmd)

    def _parse_results(self) -> Dict[str, Any]:
        """Parses the output of a Dimple run."""
        results = {}
        final_pdb = os.path.join(self.dimple_dir, "final.pdb")
        final_mtz = os.path.join(self.dimple_dir, "final.mtz")

        if os.path.exists(final_pdb):
            results["dimple_pdb"] = final_pdb
            results["dimple_mtz"] = final_mtz
            logger.info("Dimple run successful. Found final.pdb and final.mtz.")
            dimple_log = os.path.join(self.dimple_dir, "dimple.log")
            if os.path.exists(dimple_log):
                with open(dimple_log, "r") as f:
                    content = f.read()
                    rfree_match = re.search(r"Final R-free is ([\d.]+)", content)
                    if rfree_match:
                        results["dimple_r_free"] = float(rfree_match.group(1))
                        logger.info(f"Parsed final R-free: {results['dimple_r_free']}")
        else:
            logger.error("Dimple run failed: final.pdb not found.")
        return results

    def run(self) -> Dict[str, Any]:
        """Executes the Dimple command and parses the results."""
        try:
            command = self._construct_command()
            logger.info(f"Constructed Dimple command:\n{command}")
            runner_type = self.kwargs.get("runner", "slurm")
            nproc = self.kwargs.get("nproc", 8)

            ccp4_setup = self.kwargs.get(
                "ccp4_setup", ProgramConfig.get_setup_command('ccp4')
            )

            run_command(
                command,
                pre_command=ccp4_setup,  # Use the dedicated pre_command argument
                cwd=self.work_dir,
                method=runner_type,
                processors=1,
                nodes=1,
                job_name=f"dimple_{os.path.basename(self.work_dir)}",
            )

            logger.info("Dimple command finished. Parsing results...")
            self.results = self._parse_results()
            return self.results
        except Exception as e:
            logger.error(f"Dimple execution failed: {e}", exc_info=True)
            return {"error": str(e)}
