# qp2/image_viewer/actions/dataset_context_manager.py

import os
import shlex
import shutil
import time
import json
import subprocess
import sys
import importlib
from datetime import datetime
from typing import Set, List, Dict
from functools import partial

from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QMenu, QInputDialog, QMessageBox, QApplication, QFileDialog

from qp2.image_viewer.plugins.nxds.nxds_analysis_manager import NXDSAnalysisManager
from qp2.image_viewer.ui.busy_cursor import BusyCursor
from qp2.log.logging_config import get_logger
from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
from qp2.config.programs import ProgramConfig

# --- Imports for Pipeline Plugins ---
from qp2.image_viewer.plugins.xds.xds_settings_dialog import XDSSettingsDialog
from qp2.image_viewer.plugins.xds.submit_xds_job import XDSProcessDatasetWorker
from qp2.image_viewer.plugins.xia2.xia2_settings_dialog import Xia2SettingsDialog
from qp2.image_viewer.plugins.xia2.submit_xia2_job import Xia2ProcessDatasetWorker
from qp2.image_viewer.plugins.xia2_ssx.xia2_ssx_settings_dialog import (
    Xia2SSXSettingsDialog,
)
from qp2.image_viewer.plugins.xia2_ssx.submit_xia2_ssx_job import (
    Xia2SSXProcessDatasetWorker,
    Xia2SSXDistributedWorker,
)
from qp2.image_viewer.plugins.autoproc.autoproc_settings_dialog import (
    AutoPROCSettingsDialog,
)
from qp2.image_viewer.plugins.autoproc.submit_autoproc_job import (
    AutoPROCProcessDatasetWorker,
)
from qp2.image_viewer.ui.job_status_dialog import JobStatusDialog
from qp2.image_viewer.ui.combine_datasets_dialog import CombineDatasetsDialog
from qp2.image_viewer.plugins.crystfel.crystfel_settings_dialog import CrystfelSettingsDialog
from qp2.image_viewer.plugins.crystfel.submit_crystfel_job import CrystfelProcessDatasetWorker
from qp2.image_viewer.plugins.nxds.nxds_settings_dialog import NXDSSettingsDialog

from qp2.image_viewer.plugins.dozor.dozor_settings_dialog import DozorSettingsDialog
from qp2.image_viewer.plugins.dials_ssx.dials_settings_dialog import DialsSettingsDialog

logger = get_logger(__name__)


class DatasetContextMenuManager:
    """Handles the logic and actions for the dataset tree's context menu."""

    def __init__(self, main_window):
        self.main_window = main_window
        self.tree_widget = main_window.ui_manager.dataset_tree_widget
        self.nxds_analysis_manager = NXDSAnalysisManager(main_window)
        self._temp_manager = None # Keep reference to dynamically created managers
        self._job_status_dialog = None # Reference to non-blocking dialog

    def show_context_menu(self, position):
        """Analyzes selection and builds and shows the context menu."""
        selected_items = self.tree_widget.selectedItems()
        selection_info = self._analyze_selection(selected_items)

        menu = QMenu(self.tree_widget)

        # --- 1. Top Level: Visualization & Status (Context Sensitive) ---
        # These are kept at the top for quick access when applicable
        self._add_visualization_actions(menu, selection_info)
        self._add_job_status_action(menu, selection_info)

        if not menu.isEmpty():
            menu.addSeparator()

        # --- 2. Processing Submenu ---
        # Renamed to "Standard Processing" for autoPROC, xia2, XDS, Strategies
        std_proc_menu = menu.addMenu("Standard Processing")
        self._add_standard_pipeline_actions(std_proc_menu, selection_info)
        self._add_strategy_actions(std_proc_menu, selection_info)
        self._add_processing_actions(std_proc_menu, selection_info) # Keep generic here or move?

        # --- 3. Serial Processing Submenu ---
        # Groups xia2.ssx, nXDS, Combiner, CrystFEL, DIALS
        # --- 3. Serial Processing Submenu ---
        # Groups xia2.ssx, nXDS, Combiner, CrystFEL, DIALS
        serial_menu = menu.addMenu("Serial Processing")
        self._add_serial_pipeline_actions(serial_menu, selection_info)
        self._add_crystfel_batch_actions(serial_menu, selection_info)
        self._add_group_rerun_actions(serial_menu, selection_info)
        
        serial_menu.addSeparator()
        self._add_nxds_analysis_actions(serial_menu, selection_info)
        self._add_crystfel_analysis_actions(serial_menu, selection_info)
        self._add_dials_analysis_actions(serial_menu, selection_info)
        self._add_combination_actions(serial_menu, selection_info)

        menu.addSeparator()

        # --- 5. Utilities Submenu ---
        utils_menu = menu.addMenu("Utilities")
        self._add_file_system_actions(utils_menu, selection_info)
        self._add_management_actions(utils_menu, selection_info)

        menu.exec_(self.tree_widget.mapToGlobal(position))

    def _analyze_selection(self, selected_items: list) -> dict:
        """Categorizes selected items and aggregates all implied dataset paths."""
        info = {
            "samples": set(),
            "runs": set(),
            "datasets": set(),
            "run_prefixes": set(),
            "dataset_paths": set(),
        }
        dm = self.main_window.dataset_manager

        if not selected_items:
            # If nothing is selected, treat it as a request to operate on all loaded datasets.
            info["dataset_paths"].update(list(dm.reader_cache.keys()))
            return info

        for item in selected_items:
            parent = item.parent()
            if parent is None:  # Is a Sample
                info["samples"].add(item.text(0))
            elif parent.parent() is None:  # Is a Run
                info["runs"].add(item.data(0, Qt.ItemDataRole.UserRole))
            else:  # Is a Dataset
                info["datasets"].add(item.data(0, Qt.ItemDataRole.UserRole))

        info["dataset_paths"].update(info["datasets"])
        for run_prefix in info["runs"]:
            info["run_prefixes"].add(run_prefix)
            info["dataset_paths"].update(dm.get_dataset_paths_for_run(run_prefix))
        for sample_prefix in info["samples"]:
            # CORRECTED: Iterate through the 'runs' dictionary of the sample
            for run_prefix in dm.samples.get(sample_prefix, {}).get("runs", {}):
                info["run_prefixes"].add(run_prefix)
                info["dataset_paths"].update(dm.get_dataset_paths_for_run(run_prefix))

        return info

    def _add_job_status_action(self, menu: QMenu, info: dict):
        if info["dataset_paths"]:
            action = menu.addAction("Display Processing Job Status...")
            action.triggered.connect(lambda: self._show_job_status_dialog(info["dataset_paths"]))

    def _show_job_status_dialog(self, dataset_paths):
        redis_conn = self.main_window.redis_output_server
        if not redis_conn:
            self.main_window.ui_manager.show_warning_message(
                "Connection Error", "Redis Analysis connection is not available."
            )
            return

        active_plugin = "nxds"
        if hasattr(self.main_window, "ui_manager") and hasattr(self.main_window.ui_manager, "analysis_selector_combo"):
            active_plugin = self.main_window.ui_manager.analysis_selector_combo.currentText()

        if self._job_status_dialog:
            self._job_status_dialog.close()

        self._job_status_dialog = JobStatusDialog(dataset_paths, redis_conn, self.main_window, active_plugin=active_plugin)
        # Connect the resubmit signal to the existing group rerun logic
        self._job_status_dialog.resubmit_plugin_jobs.connect(self._trigger_group_rerun_no_prompt)
        
        self._job_status_dialog.setAttribute(Qt.WA_DeleteOnClose)
        self._job_status_dialog.finished.connect(lambda: setattr(self, '_job_status_dialog', None))
        
        self._job_status_dialog.show()

    def _trigger_group_rerun_no_prompt(self, dataset_paths: List[str], plugin_name: str):
        """Resubmits jobs without prompting the user (prompt handled in dialog)."""
        # Maps generic names to internal plugin names if needed
        plugin_map = {
            "nxds": "nXDS",
            # Add others if casing differs or mapping is needed
        }
        internal_name = plugin_map.get(plugin_name.lower(), plugin_name)
        
        # Reuse the logic from _trigger_group_rerun but bypass the question box
        self._perform_group_rerun(set(dataset_paths), internal_name)

    # --- Combination Logic ---

    def _add_combination_actions(self, menu: QMenu, info: dict):
        if info["dataset_paths"]:
            num = len(info["dataset_paths"])
            action = menu.addAction(f"Hits Combiner ({num} Selected)")
            action.triggered.connect(lambda: self._launch_combination_dialog(info["dataset_paths"]))

    def _launch_combination_dialog(self, dataset_paths: Set[str]):
        """Opens dialog and launches DatasetCombiner in a background process."""
        dialog = CombineDatasetsDialog(dataset_paths, self.main_window)
        if dialog.exec_() != QtWidgets.QDialog.Accepted:
            return

        params = dialog.get_params()

        # Resolve cluster-safe Python/script paths so the command works on
        # Slurm worker nodes that may have a different mount layout.
        cluster_python = os.environ.get("CLUSTER_PYTHON")
        cluster_root = os.environ.get("CLUSTER_PROJECT_ROOT")

        if cluster_python and cluster_root:
            python_exe = cluster_python
            execution_script = os.path.join(cluster_root, "xio", "hdf5_combiner.py")
        else:
            python_exe = sys.executable
            execution_script = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "xio", "hdf5_combiner.py",
            )

        # Construct command — use raw (unquoted) values so the list works
        # with both Popen (passes argv directly) and run_command (needs
        # shell quoting, applied below before submission).
        cmd = [python_exe, execution_script]
        cmd.extend(["--prefix", params["prefix"]])
        cmd.extend(["--outdir", params["outdir"]])
        cmd.extend(["--n", str(params["n"])])
        cmd.extend(["--nproc", str(params["nproc"])])

        if params["mode"] == "redis":
            cmd.extend(["--plugin", params["plugin"]])
            cmd.extend(["--metric", params["metric"]])
            cmd.extend(["--condition", params["condition"]])
            if params["redis_host"]:
                cmd.extend(["--redis_host", params["redis_host"]])

            # Restrict scanning to the selected datasets
            if dataset_paths:
                cmd.append("--files")
                cmd.extend(list(dataset_paths))
        else:
            # Pass mapping as JSON string
            mapping_json = json.dumps(params["mapping"])
            cmd.extend(["--mapping", mapping_json])

        try:
            os.makedirs(params["outdir"], exist_ok=True)

            if params["submit"]:
                # Submit directly to Slurm using run_command (same pattern as nXDS).
                # run_job joins the list with " ".join() so we must shell-quote
                # values that contain spaces or metacharacters.
                from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available

                if not is_sbatch_available():
                    self.main_window.ui_manager.show_critical_message(
                        "Combination Failed", "sbatch command not found. Cannot submit to Slurm."
                    )
                    return

                slurm_cmd = [shlex.quote(c) for c in cmd]

                # The combiner only needs the Python venv (already set via
                # CLUSTER_PYTHON) — no external module load required.
                pre_command_str = "set -e"
                job_name = f"combine_{params['prefix']}"

                job_id = run_command(
                    cmd=slurm_cmd,
                    cwd=params["outdir"],
                    method="slurm",
                    job_name=job_name,
                    walltime=params["time"],
                    memory=params["mem"],
                    processors=params["nproc"],
                    background=True,
                    pre_command=pre_command_str,
                )

                if job_id:
                    self.main_window.ui_manager.show_status_message(
                        f"Submitted Slurm job {job_id} for dataset combination.", 5000
                    )
                    logger.info(f"Submitted DatasetCombiner Slurm job {job_id}: {' '.join(slurm_cmd)}")
                else:
                    self.main_window.ui_manager.show_critical_message(
                        "Combination Failed", "Slurm job submission returned no job ID."
                    )
            else:
                # Run locally as a detached background process.
                # Popen with a list passes each element as a separate argv
                # entry — no shell quoting needed.
                log_path = os.path.join(params["outdir"], f"combiner_{params['prefix']}.log")
                with open(log_path, "w") as log_file:
                    subprocess.Popen(
                        cmd,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )

                self.main_window.ui_manager.show_status_message(
                    f"Launched background combination job. Log: {os.path.basename(log_path)}", 5000
                )
                logger.info(f"Launched DatasetCombiner: {' '.join(cmd)}")

        except Exception as e:
            self.main_window.ui_manager.show_critical_message(
                "Combination Failed", f"Could not launch combination process:\n{e}"
            )

    # --- New Pipeline Launch Logic ---

    def _add_standard_pipeline_actions(self, menu: QMenu, info: dict):
        """Adds XDS, autoPROC, and xia2 options."""
        dataset_paths = sorted(list(info["dataset_paths"]))
        count = len(dataset_paths)

        if count == 0:
            return

        if count == 1:
            path = dataset_paths[0]
            menu.addAction("Run XDS...").triggered.connect(
                lambda: self._launch_xds_standard([path])
            )
            menu.addAction("Run autoPROC...").triggered.connect(
                lambda: self._launch_pipeline_job("autoproc", [path], force_rerun=True)
            )
            menu.addAction("Run xia2...").triggered.connect(
                lambda: self._launch_pipeline_job("xia2", [path], force_rerun=True)
            )
        else:
            menu.addAction(f"Run XDS ({count} Datasets)...").triggered.connect(
                lambda: self._launch_xds_standard(dataset_paths)
            )
            menu.addAction(
                f"Run autoPROC (Merge {count} Datasets)..."
            ).triggered.connect(
                lambda: self._launch_pipeline_job(
                    "autoproc", dataset_paths, force_rerun=True
                )
            )
            menu.addAction(f"Run xia2 (Merge {count} Datasets)...").triggered.connect(
                lambda: self._launch_pipeline_job(
                    "xia2", dataset_paths, force_rerun=True
                )
            )

    def _add_serial_pipeline_actions(self, menu: QMenu, info: dict):
        """Adds xia2.ssx options."""
        dataset_paths = sorted(list(info["dataset_paths"]))
        count = len(dataset_paths)

        if count == 0:
            return

        if count == 1:
            path = dataset_paths[0]
            menu.addAction("Run xia2.ssx...").triggered.connect(
                lambda: self._launch_pipeline_job("xia2_ssx", [path], force_rerun=True)
            )
        else:
            # xia2.ssx Options
            ssx_menu = menu.addMenu(f"Run xia2.ssx ({count} Datasets)...")
            
            ssx_menu.addAction("Distributed (Cluster - Recommended)").triggered.connect(
                lambda: self._launch_xia2_ssx_distributed(dataset_paths)
            )
            
            ssx_menu.addAction("Merge (Standard)").triggered.connect(
                lambda: self._launch_pipeline_job("xia2_ssx", dataset_paths, force_rerun=True)
            )
            
            ssx_menu.addSeparator()
            ssx_menu.addAction(
                f"Batch Mode ({count} Datasets)..."
            ).triggered.connect(
                lambda: self._launch_xia2_ssx_batch(dataset_paths)
            )
            ssx_menu.addAction(
                f"Merge Batch Results ({count} Datasets)..."
            ).triggered.connect(
                lambda: self._launch_xia2_ssx_merge(dataset_paths)
            )

    def _launch_xia2_ssx_batch(self, dataset_paths: List[str]):
        """Launch xia2.ssx on multiple datasets, each in its own subdirectory within a common batch root."""
        # 1. Settings
        current_settings = self.main_window.settings_manager.as_dict()

        # Inject Global Settings Fallbacks before dialog
        if not current_settings.get("xia2_ssx_space_group") and current_settings.get("processing_common_space_group"):
            current_settings["xia2_ssx_space_group"] = current_settings.get("processing_common_space_group")
        if not current_settings.get("xia2_ssx_unit_cell") and current_settings.get("processing_common_unit_cell"):
            current_settings["xia2_ssx_unit_cell"] = current_settings.get("processing_common_unit_cell")
        if not current_settings.get("xia2_ssx_model") and current_settings.get("processing_common_model_file"):
            current_settings["xia2_ssx_model"] = current_settings.get("processing_common_model_file")
        if not current_settings.get("xia2_ssx_reference_hkl") and current_settings.get("processing_common_reference_reflection_file"):
            current_settings["xia2_ssx_reference_hkl"] = current_settings.get("processing_common_reference_reflection_file")
        if not current_settings.get("xia2_ssx_d_min") and current_settings.get("processing_common_res_cutoff_high"):
            current_settings["xia2_ssx_d_min"] = current_settings.get("processing_common_res_cutoff_high")
        if not current_settings.get("xia2_ssx_d_max") and current_settings.get("processing_common_res_cutoff_low"):
            current_settings["xia2_ssx_d_max"] = current_settings.get("processing_common_res_cutoff_low")
        if "xia2_ssx_native" not in current_settings:
            current_settings["xia2_ssx_native"] = current_settings.get("processing_common_native", True)

        dialog = Xia2SSXSettingsDialog(current_settings, self.main_window)
        
        if dialog.exec_() != QtWidgets.QDialog.Accepted:
            return

        # Update settings
        self.main_window.settings_manager.update_from_dict(dialog.new_settings)
        all_settings = self.main_window.settings_manager.as_dict()
        # Filter for xia2_ssx specific settings
        job_kwargs = {k: v for k, v in all_settings.items() if k.startswith("xia2_ssx_")}
        job_kwargs["force_rerun"] = True
        job_kwargs["xia2_ssx_steps"] = "find_spots+index+integrate"

        if not job_kwargs.get("xia2_ssx_d_min") and all_settings.get("processing_common_res_cutoff_high"):
            job_kwargs["xia2_ssx_d_min"] = all_settings.get("processing_common_res_cutoff_high")
        if not job_kwargs.get("xia2_ssx_d_max") and all_settings.get("processing_common_res_cutoff_low"):
            job_kwargs["xia2_ssx_d_max"] = all_settings.get("processing_common_res_cutoff_low")
        if "xia2_ssx_native" not in job_kwargs:
            job_kwargs["xia2_ssx_native"] = all_settings.get("processing_common_native", True)

        if not job_kwargs.get("xia2_ssx_space_group") and all_settings.get("processing_common_space_group"):
            job_kwargs["xia2_ssx_space_group"] = all_settings.get("processing_common_space_group")
        if not job_kwargs.get("xia2_ssx_unit_cell") and all_settings.get("processing_common_unit_cell"):
            job_kwargs["xia2_ssx_unit_cell"] = all_settings.get("processing_common_unit_cell")
        if not job_kwargs.get("xia2_ssx_model") and all_settings.get("processing_common_model_file"):
            job_kwargs["xia2_ssx_model"] = all_settings.get("processing_common_model_file")
        if not job_kwargs.get("xia2_ssx_reference_hkl") and all_settings.get("processing_common_reference_reflection_file"):
            job_kwargs["xia2_ssx_reference_hkl"] = all_settings.get("processing_common_reference_reflection_file")
        
        job_kwargs["processing_common_proc_dir_root"] = all_settings.get("processing_common_proc_dir_root", "")

        # 2. Prepare Root Directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        from qp2.xio.proc_utils import determine_proc_base_dir
        
        user_root = job_kwargs.get("processing_common_proc_dir_root") or job_kwargs.get("xia2_ssx_proc_dir_root")
        default_root = str(determine_proc_base_dir(user_root, dataset_paths[0]) / "xia2_ssx")
        
        # Naming scheme: xia2_ssx_batch_<first_dataset_name>_<count>sets_<timestamp>
        first_name = os.path.splitext(os.path.basename(dataset_paths[0]))[0]
        batch_dir_name = f"batch_{first_name}_{len(dataset_paths)}sets_{timestamp}"
        batch_root = os.path.join(default_root, batch_dir_name)
        
        try:
            os.makedirs(batch_root, exist_ok=True)
        except Exception as e:
            logger.error(f"Could not create batch root {batch_root}: {e}")

        self.main_window.ui_manager.show_status_message(f"Submitting {len(dataset_paths)} batch jobs...", 0)

        # 3. Submit Jobs
        for i, path in enumerate(dataset_paths):
            name = os.path.splitext(os.path.basename(path))[0]
            # Subdir for this dataset: batch_root/batch_<name>
            proc_dir = os.path.join(batch_root, f"batch_{name}")
            
            # Get Metadata
            reader = self.main_window.dataset_manager.get_reader(path)
            if reader:
                metadata = reader.get_parameters()
            else:
                # Fallback
                from qp2.xio.user_group_manager import get_esaf_from_data_path
                from qp2.xio.db_manager import get_beamline_from_hostname
                metadata = get_esaf_from_data_path(path)
                metadata["beamline"] = get_beamline_from_hostname()
                metadata["master_file"] = path

            # Worker
            worker = Xia2SSXProcessDatasetWorker(
                master_file=path,
                metadata=metadata,
                redis_conn=self.main_window.redis_output_server,
                redis_key_prefix="analysis:out:xia2_ssx",
                output_proc_dir=proc_dir, # Override output directory
                **job_kwargs
            )
            
            # Connect signals (simplified logging)
            worker.signals.error.connect(
                lambda p, err: logger.error(f"Batch job failed for {os.path.basename(p)}: {err}")
            )
            
            self.main_window.threadpool.start(worker)

        self.main_window.ui_manager.show_status_message(
            f"Submitted {len(dataset_paths)} jobs to {batch_root}", 5000
        )
        logger.info(f"Launched batch xia2.ssx run in {batch_root}")

    def _launch_xia2_ssx_distributed(self, dataset_paths: List[str]):
        """Launch xia2.ssx in distributed mode (one master script submits individual jobs + reducer)."""
        # 1. Settings
        current_settings = self.main_window.settings_manager.as_dict()

        # Inject Global Settings Fallbacks before dialog
        if not current_settings.get("xia2_ssx_space_group") and current_settings.get("processing_common_space_group"):
            current_settings["xia2_ssx_space_group"] = current_settings.get("processing_common_space_group")
        if not current_settings.get("xia2_ssx_unit_cell") and current_settings.get("processing_common_unit_cell"):
            current_settings["xia2_ssx_unit_cell"] = current_settings.get("processing_common_unit_cell")
        if not current_settings.get("xia2_ssx_model") and current_settings.get("processing_common_model_file"):
            current_settings["xia2_ssx_model"] = current_settings.get("processing_common_model_file")
        if not current_settings.get("xia2_ssx_reference_hkl") and current_settings.get("processing_common_reference_reflection_file"):
            current_settings["xia2_ssx_reference_hkl"] = current_settings.get("processing_common_reference_reflection_file")
        if not current_settings.get("xia2_ssx_d_min") and current_settings.get("processing_common_res_cutoff_high"):
            current_settings["xia2_ssx_d_min"] = current_settings.get("processing_common_res_cutoff_high")
        if not current_settings.get("xia2_ssx_d_max") and current_settings.get("processing_common_res_cutoff_low"):
            current_settings["xia2_ssx_d_max"] = current_settings.get("processing_common_res_cutoff_low")
        if "xia2_ssx_native" not in current_settings:
            current_settings["xia2_ssx_native"] = current_settings.get("processing_common_native", True)

        dialog = Xia2SSXSettingsDialog(current_settings, self.main_window)
        
        if dialog.exec_() != QtWidgets.QDialog.Accepted:
            return

        # Update settings
        self.main_window.settings_manager.update_from_dict(dialog.new_settings)
        all_settings = self.main_window.settings_manager.as_dict()
        # Filter for xia2_ssx specific settings
        job_kwargs = {k: v for k, v in all_settings.items() if k.startswith("xia2_ssx_")}
        job_kwargs["force_rerun"] = True
        job_kwargs["distributed"] = True
        job_kwargs["xia2_ssx_steps"] = "find_spots+index+integrate" # Enforce steps for distributed integration

        if not job_kwargs.get("xia2_ssx_d_min") and all_settings.get("processing_common_res_cutoff_high"):
            job_kwargs["xia2_ssx_d_min"] = all_settings.get("processing_common_res_cutoff_high")
        if not job_kwargs.get("xia2_ssx_d_max") and all_settings.get("processing_common_res_cutoff_low"):
            job_kwargs["xia2_ssx_d_max"] = all_settings.get("processing_common_res_cutoff_low")
        if "xia2_ssx_native" not in job_kwargs:
            job_kwargs["xia2_ssx_native"] = all_settings.get("processing_common_native", True)

        if not job_kwargs.get("xia2_ssx_space_group") and all_settings.get("processing_common_space_group"):
            job_kwargs["xia2_ssx_space_group"] = all_settings.get("processing_common_space_group")
        if not job_kwargs.get("xia2_ssx_unit_cell") and all_settings.get("processing_common_unit_cell"):
            job_kwargs["xia2_ssx_unit_cell"] = all_settings.get("processing_common_unit_cell")
        if not job_kwargs.get("xia2_ssx_model") and all_settings.get("processing_common_model_file"):
            job_kwargs["xia2_ssx_model"] = all_settings.get("processing_common_model_file")
        if not job_kwargs.get("xia2_ssx_reference_hkl") and all_settings.get("processing_common_reference_reflection_file"):
            job_kwargs["xia2_ssx_reference_hkl"] = all_settings.get("processing_common_reference_reflection_file")
        
        job_kwargs["processing_common_proc_dir_root"] = all_settings.get("processing_common_proc_dir_root", "")

        # 2. Prepare Root Directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        from qp2.xio.proc_utils import determine_proc_base_dir
        
        user_root = job_kwargs.get("processing_common_proc_dir_root") or job_kwargs.get("xia2_ssx_proc_dir_root")
        default_root = str(determine_proc_base_dir(user_root, dataset_paths[0]) / "xia2_ssx")
        
        first_name = os.path.splitext(os.path.basename(dataset_paths[0]))[0]
        batch_dir_name = f"distributed_{first_name}_{len(dataset_paths)}sets"
        batch_root = os.path.join(default_root, batch_dir_name)
        
        try:
            os.makedirs(batch_root, exist_ok=True)
        except Exception as e:
            logger.error(f"Could not create distributed root {batch_root}: {e}")

        self.main_window.ui_manager.show_status_message(f"Submitting distributed job for {len(dataset_paths)} datasets...", 0)

        # 3. Submit Jobs via Orchestrator logic in Worker
        # We pass the full list of files. the DistributedWorker will handle generating the script.
        # Worker expects master_file + extra_data_files
        job_kwargs["extra_data_files"] = dataset_paths[1:]
        
        # Extract metadata from the first file (master)
        reader = self.main_window.dataset_manager.get_reader(dataset_paths[0])
        if reader:
            metadata = reader.get_parameters()
        else:
            from qp2.xio.user_group_manager import get_esaf_from_data_path
            from qp2.xio.db_manager import get_beamline_from_hostname
            metadata = get_esaf_from_data_path(dataset_paths[0])
            metadata["beamline"] = get_beamline_from_hostname()
            metadata["master_file"] = dataset_paths[0]
        
        worker = Xia2SSXDistributedWorker(
            master_file=dataset_paths[0],
            metadata=metadata,
            redis_conn=self.main_window.redis_output_server,
            redis_key_prefix="analysis:out:xia2_ssx",
            output_dir=batch_root,
            **job_kwargs
        )
        
        # Connect result signal to show confirmation dialog with path
        worker.signals.result.connect(self._on_xia2_ssx_distributed_submission_success)
        worker.signals.error.connect(
            lambda path, err: self.main_window.ui_manager.show_critical_message(
                "Job Failed", f"xia2.ssx distributed submission error:\n{err}"
            )
        )
        
        self.main_window.threadpool.start(worker)

    def _on_xia2_ssx_distributed_submission_success(self, status: str, message: str, path: str):
        """
        Handles successful submission of a distributed xia2.ssx job.
        Displays a dialog with the working directory path.
        """
        if status == "SUBMITTED":
            # path is the proc_dir
            msg_box = QMessageBox(self.main_window)
            msg_box.setWindowTitle("Job Submitted")
            msg_box.setIcon(QMessageBox.Information)
            msg_box.setText(f"{message}\n\nWorking Directory:\n{path}")
            
            # Add open button?
            # open_btn = msg_box.addButton("Open Directory", QMessageBox.ActionRole)
            # msg_box.addButton(QMessageBox.Ok)
            # if msg_box.exec_() == QMessageBox.ActionRole: # Open clicked
            #    ...
            
            msg_box.exec_()
            
            self.main_window.ui_manager.show_status_message(message, 5000)

    def _launch_xia2_ssx_merge(self, dataset_paths: List[str]):
        """Attempts to merge results from a previous batch run of the selected datasets."""
        redis_conn = self.main_window.redis_output_server
        if not redis_conn: 
            self.main_window.ui_manager.show_warning_message("Connection Error", "Redis not available.")
            return

        import glob  # Local import to ensure availability

        expt_files = []
        refl_files = []
        proc_dirs_found = set()

        # 1. Gather all unique processing directories from selected datasets
        proc_dirs_to_check = set()
        for path in dataset_paths:
            key = f"analysis:out:xia2_ssx:{path}"
            proc_dir_bytes = redis_conn.hget(key, "_proc_dir")
            
            if proc_dir_bytes:
                proc_dir = proc_dir_bytes.decode('utf-8') if isinstance(proc_dir_bytes, bytes) else proc_dir_bytes
                proc_dirs_to_check.add(proc_dir)
                
        # 2. Extract valid experiments and reflections from unique directories
        with BusyCursor():
            for proc_dir in proc_dirs_to_check:
                if os.path.exists(proc_dir):
                    # Look for batch_* subdirectories to find specific integrated files (Standard Batch Mode)
                    curr_expts = glob.glob(os.path.join(proc_dir, "batch_*", "integrated*.expt"))
                    
                    # Look for job_*/DataFiles subdirectories to find specific integrated files (Distributed Mode)
                    if not curr_expts:
                        curr_expts = glob.glob(os.path.join(proc_dir, "job_*", "DataFiles", "integrated*.expt"))
                        
                    # Also try fallback to root dir if no batch subdir (though unlikely for SSX batch runs)
                    if not curr_expts:
                        curr_expts = glob.glob(os.path.join(proc_dir, "integrated*.expt"))

                    # FILTER: Strict pairing check
                    # For every .expt, we must find the corresponding .refl
                    valid_expts = []
                    valid_refls = []
                    
                    for expt in curr_expts:
                        refl = expt.replace(".expt", ".refl")
                        if os.path.exists(refl):
                            valid_expts.append(expt)
                            valid_refls.append(refl)
                        else:
                            logger.warning(f"Skipping orphan experiment file: {expt} (missing {refl})")

                    if valid_expts:
                        expt_files.extend(valid_expts)
                        refl_files.extend(valid_refls)
                        proc_dirs_found.add(proc_dir)

        if not expt_files:
             QtWidgets.QMessageBox.warning(self.main_window, "Merge Error", f"No paired integrated results found for the {len(dataset_paths)} selected datasets.")
             return
             
        # Determine a common parent directory for the merge output
        # Usually these share a common root if run as a batch.
        if proc_dirs_found:
            # Take the parent of the first found directory
            first_proc = list(proc_dirs_found)[0]
            common_root = os.path.dirname(first_proc)
        else:
            common_root = os.path.dirname(dataset_paths[0]) # Fallback

        reply = QtWidgets.QMessageBox.question(
            self.main_window, 
            "Confirm Merge", 
            f"Found results for {len(proc_dirs_found)} datasets.\n"
            f"({len(expt_files)} experiments, {len(refl_files)} reflection files)\n\n"
            f"Configure advanced merge settings next?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
        )
        if reply != QtWidgets.QMessageBox.Yes:
            return

        # 3. Settings Dialog Injection
        current_settings = self.main_window.settings_manager.as_dict()
        
        # Inject Global Settings Fallbacks
        if not current_settings.get("xia2_ssx_space_group") and current_settings.get("processing_common_space_group"):
            current_settings["xia2_ssx_space_group"] = current_settings.get("processing_common_space_group")
        if not current_settings.get("xia2_ssx_unit_cell") and current_settings.get("processing_common_unit_cell"):
            current_settings["xia2_ssx_unit_cell"] = current_settings.get("processing_common_unit_cell")
        if not current_settings.get("xia2_ssx_model") and current_settings.get("processing_common_model_file"):
            current_settings["xia2_ssx_model"] = current_settings.get("processing_common_model_file")
        if not current_settings.get("xia2_ssx_d_min") and current_settings.get("processing_common_res_cutoff_high"):
            current_settings["xia2_ssx_d_min"] = current_settings.get("processing_common_res_cutoff_high")

        # Reuse Xia2SSXSettingsDialog
        from qp2.image_viewer.plugins.xia2_ssx.xia2_ssx_settings_dialog import Xia2SSXSettingsDialog
        dialog = Xia2SSXSettingsDialog(current_settings, self.main_window)
        # Modify title to clarify this is just for merging
        dialog.setWindowTitle("xia2.ssx_reduce Merge Settings")
        # Hide the indexing and job control groups as they don't apply to reduce
        for child in dialog.findChildren(QtWidgets.QGroupBox):
            if "Indexing" in child.title() or "Job Control" in child.title() or "Reference Model" in child.title():
                child.hide()

        # Build custom Reference Model group that only shows PDB
        ref_group = QtWidgets.QGroupBox("Reference Model (Dimple)")
        ref_layout = QtWidgets.QFormLayout(ref_group)
        dialog.model_pdb_merge = dialog._create_file_input(
            dialog.new_settings.get("xia2_ssx_model", ""), "PDB/MTZ Files (*.pdb *.mtz *.hkl)"
        )
        ref_layout.addRow("Reference (PDB):", dialog.model_pdb_merge)
        dialog.layout().insertWidget(1, ref_group)

        # Build custom Job group for nproc
        job_group = QtWidgets.QGroupBox("Job Control")
        job_layout = QtWidgets.QFormLayout(job_group)
        dialog.nproc_merge = QtWidgets.QSpinBox(
            minimum=1, maximum=128, value=current_settings.get("xia2_ssx_nproc", 64)
        )
        job_layout.addRow("Processors:", dialog.nproc_merge)
        dialog.layout().insertWidget(2, job_group)

        if dialog.exec_() != QtWidgets.QDialog.Accepted:
            return

        # Extract values
        d_min = dialog.d_min.text().strip()
        space_group = dialog.space_group.text().strip()
        unit_cell = dialog.unit_cell.text().strip()
        model_pdb = dialog.model_pdb_merge.line_edit.text().strip()
        nproc = dialog.nproc_merge.value()
        
        # Save them back into settings so they persist
        if d_min: current_settings["xia2_ssx_d_min"] = float(d_min)
        if space_group: current_settings["xia2_ssx_space_group"] = space_group
        if unit_cell: current_settings["xia2_ssx_unit_cell"] = unit_cell
        if model_pdb: current_settings["xia2_ssx_model"] = model_pdb
        current_settings["xia2_ssx_nproc"] = nproc
        self.main_window.settings_manager.update_from_dict(current_settings)

        # Construct Merge Command
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        merge_dir = os.path.join(common_root, f"merge_selected_{len(proc_dirs_found)}datasets_{timestamp}")
        os.makedirs(merge_dir, exist_ok=True)
        
        # Generate a PHIL file to avoid 'Argument list too long' bash errors
        phil_path = os.path.join(merge_dir, "merge_input.phil")
        with open(phil_path, "w") as f:
            for expt, refl in zip(expt_files, refl_files):
                f.write(f"input {{\n  experiments = {expt}\n  reflections = {refl}\n}}\n")
                
        # Command setup
        cmd_str = f"{ProgramConfig.get_setup_command('dials')} && xia2.ssx_reduce {phil_path}"
        if nproc:
            cmd_str += f" nproc={nproc}"
        if d_min:
            cmd_str += f" d_min={d_min}"
        if space_group:
            # Prevent bash truncation
            quoted_sg = f"'{space_group}'"
            cmd_str += f" space_group={quoted_sg}"
        if unit_cell:
            from qp2.utils.auxillary import sanitize_unit_cell
            quoted_uc = f"'{sanitize_unit_cell(unit_cell)}'"
            cmd_str += f" unit_cell={quoted_uc}"
            
        # Post-merge Dimple if PDB provided
        if model_pdb:
            dimple_cmd = f"module load ccp4 && mkdir -p dimple && dimple DataFiles/merged.mtz '{model_pdb}' dimple"
            cmd_str += f" && {dimple_cmd}"
        
        job_name = f"xia2_merge_sel_{len(proc_dirs_found)}"
        
        run_command(
            cmd=cmd_str,
            cwd=merge_dir,
            method="slurm" if is_sbatch_available() else "shell",
            job_name=job_name,
            background=True,
            walltime="02:00:00",
            processors=nproc,
            memory="128gb" 
        )
        
        self.main_window.ui_manager.show_status_message(f"Submitted merge job '{job_name}' with {len(expt_files)} datasets.", 5000)

    def _launch_xds_standard(self, dataset_paths: List[str]):
        """Runs XDS on multiple datasets. Merges them if multiple are selected."""
        # 1. Settings
        current_settings = self.main_window.settings_manager.as_dict()
        
        # Inject Global Settings Fallbacks
        if not current_settings.get("xds_space_group") and current_settings.get("processing_common_space_group"):
            current_settings["xds_space_group"] = current_settings.get("processing_common_space_group")
        if not current_settings.get("xds_unit_cell") and current_settings.get("processing_common_unit_cell"):
            current_settings["xds_unit_cell"] = current_settings.get("processing_common_unit_cell")
        if not current_settings.get("xds_model_pdb") and current_settings.get("processing_common_model_file"):
            current_settings["xds_model_pdb"] = current_settings.get("processing_common_model_file")
        if not current_settings.get("xds_reference_hkl") and current_settings.get("processing_common_reference_reflection_file"):
            current_settings["xds_reference_hkl"] = current_settings.get("processing_common_reference_reflection_file")
        if not current_settings.get("xds_resolution") and current_settings.get("processing_common_res_cutoff_high"):
            current_settings["xds_resolution"] = current_settings.get("processing_common_res_cutoff_high")

        dialog = XDSSettingsDialog(current_settings, self.main_window)
        
        if dialog.exec_() != QtWidgets.QDialog.Accepted:
            return

        # Update settings
        self.main_window.settings_manager.update_from_dict(dialog.new_settings)
        all_settings = self.main_window.settings_manager.as_dict()
        # Filter for xds specific settings
        job_kwargs = {k: v for k, v in all_settings.items() if k.startswith("xds_") or k.startswith("processing_common_")}
        job_kwargs["force_rerun"] = True

        self.main_window.ui_manager.show_status_message(f"Submitting {len(dataset_paths)} XDS jobs...", 0)

        # 2. Submit Individual Jobs
        for path in dataset_paths:
            # Metadata logic
            reader = self.main_window.dataset_manager.get_reader(path)
            if reader:
                metadata = reader.get_parameters()
            else:
                from qp2.xio.user_group_manager import get_esaf_from_data_path
                from qp2.xio.db_manager import get_beamline_from_hostname
                metadata = get_esaf_from_data_path(path)
                metadata["beamline"] = get_beamline_from_hostname()
                metadata["master_file"] = path

            worker = XDSProcessDatasetWorker(
                master_file=path,
                metadata=metadata,
                redis_conn=self.main_window.redis_output_server,
                redis_key_prefix="analysis:out:xds",
                **job_kwargs
            )
            
            worker.signals.error.connect(
                lambda p, err: logger.error(f"XDS job failed for {os.path.basename(p)}: {err}")
            )
            
            self.main_window.threadpool.start(worker)

        # 3. Submit Merge Job (if applicable)
        if len(dataset_paths) > 1:
            logger.info(f"Multiple datasets selected ({len(dataset_paths)}). Submitting merge job.")
            
            # The merge job is just another XDS worker, but with 'extra_data_files' set
            # It uses the first dataset as the primary 'master_file' for submission purposes,
            # but the worker logic detects extra_data_files and switches to xscale/multiplex mode.
            
            # Use the first dataset for metadata
            first_path = dataset_paths[0]
            reader = self.main_window.dataset_manager.get_reader(first_path)
            if reader:
                metadata = reader.get_parameters()
            else:
                 from qp2.xio.user_group_manager import get_esaf_from_data_path
                 from qp2.xio.db_manager import get_beamline_from_hostname
                 metadata = get_esaf_from_data_path(first_path)
                 metadata["beamline"] = get_beamline_from_hostname()
                 metadata["master_file"] = first_path

            # Prepare merge kwargs
            merge_kwargs = job_kwargs.copy()
            # The worker expects extra_data_files to NOT include the primary master_file
            # So we pass dataset_paths[1:]
            merge_kwargs["extra_data_files"] = dataset_paths[1:]
            # Enable merge-specific optimization or settings if needed
            # merge_kwargs["xds_merge_method"] = "xscale" # Default in worker logic if not set
            
            worker = XDSProcessDatasetWorker(
                master_file=first_path,
                metadata=metadata,
                redis_conn=self.main_window.redis_output_server,
                redis_key_prefix="analysis:out:xds",
                **merge_kwargs
            )
            
            worker.signals.error.connect(
                lambda p, err: logger.error(f"XDS Merge job failed: {err}")
            )
            
            self.main_window.threadpool.start(worker)
            self.main_window.ui_manager.show_status_message(f"Submitted XDS Merge job for {len(dataset_paths)} datasets.", 5000)
        else:
            self.main_window.ui_manager.show_status_message(f"Submitted XDS job for {os.path.basename(dataset_paths[0])}", 5000)

    def _launch_pipeline_job(
        self,
        pipeline: str,
        dataset_paths: List[str],
        force_rerun: bool = False
    ):
        """
        Generic handler to configure and launch XDS, xia2, or autoPROC jobs.
        Handles both single and multi-dataset (via kwargs) scenarios.
        """
        redis_conn = self.main_window.redis_output_server
        if not redis_conn:
            self.main_window.ui_manager.show_warning_message(
                "Connection Error", "Redis Analysis connection is not available."
            )
            return

        # 1. Determine classes based on pipeline name
        if pipeline == "xds":
            DialogClass = XDSSettingsDialog
            WorkerClass = XDSProcessDatasetWorker
            key_prefix_template = "analysis:out:xds"
            settings_prefix = "xds_"
        elif pipeline == "xia2":
            DialogClass = Xia2SettingsDialog
            WorkerClass = Xia2ProcessDatasetWorker
            key_prefix_template = "analysis:out:xia2"
            settings_prefix = "xia2_"
        elif pipeline == "xia2_ssx":
            DialogClass = Xia2SSXSettingsDialog
            WorkerClass = Xia2SSXProcessDatasetWorker
            key_prefix_template = "analysis:out:xia2_ssx"
            settings_prefix = "xia2_ssx_"
        elif pipeline == "autoproc":
            DialogClass = AutoPROCSettingsDialog
            WorkerClass = AutoPROCProcessDatasetWorker
            key_prefix_template = "analysis:out:autoproc"
            settings_prefix = "autoproc_"
        else:
            return

        # 2. Fetch stored crystal data from Redis
        current_settings = self.main_window.settings_manager.as_dict()
        
        # Inject Global Settings Fallbacks before dialog
        fallback_mappings = {
            "xds": {"space_group": "space_group", "unit_cell": "unit_cell", "model_pdb": "model_file", "reference_hkl": "reference_reflection_file", "resolution": "res_cutoff_high"},
            "xia2": {"space_group": "space_group", "unit_cell": "unit_cell", "model": "model_file", "highres": "res_cutoff_high"},
            "xia2_ssx": {"space_group": "space_group", "unit_cell": "unit_cell", "model": "model_file", "reference_hkl": "reference_reflection_file", "d_min": "res_cutoff_high", "d_max": "res_cutoff_low"},
            "autoproc": {"space_group": "space_group", "unit_cell": "unit_cell", "model": "model_file", "highres": "res_cutoff_high"}
        }
        
        mapping = fallback_mappings.get(pipeline, {})
        for local_suffix, global_suffix in mapping.items():
            local_key = f"{settings_prefix}{local_suffix}"
            global_key = f"processing_common_{global_suffix}"
            if not current_settings.get(local_key) and current_settings.get(global_key):
                current_settings[local_key] = current_settings[global_key]
                
        if pipeline in ["autoproc", "xia2_ssx", "xia2"]:
            local_native = f"{settings_prefix}native"
            if local_native not in current_settings:
                current_settings[local_native] = current_settings.get("processing_common_native", True)

        primary_dataset = dataset_paths[0]

        try:
            redis_key = f"dataset:info:{primary_dataset}"
            stored_data = redis_conn.hgetall(redis_key)

            if stored_data:
                # Update current_settings with the fetched data
                if "space_group" in stored_data:
                    from qp2.utils.auxillary import sanitize_space_group
                    current_settings[f"{settings_prefix}space_group"] = sanitize_space_group(stored_data[
                        "space_group"
                    ]) or ""
                if "unit_cell" in stored_data:
                    from qp2.utils.auxillary import sanitize_unit_cell
                    current_settings[f"{settings_prefix}unit_cell"] = sanitize_unit_cell(stored_data[
                        "unit_cell"
                    ]) or ""
                if "model_pdb" in stored_data:
                    current_settings[f"{settings_prefix}model_pdb"] = stored_data[
                        "model_pdb"
                    ]
                    current_settings[f"{settings_prefix}model"] = stored_data[
                        "model_pdb"
                    ]  # for xia2/autoproc
                if "reference_hkl" in stored_data:
                    current_settings[f"{settings_prefix}reference_hkl"] = stored_data[
                        "reference_hkl"
                    ]
                logger.info(
                    f"Pre-populated dialog for {pipeline} with data from Redis key '{redis_key}'."
                )

        except Exception as e:
            logger.warning(f"Could not fetch crystal data from Redis: {e}")

        # 3. Open Settings Dialog
        dialog = DialogClass(current_settings, self.main_window)

        # When settings change in the dialog, update global settings
        dialog.settings_changed.connect(
            self.main_window.settings_manager.update_from_dict
        )

        if dialog.exec_() != QtWidgets.QDialog.Accepted:
            return

        # 4. Gather Job Parameters
        all_settings = self.main_window.settings_manager.as_dict()
        job_kwargs = {
            k: v for k, v in all_settings.items() if k.startswith(settings_prefix) or k.startswith("processing_common_")
        }

        if force_rerun:
            job_kwargs["force_rerun"] = True

        # 4. Handle Metadata
        # We use the first dataset to establish primary group/esaf info
        primary_dataset = dataset_paths[0]
        dm = self.main_window.dataset_manager

        # Try to get metadata from loaded reader
        reader = dm.get_reader(primary_dataset)
        if reader:
            metadata = reader.get_parameters()
        else:
            # Fallback if reader isn't cached (shouldn't happen if selected from tree)
            from qp2.xio.user_group_manager import get_esaf_from_data_path
            from qp2.xio.db_manager import get_beamline_from_hostname

            metadata = get_esaf_from_data_path(primary_dataset)
            metadata["beamline"] = get_beamline_from_hostname()
            metadata["master_file"] = primary_dataset

        # 5. Multi-Dataset Handling
        # If multiple datasets are selected, we pass the primary one to the worker constructor
        # and the rest in kwargs. The Worker implementation must handle 'extra_data_files'.
        if len(dataset_paths) > 1:
            job_kwargs["extra_data_files"] = dataset_paths[1:]
            logger.info(f"Submitting multi-dataset job for {len(dataset_paths)} files.")

        # 6. Create and Start Worker
        worker = WorkerClass(
            master_file=primary_dataset,
            metadata=metadata,
            redis_conn=self.main_window.redis_output_server,
            redis_key_prefix=key_prefix_template,
            **job_kwargs,
        )

        # Connect signals for feedback
        worker.signals.result.connect(
            lambda status, msg, path: self.main_window.ui_manager.show_status_message(
                f"{pipeline.upper()} Job: {msg}", 5000
            )
        )
        worker.signals.error.connect(
            lambda path, err: self.main_window.ui_manager.show_critical_message(
                "Job Failed", f"{pipeline.upper()} submission error:\n{err}"
            )
        )

        if pipeline == "nXDS":
            # Connect auto-merge trigger
            # We use a lambda to ensure it's called safely on the main thread if needed (though signals are thread-safe)
            worker.signals.result.connect(lambda: self.nxds_analysis_manager.check_auto_merge_conditions())

        self.main_window.threadpool.start(worker)

    def _add_strategy_actions(self, menu: QMenu, info: dict):
        if info["dataset_paths"]:
            strategy_menu = menu.addMenu("Run Strategy")
            action_xds = strategy_menu.addAction(
                f"on {len(info['dataset_paths'])} Dataset(s) (XDS)"
            )
            action_mosflm = strategy_menu.addAction(
                f"on {len(info['dataset_paths'])} Dataset(s) (MOSFLM)"
            )
            action_both = strategy_menu.addAction(
                f"on {len(info['dataset_paths'])} Dataset(s) (Both)"
            )

            action_xds.triggered.connect(
                lambda: self._run_strategy_on_selection("xds", info["dataset_paths"])
            )
            action_mosflm.triggered.connect(
                lambda: self._run_strategy_on_selection("mosflm", info["dataset_paths"])
            )
            action_both.triggered.connect(
                lambda: self._run_strategy_on_selection(
                    ["mosflm", "xds"], info["dataset_paths"]
                )
            )

    def _run_strategy_on_selection(self, program: [str, list], dataset_paths: Set[str]):
        """Constructs the mapping and triggers the strategy run in the main window."""
        if not dataset_paths:
            return
        mapping = {path: [1] for path in sorted(list(dataset_paths))}
        self.main_window.strategy_manager.run_strategy(program, mapping)

    def _add_processing_actions(self, menu: QMenu, info: dict):
        if info["dataset_paths"]:
            action = menu.addAction(
                f"Process {len(info['dataset_paths'])} Dataset(s)..."
            )
            action.triggered.connect(
                lambda: self.main_window._launch_dataset_processor_dialog(
                    list(info["dataset_paths"])
                )
            )

    def _add_nxds_analysis_actions(self, menu: QMenu, info: dict):
        nxds_menu = menu.addMenu("nXDS Analysis")
        selected_paths = list(info["dataset_paths"])

        if not selected_paths:
            nxds_menu.setEnabled(False)
            return

        # Find which of the selected datasets are actually ready for analysis
        ready_paths = [
            p for p in selected_paths if self._is_path_ready_for_plugin(p, "nxds")
        ]
        num_ready = len(ready_paths)

        cluster_action = nxds_menu.addAction("Cluster Unit Cells...")
        orientation_action = nxds_menu.addAction("Crystal Orientation Analysis...")
        merge_action = nxds_menu.addAction(
            f"Merge/Solve Pipeline ({num_ready} ready)..."
        )

        if num_ready > 0:
            cluster_action.triggered.connect(
                lambda: self.nxds_analysis_manager.run_cluster_analysis(ready_paths)
            )
            orientation_action.triggered.connect(
                lambda: self.nxds_analysis_manager.run_orientation_analysis(ready_paths)
            )
            merge_action.triggered.connect(
                lambda: self.nxds_analysis_manager.run_merging(ready_paths)
            )
        else:
            cluster_action.setEnabled(False)
            orientation_action.setEnabled(False)
            merge_action.setEnabled(False)
            cluster_action.setToolTip("Only available for nXDS results.")
            orientation_action.setToolTip("Only available for nXDS results.")
            merge_action.setToolTip("Only available for nXDS results.")

    def _add_dials_analysis_actions(self, menu: QMenu, info: dict):
        dials_menu = menu.addMenu("DIALS Analysis")
        selected_paths = list(info["dataset_paths"])
        is_dials_available = (
            any(self._is_path_ready_for_plugin(p, "dials:ssx") for p in selected_paths)
            if selected_paths
            else False
        )
        dials_menu.setEnabled(is_dials_available)

        placeholder = dials_menu.addAction("Clustering (Coming Soon)...")
        placeholder.setEnabled(False)

    def _add_crystfel_analysis_actions(self, menu: QMenu, info: dict):
        crystfel_menu = menu.addMenu("CrystFEL Analysis")
        merge_action = crystfel_menu.addAction("Merge Reflections...")

        selected_paths = list(info["dataset_paths"])
        # Determine if any selected path corresponds to a CrystFEL run
        is_crystfel_available = (
            any(
                self._is_path_ready_for_plugin(p, "crystfel")
                for p in selected_paths
            )
            if selected_paths
            else True
        )

        crystfel_menu.setEnabled(is_crystfel_available)

        if is_crystfel_available:
            dm = self.main_window.dataset_manager
            # If nothing was selected, info['dataset_paths'] contains all paths
            readers_to_merge = [
                dm.get_reader(p) for p in selected_paths if dm.get_reader(p)
            ]
            merge_action.triggered.connect(
                lambda: self.main_window.merging_manager.launch_merging_tool(
                    readers_to_merge
                )
            )
        else:
            merge_action.setToolTip("Only available for CrystFEL results.")

    def _add_crystfel_batch_actions(self, menu: QMenu, info: dict):
        """Adds Batch Processing actions for CrystFEL."""
        dataset_paths = sorted(list(info["dataset_paths"]))
        count = len(dataset_paths)
        
        if count == 0:
            return

        crystfel_batch_menu = menu.addMenu(f"Run CrystFEL Batch ({count} Datasets)...")
        
        crystfel_batch_menu.addAction("Process Selected Datasets").triggered.connect(
            lambda: self._launch_crystfel_batch(dataset_paths)
        )
        
        merge_action = crystfel_batch_menu.addAction("Merge Batch Results...")
        merge_action.triggered.connect(
            lambda: self._launch_crystfel_merge(dataset_paths)
        )

    def _launch_crystfel_batch(self, dataset_paths: List[str]):
        """Runs CrystFEL on multiple datasets, each in its own subdirectory."""
        # 1. Settings
        current_settings = self.main_window.settings_manager.as_dict()
        dialog = CrystfelSettingsDialog(current_settings, self.main_window)
        
        if dialog.exec_() != QtWidgets.QDialog.Accepted:
            return

        # Update settings
        self.main_window.settings_manager.update_from_dict(dialog.new_settings)
        all_settings = self.main_window.settings_manager.as_dict()
        # Filter for crystfel specific settings
        job_kwargs = {k: v for k, v in all_settings.items() if k.startswith("crystfel_")}
        job_kwargs["force_rerun"] = True

        # 2. Prepare Root Directory
        default_root = os.path.join(os.path.expanduser("~"), "crystfel_runs")
        
        first_name = os.path.splitext(os.path.basename(dataset_paths[0]))[0]
        batch_dir_name = f"crystfel_batch_{first_name}_{len(dataset_paths)}sets"
        batch_root = os.path.join(default_root, batch_dir_name)
        
        try:
            os.makedirs(batch_root, exist_ok=True)
        except Exception as e:
            logger.error(f"Could not create batch root {batch_root}: {e}")
            self.main_window.ui_manager.show_critical_message("Error", f"Could not create directory {batch_root}")
            return

        self.main_window.ui_manager.show_status_message(f"Submitting {len(dataset_paths)} CrystFEL batch jobs...", 0)

        # 3. Submit Jobs
        for i, path in enumerate(dataset_paths):
            name = os.path.splitext(os.path.basename(path))[0]
            # Subdir for this dataset: batch_root/dataset_name
            proc_dir = os.path.join(batch_root, name)
            
            # Get Metadata
            reader = self.main_window.dataset_manager.get_reader(path)
            if reader:
                metadata = reader.get_parameters()
            else:
                from qp2.xio.user_group_manager import get_esaf_from_data_path
                from qp2.xio.db_manager import get_beamline_from_hostname
                metadata = get_esaf_from_data_path(path)
                metadata["beamline"] = get_beamline_from_hostname()
                metadata["master_file"] = path

            # Worker
            worker = CrystfelProcessDatasetWorker(
                master_file=path,
                metadata=metadata,
                redis_conn=self.main_window.redis_output_server,
                redis_key_prefix="analysis:out:crystfel",
                output_proc_dir=proc_dir, # Explicitly set output directory
                **job_kwargs
            )
            
            worker.signals.error.connect(
                lambda p, err: logger.error(f"Batch job failed for {os.path.basename(p)}: {err}")
            )
            
            self.main_window.threadpool.start(worker)

        self.main_window.ui_manager.show_status_message(
            f"Submitted {len(dataset_paths)} CrystFEL jobs to {batch_root}", 5000
        )
        logger.info(f"Launched batch CrystFEL run in {batch_root}")

    def _launch_crystfel_merge(self, dataset_paths: List[str]):
        """Merges stream files from selected datasets and launches merging tool."""
        dm = self.main_window.dataset_manager
        readers = []
        for path in sorted(dataset_paths):
            reader = dm.get_reader(path)
            if reader:
                readers.append(reader)
        
        if not readers:
            self.main_window.ui_manager.show_warning_message("No Data", "Could not load readers for selected datasets.")
            return

        # Launch the existing Merging Strategy Manager
        # This manager handles stream gathering (via StreamManager) and tool execution (partialator/process_hkl)
        self.main_window.merging_manager.launch_merging_tool(readers)

    def _is_path_ready_for_plugin(self, path: str, plugin_key_part: str) -> bool:
        """Checks if a single dataset path has results for a given plugin."""
        if not path or not self.main_window.redis_output_server:
            return False
        
        redis_conn = self.main_window.redis_output_server
        key_to_check = f"analysis:out:{plugin_key_part}:{path}"
        
        # 1. Check for results_json field (CrystFEL specific but good general check)
        try:
            if redis_conn.hexists(key_to_check, "results_json"):
                return True
        except Exception:
            pass

        # 2. Check for the main key existence
        if redis_conn.exists(key_to_check) > 0:
            # If it's CrystFEL, we strictly require results_json or segments now
            if plugin_key_part == "crystfel":
                # Check for segments key too
                if redis_conn.exists(f"{key_to_check}:segments"):
                    return True
                return False 
            return True
            
        # 3. Fallback: check status key
        status_key = f"{key_to_check}:status"
        status_raw = redis_conn.get(status_key)
        if status_raw:
            try:
                status_data = json.loads(status_raw)
                if status_data.get("status") == "COMPLETED" or "results_json" in status_data:
                    return True
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass
                
        return False

    def _add_visualization_actions(self, menu: QMenu, info: dict):
        is_single_run = (
            len(info["runs"]) == 1 and not info["samples"] and not info["datasets"]
        )
        if is_single_run:
            run_prefix = list(info["runs"])[0]
            
            # Check if name has _ras_ OR if any dataset path for this run contains "/raster/"
            is_raster = "_ras_" in run_prefix
            if not is_raster:
                # Check the actual paths collected during selection analysis
                paths = info.get("dataset_paths", [])
                if any("/raster/" in p.lower() for p in paths):
                    is_raster = True
            
            if is_raster:
                action = menu.addAction("Show 2D Grid Heatmap...")
                action.triggered.connect(
                    lambda: self.main_window.heatmap_manager.launch_viewer(run_prefix)
                )

        if len(info["runs"]) == 2:
            run_a, run_b = list(info["runs"])
            dm = self.main_window.dataset_manager
            datasets_A, datasets_B = dm.get_datasets_for_run(
                run_a
            ), dm.get_datasets_for_run(run_b)
            if datasets_A and datasets_B:
                angle_A, angle_B = datasets_A[0].get_parameters().get(
                    "omega_start"
                ), datasets_B[0].get_parameters().get("omega_start")
                if angle_A is not None and angle_B is not None:
                    diff = abs(angle_A - angle_B)
                    if 85 < diff < 95 or 265 < diff < 275:
                        action = menu.addAction("Construct 3D Volume...")
                        action.triggered.connect(
                            lambda: self._launch_3d_viewer(
                                run_a, run_b, angle_A, angle_B
                            )
                        )

    def _add_selection_actions(self, menu: QMenu, info: dict):
        select_menu = menu.addMenu("Select")
        select_menu.addAction("Select All").triggered.connect(
            self.tree_widget.selectAll
        )
        select_menu.addAction("Clear Selection").triggered.connect(
            self.tree_widget.clearSelection
        )

    def _add_filter_actions(self, menu: QMenu):
        filter_menu = menu.addMenu("Filter")
        filter_menu.addAction("Filter by Name...").triggered.connect(
            lambda: self.main_window._apply_dataset_history_filter(
                "show_containing_text"
            )
        )
        filter_menu.addAction("Search by Name...").triggered.connect(
            self._search_datasets
        )
        if self.main_window.dataset_tree_manager.is_filtered:
            filter_menu.addAction("Clear Filter").triggered.connect(
                self.main_window._clear_dataset_history_filter
            )

    def _add_file_system_actions(self, menu: QMenu, info: dict):
        if info["dataset_paths"]:
            fs_menu = menu.addMenu("File System")
            fs_menu.addAction("Copy Path(s) to Clipboard").triggered.connect(
                lambda: self._copy_paths_to_clipboard(info["dataset_paths"])
            )
            fs_menu.addAction("Export Path(s) to File...").triggered.connect(
                lambda: self._export_paths_to_file(info["dataset_paths"])
            )
            fs_menu.addSeparator()
            fs_menu.addAction("Rescan Selected Files").triggered.connect(
                lambda: self._rescan_files(info["dataset_paths"])
            )

    def _add_management_actions(self, menu: QMenu, info: dict):
        management_menu = menu.addMenu("Management")
        if info["samples"] or info["runs"] or info["datasets"]:
            action = management_menu.addAction("Remove Selected from History")
            action.triggered.connect(lambda: self._remove_selected(info))

        self._add_selection_actions(management_menu, info)
        self._add_filter_actions(management_menu)

    def _launch_3d_viewer(self, run_a, run_b, angle_a, angle_b):
        angle_a_mod, angle_b_mod = abs(angle_a % 180), abs(angle_b % 180)
        if angle_a_mod > 135:
            angle_a_mod = 180 - angle_a_mod
        if angle_b_mod > 135:
            angle_b_mod = 180 - angle_b_mod
        run_xy, run_xz = (run_a, run_b) if angle_a_mod < angle_b_mod else (run_b, run_a)
        self.main_window.volume_manager.launch_viewer(run_xy, run_xz)

    def _search_datasets(self):
        text, ok = QInputDialog.getText(
            self.main_window, "Search Datasets", "Enter text to search for:"
        )
        if ok and text:
            self.tree_widget.clearSelection()
            items_to_select = self.tree_widget.findItems(
                text, Qt.MatchContains | Qt.MatchRecursive, 0
            )
            for item in items_to_select:
                item.setSelected(True)
                self.tree_widget.scrollToItem(item)

    def _copy_paths_to_clipboard(self, paths: Set[str]):
        clipboard = QApplication.clipboard()
        clipboard.setText("\n".join(sorted(list(paths))))
        self.main_window.ui_manager.show_status_message(
            f"Copied {len(paths)} paths to clipboard.", 3000
        )

    def _export_paths_to_file(self, paths: Set[str]):
        file_path, _ = QFileDialog.getSaveFileName(
            self.main_window, "Save File List", "", "Text Files (*.txt);;All Files (*)"
        )
        if file_path:
            with open(file_path, "w") as f:
                f.write("\n".join(sorted(list(paths))))
            self.main_window.ui_manager.show_status_message(
                f"Saved {len(paths)} paths to {os.path.basename(file_path)}.", 3000
            )

    def _remove_selected(self, info: dict):
        reply = QMessageBox.question(
            self.main_window,
            "Confirm Removal",
            f"Are you sure you want to remove the selected items from the history?"
            f"(This only affects the current session.)\n\n"
            f"({len(info['samples'])} samples, {len(info['runs'])} runs, {len(info['datasets'])} datasets)",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply == QMessageBox.Yes:
            # Get the path of the currently active file BEFORE removing anything
            active_file = self.main_window.current_master_file

            # This is the set of all individual dataset paths that will be removed.
            # This is the variable that holds the COMPLETE list.
            all_removed_paths = info["dataset_paths"]

            # Perform the removal from the manager
            self.main_window.dataset_manager.remove_items(
                sample_prefixes=info["samples"],
                run_prefixes=info["run_prefixes"],
                # Pass the complete list of paths to be removed, not just the
                # individually selected ones.
                dataset_paths=all_removed_paths,
            )

            # --- Logic to clear the main view if the active dataset was removed ---
            if active_file and active_file in all_removed_paths:
                self.main_window.ui_manager.show_status_message(
                    "Currently displayed dataset was removed. Clearing view.", 5000
                )
                self.main_window._on_load_failed()

    def _rescan_files(self, paths: Set[str]):
        """
        Forces a rescan of the selected dataset files. This involves removing
        them from the dataset manager and re-adding them.
        """
        if not paths:
            return

        reply = QMessageBox.question(
            self.main_window,
            "Confirm Rescan",
            f"This will reload {len(paths)} dataset(s) from disk. This is useful for refreshing file status after external processing. Continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes,
        )
        if reply == QMessageBox.No:
            return

        self.main_window.file_io_manager.reload_files(list(paths))

    def _add_group_rerun_actions(self, menu: QMenu, info: dict):
        """Adds actions to re-run analysis for an entire group (run or sample)."""
        num_paths = len(info["dataset_paths"])

        if num_paths > 1:
            rerun_menu = menu.addMenu(f"Group Rerun ({num_paths} Datasets)")
            
            # Generic nXDS Runner (independent of active plugin)
            rerun_menu.addAction("nXDS").triggered.connect(
                lambda: self._trigger_group_rerun(info["dataset_paths"], "nXDS")
            )
            
            # Additional explicit Dozor Runner
            rerun_menu.addAction("Dozor").triggered.connect(
                lambda: self._trigger_group_rerun(info["dataset_paths"], "Dozor")
            )

            # Plugin-dependent action (legacy behavior for others)
            active_plugin_name = (
                self.main_window.ui_manager.analysis_selector_combo.currentText()
            )
            if active_plugin_name != "None" and active_plugin_name not in ["nXDS", "Dozor"]:
                action = rerun_menu.addAction(f"{active_plugin_name}")
                action.triggered.connect(
                    lambda: self._trigger_group_rerun(
                        info["dataset_paths"], active_plugin_name
                    )
                )

    def _trigger_group_rerun(self, dataset_paths: Set[str], plugin_name: str):
        """Handles the logic for re-running a group of datasets for a specific plugin."""
        num_paths = len(dataset_paths)
        reply = QMessageBox.question(
            self.main_window,
            "Confirm Group Re-run",
            f"This will permanently delete existing results and start new '{plugin_name}' jobs for all {num_paths} selected datasets.\n\n"
            "This action cannot be undone. Are you sure you want to continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply == QMessageBox.No:
            return

        dialog_class_map = {
            "nXDS": NXDSSettingsDialog,
            "XDS": XDSSettingsDialog,
            "xia2": Xia2SettingsDialog,
            "xia2 SSX": Xia2SSXSettingsDialog,
            "autoPROC": AutoPROCSettingsDialog,
            "Crystfel": CrystfelSettingsDialog,
            "Dozor": DozorSettingsDialog,
            "Dials SSX": DialsSettingsDialog,
        }

        if plugin_name in dialog_class_map:
            current_settings = self.main_window.settings_manager.as_dict()
            dialog_class = dialog_class_map[plugin_name]
            dialog = dialog_class(current_settings, self.main_window)
            
            # When settings change in the dialog, update global settings
            dialog.settings_changed.connect(
                self.main_window.settings_manager.update_from_dict
            )
            
            if dialog.exec_() != QtWidgets.QDialog.Accepted:
                return

        self._perform_group_rerun(dataset_paths, plugin_name)

    def _perform_group_rerun(self, dataset_paths: Set[str], plugin_name: str):
        """Executes the re-run logic without UI prompts."""

            
        num_paths = len(dataset_paths)
        with BusyCursor():
            dm = self.main_window.dataset_manager

            # Determine or Instantiate plugin manager
            plugin_manager = self.main_window.analysis_plugin_manager.active_plugin
            is_new_manager = False

            # If the active manager matches the requested plugin, use it.
            # Otherwise, we need to instantiate the correct one on the fly.
            if not plugin_manager or plugin_manager.name != plugin_name:
                is_new_manager = True
                # Use dynamic lookup via AnalysisPluginManager registry
                available_plugins = self.main_window.analysis_plugin_manager.available_plugins
                
                if plugin_name in available_plugins:
                    full_path = available_plugins[plugin_name]
                    try:
                        module_path, class_name = full_path.rsplit(".", 1)
                        module = importlib.import_module(module_path)
                        ManagerClass = getattr(module, class_name)
                        plugin_manager = ManagerClass(self.main_window)
                    except (ImportError, AttributeError, ValueError) as e:
                         self.main_window.ui_manager.show_critical_message(
                            "Plugin Error",
                            f"Could not instantiate plugin '{plugin_name}':\n{e}",
                        )
                         return
                else:
                    self.main_window.ui_manager.show_warning_message(
                        "Plugin Mismatch",
                        f"The '{plugin_name}' plugin is not registered or available.",
                    )
                    return
                
                # Store to prevent GC and connect signals
                self._temp_manager = plugin_manager
                plugin_manager.request_main_threadpool.connect(self.main_window.threadpool.start)
                plugin_manager.status_update.connect(
                    lambda msg, t: self.main_window.ui_manager.show_status_message(msg, t)
                )

            original_master_file = self.main_window.current_master_file

            self.main_window.ui_manager.show_status_message(
                f"Starting group re-run for {num_paths} datasets...", 0
            )
            QtWidgets.QApplication.processEvents()

            for i, path in enumerate(sorted(list(dataset_paths))):
                self.main_window.ui_manager.show_status_message(
                    f"[{i + 1}/{num_paths}] Re-running: {os.path.basename(path)}", 0
                )

                # Temporarily switch the plugin's source to the target dataset
                reader = dm.get_reader(path)
                if reader:
                    # --- TRICK: Prevent auto-submission in update_source ---
                    # We mark the dataset as "already processed" so the initial load doesn't trigger a job.
                    # clear_and_rerun_without_prompt will then clear this flag and submit the *actual* job.
                    if hasattr(plugin_manager, "processed_datasets"):
                        plugin_manager.processed_datasets.add(path)
                    elif hasattr(plugin_manager, "processed_segments") and reader.frame_map:
                        # For segment-based managers (like Dozor), we'd need to block all segments.
                        # This is more complex, but the main use case here is for dataset-level plugins.
                        # Attempting to block by adding dummy segments if needed, or just accepting the double-run for segment plugins.
                        pass

                    plugin_manager.update_source(reader, path)
                    
                    # Force UI to process events so the plugin manager can update its internal state
                    QtWidgets.QApplication.processEvents()
                    
                    # Call the new public method to perform the action
                    # This will: 1. Clear Redis keys. 2. Remove 'path' from processed_datasets. 3. Trigger re-run.
                    plugin_manager.clear_and_rerun_without_prompt()
                    
                    # Give the threadpool a moment to start the job
                    QtWidgets.QApplication.processEvents()
                else:
                    logger.warning(
                        f"Could not find reader for path: {path}. Skipping re-run."
                    )

            # Restore the view to the originally selected file
            if original_master_file:
                reader = dm.get_reader(original_master_file)
                if reader:
                    plugin_manager.update_source(reader, original_master_file)

            self.main_window.ui_manager.show_status_message(
                f"Successfully submitted re-run jobs for {num_paths} datasets.", 5000
            )