# qp2/pipelines/raster_3d/pipeline_worker.py

"""
Raster3DPipelineWorker — sequential pipeline for 3D raster analysis.

Stages:
    0. Wait for analysis results (dozor/nxds) to be available in Redis
    1. Peak finding — reconstruct 3D volume, find hotspots
    2. Strategy — index best hits with XDS/MOSFLM (optional, may fail)
    3. RADDOSE-3D — dose-aware collection recommendation (optional, may fail)
"""

import json
import os
import re
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


import numpy as np
from PyQt5.QtCore import QObject, QRunnable, pyqtSignal, pyqtSlot

from qp2.log.logging_config import get_logger
from qp2.pipelines.raster_3d.config import get_source_config
from qp2.pipelines.raster_3d.matrix_builder import build_scan_aware_matrix

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------

class Raster3DPipelineSignals(QObject):
    stage_completed = pyqtSignal(str, dict)   # stage_name, result_data
    pipeline_completed = pyqtSignal(dict)      # final combined results
    error = pyqtSignal(str, str)               # stage_name, error_message


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_orthogonal(
    omega1: float, omega2: float, tolerance_deg: float = 20.0
) -> None:
    """Verify that two omega angles differ by ~90 degrees.

    Raises ``ValueError`` if the angular difference is outside tolerance.
    """
    diff = abs((omega1 - omega2) % 180)
    if diff > 90:
        diff = 180 - diff
    if abs(diff - 90) > tolerance_deg:
        raise ValueError(
            f"Runs are not orthogonal: omega1={omega1:.1f}°, omega2={omega2:.1f}°, "
            f"diff={diff:.1f}° (expected ~90°, tolerance={tolerance_deg}°)"
        )


def _get_omega_start(master_file: str) -> float:
    """Read omega_start from the first master file."""
    from qp2.xio.hdf5_manager import HDF5Reader

    reader = HDF5Reader(master_file, start_timer=False)
    params = reader.get_parameters()
    reader.close()
    return params.get("omega_start", 0.0)


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

class Raster3DPipelineWorker(QRunnable):
    """Sequential pipeline for 3D raster analysis.

    Parameters
    ----------
    run1_prefix, run2_prefix : str
        Run prefixes (run1 has lower run number).
    run1_master_files, run2_master_files : list of str
        Master HDF5 files for each run.
    run1_scan_mode, run2_scan_mode : str
        Scan mode for each run (row_wise, column_wise, etc.).
    data_dir : str
        Root data directory.
    metadata : dict
        Metadata from the triggering run (energy, beam size, etc.).
    redis_conn : redis.Redis
        Analysis Redis connection.
    proc_dir : str
        Processing output directory.
    config : dict
        Full ``raster_3d`` config section from analysis_config.json.
    pipeline_params : dict
        User/beamline metadata for DB logging.
    """

    def __init__(
        self,
        run1_prefix: str,
        run2_prefix: str,
        run1_master_files: List[str],
        run2_master_files: List[str],
        run1_scan_mode: str,
        run2_scan_mode: str,
        data_dir: str,
        metadata: dict,
        redis_conn,
        proc_dir: str,
        config: dict,
        pipeline_params: dict,
        redis_manager=None,
    ):
        super().__init__()
        self.signals = Raster3DPipelineSignals()

        from qp2.pipelines.raster_3d.matrix_builder import sort_master_files_numeric

        self.run1_prefix = run1_prefix
        self.run2_prefix = run2_prefix
        self.run1_master_files = sort_master_files_numeric(run1_master_files)
        self.run2_master_files = sort_master_files_numeric(run2_master_files)
        self.run1_scan_mode = run1_scan_mode
        self.run2_scan_mode = run2_scan_mode
        self.data_dir = data_dir
        self.metadata = metadata
        self.redis_conn = redis_conn
        self.redis_manager = redis_manager
        self.proc_dir = str(Path(proc_dir).expanduser().resolve())
        self.config = config
        self.pipeline_params = pipeline_params

        self.source_cfg = get_source_config(config)
        self._beam_params_cache = None

    # ------------------------------------------------------------------
    # Beam parameters (metadata → HDF5 fallback)
    # ------------------------------------------------------------------

    def _get_beam_params(self) -> Dict[str, float]:
        """Get beam/energy parameters with HDF5 fallback.

        Returns dict with: energy_kev, wavelength_A, beam_size_x_um,
        beam_size_y_um, flux, detector_distance_mm.
        """
        if self._beam_params_cache is not None:
            return self._beam_params_cache

        params: Dict[str, float] = {}

        # Try metadata first
        energy_ev = self.metadata.get("energy_ev")
        wavelength = self.metadata.get("wavelength")
        if energy_ev is not None:
            params["energy_kev"] = float(energy_ev) / 1000.0 if float(energy_ev) > 100 else float(energy_ev)
        elif wavelength is not None:
            params["energy_kev"] = 12.3984 / float(wavelength)

        for key, param_name in [
            ("beam_size_x_um", "beam_size_x_um"),
            ("beam_size_y_um", "beam_size_y_um"),
            ("flux", "flux"),
            ("attenuation", "raster_attenuation"),
        ]:
            val = self.metadata.get(key)
            if val is not None:
                params[param_name] = float(val)

        # Fill missing from HDF5
        if not all(k in params for k in ["energy_kev", "beam_size_x_um", "beam_size_y_um"]):
            try:
                from qp2.xio.hdf5_manager import HDF5Reader
                mf = self.run1_master_files[0]
                reader = HDF5Reader(mf, start_timer=False)
                hdf5_params = reader.get_parameters()
                reader.close()

                if "energy_kev" not in params:
                    wl = hdf5_params.get("wavelength")
                    if wl:
                        params["energy_kev"] = 12.3984 / float(wl)
                        params["wavelength_A"] = float(wl)

                # HDF5 doesn't typically store beam size in um;
                # pixel_size and beam_x/beam_y are in pixels.
                # Keep defaults for beam size if not in metadata.
            except Exception as e:
                logger.debug(f"Failed to read beam params from HDF5: {e}")

        # Try bluice Redis for beam size and attenuation if still missing
        if self.redis_manager is not None:
            if "beam_size_x_um" not in params or "beam_size_y_um" not in params:
                beam = self.redis_manager.get_beam_size(self.run1_prefix)
                if beam:
                    params["beam_size_x_um"] = beam[0]
                    params["beam_size_y_um"] = beam[1]

            if "raster_attenuation" not in params:
                atten = self.redis_manager.get_attenuation(self.run1_prefix)
                if atten is not None:
                    params["raster_attenuation"] = atten

        # Apply defaults for anything still missing
        params.setdefault("energy_kev", 12.0)
        params.setdefault("beam_size_x_um", 20.0)
        params.setdefault("beam_size_y_um", 20.0)
        params.setdefault("flux", 1e12)
        params.setdefault("raster_attenuation", 1.0)

        params["wavelength_A"] = 12.3984 / params["energy_kev"]

        self._beam_params_cache = params
        return params

    # ------------------------------------------------------------------
    # Redis status reporting
    # ------------------------------------------------------------------

    def _update_status(
        self, stage: str, status: str, detail: str = None
    ) -> None:
        try:
            status_data = {
                "status": status,
                "stage": stage,
                "timestamp": time.time(),
                "run1_prefix": self.run1_prefix,
                "run2_prefix": self.run2_prefix,
            }
            if detail:
                status_data["detail"] = detail
            self.redis_conn.set(
                f"analysis:out:raster_3d:{self.run1_prefix}:status",
                json.dumps(status_data),
                ex=7 * 24 * 3600,
            )
        except Exception as e:
            logger.warning(f"Failed to update 3D raster status in Redis: {e}")

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    @pyqtSlot()
    def run(self):
        results: Dict[str, Dict[str, Any]] = {}
        _t0 = time.time()

        try:
            self._log_step("init", (
                f"Pipeline started: {self.run1_prefix} + {self.run2_prefix}, "
                f"source={self.source_cfg.get('metric')}, "
                f"data_dir={self.data_dir}"
            ))

            # --- Stage 0: Wait for analysis results ---
            self._update_status("stage0", "WAITING", "Waiting for analysis results")
            self._log_step("stage0", (
                f"Waiting for analysis results "
                f"(timeout={self.config.get('wait_timeout_s')}s, "
                f"poll={self.config.get('poll_interval_s')}s)"
            ))
            self._stage0_wait_for_results()
            self._log_step("stage0", (
                f"Results available: {len(self.run1_master_files)} run1 files, "
                f"{len(self.run2_master_files)} run2 files"
            ))

            # --- Quality gate: check analysis results before reconstruction ---
            self._update_status("quality_check", "CHECKING", "Evaluating data quality")
            gate_result = self._check_quality_gate()
            results["quality_gate"] = gate_result
            stats = gate_result.get("stats", {})
            self._log_step("quality_gate", (
                f"pass={gate_result['pass']}, "
                f"max_score={stats.get('max_score')}, "
                f"resolution={stats.get('best_resolution_A')}A, "
                f"strong_frames={stats.get('strong_frames')}/{stats.get('total_frames')}"
            ))
            if not gate_result["pass"]:
                reason = gate_result["reason"]
                logger.info(f"Quality gate failed — skipping reconstruction: {reason}")
                self._log_step("final", f"ABORTED: {reason}")
                self._update_status("final", "ABORTED", reason)
                results["recommendation"] = {}
                results["recommendations"] = []
                self._store_results(results)
                self.signals.pipeline_completed.emit(results)
                return

            # --- Stage 1: Peak finding (required) ---
            self._update_status("stage1", "RUNNING_PEAKS", "Building 3D volume")
            self._log_step("stage1", "Building 3D volume from orthogonal rasters")
            peaks = self._stage1_find_peaks()
            results["peaks"] = {"status": "completed", "data": peaks}
            self.signals.stage_completed.emit("peaks", results["peaks"])

            # Log each peak
            step_w, step_h = self._get_step_size_um()
            step_avg = (step_w + step_h) / 2.0
            for i, p in enumerate(peaks):
                dims_um = [max(1, int(round(d * step_avg))) for d in p.get("dimensions", [])]
                self._log_step("stage1", (
                    f"  Peak {i+1}: coords={p.get('coords')}, "
                    f"size={dims_um}um, "
                    f"intensity={p.get('integrated_intensity', 0):.1f}"
                ))

            # --- Post-peak quality gate ---
            peak_gate = self._check_peak_quality(peaks)
            results["peak_quality_gate"] = peak_gate
            self._log_step("peak_gate", (
                f"pass={peak_gate['pass']}, "
                f"n_peaks={len(peaks)}, "
                f"reason={peak_gate.get('reason', 'OK')}"
            ))
            if not peak_gate["pass"]:
                reason = peak_gate["reason"]
                logger.info(f"Peak quality gate failed: {reason}")
                self._log_step("final", f"ABORTED: {reason}")
                self._update_status("final", "ABORTED", reason)
                results["recommendation"] = {}
                results["recommendations"] = []
                self._store_results(results)
                self.signals.pipeline_completed.emit(results)
                return

            self._update_status(
                "stage1", "PEAKS_COMPLETED",
                f"Found {len(peaks)} hotspot(s)"
            )

            # --- Overlap detection along rotation axis ---
            overlap_analysis = self._detect_rotation_axis_overlaps(peaks)
            results["overlap_analysis"] = overlap_analysis
            if overlap_analysis["has_overlaps"]:
                logger.warning(
                    f"Crystal overlap detected on rotation axis: "
                    f"{len(overlap_analysis['overlap_groups'])} group(s)"
                )
                for g in overlap_analysis["overlap_groups"]:
                    logger.warning(
                        f"  Overlap group: peaks {g['peak_indices']} — "
                        f"X range [{g['x_range'][0]:.1f}, {g['x_range'][1]:.1f}]"
                    )

            # Filter peaks based on overlap policy
            overlap_policy = self.config.get("overlap_policy", "best")
            peaks_to_process = self._apply_overlap_policy(
                peaks, overlap_analysis, overlap_policy
            )
            results["overlap_policy"] = overlap_policy
            results["peaks_selected"] = len(peaks_to_process)
            self._log_step("overlap", (
                f"groups={len(overlap_analysis.get('overlap_groups', []))}, "
                f"policy={overlap_policy}, "
                f"selected={len(peaks_to_process)}/{len(peaks)} peaks"
            ))

            if not peaks_to_process:
                reason = "All peaks skipped due to overlap"
                logger.info(reason)
                self._update_status("final", "ABORTED", reason)
                results["recommendation"] = {}
                results["recommendations"] = []
                self._store_results(results)
                self.signals.pipeline_completed.emit(results)
                return

            # --- Stages 2–3 + recommendation for each selected peak ---
            # Strategy is attempted on the best peak first. If it succeeds,
            # the same strategy result (start angle, SG, cell) is shared
            # across all peak recommendations. If it fails, each peak gets
            # default collection parameters.
            strategy_result = None
            try:
                self._update_status("stage2", "RUNNING_STRATEGY", "Indexing best hit")
                self._log_step("stage2", "Running XDS + MOSFLM strategy")
                strategy_result = self._stage2_run_strategy(peaks_to_process)
                results["strategy"] = {"status": "completed", "data": strategy_result}
                detail = ""
                if strategy_result:
                    sg = strategy_result.get("space_group", "?")
                    uc = strategy_result.get("unit_cell", "?")
                    res = strategy_result.get("resolution_from_spots", "?")
                    src = strategy_result.get("source", "?")
                    detail = f"SG={sg}"
                    self._log_step("stage2", (
                        f"Strategy OK ({src}): SG={sg}, "
                        f"cell={uc}, resolution={res}A"
                    ))
                else:
                    self._log_step("stage2", "Strategy returned no result")
                self._update_status("stage2", "STRATEGY_COMPLETED", detail)
            except Exception as e:
                logger.warning(f"3D raster strategy failed (continuing): {e}")
                self._log_step("stage2", f"Strategy FAILED: {e}")
                results["strategy"] = {"status": "failed", "error": str(e), "data": None}
                self._update_status("stage2", "STRATEGY_FAILED", str(e))
            self.signals.stage_completed.emit("strategy", results.get("strategy", {}))

            # Per-peak RADDOSE + recommendation
            # Optionally evaluate at multiple energies
            energy_list = self.config.get("collection_energies_kev")
            if not energy_list:
                energy_list = [None]  # None = use raster energy

            recommendations = []
            best_dose_result = None
            for i, peak in enumerate(peaks_to_process):
                dose_result = None
                try:
                    self._update_status(
                        "stage3", "RUNNING_RADDOSE",
                        f"Calculating dose for peak {i+1}/{len(peaks_to_process)}"
                    )
                    dose_result = self._stage3_run_raddose(peak, strategy_result)
                    if i == 0:
                        best_dose_result = dose_result
                    if dose_result:
                        self._log_step("stage3", (
                            f"RADDOSE peak {i+1}: "
                            f"crystal={dose_result.get('crystal_size_um')}um, "
                            f"avg_dwd={dose_result.get('avg_dwd_mgy')}MGy, "
                            f"lifetime={dose_result.get('lifetime_s')}s"
                        ))
                except Exception as e:
                    logger.warning(f"RADDOSE failed for peak {i+1}: {e}")
                    self._log_step("stage3", f"RADDOSE FAILED peak {i+1}: {e}")

                for energy_kev in energy_list:
                    rec = self._build_recommendation(
                        peak, strategy_result, dose_result,
                        energy_override_kev=energy_kev,
                    )
                    rec["peak_index"] = peak.get("_original_index", i + 1)
                    rec["overlap_free"] = peak.get("_overlap_free", True)
                    if energy_kev is not None:
                        rec["energy_label"] = f"{energy_kev:.3f}keV"
                    recommendations.append(rec)

                    energy_tag = f" @{energy_kev:.3f}keV" if energy_kev else ""
                    self._log_step("recommendation", (
                        f"Peak {i+1}{energy_tag}: "
                        f"beam={rec.get('beam_size_um')}um, "
                        f"atten={rec.get('attenuation')}x, "
                        f"exposure={rec.get('exposure_time_s')}s, "
                        f"n_images={rec.get('n_images')}, "
                        f"dose={rec.get('target_dose_mgy')}MGy, "
                        f"mode={rec.get('crystal_position', {}).get('collection_mode', '?')}"
                    ))

                logger.info(
                    f"  Peak {i+1}: coords={peak.get('coords')}, "
                    f"crystal={rec.get('crystal_position', {}).get('dimensions_um')} um, "
                    f"exposure={rec.get('exposure_time_s')}s, "
                    f"dose={rec.get('target_dose_mgy')} MGy"
                )

            results["raddose3d"] = {
                "status": "completed" if best_dose_result else "failed",
                "data": best_dose_result,
            }

            self._update_status(
                "stage3", "RADDOSE_COMPLETED",
                f"Recommendations for {len(recommendations)} peak(s)"
            )
            self.signals.stage_completed.emit("raddose3d", results.get("raddose3d", {}))

            results["recommendations"] = recommendations
            # Keep "recommendation" as the best peak's for backward compat
            results["recommendation"] = recommendations[0] if recommendations else {}

            elapsed = time.time() - _t0
            logger.info(f"Generated {len(recommendations)} collection recommendation(s)")
            self._log_step("final", (
                f"COMPLETED: {len(recommendations)} recommendation(s) "
                f"in {elapsed:.1f}s"
            ))

            # --- Store results ---
            self._store_results(results)
            self._update_status("final", "COMPLETED", "All stages finished")
            self.signals.pipeline_completed.emit(results)

        except Exception as e:
            elapsed = time.time() - _t0
            logger.error(f"3D raster pipeline failed: {e}", exc_info=True)
            self._log_step("final", f"FAILED after {elapsed:.1f}s: {e}")
            self._update_status("final", "FAILED", str(e))

            # Write partial results so downstream consumers can see
            # what stage failed and any data collected before the error
            results["error"] = str(e)
            results["status"] = "FAILED"
            results.setdefault("recommendations", [])
            results.setdefault("recommendation", {})
            try:
                self._store_results(results)
            except Exception:
                pass  # don't mask the original error

            self.signals.error.emit("pipeline", str(e))

    # ------------------------------------------------------------------
    # Stage 0: Wait for analysis results
    # ------------------------------------------------------------------

    def _stage0_wait_for_results(self) -> None:
        """Poll Redis until analysis results are available for both runs."""
        timeout = self.config.get("wait_timeout_s", 600)
        poll_interval = self.config.get("poll_interval_s", 15)
        max_retries = self.config.get("max_retries", 1)
        retry_timeout = self.config.get("retry_timeout_s", 300)
        min_coverage = self.config.get("min_coverage_pct", 80) / 100.0

        all_master_files = self.run1_master_files + self.run2_master_files

        for attempt in range(1 + max_retries):
            current_timeout = timeout if attempt == 0 else retry_timeout
            deadline = time.time() + current_timeout

            while time.time() < deadline:
                readiness = self._check_readiness(all_master_files)
                files_with_data = sum(
                    1 for avail, _ in readiness.values() if avail > 0
                )
                total_files = len(readiness)
                coverage = files_with_data / max(total_files, 1)

                if coverage >= 1.0:
                    logger.info("All analysis results available.")
                    return

                self._update_status(
                    "stage0", "WAITING",
                    f"Coverage: {files_with_data}/{total_files} files "
                    f"({coverage * 100:.0f}%)"
                )
                time.sleep(poll_interval)

            # After timeout, check if we have enough
            readiness = self._check_readiness(all_master_files)
            files_with_data = sum(1 for avail, _ in readiness.values() if avail > 0)
            coverage = files_with_data / max(len(readiness), 1)

            if coverage >= min_coverage:
                missing = [
                    mf for mf, (avail, _) in readiness.items() if avail == 0
                ]
                logger.warning(
                    f"Proceeding with {coverage * 100:.0f}% coverage. "
                    f"Missing: {len(missing)} files"
                )
                return

            # Retry: resubmit missing jobs
            if attempt < max_retries:
                missing = [
                    mf for mf, (avail, _) in readiness.items() if avail == 0
                ]
                logger.info(
                    f"Resubmitting {len(missing)} missing analysis jobs (attempt {attempt + 1})"
                )
                self._update_status(
                    "stage0", "RETRYING",
                    f"Resubmitting {len(missing)} failed jobs"
                )
                self._resubmit_analysis_jobs(missing)

        # All retries exhausted
        readiness = self._check_readiness(all_master_files)
        files_with_data = sum(1 for avail, _ in readiness.values() if avail > 0)
        coverage = files_with_data / max(len(readiness), 1)
        if coverage < min_coverage:
            raise RuntimeError(
                f"Insufficient analysis coverage: {coverage * 100:.0f}% "
                f"(need {min_coverage * 100:.0f}%)"
            )

    def _check_readiness(
        self, master_files: List[str]
    ) -> Dict[str, Tuple[int, int]]:
        """Return {master_file: (available_frames, expected_frames)}."""
        from qp2.xio.hdf5_manager import HDF5Reader

        readiness = {}
        for mf in master_files:
            redis_key = self.source_cfg["redis_key_template"].format(
                master_file=mf
            )
            try:
                available = self.redis_conn.hlen(redis_key)
            except Exception:
                available = 0
            try:
                reader = HDF5Reader(mf, start_timer=False)
                expected = reader.total_frames
                reader.close()
            except Exception:
                expected = 0
            readiness[mf] = (available, expected)
        return readiness

    def _resubmit_analysis_jobs(self, missing_files: List[str]) -> None:
        """Resubmit analysis jobs for master files with no results.

        Uses ``run_command`` directly to submit dozor via Slurm (or shell
        fallback), avoiding the PyQt5-dependent ``DozorWorker``.  The
        ``dozor_process.py`` script bootstraps its own PYTHONPATH, so it
        works in a bare Slurm environment.

        Clears stale status keys before resubmission — the status hash
        may show ``COMPLETED`` even after the results hash has expired.

        This is a best-effort operation — failures are logged but don't
        block the pipeline.
        """
        source = self.config.get("analysis_source", "dozor")
        try:
            if source == "dozor":
                from qp2.image_viewer.utils.run_job import run_command, is_sbatch_available
                from qp2.xio.hdf5_manager import HDF5Reader

                redis_key_prefix = self.source_cfg.get(
                    "redis_key_prefix", "analysis:out:spots:dozor2"
                )
                redis_host = self.redis_conn.connection_pool.connection_kwargs.get("host")
                redis_port = self.redis_conn.connection_pool.connection_kwargs.get("port")

                script_path = os.path.join(
                    os.path.dirname(__file__), "..", "..",
                    "image_viewer", "plugins", "dozor", "dozor_process.py"
                )
                script_path = os.path.abspath(script_path)
                python_exe = sys.executable
                run_method = "slurm" if is_sbatch_available() else "shell"

                for mf in missing_files:
                    try:
                        # Clear stale status so dozor doesn't skip
                        redis_key = self.source_cfg["redis_key_template"].format(
                            master_file=mf
                        )
                        status_key = f"{redis_key}:status"
                        try:
                            self.redis_conn.delete(status_key)
                            logger.debug(f"Cleared stale status: {status_key}")
                        except Exception:
                            pass

                        reader = HDF5Reader(mf, start_timer=False)
                        total = reader.total_frames
                        meta = dict(reader.params)  # HDF5 metadata with detector params
                        reader.close()
                        meta["master_file"] = mf
                        # Remove non-serializable objects
                        meta.pop("hdf5_reader_instance", None)
                        meta.pop("detector_mask", None)

                        import shlex
                        metadata_json = shlex.quote(json.dumps(meta))

                        cmd = [
                            python_exe, script_path,
                            "--metadata", metadata_json,
                            "--start", "1",
                            "--nimages", str(total),
                            "--redis_host", str(redis_host),
                            "--redis_port", str(redis_port),
                            "--redis_key_prefix", redis_key_prefix,
                        ]

                        job_name = f"dozor_resub_{os.path.basename(mf)}"
                        proc_dir = os.path.join(
                            self.proc_dir, "dozor_logs"
                        ) if getattr(self, "proc_dir", None) else os.path.join(
                            tempfile.gettempdir(), "dozor_logs"
                        )
                        os.makedirs(proc_dir, exist_ok=True)

                        logger.info(
                            f"Resubmitting dozor for {os.path.basename(mf)} "
                            f"({total} frames) via {run_method}"
                        )
                        run_command(
                            cmd=cmd,
                            cwd=proc_dir,
                            method=run_method,
                            job_name=job_name,
                            walltime="00:10:00",
                            background=True,
                            quiet=True,
                        )
                    except Exception as e:
                        logger.warning(f"Failed to resubmit dozor for {mf}: {e}")
            elif source == "nxds":
                from qp2.image_viewer.plugins.nxds.submit_nxds_job import (
                    NXDSProcessDatasetWorker,
                )
                for mf in missing_files:
                    try:
                        worker = NXDSProcessDatasetWorker(
                            master_file=mf,
                            metadata=self.metadata,
                            redis_conn=self.redis_conn,
                            redis_key_prefix="analysis:out:nxds",
                        )
                        worker.run()
                    except Exception as e:
                        logger.warning(f"Failed to resubmit nxds for {mf}: {e}")
        except Exception as e:
            logger.error(f"Failed to resubmit analysis jobs: {e}")

    # ------------------------------------------------------------------
    # Quality gates
    # ------------------------------------------------------------------

    def _check_quality_gate(self) -> Dict[str, Any]:
        """Pre-reconstruction quality gate.

        Scans dozor/nxds results across all master files and checks whether
        the data meets minimum quality thresholds.  If the raster shows no
        diffraction, reconstruction is skipped.

        Config keys (under ``raster_3d``)::

            quality_gate:
                min_max_score: 10.0        # at least one frame must have Main Score >= this
                min_resolution_A: 10.0     # at least one frame must have resolution <= this
                min_strong_frames: 3       # minimum frames passing score threshold
                score_threshold: 5.0       # frame score to count as "strong"

        All thresholds are optional — omitted checks are skipped.
        """
        gate_cfg = self.config.get("quality_gate", {})
        if not gate_cfg:
            return {"pass": True, "reason": "no quality gate configured"}

        min_max_score = gate_cfg.get("min_max_score")
        min_resolution = gate_cfg.get("min_resolution_A")
        min_strong_frames = gate_cfg.get("min_strong_frames")
        score_threshold = gate_cfg.get("score_threshold", 5.0)

        all_files = self.run1_master_files + self.run2_master_files
        metric = self.source_cfg["metric"]
        key_template = self.source_cfg["redis_key_template"]

        max_score = 0.0
        best_resolution = float("inf")
        strong_frame_count = 0
        total_frames = 0

        for mf in all_files:
            redis_key = key_template.format(master_file=mf)
            try:
                entries = self.redis_conn.hgetall(redis_key)
            except Exception:
                continue

            for raw in entries.values():
                try:
                    frame = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue

                total_frames += 1
                score = frame.get(metric, 0)
                try:
                    score = float(score)
                except (ValueError, TypeError):
                    score = 0.0

                if score > max_score:
                    max_score = score
                if score >= score_threshold:
                    strong_frame_count += 1

                # Resolution (lower = better)
                res = frame.get("Resol Visible") or frame.get("Spot Resol")
                if res is not None:
                    try:
                        res = float(res)
                        if 0 < res < best_resolution:
                            best_resolution = res
                    except (ValueError, TypeError):
                        pass

        stats = {
            "total_frames": total_frames,
            "max_score": round(max_score, 2),
            "best_resolution_A": round(best_resolution, 2) if best_resolution < float("inf") else None,
            "strong_frames": strong_frame_count,
            "score_threshold": score_threshold,
        }

        # Check thresholds
        reasons = []
        if min_max_score is not None and max_score < min_max_score:
            reasons.append(
                f"max {metric}={max_score:.1f} < required {min_max_score}"
            )
        if min_resolution is not None and best_resolution > min_resolution:
            reasons.append(
                f"best resolution={best_resolution:.1f}A > required {min_resolution}A"
            )
        if min_strong_frames is not None and strong_frame_count < min_strong_frames:
            reasons.append(
                f"strong frames={strong_frame_count} < required {min_strong_frames}"
            )

        if reasons:
            reason = "No diffraction: " + "; ".join(reasons)
            logger.info(f"Quality gate FAILED: {reason}")
            logger.info(f"  Stats: {stats}")
            return {"pass": False, "reason": reason, "stats": stats}

        logger.info(
            f"Quality gate PASSED: max_score={max_score:.1f}, "
            f"resolution={best_resolution:.1f}A, "
            f"strong_frames={strong_frame_count}/{total_frames}"
        )
        return {"pass": True, "reason": "all checks passed", "stats": stats}

    def _check_peak_quality(self, peaks: List[Dict]) -> Dict[str, Any]:
        """Post-reconstruction quality gate on found peaks.

        Config keys (under ``raster_3d``)::

            quality_gate:
                min_peaks: 1               # minimum number of hotspots
                min_peak_intensity: 1.0    # minimum integrated intensity for best peak

        Returns dict with ``pass`` bool and ``reason`` string.
        """
        gate_cfg = self.config.get("quality_gate", {})
        min_peaks = gate_cfg.get("min_peaks", 1)
        min_intensity = gate_cfg.get("min_peak_intensity")

        if len(peaks) < min_peaks:
            reason = f"Found {len(peaks)} peak(s), need >= {min_peaks}"
            return {"pass": False, "reason": reason}

        if min_intensity is not None and peaks:
            best_intensity = peaks[0].get("integrated_intensity", 0)
            if best_intensity < min_intensity:
                reason = (
                    f"Best peak intensity={best_intensity:.1f} "
                    f"< required {min_intensity}"
                )
                return {"pass": False, "reason": reason}

        return {"pass": True, "reason": "peaks meet quality criteria"}

    # ------------------------------------------------------------------
    # Rotation-axis overlap detection
    # ------------------------------------------------------------------

    def _detect_rotation_axis_overlaps(
        self, peaks: List[Dict]
    ) -> Dict[str, Any]:
        """Detect crystals that overlap along the rotation axis (X).

        Two crystals overlap if their X-coordinate ranges intersect,
        meaning both will be illuminated simultaneously during rotation
        and produce overlapping diffraction patterns.

        The X range for each peak is: [center_x - half_width_x, center_x + half_width_x]
        where half_width_x accounts for beam size.
        """
        if len(peaks) < 2:
            return {"has_overlaps": False, "overlap_groups": [], "peak_details": []}

        step_w, step_h = self._get_step_size_um()
        bp = self._get_beam_params()
        beam_half_x = bp["beam_size_x_um"] / 2.0 / step_w  # in voxels

        # Build X-range for each peak
        peak_ranges = []
        for i, peak in enumerate(peaks):
            coords = peak.get("coords", [0, 0, 0])
            dims = peak.get("dimensions", [1, 1, 1])
            center_x = coords[0]
            # Half-width along X: use the dimension projected onto X
            # dims[0] is the largest dimension; use a simpler approach:
            # the voxel extent along X is roughly dims[0]/2 if aligned,
            # but we use the actual bounding box. For safety, use
            # half of the largest dimension + beam half-width.
            half_extent_x = dims[0] / 2.0 + beam_half_x

            x_min = center_x - half_extent_x
            x_max = center_x + half_extent_x
            peak_ranges.append({
                "peak_index": i + 1,
                "center_x": center_x,
                "x_min": x_min,
                "x_max": x_max,
                "intensity": peak.get("integrated_intensity", 0),
            })

        # Find overlapping pairs using interval overlap
        # Two ranges overlap if: a.x_min < b.x_max AND b.x_min < a.x_max
        overlaps = []
        for a_idx in range(len(peak_ranges)):
            for b_idx in range(a_idx + 1, len(peak_ranges)):
                a = peak_ranges[a_idx]
                b = peak_ranges[b_idx]
                if a["x_min"] < b["x_max"] and b["x_min"] < a["x_max"]:
                    overlaps.append((a["peak_index"], b["peak_index"]))

        if not overlaps:
            return {
                "has_overlaps": False,
                "overlap_groups": [],
                "peak_details": peak_ranges,
            }

        # Build overlap groups using union-find
        parent = {pr["peak_index"]: pr["peak_index"] for pr in peak_ranges}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        for a, b in overlaps:
            union(a, b)

        # Group peaks by their root
        from collections import defaultdict
        groups_map = defaultdict(list)
        for pr in peak_ranges:
            root = find(pr["peak_index"])
            groups_map[root].append(pr)

        overlap_groups = []
        for members in groups_map.values():
            if len(members) > 1:
                indices = [m["peak_index"] for m in members]
                x_range = (
                    min(m["x_min"] for m in members),
                    max(m["x_max"] for m in members),
                )
                best = max(members, key=lambda m: m["intensity"])
                overlap_groups.append({
                    "peak_indices": indices,
                    "x_range": [round(x_range[0], 1), round(x_range[1], 1)],
                    "best_peak_index": best["peak_index"],
                    "n_crystals": len(members),
                })

        return {
            "has_overlaps": len(overlap_groups) > 0,
            "overlap_groups": overlap_groups,
            "overlap_pairs": overlaps,
            "peak_details": peak_ranges,
        }

    def _apply_overlap_policy(
        self,
        peaks: List[Dict],
        overlap_analysis: Dict,
        policy: str,
    ) -> List[Dict]:
        """Filter peaks based on the overlap policy.

        Policies:
            ``"best"`` (default): For each overlap group, keep only the
                peak with highest integrated intensity. Non-overlapping
                peaks are always kept.
            ``"all"``: Keep all peaks (user accepts multi-crystal data).
            ``"skip"``: Remove all overlapping peaks, keep only isolated ones.

        Each returned peak is annotated with ``_original_index`` and
        ``_overlap_free`` for downstream use.
        """
        # Annotate all peaks with their 1-based original index
        for i, peak in enumerate(peaks):
            peak["_original_index"] = i + 1
            peak["_overlap_free"] = True

        if not overlap_analysis.get("has_overlaps"):
            return list(peaks)

        # Mark overlapping peaks
        overlapping_indices = set()
        for group in overlap_analysis.get("overlap_groups", []):
            for idx in group["peak_indices"]:
                overlapping_indices.add(idx)

        for peak in peaks:
            if peak["_original_index"] in overlapping_indices:
                peak["_overlap_free"] = False

        if policy == "all":
            logger.info(
                f"Overlap policy 'all': keeping all {len(peaks)} peaks "
                f"(overlapping crystals will produce multi-crystal data)"
            )
            return list(peaks)

        if policy == "skip":
            selected = [p for p in peaks if p["_overlap_free"]]
            logger.info(
                f"Overlap policy 'skip': removed {len(peaks) - len(selected)} "
                f"overlapping peaks, {len(selected)} isolated peaks remain"
            )
            return selected

        # Default: "best" — keep best from each overlap group + all isolated
        best_indices = set()
        for group in overlap_analysis.get("overlap_groups", []):
            best_indices.add(group["best_peak_index"])

        selected = []
        for peak in peaks:
            idx = peak["_original_index"]
            if idx not in overlapping_indices:
                # Isolated peak — always keep
                selected.append(peak)
            elif idx in best_indices:
                # Best peak in an overlap group — keep
                selected.append(peak)
            else:
                logger.info(
                    f"Overlap policy 'best': skipping peak {idx} "
                    f"(overlaps with stronger crystal)"
                )

        logger.info(
            f"Overlap policy 'best': selected {len(selected)}/{len(peaks)} peaks"
        )
        return selected

    # ------------------------------------------------------------------
    # Stage 1: Peak finding
    # ------------------------------------------------------------------

    def _stage1_find_peaks(self) -> List[Dict]:
        """Reconstruct 3D volume and find hotspots."""
        from qp2.image_viewer.volume_map.volume_utils import (
            reconstruct_volume_with_shift,
            find_3d_hotspots,
        )

        # Read omega angles
        omega1 = _get_omega_start(self.run1_master_files[0])
        omega2 = _get_omega_start(self.run2_master_files[0])

        # Validate orthogonality
        _validate_orthogonal(omega1, omega2)

        # Assign XY vs XZ based on omega proximity to 0°/180° vs 90°/270°
        angle1_mod = abs(omega1 % 180)
        if angle1_mod > 90:
            angle1_mod = 180 - angle1_mod
        angle2_mod = abs(omega2 % 180)
        if angle2_mod > 90:
            angle2_mod = 180 - angle2_mod

        if angle1_mod < angle2_mod:
            xy_files, xy_mode = self.run1_master_files, self.run1_scan_mode
            xz_files, xz_mode = self.run2_master_files, self.run2_scan_mode
            omega_xy, omega_xz = omega1, omega2
            self._xy_run_prefix = self.run1_prefix
            self._xz_run_prefix = self.run2_prefix
            logger.info(
                f"XY scan: {self.run1_prefix} (omega={omega1:.1f}°), "
                f"XZ scan: {self.run2_prefix} (omega={omega2:.1f}°)"
            )
        else:
            xy_files, xy_mode = self.run2_master_files, self.run2_scan_mode
            xz_files, xz_mode = self.run1_master_files, self.run1_scan_mode
            omega_xy, omega_xz = omega2, omega1
            self._xy_run_prefix = self.run2_prefix
            self._xz_run_prefix = self.run1_prefix
            logger.info(
                f"XY scan: {self.run2_prefix} (omega={omega2:.1f}°), "
                f"XZ scan: {self.run1_prefix} (omega={omega1:.1f}°)"
            )

        # Build 2D data matrices
        data_xy, raw_xy, self._xy_scan_offset = build_scan_aware_matrix(
            xy_files, self.redis_conn, xy_mode, self.source_cfg
        )
        data_xz, raw_xz, self._xz_scan_offset = build_scan_aware_matrix(
            xz_files, self.redis_conn, xz_mode, self.source_cfg
        )
        # Store which files are XY vs XZ for strategy mapping
        self._xy_files = xy_files
        self._xz_files = xz_files

        if data_xy.size == 0 or data_xz.size == 0:
            raise RuntimeError("Failed to build one or both data matrices.")

        # Reconstruct 3D volume
        shift = self.config.get("shift", 0.0)
        volume = reconstruct_volume_with_shift(data_xy, data_xz, shift=shift)

        if volume.size == 0 or np.all(np.isnan(volume)):
            raise RuntimeError(
                "Volume reconstruction produced empty or all-NaN result."
            )

        logger.info(f"Reconstructed volume shape: {volume.shape}")

        # Find hotspots
        hotspots = find_3d_hotspots(
            volume,
            percentile_threshold=self.config.get("percentile_threshold", 95.0),
            min_size=self.config.get("min_size", 3),
        )

        max_peaks = self.config.get("max_peaks", 10)
        hotspots = hotspots[:max_peaks]

        # Convert numpy arrays for JSON serialization
        for peak in hotspots:
            if "orientation" in peak and isinstance(peak["orientation"], np.ndarray):
                peak["orientation"] = peak["orientation"].tolist()
            for k in ("coords", "dimensions", "angles_to_x", "extents"):
                if k in peak and isinstance(peak[k], (np.ndarray, tuple)):
                    peak[k] = [float(v) for v in peak[k]]

        logger.info(f"Found {len(hotspots)} hotspot(s)")

        # Log volume shape
        self._log_step("stage1", (
            f"XY matrix: {data_xy.shape}, XZ matrix: {data_xz.shape}, "
            f"volume: {volume.shape}, peaks: {len(hotspots)}"
        ))

        # Save diagnostic heatmap images
        out_dir = Path(self.proc_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        metric = self.source_cfg.get("metric", "Score")
        self._save_heatmap_image(
            data_xy, hotspots,
            f"XY Heatmap ({metric})",
            out_dir / "heatmap_xy.png",
            projection="xy",
            omega=omega_xy,
        )
        self._save_heatmap_image(
            data_xz, hotspots,
            f"XZ Heatmap ({metric})",
            out_dir / "heatmap_xz.png",
            projection="xz",
            omega=omega_xz,
        )

        return hotspots

    # ------------------------------------------------------------------
    # Stage 2: Strategy
    # ------------------------------------------------------------------

    def _stage2_run_strategy(
        self, peaks: List[Dict]
    ) -> Optional[Dict]:
        """Run strategy on the best peak position."""
        if not peaks:
            logger.info("No peaks — skipping strategy.")
            return None

        best_peak = peaks[0]
        coords = best_peak.get("coords", [0, 0, 0])

        # Build mappings for both orientations (XY at omega~0°, XZ at omega~90°)
        all_mappings = self._build_strategy_mappings(coords)
        if not all_mappings:
            raise RuntimeError(
                f"Could not map peak coords {coords} to any master files"
            )

        # Call strategy functions directly (StrategyWorker uses Qt signals
        # which don't work outside a Qt event loop)
        from qp2.pipelines.strategy.xds.xds_strategy import run_xds_strategy
        from qp2.pipelines.strategy.mosflm.mosflm_strategy import (
            run_strategy as run_mosflm_strategy,
        )
        from qp2.utils.tempdirectory import temporary_directory

        import concurrent.futures

        results: Dict[str, Any] = {}
        errors: List[str] = []
        programs = [("xds", run_xds_strategy), ("mosflm", run_mosflm_strategy)]

        def _run_single(program, runner, mapping, label):
            """Run one strategy program. Returns (program, label, result) or raises."""
            with temporary_directory(prefix=f"raster3d_strategy_{program}_") as wdir:
                logger.info(f"Running {program} strategy ({label}) in {wdir}")
                r = runner(mapping, workdir=wdir, pipeline_params=self.pipeline_params)
                if r:
                    logger.info(f"{program} strategy completed ({label})")
                    return program, label, r
                raise RuntimeError(f"{program} returned no result")

        # --- Experimental: dual-orientation strategy ---
        # When enabled, merges both XY and XZ frames into one mapping
        # so the strategy program sees two orthogonal images (better
        # reciprocal-space coverage for indexing).
        use_dual = self.config.get("dual_orientation_strategy", False)

        if use_dual and len(all_mappings) == 2:
            combined_mapping: Dict[str, List[int]] = {}
            for _, m in all_mappings:
                combined_mapping.update(m)
            logger.info(
                f"Dual-orientation strategy: combining "
                f"{len(combined_mapping)} master files"
            )
            self._log_step("stage2", (
                f"Dual-orientation: {list(combined_mapping.keys())}"
            ))

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = {
                    executor.submit(
                        _run_single, prog, runner, combined_mapping, "XY+XZ"
                    ): prog
                    for prog, runner in programs
                }
                for future in concurrent.futures.as_completed(futures):
                    prog = futures[future]
                    try:
                        _, _, result = future.result()
                        results[prog] = result
                    except Exception as e:
                        errors.append(f"{prog}(XY+XZ): {e}")
                        logger.warning(
                            f"{prog} dual-orientation strategy failed: {e}"
                        )

            if results:
                logger.info(
                    f"Dual-orientation strategy succeeded "
                    f"({', '.join(results.keys())})"
                )

        # --- Default: try each orientation separately, stop at first success ---
        if not results:
            for orientation_label, mapping in all_mappings:
                logger.info(
                    f"Attempting strategy with {orientation_label} orientation"
                )

                orientation_results = {}
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                    futures = {
                        executor.submit(
                            _run_single, prog, runner, mapping, orientation_label
                        ): prog
                        for prog, runner in programs
                    }
                    for future in concurrent.futures.as_completed(futures):
                        prog = futures[future]
                        try:
                            _, _, result = future.result()
                            orientation_results[prog] = result
                        except Exception as e:
                            errors.append(f"{prog}({orientation_label}): {e}")
                            logger.warning(
                                f"{prog} strategy failed on "
                                f"{orientation_label}: {e}"
                            )

                if orientation_results:
                    results = orientation_results
                    logger.info(
                        f"Strategy succeeded with {orientation_label} "
                        f"orientation ({', '.join(results.keys())})"
                    )
                    break
                else:
                    logger.info(
                        f"Strategy failed on {orientation_label}, "
                        f"trying next orientation..."
                    )

        if not results:
            if errors:
                raise RuntimeError(
                    f"All strategy programs failed: {'; '.join(errors)}"
                )
            raise RuntimeError("Strategy produced no results")

        # Extract summary from each program
        summaries = {}
        for program, raw_result in results.items():
            s = self._extract_strategy_summary(program, raw_result)
            if s:
                summaries[program] = s

        if not summaries:
            raise RuntimeError("Strategy produced no usable results")

        # Pick best: prefer the one with higher screen_score,
        # fall back to XDS > MOSFLM ordering
        def _score(s):
            return s.get("screen_score") or 0

        if len(summaries) == 1:
            best_program = next(iter(summaries))
        elif "xds" in summaries and "mosflm" in summaries:
            xds_score = _score(summaries["xds"])
            mosflm_score = _score(summaries["mosflm"])
            if mosflm_score > xds_score:
                best_program = "mosflm"
            else:
                best_program = "xds"  # prefer XDS on tie
        else:
            best_program = next(iter(summaries))

        summary = summaries[best_program]
        summary["selected_program"] = best_program
        summary["all_programs"] = {
            prog: {k: v for k, v in s.items() if k != "raw"}
            for prog, s in summaries.items()
        }

        logger.info(
            f"Strategy selected: {best_program} "
            f"(score={_score(summary):.3f}), "
            f"SG={summary.get('space_group')}, "
            f"cell={summary.get('unit_cell')}, "
            f"mosaicity={summary.get('mosaicity')}, "
            f"osc_start={summary.get('osc_start')}, "
            f"rotation={summary.get('total_rotation')}°, "
            f"max_osc={summary.get('max_osc_range')}°, "
            f"resolution={summary.get('resolution_from_spots')}A, "
            f"det_dist={summary.get('detector_distance_mm')}mm"
        )
        if len(summaries) > 1:
            for prog, s in summaries.items():
                if prog != best_program:
                    logger.info(
                        f"  Alternative ({prog}): SG={s.get('space_group')}, "
                        f"score={_score(s):.3f}, "
                        f"cell={s.get('unit_cell')}"
                    )

        return summary

    @staticmethod
    def _extract_strategy_summary(
        program: str, raw_result: Dict
    ) -> Optional[Dict[str, Any]]:
        """Extract a flat summary dict from a raw strategy result.

        Handles both XDS and MOSFLM result structures.
        """
        if not raw_result:
            return None

        s: Dict[str, Any] = {"raw": raw_result, "program": program}

        # --- XDS ---
        if "idxref" in raw_result:
            idxref = raw_result["idxref"]
            s["space_group"] = idxref.get("auto_index_spacegroup")
            s["lattice"] = idxref.get("auto_index_lattice")
            s["unit_cell"] = idxref.get("auto_index_unitcell")
            s["mosaicity"] = idxref.get("mosaicity")
            s["max_osc_range"] = idxref.get("max_osc_range")
        if "xplan" in raw_result:
            xplan = raw_result["xplan"]
            s["osc_start"] = xplan.get("xplan_starting_angle")
            s["total_rotation"] = xplan.get("xplan_total_rotation")
            s["completeness"] = xplan.get("xplan_completeness")
            s["multiplicity"] = xplan.get("xplan_multiplicity")
        if "detectordistance" in raw_result:
            s["detector_distance_mm"] = raw_result["detectordistance"]
        if "spot_res" in raw_result:
            s["resolution_from_spots"] = raw_result["spot_res"]
        if "screen_score" in raw_result:
            s["screen_score"] = raw_result["screen_score"]
        if "n_spots" in raw_result:
            s["n_spots"] = raw_result["n_spots"]
        if "matthews" in raw_result:
            s["matthews"] = raw_result["matthews"]

        # --- MOSFLM ---
        if "final" in raw_result:
            final = raw_result["final"]
            s.setdefault("space_group", final.get("spacegroup"))
            uc = final.get("unitcell")
            if uc and s.get("unit_cell") is None:
                s["unit_cell"] = uc.split() if isinstance(uc, str) else uc
            s.setdefault("mosaicity", final.get("mosaic"))
            s.setdefault("max_osc_range", final.get("osc"))
            if s.get("osc_start") is None:
                s["osc_start"] = final.get("startAngle")
            if s.get("total_rotation") is None:
                try:
                    s["total_rotation"] = (
                        float(final.get("endAngle", 360))
                        - float(final.get("startAngle", 0))
                    )
                except (ValueError, TypeError):
                    pass
            s.setdefault("completeness", final.get("nativeCompleteness"))
            s.setdefault("detector_distance_mm", final.get("distance"))
            s.setdefault("screen_score", final.get("score"))
        if "spot" in raw_result:
            spot = raw_result["spot"]
            s.setdefault("resolution_from_spots", spot.get("resolution_from_spots"))
            s.setdefault("n_spots", spot.get("n_merged") or spot.get("n_spots"))

        return s

    def _build_strategy_mappings(
        self, coords: List[float]
    ) -> List[Tuple[str, Dict[str, List[int]]]]:
        """Build strategy mappings for both scan orientations.

        Returns a list of ``(label, mapping)`` tuples, one for each
        orientation that maps successfully.  The XY scan (omega~0°) is
        listed first, the XZ scan (omega~90°) second.

        Volume axes:
            coords[0] (x) → frame index (shared by both scans)
            coords[1] (y) → scan line index in the XY scan (compact)
            coords[2] (z) → scan line index in the XZ scan (compact)
        """
        frame_idx = int(round(coords[0]))
        mappings = []

        # XY scan: y → scan line, x → frame
        xy_mapping = self._find_master_for_scan_idx(
            int(round(coords[1])),
            getattr(self, "_xy_scan_offset", 0),
            getattr(self, "_xy_files", self.run1_master_files),
            frame_idx,
        )
        if xy_mapping:
            mf = next(iter(xy_mapping))
            mappings.append(("XY", xy_mapping))
            logger.info(
                f"Strategy mapping XY: peak y={int(round(coords[1]))} "
                f"→ {mf.split('/')[-1]}, frame {frame_idx + 1}"
            )

        # XZ scan: z → scan line, x → frame
        xz_mapping = self._find_master_for_scan_idx(
            int(round(coords[2])),
            getattr(self, "_xz_scan_offset", 0),
            getattr(self, "_xz_files", self.run2_master_files),
            frame_idx,
        )
        if xz_mapping:
            mf = next(iter(xz_mapping))
            mappings.append(("XZ", xz_mapping))
            logger.info(
                f"Strategy mapping XZ: peak z={int(round(coords[2]))} "
                f"→ {mf.split('/')[-1]}, frame {frame_idx + 1}"
            )

        return mappings

    @staticmethod
    def _find_master_for_scan_idx(
        compact_idx: int,
        offset: int,
        master_files: List[str],
        frame_idx: int,
    ) -> Dict[str, List[int]]:
        """Find the master file for a given compact scan index."""
        abs_idx = compact_idx + offset
        for mf in master_files:
            for pat_str in [r"_RX(\d+)", r"_CX(\d+)", r"_R(\d+)", r"_C(\d+)"]:
                match = re.search(pat_str, mf, re.IGNORECASE)
                if match:
                    file_idx = int(match.group(1)) - 1  # 0-based
                    if file_idx == abs_idx:
                        return {mf: [frame_idx + 1]}  # 1-based frame
        return {}

    # ------------------------------------------------------------------
    # Stage 3: RADDOSE-3D
    # ------------------------------------------------------------------

    def _stage3_run_raddose(
        self,
        best_peak: Optional[Dict],
        strategy_result: Optional[Dict],
    ) -> Optional[Dict]:
        """Run RADDOSE-3D dose calculation."""
        if best_peak is None:
            logger.info("No peak data — skipping RADDOSE.")
            return None

        from qp2.radiation_decay.raddose3d import Sample, Beam, Wedge, run_raddose3d
        from qp2.radiation_decay.calculations import calculate_lifetime_and_rate

        # Convert peak dimensions from voxels to microns.
        # PCA dims (sorted largest→smallest) are used for shape analysis.
        # Axis-aligned extents are used for RADDOSE-3D (correctly mapped
        # to RADDOSE X/Y/Z convention — see COORDINATE_SYSTEMS.md).
        dims = best_peak.get("dimensions", [10, 10, 10])
        step_w, step_h = self._get_step_size_um()
        step_avg = (step_w + step_h) / 2.0
        crystal_size_um = [max(1, int(round(d * step_avg))) for d in dims]

        # Axis-aligned extents: (extent_x, extent_y, extent_z) in voxels
        # Volume axes: x=rotation, y=vertical, z=beam
        # RADDOSE axes: X=vertical, Y=rotation, Z=beam
        extents = best_peak.get("extents")
        if extents and len(extents) == 3:
            ext_x_um = max(1, int(round(extents[0] * step_w)))   # rotation axis
            ext_y_um = max(1, int(round(extents[1] * step_h)))   # vertical
            ext_z_um = max(1, int(round(extents[2] * step_avg))) # beam
            # Map to RADDOSE: Dimension X(vertical) Y(rotation) Z(beam)
            crystal_size_str = f"{ext_y_um} {ext_x_um} {ext_z_um}"
        else:
            # Fallback to PCA dims if extents not available
            crystal_size_str = " ".join(str(s) for s in crystal_size_um)

        logger.info(
            f"Crystal size (RADDOSE X Y Z): {crystal_size_str} um "
            f"(PCA dims={dims}, extents={extents}, step={step_w}x{step_h} um)"
        )

        # Build Sample — use strategy results for unit cell, symmetry,
        # and Matthews-derived nmon/nres/solvent if available.
        # coef_calc="RD3D" when cell is known (RADDOSE uses unit cell
        # composition); "AVERAGE" otherwise (generic protein).
        cell = "78 78 39 90 90 90"
        nmon = 8
        nres = 129
        coef_calc = "AVERAGE"
        solvent_fraction = 0.5

        if strategy_result:
            # Unit cell
            uc = strategy_result.get("unit_cell")
            if uc:
                if isinstance(uc, (list, tuple)):
                    cell = " ".join(str(v) for v in uc)
                elif isinstance(uc, str):
                    cell = uc
                coef_calc = "RD3D"  # use composition-based calculation

            # Matthews coefficient results (from XDS or MOSFLM strategy)
            matt = strategy_result.get("matthews")
            if matt and isinstance(matt, dict):
                nmon = matt.get("nmol", nmon)
                nres = matt.get("nres", nres)
                solvent_fraction = matt.get("solvent", solvent_fraction * 100)
                if solvent_fraction > 1:
                    solvent_fraction = solvent_fraction / 100.0  # convert % to fraction
                logger.info(
                    f"Using Matthews: nmon={nmon}, nres={nres}, "
                    f"solvent={solvent_fraction:.1%}, coef_calc={coef_calc}"
                )

        sample = Sample(
            crystal_size=crystal_size_str,
            cell=cell,
            nmon=nmon,
            nres=nres,
            coef_calc=coef_calc,
            solvent_fraction=solvent_fraction,
        )

        # Build Beam from metadata (with HDF5 fallback)
        bp = self._get_beam_params()
        energy_kev = bp["energy_kev"]
        beam_size_x = bp["beam_size_x_um"]
        beam_size_y = bp["beam_size_y_um"]
        flux = bp["flux"]

        beam = Beam(
            energy=energy_kev,
            beam_size=f"{beam_size_x} {beam_size_y}",
            flux=float(flux),
        )

        # Build Wedge for a typical full dataset
        osc = self.metadata.get("osc_range", 0.2)
        exposure = self.metadata.get("exposure_sec", 0.1)
        nimages = int(360.0 / max(osc, 0.01))

        wedge = Wedge(
            osc=float(osc),
            exposure_time_per_image=float(exposure),
            nimages=nimages,
        )

        # Run RADDOSE-3D — returns (data_list, summary_dict) on success,
        # or plain list on failure
        raddose_result = run_raddose3d(sample, beam, [wedge], swap_xy=False)

        # Parse results
        dose_result = {
            "crystal_size_um": crystal_size_str,
            "crystal_size_xyz": crystal_size_um,
            "step_size_um": [step_w, step_h],
            "energy_kev": energy_kev,
            "beam_size": f"{beam_size_x} {beam_size_y}",
        }

        data_list = None
        summary_dict = None
        if isinstance(raddose_result, tuple) and len(raddose_result) == 2:
            data_list, summary_dict = raddose_result
        elif isinstance(raddose_result, list):
            data_list = raddose_result

        # Summary dict has clean numeric values
        if summary_dict:
            dose_result["avg_dwd_mgy"] = summary_dict.get("Avg DWD")
            dose_result["max_dose_mgy"] = summary_dict.get("Max Dose")
            dose_result["last_dwd_mgy"] = summary_dict.get("Last DWD")

        # Fallback: parse from data_list entries
        if data_list and isinstance(data_list, list):
            for entry in data_list:
                if not isinstance(entry, dict):
                    continue
                for out_key, candidates in [
                    ("avg_dwd_mgy", ["Average DWD"]),
                    ("max_dose_mgy", ["Max Dose"]),
                ]:
                    if dose_result.get(out_key) is None:
                        for k in candidates:
                            v = entry.get(k)
                            if v is not None:
                                try:
                                    dose_result[out_key] = float(v)
                                except (ValueError, TypeError):
                                    dose_result[out_key] = v
                                break
                if "summary" in entry:
                    dose_result["summary"] = entry["summary"]
                break  # only first entry

        # Calculate lifetime
        try:
            wavelength_A = 12.398 / max(energy_kev, 0.1)
            lifetime_s, dose_rate = calculate_lifetime_and_rate(
                flux=float(flux),
                wavelength=wavelength_A,
                dose_limit_mgy=20.0,
                crystal_lx_um=crystal_size_um[0],
                crystal_ly_um=crystal_size_um[1],
                crystal_lz_um=crystal_size_um[2],
                beam_size_x_um=beam_size_x,
                beam_size_y_um=beam_size_y,
                attenuation_factor=1.0,
                translation_x_um=0,
            )
            dose_result["lifetime_s"] = lifetime_s
            dose_result["dose_rate_mgy_s"] = dose_rate
        except Exception as e:
            logger.warning(f"Lifetime calculation failed: {e}")

        return dose_result

    # ------------------------------------------------------------------
    # Collection recommendation
    # ------------------------------------------------------------------

    def _build_recommendation(
        self,
        best_peak: Optional[Dict],
        strategy_result: Optional[Dict],
        dose_result: Optional[Dict],
        energy_override_kev: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Build a dose-aware collection recommendation.

        Two-phase approach:
        1. ``find_experimental_recommendations()`` searches a discrete grid of
           (beam_size, attenuation, exposure_time) to find parameter
           combinations that stay within the target dose.
        2. The best candidate is optionally refined with ``run_raddose3d()``.

        Search space defaults (in ``config.py`` DEFAULT_SEARCH_SPACE) can be
        overridden per-run in the ``raster_3d`` config.  Setting a list to a
        single value effectively locks that parameter, e.g.::

            "beam_sizes": [10]        # fix beam to 10x10 um
            "attenuations": [1]       # no attenuation

        Strategy results (start angle, oscillation, detector distance) are
        used when available; otherwise defaults apply.
        """
        from collections import OrderedDict
        from qp2.radiation_decay.calculations import find_experimental_recommendations
        from qp2.radiation_decay.data_source import FluxManager
        from qp2.pipelines.raster_3d.config import DEFAULT_SEARCH_SPACE

        rec: Dict[str, Any] = {}

        # --- Crystal centering & size (with rod detection) ---
        if best_peak:
            step_w, step_h = self._get_step_size_um()
            step_avg = (step_w + step_h) / 2.0
            dims = best_peak.get("dimensions", [10, 10, 10])
            crystal_size_um = [max(1, int(round(d * step_avg))) for d in dims]
            center = best_peak.get("coords", [0, 0, 0])
            orientation = best_peak.get("orientation")
            angles_to_x = best_peak.get("angles_to_x", [90, 90, 90])

            position_info = {
                "coords_voxel": center,
                "dimensions_um": crystal_size_um,
                "angles_to_x": angles_to_x,
                "integrated_intensity": best_peak.get("integrated_intensity"),
            }

            # Detect rod shape: longest axis > 2x shortest axis
            aspect_ratio = dims[0] / max(dims[-1], 0.1) if len(dims) >= 2 else 1.0
            rod_threshold = self.config.get("rod_aspect_ratio", 2.0)

            if aspect_ratio >= rod_threshold and orientation is not None:
                # Crystal is rod-shaped. Find the long axis direction.
                # orientation[:,0] is the eigenvector for the longest axis (L)
                # in (z, y, x) order from np.argwhere. Swap to (x, y, z).
                if isinstance(orientation, list):
                    orientation = np.array(orientation)
                long_axis = orientation[:, 0][[2, 1, 0]]  # (x, y, z)

                # Check if the rod is aligned with the rotation axis (X).
                # angle_L_to_x is angles_to_x[0] — the angle between the
                # longest crystal axis and the X (rotation) axis.
                angle_L_to_x = float(angles_to_x[0]) if angles_to_x else 90.0
                rod_angle_threshold = self.config.get("rod_angle_threshold_deg", 45.0)
                rod_aligned_with_rotation = angle_L_to_x < rod_angle_threshold

                if rod_aligned_with_rotation:
                    # Rod along rotation axis → vector/helical collection
                    half_length_voxels = dims[0] / 2.0
                    center_arr = np.array(center, dtype=float)
                    endpoint1 = center_arr + long_axis * half_length_voxels
                    endpoint2 = center_arr - long_axis * half_length_voxels

                    position_info["collection_mode"] = "vector"
                    position_info["center_start_voxel"] = [
                        round(float(v), 1) for v in endpoint1
                    ]
                    position_info["center_end_voxel"] = [
                        round(float(v), 1) for v in endpoint2
                    ]
                    position_info["rod_length_um"] = int(round(dims[0] * step_avg))
                    position_info["rod_width_um"] = int(round(dims[1] * step_avg))
                    position_info["aspect_ratio"] = round(aspect_ratio, 1)
                    position_info["angle_to_rotation_axis"] = round(angle_L_to_x, 1)

                    translation_um = max(
                        0, int(round(dims[0] * step_avg)) - crystal_size_um[1]
                    )
                    position_info["translation_um"] = translation_um

                    logger.info(
                        f"Rod along rotation axis: aspect={aspect_ratio:.1f}, "
                        f"angle_to_X={angle_L_to_x:.1f}°, "
                        f"length={position_info['rod_length_um']} um, "
                        f"start={position_info['center_start_voxel']}, "
                        f"end={position_info['center_end_voxel']}"
                    )
                else:
                    # Rod perpendicular to rotation axis → single center is fine
                    position_info["collection_mode"] = "standard"
                    position_info["aspect_ratio"] = round(aspect_ratio, 1)
                    position_info["angle_to_rotation_axis"] = round(angle_L_to_x, 1)
                    translation_um = 0
                    logger.info(
                        f"Rod perpendicular to rotation axis: aspect={aspect_ratio:.1f}, "
                        f"angle_to_X={angle_L_to_x:.1f}° (>{rod_angle_threshold}°), "
                        f"using single center"
                    )
            else:
                position_info["collection_mode"] = "standard"
                translation_um = 0

            rec["crystal_position"] = position_info

            # Optional: convert voxel coords to motor positions
            if self.config.get("compute_motor_positions", False):
                try:
                    motor = self._voxel_to_motor(center, self.run1_prefix)
                    if motor:
                        position_info["motor_position"] = motor
                        logger.info(f"Motor position: {motor}")

                    # Also convert vector endpoints if rod
                    if position_info.get("collection_mode") == "vector":
                        start_motor = self._voxel_to_motor(
                            position_info["center_start_voxel"],
                            self.run1_prefix,
                        )
                        end_motor = self._voxel_to_motor(
                            position_info["center_end_voxel"],
                            self.run1_prefix,
                        )
                        if start_motor:
                            position_info["motor_start"] = start_motor
                        if end_motor:
                            position_info["motor_end"] = end_motor
                except Exception as e:
                    logger.debug(f"Motor position conversion failed: {e}")
        else:
            crystal_size_um = [20, 20, 20]
            rec["crystal_position"] = None
            translation_um = 0

        # --- Collection geometry from strategy ---
        has_strategy = (
            strategy_result is not None
            and strategy_result.get("raw") is not None
        )

        resolution = None
        detector_distance_mm = None
        if has_strategy:
            # Read from flat summary (populated by _stage2_run_strategy)
            osc_start = strategy_result.get("osc_start")
            total_rotation = strategy_result.get("total_rotation")
            osc_delta = strategy_result.get("max_osc_range")
            resolution = strategy_result.get("resolution_from_spots")

            det_dist = strategy_result.get("detector_distance_mm")
            if det_dist:
                try:
                    detector_distance_mm = float(det_dist)
                except (ValueError, TypeError):
                    pass

            try:
                start_angle = float(osc_start) if osc_start is not None else 0.0
            except (ValueError, TypeError):
                start_angle = 0.0
            try:
                end_angle = (
                    start_angle + float(total_rotation)
                    if total_rotation is not None
                    else start_angle + 360.0
                )
            except (ValueError, TypeError):
                end_angle = start_angle + 360.0
            try:
                osc_width = float(osc_delta) if osc_delta else 0.2
            except (ValueError, TypeError):
                osc_width = 0.2

            rec["strategy_source"] = strategy_result.get("selected_program", "strategy")
            rec["space_group"] = strategy_result.get("space_group")
            rec["lattice"] = strategy_result.get("lattice")
            rec["unit_cell"] = strategy_result.get("unit_cell")
            rec["mosaicity"] = strategy_result.get("mosaicity")
            rec["completeness"] = strategy_result.get("completeness")
            rec["screen_score"] = strategy_result.get("screen_score")
        else:
            start_angle = 0.0
            end_angle = 360.0
            osc_width = 0.2
            rec["strategy_source"] = "default"
            rec["space_group"] = None
            rec["unit_cell"] = None
            rec["mosaicity"] = None

        if resolution:
            try:
                resolution = float(resolution)
            except (ValueError, TypeError):
                resolution = None

        # Try dozor "Resol Visible" at the peak frame — typically a better
        # resolution estimate than strategy's resolution_from_spots because
        # dozor analyses the full diffraction pattern, not just strong spots.
        dozor_resolution = None
        if best_peak:
            dozor_resolution = self._get_dozor_resolution_at_peak(best_peak)
            if dozor_resolution is not None:
                logger.info(
                    f"Dozor resolution at peak: {dozor_resolution:.2f}A "
                    f"(strategy: {resolution}A)"
                )

        # Use the better (lower) resolution: prefer dozor over strategy
        best_resolution = resolution
        resolution_source = "strategy"
        if dozor_resolution is not None:
            if best_resolution is None or dozor_resolution < best_resolution:
                best_resolution = dozor_resolution
                resolution_source = "dozor"

        # Check if resolution is limited by detector edge
        resolution_at_edge = False
        edge_resolution = None
        try:
            import math
            from qp2.xio.hdf5_manager import HDF5Reader
            _reader = HDF5Reader(self.run1_master_files[0], start_timer=False)
            _p = _reader.get_parameters()
            _reader.close()
            wl_raw = _p.get("wavelength")
            wl = float(wl_raw) if wl_raw else 12.3984 / self._get_beam_params().get("energy_kev", 12.0)
            dist = _p.get("det_dist", 300)
            px = _p.get("pixel_size", 0.075)
            nx, ny = _p.get("nx", 4150), _p.get("ny", 4371)
            bx, by = _p.get("beam_x", nx / 2), _p.get("beam_y", ny / 2)
            edge_px = min(bx, nx - bx, by, ny - by)
            edge_mm = edge_px * px
            theta = 0.5 * math.atan(edge_mm / dist)
            edge_resolution = round(wl / (2 * math.sin(theta)), 2)

            if best_resolution is not None and edge_resolution is not None:
                # Within 10% of edge → likely limited by detector geometry
                if best_resolution <= edge_resolution * 1.1:
                    resolution_at_edge = True
                    logger.warning(
                        f"Resolution ({best_resolution:.2f}A) is near "
                        f"detector edge ({edge_resolution:.2f}A) — "
                        f"crystal may diffract better at shorter "
                        f"detector distance"
                    )
                    self._log_step("resolution", (
                        f"WARNING: resolution {best_resolution:.2f}A "
                        f"near detector edge {edge_resolution:.2f}A — "
                        f"consider shorter detector distance"
                    ))
            # Recalculate detector distance so the best resolution
            # (with 10% buffer) falls at the detector edge.  This uses
            # the same geometry as XDS XPLAN.
            if best_resolution is not None and best_resolution > 0:
                buffer_factor = 0.9  # 10% buffer beyond target resolution
                d_target = best_resolution * buffer_factor
                theta_target = math.asin(wl / (2.0 * d_target))
                recommended_dist = edge_mm / math.tan(2.0 * theta_target)
                recommended_dist = round(recommended_dist, 1)

                # Only override if strategy distance would lose resolution
                if (detector_distance_mm is None
                        or recommended_dist < detector_distance_mm):
                    logger.info(
                        f"Detector distance recalculated: {recommended_dist} mm "
                        f"(resolution {best_resolution:.2f}A with 10% buffer "
                        f"at edge, strategy was {detector_distance_mm} mm)"
                    )
                    self._log_step("detector", (
                        f"Recommended distance: {recommended_dist} mm "
                        f"(target {d_target:.2f}A at edge, "
                        f"strategy: {detector_distance_mm} mm)"
                    ))
                    detector_distance_mm = recommended_dist
        except Exception as e:
            logger.debug(f"Could not compute edge resolution: {e}")

        rec["resolution_A"] = best_resolution
        rec["resolution_source"] = resolution_source
        rec["resolution_strategy_A"] = resolution
        rec["resolution_dozor_A"] = dozor_resolution
        rec["resolution_at_edge"] = resolution_at_edge
        rec["edge_resolution_A"] = edge_resolution
        rec["detector_distance_mm"] = detector_distance_mm

        total_rotation = end_angle - start_angle
        rec["start_angle"] = round(start_angle, 2)
        rec["end_angle"] = round(end_angle, 2)
        rec["total_rotation"] = round(total_rotation, 2)
        rec["osc_width"] = round(osc_width, 4)

        # --- Target dose ---
        user_dose = self.config.get("target_dose_mgy")
        if user_dose is not None:
            target_dose = float(user_dose)
            dose_source = "user_config"
        elif best_resolution is not None and best_resolution > 0:
            target_dose = best_resolution * 10.0
            dose_source = f"resolution({best_resolution:.1f}A/{resolution_source}) * 10"
        else:
            target_dose = 30.0
            dose_source = "default"

        rec["target_dose_mgy"] = round(target_dose, 2)
        rec["dose_source"] = dose_source

        # --- Beam parameters ---
        bp = self._get_beam_params()
        if energy_override_kev is not None:
            energy_kev = energy_override_kev
            wavelength_A = 12.3984 / energy_kev
        else:
            energy_kev = bp["energy_kev"]
            wavelength_A = bp["wavelength_A"]
        flux = bp["flux"]

        rec["energy_kev"] = round(energy_kev, 4)
        rec["wavelength_A"] = round(wavelength_A, 4)
        rec["flux"] = flux

        # --- Build search space (defaults + user overrides) ---
        search = self.config.get("search_space", {})

        beam_sizes_list = search.get("beam_sizes", DEFAULT_SEARCH_SPACE["beam_sizes"])
        beam_sizes = [(s, s) for s in beam_sizes_list]  # square beams

        attenuations = search.get("attenuations", DEFAULT_SEARCH_SPACE["attenuations"])
        exposure_times = search.get("exposure_times", DEFAULT_SEARCH_SPACE["exposure_times"])
        translations_list = search.get("translations", DEFAULT_SEARCH_SPACE["translations"])
        n_images_search = search.get("n_images", DEFAULT_SEARCH_SPACE["n_images"])

        # If strategy gave us n_images, include that in the search
        strategy_n_images = max(1, int(round(total_rotation / osc_width)))
        if strategy_n_images not in n_images_search:
            n_images_search = [strategy_n_images] + list(n_images_search)

        # Build translations dict keyed by "BxB" beam size string.
        # For rod crystals, also include the helical translation.
        translations_to_search = {}
        for bx, by in beam_sizes:
            trans = list(translations_list)
            if translation_um > 0 and translation_um not in trans:
                trans.append(translation_um)
            translations_to_search[f"{bx}x{by}"] = trans

        flux_manager = FluxManager(OrderedDict({energy_kev: flux}))

        try:
            # --- Phase 1: rough search with find_experimental_recommendations ---
            candidates = find_experimental_recommendations(
                crystal_dims=tuple(crystal_size_um),
                dose_limit_mgy=target_dose,
                flux_manager=flux_manager,
                desired_n_images_to_search=n_images_search,
                beam_sizes_to_search=beam_sizes,
                wavelengths_to_search=[wavelength_A],
                attenuations_to_search=attenuations,
                translations_to_search=translations_to_search,
                exposure_times_to_search=exposure_times,
            )

            if not candidates:
                logger.warning("No feasible recommendations found in search space")
                rec["beam_size_um"] = [bp["beam_size_x_um"], bp["beam_size_y_um"]]
                rec["n_images"] = strategy_n_images
                rec["exposure_time_s"] = 0.1
                rec["attenuation"] = 1.0
                rec["estimated_dose_mgy"] = None
                rec["search_result"] = "no_feasible_solution"
                return rec

            # Sort: prefer dose closest to target (use the budget), then
            # lower beam–crystal mismatch. This avoids over-attenuated
            # solutions that waste most of the dose budget.
            candidates.sort(
                key=lambda c: (
                    abs(c["effective_dose_mgy"] - target_dose),
                    c["mismatch_score"],
                )
            )

            best = candidates[0]
            rec["beam_size_um"] = list(best["beam_size_um"])
            rec["n_images"] = best["n_images"]
            rec["exposure_time_s"] = best["exposure_time_s"]
            rec["attenuation"] = best["attenuation_factor"]
            rec["estimated_dose_mgy"] = round(best["effective_dose_mgy"], 2)
            rec["translation_x_um"] = best["translation_x_um"]
            rec["total_collection_time_s"] = round(
                best["exposure_time_s"] * best["n_images"], 2
            )
            rec["search_candidates"] = len(candidates)

            # Recalculate end angle from best n_images
            rec["n_images"] = best["n_images"]
            rec["end_angle"] = round(start_angle + best["n_images"] * osc_width, 2)
            rec["total_rotation"] = round(best["n_images"] * osc_width, 2)

            # Include top N alternative solutions for user inspection
            n_alternatives = int(self.config.get("n_recommendations", 1))
            if n_alternatives > 1:
                alternatives = []
                for alt in candidates[1:n_alternatives]:
                    alternatives.append({
                        "beam_size_um": list(alt["beam_size_um"]),
                        "n_images": alt["n_images"],
                        "exposure_time_s": alt["exposure_time_s"],
                        "attenuation": alt["attenuation_factor"],
                        "estimated_dose_mgy": round(alt["effective_dose_mgy"], 2),
                        "translation_x_um": alt["translation_x_um"],
                        "total_collection_time_s": round(
                            alt["exposure_time_s"] * alt["n_images"], 2
                        ),
                        "end_angle": round(
                            start_angle + alt["n_images"] * osc_width, 2
                        ),
                        "total_rotation": round(
                            alt["n_images"] * osc_width, 2
                        ),
                    })
                rec["alternatives"] = alternatives

            # --- Phase 2: RADDOSE-3D validation on best candidate ---
            try:
                from qp2.radiation_decay.raddose3d import (
                    Sample, Beam, Wedge, run_raddose3d,
                )
                from qp2.radiation_decay.calculations import (
                    _setup_raddose3d_input,
                )

                cell = "78 78 39 90 90 90"
                r3d_nmon = 8
                r3d_nres = 129
                r3d_solvent = 0.5
                r3d_coef = "AVERAGE"
                if strategy_result:
                    uc = strategy_result.get("unit_cell")
                    if uc:
                        if isinstance(uc, (list, tuple)):
                            cell = " ".join(str(v) for v in uc)
                        elif isinstance(uc, str):
                            cell = uc
                        r3d_coef = "RD3D"
                    matt = strategy_result.get("matthews")
                    if matt and isinstance(matt, dict):
                        r3d_nmon = matt.get("nmol", r3d_nmon)
                        r3d_nres = matt.get("nres", r3d_nres)
                        sv = matt.get("solvent", r3d_solvent * 100)
                        r3d_solvent = sv / 100.0 if sv > 1 else sv

                base_params = {
                    "crystal_dims": tuple(crystal_size_um),
                    "cell": cell,
                    "nres": r3d_nres,
                    "nmon": r3d_nmon,
                    "shape": "Cuboid",
                    "coef_calc": r3d_coef,
                    "solvent_fraction": r3d_solvent,
                    "flux": flux,
                    "osc": osc_width,
                    "nimages": best["n_images"],
                    "ndna": 0, "nrna": 0, "ncarb": 0,
                    "protein_heavy_atoms": "",
                    "solvent_heavy_conc": "",
                    "pdb_path_or_code": "",
                }
                dynamic_params = {
                    "beam_size_um": best["beam_size_um"],
                    "wavelength_a": best["wavelength_a"],
                    "attenuation_factor": best["attenuation_factor"],
                    "translation_x_um": best["translation_x_um"],
                    "exposure_time_s": best["exposure_time_s"],
                    "n_images": best["n_images"],
                }

                sample, beam, wedges = _setup_raddose3d_input(
                    base_params, dynamic_params
                )
                raddose_result = run_raddose3d(sample, beam, wedges, swap_xy=False)

                if isinstance(raddose_result, tuple) and len(raddose_result) == 2:
                    _, summary = raddose_result
                    if summary:
                        rec["raddose3d_avg_dwd_mgy"] = summary.get("Avg DWD")
                        rec["raddose3d_max_dose_mgy"] = summary.get("Max Dose")
                        logger.info(
                            f"RADDOSE-3D validation: Avg DWD={summary.get('Avg DWD')} MGy, "
                            f"Max Dose={summary.get('Max Dose')} MGy"
                        )
            except Exception as e:
                logger.warning(f"RADDOSE-3D validation failed (using rough estimate): {e}")

        except Exception as e:
            logger.warning(f"Recommendation search failed: {e}", exc_info=True)
            rec["beam_size_um"] = [bp["beam_size_x_um"], bp["beam_size_y_um"]]
            rec["n_images"] = strategy_n_images
            rec["exposure_time_s"] = 0.1
            rec["attenuation"] = 1.0
            rec["estimated_dose_mgy"] = None
            rec["calculation_error"] = str(e)

        return rec

    def _get_step_size_um(self) -> Tuple[float, float]:
        """Determine raster step size in microns (width, height).

        Fallback: metadata → bluice Redis (cell_w_um/cell_h_um) → config.

        Returns
        -------
        (step_w, step_h) : tuple of float
            Cell width and height in microns.  When only a single value is
            available, it is returned for both dimensions.
        """
        # Try metadata
        for key in ("raster_step_size_um", "step_size_um", "raster_step"):
            val = self.metadata.get(key)
            if val is not None:
                try:
                    v = float(val)
                    return (v, v)
                except (ValueError, TypeError):
                    pass

        # Try bluice Redis
        if self.redis_manager is not None:
            cell = self.redis_manager.get_raster_cell_size(self.run1_prefix)
            if cell:
                return cell

        # Config default
        v = float(self.config.get("step_size_um", 10.0))
        return (v, v)

    def _get_dozor_resolution_at_peak(self, peak: Dict) -> Optional[float]:
        """Read dozor 'Resol Visible' for the frame at the peak position.

        Checks both XY and XZ scan orientations and returns the best
        (lowest) resolution found.
        """
        coords = peak.get("coords", [0, 0, 0])
        frame_idx = int(round(coords[0]))  # x = frame index (1-based in Redis)
        frame_key = str(frame_idx + 1)

        best_res = None
        for projection in ("xy", "xz"):
            try:
                if projection == "xy":
                    scan_idx = int(round(coords[1]))
                    offset = getattr(self, "_xy_scan_offset", 0)
                    files = getattr(self, "_xy_files", self.run1_master_files)
                else:
                    scan_idx = int(round(coords[2]))
                    offset = getattr(self, "_xz_scan_offset", 0)
                    files = getattr(self, "_xz_files", self.run2_master_files)

                mapping = self._find_master_for_scan_idx(
                    scan_idx, offset, files, frame_idx
                )
                if not mapping:
                    continue

                master_file = next(iter(mapping))
                frame_1based = mapping[master_file][0]
                redis_key = self.source_cfg["redis_key_template"].format(
                    master_file=master_file
                )
                raw = self.redis_conn.hget(redis_key, str(frame_1based))
                if not raw:
                    continue

                import json
                frame_data = json.loads(raw)
                res = frame_data.get("Resol Visible")
                if res is not None:
                    res = float(res)
                    if res > 0 and (best_res is None or res < best_res):
                        best_res = res
            except Exception as e:
                logger.debug(f"Could not read dozor resolution ({projection}): {e}")

        return best_res

    # ------------------------------------------------------------------
    # Pipeline logging
    # ------------------------------------------------------------------

    def _log_step(self, stage: str, message: str) -> None:
        """Append a timestamped entry to the pipeline log file."""
        from datetime import datetime

        out_dir = Path(self.proc_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / "pipeline.log"
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(log_path, "a") as f:
                f.write(f"[{ts}] [{stage}] {message}\n")
        except Exception:
            pass  # never fail the pipeline due to logging

    # ------------------------------------------------------------------
    # Diagnostic heatmap images
    # ------------------------------------------------------------------

    def _get_peak_diffraction_frame(
        self,
        peak: Dict,
        projection: str,
    ) -> Optional[Tuple["np.ndarray", str, int, int]]:
        """Read the diffraction frame at the best peak's location.

        Parameters
        ----------
        peak : dict
            Peak dict with ``coords`` (x, y, z) where x=frame,
            y=XY scan line, z=XZ scan line.
        projection : str
            ``"xy"`` uses the XY scan files, ``"xz"`` uses XZ scan files.

        Returns
        -------
        ``(frame, master_file, frame_number_1based, bit_depth)`` or ``None``.
        """
        try:
            coords = peak.get("coords", [0, 0, 0])
            # coords = (x, y, z) where x=frame, y=XY_scan_line, z=XZ_scan_line
            frame_idx = int(round(coords[0]))  # x = frame index

            if projection == "xy":
                scan_idx = int(round(coords[1]))  # y = scan line in XY
                offset = getattr(self, "_xy_scan_offset", 0)
                files = getattr(self, "_xy_files", self.run1_master_files)
            else:
                scan_idx = int(round(coords[2]))  # z = scan line in XZ
                offset = getattr(self, "_xz_scan_offset", 0)
                files = getattr(self, "_xz_files", self.run2_master_files)

            mapping = self._find_master_for_scan_idx(
                scan_idx, offset, files, frame_idx
            )
            if not mapping:
                return None

            master_file = next(iter(mapping))
            frame_num_1based = mapping[master_file][0]

            from qp2.xio.hdf5_manager import HDF5Reader
            reader = HDF5Reader(master_file, start_timer=False)
            frame = reader.get_frame(frame_num_1based - 1)  # get_frame is 0-based
            bit_depth = getattr(reader, "bit_depth", 32)
            reader.close()
            return (frame, master_file, frame_num_1based, bit_depth)
        except Exception as e:
            logger.debug(f"Could not read diffraction frame: {e}")
            return None

    def _find_snapshot(self, omega: float) -> Optional[str]:
        """Find a sample snapshot JPEG near the given omega angle.

        Searches ``{data_dir}/screen/`` and ``{data_dir}/../screen/``
        for ``*_{angle}__HighRes.jpg`` files.  Also tries bluice
        ``processing_dir/screen/`` if available.

        Returns the path to the best matching snapshot, or ``None``.
        """
        import glob

        angle_int = int(round(omega))
        candidates = []

        # Build search directories
        search_dirs = []
        data_path = Path(self.data_dir)
        search_dirs.append(data_path / "screen")
        search_dirs.append(data_path.parent / "screen")
        search_dirs.append(Path(self.proc_dir) / "screen")

        if self.redis_manager is not None:
            try:
                from qp2.xio.bluice_params import get_processing_dir
                conn = self.redis_manager.get_bluice_connection()
                if conn:
                    proc_dir = get_processing_dir(conn)
                    if proc_dir:
                        search_dirs.append(Path(proc_dir) / "screen")
            except Exception:
                pass

        for d in search_dirs:
            if not d.is_dir():
                continue
            # Match exact angle
            for f in glob.glob(str(d / f"*_{angle_int}__HighRes.jpg")):
                candidates.append(f)
            # Also try angle ± 1 degree
            if not candidates:
                for f in glob.glob(str(d / f"*_{angle_int + 1}__HighRes.jpg")):
                    candidates.append(f)
                for f in glob.glob(str(d / f"*_{angle_int - 1}__HighRes.jpg")):
                    candidates.append(f)
            if candidates:
                break

        if not candidates:
            # Grab any HighRes snapshot as fallback
            for d in search_dirs:
                if not d.is_dir():
                    continue
                for f in glob.glob(str(d / "*__HighRes.jpg")):
                    candidates.append(f)
                if candidates:
                    break

        if candidates:
            # Pick the most recent
            candidates.sort(key=lambda f: Path(f).stat().st_mtime, reverse=True)
            logger.info(f"Found snapshot: {candidates[0]}")
            return candidates[0]

        return None

    def _get_grid_box_pixels(
        self,
        img_shape: Tuple[int, int],
        run_prefix: Optional[str] = None,
    ) -> Optional[Tuple[int, int, int, int]]:
        """Compute the raster grid bounding box in snapshot pixel coords.

        Uses ``act_bounds`` (microns) and camera calibration (mm/px).
        Assumes grid_ref maps to image center.

        Returns ``(x1, y1, x2, y2)`` in pixels, or ``None``.
        """
        if self.redis_manager is None:
            return None
        try:
            conn = self.redis_manager.get_bluice_connection()
            if conn is None:
                return None
            from qp2.xio.bluice_params import (
                get_raster_grid_params, get_camera_calibration,
            )
            grid = get_raster_grid_params(conn, run_prefix or self.run1_prefix)
            cal = get_camera_calibration(conn)
            if grid is None or cal is None:
                return None

            act_bounds = grid["act_bounds"]  # [x1,y1,x2,y2] in microns
            mm_per_px_h, mm_per_px_v = cal
            img_h, img_w = img_shape[:2]

            # Grid_ref ≈ image center
            cx, cy = img_w / 2.0, img_h / 2.0

            # Convert act_bounds from microns to pixels
            x1 = int(cx + act_bounds[0] / (mm_per_px_h * 1000))
            y1 = int(cy + act_bounds[1] / (mm_per_px_v * 1000))
            x2 = int(cx + act_bounds[2] / (mm_per_px_h * 1000))
            y2 = int(cy + act_bounds[3] / (mm_per_px_v * 1000))
            return (x1, y1, x2, y2)
        except Exception as e:
            logger.debug(f"Could not compute grid box: {e}")
            return None

    def _save_heatmap_image(
        self,
        matrix: "np.ndarray",
        peaks: List[Dict],
        title: str,
        filepath,
        projection: str = "xy",
        omega: float = 0.0,
    ) -> None:
        """Save a composite diagnostic image as PNG.

        Layout (1×3 when all data available):
            [Sample snapshot + grid box] [Heatmap + peaks] [Diffraction frame]

        Falls back to fewer panels when snapshot or diffraction is
        unavailable.

        Parameters
        ----------
        matrix : ndarray
            2D heatmap data (rows × cols).
        peaks : list of dict
            Peak dicts with ``coords`` [z, y, x] and ``dimensions``.
        title : str
            Plot title.
        filepath : path-like
            Output PNG path.
        projection : str
            ``"xy"`` or ``"xz"`` for peak coordinate projection.
        omega : float
            Omega angle for this scan (used to find matching snapshot).
        """
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            from matplotlib.patches import Rectangle
            from matplotlib.colors import LogNorm

            # --- Gather optional panels ---
            snapshot_img = None
            snapshot_path = self._find_snapshot(omega)
            if snapshot_path:
                try:
                    snapshot_img = plt.imread(snapshot_path)
                except Exception:
                    pass

            diffr_frame = None
            diffr_master = ""
            diffr_frame_num = 0
            diffr_bit_depth = 32
            if peaks:
                result = self._get_peak_diffraction_frame(
                    peaks[0], projection
                )
                if result is not None:
                    diffr_frame, diffr_master, diffr_frame_num, diffr_bit_depth = result

            # --- Determine layout ---
            panels = []
            if snapshot_img is not None:
                panels.append("snapshot")
            panels.append("heatmap")
            if diffr_frame is not None:
                panels.append("diffraction")

            n_panels = len(panels)
            fig, axes = plt.subplots(
                1, n_panels,
                figsize=(7 * n_panels, 7),
                squeeze=False,
            )
            axes = axes[0]
            ax_map = dict(zip(panels, axes))

            # --- Snapshot panel ---
            if "snapshot" in ax_map:
                ax = ax_map["snapshot"]
                ax.imshow(snapshot_img)
                ax.set_title(f"Sample (omega={omega:.0f}°)", fontsize=10)
                ax.axis("off")

                grid_run_prefix = (
                    getattr(self, "_xy_run_prefix", self.run1_prefix)
                    if projection == "xy"
                    else getattr(self, "_xz_run_prefix", self.run2_prefix)
                )
                grid_box = self._get_grid_box_pixels(
                    snapshot_img.shape,
                    run_prefix=grid_run_prefix,
                )
                if grid_box:
                    x1, y1, x2, y2 = grid_box
                    rect = Rectangle(
                        (x1, y1), x2 - x1, y2 - y1,
                        linewidth=2, edgecolor="cyan",
                        facecolor="none", linestyle="-",
                    )
                    ax.add_patch(rect)
                    ax.text(
                        x1, y1 - 5, "Raster grid",
                        color="cyan", fontsize=8,
                        verticalalignment="bottom",
                    )

            # --- Heatmap panel ---
            ax_heat = ax_map["heatmap"]
            masked = np.ma.masked_invalid(matrix)
            im = ax_heat.imshow(
                masked, cmap="hot", aspect="auto",
                interpolation="nearest", origin="upper",
            )
            fig.colorbar(im, ax=ax_heat, label=self.source_cfg.get("metric", "Score"))

            colors = plt.cm.tab10.colors
            for i, peak in enumerate(peaks):
                # coords = (x, y, z) where x=frame, y=XY_scan_line, z=XZ_scan_line
                coords = peak.get("coords", [0, 0, 0])
                dims = peak.get("dimensions", [1, 1, 1])
                color = colors[i % len(colors)]

                # Heatmap matrix is (scan_lines, frames)
                # imshow row = scan_line, col = frame
                if projection == "xy":
                    cx, cy = coords[0], coords[1]  # col=x(frame), row=y(scan)
                    dx, dy = dims[0], dims[1]
                else:  # xz
                    cx, cy = coords[0], coords[2]  # col=x(frame), row=z(scan)
                    dx, dy = dims[0], dims[2]

                ax_heat.plot(
                    cx, cy, "x", color=color, markersize=10,
                    markeredgewidth=2, label=f"Peak {i+1}",
                )
                rect = Rectangle(
                    (cx - dx / 2.0, cy - dy / 2.0), dx, dy,
                    linewidth=1.5, edgecolor=color,
                    facecolor="none", linestyle="--",
                )
                ax_heat.add_patch(rect)

            ax_heat.set_xlabel("Frame Index")
            ax_heat.set_ylabel("Scan Line")
            ax_heat.set_title(title)
            if peaks:
                ax_heat.legend(loc="upper right", fontsize=8)

            # --- Diffraction panel ---
            if "diffraction" in ax_map:
                ax = ax_map["diffraction"]
                frame = diffr_frame.astype(float)
                # Eiger mask value = 2^bit_depth - 1 (gap/dead pixels)
                mask_val = 2**diffr_bit_depth - 1
                eiger_mask = frame >= (mask_val - 1)
                frame[eiger_mask] = 0
                # Maximum filter to dilate spots so they survive downsampling
                from scipy.ndimage import maximum_filter
                frame = maximum_filter(frame, size=7)
                frame[eiger_mask] = np.nan
                # Compute contrast from real pixel values
                valid = frame[np.isfinite(frame) & (frame >= 0)]
                if valid.size > 0:
                    vmin = 0
                    vmax = max(np.percentile(valid, 99.9), 1)
                else:
                    vmin, vmax = 0, 1
                ax.imshow(
                    frame, cmap="gist_yarg", vmin=vmin, vmax=vmax,
                    interpolation="nearest", origin="upper",
                )
                # Draw resolution rings
                try:
                    bp = self._get_beam_params()
                    wavelength = bp.get("wavelength_A", 1.0)
                    from qp2.xio.hdf5_manager import HDF5Reader
                    _reader = HDF5Reader(
                        self.run1_master_files[0], start_timer=False
                    )
                    _params = _reader.get_parameters()
                    _reader.close()
                    det_dist = _params.get("det_dist", 300)
                    pixel_size = _params.get("pixel_size", 0.075)
                    beam_x = _params.get("beam_x", frame.shape[1] / 2)
                    beam_y = _params.get("beam_y", frame.shape[0] / 2)

                    import math
                    from matplotlib.patches import Circle
                    img_h, img_w = frame.shape[:2]
                    for res_A in [4.5, 3.0, 2.0, 1.5]:
                        theta = math.asin(wavelength / (2.0 * res_A))
                        radius_mm = det_dist * math.tan(2.0 * theta)
                        radius_px = radius_mm / pixel_size
                        circ = Circle(
                            (beam_x, beam_y), radius_px,
                            linewidth=0.8, edgecolor="red",
                            facecolor="none", linestyle="--",
                            alpha=0.7,
                        )
                        ax.add_patch(circ)
                        lx = beam_x + radius_px * 0.707
                        ly = beam_y - radius_px * 0.707
                        lx = min(lx, img_w - 30)
                        ly = max(ly, 15)
                        ax.text(
                            lx, ly, f"{res_A}A",
                            color="red", fontsize=7,
                            ha="center", va="center",
                            alpha=0.9,
                            bbox=dict(
                                facecolor="white", alpha=0.7,
                                edgecolor="none", pad=1,
                            ),
                            clip_on=True,
                        )
                except Exception as e:
                    logger.debug(f"Could not draw resolution rings: {e}")

                # Title with master file and frame number
                master_name = Path(diffr_master).name if diffr_master else "?"
                ax.set_title(
                    f"{master_name}\nframe {diffr_frame_num}",
                    fontsize=8,
                )
                ax.axis("off")

            # --- Summary text below the figure ---
            summary_lines = []
            if peaks:
                p = peaks[0]
                step_w, step_h = self._get_step_size_um()
                step_avg = (step_w + step_h) / 2.0
                dims_um = [max(1, int(round(d * step_avg)))
                           for d in p.get("dimensions", [])]
                summary_lines.append(
                    f"Peak 1: coords={p.get('coords')}, "
                    f"size={dims_um} um, "
                    f"intensity={p.get('integrated_intensity', 0):.0f}"
                )
            # Add strategy info if cached in results
            bp = self._get_beam_params()
            summary_lines.append(
                f"Energy={bp.get('energy_kev', '?')} keV, "
                f"Wavelength={bp.get('wavelength_A', '?'):.4f} A, "
                f"Beam={bp.get('beam_size_x_um', '?')}x"
                f"{bp.get('beam_size_y_um', '?')} um"
            )
            if summary_lines:
                fig.text(
                    0.5, -0.02,
                    "  |  ".join(summary_lines),
                    ha="center", va="top", fontsize=7,
                    color="gray",
                    fontfamily="monospace",
                )

            fig.savefig(str(filepath), dpi=150, bbox_inches="tight")
            plt.close(fig)
            logger.info(f"Saved heatmap image: {filepath}")
        except Exception as e:
            logger.debug(f"Could not save heatmap image: {e}")

    # ------------------------------------------------------------------
    # Voxel → motor position conversion (optional)
    # ------------------------------------------------------------------

    def _voxel_to_motor(
        self, voxel_coords: List[float], run_prefix: str
    ) -> Optional[Dict]:
        """Convert 3D voxel coordinates to motor positions.

        The 3D volume is reconstructed from two orthogonal scans:

        - **XY scan** (omega~0°): gives position in (sample_z, sample_y)
        - **XZ scan** (omega~90°): gives position in (sample_z, sample_x)

        Both scans share the x axis (frame index → sample_z, rotation axis).
        The y axis (XY scan lines) maps to sample_y at omega~0°.
        The z axis (XZ scan lines) maps to sample_x at omega~90°.

        This method combines both projections to compute the full 3D
        motor position, using each scan's grid geometry.

        Parameters
        ----------
        voxel_coords : list
            ``[x, y, z]`` where x=frame (rotation axis), y=XY scan line
            (vertical), z=XZ scan line (beam direction).
        run_prefix : str
            Run prefix for the XY scan (run1). The XZ scan uses run2.

        Returns
        -------
        dict with ``sample_x``, ``sample_y``, ``sample_z``, ``omega`` (mm/deg),
        or ``None`` if grid params unavailable.
        """
        if self.redis_manager is None:
            return None

        import math

        # --- XY scan grid (omega~0°): determines sample_y and sample_z ---
        grid_xy = self.redis_manager.get_raster_grid_params(self.run1_prefix)
        cell_xy = self.redis_manager.get_raster_cell_size(self.run1_prefix)

        # --- XZ scan grid (omega~90°): determines sample_x ---
        grid_xz = self.redis_manager.get_raster_grid_params(self.run2_prefix)
        cell_xz = self.redis_manager.get_raster_cell_size(self.run2_prefix)

        if grid_xy is None or cell_xy is None:
            return None

        vx = float(voxel_coords[0])  # x = frame index (rotation axis)
        vy = float(voxel_coords[1])  # y = XY scan line (vertical)
        vz = float(voxel_coords[2])  # z = XZ scan line (beam direction)

        # --- From XY scan: motor_y and motor_z ---
        grid_ref_xy = grid_xy["grid_ref"]      # [x, y, z, omega] mm/deg
        act_bounds_xy = grid_xy["act_bounds"]   # [x1, y1, x2, y2] microns
        cell_w_xy, cell_h_xy = cell_xy

        screen_x_mm = (act_bounds_xy[0] + vx * cell_w_xy + cell_w_xy / 2.0) / 1000.0
        screen_y_mm = (act_bounds_xy[1] + vy * cell_h_xy + cell_h_xy / 2.0) / 1000.0

        omega_xy_rad = math.radians(grid_ref_xy[3])
        motor_x_from_xy = grid_ref_xy[0] + math.sin(omega_xy_rad) * screen_y_mm
        motor_y = grid_ref_xy[1] + math.cos(omega_xy_rad) * screen_y_mm
        motor_z = grid_ref_xy[2] + screen_x_mm

        # --- From XZ scan: motor_x (beam direction) ---
        if grid_xz is not None and cell_xz is not None:
            grid_ref_xz = grid_xz["grid_ref"]
            act_bounds_xz = grid_xz["act_bounds"]
            cell_w_xz, cell_h_xz = cell_xz

            screen_y_xz_mm = (
                act_bounds_xz[1] + vz * cell_h_xz + cell_h_xz / 2.0
            ) / 1000.0

            omega_xz_rad = math.radians(grid_ref_xz[3])
            # At omega~90°: sin(90)=1, cos(90)=0
            # motor_x gets the full screen_y offset
            motor_x = (grid_ref_xz[0]
                        + math.sin(omega_xz_rad) * screen_y_xz_mm)
        else:
            # Fallback: use XY-only estimate (z not accounted for)
            motor_x = motor_x_from_xy
            logger.debug("XZ grid params not available — motor_x approximate")

        return {
            "sample_x": round(motor_x, 4),
            "sample_y": round(motor_y, 4),
            "sample_z": round(motor_z, 4),
            "omega": round(grid_ref_xy[3], 3),
        }

    # ------------------------------------------------------------------
    # Results storage
    # ------------------------------------------------------------------

    def _store_results(self, results: Dict) -> None:
        """Store combined results to JSON file and Redis."""
        out_dir = Path(self.proc_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "results.json"

        if self.config.get("compact_results", True):
            output = self._build_compact_results(results)
        else:
            output = {
                "run1_prefix": self.run1_prefix,
                "run2_prefix": self.run2_prefix,
                "canonical_run_prefix": self.run1_prefix,
                "partner_run_prefix": self.run2_prefix,
                "run_prefixes": [self.run1_prefix, self.run2_prefix],
                "timestamp": time.time(),
                "config": {
                    "analysis_source": self.config.get("analysis_source", "dozor"),
                    "metric": self.source_cfg["metric"],
                    "shift": self.config.get("shift", 0.0),
                },
                "stages": results,
            }

        try:
            with open(out_file, "w") as f:
                json.dump(output, f, indent=2, default=_json_default)
            logger.info(f"3D raster results written to {out_file}")
        except Exception as e:
            logger.error(f"Failed to write results JSON: {e}")

        # Redis hash — structured so downstream programs can access
        # individual fields without parsing the full blob
        try:
            redis_key = f"analysis:out:raster_3d:{self.run1_prefix}"
            alias_key = f"analysis:out:raster_3d:index:{self.run2_prefix}"
            peaks_stage = results.get("peaks", {})
            peak_records = peaks_stage.get("data", []) if isinstance(peaks_stage, dict) else []
            recommendations = results.get("recommendations", [])

            fields = {
                "data": json.dumps(output, default=_json_default),
                "run1_prefix": self.run1_prefix,
                "run2_prefix": self.run2_prefix,
                "canonical_run_prefix": self.run1_prefix,
                "partner_run_prefix": self.run2_prefix,
                "run_prefixes": json.dumps([self.run1_prefix, self.run2_prefix]),
                "n_peaks": str(len(peak_records)),
                "n_recommendations": str(len(recommendations)),
                "recommendations": json.dumps(
                    recommendations, default=_json_default
                ),
            }

            # Best recommendation as top-level fields for easy access
            best_rec = results.get("recommendation", {})
            if best_rec:
                cp = best_rec.get("crystal_position") or {}
                for k in [
                    "start_angle", "end_angle", "osc_width", "n_images",
                    "exposure_time_s", "attenuation", "detector_distance_mm",
                    "target_dose_mgy", "estimated_dose_mgy", "lifetime_s",
                    "resolution_A", "space_group", "strategy_source",
                    "total_collection_time_s", "energy_kev",
                ]:
                    v = best_rec.get(k)
                    if v is not None:
                        fields[k] = str(v)
                if cp.get("dimensions_um"):
                    fields["crystal_size_um"] = json.dumps(cp["dimensions_um"])
                if cp.get("coords_voxel"):
                    fields["crystal_coords"] = json.dumps(cp["coords_voxel"])
                if best_rec.get("beam_size_um"):
                    fields["beam_size_um"] = json.dumps(best_rec["beam_size_um"])

            if self.pipeline_params:
                for k, v in self.pipeline_params.items():
                    if v is not None:
                        fields[str(k)] = str(v)

            self.redis_conn.hset(redis_key, mapping=fields)
            self.redis_conn.expire(redis_key, 7 * 24 * 3600)

            alias_fields = {
                "canonical_key": redis_key,
                "canonical_run_prefix": self.run1_prefix,
                "partner_run_prefix": self.run2_prefix,
            }
            self.redis_conn.hset(alias_key, mapping=alias_fields)
            self.redis_conn.expire(alias_key, 7 * 24 * 3600)
        except Exception as e:
            logger.error(f"Failed to store results in Redis: {e}")

    def _build_compact_results(self, results: Dict) -> Dict:
        """Build a compact results dict with only collection and crystal params."""
        _COLLECTION_KEYS = [
            "start_angle", "end_angle", "total_rotation", "osc_width",
            "n_images", "exposure_time_s", "attenuation",
            "detector_distance_mm", "energy_kev", "wavelength_A",
            "beam_size_um", "flux",
            "target_dose_mgy", "estimated_dose_mgy", "dose_source",
            "total_collection_time_s", "translation_x_um",
        ]
        _CRYSTAL_KEYS = [
            "space_group", "lattice", "unit_cell", "mosaicity",
            "resolution_A", "resolution_source",
            "resolution_strategy_A", "resolution_dozor_A",
            "resolution_at_edge", "edge_resolution_A",
            "completeness", "screen_score", "strategy_source",
        ]
        _DOSE_KEYS = [
            "raddose3d_avg_dwd_mgy", "raddose3d_max_dose_mgy",
        ]

        compact_recs = []
        for rec in results.get("recommendations", []):
            cp = rec.get("crystal_position", {})
            compact_rec = {
                "crystal_position": {
                    "peak_voxel": cp.get("coords_voxel"),
                    "dimensions_um": cp.get("dimensions_um"),
                    "motor_position": cp.get("motor_position"),
                    "collection_mode": cp.get("collection_mode"),
                },
                "collection": {k: rec[k] for k in _COLLECTION_KEYS if k in rec},
                "crystal": {k: rec[k] for k in _CRYSTAL_KEYS if k in rec},
                "dose": {k: rec[k] for k in _DOSE_KEYS if k in rec},
            }
            if rec.get("alternatives"):
                compact_rec["alternatives"] = rec["alternatives"]
            compact_recs.append(compact_rec)

        peaks_stage = results.get("peaks", {})
        peak_records = peaks_stage.get("data", []) if isinstance(peaks_stage, dict) else []

        output = {
            "run1_prefix": self.run1_prefix,
            "run2_prefix": self.run2_prefix,
            "canonical_run_prefix": self.run1_prefix,
            "partner_run_prefix": self.run2_prefix,
            "run_prefixes": [self.run1_prefix, self.run2_prefix],
            "timestamp": time.time(),
            "status": results.get("status", "completed"),
            "n_peaks": len(peak_records),
            "n_recommendations": len(compact_recs),
            "recommendations": compact_recs,
        }

        if results.get("error"):
            output["error"] = results["error"]

        return output


def _json_default(obj):
    """JSON serializer for numpy types."""
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
