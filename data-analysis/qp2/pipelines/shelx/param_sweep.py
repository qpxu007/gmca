import json
import time
from abc import ABC, abstractmethod
from itertools import product
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from qp2.log.logging_config import get_logger

# Assuming logger is configured elsewhere or configure it here
logger = get_logger(__name__)


class ParameterSweepJob:
    # (Keep the ParameterSweepJob class as defined previously)
    """Represents a single job within the parameter sweep."""

    def __init__(self, job_id: str, params: Dict[str, Any], output_dir: Path):
        self.id = job_id
        self.params = params
        self.output_dir = output_dir
        self.status: str = "initialized"
        self.result: Optional[Dict[str, Any]] = None
        self.context: Any = None
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.error_message: Optional[str] = None

    def update_status(self, status: str, message: Optional[str] = None):
        # Add timestamp to status updates for clarity?
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        self.status = status
        log_prefix = f"Job {self.id} [{timestamp}] status -> {status}"
        if "fail" in status.lower() or "error" in status.lower():
            self.error_message = message if message else "An unspecified error occurred"
            logger.error(f"{log_prefix}: {self.error_message}")
        else:
            logger.info(f"{log_prefix}")
            if message:
                logger.info(f"  Message: {message}")

    def set_result(self, result_data: Dict[str, Any]):
        # Merges base info with specific result data
        self.result = {
            "job_id": self.id,
            "params": self.params,
            "status": self.status,  # Record final status in result
            "output_dir": str(self.output_dir),
            **result_data,
        }
        if self.error_message:
            self.result["error"] = self.error_message
        if self.start_time:
            self.result["start_time_unix"] = self.start_time
        if self.end_time:
            self.result["end_time_unix"] = self.end_time
            if self.start_time:
                self.result["duration_seconds"] = self.end_time - self.start_time


class ParameterSweepBase(ABC):
    """
    Abstract base class for running parameter sweeps.

    Handles parameter combination generation, job management, monitoring loop,
    timeout, result collection, and stopping after N successful jobs.
    Subclasses must implement the methods specific to the type of job being run.
    """

    DEFAULT_TIMEOUT = 3600.0  # 1 hour
    DEFAULT_MONITOR_INTERVAL = 15  # seconds

    def __init__(
            self,
            param_space: Dict[str, List[Any]],
            base_output_dir: Path,
            base_job_prefix: str = "job",
            timeout: float = DEFAULT_TIMEOUT,
            monitor_interval: int = DEFAULT_MONITOR_INTERVAL,
            stop_after_n_successes: Optional[
                int
            ] = None,  # Stop after N successes (None or 0 means run all)
            success_metric: Optional[
                str
            ] = None,  # Key in result dict to evaluate success/best
            higher_is_better: bool = True,  # For tracking the best result
    ):
        """
        Initializes the parameter sweep manager.

        Args:
            param_space: Dictionary where keys are parameter names and values
                         are lists of values to test for that parameter.
            base_output_dir: The root directory where subdirectories for each
                             job combination will be created.
            base_job_prefix: Prefix for job IDs and directories.
            timeout: Maximum total time (seconds) allowed for monitoring tasks.
            monitor_interval: Time (seconds) to wait between monitoring checks.
            stop_after_n_successes: If set to an integer N > 0, the sweep will
                                    terminate remaining jobs after N jobs have met
                                    the success criteria. If None or 0, all jobs
                                    will run their course (subject to timeout).
            success_metric: The key in the job's result dictionary used to evaluate
                            success and track the best result. Often required if
                            stop_after_n_successes is used.
            higher_is_better: If True, higher values of success_metric are better.
        """
        if not param_space:
            raise ValueError("Parameter space cannot be empty.")
        if (
                stop_after_n_successes is not None
                and stop_after_n_successes > 0
                and not success_metric
        ):
            # Success metric is needed to determine *what* constitutes a success
            raise ValueError(
                "success_metric must be provided if stop_after_n_successes is > 0."
            )
        if stop_after_n_successes is not None and stop_after_n_successes < 1:
            logger.warning(
                f"stop_after_n_successes is {stop_after_n_successes}, disabling N-th success stop condition."
            )
            stop_after_n_successes = None  # Treat <= 0 as disabled

        self.param_space = param_space
        self.base_output_dir = Path(base_output_dir)
        self.base_job_prefix = base_job_prefix
        self.timeout = timeout
        self.monitor_interval = monitor_interval
        self.stop_after_n_successes = stop_after_n_successes
        self.success_metric = success_metric
        self.higher_is_better = higher_is_better

        self.jobs: List[ParameterSweepJob] = []
        self.sweep_results: List[Dict[str, Any]] = []
        self.best_result_so_far: Optional[Dict[str, Any]] = None
        self.best_metric_value: Optional[float] = None
        self._success_count = 0  # Counter for successful jobs

    # _generate_job_id remains the same
    def _generate_job_id(self, params: Dict[str, Any]) -> str:
        """Generates a unique, filesystem-safe ID for a job based on its parameters."""
        param_str = "_".join(
            f"{k}_{str(v).replace(' ', '_').replace('/', '_')}"
            for k, v in sorted(params.items())
        )
        return f"{self.base_job_prefix}_{param_str}"

    # _prepare_jobs remains the same
    def _prepare_jobs(self) -> None:
        """Generates ParameterSweepJob objects for all parameter combinations."""
        self.jobs = []
        param_names = list(self.param_space.keys())
        param_values = list(self.param_space.values())

        for combo_values in product(*param_values):
            params = dict(zip(param_names, combo_values))
            job_id = self._generate_job_id(params)
            output_dir = self.base_output_dir / job_id
            job = ParameterSweepJob(job_id, params, output_dir)
            self.jobs.append(job)
            logger.debug(f"Prepared job: {job_id} with params: {params}")
        logger.info(f"Prepared {len(self.jobs)} jobs for the sweep.")

    def run_sweep(self) -> List[Dict[str, Any]]:
        """Executes the entire parameter sweep."""
        logger.info("Starting parameter sweep...")
        self.base_output_dir.mkdir(parents=True, exist_ok=True)

        # Reset counters and results for this run
        self._success_count = 0
        self.best_result_so_far = None
        self.best_metric_value = None
        self.sweep_results = []
        self.jobs = []  # Reset job list

        self._prepare_jobs()  # Prepare jobs for this run

        # --- 1. Setup Phase ---
        logger.info("--- Running Setup Phase ---")
        setup_failed_count = 0
        for job in self.jobs:
            # Basic check if dir exists, skip if setup failed previously (though unlikely with reset)
            if job.status == "initialized":
                try:
                    logger.info(f"Setting up job {job.id}...")
                    job.output_dir.mkdir(parents=True, exist_ok=True)
                    job.context = self.setup_job(job)  # Subclass implements this
                    job.update_status("setup_complete")
                    # Optionally run an immediate setup task (like SHELXC)
                    try:
                        self.run_job_setup_task(job)
                        job.update_status("setup_task_complete")
                    except NotImplementedError:
                        logger.debug(f"No setup task implemented for job {job.id}")
                    except Exception as setup_task_e:
                        job.update_status("setup_task_failed", str(setup_task_e))
                        setup_failed_count += 1

                except Exception as setup_e:
                    job.update_status("setup_failed", str(setup_e))
                    setup_failed_count += 1
                    job.set_result({"error": job.error_message})
                    self._update_sweep_results(job)  # Add error result immediately

        if setup_failed_count > 0:
            logger.warning(f"{setup_failed_count} jobs failed during setup phase.")

        # --- 2. Main Task Execution Phase ---
        logger.info("--- Running Main Task Phase ---")
        jobs_to_run_main = [
            j
            for j in self.jobs
            if j.status in ["setup_complete", "setup_task_complete"]
        ]
        submitted_main_count = 0
        main_failed_immediately = 0
        for job in jobs_to_run_main:
            try:
                logger.info(f"Starting main task for job {job.id}...")
                self.run_job_main_task(
                    job
                )  # Subclass implements this (can be background)
                # Assuming success if no immediate exception
                job.update_status("main_task_running")  # Assume it's running/submitted
                job.start_time = time.time()  # Record start time for this phase
                submitted_main_count += 1
            except NotImplementedError:
                logger.warning(f"Main task not implemented, skipping for job {job.id}")
                job.update_status("main_task_skipped")
            except Exception as main_e:
                job.update_status("main_task_failed", str(main_e))
                main_failed_immediately += 1
                job.set_result({"error": job.error_message})
                self._update_sweep_results(job)  # Add error result immediately
        logger.info(
            f"Main task summary: Submitted/Running: {submitted_main_count}, Failed Immediately: {main_failed_immediately}, Skipped/SetupFailed: {len(self.jobs) - submitted_main_count - main_failed_immediately}"
        )

        # --- 3. Monitoring Phase ---
        logger.info("--- Running Monitoring Phase ---")
        # Filter again based on potentially updated status
        active_jobs = [j for j in self.jobs if j.status == "main_task_running"]
        if active_jobs:
            self._monitor_active_jobs(active_jobs)
        else:
            logger.info("No active jobs to monitor.")

        # --- 4. Post-processing Phase ---
        logger.info("--- Running Post-processing Phase ---")

        # Only run post-processing on jobs that *successfully* completed the main task
        jobs_for_post = [j for j in self.jobs if j.status == "completed"]
        post_processed_count = 0
        post_failed_count = 0
        if jobs_for_post:
            logger.info(
                f"Found {len(jobs_for_post)} jobs eligible for post-processing."
            )
            for job in jobs_for_post:
                try:
                    logger.info(f"Running post-processing for job {job.id}...")
                    self.run_job_post_task(job)  # Subclass implements this
                    job.update_status("post_processing_complete")
                    post_processed_count += 1
                except NotImplementedError:
                    logger.debug(
                        f"No post-processing task implemented for job {job.id}"
                    )
                    # Decide on status - maybe 'post_processing_skipped'?
                    job.update_status("post_processing_skipped")
                except Exception as post_e:
                    job.update_status("post_processing_failed", str(post_e))
                    post_failed_count += 1
                    # Update result if it exists, otherwise create one
                    if job.result:
                        job.result["post_processing_error"] = str(post_e)
                    else:  # Should not happen if status was 'completed', but safety check
                        job.set_result({"post_processing_error": str(post_e)})

                # Ensure result is added/updated in sweep_results after post-processing attempt
                self._update_sweep_results(job)
            logger.info(
                f"Post-processing summary: Completed/Skipped: {post_processed_count + len(jobs_for_post) - post_failed_count}, Failed: {post_failed_count}"
            )
        else:
            logger.info(
                "No jobs successfully completed the main task, skipping post-processing."
            )

        # --- 5. Final Cleanup (Optional) ---
        logger.info("--- Running Cleanup Phase ---")
        cleanup_count = 0
        cleanup_failed = 0
        for job in self.jobs:
            try:
                self.cleanup_job(job)
                cleanup_count += 1
            except NotImplementedError:
                pass  # Cleanup is optional
            except Exception as clean_e:
                logger.warning(f"Cleanup failed for job {job.id}: {clean_e}")
                cleanup_failed += 1
        logger.info(
            f"Cleanup summary: Attempted on {cleanup_count} jobs, Failed on {cleanup_failed} jobs."
        )

        logger.info("Parameter sweep finished.")
        # Ensure all job results (even if just status/error) are in the final list
        final_results = []
        for job in self.jobs:
            if not job.result:
                # Create a basic result dict if none was ever set
                job.set_result({})
            final_results.append(job.result)

        self.sweep_results = final_results
        return self.sweep_results

    def _monitor_active_jobs(self, active_jobs: List[ParameterSweepJob]):
        """Internal loop to monitor running jobs."""
        start_time = time.time()
        monitoring_active = True

        while monitoring_active:
            current_time = time.time()
            elapsed_time = current_time - start_time

            # --- Termination Condition Checks ---
            # 1. No active jobs left
            if not active_jobs:
                logger.info("Monitoring complete: No more active jobs.")
                monitoring_active = False
                break

            # 2. Timeout reached
            if elapsed_time > self.timeout:
                logger.warning(f"Monitoring timeout ({self.timeout}s) reached.")
                self._terminate_running_jobs(
                    active_jobs,
                    reason="main_task_timed_out",
                    message=f"Exceeded sweep timeout of {self.timeout}s",
                )
                monitoring_active = False
                break

            # 3. N-th success reached

            if (
                    self.stop_after_n_successes is not None
                    and self._success_count >= self.stop_after_n_successes
            ):
                logger.info(
                    f"Reached target of {self.stop_after_n_successes} successful jobs. Terminating remaining jobs."
                )
                self._terminate_running_jobs(
                    active_jobs,
                    reason="terminated_early_n_success",
                    message=f"Stopped after reaching {self._success_count}/{self.stop_after_n_successes} successes.",
                )
                monitoring_active = False
                break

            # --- Monitoring Cycle ---
            logger.info(
                f"Monitoring check: {len(active_jobs)} active jobs. Elapsed: {elapsed_time:.1f}s / {self.timeout}s. Successes: {self._success_count}/{self.stop_after_n_successes or 'inf'}"
            )

            jobs_finished_this_cycle = []
            for job in active_jobs:
                try:
                    progress_status, progress_data = self.check_job_progress(
                        job
                    )  # Subclass implements

                    if progress_status == "completed":
                        logger.info(f"Job {job.id} completed main task.")
                        job.update_status("completed")
                        job.end_time = time.time()
                        job.set_result(progress_data if progress_data else {})
                        self._evaluate_result(
                            job
                        )  # Check success, update counter, track best
                        jobs_finished_this_cycle.append(job)
                        self._save_individual_result(job)  # Save intermediate
                    elif progress_status == "failed":
                        logger.error(f"Job {job.id} failed during main task.")
                        # Use error from progress_data if available
                        error_msg = "Failure detected during monitoring"
                        if (
                                progress_data
                                and isinstance(progress_data, dict)
                                and "error" in progress_data
                        ):
                            error_msg = progress_data["error"]
                        job.update_status("main_task_failed", error_msg)
                        job.end_time = time.time()
                        # Store progress_data even if failed, might contain info
                        job.set_result(progress_data if progress_data else {})
                        jobs_finished_this_cycle.append(job)
                        self._save_individual_result(job)

                    elif progress_status == "running":
                        # Optional: Log intermediate progress if progress_data available
                        if progress_data and isinstance(progress_data, dict):
                            logger.debug(f"Job {job.id} progress: {progress_data}")
                        job.set_result(progress_data if progress_data else {})
                        self._evaluate_result(job)
                        self._save_individual_result(job)  # Save intermediate

                except Exception as check_e:
                    logger.exception(
                        f"Critical error checking progress for job {job.id}: {check_e}. Marking as failed."
                    )
                    # Treat unexpected check errors as job failure
                    job.update_status(
                        "monitor_error",
                        f"Exception during check_job_progress: {str(check_e)}",
                    )
                    job.end_time = time.time()
                    job.set_result({"error": job.error_message})  # Add error result
                    jobs_finished_this_cycle.append(job)
                    self._save_individual_result(job)

            # --- Update active jobs list ---
            if jobs_finished_this_cycle:
                active_jobs = [
                    j for j in active_jobs if j not in jobs_finished_this_cycle
                ]
                # Update main results list (redundant with final collection, but good for live state)
                # for job in jobs_finished_this_cycle:
                #      self._update_sweep_results(job)

            # --- Wait before next check cycle ---
            if (
                    monitoring_active and active_jobs
            ):  # Don't sleep if loop is about to exit
                time.sleep(self.monitor_interval)

    def _evaluate_result(self, job: ParameterSweepJob):
        """Checks if a completed job meets success criteria, updates success count, and updates best result."""
        if not job.result:
            return  # Should not happen if called after completion

        # --- Check for Success ---
        is_successful = False
        if self.success_metric:  # Only check success if metric is defined
            if self.success_metric not in job.result:
                logger.warning(
                    f"Success metric '{self.success_metric}' not found in result for job {job.id}. Cannot evaluate success."
                )
            else:
                try:
                    # Attempt to use subclass method first for complex criteria
                    is_successful = self.check_success_criteria(job.result)
                    if is_successful:
                        job.result["sweep_success"] = True  # Mark in result dict
                        self._success_count += (
                            1  # Increment counter *only if successful*
                        )
                        # force a complete status for the corresponding job
                        if job.status != "completed":
                            job.update_status("completed")
                        logger.info(
                            f"*** Job {job.id} meets success criteria! Success count: {self._success_count}/{self.stop_after_n_successes or 'inf'} ***"
                        )

                except Exception as e:
                    logger.error(
                        f"Error calling check_success_criteria for job {job.id}: {e}"
                    )

        # --- Update Best Result Tracking ---
        # Requires success_metric to be present and convertible to float
        if self.success_metric and self.success_metric in job.result:
            try:
                metric_value = float(job.result[self.success_metric])
                is_new_best = False
                if self.best_metric_value is None:
                    is_new_best = True
                elif self.higher_is_better and metric_value > self.best_metric_value:
                    is_new_best = True
                elif (
                        not self.higher_is_better and metric_value < self.best_metric_value
                ):
                    is_new_best = True

                if is_new_best:
                    logger.info(
                        f"*** New best result from Job {job.id}! Metric ({self.success_metric}): {metric_value:.3f} (Previous best: {self.best_metric_value}) ***"
                    )
                    self.best_metric_value = metric_value
                    self.best_result_so_far = job.result  # Store the whole result dict
            except (TypeError, ValueError):
                logger.warning(
                    f"Could not convert success metric '{self.success_metric}' value ({job.result[self.success_metric]}) to float for job {job.id} for best result tracking."
                )

    def _terminate_running_jobs(
            self,
            jobs_to_terminate: List[ParameterSweepJob],
            reason: str,
            message: Optional[str] = None,
    ) -> None:
        """Signals termination for a list of jobs and updates their status."""
        if not jobs_to_terminate:
            return
        logger.info(f"Terminating {len(jobs_to_terminate)} job(s) due to: {reason}")
        for job in jobs_to_terminate:
            if job.status == "main_task_running":
                # Only terminate jobs supposedly running
                job.update_status(reason, message)  # Update status first
                try:
                    self.terminate_job(job)  # Call subclass implementation
                except Exception as e:
                    logger.error(f"Failed to signal termination for job {job.id}: {e}")
                    # Optionally update status again? e.g., 'termination_failed'
                finally:
                    # Ensure terminated jobs are added to results even if termination fails
                    if not job.result:
                        job.set_result({})
                    self._update_sweep_results(job)

    # _update_sweep_results remains the same
    def _update_sweep_results(self, job: ParameterSweepJob):
        """Adds or updates a job's result in the main sweep_results list."""
        if not job.result:
            # Ensure even jobs without specific data have a basic entry if they finish/fail
            job.set_result({})

        # Find existing result index
        existing_index = -1
        for i, r in enumerate(self.sweep_results):
            if r.get("job_id") == job.id:
                existing_index = i
                break

        if existing_index != -1:
            self.sweep_results[existing_index] = job.result  # Update in place
        else:
            self.sweep_results.append(job.result)  # Add new

    # _save_individual_result remains the same
    def _save_individual_result(self, job: ParameterSweepJob) -> None:
        """Saves the current result dictionary for a job to its output directory."""
        if job.result:
            result_file = job.output_dir / "result.json"
            try:
                with open(result_file, "w") as f:
                    # Custom encoder might be needed for non-serializable types in params/result
                    json.dump(job.result, f, indent=2, default=str)
                logger.debug(f"Saved result JSON for job {job.id} to {result_file}")
            except Exception as e:
                logger.error(
                    f"Failed to save result JSON for job {job.id} to {result_file}: {e}"
                )

    # save_summary remains the same
    def save_summary(self, output_file: Path) -> None:
        """Save summary of all final results to a JSON file."""
        logger.info(
            f"Saving summary of {len(self.sweep_results)} results to {output_file}"
        )
        try:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            # Sort results for consistency (e.g., by job ID)
            sorted_results = sorted(
                self.sweep_results, key=lambda r: r.get("job_id", "")
            )
            with open(output_file, "w") as f:
                json.dump(
                    sorted_results, f, indent=2, default=str
                )  # Use default=str for safety
            logger.info(f"Summary saved successfully to {output_file}")
        except Exception as e:
            logger.error(f"Failed to write summary file {output_file}: {e}")

    # --- Abstract Methods (signatures remain the same) ---
    @abstractmethod
    def setup_job(self, job: ParameterSweepJob) -> Any:
        pass

    @abstractmethod
    def run_job_main_task(self, job: ParameterSweepJob) -> None:
        pass

    @abstractmethod
    def check_job_progress(
            self, job: ParameterSweepJob
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        pass

    @abstractmethod
    def terminate_job(self, job: ParameterSweepJob) -> None:
        pass

    @abstractmethod
    def check_success_criteria(self, result: Dict[str, Any]) -> bool:
        pass

    # --- Optional Abstract Methods (signatures remain the same) ---
    def run_job_setup_task(self, job: ParameterSweepJob) -> None:
        raise NotImplementedError

    def run_job_post_task(self, job: ParameterSweepJob) -> None:
        raise NotImplementedError

    def cleanup_job(self, job: ParameterSweepJob) -> None:
        pass


"""
    # Example: Stop after the first success (N=1)
sweep_manager = ShelXSweep(
    # ... other ShelXSweep specific args ...
    stop_after_n_successes=1, # Stop after 1 successful job
    # ... other ParameterSweepBase args like param_space, base_output_dir ...
)

# Example: Stop after 5 successes (N=5)
sweep_manager = ShelXSweep(
    # ... other ShelXSweep specific args ...
    stop_after_n_successes=5, # Stop after 5 successful jobs
    # ... other ParameterSweepBase args ...
)

# Example: Run all jobs (don't stop based on success count)
sweep_manager = ShelXSweep(
    # ... other ShelXSweep specific args ...
    stop_after_n_successes=None, # Or 0, or omit the parameter
    # ... other ParameterSweepBase args ...
)

# Run the sweep
final_results = sweep_manager.run_sweep()
sweep_manager.save_summary(summary_file)

"""
