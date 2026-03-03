import json
import time
from abc import ABC, abstractmethod
from itertools import product
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

# Configure logging
logger = get_logger(__name__)

class ParameterSweepJob:
    """Represents a single job within the parameter sweep."""

    def __init__(self, job_id: str, params: Dict[str, Any], output_dir: Path):
        """Initializes a job with its ID, parameters, and output directory."""
        self.id = job_id
        self.params = params
        self.output_dir = output_dir
        self.status: str = "initialized"  # e.g., initialized, setup_complete, setup_failed, main_task_running, completed, main_task_failed, post_processing_complete, etc.
        self.result: Optional[Dict[str, Any]] = None
        self.context: Any = None  # To store job-specific objects, e.g., a processor instance
        self.start_time: Optional[float] = None # Timestamp for main task start
        self.end_time: Optional[float] = None   # Timestamp for main task end
        self.error_message: Optional[str] = None

    def update_status(self, status: str, message: Optional[str] = None) -> None:
        """Updates job status with a timestamped log message."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        self.status = status
        log_prefix = f"Job {self.id} [{timestamp}] status -> {status}"
        if "fail" in status.lower() or "error" in status.lower():
            self.error_message = message if message else "An unspecified error occurred"
            logger.error(f"{log_prefix}: {self.error_message}")
        else:
            logger.info(f"{log_prefix}")
        if message and not ("fail" in status.lower() or "error" in status.lower()): # Avoid double logging error
            logger.info(f" Message: {message}")


    def set_result(self, result_data: Dict[str, Any]) -> None:
        """Merges base job info with specific result data. Ensures essential fields are present."""
        base_info = {
            "job_id": self.id,
            "params": self.params,
            "status": self.status,
            "output_dir": str(self.output_dir),
        }
        self.result = {**base_info, **result_data} # result_data can overwrite base_info if keys clash

        if self.error_message and "error" not in self.result: # Don't overwrite if already set by result_data
            self.result["error"] = self.error_message
        if self.start_time and "start_time_unix" not in self.result:
            self.result["start_time_unix"] = self.start_time
        if self.end_time and "end_time_unix" not in self.result:
            self.result["end_time_unix"] = self.end_time
        if self.start_time and self.end_time and "duration_seconds" not in self.result:
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
        stop_after_n_successes: Optional[int] = None, # Stop after N successes (None or 0 means run all)
        success_metric: Optional[str] = None, # Key in result dict to evaluate success/best
        higher_is_better: bool = True, # For tracking the best result
    ):
        if not param_space:
            raise ValueError("Parameter space cannot be empty.")

        if stop_after_n_successes is not None and stop_after_n_successes > 0 and not success_metric:
            raise ValueError("success_metric must be provided if stop_after_n_successes is > 0.")

        if stop_after_n_successes is not None and stop_after_n_successes < 1:
            logger.warning(
                f"stop_after_n_successes is {stop_after_n_successes}, disabling N-th success stop condition."
            )
            stop_after_n_successes = None # Treat <= 0 as disabled

        self.param_space = param_space
        self.base_output_dir = Path(base_output_dir)
        self.base_job_prefix = base_job_prefix
        self.timeout = timeout
        self.monitor_interval = monitor_interval
        self.stop_after_n_successes = stop_after_n_successes
        self.success_metric = success_metric
        self.higher_is_better = higher_is_better

        self.jobs: List[ParameterSweepJob] = []
        self.sweep_results: List[Dict[str, Any]] = [] # Stores final job.result dicts
        self.best_result_so_far: Optional[Dict[str, Any]] = None
        self.best_metric_value: Optional[float] = None
        self._success_count = 0 # Counter for successful jobs

    def _generate_job_id(self, params: Dict[str, Any]) -> str:
        """Generates a unique, filesystem-safe ID for a job based on its parameters."""
        # Sort items for consistent ID generation
        param_str = "_".join(
            f"{k}_{str(v).replace(' ', '_').replace('/', '_').replace('.', 'p')}" # Sanitize common problematic chars
            for k, v in sorted(params.items())
        )
        return f"{self.base_job_prefix}_{param_str}"

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
        """Executes the entire parameter sweep, managing all phases."""
        logger.info("Starting parameter sweep...")
        self.base_output_dir.mkdir(parents=True, exist_ok=True)

        # Reset state for this run
        self._success_count = 0
        self.best_result_so_far = None
        self.best_metric_value = None
        self.sweep_results = []
        self.jobs = [] # Clear previous job list if any
        self._prepare_jobs() # Generate job objects for this sweep

        # --- Phase 1: Setup ---
        logger.info("--- Running Setup Phase ---")
        for job in self.jobs:
            if job.status == "initialized": # Only attempt setup once
                try:
                    job.output_dir.mkdir(parents=True, exist_ok=True)
                    job.context = self.setup_job(job) # Abstract method
                    job.update_status("setup_complete")
                    try:
                        self.run_job_setup_task(job) # Optional abstract method
                        job.update_status("setup_task_complete")
                    except NotImplementedError:
                        logger.debug(f"No setup task implemented for job {job.id}.")
                    except Exception as setup_task_e:
                        job.update_status("setup_task_failed", str(setup_task_e))
                except Exception as setup_e:
                    job.update_status("setup_failed", str(setup_e))
                job.set_result(job.result or {}) # Ensure result dict exists, even if only with error
                self._update_sweep_results(job) # Persist result

        # --- Phase 2: Main Task Execution ---
        logger.info("--- Running Main Task Phase ---")
        jobs_to_run_main_task = [j for j in self.jobs if j.status in ["setup_complete", "setup_task_complete"]]
        for job in jobs_to_run_main_task:
            try:
                self.run_job_main_task(job) # Abstract method (can be background)
                job.update_status("main_task_running")
                job.start_time = time.time()
            except NotImplementedError:
                job.update_status("main_task_skipped", "Main task not implemented.")
            except Exception as main_e:
                job.update_status("main_task_failed", str(main_e))
            job.set_result(job.result or {})
            self._update_sweep_results(job)

        # --- Phase 3: Monitoring ---
        logger.info("--- Running Monitoring Phase ---")
        active_jobs = [j for j in self.jobs if j.status == "main_task_running"]
        if active_jobs:
            self._monitor_active_jobs(active_jobs)
        else:
            logger.info("No active jobs to monitor.")

        # --- Phase 4: Post-processing ---
        logger.info("--- Running Post-processing Phase ---")
        jobs_for_post_processing = [j for j in self.jobs if j.status == "completed"] # Only if main task was successful
        for job in jobs_for_post_processing:
            try:
                self.run_job_post_task(job) # Optional abstract method
                job.update_status("post_processing_complete")
            except NotImplementedError:
                job.update_status("post_processing_skipped", "Post-processing task not implemented.")
            except Exception as post_e:
                job.update_status("post_processing_failed", str(post_e))
            # Result would have been set in _monitor_active_jobs, just update status and any new data
            job.set_result(job.result or {}) # Ensure result dict is there
            self._update_sweep_results(job)

        # --- Phase 5: Cleanup ---
        logger.info("--- Running Cleanup Phase ---")
        for job in self.jobs:
            try:
                self.cleanup_job(job) # Optional abstract method
            except NotImplementedError:
                pass # Cleanup is optional
            except Exception as clean_e:
                logger.warning(f"Cleanup failed for job {job.id}: {clean_e}")

        logger.info("Parameter sweep finished.")
        # Ensure all jobs have a result dictionary in the final list
        final_results_list = []
        for job_instance in self.jobs:
            if not job_instance.result:
                job_instance.set_result({}) # Create a basic result if somehow missed
            final_results_list.append(job_instance.result)
        self.sweep_results = final_results_list
        return self.sweep_results

    def _monitor_active_jobs(self, active_jobs: List[ParameterSweepJob]):
        """Internal loop to monitor running jobs, check progress, and handle termination."""
        monitoring_start_time = time.time()
        
        while active_jobs: # Loop as long as there are jobs in the active_jobs list
            current_time = time.time()
            elapsed_monitoring_time = current_time - monitoring_start_time

            # Termination condition 1: Timeout
            if elapsed_monitoring_time > self.timeout:
                logger.warning(f"Monitoring timeout ({self.timeout}s) reached.")
                self._terminate_running_jobs(
                    active_jobs, # Terminate only remaining active jobs
                    reason="main_task_timed_out",
                    message=f"Exceeded sweep timeout of {self.timeout}s"
                )
                active_jobs.clear() # All remaining jobs are now considered terminated
                break

            # Termination condition 2: N-th success
            if self.stop_after_n_successes is not None and self._success_count >= self.stop_after_n_successes:
                logger.info(
                    f"Target of {self.stop_after_n_successes} successful jobs reached. "
                    f"Terminating remaining {len(active_jobs)} non-successful active jobs."
                )
                # Only terminate jobs that are not already marked as successful.
                # However, _terminate_running_jobs will already skip non-running jobs.
                self._terminate_running_jobs(
                    active_jobs, # Terminate only remaining active jobs
                    reason="terminated_early_n_success",
                    message=f"Stopped after reaching {self._success_count}/{self.stop_after_n_successes} successes."
                )
                active_jobs.clear()
                break
            
            logger.info(
                f"Monitoring check: {len(active_jobs)} active. Elapsed: {elapsed_monitoring_time:.1f}s. "
                f"Successes: {self._success_count}/{self.stop_after_n_successes or 'all'}"
            )

            jobs_finished_this_cycle = []
            for job in active_jobs:
                if job.status != "main_task_running": # Should not happen if active_jobs is managed correctly
                    jobs_finished_this_cycle.append(job)
                    continue
                
                try:
                    progress_status, progress_data = self.check_job_progress(job) # Abstract method

                    if progress_status == "completed":
                        job.update_status("completed")
                        job.end_time = time.time()
                        job.set_result(progress_data if progress_data else {})
                        self._evaluate_result(job) # Check success, update counters
                        jobs_finished_this_cycle.append(job)
                    elif progress_status == "failed":
                        error_msg = (progress_data.get("error") if isinstance(progress_data, dict) 
                                     else "Failure detected by check_job_progress")
                        job.update_status("main_task_failed", error_msg)
                        job.end_time = time.time()
                        job.set_result(progress_data if progress_data else {})
                        jobs_finished_this_cycle.append(job)
                    elif progress_status == "running":
                        # Optionally update job.result with intermediate data if provided
                        if progress_data and isinstance(progress_data, dict):
                            current_job_result = job.result if job.result else {}
                            job.set_result({**current_job_result, **progress_data})
                            # Re-evaluate if intermediate data could lead to early success metric update
                            self._evaluate_result(job) 
                    else:
                        logger.warning(f"Job {job.id} returned unknown progress status: {progress_status}")

                except Exception as e:
                    logger.exception(f"Error checking progress for job {job.id}: {e}. Marking as failed.")
                    job.update_status("monitor_error", f"Exception in check_job_progress: {str(e)}")
                    job.end_time = time.time()
                    job.set_result({"error": job.error_message})
                    jobs_finished_this_cycle.append(job)
                
                if job in jobs_finished_this_cycle:
                     self._save_individual_result(job) # Save result as soon as it's determined
                     self._update_sweep_results(job) # Update the main list

            # Remove finished jobs from the active list
            if jobs_finished_this_cycle:
                active_jobs = [j for j in active_jobs if j not in jobs_finished_this_cycle]

            if active_jobs: # Only sleep if there are still active jobs and no break condition met
                time.sleep(self.monitor_interval)
        
        logger.info("Monitoring loop finished.")


    def _evaluate_result(self, job: ParameterSweepJob):
        """
        Checks if a job's result meets success criteria (if defined),
        updates the success counter, and tracks the best overall result.
        This can be called with intermediate or final results.
        """
        if not job.result:
            logger.debug(f"Job {job.id} has no result data to evaluate yet.")
            return

        # --- Check for Overall Sweep Success ---
        # Only mark as "sweep_success" if not already marked and criteria met
        if "sweep_success" not in job.result or not job.result["sweep_success"]:
            if self.success_metric and self.success_metric in job.result:
                try:
                    is_job_successful = self.check_success_criteria(job.result) # Abstract method
                    if is_job_successful:
                        # This check ensures we only increment _success_count once per job
                        if not job.result.get("sweep_success_counted", False):
                            job.result["sweep_success"] = True
                            job.result["sweep_success_counted"] = True # Mark that this success has been counted
                            self._success_count += 1
                            logger.info(
                                f"*** Job {job.id} meets success criteria! "
                                f"Success count: {self._success_count}/{self.stop_after_n_successes or 'all'} ***"
                            )
                            # If a job becomes successful, its status should reflect completion of the main task.
                            if job.status == "main_task_running": # If it was running and now is successful
                                job.update_status("completed") # Update status if not already completed
                                if job.end_time is None: job.end_time = time.time() # Set end time if not set

                except NotImplementedError:
                    logger.debug(f"Success criteria not implemented for job {job.id}.")
                except Exception as e:
                    logger.error(f"Error calling check_success_criteria for job {job.id}: {e}")
            elif self.success_metric:
                 logger.warning(f"Success metric '{self.success_metric}' not in result for job {job.id}. Cannot evaluate.")


        # --- Update Best Result Tracking ---
        if self.success_metric and self.success_metric in job.result:
            try:
                current_metric_value = float(job.result[self.success_metric])
                is_new_best = False
                if self.best_metric_value is None:
                    is_new_best = True
                elif self.higher_is_better and current_metric_value > self.best_metric_value:
                    is_new_best = True
                elif not self.higher_is_better and current_metric_value < self.best_metric_value:
                    is_new_best = True
                
                if is_new_best:
                    old_best_val_str = f"{self.best_metric_value:.3f}" if self.best_metric_value is not None else "None"
                    logger.info(
                        f"*** New best result from Job {job.id}! Metric ({self.success_metric}): "
                        f"{current_metric_value:.3f} (Previous best: {old_best_val_str}) ***"
                    )
                    self.best_metric_value = current_metric_value
                    self.best_result_so_far = job.result.copy() # Store a copy
            except (TypeError, ValueError) as e:
                logger.warning(
                    f"Cannot convert success metric '{self.success_metric}' value "
                    f"'{job.result[self.success_metric]}' to float for job {job.id}: {e}"
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
        
        logger.info(f"Attempting to terminate {len(jobs_to_terminate)} job(s) due to: {reason}")
        for job in jobs_to_terminate:
            # Only attempt to terminate jobs that are actually in a runnable/running state
            if job.status in ["main_task_running", "setup_task_running"]: # Add other relevant running states if any
                original_status = job.status
                job.update_status(reason, message) # Update status to reflect termination reason
                if job.end_time is None: job.end_time = time.time()

                try:
                    self.terminate_job(job) # Abstract method
                    logger.info(f"Termination signal sent for job {job.id}.")
                except NotImplementedError:
                    logger.warning(f"Terminate_job not implemented; cannot actively terminate job {job.id}.")
                except Exception as e:
                    logger.error(f"Failed to signal termination for job {job.id}: {e}")
                    # Optionally, append to error message or set a specific termination_error status
                    job.error_message = (job.error_message or "") + f"; Termination failed: {e}"
                    job.result = job.result or {}
                    job.result["termination_error"] = str(e)
                
                # Ensure result is recorded
                job.set_result(job.result or {})
                self._update_sweep_results(job)
                self._save_individual_result(job)
            else:
                logger.debug(f"Skipping termination for job {job.id} as its status is '{job.status}'.")


    def _update_sweep_results(self, job: ParameterSweepJob):
        """Adds or updates a job's result in the main sweep_results list."""
        if not job.result: # Ensure there's a dict to add
            job.set_result({})

        # Find if job.id already exists in sweep_results and update it
        # This ensures we don't have duplicate entries for a job.
        for i, res_dict in enumerate(self.sweep_results):
            if res_dict.get("job_id") == job.id:
                self.sweep_results[i] = job.result
                return
        # If not found, append it
        self.sweep_results.append(job.result)

    def _save_individual_result(self, job: ParameterSweepJob) -> None:
        """Saves the current result dictionary for a job to its output directory."""
        if job.result: # Only save if there's something to save
            result_file = job.output_dir / "result.json"
            try:
                with open(result_file, "w") as f:
                    json.dump(job.result, f, indent=2, default=str) # default=str for non-serializable
                logger.debug(f"Saved result JSON for job {job.id} to {result_file}")
            except Exception as e:
                logger.error(f"Failed to save result JSON for job {job.id} to {result_file}: {e}")

    def save_summary(self, output_file: Path) -> None:
        """Saves a summary of all job results to a single JSON file."""
        logger.info(f"Saving summary of {len(self.sweep_results)} results to {output_file}")
        try:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            # Sort results, e.g., by job ID or a success metric if available
            sorted_results = sorted(self.sweep_results, key=lambda r: r.get("job_id", ""))
            with open(output_file, "w") as f:
                json.dump(sorted_results, f, indent=2, default=str)
            logger.info(f"Summary saved successfully to {output_file}")
        except Exception as e:
            logger.error(f"Failed to write summary file {output_file}: {e}")

    # --- Abstract Methods (must be implemented by subclasses) ---
    @abstractmethod
    def setup_job(self, job: ParameterSweepJob) -> Any:
        """
        Prepare necessary configurations, files, or context for the job before any execution.
        The return value is stored in job.context.
        Raise an exception if setup fails.
        """
        pass

    @abstractmethod
    def run_job_main_task(self, job: ParameterSweepJob) -> None:
        """
        Execute the main task for the job. This can be a blocking call or
        can submit a background job (e.g., to a cluster).
        Update job.status to 'main_task_running' if submitting a background job.
        Raise an exception if submission or immediate execution fails.
        """
        pass

    @abstractmethod
    def check_job_progress(self, job: ParameterSweepJob) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Check the progress of a job whose main task is running in the background.
        Returns:
            A tuple: (status_string, data_dict_or_none)
            status_string: "running", "completed", or "failed".
            data_dict_or_none: A dictionary with current results/metrics if any,
                               or an error message if failed.
        """
        pass

    @abstractmethod
    def terminate_job(self, job: ParameterSweepJob) -> None:
        """
        Attempt to terminate/cancel a job whose main task is running in the background.
        This is called on timeout or if the sweep is stopped early.
        """
        pass

    @abstractmethod
    def check_success_criteria(self, result: Dict[str, Any]) -> bool:
        """
        Evaluate if the given result dictionary for a job meets the defined success criteria.
        This is used if `success_metric` and `stop_after_n_successes` are set.
        Returns: True if successful, False otherwise.
        """
        pass

    # --- Optional Abstract Methods (provide default implementations or raise NotImplementedError) ---
    def run_job_setup_task(self, job: ParameterSweepJob) -> None:
        """
        Execute an optional, typically short, setup task after initial setup_job.
        Example: Running SHELXC which is quick and essential before SHELXD.
        """
        raise NotImplementedError

    def run_job_post_task(self, job: ParameterSweepJob) -> None:
        """
        Execute an optional post-processing task after a job's main task has successfully completed.
        Example: Running SHELXE after SHELXD.
        """
        raise NotImplementedError

    def cleanup_job(self, job: ParameterSweepJob) -> None:
        """
        Perform any cleanup operations for a job after all other phases (optional).
        Example: Deleting large intermediate files.
        """
        pass # Default is to do nothing
