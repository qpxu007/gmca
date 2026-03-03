# --- START OF MODIFIED FILE xds.py ---

# ... (all imports remain, but add the new tracker)
from qp2.pipelines.utils.pipeline_tracker import PipelineTracker

# ... (most of the file is the same until the XDS class)

class XDS:
    def __init__(
        self,
        dataset,
        # ... (all existing parameters)
        use_redis=False,
        existing_pipeline_status_id: Optional[int] = None,
    ):
        # ... (all existing initializations remain the same)
        # ... (self.dataset, self.metadata, self.proc_dir, etc.)
        
        # --- REMOVE DB and State attributes ---
        # self.db_manager = DBManager() # This will be inside the tracker
        # self.pipeline_status_id = None
        # self.data_process_result_id = None
        
        # --- ADD the PipelineTracker ---
        redis_conf = {"host": "127.0.0.1", "db": 0} if use_redis else None
        
        tracker_params = {
            'sampleName': self.prefix,
            'imagedir': os.path.dirname(self.master_file),
            'logfile': f"{os.getenv('HOME')}/xds.log"
        }

        self.tracker = PipelineTracker(
            pipeline_name="gmcaproc",
            run_identifier=self.master_file,
            initial_params=tracker_params,
            result_mapper=self._map_results_to_sql,
            redis_config=redis_conf,
            existing_pipeline_status_id=existing_pipeline_status_id,
        )

        # ... (the rest of __init__ is the same, setting up xds_inp, etc.)
        
        self.results = {}
        if initial_results:
            self.results.update(initial_results)
            
    # REMOVE this method from XDS
    # def save_to_redis(self): ...

    # RENAME _get_sql_mapped_results to be the official mapper function
    def _map_results_to_sql(self, current_results: dict) -> dict:
        """
        Maps the internal 'results' dictionary to the database schema.
        This function is passed to the PipelineTracker.
        """
        # Note: We now use the passed-in `current_results` dictionary
        return {
            "firstFrame": str(self.user_start),
            "highresolution": current_results.get("resolution_based_on_cchalf", current_results.get("resolution")),
            "spacegroup": current_results.get("SPACE_GROUP_NUMBER"),
            "unitcell": " ".join(map(str, current_results.get("UNIT_CELL_CONSTANTS", []))),
            "wavelength": current_results.get("X-RAY_WAVELENGTH"),
            "rmerge": current_results.get("R-merge"),
            "rmeas": current_results.get("R-meas"),
            "rpim": current_results.get("R-pim"),
            "isigmai": current_results.get("I/sig(I)"),
            "multiplicity": current_results.get("redundancy"),
            "completeness": current_results.get("completeness"),
            "anom_completeness": current_results.get("anomalous_completeness"),
            "table1": current_results.get("summary_table"),
            "workdir": self.proc_dir,
            "scale_log": self.correct_lp_file,
            "truncate_mtz": self.processed_mtz_file,
            "run_stats": json.dumps(current_results, default=str),
            "solve": current_results.get("final_pdb"),
        }

    # REMOVE these DB methods
    # def _update_pipeline_status(self, state: str, message: Optional[str] = None): ...
    # def _create_or_update_data_process_result(self, new_run=False): ...

    def _handle_error(self, step: str, message: str, detail: Optional[str] = None):
        """Centralizes error logging and uses the tracker to report failure."""
        logger.error(f"Processing failed at step '{step}': {message}")
        self.results["error_step"] = step
        self.results["error_message"] = message
        if detail:
            self.results["error_detail"] = detail
        # The main 'except' block will call tracker.fail()
        
    def process(self):
        """
        Orchestrates the full XDS processing workflow using the PipelineTracker.
        """
        log_queue = get_multiprocessing_queue()
        listener = start_queue_listener(log_queue, log_file=f"{os.getenv('HOME')}/xds.log")
        logger.info("Multiprocessing logging listener started.")
        
        pointless_process = None

        try:
            # 1. Start the pipeline
            self.tracker.start()
            
            # 2. Run steps and update progress
            self.tracker.update_progress("INITIALIZING", self.results)
            if not self._run_initialization():
                raise RuntimeError("Initialization failed")

            self.tracker.update_progress("INDEXING", self.results)
            if not self._run_indexing():
                raise RuntimeError("Indexing failed")

            if not self._run_strategy_if_needed():
                if self.strategy:
                    logger.info("Strategy-only run complete.")
                    self.tracker.succeed(self.results)
                    return
                raise RuntimeError("Strategy determination failed")

            if not self._refine_indexing_with_user_input():
                raise RuntimeError("Refining indexing failed")
            
            self.tracker.update_progress("SCALING", self.results)
            if not self._run_integration_and_scaling():
                raise RuntimeError("Integration and scaling failed")

            if os.path.exists(self.xds_ascii_hkl_file):
                logger.info("Starting Pointless analysis in the background.")
                # The child process will need the ID to link its results
                pointless_args = (self.tracker.pipeline_status_id, )
                pointless_process = Process(target=self.pointless_rerun_if_needed, args=pointless_args)
                pointless_process.start()
            else:
                logger.warning(f"{self.xds_ascii_hkl_file} not found, skipping Pointless.")
            
            self.tracker.update_progress("REFINING", self.results)
            self._search_rcsb()
            self._refine_resolution_iteratively()
            
            if self.optimization:
                self.tracker.update_progress("OPTIMIZING", self.results)
                self.optimize_with_revert()
                
            self.tracker.update_progress("POST-PROCESSING", self.results)
            self.post_processing()

            self.create_summary()
            
            # 3. Mark as successful
            self.tracker.succeed(self.results)

        except Exception as e:
            error_message = f"Processing failed: {e}"
            logger.error(error_message, exc_info=True)
            self._handle_error("CRITICAL_FAILURE", error_message)
            # 4. Mark as failed
            self.tracker.fail(error_message, self.results)
            
        finally:
            if pointless_process and pointless_process.is_alive():
                logger.info("Waiting for Pointless process to complete...")
                pointless_process.join(timeout=300)
                if pointless_process.is_alive():
                    logger.warning("Pointless process timed out and will be terminated.")
                    pointless_process.terminate()
            
            logger.info("Stopping multiprocessing logging listener.")
            listener.stop()

        logger.info(f"End of XDS processing run for {self.proc_dir}")

# ... (The rest of the file, like main(), stays mostly the same)