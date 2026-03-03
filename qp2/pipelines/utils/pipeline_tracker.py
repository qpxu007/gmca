import json
import logging
from datetime import datetime
from typing import Callable, Dict, Any, Optional

import redis
from sqlalchemy.exc import IntegrityError
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy import desc

from qp2.data_viewer.models import PipelineStatus, DataProcessResults, DatasetRun
from qp2.xio.db_manager import DBManager

logger = logging.getLogger(__name__)


class PipelineTracker:
    """
    A framework for managing the lifecycle and results of a data processing pipeline.
    This version is refactored to use the generic methods of the DBManager.
    """

    def __init__(
            self,
            pipeline_name: str,
            run_identifier: str,
            initial_params: Dict[str, Any],
            result_mapper: Callable[[Dict[str, Any]], Dict[str, Any]],
            redis_config: Optional[Dict[str, Any]] = None,
            existing_pipeline_status_id: Optional[int] = None,
            results_model=None,
    ):
        """
        Initializes the tracker for a specific pipeline run.
        """
        self.pipeline_name = pipeline_name
        self.run_identifier = run_identifier
        self.initial_params = initial_params
        self.result_mapper = result_mapper
        self.redis_config = redis_config

        self.redis_pubsub_channel = "pipeline_updates"

        # DBManager is now our full-service data access layer
        beamline = initial_params.get("beamline", None)
        self.db_manager = DBManager(beamline)
        self.redis_conn = self._connect_to_redis()

        self.pipeline_status_id: Optional[int] = existing_pipeline_status_id
        # Model to persist results: default to DataProcessResults for backward compatibility
        self.results_model = results_model or DataProcessResults
        # Generic PK value for the chosen results model (DataProcessResults.id or ScreenStrategyResults.sampleNumber)
        self.result_pk_value: Optional[int] = None
        self.current_results: Dict[str, Any] = {}
        # --- ADDED: Store start time to prevent re-fetching ---
        self.start_time: Optional[datetime] = None

    def _connect_to_redis(self):
        if not self.redis_config:
            logger.info("Redis is not configured. Real-time updates will be disabled.")
            return None
        try:
            conn = redis.Redis(
                **self.redis_config,
                socket_timeout=2,
                socket_connect_timeout=2,
                decode_responses=True,
            )
            conn.ping()
            logger.info(
                f"Successfully connected to Redis at {self.redis_config.get('host')}"
            )
            return conn
        except redis.exceptions.ConnectionError as e:
            logger.warning(
                f"Could not connect to Redis. Real-time updates disabled. Error: {e}"
            )
            return None

    def start(self):
        """Creates the initial PipelineStatus record using DBManager's save_object."""
        if self.pipeline_status_id is not None:
            logger.info(
                f"Tracker attaching to existing PipelineStatus ID: {self.pipeline_status_id}"
            )
            # When re-attaching, we DO need to fetch the start time.
            status_obj = self.db_manager.get_by_pk(
                PipelineStatus, self.pipeline_status_id
            )
            if status_obj:
                self.start_time = status_obj.starttime

            self.update_progress(
                "RE-PROCESSING",
                {"message": f"Attached to pipeline for sub-process run."},
            )
            return

        # --- REFACTORED LOGIC ---
        # 1. Create the ORM object
        # Dynamically filter out keys that are not columns in the PipelineStatus model
        # to avoid TypeError: '...' is an invalid keyword argument for PipelineStatus
        status_columns = sa_inspect(PipelineStatus).columns.keys()
        
        status_params = {}
        for k, v in self.initial_params.items():
            # Standardize 'pi_badge' to 'pi_id' if present
            if k == "pi_badge" and "pi_id" in status_columns:
                status_params["pi_id"] = v
            elif k in status_columns:
                status_params[k] = v

        # --- Link to DatasetRun ---
        dataset_run_id = None
        run_prefix_val = self.initial_params.get("run_prefix") or self.initial_params.get("prefix")
        if run_prefix_val:
            try:
                # Find the most recently created DatasetRun with this prefix
                with self.db_manager.get_session() as session:
                    run_obj = (
                        session.query(DatasetRun)
                        .filter_by(run_prefix=run_prefix_val)
                        .order_by(desc(DatasetRun.created_at))
                        .first()
                    )
                    if run_obj:
                        dataset_run_id = run_obj.data_id
                        logger.info(f"Linking PipelineStatus to DatasetRun ID: {dataset_run_id}")
            except Exception as e:
                logger.warning(f"Failed to link PipelineStatus to DatasetRun: {e}")

        # Ensure run_prefix is set in status_params
        if run_prefix_val:
            status_params["run_prefix"] = run_prefix_val

        status = PipelineStatus(
            state="START",
            pipeline=self.pipeline_name,
            dataset_run_id=dataset_run_id,
            **status_params,
        )

        # 2. Use the generic save method
        # We need to get the generated ID and starttime back, so we do this in a session.
        try:
            with self.db_manager.get_session() as session:
                session.add(status)
                session.flush()  # Flush to get the ID and DB-defaults without committing
                self.pipeline_status_id = status.id
                # --- MODIFIED: Capture the start time while the object is still in the session ---
                self.start_time = status.starttime
                logger.info(
                    f"Pipeline started. Created PipelineStatus record with ID: {self.pipeline_status_id}"
                )
            # The context manager commits on exit
        except Exception as e:
            logger.error(f"Failed to create initial pipeline status record: {e}")
            # Abort if we can't even create the initial record
            return

        self.update_progress("STARTED", results={"message": "Pipeline initiated."})

    def update_progress(self, status: str, results: Dict[str, Any]):
        self.current_results = results
        # self._update_db_status("RUN", warning=status)
        self._update_db_status(status, warning=status)
        self._create_or_update_data_process_result()
        self._save_to_redis(status)

    def succeed(self, final_results: Dict[str, Any]):
        self.current_results = final_results
        self._update_db_status("DONE", warning="")
        self._create_or_update_data_process_result()
        self._save_to_redis("DONE")
        logger.info(f"Pipeline completed successfully. Final status 'DONE'.")

    def fail(self, error_message: str, results: Optional[Dict[str, Any]] = None):
        if results:
            self.current_results = results
        self.current_results["error_message"] = error_message
        self._update_db_status("FAILED", warning=error_message)
        self._create_or_update_data_process_result()
        self._save_to_redis("FAILED")
        logger.error(f"Pipeline failed. Final status 'FAILED'. Reason: {error_message}")

    def _update_db_status(self, state: str, warning: Optional[str] = None):
        """Updates the PipelineStatus record using DBManager's update_by_pk."""
        if self.pipeline_status_id is None:
            logger.warning("PipelineStatus ID not set. Cannot update database status.")
            return

        # --- REFACTORED LOGIC ---
        # 1. Calculate elapsed time using the stored start_time.
        #    This avoids re-fetching the object and causing a DetachedInstanceError.
        elapsed_time = "N/A"
        if self.start_time:
            elapsed_time = str(datetime.now() - self.start_time)

        # 2. Prepare the dictionary of updates
        updates_dict = {"state": state, "warning": warning, "elapsedtime": elapsed_time}

        # 3. Call the generic update method
        self.db_manager.update_by_pk(
            model_class=PipelineStatus,
            pk_value=self.pipeline_status_id,
            updates=updates_dict,
        )
        # The db_manager handles logging the success/failure of the update.

    def _get_results_pk_name(self) -> Optional[str]:
        try:
            return sa_inspect(self.results_model).primary_key[0].name
        except Exception:
            return None

    def _create_or_update_data_process_result(self):
        """Creates or updates the chosen results record (DataProcessResults or ScreenStrategyResults)."""
        if self.pipeline_status_id is None:
            return

        # Use the mapper to translate results into a DB-compatible format
        sql_mapped_results = self.result_mapper(self.current_results)

        model = self.results_model
        pk_name = self._get_results_pk_name()
        if pk_name is None:
            logger.error("Could not determine primary key for results model.")
            return

        # Try to find existing row for this pipeline status
        if self.result_pk_value is None:
            existing = self.db_manager.find_first(
                model, pipelinestatus_id=self.pipeline_status_id
            )
            if existing is not None:
                try:
                    self.result_pk_value = getattr(existing, pk_name)
                except Exception:
                    self.result_pk_value = None

        if self.result_pk_value is not None:
            self.db_manager.update_by_pk(
                model_class=model,
                pk_value=self.result_pk_value,
                updates=sql_mapped_results,
            )
        else:
            # Create a new record
            try:
                with self.db_manager.get_session() as session:
                    new_result = model(
                        pipelinestatus_id=self.pipeline_status_id, **sql_mapped_results
                    )
                    session.add(new_result)
                    session.flush()  # populate PK
                    try:
                        self.result_pk_value = getattr(new_result, pk_name)
                    except Exception:
                        self.result_pk_value = None
                logger.info(
                    f"DB: Successfully created {model.__name__} with PK: {self.result_pk_value}"
                )
            except IntegrityError:
                logger.warning(
                    f"DB Info: {model.__name__} record already exists. Skipped creation."
                )
            except Exception as e:
                logger.error(f"DB Error: Failed to save {model.__name__} object: {e}")

    def _save_to_redis(self, status: str):
        """
        Saves the full results to a Redis list and publishes a notification.
        """
        if not self.redis_conn:
            return

        # Prepare the data payload for both operations
        redis_data = self.result_mapper(self.current_results)
        redis_data["status"] = status
        redis_data["timestamp"] = datetime.now().isoformat()
        redis_data["pipeline_status_id"] = self.pipeline_status_id

        # 1. Save the full data to the list (existing logic)
        try:
            redis_key = f"analysis:out:data:{self.pipeline_name}:{self.run_identifier}"
            new_result_json = json.dumps(redis_data, default=str)
            self.redis_conn.rpush(redis_key, new_result_json)
            logger.debug(f"Pushed full results to Redis key: {redis_key}")
        except redis.exceptions.RedisError as e:
            logger.warning(f"Failed to push to Redis list. Error: {e}")

        # 2. Publish a lightweight notification message (new logic)
        try:
            notification_message = json.dumps(
                {
                    "pipeline_name": self.pipeline_name,
                    "status": status,
                    "pipeline_status_id": self.pipeline_status_id,
                    "sample_name": self.initial_params.get("sampleName", "N/A"),
                }
            )
            self.redis_conn.publish(self.redis_pubsub_channel, notification_message)
            logger.debug(
                f"Published notification to channel '{self.redis_pubsub_channel}'"
            )
        except redis.exceptions.RedisError as e:
            logger.warning(f"Failed to publish to Redis channel. Error: {e}")
