# in /home/qxu/data-analysis/qp2/utils/pipeline_logger.py

import json
import logging
import os
import re
from datetime import datetime
from typing import Optional, Dict, Any

import redis
from sqlalchemy import desc

from qp2.data_viewer.models import PipelineStatus, DataProcessResults, DatasetRun
from qp2.xio.db_manager import DBManager
from qp2.config.servers import ServerConfig

logger = logging.getLogger(__name__)


class PipelineLogger:
    def __init__(self, pipeline_name: str, sample_name: str, image_dir: str, use_redis: bool = False, dataset_run_id: Optional[int] = None):
        self.pipeline_name = pipeline_name
        self.sample_name = sample_name
        self.image_dir = image_dir
        self.use_redis = use_redis

        self.db_manager = DBManager()
        self.pipeline_status_id: Optional[int] = None
        self.data_process_result_id: Optional[int] = None
        self.dataset_run_id: Optional[int] = dataset_run_id
        self.run_prefix: Optional[str] = None

        # Try to automatically find the DatasetRun if not provided
        if self.dataset_run_id is None:
            self.dataset_run_id = self._find_dataset_run_id()

    def _find_dataset_run_id(self) -> Optional[int]:
        """Attempts to find a matching DatasetRun ID based on the imagedir path."""
        if not self.image_dir:
            return None

        # Extract run_prefix from imagedir (look for esafXXXXX or similar patterns)
        # Often the directory structure is .../esafXXXXX/run_prefix/...
        # Let's try to match the last part of the path that looks like a prefix
        path_parts = self.image_dir.strip("/").split("/")
        
        # Heuristic: try parts from right to left
        for part in reversed(path_parts):
            if not part: continue
            try:
                with self.db_manager.get_session() as session:
                    run_obj = (
                        session.query(DatasetRun)
                        .filter_by(run_prefix=part)
                        .order_by(desc(DatasetRun.created_at))
                        .first()
                    )
                    if run_obj:
                        logger.info(f"PipelineLogger: Linked to DatasetRun '{part}' (ID: {run_obj.data_id})")
                        self.run_prefix = part
                        return run_obj.data_id
            except Exception:
                continue
        return None

    def start(self):
        """Initializes the logging and creates the master status record."""
        with self.db_manager.get_session() as session:
            status = PipelineStatus(
                state="START",
                pipeline=self.pipeline_name,
                sampleName=self.sample_name,
                imagedir=self.image_dir,
                logfile=f"{os.getenv('HOME')}/{self.pipeline_name}.log",
                dataset_run_id=self.dataset_run_id,
                run_prefix=self.run_prefix
            )
            session.add(status)
            session.flush()
            self.pipeline_status_id = status.id
        self.update_status("RUN", "Pipeline started.")

    def update_status(self, state: str, message: Optional[str] = None):
        """Updates the status of the pipeline run."""
        if self.pipeline_status_id is None:
            return

        with self.db_manager.get_session() as session:
            status = session.query(PipelineStatus).get(self.pipeline_status_id)
            if status:
                status.state = state
                if message:
                    status.warning = message
                status.elapsedtime = str(datetime.now() - status.starttime)

        if self.use_redis:
            self._save_to_redis({"status": state, "message": message})

    def log_results(self, results_data: Dict[str, Any], new_run: bool = False):
        """Logs a set of results to the database, creating or updating a record."""
        if self.pipeline_status_id is None:
            self.start()

        with self.db_manager.get_session() as session:
            if new_run or self.data_process_result_id is None:
                new_result = DataProcessResults(
                    pipelinestatus_id=self.pipeline_status_id, **results_data
                )
                session.add(new_result)
                session.flush()
                self.data_process_result_id = new_result.id
            else:
                session.query(DataProcessResults).filter_by(id=self.data_process_result_id).update(results_data)

        if self.use_redis:
            self._save_to_redis(results_data)

    def finish(self):
        """Marks the pipeline as successfully completed."""
        self.update_status("DONE", "Pipeline finished successfully.")

    def fail(self, error_message: str):
        """Marks the pipeline as failed."""
        self.update_status("FAILED", error_message)

    def _save_to_redis(self, data: Dict[str, Any]):
        """Saves a dictionary of data to Redis."""
        try:
            # Use analysis_results as primary
            redis_host = ServerConfig.get_redis_hosts().get("analysis_results", "10.20.103.67")
            redis_conn = redis.Redis(host=redis_host, db=0, socket_timeout=2, socket_connect_timeout=2)
            redis_conn.ping()
            redis_key = f"analysis:out:data:{self.pipeline_name}:{self.sample_name}"

            data['timestamp'] = datetime.now().isoformat()

            result_json = json.dumps(data, default=str)
            redis_conn.rpush(redis_key, result_json)
            redis_conn.expire(redis_key, 7 * 24 * 3600)  # 1-week expiration
        except Exception as e:
            logger.warning(f"Could not save to Redis: {e}")
