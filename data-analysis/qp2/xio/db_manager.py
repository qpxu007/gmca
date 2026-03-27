import os
import socket  # For getting hostname
from contextlib import contextmanager
from threading import Lock
from typing import List, Any, Optional
from typing import Type, TypeVar

from sqlalchemy import Table, MetaData
from sqlalchemy import (
    create_engine,
)
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import sessionmaker

from qp2.data_viewer.models import Base
from qp2.log.logging_config import get_logger

from qp2.config.servers import ServerConfig

logger = get_logger(__name__)

T = TypeVar("T")  # For type hinting generic return types


def get_beamline_from_hostname():
    """Determines the beamline prefix from the machine's hostname."""
    hostname = socket.gethostname()
    if hostname.startswith("bl1"):
        return "23i"
    if hostname.startswith("bl2"):
        return "23o"
    return os.environ.get("BEAMLINE", "23b")


def get_default_db_path():
    return f"sqlite:///{os.path.join(os.path.expanduser('~'), '.data_viewer', 'user_data.db')}"

# Construct MySQL URLs using ServerConfig
def _build_mysql_url(host):
    auth = f"{ServerConfig.MYSQL_USER}"
    if ServerConfig.MYSQL_PASS:
        auth += f":{ServerConfig.MYSQL_PASS}"
    return f"mysql+pymysql://{auth}@{host}/{ServerConfig.MYSQL_DB_USER_DATA}"

BL1_URL = _build_mysql_url(ServerConfig.MYSQL_HOST_BL1)
BL2_URL = _build_mysql_url(ServerConfig.MYSQL_HOST_BL2)

POSTGRES_URL = ServerConfig.get_postgres_url()


# Define your MySQL connection URLs per hostname prefix
MYSQL_HOST_CONFIG = {
    "bl1": BL1_URL,
    "bl2": BL2_URL,
    # aliases for beamlines
    "23i": BL1_URL,
    "23o": BL2_URL,

    # Add other hostname prefixes and their corresponding DB URLs here
    "default": POSTGRES_URL, 
    "sqlite": get_default_db_path(),
}


class DBManager:
    def __init__(self, beamline=None, status_update_callback=None, error_callback=None):
        self.status_update_callback = status_update_callback or logger.info
        self.error_callback = error_callback or logger.error

        self.db_url: Optional[str] = None
        self.engine = None
        self.Session = None  # SQLAlchemy Session factory
        self.lock = Lock()
        self.beamline = beamline or get_beamline_from_hostname()

        self._determine_db_url(self.beamline)  # Determine the URL first

        if self.db_url:  # Only initialize if a URL was determined
            self._init_db()
        else:
            self._emit_status(
                "DB: No matching database URL configured for this host. DB logging disabled."
            )

    def _emit_status(self, msg: str):
        if self.status_update_callback:
            self.status_update_callback(msg)
        else:
            logger.info(f"DBManager Status: {msg}")

    def _emit_error(self, msg: str):
        if self.error_callback:
            self.error_callback(msg)
        else:
            logger.error(f"DBManager ERROR: {msg}")

    def _determine_db_url(self, beamline):
        logger.debug(f"beamline {beamline}")
        
        # --- Feature Switch: Allow forcing database type via QP2_DB_ENGINE env var ---
        db_engine_override = os.environ.get("QP2_DB_ENGINE", "").lower()
        
        if db_engine_override == "postgresql":
            self.db_url = ServerConfig.get_postgres_url()
            # We use 'psycopg2' explicitly if needed, but get_postgres_url gives a generic string.
            # If we need +psycopg2, we can adjust here or in ServerConfig.
            # Standard postgresql:// implies psycopg2 in modern sqlalchemy usually.
            
            # If specifically requesting +psycopg2 variant:
            if not "psycopg2" in self.db_url and "postgresql" in self.db_url:
                 self.db_url = self.db_url.replace("postgresql://", "postgresql+psycopg2://")

            self._emit_status(f"DB: Forced to PostgreSQL via QP2_DB_ENGINE. URL: {self.db_url}")
            return # Exit after setting the URL

        # --- Existing Logic (MySQL / Fallback if no override) ---
        determined_url = MYSQL_HOST_CONFIG.get(beamline, None)

        if determined_url:
            self.db_url = determined_url
            parsed = make_url(determined_url)
            logger.info(f"User Data DB: {parsed.drivername}://{parsed.host}/{parsed.database} (beamline: {beamline})")
        elif "default" in MYSQL_HOST_CONFIG:  # Fallback to general default if specific host not found
            self.db_url = MYSQL_HOST_CONFIG["default"]
            # Check if default is Postgres and log appropriately
            if self.db_url.startswith("postgresql"):
                 self._emit_status(f"DB: No specific MySQL DB for '{beamline}'. Falling back to default PostgreSQL URL.")
            else:
                 self._emit_status(f"DB: No specific MySQL DB for '{beamline}'. Falling back to default MySQL/SQLite URL.")
            logger.warning(f"Default DB URL: {self.db_url}")
        else:
            self.db_url = None  # Explicitly set to None if no match and no default
            # Error/warning will be emitted by the constructor if self.db_url remains None

    def _init_db(self):
        if not self.db_url:
            self._emit_error("DB Error: _init_db called without a valid DB URL.")
            return

        # List of URLs to try: [configured_url] + [fallback_sqlite] (if distinct)
        urls_to_try = [self.db_url]
        fallback_url = get_default_db_path()
        
        # If the primary URL is not the fallback URL, add fallback to the list
        if self.db_url != fallback_url:
            urls_to_try.append(fallback_url)

        for i, current_url in enumerate(urls_to_try):
            try:
                # --- ROBUST URL LOGGING ---
                parsed_url = make_url(current_url)
                if parsed_url.drivername.startswith("mysql") or parsed_url.drivername.startswith("postgresql"):
                    log_path = f"{parsed_url.host}/{parsed_url.database}"
                else: 
                    log_path = parsed_url.database

                self._emit_status(f"DB: Initializing connection to: {log_path}")

                # --- HANDLE SQLITE DIRECTORY CREATION ---
                if parsed_url.drivername == "sqlite":
                    db_filepath = parsed_url.database
                    if db_filepath and db_filepath != ":memory:":
                        db_dir = os.path.dirname(db_filepath)
                        if db_dir:
                            os.makedirs(db_dir, exist_ok=True)
                            self._emit_status(f"DB: Ensured directory exists for SQLite DB: {db_dir}")

                # Attempt connection
                self.engine = create_engine(current_url, pool_recycle=3600, echo=False)
                self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

                # This actively checks connectivity
                Base.metadata.create_all(self.engine, checkfirst=True)

                # If successful, update self.db_url to what actually worked
                self.db_url = current_url
                self._emit_status("DB: Database interface successfully initialized.")
                return # Stop after success

            except (SQLAlchemyError, Exception) as e:
                import sys
                print(f"DEBUG: Failed to init {current_url}: {e}", file=sys.stderr)
                msg = f"DB Error: Failed to initialize ({log_path}): {e}"
                
                # If this was the last attempt
                if i == len(urls_to_try) - 1:
                    self._emit_error(msg)
                    self.Session = None
                else:
                    self._emit_status(f"{msg}. Attempting fallback...")



    @contextmanager
    def get_session(self):
        """Provide a transactional scope around a series of operations."""
        if not self.Session:
            self._emit_error("DB Error: Cannot get session, DB is not initialized.")
            raise SQLAlchemyError(
                "Database session not available. Initialization may have failed."
            )

        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self._emit_error(f"DB Transaction Error: {e}. Transaction rolled back.")
            raise
        finally:
            session.close()

    def save_object(self, orm_object: Base) -> bool:
        """
        Saves a generic SQLAlchemy ORM object to the database.

        The calling code is responsible for creating the object (e.g., DatasetRun(...)).
        This method handles the session and transaction.

        Args:
            orm_object: An instance of a class that inherits from the declarative Base.

        Returns:
            bool: True on success, False on failure.

        example usage:

        try:
            new_result_entry = DataProcessResults(
                pipelinestatus_id=raw_results_dict.get("pipeline_id"),
                highresolution=raw_results_dict.get("res"),
                spacegroup=raw_results_dict.get("sg"),
                # ... map all other fields ...
                run_stats=json.dumps(raw_results_dict) # Example of JSON conversion
            )
        except Exception as e:
            print(f"Error creating ORM object from dictionary: {e}")
            return

        success = db_man.save_object(new_result_entry)
        """
        if not self.Session:
            self._emit_error("DB Error: Cannot save object, session is not available.")
            return False

        try:
            with self.get_session() as session:
                session.add(orm_object)

            # The context manager handles commit/rollback
            self._emit_status(
                f"DB: Successfully saved object of type {type(orm_object).__name__}."
            )
            return True
        except IntegrityError:
            self._emit_status(
                f"DB Info: Object of type {type(orm_object).__name__} already exists or violates a constraint. Skipped."
            )
            # Depending on your use case, you might want to return True here as well.
            return True
        except Exception as e:
            self._emit_error(
                f"DB Error: Failed to save object of type {type(orm_object).__name__}: {e}"
            )
            return False

    def save_dict(self, table_name: str, data_dict: dict) -> bool:
        """
        Saves a dictionary to a specified table using reflection.

        NOTE: This is less safe than the ORM approach as it bypasses
        model-level validations and type hints. Use with caution.

        Args:
            table_name: The string name of the table to insert into.
            data_dict: A dictionary where keys are column names.

        Returns:
            bool: True on success, False on failure.
        """
        if not self.engine:
            self._emit_error("DB Error: Cannot save dict, engine is not available.")
            return False

        try:
            # Reflect the table structure from the database
            metadata = MetaData()
            target_table = Table(table_name, metadata, autoload_with=self.engine)

            # The 'with self.engine.connect()' block handles the connection lifecycle
            with self.engine.connect() as connection:
                # Create an insert statement
                stmt = target_table.insert().values(**data_dict)
                # Execute and commit
                connection.execute(stmt)
                connection.commit()  # Required for some DBAPI drivers

            self._emit_status(
                f"DB: Successfully inserted dictionary into table '{table_name}'."
            )
            return True
        except IntegrityError:
            self._emit_status(
                f"DB Info: Insert into '{table_name}' failed due to a constraint violation. Skipped."
            )
            return True
        except Exception as e:
            # This could be a NoSuchTableError, a key error if a column doesn't exist, etc.
            self._emit_error(
                f"DB Error: Failed to insert dictionary into table '{table_name}': {e}"
            )
            return False

    def update_by_pk(self, model_class: type, pk_value: Any, updates: dict) -> bool:
        """
        Updates a row in the database identified by its primary key.

        This method finds the object, updates its attributes from the `updates`
        dictionary, and commits the changes.

        Args:
            model_class: The ORM model class of the table (e.g., DatasetRun).
            pk_value: The value of the primary key for the row to update.
            updates: A dictionary where keys are attribute/column names and
                     values are the new values.

        Returns:
            bool: True on success, False if the object was not found or an error occurred.

        Example usage:

        # In pipeline_tracker.py (or any other module)

        # Import the model you want to update
        from qp2.data_viewer.models import PipelineStatus
        from db_manager import DBManager
        from datetime import datetime

        class PipelineTracker:
            def __init__(self, ...):
                self.db_manager = DBManager()
                self.pipeline_status_id = None # This will be set when the pipeline starts

            def start_pipeline(self):
                # Create the initial status object
                initial_status = PipelineStatus(
                    state="START",
                    pipeline="gmcaproc",
                    # ... other initial params
                )
                # Use the generic save method
                self.db_manager.save_object(initial_status)

                # IMPORTANT: After saving, the object has the auto-generated primary key
                self.pipeline_status_id = initial_status.id
                print(f"Pipeline started with ID: {self.pipeline_status_id}")

            def update_pipeline_progress(self, new_state: str, warning_message: str = None):
                if not self.pipeline_status_id:
                    print("Cannot update status, pipeline has not been started.")
                    return

                # 1. Create a dictionary of the fields you want to update
                updates_dict = {
                    "state": new_state,
                    "warning": warning_message,
                    "elapsedtime": "some new calculated time" # Just an example
                }

                # 2. Call the generic update method
                success = self.db_manager.update_by_pk(
                    model_class=PipelineStatus,          # Tell it which table/model
                    pk_value=self.pipeline_status_id,    # Tell it which row
                    updates=updates_dict                 # Give it the new data
                )

                if success:
                    print(f"Pipeline status updated to '{new_state}'.")
                else:
                    print("Failed to update pipeline status.")

        # Example usage
        tracker = PipelineTracker()
        tracker.start_pipeline()
        # ... do some work ...
        tracker.update_pipeline_progress("RUNNING", "Indexing step completed.")


        """
        if not self.Session:
            self._emit_error("DB Error: Cannot update, session is not available.")
            return False

        try:
            with self.get_session() as session:
                # 1. Fetch the object to update using its primary key
                #    session.get() is the modern and preferred way to fetch by PK.
                obj_to_update = session.get(model_class, pk_value)

                if not obj_to_update:
                    self._emit_error(
                        f"DB Update Error: No object of type {model_class.__name__} found with primary key '{pk_value}'."
                    )
                    return False

                # 2. Update the object's attributes from the dictionary
                for key, value in updates.items():
                    # setattr() safely sets attributes on the object.
                    # This will fail if the attribute doesn't exist, which is good.
                    if hasattr(obj_to_update, key):
                        setattr(obj_to_update, key, value)
                    else:
                        logger.warning(
                            f"DB Update Warning: Attribute '{key}' not found on model '{model_class.__name__}'. Skipping."
                        )

                # 3. The session context manager will handle the commit.
                #    SQLAlchemy is smart enough to know the object is "dirty"
                #    and will issue an UPDATE statement.
                self._emit_status(
                    f"DB: Successfully updated object of type {model_class.__name__} with PK '{pk_value}'."
                )
            return True
        except Exception as e:
            self._emit_error(
                f"DB Error: Failed to update object of type {model_class.__name__} with PK '{pk_value}': {e}"
            )
            # The context manager handles rollback
            return False

    def get_by_pk(self, model_class: Type[T], pk_value: Any) -> Optional[T]:
        """
        Fetches a single object from the database by its primary key.

        Args:
            model_class: The ORM model class to query.
            pk_value: The primary key value.

        Returns:
            The ORM object if found, otherwise None.


        # --- Usage ---
        # pipeline_status = db_manager.get_by_pk(PipelineStatus, 123)
        # if pipeline_status:
        #     print(pipeline_status.state)

        """
        if not self.Session:
            self._emit_error("DB Error: Cannot query, session not available.")
            return None
        try:
            with self.get_session() as session:
                # session.get() is the most efficient way to query by primary key.
                # It returns the object or None if not found.
                return session.get(model_class, pk_value)
        except Exception as e:
            self._emit_error(
                f"DB Error fetching {model_class.__name__} by PK '{pk_value}': {e}"
            )
            return None

    def find_first(self, model_class: Type[T], **filter_criteria) -> Optional[T]:
        """
        Finds the first object that matches the given filter criteria.

        Args:
            model_class: The ORM model class to query.
            **filter_criteria: Keyword arguments to use for filtering (e.g., username="test").

        Returns:
            The first matching ORM object, or None if no match is found.

        # --- Usage ---
        # first_failed_run = db_manager.find_first(PipelineStatus, state="FAILED", pipeline="gmcaproc")
        """
        if not self.Session:
            return None
        try:
            with self.get_session() as session:
                return session.query(model_class).filter_by(**filter_criteria).first()
        except Exception as e:
            self._emit_error(f"DB Error in find_first for {model_class.__name__}: {e}")
            return None

    def find_all(self, model_class: Type[T], **filter_criteria) -> List[T]:
        """
        Finds all objects that match the given filter criteria.

        Args:
            model_class: The ORM model class to query.
            **filter_criteria: Keyword arguments to use for filtering.

        Returns:
            A list of matching ORM objects (can be an empty list).

        # --- Usage ---
        # all_guest_runs = db_manager.find_all(DatasetRun, username="guest")
        # for run in all_guest_runs:
        #     print(run.run_prefix)
        """
        if not self.Session:
            return []
        try:
            with self.get_session() as session:
                return session.query(model_class).filter_by(**filter_criteria).all()
        except Exception as e:
            self._emit_error(f"DB Error in find_all for {model_class.__name__}: {e}")
            return []

    def delete_object(self, orm_object: Base) -> bool:
        """
        Deletes a specific ORM object from the database.

        Args:
            orm_object: The instance of the ORM object to delete.

        Returns:
            True on success, False on failure.

        # --- Usage ---
        # run_to_delete = db_manager.get_by_pk(DatasetRun, 45)
        # if run_to_delete:
        #     db_manager.delete_object(run_to_delete)

        """
        if not self.Session:
            return False
        try:
            with self.get_session() as session:
                # The object must be "attached" to the session to be deleted.
                # If it came from another session, merge it into the current one first.
                session.merge(orm_object)
                session.delete(orm_object)
            self._emit_status(
                f"DB: Successfully deleted object of type {type(orm_object).__name__}."
            )
            return True
        except Exception as e:
            self._emit_error(
                f"DB Error deleting object of type {type(orm_object).__name__}: {e}"
            )
            return False

    def delete_by_pk(self, model_class: type, pk_value: Any) -> bool:
        """
        Finds and deletes an object by its primary key.
        """
        obj_to_delete = self.get_by_pk(model_class, pk_value)
        if obj_to_delete:
            return self.delete_object(obj_to_delete)
        else:
            self._emit_status(
                f"DB Delete Info: No object of type {model_class.__name__} with PK '{pk_value}' found to delete."
            )
            return False  # Or True if "not existing" is a success state for you

    def exists(self, model_class: type, **filter_criteria) -> bool:
        """
        Checks if at least one object exists that matches the filter criteria.

        # --- Usage ---
        # if db_manager.exists(DatasetRun, run_prefix="my_special_run"):
        #     print("The special run exists!")
        #
        # num_failed = db_manager.count(PipelineStatus, state="FAILED")
        # print(f"There are {num_failed} failed pipelines.")
        """
        if not self.Session:
            return False
        try:
            with self.get_session() as session:
                # A more efficient way to check for existence than .first()
                q = session.query(model_class).filter_by(**filter_criteria)
                return session.query(q.exists()).scalar()
        except Exception as e:
            self._emit_error(f"DB Error in exists for {model_class.__name__}: {e}")
            return False

    def count(self, model_class: type, **filter_criteria) -> int:
        """
        Counts the number of objects matching the filter criteria.
        """
        if not self.Session:
            return 0
        try:
            with self.get_session() as session:
                return session.query(model_class).filter_by(**filter_criteria).count()
        except Exception as e:
            self._emit_error(f"DB Error in count for {model_class.__name__}: {e}")
            return 0
