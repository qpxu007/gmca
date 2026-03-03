import os
import socket
import logging

# Set up a logger for configuration
logger = logging.getLogger("qp2.config")

class ServerConfig:
    """
    Centralized configuration for server addresses and ports.
    Priority:
    1. Environment Variables
    2. Defined Values (Defaults)
    3. Auto-determination logic (methods)
    
    Environment Modes (QP2_ENV):
    - prod (default): Uses standard IP addresses and hostname detection.
    - test: Forces localhost/127.0.0.1 for all services.
    """
    
    _env_mode = os.environ.get("QP2_ENV", "prod").lower()
    _is_test = _env_mode == "test"

    # --- Data Processing Server ---
    _DATAPROC_PORT_DEFAULT = 8025
    DATAPROC_PORT = int(os.environ.get("DATAPROC_PORT", _DATAPROC_PORT_DEFAULT))
    
    # --- WebSocket Server ---
    _WEBSOCKET_PORT_DEFAULT = 8000
    WEBSOCKET_PORT = int(os.environ.get("WEBSOCKET_PORT", _WEBSOCKET_PORT_DEFAULT))
    
    # --- Redis ---
    # Default IPs
    if _is_test:
        _DEFAULT_REDIS_HOSTS = {
            "bl2": "127.0.0.1",
            "bl1": "127.0.0.1",
            "analysis_results": "127.0.0.1",
            "analysis_fallback": "127.0.0.1",
            "fallback_redis": "127.0.0.1",
        }
    else:
        _DEFAULT_REDIS_HOSTS = {
            "bl2": "127.0.0.1",
            "bl1": "127.0.0.1",
            "analysis_results": "127.0.0.1",
            "analysis_fallback": "127.0.0.1",
            "fallback_redis": "127.0.0.1",
        }
    
    # --- Database ---
    POSTGRES_HOST = os.environ.get("QP2_PG_HOST", "localhost")
    POSTGRES_PORT = os.environ.get("QP2_PG_PORT", "5432")
    POSTGRES_USER = os.environ.get("QP2_PG_USER", "qp2user")
    POSTGRES_PASS = os.environ.get("QP2_PG_PASS", "")
    POSTGRES_DB = os.environ.get("QP2_PG_DB", "user_data")
    
    # --- MySQL ---
    MYSQL_GMCA_ACCOUNTS = os.environ.get("MYSQL_GMCA_ACCOUNTS", "localhost")
    MYSQL_HOST_BL1 = os.environ.get("MYSQL_HOST_BL1", "localhost")
    MYSQL_HOST_BL2 = os.environ.get("MYSQL_HOST_BL2", "localhost")
    MYSQL_USER = os.environ.get("MYSQL_USER", "qp2user")
    MYSQL_PASS = os.environ.get("MYSQL_PASS", "")
    MYSQL_DB_USER_DATA = "user_data"
    MYSQL_DB_BLC = "blc2004"
    MYSQL_DB_GMCA_ACCOUNTS = "gmca_accounts"
    
    # --- AI Server ---
    _AI_SERVER_DEFAULT = "http://localhost:8888/v1"
    
    # --- Web App ---
    _WEB_APP_PORT_DEFAULT = 8000
    WEB_APP_PORT = int(os.environ.get("WEB_APP_PORT", _WEB_APP_PORT_DEFAULT))
    _WEB_APP_URL_DEFAULT = f"http://localhost:{WEB_APP_PORT}"
    
    # --- Dose Planner ---
    _DOSE_PLANNER_PORT_DEFAULT = 5000
    DOSE_PLANNER_PORT = int(os.environ.get("DOSE_PLANNER_PORT", _DOSE_PLANNER_PORT_DEFAULT))

    # --- HDF5 File Monitoring ---
    _HDF5_POLL_INTERVAL_MS_DEFAULT = 200
    # Allow tuning via env var for slower filesystems (e.g. BeeGFS)
    HDF5_POLL_INTERVAL_MS = int(os.environ.get("QP2_HDF5_POLL_INTERVAL_MS", _HDF5_POLL_INTERVAL_MS_DEFAULT))

    # --- Data Processing Timeouts & Intervals ---
    _RUN_TIMEOUT_SECONDS_DEFAULT = 3600 # 1 hour
    RUN_TIMEOUT_SECONDS = int(os.environ.get("QP2_RUN_TIMEOUT_SECONDS", _RUN_TIMEOUT_SECONDS_DEFAULT))

    _DATA_POLL_INTERVAL_SEC_DEFAULT = 2
    DATA_POLL_INTERVAL_SEC = int(os.environ.get("QP2_DATA_POLL_INTERVAL_SEC", _DATA_POLL_INTERVAL_SEC_DEFAULT))

    _REDIS_CONNECT_TIMEOUT_SEC_DEFAULT = 5
    REDIS_CONNECT_TIMEOUT_SEC = int(os.environ.get("QP2_REDIS_CONNECT_TIMEOUT_SEC", _REDIS_CONNECT_TIMEOUT_SEC_DEFAULT))

    _ANALYSIS_REFRESH_INTERVAL_MS_DEFAULT = 5000 # 5 seconds
    ANALYSIS_REFRESH_INTERVAL_MS = int(os.environ.get("QP2_ANALYSIS_REFRESH_INTERVAL_MS", _ANALYSIS_REFRESH_INTERVAL_MS_DEFAULT))

    @classmethod
    def is_test_env(cls):
        return cls._is_test

    @classmethod
    def get_dataproc_url(cls):
        """
        Returns the Data Processing Server URL.
        Priority: DATAPROC_SERVER_URL -> DATAPROC_HOST -> localhost
        """
        if os.environ.get("DATAPROC_SERVER_URL"):
            url = os.environ["DATAPROC_SERVER_URL"]
            source = "DATAPROC_SERVER_URL env var"
        elif os.environ.get("DATAPROC_HOST"):
            url = f"http://{os.environ['DATAPROC_HOST']}:{cls.DATAPROC_PORT}"
            source = "DATAPROC_HOST env var"
        else:
            url = f"http://localhost:{cls.DATAPROC_PORT}"
            source = "default"

        logger.debug(f"DataProc URL: {url} (source: {source})")
        return url

    @classmethod
    def get_websocket_url(cls):
        """
        Returns the WebSocket Server URL.
        Priority: WEBSOCKET_SERVER_URL -> WEBSOCKET_HOST -> localhost
        """
        if os.environ.get("WEBSOCKET_SERVER_URL"):
            url = os.environ["WEBSOCKET_SERVER_URL"]
            source = "WEBSOCKET_SERVER_URL env var"
        else:
            host = os.environ.get("WEBSOCKET_HOST", "localhost")
            url = f"ws://{host}:{cls.WEBSOCKET_PORT}"
            source = "WEBSOCKET_HOST env var" if "WEBSOCKET_HOST" in os.environ else "default"

        logger.debug(f"WebSocket URL: {url} (source: {source})")
        return url

    @classmethod
    def get_redis_hosts(cls):
        """
        Returns the dictionary of Redis hosts.
        Allows overriding specific keys via REDIS_HOST_<KEY> env vars.
        """
        hosts = cls._DEFAULT_REDIS_HOSTS.copy()
        overridden = []
        for key in hosts:
            env_key = f"REDIS_HOST_{key.upper()}"
            if os.environ.get(env_key):
                hosts[key] = os.environ[env_key]
                overridden.append(key)
        
        mode_str = "TEST" if cls._is_test else "PROD"
        logger.debug(f"Redis Hosts ({mode_str}): {hosts} (overridden: {overridden})")
        return hosts

    @classmethod
    def get_postgres_url(cls):
        """
        Returns the PostgreSQL connection string.
        """
        if os.environ.get("POSTGRES_URL"):
            url = os.environ["POSTGRES_URL"]
            source = "POSTGRES_URL env var"
        else:
            if cls.POSTGRES_PASS:
                url = f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASS}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
            else:
                url = f"postgresql://{cls.POSTGRES_USER}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
            source = "constructed"

        logger.debug(f"Postgres URL: {url} (source: {source})")
        return url

    @classmethod
    def get_ai_server_url(cls):
        """
        Returns the AI Server URL.
        """
        url = os.environ.get("AI_SERVER_URL", cls._AI_SERVER_DEFAULT)
        source = "AI_SERVER_URL env var" if "AI_SERVER_URL" in os.environ else "default"
        logger.debug(f"AI Server URL: {url} (source: {source})")
        return url

    @classmethod
    def get_web_app_url(cls):
        """
        Returns the Web App Backend URL.
        """
        url = os.environ.get("WEB_APP_URL", cls._WEB_APP_URL_DEFAULT)
        source = "WEB_APP_URL env var" if "WEB_APP_URL" in os.environ else "default"
        logger.debug(f"Web App URL: {url} (source: {source})")
        return url

    @classmethod
    def get_dose_planner_url(cls):
        """
        Returns the Dose Planner Server URL.
        Priority: DOSE_PLANNER_URL -> DOSE_PLANNER_HOST -> localhost
        """
        if cls.is_test_env():
            url = f"http://localhost:{cls.DOSE_PLANNER_PORT}"
            source = "test mode"
        elif os.environ.get("DOSE_PLANNER_URL"):
            url = os.environ["DOSE_PLANNER_URL"]
            source = "DOSE_PLANNER_URL env var"
        else:
            host = os.environ.get("DOSE_PLANNER_HOST", "localhost")
            url = f"http://{host}:{cls.DOSE_PLANNER_PORT}"
            source = "DOSE_PLANNER_HOST env var" if "DOSE_PLANNER_HOST" in os.environ else "default"

        logger.debug(f"Dose Planner URL: {url} (source: {source})")
        return url

    @classmethod
    def get_pbs_rpc_url(cls):
        """
        Returns the PBS RPC URL.
        Priority: PBS_RPC_URL -> Auto-detect via MySQL query -> None
        """
        if cls.is_test_env() and not os.environ.get("PBS_RPC_URL"):
             url = "http://localhost:8001/rpc"
             logger.debug(f"PBS RPC URL: {url} (source: test mode)")
             return url

        if os.environ.get("PBS_RPC_URL"):
            url = os.environ["PBS_RPC_URL"]
            logger.debug(f"PBS RPC URL: {url} (source: PBS_RPC_URL env var)")
            return url

        try:
            sql_server = os.environ.get("MYSQL_HOST_BL1", cls.MYSQL_HOST_BL1)
            sql_query = 'select location from Locations where name="pbs"'

            import subprocess
            cmd = ["mysql", "-u", cls.MYSQL_USER, "-h", sql_server, "-e", sql_query, cls.MYSQL_DB_BLC]
            if cls.MYSQL_PASS:
                cmd.insert(2, f"-p{cls.MYSQL_PASS}")
                
            sql_response = subprocess.check_output(
                cmd,
                text=True,
                stderr=subprocess.DEVNULL
            )

            lines = sql_response.strip().split("\n")
            if len(lines) > 1:
                pbs_loc = lines[1]
                url = f"http://{pbs_loc}/rpc"
                logger.debug(f"PBS RPC URL: {url} (source: auto-detected via {sql_server})")
                return url
        except Exception:
            pass

        return None

    @classmethod
    def log_all_configs(cls):
        """
        Logs a summary of all active configurations.
        """
        logger.info(f"--- QP2 Server Configuration (Env: {cls._env_mode}) ---")
        cls.get_dataproc_url()
        cls.get_websocket_url()
        cls.get_redis_hosts()
        cls.get_postgres_url()
        cls.get_ai_server_url()
        cls.get_web_app_url()
        cls.get_dose_planner_url()
        cls.get_pbs_rpc_url()
        logger.info("--------------------------------------------------")