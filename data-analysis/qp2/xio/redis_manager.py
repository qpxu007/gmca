# redis_manager.py

import json
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
from urllib.parse import urlparse

import numpy as np
from pyqtgraph.Qt import QtCore
from redis import Redis, exceptions as redis_exceptions

from qp2.log.logging_config import get_logger
from qp2.xio.user_group_manager import UserGroupManager
from qp2.config.servers import ServerConfig

logger = get_logger(__name__)


class RedisConfig:
    HOSTS = ServerConfig.get_redis_hosts()
    REDIS_STREAM_NAME = "eiger"
    REDIS_MESSAGE_COUNT = 500  # For get_latest_dataset_path
    REDIS_CONNECT_TIMEOUT = ServerConfig.REDIS_CONNECT_TIMEOUT_SEC  # seconds
    DEFAULT_REDIS_PORT = 6379


def get_redis_server(beamline=None, location="redis"):
    """get redis configuration from mysql DB"""
    if beamline in ("23i", "23b"):
        sql_server = ServerConfig.MYSQL_HOST_BL1
    elif beamline == "23o":
        sql_server = ServerConfig.MYSQL_HOST_BL2
    else:
        sql_server = ServerConfig.MYSQL_HOST_BL1

    sql_response = subprocess.check_output(
        [
            "mysql",
            "-u",
            ServerConfig.MYSQL_USER,
            "-h",
            sql_server,
            "-e",
            f'select location from Locations where name="{location}"',
            ServerConfig.MYSQL_DB_BLC,
        ]
    )
    out = sql_response.decode("utf-8").split("\n")[1].split(":")
    redis_server = out[0]
    if len(out) == 2:
        redis_port = out[1]
    else:
        redis_port = "6379"

    logger.info(f"SQL config: {sql_response} {redis_server} {redis_port}")

    return f"{redis_server}:{redis_port}"


class RedisConnection:
    """Manages a single Redis connection with retry logic."""

    def __init__(
        self,
        host: str,
        status_update_signal: QtCore.pyqtSignal,
        description: str = "Redis",
    ):
        self.host = host
        self.status_update_signal = status_update_signal
        self.description = description
        self.conn = None
        self.last_failure_time = 0
        self.retry_cooldown = 30  # seconds

    def _parse_host_string(self, host_string: str) -> Tuple[str, int]:
        try:
            parsed_url = urlparse(f"redis://{host_string}")
            hostname = parsed_url.hostname or host_string
            port = parsed_url.port or RedisConfig.DEFAULT_REDIS_PORT
            return hostname, port
        except Exception:
            if ":" in host_string:
                parts = host_string.split(":")
                host_part = parts[0]
                port_part_str = parts[-1]
                try:
                    port_val = int(port_part_str)
                    if len(parts) > 2:  # IPv6
                        host_part = ":".join(parts[:-1])
                    return host_part, port_val
                except ValueError:
                    return host_string, RedisConfig.DEFAULT_REDIS_PORT
            return host_string, RedisConfig.DEFAULT_REDIS_PORT

    def connect(self) -> bool:
        # Circuit Breaker: Skip attempt if we failed recently to avoid blocking UI
        if time.time() - self.last_failure_time < self.retry_cooldown:
            return False

        if self.conn and self.ping():
            return True

        hostname, port = self._parse_host_string(self.host)
        self.status_update_signal.emit(
            f"Connecting to {self.description} at {hostname}:{port}..."
        )

        try:
            self.conn = Redis(
                host=hostname,
                port=port,
                decode_responses=True,
                socket_connect_timeout=RedisConfig.REDIS_CONNECT_TIMEOUT,
            )
            self.conn.ping()
            db_index = self.conn.connection_pool.connection_kwargs.get("db", 0)
            msg = f"Connected to {self.description} at {hostname}:{port} (DB: {db_index})."
            logger.info(msg)
            self.status_update_signal.emit(msg)
            # Reset failure time on success
            self.last_failure_time = 0 
            return True
        except redis_exceptions.ConnectionError as e:
            self.last_failure_time = time.time()
            self.status_update_signal.emit(f"{self.description} connection failed: {e}")
        except Exception as e:
            self.last_failure_time = time.time()
            self.status_update_signal.emit(
                f"Unexpected {self.description} connection error: {e}"
            )
        self.conn = None
        return False

    def ping(self) -> bool:
        if not self.conn:
            return False
        try:
            return self.conn.ping()
        except redis_exceptions.ConnectionError:
            self.status_update_signal.emit(
                f"{self.description} ping failed - connection likely lost."
            )
        except Exception as e:
            self.status_update_signal.emit(f"Error during {self.description} ping: {e}")
        self.conn = None
        return False

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                self.status_update_signal.emit(
                    f"Error closing {self.description} connection: {e}"
                )
            finally:
                self.conn = None
                self.status_update_signal.emit(f"{self.description} connection closed.")

    def get_connection(self) -> Optional[Redis]:
        return self.conn if self.connect() else None


class RedisAnalysisConnection(QtCore.QObject):
    status_update = QtCore.pyqtSignal(str)
    connection_error = QtCore.pyqtSignal(str)

    def __init__(
        self,
        status_update_signal: QtCore.pyqtSignal,
        connection_error_signal: QtCore.pyqtSignal,
    ):
        super().__init__()
        self.status_update = status_update_signal
        self.connection_error = connection_error_signal

        self.connection_pool: List[RedisConnection] = []

        # --- MODIFICATION: Define connection attempts in order of priority ---
        primary_host = RedisConfig.HOSTS.get("analysis_results")
        fallback_host = RedisConfig.HOSTS.get("analysis_fallback")

        if primary_host:
            self.connection_pool.append(
                RedisConnection(
                    primary_host, self.status_update, "Primary Analysis Redis"
                )
            )
        else:
            self.status_update.emit(
                "No 'analysis_results' (primary) Redis host configured."
            )

        if fallback_host:
            self.connection_pool.append(
                RedisConnection(
                    fallback_host, self.status_update, "Fallback Analysis Redis"
                )
            )
        else:
            self.status_update.emit("No 'analysis_fallback' Redis host configured.")

        if not self.connection_pool:
            self.connection_error.emit("No analysis Redis hosts are configured at all.")

    def get_analysis_connection(self) -> Optional[Redis]:
        """
        Tries to get a connection from the pool, starting with the primary.
        If the primary fails, it tries the fallback.
        Returns the first successful connection.
        """
        if not self.connection_pool:
            self.connection_error.emit(
                "Cannot get connection: No analysis hosts configured."
            )
            return None

        # Iterate through the connection objects in order (primary, then fallback)
        for redis_conn_obj in self.connection_pool:
            self.status_update.emit(
                f"Attempting to connect to {redis_conn_obj.description}..."
            )

            # The get_connection() method already contains the connect-and-ping logic
            connection = redis_conn_obj.get_connection()

            if connection:
                # Success! We found a working connection.
                self.status_update.emit(
                    f"Successfully connected to {redis_conn_obj.description}."
                )
                return connection  # Return the active connection immediately

            else:
                # This attempt failed, log it and the loop will try the next one.
                # Use status_update instead of connection_error to avoid intrusive popups
                # while we still have other servers to try.
                self.status_update.emit(
                    f"Connection to {redis_conn_obj.description} failed. Trying next available server."
                )

        # If the loop completes without returning, it means all connections failed.
        self.connection_error.emit("All analysis Redis servers are unavailable.")
        return None

    def close_analysis_connection(self):
        """Closes all configured analysis connections."""
        for redis_conn_obj in self.connection_pool:
            redis_conn_obj.close()


class RedisBluiceConnection(QtCore.QObject):
    status_update = QtCore.pyqtSignal(str)
    connection_error = QtCore.pyqtSignal(str)

    def __init__(
        self,
        status_update_signal: QtCore.pyqtSignal,
        connection_error_signal: QtCore.pyqtSignal,
    ):
        super().__init__()
        self.status_update = status_update_signal
        self.connection_error = connection_error_signal

        bluice_host = get_redis_server(location="redis")

        self.bluice_redis_connection_obj = None
        if not bluice_host:
            self.status_update.emit("No bluice Redis host configured.")
        else:
            self.bluice_redis_connection_obj = RedisConnection(
                bluice_host, self.status_update, "Bluice Redis"
            )

    def get_bluice_connection(self) -> Optional[Redis]:
        return (
            self.bluice_redis_connection_obj.get_connection()
            if self.bluice_redis_connection_obj
            else None
        )

    def close_bluice_connection(self):
        if self.bluice_redis_connection_obj:
            self.bluice_redis_connection_obj.close()


class RedisStreamManager(QtCore.QObject):
    new_master_file_stream = QtCore.pyqtSignal(str, dict)
    status_update = QtCore.pyqtSignal(str)
    connection_error = QtCore.pyqtSignal(str)
    start_poller = QtCore.pyqtSignal(str, str, list, list, object, tuple)

    run_started = QtCore.pyqtSignal(str, int, int, list, list)
    run_progress_25 = QtCore.pyqtSignal(str, int, int, list, list)
    run_progress_50 = QtCore.pyqtSignal(str, int, int, list, list)
    run_completed = QtCore.pyqtSignal(str, int, int, list, list)

    def __init__(
        self,
        status_update_signal: QtCore.pyqtSignal,
        connection_error_signal: QtCore.pyqtSignal,
    ):
        super().__init__()
        self.status_update = status_update_signal
        self.connection_error = connection_error_signal
        self._max_retries = 5
        self._retry_delay = 2
        self.last_stream_id_for_file_stream: Optional[int] = None
        self._monitoring_active = False
        self._last_stream_id = "$"
        self._monitor_thread = None
        self.active_runs: Dict[str, Dict[str, Any]] = {}

        self.connection_pool: List[RedisConnection] = []
        self._setup_connections()

        self.user_group_manager = UserGroupManager()

        self.pending_series = (
            {}
        )  # Format: {stream_series_id: {metadata, retries_remaining}}
        self.retry_timer = QtCore.QTimer()
        self.retry_timer.timeout.connect(self._retry_pending_series)
        self.max_retries = 30

        # BUG FIX: Move the timer to the main application thread to ensure it has an
        # event loop. This prevents the "QObject::startTimer" error.
        if QtCore.QCoreApplication.instance():
            self.retry_timer.moveToThread(QtCore.QCoreApplication.instance().thread())

        # BUG FIX: Start the timer safely using an invoked method call, which queues
        # the start command in the target thread's event loop.
        QtCore.QMetaObject.invokeMethod(
            self.retry_timer,
            "start",
            QtCore.Qt.QueuedConnection,
            QtCore.Q_ARG(int, 2000),
        )

    def _setup_connections(self):
        hostname = socket.gethostname()
        primary_ip = next(
            (
                ip
                for prefix, ip in RedisConfig.HOSTS.items()
                if prefix not in ["analysis_results", "analysis_fallback", "fallback_redis"]
                and hostname.startswith(prefix)
            ),
            None,
        )

        if primary_ip:
            self.connection_pool.append(
                RedisConnection(
                    primary_ip, self.status_update, f"Stream Redis ({primary_ip})"
                )
            )
        else:
            fallback = RedisConfig.HOSTS.get("fallback_redis")
            if fallback:
                self.connection_pool.append(
                    RedisConnection(
                        fallback, self.status_update, f"Stream Redis Fallback ({fallback})"
                    )
                )
                self.status_update.emit(
                    f"No primary Redis host for {hostname}. Using configured fallback: {fallback}"
                )
            else:
                self.status_update.emit(
                    f"No Redis host configured for {hostname} and no fallback_redis set. Live stream disabled."
                )
                return

        fallback_ip = RedisConfig.HOSTS.get("analysis_fallback", "127.0.0.1")
        # Add analysis fallback only if we have at least one connection established
        # and it's different from what we already have
        if self.connection_pool:
            current_ips = [c.host for c in self.connection_pool]
            if fallback_ip not in current_ips:
                self.connection_pool.append(
                    RedisConnection(
                        fallback_ip,
                        self.status_update,
                        f"Stream Redis Fallback ({fallback_ip})",
                    )
                )

    def get_working_connection(self) -> Optional[Redis]:
        """Iterates through connection pool to find a working connection."""
        for conn_obj in self.connection_pool:
            conn = conn_obj.get_connection()
            if conn:
                return conn
        return None

    def start_monitoring(self):
        if self._monitoring_active:
            self.status_update.emit("Monitoring is already active.")
            return
        self.status_update.emit("Starting Redis stream monitoring...")
        self._monitoring_active = True
        self._last_stream_id = "$"
        self.active_runs.clear()

        self._monitor_thread = threading.Thread(
            target=self._run_monitoring_loop, daemon=True
        )
        self._monitor_thread.start()

    def stop_monitoring(self):
        if not self._monitoring_active:
            return
        self.status_update.emit("Stopping Redis stream monitoring...")
        self._monitoring_active = False
        if self.retry_timer.isActive():
            # BUG FIX: Stop the timer safely using an invoked method call.
            QtCore.QMetaObject.invokeMethod(
                self.retry_timer, "stop", QtCore.Qt.QueuedConnection
            )
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=1.5)
        
        for conn in self.connection_pool:
            conn.close()
            
        self._monitor_thread = None
        self.status_update.emit("Redis stream monitoring stopped.")

    def get_recent_dataset_paths(self, count: int = 10) -> List[str]:
        """
        Scans recent Redis messages to find multiple unique and valid dataset paths.
        """
        redis_conn = self.get_working_connection()
        if not redis_conn:
            self.connection_error.emit(
                "Failed to get Redis connection for get_recent_dataset_paths."
            )
            return []

        recent_paths = []
        seen_paths = set()
        try:
            messages = redis_conn.xrevrange(
                RedisConfig.REDIS_STREAM_NAME, count=RedisConfig.REDIS_MESSAGE_COUNT * 6
            )
            for _, message_data_raw in messages:
                if len(recent_paths) >= count:
                    break
                parsed = self._parse_message_basics(message_data_raw)
                if parsed:
                    h5_master, data1_file, *rest = parsed
                    # Check for existence and uniqueness
                    if h5_master and h5_master not in seen_paths:
                        if Path(h5_master).exists() and Path(data1_file).exists():
                            recent_paths.append(h5_master)
                            seen_paths.add(h5_master)
            if not recent_paths:
                self.status_update.emit(
                    "No recent, valid HDF5 master files found (xrevrange)."
                )
            return recent_paths
        except Exception as e:
            self.status_update.emit(f"Error querying Redis stream (xrevrange): {e}")
        return []

    def get_latest_dataset_path(self) -> Optional[str]:
        redis_conn = self.get_working_connection()
        if not redis_conn:
            self.connection_error.emit(
                "Failed to get Redis connection for get_latest_dataset_path."
            )
            return None
        try:
            messages = redis_conn.xrevrange(
                RedisConfig.REDIS_STREAM_NAME, count=RedisConfig.REDIS_MESSAGE_COUNT
            )
            for _, message_data_raw in messages:
                parsed = self._parse_message_basics(message_data_raw)
                if parsed:
                    h5_master, data1_file, _, _, _, _, _, _, _ = parsed
                    # Use Path objects for checks
                    if (
                        h5_master
                        and data1_file
                        and Path(h5_master).exists()
                        and Path(data1_file).exists()
                    ):
                        return h5_master
            self.status_update.emit(
                "No recent, valid HDF5 master file found (xrevrange)."
            )
        except Exception as e:
            self.status_update.emit(f"Error querying Redis stream (xrevrange): {e}")
        return None

    def _run_monitoring_loop(self):
        retry_count = 0
        msgs_processed = 0
        last_rate_log_time = time.time()

        while self._monitoring_active:
            try:
                redis_conn = self.get_working_connection()
                if not redis_conn:
                    self._handle_connection_error(retry_count)
                    retry_count += 1
                    if retry_count >= self._max_retries:
                        self.connection_error.emit(
                            f"Stream: Max retries ({self._max_retries}) exceeded. Stopping monitor."
                        )
                        self._monitoring_active = False
                        break
                    continue
                retry_count = 0

                messages = redis_conn.xread(
                    {RedisConfig.REDIS_STREAM_NAME: self._last_stream_id},
                    block=1000,
                    count=100,
                )

                if not messages:
                    continue

                for _stream_name, message_list in messages:
                    for message_id, message_data_raw in message_list:
                        if not self._monitoring_active:
                            break

                        msgs_processed += 1
                        now = time.time()

                        # --- DIAGNOSTIC: Rate Logging ---
                        if now - last_rate_log_time > 10.0:
                            rate = msgs_processed / (now - last_rate_log_time)
                            logger.info(f"Stream processing rate: {rate:.2f} msgs/sec")
                            msgs_processed = 0
                            last_rate_log_time = now

                        parsed_basics = self._parse_message_basics(message_data_raw)
                        if not parsed_basics:
                            self._last_stream_id = message_id
                            continue

                        (
                            h5_master_file,
                            data1_file,
                            series_msg_prefix,
                            series_frame_num,
                            stream_series_id,
                            basic_run_prefix,
                            basic_run_fr_start,
                            msg_json_content,
                            img_data_json_content,
                        ) = parsed_basics

                        # --- DIAGNOSTIC: Lag Calculation ---
                        try:
                            msg_ts = msg_json_content.get("timestamp")
                            if msg_ts:
                                lag = now - float(msg_ts)
                                if lag > 5.0:
                                    logger.warning(
                                        f"High Lag Detected! Msg {message_id} (Frame {series_frame_num}) "
                                        f"lag is {lag:.2f}s. "
                                    )
                        except (ValueError, TypeError):
                            pass

                        if (
                            stream_series_id is not None
                            and h5_master_file
                            and data1_file
                            and self.last_stream_id_for_file_stream != stream_series_id
                        ):
                            trigger_meta_for_new_file = self._extract_metadata(
                                msg_json_content,
                                img_data_json_content,
                                h5_master_file,
                                data1_file,
                            )
                            if trigger_meta_for_new_file:
                                # Blocking I/O is in this thread, which is OK since this is not the main/GUI thread.
                                if self._files_accessible(h5_master_file, data1_file):
                                    logger.info(
                                        f"Series {stream_series_id}: files on disk immediately. "
                                        f"Master: {h5_master_file}"
                                    )
                                    self._emit_signal_for_series(
                                        h5_master_file,
                                        trigger_meta_for_new_file,
                                        stream_series_id,
                                    )
                                else:
                                    logger.warning(
                                        f"Series {stream_series_id}: master file NOT on disk at Redis message time. "
                                        f"Will poll every 2s (max {self.max_retries} retries = {self.max_retries * 2}s). "
                                        f"Master: {h5_master_file}"
                                    )
                                    self.pending_series[stream_series_id] = {
                                        "h5_master_file": h5_master_file,
                                        "data1_file": data1_file,
                                        "metadata": trigger_meta_for_new_file,
                                        "retries_remaining": self.max_retries,
                                        "enqueue_time": time.time(),
                                    }
                                    self.status_update.emit(
                                        f"Files for series {stream_series_id} not ready — waiting for filesystem."
                                    )

                            else:
                                self.status_update.emit(
                                    f"Could not extract metadata for new series ID {stream_series_id} (prefix {series_msg_prefix})."
                                )

                        if basic_run_prefix:
                            run_prefix = basic_run_prefix

                            if run_prefix not in self.active_runs:
                                if (
                                    series_frame_num == 0
                                    and stream_series_id is not None
                                ):
                                    series_start_meta = self._extract_metadata(
                                        msg_json_content,
                                        img_data_json_content,
                                        h5_master_file,
                                        data1_file,
                                    )
                                    if not series_start_meta:
                                        self.status_update.emit(
                                            f"Run '{basic_run_prefix}', Series '{series_msg_prefix}': Failed to extract full metadata for frame 0. Skipping run init."
                                        )
                                        self._last_stream_id = message_id
                                        continue

                                    run_total_frames = series_start_meta.get("n_images")
                                    if (
                                        run_total_frames is None
                                        or run_total_frames <= 0
                                    ):
                                        self.status_update.emit(
                                            f"Run '{basic_run_prefix}': Missing or invalid 'n_images' in frame 0 metadata. Skipping run init."
                                        )
                                        self._last_stream_id = message_id
                                        continue

                                    self.active_runs[run_prefix] = {
                                        "run_total_frames": run_total_frames,
                                        "processed_run_frames_count": 0,
                                        "processed_frame_identifiers": set(),
                                        "series_info_list": [
                                            {
                                                "series_prefix": series_msg_prefix,
                                                "frame0_metadata": series_start_meta,
                                                "stream_series_id": stream_series_id,
                                            }
                                        ],
                                        "collected_series_ids_for_metadata": {
                                            stream_series_id
                                        },
                                        "run_started_signal_emitted": False,
                                        "milestone_25_emitted": False,
                                        "milestone_50_emitted": False,
                                    }
                                    self.status_update.emit(
                                        f"Run '{run_prefix}': Tracking started. Expecting {run_total_frames} frames. First series: '{series_msg_prefix}'."
                                    )

                                    run_info_for_start = self.active_runs[run_prefix]
                                    first_frame_key = (
                                        stream_series_id,
                                        series_frame_num,
                                    )
                                    if (
                                        first_frame_key
                                        not in run_info_for_start[
                                            "processed_frame_identifiers"
                                        ]
                                    ):
                                        run_info_for_start[
                                            "processed_frame_identifiers"
                                        ].add(first_frame_key)
                                        run_info_for_start[
                                            "processed_run_frames_count"
                                        ] += 1

                                    first_series_info = run_info_for_start[
                                        "series_info_list"
                                    ][0]
                                    master_files_for_start = [
                                        mf
                                        for mf in [
                                            first_series_info["frame0_metadata"].get(
                                                "master_file"
                                            )
                                        ]
                                        if mf
                                    ]
                                    metadata_list_for_start = [
                                        first_series_info["frame0_metadata"]
                                    ]
                                    current_frames_at_start = run_info_for_start[
                                        "processed_run_frames_count"
                                    ]

                                    QtCore.QMetaObject.invokeMethod(
                                        self,
                                        "emit_run_started_signal_slot",
                                        QtCore.Qt.ConnectionType.QueuedConnection,
                                        QtCore.Q_ARG(str, run_prefix),
                                        QtCore.Q_ARG(int, current_frames_at_start),
                                        QtCore.Q_ARG(int, run_total_frames),
                                        QtCore.Q_ARG(list, master_files_for_start),
                                        QtCore.Q_ARG(list, metadata_list_for_start),
                                    )
                                    self.active_runs[run_prefix][
                                        "run_started_signal_emitted"
                                    ] = True
                                else:
                                    self.status_update.emit(
                                        f"Run '{basic_run_prefix}': First message is not frame 0 or lacks series_id. Run tracking deferred."
                                    )
                                    self._last_stream_id = message_id
                                    continue

                            run_info = self.active_runs[run_prefix]

                            if (
                                series_frame_num == 0
                                and stream_series_id is not None
                                and stream_series_id
                                not in run_info["collected_series_ids_for_metadata"]
                            ):
                                current_series_frame0_meta = self._extract_metadata(
                                    msg_json_content,
                                    img_data_json_content,
                                    h5_master_file,
                                    data1_file,
                                )
                                if current_series_frame0_meta:
                                    run_info["series_info_list"].append(
                                        {
                                            "series_prefix": series_msg_prefix,
                                            "frame0_metadata": current_series_frame0_meta,
                                            "stream_series_id": stream_series_id,
                                        }
                                    )
                                    run_info["collected_series_ids_for_metadata"].add(
                                        stream_series_id
                                    )
                                    self.status_update.emit(
                                        f"Run '{run_prefix}': Added metadata for series '{series_msg_prefix}' (ID: {stream_series_id})."
                                    )

                            frame_key = (stream_series_id, series_frame_num)
                            if frame_key not in run_info["processed_frame_identifiers"]:
                                run_info["processed_frame_identifiers"].add(frame_key)
                                run_info["processed_run_frames_count"] += 1

                            current_acc_frames = run_info["processed_run_frames_count"]
                            total_run_frames = run_info["run_total_frames"]

                            current_master_files_for_signal = [
                                str(Path(s["frame0_metadata"]["master_file"]).resolve())
                                for s in run_info["series_info_list"]
                                if "master_file" in s.get("frame0_metadata", {})
                            ]
                            current_metadata_list_for_signal = [
                                s["frame0_metadata"]
                                for s in run_info["series_info_list"]
                            ]

                            if total_run_frames > 0:
                                progress_percent = (
                                    current_acc_frames / total_run_frames
                                ) * 100

                                if progress_percent >= 25 and not run_info.get(
                                    "milestone_25_emitted"
                                ):
                                    run_info["milestone_25_emitted"] = (
                                        True  # Emit only once
                                    )
                                    self.status_update.emit(
                                        f"Run '{run_prefix}': 25% progress. Emitting signal."
                                    )
                                    self.emit_milestone_signal(
                                        self.emit_run_progress_25_signal_slot,
                                        run_prefix,
                                        current_acc_frames,
                                        total_run_frames,
                                        current_master_files_for_signal,
                                        current_metadata_list_for_signal,
                                    )

                                if progress_percent >= 50 and not run_info.get(
                                    "milestone_50_emitted"
                                ):
                                    run_info["milestone_50_emitted"] = (
                                        True  # Emit only once
                                    )
                                    self.status_update.emit(
                                        f"Run '{run_prefix}': 50% progress. Emitting signal."
                                    )
                                    self.emit_milestone_signal(
                                        self.emit_run_progress_50_signal_slot,
                                        run_prefix,
                                        current_acc_frames,
                                        total_run_frames,
                                        current_master_files_for_signal,
                                        current_metadata_list_for_signal,
                                    )

                            if current_acc_frames >= total_run_frames:
                                self.status_update.emit(
                                    f"Run '{run_prefix}' COMPLETED. Emitting final signal."
                                )
                                self.emit_milestone_signal(
                                    self.emit_run_completed_signal_slot,
                                    run_prefix,
                                    current_acc_frames,
                                    total_run_frames,
                                    current_master_files_for_signal,
                                    current_metadata_list_for_signal,
                                )
                                del self.active_runs[run_prefix]

                        self._last_stream_id = message_id
                        if not self._monitoring_active:
                            break
                if not self._monitoring_active:
                    break
            except redis_exceptions.ConnectionError:
                self.status_update.emit("Stream Redis connection lost, reconnecting...")
                self._handle_connection_error(retry_count)
                retry_count += 1
            except Exception as e:
                logger.exception("Unexpected error in stream monitoring loop.")
                time.sleep(self._retry_delay)
        self.status_update.emit("Redis stream monitoring loop finished.")

    def _files_accessible(self, h5_path: str, data1_path: str) -> bool:
        # Using pathlib for checks
        start_time = time.time()
        exists = Path(h5_path).exists() and Path(data1_path).exists()
        end_time = time.time()
        
        fs_lag = end_time - start_time
        if fs_lag > 0.1:
            logger.warning(
                f"High File System Latency! Existence check took {fs_lag:.3f}s for:\n"
                f"  Master: {h5_path}\n"
                f"  Data1:  {data1_path}"
            )
        return exists

    def _emit_signal_for_series(self, h5_path: str, metadata: dict, stream_id: int):
        QtCore.QMetaObject.invokeMethod(
            self,
            "emit_new_master_file_signal_slot",
            QtCore.Qt.ConnectionType.QueuedConnection,
            QtCore.Q_ARG(str, h5_path),
            QtCore.Q_ARG(dict, metadata),
        )
        self.last_stream_id_for_file_stream = stream_id
        self.status_update.emit(f"Emitted signal for new series {stream_id}")

    def _check_and_emit_pending(self, stream_id: int):
        """This function runs in a background thread to avoid blocking the GUI."""
        series = self.pending_series.get(stream_id)
        if not series:
            return

        if self._files_accessible(series["h5_master_file"], series["data1_file"]):
            wait_sec = time.time() - series.get("enqueue_time", time.time())
            retries_used = self.max_retries - series["retries_remaining"]
            logger.info(
                f"Series {stream_id}: files now on disk after {retries_used} retries "
                f"({wait_sec:.1f}s wait). Emitting signal."
            )
            self.status_update.emit(
                f"Series {stream_id}: files appeared after {wait_sec:.1f}s. Loading."
            )
            self._emit_signal_for_series(
                series["h5_master_file"], series["metadata"], stream_id
            )
            self.pending_series.pop(stream_id, None)
        else:
            series["retries_remaining"] -= 1
            if series["retries_remaining"] <= 0:
                msg = (
                    f"Abandoned series {stream_id} after max retries. "
                    f"Files found in Redis but inaccessible (check permissions?):\n"
                    f"Master: {series['h5_master_file']}"
                )
                self.status_update.emit(msg)
                # Escalate to connection_error to trigger a visible UI dialog
                self.connection_error.emit(msg)
                
                self.pending_series.pop(stream_id, None)

    def _retry_pending_series(self):
        """Timer-based slot that dispatches non-blocking checks."""
        for stream_id in list(self.pending_series.keys()):
            # Dispatch the check to a new thread to keep the event loop free
            threading.Thread(
                target=self._check_and_emit_pending, args=(stream_id,)
            ).start()

    def emit_milestone_signal(self, emit_func_slot, *args):
        """Helper to emit milestone signals via the Qt event loop."""
        QtCore.QMetaObject.invokeMethod(
            self,
            emit_func_slot.__name__,
            QtCore.Qt.QueuedConnection,
            QtCore.Q_ARG(str, args[0]),
            QtCore.Q_ARG(int, args[1]),
            QtCore.Q_ARG(int, args[2]),
            QtCore.Q_ARG(list, args[3]),
            QtCore.Q_ARG(list, args[4]),
        )

    def _parse_message_basics(
        self, message_data_raw: Dict[str, Any]
    ) -> Optional[Tuple]:
        try:
            message_json_content = json.loads(message_data_raw["message"])
            block0 = message_json_content.get("0", {})
            if block0.get("htype") != "dimage-1.0" or "4" not in message_json_content:
                return None

            img_data = message_json_content["4"]
            series_prefix = img_data.get("prefix")
            data_dir_root = img_data.get("data_dir")
            user_dir = img_data.get("user_dir")

            if not all([series_prefix, data_dir_root, user_dir is not None]):
                return None

            data_dir = Path(data_dir_root) / user_dir
            h5_master_path = str(data_dir / f"{series_prefix}_master.h5")
            data_file1_path = str(data_dir / f"{series_prefix}_data_000001.h5")

            return (
                h5_master_path,
                data_file1_path,
                series_prefix,
                int(block0.get("frame", -1)),
                int(s) if (s := block0.get("series")) is not None else None,
                img_data.get("run_prefix"),
                int(rfs) if (rfs := img_data.get("run_fr_start")) is not None else None,
                message_json_content,
                img_data,
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            return None

    def _extract_metadata(
        self,
        message_json_content: Dict[str, Any],
        img_data_json_content: Dict[str, Any],
        h5_master_path: str,
        data_file1_path: str,
    ) -> Optional[Dict[str, Any]]:
        try:
            block0 = message_json_content.get("0", {})
            metadata = {
                "frame": block0.get("frame"),
                "stream_series_id": block0.get("series"),
                "message_hash": block0.get("hash"),
                "htype": block0.get("htype"),
                "dtype": block0.get("type", np.uint32.__name__),
                "timestamp": message_json_content.get("timestamp", ""),
                "run_fr_start": img_data_json_content.get("run_fr_start"),
                "n_images": img_data_json_content.get("run_fr_count"),
                "n_images_per_series": img_data_json_content.get("series_fr_count"),
                "images_per_hdf": img_data_json_content.get("images_per_hdf", 1),
                "collect_mode": img_data_json_content.get("collect_mode", "STANDARD"),
                "beam_x": img_data_json_content.get("xbeam_px"),
                "beam_y": img_data_json_content.get("ybeam_px"),
                "robot_mounted": img_data_json_content.get("robot_mounted", ""),
                "exposure_sec": img_data_json_content.get("exposure_sec"),
                "data_dir_root": img_data_json_content.get("data_dir"),
                "data_rel_dir": img_data_json_content.get("user_dir"),
                "prefix": img_data_json_content.get("prefix"),
                "run_prefix": img_data_json_content.get("run_prefix"),
                "beamline": img_data_json_content.get("beamline"),
                "energy_ev": img_data_json_content.get("energy_eV"),
                "det_dist_m": img_data_json_content.get("detector_dist_m"),
                "master_file": h5_master_path,
                "data_file1": data_file1_path,
                "username": img_data_json_content.get("username"),
            }

            # adding data ownership information to metadata
            if self.user_group_manager and metadata.get("username"):
                try:
                    # Get the most recent ESAF info for the user
                    # in bluice, the user is the esaf group name
                    group_info = self.user_group_manager.groupinfo_from_groupname(
                        metadata["username"]
                    )
                    if group_info:
                        metadata["primary_group"] = group_info.get("group_name")
                        metadata["pi_badge"] = group_info.get("pi_badge")
                        metadata["esaf_id"] = group_info.get("esaf_number")
                        logger.info(
                            f"Enriched metadata for user '{metadata['username']}' with group info: {group_info.get('group_name')}"
                        )
                except Exception as e:
                    metadata["primary_group"] = metadata.get("username")
                    logger.warning(
                        f"Could not get group info for user '{metadata['username']}': {e}"
                    )

            for key in [
                "frame",
                "stream_series_id",
                "run_fr_start",
                "n_images",
                "n_images_per_series",
                "images_per_hdf",
            ]:
                if metadata.get(key) is not None:
                    try:
                        metadata[key] = int(metadata[key])
                    except (ValueError, TypeError):
                        pass

            for key in [
                "timestamp",
                "exposure_sec",
                "energy_ev",
                "det_dist_m",
                "beam_x",
                "beam_y",
            ]:
                if metadata.get(key) is not None:
                    try:
                        metadata[key] = float(metadata[key])
                    except (ValueError, TypeError):
                        pass

            return metadata
        except Exception as e:
            logger.warning(f"Warning: Error during metadata extraction: {e}")
            return None

    def _handle_connection_error(self, current_retry_count: int):
        if current_retry_count < self._max_retries:
            delay = min(self._retry_delay * (2**current_retry_count), 30)
            self.status_update.emit(f"Stream: Retrying Redis connection in {delay}s...")
            time.sleep(delay)

    def prepare_for_app_close(self):
        self.stop_monitoring()

    @QtCore.pyqtSlot(str, dict)
    def emit_new_master_file_signal_slot(self, file_path: str, metadata: dict):
        self.new_master_file_stream.emit(file_path, metadata)

    @QtCore.pyqtSlot(str, int, int, list, list)
    def emit_run_started_signal_slot(
        self,
        run_prefix: str,
        accumulated_frames: int,
        total_frames: int,
        master_files: List[str],
        metadata_list: List[Dict[str, Any]],
    ):
        self.run_started.emit(
            run_prefix, accumulated_frames, total_frames, master_files, metadata_list
        )

    @QtCore.pyqtSlot(str, int, int, list, list)
    def emit_run_progress_25_signal_slot(
        self,
        run_prefix: str,
        accumulated_frames: int,
        total_frames: int,
        master_files: List[str],
        metadata_list: List[Dict[str, Any]],
    ):
        self.run_progress_25.emit(
            run_prefix, accumulated_frames, total_frames, master_files, metadata_list
        )

    @QtCore.pyqtSlot(str, int, int, list, list)
    def emit_run_progress_50_signal_slot(
        self,
        run_prefix: str,
        accumulated_frames: int,
        total_frames: int,
        master_files: List[str],
        metadata_list: List[Dict[str, Any]],
    ):
        self.run_progress_50.emit(
            run_prefix, accumulated_frames, total_frames, master_files, metadata_list
        )

    @QtCore.pyqtSlot(str, int, int, list, list)
    def emit_run_completed_signal_slot(
        self,
        run_prefix: str,
        accumulated_frames: int,
        total_frames: int,
        master_files: List[str],
        completion_trigger_series_metadata_list: List[Dict[str, Any]],
    ):
        self.run_completed.emit(
            run_prefix,
            accumulated_frames,
            total_frames,
            master_files,
            completion_trigger_series_metadata_list,
        )


class RedisManager(QtCore.QObject):
    new_master_file_stream = QtCore.pyqtSignal(str, dict)
    run_started = QtCore.pyqtSignal(str, int, int, list, list)
    run_progress_25 = QtCore.pyqtSignal(str, int, int, list, list)
    run_progress_50 = QtCore.pyqtSignal(str, int, int, list, list)
    run_completed = QtCore.pyqtSignal(str, int, int, list, list)

    status_update = QtCore.pyqtSignal(str)
    connection_error = QtCore.pyqtSignal(str)

    def __init__(self):
        super().__init__()

        self.stream_manager = None
        self.analysis_connection_manager = None
        self.bluice_connection_manager = None

        # 1. Initialize Stream Manager (for live data collection events)
        try:
            self.stream_manager = RedisStreamManager(
                self.status_update,
                self.connection_error,
            )
            # Connect its signals only upon successful creation
            self.stream_manager.new_master_file_stream.connect(
                self.new_master_file_stream
            )
            self.stream_manager.run_started.connect(self.run_started)
            self.stream_manager.run_progress_25.connect(self.run_progress_25)
            self.stream_manager.run_progress_50.connect(self.run_progress_50)
            self.stream_manager.run_completed.connect(self.run_completed)
            self.status_update.emit("Redis Stream Manager initialized.")
        except RuntimeError as e:
            # This is the error you were seeing. Now it's handled gracefully.
            error_msg = f"Failed to initialize Redis Stream Manager (live updates disabled): {e}"
            self.status_update.emit(error_msg)
            self.connection_error.emit(f"Stream Manager Init Error: {e}")
            self.stream_manager = None  # Ensure it's None on failure

        # 2. Initialize Analysis Connection (for Dozor, Spot Finder results)
        try:
            self.analysis_connection_manager = RedisAnalysisConnection(
                self.status_update, self.connection_error
            )
            self.status_update.emit("Redis Analysis Connection Manager initialized.")
        except Exception as e:
            error_msg = f"Failed to initialize Redis Analysis Connection Manager: {e}"
            self.status_update.emit(error_msg)
            self.connection_error.emit(f"Analysis Connection Init Error: {e}")
            self.analysis_connection_manager = None

        # 3. Initialize Bluice Connection (for beamline parameters)
        try:
            self.bluice_connection_manager = RedisBluiceConnection(
                self.status_update, self.connection_error
            )
            self.status_update.emit("Redis Bluice Connection Manager initialized.")
        except Exception as e:
            error_msg = f"Failed to initialize Redis Bluice Connection Manager: {e}"
            self.status_update.emit(error_msg)
            self.connection_error.emit(f"Bluice Connection Init Error: {e}")
            self.bluice_connection_manager = None

    def start_monitoring(self):
        if self.stream_manager:
            self.stream_manager.start_monitoring()
        else:
            self.status_update.emit(
                "Cannot start monitoring: Stream manager not initialized."
            )

    def stop_monitoring(self):
        if self.stream_manager:
            self.stream_manager.stop_monitoring()

    def get_recent_dataset_paths(self, count: int = 20) -> List[str]:
        return (
            self.stream_manager.get_recent_dataset_paths(count)
            if self.stream_manager
            else []
        )

    def get_latest_dataset_path(self) -> Optional[str]:
        return (
            self.stream_manager.get_latest_dataset_path()
            if self.stream_manager
            else None
        )

    def get_analysis_connection(self) -> Optional[Redis]:
        """Safely gets the analysis connection, checking if the manager exists."""
        if self.analysis_connection_manager:
            return self.analysis_connection_manager.get_analysis_connection()
        return None

    def get_bluice_connection(self) -> Optional[Redis]:
        """Safely gets the bluice connection, checking if the manager exists."""
        if self.bluice_connection_manager:
            return self.bluice_connection_manager.get_bluice_connection()
        return None

    @property
    def is_monitoring_active(self) -> bool:
        """
        Returns True if the Redis stream monitoring is currently active.
        """
        if self.stream_manager:
            return self.stream_manager._monitoring_active
        return False

    def prepare_for_app_close(self):
        if self.stream_manager:
            self.stream_manager.prepare_for_app_close()
        if self.analysis_connection_manager:
            self.analysis_connection_manager.close_analysis_connection()
        if self.bluice_connection_manager:
            self.bluice_connection_manager.close_bluice_connection()
        self.status_update.emit("All Redis connections prepared for app close.")
