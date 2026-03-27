# logging_config.py
# Default to stdlib logging; import Loguru only when explicitly requested.

import importlib
import logging
import logging.handlers
import multiprocessing
import sys
from pathlib import Path
from typing import Optional

_log_queue = None


def _prefixed_name(root_name: str, name: str) -> str:
    if name.startswith(f"{root_name}."):
        return name
    return f"{root_name}.{name}"


def setup_logging(
        root_name: str = "qp2",
        log_level: str = "DEBUG",
        log_file: Optional[str] = None,
        is_multiprocess_worker: bool = False,
        enable_loguru_bridge: bool = False,
        require_loguru: bool = False,
):
    """
    Configure stdlib logging as the default; optionally bridge Loguru into it.

    Args:
        root_name: Root logger name (e.g., 'qp2').
        log_level: Level name (e.g., 'INFO', 'DEBUG').
        log_file: Optional path to a log file.
        is_multiprocess_worker: True when called in child processes.
        enable_loguru_bridge: If True, import Loguru at runtime and forward into stdlib.
        require_loguru: If True and enable_loguru_bridge is True, raise if Loguru is absent.
    """
    global _log_queue

    if log_file is None:
        try:
            from qp2.config.servers import ServerConfig
            log_file = ServerConfig.LOG_FILE
        except ImportError:
            pass

    # --- Standard Library logging (default) ---
    root_logger = logging.getLogger(root_name)
    root_logger.setLevel(logging.getLevelName(log_level))
    root_logger.handlers.clear()
    root_logger.propagate = False

    if is_multiprocess_worker:
        if _log_queue is None:
            raise RuntimeError("Multiprocessing Queue not set up for worker.")
        queue_handler = logging.handlers.QueueHandler(_log_queue)
        root_logger.addHandler(queue_handler)
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        handlers: list[logging.Handler] = []

        console_handler = logging.StreamHandler(sys.stdout)
        handlers.append(console_handler)

        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file, mode="a")
            handlers.append(file_handler)

        for h in handlers:
            h.setFormatter(formatter)
            root_logger.addHandler(h)

    # --- Optional: bridge Loguru -> stdlib logging (lazy import) ---
    if enable_loguru_bridge:
        spec = importlib.util.find_spec("loguru")
        if spec is None:
            if require_loguru:
                raise ImportError(
                    "enable_loguru_bridge=True but 'loguru' is not installed"
                )
            # If not required, silently skip bridging
            return

        loguru = importlib.import_module("loguru")
        loguru_logger = loguru.logger
        loguru_logger.remove()

        def _loguru_sink(message):
            rec = message.record
            logger_name = _prefixed_name(root_name, rec["name"] or "loguru")
            log_record = logging.makeLogRecord(
                {
                    "name": logger_name,
                    "levelno": rec["level"].no,
                    "levelname": rec["level"].name,
                    "pathname": rec["file"].path,
                    "lineno": rec["line"],
                    "funcName": rec["function"],
                    "created": rec["time"].timestamp(),
                    "msg": rec["message"],
                    "args": (),
                    "exc_info": rec["exception"],
                    "stack_info": None,
                }
            )
            logging.getLogger(logger_name).handle(log_record)

        loguru_logger.add(_loguru_sink, level=log_level)


def get_logger(name: str):
    """
    Always return a stdlib logger for the given name, prefixed with 'qp2' if missing.
    """
    root_name = "qp2"
    return logging.getLogger(_prefixed_name(root_name, name))


def get_multiprocessing_queue():
    """Initialize and return the global multiprocessing queue for logging."""
    global _log_queue
    if _log_queue is None:
        _log_queue = multiprocessing.Queue(-1)
    return _log_queue


def start_queue_listener(log_queue, root_name: str = "qp2"):
    """
    Start a QueueListener that consumes LogRecord objects from 'log_queue'
    and emits them to the handlers configured on the root logger.
    Call once in the main process after setup_logging(..., is_multiprocess_worker=False).
    """
    root_logger = logging.getLogger(root_name)
    if not root_logger.handlers:
        raise RuntimeError("Root logger has no handlers; call setup_logging first.")

    listener = logging.handlers.QueueListener(log_queue, *root_logger.handlers)
    listener.start()
    return listener
