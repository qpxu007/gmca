import json
import os
import socket
from typing import Optional

from redis import Redis, exceptions as redis_exceptions

from qp2.log.logging_config import get_logger
from qp2.config.servers import ServerConfig

logger = get_logger(__name__)


class RedisConfig:
    HOSTS = ServerConfig.get_redis_hosts()
    REDIS_STREAM_NAME = "eiger"
    REDIS_MESSAGE_COUNT = 500
    REDIS_CONNECT_TIMEOUT = 5  # seconds


def get_redis() -> Optional[Redis]:
    """
    Connects to the appropriate Redis server based on the hostname.

    Returns:
        Redis connection object or None if hostname doesn't match or connection fails.

    Raises:
        RuntimeError: If no suitable Redis host is found for the current machine.
        redis_exceptions.ConnectionError: If the Redis server cannot be reached.
    """
    hostname = socket.gethostname()
    redis_host = next(
        (ip for prefix, ip in RedisConfig.HOSTS.items() if hostname.startswith(prefix)),
        None,
    )
    if not redis_host:
        msg = f"No Redis host configured for hostname {hostname}"
        logger.error(msg)
        raise RuntimeError(msg)

    try:
        logger.info("Attempting to connect to Redis at %s", redis_host)
        eiger_redis = Redis(
            host=redis_host,
            decode_responses=True,
            socket_connect_timeout=RedisConfig.REDIS_CONNECT_TIMEOUT,
        )
        eiger_redis.ping()  # Check connection
        logger.info("Successfully connected to Redis at %s", redis_host)
        return eiger_redis
    except redis_exceptions.ConnectionError as e:
        logger.error("Failed to connect to Redis at %s: %s", redis_host, e)
        raise  # Re-raise the specific connection error
    except Exception as e:
        logger.exception(
            "An unexpected error occurred during Redis connection to %s", redis_host
        )
        raise RuntimeError(
            f"Unexpected error connecting to Redis at {redis_host}"
        ) from e


def get_latest_collected_image():
    eiger_redis = get_redis()
    if not eiger_redis:
        return

    messages = eiger_redis.xrevrange(
        RedisConfig.REDIS_STREAM_NAME,
        max="+",
        min="-",
        count=RedisConfig.REDIS_MESSAGE_COUNT,
    )
    for message in messages:
        message_json = json.loads(message[1]["message"])
        if message_json["0"]["htype"] != "dimage-1.0" or "4" not in message_json:
            continue

        try:
            img_data = message_json["4"]
            frame = message_json["0"]["frame"]
            run_fr = img_data["run_fr_start"] + frame

            data_dir_root = img_data["data_dir"]
            data_rel_dir = img_data["user_dir"]
            prefix = img_data["prefix"]

            data_dir = os.path.join(data_dir_root, data_rel_dir)
            h5_master_file = f"{prefix}_master.h5"
            h5_master_file = os.path.join(data_dir, h5_master_file)
            if os.path.exists(h5_master_file) and os.access(h5_master_file, os.R_OK):
                return h5_master_file
            else:
                logger.error(f"{h5_master_file} does not exist or not readable")
        except Exception as e:
            pass
