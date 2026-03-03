import os
import socket
import subprocess

import redis

try:
    from qp2.log.logging_config import get_logger
    from qp2.xio.redis_manager import RedisManager
    from qp2.config.servers import ServerConfig

    logger = get_logger(__name__)
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


def get_rpc_url():
    """
    Returns the PBS RPC URL from the central configuration.
    """
    return ServerConfig.get_pbs_rpc_url()


def send_strategy_to_redis(beamline, options):
    """
    Connects to Redis and sends the strategy options.
    NOTE: You may need to configure the host, port, and password for your Redis instance.
    """
    try:
        with RedisManager().get_bluice_connection() as r:
            # Example of sending data: using a specific key for the beamline
            # This part needs to be adapted to your actual Redis schema/logic
            redis_key = f"strategy_export:{beamline}:{options['id']}"
            r.hset(redis_key, mapping=options)
            r.expire(redis_key, 7 * 24 * 3600)  # 1-week expiration
            logger.info(f"Sent to Redis on key {redis_key}: {options}")
            return True
    except Exception as e:
        logger.error(f"ERROR: Could not send data to Redis. {e}")
        return False


def make_path_relative(target_path, base_dir):
    """
    Converts an absolute target_path to a relative path from base_dir.
    Returns the original path if it cannot be made relative.
    """
    if not target_path or not base_dir:
        return target_path
    try:
        # os.path.relpath is the perfect tool for this
        relative_path = os.path.relpath(target_path, base_dir)
        return relative_path
    except ValueError:
        # This can happen on Windows if paths are on different drives
        return target_path
