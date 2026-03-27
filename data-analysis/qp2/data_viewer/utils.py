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


def send_strategy_to_redis(beamline, options, redis_manager=None):
    """
    Sends strategy options to the bluice Redis server.

    Parameters
    ----------
    beamline : str
        Beamline identifier (e.g. "23i").
    options : dict
        Strategy options to publish (must include ``'id'`` key).
    redis_manager : RedisManager, optional
        If provided, reuses its bluice connection.  Otherwise creates
        a temporary RedisManager instance.
    """
    try:
        rm = redis_manager or RedisManager()
        conn = rm.get_bluice_connection()
        if conn is None:
            logger.error("No bluice Redis connection available.")
            return False
        redis_key = f"strategy_export:{beamline}:{options['id']}"
        conn.hset(redis_key, mapping=options)
        conn.expire(redis_key, 7 * 24 * 3600)  # 1-week expiration
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
