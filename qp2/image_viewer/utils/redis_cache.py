# qp2/image_viewer/utils/redis_cache.py
import pickle
from typing import Optional

import numpy as np
import redis

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

# Define a standard prefix for these cached items
CACHE_PREFIX = "viewer_cache:numpy"
CACHE_EXPIRATION_SECONDS = 3600  # Cache items for 1 hour


def save_numpy_array_to_redis(
        redis_host: str, redis_port: int, key: str, array: np.ndarray
) -> bool:
    """
    Connects to Redis, serializes a NumPy array, and saves it to a key.
    Manages its own BINARY connection.
    """
    if not redis_host:
        return False
    try:
        # Create a dedicated, short-lived binary connection
        redis_conn = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=False
        )
        pickled_array = pickle.dumps(array)
        full_key = f"{CACHE_PREFIX}:{key}"
        redis_conn.setex(full_key, CACHE_EXPIRATION_SECONDS, pickled_array)
        logger.info(f"Successfully cached NumPy array to Redis key: {full_key}")
        return True
    except Exception as e:
        logger.error(
            f"Failed to save NumPy array to Redis key '{key}': {e}", exc_info=True
        )
        return False


def load_numpy_array_from_redis(
        redis_host: str, redis_port: int, key: str
) -> Optional[np.ndarray]:
    """
    Connects to Redis, loads, and deserializes a NumPy array from a key.
    Manages its own BINARY connection.
    """
    if not redis_host:
        return None
    try:
        # Create a dedicated, short-lived binary connection
        redis_conn = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=False
        )
        full_key = f"{CACHE_PREFIX}:{key}"
        pickled_array = redis_conn.get(full_key)

        if pickled_array is None:
            logger.warning(f"Numpy array cache miss for key: {full_key}")
            return None

        array = pickle.loads(pickled_array)
        logger.info(f"Successfully loaded NumPy array from Redis cache: {full_key}")
        return array
    except Exception as e:
        logger.error(
            f"Failed to load NumPy array from Redis key '{key}': {e}", exc_info=True
        )
        return None
