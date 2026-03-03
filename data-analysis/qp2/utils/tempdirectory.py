import shutil
import tempfile
from contextlib import contextmanager

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


@contextmanager
def temporary_directory(prefix=None, delete=True):
    """A context manager for a temporary directory that is conditionally deleted."""
    temp_dir = tempfile.mkdtemp(prefix=prefix)
    logger.debug(f"Created temporary directory: {temp_dir}")
    try:
        yield temp_dir
    finally:
        if delete:
            logger.debug(f"Deleting temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)
        else:
            logger.debug(f"Preserving temporary directory: {temp_dir}")
