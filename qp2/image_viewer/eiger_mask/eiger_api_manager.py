# qp2/image_viewer/eiger_mask/eiger_api_manager.py

import requests
import json
import numpy as np
from base64 import b64encode, b64decode

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class EigerAPIManager:
    """Manages communication with the EIGER detector Simplon API."""

    def __init__(self, ip, port, api_version="1.8.0"):
        self.base_url = f"http://{ip}:{port}/detector/api/{api_version}"
        self.timeout = 10

    def get_pixel_mask(self) -> np.ndarray:
        """Retrieves the current pixel mask from the detector."""
        url = f"{self.base_url}/config/pixel_mask"
        try:
            reply = requests.get(url, timeout=self.timeout)
            reply.raise_for_status()
            mask_data_dict = reply.json()["value"]

            decoded_data = b64decode(mask_data_dict["data"])
            dtype = np.dtype(str(mask_data_dict["type"]))
            shape = tuple(mask_data_dict["shape"])

            mask_array = np.frombuffer(decoded_data, dtype=dtype).reshape(shape)
            logger.info(f"Successfully retrieved mask with shape: {mask_array.shape}")
            return mask_array
        except Exception as e:
            logger.error(f"Failed to get EIGER mask: {e}", exc_info=True)
            raise ConnectionError(f"Failed to get EIGER mask: {e}") from e

    def set_pixel_mask(self, mask_array: np.ndarray):
        """Uploads a new pixel mask to the detector."""
        url = f"{self.base_url}/config/pixel_mask"
        payload = {
            "value": {
                "__darray__": (1, 0, 0),
                "type": mask_array.dtype.str,
                "shape": mask_array.shape,
                "filters": ["base64"],
                "data": b64encode(mask_array.tobytes()).decode("ascii"),
            }
        }
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.put(
                url, data=json.dumps(payload), headers=headers, timeout=self.timeout
            )
            response.raise_for_status()
            logger.info("Successfully set EIGER pixel mask.")
        except Exception as e:
            logger.error(f"Failed to set EIGER mask: {e}", exc_info=True)
            raise ConnectionError(f"Failed to set EIGER mask: {e}") from e
