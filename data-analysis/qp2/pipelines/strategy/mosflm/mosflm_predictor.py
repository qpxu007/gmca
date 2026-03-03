import re
import os
import socket
import time
import subprocess
import threading
from typing import Optional

from qp2.log.logging_config import get_logger
from qp2.config.programs import ProgramConfig


logger = get_logger(__name__)


class MosflmPredictor:
    """
    Encapsulates the logic to run MOSFLM in prediction mode for a single image.
    """

    def __init__(self, executable_path: str, workdir: str):
        self.executable_path = executable_path
        self.workdir = workdir

    def run(
        self,
        template: str,
        image_num: int,
        phi: float,
        osc: float,
        matrix_file: str,
        mosaicity: float,
        resolution: float,
    ) -> Optional[dict]:
        """
        Runs MOSFLM to get predictions for a specific image.

        Returns:
            A dictionary with 'fulls' and 'partials' lists of predictions, or None on failure.
        """
        commands_to_send = [
            f"DIRECTORY {os.path.dirname(template)}",
            f"TEMPLATE {os.path.basename(template)}",
            f"image {image_num} phi {phi:.2f} {phi + osc:.2f}",
            f"matrix {matrix_file}",
            f"mosaicity {mosaicity:.2f}",
            f"resolution {resolution:.2f}",
            "xgui on",
            "go",
            "predict_spots",
            "return",
            "exit",
        ]

        server_socket = None
        client_socket = None
        mosflm_process = None

        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind(("127.0.0.1", 0))
            server_socket.listen(1)
            host, port = server_socket.getsockname()

            # Use ProgramConfig to get setup command for CCP4
            setup_cmd = ProgramConfig.get_setup_command("ccp4")
            
            # Construct the execution command for ipmosflm in socket mode
            # Note: self.executable_path is usually 'ipmosflm'
            ipmosflm_cmd = f"{self.executable_path} MOSFLMSOCKET {port}"
            
            # Combine setup and execution into a full shell command
            full_command = f"{setup_cmd} && {ipmosflm_cmd}"

            mosflm_process = subprocess.Popen(
                ["bash", "-c", full_command],
                cwd=self.workdir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )

            # Use threads to consume stdout/stderr to prevent blocking
            stdout_thread = threading.Thread(
                target=self._pipe_reader, args=(mosflm_process.stdout, "[PREDICT_LOG]")
            )
            stderr_thread = threading.Thread(
                target=self._pipe_reader, args=(mosflm_process.stderr, "[PREDICT_ERR]")
            )
            stdout_thread.daemon = True
            stderr_thread.daemon = True
            stdout_thread.start()
            stderr_thread.start()

            server_socket.settimeout(15.0)
            client_socket, _ = server_socket.accept()
            server_socket.settimeout(None)

            for cmd in commands_to_send:
                client_socket.sendall((cmd + "\n").encode("utf-8"))
                time.sleep(0.1)

            all_socket_data = b""
            while True:
                response_chunk = client_socket.recv(65536)
                if not response_chunk:
                    break
                all_socket_data += response_chunk

            return self._parse_predictions(
                all_socket_data.decode("utf-8", errors="ignore")
            )

        except Exception as e:
            logger.error(f"Mosflm prediction failed for image {image_num}: {e}")
            return None
        finally:
            if client_socket:
                client_socket.close()
            if server_socket:
                server_socket.close()
            if mosflm_process:
                try:
                    mosflm_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    mosflm_process.kill()

    @staticmethod
    def _pipe_reader(pipe, prefix):
        """Helper to read pipe output in a thread."""
        try:
            for line in iter(pipe.readline, ""):
                # Suppress noisy log during prediction
                pass  # logger.debug(f"{prefix} {line.strip()}")
        except Exception:
            pass
        finally:
            pipe.close()

    @staticmethod
    def _parse_predictions(full_output_string: str) -> Optional[dict]:
        """Parses the full output string to find and extract prediction data."""
        predictions = {"fulls": [], "partials": []}
        prediction_block_pattern = re.compile(
            r"<prediction_response>(.*?)</prediction_response>", re.DOTALL
        )
        reflection_pattern = re.compile(
            r"(\d+)\s+(\d+)\s+([-\d]+)\s+([-\d]+)\s+([-\d]+)"
        )

        match = prediction_block_pattern.search(full_output_string)
        if not match:
            return None

        prediction_content = match.group(1)
        parts = re.split(r"</?fulls>|</?partials>", prediction_content)

        try:
            if len(parts) > 1 and parts[1].strip():
                matches = reflection_pattern.findall(parts[1])
                for m in matches:
                    predictions["fulls"].append(
                        {
                            "x": int(m[0]),
                            "y": int(m[1]),
                            "h": int(m[2]),
                            "k": int(m[3]),
                            "l": int(m[4]),
                        }
                    )

            if len(parts) > 3 and parts[3].strip():
                matches = reflection_pattern.findall(parts[3])
                for m in matches:
                    predictions["partials"].append(
                        {
                            "x": int(m[0]),
                            "y": int(m[1]),
                            "h": int(m[2]),
                            "k": int(m[3]),
                            "l": int(m[4]),
                        }
                    )

            return predictions
        except Exception:
            return None
