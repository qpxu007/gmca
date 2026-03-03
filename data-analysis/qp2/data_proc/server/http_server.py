# qp2/data_proc/server/http_server.py

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import Any, Optional, TYPE_CHECKING

from qp2.log.logging_config import get_logger

if TYPE_CHECKING:
    from .data_processing_server import ProcessingServer

logger = get_logger(__name__)


class JobRequestHandler(BaseHTTPRequestHandler):
    """Handles incoming HTTP requests to launch jobs."""

    # This class-level attribute will be set by the factory function.
    processing_server_ref: Optional["ProcessingServer"] = None

    def do_POST(self):
        if self.path == "/launch_job":
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length == 0:
                self.send_response(411)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps(
                        {
                            "status": "error",
                            "message": "Content-Length header is required.",
                        }
                    ).encode("utf-8")
                )
                return

            post_data = self.rfile.read(content_length)
            try:
                job_data = json.loads(post_data.decode("utf-8"))

                if not self.processing_server_ref:
                    self.send_response(503)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps(
                            {
                                "status": "error",
                                "message": "Processing server not configured for handler.",
                            }
                        ).encode("utf-8")
                    )
                    return

                # Submit the job using the ProcessingServer's executor
                self.processing_server_ref.executor.submit(
                    self.processing_server_ref.launch_job_from_external_request,
                    job_data,
                )

                self.send_response(202)  # Accepted
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps(
                        {
                            "status": "success",
                            "message": "Job submission request accepted.",
                        }
                    ).encode("utf-8")
                )

            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps(
                        {"status": "error", "message": "Invalid JSON format."}
                    ).encode("utf-8")
                )
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps(
                        {
                            "status": "error",
                            "message": f"Error processing request: {str(e)}",
                        }
                    ).encode("utf-8")
                )
                logger.error(
                    f"Error handling external POST request: {e}", exc_info=True
                )
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Endpoint not found. Use POST /launch_job")

    def log_message(self, format_str: str, *args: Any):
        """Redirects HTTP server logs to the main application logger."""
        logger.info(f"HTTP Request: {format_str % args}")


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

    allow_reuse_address = True


class HTTPServerManager:
    """Manages the lifecycle of the HTTP server."""

    def __init__(self, port: int, server_instance: "ProcessingServer"):
        self.port = port
        self.server_instance = server_instance
        self._httpd: Optional[ThreadedHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Starts the HTTP server in a separate, non-daemon thread."""
        if self._thread and self._thread.is_alive():
            logger.warning("HTTP server is already running.")
            return

        try:
            # Factory to create the handler class with a reference to the processing server
            handler_class = self._make_handler_class()
            self._httpd = ThreadedHTTPServer(("", self.port), handler_class)

            self._thread = threading.Thread(
                target=self._httpd.serve_forever,
                name="HTTPServerThread",
                daemon=False,
            )
            self._thread.start()
            logger.info(f"HTTP server started successfully on port {self.port}.")

        except Exception as e:
            logger.error(
                f"Failed to start HTTP server on port {self.port}: {e}", exc_info=True
            )
            self._httpd = None
            self._thread = None
            raise  # Re-raise the exception to be handled by the caller

    def stop(self):
        """Stops the HTTP server gracefully."""
        if not self._httpd or not self._thread or not self._thread.is_alive():
            logger.info("HTTP server is not running.")
            return

        logger.info("Initiating HTTP server shutdown...")
        # shutdown() must be called from a different thread than serve_forever()
        shutdown_thread = threading.Thread(
            target=self._httpd.shutdown, name="HTTPShutdownThread"
        )
        shutdown_thread.start()

        # Wait for the main server thread to terminate
        self._thread.join()
        shutdown_thread.join()

        logger.info("HTTP server has been shut down.")
        self._httpd = None
        self._thread = None

    def _make_handler_class(self) -> type:
        """
        Factory function to create the JobRequestHandler class with a server reference.
        This is a nested function to keep the scope clean.
        """

        class CustomJobRequestHandler(JobRequestHandler):
            processing_server_ref = self.server_instance

        return CustomJobRequestHandler
