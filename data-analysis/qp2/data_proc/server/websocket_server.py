# qp2/data_proc/server/websocket_server.py
import logging
import asyncio
import threading
from typing import Set

import websockets
from pyqtgraph.Qt import QtCore

logger = logging.getLogger(__name__)


class WebSocketServerManager(QtCore.QObject):
    """
    Manages a WebSocket server to push real-time updates to connected clients.
    """

    new_message_to_send = QtCore.pyqtSignal(str)

    def __init__(self, port: int, server_instance: QtCore.QObject):
        super().__init__()
        self.port = port
        self.server_instance = server_instance  # Reference to the main ProcessingServer
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self.websocket_server = None
        self.loop: asyncio.AbstractEventLoop = None
        self.server_thread: threading.Thread = None

        # Connect internal signal to the async send method
        self.new_message_to_send.connect(self._sync_send_to_clients)

        # Connect the ProcessingServer's status updates to this manager
        self.server_instance.status_update.connect(self.send_status_update)

    def _sync_send_to_clients(self, message: str):
        """
        Synchronous wrapper to push messages to WebSocket clients.
        Called from a Qt signal, which typically operates in the main thread.
        """
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(self._send_to_clients(message), self.loop)
        else:
            logger.warning("WebSocket event loop not running, cannot send message.")

    async def _send_to_clients(self, message: str):
        """
        Asynchronously sends a message to all connected WebSocket clients.
        """
        if not self.clients:
            logger.debug("No WebSocket clients connected to send message.")
            return

        # Prepare a list of send coroutines
        send_tasks = [client.send(message) for client in list(self.clients)]

        # Run them concurrently and handle potential disconnections
        done, pending = await asyncio.wait(send_tasks, timeout=3)
        for task in done:
            if task.exception() is not None:
                # Handle clients that might have disconnected
                # Note: websockets library usually handles client removal on disconnect
                # but explicit error handling here can catch edge cases.
                logger.warning(f"Error sending to client: {task.exception()}")
        if pending:
            logger.warning(f"Timed out sending to {len(pending)} clients.")

    async def _register_client(self, websocket: websockets.WebSocketServerProtocol):
        """Registers a new client connection."""
        self.clients.add(websocket)
        logger.info(f"WebSocket client connected. Total clients: {len(self.clients)}")
        try:
            # Keep the connection open
            await websocket.wait_closed()
        finally:
            # Unregister the client when the connection is closed
            self.clients.remove(websocket)
            logger.info(
                f"WebSocket client disconnected. Total clients: {len(self.clients)}"
            )

    async def _start_websocket_server(self):
        """Starts the WebSocket server."""
        try:
            self.websocket_server = await websockets.serve(
                self._register_client, "0.0.0.0", self.port
            )
            logger.info(f"WebSocket server started on port {self.port}")
            await self.websocket_server.wait_closed()
        except asyncio.CancelledError:
            logger.info("WebSocket server task cancelled.")
        except Exception as e:
            logger.critical(f"Failed to start WebSocket server: {e}")

    def _run_server_loop(self):
        """Runs the asyncio event loop in a separate thread."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._start_websocket_server())
        except Exception as e:
            logger.critical(f"WebSocket server loop encountered an error: {e}")
        finally:
            self.loop.close()
            logger.info("WebSocket server event loop closed.")

    def start(self):
        """Starts the WebSocket server in a new thread."""
        if self.server_thread and self.server_thread.is_alive():
            logger.info("WebSocket server is already running.")
            return

        logger.info(f"Attempting to start WebSocket server on port {self.port}...")
        self.server_thread = threading.Thread(
            target=self._run_server_loop, name="WebSocketServerThread"
        )
        self.server_thread.start()
        logger.info("WebSocket server thread started.")

    def stop(self):
        """Stops the WebSocket server and its thread."""
        if not (self.server_thread and self.server_thread.is_alive()):
            logger.info("WebSocket server is not running.")
            return

        logger.info("Attempting to stop WebSocket server...")
        if self.websocket_server:
            # This will cause wait_closed() in _start_websocket_server to return
            self.websocket_server.close()
            # Cancel any pending tasks in the loop
            if self.loop and self.loop.is_running():
                for task in asyncio.all_tasks(self.loop):
                    task.cancel()
            logger.info("WebSocket server close initiated.")

        if self.server_thread:
            # Give some time for the loop to shut down gracefully
            self.server_thread.join(timeout=5)
            if self.server_thread.is_alive():
                logger.warning("WebSocket server thread did not terminate gracefully.")
            else:
                logger.info("WebSocket server thread stopped.")

    @QtCore.pyqtSlot(str)
    def send_status_update(self, message: str):
        """
        Slot to receive status updates from the ProcessingServer and
        emit them to WebSocket clients.
        """
        logger.debug(f"Received status update from ProcessingServer: {message}")
        self.new_message_to_send.emit(message)


if __name__ == "__main__":
    # Example usage:
    class MockProcessingServer(QtCore.QObject):
        status_update = QtCore.pyqtSignal(str)

        def __init__(self):
            super().__init__()
            self.counter = 0
            self.timer = QtCore.QTimer()
            self.timer.timeout.connect(self._emit_status)
            self.timer.start(1000)  # Emit every second

        def _emit_status(self):
            self.counter += 1
            self.status_update.emit(f"Mock status update {self.counter}")
            if self.counter > 5:
                # To test client disconnection, we can stop emitting
                self.timer.stop()
                print("Mock server stopped emitting status updates.")

    from qp2.config.servers import ServerConfig
    
    app = QtCore.QCoreApplication([])
    logging.basicConfig(level=logging.INFO)

    mock_server = MockProcessingServer()
    websocket_manager = WebSocketServerManager(port=ServerConfig.WEBSOCKET_PORT, server_instance=mock_server)
    websocket_manager.start()

    print(f"WebSocket server running on {ServerConfig.get_websocket_url()}")
    print("Connect with a WebSocket client to receive mock status updates.")
    print("Press Ctrl+C to exit.")

    try:
        sys.exit(app.exec_())
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        websocket_manager.stop()
        print("Shutdown complete.")
