import os
import re
import json
import threading
import time
from typing import Dict, List, Optional
import requests
import redis
import uuid
import socket
from PyQt5 import QtWidgets, QtCore, QtGui
import markdown
from openai import OpenAI  # Import OpenAI
import yaml
import io
import contextlib

from qp2.utils.icon import generate_icon_with_text
from qp2.xio.redis_manager import RedisConfig
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.config.servers import ServerConfig
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class RedisChatHistory:
    """Manages chat history persistence using Redis."""

    def __init__(self, room_id=get_beamline_from_hostname()):
        self.room_id = room_id
        self.redis_client = None # Lazy connect
        self.key = f"ai_assistant:chat:{self.room_id}"
        self.channel_key = f"ai_assistant:chat_channel:{self.room_id}"
        
        user = os.environ.get("USER", "unknown")
        try:
            host = socket.gethostname().split('.')[0]
        except Exception:
            host = "unknown"
        self.username = f"{user}-{host}"
        self.presence_key = f"ai_assistant:presence:{self.room_id}"

        # Removed: self._ensure_connection() to make connection truly lazy

    def _ensure_connection(self):
        if self.redis_client:
            try:
                self.redis_client.ping()
                return self.redis_client
            except redis.ConnectionError:
                self.redis_client = None # Force reconnect

        try:
            # Use analysis_results as primary, fallback to localhost
            # We fetch hosts from RedisConfig which now uses ServerConfig
            host = RedisConfig.HOSTS.get("analysis_results", "127.0.0.1")
            client = redis.Redis(
                host=host, port=6379, decode_responses=True, socket_connect_timeout=2
            )
            client.ping()
            self.redis_client = client
            return client
        except Exception as e:
            logger.error(f"RedisChatHistory connection failed: {e}")
            return None

    def get_client(self):
        return self._ensure_connection()

    def update_presence(self, user: str, status: str):
        client = self.get_client()
        if not client:
            return
        try:
            if status == "join":
                client.sadd(self.presence_key, user)
            elif status == "leave":
                client.srem(self.presence_key, user)
        except Exception as e:
            logger.error(f"Presence update failed: {e}")

    def get_active_users(self) -> List[str]:
        client = self.get_client()
        if not client:
            return []
        try:
            return list(client.smembers(self.presence_key))
        except Exception:
            return []

    def add_message(self, role: str, content: str, user: str = None, msg_id: str = None):
        client = self.get_client()
        if not client:
            return

        if user is None:
            user = self.username if role == "user" else "AI"

        if msg_id is None:
            msg_id = str(uuid.uuid4())

        msg_data = {
            "role": role,
            "content": content,
            "user": user,
            "timestamp": time.time(),
            "msg_id": msg_id,
        }
        try:
            client.rpush(self.key, json.dumps(msg_data))
            # Keep only last 100 messages
            client.ltrim(self.key, -100, -1)
            # Publish to channel
            client.publish(self.channel_key, json.dumps(msg_data))
        except Exception as e:
            logger.error(f"Failed to save/publish message to Redis: {e}")

    def get_history(self, limit=50) -> List[Dict]:
        client = self.get_client()
        if not client:
            return []
        try:
            raw_msgs = client.lrange(self.key, -limit, -1)
            return [json.loads(m) for m in raw_msgs]
        except Exception as e:
            logger.error(f"Failed to retrieve history from Redis: {e}")
            return []

    def clear_history(self):
        client = self.get_client()
        if client:
            client.delete(self.key)

    def publish_event(self, event_type: str, content: str):
        client = self.get_client()
        if not client:
            return

        msg_id = str(uuid.uuid4())
        msg_data = {
            "role": "event",
            "event_type": event_type,
            "content": content,
            "user": self.username,
            "timestamp": time.time(),
            "msg_id": msg_id,
        }
        try:
            client.publish(self.channel_key, json.dumps(msg_data))
            return msg_id
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return None


class RedisListener(QtCore.QThread):
    message_received = QtCore.pyqtSignal(dict)
    connection_error = QtCore.pyqtSignal(str) # New signal for connection errors

    MAX_RETRIES = 2 # Max attempts after initial failure, so 3 total tries

    def __init__(self, redis_history):
        super().__init__()
        self.redis_history = redis_history
        self.running = True
        self.failure_count = 0 # Track consecutive connection failures

    def run(self):
        while self.running:
            try:
                client = self.redis_history.get_client()
                if not client:
                    self.failure_count += 1
                    if self.failure_count > self.MAX_RETRIES:
                        error_msg = f"Redis connection failed after {self.MAX_RETRIES + 1} attempts. AI Assistant will not be able to connect."
                        logger.error(f"RedisListener: {error_msg}")
                        self.connection_error.emit(error_msg)
                        self.running = False # Stop trying to connect
                        return # Exit the thread
                    else:
                        logger.warning(f"Redis connection attempt {self.failure_count}/{self.MAX_RETRIES + 1} failed. Retrying in 2 seconds...")
                        time.sleep(2) # Wait longer before retrying
                        continue # Try to get client again

                # If connection is successful, reset failure count
                self.failure_count = 0

                pubsub = client.pubsub()
                pubsub.subscribe(self.redis_history.channel_key)

                while self.running:
                    try:
                        message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                        if message and message["type"] == "message":
                            data = json.loads(message["data"])
                            self.message_received.emit(data)
                        elif message is None:
                            # Keepalive / sleep to prevent CPU spin
                            time.sleep(0.1)
                    except (redis.ConnectionError, redis.TimeoutError) as e:
                        logger.warning(f"Redis pubsub connection lost: {e}. Attempting to re-establish...")
                        break # Break inner loop to re-subscribe
                    except Exception as e:
                        logger.error(f"Redis listener (pubsub) error: {e}")
                        time.sleep(1)
                
                # Cleanup pubsub before reconnecting
                try:
                    pubsub.unsubscribe()
                    pubsub.close()
                except Exception:
                    pass # Ignore errors on close

            except Exception as e:
                # This catch is for initial client connection or pubsub creation errors
                self.failure_count += 1
                if self.failure_count > self.MAX_RETRIES:
                    error_msg = f"Redis listener critical failure after {self.MAX_RETRIES + 1} attempts: {e}. AI Assistant will not be able to connect."
                    logger.error(f"RedisListener: {error_msg}")
                    self.connection_error.emit(error_msg)
                    self.running = False # Stop trying to connect
                    return # Exit the thread
                else:
                    logger.error(f"Redis listener critical error ({self.failure_count}/{self.MAX_RETRIES + 1}): {e}. Retrying in 5 seconds...")
                    time.sleep(5)  # Wait longer before retrying everything

    def stop(self):
        self.running = False
        self.wait()


class AIClient:
    def __init__(
        self, api_key=os.environ.get("AI_API_KEY", os.environ.get("USER", None))
    ):
        self.api_key = api_key
        # Use the local AI API proxy base URL
        self.base_url = ServerConfig.get_ai_server_url()
        self.chat_completion_url = f"{self.base_url}/chat/completions"  # Still keep for models fetching if needed
        self.model_name = "argo:gpt-5-mini"  # Default model for proxy
        self.default_model = self.model_name

        self.openai_client = OpenAI(api_key=self.api_key, base_url=self.base_url)

    def set_api_key(self, key):
        self.api_key = key
        self.openai_client.api_key = key  # Update the client's API key

    def set_model(self, model_name):
        self.model_name = model_name

    def get_available_models(self):
        try:
            # Use direct requests for models endpoint, as openai client doesn't have a direct models() call for custom base_url
            headers = {
                "Authorization": f"Bearer {self.api_key}"  # Models endpoint might require auth too
            }
            response = requests.get(f"{self.base_url}/models", headers=headers)
            response.raise_for_status()
            models_data = response.json()
            model_ids = [model["id"] for model in models_data.get("data", [])]
            if self.default_model in model_ids:
                model_ids.remove(self.default_model)
                model_ids.insert(
                    0, self.default_model
                )  # Ensure default model is listed first
            return model_ids
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch available models: {e}")

    def check_health(self):
        try:
            # Construct health URL assuming base_url ends with /v1 or similar suffix
            base = self.base_url.rsplit('/', 1)[0]
            health_url = f"{base}/health"
            response = requests.get(health_url, timeout=2)
            if response.status_code == 200:
                data = response.json()
                return data.get("status") == "healthy"
        except Exception:
            pass
        return False

    def generate_code(self, messages):  # Now accepts conversation history as messages
        if not self.api_key:
            raise ValueError("API Key is not set.")

        full_response_content = ""
        try:
            stream = self.openai_client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                stream=True,
            )
            for chunk in stream:
                if not chunk.choices:
                    continue
                content = chunk.choices[0].delta.content or ""
                full_response_content += content
            return full_response_content
        except Exception as e:
            # Reraise as a more specific exception for UI handling
            raise Exception(f"API call failed: {e}")


class AIAssistantWidget(QtWidgets.QWidget):
    def __init__(self, namespace_provider, parent=None):
        super().__init__(parent)
        self.namespace_provider = namespace_provider  # function returning the dict
        self.client = AIClient()
        self.messages = []  # Initialize conversation history
        self.project_context: Dict[str, str] = (
            {}
        )  # Initialize project context (manual files)
        self.context_file_paths = []  # Track added context files
        self.rag_client = None  # Initialize RAG client
        self.rag_indexed_dir = ""  # Path of currently indexed RAG directory
        self.rag_dialog = None  # Lazy-loaded RAG settings dialog

        # Define System Instruction Templates
        self.system_templates = {
            "Code Generator": (
                "You are an expert Python coding assistant for a scientific image analysis application (PyQt5 + PyQtGraph). "
                "You have access to a python console namespace with the following variables:\n"
                "{available_vars_desc}\n\n"
                "Key objects and how to use them:\n"
                "- **`viewer`**: The main application window (DiffractionViewerWindow). Use it to access managers.\n"
                "  - **`viewer.graphics_manager`**: Manages display of images and overlays (spots, rings, annotations).\n"
                "    - To draw anything on the main image, use `viewer.graphics_manager.view_box.addItem(your_pyqtgraph_item)`.\n"
                "    - To clear current visuals: `viewer.graphics_manager.clear_all_visuals()`.\n"
                "    - To display spots: `viewer.graphics_manager.display_spots(spots_array)`.\n"
                "  - **`viewer.update_frame_display(frame_index)`**: To update the displayed frame programmatically.\n"
                "  - **`viewer.get_analysis_image()`**: Get the current image as a numpy array.\n"
                "  - **`viewer.get_params()`**: Get the current HDF5 metadata as a dictionary.\n"
                "- **`image`**: The current image data (numpy array). Directly available. Example: `image.shape`.\n"
                "- **`params`**: The current HDF5 metadata (dictionary). Directly available. Keys: `wavelength`, `distance`, `pixel_size`, `beam_x`, `beam_y`, `energy_eV`. Example: `params['wavelength']`.\n"
                "- **`np`**: The NumPy library, already imported. Example: `np.mean(image)`.\n"
                "- **`pg`**: The PyQtGraph library, already imported as `pg`. Example: `pg.QtGui.QGraphicsEllipseItem`.\n\n"
                "If the user asks to perform an image analysis or manipulation task:\n"
                "1. Write SHORT, CONCISE, and WORKING Python code to fulfill the request.\n"
                "2. Use the provided variables (image, params, viewer, np, pg, etc.).\n"
                "3. Wrap the code in a markdown block (```python ... ```).\n"
                "4. Do not provide lengthy explanations for the code unless asked.\n"
                "\n"
                "If the user asks a general question or says hello, answer normally and helpfully."
            ),
            "User Guide": (
                "You are a helpful assistant for the Diffraction Viewer application. "
                "Your primary role is to explain how to use the application, interpret data, and guide the user.\n"
                "You have access to the current application state:\n"
                "{available_vars_desc}\n\n"
                "Guidelines:\n"
                "- Explain features and workflows clearly.\n"
                "- Answer questions about the loaded image/metadata using the provided variables.\n"
                "- Do NOT generate Python code unless explicitly asked by the user.\n"
                "- Be concise and professional."
            ),
        }

        # Load Mode Setting
        settings = QtCore.QSettings("GMCA", "ImageViewer")
        self.current_mode = settings.value("ai_assistant_mode", "User Guide", type=str)
        if self.current_mode not in self.system_templates:
            self.current_mode = "User Guide"

        # Set initial template based on mode
        self.system_instruction_template = self.system_templates[self.current_mode]

        self._update_system_message_with_context()  # Set initial system message in self.messages

        # Attempt to load key from env
        env_key = os.environ.get("AI_API_KEY")
        if env_key:
            self.client.set_api_key(env_key)

        # Initialize Redis Chat History
        self.chat_history = RedisChatHistory()

        self.seen_msg_ids = set()
        
        self.is_muted = False # New attribute for mute state
        self.active_users = set() # Initialize empty, populate on start_listening
        self.history_loaded = False # Track if history has been loaded for this session

        self._setup_ui()
        # self._restore_chat_history() # Removed - lazy loaded in start_listening

        # Auto-load last indexed codebase
        settings = QtCore.QSettings("GMCA", "ImageViewer")
        last_rag_dir = settings.value("last_rag_directory", "", type=str)

        if not last_rag_dir or not os.path.isdir(last_rag_dir):
            try:
                from qp2.image_viewer.config import COMMON_RAG_CODEBASES

                if COMMON_RAG_CODEBASES and os.path.isdir(COMMON_RAG_CODEBASES[0]):
                    last_rag_dir = COMMON_RAG_CODEBASES[0]
            except (ImportError, IndexError):
                pass

        if last_rag_dir and os.path.isdir(last_rag_dir):
            if self.client.api_key:
                self._append_system_message(
                    f"Auto-loading index for '{os.path.basename(last_rag_dir)}'..."
                )
                thread = threading.Thread(
                    target=self._index_codebase_worker,
                    args=(last_rag_dir,),
                    kwargs={"force_refresh": False},
                )
                thread.start()
            else:
                self._append_system_message(
                    f"Found saved index for '{last_rag_dir}', but API Key is missing."
                )

        self.last_generated_code = ""

    def start_listening(self):
        # Prevent starting multiple listeners
        if hasattr(self, "redis_listener") and self.redis_listener.isRunning():
            return
        
        # Check for initial Redis client availability
        # This initial check will leverage the RedisChatHistory's _ensure_connection with its retries
        initial_client = self.chat_history.get_client()
        if not initial_client:
            error_msg = "AI Assistant: Initial connection to Redis failed. Chat features are unavailable."
            logger.error(error_msg)
            # Only show message box if this is the first attempt to start listening
            if not hasattr(self, '_first_redis_connect_attempted') or not self._first_redis_connect_attempted:
                QtWidgets.QMessageBox.warning(self, "Redis Connection Error", error_msg)
                self._first_redis_connect_attempted = True
            self._append_system_message(f"<span style='color:red;'>{error_msg}</span>")
            self.generate_btn.setEnabled(False)
            self.generate_btn.setText("Redis Offline")
            return
        
        self._first_redis_connect_attempted = True # Mark that an attempt has been made

        self.redis_listener = RedisListener(self.chat_history)
        self.redis_listener.message_received.connect(self._on_remote_message_received)
        self.redis_listener.connection_error.connect(self._on_redis_listener_error) # Connect new signal
        self.redis_listener.start()

        # Only load history once per session
        if not self.history_loaded:
            self._restore_chat_history()
            self.history_loaded = True
        
        # Update active users and publish join event
        current_users = self.chat_history.get_active_users()
        self.active_users = set(current_users)
        self.active_users.add(self.chat_history.username) # Ensure self is there
        self._update_active_users_display() # Update UI immediately

        join_msg = f"{self.chat_history.username} entered the room."
        join_msg_id = self.chat_history.publish_event("join", join_msg)
        
        # Register presence in Redis (only if successfully connected)
        self.chat_history.update_presence(self.chat_history.username, "join")

        if join_msg_id:
            self.seen_msg_ids.add(join_msg_id)
            # Manually display for local user if not already in history
            # (history loading already displays old messages)
            if not any(msg.get("msg_id") == join_msg_id for msg in self.messages):
                self.display_browser.append(
                    f"<div style='color: #95a5a6; text-align: center;'><i>{join_msg}</i></div>"
                )

    def stop_listening(self):
        if hasattr(self, "redis_listener"):
            # Before stopping, publish leave event and update presence
            if self.chat_history.get_client(): # Only if connected
                self.chat_history.publish_event(
                    "leave", f"{self.chat_history.username} has left the chat room."
                )
                self.chat_history.update_presence(
                    self.chat_history.username, "leave"
                )
            
            self.redis_listener.stop()
            self.redis_listener.wait()

    def _restore_chat_history(self):
        """Loads chat history from Redis and populates the display and context."""
        history = self.chat_history.get_history()
        if not history:
            return

        self._append_system_message(
            f"Restored {len(history)} messages from shared history."
        )
        for msg in history:
            role = msg.get("role")
            content = msg.get("content")
            user = msg.get("user", "User")
            msg_id = msg.get("msg_id")

            if msg_id:
                self.seen_msg_ids.add(msg_id)

            # Add to local LLM context (essential for memory)
            # Note: We append blindly here. A more sophisticated approach might limit context window size.
            self.messages.append({"role": role, "content": content})

            # Display in UI
            if role == "user":
                self.display_browser.append(
                    f"<div style='color: #3498db;'><b>{user}:</b> {content}</div><br>"
                )
            elif role == "assistant":
                # Render markdown for assistant
                try:
                    html = markdown.markdown(content, extensions=["fenced_code"])
                except Exception:
                    html = content
                self.display_browser.append(
                    f"<div style='color: #e67e22;'><b>AI:</b><br>{html}</div><br>"
                )
            elif role == "event":
                self.display_browser.append(
                    f"<div style='color: #95a5a6; text-align: center;'><i>{content}</i></div>"
                )

        # Scroll to bottom
        self.display_browser.moveCursor(QtGui.QTextCursor.End)

    @QtCore.pyqtSlot(str)
    def _on_redis_listener_error(self, error_msg):
        """Handle connection errors reported by the RedisListener."""
        QtWidgets.QMessageBox.warning(self, "Redis Connection Lost", error_msg)
        self._append_system_message(f"<span style='color:red;'>{error_msg}</span>")
        # Disable features that rely on Redis
        self.generate_btn.setEnabled(False)
        self.generate_btn.setText("Redis Offline")

    def _setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)

        # --- Model Toolbar ---
        model_layout = QtWidgets.QHBoxLayout()
        
        # Status Indicator
        self.status_label = QtWidgets.QLabel("●")
        self.status_label.setToolTip("Server Status: Unknown")
        self.status_label.setStyleSheet("color: gray; font-size: 16px;")
        model_layout.addWidget(self.status_label)
        
        model_layout.addWidget(QtWidgets.QLabel("Model:"))
        self.model_combo = QtWidgets.QComboBox()
        # Initial population and selection will be handled by _refresh_models_list()

        self.model_combo.currentTextChanged.connect(self._on_model_changed)
        model_layout.addWidget(self.model_combo)

        self.refresh_models_btn = QtWidgets.QPushButton("Refresh")
        self.refresh_models_btn.clicked.connect(self._refresh_models_list)
        self.refresh_models_btn.clicked.connect(self._check_server_status) # Also check status on refresh
        model_layout.addWidget(self.refresh_models_btn)

        # Settings Button (for API Key)
        self.settings_btn = QtWidgets.QPushButton()
        self.settings_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogDetailedView))
        self.settings_btn.setToolTip("Settings (API Key)")
        self.settings_btn.setFixedSize(25, 25)
        self.settings_btn.clicked.connect(self._open_settings_dialog)
        model_layout.addWidget(self.settings_btn)

        # RAG Settings Button
        self.rag_settings_btn = QtWidgets.QPushButton()
        self.rag_settings_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogInfoView))
        self.rag_settings_btn.setToolTip("RAG / Codebase Indexing")
        self.rag_settings_btn.setFixedSize(25, 25)
        self.rag_settings_btn.clicked.connect(self._open_rag_settings_dialog)
        model_layout.addWidget(self.rag_settings_btn)

        # Context Button
        self.context_btn = QtWidgets.QPushButton()
        self.context_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_FileIcon))
        self.context_btn.setToolTip("Add local files to AI context")
        self.context_btn.setFixedSize(25, 25)
        self.context_btn.clicked.connect(self._add_file_context)
        model_layout.addWidget(self.context_btn)

        # Save Chat Button
        self.save_chat_btn = QtWidgets.QPushButton()
        self.save_chat_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_DialogSaveButton))
        self.save_chat_btn.setToolTip("Save Chat History")
        self.save_chat_btn.setFixedSize(25, 25)
        self.save_chat_btn.clicked.connect(self._save_chat_history)
        model_layout.addWidget(self.save_chat_btn)

        # Clear Chat Button
        self.clear_chat_btn = QtWidgets.QPushButton()
        self.clear_chat_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_TrashIcon))
        self.clear_chat_btn.setToolTip("Clear Chat History (for everyone)")
        self.clear_chat_btn.setFixedSize(25, 25)
        self.clear_chat_btn.clicked.connect(self._clear_chat_history)
        model_layout.addWidget(self.clear_chat_btn)

        # Mute AI Button
        self.mute_ai_btn = QtWidgets.QPushButton()
        self.mute_ai_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_MediaVolume)) # Unmuted by default
        self.mute_ai_btn.setToolTip("Toggle AI Mute (local only)")
        self.mute_ai_btn.setFixedSize(25, 25)
        self.mute_ai_btn.clicked.connect(self._toggle_mute)
        model_layout.addWidget(self.mute_ai_btn)

        layout.addLayout(model_layout)

        # --- Chat History / Display ---
        self.display_browser = QtWidgets.QTextBrowser()
        self.display_browser.setOpenExternalLinks(True)
        layout.addWidget(self.display_browser)

        # --- Input Area ---
        input_layout = QtWidgets.QHBoxLayout()
        self.prompt_input = QtWidgets.QTextEdit()
        self.prompt_input.setPlaceholderText(
            "Ask AI to modify the image (e.g., 'Draw a red circle at the max intensity pixel')..."
        )
        self.prompt_input.setMaximumHeight(60)
        input_layout.addWidget(self.prompt_input)

        self.generate_btn = QtWidgets.QPushButton("Send")
        self.generate_btn.clicked.connect(self._start_generation)
        self.generate_btn.setShortcut(QtGui.QKeySequence("Ctrl+Return"))
        self.generate_btn.setFixedHeight(60) # Set fixed height
        input_layout.addWidget(self.generate_btn)
        layout.addLayout(input_layout)

        # --- Code Action Area ---
        action_layout = QtWidgets.QHBoxLayout()
        self.run_code_btn = QtWidgets.QPushButton("▶ Run Code")
        self.run_code_btn.setStyleSheet(
            "background-color: #2ecc71; color: white; font-weight: bold;"
        )
        self.run_code_btn.setEnabled(False)
        self.run_code_btn.clicked.connect(self._run_generated_code)

        self.copy_btn = QtWidgets.QPushButton("Copy Code")
        self.copy_btn.setEnabled(False)
        self.copy_btn.clicked.connect(self._copy_code)

        action_layout.addWidget(self.run_code_btn)
        action_layout.addWidget(self.copy_btn)
        layout.addLayout(action_layout)

        # Active Users Label
        self.active_users_label = QtWidgets.QLabel("Active Users: ")
        self.active_users_label.setStyleSheet("color: #7f8c8d; font-size: 10px;")
        layout.addWidget(self.active_users_label)

        self._refresh_models_list()  # Initial population of models
        self._check_server_status() # Initial health check
        self._update_active_users_display()

    def _check_server_status(self):
        # Run in thread to avoid blocking UI
        thread = threading.Thread(target=self._check_server_status_worker)
        thread.start()

    def _check_server_status_worker(self):
        is_healthy = self.client.check_health()
        QtCore.QMetaObject.invokeMethod(
            self,
            "_update_status_label",
            QtCore.Qt.QueuedConnection,
            QtCore.Q_ARG(bool, is_healthy),
        )

    @QtCore.pyqtSlot(bool)
    def _update_status_label(self, is_healthy):
        if is_healthy:
            self.status_label.setStyleSheet("color: #2ecc71; font-size: 16px;") # Green
            self.status_label.setToolTip("Server Status: Healthy")
        else:
            self.status_label.setStyleSheet("color: #e74c3c; font-size: 16px;") # Red
            self.status_label.setToolTip("Server Status: Unreachable/Unhealthy")
            # Only warn if it was previously considered healthy or unknown (to avoid spamming)
            # For now, just logging it to system message might be too noisy on every refresh if down.
            # So we rely on the visual indicator mainly.

    def _update_active_users_display(self):
        users_list = ", ".join(sorted(self.active_users))
        self.active_users_label.setText(f"Active Users: {users_list}")

    def _toggle_mute(self):
        self.is_muted = not self.is_muted
        if self.is_muted:
            self.mute_ai_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_MediaVolumeMuted))
            self.mute_ai_btn.setToolTip("AI is Muted (local only)")
            self._append_system_message("AI Assistant has been muted. It will not respond to questions.")
        else:
            self.mute_ai_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_MediaVolume))
            self.mute_ai_btn.setToolTip("Toggle AI Mute (local only)")
            self._append_system_message("AI Assistant has been unmuted. It will now respond to questions.")


    def _save_chat_history(self):
        filename, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "Save Chat History", os.getcwd(), "Markdown Files (*.md);;Text Files (*.txt)"
        )
        if not filename:
            return

        try:
            content = ""
            for msg in self.messages:
                role = msg.get("role")
                if role == "system":
                    continue
                user = "AI" if role == "assistant" else "User"
                content += f"**{user}:**\n{msg.get('content')}\n\n---\n\n"

            with open(filename, "w", encoding="utf-8") as f:
                f.write(content)
            
            self._append_system_message(f"Chat history saved to {filename}")
        except Exception as e:
            self._append_system_message(f"Error saving chat history: {e}")

    def _clear_chat_history(self):
        reply = QtWidgets.QMessageBox.question(
            self,
            "Clear History",
            "Are you sure you want to clear the chat history for EVERYONE in this room?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No,
        )

        if reply == QtWidgets.QMessageBox.Yes:
            self.chat_history.clear_history()
            self.chat_history.publish_event(
                "clear", f"Chat history cleared by {self.chat_history.username}"
            )
            # Local clear handled by event receipt to ensure sync, or force it here immediately?
            # It's better to force it immediately for the sender to give instant feedback.
            self._handle_clear_event()

    def _handle_clear_event(self):
        self.messages = []
        self.display_browser.clear()
        self._update_system_message_with_context() # Restore system prompt
        self.seen_msg_ids.clear()
        self._append_system_message("Chat history has been cleared.")

    def _on_model_changed(self, text):
        self.client.set_model(text)
        self._append_system_message(f"Model switched to {text}")

    def _refresh_models_list(self):
        # Disable the combo box and button during refresh
        self.model_combo.setEnabled(False)
        self.refresh_models_btn.setEnabled(False)
        self.refresh_models_btn.setText("Refreshing...")

        # Run in a separate thread to avoid freezing UI
        thread = threading.Thread(target=self._fetch_models_worker)
        thread.start()

    def _fetch_models_worker(self):
        try:
            available_models = self.client.get_available_models()
            QtCore.QMetaObject.invokeMethod(
                self,
                "_update_model_combo",
                QtCore.Qt.QueuedConnection,
                QtCore.Q_ARG(list, available_models),
            )
        except Exception as e:
            QtCore.QMetaObject.invokeMethod(
                self,
                "_handle_models_error",
                QtCore.Qt.QueuedConnection,
                QtCore.Q_ARG(str, str(e)),
            )

    @QtCore.pyqtSlot(list)
    def _update_model_combo(self, models):
        self.model_combo.clear()
        self.model_combo.addItems(models)

        # Attempt to set the client's current model if it's in the new list
        current_model = self.client.model_name
        index = self.model_combo.findText(current_model)
        if index >= 0:
            self.model_combo.setCurrentIndex(index)
        else:
            # If current model is not available, select the first one if any
            if models:
                self.client.set_model(models[0])
                self.model_combo.setCurrentIndex(0)
            self._append_system_message(
                f"Current model '{current_model}' not found. Defaulted to '{self.client.model_name}'."
            )

        self.model_combo.setEnabled(True)
        self.refresh_models_btn.setEnabled(True)
        self.refresh_models_btn.setText("Refresh Models")
        self._append_system_message("Model list updated.")

    @QtCore.pyqtSlot(str)
    def _handle_models_error(self, error_msg):
        self._append_system_message(f"<b>Error fetching models:</b> {error_msg}")
        self.model_combo.setEnabled(True)
        self.refresh_models_btn.setEnabled(True)
        self.refresh_models_btn.setText("Refresh Models")

    def _open_rag_settings_dialog(self):
        if self.rag_dialog is None:
            self.rag_dialog = QtWidgets.QDialog(self)
            self.rag_dialog.setWindowTitle("RAG / Codebase Indexing")
            layout = QtWidgets.QVBoxLayout(self.rag_dialog)

            self.index_code_btn = QtWidgets.QPushButton("Browse && Index New...")
            self.index_code_btn.setToolTip("Select a new directory to index")
            self.index_code_btn.clicked.connect(self._index_codebase)
            layout.addWidget(self.index_code_btn)

            # History Combo
            layout.addWidget(QtWidgets.QLabel("Recent Codebases:"))
            self.rag_dir_combo = QtWidgets.QComboBox()
            layout.addWidget(self.rag_dir_combo)

            # Load button for history
            load_btn = QtWidgets.QPushButton("Load Selected")
            load_btn.clicked.connect(
                lambda: self._load_rag_from_combo(force_refresh=False)
            )
            layout.addWidget(load_btn)

            # Force Reindex button
            force_reindex_btn = QtWidgets.QPushButton("Force Reindex Selected")
            force_reindex_btn.setToolTip(
                "Re-scan and re-index the selected directory, ignoring cache."
            )
            force_reindex_btn.setStyleSheet(
                "color: #e74c3c;"
            )  # Red text to indicate caution/expense
            force_reindex_btn.clicked.connect(
                lambda: self._load_rag_from_combo(force_refresh=True)
            )
            layout.addWidget(force_reindex_btn)

            # Initial population
            settings = QtCore.QSettings("GMCA", "ImageViewer")
            history = settings.value("rag_history", [], type=list)
            self._update_rag_combo(history)

            # Select currently loaded if any
            if self.rag_indexed_dir:
                index = self.rag_dir_combo.findText(self.rag_indexed_dir)
                if index >= 0:
                    self.rag_dir_combo.setCurrentIndex(index)

            close_btn = QtWidgets.QPushButton("Close")
            close_btn.clicked.connect(self.rag_dialog.close)
            layout.addWidget(close_btn)

        self.rag_dialog.show()
        self.rag_dialog.raise_()
        self.rag_dialog.activateWindow()

    def _load_rag_from_combo(self, force_refresh=False):
        selected_dir = self.rag_dir_combo.currentText()
        if selected_dir and os.path.isdir(selected_dir):
            if not self.client.api_key:
                self._append_system_message(
                    "Error: API Key is required to create embeddings for RAG."
                )
                return

            self.index_code_btn.setEnabled(False)
            action_text = "Re-indexing" if force_refresh else "Loading index for"
            self._append_system_message(
                f"{action_text} '{os.path.basename(selected_dir)}'..."
            )

            thread = threading.Thread(
                target=self._index_codebase_worker,
                args=(selected_dir,),
                kwargs={"force_refresh": force_refresh},
            )
            thread.start()
        else:
            self._append_system_message(
                f"Error: Selected directory '{selected_dir}' is invalid."
            )

    def _open_settings_dialog(self):
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle("AI Assistant Settings")
        layout = QtWidgets.QVBoxLayout(dialog)

        # API Key Input
        key_layout = QtWidgets.QHBoxLayout()
        key_layout.addWidget(QtWidgets.QLabel("API Key:"))

        api_key_input = QtWidgets.QLineEdit()
        api_key_input.setPlaceholderText("Enter API Key")
        api_key_input.setEchoMode(QtWidgets.QLineEdit.Password)

        # Default to current key or user environment variable
        current_key = self.client.api_key or os.environ.get("USER", "")
        api_key_input.setText(current_key)

        key_layout.addWidget(api_key_input)
        layout.addLayout(key_layout)

        # Assistant Mode Selection
        mode_layout = QtWidgets.QHBoxLayout()
        mode_layout.addWidget(QtWidgets.QLabel("Assistant Mode:"))
        mode_combo = QtWidgets.QComboBox()
        mode_combo.addItems(list(self.system_templates.keys()))
        mode_combo.setCurrentText(self.current_mode)
        mode_layout.addWidget(mode_combo)
        layout.addLayout(mode_layout)

        # Dialog Buttons
        buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
        )
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        if dialog.exec_() == QtWidgets.QDialog.Accepted:
            new_key = api_key_input.text().strip()
            if new_key:
                self.client.set_api_key(new_key)
                self._append_system_message("API Key updated via settings.")
            else:
                self._append_system_message("API Key cleared.")
                self.client.set_api_key("")

            # Mode Handling
            new_mode = mode_combo.currentText()
            if new_mode != self.current_mode:
                self.current_mode = new_mode
                settings = QtCore.QSettings("GMCA", "ImageViewer")
                settings.setValue("ai_assistant_mode", self.current_mode)

                # Update template reference
                self.system_instruction_template = self.system_templates[
                    self.current_mode
                ]
                # Rebuild context message
                self._update_system_message_with_context()
                self._append_system_message(f"Switched to '{self.current_mode}' mode.")

    def _append_user_message(self, text):
        msg_id = str(uuid.uuid4())
        self.seen_msg_ids.add(msg_id)

        self.messages.append({"role": "user", "content": text})  # Add to history
        self.chat_history.add_message("user", text, msg_id=msg_id)
        self.display_browser.append(
            f"<div style='color: #3498db;'><b>Me:</b> {text}</div><br>"
        )

    def _append_ai_message(self, html_content):
        self.display_browser.append(
            f"<div style='color: #e67e22;'><b>AI:</b><br>{html_content}</div><br>"
        )
        self.display_browser.moveCursor(QtGui.QTextCursor.End)

    def _append_system_message(self, text):
        # System messages are only for display, not added to self.messages directly as they are
        # managed by _update_system_message_with_context as the first message
        self.display_browser.append(f"<div style='color: #7f8c8d;'><i>{text}</i></div>")

    def _start_generation(self):
        user_prompt = self.prompt_input.toPlainText().strip()
        if not user_prompt:
            return

        if not self.client.api_key:  # Simplified API key check
            self._append_system_message("Error: Please set an API Key first.")
            return

        # Check if an image is loaded, as most AI image-related tasks require one
        ns = self.namespace_provider()
        current_image = ns.get("image")
        
        # Check if image is None
        if current_image is None:
            self._append_system_message(
                "Error: No image loaded. Please load an image in the viewer first."
            )
            return

        # Warn if using Mock Data but allow to proceed
        if isinstance(current_image, str) and "Mock Image Data" in current_image:
            self._append_system_message(
                "Warning: Running with Mock Image Data. Image analysis features may not work."
            )

        self._append_user_message(user_prompt)  # This now also updates self.messages
        self.prompt_input.clear()
        self.generate_btn.setEnabled(False)
        self.generate_btn.setText("Sent") # Acknowledge user input immediately

        if self.is_muted:
            self._append_system_message("AI Assistant is muted. It will not respond to your question.")
            self.generate_btn.setEnabled(True)
            self.generate_btn.setText("Send") # Reset to "Send" if muted
            return

        self.generate_btn.setText("Thinking...") # AI will process
        # Re-update system message with current context before sending, in case context changed
        self._update_system_message_with_context()

        rag_context = None  # Initialize to prevent UnboundLocalError

        # If RAG is enabled, retrieve relevant context for the user's prompt
        if self.rag_client and self.rag_indexed_dir:
            try:
                # Prepend the retrieved RAG context to the user's message
                rag_context = self.rag_client.build_context_string(user_prompt)
                if rag_context:
                    user_prompt = f"Context from codebase:\n{rag_context}\n\nUser's Request: {user_prompt}"
                    self._append_system_message("RAG context appended to user query.")
            except Exception as e:
                self._append_system_message(f"Error retrieving RAG context: {e}")

        # If RAG updated user_prompt, update the history (but don't re-display)
        if rag_context and self.messages:
            self.messages[-1]["content"] = user_prompt

        # Run in thread, passing the messages list directly
        thread = threading.Thread(target=self._generate_worker, args=(self.messages,))
        thread.start()

    def _generate_worker(self, messages):  # Now takes messages
        try:
            response_text = self.client.generate_code(
                messages
            )  # Pass messages directly
            QtCore.QMetaObject.invokeMethod(
                self,
                "_handle_response",
                QtCore.Qt.QueuedConnection,
                QtCore.Q_ARG(str, response_text),
            )
        except Exception as e:
            QtCore.QMetaObject.invokeMethod(
                self,
                "_handle_error",
                QtCore.Qt.QueuedConnection,
                QtCore.Q_ARG(str, str(e)),
            )

    @QtCore.pyqtSlot(dict)
    def _on_remote_message_received(self, msg_data):
        msg_id = msg_data.get("msg_id")
        if msg_id and msg_id in self.seen_msg_ids:
            return

        if msg_id:
            self.seen_msg_ids.add(msg_id)

        role = msg_data.get("role")
        content = msg_data.get("content")
        user = msg_data.get("user", "Unknown")

        # Handle Event (e.g., User Joined)
        if role == "event":
            event_type = msg_data.get("event_type")
            
            # Check for clear event
            if event_type == "clear":
                self._handle_clear_event()
                return
            
            # Update active users list
            if event_type == "join":
                self.active_users.add(user)
                self._update_active_users_display()
            elif event_type == "leave":
                self.active_users.discard(user)
                self._update_active_users_display()

            self.display_browser.append(
                f"<div style='color: #95a5a6; text-align: center;'><i>{content}</i></div>"
            )
            self._trigger_notification()
            return

        # Ensure sender is in active list
        if role == "user" and user not in self.active_users:
            self.active_users.add(user)
            self._update_active_users_display()

        # Add to local LLM context
        self.messages.append({"role": role, "content": content})

        # Display in UI
        if role == "user":
            self.display_browser.append(
                f"<div style='color: #3498db;'><b>{user}:</b> {content}</div><br>"
            )
        elif role == "assistant":
            try:
                html = markdown.markdown(content, extensions=["fenced_code"])
            except Exception:
                html = content
            self.display_browser.append(
                f"<div style='color: #e67e22;'><b>AI:</b><br>{html}</div><br>"
            )
            
            code_match = re.search(r"```python\n(.*?)```", content, re.DOTALL)
            if code_match:
                self.last_generated_code = code_match.group(1).strip()
                self.run_code_btn.setEnabled(True)
                self.copy_btn.setEnabled(True)

        self.display_browser.moveCursor(QtGui.QTextCursor.End)
        self._trigger_notification()

    def _trigger_notification(self):
        """Flashes the window taskbar icon and raises the window to the top if the window is not active."""
        window = self.window()
        
        # Un-minimize if necessary
        if window.isMinimized():
            window.showNormal()
            
        if not window.isActiveWindow():
            QtWidgets.QApplication.alert(window)
            window.raise_()
            window.activateWindow()

    @QtCore.pyqtSlot(str)
    def _handle_response(self, text):
        msg_id = str(uuid.uuid4())
        self.seen_msg_ids.add(msg_id)

        self.messages.append({"role": "assistant", "content": text})  # Add to history
        self.chat_history.add_message("assistant", text, msg_id=msg_id)
        self.generate_btn.setEnabled(True)
        self.generate_btn.setText("Send") # Reset button text to Send

        # Render Markdown
        html = markdown.markdown(text, extensions=["fenced_code"])
        self._append_ai_message(html)

        # Extract Code for execution
        code_match = re.search(r"```python\n(.*?)```", text, re.DOTALL)
        if code_match:
            self.last_generated_code = code_match.group(1).strip()
            self.run_code_btn.setEnabled(True)
            self.copy_btn.setEnabled(True)
        else:
            # Fallback: assume the whole text is code if it starts with import or usual logic,
            # but safer to require markdown blocks.
            self.last_generated_code = ""
            self.run_code_btn.setEnabled(False)
            self.copy_btn.setEnabled(False)

    @QtCore.pyqtSlot(str)
    def _handle_error(self, error_msg):
        self.generate_btn.setEnabled(True)
        self.generate_btn.setText("Send") # Reset button text to Send
        self._append_system_message(f"<b>Error:</b> {error_msg}")

    def _run_generated_code(self):
        if not self.last_generated_code:
            return

        ns = self.namespace_provider()
        try:
            self._append_system_message("Executing code...")

            # Capture stdout and stderr
            stdout_buffer = io.StringIO()
            stderr_buffer = io.StringIO()

            # Ensure __name__ is __main__ so that script-like code runs
            ns["__name__"] = "__main__"

            with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(
                stderr_buffer
            ):
                # Use ns as both globals and locals to support function closures properly
                exec(self.last_generated_code, ns)

            stdout_val = stdout_buffer.getvalue()
            stderr_val = stderr_buffer.getvalue()

            if stdout_val:
                self._append_system_message(
                    f"<b>Output:</b><br><pre>{stdout_val}</pre>"
                )
            if stderr_val:
                self._append_system_message(
                    f"<b>Errors (stderr):</b><br><pre style='color:red'>{stderr_val}</pre>"
                )

            self._append_system_message("Execution successful.")
        except Exception as e:
            self._append_system_message(
                f"<span style='color:red'>Execution Error: {e}</span>"
            )

    def _copy_code(self):
        if self.last_generated_code:
            clipboard = QtWidgets.QApplication.clipboard()
            clipboard.setText(self.last_generated_code)
            self._append_system_message("Code copied to clipboard.")

    def _add_file_context(self):
        dialog = QtWidgets.QFileDialog(self, "Select Files or Directory", os.getcwd())
        dialog.setFileMode(QtWidgets.QFileDialog.ExistingFiles)
        dialog.setNameFilter(
            "Code files (*.py *.md *.txt *.json *.yaml *.yml);;All files (*)"
        )

        if dialog.exec_():
            files = dialog.selectedFiles()
            if not files:
                return

            newly_added_count = 0
            for file_path in files:
                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        rel_path = os.path.relpath(file_path)
                        # Store/update content in the dictionary
                        self.project_context[rel_path] = (
                            f"--- File: {rel_path} ---\n{content}\n"
                        )
                        newly_added_count += 1
                except Exception as e:
                    self._append_system_message(f"Error reading {file_path}: {e}")

            if newly_added_count > 0:
                # Rebuild the list of file paths from the dictionary keys for display
                self.context_file_paths = list(self.project_context.keys())
                self._update_system_message_with_context()  # This will rebuild the project_context_string for AI

                file_list_html = (
                    "<ul>"
                    + "".join([f"<li>{f}</li>" for f in self.context_file_paths])
                    + "</ul>"
                )
                self._append_system_message(
                    f"Added/Updated {newly_added_count} files to context.<br><b>Current Context Files:</b>{file_list_html}"
                )
            else:
                self._append_system_message("No new files added to context.")

    def _index_codebase(self):
        directory = QtWidgets.QFileDialog.getExistingDirectory(
            self, "Select Codebase Directory", os.getcwd()
        )
        if not directory:
            return

        if not self.client.api_key:
            self._append_system_message(
                "Error: API Key is required to create embeddings for RAG."
            )
            return

        self.index_code_btn.setEnabled(False)
        self.rag_indexed_dir_input.setText(
            f"Indexing '{os.path.basename(directory)}'..."
        )
        self._append_system_message(f"Starting to index codebase at '{directory}'...")

        # Run indexing in a separate thread
        thread = threading.Thread(
            target=self._index_codebase_worker,
            args=(directory,),
            kwargs={"force_refresh": False},
        )
        thread.start()

    def _index_codebase_worker(self, directory, force_refresh=False):
        from qp2.image_viewer.ai.rag_helper import CodebaseRAG
        import redis
        from qp2.xio.redis_manager import RedisConfig

        redis_client = None
        try:
            # Try to connect to the analysis Redis server
            redis_host = RedisConfig.HOSTS.get("analysis_results", "127.0.0.1")
            redis_client = redis.Redis(
                host=redis_host, port=6379, decode_responses=False
            )
            redis_client.ping()  # Check connection
        except Exception as e:
            logger.warning(f"Warning: Could not connect to Redis for RAG caching: {e}")
            redis_client = None

        try:
            self.rag_client = CodebaseRAG(
                client=self.client.openai_client, redis_client=redis_client
            )
            self.rag_client.index_directory(directory, force_refresh=force_refresh)
            self.rag_indexed_dir = directory
            QtCore.QMetaObject.invokeMethod(
                self,
                "_handle_codebase_indexed",
                QtCore.Qt.QueuedConnection,
                QtCore.Q_ARG(str, directory),
            )
        except Exception as e:
            self.rag_client = None  # Clear client on error
            QtCore.QMetaObject.invokeMethod(
                self,
                "_handle_codebase_index_error",
                QtCore.Qt.QueuedConnection,
                QtCore.Q_ARG(str, str(e)),
            )

    @QtCore.pyqtSlot(str)
    def _handle_codebase_indexed(self, directory):
        self.rag_indexed_dir = directory  # Ensure this is set

        # Save to settings and update history
        settings = QtCore.QSettings("GMCA", "ImageViewer")
        settings.setValue("last_rag_directory", directory)

        history = settings.value("rag_history", [], type=list)
        if directory in history:
            history.remove(directory)
        history.insert(0, directory)
        # Limit history size
        history = history[:10]
        settings.setValue("rag_history", history)

        if self.rag_dialog:
            self._update_rag_combo(history)
            # Select the newly indexed directory
            index = self.rag_dir_combo.findText(directory)
            if index >= 0:
                self.rag_dir_combo.setCurrentIndex(index)

            # Re-enable button if dialog is open
            if hasattr(self, "index_code_btn"):
                self.index_code_btn.setEnabled(True)

        self._append_system_message(
            f"Codebase '{directory}' indexed successfully. {len(self.rag_client.knowledge_base)} chunks available."
        )
        # Trigger system message update to include RAG readiness
        self._update_system_message_with_context()

    def _update_rag_combo(self, history):
        if hasattr(self, "rag_dir_combo"):
            from qp2.image_viewer.config import COMMON_RAG_CODEBASES

            self.rag_dir_combo.clear()

            # Add Common Codebases
            if COMMON_RAG_CODEBASES:
                self.rag_dir_combo.addItems(COMMON_RAG_CODEBASES)
                self.rag_dir_combo.insertSeparator(len(COMMON_RAG_CODEBASES))

            # Add User History
            self.rag_dir_combo.addItems(history)

    @QtCore.pyqtSlot(str)
    def _handle_codebase_index_error(self, error_msg):
        self.rag_indexed_dir_input.setText("Indexing failed.")
        self.index_code_btn.setEnabled(True)
        self._append_system_message(f"<b>Error indexing codebase:</b> {error_msg}")

    # Helper method to update the system message with current context
    def _update_system_message_with_context(self):
        ns = self.namespace_provider()
        vars_desc = []
        for k, v in ns.items():
            if k.startswith("__"):
                continue
            type_name = type(v).__name__
            if k == "image" and hasattr(v, "shape"):
                desc = f"numpy array with shape {v.shape}"
            elif k == "params":
                desc = "dict containing metadata (wavelength, distance, etc)"
            else:
                desc = type_name
            vars_desc.append(f"- {k}: {desc}")
        context_str = "\n".join(vars_desc)

        # Append project file context if available
        if self.project_context:  # self.project_context is now a dict
            # Reconstruct the string from the dictionary values
            project_context_string = "\n".join(self.project_context.values())
            context_str += "\n\nProject Context Files:\n" + project_context_string

        # Inform AI if a codebase is indexed for RAG
        if self.rag_indexed_dir:
            context_str += f"\n\nNote: A codebase at '{os.path.basename(self.rag_indexed_dir)}' has been indexed for Retrieval-Augmented Generation (RAG). When answering questions about the codebase, relevant snippets will be provided to you."

        system_content = self.system_instruction_template.format(
            available_vars_desc=context_str
        )

        # Ensure system message is the first one, or add it if not present
        if not self.messages or self.messages[0]["role"] != "system":
            self.messages.insert(0, {"role": "system", "content": system_content})
        else:
            self.messages[0]["content"] = system_content


class AIAssistantWindow(QtWidgets.QMainWindow):
    closed = QtCore.pyqtSignal()

    def __init__(self, namespace_provider, parent=None):
        super().__init__(parent)
        self.assistant_widget = AIAssistantWidget(namespace_provider, self)
        self.setWindowTitle(f"GMCA ARGO AI Assistant - Room: {self.assistant_widget.chat_history.room_id}")

        # Set specific icon for the AI window
        icon = generate_icon_with_text(text="AI", bg_color="#9b59b6", size=64)
        self.setWindowIcon(icon)

        self.resize(600, 800)

        self.setCentralWidget(self.assistant_widget)
        
        # --- System Tray Setup ---
        self.tray_icon = QtWidgets.QSystemTrayIcon(self)
        self.tray_icon.setIcon(icon)
        
        # Tray Menu
        tray_menu = QtWidgets.QMenu()
        show_action = tray_menu.addAction("Show/Hide")
        show_action.triggered.connect(self._toggle_visibility)
        quit_action = tray_menu.addAction("Quit")
        quit_action.triggered.connect(self._force_quit)
        self.tray_icon.setContextMenu(tray_menu)
        
        # Tray Activation (Click)
        self.tray_icon.activated.connect(self._on_tray_activated)
        
        self.tray_icon.show()
        self._force_close = False

    def _toggle_visibility(self):
        if self.isVisible():
            self.hide()
        else:
            self.showNormal()
            self.activateWindow()

    def _on_tray_activated(self, reason):
        if reason == QtWidgets.QSystemTrayIcon.Trigger:
            self._toggle_visibility()

    def _force_quit(self):
        self._force_close = True
        self.close()

    def showEvent(self, event):
        self.assistant_widget.start_listening()
        super().showEvent(event)

    def closeEvent(self, event):
        # If running as a standalone widget (no parent) and not forced, check for standalone mode
        if hasattr(self, "standalone_mode") and self.standalone_mode:
             self._force_quit()
             event.accept()
             return

        if not self._force_close and self.parent() is None:
            if self.tray_icon.isVisible():
                self.hide()
                event.ignore()
                self.tray_icon.showMessage(
                    "AI Assistant",
                    "Application minimized to tray.",
                    QtWidgets.QSystemTrayIcon.Information,
                    2000
                )
                return

        # Publish leave event
        if hasattr(self.assistant_widget, "chat_history"):
            self.assistant_widget.chat_history.publish_event(
                "leave", f"{self.assistant_widget.chat_history.username} has left the chat room."
            )
            self.assistant_widget.chat_history.update_presence(
                self.assistant_widget.chat_history.username, "leave"
            )
        
        self.assistant_widget.stop_listening()
        self.closed.emit()
        super().closeEvent(event)


if __name__ == "__main__":
    import sys

    # Mock namespace for standalone testing
    def mock_namespace_provider():
        return {
            "image": "Mock Image Data (numpy array)",
            "params": {"wavelength": 0.9795, "distance": 200},
            "test_var": 123,
        }

    app = QtWidgets.QApplication(sys.argv)
    # Allow setting API key via env for testing
    window = AIAssistantWindow(mock_namespace_provider)
    window.show()
    sys.exit(app.exec_())
