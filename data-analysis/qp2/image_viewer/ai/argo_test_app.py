import sys
import os
import requests
import threading
from PyQt5 import QtWidgets, QtCore, QtGui
from openai import OpenAI

class ArgoTestApp(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Argo Service Test App")
        self.resize(600, 700)
        self.client = None
        self.models = []
        
        # Default configuration
        self.default_base_url = "https://apps.inside.anl.gov/argoapi/v1"
        self.default_user = os.environ.get("USER", "")

        self._setup_ui()

    def _setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)

        # Configuration Area
        config_group = QtWidgets.QGroupBox("Configuration")
        config_layout = QtWidgets.QFormLayout()

        self.api_key_input = QtWidgets.QLineEdit(self.default_user)
        self.api_key_input.setPlaceholderText("Enter ANL Username (API Key)")
        config_layout.addRow("API Key (User):", self.api_key_input)

        self.base_url_input = QtWidgets.QLineEdit(self.default_base_url)
        config_layout.addRow("Base URL:", self.base_url_input)

        self.connect_btn = QtWidgets.QPushButton("Connect && Fetch Models")
        self.connect_btn.clicked.connect(self._connect_and_fetch)
        config_layout.addRow(self.connect_btn)

        self.model_combo = QtWidgets.QComboBox()
        config_layout.addRow("Model:", self.model_combo)

        self.embed_btn = QtWidgets.QPushButton("Test Embedding")
        self.embed_btn.clicked.connect(self._test_embedding)
        self.embed_btn.setEnabled(False)
        config_layout.addRow(self.embed_btn)

        config_group.setLayout(config_layout)
        layout.addWidget(config_group)

        # Chat Area
        self.chat_display = QtWidgets.QTextBrowser()
        layout.addWidget(self.chat_display)

        input_layout = QtWidgets.QHBoxLayout()
        self.message_input = QtWidgets.QTextEdit()
        self.message_input.setMaximumHeight(60)
        self.message_input.setPlaceholderText("Type your message here...")
        input_layout.addWidget(self.message_input)

        self.send_btn = QtWidgets.QPushButton("Send")
        self.send_btn.clicked.connect(self._send_message)
        self.send_btn.setEnabled(False) # Disabled until connected
        input_layout.addWidget(self.send_btn)

        layout.addLayout(input_layout)

        # Status Bar
        self.status_label = QtWidgets.QLabel("Ready to connect.")
        layout.addWidget(self.status_label)

    @QtCore.pyqtSlot(str, str)
    def _log(self, message, color="black"):
        self.chat_display.append(f"<span style='color:{color}'>{message}</span>")

    def _connect_and_fetch(self):
        api_key = self.api_key_input.text().strip()
        base_url = self.base_url_input.text().strip()

        if not api_key:
            QtWidgets.QMessageBox.warning(self, "Error", "Please enter an API Key (Username).")
            return

        self.status_label.setText("Connecting...")
        self.connect_btn.setEnabled(False)
        self.send_btn.setEnabled(False)
        self.model_combo.clear()

        # Initialize Client
        try:
            self.client = OpenAI(
                api_key=api_key,
                base_url=base_url
            )
            self._log(f"Initialized OpenAI client with base_url: {base_url}", "gray")
        except Exception as e:
            self._log(f"Error initializing client: {e}", "red")
            self.status_label.setText("Initialization Failed")
            self.connect_btn.setEnabled(True)
            return

        # Fetch Models (Run in background to avoid freezing)
        threading.Thread(target=self._fetch_models_worker).start()

    def _fetch_models_worker(self):
        try:
            # Try standard OpenAI list
            try:
                QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, "Attempting to fetch models via client.models.list()..."), QtCore.Q_ARG(str, "blue"))
                models_response = self.client.models.list()
                models = [m.id for m in models_response.data]
            except Exception as e:
                QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"client.models.list() failed: {e}"), QtCore.Q_ARG(str, "orange"))
                # Fallback: Manual request to the models endpoint mentioned by user
                # User said: https://apps-dev.inside.anl.gov/argoapi/api/v1/models/
                # Check if base_url ends in /v1 or /api/v1
                base = self.client.base_url
                if str(base).endswith("/v1") or str(base).endswith("/v1/"):
                     # If base is .../v1/, construct .../api/v1/models/ if needed, or just .../models
                     # But user said specific URL. Let's try requests.
                     manual_url = "https://apps-dev.inside.anl.gov/argoapi/api/v1/models/"
                     QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"Fallback: Requesting {manual_url}"), QtCore.Q_ARG(str, "blue"))
                     headers = {"Authorization": f"Bearer {self.client.api_key}"}
                     resp = requests.get(manual_url, headers=headers, timeout=10)
                     resp.raise_for_status()
                     data = resp.json()
                     # Assume data is list of strings or dicts with 'id'
                     if isinstance(data, list):
                         models = [m['id'] if isinstance(m, dict) else m for m in data]
                     elif 'data' in data:
                         models = [m['id'] for m in data['data']]
                     else:
                         models = []

            if models:
                QtCore.QMetaObject.invokeMethod(self, "_on_models_fetched", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(list, models))
            else:
                QtCore.QMetaObject.invokeMethod(self, "_on_models_error", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, "No models found in response."))

        except Exception as e:
            QtCore.QMetaObject.invokeMethod(self, "_on_models_error", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, str(e)))

    @QtCore.pyqtSlot(list)
    def _on_models_fetched(self, models):
        self.models = models
        self.model_combo.addItems(self.models)
        self.status_label.setText(f"Connected. Found {len(models)} models.")
        self.connect_btn.setEnabled(True)
        self.send_btn.setEnabled(True)
        self.embed_btn.setEnabled(True)
        QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"Successfully fetched models: {', '.join(models[:5])}..."), QtCore.Q_ARG(str, "green"))

    @QtCore.pyqtSlot(str)
    def _on_models_error(self, error_msg):
        self.status_label.setText("Connection Failed")
        self.connect_btn.setEnabled(True)
        QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"Error fetching models: {error_msg}"), QtCore.Q_ARG(str, "red"))

    def _test_embedding(self):
        if not self.client:
            return
        
        self.status_label.setText("Testing Embedding...")
        self.embed_btn.setEnabled(False)
        threading.Thread(target=self._embedding_worker).start()

    def _embedding_worker(self):
        try:
            QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, "Testing embedding with 'text-embedding-3-small'..."), QtCore.Q_ARG(str, "blue"))
            
            # Use a typical embedding model, or maybe the first available one if we were smarter,
            # but let's try the standard one first.
            model = "text-embedding-3-small"
            
            response = self.client.embeddings.create(
                input="The quick brown fox jumps over the lazy dog",
                model=model
            )
            
            if response.data:
                embedding = response.data[0].embedding
                dim = len(embedding)
                preview = str(embedding[:3]) + "..."
                msg = f"Embedding success! Dimension: {dim}, Preview: {preview}"
                QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, msg), QtCore.Q_ARG(str, "green"))
            else:
                QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, "Embedding response contained no data."), QtCore.Q_ARG(str, "red"))

        except Exception as e:
            msg = str(e)
            QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"Standard embedding failed: {msg}. Trying fallback..."), QtCore.Q_ARG(str, "orange"))
            
            # Fallback to manual request for known custom endpoint structure
            try:
                base = self.client.base_url
                # Try constructing the resource URL manually
                # User hint: .../resource/embed/
                if str(base).endswith("/v1") or str(base).endswith("/v1/"):
                     # Construct .../api/v1/resource/embeddings or embed
                     # Let's try to infer from the base
                     base_str = str(base).rstrip("/")
                     target_url = f"{base_str}/resource/embeddings" # Try standard-ish first?
                     
                     # Or stick to the user's specific hint if they are sure
                     # "https://apps.inside.anl.gov/argoapi/api/v1/resource/embed/"
                     # Let's try to be generic: replace /v1 with /v1/resource/embeddings if that's the pattern?
                     # No, let's just append /resource/embeddings to the base if the standard failed?
                     # Actually, let's try the user's hinted path specifically.
                     target_url = f"{base_str}/resource/embeddings" 
                
                QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"Fallback: POST {target_url}"), QtCore.Q_ARG(str, "blue"))
                
                headers = {
                    "Authorization": f"Bearer {self.client.api_key}",
                    "Content-Type": "application/json"
                }
                payload = {
                    "input": "The quick brown fox jumps over the lazy dog",
                    "model": model
                }
                
                resp = requests.post(target_url, headers=headers, json=payload, timeout=10)
                
                if resp.status_code == 404:
                     # Try "embed" instead of "embeddings"
                     target_url = f"{base_str}/resource/embed"
                     QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"Fallback 2: POST {target_url}"), QtCore.Q_ARG(str, "blue"))
                     resp = requests.post(target_url, headers=headers, json=payload, timeout=10)

                resp.raise_for_status()
                data = resp.json()
                
                if 'data' in data and len(data['data']) > 0:
                    embedding = data['data'][0]['embedding']
                    dim = len(embedding)
                    preview = str(embedding[:3]) + "..."
                    msg = f"Fallback Embedding success! Dimension: {dim}, Preview: {preview}"
                    QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, msg), QtCore.Q_ARG(str, "green"))
                else:
                     QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"Fallback response format unexpected: {data.keys()}"), QtCore.Q_ARG(str, "red"))

            except Exception as inner_e:
                QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"All embedding attempts failed: {inner_e}"), QtCore.Q_ARG(str, "red"))
        
        QtCore.QMetaObject.invokeMethod(self, "_on_embedding_finished", QtCore.Qt.QueuedConnection)

    @QtCore.pyqtSlot()
    def _on_embedding_finished(self):
        self.embed_btn.setEnabled(True)
        self.status_label.setText("Ready")

    def _send_message(self):
        msg = self.message_input.toPlainText().strip()
        if not msg:
            return
        
        self._log(f"<b>Me:</b> {msg}", "black")
        self.message_input.clear()
        self.send_btn.setEnabled(False)
        self.status_label.setText("Sending...")

        model = self.model_combo.currentText()
        if not model:
            model = "gpt-3.5-turbo" # Fallback

        threading.Thread(target=self._send_worker, args=(msg, model)).start()

    def _send_worker(self, text, model):
        try:
            # We use stream=True for responsiveness
            stream = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": text}],
                stream=True
            )
            
            full_response = ""
            # Prepare to stream response to UI
            QtCore.QMetaObject.invokeMethod(self, "_on_stream_start", QtCore.Qt.QueuedConnection)

            for chunk in stream:
                if chunk.choices:
                    content = chunk.choices[0].delta.content or ""
                    full_response += content
                    QtCore.QMetaObject.invokeMethod(self, "_on_stream_chunk", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, content))
            
            QtCore.QMetaObject.invokeMethod(self, "_on_stream_end", QtCore.Qt.QueuedConnection)

        except Exception as e:
            QtCore.QMetaObject.invokeMethod(self, "_on_send_error", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, str(e)))

    @QtCore.pyqtSlot()
    def _on_stream_start(self):
        self.chat_display.append("<b>AI:</b> ")
        self.chat_display.moveCursor(QtGui.QTextCursor.End)

    @QtCore.pyqtSlot(str)
    def _on_stream_chunk(self, chunk):
        self.chat_display.insertPlainText(chunk)
        self.chat_display.moveCursor(QtGui.QTextCursor.End)

    @QtCore.pyqtSlot()
    def _on_stream_end(self):
        self.chat_display.append("") # Newline
        self.status_label.setText("Ready")
        self.send_btn.setEnabled(True)

    @QtCore.pyqtSlot(str)
    def _on_send_error(self, error):
        QtCore.QMetaObject.invokeMethod(self, "_log", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, f"Error sending message: {error}"), QtCore.Q_ARG(str, "red"))
        self.status_label.setText("Send Failed")
        self.send_btn.setEnabled(True)

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = ArgoTestApp()
    window.show()
    sys.exit(app.exec_())
