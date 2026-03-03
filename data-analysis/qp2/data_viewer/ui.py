# ui.py
# ==============================================================================
# 5. PYQT5 APPLICATION
# ==============================================================================
import csv
import json
import os
import re
import shutil
import subprocess
from datetime import datetime
from functools import partial

import requests
from PyQt5.QtCore import QObject, pyqtSignal, QThread
from PyQt5.QtCore import Qt, QAbstractTableModel, QModelIndex, QVariant, QUrl, QSize
from PyQt5.QtGui import QDesktopServices, QTextDocument, QFont, QColor, QCursor
# --- PyQt5 Imports ---
from PyQt5.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QTableView,
    QHeaderView,
    QLineEdit,
    QPushButton,
    QMenu,
    QAction,
    QHBoxLayout,
    QAbstractItemView,
    QStyledItemDelegate,
    QStyle,
    QMessageBox,
    QTextEdit,
    QDialogButtonBox,
    QDialog,
    QFileDialog,
    QColorDialog
)

try:
    from PyQt5.QtWebEngineWidgets import (
        QWebEngineView,
        QWebEngineDownloadItem,
        QWebEnginePage,
    )

    WEB_ENGINE_AVAILABLE = True
except ImportError:
    WEB_ENGINE_AVAILABLE = False

from qp2.data_viewer.utils import (
    get_rpc_url,
    send_strategy_to_redis,
    make_path_relative,
)
from qp2.data_proc.client.dataset_processor_dialog import DatasetProcessorDialog
from qp2.data_proc.client.client import validate_and_submit
from qp2.data_viewer.query import delete_by_pid, delete_by_pids
from qp2.log.logging_config import get_logger
from qp2.config.programs import ProgramConfig
import sys

# --- AI Imports ---
try:
    from qp2.image_viewer.ai.assistant import AIClient, RedisChatHistory
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False
    logger = get_logger(__name__)
    logger.warning("Could not import AI modules. AI features will be disabled.")

logger = get_logger(__name__)

if WEB_ENGINE_AVAILABLE:

    class CustomWebEnginePage(QWebEnginePage):
        def __init__(self, parent=None):
            super().__init__(parent)
            self.parent_dialog = parent
            # will optionally be set when this page lives in a popup dialog
            self._container_dialog = None

        def createWindow(self, _type):
            # Create a real separate window for target="_blank"
            popup = QDialog(self.parent_dialog)
            popup.setAttribute(Qt.WA_DeleteOnClose, True)
            popup.setWindowTitle("Preview")
            popup.resize(900, 700)

            layout = QVBoxLayout(popup)
            view = QWebEngineView(popup)

            # Use the same CustomWebEnginePage so interception still works
            page = CustomWebEnginePage(parent=popup)
            page._container_dialog = popup  # let the page know its owning dialog

            view.setPage(page)
            layout.addWidget(view)
            popup.show()

            # Return the page that will host the new-window navigation
            return page

        def acceptNavigationRequest(self, url, _type, isMainFrame):
            local_path = url.toLocalFile()
            logger.debug(f"Intercepting navigation to: {local_path}")

            if local_path and local_path.lower().endswith(
                    (".lp", ".log", ".txt", ".cif", ".inp", ".json")
            ):
                # If this page belongs to a popup created for target="_blank", close it
                if getattr(self, "_container_dialog", None) is not None:
                    try:
                        self._container_dialog.close()
                    except Exception:
                        pass

                try:
                    with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                    text_dialog = PreformattedTextDialog(content, self.parent_dialog)
                    text_dialog.setWindowTitle(os.path.basename(local_path))
                    text_dialog.exec_()
                except Exception as e:
                    QMessageBox.critical(
                        self.parent_dialog, "Error", f"Could not open file:\n{e}"
                    )
                # Block WebEngine from loading it as a page
                return False

            # Default behavior for non-intercepted links
            return super().acceptNavigationRequest(url, _type, isMainFrame)


class Worker(QObject):
    """
    A generic worker to run a function in a separate thread.
    """

    finished = pyqtSignal(object)
    error = pyqtSignal(Exception)

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.func(*self.args, **self.kwargs)
            self.finished.emit(result)
        except Exception as e:
            self.error.emit(e)


class FileViewerDialog(QDialog):
    """
    A dialog for displaying local files. It uses QWebEngineView if available
    for rich HTML rendering, otherwise it falls back to a simpler QTextEdit.
    """

    def __init__(self, file_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle(os.path.basename(file_path))
        self.setGeometry(200, 200, 900, 700)
        layout = QVBoxLayout(self)

        # Read the file content
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        except Exception as e:
            content = f"Error reading file: {e}"

        if WEB_ENGINE_AVAILABLE and file_path.endswith((".html", ".htm")):
            # Use the powerful WebEngine for full HTML support
            viewer = QWebEngineView()

            custom_page = CustomWebEnginePage(self)
            viewer.setPage(custom_page)

            viewer.page().profile().downloadRequested.connect(
                self.on_download_requested
            )
            viewer.setUrl(QUrl.fromLocalFile(os.path.abspath(file_path)))
        else:
            # Fallback to QTextEdit for .log files or if WebEngine is unavailable
            viewer = QTextEdit()
            viewer.setReadOnly(True)
            # For HTML files, QTextEdit provides basic rendering.
            # For log files, it will be displayed as plain text.
            viewer.setHtml(content)

        layout.addWidget(viewer)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok)
        button_box.accepted.connect(self.accept)
        layout.addWidget(button_box)

    def on_download_requested(self, download_item: QWebEngineDownloadItem):
        """
        This method is called when QWebEngineView tries to download a file
        (e.g., when clicking a link to an .mtz, .zip, or other non-viewable file).
        """
        # Suggest a filename and default location (e.g., user's Downloads folder)
        suggested_path = os.path.join(
            os.path.expanduser("~"), "Downloads", download_item.suggestedFileName()
        )

        # Open a standard "Save File" dialog
        save_path, _ = QFileDialog.getSaveFileName(self, "Save File As", suggested_path)

        if save_path:
            # If the user selected a path, tell the download item where to save
            download_item.setPath(save_path)
            # Accept the download to start it
            download_item.accept()
            # Optional: Show a confirmation message when it's done
            download_item.finished.connect(
                lambda: QMessageBox.information(
                    self, "Download Complete", f"File saved to:\n{save_path}"
                )
            )
        else:
            # If the user clicked "Cancel", cancel the download
            download_item.cancel()



class PreformattedTextDialog(QDialog):
    """A dialog for displaying preformatted text in a monospaced font."""

    def __init__(self, text, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Detail Viewer")
        self.setGeometry(150, 150, 700, 500)
        layout = QVBoxLayout(self)
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setFont(QFont("Monospace"))
        text_edit.setText(text or "No content available.")
        layout.addWidget(text_edit)
        button_box = QDialogButtonBox(QDialogButtonBox.Ok)
        button_box.accepted.connect(self.accept)
        layout.addWidget(button_box)


class AIAnalysisDialog(QDialog):
    """
    Dialog to display AI Analysis results and optionally send them to the
    Shared AI Chat History.
    """
    def __init__(self, analysis_text, extracted_data_summary, model_name="Unknown Model", parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"AI Data Summary ({model_name})") # Show model in title
        self.resize(800, 600)
        self.analysis_text = analysis_text
        self.extracted_data_summary = extracted_data_summary
        self.model_name = model_name
        self.chat_history = RedisChatHistory() if AI_AVAILABLE else None
        
        layout = QVBoxLayout(self)
        
        # Display Area
        self.text_browser = QTextEdit()
        self.text_browser.setReadOnly(True)
        self.text_browser.setHtml(self._format_response(analysis_text))
        layout.addWidget(self.text_browser)
        
        # Buttons
        btn_layout = QHBoxLayout()
        
        self.send_chat_btn = QPushButton("Send to AI Chatbot")
        self.send_chat_btn.setToolTip("Send this analysis to the shared AI Assistant context and open the chatbot.")
        self.send_chat_btn.clicked.connect(self.on_send_to_chat)
        if not AI_AVAILABLE:
            self.send_chat_btn.setEnabled(False)
            
        self.close_btn = QPushButton("Close")
        self.close_btn.clicked.connect(self.accept)
        
        btn_layout.addWidget(self.send_chat_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(self.close_btn)
        
        layout.addLayout(btn_layout)

    def _format_response(self, text):
        """Simple formatting for the display."""
        html = f"<h2>AI Analysis Result <small style='color:gray'>({self.model_name})</small></h2>"
        html += f"<pre style='white-space: pre-wrap; font-family: sans-serif;'>{text}</pre>"
        return html

    def on_send_to_chat(self):
        if not self.chat_history:
            return
            
        # Construct a meaningful message for the chat history
        unique_id = str(datetime.now().timestamp())
        
        # 1. User context message
        user_msg = (
            f"I have analyzed the following data processing results (using {self.model_name}):\n\n"
            f"```json\n{self.extracted_data_summary}\n```\n\n"
            f"Here is the summary you provided:\n{self.analysis_text}\n\n"
            "I might have more questions about this."
        )
        
        self.chat_history.add_message("user", user_msg)
        
        # QMessageBox.information(self, "Sent", "Context sent to Shared AI Chat.")
        self.send_chat_btn.setEnabled(False)
        self.send_chat_btn.setText("Opening Chat...")
        
        # 2. Launch the Standalone Chatbot
        try:
            # Construct path to standalone_assistant.py
            # Assuming it's in qp2/image_viewer/ai/relativePath... relative to this file?
            # Safer to use ProgramConfig or relative path from this file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            # qp2/data_viewer -> qp2/image_viewer/ai
            script_path = os.path.join(current_dir, "..", "image_viewer", "ai", "standalone_assistant.py")
            script_path = os.path.normpath(script_path)
            
            if os.path.exists(script_path):
                subprocess.Popen([sys.executable, script_path, "--widget"])
            else:
                logger.error(f"Could not find standalone assistant script at: {script_path}")
                QMessageBox.warning(self, "Launch Error", "Could not find AI Assistant script.")
                
        except Exception as e:
            logger.error(f"Failed to launch AI Assistant: {e}")
            QMessageBox.warning(self, "Launch Error", f"Failed to launch AI Assistant: {e}")


class CustomColumnDelegate(QStyledItemDelegate):
    def __init__(self, parent_widget, parent=None):
        super().__init__(parent)
        self.main_widget = parent_widget
        # Flag to prevent multiple clicks while a check is in progress
        self._is_checking_path = False
        # Keep references to the thread and worker to prevent premature garbage collection
        self.thread = None
        self.worker = None

    def paint(self, painter, option, index):
        if index.data(GenericTableModel.RENDERER_ROLE):
            painter.save()
            option.text = ""
            style = QApplication.instance().style()
            style.drawControl(QStyle.CE_ItemViewItem, option, painter)
            doc = QTextDocument()
            doc.setTextWidth(option.rect.width())
            doc.setHtml(index.data(Qt.DisplayRole))
            y_offset = (option.rect.height() - doc.size().height()) / 2
            painter.setClipRect(option.rect)
            painter.translate(
                option.rect.topLeft().x(), option.rect.topLeft().y() + y_offset
            )
            doc.drawContents(painter)
            painter.restore()
        else:
            super().paint(painter, option, index)

    def sizeHint(self, option, index):
        if index.data(GenericTableModel.RENDERER_ROLE):
            doc = QTextDocument()
            doc.setHtml(index.data(Qt.DisplayRole))
            doc.setTextWidth(option.rect.width())
            return QSize(int(doc.idealWidth()) + 10, int(doc.size().height()) + 4)
        return super().sizeHint(option, index)

    def _check_path_worker(self, path):
        """
        This is the function that runs in the background thread.
        It contains the slow, blocking file system calls.
        """
        if not os.path.exists(path):
            return ("error", f"The path does not exist:\n{path}")
        if os.path.isdir(path):
            return ("directory", path)
        if os.path.isfile(path):
            return ("file", path)
        return ("error", f"The path is not a regular file or directory:\n{path}")

    def _on_path_check_finished(self, result):
        """
        This function runs in the main UI thread after the worker is finished.
        It safely performs the UI actions based on the worker's result.
        """
        self._is_checking_path = False  # Reset the flag
        result_type, data = result
        if result_type == "error":
            QMessageBox.warning(self.main_widget, "File Error", data)
            return

        local_path = data
        if result_type == "directory":
            QDesktopServices.openUrl(QUrl.fromLocalFile(local_path))
        elif result_type == "file":
            # This is the original file handling logic, now moved here
            if local_path.endswith((".html", ".htm")):
                viewer = FileViewerDialog(local_path, self.main_widget)
                viewer.exec_()
            elif local_path.endswith((".log", ".txt", ".csv")):
                try:
                    with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                    dialog = PreformattedTextDialog(content, self.main_widget)
                    dialog.exec_()
                except Exception as e:
                    QMessageBox.critical(
                        self, "Error Reading File", f"Could not read log file:\n{e}"
                    )
            elif local_path.endswith((".mtz")):
                default_name = os.path.basename(local_path)
                save_path, _ = QFileDialog.getSaveFileName(
                    self.main_widget, "Save As", default_name, "MTZ Files (*.mtz)"
                )
                if save_path:
                    try:
                        shutil.copy(local_path, save_path)
                        QMessageBox.information(
                            self.main_widget,
                            "Success",
                            f"File saved to:\n{save_path}",
                        )
                    except Exception as e:
                        QMessageBox.critical(
                            self.main_widget, "Error", f"Could not save file:\n{e}"
                        )
            else:
                # Fallback for other file types: let the OS handle it
                QDesktopServices.openUrl(QUrl.fromLocalFile(local_path))

    def _on_path_check_error(self, e):
        """Handles any unexpected exceptions from the worker thread."""
        self._is_checking_path = False  # Reset the flag
        QMessageBox.critical(
            self.main_widget, "Processing Error", f"An unexpected error occurred:\n{e}"
        )

    def editorEvent(self, event, model, option, index):
        if event.type() != event.MouseButtonRelease:
            return super().editorEvent(event, model, option, index)

        if not index.data(GenericTableModel.RENDERER_ROLE):
            return super().editorEvent(event, model, option, index)

        doc = QTextDocument()
        doc.setHtml(index.data())
        doc.setTextWidth(option.rect.width())
        anchor = doc.documentLayout().anchorAt(event.pos() - option.rect.topLeft())

        if not anchor:
            return super().editorEvent(event, model, option, index)

        # --- Process the clicked link based on its custom scheme ---

        if anchor.startswith("export_strategy:"):
            # This logic remains synchronous as it is fast.
            try:
                pipelinestatus_id = int(anchor.split(":", 1)[1])
                self.main_widget.on_export_strategy(pipelinestatus_id)
            except (ValueError, IndexError):
                logger.error(f"Error: could not parse ID from anchor {anchor}")
            return True

        elif anchor.startswith("delete_entry:"):
            try:
                # Extract the ID from the anchor
                pipelinestatus_id = int(anchor.split(":", 1)[1])
                # Call a new handler method on the main widget (QueryTab)
                self.main_widget.on_delete_single_entry(pipelinestatus_id)
            except (ValueError, IndexError):
                logger.error(f"Error: could not parse ID from delete anchor {anchor}")
            return True  # Event handled

        elif anchor.startswith("view_preformatted:"):
            # This logic remains synchronous.
            raw_text = model.data(index, GenericTableModel.RAW_DATA_ROLE)
            if raw_text and str(raw_text).strip():
                formatted_text = raw_text
                try:
                    # Attempt to parse the raw text as JSON
                    json_obj = json.loads(raw_text)
                    # If successful, format it with an indent of 2 spaces
                    formatted_text = json.dumps(json_obj, indent=2)
                except (json.JSONDecodeError, TypeError):
                    # If it's not valid JSON, just pass, and the raw text will be shown
                    pass

                dialog = PreformattedTextDialog(formatted_text, self.parent())
                dialog.setAttribute(Qt.WA_DeleteOnClose, True)
                dialog.show()
                return True

        elif anchor.startswith("coot:"):
            # This logic is already non-blocking (subprocess.Popen).
            solve_dir = anchor.split(":", 1)[1]
            if os.path.isfile(solve_dir):
                solve_dir = os.path.dirname(solve_dir)

            pdb = os.path.join(solve_dir, "final.pdb")
            mtz = os.path.join(solve_dir, "final.mtz")
            if os.path.exists(pdb) and os.path.exists(mtz):
                try:
                    subprocess.Popen(["coot", "--pdb", pdb, "--auto", mtz])
                except Exception as e:
                    QMessageBox.warning(
                        self.parent(), "Error", f"Could not run coot: {e}"
                    )
            else:
                QMessageBox.warning(
                    self.parent(),
                    "File Not Found",
                    f"Files not in:\n{solve_dir}",
                )
            return True

        elif anchor.startswith("show_choices:"):
            # 1. Extract the data from the link
            href_data = anchor.split(":", 1)[1]
            choices = [v.strip() for v in href_data.split("|") if v.strip()]

            # 2. Create the pop-up menu
            menu = QMenu(self.main_widget)
            for choice in choices:
                action = QAction(choice, menu)
                menu.addAction(action)

            # 3. Connect the menu's signal to our handler
            menu.triggered.connect(self._on_choice_selected)

            # 4. Show the menu at the current cursor position
            menu.exec_(QCursor.pos())

            return True  # Event handled

        elif anchor.startswith("file://"):
            # --- ASYNCHRONOUS HANDLING FOR FILE LINKS ---
            if self._is_checking_path:
                return True  # Ignore click if a check is already running

            self._is_checking_path = True
            local_path = os.path.normpath(QUrl(anchor).toLocalFile())

            # 1. Set up Worker and Thread
            self.thread = QThread()
            self.worker = Worker(self._check_path_worker, local_path)
            self.worker.moveToThread(self.thread)

            # 2. Connect signals from worker to slots in this delegate
            self.thread.started.connect(self.worker.run)
            self.worker.finished.connect(self._on_path_check_finished)
            self.worker.error.connect(self._on_path_check_error)

            # 3. Schedule cleanup
            self.worker.finished.connect(self.thread.quit)
            self.worker.finished.connect(self.worker.deleteLater)
            self.thread.finished.connect(self.thread.deleteLater)

            # 4. Start the background work
            self.thread.start()

            # 5. Immediately return True to keep the UI responsive and confirm event handled
            return True

        else:
            # Fallback for other unhandled anchors
            QDesktopServices.openUrl(QUrl(anchor))
            return True

    def _on_choice_selected(self, action):
        """A simple handler to show the user's selection from the menu."""
        choice = action.text()
        QMessageBox.information(
            self.main_widget,
            "Selection Made",
            f"You selected the point group: {choice}",
        )


class GenericTableModel(QAbstractTableModel):
    RENDERER_ROLE = Qt.UserRole + 1
    RAW_DATA_ROLE = Qt.UserRole + 2

    sort_requested = pyqtSignal(str, Qt.SortOrder)

    def __init__(self, config, parent=None):
        super().__init__(parent)
        self._data, self._column_configs = [], config
        self.row_colors = {}  # Map: row_id -> QColor

    def rowCount(self, p=QModelIndex()):
        return len(self._data)

    def columnCount(self, p=QModelIndex()):
        return len(self._column_configs)

    def column_config(self, column):  # Helper to get config
        return self._column_configs[column]

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return QVariant()
        
        row_data = self._data[index.row()]
        
        if role == Qt.BackgroundRole:
            # Check if this row has a specific background color set
            row_id = getattr(row_data, "id", None)
            if row_id and row_id in self.row_colors:
                return self.row_colors[row_id]
        
        col_conf = self._column_configs[index.column()]
        key = col_conf["key"]
        
        raw_value = getattr(row_data, key, "")

        renderer = col_conf.get("renderer")

        # Handle simple color rendering with ForegroundRole for huge performance gain
        if role == Qt.ForegroundRole and renderer and renderer == "state_renderer":
            state_str = str(raw_value).upper()
            color = {
                "DONE": QColor("green"),
                "FAIL": QColor("red"),
                "RUNNING": QColor("blue"),
            }.get(state_str)
            if color:
                return color

        if role == self.RAW_DATA_ROLE:
            return raw_value
        if role == Qt.ToolTipRole:
            return str(raw_value or "")

        if role == Qt.DisplayRole:
            # CORRECTED LINE: Check the renderer's name instead of its object identity.
            if renderer and renderer != "state_renderer":
                return (
                    renderer(row_data)
                    if col_conf.get("renderer_uses_row")
                    else renderer(raw_value)
                )
            return str(raw_value or "")

        if role == Qt.TextAlignmentRole:
            return Qt.AlignCenter

        # Let the delegate know which cells still need rich text processing
        if role == self.RENDERER_ROLE:
            return renderer and renderer != "state_renderer"

        return QVariant()

    def headerData(self, s, o, r):
        if r == Qt.DisplayRole and o == Qt.Horizontal:
            return self._column_configs[s]["display"]
        return QVariant()

    def refresh_data(self, new_data):
        self.beginResetModel()
        self._data = new_data
        self.endResetModel()

    def sort(self, column, order):
        """
        Overrides the default sort. Instead of sorting in-place,
        it emits a signal to trigger a new database query.
        """
        key = self._column_configs[column]["key"]
        self.sort_requested.emit(key, order)


class QueryTab(QWidget):
    def __init__(self, tab_name, tab_config, db_manager, query_func):
        super().__init__()
        self.tab_name = tab_name
        self.tab_config, self.db_manager, self.query_func = (
            tab_config,
            db_manager,
            query_func,
        )

        self.model = GenericTableModel(self.tab_config["columns"])
        self.menu_actions = {}
        self.thread = None
        self.worker = None
        self._is_loading = False
        self._columns_sized = False  # Auto-size only on first load

        self.model.sort_requested.connect(self.on_sort_requested)
        self.current_sort_key = "id"  # Default sort column
        self.current_sort_order = Qt.DescendingOrder

        self._setup_ui()
        self.load_data()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        controls_layout = QHBoxLayout()
        self.search_box = QLineEdit(placeholderText="Search...")
        # self.search_box.textChanged.connect(self.on_search)

        # Connect the Enter key press
        self.search_box.returnPressed.connect(self.on_search_triggered)
        controls_layout.addWidget(self.search_box)

        # Create and connect the Search button
        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self.on_search_triggered)
        controls_layout.addWidget(self.search_button)

        self.column_button = QPushButton("Columns")
        self.column_menu = QMenu()
        self.column_button.setMenu(self.column_menu)
        controls_layout.addWidget(self.column_button)
        controls_layout.addStretch(1)
        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.clicked.connect(self.load_data)
        controls_layout.addWidget(self.refresh_button)


        self.export_button = QPushButton("Export Data")
        # self.export_button.clicked.connect(self.on_export_selected)
        self.export_button.clicked.connect(self.on_export_all_data)

        controls_layout.addWidget(self.export_button)

        if self.tab_name == "Processing":
            self.export_html_button = QPushButton("Export HTML")
            self.export_html_button.clicked.connect(self.on_export_html)
            controls_layout.addWidget(self.export_html_button)

            # AI Summary Button (Renamed to Summary and moved to end)
            if AI_AVAILABLE:
                self.ai_summary_button = QPushButton("Summary")
                self.ai_summary_button.clicked.connect(self.on_ai_summary)
                # Style it to stand out slightly
                self.ai_summary_button.setStyleSheet("font-weight: bold; color: #2980b9;")
                controls_layout.addWidget(self.ai_summary_button)

        layout.addLayout(controls_layout)
        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        # self.table_view.setItemDelegate(CustomColumnDelegate(self.table_view))
        self.table_view.setItemDelegate(CustomColumnDelegate(self, self.table_view))
        self.table_view.setSortingEnabled(False)  # disable sort to avoid confusion
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)

        # Only enable the context menu for the "Datasets" and "Processing" tabs
        if self.tab_name in ["Datasets", "Processing", "Strategy"]:
            self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
            self.table_view.customContextMenuRequested.connect(
                self.show_table_context_menu
            )

        self.model.modelReset.connect(self._auto_resize_columns)
        layout.addWidget(self.table_view)
        self._setup_column_visibility_menu()

    def load_data(self, search_text=None):
        # Prevent a new load if one is already running
        if self._is_loading:
            return

        self._is_loading = True
        self.refresh_button.setEnabled(False)
        self.search_box.setEnabled(False)

        sort_order_str = (
            "desc" if self.current_sort_order == Qt.DescendingOrder else "asc"
        )
        query_task = partial(
            self.query_func,
            search_text=search_text,
            sort_by=self.current_sort_key,
            sort_order=sort_order_str,
        )

        self.thread = QThread()
        self.worker = Worker(self._execute_query, query_task)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.error.connect(self._on_load_error)
        self.worker.finished.connect(self._on_data_loaded)

        # Tell the thread's event loop to stop on both success and error.
        self.worker.finished.connect(self.thread.quit)
        self.worker.error.connect(self.thread.quit)

        # Schedule the worker and thread for deletion after the thread's event loop has finished.
        self.thread.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    def _execute_query(self, query_callable):
        """Executes the query within the DBManager's session context."""
        try:
            with self.db_manager.get_session() as s:
                # The 'user' argument is already baked into self.query_func via partial
                results = query_callable(db_session=s)
                return results
        except Exception as e:
            # Propagate the exception to be caught by the worker's error handler
            raise e

    def _on_data_loaded(self, results):
        self.model.refresh_data(results)
        self.update_responsive_columns(self.width())
        self._cleanup_ui()  # Changed from _cleanup_thread

    def _cleanup_ui(self):
        """Only responsible for UI state and our internal loading flag."""
        self._is_loading = False
        self.refresh_button.setEnabled(True)
        self.search_box.setEnabled(True)

    def _on_load_error(self, e):
        """Handles errors from the background thread."""
        logger.error(f"Error executing query: {e}")
        self.model.refresh_data([])  # Clear view on error
        self._cleanup_ui()

    def _auto_resize_columns(self):
        if not self._columns_sized and self.model.rowCount() > 0:
            if self.isVisible() and self.table_view.viewport().width() > 0:
                self._columns_sized = True
                self._switch_header_to_interactive()
            # else: tab is hidden; showEvent will trigger sizing when first shown

    def showEvent(self, event):
        super().showEvent(event)
        if not self._columns_sized and self.model.rowCount() > 0:
            self._columns_sized = True
            self._switch_header_to_interactive()

    def _switch_header_to_interactive(self):
        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(True)
        header.setMinimumSectionSize(80)
        # Distribute equal widths across visible columns
        visible_cols = [i for i in range(header.count()) if not header.isSectionHidden(i)]
        if visible_cols:
            viewport_width = self.table_view.viewport().width()
            col_width = max(80, viewport_width // len(visible_cols))
            for i in visible_cols:
                header.resizeSection(i, col_width)

    def on_search(self, text):
        self.load_data(search_text=text or None)

    def on_search_triggered(self):
        """Triggers a search using the current text in the search box."""
        search_text = self.search_box.text()
        self.load_data(search_text=search_text or None)

    def _setup_column_visibility_menu(self):
        self.column_menu.clear()
        self.menu_actions.clear()
        for i, config in enumerate(self.tab_config["columns"]):
            action = QAction(config["display"], self.column_menu, checkable=True)
            initial_visibility = config.get("visible", True)
            action.setChecked(initial_visibility)
            self.table_view.setColumnHidden(i, not initial_visibility)
            action.toggled.connect(partial(self.on_visibility_toggled, i))
            self.column_menu.addAction(action)
            self.menu_actions[i] = action

    def on_visibility_toggled(self, index, is_checked):
        self.table_view.setColumnHidden(index, not is_checked)
        if is_checked:
            self.table_view.resizeColumnToContents(index)

    def update_responsive_columns(self, width):
        WIDE_WIDTH, NORMAL_WIDTH = 1400, 1000
        for i, config in enumerate(self.tab_config["columns"]):
            priority = config.get("priority", 3)
            action = self.menu_actions.get(i)
            if not action or not action.isChecked():
                continue
            is_visible = not (
                    (priority == 2 and width < WIDE_WIDTH)
                    or (priority == 3 and width < NORMAL_WIDTH)
            )
            if self.table_view.isColumnHidden(i) == is_visible:
                self.table_view.setColumnHidden(i, not is_visible)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.update_responsive_columns(event.size().width())

    def on_sort_requested(self, key, order):
        self.current_sort_key = key
        self.current_sort_order = order
        self.load_data(search_text=self.search_box.text() or None)

    def on_export_all_data(self):
        """
        Extracts all rows currently in the model and saves them to a CSV file.
        """
        # 1. Get all rows directly from the model
        num_rows = self.model.rowCount()

        if num_rows == 0:
            QMessageBox.information(
                self, "No Data", "There is no data in the table to export."
            )
            return

        # 2. Open "Save File" Dialog
        default_filename = f"exported_all_{self.window().tabs.tabText(self.window().tabs.currentIndex())}.csv"
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save All Data", default_filename, "CSV Files (*.csv);;All Files (*)"
        )

        if not file_path:
            return

        # 3. Extract Data and Write to CSV
        try:
            first_row_object = self.model._data[0]
            if hasattr(first_row_object, "_fields"):
                headers = first_row_object._fields
            else:
                headers = [
                    col["key"]
                    for col in self.model._column_configs
                    if col.get("visible", True)
                ]

            with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)

                # Loop through all row numbers from 0 to num_rows-1
                for row_num in range(num_rows):
                    data_object = self.model._data[row_num]
                    row_to_write = [
                        getattr(data_object, header, "") for header in headers
                    ]
                    writer.writerow(row_to_write)

            QMessageBox.information(
                self,
                "Export Successful",
                f"Successfully saved {num_rows} rows to:\n{file_path}",
            )

        except Exception as e:
            QMessageBox.critical(
                self, "Error", f"An error occurred while saving the file:\n{e}"
            )

    def on_export_strategy(self, pipelinestatus_id):
        """
        Handles the logic for exporting a strategy when a button is clicked.
        """
        # Find the full data object for the given ID from the model's data
        target_row = None
        for row in self.model._data:
            if getattr(row, "id", None) == pipelinestatus_id:
                target_row = row
                break

        if not target_row:
            QMessageBox.critical(
                self, "Error", "Could not find data for the selected row."
            )
            return

        # 1. Gather all required options
        user = self.window().current_user
        opt = {
            "id": pipelinestatus_id,
            "pipeline": getattr(target_row, "pipeline", None),
            "username": user.username,
            "beamline": user.beamline,
            "osc_start": getattr(target_row, "osc_start", 0),
            "osc_end": getattr(target_row, "osc_end", 180),
            "osc_delta": getattr(target_row, "osc_delta", 0.2),
            "distance": getattr(target_row, "distance", 500),
        }

        # 2. Send strategy to Redis
        if not send_strategy_to_redis(opt["beamline"], opt):
            QMessageBox.warning(
                self, "Redis Error", "Failed to send strategy data to Redis."
            )
            # Decide if you want to continue or stop if Redis fails

        # 3. Get the RPC URL from the database via helper
        rpc_url = get_rpc_url()
        if not rpc_url:
            QMessageBox.critical(
                self, "DB Error", "Failed to retrieve RPC URL from database."
            )
            return

        # 4. Construct POST data and send the request
        post_data = {
            "module": "run_create",
            "frame_deg_start": opt["osc_start"],
            "frame_deg_end": opt["osc_end"],
            "delta_deg": opt["osc_delta"],
            "det_z_mm": opt["distance"],
            "atten_factors": "",
            "expTime_sec": "",
            "energy1_keV": "",
            "mode": "",
        }

        try:
            self.window().status_bar.showMessage("Sending export request...")
            resp = requests.post(rpc_url, data=post_data, timeout=10)
            resp.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

            # 5. Show success message with the response
            QMessageBox.information(
                self,
                "Export Successful",
                f"Request sent to PBS successfully.\n\nResponse:\n{resp.content.decode('utf-8')}",
            )

        except requests.exceptions.RequestException as e:
            QMessageBox.critical(
                self, "Request Error", f"Failed to post request to {rpc_url}:\n{e}"
            )
        finally:
            self.window().status_bar.clearMessage()

    def on_delete_selected(self):
        """
        Handles the 'Delete Selected' action from the context menu.
        Collects all selected IDs and performs a bulk delete.
        """
        selection_model = self.table_view.selectionModel()
        selected_rows = selection_model.selectedRows()

        if not selected_rows:
            return

        # Snapshot row indices AND the model data immediately, before showing any
        # dialog. The background poll timer can call refresh_data() while the
        # confirmation dialog is open, which resets self.model._data and makes
        # stale row indices out-of-bounds (intermittent IndexError / crash).
        row_indices = [index.row() for index in selected_rows]
        data_snapshot = list(self.model._data)

        pids = []
        for row_num in row_indices:
            if row_num < len(data_snapshot):
                pid = getattr(data_snapshot[row_num], "id", None)
                if pid is not None:
                    pids.append(pid)

        if not pids:
            return

        reply = QMessageBox.question(
            self,
            "Confirm Deletion",
            f"Are you sure you want to permanently delete {len(pids)} selected entries?\n\nThis action cannot be undone.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            self.window().status_bar.showMessage(f"Deleting {len(pids)} entries...")
            QApplication.processEvents()

            try:
                with self.db_manager.get_session() as session:
                    delete_by_pids(session, pids)

                self.window().status_bar.showMessage(
                    f"Successfully deleted {len(pids)} entries. Refreshing...", 5000
                )
                self.load_data()
            except Exception as e:
                QMessageBox.critical(self, "Deletion Failed", f"An error occurred: {e}")
                self.window().status_bar.clearMessage()


    def on_mark_color(self):
        """
        Allows the user to select a color and applies it to the background
        of the selected rows in the table.
        """
        selection_model = self.table_view.selectionModel()
        selected_rows = selection_model.selectedRows()

        if not selected_rows:
            return

        # Open color picker
        color = QColorDialog.getColor(Qt.white, self, "Select Row Color")
        if not color.isValid():
            return

        # Apply color to all selected rows
        for index in selected_rows:
            row_data = self.model._data[index.row()]
            row_id = getattr(row_data, "id", None)
            if row_id is not None:
                self.model.row_colors[row_id] = color

        # Trigger a refresh of the view to show the new colors
        # dataChanged requires a range, easiest is to layoutChanged for bulk updates 
        # or emit dataChanged for the specific rows. layoutChanged is simpler here.
        self.model.layoutChanged.emit() 

    def show_table_context_menu(self, position):
        """
        Creates and shows a context menu when the user right-clicks the table.
        The content of the menu depends on which tab is active.
        """
        context_menu = QMenu(self)
        has_selection = self.table_view.selectionModel().hasSelection()

        # Action for the "Datasets" tab
        if self.tab_name == "Datasets":

            view_images_action = QAction("View Images", self)
            view_images_action.setEnabled(has_selection)
            view_images_action.triggered.connect(self.on_view_images)
            context_menu.addAction(view_images_action)

            adxv_action = QAction("ADXV", self)
            # This action is only enabled if exactly one row is selected
            adxv_action.setEnabled(
                len(self.table_view.selectionModel().selectedRows()) == 1
            )
            adxv_action.triggered.connect(self.on_view_with_adxv)
            context_menu.addAction(adxv_action)

            open_folder_action = QAction("Open Image Folder", self)
            open_folder_action.setEnabled(has_selection)
            open_folder_action.triggered.connect(self.on_open_containing_folder)
            context_menu.addAction(open_folder_action)

            open_terminal_action = QAction("Open Terminal Here", self)
            open_terminal_action.setEnabled(has_selection)
            open_terminal_action.triggered.connect(self.on_open_terminal)
            context_menu.addAction(open_terminal_action)

            # Add a separator for visual clarity
            context_menu.addSeparator()

            process_action = QAction("Process Selected Datasets...", self)
            process_action.setEnabled(has_selection)
            process_action.triggered.connect(self.on_process_selected_datasets)
            context_menu.addAction(process_action)

        # Add action for the "Processing" tab ---
        elif self.tab_name == "Processing":
            # --- Added View Options ---
            view_images_action = QAction("View Images", self)
            view_images_action.setEnabled(has_selection)
            view_images_action.triggered.connect(self.on_view_images)
            context_menu.addAction(view_images_action)

            adxv_action = QAction("ADXV", self)
            adxv_action.setEnabled(len(self.table_view.selectionModel().selectedRows()) == 1)
            adxv_action.triggered.connect(self.on_view_with_adxv)
            context_menu.addAction(adxv_action)

            open_folder_action = QAction("Open Image Folder", self)
            open_folder_action.setEnabled(has_selection)
            open_folder_action.triggered.connect(self.on_open_containing_folder)
            context_menu.addAction(open_folder_action)
            
            open_terminal_action = QAction("Open Terminal Here", self)
            open_terminal_action.setEnabled(has_selection)
            open_terminal_action.triggered.connect(self.on_open_terminal)
            context_menu.addAction(open_terminal_action)
            
            context_menu.addSeparator()
            # --------------------------

            reprocess_action = QAction("Re-process Selected...", self)
            reprocess_action.setEnabled(has_selection)
            reprocess_action.triggered.connect(self.on_reprocess_selected)
            context_menu.addAction(reprocess_action)
            
            delete_action = QAction("Delete Selected", self)
            delete_action.setEnabled(has_selection)
            delete_action.triggered.connect(self.on_delete_selected)
            context_menu.addAction(delete_action)

            # Add context menu item for row coloring
            mark_color_action = QAction("Mark with Color...", self)
            mark_color_action.setEnabled(has_selection)
            mark_color_action.triggered.connect(self.on_mark_color)
            context_menu.addAction(mark_color_action)

        # Add action for the "Strategy" tab ---
        elif self.tab_name == "Strategy":
            # --- Added View Options ---
            view_images_action = QAction("View Images", self)
            view_images_action.setEnabled(has_selection)
            view_images_action.triggered.connect(self.on_view_images)
            context_menu.addAction(view_images_action)

            adxv_action = QAction("ADXV", self)
            adxv_action.setEnabled(len(self.table_view.selectionModel().selectedRows()) == 1)
            adxv_action.triggered.connect(self.on_view_with_adxv)
            context_menu.addAction(adxv_action)

            open_folder_action = QAction("Open Image Folder", self)
            open_folder_action.setEnabled(has_selection)
            open_folder_action.triggered.connect(self.on_open_containing_folder)
            context_menu.addAction(open_folder_action)

            open_terminal_action = QAction("Open Terminal Here", self)
            open_terminal_action.setEnabled(has_selection)
            open_terminal_action.triggered.connect(self.on_open_terminal)
            context_menu.addAction(open_terminal_action)

            context_menu.addSeparator()
            # --------------------------
            
            delete_action = QAction("Delete Selected", self)
            delete_action.setEnabled(has_selection)
            delete_action.triggered.connect(self.on_delete_selected)
            context_menu.addAction(delete_action)

        # Only show the menu if it has any actions
        if context_menu.actions():
            context_menu.exec_(self.table_view.viewport().mapToGlobal(position))

    def default_jobcontext(self):
        current_user = self.window().current_user
        job_context = {
            "username": current_user.username,
            "primary_group": current_user.primary_group,
            "beamline": current_user.beamline,
        }
        if current_user.primary_group and current_user.primary_group.lower().startswith(
                "esaf"
        ):
            try:
                # Extracts digits from a string like 'esaf12345'
                esaf_id = int("".join(filter(str.isdigit, current_user.primary_group)))
                job_context["esaf_id"] = esaf_id
            except (ValueError, TypeError):
                pass  # Ignore if parsing fails
        return job_context

    def submit_legacy_processing_job(self, config):
        """
        This is the callback function. It takes the configuration from the dialog,
        prepares it, and calls the job submitter.
        """
        self.window().status_bar.showMessage("Preparing job for submission...")

        # --- Transform dialog config into the format the server expects ---
        # The client requires 'proc_dir', 'data_dir', 'pipeline', 'sample_id', etc.
        # job_data = {
        #     "pipeline": config.get("pipeline"),
        #     "proc_dir": config.get("proc_dir"),
        #     "data_dir": config.get("data_dir"),
        #     "sample_id": config.get("sample_id"),
        #     "username": self.window().current_user.username,
        #     "beamline": self.window().current_user.beamline,
        #     "highres": config.get("highres"),
        #     "space_group": config.get("space_group"),
        #     "native": config.get("native"),
        #     "model": config.get("models")[0] if config.get("models") else None,
        #     "sequence": config.get("sequences")[0] if config.get("sequences") else None,
        # }

        job_data = config

        # Add special handling for single vs multi-dataset jobs
        if "start" in config:
            job_data["start"] = config.get("start")
            job_data["end"] = config.get("end")
            job_data["prefix"] = config.get("prefix")
        elif "filelist" in config:
            job_data["filelist"] = config.get("filelist")

        # Clean up any keys that have None values
        job_data = {
            k: v for k, v in job_data.items() if v is not None and str(v).strip()
        }

        # Use the job_submitter module to send the request
        # This is a blocking call. For a fully non-blocking UI, this should
        # be run in a separate QThread, but for simplicity, we do it directly.
        success, message = validate_and_submit(job_data)

        if success:
            QMessageBox.information(self, "Submission Successful", message)
            self.window().status_bar.showMessage("Job submitted successfully.", 5000)
        else:
            QMessageBox.critical(self, "Submission Failed", message)
            self.window().status_bar.showMessage("Job submission failed.", 5000)
            # We raise an exception to signal the dialog not to close
            raise RuntimeError(f"Job submission failed: {message}")

    def on_delete_single_entry(self, pid):
        """Handles the 'Delete' action from a link click in a table row."""
        if pid is None:
            return

        reply = QMessageBox.question(
            self,
            "Confirm Deletion",
            f"Are you sure you want to permanently delete entry with ID {pid}?\n\nThis action cannot be undone.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            self.window().status_bar.showMessage(f"Deleting entry {pid}...")
            # Force the UI to update before we block it with the database call
            QApplication.processEvents()

            try:
                # --- Execute delete directly in the GUI thread ---
                with self.db_manager.get_session() as session:
                    delete_by_pid(session, pid)

                self.window().status_bar.showMessage(
                    f"Successfully deleted entry {pid}. Refreshing...", 5000
                )
                # Refresh the table with the latest data
                self.load_data()

            except Exception as e:
                QMessageBox.critical(self, "Deletion Failed", f"An error occurred: {e}")
                self.window().status_bar.clearMessage()

    def on_process_selected_datasets(self):
        """
        Gathers master files from "Datasets" tab, creates a job context from the
        current user, and launches the DatasetProcessorDialog.
        """
        selection_model = self.table_view.selectionModel()
        selected_indices = selection_model.selectedRows()
        selected_rows = sorted(list(set(index.row() for index in selected_indices)))

        all_master_files = []
        for row_num in selected_rows:
            data_object = self.model._data[row_num]
            json_string = getattr(data_object, "master_files", "[]")
            try:
                file_list = json.loads(json_string)
                if isinstance(file_list, list):
                    all_master_files.extend(file_list)
            except json.JSONDecodeError:
                continue

        if not all_master_files:
            QMessageBox.warning(
                self,
                "No Files Found",
                "Could not find any master files in the selected rows.",
            )
            return

        job_context = self.default_jobcontext()

        logger.debug("DEBUG: Job context for new job:", job_context)

        dialog = DatasetProcessorDialog(
            initial_dataset_paths=all_master_files,
            on_accept_callback=self.submit_legacy_processing_job,
            parent=self,
            job_context=job_context,
        )
        dialog.exec_()

    def on_reprocess_selected(self):
        """
        Gathers dataset master files and job context from selected 'Processing' rows
        and launches the DatasetProcessorDialog.
        """
        selection_model = self.table_view.selectionModel()
        selected_rows = selection_model.selectedRows()

        if not selected_rows:
            QMessageBox.information(
                self,
                "No Selection",
                "Please select at least one job to re-process.",
            )
            return

        # Use the first selected row for context
        index = selected_rows[0]
        data_object = self.model._data[index.row()]

        # --- GATHER JOB CONTEXT WITH FALLBACKS ---
        job_context = self.default_jobcontext()

        # 1. update some values from the database record
        for fields in ["esaf_id", "primary_group", "pi_id"]:
            value = getattr(data_object, fields, None)
            if value is not None:
                job_context[fields] = value

        # --- FIND MASTER FILE(S) ---
        all_master_files = set()

        for idx in selected_rows:
            row_data = self.model._data[idx.row()]
            found_for_row = False

            # Method 1 (Primary): Check the 'datasets' column
            datasets_json = getattr(row_data, "datasets", None)
            if datasets_json:
                try:
                    datasets_list = json.loads(datasets_json)
                    if isinstance(datasets_list, list):
                        all_master_files.update(datasets_list)
                        found_for_row = True
                except (json.JSONDecodeError, TypeError):
                    pass

            # Method 2 (Fallback): Check 'run_stats'
            if not found_for_row:
                run_stats_json = getattr(row_data, "run_stats", None)
                if run_stats_json:
                    try:
                        stats = json.loads(run_stats_json)
                        dataset_path = stats.get("dataset")
                        if dataset_path and isinstance(dataset_path, str):
                            all_master_files.add(dataset_path)
                            found_for_row = True
                    except (json.JSONDecodeError, TypeError):
                        pass

            # Method 3 (Fallback): Reconstruct from imagedir and imageSet
            if not found_for_row:
                image_dir = getattr(row_data, "imagedir", None)
                image_set = getattr(row_data, "imageSet", None)
                if image_dir and image_set:
                    parts = image_set.split(":")
                    if len(parts) > 0:
                        prefix = parts[-1]
                        if not prefix.endswith("_"):
                            prefix += "_"
                        master_file = os.path.join(image_dir, f"{prefix}master.h5")
                        all_master_files.add(master_file)

        # Clean up context: remove keys with None values
        job_context = {k: v for k, v in job_context.items() if v is not None}
        logger.debug("DEBUG: Job context for reprocessing:", job_context)

        if not all_master_files:
            QMessageBox.warning(
                self,
                "No Datasets Found",
                "Could not determine the original dataset master file(s) for the selected rows.",
            )
            return

        existing_files = [f for f in all_master_files if os.path.exists(f)]
        if not existing_files:
            QMessageBox.warning(
                self,
                "Files Not Found",
                "The determined master files could not be found.",
            )
            return

        # Launch the dialog, PASSING THE NEW JOB CONTEXT
        dialog = DatasetProcessorDialog(
            initial_dataset_paths=list(existing_files),
            on_accept_callback=self.submit_legacy_processing_job,
            parent=self,
            job_context=job_context,
        )
        dialog.exec_()

    def on_export_html(self):
        """
        Exports the current 'Processing' table view to a self-contained HTML file
        with relative links.
        """
        # 1. Get the data from the model
        data_rows = self.model._data
        if not data_rows:
            QMessageBox.information(self, "No Data", "There is no data to export.")
            return

        # 2. Determine the base directory (e.g., PROCESSING/esaf12345)
        # We can derive this from the workdir of the first entry.
        first_workdir = getattr(data_rows[0], "workdir", "")
        base_dir = ""
        if first_workdir:
            # Find the esaf directory in the path
            match = re.search(r"(.*?[/|\\]esaf\d+)", first_workdir, re.IGNORECASE)
            if match:
                # The base path for all relative links will be this ESAF directory
                base_dir = match.group(1)

        if not base_dir:
            QMessageBox.warning(
                self,
                "Cannot Determine Path",
                "Could not determine the ESAF base directory from the first entry's workdir.",
            )
            # Fallback to asking the user for a save location
            base_dir, _ = QFileDialog.getSaveFileName(
                self, "Save HTML Report", "", "HTML Files (*.html)"
            )
            if not base_dir:
                return
            save_path = base_dir
            base_dir = os.path.dirname(save_path)
        else:
            save_path = os.path.join(base_dir, "processing_summary.html")

        # 3. Generate HTML content
        html = ["<html><head><title>Processing Summary</title>"]
        html.append(
            "<style>body {font-family: sans-serif;} table {border-collapse: collapse; width: 100%;} th, td {border: 1px solid #dddddd; text-align: left; padding: 8px;} tr:nth-child(even) {background-color: #f2f2f2;}</style>"
        )
        html.append("</head><body>")
        html.append(f"<h1>Processing Summary</h1>")
        html.append(
            f"<p>Report generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>"
        )
        html.append("<table>")

        # Create table header
        visible_columns = [
            col
            for col in self.tab_config["columns"]
            if col.get("visible", True) and col["key"] != "delete"
        ]
        html.append("<tr>")
        for col in visible_columns:
            html.append(f"<th>{col['display']}</th>")
        html.append("</tr>")

        # Create table rows
        for row_data in data_rows:
            html.append("<tr>")
            for col in visible_columns:
                key = col["key"]
                value = getattr(row_data, key, "")

                # --- Path Relativization Logic ---
                # Check if this column typically contains a file path
                path_keys = [
                    "logfile",
                    "imagedir",
                    "workdir",
                    "report",
                    "scale_log",
                    "truncate_log",
                    "truncate_mtz",
                    "solve",
                    "Summary",
                ]
                if key in path_keys and isinstance(value, str) and value:
                    relative_path = make_path_relative(value, base_dir)
                    # For the 'Summary' column (report), link to the file itself
                    if key == "Summary":
                        report_relative_path = make_path_relative(
                            getattr(row_data, "Summary", ""), base_dir
                        )
                        map_relative_path = make_path_relative(
                            getattr(row_data, "solve", ""), base_dir
                        )
                        links = []
                        if report_relative_path:
                            links.append(f"<a href='{report_relative_path}'>Report</a>")
                        if map_relative_path:
                            # Coot links won't work in a browser, so we just link to the directory
                            links.append(
                                f"<a href='{os.path.dirname(map_relative_path)}'>Map Dir</a>"
                            )
                        cell_content = " | ".join(links)
                    else:
                        cell_content = f"<a href='{relative_path}'>{os.path.basename(value) or '[Open]'}</a>"
                else:
                    cell_content = str(value)

                html.append(f"<td>{cell_content}</td>")
            html.append("</tr>")

        html.append("</table></body></html>")

        # 4. Write to file
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                f.write("\n".join(html))
            QMessageBox.information(
                self, "Export Successful", f"HTML report saved to:\n{save_path}"
            )
            # Optionally open the file
            QDesktopServices.openUrl(QUrl.fromLocalFile(save_path))
        except Exception as e:
            QMessageBox.critical(
                self, "Export Failed", f"Could not write HTML file:\n{e}"
            )

    def _get_master_files_from_row(self, row_data):
        """
        Helper to extract a list of master file paths from a row object.
        Supports methods for 'Datasets' (master_files), 'Processing' (datasets, run_stats),
        and fallbacks (imagedir/set).
        """
        files = set()
        
        # 1. 'master_files' (Datasets tab standard)
        mf_json = getattr(row_data, "master_files", None)
        if mf_json:
            try:
                fl = json.loads(mf_json)
                if isinstance(fl, list):
                    files.update(fl)
            except (json.JSONDecodeError, TypeError):
                pass
        
        # 2. 'datasets' (Processing tab standard)
        ds_json = getattr(row_data, "datasets", None)
        if ds_json:
            try:
                fl = json.loads(ds_json)
                if isinstance(fl, list):
                    files.update(fl)
            except (json.JSONDecodeError, TypeError):
                pass

        # 3. 'run_stats' (Processing fallback)
        if not files:
            rs_json = getattr(row_data, "run_stats", None)
            if rs_json:
                try:
                    stats = json.loads(rs_json)
                    dpath = stats.get("dataset")
                    if dpath and isinstance(dpath, str):
                        files.add(dpath)
                except (json.JSONDecodeError, TypeError):
                    pass

        # 4. 'imagedir' + 'imageSet' (Legacy fallback)
        if not files:
            idir = getattr(row_data, "imagedir", None)
            iset = getattr(row_data, "imageSet", None)
            if idir and iset:
                # heuristic: imageSet often format "file:name_".
                parts = iset.split(":")
                prefix = parts[-1] if len(parts) > 1 else parts[0]
                if not prefix.endswith("_"):
                     prefix += "_"
                # Assume master.h5 standard
                files.add(os.path.join(idir, f"{prefix}master.h5"))

        return list(files)

    def on_view_images(self):
        """
        Gathers master files from selected 'Datasets' rows and launches the
        image_viewer application in a detached process.
        """
        selection_model = self.table_view.selectionModel()
        selected_indices = selection_model.selectedRows()

        all_master_files = set()  # Use a set to gather unique file paths

        for index in selected_indices:
            row_num = index.row()
            data_object = self.model._data[row_num]
            
            # Use unified helper
            row_files = self._get_master_files_from_row(data_object)
            all_master_files.update(row_files)

        if not all_master_files:
            QMessageBox.warning(
                self,
                "No Master Files Found",
                "Could not find any master files in the selected rows.",
            )
            return

        # --- Launch the image_viewer process ---
        try:
            # Assume 'iv' is the command to run your image viewer.
            # This might need to be an absolute path depending on your environment.
            iv_path = ProgramConfig.get_program_path("iv")
            if not iv_path:
                raise FileNotFoundError("Image Viewer (iv) path not configured.")
            command = [iv_path] + list(all_master_files)

            logger.info(f"Launching image viewer with command: {' '.join(command)}")

            # Use Popen to launch the process and immediately detach from it.
            # This allows the Data Viewer to remain responsive and not wait for
            # the image viewer to close.
            subprocess.Popen(command)

            self.window().status_bar.showMessage(
                f"Launched image viewer for {len(all_master_files)} dataset(s).", 5000
            )

        except FileNotFoundError:
            error_msg = "Error: The 'iv' command was not found.\nPlease ensure the image viewer is installed and in your system's PATH."
            QMessageBox.critical(self, "Command Not Found", error_msg)
            logger.error(error_msg)
        except Exception as e:
            error_msg = (
                f"An unexpected error occurred while launching the image viewer:\n\n{e}"
            )
            QMessageBox.critical(self, "Launch Error", error_msg)
            logger.error(error_msg, exc_info=True)

    def on_ai_summary(self):
        """
        Extracts visible data, warns the user, and sends it to the AI for analysis.
        """
        # 1. Ask for Consent
        consent_msg = (
            "You are about to send the CURRENTLY FILTERED data metadata to an AI model for analysis.\n\n"
            "RISK WARNING: Your metadata (filenames, unit cells, stats) will be transmitted over the "
            "network to the Argo AI service (https://argo.anl.gov). Ensure you are authorized to share this data.\n\n"
            "Do you want to proceed?"
        )
        reply = QMessageBox.question(
            self, "Data Privacy Warning", consent_msg, 
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return

        # 2. Extract Data
        # We assume the current model._data is what we want to analyze (respects filters)
        rows_data = self.model._data
        if not rows_data:
            QMessageBox.information(self, "No Data", "No data to analyze.")
            return

        # Prepare AI Client to check model capabilities
        client = AIClient()
        if not client.api_key and not os.environ.get("USER"):
             # Fallback if no key set, though AIClient tries to use USER env 
             pass
        
        model_name = client.model_name
        
        # Dynamic Row Limit Logic
        # Estimate: 1 row ~ 50-80 tokens (JSON format)
        # Default limit: 300 rows (approx 15k-24k tokens) -> safe for modern models
        MAX_ROWS = 300 
        
        if len(rows_data) > MAX_ROWS:
            QMessageBox.warning(self, "Data Truncated", f"Analysis limited to top {MAX_ROWS} rows (out of {len(rows_data)}) to fit within AI context limits.")
            rows_data = rows_data[:MAX_ROWS]

        # Extract relevant fields to JSON
        extracted = []
        for row in rows_data:
            item = {
                "id": getattr(row, "id", None),
                "sample": getattr(row, "sampleName", None) or getattr(row, "name", "N/A"),
                "state": getattr(row, "state", ""),
                "unitcell": getattr(row, "unitcell", getattr(row, "Cell", "")),
                "spacegroup": getattr(row, "spacegroup", getattr(row, "Symm", "")),
                "resolution": getattr(row, "highresolution", getattr(row, "h_res", "")),
                "isa": getattr(row, "isa", ""),
                "rsym": getattr(row, "rmerge", getattr(row, "Rsym", "")),
                "completeness": getattr(row, "completeness", getattr(row, "Cmpl", "")),
                "isigma": getattr(row, "isigmai", getattr(row, "IsigI", "")),
                "imagedir": getattr(row, "imagedir", ""),
                "table1": getattr(row, "table1", ""),
            }
            extracted.append(item)
        
        data_json = json.dumps(extracted, indent=2)
        
        # Log estimate
        est_tokens = len(data_json) / 4.0 # Crude char/4 estimate
        logger.info(f"AI Summary: Preparing to send {len(extracted)} rows. Approx {int(est_tokens)} tokens. Model: {model_name}")

        # 4. Construct Prompt
        prompt_file_path = os.path.join(os.path.dirname(__file__), "ai_summary_prompt.txt")
        try:
            with open(prompt_file_path, "r") as f:
                system_prompt = f.read()
        except Exception as e:
            logger.warning(f"Could not load AI prompt from {prompt_file_path}: {e}. Using default.")
            system_prompt = (
                "You are an expert X-ray crystallography data analyst. "
                "Analyze the provided JSON data of processing results. "
                "Tasks:\n"
                "1. Group samples by similar Unit Cell parameters (clustering).\n"
                "2. Identify the BEST dataset for each group based on Resolution, ISa, Completeness, and Rsym.\n"
                "3. Identify POTENTIAL PROBLEMS:\n"
                "   - Detector too far? (Check if I/sigma > 2 at highest resolution shell, implying potential for higher res).\n"
                "   - Radiation decay? (Check R-factors, though limited data here).\n"
                "   - Incomplete data (< 90%).\n"
                "4. Suggest improvements.\n"
                "Output format: Concise Markdown."
            )
        
        user_prompt = f"Here is the data:\n```json\n{data_json}\n```"
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        # 5. Run in Worker (reusing existing Worker class)
        self.window().status_bar.showMessage(f"AI ({model_name}) is analyzing {len(extracted)} rows... this may take a moment...")
        self.ai_summary_button.setEnabled(False)
        
        # We need a wrapper to call client.generate_code (which handles chat completion)
        def run_ai():
            return client.generate_code(messages)

        self.ai_thread = QThread()
        self.ai_worker = Worker(run_ai)
        self.ai_worker.moveToThread(self.ai_thread)
        
        self.ai_thread.started.connect(self.ai_worker.run)
        
        # Success handler
        def on_success(result_text):
            self.ai_summary_button.setEnabled(True)
            self.window().status_bar.clearMessage()
            dialog = AIAnalysisDialog(result_text, data_json, model_name=model_name, parent=self)
            dialog.exec_()
            
        # Error handler
        def on_error(e):
            self.ai_summary_button.setEnabled(True)
            self.window().status_bar.clearMessage()
            QMessageBox.critical(self, "AI Analysis Failed", f"Error: {e}")

        self.ai_worker.finished.connect(on_success)
        self.ai_worker.error.connect(on_error)
        
        # Cleanup
        self.ai_worker.finished.connect(self.ai_thread.quit)
        self.ai_worker.error.connect(self.ai_thread.quit)
        self.ai_thread.finished.connect(self.ai_worker.deleteLater)
        self.ai_thread.finished.connect(self.ai_thread.deleteLater)
        
        self.ai_thread.start()

    def on_view_with_adxv(self):
        """
        Launches ADXV with the first master file of the selected dataset.
        """
        selection_model = self.table_view.selectionModel()
        # This action is only enabled for a single selection, so we can safely take the first one
        index = selection_model.selectedRows()[0]
        data_object = self.model._data[index.row()]

        try:
            file_list = self._get_master_files_from_row(data_object)
            if not file_list:
                raise ValueError("No master files found for this entry.")

            first_master_file = file_list[0]

            # Path to the adxv executable
            adxv_path = ProgramConfig.get_program_path("adxv")
            if not os.path.exists(adxv_path):
                raise FileNotFoundError(f"ADXV executable not found at {adxv_path}")

            command = [adxv_path, first_master_file]
            logger.info(f"Launching ADXV with command: {' '.join(command)}")
            subprocess.Popen(command)
            self.window().status_bar.showMessage(
                f"Launching ADXV for {os.path.basename(first_master_file)}...", 4000
            )

        except (ValueError, IndexError, json.JSONDecodeError) as e:
            QMessageBox.warning(
                self, "ADXV Launch Error", f"Could not determine master file: {e}"
            )
        except FileNotFoundError as e:
            QMessageBox.critical(self, "ADXV Not Found", str(e))
        except Exception as e:
            QMessageBox.critical(
                self,
                "Launch Error",
                f"An unexpected error occurred while launching ADXV:\n\n{e}",
            )

    def on_open_containing_folder(self):
        """
        Opens the system's file explorer to the directory containing the dataset.
        Uses the first selected row as the target.
        """
        selection_model = self.table_view.selectionModel()
        # Use the first selected row to determine the directory
        index = selection_model.selectedRows()[0]
        data_object = self.model._data[index.row()]

        data_dir = None

        # Try to get data_dir from the headers first (Dataset tab specific)
        headers_json = getattr(data_object, "headers", "[]")
        try:
            headers_list = json.loads(headers_json)
            if headers_list and isinstance(headers_list, list):
                # The headers metadata contains a 'data_rel_dir' and 'data_dir_root'
                header0 = headers_list[0]
                data_dir_root = header0.get("data_dir_root")
                data_rel_dir = header0.get("data_rel_dir")
                if data_dir_root and data_rel_dir is not None:
                    data_dir = os.path.join(data_dir_root, data_rel_dir.lstrip("/\\"))
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Could not parse headers JSON to find data_dir.")

        # Fallback using unified helper
        if not data_dir:
            file_list = self._get_master_files_from_row(data_object)
            if file_list:
                data_dir = os.path.dirname(file_list[0])

        if not data_dir or not os.path.isdir(data_dir):
            QMessageBox.warning(
                self,
                "Directory Not Found",
                f"Could not determine or find a valid data directory for the selected item.",
            )
            return

        logger.info(f"Opening folder: {data_dir}")
        QDesktopServices.openUrl(QUrl.fromLocalFile(data_dir))

    def on_open_terminal(self):
        """
        Opens a terminal in the 'workdir' if available, otherwise falls back to
        the main data directory. Uses the first selected row.
        """
        selection_model = self.table_view.selectionModel()
        index = selection_model.selectedRows()[0]
        data_object = self.model._data[index.row()]

        target_dir = None

        # 1. Processing and Strategy usually have a 'workdir' or 'directory' column in the ORM model
        if hasattr(data_object, "workdir") and data_object.workdir:
            target_dir = data_object.workdir
        elif hasattr(data_object, "directory") and data_object.directory:
            target_dir = data_object.directory
            
        # 2. If neither exists, or we are in the Datasets tab, fall back to extracting it
        # similarly to how open_containing_folder does it
        if not target_dir:
            headers_json = getattr(data_object, "headers", "[]")
            try:
                headers_list = json.loads(headers_json)
                if headers_list and isinstance(headers_list, list):
                    header0 = headers_list[0]
                    data_dir_root = header0.get("data_dir_root")
                    data_rel_dir = header0.get("data_rel_dir")
                    if data_dir_root and data_rel_dir is not None:
                        target_dir = os.path.join(data_dir_root, data_rel_dir.lstrip("/\\"))
            except (json.JSONDecodeError, TypeError):
                pass
            
            # Final fallback
            if not target_dir:
                file_list = self._get_master_files_from_row(data_object)
                if file_list:
                    target_dir = os.path.dirname(file_list[0])

        if not target_dir or not os.path.isdir(target_dir):
            QMessageBox.warning(
                self,
                "Directory Not Found",
                f"Could not determine or find a valid directory for the selected item.",
            )
            return

        logger.info(f"Opening terminal in: {target_dir}")
        self._launch_terminal(target_dir)

    def _launch_terminal(self, working_directory):
        """Attempts to launch a terminal emulator in the given directory."""
        # Common linux terminal emulators to try
        terminals = [
            ["gnome-terminal", "--working-directory"],
            ["konsole", "--workdir"],
            ["xfce4-terminal", "--working-directory"],
            ["xterm", "-e", "bash", "-c", f"cd {working_directory} && exec bash"],
        ]
        
        for term_cmd in terminals:
            try:
                # E.g. gnome-terminal --working-directory=/path/to/workdir
                if term_cmd[0] != "xterm":
                   cmd = term_cmd + [working_directory]
                else:
                   cmd = term_cmd
                   
                # Attempt to launch the terminal without blocking
                subprocess.Popen(cmd, cwd=working_directory, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return
            except FileNotFoundError:
                continue
            except Exception as e:
                logger.warning(f"Failed to launch terminal {term_cmd[0]}: {e}")
                
        QMessageBox.warning(
            self,
            "Terminal Launcher Failed",
            "Could not find a supported terminal emulator (gnome-terminal, konsole, xfce4-terminal, xterm).",
        )
