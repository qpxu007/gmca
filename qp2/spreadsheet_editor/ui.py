
import os
import re
import requests
import tempfile
import grp
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, 
    QPushButton, QLabel, QFileDialog, QMessageBox, QFrame, QApplication,
    QScrollArea, QInputDialog, QTableWidget, QTableWidgetItem, QDialog, 
    QDialogButtonBox, QHeaderView
)
from PyQt5.QtCore import Qt, QMimeData, QSize
from PyQt5.QtGui import QDrag, QPixmap, QPainter, QColor, QFont

from .logic import SpreadsheetManager, Puck, REQUIRED_HEADERS
from qp2.utils.icon import generate_icon_with_text
from qp2.xio.user_group_manager import UserGroupManager
from qp2.config.servers import ServerConfig

try:
    from qp2.data_viewer.utils import get_rpc_url
except ImportError:
    def get_rpc_url():
        return ServerConfig.get_pbs_rpc_url()

class PuckEditorDialog(QDialog):
    def __init__(self, puck: Puck, slot_name=None, parent=None):
        super().__init__(parent)
        self.puck = puck
        title = f"Edit Puck {puck.original_label}"
        if slot_name:
            title += f" (in Slot {slot_name})"
        self.setWindowTitle(title)
        self.resize(1000, 600)
        
        layout = QVBoxLayout(self)
        
        # Table
        self.table = QTableWidget()
        self.table.setRowCount(len(puck.rows))
        self.table.setColumnCount(len(REQUIRED_HEADERS))
        self.table.setHorizontalHeaderLabels(REQUIRED_HEADERS)
        
        # Populate
        for r, row_data in enumerate(puck.rows):
            old_port = row_data.get("Port", "").strip()
            
            # Determine values to display
            # If slot_name is provided, we simulate the transforms that happen on save
            new_port = None
            if slot_name:
                new_port = f"{slot_name}{r+1}"
            
            for c, header in enumerate(REQUIRED_HEADERS):
                value = row_data.get(header, "")
                
                if slot_name and new_port:
                    if header == "Port":
                        value = new_port
                    elif header == "CrystalID":
                        if value == old_port:
                            value = new_port
                    elif header == "Directory":
                        if value and old_port:
                             # Same regex as logic.py for consistency
                             pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(old_port)}(?![A-Za-z0-9])")
                             value = pattern.sub(new_port, value)
                
                item = QTableWidgetItem(value)
                
                # Make Port read-only
                if header == "Port":
                    item.setFlags(item.flags() ^ Qt.ItemIsEditable)
                    item.setBackground(QColor("#f0f0f0"))
                
                self.table.setItem(r, c, item)
        
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        layout.addWidget(self.table)
        
        # Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept(self):
        # Save data back to puck
        new_rows = []
        for r in range(self.table.rowCount()):
            row_dict = {}
            for c in range(self.table.columnCount()):
                header = REQUIRED_HEADERS[c]
                item = self.table.item(r, c)
                row_dict[header] = item.text().strip() if item else ""
            
            # Logic: If Directory is empty, default to CrystalID
            if not row_dict.get("Directory") and row_dict.get("CrystalID"):
                row_dict["Directory"] = row_dict["CrystalID"]
            
            new_rows.append(row_dict)
        
        self.puck.rows = new_rows
        super().accept()

class PuckWidget(QFrame):
    """
    Visual representation of a Puck.
    """
    def __init__(self, puck: Puck, parent=None):
        super().__init__(parent)
        self.puck = puck
        self.drag_start_pos = None
        
        self.setFrameStyle(QFrame.Panel | QFrame.Raised)
        self.setLineWidth(2)
        
        layout = QVBoxLayout(self)
        
        # Original Label
        self.lbl_name = QLabel()
        self.lbl_name.setAlignment(Qt.AlignCenter)
        font = self.lbl_name.font()
        font.setBold(True)
        self.lbl_name.setFont(font)
        layout.addWidget(self.lbl_name)
        
        # Summary
        self.lbl_info = QLabel()
        self.lbl_info.setAlignment(Qt.AlignCenter)
        self.lbl_info.setWordWrap(True)
        layout.addWidget(self.lbl_info)
        
        self.refresh_ui()
        
        # Visual style
        self.setStyleSheet("background-color: #d4e6f1; border-radius: 5px;")
        self.setAutoFillBackground(True)

    def refresh_ui(self):
        self.lbl_name.setText(f"Puck {self.puck.original_label}")
        self.lbl_info.setText(self.puck.get_summary())

    def sizeHint(self):
        return QSize(100, 80)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.drag_start_pos = event.pos()
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if not self.drag_start_pos:
            return
            
        if (event.pos() - self.drag_start_pos).manhattanLength() < QApplication.startDragDistance():
            return
            
        # Start Drag
        drag = QDrag(self)
        mime = QMimeData()
        
        # Identify source slot
        parent_slot = self.parent()
        if hasattr(parent_slot, 'letter'):
            mime.setText(parent_slot.letter)
        
        drag.setMimeData(mime)
        
        pixmap = self.grab()
        drag.setPixmap(pixmap)
        drag.setHotSpot(event.pos())
        
        self.hide()
        drag.exec_(Qt.MoveAction)
        self.show()
        self.drag_start_pos = None

    def mouseDoubleClickEvent(self, event):
        parent_slot = self.parent()
        slot_name = parent_slot.letter if hasattr(parent_slot, 'letter') else None
        
        dialog = PuckEditorDialog(self.puck, slot_name=slot_name, parent=self)
        if dialog.exec_() == QDialog.Accepted:
            self.refresh_ui()

class SlotWidget(QFrame):
    """
    A slot that can hold a PuckWidget. Supports Drag & Drop.
    """
    def __init__(self, letter, parent_window):
        super().__init__()
        self.letter = letter
        self.parent_window = parent_window
        self.puck_widget = None
        self.puck_data = None  # The Puck object
        
        self.setFrameStyle(QFrame.StyledPanel | QFrame.Sunken)
        self.setLineWidth(2)
        self.setAcceptDrops(True)
        self.setMinimumSize(120, 100)
        
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(2, 2, 2, 2)
        
        self.lbl_slot = QLabel(f"Slot {letter}")
        self.lbl_slot.setAlignment(Qt.AlignCenter)
        self.lbl_slot.setStyleSheet("color: gray; font-size: 10px;")
        self.layout.addWidget(self.lbl_slot)
        
        self.layout.addStretch()

    def set_puck(self, puck: Puck):
        # Remove existing if any
        self.clear_puck()
        
        if puck:
            self.puck_data = puck
            self.puck_widget = PuckWidget(puck, self)
            # Insert before the stretch (index 1)
            self.layout.insertWidget(1, self.puck_widget)
            self.lbl_slot.setText(f"Slot {self.letter}") # Keep title simple

    def clear_puck(self):
        if self.puck_widget:
            self.layout.removeWidget(self.puck_widget)
            self.puck_widget.deleteLater()
            self.puck_widget = None
        self.puck_data = None
    
    # Drag initiation moved to PuckWidget
    
    def dragEnterEvent(self, event):
        if event.mimeData().hasText():
            event.accept()
        else:
            event.ignore()

    def dropEvent(self, event):
        source_letter = event.mimeData().text()
        if source_letter == self.letter:
            event.ignore()
            return
            
        # Call the main window to handle the move/swap
        self.parent_window.move_puck(source_letter, self.letter)
        event.setDropAction(Qt.MoveAction)
        event.accept()

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Spreadsheet Puck Editor")
        self.resize(1000, 600)
        app_icon = generate_icon_with_text(text="SE", bg_color="#3498db", size=128)
        self.setWindowIcon(app_icon)
        
        self.manager = SpreadsheetManager()
        self.slots = {} # Map 'A' -> SlotWidget
        self.user_group_manager = UserGroupManager()
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Toolbar / Top Area
        top_layout = QHBoxLayout()
        self.btn_new = QPushButton("New Spreadsheet")
        self.btn_new.clicked.connect(self.create_new_spreadsheet)
        
        self.btn_load = QPushButton("Load Spreadsheet")
        self.btn_load.clicked.connect(self.load_spreadsheet)
        self.btn_save = QPushButton("Export New Spreadsheet")
        self.btn_save.clicked.connect(self.save_spreadsheet)
        self.btn_save.setEnabled(False)
        
        self.btn_http = QPushButton("Send to pyBluice")
        self.btn_http.clicked.connect(self.upload_to_http)
        self.btn_http.setEnabled(False) # Enable only when data loaded/new
        
        self.btn_config = QPushButton("Configure Pucks")
        self.btn_config.clicked.connect(self.configure_pucks)

        self.lbl_filename = QLabel("No file loaded.")
        font = self.lbl_filename.font()
        font.setItalic(True)
        self.lbl_filename.setFont(font)

        top_layout.addWidget(self.btn_new)
        top_layout.addWidget(self.btn_load)
        top_layout.addWidget(self.btn_save)
        top_layout.addWidget(self.btn_http)
        top_layout.addWidget(self.btn_config)
        top_layout.addStretch()
        top_layout.addWidget(self.lbl_filename)
        
        main_layout.addLayout(top_layout)
        
        # Grid Area
        grid_layout = QGridLayout()
        self.grid_container = QWidget()
        self.grid_container.setLayout(grid_layout)
        
        # Build initial grid
        self.build_grid()
            
        # Scroll Area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self.grid_container)
        
        main_layout.addWidget(scroll)
        
        self.status_bar = self.statusBar()
        self.status_bar.showMessage("Ready.")

    def build_grid(self):
        # Clear existing layout
        layout = self.grid_container.layout()
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()
        
        self.slots = {}
        cols = 6
        for i, puck_name in enumerate(self.manager.puck_names):
            slot = SlotWidget(puck_name, self)
            self.slots[puck_name] = slot
            row = i // cols
            col = i % cols
            layout.addWidget(slot, row, col)

    def configure_pucks(self):
        current_names = ", ".join(self.manager.puck_names)
        text, ok = QInputDialog.getText(
            self, 
            "Configure Pucks", 
            "Comma-separated Puck Names:", 
            text=current_names
        )
        if ok and text:
            new_names = [n.strip() for n in text.split(",") if n.strip()]
            if not new_names:
                QMessageBox.warning(self, "Invalid Input", "Puck list cannot be empty.")
                return
                
            self.manager.puck_names = new_names
            self.build_grid()
            self.status_bar.showMessage(f"Updated configuration: {len(new_names)} pucks.")

    def check_user_permission(self):
        username = os.getenv("USER")
        if not username:
            return False
            
        # Check Staff
        if self.user_group_manager.is_staff(username):
            return True
            
        # Check specific groups
        special_groups = ['bl1-first-day', 'bl2-first-day']
        try:
            # Check secondary groups
            user_groups = [g.gr_name for g in grp.getgrall() if username in g.gr_mem]
            # Check primary group
            import pwd
            gid = pwd.getpwnam(username).pw_gid
            primary_group = grp.getgrgid(gid).gr_name
            user_groups.append(primary_group)
            
            for g in special_groups:
                if g in user_groups:
                    return True
        except Exception:
            pass # Fail safe
            
        return False

    def create_new_spreadsheet(self):
        pucks_map = self.manager.create_empty_pucks()
        
        # Clear existing
        for slot in self.slots.values():
            slot.clear_puck()
            
        # Populate slots
        for letter, puck in pucks_map.items():
            if letter in self.slots:
                self.slots[letter].set_puck(puck)
        
        self.btn_save.setEnabled(True)
        self.btn_http.setEnabled(self.check_user_permission())
        self.lbl_filename.setText("New Spreadsheet")
        self.status_bar.showMessage(f"Created new empty spreadsheet with {len(pucks_map)} pucks.")

    def load_spreadsheet(self):
        default_dir = os.path.expanduser("~/Downloads")
        filepath, _ = QFileDialog.getOpenFileName(
            self, 
            "Open Spreadsheet", 
            default_dir, 
            "Spreadsheet Files (*.csv *.xls *.xlsx);;CSV Files (*.csv);;Excel Files (*.xls *.xlsx)"
        )
        if not filepath:
            return
            
        pucks_map = self.manager.load_file(filepath)
        
        if self.manager.errors:
            QMessageBox.critical(self, "Error Loading File", "\n".join(self.manager.errors))
            return
        
        # Clear existing content from slots
        for slot in self.slots.values():
            slot.clear_puck()
            
        # Populate slots
        # If the file contains Puck 'C', it goes to Slot 'C' initially.
        for letter, puck in pucks_map.items():
            if letter in self.slots:
                self.slots[letter].set_puck(puck)
            else:
                print(f"Warning: Found puck with label {letter} but no matching slot.")

        self.btn_save.setEnabled(True)
        self.btn_http.setEnabled(self.check_user_permission())
        self.lbl_filename.setText(os.path.basename(filepath)) # Display filename persistently
        self.status_bar.showMessage(f"Loaded {len(pucks_map)} pucks from {os.path.basename(filepath)}")

    def upload_to_http(self):
        url = get_rpc_url()
        if not url:
            default_url = ServerConfig.get_pbs_rpc_url() or "http://bl1ws3-40g:8001/rpc"
            text, ok = QInputDialog.getText(self, "RPC URL", "Enter HTTP RPC URL:", text=default_url)
            if ok and text:
                url = text.strip()
            else:
                return

        # Create a temp file
        # We default to .xlsx for structured data, or .csv if pandas missing (handled by logic but filename matters)
        # Assuming pandas is present as per recent updates
        try:
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                temp_path = tmp.name
            
            # Save current state to temp file
            ordered_pucks = []
            for name in self.manager.puck_names:
                slot = self.slots[name]
                ordered_pucks.append(slot.puck_data) # Can be None
            
            self.manager.save_file(temp_path, ordered_pucks)
            
            # Send Request
            puck_map = "".join(self.manager.puck_names)
            payload = {
                "module": "spreadsheet_import",
                "path": temp_path,
                "map": puck_map
            }
            
            self.status_bar.showMessage(f"Uploading to {url}...")
            resp = requests.post(url, data=payload, timeout=10)
            
            if resp.status_code == 200:
                QMessageBox.information(self, "Success", "Spreadsheet uploaded successfully.")
                self.status_bar.showMessage("Upload complete.")
            else:
                QMessageBox.critical(self, "Error", f"Upload failed: {resp.status_code}\n{resp.text}")
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred: {str(e)}")
        finally:
            # We assume the server reads the file immediately or we leave it?
            # If server is remote (not sharing filesystem), "path" param won't work.
            # But the user specifically asked for "spreadsheet_import(path=...)" style.
            # This implies shared filesystem.
            # We should probably NOT delete it immediately if the server is async, 
            # but usually RPC calls are fast or we can't manage lifecycle.
            # Let's leave it or delete?
            # If we delete, server might fail if it reads later.
            # But tempfile usually deletes on close if delete=True. We set delete=False.
            # Let's leave it for now, typically /tmp is cleaned up.
            pass

    def move_puck(self, source_letter, target_letter):
        # Swap logic
        source_slot = self.slots[source_letter]
        target_slot = self.slots[target_letter]
        
        source_puck = source_slot.puck_data
        target_puck = target_slot.puck_data
        
        # Perform visual swap
        source_slot.set_puck(target_puck)
        target_slot.set_puck(source_puck)
        
        self.status_bar.showMessage(f"Moved Puck from {source_letter} to {target_letter}")

    def save_spreadsheet(self):
        filepath, _ = QFileDialog.getSaveFileName(
            self, 
            "Save Spreadsheet", 
            "", 
            "Spreadsheet Files (*.csv *.xls *.xlsx);;CSV Files (*.csv);;Excel Files (*.xls *.xlsx)"
        )
        if not filepath:
            return
            
        # Collect data in order of defined puck names
        ordered_pucks = []
        for name in self.manager.puck_names:
            slot = self.slots[name]
            ordered_pucks.append(slot.puck_data) # Can be None
            
        try:
            self.manager.save_file(filepath, ordered_pucks)
            QMessageBox.information(self, "Success", "Spreadsheet saved successfully.")
            self.status_bar.showMessage(f"Saved to {filepath}")
        except Exception as e:
            QMessageBox.critical(self, "Error Saving", str(e))

