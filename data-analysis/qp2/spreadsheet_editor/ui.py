
import os
import re
import requests
import tempfile
import grp
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QPushButton, QLabel, QFileDialog, QMessageBox, QFrame, QApplication,
    QScrollArea, QInputDialog, QTableWidget, QTableWidgetItem, QDialog,
    QDialogButtonBox, QHeaderView, QLineEdit, QMenu, QSizePolicy
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
    _MODEL_COL = REQUIRED_HEADERS.index("ModelPath")
    _SEQ_COL   = REQUIRED_HEADERS.index("SequencePath")
    _PUCK_COL  = REQUIRED_HEADERS.index("Puck")
    _NO_ACTIONS = {"Port", "Puck"}

    def __init__(self, puck: Puck, slot_name=None, parent=None):
        super().__init__(parent)
        self.puck = puck
        self.slot_name = slot_name
        self._updating = False

        title = f"Edit Puck {puck.original_label}"
        if slot_name:
            title += f" (in Slot {slot_name})"
        self.setWindowTitle(title)
        self.resize(1100, 600)

        layout = QVBoxLayout(self)

        # Hint label
        hint = QLabel("Right-click a column header for Fill Rest / Fill All helpers.")
        hint.setStyleSheet("color: gray; font-size: 10px;")
        layout.addWidget(hint)

        self.table = QTableWidget()
        self.table.setRowCount(len(puck.rows))
        self.table.setColumnCount(len(REQUIRED_HEADERS))
        self.table.setHorizontalHeaderLabels(REQUIRED_HEADERS)

        hdr = self.table.horizontalHeader()
        hdr.setContextMenuPolicy(Qt.CustomContextMenu)
        hdr.customContextMenuRequested.connect(self._on_header_context_menu)

        for r, row_data in enumerate(puck.rows):
            old_port = row_data.get("Port", "").strip()
            new_port = f"{slot_name}{r+1}" if slot_name else None

            for c, header in enumerate(REQUIRED_HEADERS):
                value = row_data.get(header, "")
                if slot_name and new_port:
                    if header == "Port":
                        value = new_port
                    elif header == "CrystalID" and value == old_port:
                        value = new_port
                    elif header == "Directory" and value and old_port:
                        pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(old_port)}(?![A-Za-z0-9])")
                        value = pattern.sub(new_port, value)

                if c in (self._MODEL_COL, self._SEQ_COL):
                    self._set_browse_cell(r, c, value)
                else:
                    item = QTableWidgetItem(value)
                    if header == "Port":
                        item.setFlags(item.flags() ^ Qt.ItemIsEditable)
                        item.setBackground(QColor("#f0f0f0"))
                    self.table.setItem(r, c, item)

        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.cellChanged.connect(self._on_cell_changed)
        layout.addWidget(self.table)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    # --- Browse cell helpers ---

    def _set_browse_cell(self, row, col, value):
        widget = QWidget()
        h = QHBoxLayout(widget)
        h.setContentsMargins(0, 0, 0, 0)
        h.setSpacing(1)
        edit = QLineEdit(value)
        btn = QPushButton("…")
        btn.setFixedWidth(26)
        h.addWidget(edit)
        h.addWidget(btn)
        is_model = (col == self._MODEL_COL)
        btn.clicked.connect(lambda _, e=edit, im=is_model: self._browse_file(e, im))
        self.table.setCellWidget(row, col, widget)

    def _browse_file(self, edit_widget, is_model):
        if is_model:
            title, filt = "Select Model File", "Structure files (*.pdb *.cif *.ent);;All files (*)"
        else:
            title, filt = "Select Sequence File", "Sequence files (*.fasta *.fa *.seq *.txt);;All files (*)"
        path, _ = QFileDialog.getOpenFileName(self, title, os.path.expanduser("~"), filt)
        if path:
            edit_widget.setText(path)

    # --- Generic cell value get/set (handles both widget cells and item cells) ---

    def _get_cell_value(self, row, col):
        widget = self.table.cellWidget(row, col)
        if widget:
            edit = widget.findChild(QLineEdit)
            return edit.text() if edit else ""
        item = self.table.item(row, col)
        return item.text() if item else ""

    def _set_cell_value(self, row, col, value):
        widget = self.table.cellWidget(row, col)
        if widget:
            edit = widget.findChild(QLineEdit)
            if edit:
                edit.setText(value)
            return
        item = self.table.item(row, col)
        if item:
            item.setText(value)
        else:
            self.table.setItem(row, col, QTableWidgetItem(value))

    # --- Puck column: bulk update all rows ---

    def _on_cell_changed(self, row, col):
        if self._updating or col != self._PUCK_COL:
            return
        item = self.table.item(row, col)
        if not item:
            return
        value = item.text()
        self._updating = True
        for r in range(self.table.rowCount()):
            if r != row:
                self._set_cell_value(r, self._PUCK_COL, value)
        self._updating = False

    # --- Column header context menu: Fill Rest / Fill All ---

    def _on_header_context_menu(self, pos):
        col = self.table.horizontalHeader().logicalIndexAt(pos)
        if col < 0 or REQUIRED_HEADERS[col] in self._NO_ACTIONS:
            return
        menu = QMenu(self)
        fill_rest = menu.addAction("Fill Rest — auto-number from last filled row")
        fill_all  = menu.addAction("Fill All — copy first value to every row")
        action = menu.exec_(self.table.horizontalHeader().mapToGlobal(pos))
        if action == fill_rest:
            self._fill_rest(col)
        elif action == fill_all:
            self._fill_all(col)

    def _fill_rest(self, col):
        n = self.table.rowCount()
        seed_index = -1
        for i in range(n - 1, -1, -1):
            val = self._get_cell_value(i, col).strip()
            # Skip rows that still hold the auto-generated default port value
            default_val = f"{self.slot_name}{i + 1}" if self.slot_name else ""
            if val and val != default_val:
                seed_index = i
                break
        if seed_index == -1:
            return

        seed_value = self._get_cell_value(seed_index, col).strip()
        match = re.match(r'^(.*?)(\d+)$', seed_value)
        if match:
            prefix, next_num = match.group(1), int(match.group(2)) + 1
        else:
            prefix, next_num = seed_value + "_", 1

        existing = set()
        for i in range(seed_index + 1):
            v = self._get_cell_value(i, col).strip()
            if v:
                existing.add(v)

        for i in range(seed_index + 1, n):
            candidate = f"{prefix}{next_num}"
            while candidate in existing:
                next_num += 1
                candidate = f"{prefix}{next_num}"
            self._set_cell_value(i, col, candidate)
            existing.add(candidate)
            next_num += 1

    def _fill_all(self, col):
        first_value = None
        for i in range(self.table.rowCount()):
            v = self._get_cell_value(i, col).strip()
            if v:
                first_value = v
                break
        if not first_value:
            return
        for i in range(self.table.rowCount()):
            self._set_cell_value(i, col, first_value)

    # --- Save ---

    def accept(self):
        new_rows = []
        for r in range(self.table.rowCount()):
            row_dict = {h: self._get_cell_value(r, c) for c, h in enumerate(REQUIRED_HEADERS)}
            if not row_dict.get("Directory") and row_dict.get("CrystalID"):
                row_dict["Directory"] = row_dict["CrystalID"]
            new_rows.append(row_dict)
        self.puck.rows = new_rows
        super().accept()

class PuckAssignmentDialog(QDialog):
    """Bulk-assign physical puck names (the Puck column) to all rows in each slot."""

    def __init__(self, puck_names, slots, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Puck Assignment")
        self.resize(320, 500)
        self.inputs = {}

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Assign a physical puck name to each slot:"))

        table = QTableWidget(len(puck_names), 2)
        table.setHorizontalHeaderLabels(["Slot", "Puck Name"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        table.verticalHeader().setVisible(False)

        for i, name in enumerate(puck_names):
            slot = slots.get(name)
            has_puck = slot is not None and slot.puck_data is not None

            slot_item = QTableWidgetItem(name)
            slot_item.setFlags(Qt.ItemIsEnabled)
            table.setItem(i, 0, slot_item)

            existing = ""
            if has_puck:
                for row in slot.puck_data.rows:
                    v = row.get("Puck", "").strip()
                    if v:
                        existing = v
                        break

            edit = QLineEdit(existing)
            edit.setPlaceholderText("Empty slot" if not has_puck else "e.g. CU 1234")
            edit.setEnabled(has_puck)
            self.inputs[name] = edit
            table.setCellWidget(i, 1, edit)

        layout.addWidget(table)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_assignments(self):
        return {name: edit.text().strip() for name, edit in self.inputs.items()}


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
        puck_name = next(
            (r.get("Puck", "").strip() for r in self.puck.rows if r.get("Puck", "").strip()),
            ""
        )
        label = f"Puck {puck_name}" if puck_name else f"Puck {self.puck.original_label}"
        self.lbl_name.setText(label)
        slot_name = self.parent().letter if hasattr(self.parent(), 'letter') else None
        self.lbl_info.setText(self.puck.get_summary(slot_name=slot_name))

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
        self.current_filepath = None  # Path of the last loaded/saved spreadsheet
        
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

        self.btn_puck_assign = QPushButton("Puck Assignment")
        self.btn_puck_assign.clicked.connect(self.open_puck_assignment)
        self.btn_puck_assign.setEnabled(False)

        self.lbl_filename = QLabel("No file loaded.")
        font = self.lbl_filename.font()
        font.setItalic(True)
        self.lbl_filename.setFont(font)

        top_layout.addWidget(self.btn_new)
        top_layout.addWidget(self.btn_load)
        top_layout.addWidget(self.btn_save)
        top_layout.addWidget(self.btn_http)
        top_layout.addWidget(self.btn_puck_assign)
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
        self.btn_puck_assign.setEnabled(True)
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
            # Check whether every error is a duplicate-CrystalID error so we
            # can offer to auto-fix rather than just rejecting the file.
            dup_prefix = "Duplicate CrystalID"
            all_dup_errors = all(dup_prefix in e for e in self.manager.errors)

            if all_dup_errors:
                dup_lines = "\n".join(self.manager.errors)
                answer = QMessageBox.question(
                    self,
                    "Duplicate CrystalIDs Detected",
                    f"The following duplicate CrystalIDs were found:\n\n{dup_lines}\n\n"
                    "Would you like to fix them automatically by appending a numbered "
                    "suffix (e.g. mycrystal_1, mycrystal_2)?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.Yes,
                )
                if answer == QMessageBox.Yes:
                    pucks_map = self.manager.load_file(filepath, auto_fix_duplicates=True)
                    if self.manager.errors:
                        QMessageBox.critical(
                            self, "Error Loading File", "\n".join(self.manager.errors)
                        )
                        return
                else:
                    return
            else:
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
        self.btn_puck_assign.setEnabled(True)
        self.current_filepath = filepath
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

        # The PyBluice server reads the file by its absolute path on the shared filesystem
        # (/mnt/beegfs).  Writing to /tmp fails because the server runs on a different host
        # that does not share the client's /tmp.  Write the temp file next to the source
        # spreadsheet so it lands on the shared NFS mount.  When no source file is known
        # (new spreadsheet workflow), fall back to /mnt/beegfs/tmp.
        if self.current_filepath and os.path.isdir(os.path.dirname(self.current_filepath)):
            temp_dir = os.path.dirname(self.current_filepath)
        else:
            # The temp file must live on the shared filesystem so the
            # PyBluice server on another host can read it.  Home dirs
            # are on beegfs and always writable by the user.
            temp_dir = os.path.join(os.path.expanduser("~"), ".qp2", "tmp")
            os.makedirs(temp_dir, exist_ok=True)

        temp_path = None
        try:
            ordered_pucks = [self.slots[name].puck_data for name in self.manager.puck_names]
            puck_map = "".join(self.manager.puck_names)

            # Prefer .xlsx (openpyxl); fall back to .xls (xlrd/xlwt) if the server
            # reports a missing-openpyxl error, so the code stays forward-compatible
            # when openpyxl is eventually added to the PyBluice venv.
            for suffix in (".xlsx", ".xls"):
                if temp_path and os.path.exists(temp_path):
                    os.unlink(temp_path)
                with tempfile.NamedTemporaryFile(
                    suffix=suffix, dir=temp_dir, delete=False
                ) as tmp:
                    temp_path = tmp.name

                self.manager.save_file(temp_path, ordered_pucks)

                payload = {
                    "module": "spreadsheet_import",
                    "path": temp_path,
                    "map": puck_map,
                }

                self.status_bar.showMessage(f"Uploading to {url} ({suffix})...")
                resp = requests.post(url, data=payload, timeout=10)

                if resp.status_code == 200:
                    QMessageBox.information(self, "Success", "Spreadsheet uploaded successfully.")
                    self.status_bar.showMessage("Upload complete.")
                    return

                # Retry with .xls only if the failure is an openpyxl import error
                if "openpyxl" in resp.text and suffix == ".xlsx":
                    continue

                QMessageBox.critical(self, "Error", f"Upload failed: {resp.status_code}\n{resp.text}")
                return

        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred: {str(e)}")
        finally:
            # Leave the file on disk; the PyBluice RPC call is synchronous but the server
            # may read it fractionally after the HTTP response returns.  The shared NFS
            # directory is not /tmp so it will not be auto-purged unexpectedly.
            pass

    def open_puck_assignment(self):
        dialog = PuckAssignmentDialog(self.manager.puck_names, self.slots, self)
        if dialog.exec_() == QDialog.Accepted:
            assignments = dialog.get_assignments()
            for slot_name, puck_name in assignments.items():
                slot = self.slots.get(slot_name)
                if slot and slot.puck_data:
                    for row in slot.puck_data.rows:
                        row["Puck"] = puck_name
                    if slot.puck_widget:
                        slot.puck_widget.refresh_ui()
            self.status_bar.showMessage("Puck names assigned.")

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

