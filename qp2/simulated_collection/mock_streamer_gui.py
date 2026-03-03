import sys
import os
import socket
from pathlib import Path
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, 
    QHBoxLayout, QLabel, QLineEdit, QSpinBox, 
    QDoubleSpinBox, QCheckBox, QPushButton, QFileDialog, 
    QListWidget, QTextEdit, QFormLayout, QGroupBox, QMessageBox, QComboBox
)
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import QProcess, Qt

try:
    from qp2.config.servers import ServerConfig
except ImportError:
    # Fallback if running directly without PYTHONPATH set correctly
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from qp2.config.servers import ServerConfig

# Ensure we can find the sibling script
CURRENT_DIR = Path(__file__).resolve().parent
STREAMER_SCRIPT = CURRENT_DIR / "mock_redis_streamer.py"
ICON_PATH = CURRENT_DIR / "mock_icon.svg"

def get_beamline_by_hostname(hostname):
    """
    Determines the beamline (bl1, bl2) based on the hostname.
    Returns None if not detected.
    """
    if hostname.startswith("bl1"):
        return "bl1"
    elif hostname.startswith("bl2"):
        return "bl2"
    return None

class MockStreamerGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("QP2 Mock Redis Streamer")
        if ICON_PATH.exists():
            self.setWindowIcon(QIcon(str(ICON_PATH)))
        self.resize(800, 750)
        
        self.process = None
        
        self.init_ui()
        
    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # --- Configuration Section ---
        config_group = QGroupBox("Configuration")
        config_layout = QFormLayout()
        
        self.host_input = QComboBox()
        self.host_input.setEditable(True) # Allow custom IPs
        
        # Populate Redis hosts from ServerConfig
        redis_hosts = ServerConfig.get_redis_hosts()
        for name, ip in redis_hosts.items():
            self.host_input.addItem(f"{name} ({ip})", ip)
            
        # Determine default selection
        current_hostname = socket.gethostname()
        target_bl = get_beamline_by_hostname(current_hostname)
        
        default_found = False
        if target_bl:
            # Try to find the item that corresponds to the target beamline
            for i in range(self.host_input.count()):
                # Check if the item text starts with the beamline name
                if self.host_input.itemText(i).startswith(target_bl):
                    self.host_input.setCurrentIndex(i)
                    default_found = True
                    break
        
        if not default_found:
            # Fallback to 127.0.0.1 logic
            # Look for an item with data '127.0.0.1' or set text
            index = self.host_input.findData("127.0.0.1")
            if index >= 0:
                self.host_input.setCurrentIndex(index)
            else:
                self.host_input.setEditText("127.0.0.1")
        
        self.port_input = QSpinBox()
        self.port_input.setRange(1, 65535)
        self.port_input.setValue(6379)
        
        self.stream_input = QLineEdit("eiger")
        
        self.rate_input = QDoubleSpinBox()
        self.rate_input.setRange(0.1, 10000.0)
        self.rate_input.setValue(100.0)
        self.rate_input.setSuffix(" Hz")
        
        self.mode_input = QComboBox()
        self.mode_input.addItems(["", "STANDARD", "VECTOR", "RASTER", "SITE"])
        self.mode_input.setEditable(True)
        self.mode_input.setPlaceholderText("Optional override (e.g., RASTER)")

        self.artificial_lag_input = QDoubleSpinBox()
        self.artificial_lag_input.setRange(0.0, 10.0)
        self.artificial_lag_input.setSingleStep(0.1)
        self.artificial_lag_input.setValue(0.0)
        self.artificial_lag_input.setToolTip("Seconds to freeze stream periodically (simulating latency).")

        self.lag_frames_input = QSpinBox()
        self.lag_frames_input.setRange(1, 10000)
        self.lag_frames_input.setValue(100)
        self.lag_frames_input.setToolTip("Inject artificial lag every N frames.")

        self.file_arrival_delay_input = QDoubleSpinBox()
        self.file_arrival_delay_input.setRange(0.0, 60.0)
        self.file_arrival_delay_input.setSingleStep(0.5)
        self.file_arrival_delay_input.setValue(0.0)
        self.file_arrival_delay_input.setToolTip("Wait N seconds before renaming .h5 data files into existence (simulates NFS lag).")

        self.loop_check = QCheckBox("Loop Infinitely")
        self.reset_check = QCheckBox("Reset Stream on Start")
        
        config_layout.addRow("Redis Host:", self.host_input)
        config_layout.addRow("Redis Port:", self.port_input)
        config_layout.addRow("Stream Name:", self.stream_input)
        config_layout.addRow("Rate (Hz):", self.rate_input)
        config_layout.addRow("Collect Mode:", self.mode_input)
        config_layout.addRow("Artificial Lag (s):", self.artificial_lag_input)
        config_layout.addRow("Lag Interval (frames):", self.lag_frames_input)
        config_layout.addRow("File Arrival Delay (s):", self.file_arrival_delay_input)
        config_layout.addRow("", self.loop_check)
        config_layout.addRow("", self.reset_check)
        
        config_group.setLayout(config_layout)
        main_layout.addWidget(config_group)
        
        # --- File Selection Section ---
        files_group = QGroupBox("Master Files / Directories")
        files_layout = QVBoxLayout()
        
        self.path_list = QListWidget()
        self.path_list.setSelectionMode(QListWidget.ExtendedSelection)
        self.path_list.setAlternatingRowColors(True)
        
        btn_layout = QHBoxLayout()
        self.add_file_btn = QPushButton("Add Files")
        self.add_file_btn.clicked.connect(self.add_files)
        
        self.add_dir_btn = QPushButton("Add Directory")
        self.add_dir_btn.clicked.connect(self.add_directory)
        
        self.remove_btn = QPushButton("Remove Selected")
        self.remove_btn.clicked.connect(self.remove_selected)
        
        self.clear_btn = QPushButton("Clear All")
        self.clear_btn.clicked.connect(self.path_list.clear)
        
        btn_layout.addWidget(self.add_file_btn)
        btn_layout.addWidget(self.add_dir_btn)
        btn_layout.addWidget(self.remove_btn)
        btn_layout.addWidget(self.clear_btn)
        
        files_layout.addWidget(self.path_list)
        files_layout.addLayout(btn_layout)
        files_group.setLayout(files_layout)
        main_layout.addWidget(files_group)
        
        # --- Controls Section ---
        controls_layout = QHBoxLayout()
        
        self.start_btn = QPushButton("Start Streaming")
        self.start_btn.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold; padding: 10px; font-size: 14px;")
        self.start_btn.clicked.connect(self.start_stream)
        
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setStyleSheet("background-color: #f44336; color: white; font-weight: bold; padding: 10px; font-size: 14px;")
        self.stop_btn.clicked.connect(self.stop_stream)
        self.stop_btn.setEnabled(False)
        
        controls_layout.addWidget(self.start_btn)
        controls_layout.addWidget(self.stop_btn)
        main_layout.addLayout(controls_layout)
        
        # --- Log Section ---
        log_group = QGroupBox("Log Output")
        log_layout = QVBoxLayout()
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet("font-family: monospace; background-color: #f0f0f0;")
        log_layout.addWidget(self.log_output)
        log_group.setLayout(log_layout)
        main_layout.addWidget(log_group)

    def add_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select Master Files", "", "HDF5 Files (*.h5);;All Files (*)")
        if files:
            self.path_list.addItems(files)

    def add_directory(self):
        directory = QFileDialog.getExistingDirectory(self, "Select Directory")
        if directory:
            self.path_list.addItem(directory)

    def remove_selected(self):
        for item in self.path_list.selectedItems():
            self.path_list.takeItem(self.path_list.row(item))

    def get_paths(self):
        return [self.path_list.item(i).text() for i in range(self.path_list.count())]

    def start_stream(self):
        paths = self.get_paths()
        if not paths:
            QMessageBox.warning(self, "No Paths", "Please add at least one file or directory.")
            return

        self.log_output.clear()
        self.log_output.append(">>> Starting Mock Streamer...")

        args = [str(STREAMER_SCRIPT)]
        
        # Add flags
        # Use currentData() if available (selected from list), otherwise currentText() (typed manually)
        host_val = self.host_input.currentData()
        if not host_val:
            host_val = self.host_input.currentText()
            
        args.extend(["--host", host_val])
        args.extend(["--port", str(self.port_input.value())])
        args.extend(["--stream", self.stream_input.text()])
        args.extend(["--rate", str(self.rate_input.value())])
        
        if self.mode_input.currentText().strip():
             args.extend(["--mode", self.mode_input.currentText().strip()])
             
        if self.artificial_lag_input.value() > 0:
             args.extend(["--artificial-lag", str(self.artificial_lag_input.value())])
             args.extend(["--lag-frames", str(self.lag_frames_input.value())])
             
        if self.file_arrival_delay_input.value() > 0:
             args.extend(["--file-arrival-delay", str(self.file_arrival_delay_input.value())])
             
        if self.loop_check.isChecked():
            args.append("--loop")
            
        if self.reset_check.isChecked():
            args.append("--reset")
            
        # Always tell the child process to preserve the temp mock directories. 
        # The GUI now manages deletion.
        args.append("--keep-data")
            
        # Add paths
        args.extend(paths)
        
        self.log_output.append(f"Command: python {' '.join(args)}\n")

        self.process = QProcess()
        self.process.setProcessChannelMode(QProcess.MergedChannels)
        self.process.readyReadStandardOutput.connect(self.handle_stdout)
        self.process.finished.connect(self.process_finished)
        
        # Set python executable
        python_exe = sys.executable
        self.process.start(python_exe, args)
        
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        
        # Disable inputs
        self.set_inputs_enabled(False)

    def stop_stream(self):
        if self.process and self.process.state() == QProcess.Running:
            self.log_output.append(">>> Stopping...")
            self.process.terminate()
            if not self.process.waitForFinished(2000):
                self.process.kill()
                
        # Ask user if they want to clear the staged mock streams
        mock_dir = Path("/tmp/mock_streaming")
        if mock_dir.exists() and any(mock_dir.iterdir()):
            reply = QMessageBox.question(self, "Cleanup Mock Data",
                                         "Do you want to delete the temporarily generated mock datasets in /tmp/mock_streaming?",
                                         QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if reply == QMessageBox.Yes:
                self.log_output.append(">>> Emptying /tmp/mock_streaming...")
                import shutil
                shutil.rmtree(mock_dir, ignore_errors=True)
                self.log_output.append(">>> Cleanup finished.")

    def closeEvent(self, event):
        """Ensure the background streamer is killed if the window is closed."""
        self.stop_stream()
        event.accept()

    def handle_stdout(self):
        data = self.process.readAllStandardOutput()
        text = bytes(data).decode("utf8")
        self.log_output.insertPlainText(text)
        self.log_output.ensureCursorVisible()

    def process_finished(self):
        self.log_output.append(">>> Process finished.")
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.set_inputs_enabled(True)
        self.process = None

    def set_inputs_enabled(self, enabled):
        self.host_input.setEnabled(enabled)
        self.port_input.setEnabled(enabled)
        self.stream_input.setEnabled(enabled)
        self.rate_input.setEnabled(enabled)
        self.mode_input.setEnabled(enabled)
        self.loop_check.setEnabled(enabled)
        self.reset_check.setEnabled(enabled)
        self.add_file_btn.setEnabled(enabled)
        self.add_dir_btn.setEnabled(enabled)
        self.remove_btn.setEnabled(enabled)
        self.clear_btn.setEnabled(enabled)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MockStreamerGUI()
    window.show()
    sys.exit(app.exec_())
