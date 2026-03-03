# Create new file: qp2/image_viewer/ui/crystfel_merging_dialog.py

import os
import shlex

from pyqtgraph.Qt import QtWidgets, QtCore, QtGui


class MergingDialog(QtWidgets.QDialog):
    """A non-blocking dialog to LAUNCH and monitor CrystFEL merging processes."""

    run_command_requested = QtCore.pyqtSignal(str, list, str)  # program, args_list, job_name
    view_stream_requested = QtCore.pyqtSignal()
    output_dir_changed = QtCore.pyqtSignal(str)

    def __init__(self, stream_file_path, parent=None):
        super().__init__(parent)
        self.stream_file_path = stream_file_path

        self.setWindowTitle("CrystFEL Merging Tools")
        self.setMinimumSize(800, 650)

        layout = QtWidgets.QVBoxLayout(self)

        # --- Input Stream Group ---
        stream_group = QtWidgets.QGroupBox("Input Stream")
        stream_layout = QtWidgets.QHBoxLayout(stream_group)
        self.stream_file_label = QtWidgets.QLineEdit(
            self.stream_file_path or "Set output directory to generate stream...")
        self.stream_file_label.setReadOnly(True)
        self.btn_view_stream = QtWidgets.QPushButton("View Header")
        self.btn_view_stream.setToolTip("View the first 1000 lines of the stream file")
        stream_layout.addWidget(self.stream_file_label)
        stream_layout.addWidget(self.btn_view_stream)
        layout.addWidget(stream_group)

        # --- Analysis Group ---
        analysis_group = QtWidgets.QGroupBox("Analysis && Visualization")
        analysis_layout = QtWidgets.QVBoxLayout(analysis_group)
        cell_layout = QtWidgets.QHBoxLayout()
        self.btn_cell_explorer = QtWidgets.QPushButton("Run cell_explorer")
        self.cell_explorer_info = QtWidgets.QLabel("Finds likely unit cells from the stream.")
        cell_layout.addWidget(self.btn_cell_explorer)
        cell_layout.addWidget(self.cell_explorer_info, 1)
        analysis_layout.addLayout(cell_layout)
        hklview_layout = QtWidgets.QHBoxLayout()
        self.btn_crystfel = QtWidgets.QPushButton("Run crystfel")
        self.crystfel_info = QtWidgets.QLabel("Launches the CrystFEL GUI to view the stream file.")
        hklview_layout.addWidget(self.btn_crystfel)
        hklview_layout.addWidget(self.crystfel_info, 1)
        analysis_layout.addLayout(hklview_layout)
        layout.addWidget(analysis_group)

        # --- Merging Group ---
        merging_group = QtWidgets.QGroupBox("Merging and Scaling")
        form_layout = QtWidgets.QFormLayout(merging_group)
        self.program_selector = QtWidgets.QComboBox()
        self.program_selector.addItems(["partialator", "process_hkl"])
        self.program_selector.currentIndexChanged.connect(self._on_program_changed)
        form_layout.addRow("Merging Program:", self.program_selector)

        output_dir_layout = QtWidgets.QHBoxLayout()
        default_dir = os.path.dirname(stream_file_path) if stream_file_path else os.path.expanduser("~")
        self.output_dir_input = QtWidgets.QLineEdit(default_dir)
        self.output_dir_input.textChanged.connect(self.output_dir_changed.emit)
        self.browse_out_dir_button = QtWidgets.QPushButton("Browse...")
        self.browse_out_dir_button.clicked.connect(self._browse_for_output_dir)
        output_dir_layout.addWidget(self.output_dir_input)
        output_dir_layout.addWidget(self.browse_out_dir_button)
        form_layout.addRow("Output Directory:", output_dir_layout)

        default_hkl_name = f"{os.path.splitext(os.path.basename(stream_file_path or 'merged'))[0]}.hkl"
        self.output_file_input = QtWidgets.QLineEdit(default_hkl_name)
        self.symmetry_input = QtWidgets.QLineEdit("p1")
        form_layout.addRow("Output Filename (.hkl):", self.output_file_input)
        form_layout.addRow("Symmetry:", self.symmetry_input)

        self.iterations_spinbox = QtWidgets.QSpinBox()
        self.iterations_spinbox.setRange(1, 10)
        self.iterations_spinbox.setValue(3)
        self.model_selector = QtWidgets.QComboBox()
        self.model_selector.addItems(["unity", "xsphere", "scatt"])
        self.iterations_label = QtWidgets.QLabel("Iterations:")
        self.model_label = QtWidgets.QLabel("Model:")
        form_layout.addRow(self.iterations_label, self.iterations_spinbox)
        form_layout.addRow(self.model_label, self.model_selector)

        self.extra_args_input = QtWidgets.QLineEdit()
        self.extra_args_input.setPlaceholderText("--lowres=50 --push-res=1.8 ...")
        form_layout.addRow("Additional Arguments:", self.extra_args_input)

        self.btn_run_merging = QtWidgets.QPushButton("Run Merging")
        btn_layout = QtWidgets.QHBoxLayout()
        btn_layout.addStretch(1)
        btn_layout.addWidget(self.btn_run_merging)
        form_layout.addRow(btn_layout)
        layout.addWidget(merging_group)

        self.output_log = QtWidgets.QTextEdit()
        self.output_log.setReadOnly(True)
        self.output_log.setFont(QtGui.QFont("Monospace", 9))
        layout.addWidget(self.output_log, 1)

        # --- Connections ---
        self.btn_view_stream.clicked.connect(self.view_stream_requested.emit)
        self.btn_cell_explorer.clicked.connect(self._on_run_cell_explorer)
        self.btn_crystfel.clicked.connect(self._on_run_hklviewer)
        self.btn_run_merging.clicked.connect(self._on_run_merging_process)
        self._on_program_changed(0)  # Set initial visibility

    def _on_program_changed(self, index):
        is_partialator = self.program_selector.currentText() == "partialator"
        self.iterations_label.setVisible(is_partialator)
        self.iterations_spinbox.setVisible(is_partialator)
        self.model_label.setVisible(is_partialator)
        self.model_selector.setVisible(is_partialator)

    def _browse_for_output_dir(self):
        dir_path = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Output Directory",
                                                              self.output_dir_input.text())
        if dir_path:
            self.output_dir_input.setText(dir_path)

    def _on_run_cell_explorer(self):
        self.clear_log()
        self.append_log("> Staging cell_explorer job...")
        program = "cell_explorer"
        args = [self.stream_file_path]
        job_name = f"cell_explorer_{os.path.basename(self.stream_file_path or 'stream').split('.')[0]}"
        self.run_command_requested.emit(program, args, job_name)

    def _on_run_hklviewer(self):
        self.clear_log()
        self.append_log("> Staging crystfel GUI job...")
        program = "crystfel"
        args = [self.stream_file_path]
        job_name = f"crystfel_gui_{os.path.basename(self.stream_file_path or 'stream').split('.')[0]}"
        self.run_command_requested.emit(program, args, job_name)

    def _on_run_merging_process(self):
        self.clear_log()
        program = self.program_selector.currentText()
        output_path = os.path.join(self.output_dir_input.text(), self.output_file_input.text())
        job_name = f"{program}_{os.path.basename(output_path).split('.')[0]}"
        self.append_log(f"> Staging {program} job...")
        args = ["-i", self.stream_file_path, "-o", output_path, "-y", self.symmetry_input.text()]
        if program == "partialator":
            args.extend(
                ["--iterations", str(self.iterations_spinbox.value()), "--model", self.model_selector.currentText()])
        extra_args_str = self.extra_args_input.text()
        if extra_args_str:
            try:
                args.extend(shlex.split(extra_args_str))
            except Exception as e:
                self.append_log(f"ERROR: Could not parse Additional Arguments.\n{e}")
                return
        self.run_command_requested.emit(program, args, job_name)

    def set_process_running(self, running: bool):
        self.btn_run_merging.setEnabled(not running)
        self.btn_cell_explorer.setEnabled(not running)
        self.btn_crystfel.setEnabled(not running)

    def append_log(self, text: str):
        self.output_log.append(text)
        self.output_log.verticalScrollBar().setValue(self.output_log.verticalScrollBar().maximum())

    def clear_log(self):
        self.output_log.clear()

    def update_stream_file_display(self, new_path: str):
        self.stream_file_path = new_path
        self.stream_file_label.setText(new_path)
