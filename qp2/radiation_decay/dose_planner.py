import math
import sys
import os
import traceback

import numpy as np
import requests

from qp2.data_viewer.utils import get_rpc_url
from qp2.log.logging_config import get_logger, setup_logging
from qp2.utils.icon import generate_icon_with_text

logger = get_logger(__name__)

from PyQt5.QtWidgets import (
    QApplication,
    QWidget,
    QGridLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QComboBox,
    QCheckBox,
    QVBoxLayout,
    QGroupBox,
    QHBoxLayout,
    QMessageBox,
    QFileDialog,
    QTableWidget,
    QTableWidgetItem,
    QAbstractItemView,
    QHeaderView,
)
from PyQt5.QtGui import QDoubleValidator, QIntValidator
from PyQt5.QtCore import Qt, QObject, pyqtSignal, QRunnable, QThreadPool, QTimer

try:
    from qp2.radiation_decay.raddose3d import Sample, Beam, Wedge, run_raddose3d
    from qp2.radiation_decay.data_source import ExternalDataSource
except ImportError:
    from qp2.radiation_decay.mock import Sample, Beam, Wedge, run_raddose3d

from qp2.radiation_decay.calculations import (
    find_experimental_recommendations,
    _prune_recommendations_for_raddose3d,
    _setup_raddose3d_input,
    _calculate_rotisserie_factor,
    calculate_interactive_dose_rate,
)


class WorkerSignals(QObject):
    result = pyqtSignal(dict)
    finished = pyqtSignal()
    error = pyqtSignal(tuple)


class InteractiveWorkerSignals(QObject):
    result = pyqtSignal(tuple)
    error = pyqtSignal(tuple)


class R3DWorker(QRunnable):
    def __init__(self, r3d_params, recommendation, worker_signals):
        super(R3DWorker, self).__init__()
        self.signals, self.r3d_params, self.recommendation, self.is_cancelled = (
            worker_signals,
            r3d_params,
            recommendation,
            False,
        )

    def run(self):
        if self.is_cancelled:
            return
        try:
            sample, beam, wedges = _setup_raddose3d_input(
                self.r3d_params, self.recommendation
            )
            data, summary = run_raddose3d(
                sample, beam, wedges, swap_xy=False, debug=False
            )
            self.recommendation["avg_dwd_mgy"] = summary.get("Avg DWD", 0.0)
            self.recommendation["max_dose_mgy"] = summary.get("Max Dose", 0.0)
            self.recommendation["last_dwd_mgy"] = summary.get("Last DWD", 0.0)
            logger.debug(f"R3D parameters: {self.r3d_params}")
            logger.debug(f"R3D summary: {summary}")
            logger.debug(f"Recommendation: {self.recommendation}")
            if not self.is_cancelled:
                self.signals.result.emit(self.recommendation)
        except Exception as e:
            if not self.is_cancelled:
                self.signals.error.emit((e, sys.exc_info()))
        finally:
            self.signals.finished.emit()


class InteractiveR3DWorker(QRunnable):
    def __init__(self, r3d_params, dynamic_params):
        super().__init__()
        self.signals, self.r3d_params, self.dynamic_params = (
            InteractiveWorkerSignals(),
            r3d_params,
            dynamic_params,
        )

    def run(self):
        try:
            sample, beam, wedges = _setup_raddose3d_input(
                self.r3d_params, self.dynamic_params
            )
            _, summary = run_raddose3d(sample, beam, wedges, swap_xy=False, debug=False)
            dwd_value, max_dose_value, last_dwd_value = (
                summary.get("Avg DWD", 0.0),
                summary.get("Max Dose", 0.0),
                summary.get("Last DWD", 0.0),
            )
            self.signals.result.emit((dwd_value, max_dose_value, last_dwd_value))
        except Exception as e:
            self.signals.error.emit((e, sys.exc_info()))


class NumericTableWidgetItem(QTableWidgetItem):
    def __lt__(self, other):
        try:
            return float(self.text()) < float(other.text())
        except (ValueError, TypeError):
            return super().__lt__(other)


class CrystalLifetimeGUI(QWidget):
    def __init__(self):
        super().__init__()
        (
            self.BEAM_SIZES,
            self.ATTENUATIONS,
            self.ENERGIES_KEV,
            self.EXPOSURE_TIMES,
        ) = (
            [(5, 5), (10, 10), (20, 20), (50, 50), (100, 100), (200, 200)],
            [100, 50, 1, 2, 3, 5, 10, 500],
            [7.0, 12.0, 18.0, 24.0, 30.0, 35.0],
            [0.1, 0.2, 0.05, 0.5, 1.0],
        )
        self.normal_style, self.red_style, self.error_style = (
            "background-color: #f0f0f0;",
            "background-color: #f0f0f0; color: red; font-weight: bold;",
            "background-color: #f8d7da; color: #721c24;",
        )
        (
            self.threadpool,
            self.active_workers,
            self.calculation_running,
            self.final_results,
            self.sorted_results,
            self.tasks_to_run,
            self.tasks_finished,
        ) = (QThreadPool(), [], False, [], [], 0, 0)

        self.all_unique_recommendations = []
        self.current_batch_index = 0
        self.BATCH_SIZE = 16

        self.threadpool.setMaxThreadCount(16)
        self.initial_compact_width = 360

        self.r3d_debounce_timer = QTimer(self)
        self.r3d_debounce_timer.setSingleShot(True)
        self.r3d_debounce_timer.timeout.connect(self._run_interactive_r3d_debounced)
        self.r3d_debounce_timer.setInterval(1000) # ms

        self.data_source = ExternalDataSource()
        self.external_data_cache = {}
        self.imported_strategy_params = {}

        self.update_check_timer = QTimer(self)
        self.update_check_timer.setInterval(86400000)
        self.update_check_timer.timeout.connect(self._check_for_external_updates)

        self.initUI()

        self._load_initial_parameters()
        self.update_check_timer.start()

    def _browse_for_pdb(self):
        """Opens a file dialog to select a PDB file."""
        fileName, _ = QFileDialog.getOpenFileName(
            self, "Open PDB File", "", "PDB Files (*.pdb *.ent);;All Files (*)"
        )
        if fileName:
            self.pdb_edit.setText(fileName)

    def _e2w(self, kev: float) -> float:
        """Convert energy in KeV to wavelength in Angstrom."""
        if kev <= 0:
            return float("inf")
        return 12.3984 / kev

    def _w2e(self, ang: float) -> float:
        """Convert wavelength in Angstrom to energy in KeV."""
        if ang <= 0:
            return float("inf")
        return 12.3984 / ang

    def initUI(self):
        logger.debug("Initializing Dose Planner UI")
        self.setWindowTitle("Dose Planner")
        app_icon = generate_icon_with_text(text="D", bg_color="#e74c3c", size=128)
        self.setWindowIcon(app_icon)

        self.setGeometry(100, 100, self.initial_compact_width, 900)
        self.setMaximumWidth(self.initial_compact_width * 2 + 100)

        main_layout = QVBoxLayout()
        self.setLayout(main_layout)

        top_layout = QHBoxLayout()
        main_layout.addLayout(top_layout)

        left_layout = QVBoxLayout()
        top_layout.addLayout(left_layout, 1)

        params_group = QGroupBox("Experiment Parameters")
        left_layout.addWidget(params_group)
        grid = QGridLayout()
        params_group.setLayout(grid)
        grid.addWidget(QLabel("Full Flux (photons/s):"), 0, 0)
        self.flux_edit = QLineEdit("5e12")
        self.flux_edit.setToolTip("Total incident flux in photons per second")
        self.flux_edit.setReadOnly(False)
        self.flux_edit.textChanged.connect(self.on_parameter_change)
        grid.addWidget(self.flux_edit, 0, 1, 1, 3)
        dose_res_hbox = QHBoxLayout()
        dose_res_hbox.addWidget(QLabel("Target Res (Å):"))

        self.resolution_edit = QLineEdit("3.0")
        self.resolution_edit.setToolTip(
            "Target resolution in Angstroms, help to set dose limit"
        )
        self.resolution_edit.textChanged.connect(self._update_dose_from_resolution)
        dose_res_hbox.addWidget(self.resolution_edit)
        dose_res_hbox.addStretch()
        self.dose_limit_check = QCheckBox("Set Dose Limit (MGy):")
        self.dose_limit_check.stateChanged.connect(self._toggle_dose_input)
        self.dose_limit_edit = QLineEdit()
        self.dose_limit_edit.setToolTip("Maximum dose limit in MGy")
        self.dose_limit_edit.setEnabled(False)
        self.dose_limit_edit.textChanged.connect(self.on_parameter_change)
        dose_res_hbox.addWidget(self.dose_limit_check)
        dose_res_hbox.addWidget(self.dose_limit_edit)
        grid.addLayout(dose_res_hbox, 1, 0, 1, 4)

        grid.addWidget(QLabel("Crystal Size (μm):"), 2, 0)
        self.lx_edit = QLineEdit("50")
        self.lx_edit.setToolTip("Width, along gonio axis")
        self.lx_edit.textChanged.connect(self._sync_crystal_dims)
        self.lx_edit.textChanged.connect(self.on_parameter_change)

        self.ly_edit = QLineEdit("50")
        self.ly_edit.setToolTip("Height, along gravity")
        self.ly_edit.textChanged.connect(self._sync_crystal_dims)
        self.ly_edit.textChanged.connect(self.on_parameter_change)
        self.lz_edit = QLineEdit("50")
        self.lz_edit.setToolTip("Thickness, along x-ray path")
        self.lz_edit.textChanged.connect(self._sync_crystal_dims)
        self.lz_edit.textChanged.connect(self.on_parameter_change)

        crystal_hbox = QHBoxLayout()
        crystal_hbox.addWidget(QLabel("Hori"))
        crystal_hbox.addWidget(self.lx_edit)

        crystal_hbox.addWidget(QLabel("Vert"))
        crystal_hbox.addWidget(self.ly_edit)
        crystal_hbox.addWidget(QLabel("Thick"))
        crystal_hbox.addWidget(self.lz_edit)

        grid.addLayout(crystal_hbox, 2, 1, 1, 3)

        tunable_group = QGroupBox("Optimizable Collection Parameters")
        left_layout.addWidget(tunable_group)
        tunable_grid = QGridLayout()
        tunable_group.setLayout(tunable_grid)

        def add_optim_row(grid_layout, label, items, row_idx):
            combo = QComboBox()
            combo.addItems([str(i) for i in items])
            combo.currentIndexChanged.connect(self.on_parameter_change)
            fix_check = QCheckBox("Fix")
            grid_layout.addWidget(QLabel(label), row_idx, 0)
            grid_layout.addWidget(combo, row_idx, 1)
            grid_layout.addWidget(fix_check, row_idx, 2)
            return combo, fix_check

        self.beam_combo, self.beam_fix_check = add_optim_row(
            tunable_grid, "Beam (XxY, μm):", [f"{x}x{y}" for x, y in self.BEAM_SIZES], 0
        )
        self.beam_fix_check.setChecked(True)
        tunable_grid.addWidget(QLabel("Translation X (μm):"), 1, 0)
        self.translation_edit = QLineEdit("0.0")
        self.translation_edit.setToolTip(
            "Translation of crystal along gonio axis direction during helical collection"
        )
        self.translation_edit.setValidator(QDoubleValidator())
        self.translation_edit.editingFinished.connect(self.on_parameter_change)
        tunable_grid.addWidget(self.translation_edit, 1, 1)
        self.translation_fix_check = QCheckBox("Fix")
        self.translation_fix_check.setChecked(True)
        tunable_grid.addWidget(self.translation_fix_check, 1, 2)

        self.attenuation_combo, self.attenuation_fix_check = add_optim_row(
            tunable_grid, "Attenuation:", self.ATTENUATIONS, 2
        )

        tunable_grid.addWidget(QLabel("Energy (KeV):"), 3, 0)
        self.energy_edit = QLineEdit("12.000")
        self.energy_edit.setToolTip("X-ray energy in KeV")
        self.energy_edit.setValidator(QDoubleValidator(7.0, 35.0, 3))
        self.energy_edit.textChanged.connect(self._update_flux_from_energy)
        self.energy_edit.textChanged.connect(self.on_parameter_change)
        tunable_grid.addWidget(self.energy_edit, 3, 1)
        self.energy_fix_check = QCheckBox("Fix")
        self.energy_fix_check.setChecked(True)
        tunable_grid.addWidget(self.energy_fix_check, 3, 2)

        self.exposure_combo, self.exposure_fix_check = add_optim_row(
            tunable_grid, "Exposure (s):", self.EXPOSURE_TIMES, 4
        )

        tunable_grid.addWidget(QLabel("Osc/Image (°):"), 5, 0)
        self.osc_edit = QLineEdit("0.1")
        self.osc_edit.setToolTip("Oscillation range per image in degrees")
        self.osc_edit.textChanged.connect(self._update_nimages_options)
        self.osc_edit.textChanged.connect(self.on_parameter_change)
        tunable_grid.addWidget(self.osc_edit, 5, 1)

        tunable_grid.addWidget(QLabel("No. Images to Collect:"), 6, 0)
        self.nimages_combo = QComboBox()
        self.nimages_combo.setToolTip("Total number of images to collect")
        self.nimages_combo.setEditable(True)
        self.nimages_combo.lineEdit().setValidator(QIntValidator(1, 99999))
        self.nimages_combo.currentIndexChanged.connect(self.on_parameter_change)
        self.nimages_combo.lineEdit().editingFinished.connect(self.on_parameter_change)
        tunable_grid.addWidget(self.nimages_combo, 6, 1)
        self.nimages_fix_check = QCheckBox("Fix")
        tunable_grid.addWidget(self.nimages_fix_check, 6, 2)

        r3d_update_hbox = QHBoxLayout()
        self.use_r3d_check = QCheckBox("Enable RADDOSE-3D")
        self.use_r3d_check.toggled.connect(self.toggle_r3d_visibility)
        r3d_update_hbox.addWidget(self.use_r3d_check)

        self.update_button = QPushButton("Pull Strategy Results")
        self.update_button.setToolTip("Get latest strategy from external source")
        self.update_button.clicked.connect(self._force_strategy_update)
        r3d_update_hbox.addWidget(self.update_button)
        left_layout.addLayout(r3d_update_hbox)

        left_layout.addStretch(1)

        self.right_column_widget = QWidget()
        right_layout = QVBoxLayout()
        right_layout.setContentsMargins(0, 0, 0, 0)
        self.right_column_widget.setLayout(right_layout)
        top_layout.addWidget(self.right_column_widget, 1)
        self.r3d_group = QGroupBox("RADDOSE-3D Parameters")
        right_layout.addWidget(self.r3d_group)
        r3d_main_layout = QVBoxLayout()
        self.r3d_group.setLayout(r3d_main_layout)
        r3d_grid = QGridLayout()
        r3d_main_layout.addLayout(r3d_grid)
        self.shape_combo = QComboBox()
        self.shape_combo.addItems(["Cuboid", "Spherical", "Cylinder"])
        self.shape_combo.currentIndexChanged.connect(self._on_shape_change)
        r3d_grid.addWidget(QLabel("Crystal Shape:"), 0, 0)
        r3d_grid.addWidget(self.shape_combo, 0, 1)

        r3d_grid.addWidget(QLabel("ABS Coef Calc:"), 1, 0)

        self.coef_calc_combo = QComboBox()
        self.coef_calc_combo.addItems(["AVERAGE", "RD3D", "EXP"])
        self.coef_calc_combo.currentIndexChanged.connect(self._toggle_r3d_coef_inputs)

        r3d_grid.addWidget(self.coef_calc_combo, 1, 1)

        pdb_hbox = QHBoxLayout()
        self.pdb_edit = QLineEdit()
        self.pdb_edit.setPlaceholderText("PDB Code or File Path")
        self.pdb_edit.editingFinished.connect(self.on_parameter_change)
        pdb_hbox.addWidget(self.pdb_edit)
        self.pdb_browse_button = QPushButton("Browse...")
        self.pdb_browse_button.clicked.connect(self._browse_for_pdb)
        pdb_hbox.addWidget(self.pdb_browse_button)
        r3d_grid.addWidget(QLabel("PDB Code/File:"), 2, 0)
        r3d_grid.addLayout(pdb_hbox, 2, 1)

        self.cell_label = QLabel("Unit Cell:")
        self.cell_edit = QLineEdit("78 78 39 90 90 90")
        self.cell_edit.setToolTip("Unit cell parameters: a b c alpha beta gamma")
        self.cell_edit.editingFinished.connect(self.on_parameter_change)
        r3d_grid.addWidget(self.cell_label, 3, 0)
        r3d_grid.addWidget(self.cell_edit, 3, 1)
        self.nres_label = QLabel("NRes/Monomer:")
        self.nres_edit = QLineEdit("129")
        self.nres_edit.setToolTip("Number of residues per monomer")
        self.nres_edit.editingFinished.connect(self.on_parameter_change)
        r3d_grid.addWidget(self.nres_label, 4, 0)
        r3d_grid.addWidget(self.nres_edit, 4, 1)
        self.nmon_label = QLabel("NMon/Cell:")
        self.nmon_edit = QLineEdit("8")
        self.nres_edit.setToolTip("Number of monomers per unit cell")
        self.nmon_edit.editingFinished.connect(self.on_parameter_change)
        r3d_grid.addWidget(self.nmon_label, 5, 0)
        r3d_grid.addWidget(self.nmon_edit, 5, 1)
        self.advanced_r3d_group = QGroupBox("Advanced Settings")
        self.advanced_r3d_group.setCheckable(True)
        self.advanced_r3d_group.setChecked(False)
        r3d_main_layout.addWidget(self.advanced_r3d_group)
        self.advanced_r3d_group.toggled.connect(self._toggle_advanced_widgets)
        advanced_layout = QVBoxLayout()
        self.advanced_r3d_group.setLayout(advanced_layout)
        advanced_grid = QGridLayout()
        advanced_layout.addLayout(advanced_grid)
        self.angle_container = QWidget()
        angle_layout = QHBoxLayout()
        angle_layout.setContentsMargins(0, 0, 0, 0)
        self.angle_l_label = QLabel("AngleL (°):")
        self.angle_l_edit = QLineEdit("0.0")
        self.angle_l_edit.setToolTip(
            "Loop angle--angle between loop plane and gonio axis"
        )
        self.angle_l_edit.setValidator(QDoubleValidator())
        self.angle_l_edit.textChanged.connect(self.on_parameter_change)
        angle_layout.addWidget(self.angle_l_label)
        angle_layout.addWidget(self.angle_l_edit)
        angle_layout.addSpacing(10)
        self.angle_p_label = QLabel("AngleP (°):")
        self.angle_p_edit = QLineEdit("0.0")
        self.angle_p_edit.setValidator(QDoubleValidator())
        self.angle_p_edit.setToolTip(
            "Plane angle, angle between crystal y axis (vertical) and gonio axis"
        )
        self.angle_p_edit.textChanged.connect(self.on_parameter_change)
        angle_layout.addWidget(self.angle_p_label)
        angle_layout.addWidget(self.angle_p_edit)
        self.angle_container.setLayout(angle_layout)

        advanced_layout.addWidget(self.angle_container)
        self.heavy_atoms_group = QGroupBox("Heavy Atoms per Monomer")
        self.heavy_atoms_layout = QVBoxLayout()
        self.heavy_atoms_group.setLayout(self.heavy_atoms_layout)
        advanced_layout.addWidget(self.heavy_atoms_group)
        add_heavy_atom_btn = QPushButton("Add Heavy Atom")
        add_heavy_atom_btn.clicked.connect(self._add_heavy_atom_row)
        self.heavy_atoms_layout.addWidget(add_heavy_atom_btn)
        self.solvent_atoms_group = QGroupBox("Solvent Atom Concentration")
        self.solvent_atoms_layout = QVBoxLayout()
        self.solvent_atoms_group.setLayout(self.solvent_atoms_layout)
        advanced_layout.addWidget(self.solvent_atoms_group)
        add_solvent_atom_btn = QPushButton("Add Solvent Atom")
        add_solvent_atom_btn.clicked.connect(self._add_solvent_atom_row)
        self.solvent_atoms_layout.addWidget(add_solvent_atom_btn)
        self.other_molecules_group = QGroupBox("Other Molecules per Monomer")
        self.other_molecules_hbox = QHBoxLayout()
        self.other_molecules_group.setLayout(self.other_molecules_hbox)
        advanced_layout.addWidget(self.other_molecules_group)
        self.dna_label = QLabel("DNA:")
        self.dna_edit = QLineEdit("0")
        self.dna_edit.textChanged.connect(self.on_parameter_change)
        self.other_molecules_hbox.addWidget(self.dna_label)
        self.other_molecules_hbox.addWidget(self.dna_edit)
        self.rna_label = QLabel("RNA:")
        self.rna_edit = QLineEdit("0")
        self.rna_edit.textChanged.connect(self.on_parameter_change)
        self.other_molecules_hbox.addWidget(self.rna_label)
        self.other_molecules_hbox.addWidget(self.rna_edit)
        self.carb_label = QLabel("Carb:")
        self.carb_edit = QLineEdit("0")
        self.carb_edit.textChanged.connect(self.on_parameter_change)
        self.other_molecules_hbox.addWidget(self.carb_label)
        self.other_molecules_hbox.addWidget(self.carb_edit)
        right_layout.addStretch(1)
        interactive_summary_container = QGroupBox("Results Summary")
        main_layout.addWidget(interactive_summary_container)
        summary_layout = QHBoxLayout()
        interactive_summary_container.setLayout(summary_layout)
        lifetime_group = QGroupBox("Crystal Lifetime")
        summary_layout.addWidget(lifetime_group)
        lifetime_grid = QGridLayout()
        lifetime_group.setLayout(lifetime_grid)
        lifetime_grid.addWidget(QLabel("Dose Rate (MGy/s):"), 0, 0)
        self.dose_rate_output = QLineEdit()
        self.dose_rate_output.setReadOnly(True)
        self.dose_rate_output.setStyleSheet(self.normal_style)
        lifetime_grid.addWidget(self.dose_rate_output, 0, 1)
        lifetime_grid.addWidget(QLabel("Rotisserie Factor:"), 1, 0)
        self.rotisserie_output = QLineEdit()
        self.rotisserie_output.setReadOnly(True)
        self.rotisserie_output.setStyleSheet(self.normal_style)
        lifetime_grid.addWidget(self.rotisserie_output, 1, 1)
        lifetime_grid.addWidget(QLabel("Est. Total Dose (MGy):"), 2, 0)
        self.total_dose_output = QLineEdit()
        self.total_dose_output.setReadOnly(True)
        self.total_dose_output.setStyleSheet(self.normal_style)
        lifetime_grid.addWidget(self.total_dose_output, 2, 1)
        lifetime_grid.addWidget(QLabel("Est. Avg Dose (MGy):"), 3, 0)
        self.est_avg_dose_output = QLineEdit()
        self.est_avg_dose_output.setReadOnly(True)
        self.est_avg_dose_output.setStyleSheet(self.normal_style)
        lifetime_grid.addWidget(self.est_avg_dose_output, 3, 1)
        self.r3d_summary_group = QGroupBox("RADDOSE-3D")
        summary_layout.addWidget(self.r3d_summary_group)
        r3d_summary_grid = QGridLayout()
        self.r3d_summary_group.setLayout(r3d_summary_grid)
        r3d_summary_grid.addWidget(QLabel("Total Exposure (s):"), 0, 0)
        self.total_exposure_time_output = QLineEdit()
        self.total_exposure_time_output.setReadOnly(True)
        self.total_exposure_time_output.setStyleSheet(self.normal_style)
        r3d_summary_grid.addWidget(self.total_exposure_time_output, 0, 1)
        r3d_summary_grid.addWidget(QLabel("Max Dose (MGy):"), 1, 0)
        self.interactive_max_dose_output = QLineEdit()
        self.interactive_max_dose_output.setReadOnly(True)
        self.interactive_max_dose_output.setStyleSheet(self.normal_style)
        r3d_summary_grid.addWidget(self.interactive_max_dose_output, 1, 1)
        r3d_summary_grid.addWidget(QLabel("Avg DWD (MGy):"), 2, 0)
        self.interactive_dwd_output = QLineEdit()
        self.interactive_dwd_output.setReadOnly(True)
        self.interactive_dwd_output.setStyleSheet(self.normal_style)
        r3d_summary_grid.addWidget(self.interactive_dwd_output, 2, 1)
        r3d_summary_grid.addWidget(QLabel("Last DWD (MGy):"), 3, 0)
        self.last_dwd_output = QLineEdit()
        self.last_dwd_output.setReadOnly(True)
        self.last_dwd_output.setStyleSheet(self.normal_style)
        r3d_summary_grid.addWidget(self.last_dwd_output, 3, 1)
        self.status_label = QLabel("Ready.")
        self.status_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(self.status_label)
        action_hbox = QHBoxLayout()
        main_layout.addLayout(action_hbox)
        self.calc_button = QPushButton("Find Best Parameters")
        self.calc_button.clicked.connect(self.run_calculation)
        action_hbox.addWidget(self.calc_button)
        self.create_run_button = QPushButton("Export Current Strategy")
        self.create_run_button.clicked.connect(self.create_run_from_gui)
        action_hbox.addWidget(self.create_run_button)
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.cancel_calculation)
        self.cancel_button.setVisible(False)
        action_hbox.addWidget(self.cancel_button)
        self.results_group = QGroupBox("Results")
        self.results_table = QTableWidget()
        self.results_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.results_table.itemSelectionChanged.connect(self.on_result_selection_change)
        self.results_table.verticalHeader().setVisible(False)
        self.results_table.setSortingEnabled(True)
        results_layout = QVBoxLayout()
        results_layout.addWidget(self.results_table)
        self.export_button = QPushButton("Export Selected Strategy")
        self.export_button.clicked.connect(self.export_strategy)
        self.export_button.setEnabled(False)
        results_layout.addWidget(self.export_button)
        self.results_group.setLayout(results_layout)
        main_layout.addWidget(self.results_group)

        self._update_dose_from_resolution()
        self._toggle_advanced_widgets(False)
        self.use_r3d_check.setChecked(False)
        self.right_column_widget.setVisible(False)
        self.r3d_summary_group.setVisible(False)
        self._validate_translation_x()
        self._update_nimages_options()
        self._toggle_r3d_coef_inputs()

    def _update_flux_from_energy(self):
        """Updates the read-only flux field based on the current energy value."""
        try:
            energy_kev = float(self.energy_edit.text())
            flux = self.data_source.flux_manager.get_flux(energy_kev)

            # Block signals to prevent on_parameter_change from firing for this change
            self.flux_edit.blockSignals(True)
            self.flux_edit.setText(f"{flux:.2e}")
            self.flux_edit.blockSignals(False)
        except ValueError:
            # Handle cases where the energy text is not a valid float (e.g., empty)
            self.flux_edit.blockSignals(True)
            self.flux_edit.setText("Invalid Energy")
            self.flux_edit.blockSignals(False)

    def _update_nimages_options(self):
        """Recalculates and updates the nimages combobox based on the osc value, preserving user input."""
        try:
            osc = float(self.osc_edit.text())
            if osc <= 0:
                self.nimages_combo.clear()
                return
        except ValueError:
            self.nimages_combo.clear()
            return

        base_rotations = [90, 180, 270, 360]
        image_options = [int(round(rot / osc)) for rot in base_rotations]

        # MODIFICATION: Preserve the current text before clearing
        current_text = self.nimages_combo.currentText()

        self.nimages_combo.blockSignals(True)
        self.nimages_combo.clear()
        self.nimages_combo.addItems([str(n) for n in image_options])

        # MODIFICATION: Restore the previous text. This will select an item if it matches,
        # or just set the editable text if it's a custom value.
        self.nimages_combo.setCurrentText(current_text)

        # If the box is empty after trying to restore (e.g., on first launch), set a default
        if not self.nimages_combo.currentText():
            default_images = int(round(180 / osc))
            self.nimages_combo.setCurrentText(str(default_images))

        self.nimages_combo.blockSignals(False)

    def _load_initial_parameters(self):
        logger.info("Loading initial beamline default parameters")
        self.status_label.setText("Loading initial beamline defaults...")
        try:
            initial_data = self.data_source.get_beamline_defaults()
            self._apply_beamline_defaults(initial_data)
            self.status_label.setText("Ready. Loaded beamline defaults.")
            logger.info("Successfully loaded beamline defaults.")
        except Exception as e:
            logger.error(f"Failed to load initial data: {e}")
            self.status_label.setText("Error loading initial data.")
            QMessageBox.critical(
                self, "Data Source Error", f"Could not load initial data: {e}"
            )

    def _apply_beamline_defaults(self, data_dict):
        """Applies a dictionary of beamline parameters to the GUI widgets."""
        energy_kev = data_dict.get("energy_keV")
        if energy_kev:
            self.energy_edit.setText(f"{energy_kev:.3f}")

        flux_val = data_dict.get("flux")
        if flux_val is not None:
            self.flux_edit.setText(f"{flux_val:.2e}")

        self.attenuation_combo.setCurrentText(
            str(data_dict.get("attenuation_factor", ""))
        )
        self.nres_edit.setText(str(data_dict.get("nres", "")))
        self.nmon_edit.setText(str(data_dict.get("nmon", "")))

        beam_x, beam_y = data_dict.get("beam_size_um", (None, None))
        if beam_y is not None and beam_x is not None:
            self.beam_combo.setCurrentText(f"{beam_x}x{beam_y}")

        # This cache is now only for beamline defaults, not strategy
        self.external_data_cache = data_dict
        self.on_parameter_change()

    def _check_for_external_updates(self):
        """This function is now deprecated as updates are manual."""
        pass

    def _force_strategy_update(self):
        """Handles the 'Update Exp Params' button click."""
        self.status_label.setText("Querying for latest strategy...")
        QApplication.processEvents()
        try:
            strategy_data = self.data_source.get_latest_strategy()

            if not strategy_data:
                QMessageBox.information(
                    self, "No Update", "No new strategy results found."
                )
                self.status_label.setText("Ready.")
                return

            # Build a list of changes to show the user
            changes = []
            if self.cell_edit.text() != strategy_data.get("cell"):
                changes.append(
                    f"- Cell: {self.cell_edit.text()} → {strategy_data.get('cell')}"
                )
            if self.nres_edit.text() != str(strategy_data.get("nres")):
                changes.append(
                    f"- NRes/Monomer: {self.nres_edit.text()} → {strategy_data.get('nres')}"
                )
            if self.nmon_edit.text() != str(strategy_data.get("nmon")):
                changes.append(
                    f"- NMon/Cell: {self.nmon_edit.text()} → {strategy_data.get('nmon')}"
                )

            if self.osc_edit.text() != str(strategy_data.get("osc_delta")):
                changes.append(
                    f"- osc: {self.osc_edit.text()} → {strategy_data.get('osc_delta')}"
                )

            if not changes:
                QMessageBox.information(
                    self, "No Update", "Latest strategy already loaded."
                )
                self.status_label.setText("Ready.")
                return

            # If there are changes, show the confirmation dialog
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Question)
            msg_box.setWindowTitle("New Strategy Found")
            msg_box.setText(
                f"A new strategy was found in Redis for sample {strategy_data['sample']}.\nDo you want to apply these parameters?"
            )
            msg_box.setInformativeText("\n".join(changes))
            msg_box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            msg_box.setDefaultButton(QMessageBox.Yes)

            user_response = msg_box.exec_()
            if user_response == QMessageBox.Yes:
                self._apply_strategy_parameters(strategy_data)
                self.status_label.setText("Successfully applied new strategy.")
            else:
                self.status_label.setText("Ready. Ignored new strategy.")

        except Exception as e:
            self.status_label.setText("Error fetching strategy.")
            QMessageBox.critical(
                self, "Data Source Error", f"Could not fetch strategy: {e}"
            )

    def _apply_strategy_parameters(self, strategy_dict):
        """Applies strategy-specific parameters to the UI and internal state."""
        self.cell_edit.setText(strategy_dict.get("cell", self.cell_edit.text()))
        self.nres_edit.setText(str(strategy_dict.get("nres", self.nres_edit.text())))
        self.nmon_edit.setText(str(strategy_dict.get("nmon", self.nmon_edit.text())))
        self.osc_edit.setText(str(strategy_dict.get("osc_delta", self.osc_edit.text())))

        # Automatically switch to RD3D mode
        self.coef_calc_combo.setCurrentText("RD3D")

        # Store other parameters for export
        self.imported_strategy_params = {
            "osc_start": strategy_dict.get("osc_start"),
            "osc_end": strategy_dict.get("osc_end"),
            "osc_delta": strategy_dict.get("osc_delta"),
            "distance": strategy_dict.get("distance"),
        }
        logger.info(
            f"Stored strategy parameters for export: {self.imported_strategy_params}"
        )
        self.on_parameter_change()

    def _toggle_advanced_widgets(self, checked):
        for i in range(self.advanced_r3d_group.layout().count()):
            item = self.advanced_r3d_group.layout().itemAt(i)
            if item and item.widget():
                item.widget().setVisible(checked)
            elif item and item.layout():
                for j in range(item.layout().count()):
                    if item.layout().itemAt(j).widget():
                        item.layout().itemAt(j).widget().setVisible(checked)

    def _toggle_r3d_coef_inputs(self):
        """
        Central function to manage the UI state of PDB, cell, nres, and nmon widgets
        based on the selected ABS Coef Calc method.
        """
        current_mode = self.coef_calc_combo.currentText()

        is_exp_mode = current_mode == "EXP"
        is_rd3d_mode = current_mode == "RD3D"

        # --- Manage PDB field state ---
        self.pdb_edit.setEnabled(is_exp_mode)
        self.pdb_edit.setToolTip("PDB Code or File Path")
        self.pdb_browse_button.setEnabled(is_exp_mode)
        if is_exp_mode and not self.pdb_edit.text():
            self.pdb_edit.setText("9RVI")  # Set default PDB code
        elif not is_exp_mode:
            self.pdb_edit.clear()  # Clear PDB field when not in EXP mode

        # --- Manage cell, nres, nmon field state ---
        self.cell_edit.setEnabled(is_rd3d_mode)
        self.cell_label.setEnabled(is_rd3d_mode)
        self.nres_edit.setEnabled(is_rd3d_mode)
        self.nres_label.setEnabled(is_rd3d_mode)
        self.nmon_edit.setEnabled(is_rd3d_mode)
        self.nmon_label.setEnabled(is_rd3d_mode)

        self.advanced_r3d_group.setEnabled(is_rd3d_mode)
        # If the group is disabled, also make sure it's unchecked and its contents are hidden
        if not is_rd3d_mode:
            self.advanced_r3d_group.setChecked(False)

        self.on_parameter_change()

    def toggle_r3d_visibility(self, checked):
        self.right_column_widget.setVisible(checked)
        self.r3d_summary_group.setVisible(checked)
        self.calc_button.setText(
            "Find Recommendations && Run RADDOSE-3D"
            if checked
            else "Find Best Parameters"
        )
        self.window().adjustSize()
        self.on_parameter_change()

    def on_parameter_change(self):
        base_params = self._get_base_r3d_params()
        dynamic_params = self._get_dynamic_r3d_params()
        if base_params and dynamic_params:
            all_params = {**base_params, **dynamic_params}
            keys_to_update = self.external_data_cache.keys()
            updated_cache_values = {
                k: v for k, v in all_params.items() if k in keys_to_update
            }
            self.external_data_cache.update(updated_cache_values)
        self._validate_translation_x()
        self._update_fast_summary()
        if self.use_r3d_check.isChecked():
            self.r3d_debounce_timer.start()

    def _update_fast_summary(self):
        try:
            flux = float(self.flux_edit.text())
            energy_kev = float(self.energy_edit.text())
            wavelength = self._e2w(energy_kev)
            x_str, y_str = self.beam_combo.currentText().split("x")
            beam_x, beam_y = float(x_str), float(y_str)
            attenuation = float(self.attenuation_combo.currentText())
            lx = float(self.lx_edit.text())
            ly = float(self.ly_edit.text())
            lz = float(self.lz_edit.text())
            n_images_text = self.nimages_combo.currentText()
            n_images = int(n_images_text) if n_images_text else 0
            exposure_time = float(self.exposure_combo.currentText())
            dose_limit = float(self.dose_limit_edit.text())
            translation_x = float(self.translation_edit.text())
            dose_rate = calculate_interactive_dose_rate(
                flux, wavelength, beam_x, beam_y, attenuation
            )
            self.dose_rate_output.setText(f"{dose_rate:.3f}")
            rotisserie_factor, _ = _calculate_rotisserie_factor(
                lx, ly, lz, beam_x, beam_y, translation_x
            )
            self.rotisserie_output.setText(f"{rotisserie_factor:.2f}")
            total_dose = dose_rate * n_images * exposure_time
            self.total_dose_output.setText(f"{total_dose:.2f}")
            total_exposure = n_images * exposure_time
            self.total_exposure_time_output.setText(f"{total_exposure:.2f}")
            est_avg_dose = (
                total_dose / rotisserie_factor if rotisserie_factor > 0 else 0
            )
            self.est_avg_dose_output.setText(f"{est_avg_dose:.2f}")
            self.est_avg_dose_output.setStyleSheet(
                self.red_style if est_avg_dose > dose_limit else self.normal_style
            )
        except (ValueError, IndexError):
            self.dose_rate_output.setText("Invalid")
            self.rotisserie_output.setText("Invalid")
            self.total_dose_output.setText("Invalid")
            self.est_avg_dose_output.setText("Invalid")
            self.total_exposure_time_output.setText("Invalid")

    def _run_interactive_r3d_debounced(self):
        self.interactive_dwd_output.setText("Calculating...")
        self.last_dwd_output.setText("Calculating...")
        self.interactive_max_dose_output.setText("Calculating...")
        try:
            base_params = self._get_base_r3d_params()
            dynamic_params = self._get_dynamic_r3d_params()
            if base_params is None or dynamic_params is None:
                raise ValueError("Invalid R3D parameters")
            worker = InteractiveR3DWorker(base_params, dynamic_params)
            worker.signals.result.connect(self.set_interactive_r3d_results)
            worker.signals.error.connect(self._handle_worker_error)
            self.threadpool.start(worker)
        except (ValueError, IndexError, ZeroDivisionError):
            self.interactive_dwd_output.setText("Input Error")
            self.last_dwd_output.setText("Input Error")
            self.interactive_max_dose_output.setText("Input Error")

    def set_interactive_r3d_results(self, results):
        dwd_value, max_dose_value, last_dwd_value = results
        self.interactive_dwd_output.setText(f"{dwd_value:.2f}")
        self.last_dwd_output.setText(f"{last_dwd_value:.2f}")
        self.interactive_max_dose_output.setText(f"{max_dose_value:.2f}")
        try:
            dose_limit = float(self.dose_limit_edit.text())
            self.interactive_dwd_output.setStyleSheet(
                self.red_style if dwd_value > dose_limit else self.normal_style
            )
        except ValueError:
            self.interactive_dwd_output.setStyleSheet(self.normal_style)

    def _validate_translation_x(self):
        """
        Validates the current Translation X value against the crystal and beam sizes.
        Updates the input field's style and tooltip to provide user feedback.
        Returns True if valid, False otherwise.
        """
        try:
            lx = float(self.lx_edit.text())
            x_str, y_str = self.beam_combo.currentText().split("x")
            beam_x = float(x_str)
            current_trans_str = self.translation_edit.text()
            current_trans = float(current_trans_str) if current_trans_str else 0.0
            max_trans = lx - beam_x
            if max_trans >= 0:
                self.translation_edit.setToolTip(
                    f"Max allowed translation: {max_trans:.2f} µm"
                )
            else:
                self.translation_edit.setToolTip(
                    "Translation not possible (beam larger than crystal)"
                )
            if current_trans > max_trans or current_trans < 0:
                self.translation_edit.setStyleSheet(self.error_style)
                return False
            else:
                self.translation_edit.setStyleSheet("")
                return True
        except (ValueError, IndexError):
            self.translation_edit.setStyleSheet(self.error_style)
            self.translation_edit.setToolTip("Invalid input for crystal or beam size")
            return False

    def _update_dose_from_resolution(self):
        if not self.dose_limit_check.isChecked():
            try:
                res = float(self.resolution_edit.text())
                self.dose_limit_edit.setText(f"{res * 10.0:.1f}")
            except ValueError:
                self.dose_limit_edit.setText("")

    def _toggle_dose_input(self, state):
        is_direct = state == Qt.Checked
        self.dose_limit_edit.setEnabled(is_direct)
        self.resolution_edit.setEnabled(not is_direct)
        if not is_direct:
            self._update_dose_from_resolution()

    def _get_base_r3d_params(self):
        try:
            n_images_text = self.nimages_combo.currentText()
            n_images = int(n_images_text) if n_images_text else 0
            pdb_code = ""
            if self.coef_calc_combo.currentText() == "EXP":
                pdb_code = self.pdb_edit.text().strip()
                if (
                    not pdb_code
                    or not (len(pdb_code) == 4 and pdb_code.isalnum())
                    or os.path.isfile(pdb_code)
                ):
                    return None

            params = {
                "flux": float(self.flux_edit.text()),
                "crystal_dims": (
                    float(self.lx_edit.text()),
                    float(self.ly_edit.text()),
                    float(self.lz_edit.text()),
                ),
                "angle_l": float(self.angle_l_edit.text()),
                "angle_p": float(self.angle_p_edit.text()),
                "nimages": n_images,
                "osc": float(self.osc_edit.text()),
                "cell": self.cell_edit.text(),
                "nres": int(self.nres_edit.text()),
                "nmon": int(self.nmon_edit.text()),
                "shape": self.shape_combo.currentText().upper(),
                "coef_calc": self.coef_calc_combo.currentText(),
                "pdb_path_or_code": pdb_code,
                "ndna": int(self.dna_edit.text()),
                "nrna": int(self.rna_edit.text()),
                "ncarb": int(self.carb_edit.text()),
                "protein_heavy_atoms": self._get_dynamic_row_data(
                    self.heavy_atoms_layout
                ),
                "solvent_heavy_conc": self._get_dynamic_row_data(
                    self.solvent_atoms_layout
                ),
            }
            return params
        except (ValueError, IndexError):
            return None

    def _get_dynamic_r3d_params(self):
        try:
            x_str, y_str = self.beam_combo.currentText().split("x")
            translation_x = float(self.translation_edit.text())
            energy_kev = float(self.energy_edit.text())
            params = {
                "wavelength_a": self._e2w(energy_kev),
                "beam_size_um": (float(x_str), float(y_str)),
                "attenuation_factor": float(self.attenuation_combo.currentText()),
                "exposure_time_s": float(self.exposure_combo.currentText()),
                "translation_x_um": translation_x,
            }
            return params
        except (ValueError, IndexError):
            return None

    def cancel_calculation(self):
        logger.info("Cancellation requested by user.")
        self.status_label.setText("Cancelling... waiting for active tasks to finish.")
        self.cancel_button.setEnabled(False)  # Prevent multiple clicks

        self.calculation_running = (
            False  # Signal to all parts of the app that we are stopping
        )
        for worker in self.active_workers:
            worker.is_cancelled = True

        if self.tasks_to_run == self.tasks_finished:
            self.all_tasks_finished(cancelled=True)

    def run_calculation(self):
        if self.translation_fix_check.isChecked():
            if not self._validate_translation_x():
                QMessageBox.critical(
                    self,
                    "Invalid Translation",
                    "The specified Translation X is not possible.",
                )
                return

        # --- Reset ALL state for a fresh run ---
        self.calc_button.setEnabled(False)
        self.create_run_button.setEnabled(False)
        self.cancel_button.setVisible(True)
        self.cancel_button.setEnabled(True)
        self.results_table.setRowCount(0)
        self.final_results = []
        self.active_workers = []
        self.sorted_results = []
        self.all_unique_recommendations = []
        self.current_batch_index = 0
        self.tasks_to_run = 0
        self.tasks_finished = 0
        self.calculation_running = True

        try:
            self.status_label.setText("Finding optimal parameters...")
            QApplication.processEvents()
            try:
                crystal_dims = (
                    float(self.lx_edit.text()),
                    float(self.ly_edit.text()),
                    float(self.lz_edit.text()),
                )
                dose_limit = float(self.dose_limit_edit.text())
                flux = float(self.flux_edit.text())
            except ValueError:
                raise ValueError("Invalid numeric value in Experiment Parameters.")

            if self.nimages_fix_check.isChecked():
                images_to_search = [int(self.nimages_combo.currentText())]
            else:
                images_to_search = {
                    int(self.nimages_combo.itemText(i))
                    for i in range(self.nimages_combo.count())
                }
                try:
                    current_val = int(self.nimages_combo.currentText())
                    images_to_search.add(current_val)
                except ValueError:
                    pass
                images_to_search = sorted(list(images_to_search))

            beam_sizes_to_search = (
                [self.BEAM_SIZES[self.beam_combo.currentIndex()]]
                if self.beam_fix_check.isChecked()
                else self.BEAM_SIZES
            )
            translations_to_search = {}
            if self.translation_fix_check.isChecked():
                fixed_translation = float(self.translation_edit.text())
                for y, z in self.BEAM_SIZES:
                    max_trans = crystal_dims[2] - z
                    if fixed_translation >= 0 and fixed_translation <= max_trans:
                        translations_to_search[f"{y}x{z}"] = [fixed_translation]
                    else:
                        translations_to_search[f"{y}x{z}"] = []
            else:
                for y, z in self.BEAM_SIZES:
                    max_trans = crystal_dims[2] - z
                    translations_to_search[f"{y}x{z}"] = (
                        np.linspace(0, max_trans, 10) if max_trans > 0 else [0]
                    )

            exposure_times_to_search = (
                [float(self.exposure_combo.currentText())]
                if self.exposure_fix_check.isChecked()
                else self.EXPOSURE_TIMES
            )
            wavelengths_to_search = (
                [self._e2w(float(self.energy_edit.text()))]
                if self.energy_fix_check.isChecked()
                else [self._e2w(e) for e in self.ENERGIES_KEV]
            )

            recommendations = find_experimental_recommendations(
                crystal_dims=crystal_dims,
                dose_limit_mgy=dose_limit,
                flux_manager=self.data_source.flux_manager,
                desired_n_images_to_search=images_to_search,
                beam_sizes_to_search=beam_sizes_to_search,
                wavelengths_to_search=wavelengths_to_search,
                attenuations_to_search=(
                    [int(self.attenuation_combo.currentText())]
                    if self.attenuation_fix_check.isChecked()
                    else self.ATTENUATIONS
                ),
                translations_to_search=translations_to_search,
                exposure_times_to_search=exposure_times_to_search,
            )

            if not self.calculation_running:
                self.all_tasks_finished(cancelled=True)
                return

            if not recommendations:
                QMessageBox.warning(
                    self,
                    "No Solution Found",
                    "Could not find any parameter set that meets the requirements.",
                )
                self.all_tasks_finished()  # Call cleanup even if no solutions
                return

            if self.use_r3d_check.isChecked():
                sorted_full_recommendations = sorted(recommendations, key=self.sort_key)

                self.all_unique_recommendations = _prune_recommendations_for_raddose3d(
                    sorted_full_recommendations
                )

                if not self.all_unique_recommendations:
                    QMessageBox.warning(
                        self,
                        "No Unique Solutions",
                        "No unique parameter combinations were found to test.",
                    )
                    self.all_tasks_finished()
                    return

                self.start_next_r3d_batch()  # Start the first batch

            else:
                self.final_results = recommendations
                self.all_tasks_finished()
        except (ValueError, ZeroDivisionError) as e:
            QMessageBox.critical(
                self, "Input Error", f"Invalid input parameters:\n\n{e}"
            )
            self.all_tasks_finished(cancelled=True)

    def start_next_r3d_batch(self):
        """Kicks off the next batch of RADDOSE-3D calculations."""
        if not self.calculation_running:
            self.all_tasks_finished(cancelled=True)
            return

        start_index = self.current_batch_index * self.BATCH_SIZE
        end_index = start_index + self.BATCH_SIZE

        batch_to_run = self.all_unique_recommendations[start_index:end_index]

        if not batch_to_run:
            # This means we've run out of batches and found no solutions
            self.all_tasks_finished()
            return

        self.tasks_to_run = len(batch_to_run)
        self.tasks_finished = 0
        self.active_workers = []

        total_batches = math.ceil(
            len(self.all_unique_recommendations) / self.BATCH_SIZE
        )
        self.status_label.setText(
            f"Running RADDOSE-3D Batch {self.current_batch_index + 1}/{total_batches} ({self.tasks_to_run} tasks)..."
        )

        r3d_params = self._get_base_r3d_params()
        if r3d_params is None:
            QMessageBox.critical(self, "Input Error", "Invalid R3D parameters.")
            self.all_tasks_finished(cancelled=True)
            return

        worker_signals = WorkerSignals()
        worker_signals.result.connect(self.process_r3d_result)
        worker_signals.finished.connect(self.check_if_all_tasks_finished)
        worker_signals.error.connect(self._handle_worker_error)

        for rec in batch_to_run:
            if not self.calculation_running:
                break
            worker = R3DWorker(r3d_params, rec, worker_signals)
            self.active_workers.append(worker)
            self.threadpool.start(worker)

        if not self.calculation_running and len(self.active_workers) == 0:
            self.all_tasks_finished(cancelled=True)

    def process_r3d_result(self, result_with_dwd):
        if self.calculation_running:
            self.final_results.append(result_with_dwd)

    def check_if_all_tasks_finished(self):
        self.tasks_finished += 1

        if self.calculation_running:
            self.status_label.setText(
                f"Processing Batch {self.current_batch_index + 1}... {self.tasks_finished}/{self.tasks_to_run} complete."
            )

        if self.tasks_finished >= self.tasks_to_run:
            # --- BATCH COMPLETION LOGIC ---
            if self.use_r3d_check.isChecked() and self.calculation_running:
                dose_limit = float(self.dose_limit_edit.text())
                # Check if this batch produced any valid results
                valid_results_in_batch = [
                    rec
                    for rec in self.final_results
                    if rec.get("last_dwd_mgy", 0.0) <= dose_limit
                ]

                if valid_results_in_batch:
                    # Success! We found solutions, so we can stop.
                    self.all_tasks_finished()
                else:
                    # No valid solutions in this batch, try the next one.
                    self.current_batch_index += 1
                    self.start_next_r3d_batch()
            else:
                # Normal completion or cancellation
                self.all_tasks_finished(cancelled=not self.calculation_running)

    def sort_key(self, rec):
        """Provides a sorting key to rank recommendations."""
        dose_limit = float(self.dose_limit_edit.text())
        avg_dwd = rec.get("avg_dwd_mgy")
        if avg_dwd is None:
            avg_dwd = rec.get("effective_dose_mgy", float("inf"))

        # Primary sort key: how close is the dose to the limit (without going over, ideally)
        dwd_score = (abs(avg_dwd - dose_limit), avg_dwd)
        # Secondary keys for tie-breaking
        mismatch_score = rec.get("mismatch_score", float("inf"))
        wavelength_score = abs(rec.get("wavelength_a", 1.0) - 1.0)
        exposure_time_score = abs(rec.get("exposure_time_s", 0.1) - 0.1)
        translation_score = rec.get("translation_x_um", 0.0)

        return (
            dwd_score,
            mismatch_score,
            wavelength_score,
            exposure_time_score,
            translation_score,
        )

    def all_tasks_finished(self, cancelled=False):
        if cancelled:
            self.status_label.setText("Calculation cancelled.")
        elif not self.final_results:
            self.status_label.setText(
                "Calculation finished. No valid solutions found after search."
            )
        else:
            self.status_label.setText(
                f"Calculation finished. Found {len(self.final_results)} valid solutions."
            )

        # Only sort and display if the process wasn't cancelled early
        if not cancelled and self.final_results:
            dose_limit = float(self.dose_limit_edit.text())
            if self.use_r3d_check.isChecked():
                self.final_results = [
                    rec
                    for rec in self.final_results
                    if rec.get("last_dwd_mgy", 0.0) <= dose_limit
                ]
            self.sorted_results = sorted(self.final_results, key=self.sort_key)

            if self.sorted_results:
                top_rec = self.sorted_results[0]
                self.beam_combo.setCurrentText(
                    f"{top_rec['beam_size_um'][0]}x{top_rec['beam_size_um'][1]}"
                )
                self.translation_edit.setText(f"{top_rec['translation_x_um']:.2f}")
                wavelength_a = top_rec["wavelength_a"]
                energy_kev = self._w2e(wavelength_a)
                self.energy_edit.setText(f"{energy_kev:.2f}")
                self.attenuation_combo.setCurrentText(
                    str(int(top_rec["attenuation_factor"]))
                )
                self.exposure_combo.setCurrentText(str(top_rec["exposure_time_s"]))
                self.nimages_combo.setCurrentText(str(top_rec.get("n_images", "")))
                self.display_results_in_table(self.sorted_results)
            else:
                QMessageBox.warning(
                    self,
                    "No Valid Solutions",
                    "No recommendations met the dose limit criteria after RADDOSE-3D analysis.",
                )

        # --- This is the crucial part that must always run at the very end ---
        self.calculation_running = False
        self.calc_button.setEnabled(True)
        self.create_run_button.setEnabled(True)  # Re-enable this button too
        self.cancel_button.setVisible(False)
        self.active_workers = []  # Clear the list of workers

    def display_results_in_table(self, recommendations):
        self.results_table.setSortingEnabled(
            False
        )  # Turn off sorting during population
        self.results_table.clear()
        is_r3d = (
            self.use_r3d_check.isChecked()
            and recommendations
            and "avg_dwd_mgy" in recommendations[0]
        )

        headers = [
            "Rank",
            "Beam (XxY)",
            "Trans X",
            "Energy (KeV)",
            "Atten.",
            "Exp Time",
            "N Images",
        ]
        if is_r3d:
            headers.extend(["Avg DWD", "Last DWD", "Max Dose"])
        else:
            headers.append("Effective Dose")

        self.results_table.setColumnCount(len(headers))
        self.results_table.setHorizontalHeaderLabels(headers)
        self.results_table.setRowCount(len(recommendations[:30]))

        for i, rec in enumerate(recommendations[:30]):
            # --- Corrected and Simplified Logic ---

            # Column 0: Rank (but we will store the original index here)
            rank_item = NumericTableWidgetItem(str(i + 1))
            rank_item.setData(
                Qt.UserRole, i
            )  # Store original index 'i' from the sorted_results list
            self.results_table.setItem(i, 0, rank_item)

            # Column 1: Beam Size
            beam_str = f"{rec['beam_size_um'][0]}x{rec['beam_size_um'][1]}"
            self.results_table.setItem(i, 1, NumericTableWidgetItem(beam_str))

            # Column 2: Translation
            self.results_table.setItem(
                i, 2, NumericTableWidgetItem(f"{rec['translation_x_um']:.2f}")
            )

            # Column 3: Energy
            energy_kev = self._w2e(rec["wavelength_a"])
            self.results_table.setItem(
                i, 3, NumericTableWidgetItem(f"{energy_kev:.2f}")
            )

            # Column 4: Attenuation
            self.results_table.setItem(
                i, 4, NumericTableWidgetItem(str(int(rec["attenuation_factor"])))
            )

            # Column 5: Exposure Time
            self.results_table.setItem(
                i, 5, NumericTableWidgetItem(f"{rec['exposure_time_s']:.2f}")
            )

            # Column 6: N Images
            self.results_table.setItem(
                i, 6, NumericTableWidgetItem(str(rec["n_images"]))
            )

            # Columns 7+ (Conditional)
            if is_r3d:
                self.results_table.setItem(
                    i, 7, NumericTableWidgetItem(f"{rec.get('avg_dwd_mgy', 0.0):.2f}")
                )
                self.results_table.setItem(
                    i, 8, NumericTableWidgetItem(f"{rec.get('last_dwd_mgy', 0.0):.2f}")
                )
                self.results_table.setItem(
                    i, 9, NumericTableWidgetItem(f"{rec.get('max_dose_mgy', 0.0):.2f}")
                )
            else:
                self.results_table.setItem(
                    i,
                    7,
                    NumericTableWidgetItem(f"{rec.get('effective_dose_mgy', 0.0):.2f}"),
                )

            # Center all items for better appearance
            for j in range(self.results_table.columnCount()):
                if self.results_table.item(i, j):  # Check if item exists
                    self.results_table.item(i, j).setTextAlignment(Qt.AlignCenter)

        self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.results_table.hideColumn(
            0
        )  # Hide the Rank column, but keep it for index data
        self.results_table.setSortingEnabled(True)  # Re-enable sorting

    def on_result_selection_change(self):
        self.export_button.setEnabled(len(self.results_table.selectedItems()) > 0)

    def export_strategy(self):

        if self.imported_strategy_params:
            osc_start = self.imported_strategy_params.get("osc_start", 0)
            osc_delta = self.imported_strategy_params.get(
                "osc_delta", float(self.osc_edit.text())
            )
            distance = self.imported_strategy_params.get("distance", 300.0)
        else:
            # Fallback to old method if no strategy was imported
            try:
                external_data = self.data_source.get_latest_data()
                osc_start = external_data.get("osc_start", 0)
                distance = external_data.get("distance", 300.0)
            except Exception as e:
                QMessageBox.critical(
                    self, "Data Source Error", f"Could not fetch external data: {e}"
                )
                return
            osc_delta = float(self.osc_edit.text())

        selected_rows = self.results_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.warning(
                self, "No Selection", "Please select a recommendation to export."
            )
            return
        selected_row_visual_index = selected_rows[0].row()
        rank_item = self.results_table.item(selected_row_visual_index, 0)
        original_index = rank_item.data(Qt.UserRole)
        strategy_to_export = self.sorted_results[original_index]

        osc_end = osc_start + strategy_to_export.get("n_images", 1800) * float(
            osc_delta
        )

        translation_x = strategy_to_export.get("translation_x_um", 0.0)

        if translation_x > 0.01:
            mode = "helical"
        else:
            mode = "standard"

        post_data = {
            "module": "run_create",
            "mode": mode,
            "colli_um": strategy_to_export.get("beam_size_um"),
            "atten_factors": strategy_to_export.get("attenuation_factor"),
            "energy1_keV": self._w2e(strategy_to_export.get("wavelength_a")),
            "expTime_sec": strategy_to_export.get("exposure_time_s"),
            "delta_deg": osc_delta,
            "frame_deg_start": osc_start,
            "frame_deg_end": round(osc_end, 2),
            "det_z_mm": round(float(distance), 2),
        }

        self.export_to_pybluice(post_data)

    def create_run_from_gui(self):
        if self.imported_strategy_params:
            osc_start = self.imported_strategy_params.get("osc_start", 0)
            osc_delta = self.imported_strategy_params.get(
                "osc_delta", float(self.osc_edit.text())
            )
            distance = self.imported_strategy_params.get("distance")
        else:
            # Fallback to old method if no strategy was imported
            try:
                external_data = self.data_source.get_latest_data()
                osc_start = external_data.get("osc_start", 0)
                distance = external_data.get("distance")
            except Exception as e:
                QMessageBox.critical(
                    self, "Data Source Error", f"Could not fetch external data: {e}"
                )
                return
            osc_delta = float(self.osc_edit.text())

        try:
            x_str, y_str = self.beam_combo.currentText().split("x")
            beam_x, beam_y = float(x_str), float(y_str)
            energy_kev = float(self.energy_edit.text())
            nimages = int(self.nimages_combo.currentText())
            osc_end = osc_start + nimages * osc_delta
            translation_x = float(self.translation_edit.text())
            if translation_x > 0.01:
                mode = "helical"
            else:
                mode = "standard"

            post_data = {
                "module": "run_create",
                "mode": mode,
                "colli_um": (beam_x, beam_y),
                "atten_factors": float(self.attenuation_combo.currentText()),
                "energy1_keV": energy_kev,
                "expTime_sec": float(self.exposure_combo.currentText()),
                "frame_deg_start": osc_start,
                "frame_deg_end": osc_end,
                "delta_deg": osc_delta,
                "det_z_mm": distance,
            }
        except (ValueError, IndexError) as e:
            QMessageBox.critical(self, "Input Error", f"Invalid GUI parameters: {e}")
            return

        self.export_to_pybluice(post_data)

    def export_to_pybluice(self, run_parameters):
        rpc_url = get_rpc_url()
        if not rpc_url:
            QMessageBox.critical(
                self,
                "Export Error",
                "Failed to retrieve RPC URL. Cannot export strategy.",
            )
            return

        logger.debug(f"Exporting with params: {run_parameters}")

        try:
            self.status_label.setText("Sending export request...")
            resp = requests.post(rpc_url, data=run_parameters, timeout=10)
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
            self.status_label.setText("Export finished.")

    def _add_heavy_atom_row(self):
        row_layout = QHBoxLayout()
        row_layout.addWidget(QLabel("Element:"))
        elem_edit = QLineEdit()
        elem_edit.textChanged.connect(self.on_parameter_change)
        row_layout.addWidget(elem_edit)
        row_layout.addWidget(QLabel("Number:"))
        num_edit = QLineEdit()
        num_edit.setToolTip(
            "number of heavy atom per monomer, only effective when legacy coef calc mode is choosen"
        )
        num_edit.setValidator(QDoubleValidator())
        num_edit.textChanged.connect(self.on_parameter_change)
        row_layout.addWidget(num_edit)
        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(lambda: self._remove_row(row_layout))
        row_layout.addWidget(remove_btn)
        self.heavy_atoms_layout.addLayout(row_layout)

    def _add_solvent_atom_row(self):
        row_layout = QHBoxLayout()
        row_layout.addWidget(QLabel("Element:"))
        elem_edit = QLineEdit()
        elem_edit.textChanged.connect(self.on_parameter_change)
        row_layout.addWidget(elem_edit)
        row_layout.addWidget(QLabel("Conc (mM):"))
        conc_edit = QLineEdit()
        conc_edit.setValidator(QDoubleValidator())
        conc_edit.textChanged.connect(self.on_parameter_change)
        row_layout.addWidget(conc_edit)
        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(lambda: self._remove_row(row_layout))
        row_layout.addWidget(remove_btn)
        self.solvent_atoms_layout.addLayout(row_layout)

    def _remove_row(self, layout):
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        layout.deleteLater()
        self.on_parameter_change()

    def _get_dynamic_row_data(self, layout):
        data_list = []
        for i in range(layout.count()):
            item = layout.itemAt(i)
            if isinstance(item, QHBoxLayout):
                elem_widget = item.itemAt(1).widget()
                num_widget = item.itemAt(3).widget()
                if elem_widget and num_widget:
                    elem, num = (elem_widget.text().strip(), num_widget.text().strip())
                    if elem and num:
                        data_list.extend([elem, num])
        return " ".join(data_list)

    def _on_shape_change(self):
        self.lx_edit.blockSignals(True)
        self.ly_edit.blockSignals(True)
        self.lz_edit.blockSignals(True)
        try:
            shape = self.shape_combo.currentText()
            if shape == "Spherical":
                self.ly_edit.setText(self.lx_edit.text())
                self.lz_edit.setText(self.lx_edit.text())
            elif shape == "Cylinder":
                self.ly_edit.setText(self.lx_edit.text())
        finally:
            self.lx_edit.blockSignals(False)
            self.ly_edit.blockSignals(False)
            self.lz_edit.blockSignals(False)
        self.on_parameter_change()

    def _sync_crystal_dims(self):
        self.lx_edit.blockSignals(True)
        self.ly_edit.blockSignals(True)
        self.lz_edit.blockSignals(True)
        try:
            shape = self.shape_combo.currentText()
            sender = self.sender()
            if shape == "Spherical":
                if sender == self.lx_edit:
                    self.ly_edit.setText(self.lx_edit.text())
                    self.lz_edit.setText(self.lx_edit.text())
                elif sender == self.ly_edit:
                    self.lx_edit.setText(self.ly_edit.text())
                    self.lz_edit.setText(self.ly_edit.text())
                elif sender == self.lz_edit:
                    self.lx_edit.setText(self.lz_edit.text())
                    self.ly_edit.setText(self.lz_edit.text())
            elif shape == "Cylinder":
                if sender == self.lx_edit:
                    self.ly_edit.setText(self.lx_edit.text())
                elif sender == self.ly_edit:
                    self.lx_edit.setText(self.ly_edit.text())
        finally:
            self.lx_edit.blockSignals(False)
            self.ly_edit.blockSignals(False)
            self.lz_edit.blockSignals(False)

    def _handle_worker_error(self, error_info):
        exception, tb_info = error_info
        tb_text = "".join(
            traceback.format_exception(type(exception), exception, tb_info[2])
        )
        logger.error(f"An error occurred in a worker thread:\n{tb_text}")
        QMessageBox.critical(
            self,
            "Calculation Error",
            f"An error occurred during the calculation:\n\n{str(exception)}\n\nSee the console for a detailed traceback.",
        )
        if self.calculation_running:
            self.all_tasks_finished(cancelled=True)
        else:
            self.interactive_dwd_output.setText("Error")
            self.last_dwd_output.setText("Error")
            self.interactive_max_dose_output.setText("Error")


if __name__ == "__main__":
    setup_logging(root_name="qp2")
    
    try:
        from qp2.config.servers import ServerConfig
        ServerConfig.log_all_configs()
    except Exception as e:
        logger.warning(f"Failed to log server configurations: {e}")

    logger.info("Starting Dose Planner GUI client")
    qt_app = QApplication(sys.argv)
    ex = CrystalLifetimeGUI()
    ex.show()
    logger.info("Dose Planner GUI displayed successfully")
    sys.exit(qt_app.exec_())
