import math
import sys

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QApplication,
    QWidget,
    QLabel,
    QLineEdit,
    QComboBox,
    QGridLayout,
    QVBoxLayout,
    QCheckBox,
    QMessageBox,
    QScrollArea,
    QGroupBox,  # Added QGroupBox
)

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class CrystalLifetimeCalculatorApp(QWidget):
    def __init__(self):
        super().__init__()
        logger.info("Initializing Crystal Lifetime Calculator")
        self.id_widgets = {}
        self.old_reso = 0.0
        self.need_180 = 0
        self._init_ui()
        self._set_initial_values()
        logger.debug("Crystal Lifetime Calculator initialization complete")

    def _init_ui(self):
        logger.debug("Initializing UI components")
        self.setWindowTitle("Holton's Expected Crystal Lifetime Calculator")
        self.resize(700, 950)
        app_main_layout = QVBoxLayout()  # Renamed to avoid conflict with group layouts

        # Title
        title_label = QLabel("Holton's Expected Crystal Lifetime Calculator")
        title_label.setStyleSheet("font-size: 16pt; font-weight: bold;")
        app_main_layout.addWidget(title_label, alignment=Qt.AlignCenter)

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        content_widget = QWidget()
        scroll_area.setWidget(content_widget)

        # Main layout for content_widget will be QVBoxLayout to stack GroupBoxes
        main_content_layout = QVBoxLayout(content_widget)
        main_content_layout.setSpacing(15)  # Spacing between group boxes

        # --- Source Parameters Group ---
        source_groupbox = QGroupBox("Xray Source Parameters")
        source_layout = QGridLayout()
        source_layout.setSpacing(10)
        source_groupbox.setLayout(source_layout)

        # Full Flux
        source_layout.addWidget(QLabel("full flux ="), 0, 0)
        self.flux_input = QLineEdit()
        self.flux_input.editingFinished.connect(self.update_flux)
        source_layout.addWidget(self.flux_input, 0, 1)
        source_layout.addWidget(QLabel("photons/s"), 0, 2)

        # Attenuation and Transmittance
        source_layout.addWidget(QLabel("attenuation factor ="), 1, 0)
        self.attenuation_input = QLineEdit()
        self.attenuation_input.editingFinished.connect(self.update_attn)
        source_layout.addWidget(self.attenuation_input, 1, 1)
        source_layout.addWidget(QLabel("x"), 1, 2)

        source_layout.addWidget(QLabel("transmittance ="), 1, 3)
        self.transmittance_input = QLineEdit()
        self.transmittance_input.setDisabled(True)
        self.transmittance_input.editingFinished.connect(self.update_trans)
        source_layout.addWidget(self.transmittance_input, 1, 4)
        source_layout.addWidget(QLabel("%"), 1, 5)

        # Beam Size
        source_layout.addWidget(QLabel("beam size<sub>horiz</sub> ="), 2, 0)
        self.lbeam_z_input = QLineEdit()
        self.lbeam_z_input.editingFinished.connect(self.update_bs)
        source_layout.addWidget(self.lbeam_z_input, 2, 1)
        source_layout.addWidget(QLabel("microns"), 2, 2)

        source_layout.addWidget(QLabel("beam size<sub>vert</sub> ="), 2, 3)
        self.lbeam_y_input = QLineEdit()
        self.lbeam_y_input.editingFinished.connect(self.update_bs)
        source_layout.addWidget(self.lbeam_y_input, 2, 4)
        source_layout.addWidget(QLabel("microns"), 2, 5)

        self.flux_density_value = 0.0

        # Wavelength and k_dose
        source_layout.addWidget(QLabel("wavelength ="), 3, 0)
        self.wavelength_input = QLineEdit()
        self.wavelength_input.editingFinished.connect(self.update_wave)
        source_layout.addWidget(self.wavelength_input, 3, 1)
        source_layout.addWidget(QLabel("Ang"), 3, 2)

        source_layout.addWidget(QLabel("k<sub>dose</sub> ="), 3, 3)
        self.kdose_input = QLineEdit()
        self.kdose_input.setReadOnly(True)
        source_layout.addWidget(self.kdose_input, 3, 4)
        source_layout.addWidget(QLabel("photons/micron<sup>2</sup>/Gy"), 3, 5)

        # Dose Rate
        source_layout.addWidget(QLabel("dose rate ="), 4, 0)
        self.dose_rate_input = QLineEdit()
        self.dose_rate_input.setReadOnly(True)
        source_layout.addWidget(self.dose_rate_input, 4, 1)
        source_layout.addWidget(QLabel("Gy/s"), 4, 2)

        main_content_layout.addWidget(source_groupbox)

        # --- Sample & Goal Parameters Group ---
        sample_groupbox = QGroupBox("Sample Parameters")
        sample_layout = QGridLayout()
        sample_layout.setSpacing(10)
        sample_groupbox.setLayout(sample_layout)

        # Experiment Goal
        sample_layout.addWidget(QLabel("experiment goal ="), 0, 0)
        self.experiment_type_combo = QComboBox()
        self.experiment_type_combo.addItems(
            [
                "high resolution (cryo)",
                "MAD/SAD phasing",
                "S-SAD phasing",
                "room temperature",
                "Se-Met",
                "Hg-Cys",
                "Cys-Cys",
                "Br-RNA",
                "Cl-ligand",
                "photosystem II",
                "putidaredoxin",
                "bacteriorhodopsin",
                "Fe in myoglobin",
                "Custom ...",
            ]
        )
        self.experiment_type_combo.currentTextChanged.connect(self.update_exptype)
        sample_layout.addWidget(self.experiment_type_combo, 0, 1, 1, 2)

        # Resolution (resostuff1) - Conditional
        self.resostuff1_label = QLabel("resolution =")
        self.reso_input = QLineEdit()
        self.reso_input.editingFinished.connect(self.update_exptype)
        self.resostuff1_ang_label = QLabel("Ang")
        sample_layout.addWidget(self.resostuff1_label, 1, 0)
        sample_layout.addWidget(self.reso_input, 1, 1)
        sample_layout.addWidget(self.resostuff1_ang_label, 1, 2)
        self.id_widgets["resostuff1"] = [
            self.resostuff1_label,
            self.reso_input,
            self.resostuff1_ang_label,
        ]

        # MAD stuff (madstuff1-4) - Conditional
        self.madstuff1_label = QLabel("molecular weight =")
        self.MW_input = QLineEdit()
        self.MW_input.editingFinished.connect(self.update_anom)
        self.madstuff1_kda_label = QLabel("kDa")
        sample_layout.addWidget(self.madstuff1_label, 2, 0)
        sample_layout.addWidget(self.MW_input, 2, 1)
        sample_layout.addWidget(self.madstuff1_kda_label, 2, 2)
        self.id_widgets["madstuff1"] = [
            self.madstuff1_label,
            self.MW_input,
            self.madstuff1_kda_label,
        ]

        self.madstuff2_label = QLabel("number of sites =")
        self.sites_input = QLineEdit()
        self.sites_input.editingFinished.connect(self.update_anom)
        self.madstuff2_in_label = QLabel("in above")
        sample_layout.addWidget(self.madstuff2_label, 3, 0)
        sample_layout.addWidget(self.sites_input, 3, 1)
        sample_layout.addWidget(self.madstuff2_in_label, 3, 2)
        self.id_widgets["madstuff2"] = [
            self.madstuff2_label,
            self.sites_input,
            self.madstuff2_in_label,
        ]

        self.madstuff3_label = QLabel("fpp =")
        self.fpp_input = QLineEdit()
        self.fpp_input.editingFinished.connect(self.update_anom)
        self.madstuff3_el_label = QLabel("electrons")
        sample_layout.addWidget(self.madstuff3_label, 4, 0)
        sample_layout.addWidget(self.fpp_input, 4, 1)
        sample_layout.addWidget(self.madstuff3_el_label, 4, 2)
        self.id_widgets["madstuff3"] = [
            self.madstuff3_label,
            self.fpp_input,
            self.madstuff3_el_label,
        ]

        self.madstuff4_label = QLabel("Bijvoet ratio =")
        self.Bijvoet_input = QLineEdit()
        self.Bijvoet_input.editingFinished.connect(self.update_Bijvoet)
        self.madstuff4_pct_label = QLabel("%")
        sample_layout.addWidget(self.madstuff4_label, 5, 0)
        sample_layout.addWidget(self.Bijvoet_input, 5, 1)
        sample_layout.addWidget(self.madstuff4_pct_label, 5, 2)
        self.id_widgets["madstuff4"] = [
            self.madstuff4_label,
            self.Bijvoet_input,
            self.madstuff4_pct_label,
        ]

        # Dose Limit
        sample_layout.addWidget(QLabel("dose limit ="), 6, 0)
        self.dose_limit_input = QLineEdit()
        self.dose_limit_input.editingFinished.connect(self.update_doselim)
        sample_layout.addWidget(self.dose_limit_input, 6, 1)
        sample_layout.addWidget(QLabel("MGy"), 6, 2)

        # Crystal Size
        sample_layout.addWidget(QLabel("xtal size<sub>horiz</sub> ="), 7, 0)
        self.l_z_input = QLineEdit()
        self.l_z_input.editingFinished.connect(self.update_xtalsize)
        sample_layout.addWidget(self.l_z_input, 7, 1)
        sample_layout.addWidget(QLabel("microns"), 7, 2)

        sample_layout.addWidget(QLabel("xtal size<sub>vert</sub> ="), 7, 3)
        self.l_y_input = QLineEdit()
        self.l_y_input.editingFinished.connect(self.update_xtalsize)
        sample_layout.addWidget(self.l_y_input, 7, 4)
        sample_layout.addWidget(QLabel("microns"), 7, 5)

        sample_layout.addWidget(QLabel("xtal size<sub>thick</sub> ="), 8, 0)
        self.l_x_input = QLineEdit()
        self.l_x_input.editingFinished.connect(self.update_xtalsize)
        sample_layout.addWidget(self.l_x_input, 8, 1)
        sample_layout.addWidget(QLabel("microns"), 8, 2)

        main_content_layout.addWidget(sample_groupbox)

        # --- Experimental Setup Group ---
        exp_setup_groupbox = QGroupBox("Experimental Setup")
        exp_setup_layout = QGridLayout()
        exp_setup_layout.setSpacing(10)
        exp_setup_groupbox.setLayout(exp_setup_layout)

        # Exposure Time
        exp_setup_layout.addWidget(QLabel("exposure time ="), 0, 0)
        self.exposure_time_input = QLineEdit()
        self.exposure_time_input.editingFinished.connect(self.update_expo)
        exp_setup_layout.addWidget(self.exposure_time_input, 0, 1)
        exp_setup_layout.addWidget(QLabel("seconds/image"), 0, 2)

        # Translation
        exp_setup_layout.addWidget(QLabel("translation during dataset ="), 1, 0)
        self.translation_input = QLineEdit()
        self.translation_input.editingFinished.connect(self.update_xtalsize)
        exp_setup_layout.addWidget(self.translation_input, 1, 1)
        exp_setup_layout.addWidget(QLabel("microns"), 1, 2)

        # Rotisserie Factor & Disable Warnings
        exp_setup_layout.addWidget(QLabel("rotisserie factor"), 2, 0)
        self.rotisserie_factor_input = QLineEdit()
        self.rotisserie_factor_input.setReadOnly(True)
        self.rotisserie_factor_input.setDisabled(True)
        exp_setup_layout.addWidget(self.rotisserie_factor_input, 2, 1)

        self.disable_warnings_checkbox = QCheckBox("disable warnings")
        exp_setup_layout.addWidget(self.disable_warnings_checkbox, 2, 2, 1, 2)

        # Max Images
        exp_setup_layout.addWidget(QLabel("max images ="), 3, 0)
        self.total_images_input = QLineEdit()
        self.total_images_input.setReadOnly(True)
        exp_setup_layout.addWidget(self.total_images_input, 3, 1)
        exp_setup_layout.addWidget(QLabel("at damage limit"), 3, 2)

        # Inverse Beam
        exp_setup_layout.addWidget(QLabel("inverse beam ="), 4, 0)
        self.inverse_beam_combo = QComboBox()
        self.inverse_beam_combo.addItems(["no", "yes"])
        self.inverse_beam_combo.currentTextChanged.connect(self.update_ib)
        exp_setup_layout.addWidget(self.inverse_beam_combo, 4, 1)

        # Number of Wavelengths
        exp_setup_layout.addWidget(QLabel("number of wavelengths ="), 5, 0)
        self.wavelengths_input = QLineEdit()
        self.wavelengths_input.editingFinished.connect(self.update_waves)
        exp_setup_layout.addWidget(self.wavelengths_input, 5, 1)

        # Images/Wedge
        exp_setup_layout.addWidget(QLabel("images/wedge ="), 6, 0)
        self.wedge_size_input = QLineEdit()
        self.wedge_size_input.setReadOnly(True)
        exp_setup_layout.addWidget(self.wedge_size_input, 6, 1)

        main_content_layout.addWidget(exp_setup_groupbox)
        # Add stretch to push group boxes to the top if space allows
        main_content_layout.addStretch(1)

        self.all_input_fields = [
            self.attenuation_input,
            self.Bijvoet_input,
            self.dose_limit_input,
            self.dose_rate_input,
            self.exposure_time_input,
            self.flux_input,
            self.fpp_input,
            self.kdose_input,
            self.lbeam_y_input,
            self.lbeam_z_input,
            self.l_x_input,
            self.l_y_input,
            self.l_z_input,
            self.MW_input,
            self.reso_input,
            self.rotisserie_factor_input,
            self.sites_input,
            self.total_images_input,
            self.translation_input,
            self.transmittance_input,
            self.wavelengths_input,
            self.wavelength_input,
            self.wedge_size_input,
        ]
        self.all_combos = [self.experiment_type_combo, self.inverse_beam_combo]

        app_main_layout.addWidget(scroll_area)
        self.setLayout(app_main_layout)

    def _set_initial_values(self):
        # Set initial values as per request and original defaults
        self.flux_input.setText("5e12")
        self.wavelength_input.setText("1")
        self.kdose_input.setText("2000")
        self.dose_rate_input.setText("0")
        self.rotisserie_factor_input.setText("1.0")

        self.transmittance_input.setText("100")
        self.attenuation_input.setText("1")
        self.flux_density_value = 0.0

        self.lbeam_y_input.setText("50")
        self.lbeam_z_input.setText("50")
        self.translation_input.setText("0")

        self.l_x_input.setText("50")
        self.l_y_input.setText("50")
        self.l_z_input.setText("50")

        self.dose_limit_input.setText("30")
        self.reso_input.setText("3")

        self.experiment_type_combo.setCurrentText("high resolution (cryo)")

        self.exposure_time_input.setText("0.2")
        self.total_images_input.setText("0")
        self.inverse_beam_combo.setCurrentText("no")
        self.wavelengths_input.setText("1")
        self.wedge_size_input.setText("0")
        self.disable_warnings_checkbox.setChecked(False)

        self.MW_input.setText("14")
        self.sites_input.setText("1")
        self.fpp_input.setText("4")
        self.Bijvoet_input.setText("2.56")

        self.calc_kdose()
        self.calc_flux()
        self.calc_spread()
        self.update_exptype()

    def _toggle_widget_group_visibility(self, group_id, show):
        if group_id in self.id_widgets:
            widgets = self.id_widgets[group_id]
            if isinstance(widgets, list):
                for widget in widgets:
                    widget.setVisible(show)
            else:
                widgets.setVisible(show)

    def _clear_borders(self):
        default_stylesheet = ""
        for field in self.all_input_fields:
            field.setStyleSheet(default_stylesheet)
        for combo in self.all_combos:
            combo.setStyleSheet(default_stylesheet)

    def _set_border_color(self, widget, color):
        widget.setStyleSheet(f"border: 1px solid {color};")

    def calc_kdose(self):
        try:
            wave = float(self.wavelength_input.text())
            if wave > 3.0:
                wave = 3.0
            if wave < 0.5:
                wave = 0.5

            kdose_val = 2000.0 / wave / wave
            self.kdose_input.setText(f"{kdose_val:.2g}")
            self._set_border_color(self.kdose_input, "blue")
            logger.debug(f"Calculated kdose: {kdose_val:.2g} for wavelength: {wave}")
        except ValueError as e:
            logger.error(f"Error calculating kdose: {e}")
            self.kdose_input.setText("0")

    def calc_flux(self):
        try:
            flux = float(self.flux_input.text())
            lbeam_y = float(self.lbeam_y_input.text())
            lbeam_z = float(self.lbeam_z_input.text())
            trans = float(self.transmittance_input.text())
            kdose = float(self.kdose_input.text())

            if lbeam_y == 0 or lbeam_z == 0 or kdose == 0:
                flux_density = 0.0
                dose_rate = 0.0
            else:
                flux_density = flux / lbeam_y / lbeam_z * trans / 100.0
                dose_rate = flux_density / kdose

            self.flux_density_value = flux_density
            self.dose_rate_input.setText(f"{dose_rate:.2e}")
            self._set_border_color(self.dose_rate_input, "blue")
            logger.debug(f"Calculated flux density: {flux_density:.2e}, dose rate: {dose_rate:.2e}")

        except ValueError as e:
            logger.error(f"Error calculating flux: {e}")
            self.dose_rate_input.setText("0")
            self.flux_density_value = 0.0

        self.calc_frames()

    def update_flux(self):
        self._clear_borders()
        self.calc_flux()
        self.roundoff()

    def update_wave(self):
        self._clear_borders()
        self.calc_kdose()
        self._set_border_color(self.wavelength_input, "blue")

        self.calc_flux()
        self.roundoff()

    def update_attn(self):
        self._clear_borders()
        try:
            attn = float(self.attenuation_input.text())
            if attn >= 500:
                attn = 500
            if attn <= 1:
                attn = 1.0

            trans = 100.0 / attn

            self.transmittance_input.setText(f"{trans:.3g}")
            self.attenuation_input.setText(f"{attn:.3g}")
            self._set_border_color(self.transmittance_input, "blue")

            self.calc_flux()
        except ValueError:
            pass
        self.roundoff()

    def update_trans(self):
        self._clear_borders()
        try:
            trans = float(self.transmittance_input.text())
            if trans > 100.0:
                trans = 100.0
            if trans < 0.01:
                trans = 0.01
            if trans <= 0:
                trans = 0.01

            attn = 100.0 / trans

            self.attenuation_input.setText(f"{attn:.3g}")
            self.transmittance_input.setText(f"{trans:.3g}")
            self._set_border_color(self.attenuation_input, "blue")

            self.calc_flux()
        except ValueError:
            pass
        self.roundoff()

    def update_bs(self):
        self._clear_borders()

        self.calc_flux()
        self.calc_spread()
        self.roundoff()

    def calc_spread(self):
        try:
            lbeam_y = float(self.lbeam_y_input.text())
            lbeam_z = float(self.lbeam_z_input.text())
            l_x = float(self.l_x_input.text())
            l_y = float(self.l_y_input.text())
            l_z_xtal = float(self.l_z_input.text())
            translation = float(self.translation_input.text())

            if translation >= l_z_xtal - lbeam_z:
                translation = l_z_xtal - lbeam_z
                if translation < 0:
                    translation = 0
                self._set_border_color(self.translation_input, "blue")
            if translation < 0:
                translation = 0

            self.translation_input.setText(f"{translation:.3g}")

            eff_horiz = translation + lbeam_z
            eff_vert = lbeam_y
            self.need_180 = 0
            if lbeam_y > 0 and (eff_vert < l_y or eff_vert < l_x):
                if l_x > 0 and l_y > 0:
                    self.need_180 = 1
                    eff_vert = math.sqrt(l_x * l_y)

            rotisserie_factor = 1.0
            if lbeam_y > 0 and lbeam_z > 0:
                eff_area = eff_vert * eff_horiz
                rotisserie_factor = eff_area / (lbeam_y * lbeam_z)

            self.rotisserie_factor_input.setText(f"{rotisserie_factor:.4g}")
            self._set_border_color(self.rotisserie_factor_input, "blue")

        except ValueError:
            self.rotisserie_factor_input.setText("1.0")

        self.calc_frames()

    def calc_frames(self):
        try:
            dose_rate = float(self.dose_rate_input.text())
            rotisserie_factor = float(self.rotisserie_factor_input.text())
            dose_limit = float(self.dose_limit_input.text())
            exposure_time_from_input = float(self.exposure_time_input.text())

            ib = 2 if self.inverse_beam_combo.currentText() == "yes" else 1
            waves = float(self.wavelengths_input.text())

            clamped_exposure_time_for_calc = exposure_time_from_input
            if clamped_exposure_time_for_calc < 0.001:
                clamped_exposure_time_for_calc = 0.001

            self.exposure_time_input.setText(str(clamped_exposure_time_for_calc))

            images = 0
            if dose_rate > 0 and clamped_exposure_time_for_calc > 0:
                images = (
                        dose_limit
                        * 1e6
                        / dose_rate
                        / clamped_exposure_time_for_calc
                        * rotisserie_factor
                )

            wedgesize = 0
            if ib > 0 and waves > 0:
                wedgesize = images / ib / waves

            self.total_images_input.setText(str(round(images)))
            self.wedge_size_input.setText(str(round(wedgesize)))

            if (
                    wedgesize < 180
                    and self.need_180
                    and not self.disable_warnings_checkbox.isChecked()
            ):
                QMessageBox.warning(
                    self,
                    "Warning",
                    "WARNING: impossible to utilize whole crystal!\nAdjusting exposure time.",
                )

                ideal_new_exposure_time = 0.001
                if 181.0 > 0 and wedgesize > 0:
                    ideal_new_exposure_time = (
                            clamped_exposure_time_for_calc * wedgesize / 181.0
                    )

                actual_new_exposure_time_to_set = ideal_new_exposure_time
                if actual_new_exposure_time_to_set < 0.001:
                    actual_new_exposure_time_to_set = 0.001

                self.exposure_time_input.setText(str(actual_new_exposure_time_to_set))
                self._set_border_color(self.exposure_time_input, "red")

                if (
                        abs(
                            actual_new_exposure_time_to_set - clamped_exposure_time_for_calc
                        )
                        > 1e-9
                ):
                    self.update_expo()

        except ValueError:
            self.total_images_input.setText("Error")
            self.wedge_size_input.setText("Error")

    def update_exptype(self):
        self._clear_borders()
        exp_type_text = self.experiment_type_combo.currentText()
        dose_limit_val_str = self.dose_limit_input.text()
        reso_val_str = self.reso_input.text()

        try:
            dose_limit = float(dose_limit_val_str if dose_limit_val_str else "0")
            reso = float(reso_val_str if reso_val_str else "0.0")
        except ValueError:
            dose_limit = 30
            reso = 3.0

        exp_type_map = {
            "high resolution (cryo)": "reso",
            "MAD/SAD phasing": "MAD",
            "S-SAD phasing": "SSAD",
            "room temperature": "RT",
            "Se-Met": "SeMet",
            "Hg-Cys": "Hg",
            "Cys-Cys": "SS",
            "Br-RNA": "Br",
            "Cl-ligand": "Cl",
            "photosystem II": "PSII",
            "putidaredoxin": "PT",
            "bacteriorhodopsin": "BR",
            "Fe in myoglobin": "FeMb",
            "Custom ...": "Custom",
        }
        exp_type = exp_type_map.get(exp_type_text, "Custom")

        if exp_type == "reso":
            dose_limit = 10.0 * reso if reso > 0 else 30.0
        elif exp_type == "RT":
            dose_limit = 0.2
        elif exp_type == "SeMet":
            dose_limit = 5.0
        elif exp_type == "Hg":
            dose_limit = 4.0
        elif exp_type == "SS":
            dose_limit = 2.0
        elif exp_type == "Br" or exp_type == "Cl":
            dose_limit = 0.5
        elif exp_type == "PSII":
            dose_limit = 0.5
        elif exp_type == "PT":
            dose_limit = 0.06
        elif exp_type == "BR":
            dose_limit = 0.06
        elif exp_type == "FeMb":
            dose_limit = 0.02

        self.dose_limit_input.setText(f"{dose_limit:.2g}")
        self._set_border_color(self.dose_limit_input, "blue")

        show_reso = exp_type == "reso"
        self._toggle_widget_group_visibility("resostuff1", show_reso)
        if show_reso:
            self.inverse_beam_combo.setCurrentText("no")
            self.wavelengths_input.setText("1")

        show_mad = exp_type == "MAD" or exp_type == "SSAD"
        self._toggle_widget_group_visibility("madstuff1", show_mad)
        self._toggle_widget_group_visibility("madstuff2", show_mad)
        self._toggle_widget_group_visibility("madstuff3", show_mad)
        self._toggle_widget_group_visibility("madstuff4", show_mad)

        bijvoet_updated_via_ssad_path = False

        if show_mad:
            self.inverse_beam_combo.setCurrentText("yes")
            self.wavelengths_input.setText("2")
            if exp_type == "SSAD":
                self.wavelengths_input.setText("1")
                new_wave_ssad = 12398.42 / 7235.0
                self.wavelength_input.setText(f"{new_wave_ssad:.5g}")
                self.fpp_input.setText("0.685")
                self.update_anom()
                bijvoet_updated_via_ssad_path = True
                self._set_border_color(self.flux_input, "red")
                self._set_border_color(self.wavelength_input, "red")
                self.calc_kdose()
                self.calc_flux()

        if exp_type == "MAD" or exp_type == "SSAD":
            if not bijvoet_updated_via_ssad_path:
                self.update_Bijvoet()
        else:
            self.calc_frames()

        self.roundoff()

    def update_doselim(self):
        self._clear_borders()
        try:
            dose_limit = float(self.dose_limit_input.text())
            self.dose_limit_input.setText(f"{dose_limit:.2g}")
            current_exp_type = self.experiment_type_combo.currentText()
            if current_exp_type != "Custom ...":
                self.experiment_type_combo.setCurrentText("Custom ...")
            else:
                self._toggle_widget_group_visibility("resostuff1", False)
                self._set_border_color(self.dose_limit_input, "blue")
                self.calc_frames()
                self.roundoff()

        except ValueError:
            pass

    def update_xtalsize(self):
        self._clear_borders()
        self.calc_spread()
        self.roundoff()

    def update_expo(self):
        self._clear_borders()
        self.calc_frames()
        self.roundoff()

    def update_images(self):
        self._clear_borders()
        self.calc_frames()
        self.roundoff()

    def update_ib(self):
        self._clear_borders()
        self.calc_frames()
        self.roundoff()

    def update_waves(self):
        self._clear_borders()
        self.calc_frames()
        self.roundoff()

    def update_wedge(self):
        self._clear_borders()
        self.calc_frames()
        self.roundoff()

    def update_Bijvoet(self):
        self._clear_borders()
        try:
            Bijvoet = float(self.Bijvoet_input.text())
            MW = float(self.MW_input.text() if self.MW_input.text() else "14")
            sites = float(self.sites_input.text() if self.sites_input.text() else "1")

            if Bijvoet <= 0:
                current_exp_type = self.experiment_type_combo.currentText()
                if current_exp_type != "high resolution (cryo)":
                    self.experiment_type_combo.setCurrentText("high resolution (cryo)")
                else:
                    self.update_exptype()
                return

            if sites == 0:
                sites = 1.0
                self.sites_input.setText("1.0")
            if MW <= 0:
                MW = 14.0
                self.MW_input.setText(f"{MW:.3g}")

            fpp_val = (
                    Bijvoet
                    / 100.0
                    / math.sqrt(2)
                    / math.sqrt(sites)
                    * math.sqrt(MW * 1000.0 / 14.0)
                    * 7.0
            )

            self.dose_limit_input.setText(f"{Bijvoet:.2g}")
            self._set_border_color(self.dose_limit_input, "blue")

            self.calc_frames()

            self.Bijvoet_input.setText(f"{Bijvoet:.3g}")
            self.fpp_input.setText(f"{fpp_val:.2g}")
            self._set_border_color(self.Bijvoet_input, "blue")
            self._set_border_color(self.fpp_input, "blue")

        except ValueError:
            pass
        self.roundoff()

    def calc_Bijvoet(self):
        try:
            MW = float(self.MW_input.text() if self.MW_input.text() else "14")
            sites = float(self.sites_input.text() if self.sites_input.text() else "1")
            fpp = float(self.fpp_input.text() if self.fpp_input.text() else "4")

            if sites <= 0:
                current_exp_type = self.experiment_type_combo.currentText()
                if current_exp_type != "high resolution (cryo)":
                    self.experiment_type_combo.setCurrentText("high resolution (cryo)")
                else:
                    self.update_exptype()
                return

            if MW <= 0:
                MW = 14.0
                self.MW_input.setText(f"{MW:.3g}")
                self._set_border_color(self.MW_input, "blue")

            denominator_sqrt_arg = MW * 1000.0 / 14.0
            if denominator_sqrt_arg <= 0:
                Bijvoet_val = 0.0
            else:
                Bijvoet_val = (
                        100.0
                        * math.sqrt(2)
                        * math.sqrt(sites)
                        * fpp
                        / (math.sqrt(denominator_sqrt_arg) * 7.0)
                )

            self.Bijvoet_input.setText(f"{Bijvoet_val:.3g}")
            self._set_border_color(self.Bijvoet_input, "blue")

            self.update_Bijvoet()

        except ValueError:
            pass

    def update_anom(self):
        self._clear_borders()
        self.calc_Bijvoet()

    def roundoff(self):
        def format_text(widget, formatter_str, is_exp=False):
            try:
                val_str = widget.text()
                if not val_str:
                    return
                val = float(val_str)
                if is_exp:
                    widget.setText(f"{val:{formatter_str}}")
                else:

                    if formatter_str == ".2g" and abs(val) < 0.01 and val != 0:
                        widget.setText(f"{val:.1e}")
                    elif formatter_str == ".3g" and abs(val) < 0.01 and val != 0:
                        widget.setText(f"{val:.2e}")
                    else:
                        widget.setText(f"{val:{formatter_str}}")

            except ValueError:
                pass

        format_text(self.lbeam_z_input, ".3g")
        format_text(self.lbeam_y_input, ".3g")
        format_text(self.flux_input, ".1e", is_exp=True)
        format_text(self.dose_rate_input, ".1e", is_exp=True)
        format_text(self.dose_limit_input, ".2g")
        format_text(self.wavelength_input, ".5g")
        format_text(self.kdose_input, ".2g")
        format_text(self.rotisserie_factor_input, ".4g")

        format_text(self.transmittance_input, ".3g")
        format_text(self.attenuation_input, ".3g")

        format_text(self.MW_input, ".3g")
        format_text(self.sites_input, ".3g")
        format_text(self.fpp_input, ".2g")
        format_text(self.Bijvoet_input, ".3g")

        if self.dose_rate_input.text() == "0":
            self.dose_rate_input.setText("0.0e+0")
        if self.flux_input.text() == "0":
            self.flux_input.setText("0.0e+0")


if __name__ == "__main__":
    logger.info("Starting Crystal Lifetime Calculator application")
    app = QApplication(sys.argv)
    ex = CrystalLifetimeCalculatorApp()
    ex.show()
    logger.info("Crystal Lifetime Calculator GUI displayed")
    sys.exit(app.exec_())
