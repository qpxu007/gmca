import math

from qp2.log.logging_config import get_logger
from qp2.radiation_decay.data_source import FluxManager
from qp2.radiation_decay.raddose3d import Sample, Beam, Wedge

logger = get_logger(__name__)


def _calculate_rotisserie_factor(lx, ly, lz, beam_x, beam_y, translation_x=0.0):
    if lx - beam_x <= 0:
        translation_x = 0
    elif translation_x >= lx - beam_x:
        translation_x = lx - beam_x
    if translation_x < 0:
        translation_x = 0
    effective_horizontal_coverage = translation_x + beam_x
    effective_vertical_coverage = beam_y
    if effective_vertical_coverage < ly or effective_vertical_coverage < lz:
        effective_vertical_coverage = math.sqrt(lz * ly)
    if beam_y * beam_x == 0:
        return 1.0, False
    return (effective_vertical_coverage * effective_horizontal_coverage) / (
            beam_y * beam_x
    ), False


def calculate_lifetime_and_rate(
        flux,
        wavelength,
        dose_limit_mgy,
        crystal_lx_um,
        crystal_ly_um,
        crystal_lz_um,
        beam_size_x_um,
        beam_size_y_um,
        attenuation_factor,
        translation_x_um,
):
    if any(
            val <= 0
            for val in [
                flux,
                wavelength,
                dose_limit_mgy,
                crystal_lx_um,
                crystal_ly_um,
                crystal_lz_um,
                beam_size_y_um,
                beam_size_x_um,
                attenuation_factor,
            ]
    ):
        return 0, 0
    dose_limit_gy = dose_limit_mgy * 1e6
    flux_density = flux / (beam_size_y_um * beam_size_x_um) / attenuation_factor
    kdose = 2000.0 / (wavelength * wavelength) if wavelength > 0 else 0
    if kdose == 0:
        return 0, 0
    dose_rate_gy_s = flux_density / kdose
    if dose_rate_gy_s == 0:
        return 0, 0
    rotisserie_factor, _ = _calculate_rotisserie_factor(
        crystal_lx_um,
        crystal_ly_um,
        crystal_lz_um,
        beam_size_x_um,
        beam_size_y_um,
        translation_x=translation_x_um,
    )
    lifetime_s = dose_limit_gy * rotisserie_factor / dose_rate_gy_s
    return lifetime_s, dose_rate_gy_s / 1e6


def calculate_interactive_dose_rate(flux, wavelength, beam_x, beam_y, attenuation):
    if any(val <= 0 for val in [flux, wavelength, beam_x, beam_y, attenuation]):
        return 0.0
    flux_density = flux / (beam_y * beam_x) / attenuation
    kdose = 2000.0 / (wavelength * wavelength) if wavelength > 0 else 0
    if kdose == 0:
        return 0.0
    return (flux_density / kdose) / 1e6


def find_experimental_recommendations(
        crystal_dims,
        dose_limit_mgy,
        flux_manager: FluxManager,
        desired_n_images_to_search,
        beam_sizes_to_search,
        wavelengths_to_search,
        attenuations_to_search,
        translations_to_search,
        exposure_times_to_search,
):
    logger.info(
        f"Finding experimental recommendations with dose limit {dose_limit_mgy} MGy"
    )
    logger.debug(
        f"Search space: {len(beam_sizes_to_search)} beam sizes, {len(wavelengths_to_search)} wavelengths, {len(attenuations_to_search)} attenuations"
    )
    crystal_lx_um, crystal_ly_um, crystal_lz_um = crystal_dims
    recommendations, seen_recommendations = [], set()
    for beam_x, beam_y in beam_sizes_to_search:
        current_translations = translations_to_search.get(f"{beam_x}x{beam_y}", [])
        for translation_x in current_translations:
            for wavelength in wavelengths_to_search:
                energy_kev = 12.3984 / wavelength if wavelength > 0 else 0
                current_flux = flux_manager.get_flux(energy_kev)
                for attenuation in attenuations_to_search:
                    for exposure_time in exposure_times_to_search:
                        for desired_n_images in desired_n_images_to_search:
                            lifetime_s, dose_rate_mgy_s = calculate_lifetime_and_rate(
                                flux=current_flux,
                                wavelength=wavelength,
                                dose_limit_mgy=dose_limit_mgy,
                                crystal_lx_um=crystal_lx_um,
                                crystal_ly_um=crystal_ly_um,
                                crystal_lz_um=crystal_lz_um,
                                beam_size_x_um=beam_x,
                                beam_size_y_um=beam_y,
                                attenuation_factor=attenuation,
                                translation_x_um=translation_x,
                            )
                            max_images = (
                                lifetime_s / exposure_time if exposure_time > 0 else 0
                            )
                            if max_images >= desired_n_images:
                                rotisserie_factor, _ = _calculate_rotisserie_factor(
                                    crystal_lx_um,
                                    crystal_ly_um,
                                    crystal_lz_um,
                                    beam_x,
                                    beam_y,
                                    translation_x,
                                )
                                total_dose = (
                                        dose_rate_mgy_s * desired_n_images * exposure_time
                                )
                                effective_dose = (
                                    total_dose / rotisserie_factor
                                    if rotisserie_factor > 0
                                    else total_dose
                                )
                                rec_tuple = (
                                    beam_x,
                                    beam_y,
                                    wavelength,
                                    attenuation,
                                    round(translation_x, 2),
                                    exposure_time,
                                    desired_n_images,
                                )
                                if rec_tuple not in seen_recommendations:
                                    recommendations.append(
                                        {
                                            "beam_size_um": (beam_x, beam_y),
                                            "wavelength_a": wavelength,
                                            "attenuation_factor": attenuation,
                                            "translation_x_um": round(translation_x, 2),
                                            "exposure_time_s": exposure_time,
                                            "n_images": desired_n_images,
                                            "effective_dose_mgy": effective_dose,
                                            "mismatch_score": abs(
                                                beam_x - crystal_lx_um
                                            )
                                                              + abs(beam_y - crystal_ly_um),
                                        }
                                    )
                                    seen_recommendations.add(rec_tuple)
    logger.info(f"Found {len(recommendations)} total recommendations")
    return recommendations


def _prune_recommendations_for_raddose3d(recommendations):
    """
    Prunes a list of recommendations to keep only one representative for each
    unique set of physical parameters relevant to a RADDOSE-3D calculation.
    This version uses the calculated effective dose as a core part of the signature.
    """
    if not recommendations:
        return []

    seen_signatures = set()
    pruned_list = []

    # The list should be pre-sorted by a quality heuristic before being passed to this function.
    # This ensures that the first item we see for a given signature is the "best" one to keep.
    for rec in recommendations:
        # Create a "dose signature" tuple.
        # This groups recommendations that are physically equivalent from RADDOSE-3D's perspective.
        signature = (
            rec["beam_size_um"],
            rec["translation_x_um"],
            round(rec["wavelength_a"], 3),
            # The effective dose is a proxy for total photons delivered, considering geometry.
            # Rounding it groups strategies that are nearly identical in dose delivery.
            round(rec["effective_dose_mgy"], 2),
        )

        if signature not in seen_signatures:
            seen_signatures.add(signature)
            pruned_list.append(rec)

    logger.info(
        f"Pruned RADDOSE-3D search space from {len(recommendations)} to {len(pruned_list)} unique calculations."
    )
    return pruned_list


def _setup_raddose3d_input(base_params, dynamic_params):
    lx, ly, lz = base_params["crystal_dims"]
    r3d_crystal_size = f"{ly} {lx} {lz}"
    beam_x, beam_y = dynamic_params["beam_size_um"]
    r3d_beam_size = f"{beam_y} {beam_x}"
    nimages = dynamic_params.get("n_images", base_params["nimages"])
    osc = base_params["osc"]
    translation_x = dynamic_params["translation_x_um"]
    total_rotation = nimages * osc
    trans_per_deg = translation_x / total_rotation if total_rotation > 0 else 0
    r3d_translate_per_degree = f"{trans_per_deg} 0 0"
    wavelength = dynamic_params["wavelength_a"]
    energy_kev = 12.398 / wavelength if wavelength > 0 else 0
    pdb_path_or_code = base_params.get("pdb_path_or_code", "").strip()
    coef_calc = base_params["coef_calc"]
    if coef_calc == "EXP" and not pdb_path_or_code:
        # This is a safety check in case the GUI logic fails.
        # It's better to fall back than to crash.
        logger.warning(
            "EXP mode selected but no PDB provided. Falling back to AVERAGE."
        )
        coef_calc = "AVERAGE"

    logger.debug(f"base params:{base_params}")

    sample = Sample(
        cell=base_params["cell"],
        nres=base_params["nres"],
        nmon=base_params["nmon"],
        crystal_shape=base_params["shape"],
        crystal_size=r3d_crystal_size,
        angle_l=base_params.get("angle_l", 0.0),
        angle_p=base_params.get("angle_p", 0.0),
        coef_calc=base_params["coef_calc"],
        pdbcode=pdb_path_or_code,
        ndna=base_params["ndna"],
        nrna=base_params["nrna"],
        ncarb=base_params["ncarb"],
        solvent_fraction=base_params.get("solvent_fraction", 0.5),
        protein_heavy_atoms=base_params["protein_heavy_atoms"],
        solvent_heavy_conc=base_params["solvent_heavy_conc"],
    )

    beam = Beam(
        flux=base_params["flux"],
        energy=energy_kev,
        beam_size=r3d_beam_size,
        attenuation_factor=dynamic_params["attenuation_factor"],
    )

    wedge = Wedge(
        osc=osc,
        nimages=nimages,
        exposure_time_per_image=dynamic_params["exposure_time_s"],
        translate_per_degree=r3d_translate_per_degree,
    )
    return sample, beam, [wedge]
