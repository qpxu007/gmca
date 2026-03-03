import math

from flask import Flask, request, jsonify


# --- Backend Calculation Logic (Identical to GUI version) ---
def _calculate_rotisserie_factor(lx, ly, lz, beam_y, beam_z, translation_z=0):
    if translation_z >= lz - beam_z: translation_z = lz - beam_z
    if translation_z < 0: translation_z = 0
    effective_horizontal_coverage = translation_z + beam_z
    effective_vertical_coverage = beam_y
    if effective_vertical_coverage < ly or effective_vertical_coverage < lx:
        effective_vertical_coverage = math.sqrt(lx * ly)
    effective_exposed_area = effective_vertical_coverage * effective_horizontal_coverage
    return effective_exposed_area / (beam_y * beam_z), False


def calculate_crystal_lifetime(
        flux, wavelength, dose_limit_mgy, crystal_lx_um, crystal_ly_um,
        crystal_lz_um, beam_size_y_um, beam_size_z_um, attenuation_factor,
        translation_z_um, exposure_time_s=1.0):
    if any(val <= 0 for val in [flux, wavelength, dose_limit_mgy, crystal_lx_um,
                                crystal_ly_um, crystal_lz_um, beam_size_y_um,
                                beam_size_z_um, attenuation_factor]):
        return 0, 0
    dose_limit_gy = dose_limit_mgy * 1e6
    flux_density = flux / (beam_size_y_um * beam_size_z_um) / attenuation_factor
    kdose = 2000.0 / (wavelength * wavelength)
    dose_rate = flux_density / kdose
    if dose_rate == 0: return 0, 0
    rotisserie_factor, _ = _calculate_rotisserie_factor(
        crystal_lx_um, crystal_ly_um, crystal_lz_um,
        beam_size_y_um, beam_size_z_um, translation_z=translation_z_um,
    )
    lifetime_s = dose_limit_gy * rotisserie_factor / dose_rate
    return int(lifetime_s / exposure_time_s) if exposure_time_s > 0 else float('inf'), lifetime_s


def find_experimental_recommendations(
        crystal_dims, dose_limit_mgy, flux, desired_n_images,
        beam_sizes_to_search, wavelengths_to_search,
        attenuations_to_search, translations_to_search,
        min_allowable_exposure_time_s):
    crystal_lx_um, crystal_ly_um, crystal_lz_um = crystal_dims
    recommendations = []
    for beam_y, beam_z in beam_sizes_to_search:
        current_translations = translations_to_search.get(f"{beam_y}x{beam_z}", [])
        for translation_z in current_translations:
            for wavelength in wavelengths_to_search:
                for attenuation in attenuations_to_search:
                    _, lifetime_s = calculate_crystal_lifetime(
                        flux=flux, wavelength=wavelength, dose_limit_mgy=dose_limit_mgy,
                        crystal_lx_um=crystal_lx_um, crystal_ly_um=crystal_ly_um,
                        crystal_lz_um=crystal_lz_um, beam_size_y_um=beam_y,
                        beam_size_z_um=beam_z, attenuation_factor=attenuation,
                        translation_z_um=translation_z,
                    )
                    if desired_n_images <= 0: continue
                    required_exposure_time = lifetime_s / desired_n_images
                    if required_exposure_time >= min_allowable_exposure_time_s:
                        mismatch_score = abs(beam_y - crystal_ly_um) + abs(beam_z - crystal_lz_um)
                        recommendations.append({
                            "beam_size_um": (beam_y, beam_z), "wavelength_a": wavelength,
                            "attenuation_factor": attenuation, "translation_z_um": round(translation_z, 2),
                            "required_exposure_time_s": round(required_exposure_time, 4),
                            "total_collection_time_s": round(lifetime_s, 2),
                            "mismatch_score": round(mismatch_score, 1),
                        })
    return sorted(recommendations, key=lambda x: (x['mismatch_score'], -x['required_exposure_time_s']))


# --- Flask Server Setup ---
app = Flask(__name__)


@app.route('/calculate', methods=['POST'])
def handle_calculation():
    """
    API endpoint to receive experiment parameters and return recommendations.
    """
    print("Received a request...")
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400

        # Extract parameters from the JSON payload
        recommendations = find_experimental_recommendations(
            crystal_dims=tuple(data['crystal_dims']),
            dose_limit_mgy=data['dose_limit_mgy'],
            flux=data['flux'],
            desired_n_images=data['desired_n_images'],
            beam_sizes_to_search=data['beam_sizes_to_search'],
            wavelengths_to_search=data['wavelengths_to_search'],
            attenuations_to_search=data['attenuations_to_search'],
            translations_to_search=data['translations_to_search'],
            min_allowable_exposure_time_s=data['min_allowable_exposure_time_s']
        )
        print(f"Calculation successful. Found {len(recommendations)} solutions.")
        return jsonify(recommendations)
    except KeyError as e:
        print(f"Error: Missing key in request: {e}")
        return jsonify({"error": f"Missing parameter in request: {e}"}), 400
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return jsonify({"error": f"An internal server error occurred: {e}"}), 500


if __name__ == '__main__':
    print("Starting Crystal Lifetime Calculation Server...")
    # host='0.0.0.0' makes the server accessible from other machines on the network
    app.run(host='0.0.0.0', port=5000, debug=True)
