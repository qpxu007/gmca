
curl -X POST -H "Content-Type: application/json" -d '{
    "flux": 5e12,
    "dose_limit_mgy": 25.0,
    "crystal_dims": [50, 60, 70],
    "nimages": 900,
    "beam_size_um": [15, 15],
    "wavelength_a": 0.97,
    "attenuation_factor": 1.0,
    "translation_z_um": 50.0,
    "exposure_time_s": 0.1,
    "enable_raddose3d": true,
    "osc": 0.2,
    "cell": "80 80 40 90 90 90",
    "coef_calc": "RD3D"
}' ${DOSE_PLANNER_URL:-http://localhost:5000}/calculate_dose | python -m json.tool
