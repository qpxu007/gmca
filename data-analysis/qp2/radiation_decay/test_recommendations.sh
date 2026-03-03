
curl -X POST -H "Content-Type: application/json" -d '{
    "crystal_dims": [50, 50, 50],
    "dose_limit_mgy": 30.0,
    "flux": 5e12,
    "desired_n_images": 1800,
    "beam_sizes_to_search": [[20, 20], [10, 10]],
    "wavelengths_to_search": [1.0, 0.9],
    "attenuations_to_search": [1, 10, 100],
    "translations_to_search": {
        "20x20": [0, 10, 20],
        "10x10": [0, 15, 30]
    },
    "exposure_times_to_search": [0.1, 0.05],
    "max_recommendations": 5
}' ${DOSE_PLANNER_URL:-http://localhost:5000}/recommendations
