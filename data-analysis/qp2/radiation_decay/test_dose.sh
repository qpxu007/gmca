curl -X POST -H "Content-Type: application/json" -d '{
    "flux": 1e12,
    "crystal_dims": [20, 20, 20]
}' ${DOSE_PLANNER_URL:-http://localhost:5000}/calculate_dose
