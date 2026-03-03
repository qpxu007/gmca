Of course. Here are several `curl` test cases designed to query your FastAPI HTTP interface.

These examples cover the "happy path" for each endpoint, as well as tests for the default parameter system and input validation.

You can run these commands from the terminal of any machine that has network access to your server (e.g., another machine on the `10.20.x.x` network, or the server itself using `localhost`).

---

### Prerequisites

1.  **Start your server:** Make sure the `dose_planner.service` is running.
2.  **Replace IP Address:** In the commands below, replace `YOUR_SERVER_IP` with the actual `10.20.x.x` IP address your service is running on. If you're running the commands on the same machine as the server, you can use `localhost`.

---

### Test Case 1: Managing Defaults (`/defaults` endpoint)

#### A) Check the Initial Defaults

This test verifies that the server is running and shows the default parameters it started with.

**Command:**
```bash
curl http://YOUR_SERVER_IP:5000/defaults | python -m json.tool
```
*(Piping to `| python -m json.tool` pretty-prints the JSON output, making it easier to read.)*

**Expected Output:**
A JSON object containing all the hardcoded default parameters from your `server.py` file.

```json
{
    "flux": 5000000000000.0,
    "dose_limit_mgy": 30.0,
    "crystal_dims": [
        50.0,
        50.0,
        50.0
    ],
    "nimages": 1800,
    "beam_size_um": [
        20.0,
        20.0
    ],
    "wavelength_a": 1.0,
    "...": "..."
}
```

#### B) Update a Few Defaults

This test updates the persistent defaults on the server.

**Command:**
```bash
curl -X POST -H "Content-Type: application/json" -d '{
    "dose_limit_mgy": 15.5,
    "beam_size_um": [10, 10],
    "shape": "Spherical"
}' http://YOUR_SERVER_IP:5000/defaults | python -m json.tool
```

**Expected Output:**
A success message along with the *new* full set of defaults, reflecting your changes.
```json
{
    "message": "Defaults updated successfully.",
    "new_defaults": {
        "flux": 5000000000000.0,
        "dose_limit_mgy": 15.5,
        "crystal_dims": [
            50.0,
            50.0,
            50.0
        ],
        "beam_size_um": [
            10.0,
            10.0
        ],
        "shape": "Spherical",
        "...": "..."
    }
}
```
*You can run the `GET` command from step **A)** again to confirm the change is persistent.*

#### C) Reset All Defaults

This test resets the server back to its original, hardcoded defaults.

**Command:**
```bash
curl -X DELETE http://YOUR_SERVER_IP:5000/defaults | python -m json.tool
```

**Expected Output:**
```json
{
    "message": "Defaults have been reset to their original values."
}
```
*Run the `GET` command from step **A)** one more time to see that the `dose_limit_mgy` is back to `30.0`.*

---

### Test Case 2: On-the-Fly Dose Calculation (`/calculate_dose` endpoint)

#### A) Minimal Input (Testing the Default System)

This is a common use case. Provide only the essential parameters and let the server fill in the rest from its active defaults.

**Command:**
```bash
curl -X POST -H "Content-Type: application/json" -d '{
    "flux": 1e12,
    "crystal_dims": [25, 30, 40]
}' http://YOUR_SERVER_IP:5000/calculate_dose | python -m json.tool
```

**Expected Output:**
A successful calculation. The `warnings` array will be long, listing all the parameters that were not provided and were therefore defaulted. The calculation will use the server's current defaults (e.g., `dose_limit_mgy: 30.0`).
```json
{
    "lifetime_results": {
        "dose_rate_mgy_s": 0.0025,
        "rotisserie_factor": 1.0,
        "estimated_total_dose_mgy": 450.0,
        "estimated_effective_dose_mgy": 450.0,
        "crystal_lifetime_s": 12000.0,
        "max_images_at_dose_limit": 120000
    },
    "raddose3d_results": null,
    "warnings": [
        "Parameter 'dose_limit_mgy' not provided. Using active default: 30.0",
        "Parameter 'nimages' not provided. Using active default: 1800",
        "..."
    ]
}
```

#### B) Full Input with RADDOSE-3D

This test provides all necessary parameters and enables the more complex RADDOSE-3D calculation.

**Command:**
```bash
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
}' http://YOUR_SERVER_IP:5000/calculate_dose | python -m json.tool
```

**Expected Output:**
A successful calculation with results in *both* `lifetime_results` and `raddose3d_results`. The `warnings` array should be empty.
```json
{
    "lifetime_results": {
        "dose_rate_mgy_s": 0.0234,
        "...": "..."
    },
    "raddose3d_results": {
        "Avg DWD": 20.1234,
        "Max Dose": 28.5678,
        "Last DWD": 24.9876
    },
    "warnings": []
}
```
*(Note: RADDOSE-3D results are simulated here; your actual values will vary.)*

#### C) Invalid Input (Testing Validation)

This test sends incorrectly typed or constrained data to see how the server handles it.

**Command:**
```bash
curl -X POST -H "Content-Type: application/json" -d '{
    "flux": "fast",
    "crystal_dims": [50, 50],
    "attenuation_factor": 0.5
}' http://YOUR_SERVER_IP:5000/calculate_dose | python -m json.tool
```

**Expected Output:**
An HTTP `422 Unprocessable Entity` error. The JSON body will detail exactly what was wrong with the request.
```json
{
    "detail": [
        {
            "loc": [
                "body",
                "flux"
            ],
            "msg": "value is not a valid float",
            "type": "type_error.float"
        },
        {
            "loc": [
                "body",
                "crystal_dims"
            ],
            "msg": "value is not a valid tuple",
            "type": "type_error.tuple"
        },
        {
            "loc": [
                "body",
                "attenuation_factor"
            ],
            "msg": "ensure this value is greater than or equal to 1",
            "type": "value_error.number.not_ge",
            "ctx": {
                "limit": 1
            }
        }
    ]
}
```

---

### Test Case 3: Finding Recommendations (`/recommendations` endpoint)

This tests the parameter search functionality.

**Command:**
```bash
curl -X POST -H "Content-Type: application/json" -d '{
    "crystal_dims": [50, 50, 50],
    "dose_limit_mgy": 30.0,
    "flux": 5e12,
    "desired_n_images": 1800,
    "beam_sizes_to_search": [[20, 20], [10, 10]],
    "wavelengths_to_search": [1.0],
    "attenuations_to_search": [1, 10, 100],
    "translations_to_search": {
        "20x20": [0, 10, 20],
        "10x10": [0, 15, 30]
    },
    "exposure_times_to_search": [0.1, 0.05],
    "max_recommendations": 5
}' http://YOUR_SERVER_IP:5000/recommendations | python -m json.tool
```

**Expected Output:**
A JSON array of recommendation objects (or an empty array `[]` if no valid strategies are found).
```json
[
    {
        "beam_size_um": [
            10.0,
            10.0
        ],
        "wavelength_a": 1.0,
        "attenuation_factor": 10.0,
        "translation_z_um": 30.0,
        "exposure_time_s": 0.05,
        "effective_dose_mgy": 12.5,
        "mismatch_score": 80.0
    },
    {
        "...": "..."
    }
]
```

