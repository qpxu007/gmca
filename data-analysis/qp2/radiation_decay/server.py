import copy
import json
import logging
import os
from typing import Optional, List, Tuple, Dict, Any

# --- Third-party imports ---
import uvicorn
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from qp2.log.logging_config import get_logger, setup_logging
# --- Local application imports ---
from qp2.radiation_decay.calculations import (
    find_experimental_recommendations,
    calculate_lifetime_and_rate,
    _calculate_rotisserie_factor,
    _setup_raddose3d_input,
)
from qp2.radiation_decay.data_source import FluxManager
from qp2.radiation_decay.raddose3d import run_raddose3d

# --- Global Configuration and State ---
CUSTOM_DEFAULTS_FILE = "custom_defaults.json"
logger = get_logger(__name__)

HARDCODED_DEFAULT_PARAMETERS = {
    "flux": 5e12,
    "dose_limit_mgy": 30.0,
    "crystal_dims": (50, 50, 50),
    "nimages": 1800,
    "beam_size_um": (20, 20),
    "wavelength_a": 1.0,
    "attenuation_factor": 1.0,
    "translation_z_um": 0.0,
    "exposure_time_s": 0.1,
    "enable_raddose3d": False,
    "osc": 0.1,
    "cell": "78 78 39 90 90 90",
    "nres": 129,
    "nmon": 8,
    "shape": "Cuboid",
    "coef_calc": "AVERAGE",
    "angle_l": 0.0,
    "angle_p": 0.0,
    "ndna": 0,
    "nrna": 0,
    "ncarb": 0,
    "solvent_fraction": 0.5,
    "protein_heavy_atoms": "",
    "solvent_heavy_conc": "",
}


# --- Pydantic Models for Request and Response Validation ---


# Model for on-the-fly dose calculation
class DoseCalculationInput(BaseModel):
    flux: Optional[float] = Field(None, gt=0, description="X-ray flux in photons/s.")
    dose_limit_mgy: Optional[float] = Field(
        None, gt=0, description="Maximum allowed dose in MGy."
    )
    crystal_dims: Optional[Tuple[float, float, float]] = Field(
        None, description="Crystal dimensions (lx, ly, lz) in microns."
    )
    nimages: Optional[int] = Field(
        None, gt=0, description="Desired number of images to collect."
    )
    beam_size_um: Optional[Tuple[float, float]] = Field(
        None, description="Beam size (y, z) in microns."
    )
    wavelength_a: Optional[float] = Field(
        None, gt=0, description="Wavelength in Ångstroms."
    )
    attenuation_factor: Optional[float] = Field(
        None, ge=1, description="Attenuation factor (>= 1)."
    )
    translation_z_um: Optional[float] = Field(
        None, ge=0, description="Translation along gonio axis (Z) in microns."
    )
    exposure_time_s: Optional[float] = Field(
        None, gt=0, description="Exposure time per image in seconds."
    )

    enable_raddose3d: Optional[bool] = Field(
        None, description="If true, also run RADDOSE-3D."
    )
    osc: Optional[float] = Field(
        None, gt=0, description="Oscillation per image in degrees (for R3D)."
    )
    cell: Optional[str] = Field(
        None, description="Unit cell string (a b c alpha beta gamma) (for R3D)."
    )
    nres: Optional[int] = Field(
        None, ge=0, description="Number of residues per monomer (for R3D)."
    )
    nmon: Optional[int] = Field(
        None, ge=0, description="Number of monomers per unit cell (for R3D)."
    )
    shape: Optional[str] = Field(
        None, description='Crystal shape ("Cuboid", "Spherical", "Cylinder") (for R3D).'
    )
    coef_calc: Optional[str] = Field(
        None,
        description='Absorption coefficient calculation method ("AVERAGE", "RD3D") (for R3D).',
    )
    angle_l: Optional[float] = Field(None, description="Angle L in degrees (for R3D).")
    angle_p: Optional[float] = Field(None, description="Angle P in degrees (for R3D).")
    ndna: Optional[int] = Field(None, ge=0)
    nrna: Optional[int] = Field(None, ge=0)
    ncarb: Optional[int] = Field(None, ge=0)
    solvent_fraction: Optional[float] = Field(None, ge=0, le=1)
    protein_heavy_atoms: Optional[str] = Field(None)
    solvent_heavy_conc: Optional[str] = Field(None)


# Model for updating defaults
class DefaultsUpdateInput(DoseCalculationInput):  # Inherits all fields as optional
    pass


# Model for the recommendation search endpoint
class RecommendationInput(BaseModel):
    crystal_dims: Tuple[float, float, float] = Field(
        ..., description="Crystal dimensions (lx, ly, lz) in microns."
    )
    dose_limit_mgy: float = Field(..., gt=0, description="Maximum allowed dose in MGy.")
    flux: float = Field(..., gt=0, description="X-ray flux in photons/s.")

    # MODIFICATION: Changed from single int to a list of ints to search over
    desired_n_images_to_search: List[int] = Field(
        ..., min_items=1, description="List of desired image counts to search through."
    )

    beam_sizes_to_search: List[Tuple[float, float]] = Field(
        ...,
        min_items=1,
        description="List of beam sizes to search, e.g., [[10, 10], [20, 20]].",
    )
    wavelengths_to_search: List[float] = Field(
        ..., min_items=1, description="List of wavelengths to search."
    )
    attenuations_to_search: List[float] = Field(
        ..., min_items=1, description="List of attenuation factors to search."
    )
    translations_to_search: Dict[str, List[float]] = Field(
        ...,
        description='Dictionary mapping beam size strings ("YxZ") to a list of Z translations to search.',
    )
    exposure_times_to_search: List[float] = Field(
        ..., min_items=1, description="List of exposure times to search."
    )
    max_recommendations: int = Field(
        10, gt=0, description="Maximum number of recommendations to return."
    )


# Response Models
class LifetimeResults(BaseModel):
    dose_rate_mgy_s: float
    rotisserie_factor: float
    estimated_total_dose_mgy: float
    estimated_effective_dose_mgy: float
    crystal_lifetime_s: float
    max_images_at_dose_limit: int


class DoseCalculationResponse(BaseModel):
    lifetime_results: LifetimeResults
    raddose3d_results: Optional[Dict[str, Any]] = None
    warnings: List[str]


class MessageResponse(BaseModel):
    message: str


# --- State Management Functions ---
def load_defaults(logger_instance):
    defaults = copy.deepcopy(HARDCODED_DEFAULT_PARAMETERS)
    if os.path.exists(CUSTOM_DEFAULTS_FILE):
        try:
            with open(CUSTOM_DEFAULTS_FILE, "r") as f:
                custom_defaults = json.load(f)
            defaults.update(custom_defaults)
            logger_instance.info(f"Loaded custom defaults from {CUSTOM_DEFAULTS_FILE}")
        except (json.JSONDecodeError, IOError) as e:
            logger_instance.error(
                f"Could not load {CUSTOM_DEFAULTS_FILE}: {e}. Using hardcoded defaults."
            )
    else:
        logger_instance.info("No custom defaults file found. Using hardcoded defaults.")
    return defaults


def save_defaults(defaults_to_save, logger_instance):
    try:
        with open(CUSTOM_DEFAULTS_FILE, "w") as f:
            json.dump(defaults_to_save, f, indent=4)
        logger_instance.info(f"Saved custom defaults to {CUSTOM_DEFAULTS_FILE}")
        return True, None
    except IOError as e:
        logger_instance.error(f"Failed to save custom defaults: {e}")
        return False, str(e)


# --- FastAPI Application Setup ---
setup_logging(root_name="qp2", log_level=logging.INFO, log_file="dose_server.log")
CURRENT_DEFAULTS = load_defaults(logger)

app = FastAPI(
    title="Dose Planner Calculation Server",
    description="An API for radiation dose calculations, experiment planning, and managing default parameters.",
    version="1.2.0",  # Bumped version for new feature
)


# --- API Endpoints ---


@app.get("/defaults", response_model=Dict[str, Any], tags=["Defaults"])
def get_current_defaults():
    """Returns the current set of active default parameters."""
    return CURRENT_DEFAULTS


@app.post("/defaults", tags=["Defaults"])
def update_defaults(updates: DefaultsUpdateInput):
    """
    Updates one or more default parameters. The provided JSON will be merged
    with the existing defaults and persisted for future requests.
    """
    global CURRENT_DEFAULTS
    update_data = updates.dict(exclude_unset=True)
    CURRENT_DEFAULTS.update(update_data)

    success, error_msg = save_defaults(CURRENT_DEFAULTS, logger)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save defaults to file: {error_msg}",
        )

    return {
        "message": "Defaults updated successfully.",
        "new_defaults": CURRENT_DEFAULTS,
    }


@app.delete("/defaults", response_model=MessageResponse, tags=["Defaults"])
def reset_defaults():
    """Resets all parameters back to their original hardcoded default values."""
    global CURRENT_DEFAULTS
    CURRENT_DEFAULTS = copy.deepcopy(HARDCODED_DEFAULT_PARAMETERS)
    if os.path.exists(CUSTOM_DEFAULTS_FILE):
        try:
            os.remove(CUSTOM_DEFAULTS_FILE)
            logger.info("Custom defaults file removed.")
        except OSError as e:
            logger.error(f"Error removing defaults file: {e}")
            raise HTTPException(
                status_code=500, detail=f"Could not remove defaults file: {e}"
            )

    return {"message": "Defaults have been reset to their original values."}


@app.post(
    "/calculate_dose", response_model=DoseCalculationResponse, tags=["Calculations"]
)
def handle_dose_calculation(user_input: DoseCalculationInput):
    """
    Performs an on-the-fly dose calculation for a single set of parameters.
    Any parameters not provided will be filled from the active server defaults.
    """
    warnings = []
    user_data = user_input.dict(exclude_unset=True)
    params = copy.deepcopy(CURRENT_DEFAULTS)
    params.update(user_data)

    for key in HARDCODED_DEFAULT_PARAMETERS:
        if key not in user_data:
            warnings.append(
                f"Parameter '{key}' not provided. Using active default: {params[key]}"
            )

    try:
        # 1. Lifetime Calculation
        lifetime_s, dose_rate_mgy_s = calculate_lifetime_and_rate(
            flux=params["flux"],
            wavelength=params["wavelength_a"],
            dose_limit_mgy=params["dose_limit_mgy"],
            crystal_lx_um=params["crystal_dims"][0],
            crystal_ly_um=params["crystal_dims"][1],
            crystal_lz_um=params["crystal_dims"][2],
            beam_size_x_um=params["beam_size_um"][0],
            beam_size_y_um=params["beam_size_um"][1],
            attenuation_factor=params["attenuation_factor"],
            translation_x_um=params["translation_z_um"],
        )
        rotisserie_factor, _ = _calculate_rotisserie_factor(
            *params["crystal_dims"], *params["beam_size_um"], params["translation_z_um"]
        )
        total_dose = dose_rate_mgy_s * params["nimages"] * params["exposure_time_s"]
        effective_dose = (
            total_dose / rotisserie_factor if rotisserie_factor > 0 else total_dose
        )
        max_images = (
            lifetime_s / params["exposure_time_s"]
            if params["exposure_time_s"] > 0
            else 0
        )

        lifetime_results = LifetimeResults(
            dose_rate_mgy_s=round(dose_rate_mgy_s, 4),
            rotisserie_factor=round(rotisserie_factor, 2),
            estimated_total_dose_mgy=round(total_dose, 2),
            estimated_effective_dose_mgy=round(effective_dose, 2),
            crystal_lifetime_s=round(lifetime_s, 2),
            max_images_at_dose_limit=int(max_images),
        )

        # 2. Raddose3D Calculation
        r3d_summary = None
        if params["enable_raddose3d"]:
            logger.info("Running RADDOSE-3D simulation...")
            base_params = {
                k: params[k] for k in params if k not in ["enable_raddose3d"]
            }
            dynamic_params = {
                k: params[k]
                for k in [
                    "beam_size_um",
                    "translation_z_um",
                    "wavelength_a",
                    "attenuation_factor",
                    "exposure_time_s",
                    "n_images",
                ]
            }
            sample, beam, wedges = _setup_raddose3d_input(base_params, dynamic_params)
            _, r3d_summary = run_raddose3d(
                sample, beam, wedges, swap_xy=False, debug=False
            )
            logger.info(
                f"RADDOSE-3D calculation complete. Avg DWD: {r3d_summary.get('Avg DWD', 'N/A')} MGy"
            )

        return DoseCalculationResponse(
            lifetime_results=lifetime_results,
            raddose3d_results=r3d_summary,
            warnings=warnings,
        )
    except Exception as e:
        logger.error(f"Error during calculation: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"An unexpected server error occurred: {str(e)}"
        )


@app.post(
    "/recommendations", response_model=List[Dict[str, Any]], tags=["Calculations"]
)
def find_recommendations(search_input: RecommendationInput):
    """
    Searches a defined parameter space to find a list of experimental strategies
    that meet the desired criteria.
    """
    logger.info("Received recommendation search request")
    try:
        # MODIFICATION: Pass the new list-based parameter to the calculation function
        # Create a constant FluxManager from the single flux value.
        # This returns the same flux for all energies in the search space.
        from collections import OrderedDict
        flux_manager = FluxManager(OrderedDict({12.0: search_input.flux}))

        recommendations = find_experimental_recommendations(
            crystal_dims=search_input.crystal_dims,
            dose_limit_mgy=search_input.dose_limit_mgy,
            flux_manager=flux_manager,
            desired_n_images_to_search=search_input.desired_n_images_to_search,
            beam_sizes_to_search=search_input.beam_sizes_to_search,
            wavelengths_to_search=search_input.wavelengths_to_search,
            attenuations_to_search=search_input.attenuations_to_search,
            translations_to_search=search_input.translations_to_search,
            exposure_times_to_search=search_input.exposure_times_to_search,
        )
        recommendations = recommendations[:search_input.max_recommendations]
        logger.info(f"Returning {len(recommendations)} recommendations")
        return recommendations
    except Exception as e:
        logger.error(f"Error processing recommendation request: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"An unexpected server error occurred: {str(e)}"
        )


@app.get("/health", tags=["Utilities"])
def health_check():
    """Returns the health status of the server."""
    return {"status": "healthy", "service": "dose_planner_server"}


if __name__ == "__main__":
    from qp2.config.servers import ServerConfig
    logger.info("Starting DosePlanner Calculation Server with FastAPI/Uvicorn...")
    uvicorn.run("server:app", host="0.0.0.0", port=ServerConfig.DOSE_PLANNER_PORT, reload=True)
