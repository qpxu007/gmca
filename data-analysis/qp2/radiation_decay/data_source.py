from collections import OrderedDict

import numpy as np
import redis

from qp2.log.logging_config import get_logger
from qp2.utils.matthews_coef import run_matthews_coef
from qp2.xio.redis_manager import get_redis_server

logger = get_logger(__name__)


def estimate_cell_content(symm, cell):
    logger.debug(f"Estimating cell content for symmetry: {symm}, cell: {cell}")
    try:
        import gemmi

        nops = len(gemmi.SpaceGroup(symm).operations())
        logger.debug(f"Found {nops} symmetry operations for space group {symm}")
    except:
        logger.warning("Failed to import gemmi, using nops=1")
        nops = 1

    try:
        out = run_matthews_coef(symm, cell, molsize=None)
        logger.debug(f"Matthews coefficient estimation: {out}")
        nasu = out.get("nmol")
        nres = out.get("nres")
        solvent_percent = round(out.get("solvent", 0.50), 2)
        logger.info(
            f"Estimated cell content: nres={nres}, nmon={nasu * nops}, solvent={solvent_percent}%"
        )

    except Exception as e:
        logger.error(f"Failed to estimate cell content: {e}")
        nasu, nres, solvent_percent = 1, 100, 0.50
    return {"nres": nres, "nmon": nasu * nops, "solvent": solvent_percent}


def get_latest_strategy_with_unit_cell_(key_pattern="*strategy*"):
    logger.debug(f"Searching for latest strategy with pattern: {key_pattern}")
    server, port = get_redis_server().split(":")
    logger.debug(f"Connecting to Redis server: {server}:{port}")

    try:
        r = redis.Redis(host=server, port=int(port), decode_responses=True)
        r.ping()
        logger.info("Successfully connected to Redis server")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Could not connect to Redis: {e}")
        return None, -1, None

    largest_pipelinestatus_id = -1
    key_with_largest_pipelinestatus_id = None
    latest_hash_data = None

    try:
        for key in r.scan_iter(match=key_pattern):
            if key == "bluice:sample:strategy_ver__s":
                continue
            try:
                hash_data = r.hgetall(key)
                # Check for both 'pipelinestatus_id' and 'unit_cell'
                if (
                        hash_data
                        and "pipelinestatus_id" in hash_data
                        and "unitcell" in hash_data
                ):
                    try:
                        pipelinestatus_id = int(hash_data["pipelinestatus_id"])
                        if pipelinestatus_id > largest_pipelinestatus_id:
                            largest_pipelinestatus_id = pipelinestatus_id
                            key_with_largest_pipelinestatus_id = key
                            latest_hash_data = hash_data
                    except (ValueError, TypeError):
                        logger.error(
                            f"Warning: Found non-integer 'pipelinestatus_id' in key '{key}'. Skipping."
                        )
                        continue
            except redis.exceptions.ResponseError:
                logger.warning(f"Warning: Key '{key}' does not hold a hash. Skipping.")
                continue
    except redis.exceptions.RedisError as e:
        logger.error(f"An error occurred while communicating with Redis: {e}")
        return None, -1, None

    """('bluice:strategy:table#A8/screen:CC_I23_1_run0', 58298, {'unitcell': '45.6 44.9 168.9 89.7 88.0 59.9', 'laue': 'P1', 'osc_start': '197.0', 'osc_end': '377.0', 'mosaicity': '1.6', 'rmsd': '0.23', 'score': '0.172', 'completeness': '94.4', 'acompleteness': '62.4', 'software': 'mosflm_strategy', 'state': 'SPOT', 'images': 'CC_I23_1_run0_000001.cbf;CC_I23_1_run0_000901.cbf', 'n_spots': '272', 'n_spots_ice': '40', 'n_ice_rings': 'NA', 'resolution_from_spots': '2.75', 'avg_spotsize': '3x3', 'pipelinestatus_id': '58298', 'osc_delta': '0.2', 'distance': '250'})"""

    if latest_hash_data is None:
        logger.warning("No strategy results found in Redis")
        return None, -1, None

    strategy = {}
    strategy["sample"] = str(key_with_largest_pipelinestatus_id).split("#")[-1]
    strategy["cell"] = latest_hash_data.get("unitcell", None)
    strategy["symm"] = latest_hash_data.get("laue", None)
    logger.debug(
        f"Found strategy with cell: {strategy['cell']}, symmetry: {strategy['symm']}"
    )

    if strategy["cell"] and strategy["symm"]:
        params = estimate_cell_content(strategy["symm"], strategy["cell"])
        strategy.update(params)
        logger.info(f"Retrieved strategy with pipeline ID {largest_pipelinestatus_id}")

    strategy["distance"] = latest_hash_data.get("distance", None)
    strategy["osc_start"] = float(latest_hash_data.get("osc_start", 0))
    strategy["osc_end"] = float(latest_hash_data.get("osc_end", 180))
    strategy["osc_delta"] = float(latest_hash_data.get("osc_delta", 0.2))

    return (
        key_with_largest_pipelinestatus_id,
        largest_pipelinestatus_id,
        strategy,
    )


def get_latest_strategy_with_unit_cell(key_pattern="*strategy*"):
    logger.debug(f"Searching for latest strategy with pattern: {key_pattern}")
    server, port = get_redis_server().split(":")
    logger.debug(f"Connecting to Redis server: {server}:{port}")

    try:
        r = redis.Redis(host=server, port=int(port), decode_responses=True)
        r.ping()
        logger.info("Successfully connected to Redis server")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Could not connect to Redis: {e}")
        return None, -1, None

    largest_pipelinestatus_id = -1
    key_with_largest_pipelinestatus_id = None
    latest_hash_data = None

    try:
        # Step 1: Get all matching keys first. This is still iterative but fast.
        keys = [key for key in r.scan_iter(match=key_pattern) if key != "bluice:sample:strategy_ver__s"]

        if not keys:
            logger.warning("No strategy keys found in Redis.")
            return None, -1, None

        # Step 2: Create a pipeline to fetch all hashes in one network round trip.
        pipe = r.pipeline()
        for key in keys:
            pipe.hgetall(key)
        all_hashes = pipe.execute()  # This sends all commands and gets all results

        # Step 3: Process the results in memory (which is extremely fast).
        for key, hash_data in zip(keys, all_hashes):
            if not isinstance(hash_data, dict):
                logger.warning(f"Warning: Key '{key}' does not hold a hash. Skipping.")
                continue

            if "pipelinestatus_id" in hash_data and "unitcell" in hash_data:
                try:
                    pipelinestatus_id = int(hash_data["pipelinestatus_id"])
                    if pipelinestatus_id > largest_pipelinestatus_id:
                        largest_pipelinestatus_id = pipelinestatus_id
                        key_with_largest_pipelinestatus_id = key
                        latest_hash_data = hash_data
                except (ValueError, TypeError):
                    logger.error(
                        f"Warning: Found non-integer 'pipelinestatus_id' in key '{key}'. Skipping."
                    )
                    continue

    except redis.exceptions.RedisError as e:
        logger.error(f"An error occurred while communicating with Redis: {e}")
        return None, -1, None

    """('bluice:strategy:table#A8/screen:CC_I23_1_run0', 58298, {'unitcell': '45.6 44.9 168.9 89.7 88.0 59.9', 'laue': 'P1', 'osc_start': '197.0', 'osc_end': '377.0', 'mosaicity': '1.6', 'rmsd': '0.23', 'score': '0.172', 'completeness': '94.4', 'acompleteness': '62.4', 'software': 'mosflm_strategy', 'state': 'SPOT', 'images': 'CC_I23_1_run0_000001.cbf;CC_I23_1_run0_000901.cbf', 'n_spots': '272', 'n_spots_ice': '40', 'n_ice_rings': 'NA', 'resolution_from_spots': '2.75', 'avg_spotsize': '3x3', 'pipelinestatus_id': '58298', 'osc_delta': '0.2', 'distance': '250'})"""

    if latest_hash_data is None:
        logger.warning("No strategy results found in Redis")
        return None, -1, None

    strategy = {}
    strategy["sample"] = str(key_with_largest_pipelinestatus_id).split("#")[-1]
    strategy["cell"] = latest_hash_data.get("unitcell", None)
    strategy["symm"] = latest_hash_data.get("laue", None)
    logger.debug(
        f"Found strategy with cell: {strategy['cell']}, symmetry: {strategy['symm']}"
    )

    if strategy["cell"] and strategy["symm"]:
        params = estimate_cell_content(strategy["symm"], strategy["cell"])
        strategy.update(params)
        logger.info(f"Retrieved strategy with pipeline ID {largest_pipelinestatus_id}")

    strategy["distance"] = latest_hash_data.get("distance", None)
    strategy["osc_start"] = float(latest_hash_data.get("osc_start", 0))
    strategy["osc_end"] = float(latest_hash_data.get("osc_end", 180))
    strategy["osc_delta"] = float(latest_hash_data.get("osc_delta", 0.2))

    return (
        key_with_largest_pipelinestatus_id,
        largest_pipelinestatus_id,
        strategy,
    )


class FluxManager:
    """
    Manages and interpolates flux values based on energy.
    """

    def __init__(self, flux_data: OrderedDict):
        if not flux_data or not isinstance(flux_data, (dict, OrderedDict)):
            raise ValueError("Flux data must be a non-empty dictionary or OrderedDict.")

        # Store data and prepare numpy arrays for interpolation
        self._flux_data = OrderedDict(sorted(flux_data.items()))
        self._energies = np.array(list(self._flux_data.keys()))
        self._fluxes = np.array(list(self._flux_data.values()))

        logger.info(f"FluxManager initialized with {len(self._energies)} data points.")
        logger.debug(f"Energy points (KeV): {self._energies}")
        logger.debug(f"Flux points (ph/s): {self._fluxes}")

    def get_flux(self, energy_kev: float) -> float:
        """
        Calculates the flux for a given energy.
        - Interpolates for energies within the defined range.
        - Uses the boundary flux values for energies outside the range.
        """
        # np.interp handles interpolation and boundary conditions perfectly.
        # It clamps to the first value for x < xp[0] and the last for x > xp[-1].
        interpolated_flux = np.interp(energy_kev, self._energies, self._fluxes)
        return float(interpolated_flux)


class ExternalDataSource:
    def __init__(self):
        flux_vs_energy = OrderedDict(
            {7.0: 1e12, 9.0: 5e12, 12.0: 1e13, 14.0: 5e12, 20.0: 1e12}
        )
        self.flux_manager = FluxManager(flux_vs_energy)
        self._update_counter = 0

    def get_beamline_defaults(self):
        """
        Returns a dictionary of sensible, hardcoded beamline defaults.
        This is used for the initial state of the application.
        """
        logger.debug("Fetching beamline default parameters")
        default_energy = 12.0
        default_flux = self.flux_manager.get_flux(default_energy)
        data = {
            "flux": default_flux,
            "attenuation_factor": 100,
            "beam_size_um": (5, 5),  # A more standard default
            "wavelength_a": 12.3984 / default_energy,
            "cell": None,
            "nres": 129,
            "nmon": 8,
            "exposure_time_s": 0.1,
            "energy_keV": default_energy,
        }
        return data

    def get_latest_strategy(self):
        """
        Queries Redis for the latest strategy results.
        Returns a dictionary with strategy parameters if found, otherwise None.
        """
        logger.debug("Querying Redis for the latest strategy.")
        _, _, new_params = get_latest_strategy_with_unit_cell()

        if new_params and new_params.get("cell"):
            if new_params.get("solvent", 0) > 1:
                new_params["solvent_fraction"] = round(new_params["solvent"] / 100.0, 2)
            logger.info("Found new strategy in Redis.")
            return new_params
        else:
            logger.info("No new strategy found in Redis.")
            return None

    def get_latest_data(self):
        """
        Fetches the latest data. Simulates a data change after a few calls.
        """
        logger.debug("Fetching latest experimental data")
        default_energy = 12.0
        default_flux = self.flux_manager.get_flux(default_energy)
        data = {
            "flux": default_flux,
            "attenuation_factor": 100,
            "beam_size_um": (5, 5),
            "wavelength_a": 12.3984 / default_energy,
            "cell": None,
            "nres": 129,
            "nmon": 8,
            "osc_start": 0.0,
            "osc_end": 180.0,
            "osc_range": 0.2,
            "distance": 250.0,
            "exposure_time_s": 0.1,
            "nimages": 900,
            "energy1_keV": default_energy,
        }

        _, _, new_params = get_latest_strategy_with_unit_cell()
        if new_params:
            if new_params.get("solvent", 0) > 1:
                new_params["solvent_fraction"] = round(new_params["solvent"] / 100.0, 2)
            data.update(new_params)
            logger.debug(
                f"Updated data with strategy parameters: {list(new_params.keys())}"
            )
        else:
            logger.debug("Using default data parameters (no strategy found)")

        return data


if __name__ == "__main__":
    d = ExternalDataSource()
    n = d.get_latest_data()
    logger.info(n)
