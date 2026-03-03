import json
import os
import re
from functools import reduce
from typing import Tuple, Optional, Dict, Union, List

import gemmi
import numpy as np
import requests

from qp2.pipelines.gmcaproc.symmetry import Symmetry
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class RCSB:
    """A class to handle protein structure data retrieval, processing, and analysis."""

    def __init__(self, default_directory: str = os.getcwd()):
        """Initialize with a default working directory."""
        self.default_directory = default_directory
        if not os.path.exists(default_directory):
            os.makedirs(default_directory)
        self._rcsb_search_api = None

    def _get_rcsb_search_api(self):
        """Lazy-loads and returns the rcsbapi search modules."""
        if self._rcsb_search_api is None:
            try:
                # This is where the internet connection is required.
                from rcsbapi.search import AttributeQuery, SeqSimilarityQuery

                self._rcsb_search_api = (AttributeQuery, SeqSimilarityQuery)
            except Exception as e:
                logger.error(
                    f"Could not import rcsbapi.search. Internet connection may be required. Error: {e}",
                    exc_info=True,
                )
                # Raise an exception that can be caught by the calling methods.
                raise ConnectionError(
                    "Failed to initialize RCSB search API. Check internet connection."
                ) from e
        return self._rcsb_search_api

    def download_af_model(self, pdb_id: str, directory: str = None) -> Optional[str]:
        """Download AlphaFold models from EBI."""
        directory = directory or self.default_directory
        ebi_id = f"AF-{pdb_id[5:-2]}-F1-model_v4.cif"
        url = f"https://alphafold.ebi.ac.uk/files/{ebi_id}"
        cif_filename = os.path.join(directory, ebi_id)
        pdb_filename = cif_filename.replace(".cif", ".pdb")

        try:
            r = requests.get(url)
            if r.status_code == 200:
                with open(cif_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=128):
                        f.write(chunk)
                logger.info(f"write {cif_filename}")
                structure = gemmi.read_structure(cif_filename)
                structure.write_pdb(pdb_filename)
                return pdb_filename
        except Exception as err:
            logger.warning(f"Failed to download {pdb_id}: {err}")
            return None

    def clean_up(self, pdb_file: str, remove_alt_confs: bool = True) -> str:
        """Clean up PDB file by removing ligands, waters, and empty chains."""
        structure = gemmi.read_structure(pdb_file)
        structure.remove_ligands_and_waters()
        structure.remove_empty_chains()
        if remove_alt_confs:
            structure.remove_alternative_conformations()
        os.rename(pdb_file, f"{pdb_file}_bak")
        structure.write_pdb(pdb_file)
        return pdb_file

    def download(
        self,
        pdb_id: str,
        directory: str = os.getcwd(),
        file_type: str = "pdb",
        overwrite: bool = False,
        cleanup: bool = True,
        remove_alt_confs: bool = True,
    ) -> Optional[str]:
        """Download PDB or CIF files from RCSB or AlphaFold."""
        directory = directory or self.default_directory
        if file_type not in ("pdb", "cif"):
            logger.info("Can only download pdb or cif files.")
            return None

        filename = os.path.join(directory, f"{pdb_id}.{file_type}")
        if os.path.exists(filename) and not overwrite:
            logger.info(f"Skipping file already existed {filename}")
            return filename

        if len(pdb_id) == 4:  # RCSB PDB
            url = f"https://files.rcsb.org/download/{pdb_id}.{file_type}"
            try:
                r = requests.get(url)
                if r.status_code == 200:
                    with open(filename, "wb") as f:
                        for chunk in r.iter_content(chunk_size=128):
                            f.write(chunk)
                    logger.info(f"write {filename}")
                elif r.status_code == 404:
                    logger.warning(
                        f"ERR: failed to download {pdb_id} with {url}, trying an alternative"
                    )
                    return None
            except Exception as err:
                logger.warning(f"Failed to download {pdb_id}: {err}")
                return None
        elif pdb_id.startswith("AF_"):  # AlphaFold
            filename = self.download_af_model(pdb_id, directory)

        if cleanup and filename:
            self.clean_up(filename, remove_alt_confs=remove_alt_confs)
        return filename

    def mtz_search(
        self, mtzfile: str, edge_err: float = 0.01, angle_err: float = 0.01
    ) -> Tuple[List, Tuple, str]:
        """Search for unit cell matches in MTZ file with permutations."""

        def swap_cell(cell):
            return cell[1:3] + cell[:1] + cell[3:] + cell[4:5]

        mtz = gemmi.read_mtz_file(mtzfile)
        cell = mtz.cell.parameters
        spgn = mtz.spacegroup.number
        logger.info(f"cell={cell}, symm={spgn}")

        result = self.unitcell_search(cell, edge_err, angle_err)
        reindex = "h, k, l"

        if not result and spgn in [16, 17, 18]:
            cell = swap_cell(cell)
            result = self.unitcell_search(cell, edge_err, angle_err)
            reindex = "k,l,h"
            if not result:
                cell = swap_cell(cell)
                result = self.unitcell_search(cell, edge_err, angle_err)
                reindex = "l,h,k"

        return result, cell, reindex

    def unitcell_search(
        self,
        cell: Tuple[float, ...],
        edge_err: float = 0.02,
        angle_err: float = 0.01,
        metadata: str = "compact",
    ) -> List:
        """Search RCSB for structures matching unit cell parameters."""

        try:
            AttributeQuery, _ = self._get_rcsb_search_api()
        except ConnectionError as e:
            logger.warning(f"Skipping unit cell search: {e}")
            return []  # Return an empty list on failure

        attrs = [
            "cell.length_a",
            "cell.length_b",
            "cell.length_c",
            "cell.angle_alpha",
            "cell.angle_beta",
            "cell.angle_gamma",
        ]
        errs = [edge_err] * 3 + [angle_err] * 3
        ranges = [{"from": c * (1 - e), "to": c * (1 + e)} for c, e in zip(cell, errs)]
        queries = [
            AttributeQuery(attribute=a, operator="range", value=v)
            for a, v in zip(attrs, ranges)
        ]
        q = reduce(lambda x, y: x & y, queries)
        return list(q(results_verbosity=metadata))

    def sequence_search(
        self,
        sequence: str,
        evalue_cutoff: float = 1.0e-5,
        identity_cutoff: float = 0.5,
        metadata: str = "compact",
    ) -> List:
        """Search RCSB for structures matching a protein sequence."""
        try:
            _, SeqSimilarityQuery = self._get_rcsb_search_api()
        except ConnectionError as e:
            logger.warning(f"Skipping unit cell search: {e}")
            return []  # Return an empty list on failure

        results = SeqSimilarityQuery(sequence, evalue_cutoff, identity_cutoff)(
            results_verbosity=metadata
        )
        return list(results)

    def get_space_group_for_pdbid(
        self, pdb_code: str
    ) -> Dict[str, Union[str, int, List[float], None]]:
        """Retrieve space group information for a PDB ID."""

        def _is_valid_pdb_code(pdb_code: str) -> bool:
            return bool(pdb_code and len(pdb_code) == 4 and pdb_code.isalnum())

        pdb_code = pdb_code.strip().upper()
        if not _is_valid_pdb_code(pdb_code):
            raise ValueError("PDB code must be a 4-character alphanumeric string")

        url = f"https://data.rcsb.org/rest/v1/core/entry/{pdb_code}"
        logger.debug("Fetching metadata for PDB code '%s'", pdb_code)

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            symmetry = data.get("symmetry", None)
            if not symmetry:
                raise KeyError("Space group name not found in response")

            cell_data = data.get("cell", {})
            unit_cell = [
                cell_data.get("length_a"),
                cell_data.get("length_b"),
                cell_data.get("length_c"),
                cell_data.get("angle_alpha"),
                cell_data.get("angle_beta"),
                cell_data.get("angle_gamma"),
            ]

            result = {
                "rcsb_id": pdb_code,
                "rcsb_hm_name": symmetry.get("space_group_name_hm"),
                "rcsb_space_group_number": symmetry.get("int_tables_number"),
                "rcsb_unit_cell": unit_cell,
            }
            logger.debug(
                "Space group for '%s': %s",
                pdb_code,
                symmetry.get("space_group_name_hm"),
            )
            return result
        except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
            logger.error("Failed to process data for '%s': %s", pdb_code, str(e))
            raise RuntimeError(
                f"Could not retrieve space group for {pdb_code}: {str(e)}"
            ) from e

    def search_with_unit_cell_and_spg(
        self,
        unit_cell: Union[List[float], str],
        space_group: Union[int, str],
        edge_err: float = 0.02,
        angle_err: float = 0.01,
    ) -> List:
        if isinstance(unit_cell, str):
            unit_cell = [float(x.strip()) for x in re.split(r"[ ,]+", unit_cell)]

        if len(unit_cell) != 6:
            raise ValueError(f"Unit cell must be a list of 6 floats, got {unit_cell}")

        logger.info(f"Searching for unit cell {unit_cell} in rcsb")

        try:
            result = self.unitcell_search(unit_cell, edge_err, angle_err)
        except Exception as e:
            logger.error(f"Unit cell search failed with an unexpected error: {e}")
            return None  # Fail gracefully

        min_diff = 1e10
        best_match_pdb = None
        best_match_cell = None
        for pdbid in result:
            try:
                r = self.get_space_group_for_pdbid(pdbid)
            except RuntimeError as e:  # get_space_group_for_pdbid raises RuntimeError
                logger.warning(
                    f"Could not get space group for {pdbid}, skipping. Error: {e}"
                )
                continue  # Skip to the next PDB ID

            spgnum = r["rcsb_space_group_number"]
            if Symmetry.same_point_group(space_group, spgnum):
                rcsb_unit_cell = r["rcsb_unit_cell"]
                logger.info(
                    f"Found {pdbid} with unit cell {rcsb_unit_cell} and {r['rcsb_hm_name']}"
                )
                diff = abs(np.mean(np.array(unit_cell) - np.array(rcsb_unit_cell)))
                if diff < min_diff:
                    min_diff = diff
                    best_match_pdb = pdbid
                    best_match_cell = rcsb_unit_cell
                    best_math_spg = spgnum

        if best_match_pdb:
            try:
                pdbfile = self.download(best_match_pdb, overwrite=True, cleanup=True)
                logger.info(
                    f"Found {best_match_pdb}: {pdbfile} with space group {best_math_spg} and unit cell {best_match_cell}, mean diff {min_diff}"
                )
                return pdbfile
            except Exception as e:
                logger.error(f"Failed to download best match PDB {best_match_pdb}: {e}")
                return None
        return None


if __name__ == "__main__":
    rcsb = RCSB()
    unit_cell = [78, 78, 39, 90, 90, 90]
    spg = 96
    r = rcsb.search_with_unit_cell_and_spg(unit_cell, spg)
    print(r)
