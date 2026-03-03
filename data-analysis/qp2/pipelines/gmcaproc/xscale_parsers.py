# qp2/pipelines/gmcaproc/xscale_parsers.py

import os
import math
import logging
from typing import Dict, Any, List, Optional
from qp2.pipelines.gmcaproc.xds_config import XdsConfig
from qp2.pipelines.gmcaproc.xds_parsers import estimate_resolution

logger = logging.getLogger(__name__)

def parse_xscale_lp(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Parses XSCALE.LP to extract summary statistics and global information.
    Returns a dictionary of results, or None if the file is missing.
    """
    if not os.path.exists(file_path):
        logger.error(f"XSCALE log file not found: {file_path}")
        return None

    result_dict = {
        "SPACE_GROUP_NUMBER": None,
        "UNIT_CELL_CONSTANTS": None,
        "table1": [],
        "table1_total": [],
        "table1_text": "",
        "resolution_based_on_cchalf": None,
        "resolution_based_on_isigma": None,
    }

    # Standard XSCALE table header columns
    table1_header = [
        "RESOLUTION_LIMIT",
        "NUMBER_OBSERVED",
        "NUMBER_UNIQUE",
        "NUMBER_POSSIBLE",
        "COMPLETENESS",
        "R_FACTOR_OBSERVED",
        "R_FACTOR_EXPECTED",
        "NREFL_COMPARED",
        "I_SIGMA",
        "R_MEAS",
        "CC_HALF",
        "ANOMAL_CORR",
        "SIGANO",
        "NANO",
    ]
    result_dict["table1_header"] = table1_header

    try:
        with open(file_path, "r") as f:
            content = f.read()
            lines = content.splitlines()

        # 1. Parse Cell & Space Group (Usually in reindexing section or header)
        for line in lines:
            if "SPACE_GROUP_NUMBER=" in line:
                result_dict["SPACE_GROUP_NUMBER"] = line.split("=")[1].strip()
            if "UNIT_CELL_CONSTANTS=" in line:
                result_dict["UNIT_CELL_CONSTANTS"] = line.split("=")[1].strip()

        # 2. Parse Summary Table
        # We look for the last occurrence of the summary table
        table_start_marker = "SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE"
        last_table_start = content.rfind(table_start_marker)
        
        if last_table_start != -1:
            table_section = content[last_table_start:]
            t_lines = table_section.splitlines()
            
            table_data = []
            capture = False
            total_line_idx = -1
            
            for i, line in enumerate(t_lines):
                if "LIMIT" in line and "OBSERVED" in line: # Header second line
                    capture = True
                    continue
                
                if capture:
                    if not line.strip(): continue
                    
                    parts = line.split()
                    # Check if it is a data line (starts with resolution number)
                    if parts[0].replace('.','',1).isdigit():
                        clean_parts = [p.replace('%', '').replace('*', '') for p in parts]
                        table_data.append(clean_parts)
                    
                    elif "total" in line:
                        clean_total = [p.replace('%', '').replace('*', '') for p in parts]
                        result_dict["table1_total"] = clean_total
                        total_line_idx = i
                        break

            result_dict["table1"] = table_data
            if total_line_idx != -1:
                result_dict["table1_text"] = "\n".join(t_lines[:total_line_idx+1])

        # 3. Interpolate Resolution limits
        if result_dict["table1"]:
            # Standard mapping for XSCALE table columns:
            # 0: Res, ..., 10: CC(1/2), 8: I/Sigma
            res_col_idx = 0
            isig_col_idx = 8
            cchalf_col_idx = 10
            
            first_row = result_dict["table1"][0]
            if len(first_row) > cchalf_col_idx:
                resolutions = [float(row[res_col_idx]) for row in result_dict["table1"]]
                
                # CC1/2 resolution limit
                cchalfs = [float(row[cchalf_col_idx]) for row in result_dict["table1"]]
                res_cchalf = estimate_resolution(cchalfs, resolutions, XdsConfig.CC_HALF_TARGET)
                if res_cchalf:
                    result_dict["resolution_based_on_cchalf"] = math.ceil(res_cchalf * 20) / 20
                
                # I/Sigma resolution limit
                isigmas = [float(row[isig_col_idx]) for row in result_dict["table1"]]
                res_isig = estimate_resolution(isigmas, resolutions, XdsConfig.ISIGMA_TARGET)
                if res_isig:
                    result_dict["resolution_based_on_isigma"] = math.ceil(res_isig * 20) / 20

            # Extract high resolution limit from the last shell
            # table1 is a list of lists, where the first element is the resolution limit
            try:
                last_row = result_dict["table1"][-1]
                if last_row and len(last_row) > 0:
                    result_dict["resolution_highres"] = float(last_row[0])
            except (ValueError, IndexError):
                pass

    except Exception as e:
        logger.error(f"Error parsing XSCALE.LP: {e}", exc_info=True)

    return result_dict
