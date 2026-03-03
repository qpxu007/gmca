from __future__ import absolute_import

import logging
import os
import re
from collections import namedtuple
from typing import Dict, Any, Optional, List, Union, NamedTuple

logger = logging.getLogger(__name__)


class Xia2Parser:
    """
    Parses xia2 output files (xia2.txt) to extract key statistics and summary information.

    This class is self-contained and does not have external dependencies on
    other local file parsing modules. It specifically targets the final summary
    block in the log file to ensure the correct results are parsed.
    """

    def __init__(self, wdir: str = ".", filename: str = "xia2.txt"):
        """
        Initializes the parser by reading the specified log file.

        Args:
            wdir: The working directory where the log file is located.
            filename: The name of the xia2 log file.

        Raises:
            FileNotFoundError: If the specified file does not exist.
        """
        self.wdir = os.path.abspath(wdir)
        self.filename = os.path.join(self.wdir, filename)
        self.lines: List[str] = []
        self.data_dict: Optional[Dict[str, Any]] = None

        try:
            with open(self.filename, "r") as fh:
                self.lines = fh.readlines()
        except FileNotFoundError:
            logger.error("File not found: %s", self.filename)
            raise
        except Exception as e:
            logger.error("Error reading file %s: %s", self.filename, e)
            raise

    def _find_final_summary_block(self) -> List[str]:
        """
        Locates the final summary block in the xia2.txt file.

        It searches backwards from the end of the file for the characteristic
        start ("Unit cell refinement" or "For ...") and end ("Assuming spacegroup:")
        markers of the summary table.
        """
        end_idx = -1
        # Search backwards for the end of the block
        for i, line in reversed(list(enumerate(self.lines))):
            if "Assuming spacegroup:" in line:
                end_idx = i
                break

        if end_idx == -1:
            return []

        # Now search backwards from that point for the start of the block
        start_idx = -1
        
        # --- MODIFICATION START ---
        # Prioritize "Unit cell refinement" as the start marker, as it correctly
        # includes the line with the unit cell parameters.
        for i, line in reversed(list(enumerate(self.lines[:end_idx]))):
            if "Unit cell refinement" in line:
                start_idx = i
                break

        # Fallback to the old marker for different xia2 versions
        if start_idx == -1:
            logger.debug("Could not find 'Unit cell refinement' marker, falling back to 'For '")
            for i, line in reversed(list(enumerate(self.lines[:end_idx]))):
                if line.strip().startswith("For "):
                    start_idx = i
                    break
        # --- MODIFICATION END ---

        if start_idx == -1:
            return []

        return self.lines[start_idx : end_idx + 1]

    def get_cell_space_group(self) -> Dict[str, str]:
        """
        Extracts the unit cell and space group from the final summary block.
        """
        results: Dict[str, str] = {}
        summary_block = self._find_final_summary_block()
        if not summary_block:
            return results

        cell_re = re.compile(
            r"Overall:\s+.*?([\d.]+\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+[\d.]+)\s*"
        )
        spg_re = re.compile(r"Assuming spacegroup:\s+(.*)")

        for line in reversed(summary_block):
            if "unitcell" not in results:
                cell_match = cell_re.match(line)
                if cell_match:
                    results["unitcell"] = " ".join(cell_match.group(1).split())
            if "spacegroup" not in results:
                spg_match = spg_re.search(line)
                if spg_match:
                    results["spacegroup"] = spg_match.group(1).strip().replace(" ", "")

            if "unitcell" in results and "spacegroup" in results:
                break
        return results

    def get_table1(
        self, title: bool = False, output: str = "text"
    ) -> Optional[Union[str, List[str], List[NamedTuple]]]:
        """
        Extracts the main statistics table (Table 1) from the final summary block.
        """
        summary_block = self._find_final_summary_block()
        if not summary_block:
            logger.info("Failed to find summary block in %s", self.filename)
            return None

        header_pattern = re.compile(r"Overall\s+(?:InnerShell|Low)\s+(?:OuterShell|High)")
        table_start_idx = -1
        for i, line in enumerate(summary_block):
            if header_pattern.search(line):
                table_start_idx = i
                break

        if table_start_idx == -1:
            logger.warning("Failed to find Table 1 header in summary block.")
            return None

        # The table ends right before the "Overall:" line with the unit cell
        table_end_idx = len(summary_block)
        for i in range(table_start_idx, len(summary_block)):
            if summary_block[i].strip().startswith("Overall:"):
                table_end_idx = i
                break

        table_lines = summary_block[table_start_idx:table_end_idx]
        if not title:
            table_lines.pop(0)

        if output == "text":
            return "".join(table_lines)
        elif output == "list":
            return table_lines
        elif output == "namedtuple":
            table1row = namedtuple(
                "TABLE1", "paraName, Overall,  InnerShell,  OuterShell"
            )
            ndtable1: List[NamedTuple] = []
            lines_to_parse = table_lines[1:] if title else table_lines
            for line in lines_to_parse:
                values = line.split()
                if not values:
                    continue
                try:
                    # Heuristic to distinguish 3-column from 1-column lines.
                    # If the second-to-last item is a number, it's a 3-column line.
                    float(values[-2])
                    is_three_column = True
                except (ValueError, IndexError):
                    is_three_column = False

                if is_three_column:
                    parameter_name, cols = " ".join(values[:-3]), values[-3:]
                    row = table1row(parameter_name, *cols)
                    ndtable1.append(row)
                else:
                    # This is a 1-value line
                    parameter_name = " ".join(values[:-1])
                    row = table1row(parameter_name, values[-1], "", "")
                    ndtable1.append(row)
            return ndtable1
        else:
            logger.warning("Unknown output format in get_table1: %s", output)
            return None

    def summarize(self) -> Dict[str, Any]:
        """
        Gathers all key statistics into a single dictionary.

        This is the main method to call for parsing results.

        Returns:
            A dictionary containing all parsed statistics, or an empty dictionary
            if parsing fails.
        """
        if self.data_dict is not None:
            return self.data_dict

        table1 = self.get_table1(output="namedtuple")
        if not table1:
            logger.error(
                "Could not parse Table 1 from %s. Summary cannot be generated.",
                self.filename,
            )
            return {}

        db_dict: Dict[str, Any] = {"table1": self.get_table1(title=True, output="text")}
        db_dict.update(self.get_cell_space_group())

        # Extract wavelength
        wavelength = None
        # Match 'Wavelength 1.0' or 'Wavelength: 1.0', ignoring 'Wavelength name: ...'
        wavelength_re = re.compile(r"^\s*Wavelength(?:\s+|:\s*)([\d.]+)")

        for line in self.lines:
            match = wavelength_re.search(line)
            if match:
                try:
                    wavelength = float(match.group(1))
                    break
                except ValueError:
                    continue
        
        if wavelength:
            db_dict["wavelength"] = wavelength

        param_map = {
            "Low resolution limit": "lowresolution",
            "High resolution limit": "highresolution",
            "Rmerge(I)": "rmerge",
            "Rmerge(I+/-)": "anom_rmerge",
            "Rmeas(I)": "rmeas",
            "Rpim(I)": "rpim",
            "I/sigma": "isigmai",
            "Completeness": "completeness",
            "Multiplicity": "multiplicity",
            "CC half": "cchalf",
            "Anomalous completeness": "anom_completeness",
            "Anomalous multiplicity": "anom_multiplicity",
            "Anomalous correlation": "anom_cchalf",
            "Total observations": "Nobs",
            "Total unique": "Nuniq",
            "Anomalous slope": "anom_slope",
        }

        for entry in table1:
            param_name = entry.paraName.strip()
            if param_name in param_map:
                db_key = param_map[param_name]
                db_dict[db_key] = entry.Overall
                if entry.InnerShell and entry.OuterShell:
                    db_dict[f"{db_key}_inner"] = entry.InnerShell
                    db_dict[f"{db_key}_outer"] = entry.OuterShell

        db_dict["anomalous_signal"] = self.evaluate_anomalous_signal(db_dict)
        db_dict["data_quality"] = self.evaluate_data_quality(db_dict)
        self.data_dict = db_dict
        return self.data_dict

    @staticmethod
    def evaluate_anomalous_signal(data_dict: Dict[str, Any]) -> int:
        """
        Evaluates the strength of the anomalous signal based on parsed metrics.

        Returns:
            0: Weak signal.
            5: Moderate signal.
            10: Strong signal.
        """
        try:
            anom_slope = float(data_dict.get("anom_slope", 0.0))
            anom_cc_inner = float(data_dict.get("anom_cchalf_inner", 0.0))
            anom_cc_outer = float(data_dict.get("anom_cchalf_outer", 0.0))

            if anom_slope > 1.3 or (anom_cc_inner > 0.90 and anom_cc_outer > 0.20):
                return 10  # Strong signal

            if anom_cc_inner < 0.30 and anom_cc_outer < 0.20:
                return 0  # Weak signal

            return 5  # Moderate/ambiguous signal
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def evaluate_data_quality(data_dict: Dict[str, Any]) -> bool:
        """
        Performs a basic check on key metrics to evaluate data quality.
        """
        try:
            resol = float(data_dict.get("highresolution", 99.0))
            completeness = float(data_dict.get("completeness", 0.0))
            rmerge = float(data_dict.get("rmerge", 1.0))

            if rmerge < 0.3 and resol < 3.5 and completeness > 90.0:
                return True
        except (ValueError, TypeError, AttributeError):
            return False
        return False


if __name__ == "__main__":
    # This block allows for direct testing of the parser.
    logging.basicConfig(level=logging.INFO)

    # Example usage:
    # try:
    #     # Assuming a test file exists at '/path/to/test/dir/xia2.txt'
    #     parser = Xia2Parser(wdir='/path/to/test/dir', filename='xia2.txt')
    #     summary = parser.summarize()
    #     import json
    #     if summary:
    #         print(json.dumps(summary, indent=2))
    #     else:
    #         print("Parsing failed.")
    # except Exception as e:
    #     print(f"Test failed: {e}")
    pass
