from __future__ import absolute_import

import logging
import os
import re
from collections import namedtuple

logger = logging.getLogger(__name__)


def getNumbers(string: str) -> list[float]:
    """Extracts all numbers (integers or floats) from a string."""
    try:
        return [float(x) for x in re.findall(r"[-+]?\d*\.\d+|\d+", string)]
    except (TypeError, ValueError):
        return []


class AimlessParser:
    """
    Parses an aimless.log file to extract key statistics and summary information.

    This class is self-contained and does not have external dependencies on
    other local file parsing modules.
    """

    def __init__(self, wdir: str = ".", filename: str = 'aimless.log'):
        """
        Initializes the parser by reading the specified log file.

        Args:
            wdir: The working directory where the log file is located.
            filename: The name of the aimless log file.

        Raises:
            FileNotFoundError: If the specified file does not exist.
        """
        self.wdir = os.path.abspath(wdir)
        self.logfile = os.path.join(self.wdir, filename)
        self.lines = []

        try:
            with open(self.logfile, 'r') as f:
                self.lines = f.readlines()
        except FileNotFoundError:
            logger.error("File not found: %s", self.logfile)
            raise
        except Exception as e:
            logger.error("Error reading file %s: %s", self.logfile, e)
            raise

    def _get_lines_contain(self, text: str) -> list[str]:
        """Helper to find lines containing a specific substring."""
        return [line for line in self.lines if text in line]

    def get_resolution_estimation(self, criterion: str = "cchalf~30%") -> float:
        """
        Estimates the resolution limit based on a given criterion from the log.
        """
        signatures = {
            "cchalf~30%": "from half-dataset correlation CC(1/2) >  0.30: limit =",
            "<i/sigmai>~1.5": "from Mn(I/sd) >  1.50:                         limit =",
            "<i/sigmai>~2.0": "from Mn(I/sd) >  2.00:                         limit =",
        }
        signature = signatures.get(criterion)
        if not signature:
            logger.warning("Unknown resolution criterion: %s", criterion)
            return 0.0

        reslimit = []
        # Search last 500 lines for efficiency
        for line in self.lines[-500:]:
            if signature in line:
                res_str = line.split("=")[-1].split("A")[0].strip()
                try:
                    res = float(res_str)
                    if res > 0.2:
                        reslimit.append(res)
                    else:
                        logger.warning(
                            "Resolution limit > 0.2, ignored: %f", res)
                except (ValueError, IndexError):
                    continue
        return min(reslimit) if reslimit else 0.0

    def _find_summary_block(self) -> list[str]:
        """Finds the main summary block within the log file."""
        # Find the last summary block by searching backwards
        for i in range(len(self.lines) - 1, -1, -1):
            if "S U M M A R Y" in self.lines[i]:
                return self.lines[i:]
        logger.warning("Could not find summary block in aimless.log")
        return []

    def get_table1(self, title: bool = True, output: str = 'text') -> any:
        """Extracts the final summary table (aka Table 1)."""
        summary_lines = self._find_summary_block()
        if not summary_lines:
            return None

        header_pattern = re.compile(
            r"Overall\s+Inner\s+Outer|Overall\s+InnerShell\s+OuterShell")
        start_idx = -1
        for i, line in enumerate(summary_lines):
            if header_pattern.search(line):
                start_idx = i
                break

        if start_idx == -1:
            logger.warning(
                "Could not find Table 1 header in aimless.log summary.")
            return None

        # Table is typically ~22 lines from the header
        table_lines = summary_lines[start_idx: start_idx + 22]

        if not title:
            table_lines.pop(0)  # Remove header line

        if output == 'text':
            return "".join(table_lines)
        elif output == 'list':
            return table_lines
        elif output == 'namedtuple':
            table1row = namedtuple(
                'TABLE1', "paraName, Overall,  InnerShell,  OuterShell")
            ndtable1 = []
            for line in table_lines:
                if header_pattern.search(line) or line.strip().startswith('---') or not line.strip():
                    continue
                values = line.split()
                if len(values) >= 4:
                    parameter_name, cols = ' '.join(values[:-3]), values[-3:]
                    row = table1row(parameter_name, *cols)
                    ndtable1.append(row)
            return ndtable1
        else:
            logger.warning("Unknown output format in get_table1: %s", output)
            return None

    def get_cell_space_group(self) -> dict[str, str]:
        """Extracts cell and space group from the summary block."""
        summary_lines = self._find_summary_block() or self.lines[-50:]

        result = {}
        cell_re = re.compile(r'Average unit cell:\s+([\d.\s]+)')
        spg_re = re.compile(r'Space group:\s+(.*)')

        for line in reversed(summary_lines):
            if 'unitcell' not in result:
                match = cell_re.search(line)
                if match:
                    result['unitcell'] = " ".join(
                        match.group(1).strip().split())
            if 'spacegroup' not in result:
                match = spg_re.search(line)
                if match:
                    result['spacegroup'] = match.group(
                        1).strip().replace(" ", "")
            if 'unitcell' in result and 'spacegroup' in result:
                break
        return result

    def summarize(self) -> dict[str, any]:
        """Gathers all key statistics into a single dictionary."""
        table1 = self.get_table1(title=False, output='namedtuple')
        if not table1:
            logger.error("Failed to parse Table 1. Cannot generate summary.")
            return {}

        db_dict = {'table1': self.get_table1(title=True, output='text')}
        db_dict.update(self.get_cell_space_group())

        # Extract wavelength
        wavelength = None
        for line in self.lines:
            if "Wavelength" in line and ":" in line:
                try:
                    parts = line.split(":")
                    wavelength = float(parts[1].strip().split()[0])
                    break
                except (ValueError, IndexError):
                    continue
        if wavelength:
            db_dict['wavelength'] = wavelength

        param_map = {
            "Low resolution": "lowresolution",
            "High resolution": "highresolution",
            "Rmerge (all I+ & I-)": "rmerge",
            "Rmerge (within I+/I-)": "anom_rmerge",
            "Rmeas (all I+ & I-)": "rmeas",
            "Rpim (all I+ & I-)": "rpim",
            "Mean((I)/sd(I))": "isigmai",
            "Completeness": "completeness",
            "Multiplicity": "multiplicity",
            "Mn(I) half-set correlation CC(1/2)": "cchalf",
            "Anomalous completeness": "anom_completeness",
            "Anomalous multiplicity": "anom_multiplicity",
            "DelAnom correlation with itself": "anom_cchalf",
            "Total observations": "Nobs",
            "Total unique": "Nuniq",
        }

        for entry in table1:
            norm_param = entry.paraName.strip()
            for key, db_key in param_map.items():
                if key in norm_param:
                    db_dict[db_key] = entry.Overall
                    # Add inner and outer shell values if needed
                    db_dict[f'{db_key}_inner'] = entry.InnerShell
                    db_dict[f'{db_key}_outer'] = entry.OuterShell
                    break

        db_dict["data_quality"] = self.evaluate_data_quality(db_dict)
        db_dict['anomalous_signal'] = self.evaluate_anomalous_signal()

        return db_dict

    def evaluate_anomalous_signal(self) -> float:
        """
        Evaluates the strength of the anomalous signal.

        Returns:
            0: Weak or no anomalous signal.
            10: Strong anomalous signal.
            float: Resolution limit for significant anomalous signal.
        """
        strong_signal_found = self._get_lines_contain(
            "strong anomalous signal found")
        missed_signal_found = self._get_lines_contain(
            "Anomalous flag switched OFF in input but there appears to be a significant anomalous signal"
        )

        if missed_signal_found:
            logger.warning(
                "Anomalous signal detected, but data was scaled as native.")

        if strong_signal_found or missed_signal_found:
            limit_lines = self._get_lines_contain(
                "Estimate of the resolution limit for a significant anomalous signal"
            )
            if limit_lines:
                numbers = getNumbers(limit_lines[0])
                if numbers:
                    res_limit = numbers[0]
                    logger.info(
                        "Anomalous signal resolution cutoff: %s A", res_limit)
                    return res_limit
            return 10.0  # Strong signal, but limit not found
        return 0.0

    @staticmethod
    def evaluate_data_quality(data_dict: dict) -> bool:
        """
        Performs a basic check on key metrics to evaluate data quality.
        """
        try:
            resol = float(data_dict.get('highresolution', 99.0))
            completeness = float(data_dict.get('completeness', 0.0))
            rmerge = float(data_dict.get('rmerge', 1.0))

            if rmerge < 0.3 and resol < 3.5 and completeness > 90.0:
                return True
        except (ValueError, TypeError, AttributeError):
            return False
        return False


if __name__ == '__main__':
    # This block allows for direct testing of the parser.
    # To use, create a dummy aimless.log file in a test directory.
    logging.basicConfig(level=logging.INFO)

    # Example usage:
    # try:
    #     # Assuming a test file exists at '/path/to/test/dir/aimless.log'
    #     parser = AimlessParser(wdir='/path/to/test/dir', filename='aimless.log')
    #     summary = parser.summarize()
    #     import json
    #     print(json.dumps(summary, indent=2))
    # except Exception as e:
    #     print(f"Test failed: {e}")
    pass
