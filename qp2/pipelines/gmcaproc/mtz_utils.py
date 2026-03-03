import logging
import os
import subprocess

try:
    import gemmi
except ImportError:
    gemmi = None

logger = logging.getLogger(__name__)


# Helper function from your provided code
def get_string_between(string, str1, str2):
    return string[string.find(str1) + 1: string.rfind(str2)]


# Fallback MTZ parser class from your provided code
class Mtzfile:
    def __init__(self, mtzin):
        self.mtzin = mtzin
        if not os.path.exists(mtzin):
            raise RuntimeError("mtzin does not exist.")
        self.header_lines = self._get_mtz_header()

    def _get_mtz_header(self):
        try:
            mtzdmp = ["mtzdmp", self.mtzin, "-e"]
            header = (
                subprocess.run(mtzdmp, stdout=subprocess.PIPE, check=True)
                .stdout.decode("utf-8")
                .splitlines()
            )
            return header
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("mtzdmp command failed or not found.")
            return []

    def get_spacegroup(self):
        for line in self.header_lines:
            if line.strip().startswith("* Space group"):
                spg_symbol = get_string_between(line, "'", "'").replace(" ", "")
                spg_num = (
                    get_string_between(line, "(", ")").replace("number", "").strip()
                )
                return spg_symbol, spg_num
        return None, None

    def get_cell(self):
        in_cell_block = False
        for line in self.header_lines:
            if line.strip().startswith("* Dataset ID"):
                in_cell_block = True
            if in_cell_block and line.strip().startswith("* Number of Columns"):
                break
            if in_cell_block:
                fields = line.split()
                if len(fields) == 6:
                    try:
                        # Validate that all are numbers
                        [float(f) for f in fields]
                        return " ".join(fields)
                    except ValueError:
                        continue
        return None


def get_cell_symm_from_mtz(mtzfile):
    new_spg_num = None
    new_cell = None
    use_gemmi = gemmi is not None
    
    if use_gemmi:
        try:
            mtz = gemmi.read_mtz_file(mtzfile)
            new_cell_params = mtz.cell.parameters
            new_cell = " ".join(map(str, new_cell_params))
            new_spg_num = mtz.spacegroup.number
            logger.info(
                f"Extracted from dimple MTZ using gemmi: SG={new_spg_num}, Cell={new_cell}"
            )
        except Exception as e:
            logger.warning(f"gemmi failed to parse {mtzfile}: {e}. Trying fallback.")
            use_gemmi = False  # Prevent trying again if it fails

    # Fallback method: Use Mtzfile parser
    if not new_spg_num and not use_gemmi:
        try:
            mtz_parser = Mtzfile(mtzfile)
            new_cell = mtz_parser.get_cell()
            _, new_spg_num = mtz_parser.get_spacegroup()
            if new_spg_num and new_cell:
                logger.info(
                    f"Extracted from dimple MTZ using mtzdmp: SG={new_spg_num}, Cell={new_cell}"
                )
        except Exception as e:
            logger.error(f"Fallback mtzdmp parser also failed for {mtzfile}: {e}")

    return new_spg_num, new_cell
