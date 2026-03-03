# qp2/data_proc/server/reference_converter.py
import os
from qp2.log.logging_config import get_logger
from qp2.data_proc.server.sca2xds import SCA2XDS
from qp2.data_proc.server.mtzfile import Mtzfile
from qp2.data_proc.server.xdsasciiparser import XdsAsciiParser

logger = get_logger(__name__)


def process_reference_data(ref_file_path):
    """
    Converts SCA/MTZ to XDS_ASCII and extracts metadata.
    """
    if not ref_file_path or not os.path.exists(ref_file_path):
        return None, None, None

    final_path = ref_file_path
    space_group = None
    unit_cell = None

    try:
        if ref_file_path.lower().endswith((".sca", ".scalepack")):
            converter = SCA2XDS(sca_file=ref_file_path)
            final_path = converter.convert()  # Returns new path
            space_group = converter.space_group
            unit_cell = converter.unit_cell

        elif ref_file_path.lower().endswith(".mtz"):
            mtz = Mtzfile(ref_file_path)
            final_path = mtz.convert_to_XDS()
            space_group, _ = mtz.get_spacegroup()
            unit_cell = mtz.get_cell()

        # If we have an XDS file now (converted or original), try to parse header for info
        # if we missed it during conversion
        if (
            final_path
            and os.path.exists(final_path)
            and (not space_group or not unit_cell)
        ):
            try:
                hkldir, hklname = os.path.split(final_path)
                parser = XdsAsciiParser(hkldir, filename=hklname)
                spg, cell = parser.get_spg_cell(spg_type="symbol")
                if spg:
                    space_group = spg
                if cell:
                    unit_cell = " ".join(map(str, cell))
            except:
                pass

        return final_path, space_group, unit_cell

    except Exception as e:
        logger.error(f"Reference data processing failed: {e}", exc_info=True)
        return ref_file_path, None, None
