# image_viewer/plugins/crystfel/stream_utils.py
import numpy as np
import logging
import os

logger = logging.getLogger(__name__)


class TriclinicCalculator:
    """
    Pre-compute reciprocal metric tensor for efficient d-spacing calculations.

    # Usage example
    cell = np.array([5.0, 6.0, 7.0, 80.0, 85.0, 75.0])
    calc = TriclinicCalculator(cell)

    # For many Miller indices
    miller_indices = np.array([
        [1, 0, 0], [0, 1, 0], [0, 0, 1],
        [1, 1, 0], [1, 0, 1], [0, 1, 1],
        [1, 1, 1], [2, 0, 0], [0, 2, 0]
    ])

    # Fast batch calculation
    d_spacings = calc.d_spacing_batch(miller_indices)
    print("d-spacings:", d_spacings)


    """

    def __init__(self, cell):
        """
        Initialize with unit cell parameters and pre-compute constants.

        Parameters:
        -----------
        cell : numpy array [a, b, c, alpha, beta, gamma]
            Unit cell parameters (angles in degrees)
        """
        self.cell = cell
        self._compute_reciprocal_metric()

    def _compute_reciprocal_metric(self):
        """Pre-compute all the constant terms."""
        a, b, c, alpha, beta, gamma = self.cell

        # Convert angles to radians
        alpha = np.radians(alpha)
        beta = np.radians(beta)
        gamma = np.radians(gamma)

        # Calculate trigonometric values
        c_alpha = np.cos(alpha)
        c_beta = np.cos(beta)
        c_gamma = np.cos(gamma)
        s_alpha = np.sin(alpha)
        s_beta = np.sin(beta)
        s_gamma = np.sin(gamma)

        # Volume factor
        V2 = 1 - c_alpha**2 - c_beta**2 - c_gamma**2 + 2 * c_alpha * c_beta * c_gamma

        # Store reciprocal metric tensor elements
        self.g11 = (s_alpha**2) / (a**2 * V2)
        self.g22 = (s_beta**2) / (b**2 * V2)
        self.g33 = (s_gamma**2) / (c**2 * V2)
        self.g12 = (c_alpha * c_beta - c_gamma) / (a * b * V2)
        self.g13 = (c_beta * c_gamma - c_alpha) / (a * c * V2)
        self.g23 = (c_gamma * c_alpha - c_beta) / (b * c * V2)

    def d_spacing(self, miller):
        """
        Calculate d-spacing for single Miller index (fast).

        Parameters:
        -----------
        miller : numpy array [h, k, l]

        Returns:
        --------
        float : d-spacing
        """
        h, k, l = miller
        inv_d_sq = (
            self.g11 * h**2
            + self.g22 * k**2
            + self.g33 * l**2
            + 2 * self.g12 * h * k
            + 2 * self.g13 * h * l
            + 2 * self.g23 * k * l
        )
        return 1 / np.sqrt(inv_d_sq)

    def d_spacing_batch(self, miller_list):
        """
        Calculate d-spacings for multiple Miller indices (vectorized).

        Parameters:
        -----------
        miller_list : numpy array of shape (N, 3)

        Returns:
        --------
        numpy array : d-spacings for each Miller index
        """
        h, k, l = miller_list.T
        inv_d_sq = (
            self.g11 * h**2
            + self.g22 * k**2
            + self.g33 * l**2
            + 2 * self.g12 * h * k
            + 2 * self.g13 * h * l
            + 2 * self.g23 * k * l
        )
        return 1 / np.sqrt(inv_d_sq)


# --- Stream Parser (Adapted from your streamparser.py) ---
class StreamParser:
    """
    Parses a CrystFEL .stream file to extract detailed information
    about peaks, indexing, and cell parameters for each image chunk.
    """

    def __init__(self, stream_file_path, high_res_limit=None, max_reflections=9999):
        self.stream_file = stream_file_path
        self.all_results = []
        if not os.path.exists(self.stream_file):
            logger.warning(f"Stream file not found: {self.stream_file}")
            return

        self.header = ""

        self.high_res_limit = high_res_limit
        self.max_reflections = max_reflections
        logger.info(f"max reflections/spots sets to {self.max_reflections}")
        self._parse_header_and_chunks()

    def get_header_content(self) -> str:
        """Returns the parsed header content."""
        return self.header

    def _parse_header_and_chunks(self):
        """
        Reads the stream file once, separating the initial header from all subsequent chunks.
        """
        header_lines = []
        is_in_header = True

        with open(self.stream_file, "r") as f:
            for chunk in self._chunk_generator(f):
                if is_in_header:
                    # The first "chunk" from our generator might be the header
                    if "----- Begin chunk -----" in chunk[0]:
                        self.header = "".join(header_lines)
                        is_in_header = False
                        # This chunk is a real chunk, so process it
                        parsed_chunk = self._parse_chunk(chunk)
                        if parsed_chunk:
                            self.all_results.append(parsed_chunk)
                    else:
                        header_lines.extend(chunk)
                else:
                    # Process all subsequent chunks normally
                    parsed_chunk = self._parse_chunk(chunk)
                    if parsed_chunk:
                        self.all_results.append(parsed_chunk)

        if is_in_header:  # In case the file had a header but no chunks
            self.header = "".join(header_lines)

    def _chunk_generator(self, file_handle):
        """Generator to yield sections from the stream file, including the initial header."""
        chunk = []
        # The first part of the file before any chunk is the header.
        # We'll treat it as the first "chunk" to be yielded.
        for line in file_handle:
            if line.startswith("----- Begin chunk -----"):
                if chunk:
                    yield chunk  # Yield the header or the previous chunk
                chunk = [line]
            else:
                chunk.append(line)
        if chunk:
            yield chunk  # Yield the last chunk

    def _parse_chunk(self, chunk: list) -> dict:
        """Parses a single chunk of text from the stream file."""
        results = {"indexed_by": "none", "chunk": "".join(chunk)}  # Default value
        peak_start = peak_end = refl_start = refl_end = None
        for i, line in enumerate(chunk):
            line = line.strip()
            if not line:
                continue

            parts = line.split()
            if line.startswith("Image filename:"):
                full_filename = parts[-1]
                results["image_filename"] = full_filename
            elif line.startswith("Event:"):
                try:
                    event_num = int(parts[-1].replace("//", ""))
                    results["event_num"] = event_num
                except ValueError:
                    logger.warning(f"Invalid event number in line: {line}")
            elif line.startswith("Image serial number:"):
                try:
                    serial_num = int(parts[-1])
                    results["image_serial_number"] = serial_num
                except ValueError:
                    logger.warning(f"Invalid serial number in line: {line}")
            elif line.startswith("hit ="):
                results["hit"] = int(parts[-1])
            elif line.startswith("num_peaks ="):
                results["num_peaks"] = int(parts[-1])
            elif line.startswith("Peaks from peak search"):
                peak_start = i + 2  # Data starts 2 lines after header
            elif line.startswith("End of peak list"):
                peak_end = i
            elif line.startswith("Cell parameters"):
                # Cell parameters 3.76313 7.78202 7.81337 nm, 89.87862 90.00602 89.97159 deg
                try:
                    cell = (
                        line.replace("Cell parameters", "")
                        .replace("nm,", "")
                        .replace("deg", "")
                        .split()
                    )
                    # Convert nm to Angstrom for first 3 values
                    cell_len = [
                        np.round(10 * float(p.strip(",")), 2) for p in cell[0:3]
                    ]
                    cell_ang = [np.round(float(p.strip(",")), 2) for p in cell[3:6]]
                    results["unit_cell_crystfel"] = cell_len + cell_ang
                except (ValueError, IndexError):
                    pass
            elif line.startswith("lattice_type ="):
                results["lattice_type"] = parts[-1]
            elif line.startswith("centering ="):
                results["centering"] = parts[-1]
            elif line.startswith("indexed_by ="):
                results["indexed_by"] = parts[-1]
            elif line.startswith("Reflections measured after indexing"):
                refl_start = i + 2
            elif line.startswith("End of reflections"):
                refl_end = i

        if peak_start and peak_end and peak_start < peak_end:

            peaks = [p.split() for p in chunk[peak_start:peak_end]]
            # only keep sports within resolution limit if specified
            if self.high_res_limit:
                peaks = [p for p in peaks if 10.0 / float(p[2]) >= self.high_res_limit]
            results["spots_crystfel"] = [
                (float(p[0]), float(p[1])) for p in peaks if len(p) >= 2
            ][: self.max_reflections]

        if refl_start and refl_end and refl_start < refl_end:
            # h    k    l          I   sigma(I)       peak background  fs/px  ss/px panel
            reflns = [p.split() for p in chunk[refl_start:refl_end]]
            miller_indices = np.array(
                [[int(r[0]), int(r[1]), int(r[2])] for r in reflns]
            )

            calc = TriclinicCalculator(results.get("unit_cell_crystfel"))

            d_spacings = calc.d_spacing_batch(miller_indices)
            if len(d_spacings) == len(reflns):
                if self.high_res_limit:
                    # Filter reflections based on high resolution limit
                    reflns = [
                        r
                        for r, d in zip(reflns, d_spacings)
                        if d >= self.high_res_limit
                    ]
            results["reflections_crystfel"] = reflns[: self.max_reflections]
        return results
