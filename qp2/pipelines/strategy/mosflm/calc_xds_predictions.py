import math
import numpy as np
from typing import Dict, Any, List, Tuple, Optional

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, *args, **kwargs):
        return iterable


# ---------- XDS parsing ----------

def _reciprocal_from_direct(a: np.ndarray, b: np.ndarray, c: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Compute reciprocal basis vectors from direct a,b,c (Å) in lab frame."""
    V = float(np.dot(a, np.cross(b, c)))
    if abs(V) < 1e-16:
        raise ValueError("Degenerate unit cell vectors (volume ~ 0)")
    a_star = np.cross(b, c) / V
    b_star = np.cross(c, a) / V
    c_star = np.cross(a, b) / V
    return a_star, b_star, c_star


def parse_xparm_xds(path: str) -> Dict[str, Any]:
    """
    Parse XPARM.XDS or GXPARM.XDS using iotbx.xds if present, with a minimal fallback for v2 format.

    Returns a dict with:
      A (3x3), rotation_axis (3,), wavelength, s0 (3,),  # s0 in Å^-1
      det: {ex, ey, ez, ORG (3, mm), QX, QY, distance_mm},
      osc: {start_deg, range_deg, start_frame}
    """
    # Preferred: iotbx.xds
    try:
        from iotbx.xds import xparm as xds_xparm  # type: ignore
        xp = xds_xparm.reader().read_file(path)
        # Crystal direct axes (Å) in lab frame if available
        a_dir = np.array(getattr(xp, "unit_cell_a_axis"))
        b_dir = np.array(getattr(xp, "unit_cell_b_axis"))
        c_dir = np.array(getattr(xp, "unit_cell_c_axis"))
        a_star, b_star, c_star = _reciprocal_from_direct(a_dir, b_dir, c_dir)

        A = np.column_stack([a_star, b_star, c_star])

        wavelength = float(getattr(xp, "wavelength"))
        # beam_vector may already be 1/Å; otherwise use incident_beam_direction and wavelength
        if hasattr(xp, "beam_vector") and xp.beam_vector is not None:
            s0 = np.array(xp.beam_vector, dtype=float)
        else:
            beam_dir = np.array(getattr(xp, "incident_beam_direction"), dtype=float)
            s0 = beam_dir / wavelength

        rot_axis = np.array(getattr(xp, "rotation_axis"), dtype=float)

        ex = np.array(getattr(xp, "detector_x_axis"), dtype=float)
        ey = np.array(getattr(xp, "detector_y_axis"), dtype=float)
        if hasattr(xp, "detector_normal") and xp.detector_normal is not None:
            ez = np.array(getattr(xp, "detector_normal"), dtype=float)
        else:
            ez = np.cross(ex, ey)  # XDS uses right-handed ED
            ez /= np.linalg.norm(ez)

        ORG = np.array(getattr(xp, "detector_origin"), dtype=float)  # mm
        px_size = getattr(xp, "pixel_size")
        QX, QY = float(px_size[0]), float(px_size[1])

        distance_mm = float(getattr(xp, "detector_distance"))

        start_angle = float(getattr(xp, "starting_angle"))
        osc_range = float(getattr(xp, "oscillation_range"))
        start_frame = int(getattr(xp, "starting_frame"))

        return dict(
            A=A,
            rotation_axis=rot_axis,
            wavelength=wavelength,
            s0=s0,
            det=dict(ex=ex, ey=ey, ez=ez, ORG=ORG, QX=QX, QY=QY, distance_mm=distance_mm),
            osc=dict(start_deg=start_angle, range_deg=osc_range, start_frame=start_frame),
        )
    except Exception:
        pass

    # Fallback: minimal v2 token parser based on iotbx.xds.xparm.write signature
    with open(path, "r") as f:
        tokens = f.read().split()
    # The common v2 format starts with 'XPARM.XDS' then fields; skip leading label if present
    if tokens[0].upper().endswith("XDS"):
        tokens = tokens[1:]
    # Expected order (see iotbx.xds docs): starting_frame, starting_angle, oscillation_range,
    # rotation_axis(3), wavelength, beam_vector(3),
    # space_group, unit_cell(6), unit_cell_a_axis(3), unit_cell_b_axis(3), unit_cell_c_axis(3),
    # num_segments, detector_size(2), pixel_size(2), detector_origin(3),
    # detector_distance, detector_x_axis(3), detector_y_axis(3), detector_normal(3)
    it = iter(tokens)
    try:
        start_frame = int(next(it))
        start_angle = float(next(it))
        osc_range = float(next(it))
        rot_axis = np.array([float(next(it)), float(next(it)), float(next(it))], dtype=float)
        wavelength = float(next(it))
        beam_vec = np.array([float(next(it)), float(next(it)), float(next(it))], dtype=float)
        # discard space_group
        _ = int(next(it))
        uc = [float(next(it)) for _ in range(6)]
        a_dir = np.array([float(next(it)), float(next(it)), float(next(it))], dtype=float)
        b_dir = np.array([float(next(it)), float(next(it)), float(next(it))], dtype=float)
        c_dir = np.array([float(next(it)), float(next(it)), float(next(it))], dtype=float)
        # segments and detector layout
        _nseg = int(next(it))
        _det_nx = int(next(it)); _det_ny = int(next(it))
        QX = float(next(it)); QY = float(next(it))
        ORG = np.array([float(next(it)), float(next(it)), float(next(it))], dtype=float)
        distance_mm = float(next(it))
        ex = np.array([float(next(it)), float(next(it)), float(next(it))], dtype=float)
        ey = np.array([float(next(it)), float(next(it)), float(next(it))], dtype=float)
        ez = np.array([float(next(it)), float(next(it)), float(next(it))], dtype=float)
    except Exception as e:
        raise ValueError(f"Unsupported or unexpected XPARM.XDS layout: {e}")

    # Build A from direct axes
    a_star, b_star, c_star = _reciprocal_from_direct(a_dir, b_dir, c_dir)
    A = np.column_stack([a_star, b_star, c_star])

    # Prefer beam_vector if non-zero; else use wavelength + direction
    s0 = beam_vec if np.linalg.norm(beam_vec) > 1e-12 else (beam_vec / 0.0)  # fallback handled below
    if not np.isfinite(s0).all() or np.linalg.norm(s0) < 1e-12:
        # Assume beam_vec was direction only; use wavelength
        s0 = beam_vec / (np.linalg.norm(beam_vec) + 1e-16) / wavelength

    return dict(
        A=A,
        rotation_axis=rot_axis,
        wavelength=wavelength,
        s0=s0,
        det=dict(ex=ex, ey=ey, ez=ez, ORG=ORG, QX=QX, QY=QY, distance_mm=distance_mm),
        osc=dict(start_deg=start_angle, range_deg=osc_range, start_frame=start_frame),
    )


# ---------- Geometry & solver ----------

def normalize_angle_deg(phi: float) -> float:
    x = (phi + 180.0) % 360.0 - 180.0
    return 180.0 if abs(x + 180.0) < 1e-12 else x


def interval_contains_angle(phi: float, start: float, end: float) -> bool:
    phi = (phi % 360.0 + 360.0) % 360.0
    s = (start % 360.0 + 360.0) % 360.0
    e = (end % 360.0 + 360.0) % 360.0
    if s <= e:
        return s - 1e-9 <= phi <= e + 1e-9
    else:
        return phi >= s - 1e-9 or phi <= e + 1e-9


def get_rotation_matrix(axis: np.ndarray, angle_rad: float) -> np.ndarray:
    axis = np.asarray(axis, dtype=float)
    axis = axis / np.linalg.norm(axis)
    kx, ky, kz = axis
    c, s, vc = math.cos(angle_rad), math.sin(angle_rad), 1.0 - math.cos(angle_rad)
    return np.array(
        [
            [kx * kx * vc + c,     kx * ky * vc - kz * s, kx * kz * vc + ky * s],
            [ky * kx * vc + kz * s, ky * ky * vc + c,     ky * kz * vc - kx * s],
            [kz * kx * vc - ky * s, kz * ky * vc + kx * s, kz * kz * vc + c],
        ],
        dtype=float,
    )


def solve_ewald_roots_general_axis(p0: np.ndarray, u_axis: np.ndarray, s0: np.ndarray) -> Optional[Tuple[float, float]]:
    """
    Solve f(φ) = 2 s0·pφ + |p0|^2 = 0 with rotation about unit axis u_axis.
    """
    u = u_axis / np.linalg.norm(u_axis)
    p0 = p0.reshape(3)
    p_par = np.dot(u, p0) * u
    p_perp = p0 - p_par
    q = np.cross(u, p_perp)

    A = 2.0 * np.dot(s0, p_perp)
    B = 2.0 * np.dot(s0, q)
    C = 2.0 * np.dot(s0, p_par) + float(np.dot(p0, p0))

    R = math.hypot(A, B)
    if R < 1e-12:
        if abs(C) < 1e-12:
            return 0.0, math.pi
        return None
    x = -C / R
    if x < -1.0 - 1e-12 or x > 1.0 + 1e-12:
        return None
    x = min(1.0, max(-1.0, x))
    phi0 = math.atan2(B, A)
    delta = math.acos(x)
    return (phi0 - delta, phi0 + delta)


# ---------- Prediction using XPARM ----------

def generate_predictions_from_xparm(
    xparm_path: str,
    image_number: Optional[int] = None,
    osc_range_deg: Optional[Tuple[float, float]] = None,
    resolution_limit: float = 1.5,
    mosaicity_deg: float = 0.0,
) -> List[Dict[str, Any]]:
    """
    Predict reflections using geometry from XPARM.XDS/GXPARM.XDS.
    """
    x = parse_xparm_xds(xparm_path)

    # φ range
    if osc_range_deg is not None:
        phi_start_deg, phi_end_deg = float(osc_range_deg[0]), float(osc_range_deg[1])
    else:
        if image_number is None:
            raise ValueError("Provide image_number or osc_range_deg")
        start = x["osc"]["start_deg"]
        width = x["osc"]["range_deg"]
        start_frame = x["osc"]["start_frame"]
        phi_start_deg = start + (int(image_number) - int(start_frame)) * width
        phi_end_deg = phi_start_deg + width

    # Crystal A and HKL bounds
    A = np.array(x["A"], dtype=float)
    a_star_mag, b_star_mag, c_star_mag = np.linalg.norm(A, axis=0)  # column norms
    s_max = 1.0 / float(resolution_limit)
    h_max = math.ceil(s_max / a_star_mag) if a_star_mag > 1e-12 else 0
    k_max = math.ceil(s_max / b_star_mag) if b_star_mag > 1e-12 else 0
    l_max = math.ceil(s_max / c_star_mag) if c_star_mag > 1e-12 else 0

    # Geometry
    s0 = np.array(x["s0"], dtype=float)  # Å^-1 direction of beam scaled by 1/λ
    u_axis = np.array(x["rotation_axis"], dtype=float)
    det = x["det"]
    ex, ey, ez = np.array(det["ex"]), np.array(det["ey"]), np.array(det["ez"])
    ORG = np.array(det["ORG"], dtype=float)  # mm
    QX, QY = float(det["QX"]), float(det["QY"])

    # Wedge expansion by mosaicity
    phi_start_deg = normalize_angle_deg(phi_start_deg)
    phi_end_deg = normalize_angle_deg(phi_end_deg)
    gate_start = phi_start_deg - mosaicity_deg / 2.0
    gate_end = phi_end_deg + mosaicity_deg / 2.0

    preds: List[Dict[str, Any]] = []

    for h in tqdm(range(-h_max, h_max + 1), desc=f"Predict φ {phi_start_deg:.2f}-{phi_end_deg:.2f}°"):
        for k in range(-k_max, k_max + 1):
            for l in range(-l_max, l_max + 1):
                if h == 0 and k == 0 and l == 0:
                    continue
                hkl = np.array([h, k, l], dtype=float)
                p0 = A @ hkl  # Å^-1

                p0_norm = float(np.linalg.norm(p0))
                if p0_norm > s_max + 1e-12:
                    continue

                roots = solve_ewald_roots_general_axis(p0, u_axis, s0)
                if roots is None:
                    continue

                for phi_rad in roots:
                    phi_deg = normalize_angle_deg(math.degrees(phi_rad))
                    if not interval_contains_angle(phi_deg, gate_start, gate_end):
                        continue

                    # Rotate to φ and form scattered ray s = s0 + pφ
                    R = get_rotation_matrix(u_axis, math.radians(phi_deg))
                    p_phi = R @ p0
                    s = s0 + p_phi  # Å^-1

                    # Intersect ray r(t) = t*s with detector plane through ORG with normal ez
                    denom = float(np.dot(ez, s))
                    if denom <= 1e-12:
                        continue  # no intersection with detector plane
                    t = float(np.dot(ez, ORG) / denom)
                    if t <= 0:
                        continue  # intersection behind source

                    r = t * s  # in arbitrary scale; only direction matters, plane intersection gives absolute mm via ORG projection below
                    # Compute mm coordinates in detector frame
                    w = r - ORG  # mm, because ORG is mm and r is scaled so that dot(ez, r) = dot(ez, ORG)
                    x_mm = float(np.dot(w, ex))
                    y_mm = float(np.dot(w, ey))

                    ix = float(x_mm / QX)
                    iy = float(y_mm / QY)
                    # XDS pixel coordinates are (IX, IY) relative to the origin ORG in detector frame; add ORGX/ORGY if needed for display

                    preds.append(
                        {
                            "hkl": (int(h), int(k), int(l)),
                            "phi_deg": round(phi_deg, 4),
                            "x_pix_rel": round(ix, 2),
                            "y_pix_rel": round(iy, 2),
                        }
                    )

    return preds


if __name__ == "__main__":
    # Example usage
    preds = generate_predictions_from_xparm(
        xparm_path="XPARM.XDS",
        image_number=451,
        resolution_limit=2.0,
        mosaicity_deg=0.3,
    )
    print(f"Found {len(preds)} predictions.")
    print(preds[:5])
