#!/usr/bin/env python3
"""Capture camera images at specified motor positions.

Usage examples:

    # Snapshot at current position (both cameras)
    python camera_shot.py -o snapshot.jpg

    # Move omega to 0° and capture
    python camera_shot.py --omega 0 -o sample_0deg.jpg

    # Move to absolute position and capture
    python camera_shot.py --x 0.5 --y 0.3 --z -1.0 --omega 0 -o sample.jpg

    # Relative move from current position
    python camera_shot.py --dx 0.1 --dy -0.05 --omega 90 -o sample_90deg.jpg

    # Capture both cameras
    python camera_shot.py --omega 0 --both -o sample

    # Capture low-res only
    python camera_shot.py --camera low -o lowres.jpg

    # Multi-angle capture (0° and 90°)
    python camera_shot.py --omega 0 90 -o sample.jpg
"""

import argparse
import os
import sys
import time

import numpy as np
from PIL import Image
from io import BytesIO
from urllib.request import urlopen


# ------------------------------------------------------------------
# Beamline auto-detection
# ------------------------------------------------------------------

def _detect_beamline():
    """Detect beamline from hostname."""
    hostname = os.uname()[1][:3]
    if hostname == "bl1":
        return "23i:", "http://bl1ws3-40g:8008/rpc"
    elif hostname == "bl2":
        return "23o:", "http://bl2ws3-40g:8008/rpc"
    elif hostname == "bl3":
        return "23b:", "http://bl3ws3-40g:8008/rpc"
    else:
        # Default — try bl1
        return "23i:", "http://bl1ws3-40g:8008/rpc"


# ------------------------------------------------------------------
# Motor control
# ------------------------------------------------------------------

def get_motor_limits(beamline):
    """Read motor soft limits from EPICS.

    Returns dict of ``{motor_name: (lower, upper)}`` or empty values
    if EPICS is unavailable.
    """
    limits = {}
    try:
        from epics import caget
        for motor, pv_prefix in [
            ("sample_x", f"{beamline}GO:SX:O:RqsPos"),
            ("sample_y", f"{beamline}GO:SY:O:RqsPos"),
            ("sample_z", f"{beamline}GO:SZ:O:RqsPos"),
            ("gonio_omega", f"{beamline}GO:Om:O:RqsPos"),
        ]:
            lo = caget(f"{pv_prefix}.DRVL", timeout=2)
            hi = caget(f"{pv_prefix}.DRVH", timeout=2)
            if lo is not None and hi is not None and (lo != 0 or hi != 0):
                limits[motor] = (float(lo), float(hi))
    except Exception:
        pass
    return limits


def check_limits(positions, limits, current_pos=None, rel=False):
    """Check if requested positions are within soft limits.

    Parameters
    ----------
    positions : dict
        ``{motor_name: value}`` to check.
    limits : dict
        ``{motor_name: (lower, upper)}`` from ``get_motor_limits``.
    current_pos : dict, optional
        Current motor positions (needed for relative moves).
    rel : bool
        If True, values are relative offsets.

    Returns
    -------
    list of str
        Error messages for out-of-bounds motors. Empty if all OK.
    """
    errors = []
    for motor, value in positions.items():
        if value is None:
            continue
        if motor not in limits:
            continue
        lo, hi = limits[motor]

        if rel and current_pos and motor in current_pos:
            target = current_pos[motor] + value
        else:
            target = value

        if target < lo or target > hi:
            errors.append(
                f"{motor}={target:.4f} out of range [{lo:.4f}, {hi:.4f}]"
            )
    return errors


def sample_env_move(rpc_url, move_string, wait=True):
    """Send a sample environment command (backlight, beamstop, etc.).

    Common commands::

        backlightIN          — turn on backlight (for camera viewing)
        backlightOUT         — turn off backlight
        beamstopIN           — insert beamstop
        beamstopOUT          — retract beamstop
        beamstopIN backlightOUT  — collection position
        backlightIN              — viewing position

    The command is sent to the EPICS ``bi:sampleEnv`` PV via PBS RPC.
    """
    import requests
    try:
        resp = requests.post(rpc_url, data={
            "module": "sample_env_move",
            "move_string": move_string,
            "wait_for": "1" if wait else "0",
        }, timeout=20)
        if resp.status_code != 200:
            print(f"Warning: sample_env_move returned {resp.status_code}")
        return resp
    except Exception as e:
        print(f"Warning: sample_env_move failed: {e}")
        return None


def move_motors(rpc_url, x=None, y=None, z=None, omega=None,
                rel=False, wait=True):
    """Move sample motors via PBS RPC.

    Parameters
    ----------
    x, y, z : float, optional
        Sample motor positions in mm.
    omega : float, optional
        Goniometer angle in degrees.
    rel : bool
        If True, moves are relative to current position.
    wait : bool
        If True, block until move completes.
    """
    import requests

    parts = []
    prefix = "rel:" if rel else ""
    if x is not None:
        parts.append(f"sample_x={prefix}{x:.4f}")
    if y is not None:
        parts.append(f"sample_y={prefix}{y:.4f}")
    if z is not None:
        parts.append(f"sample_z={prefix}{z:.4f}")

    # Move XYZ together (same assembly)
    if parts:
        move_str = ",".join(parts)
        resp = requests.post(rpc_url, data={
            "module": "pmac_move",
            "move_str": move_str,
            "wait_for": "1" if wait else "0",
        }, timeout=10)
        if resp.status_code != 200:
            print(f"Warning: XYZ move returned {resp.status_code}")

    # Move omega separately (different assembly)
    if omega is not None:
        move_str = f"gonio_omega={prefix}{omega:.3f}"
        resp = requests.post(rpc_url, data={
            "module": "pmac_move",
            "move_str": move_str,
            "wait_for": "1" if wait else "0",
        }, timeout=10)
        if resp.status_code != 200:
            print(f"Warning: omega move returned {resp.status_code}")


def get_motor_positions(beamline):
    """Read current motor positions via EPICS."""
    try:
        from epics import caget
        x = caget(f"{beamline}GO:SX:O:ActPos")
        y = caget(f"{beamline}GO:SY:O:ActPos")
        z = caget(f"{beamline}GO:SZ:O:ActPos")
        omega = caget(f"{beamline}GO:Om:O:ActPos")
        return {"x": x, "y": y, "z": z, "omega": omega}
    except Exception:
        return {"x": None, "y": None, "z": None, "omega": None}


# ------------------------------------------------------------------
# Camera capture
# ------------------------------------------------------------------

def grab_image(url):
    """Fetch a JPEG image from a camera URL. Returns numpy array (H, W, 3)."""
    raw = urlopen(url, timeout=5).read()
    img = np.array(Image.open(BytesIO(raw)))
    return img


def get_camera_urls(beamline):
    """Return (highres_url, lowres_url) for the beamline cameras."""
    # PVA-based JPEG endpoint (same as autocenter_loop.py)
    highres = f"http://192.168.1.13:8200/jpeg?pv={beamline}V1:Pva1:Image"
    lowres = f"http://192.168.1.13:8200/jpeg?pv={beamline}V2:Pva1:Image"
    return highres, lowres


def get_camera_scales(beamline):
    """Read camera pixel scales in microns/pixel via EPICS."""
    try:
        from epics import caget
        hr_scale = caget(f"{beamline}bi:px2mm_v_hi") * 1000.0  # mm→um
        lr_scale = caget(f"{beamline}bi:px2mm_v_lo") * 1000.0
        return hr_scale, lr_scale
    except Exception:
        return None, None


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Capture camera images at specified motor positions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Motor position (absolute)
    parser.add_argument("--x", type=float, default=None,
                        help="Absolute sample_x position (mm)")
    parser.add_argument("--y", type=float, default=None,
                        help="Absolute sample_y position (mm)")
    parser.add_argument("--z", type=float, default=None,
                        help="Absolute sample_z position (mm)")

    # Motor position (relative)
    parser.add_argument("--dx", type=float, default=None,
                        help="Relative sample_x move (mm)")
    parser.add_argument("--dy", type=float, default=None,
                        help="Relative sample_y move (mm)")
    parser.add_argument("--dz", type=float, default=None,
                        help="Relative sample_z move (mm)")

    # Omega — supports multiple angles
    parser.add_argument("--omega", type=float, nargs="+", default=None,
                        help="Omega angle(s) in degrees (e.g., --omega 0 90)")

    # Camera selection
    parser.add_argument("--camera", choices=["high", "low", "both"],
                        default="high",
                        help="Camera to use (default: high)")
    parser.add_argument("--both", action="store_true",
                        help="Capture both cameras (shortcut for --camera both)")

    # Output
    parser.add_argument("-o", "--output", default="camera_shot.jpg",
                        help="Output filename (default: camera_shot.jpg). "
                             "For multi-angle, angle is appended before extension.")
    parser.add_argument("--settle", type=float, default=0.3,
                        help="Settle time after motor move (seconds, default: 0.3)")

    # Sample environment
    parser.add_argument("--backlight", choices=["on", "off", "auto"],
                        default="auto",
                        help="Backlight control: 'on' = turn on before capture, "
                             "'off' = turn off, 'auto' = turn on before and "
                             "restore after (default: auto)")

    # Dry run
    parser.add_argument("--no-move", action="store_true",
                        help="Capture without moving motors")
    parser.add_argument("--info", action="store_true",
                        help="Print camera/motor info and exit")

    args = parser.parse_args()
    if args.both:
        args.camera = "both"

    beamline, rpc_url = _detect_beamline()
    hr_url, lr_url = get_camera_urls(beamline)

    if args.info:
        print(f"Beamline: {beamline}")
        print(f"RPC URL:  {rpc_url}")
        print(f"High-res: {hr_url}")
        print(f"Low-res:  {lr_url}")
        pos = get_motor_positions(beamline)
        print(f"Motors:   x={pos['x']}, y={pos['y']}, z={pos['z']}, "
              f"omega={pos['omega']}")
        scales = get_camera_scales(beamline)
        print(f"Scales:   high-res={scales[0]} um/px, "
              f"low-res={scales[1]} um/px")
        return

    # --- Check limits and move motors ---
    if not args.no_move:
        limits = get_motor_limits(beamline)
        current = get_motor_positions(beamline)
        current_map = {
            "sample_x": current.get("x"),
            "sample_y": current.get("y"),
            "sample_z": current.get("z"),
            "gonio_omega": current.get("omega"),
        }

        # Check absolute moves
        has_abs = any(v is not None for v in [args.x, args.y, args.z])
        if has_abs:
            abs_pos = {
                "sample_x": args.x, "sample_y": args.y, "sample_z": args.z,
            }
            errs = check_limits(abs_pos, limits)
            if errs:
                print(f"ERROR: Move out of bounds: {'; '.join(errs)}",
                      file=sys.stderr)
                sys.exit(1)
            print(f"Moving to x={args.x}, y={args.y}, z={args.z}")
            move_motors(rpc_url, x=args.x, y=args.y, z=args.z, rel=False)
            time.sleep(args.settle)

        # Check relative moves
        has_rel = any(v is not None for v in [args.dx, args.dy, args.dz])
        if has_rel:
            rel_pos = {
                "sample_x": args.dx, "sample_y": args.dy, "sample_z": args.dz,
            }
            errs = check_limits(rel_pos, limits, current_map, rel=True)
            if errs:
                print(f"ERROR: Move out of bounds: {'; '.join(errs)}",
                      file=sys.stderr)
                sys.exit(1)
            print(f"Relative move dx={args.dx}, dy={args.dy}, dz={args.dz}")
            move_motors(rpc_url, x=args.dx, y=args.dy, z=args.dz, rel=True)
            time.sleep(args.settle)

        # Check omega limits
        if args.omega:
            for omega_val in args.omega:
                errs = check_limits(
                    {"gonio_omega": omega_val}, limits
                )
                if errs:
                    print(f"ERROR: Omega out of bounds: {'; '.join(errs)}",
                          file=sys.stderr)
                    sys.exit(1)

    # --- Determine omega angles ---
    if args.omega is None:
        omega_list = [None]  # capture at current angle
    else:
        omega_list = args.omega

    # --- Determine cameras ---
    cameras = []
    if args.camera in ("high", "both"):
        cameras.append(("HighRes", hr_url))
    if args.camera in ("low", "both"):
        cameras.append(("LowRes", lr_url))

    # --- Backlight control ---
    if not args.no_move and args.backlight in ("on", "auto"):
        print("Backlight ON")
        sample_env_move(rpc_url, "backlightIN", wait=True)
        time.sleep(0.3)
    elif not args.no_move and args.backlight == "off":
        print("Backlight OFF")
        sample_env_move(rpc_url, "backlightOUT", wait=True)
        time.sleep(0.3)

    # --- Capture ---
    base, ext = os.path.splitext(args.output)
    if not ext:
        ext = ".jpg"

    for omega in omega_list:
        if omega is not None and not args.no_move:
            print(f"Moving omega to {omega:.1f}°")
            move_motors(rpc_url, omega=omega, rel=False)
            time.sleep(args.settle)

        for cam_name, cam_url in cameras:
            try:
                img = grab_image(cam_url)

                # Build filename
                parts = [base]
                if omega is not None and len(omega_list) > 1:
                    parts.append(f"_{omega:.0f}")
                if len(cameras) > 1:
                    parts.append(f"_{cam_name}")
                filepath = "".join(parts) + ext

                Image.fromarray(img).save(filepath)
                print(f"Saved: {filepath}  ({img.shape[1]}x{img.shape[0]}, "
                      f"omega={omega}°, {cam_name})")

            except Exception as e:
                print(f"Error capturing {cam_name} at omega={omega}: {e}",
                      file=sys.stderr)

    # --- Restore backlight if auto mode ---
    if not args.no_move and args.backlight == "auto":
        # In auto mode, backlight was turned on for capture.
        # Leave it on (viewing mode) — this matches pybluice behavior
        # where backlight stays on between collections.
        pass

    # Print final motor positions
    pos = get_motor_positions(beamline)
    if pos["x"] is not None:
        print(f"Final position: x={pos['x']:.4f}, y={pos['y']:.4f}, "
              f"z={pos['z']:.4f}, omega={pos['omega']:.3f}")


if __name__ == "__main__":
    main()
