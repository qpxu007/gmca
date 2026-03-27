#!/usr/bin/env python3
"""Automated data collection from raster3d pipeline recommendations.

Reads results.json from the raster3d pipeline and executes data collection
via bluice RPC calls. Supports SINGLE, SITE (multi-crystal), and VECTOR
(helical) collection modes.

Usage:

    # Dry run — show what would be collected, no motor moves
    python auto_collect.py results.json --dry-run

    # Collect the best peak
    python auto_collect.py results.json

    # Collect all peaks as multi-site
    python auto_collect.py results.json --all-peaks

    # Collect with energy change
    python auto_collect.py results.json --energy 12.661

    # Override specific parameters
    python auto_collect.py results.json --attenuation 10 --exposure 0.1

    # Specify beamline (auto-detected from hostname by default)
    python auto_collect.py results.json --beamline 23i
"""

import argparse
import json
import logging
import os
import sys
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)
logger = logging.getLogger("auto_collect")


# ------------------------------------------------------------------
# Beamline config
# ------------------------------------------------------------------

_BEAMLINE_CONFIG = {
    "23i": {"rpc": "http://bl1ws3-40g:8008/rpc", "epics": "23i:"},
    "23o": {"rpc": "http://bl2ws3-40g:8008/rpc", "epics": "23o:"},
    "23b": {"rpc": "http://bl3ws3-40g:8008/rpc", "epics": "23b:"},
}


def detect_beamline():
    hostname = os.uname()[1][:3]
    if hostname == "bl1":
        return "23i"
    elif hostname == "bl2":
        return "23o"
    elif hostname == "bl3":
        return "23b"
    return "23i"


# ------------------------------------------------------------------
# RPC helpers
# ------------------------------------------------------------------

class BluiceRPC:
    """Thin wrapper around bluice PBS RPC calls."""

    def __init__(self, rpc_url, dry_run=False):
        self.rpc_url = rpc_url
        self.dry_run = dry_run

    def _post(self, module, **params):
        data = {"module": module}
        data.update({k: str(v) for k, v in params.items() if v is not None})
        if self.dry_run:
            logger.info(f"  [DRY RUN] POST {module}: {params}")
            return "OK"
        try:
            resp = requests.post(self.rpc_url, data=data, timeout=30)
            result = resp.text.strip() if resp.status_code == 200 else f"ERR:{resp.status_code}"
            if not result.startswith("OK"):
                logger.warning(f"  RPC {module} returned: {result}")
            return result
        except Exception as e:
            logger.error(f"  RPC {module} failed: {e}")
            return f"ERR:{e}"

    def move_motors(self, x=None, y=None, z=None, wait=True):
        parts = []
        if x is not None:
            parts.append(f"sample_x={x:.4f}")
        if y is not None:
            parts.append(f"sample_y={y:.4f}")
        if z is not None:
            parts.append(f"sample_z={z:.4f}")
        if parts:
            return self._post("pmac_move",
                              move_str=",".join(parts),
                              wait_for=1 if wait else 0)

    def move_omega(self, omega, wait=True):
        return self._post("pmac_move",
                          move_str=f"gonio_omega={omega:.3f}",
                          wait_for=1 if wait else 0)

    def move_detector(self, distance_mm, wait=True):
        return self._post("pmac_move",
                          move_str=f"detector_z={distance_mm:.1f}",
                          wait_for=1 if wait else 0)

    def set_beam_size(self, x_um, y_um, wait=True):
        return self._post("pmac_move",
                          move_str=f"beam_size_x={x_um/1000:.4f},"
                                   f"beam_size_y={y_um/1000:.4f}",
                          wait_for=1 if wait else 0)

    def set_attenuation(self, factor, wait=True):
        return self._post("attenuation_change",
                          dest_factors=factor, wait_for=1 if wait else 0)

    def set_energy(self, energy_kev, wait=True):
        return self._post("move_energy_py",
                          dest_keV=f"{energy_kev:.4f}",
                          wait_for=1 if wait else 0)

    def sample_env(self, move_string, wait=True):
        return self._post("sample_env_move",
                          move_string=move_string,
                          wait_for=1 if wait else 0)

    def run_create(self, mode="SINGLE", **params):
        return self._post("run_create", mode=mode, auto="1", **params)

    def collect(self, run_idx=None, wait=True):
        kw = {"auto": "1", "wait_for": 1 if wait else 0}
        if run_idx is not None:
            kw["runs"] = str(run_idx)
        return self._post("collect_runs_py", **kw)


# ------------------------------------------------------------------
# Collection logic
# ------------------------------------------------------------------

def print_summary(recs):
    """Print a summary table of recommendations."""
    print(f"\n{'='*70}")
    print(f"{'Peak':>4} {'Mode':>8} {'Size(um)':>12} {'Res(A)':>7} "
          f"{'Beam':>6} {'Atten':>5} {'Exp(s)':>7} {'Dose':>7}")
    print(f"{'-'*70}")
    for i, rec in enumerate(recs):
        pos = rec.get("crystal_position") or {}
        dims = pos.get("dimensions_um", [])
        mode = pos.get("collection_mode", "?")
        size_str = "x".join(str(d) for d in dims[:3]) if dims else "?"
        beam = rec.get("beam_size_um", [])
        beam_str = str(beam[0]) if beam else "?"
        print(f"{i+1:>4} {mode:>8} {size_str:>12} "
              f"{rec.get('resolution_A', '?'):>7} "
              f"{beam_str:>6} {rec.get('attenuation', '?'):>5} "
              f"{rec.get('exposure_time_s', '?'):>7} "
              f"{rec.get('target_dose_mgy', '?'):>7}")
    print(f"{'='*70}\n")


def collect_single(rpc, rec, energy_kev=None):
    """Collect a single peak in SINGLE mode."""
    pos = rec.get("crystal_position") or {}
    motor = pos.get("motor_position")

    # 1. Move to crystal position
    if motor:
        logger.info(f"Moving to crystal: x={motor['sample_x']:.4f}, "
                    f"y={motor['sample_y']:.4f}, z={motor['sample_z']:.4f}")
        rpc.move_motors(x=motor["sample_x"],
                        y=motor["sample_y"],
                        z=motor["sample_z"])
    else:
        logger.warning("No motor position available — skipping sample move")

    # 2. Energy (if specified or different from current)
    if energy_kev:
        logger.info(f"Setting energy: {energy_kev:.4f} keV")
        rpc.set_energy(energy_kev)

    # 3. Beam size
    beam = rec.get("beam_size_um")
    if beam:
        logger.info(f"Setting beam size: {beam[0]}x{beam[1]} um")
        rpc.set_beam_size(beam[0], beam[1])

    # 4. Attenuation
    atten = rec.get("attenuation")
    if atten:
        logger.info(f"Setting attenuation: {atten}x")
        rpc.set_attenuation(atten)

    # 5. Detector distance
    det_dist = rec.get("detector_distance_mm")
    if det_dist:
        logger.info(f"Moving detector to {det_dist:.1f} mm")
        rpc.move_detector(det_dist)

    # 6. Create run and collect
    logger.info(f"Creating SINGLE run: "
                f"omega {rec.get('start_angle', 0)}-{rec.get('end_angle', 360)}°, "
                f"osc {rec.get('osc_width', 0.2)}°, "
                f"exp {rec.get('exposure_time_s', 0.1)}s")
    rpc.run_create(
        mode="SINGLE",
        frame_deg_start=rec.get("start_angle", 0),
        frame_deg_end=rec.get("end_angle", 360),
        delta_deg=rec.get("osc_width", 0.2),
        det_z_mm=det_dist or 350,
        atten_factors=atten or 1,
        expTime_sec=rec.get("exposure_time_s", 0.1),
    )
    logger.info("Starting collection...")
    rpc.collect(wait=True)
    logger.info("Collection complete")


def collect_vector(rpc, rec, energy_kev=None):
    """Collect a rod crystal in VECTOR (helical) mode."""
    pos = rec.get("crystal_position") or {}
    motor_start = pos.get("motor_start")
    motor_end = pos.get("motor_end")

    if not motor_start or not motor_end:
        logger.error("Vector mode requires motor_start and motor_end "
                     "(set compute_motor_positions: true)")
        return

    # 1. Move to vector start
    logger.info(f"Moving to vector start: x={motor_start['sample_x']:.4f}, "
                f"y={motor_start['sample_y']:.4f}, "
                f"z={motor_start['sample_z']:.4f}")
    rpc.move_motors(x=motor_start["sample_x"],
                    y=motor_start["sample_y"],
                    z=motor_start["sample_z"])

    # 2. Energy
    if energy_kev:
        logger.info(f"Setting energy: {energy_kev:.4f} keV")
        rpc.set_energy(energy_kev)

    # 3. Beam, attenuation, detector
    beam = rec.get("beam_size_um")
    if beam:
        rpc.set_beam_size(beam[0], beam[1])
    atten = rec.get("attenuation")
    if atten:
        rpc.set_attenuation(atten)
    det_dist = rec.get("detector_distance_mm")
    if det_dist:
        rpc.move_detector(det_dist)

    # 4. Set vector endpoints in bluice Redis
    # This requires direct Redis access to set run_v fields
    logger.info(f"Setting vector endpoints:")
    logger.info(f"  start: ({motor_start['sample_x']:.4f}, "
                f"{motor_start['sample_y']:.4f}, "
                f"{motor_start['sample_z']:.4f})")
    logger.info(f"  end:   ({motor_end['sample_x']:.4f}, "
                f"{motor_end['sample_y']:.4f}, "
                f"{motor_end['sample_z']:.4f})")

    try:
        import redis as _redis
        from qp2.xio.bluice_params import extract_run_index

        beamline = detect_beamline()
        bl_cfg = _BEAMLINE_CONFIG.get(beamline, {})
        # Get bluice Redis connection
        from qp2.xio.redis_manager import RedisManager
        rm = RedisManager()
        bluice_conn = rm.get_bluice_connection()

        if bluice_conn:
            # Create run first to get run index
            result = rpc.run_create(
                mode="VECTOR",
                frame_deg_start=rec.get("start_angle", 0),
                frame_deg_end=rec.get("end_angle", 360),
                delta_deg=rec.get("osc_width", 0.2),
                det_z_mm=det_dist or 350,
                atten_factors=atten or 1,
                expTime_sec=rec.get("exposure_time_s", 0.1),
            )
            # Set vector-specific fields
            # The run_idx would need to be read from collect_state
            run_idx = bluice_conn.hget("bluice:collect:state", "cur_run_idx")
            if run_idx:
                run_idx = int(run_idx)
                start_str = (f"{motor_start['sample_x']:.4f},"
                             f"{motor_start['sample_y']:.4f},"
                             f"{motor_start['sample_z']:.4f}")
                end_str = (f"{motor_end['sample_x']:.4f},"
                           f"{motor_end['sample_y']:.4f},"
                           f"{motor_end['sample_z']:.4f}")
                bluice_conn.hset(f"bluice:run:v#{run_idx}",
                                 "start_pt", start_str)
                bluice_conn.hset(f"bluice:run:v#{run_idx}",
                                 "end_pt", end_str)
                spacing = pos.get("rod_width_um", 20)
                bluice_conn.hset(f"bluice:run:v#{run_idx}",
                                 "spacing_um", str(spacing))
                logger.info(f"Set vector fields on run {run_idx}")
        else:
            logger.warning("No bluice Redis — cannot set vector endpoints. "
                           "Creating SINGLE run instead.")
            rpc.run_create(
                mode="SINGLE",
                frame_deg_start=rec.get("start_angle", 0),
                frame_deg_end=rec.get("end_angle", 360),
                delta_deg=rec.get("osc_width", 0.2),
                det_z_mm=det_dist or 350,
                atten_factors=atten or 1,
                expTime_sec=rec.get("exposure_time_s", 0.1),
            )
    except Exception as e:
        logger.warning(f"Could not set vector endpoints ({e}). "
                       f"Falling back to SINGLE mode.")
        rpc.run_create(
            mode="SINGLE",
            frame_deg_start=rec.get("start_angle", 0),
            frame_deg_end=rec.get("end_angle", 360),
            delta_deg=rec.get("osc_width", 0.2),
            det_z_mm=det_dist or 350,
            atten_factors=atten or 1,
            expTime_sec=rec.get("exposure_time_s", 0.1),
        )

    # 5. Collect
    logger.info("Starting vector collection...")
    rpc.collect(wait=True)
    logger.info("Collection complete")


def collect_multi_site(rpc, recs, energy_kev=None):
    """Collect multiple peaks in SITE mode."""
    # 1. Add sites to bluice site list
    logger.info(f"Setting up {len(recs)} sites")

    try:
        from qp2.xio.redis_manager import RedisManager
        rm = RedisManager()
        bluice_conn = rm.get_bluice_connection()

        if bluice_conn:
            # Clear existing auto sites
            bluice_conn.delete("bluice:sites:user__l")

            for i, rec in enumerate(recs):
                pos = rec.get("crystal_position") or {}
                motor = pos.get("motor_position")
                if not motor:
                    logger.warning(f"  Peak {i+1}: no motor position, skipping")
                    continue
                site = {
                    "SELECT": 1,
                    "NUMBER": i + 1,
                    "RUN": -1,
                    "RUN_TS": int(time.time()),
                    "DETAILS": (f"Raster3D peak {i+1}: "
                                f"{pos.get('dimensions_um', [])} um"),
                    "XYZ": (f"{motor['sample_x']:.4f},"
                            f"{motor['sample_y']:.4f},"
                            f"{motor['sample_z']:.4f}"),
                }
                bluice_conn.rpush("bluice:sites:user__l", json.dumps(site))
                logger.info(f"  Site {i+1}: {site['XYZ']} — {site['DETAILS']}")

            bluice_conn.incr("bluice:sites:user_ver__s")
        else:
            logger.error("No bluice Redis — cannot set sites. "
                         "Falling back to sequential SINGLE collections.")
            for i, rec in enumerate(recs):
                logger.info(f"\n--- Peak {i+1}/{len(recs)} ---")
                collect_single(rpc, rec, energy_kev)
            return
    except Exception as e:
        logger.error(f"Could not set sites ({e}). "
                     f"Falling back to sequential SINGLE collections.")
        for i, rec in enumerate(recs):
            logger.info(f"\n--- Peak {i+1}/{len(recs)} ---")
            collect_single(rpc, rec, energy_kev)
        return

    # 2. Use first recommendation for collection parameters
    rec0 = recs[0]

    # 3. Energy
    if energy_kev:
        logger.info(f"Setting energy: {energy_kev:.4f} keV")
        rpc.set_energy(energy_kev)

    # 4. Beam, attenuation, detector from best peak
    beam = rec0.get("beam_size_um")
    if beam:
        rpc.set_beam_size(beam[0], beam[1])
    atten = rec0.get("attenuation")
    if atten:
        rpc.set_attenuation(atten)
    det_dist = rec0.get("detector_distance_mm")
    if det_dist:
        rpc.move_detector(det_dist)

    # 5. Create SITE run
    logger.info(f"Creating SITE run with {len(recs)} sites")
    rpc.run_create(
        mode="SITE",
        frame_deg_start=rec0.get("start_angle", 0),
        frame_deg_end=rec0.get("end_angle", 360),
        delta_deg=rec0.get("osc_width", 0.2),
        det_z_mm=det_dist or 350,
        atten_factors=atten or 1,
        expTime_sec=rec0.get("exposure_time_s", 0.1),
    )

    # 6. Collect
    logger.info("Starting multi-site collection...")
    rpc.collect(wait=True)
    logger.info("Multi-site collection complete")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Collect data from raster3d pipeline recommendations.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("results_json",
                        help="Path to raster3d results.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print actions without executing")
    parser.add_argument("--all-peaks", action="store_true",
                        help="Collect all peaks (multi-site mode). "
                             "Default: collect best peak only.")
    parser.add_argument("--peak", type=int, default=None,
                        help="Collect a specific peak index (1-based)")
    parser.add_argument("--beamline", default=None,
                        help="Beamline ID (default: auto-detect from hostname)")

    # Parameter overrides
    parser.add_argument("--energy", type=float, default=None,
                        help="Override energy in keV (triggers mono move)")
    parser.add_argument("--attenuation", type=float, default=None,
                        help="Override attenuation factor")
    parser.add_argument("--exposure", type=float, default=None,
                        help="Override exposure time (seconds)")
    parser.add_argument("--detector-distance", type=float, default=None,
                        help="Override detector distance (mm)")
    parser.add_argument("--start-angle", type=float, default=None,
                        help="Override start angle (degrees)")
    parser.add_argument("--end-angle", type=float, default=None,
                        help="Override end angle (degrees)")
    parser.add_argument("--osc-width", type=float, default=None,
                        help="Override oscillation width (degrees)")

    parser.add_argument("--no-confirm", action="store_true",
                        help="Skip confirmation prompt")

    args = parser.parse_args()

    # Load results
    with open(args.results_json) as f:
        data = json.load(f)

    # Support both compact and full results formats
    recs = data.get("recommendations") or data.get("stages", {}).get("recommendations", [])
    if not recs:
        logger.error("No recommendations found in results.json")
        sys.exit(1)

    # Normalize compact format: flatten collection/crystal/dose into top-level
    for rec in recs:
        for section in ("collection", "crystal", "dose"):
            if section in rec:
                for k, v in rec[section].items():
                    rec.setdefault(k, v)

    # Apply overrides
    for rec in recs:
        if args.attenuation is not None:
            rec["attenuation"] = args.attenuation
        if args.exposure is not None:
            rec["exposure_time_s"] = args.exposure
        if args.detector_distance is not None:
            rec["detector_distance_mm"] = args.detector_distance
        if args.start_angle is not None:
            rec["start_angle"] = args.start_angle
        if args.end_angle is not None:
            rec["end_angle"] = args.end_angle
        if args.osc_width is not None:
            rec["osc_width"] = args.osc_width

    # Select peaks
    if args.peak is not None:
        if args.peak < 1 or args.peak > len(recs):
            logger.error(f"Peak {args.peak} out of range (1-{len(recs)})")
            sys.exit(1)
        recs = [recs[args.peak - 1]]
    elif not args.all_peaks:
        recs = [recs[0]]

    # Energy: use override, or from recommendation, or None (no change)
    energy_kev = args.energy
    if energy_kev is None:
        rec_energy = recs[0].get("energy_kev")
        if rec_energy:
            energy_kev = float(rec_energy)
            # Only move energy if explicitly requested via --energy
            energy_kev = None  # don't move by default

    # Print summary
    print_summary(recs)

    # Detect beamline
    beamline = args.beamline or detect_beamline()
    bl_cfg = _BEAMLINE_CONFIG.get(beamline)
    if not bl_cfg:
        logger.error(f"Unknown beamline: {beamline}")
        sys.exit(1)

    rpc = BluiceRPC(bl_cfg["rpc"], dry_run=args.dry_run)

    logger.info(f"Beamline: {beamline}")
    logger.info(f"RPC URL:  {bl_cfg['rpc']}")
    logger.info(f"Peaks:    {len(recs)}")
    logger.info(f"Dry run:  {args.dry_run}")
    if energy_kev:
        logger.info(f"Energy:   {energy_kev:.4f} keV")
    else:
        logger.info(f"Energy:   no change (current beamline energy)")

    # Confirm
    if not args.dry_run and not args.no_confirm:
        resp = input("\nProceed with collection? [y/N] ").strip().lower()
        if resp != "y":
            logger.info("Aborted by user")
            sys.exit(0)

    # --- Execute collection ---
    t0 = time.time()

    # Prepare sample environment
    logger.info("Setting sample environment: beamstopIN backlightOUT")
    rpc.sample_env("beamstopIN backlightOUT")

    if len(recs) == 1:
        rec = recs[0]
        pos = rec.get("crystal_position") or {}
        mode = pos.get("collection_mode", "standard")

        if mode == "vector":
            collect_vector(rpc, rec, energy_kev)
        else:
            collect_single(rpc, rec, energy_kev)
    else:
        # Multiple peaks
        has_vectors = any(
            (r.get("crystal_position") or {}).get("collection_mode") == "vector"
            for r in recs
        )
        if has_vectors:
            # Mixed modes — collect sequentially
            for i, rec in enumerate(recs):
                logger.info(f"\n{'='*40}")
                logger.info(f"Peak {i+1}/{len(recs)}")
                logger.info(f"{'='*40}")
                pos = rec.get("crystal_position") or {}
                if pos.get("collection_mode") == "vector":
                    collect_vector(rpc, rec, energy_kev)
                else:
                    collect_single(rpc, rec, energy_kev)
        else:
            # All standard — use SITE mode
            collect_multi_site(rpc, recs, energy_kev)

    # Restore sample environment
    logger.info("Restoring sample environment: backlightIN")
    rpc.sample_env("backlightIN")

    elapsed = time.time() - t0
    logger.info(f"\nAll collections completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
