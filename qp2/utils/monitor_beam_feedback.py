#!/usr/bin/env python3
"""
Monitor script for APS ring current and feedback status.
Sends an email when the beam is lost or when the feedback lock/pause status changes.
"""
import argparse
import time
import smtplib
from email.message import EmailMessage
import numpy as np
from epics import caget
import socket

# =============================================================================
# Configuration
# =============================================================================
EMAIL_FROM = "qxu@anl.gov"
SMTP_SERVER = "localhost"  # Update this if your facility uses a specific SMTP relay
POLL_INTERVAL_SEC = 60     # How often to check the PVs
BEAM_LOST_THRESHOLD_mA = 5.0 # Current in mA below which we consider the beam "lost"
DEFAULT_COOLDOWN_SEC = 300 # Default 5 minute cooldown before repeating the same alert email

# EPICS PVs
PV_UOPS = "S:UserOpsCurrent"

hostname = socket.gethostname()
if hostname.startswith("bl2"):
    prefix = "23o"
elif hostname.startswith("bl1"):
    prefix = "23i"
else:
    prefix = None

if prefix:
    PV_LOCK = f"{prefix}:mostab:lock"
    PV_PAUSE = f"{prefix}:mostab:pause"
else:
    PV_LOCK = None
    PV_PAUSE = None

# =============================================================================
# Functions
# =============================================================================
def send_email(subject, body, email_addresses, last_email_times, cooldown_sec):
    """Sends an email notification if not blocked by the cooldown."""
    if not email_addresses:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ALERT (No Email Configured): {subject}")
        return

    # Rate limiting check
    now = time.time()
    if subject in last_email_times:
        time_since_last = now - last_email_times[subject]
        if time_since_last < cooldown_sec:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] SKIP (Cooldown): {subject}")
            return

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_FROM
    msg['To'] = ", ".join(email_addresses)

    try:
        with smtplib.SMTP(SMTP_SERVER) as s:
            s.send_message(msg)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Email sent: {subject}")
        last_email_times[subject] = now
    except Exception as e:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Failed to send email: {e}")

def get_latest_current():
    """Fetches the 1440-point history array and returns the most recent current."""
    uops_mA = caget(PV_UOPS, count=1440, as_numpy=True)
    if uops_mA is None:
        return None
    uops_mA = np.asarray(uops_mA, dtype=float).ravel()
    # The last element is the latest reading
    return uops_mA[-1]

def monitor_loop(email_addresses, cooldown_sec):
    """Main polling loop to monitor PVs and trigger alerts."""
    # State tracking
    last_beam_state = None  # True if beam is up, False if lost
    last_lock_state = None
    last_pause_state = None

    # Rate limiting tracking (subject -> epoch timestamp)
    last_email_times = {}

    print(f"Starting beam and feedback monitor.")
    print(f"Hostname detected: {hostname}")
    print(f"Monitoring PVs... Lock: {PV_LOCK}, Pause: {PV_PAUSE}")
    
    if email_addresses:
        print(f"Polling every {POLL_INTERVAL_SEC} seconds. Alerts going to: {', '.join(email_addresses)}")
        print(f"Alert cooldown set to {cooldown_sec} seconds to prevent spam.")
    else:
        print(f"Polling every {POLL_INTERVAL_SEC} seconds. NO EMAILS CONFIGURED.")

    while True:
        try:
            # 1. Check beam current
            current_mA = get_latest_current()
            if current_mA is not None:
                beam_is_up = current_mA > BEAM_LOST_THRESHOLD_mA
                if last_beam_state is None:
                    last_beam_state = beam_is_up # Initialize state without alerting
                elif last_beam_state and not beam_is_up:
                    send_email(
                        "ALERT: Beam Lost", 
                        f"UserOpsCurrent has dropped to {current_mA:.2f} mA.",
                        email_addresses, last_email_times, cooldown_sec
                    )
                    last_beam_state = False
                elif not last_beam_state and beam_is_up:
                    send_email(
                        "INFO: Beam Restored", 
                        f"UserOpsCurrent is now {current_mA:.2f} mA.",
                        email_addresses, last_email_times, cooldown_sec
                    )
                    last_beam_state = True

            # 2. Check feedback lock status
            if PV_LOCK:
                lock_val = caget(PV_LOCK)
                if lock_val is not None:
                    if last_lock_state is None:
                        last_lock_state = lock_val # Initialize
                    elif lock_val != last_lock_state:
                        if lock_val == 1:
                            send_email(
                                "INFO: Feedback Lock Restored", 
                                f"PV {PV_LOCK} is now Locked ({lock_val})",
                                email_addresses, last_email_times, cooldown_sec
                            )
                        else:
                            send_email(
                                "ALERT: Feedback Lock Lost", 
                                f"PV {PV_LOCK} is now NotLocked ({lock_val})",
                                email_addresses, last_email_times, cooldown_sec
                            )
                        last_lock_state = lock_val

            # 3. Check feedback pause status
            if PV_PAUSE:
                pause_val = caget(PV_PAUSE)
                if pause_val is not None:
                    if last_pause_state is None:
                        last_pause_state = pause_val # Initialize
                    elif pause_val != last_pause_state:
                        if pause_val == 0:
                            send_email(
                                "INFO: Feedback Status Restored", 
                                f"PV {PV_PAUSE} is now Run ({pause_val})",
                                email_addresses, last_email_times, cooldown_sec
                            )
                        else:
                            send_email(
                                "ALERT: Feedback Paused", 
                                f"PV {PV_PAUSE} is now Pause ({pause_val})",
                                email_addresses, last_email_times, cooldown_sec
                            )
                        last_pause_state = pause_val

        except Exception as e:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Error in monitor loop: {e}")

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor APS ring current and feedback status.")
    parser.add_argument(
        "-e", "--email", 
        action="append", 
        help="Email address to send alerts to. Can specify multiple times (e.g. -e one@aps.anl.gov -e two@aps.anl.gov)"
    )
    parser.add_argument(
        "-c", "--cooldown",
        type=int,
        default=DEFAULT_COOLDOWN_SEC,
        help=f"Minimum seconds between identical email alerts (default: {DEFAULT_COOLDOWN_SEC})"
    )
    args = parser.parse_args()
    
    monitor_loop(args.email, args.cooldown)
