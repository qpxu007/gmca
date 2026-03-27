# qp2/pipelines/raster_3d/tracker.py

"""Tracks completed RASTER runs for 3D raster pair detection."""

import re
import threading
import time
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

from qp2.pipelines.raster_3d.scan_mode import VALID_SCAN_MODES

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


class RasterRunTracker:
    """Remembers completed RASTER runs to detect consecutive orthogonal pairs.

    When two RASTER runs with the same base name and consecutive run numbers
    both complete, the tracker returns the pair so the 3D pipeline can be launched.

    Thread-safe. Entries expire after ``ttl_seconds`` to avoid unbounded growth
    when a single raster run never gets its partner.
    """

    def __init__(self, ttl_seconds: int = 3600):
        self._completed: OrderedDict[str, Dict] = OrderedDict()
        self._lock = threading.Lock()
        self._ttl = ttl_seconds

    # ------------------------------------------------------------------
    # Run-prefix parsing
    # ------------------------------------------------------------------

    @staticmethod
    def parse_run_prefix(run_prefix: str) -> Optional[Tuple[str, int]]:
        """Extract ``(base_name, run_number)`` from a run prefix.

        Examples::

            "sample_ras_run1"  → ("sample_ras_run", 1)
            "B1_ras_run3"      → ("B1_ras_run", 3)
            "no_number_here"   → None
        """
        match = re.match(r"^(.+?)(\d+)$", run_prefix)
        if match:
            return match.group(1), int(match.group(2))
        return None

    # ------------------------------------------------------------------
    # Registration & pair detection
    # ------------------------------------------------------------------

    def register_completed_raster(
        self,
        run_prefix: str,
        master_files: List[str],
        metadata_list: List[Dict],
        data_dir: str,
        scan_mode: str = "row_wise",
    ) -> Optional[Tuple[Dict, Dict]]:
        """Register a completed raster run and check for a consecutive partner.

        Returns ``(run1_info, run2_info)`` sorted by run number if a pair is
        found, or ``None`` if no partner exists yet.  Both entries are removed
        from the tracker on match to prevent re-triggering.
        """
        parsed = self.parse_run_prefix(run_prefix)
        if parsed is None:
            logger.warning(
                f"RasterRunTracker: cannot parse run number from '{run_prefix}', "
                f"skipping 3D pair detection."
            )
            return None

        base, run_num = parsed

        if not master_files:
            logger.warning(
                f"RasterRunTracker: no master files for '{run_prefix}', skipping."
            )
            return None
        if scan_mode not in VALID_SCAN_MODES:
            logger.warning(
                f"RasterRunTracker: invalid scan mode '{scan_mode}' for '{run_prefix}', skipping."
            )
            return None

        collect_mode = self._get_collect_mode(metadata_list)
        if collect_mode not in {None, "RASTER"}:
            logger.info(
                f"RasterRunTracker: run '{run_prefix}' collect_mode={collect_mode}, "
                "not eligible for 3D raster pairing."
            )
            return None

        this_info = {
            "run_prefix": run_prefix,
            "base": base,
            "run_num": run_num,
            "master_files": list(master_files),
            "metadata_list": list(metadata_list),
            "data_dir": data_dir,
            "scan_mode": scan_mode,
            "collect_mode": collect_mode,
            "timestamp": time.time(),
        }

        with self._lock:
            self._purge_expired()

            # Store this run
            self._completed[run_prefix] = this_info
            self._completed.move_to_end(run_prefix)

            # Look for a consecutive partner (run_num ± 1)
            for partner_num in (run_num - 1, run_num + 1):
                partner_prefix = f"{base}{partner_num}"
                if partner_prefix in self._completed:
                    partner_info = self._completed[partner_prefix]
                    if not self._is_valid_pair(this_info, partner_info):
                        logger.info(
                            f"RasterRunTracker: adjacent runs '{run_prefix}' and "
                            f"'{partner_prefix}' are not a valid 3D pair."
                        )
                        continue

                    partner_info = self._completed.pop(partner_prefix)
                    self._completed.pop(run_prefix, None)

                    if this_info["run_num"] < partner_info["run_num"]:
                        pair = (this_info, partner_info)
                    else:
                        pair = (partner_info, this_info)

                    logger.info(
                        f"RasterRunTracker: 3D pair detected — "
                        f"'{pair[0]['run_prefix']}' + '{pair[1]['run_prefix']}'"
                    )
                    return pair

        logger.info(
            f"RasterRunTracker: registered '{run_prefix}' (run #{run_num}), "
            f"waiting for partner."
        )
        return None

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def _purge_expired(self) -> None:
        """Remove entries older than TTL (must be called under lock)."""
        if self._ttl <= 0:
            return
        cutoff = time.time() - self._ttl
        while self._completed:
            key, info = next(iter(self._completed.items()))
            if info["timestamp"] < cutoff:
                self._completed.popitem(last=False)
                logger.debug(f"RasterRunTracker: expired '{key}'")
            else:
                break

    @staticmethod
    def _get_collect_mode(metadata_list: List[Dict]) -> Optional[str]:
        for metadata in metadata_list or []:
            value = metadata.get("collect_mode")
            if value is not None:
                return str(value).upper()
        return None

    @staticmethod
    def _is_valid_pair(run_a: Dict, run_b: Dict) -> bool:
        if run_a.get("base") != run_b.get("base"):
            return False
        if abs(run_a.get("run_num", -1) - run_b.get("run_num", -1)) != 1:
            return False
        if run_a.get("data_dir") != run_b.get("data_dir"):
            return False

        collect_a = run_a.get("collect_mode")
        collect_b = run_b.get("collect_mode")
        if collect_a not in {None, "RASTER"} or collect_b not in {None, "RASTER"}:
            return False

        mode_a = run_a.get("scan_mode")
        mode_b = run_b.get("scan_mode")
        if mode_a not in VALID_SCAN_MODES or mode_b not in VALID_SCAN_MODES:
            return False
        if mode_a == mode_b:
            return False

        return True

    def clear(self) -> None:
        """Remove all tracked entries."""
        with self._lock:
            self._completed.clear()
