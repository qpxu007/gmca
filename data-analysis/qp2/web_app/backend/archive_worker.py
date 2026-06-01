"""
archive_worker.py — Background scan + status-sync loops for the archive tracker.
Imported by main.py; never runs standalone.
"""

import asyncio
import json
import logging
import os
import shlex
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

AGENT_PATH = os.environ.get("QP2_DM_AGENT_PATH", "/mnt/beegfs/qxu/data-analysis/qp2/dm_gmca/dm_agent.py")
DM_SETUP = os.environ.get("QP2_DM_SETUP", "source /home/dm/etc/dm.setup.sh; source /mnt/beegfs/dmadmin/.bashrc")
DM_NODE = os.environ.get("QP2_DM_NODE", "bl2ws5")
GLOBUS_ENDPOINT = os.environ.get("QP2_GLOBUS_ENDPOINT", "4be6a66d-291e-4383-987e-c3c0162d645c")
ARCHIVE_DATA_ROOT = os.environ.get("QP2_ARCHIVE_DATA_ROOT", "/mnt/beegfs/DATA")
ARCHIVE_PROCESSING_ROOT = os.environ.get("QP2_ARCHIVE_PROCESSING_ROOT", "/mnt/beegfs/PROCESSING")
APS_ARCHIVE_ROOT = os.environ.get("QP2_APS_ARCHIVE_ROOT", "/DATA")
STALL_THRESHOLD_HOURS = 2

# Injected by main.py after DB init
_session_factory = None


def set_session_factory(factory):
    global _session_factory
    _session_factory = factory


def _get_session():
    if _session_factory is None:
        raise RuntimeError("session_factory not set")
    return _session_factory()


def _build_globus_url(esaf_id: str, run_name: str, dir_type: str) -> Optional[str]:
    if not run_name:
        return None
    base = f"https://app.globus.org/file-manager?origin_id={GLOBUS_ENDPOINT}&origin_path="
    root = APS_ARCHIVE_ROOT.rstrip("/")
    if dir_type == "DATA":
        return base + f"{root}/{run_name}/esaf{esaf_id}/data"
    else:
        return base + f"{root}/{run_name}/esaf{esaf_id}/analysis"


async def run_agent(args: list, timeout: int = 300) -> Optional[dict | list]:
    """Call dm_agent.py on bl2ws5 via srun. Returns parsed JSON or None on error."""
    quoted_args = " ".join(shlex.quote(a) for a in args)
    dm_cmd = f"{DM_SETUP}; python {shlex.quote(AGENT_PATH)} {quoted_args}"
    loop = asyncio.get_running_loop()
    result = None
    try:
        result = await loop.run_in_executor(
            None,
            lambda: subprocess.run(
                ["srun", f"--nodelist={DM_NODE}", "bash", "-c", dm_cmd],
                capture_output=True, text=True, timeout=timeout,
            )
        )
        if result.returncode != 0:
            logger.warning(f"dm_agent stderr: {result.stderr[:500]}")
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        stdout_preview = result.stdout[:200] if result else "(no output)"
        logger.error(f"dm_agent returned invalid JSON: {e}. stdout: {stdout_preview}")
        return None
    except Exception as e:
        logger.error(f"dm_agent invocation failed: {e}")
        return None


def _build_skip_dirs(session) -> list:
    """Return paths that are done and have no new data since last archive."""
    from qp2.db import ArchiveJob
    done_jobs = session.query(ArchiveJob).filter(
        ArchiveJob.status == "done",
        ArchiveJob.dir_mtime_at_submit.isnot(None),
    ).all()
    skip = []
    for job in done_jobs:
        try:
            current_mtime = os.path.getmtime(job.data_directory)
            if current_mtime <= job.dir_mtime_at_submit:
                skip.append(job.data_directory)
        except OSError:
            skip.append(job.data_directory)  # dir gone — skip it
    return skip


def _upsert_jobs(session, agent_result: dict):
    """Insert or update ArchiveJob rows from agent scan result."""
    from qp2.db import ArchiveJob
    submitted_count = 0

    for item in agent_result.get("submitted", []):
        existing = session.query(ArchiveJob).filter_by(
            data_directory=item["data_directory"]
        ).first()
        globus_url = _build_globus_url(
            item["esaf_id"], item.get("run_name"), item["dir_type"]
        )
        if existing:
            existing.dm_job_id = item.get("dm_job_id")
            existing.run_name = item.get("run_name")
            existing.status = "submitted"
            existing.dir_mtime_at_submit = item.get("dir_mtime")
            existing.submitted_at = datetime.now(timezone.utc).replace(tzinfo=None)
            existing.globus_url = globus_url or existing.globus_url
            existing.error_message = item.get("error")
        else:
            job = ArchiveJob(
                dm_job_id=item.get("dm_job_id"),
                esaf_id=item["esaf_id"],
                experiment_name=item["experiment_name"],
                data_directory=item["data_directory"],
                dir_type=item["dir_type"],
                run_name=item.get("run_name"),
                status="submitted",
                dir_mtime_at_submit=item.get("dir_mtime"),
                submitted_at=datetime.now(timezone.utc).replace(tzinfo=None),
                globus_url=globus_url,
                error_message=item.get("error"),
            )
            session.add(job)
        submitted_count += 1

    for item in agent_result.get("permission_denied", []):
        existing = session.query(ArchiveJob).filter_by(
            data_directory=item["data_directory"]
        ).first()
        if existing:
            existing.status = "permission_denied"
        else:
            session.add(ArchiveJob(
                esaf_id=item["esaf_id"],
                experiment_name=f"esaf{item['esaf_id']}",
                data_directory=item["data_directory"],
                dir_type=item["dir_type"],
                status="permission_denied",
                submitted_at=datetime.now(timezone.utc).replace(tzinfo=None),
            ))

    session.commit()
    return submitted_count


async def run_scan_job(scan_type: str = "scheduled", days: int = 7,
                       dry_run: bool = False) -> dict:
    """Core scan logic — shared by scheduled loop, POST /archive/scan, POST /archive/audit."""
    from qp2.db import ArchiveScanLog
    session = _get_session()
    try:
        # Check for stalled/running scan
        latest = session.query(ArchiveScanLog).order_by(
            ArchiveScanLog.started_at.desc()
        ).first()
        if latest and latest.status == "running":
            age = datetime.now(timezone.utc).replace(tzinfo=None) - latest.started_at
            if age < timedelta(hours=STALL_THRESHOLD_HOURS):
                logger.info("Previous scan still running, skipping this trigger")
                session.close()
                return {"skipped": True, "reason": "scan already running"}
            else:
                latest.status = "failed"
                latest.error_message = f"Stalled after {age}"
                session.commit()

        if not dry_run:
            log_entry = ArchiveScanLog(
                started_at=datetime.now(timezone.utc).replace(tzinfo=None),
                status="running",
                scan_type=scan_type,
            )
            session.add(log_entry)
            session.commit()
        else:
            log_entry = None

        # Build skip_dirs in all cases — ensures dry-run preview matches what a real run would submit
        skip_dirs = _build_skip_dirs(session)

        agent_args = ["--action", "scan", "--days", str(days),
                      "--data-root", ARCHIVE_DATA_ROOT,
                      "--processing-root", ARCHIVE_PROCESSING_ROOT,
                      "--aps-root", APS_ARCHIVE_ROOT]
        if dry_run:
            agent_args.append("--dry-run")
        if skip_dirs:
            agent_args.extend(["--skip-dirs"] + skip_dirs)

        agent_result = await run_agent(agent_args, timeout=600)
        if agent_result is None:
            if log_entry:
                log_entry.status = "failed"
                log_entry.error_message = "Agent returned no output"
                log_entry.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
                session.commit()
            return {"error": "agent failed"}

        submitted_count = 0
        if not dry_run:
            submitted_count = _upsert_jobs(session, agent_result)
            log_entry.status = "done"
            log_entry.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            log_entry.jobs_submitted = submitted_count
            log_entry.jobs_skipped = len(skip_dirs)
            session.commit()

        return {
            "dry_run": dry_run,
            "scan_type": scan_type,
            "submitted": agent_result.get("submitted", []),
            "permission_denied": agent_result.get("permission_denied", []),
            "jobs_submitted": submitted_count,
        }
    except Exception as e:
        logger.error(f"run_scan_job failed: {e}")
        return {"error": str(e)}
    finally:
        session.close()


async def run_status_sync():
    """Poll DM for upload status and update DB."""
    from qp2.db import ArchiveJob
    agent_result = await run_agent(["--action", "list-uploads"], timeout=60)
    if not agent_result:
        return
    if not isinstance(agent_result, list):
        logger.error(f"list-uploads returned unexpected type: {type(agent_result)}")
        return
    # Filter out error sentinel records
    uploads = [r for r in agent_result if "error" not in r]
    session = _get_session()
    try:
        for item in uploads:
            job = session.query(ArchiveJob).filter_by(
                dm_job_id=item.get("dm_job_id")
            ).first()
            if not job:
                continue
            old_status = job.status
            job.status = item.get("status", job.status)
            job.count_files = item.get("count_files", job.count_files)
            job.last_synced_at = datetime.now(timezone.utc).replace(tzinfo=None)
            job.error_message = item.get("error_message")
            if old_status != "done" and job.status == "done":
                job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        session.commit()
    finally:
        session.close()


# Scheduling for these work functions lives in main.py's APScheduler so cron
# state is shared across uvicorn workers via the SQLAlchemyJobStore. The
# wrappers below let the sync scheduler thread invoke the async work.

def run_scan_job_scheduled():
    """APScheduler entry point — runs the scheduled archive scan."""
    try:
        asyncio.run(run_scan_job(scan_type="scheduled", days=7))
    except Exception as e:
        logger.error(f"Scheduled scan failed: {e}")


def run_status_sync_scheduled():
    """APScheduler entry point — runs the DM status sync."""
    try:
        asyncio.run(run_status_sync())
    except Exception as e:
        logger.error(f"Scheduled status sync failed: {e}")
