import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

try:
    from qp2.db import ArchiveJob, ArchiveScanLog
except ImportError:
    ArchiveJob = None
    ArchiveScanLog = None

try:
    from qp2.web_app.backend.auth import is_staff_member
    from qp2.web_app.backend.security import verify_token
except ImportError:
    def is_staff_member(u): return False
    def verify_token(): return "user"

try:
    from qp2.xio.user_group_manager import UserGroupManager
except ImportError:
    UserGroupManager = None

try:
    from qp2.web_app.backend import archive_worker
except ImportError:
    archive_worker = None
from qp2.log.logging_config import get_logger
logger = get_logger(__name__)

ugm = UserGroupManager() if UserGroupManager else None

router = APIRouter(prefix="/archive", tags=["archive"])

STALL_HOURS = 2


def get_db_session():
    raise RuntimeError("get_db_session not overridden")


def _get_allowed_esafs(user: str) -> List[str]:
    """Return list of esaf_ids the user may view (their username + ESAF group numbers)."""
    allowed = [user]
    if ugm:
        try:
            groups = ugm.groupnames_from_username(user)
            if groups:
                for g in groups:
                    name = g.get("group_name", "")
                    if name.startswith("esaf"):
                        allowed.append(name[4:])  # bare number only
        except Exception:
            pass
    return allowed


def _safe_error(job, is_staff: bool) -> Optional[str]:
    if not job.error_message:
        return None
    return job.error_message if is_staff else "Transfer failed — contact staff"


def _job_dict(job, is_staff: bool) -> dict:
    return {
        "id": job.id,
        "dm_job_id": job.dm_job_id,
        "esaf_id": job.esaf_id,
        "experiment_name": job.experiment_name,
        "data_directory": job.data_directory,
        "dir_type": job.dir_type,
        "run_name": job.run_name,
        "status": job.status,
        "count_files": job.count_files,
        "globus_url": job.globus_url,
        "submitted_at": job.submitted_at.isoformat() if job.submitted_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "last_synced_at": job.last_synced_at.isoformat() if job.last_synced_at else None,
        "error_message": _safe_error(job, is_staff),
    }


@router.get("/jobs")
def list_jobs(
    esaf_id: Optional[str] = None,
    status: Optional[str] = None,
    dir_type: Optional[str] = None,
    limit: int = 100,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    if ArchiveJob is None:
        raise HTTPException(status_code=500, detail="Models not loaded")
    is_staff = is_staff_member(user)
    q = session.query(ArchiveJob)
    if not is_staff:
        allowed = _get_allowed_esafs(user)
        q = q.filter(ArchiveJob.esaf_id.in_(allowed))
    if esaf_id:
        q = q.filter(ArchiveJob.esaf_id == esaf_id)
    if status:
        q = q.filter(ArchiveJob.status == status)
    if dir_type:
        q = q.filter(ArchiveJob.dir_type == dir_type)
    jobs = q.order_by(ArchiveJob.submitted_at.desc()).limit(limit).all()
    return [_job_dict(j, is_staff) for j in jobs]


@router.get("/jobs/{job_id}")
def get_job(
    job_id: int,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    if ArchiveJob is None:
        raise HTTPException(status_code=500, detail="Models not loaded")
    is_staff = is_staff_member(user)
    job = session.get(ArchiveJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if not is_staff:
        allowed = _get_allowed_esafs(user)
        if job.esaf_id not in allowed:
            raise HTTPException(status_code=403, detail="Access denied")
    return _job_dict(job, is_staff)


@router.get("/status")
def get_status(
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    if ArchiveScanLog is None or ArchiveJob is None:
        raise HTTPException(status_code=500, detail="Models not loaded")

    latest = session.query(ArchiveScanLog).order_by(
        ArchiveScanLog.started_at.desc()
    ).first()

    stalled = False
    if latest and latest.status == "running":
        age = datetime.now(timezone.utc).replace(tzinfo=None) - latest.started_at
        stalled = age > timedelta(hours=STALL_HOURS)

    counts = {}
    for status_val in ("submitted", "pending", "running", "done", "failed", "permission_denied"):
        counts[status_val] = session.query(ArchiveJob).filter(
            ArchiveJob.status == status_val
        ).count()

    return {
        "last_scan": {
            "started_at": latest.started_at.isoformat() if latest else None,
            "completed_at": latest.completed_at.isoformat() if latest and latest.completed_at else None,
            "status": latest.status if latest else None,
            "scan_type": latest.scan_type if latest else None,
            "stalled": stalled,
        },
        "job_counts": counts,
    }


@router.post("/scan")
async def trigger_scan(
    dry_run: bool = False,
    user: str = Depends(verify_token),
):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    if archive_worker is None:
        raise HTTPException(status_code=500, detail="archive_worker not available")
    result = await archive_worker.run_scan_job(
        scan_type="manual", days=7, dry_run=dry_run
    )
    return result


@router.post("/audit")
async def trigger_audit(
    dry_run: bool = False,
    user: str = Depends(verify_token),
):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    if archive_worker is None:
        raise HTTPException(status_code=500, detail="archive_worker not available")
    result = await archive_worker.run_scan_job(
        scan_type="audit", days=0, dry_run=dry_run
    )
    return result


class ReuploadRequest(BaseModel):
    ids: List[int]
    skip_completed: bool = True
    dry_run: bool = False


@router.post("/reupload")
async def bulk_reupload(
    req: ReuploadRequest,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    if archive_worker is None:
        raise HTTPException(status_code=500, detail="archive_worker not available")
    if ArchiveJob is None:
        raise HTTPException(status_code=500, detail="Models not loaded")

    submitted = []
    skipped = []

    for job_id in req.ids:
        job = session.get(ArchiveJob, job_id)
        if not job:
            skipped.append({"id": job_id, "reason": "not found"})
            continue
        if req.skip_completed and job.status == "done":
            skipped.append({"id": job_id, "reason": "already done"})
            continue
        _data_root = os.environ.get("QP2_ARCHIVE_DATA_ROOT", "/mnt/beegfs/DATA")
        _proc_root = os.environ.get("QP2_ARCHIVE_PROCESSING_ROOT", "/mnt/beegfs/PROCESSING")
        if not (job.data_directory.startswith(_data_root) or
                job.data_directory.startswith(_proc_root)):
            skipped.append({"id": job_id, "reason": "invalid path"})
            continue

        agent_args = ["--action", "reupload", "--dir", job.data_directory]
        if req.dry_run:
            agent_args.append("--dry-run")

        result = await archive_worker.run_agent(agent_args, timeout=120)
        if result and result.get("submitted"):
            item = result["submitted"][0]
            if not req.dry_run and item.get("dm_job_id"):
                job.dm_job_id = item["dm_job_id"]
                job.status = "submitted"
                job.submitted_at = datetime.now(timezone.utc).replace(tzinfo=None)
                job.dir_mtime_at_submit = item.get("dir_mtime")
                session.commit()
            submitted.append({
                "id": job_id,
                "esaf_id": job.esaf_id,
                "data_directory": job.data_directory,
                "planned_command": item.get("planned_command"),
                "dm_job_id": item.get("dm_job_id"),
                "dry_run": req.dry_run,
            })
        else:
            skipped.append({"id": job_id, "reason": "agent error"})

    return {
        "submitted": len(submitted),
        "skipped": len(skipped),
        "details": submitted,
        "skipped_details": skipped,
        "dry_run": req.dry_run,
    }
