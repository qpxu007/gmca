from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc, or_, asc
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import sys
import os
import threading
import time
import uuid
import zipfile
import tempfile
import glob

# Import models
try:
    from qp2.db import DatasetRun
except ImportError:
    print("Warning: Failed to import DatasetRun model in dataset_routes", file=sys.stderr)
    DatasetRun = None

# Import auth
# Assuming these are available in the path as set by main.py
try:
    from qp2.web_app.backend.auth import is_staff_member
    from qp2.web_app.backend.security import verify_token
except ImportError:
    # Fallback for linter/dev
    def is_staff_member(u): return False
    def verify_token(): return "user"

try:
    from qp2.xio.user_group_manager import UserGroupManager
except ImportError:
    print("Warning: Failed to import UserGroupManager", file=sys.stderr)
    UserGroupManager = None

# Instantiate UGM
ugm = UserGroupManager() if UserGroupManager else None

# Dependency placeholder
def get_db_session():
    raise RuntimeError("get_db_session dependency not properly overridden")

router = APIRouter(prefix="/datasets", tags=["datasets"])

# --- Background zip job store ---
_zip_jobs: dict = {}
_zip_jobs_lock = threading.Lock()


def _update_zip_job(job_id: str, **kwargs) -> None:
    with _zip_jobs_lock:
        if job_id in _zip_jobs:
            _zip_jobs[job_id].update(kwargs)


def _zip_job_worker(job_id: str, all_paths: list, zip_prefix: str) -> None:
    try:
        files_to_zip: set = set()
        for master in all_paths:
            fn = os.path.basename(master)
            d = os.path.dirname(master)
            if not d:
                continue
            prefix = fn.replace("_master.h5", "").replace("master.h5", "")
            if prefix:
                files_to_zip.update(glob.glob(os.path.join(d, f"{prefix}*")))

        if not files_to_zip:
            _update_zip_job(job_id, status="error", error="No matching files found")
            return

        files_list = sorted(files_to_zip)
        total = len(files_list)
        fd, temp_path = tempfile.mkstemp(suffix=".zip")
        os.close(fd)

        with zipfile.ZipFile(temp_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for i, f in enumerate(files_list):
                _update_zip_job(job_id, progress=f"Zipping file {i + 1} of {total}: {os.path.basename(f)}")
                zipf.write(f, arcname=os.path.basename(f))

        zip_filename = f"{zip_prefix}_dataset.zip"
        _update_zip_job(job_id, status="done", temp_path=temp_path,
                        zip_filename=zip_filename,
                        progress=f"Ready — {total} files zipped")
    except Exception as e:
        print(f"Zip job {job_id} failed: {e}", file=sys.stderr)
        _update_zip_job(job_id, status="error", error=str(e))
        if 'temp_path' in dir() and os.path.exists(temp_path):  # type: ignore[name-defined]
            os.remove(temp_path)

class DatasetRunResponse(BaseModel):
    data_id: int
    username: Optional[str] = None
    run_prefix: str
    total_frames: Optional[int] = None
    collect_type: Optional[str] = None
    master_files: Optional[str] = None
    headers: Optional[str] = None
    mounted: Optional[str] = None
    meta_user: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True

@router.get("/list", response_model=List[DatasetRunResponse])
async def list_datasets(
    user: str = Depends(verify_token),
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "created_at",
    sort_desc: bool = True,
    session: Session = Depends(get_db_session)
):
    if DatasetRun is None:
        raise HTTPException(status_code=500, detail="Models not loaded")

    query = session.query(DatasetRun)
    
    # Permission Logic:
    # If staff, can see all. If not, see datasets for any of their groups.
    if not is_staff_member(user):
        allowed_names = [user]
        if ugm:
            try:
                groups = ugm.groupnames_from_username(user)
                if groups:
                    # Result is list of dicts: [{'group_name': '...'}]
                    allowed_names.extend([g['group_name'] for g in groups])
            except Exception as e:
                print(f"Warning: Group lookup for {user} failed: {e}", file=sys.stderr)
        
        query = query.filter(DatasetRun.username.in_(allowed_names))
    
    if search:
        safe_search = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        search_filter = or_(
            DatasetRun.run_prefix.ilike(f"%{safe_search}%"),
            DatasetRun.collect_type.ilike(f"%{safe_search}%"),
            DatasetRun.master_files.ilike(f"%{safe_search}%")
        )
        query = query.filter(search_filter)
        
    # Sorting — allowlist to prevent attribute exposure
    _ALLOWED_SORT = {"data_id", "created_at", "run_prefix", "collect_type", "username", "total_frames"}
    if sort_by in _ALLOWED_SORT:
        col = getattr(DatasetRun, sort_by)
        if sort_desc:
            query = query.order_by(desc(col))
        else:
            query = query.order_by(asc(col))
    else:
        query = query.order_by(desc(DatasetRun.created_at))
        
    results = query.offset(offset).limit(limit).all()
    return results

@router.get("/download/{data_id}")
async def download_dataset(
    data_id: int,
    mode: str = "master", # 'master' or 'archive'
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    if DatasetRun is None:
        raise HTTPException(status_code=500, detail="Models not loaded")

    dataset = session.get(DatasetRun, data_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Permission check
    if not is_staff_member(user):
        allowed_names = [user]
        if ugm:
            try:
                groups = ugm.groupnames_from_username(user)
                if groups:
                    allowed_names.extend([g['group_name'] for g in groups])
            except Exception:
                pass

        if dataset.username not in allowed_names:
            raise HTTPException(status_code=403, detail="Not authorized to download this dataset")

    raw_path = dataset.master_files
    if not raw_path:
        raise HTTPException(status_code=404, detail="No master file path record")

    # Parse JSON array to get all master file paths
    import json
    try:
        parsed = json.loads(raw_path)
        if isinstance(parsed, list) and parsed:
            all_paths = parsed
        else:
            all_paths = [raw_path]
    except (json.JSONDecodeError, TypeError):
        all_paths = [raw_path]

    file_path = all_paths[0]
    directory = os.path.dirname(file_path)
    filename = os.path.basename(file_path)

    if mode == "master":
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="Requested file not found")
        return FileResponse(path=file_path, filename=filename, media_type='application/octet-stream')

    elif mode == "archive":
        # Collect data files for ALL master files in this dataset
        files_to_zip = set()
        for master in all_paths:
            fn = os.path.basename(master)
            d = os.path.dirname(master)
            if not d:
                continue
            prefix = fn.replace("_master.h5", "").replace("master.h5", "")
            if prefix:
                files_to_zip.update(glob.glob(os.path.join(d, f"{prefix}*")))

        if not files_to_zip:
            raise HTTPException(status_code=404, detail="No matching files found")

        # Use run_prefix for the zip filename
        zip_prefix = dataset.run_prefix or "dataset"

        # Create temp zip
        try:
            fd, temp_path = tempfile.mkstemp(suffix=".zip")
            os.close(fd)
            
            with zipfile.ZipFile(temp_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for f in files_to_zip:
                    # Add file with just its basename (flat structure in zip)
                    zipf.write(f, arcname=os.path.basename(f))
            
            # Schedule cleanup
            background_tasks.add_task(os.remove, temp_path)
            
            zip_filename = f"{zip_prefix}_dataset.zip"
            return FileResponse(path=temp_path, filename=zip_filename, media_type='application/zip')
            
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise HTTPException(status_code=500, detail="Failed to create archive")
            
    else:
        raise HTTPException(status_code=400, detail="Invalid mode")


# --- Async zip endpoints ---

def _resolve_zip_paths(dataset):
    """Return (all_paths, zip_prefix) for a dataset, or raise HTTPException."""
    import json as _json
    raw = dataset.master_files
    if not raw:
        raise HTTPException(status_code=404, detail="No master file path record")
    try:
        parsed = _json.loads(raw)
        all_paths = parsed if isinstance(parsed, list) and parsed else [raw]
    except (ValueError, TypeError):
        all_paths = [raw]
    zip_prefix = dataset.run_prefix or "dataset"
    return all_paths, zip_prefix


@router.post("/zip/{data_id}")
async def start_zip_job(
    data_id: int,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """Start a background zip job; return job_id to poll."""
    if DatasetRun is None:
        raise HTTPException(status_code=500, detail="Models not loaded")
    dataset = session.get(DatasetRun, data_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not is_staff_member(user):
        allowed_names = [user]
        if ugm:
            try:
                groups = ugm.groupnames_from_username(user)
                if groups:
                    allowed_names.extend([g['group_name'] for g in groups])
            except Exception:
                pass
        if dataset.username not in allowed_names:
            raise HTTPException(status_code=403, detail="Not authorized")

    all_paths, zip_prefix = _resolve_zip_paths(dataset)
    job_id = str(uuid.uuid4())
    with _zip_jobs_lock:
        _zip_jobs[job_id] = {"status": "running", "progress": "Starting…", "created_at": time.time()}
    thread = threading.Thread(target=_zip_job_worker, args=(job_id, all_paths, zip_prefix), daemon=True)
    thread.start()
    return {"job_id": job_id, "status": "running"}


@router.get("/zip/status/{job_id}")
async def get_zip_status(job_id: str, user: str = Depends(verify_token)):
    """Poll status of a background zip job."""
    with _zip_jobs_lock:
        job = _zip_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    return {k: v for k, v in job.items() if k not in ("temp_path", "created_at")}


@router.get("/zip/download/{job_id}")
async def download_zip(
    job_id: str,
    user: str = Depends(verify_token),
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    """Download the finished zip archive and clean up."""
    with _zip_jobs_lock:
        job = _zip_jobs.get(job_id)
    if not job or job.get("status") != "done":
        raise HTTPException(status_code=404, detail="Job not ready or not found")

    temp_path = job.get("temp_path")
    zip_filename = job.get("zip_filename", "dataset.zip")
    if not temp_path or not os.path.exists(temp_path):
        raise HTTPException(status_code=404, detail="Archive file not found")

    def _cleanup():
        with _zip_jobs_lock:
            _zip_jobs.pop(job_id, None)
        if os.path.exists(temp_path):
            os.remove(temp_path)

    background_tasks.add_task(_cleanup)
    return FileResponse(path=temp_path, filename=zip_filename, media_type="application/zip")
