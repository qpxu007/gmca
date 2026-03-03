from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc, or_, asc
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import sys
import os
import zipfile
import tempfile
import glob

# Import models
try:
    from qp2.data_viewer.models import DatasetRun
except ImportError:
    print("Warning: Failed to import DatasetRun model in dataset_routes", file=sys.stderr)
    DatasetRun = None

# Import auth
# Assuming these are available in the path as set by main.py
try:
    from auth import is_staff_member
    from security import verify_token
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
        search_filter = or_(
            DatasetRun.run_prefix.ilike(f"%{search}%"),
            DatasetRun.collect_type.ilike(f"%{search}%"),
            DatasetRun.master_files.ilike(f"%{search}%")
        )
        query = query.filter(search_filter)
        
    # Sorting
    if hasattr(DatasetRun, sort_by):
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

    dataset = session.query(DatasetRun).get(data_id)
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

    file_path = dataset.master_files
    if not file_path:
        raise HTTPException(status_code=404, detail="No master file path record")

    # Determine file info
    directory = os.path.dirname(file_path)
    filename = os.path.basename(file_path)

    if mode == "master":
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"File not found on server: {file_path}")
        return FileResponse(path=file_path, filename=filename, media_type='application/octet-stream')

    elif mode == "archive":
        # Heuristic for prefix: "prefix_master.h5" -> "prefix_"
        # Or just match everything starting with the prefix if it's consistent
        # The user said "string before master.h5".
        if "master.h5" in filename:
            prefix = filename.replace("master.h5", "")
        else:
            # Fallback if naming convention differs
            prefix = os.path.splitext(filename)[0]
        
        # Security: Ensure we are only looking in the specific directory
        # glob pattern
        if not directory or not prefix:
             raise HTTPException(status_code=400, detail="Invalid file path format")

        pattern = os.path.join(directory, f"{prefix}*")
        files_to_zip = glob.glob(pattern)
        
        if not files_to_zip:
             raise HTTPException(status_code=404, detail=f"No matching files found for pattern: {pattern}")

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
            
            zip_filename = f"{prefix}dataset.zip" if prefix else "dataset.zip"
            return FileResponse(path=temp_path, filename=zip_filename, media_type='application/zip')
            
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise HTTPException(status_code=500, detail=f"Failed to create archive: {str(e)}")
            
    else:
        raise HTTPException(status_code=400, detail="Invalid mode")
