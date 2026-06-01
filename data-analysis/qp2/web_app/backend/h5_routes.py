from fastapi import APIRouter, Depends, HTTPException, Query
from h5grove.fastapi_utils import router as h5grove_router
from sqlalchemy.orm import Session
import sys
import os

# Import models
try:
    from qp2.db import DatasetRun
except ImportError:
    DatasetRun = None

try:
    from qp2.web_app.backend.auth import is_staff_member
    from qp2.web_app.backend.security import verify_token
except ImportError:
    def is_staff_member(u): return False
    def verify_token(): return "user"

# Placeholder
def get_db_session():
    raise RuntimeError("Overridden in main")

async def verify_h5_access(
    file: str = Query(..., description="Path to the HDF5 file"),
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session)
):
    # Allow staff to access anything
    if is_staff_member(user):
        return

    if DatasetRun is None:
        raise HTTPException(status_code=500, detail="Models not loaded")

    # Check strict ownership of the master file
    # Note: This prevents accessing external links if they are not also registered as master files
    # or if we don't implement directory-based checks.
    dataset = session.query(DatasetRun).filter(DatasetRun.master_files.contains(file)).first()
    
    if not dataset:
        # Check if file is in a directory owned by user?
        # For security, let's be strict for now.
        raise HTTPException(status_code=403, detail="File not found in database or access denied")
        
    if dataset.username != user:
        # Check group-based access (consistent with dataset_routes)
        try:
            from qp2.xio.user_group_manager import UserGroupManager
            ugm = UserGroupManager()
            groups = ugm.groupnames_from_username(user)
            allowed = [user] + [g['group_name'] for g in groups] if groups else [user]
            if dataset.username not in allowed:
                raise HTTPException(status_code=403, detail="Access denied")
        except ImportError:
            raise HTTPException(status_code=403, detail="Access denied")

# Define a wrapper router that includes the h5grove router with security
router = APIRouter()
router.include_router(h5grove_router, prefix="/h5grove", dependencies=[Depends(verify_h5_access)])
