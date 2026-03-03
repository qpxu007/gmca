from fastapi import APIRouter, Depends, HTTPException, Query
from h5grove.fastapi_utils import router as h5grove_router
from sqlalchemy.orm import Session
import sys
import os

# Import models
try:
    from qp2.data_viewer.models import DatasetRun
except ImportError:
    DatasetRun = None

try:
    from auth import is_staff_member
    from security import verify_token
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
    dataset = session.query(DatasetRun).filter(DatasetRun.master_files == file).first()
    
    if not dataset:
        # Check if file is in a directory owned by user?
        # For security, let's be strict for now.
        raise HTTPException(status_code=403, detail="File not found in database or access denied")
        
    if dataset.username != user:
        raise HTTPException(status_code=403, detail="Access denied")

# Define a wrapper router that includes the h5grove router with security
router = APIRouter()
router.include_router(h5grove_router, prefix="/h5grove", dependencies=[Depends(verify_h5_access)])
