from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc, or_, asc, and_
from typing import List, Optional
from pydantic import BaseModel
import sys
import os

# Import models
try:
    from qp2.data_viewer.models import PipelineStatus, DataProcessResults
except ImportError:
    print("Warning: Failed to import Processing models", file=sys.stderr)
    PipelineStatus = None
    DataProcessResults = None

try:
    from auth import is_staff_member
    from security import verify_token
except ImportError:
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

router = APIRouter(prefix="/processing", tags=["processing"])

class ProcessingResult(BaseModel):
    id: int
    name: Optional[str] = None
    pipeline: Optional[str] = None
    imageSet: Optional[str] = None
    state: Optional[str] = None
    isa: Optional[str] = None
    Summary: Optional[str] = None
    wav: Optional[str] = None
    Symm: Optional[str] = None
    Cell: Optional[str] = None
    h_res: Optional[str] = None
    Rsym: Optional[str] = None
    Rmeas: Optional[str] = None
    Rpim: Optional[str] = None
    IsigI: Optional[str] = None
    multi: Optional[str] = None
    Cmpl: Optional[str] = None
    a_Cmpl: Optional[str] = None
    warning: Optional[str] = None
    logfile: Optional[str] = None
    table1: Optional[str] = None
    elapsedtime: Optional[str] = None
    imagedir: Optional[str] = None
    firstFrame: Optional[str] = None
    workdir: Optional[str] = None
    scale_log: Optional[str] = None
    truncate_log: Optional[str] = None
    truncate_mtz: Optional[str] = None
    run_stats: Optional[str] = None
    reprocess: Optional[int] = None
    solve: Optional[str] = None
    delete: Optional[int] = None

    class Config:
        from_attributes = True

@router.get("/list", response_model=List[ProcessingResult])
async def list_processing(
    user: str = Depends(verify_token),
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "id",
    sort_desc: bool = True,
    session: Session = Depends(get_db_session)
):
    if PipelineStatus is None:
        raise HTTPException(status_code=500, detail="Models not loaded")

    # Use .label() to match Pydantic model fields
    query = session.query(
        PipelineStatus.id,
        PipelineStatus.sampleName.label("name"),
        PipelineStatus.pipeline,
        PipelineStatus.imageSet,
        PipelineStatus.state,
        DataProcessResults.isa,
        DataProcessResults.report_url.label("Summary"),
        DataProcessResults.wavelength.label("wav"),
        DataProcessResults.spacegroup.label("Symm"),
        DataProcessResults.unitcell.label("Cell"),
        DataProcessResults.highresolution.label("h_res"),
        DataProcessResults.rmerge.label("Rsym"),
        DataProcessResults.rmeas.label("Rmeas"),
        DataProcessResults.rpim.label("Rpim"),
        DataProcessResults.isigmai.label("IsigI"),
        DataProcessResults.multiplicity.label("multi"),
        DataProcessResults.completeness.label("Cmpl"),
        DataProcessResults.anom_completeness.label("a_Cmpl"),
        PipelineStatus.warning,
        PipelineStatus.logfile,
        DataProcessResults.table1,
        PipelineStatus.elapsedtime,
        PipelineStatus.imagedir,
        DataProcessResults.firstFrame,
        DataProcessResults.workdir,
        DataProcessResults.scale_log,
        DataProcessResults.truncate_log,
        DataProcessResults.truncate_mtz,
        DataProcessResults.run_stats,
        DataProcessResults.id.label("reprocess"),
        DataProcessResults.solve,
        PipelineStatus.id.label("delete")
    ).outerjoin(
        DataProcessResults, PipelineStatus.id == DataProcessResults.pipelinestatus_id
    )

    filters = [~(PipelineStatus.pipeline.contains("_strategy"))]
    
    if not is_staff_member(user):
        allowed_names = [user]
        if ugm:
            try:
                groups = ugm.groupnames_from_username(user)
                if groups:
                    allowed_names.extend([g['group_name'] for g in groups])
            except Exception:
                pass
        filters.append(PipelineStatus.username.in_(allowed_names))
    
    if search:
        filters.append(
            or_(
                PipelineStatus.sampleName.ilike(f"%{search}%"),
                PipelineStatus.pipeline.ilike(f"%{search}%"),
                PipelineStatus.imagedir.ilike(f"%{search}%"),
                PipelineStatus.state.ilike(f"%{search}%"),
            )
        )
    
    query = query.filter(and_(*filters))
    
    if sort_desc:
        query = query.order_by(desc(PipelineStatus.id))
    else:
        query = query.order_by(asc(PipelineStatus.id))

    return query.offset(offset).limit(limit).all()

@router.get("/download/{id}/{field}")
async def download_processing_file(
    id: int,
    field: str,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session)
):
    if PipelineStatus is None:
        raise HTTPException(status_code=500, detail="Models not loaded")

    # 1. Get PipelineStatus
    status = session.query(PipelineStatus).get(id)
    if not status:
        raise HTTPException(status_code=404, detail="Processing record not found")
        
    # 2. Check Auth
    if not is_staff_member(user):
        allowed_names = [user]
        if ugm:
            try:
                groups = ugm.groupnames_from_username(user)
                if groups:
                    allowed_names.extend([g['group_name'] for g in groups])
            except Exception:
                pass
        
        if status.username not in allowed_names:
            raise HTTPException(status_code=403, detail="Access denied")
        
    # 3. Get file path
    # Allow-list specific fields for security?
    allowed_fields = ["truncate_mtz", "logfile", "scale_log", "truncate_log", "report_url"]
    if field not in allowed_fields:
         raise HTTPException(status_code=400, detail=f"Download not allowed for field: {field}")

    # Check PipelineStatus fields
    file_path = getattr(status, field, None)
    
    # Check DataProcessResults fields
    if not file_path:
        result = session.query(DataProcessResults).filter(DataProcessResults.pipelinestatus_id == id).first()
        if result:
            file_path = getattr(result, field, None)
            
    if not file_path:
        raise HTTPException(status_code=404, detail=f"File path for field '{field}' not found")
        
    # 4. Check existence
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found on server: {file_path}")
        
    return FileResponse(path=file_path, filename=os.path.basename(file_path), media_type='application/octet-stream')