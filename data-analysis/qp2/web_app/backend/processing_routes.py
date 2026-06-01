import asyncio
import logging
import os
import sys
import tempfile

logger = logging.getLogger(__name__)
from typing import List, Literal, Optional
from pydantic import BaseModel

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import FileResponse, Response
from sqlalchemy.orm import Session
from sqlalchemy import desc, or_, asc, and_

# Import models
try:
    from qp2.db import PipelineStatus, DataProcessResults
except ImportError:
    print("Warning: Failed to import Processing models", file=sys.stderr)
    PipelineStatus = None
    DataProcessResults = None

try:
    from qp2.web_app.backend.auth import is_staff_member
    from qp2.web_app.backend.security import verify_token
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

_ALLOWED_DATA_ROOT = os.path.realpath(os.environ.get("QP2_DATA_ROOT", "/mnt/beegfs"))

def _validate_processing_path(file_path: str) -> None:
    resolved = os.path.realpath(file_path)
    if not (resolved == _ALLOWED_DATA_ROOT or resolved.startswith(_ALLOWED_DATA_ROOT + os.sep)):
        raise HTTPException(status_code=403, detail="Access denied")

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
        filters.append(or_(
            PipelineStatus.username.in_(allowed_names),
            PipelineStatus.primary_group.in_(allowed_names),
        ))
    
    if search:
        safe_search = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        filters.append(
            or_(
                PipelineStatus.sampleName.ilike(f"%{safe_search}%"),
                PipelineStatus.pipeline.ilike(f"%{safe_search}%"),
                PipelineStatus.imagedir.ilike(f"%{safe_search}%"),
                PipelineStatus.state.ilike(f"%{safe_search}%"),
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
    status = session.get(PipelineStatus, id)
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
        
    # 4. Validate path and check existence
    _validate_processing_path(file_path)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Requested file not found")

    return FileResponse(path=file_path, filename=os.path.basename(file_path), media_type='application/octet-stream')

# ── Structure + density map viewer ────────────────────────────────────────────

def _auth_and_fetch_result(id: int, user: str, session: Session):
    """Shared auth helper: returns (PipelineStatus, DataProcessResults|None)."""
    if PipelineStatus is None:
        raise HTTPException(status_code=500, detail="Models not loaded")
    status = session.get(PipelineStatus, id)
    if not status:
        raise HTTPException(status_code=404, detail="Processing record not found")
    if not is_staff_member(user):
        allowed = [user]
        if ugm:
            try:
                groups = ugm.groupnames_from_username(user)
                if groups:
                    allowed.extend(g['group_name'] for g in groups)
            except Exception:
                pass
        primary = getattr(status, 'primary_group', None)
        if status.username not in allowed and primary not in allowed:
            raise HTTPException(status_code=403, detail="Access denied")
    result = session.query(DataProcessResults).filter(
        DataProcessResults.pipelinestatus_id == id).first()
    return status, result


def _solve_paths(result):
    """Return (pdb_path, mtz_path) derived from DataProcessResults.solve."""
    if not result or not result.solve:
        return None, None
    pdb_path = result.solve.strip()
    mtz_path = os.path.splitext(pdb_path)[0] + '.mtz'
    return pdb_path, mtz_path


# _carve_grid removed to preserve map statistics


def _mtz_to_ccp4_best(mtz_path: str, map_type: str, pdb_path: str = None) -> bytes:
    """Convert MTZ to CCP4 bytes, trying common column naming conventions.

    Reads the MTZ once, selects the best F/PHI columns for the requested
    map type, runs FFT, carves the map around the model (if pdb_path given),
    and returns CCP4 bytes via a temp file.
    """
    import gemmi
    mtz = gemmi.read_mtz_file(mtz_path)   # read once
    labels = set(mtz.column_labels())
    if map_type == '2fofc':
        candidates = [('FWT', 'PHWT'), ('2FOFCWT', 'PH2FOFCWT')]
    else:
        candidates = [('DELFWT', 'PHDELWT'), ('FOFCWT', 'PHFOFCWT')]
    for f_col, phi_col in candidates:
        if f_col in labels and phi_col in labels:
            grid = mtz.transform_f_phi_to_map(f_col, phi_col, sample_rate=3.0)
            # Compute header stats (ARMS/sigma) from the full uncarved map.
            # Mol* uses header.ARMS for relative isovalue — carving zeros most
            # voxels and would deflate sigma if we updated stats afterwards.
            ccp4 = gemmi.Ccp4Map()
            ccp4.grid = grid
            ccp4.update_ccp4_header(2, True)
            if pdb_path and os.path.exists(pdb_path):
                try:
                    import gemmi
                    st = gemmi.read_structure(pdb_path)
                    box = st.calculate_fractional_box(margin=15.0)
                    
                    # Cap box dimensions to a maximum of 1 unit cell.
                    # This prevents massive downloads when molecules are scattered 
                    # far apart due to symmetry operations.
                    if box.maximum.x - box.minimum.x > 1.0:
                        box.maximum.x = box.minimum.x + 1.0
                    if box.maximum.y - box.minimum.y > 1.0:
                        box.maximum.y = box.minimum.y + 1.0
                    if box.maximum.z - box.minimum.z > 1.0:
                        box.maximum.z = box.minimum.z + 1.0
                        
                    ccp4.set_extent(box)
                except Exception as e:
                    logger.warning(f"Map extent update failed: {e}")
            with tempfile.NamedTemporaryFile(suffix='.ccp4', delete=False) as tmp:
                tmp_path = tmp.name
            try:
                ccp4.write_ccp4_map(tmp_path)
                with open(tmp_path, 'rb') as f:
                    return f.read()
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
    raise ValueError(
        f"No suitable {map_type} columns found in MTZ "
        f"(tried {candidates}, available: {sorted(labels)})"
    )


@router.get("/{id}/model-info")
async def get_model_info(
    id: int,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """Return existence flags for structure (PDB) and maps (MTZ) from solve field."""
    status, result = _auth_and_fetch_result(id, user, session)
    pdb_path, mtz_path = _solve_paths(result)
    return {
        "has_structure": bool(pdb_path and os.path.exists(pdb_path)),
        "has_maps":      bool(mtz_path and os.path.exists(mtz_path)),
        "sample_name":   status.sampleName,
    }


@router.get("/{id}/model/pdb")
async def get_model_pdb(
    id: int,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """Serve the solved structure PDB file."""
    _, result = _auth_and_fetch_result(id, user, session)
    pdb_path, _ = _solve_paths(result)
    if not pdb_path or not os.path.exists(pdb_path):
        raise HTTPException(status_code=404, detail="Structure file not found")
    return FileResponse(pdb_path, filename=os.path.basename(pdb_path),
                        media_type='application/octet-stream')


@router.get("/{id}/model/{map_type}")
async def get_model_map(
    id: int,
    map_type: Literal['2fofc', 'fofc'],
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """Convert MTZ → CCP4 on-the-fly using gemmi and stream the result.

    map_type: '2fofc' (FWT/PHWT at ~1σ) or 'fofc' (DELFWT/PHDELWT at ~3σ).
    Requires gemmi to be installed in the active Python environment.
    """
    _, result = _auth_and_fetch_result(id, user, session)
    pdb_path, mtz_path = _solve_paths(result)
    if not mtz_path or not os.path.exists(mtz_path):
        raise HTTPException(status_code=404, detail="MTZ file not found")
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None, _mtz_to_ccp4_best, mtz_path, map_type, pdb_path)
    except ImportError:
        raise HTTPException(status_code=503,
                            detail="gemmi not available — install it in the active environment")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return Response(
        content=data,
        media_type='application/octet-stream',
        headers={'Content-Disposition': f'attachment; filename="{map_type}.ccp4"'},
    )
