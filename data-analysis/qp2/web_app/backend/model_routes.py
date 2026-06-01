"""
Structure Model management routes.

Upload, list, download, delete, and serve PDB/CIF model files,
scoped to ESAF groups.
"""

import os
import re
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from qp2.web_app.backend.security import verify_token
from qp2.web_app.backend.auth import is_staff_member

from qp2.db import StructureModel
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/models", tags=["models"])

# --- DB session dependency (overridden in main.py) ---
def get_db_session():
    raise NotImplementedError("Must be overridden via app.dependency_overrides")


# --- Constants ---

_DEFAULT_STORAGE = os.path.join("/mnt/beegfs/dmadmin", "models")
_TEST_STORAGE = os.path.join(os.path.expanduser("~"), ".data_viewer", "models")
MODEL_STORAGE_DIR = _DEFAULT_STORAGE if os.path.exists("/mnt/beegfs/dmadmin") else _TEST_STORAGE

# Roots from which model files may be served (uploads + prediction outputs on beegfs)
_MODEL_SERVE_ROOTS = tuple(
    os.path.realpath(r) for r in [MODEL_STORAGE_DIR, "/mnt/beegfs", os.path.expanduser("~/.data_viewer")]
)

def _validate_model_path(file_path: str) -> None:
    resolved = os.path.realpath(file_path)
    if not any(resolved == r or resolved.startswith(r + os.sep) for r in _MODEL_SERVE_ROOTS):
        raise HTTPException(status_code=403, detail="Access denied")

ALLOWED_EXTENSIONS = {".pdb", ".cif"}
MAX_FILE_SIZE = 20 * 1024 * 1024  # 20 MB

# Executable magic bytes to reject
_EXECUTABLE_MAGIC = [b"MZ", b"\x7fELF", b"\xca\xfe\xba\xbe", b"\xfe\xed\xfa\xce"]


# --- Helpers ---

def _check_esaf_access(user: str, esaf_id: str):
    """Verify user has access to this ESAF."""
    if not esaf_id.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ESAF ID format")
        
    if is_staff_member(user):
        return
    try:
        from qp2.xio.user_group_manager import UserGroupManager
        ugm = UserGroupManager()
        groups = ugm.groupnames_from_username(user)
        if groups:
            for g in groups:
                name = g.get("group_name", "")
                match = re.match(r'^esaf(\d+)$', name, re.IGNORECASE)
                if match and match.group(1) == esaf_id:
                    return
    except Exception as e:
        logger.debug("ESAF access check failed for user=%s esaf=%s: %s", user, esaf_id, e)
    raise HTTPException(status_code=403, detail="Access denied")


def _sanitize_filename(filename: str) -> str:
    name = os.path.basename(filename)
    name = re.sub(r'[^\w\-.]', '_', name)
    return name


def _validate_content(content: bytes, ext: str):
    header = content[:8]
    for magic in _EXECUTABLE_MAGIC:
        if header.startswith(magic):
            raise HTTPException(status_code=400, detail="File rejected: executable content detected")
    # PDB/CIF are text-based
    try:
        content.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail=f"{ext} files must be valid UTF-8 text")


# --- Endpoints ---

# NOTE: /list-for-spreadsheet must be registered before /{esaf_id}
# to avoid being shadowed by the catch-all path parameter.

@router.get("/list-for-spreadsheet")
def list_for_spreadsheet(esaf_id: str, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Simplified model list for populating ModelPath in spreadsheet editor."""
    _check_esaf_access(user, esaf_id)
    models = session.query(StructureModel).filter_by(esaf_id=esaf_id).order_by(StructureModel.created_at.desc()).all()
    return [
        {"id": m.id, "filename": m.filename, "file_path": m.file_path}
        for m in models
    ]


@router.get("/{esaf_id}")
def list_models(esaf_id: str, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """List all models for an ESAF."""
    _check_esaf_access(user, esaf_id)
    models = session.query(StructureModel).filter_by(esaf_id=esaf_id).order_by(StructureModel.created_at.desc()).all()
    return [
        {
            "id": m.id,
            "filename": m.filename,
            "file_type": m.file_type,
            "source": m.source,
            "job_id": m.job_id,
            "uploaded_by": m.uploaded_by,
            "created_at": m.created_at.isoformat() if m.created_at else None,
        }
        for m in models
    ]


@router.post("/{esaf_id}/upload")
async def upload_model(
    esaf_id: str,
    file: UploadFile = File(...),
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """Upload a PDB or CIF model file."""
    _check_esaf_access(user, esaf_id)

    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Only .pdb and .cif files are allowed")

    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail=f"File too large. Max {MAX_FILE_SIZE // (1024*1024)}MB")

    _validate_content(content, ext)

    safe_name = _sanitize_filename(file.filename)
    dest_dir = os.path.join(MODEL_STORAGE_DIR, esaf_id)
    os.makedirs(dest_dir, exist_ok=True)

    dest_path = os.path.join(dest_dir, safe_name)
    if os.path.exists(dest_path):
        base, extension = os.path.splitext(safe_name)
        counter = 1
        while os.path.exists(dest_path):
            dest_path = os.path.join(dest_dir, f"{base}_{counter}{extension}")
            counter += 1
        safe_name = os.path.basename(dest_path)

    with open(dest_path, "wb") as f:
        f.write(content)

    model = StructureModel(
        esaf_id=esaf_id,
        filename=safe_name,
        file_path=dest_path,
        file_type=ext.lstrip("."),
        source="upload",
        uploaded_by=user,
    )
    session.add(model)
    session.flush()

    return {"status": "ok", "id": model.id, "filename": safe_name}


@router.get("/{esaf_id}/{model_id}/download")
def download_model(esaf_id: str, model_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Download a model file."""
    _check_esaf_access(user, esaf_id)
    model = session.query(StructureModel).filter_by(id=model_id, esaf_id=esaf_id).first()
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    _validate_model_path(model.file_path)
    if not os.path.exists(model.file_path):
        raise HTTPException(status_code=404, detail="Model file missing from storage")
    return FileResponse(model.file_path, filename=model.filename)


@router.get("/{esaf_id}/{model_id}/view")
def view_model(esaf_id: str, model_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Serve a model file for the 3D viewer."""
    _check_esaf_access(user, esaf_id)
    model = session.query(StructureModel).filter_by(id=model_id, esaf_id=esaf_id).first()
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    _validate_model_path(model.file_path)
    if not os.path.exists(model.file_path):
        raise HTTPException(status_code=404, detail="Model file missing from storage")
    media_type = "chemical/x-pdb" if model.file_type == "pdb" else "chemical/x-cif"
    return FileResponse(model.file_path, filename=model.filename, media_type=media_type)


@router.delete("/{esaf_id}/{model_id}")
def delete_model(esaf_id: str, model_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Delete a model."""
    _check_esaf_access(user, esaf_id)
    model = session.query(StructureModel).filter_by(id=model_id, esaf_id=esaf_id).first()
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    # Only uploader or staff can delete
    if model.uploaded_by != user and not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Not authorized to delete this model")
    # Remove file if it's in our managed storage (not prediction output)
    if model.source == "upload" and os.path.exists(model.file_path):
        resolved = os.path.realpath(model.file_path)
        if resolved.startswith(os.path.realpath(MODEL_STORAGE_DIR)):
            os.remove(resolved)
    session.delete(model)
    return {"status": "ok"}



