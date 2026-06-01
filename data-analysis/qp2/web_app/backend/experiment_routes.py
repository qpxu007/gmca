"""
Experiment Preparation Form — backend routes.

Users submit per-ESAF forms with spreadsheets, files, IP addresses,
experiment instructions, shipping tracking, and local host assignments.
"""

import json
import os
import re
import smtplib
import shutil
import subprocess
from datetime import datetime
from email.message import EmailMessage
from ipaddress import ip_address as parse_ip
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, UploadFile, File
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload

from qp2.web_app.backend.security import verify_token
from qp2.web_app.backend.auth import is_staff_member

from qp2.db import (
    ExperimentForm, ExperimentFile, ExperimentIP,
    ExperimentTracking, ExperimentHost, Staff, Spreadsheet,
    StructureModel,
)
from qp2.web_app.backend.model_routes import MODEL_STORAGE_DIR as _MODEL_STORAGE_DIR
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

_DM_NODE = os.environ.get("QP2_DM_NODE", "bl2ws5")
_DM_SETUP = os.environ.get("QP2_DM_SETUP", "source /home/dm/etc/dm.setup.sh ; source /mnt/beegfs/dmadmin/.bashrc")
_NOTIFICATION_FROM = os.environ.get("QP2_NOTIFICATION_FROM_EMAIL", "qxu@anl.gov")
_DEFAULT_HOST_EMAIL = os.environ.get("QP2_DEFAULT_HOST_EMAIL", "gmcahosts@anl.gov")

router = APIRouter(prefix="/experiment", tags=["experiment"])

# --- DB session dependency (overridden in main.py) ---
def get_db_session():
    raise NotImplementedError("Must be overridden via app.dependency_overrides")


# --- Constants ---

CARRIER_URLS = {
    "fedex_overnight": "https://www.fedex.com/fedextrack/?trknbr={}",
    "fedex_2day": "https://www.fedex.com/fedextrack/?trknbr={}",
    "ups": "https://www.ups.com/track?tracknum={}",
}

CARRIER_LABELS = {
    "fedex_overnight": "FedEx Overnight",
    "fedex_2day": "FedEx 2-Day",
    "ups": "UPS",
}

ALLOWED_EXTENSIONS = {".pdb", ".cif", ".fasta", ".fa", ".seq", ".csv", ".xls", ".xlsx", ".txt", ".pdf"}
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB hard cap for all files

# Per-extension size limits (bytes) — all capped at 5 MB max
_EXT_SIZE_LIMITS = {
    ".pdb": 5 * 1024 * 1024,
    ".cif": 5 * 1024 * 1024,
    ".fasta": 5 * 1024 * 1024,
    ".fa": 5 * 1024 * 1024,
    ".seq": 5 * 1024 * 1024,
    ".csv": 5 * 1024 * 1024,
    ".xls": 5 * 1024 * 1024,
    ".xlsx": 5 * 1024 * 1024,
    ".txt": 5 * 1024 * 1024,
    ".pdf": 5 * 1024 * 1024,
}

# Magic bytes: (expected_prefixes, must_be_text) per extension group
# PE (MZ) and ELF headers are always rejected regardless of extension
_EXECUTABLE_MAGIC = [b"MZ", b"\x7fELF", b"\xca\xfe\xba\xbe", b"\xfe\xed\xfa\xce"]

# Expected magic bytes for binary formats
_BINARY_MAGIC = {
    ".xlsx": b"PK\x03\x04",          # ZIP container
    ".xls": b"\xd0\xcf\x11\xe0",    # OLE2 Compound Document
    ".pdf": b"%PDF",
}

# Text-based extensions — content must be valid UTF-8 (or ASCII)
_TEXT_EXTENSIONS = {".pdb", ".cif", ".fasta", ".fa", ".seq", ".csv", ".txt"}


def _validate_file_content(content: bytes, ext: str):
    """Validate file content matches declared extension. Raises HTTPException on failure."""
    header = content[:8]

    # 1. Reject executables regardless of extension
    for magic in _EXECUTABLE_MAGIC:
        if header.startswith(magic):
            raise HTTPException(status_code=400, detail="File rejected: executable content detected")

    # 2. Binary formats — check magic bytes
    if ext in _BINARY_MAGIC:
        expected = _BINARY_MAGIC[ext]
        if not header.startswith(expected):
            raise HTTPException(status_code=400, detail=f"File content does not match {ext} format")
        return

    # 3. Text formats — must be valid UTF-8
    if ext in _TEXT_EXTENSIONS:
        try:
            content.decode("utf-8")
        except UnicodeDecodeError:
            raise HTTPException(status_code=400, detail=f"File rejected: {ext} must be valid UTF-8 text")


FILE_STORAGE_DIR = os.path.join(os.path.expanduser("~"), ".data_viewer", "experiment_files")


# --- Pydantic Models ---

class CreateFormRequest(BaseModel):
    esaf_id: str
    beamline: Optional[str] = None
    pi_name: Optional[str] = None
    experiment_dates: Optional[str] = None
    contact_phone: Optional[str] = None

class UpdateFormRequest(BaseModel):
    instructions: Optional[str] = None
    has_hard_drive: Optional[bool] = None
    spreadsheet_id: Optional[int] = None
    beamline: Optional[str] = None
    pi_name: Optional[str] = None
    experiment_dates: Optional[str] = None
    contact_phone: Optional[str] = None

class AddIPRequest(BaseModel):
    ip_address: str
    label: Optional[str] = None

class UpdateIPRequest(BaseModel):
    ip_address: Optional[str] = None
    label: Optional[str] = None

class AddTrackingRequest(BaseModel):
    carrier: str
    tracking_number: str
    direction: str = "inbound"

class AddHostRequest(BaseModel):
    staff_id: int


# --- Helpers ---

def _check_esaf_access(user: str, esaf_id: str, session: Session = None):
    """Verify user is a member of the ESAF group, the form creator, or staff."""
    if not esaf_id.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ESAF ID format")
        
    if is_staff_member(user):
        return
    # Allow access if user created this form
    if session:
        form = session.query(ExperimentForm).filter_by(esaf_id=esaf_id).first()
        if form and form.created_by == user:
            return
    try:
        from qp2.xio.user_group_manager import UserGroupManager
        ugm = UserGroupManager()
        groups = ugm.groupnames_from_username(user)
        if groups:
            for g in groups:
                name = g.get("group_name", "")
                # Group names are like "esaf12345" — exact match only
                match = re.match(r'^esaf(\d+)$', name, re.IGNORECASE)
                if match and match.group(1) == esaf_id:
                    return
    except Exception:
        pass
    raise HTTPException(status_code=403, detail="Access denied to this experiment form")


def _get_form_or_404(session: Session, esaf_id: str) -> ExperimentForm:
    form = session.query(ExperimentForm).filter_by(esaf_id=esaf_id).first()
    if not form:
        raise HTTPException(status_code=404, detail="Experiment form not found")
    return form


def _form_to_dict(form: ExperimentForm) -> dict:
    return {
        "id": form.id,
        "esaf_id": form.esaf_id,
        "beamline": form.beamline,
        "pi_name": form.pi_name,
        "experiment_dates": form.experiment_dates,
        "spreadsheet_id": form.spreadsheet_id,
        "instructions": form.instructions,
        "contact_phone": form.contact_phone,
        "has_hard_drive": form.has_hard_drive,
        "created_by": form.created_by,
        "created_at": form.created_at.isoformat() if form.created_at else None,
        "updated_at": form.updated_at.isoformat() if form.updated_at else None,
        "files": [
            {
                "id": f.id,
                "filename": f.filename,
                "file_type": f.file_type,
                "uploaded_by": f.uploaded_by,
                "uploaded_at": f.uploaded_at.isoformat() if f.uploaded_at else None,
            }
            for f in form.files
        ],
        "ips": [
            {
                "id": ip.id,
                "ip_address": ip.ip_address,
                "label": ip.label,
                "added_by": ip.added_by,
                "added_at": ip.added_at.isoformat() if ip.added_at else None,
            }
            for ip in form.ips
        ],
        "tracking": [
            {
                "id": t.id,
                "carrier": t.carrier,
                "carrier_label": CARRIER_LABELS.get(t.carrier, t.carrier),
                "tracking_number": t.tracking_number,
                "direction": t.direction,
                "status": t.status,
                "tracking_url": t.tracking_url,
                "added_by": t.added_by,
                "added_at": t.added_at.isoformat() if t.added_at else None,
            }
            for t in form.tracking_entries
        ],
        "hosts": [
            {
                "id": h.id,
                "staff_id": h.staff_id,
                "username": h.staff.username if h.staff else None,
                "full_name": h.staff.full_name if h.staff else None,
                "email": h.staff.email if h.staff else None,
                "assigned_by": h.assigned_by,
                "assigned_at": h.assigned_at.isoformat() if h.assigned_at else None,
            }
            for h in form.hosts
        ],
    }


def _sanitize_filename(filename: str) -> str:
    """Remove path separators and dangerous characters."""
    name = os.path.basename(filename)
    name = re.sub(r'[^\w\-.]', '_', name)
    return name


def _notify_hosts(form: ExperimentForm, changed_fields: list, changed_by: str):
    """Send email notification to assigned hosts."""
    host_emails = []
    for h in form.hosts:
        if h.staff and h.staff.email:
            host_emails.append(h.staff.email)
    if not host_emails:
        host_emails = [_DEFAULT_HOST_EMAIL]

    # Look up submitter contact info
    submitter_email = None
    submitter_name = changed_by
    try:
        from qp2.xio.user_group_manager import UserGroupManager
        ugm = UserGroupManager()
        info = ugm.get_user_info(changed_by)
        if info:
            submitter_email = info.get("email")
            submitter_name = info.get("full_name") or changed_by
    except Exception:
        pass

    try:
        msg = EmailMessage()
        msg["Subject"] = f"[GM/CA] Experiment update - ESAF {form.esaf_id}"
        msg["From"] = _NOTIFICATION_FROM
        msg["To"] = ", ".join(host_emails)
        if submitter_email:
            msg["Reply-To"] = submitter_email
        submitter_line = f"{submitter_name} ({submitter_email})" if submitter_email else submitter_name
        body = (
            f"Experiment form updated for ESAF {form.esaf_id}\n"
            f"Beamline: {form.beamline or 'N/A'}\n"
            f"PI: {form.pi_name or 'N/A'}\n"
            f"Contact phone: {form.contact_phone or 'N/A'}\n"
            f"Updated by: {submitter_line}\n"
            f"Changed: {', '.join(changed_fields)}\n"
        )
        msg.set_content(body)
        with smtplib.SMTP("localhost") as smtp:
            smtp.send_message(msg)
    except Exception as e:
        logger.warning(f"Failed to send host notification: {e}")


# --- Endpoints ---

@router.get("/list")
def list_forms(user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """List experiment forms. Staff sees all; regular users see their ESAFs."""
    if is_staff_member(user):
        forms = (
            session.query(ExperimentForm)
            .options(joinedload(ExperimentForm.hosts).joinedload(ExperimentHost.staff))
            .options(joinedload(ExperimentForm.tracking_entries))
            .options(joinedload(ExperimentForm.files))
            .options(joinedload(ExperimentForm.ips))
            .all()
        )
    else:
        # Get user's ESAF groups
        esaf_ids = []
        try:
            from qp2.xio.user_group_manager import UserGroupManager
            ugm = UserGroupManager()
            groups = ugm.groupnames_from_username(user)
            if groups:
                for g in groups:
                    name = g.get("group_name", "")
                    # Extract ESAF number — exact match only (e.g., "esaf12345" → "12345")
                    match = re.match(r'^esaf(\d+)$', name, re.IGNORECASE)
                    if match:
                        esaf_ids.append(match.group(1))
        except Exception:
            pass

        # Include forms from user's ESAF groups OR created by the user
        from sqlalchemy import or_
        conditions = [ExperimentForm.created_by == user]
        if esaf_ids:
            conditions.append(ExperimentForm.esaf_id.in_(esaf_ids))

        forms = (
            session.query(ExperimentForm)
            .filter(or_(*conditions))
            .options(joinedload(ExperimentForm.hosts).joinedload(ExperimentHost.staff))
            .options(joinedload(ExperimentForm.tracking_entries))
            .options(joinedload(ExperimentForm.files))
            .options(joinedload(ExperimentForm.ips))
            .all()
        )

    return {"forms": [_form_to_dict(f) for f in forms]}


@router.get("/my-ip")
def get_my_ip(request: Request, user: str = Depends(verify_token)):
    """Return the client's IP address as seen by the server."""
    forwarded = request.headers.get("X-Forwarded-For")
    ip = forwarded.split(",")[0].strip() if forwarded else (request.client.host if request.client else "127.0.0.1")
    return {"ip": ip}


@router.get("/staff-list")
def get_staff_list(user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """List active staff members for host assignment dropdown."""
    staff = session.query(Staff).filter_by(is_active=True).order_by(Staff.full_name).all()
    return {
        "staff": [
            {"id": s.id, "username": s.username, "full_name": s.full_name, "email": s.email}
            for s in staff
        ]
    }


def _fetch_esafs_from_dm():
    """Fallback: fetch ESAFs from the facility DM server via dm-list-esafs on bl2ws5."""
    dm_cmd = f"{_DM_SETUP} ; dm-list-esafs -a --display-format json"
    cmd = f'srun --nodelist={_DM_NODE} bash -c "{dm_cmd}"'
    try:
        result = subprocess.run(
            ["bash", "-c", cmd],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            logger.warning(f"dm-list-esafs failed: {result.stderr}")
            return []
    except Exception as e:
        logger.warning(f"dm-list-esafs error: {e}")
        return []

    groups = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        esaf_id = str(entry.get("esafId", ""))
        # Find PI from experimentUsers
        pi_name = None
        users = entry.get("experimentUsers", [])
        for u in users:
            if u.get("piFlag") == "Yes":
                pi_name = f"{u.get('firstName', '')} {u.get('lastName', '')}".strip()
                break

        beamlines = entry.get("beamline", [])
        beamline = ", ".join(beamlines) if beamlines else entry.get("beamlineStation")

        groups.append({
            "esaf_id": esaf_id,
            "group_name": f"esaf{esaf_id}",
            "beamline": beamline,
            "pi_name": pi_name,
            "esaf_title": entry.get("esafTitle"),
            "start_date": entry.get("experimentStartDate"),
            "end_date": entry.get("experimentEndDate"),
        })

    return groups


@router.get("/esaf-groups")
def get_user_esaf_groups(user: str = Depends(verify_token)):
    """Return ESAF groups for the dropdown. Staff sees all active ESAFs."""
    groups_out = []
    try:
        from qp2.xio.user_group_manager import UserGroupManager
        ugm = UserGroupManager()

        if is_staff_member(user):
            # Staff: show all active/upcoming ESAFs from the accounts DB
            all_esafs = ugm.get_all_active_esafs(days_past=30, days_future=90)
            for info in all_esafs:
                name = info.get("group_name", "")
                match = re.search(r'(\d+)', name)
                esaf_num = match.group(1) if match else name
                groups_out.append({
                    "esaf_id": esaf_num,
                    "group_name": name,
                    "beamline": info.get("beamline"),
                    "pi_name": info.get("pi_full_name"),
                    "start_date": str(info.get("esaf_collect_start", "")) if info.get("esaf_collect_start") else None,
                    "end_date": str(info.get("esaf_collect_end", "")) if info.get("esaf_collect_end") else None,
                })
        else:
            # Regular user: show only their ESAF groups
            groups = ugm.groupnames_from_username(user)
            if groups:
                for g in groups:
                    name = g.get("group_name", "")
                    info = ugm.groupinfo_from_groupname(name)
                    if info:
                        match = re.search(r'(\d+)', name)
                        esaf_num = match.group(1) if match else name
                        groups_out.append({
                            "esaf_id": esaf_num,
                            "group_name": name,
                            "beamline": info.get("beamline"),
                            "pi_name": info.get("pi_full_name"),
                            "start_date": str(info.get("esaf_collect_start", "")) if info.get("esaf_collect_start") else None,
                            "end_date": str(info.get("esaf_collect_end", "")) if info.get("esaf_collect_end") else None,
                        })
    except Exception as e:
        logger.warning(f"Failed to fetch ESAF groups: {e}")

    # Fallback: if no results from UserGroupManager (staff only), try DM server
    if not groups_out and is_staff_member(user):
        logger.info("UserGroupManager returned no ESAFs, trying dm-list-esafs fallback")
        groups_out = _fetch_esafs_from_dm()

    return {"groups": groups_out}


@router.post("/create")
def create_form(req: CreateFormRequest, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Create a new experiment form for an ESAF.

    Access is relaxed for creation: any authenticated user can create a form.
    The ESAF group check may fail for users whose groups aren't yet in the
    accounts DB (e.g. new proposals). Viewing/editing still requires membership.
    """

    existing = session.query(ExperimentForm).filter_by(esaf_id=req.esaf_id).first()
    if existing:
        raise HTTPException(status_code=409, detail="Form already exists for this ESAF")

    form = ExperimentForm(
        esaf_id=req.esaf_id,
        beamline=req.beamline,
        pi_name=req.pi_name,
        experiment_dates=req.experiment_dates,
        contact_phone=req.contact_phone,
        created_by=user,
    )
    session.add(form)
    session.flush()
    return {"status": "ok", "esaf_id": req.esaf_id}


@router.delete("/{esaf_id}")
def delete_form(esaf_id: str, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Delete an experiment form and all its children (files, IPs, tracking, hosts)."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)

    # Remove uploaded files from disk
    for f in form.files:
        if os.path.exists(f.file_path):
            os.remove(f.file_path)

    session.delete(form)
    return {"status": "ok"}


@router.get("/{esaf_id}")
def get_form(esaf_id: str, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Get full experiment form detail."""
    _check_esaf_access(user, esaf_id, session)
    form = (
        session.query(ExperimentForm)
        .filter_by(esaf_id=esaf_id)
        .options(joinedload(ExperimentForm.files))
        .options(joinedload(ExperimentForm.ips))
        .options(joinedload(ExperimentForm.tracking_entries))
        .options(joinedload(ExperimentForm.hosts).joinedload(ExperimentHost.staff))
        .first()
    )
    if not form:
        raise HTTPException(status_code=404, detail="Experiment form not found")
    return _form_to_dict(form)


@router.put("/{esaf_id}")
def update_form(esaf_id: str, req: UpdateFormRequest, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Update form fields (instructions, hard_drive, spreadsheet, etc.)."""
    _check_esaf_access(user, esaf_id, session)
    form = (
        session.query(ExperimentForm)
        .filter_by(esaf_id=esaf_id)
        .options(joinedload(ExperimentForm.hosts).joinedload(ExperimentHost.staff))
        .first()
    )
    if not form:
        raise HTTPException(status_code=404, detail="Experiment form not found")

    changed = []
    if req.instructions is not None and req.instructions != form.instructions:
        form.instructions = req.instructions
        changed.append("instructions")
    if req.has_hard_drive is not None and req.has_hard_drive != form.has_hard_drive:
        form.has_hard_drive = req.has_hard_drive
        changed.append("hard_drive")
    if req.spreadsheet_id is not None and req.spreadsheet_id != form.spreadsheet_id:
        form.spreadsheet_id = req.spreadsheet_id
        changed.append("spreadsheet")
    if req.beamline is not None and req.beamline != form.beamline:
        form.beamline = req.beamline
        changed.append("beamline")
    if req.pi_name is not None and req.pi_name != form.pi_name:
        form.pi_name = req.pi_name
        changed.append("pi_name")
    if req.experiment_dates is not None and req.experiment_dates != form.experiment_dates:
        form.experiment_dates = req.experiment_dates
        changed.append("experiment_dates")
    if req.contact_phone is not None and req.contact_phone != form.contact_phone:
        form.contact_phone = req.contact_phone
        changed.append("contact_phone")

    if changed:
        form.updated_at = datetime.now()
        _notify_hosts(form, changed, user)

    return {"status": "ok", "changed": changed}


# --- File endpoints ---

@router.post("/{esaf_id}/files")
async def upload_experiment_file(
    esaf_id: str,
    file: UploadFile = File(...),
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    """Upload a file attachment (PDB, FASTA, etc.)."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)

    # Validate extension
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"File type {ext} not allowed. Allowed: {', '.join(ALLOWED_EXTENSIONS)}")

    # Validate size (global cap then per-extension cap)
    content = await file.read()
    size_limit = _EXT_SIZE_LIMITS.get(ext, MAX_FILE_SIZE)
    if len(content) > size_limit:
        raise HTTPException(status_code=400, detail=f"File too large. Max {size_limit // (1024*1024)}MB for {ext} files")

    # Validate content (magic bytes, executable detection, UTF-8 for text)
    _validate_file_content(content, ext)

    # Save to disk
    safe_name = _sanitize_filename(file.filename)
    dest_dir = os.path.join(FILE_STORAGE_DIR, esaf_id)
    os.makedirs(dest_dir, exist_ok=True)

    # Avoid overwriting — append counter if needed
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

    # Determine file type from extension
    type_map = {
        ".pdb": "pdb", ".cif": "cif",
        ".fasta": "fasta", ".fa": "fasta", ".seq": "sequence",
        ".csv": "spreadsheet", ".xlsx": "spreadsheet",
        ".txt": "text", ".pdf": "pdf",
    }
    file_type = type_map.get(ext, "other")

    exp_file = ExperimentFile(
        experiment_id=form.id,
        filename=safe_name,
        file_type=file_type,
        file_path=dest_path,
        uploaded_by=user,
    )
    session.add(exp_file)
    session.flush()

    # If PDB/CIF, copy to model storage and register as StructureModel
    model_id = None
    if ext in (".pdb", ".cif"):
        try:
            model_dir = os.path.join(_MODEL_STORAGE_DIR, esaf_id)
            os.makedirs(model_dir, exist_ok=True)
            model_dest = os.path.join(model_dir, safe_name)
            # Avoid collision in model storage
            if os.path.exists(model_dest):
                base_m, ext_m = os.path.splitext(safe_name)
                counter_m = 1
                while os.path.exists(model_dest):
                    model_dest = os.path.join(model_dir, f"{base_m}_{counter_m}{ext_m}")
                    counter_m += 1
            shutil.copy2(dest_path, model_dest)
            model = StructureModel(
                esaf_id=esaf_id,
                filename=os.path.basename(model_dest),
                file_path=model_dest,
                file_type=ext.lstrip("."),
                source="experiment",
                uploaded_by=user,
            )
            session.add(model)
            session.flush()
            model_id = model.id
            logger.info(f"Auto-registered experiment model: {safe_name} -> model_id={model_id}")
        except Exception as e:
            logger.warning(f"Failed to auto-register model from experiment upload: {e}")

    # If spreadsheet (CSV/XLSX), parse and save as Spreadsheet record
    sheet_id = None
    if file_type == "spreadsheet":
        try:
            from qp2.spreadsheet_editor.logic import SpreadsheetManager
            manager = SpreadsheetManager()
            pucks_map = manager.load_file(dest_path)

            if not manager.errors and pucks_map:
                slots = []
                for letter in manager.puck_names:
                    puck = pucks_map.get(letter)
                    if puck:
                        slots.append({"original_label": puck.original_label, "rows": puck.rows})
                    else:
                        slots.append(None)

                data_payload = {
                    "puck_names": manager.puck_names,
                    "slots": slots,
                }

                sheet = Spreadsheet(
                    username=user,
                    name=f"{esaf_id}_{safe_name}",
                    esaf_id=esaf_id,
                    data=json.dumps(data_payload),
                )
                session.add(sheet)
                session.flush()
                sheet_id = sheet.id
                logger.info(f"Auto-imported spreadsheet: {safe_name} -> sheet_id={sheet_id}")
        except Exception as e:
            logger.warning(f"Failed to auto-import spreadsheet from experiment upload: {e}")

    # Notify hosts
    form_with_hosts = (
        session.query(ExperimentForm)
        .filter_by(id=form.id)
        .options(joinedload(ExperimentForm.hosts).joinedload(ExperimentHost.staff))
        .first()
    )
    _notify_hosts(form_with_hosts, [f"file uploaded: {safe_name}"], user)

    return {"status": "ok", "file_id": exp_file.id, "filename": safe_name, "model_id": model_id, "sheet_id": sheet_id}


@router.get("/{esaf_id}/files/{file_id}")
def download_experiment_file(esaf_id: str, file_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Download a file attachment."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)
    exp_file = session.query(ExperimentFile).filter_by(id=file_id, experiment_id=form.id).first()
    if not exp_file:
        raise HTTPException(status_code=404, detail="File not found")
    # Guard against path traversal — ensure file is within the storage directory
    resolved = os.path.realpath(exp_file.file_path)
    if not resolved.startswith(os.path.realpath(FILE_STORAGE_DIR) + os.sep):
        raise HTTPException(status_code=403, detail="Invalid file path")
    if not os.path.exists(resolved):
        raise HTTPException(status_code=404, detail="File missing from storage")
    return FileResponse(resolved, filename=exp_file.filename)


@router.delete("/{esaf_id}/files/{file_id}")
def delete_experiment_file(esaf_id: str, file_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Remove a file attachment."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)
    exp_file = session.query(ExperimentFile).filter_by(id=file_id, experiment_id=form.id).first()
    if not exp_file:
        raise HTTPException(status_code=404, detail="File not found")

    # Guard against path traversal before touching disk
    resolved = os.path.realpath(exp_file.file_path)
    if not resolved.startswith(os.path.realpath(FILE_STORAGE_DIR) + os.sep):
        raise HTTPException(status_code=403, detail="Invalid file path")

    # Remove from disk
    if os.path.exists(resolved):
        os.remove(resolved)

    # If PDB/CIF, also remove the auto-created StructureModel and its copied file
    ext = os.path.splitext(exp_file.filename)[1].lower()
    if ext in (".pdb", ".cif"):
        # Find models created from this experiment upload (matched by filename + source)
        exp_models = (
            session.query(StructureModel)
            .filter_by(esaf_id=esaf_id, source="experiment")
            .filter(StructureModel.filename.like(os.path.splitext(exp_file.filename)[0] + "%"))
            .all()
        )
        for m in exp_models:
            model_resolved = os.path.realpath(m.file_path)
            if model_resolved.startswith(os.path.realpath(_MODEL_STORAGE_DIR)):
                if os.path.exists(model_resolved):
                    os.remove(model_resolved)
            session.delete(m)

    session.delete(exp_file)
    return {"status": "ok"}


# --- Sequence endpoints (for Prediction Form and Spreadsheet SequencePath) ---

@router.get("/{esaf_id}/sequences")
def list_experiment_sequences(esaf_id: str, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Return parsed FASTA content from sequence files uploaded to this experiment.

    Used by the Prediction Form to pre-populate sequence input.
    """
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)

    seq_types = {"fasta", "sequence"}
    seq_files = [f for f in form.files if f.file_type in seq_types]

    results = []
    for sf in seq_files:
        if not os.path.exists(sf.file_path):
            continue
        try:
            with open(sf.file_path, "r") as fh:
                content = fh.read()
            results.append({
                "id": sf.id,
                "filename": sf.filename,
                "content": content,
            })
        except Exception:
            pass

    return results


@router.get("/{esaf_id}/sequence-files")
def list_sequence_files_for_spreadsheet(esaf_id: str, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Simplified list of sequence file paths for populating SequencePath in the spreadsheet editor."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)

    seq_types = {"fasta", "sequence"}
    seq_files = [f for f in form.files if f.file_type in seq_types]

    return [
        {"id": sf.id, "filename": sf.filename, "file_path": sf.file_path}
        for sf in seq_files
    ]


# --- IP endpoints ---

@router.post("/{esaf_id}/ips")
def add_ip(esaf_id: str, req: AddIPRequest, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Add an IP address."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)

    # Validate IP format
    try:
        parse_ip(req.ip_address)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid IP address format")

    exp_ip = ExperimentIP(
        experiment_id=form.id,
        ip_address=req.ip_address,
        label=req.label,
        added_by=user,
    )
    session.add(exp_ip)
    session.flush()
    return {"status": "ok", "ip_id": exp_ip.id}


@router.put("/{esaf_id}/ips/{ip_id}")
def update_ip(esaf_id: str, ip_id: int, req: UpdateIPRequest, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Edit an IP address or its label."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)
    exp_ip = session.query(ExperimentIP).filter_by(id=ip_id, experiment_id=form.id).first()
    if not exp_ip:
        raise HTTPException(status_code=404, detail="IP not found")

    if req.ip_address is not None:
        exp_ip.ip_address = req.ip_address
    if req.label is not None:
        exp_ip.label = req.label
    return {"status": "ok"}


@router.delete("/{esaf_id}/ips/{ip_id}")
def delete_ip(esaf_id: str, ip_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Remove an IP address."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)
    exp_ip = session.query(ExperimentIP).filter_by(id=ip_id, experiment_id=form.id).first()
    if not exp_ip:
        raise HTTPException(status_code=404, detail="IP not found")
    session.delete(exp_ip)
    return {"status": "ok"}


# --- Tracking endpoints ---

@router.post("/{esaf_id}/tracking")
def add_tracking(esaf_id: str, req: AddTrackingRequest, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Add a shipping tracking entry."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)

    if req.carrier not in CARRIER_URLS:
        raise HTTPException(status_code=400, detail=f"Invalid carrier. Choose from: {', '.join(CARRIER_URLS.keys())}")

    if req.direction not in ("inbound", "outbound"):
        raise HTTPException(status_code=400, detail="Direction must be 'inbound' or 'outbound'")

    tracking_url = CARRIER_URLS[req.carrier].format(req.tracking_number)

    entry = ExperimentTracking(
        experiment_id=form.id,
        carrier=req.carrier,
        tracking_number=req.tracking_number,
        direction=req.direction,
        tracking_url=tracking_url,
        added_by=user,
    )
    session.add(entry)
    session.flush()

    # Notify hosts
    form_with_hosts = (
        session.query(ExperimentForm)
        .filter_by(id=form.id)
        .options(joinedload(ExperimentForm.hosts).joinedload(ExperimentHost.staff))
        .first()
    )
    label = CARRIER_LABELS.get(req.carrier, req.carrier)
    _notify_hosts(form_with_hosts, [f"tracking added: {label} {req.tracking_number}"], user)

    return {"status": "ok", "tracking_id": entry.id, "tracking_url": tracking_url}


@router.delete("/{esaf_id}/tracking/{track_id}")
def delete_tracking(esaf_id: str, track_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Remove a tracking entry."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)
    entry = session.query(ExperimentTracking).filter_by(id=track_id, experiment_id=form.id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="Tracking entry not found")
    session.delete(entry)
    return {"status": "ok"}


# --- Host endpoints ---

@router.post("/{esaf_id}/hosts")
def add_host(esaf_id: str, req: AddHostRequest, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Assign a local host (staff member)."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)

    # Verify staff exists
    staff = session.query(Staff).filter_by(id=req.staff_id).first()
    if not staff:
        raise HTTPException(status_code=404, detail="Staff member not found")

    # Check not already assigned
    existing = session.query(ExperimentHost).filter_by(
        experiment_id=form.id, staff_id=req.staff_id
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="Staff member already assigned as host")

    host = ExperimentHost(
        experiment_id=form.id,
        staff_id=req.staff_id,
        assigned_by=user,
    )
    session.add(host)
    session.flush()

    # Send welcome notification to the newly assigned host
    if staff.email:
        try:
            msg = EmailMessage()
            msg["Subject"] = f"[GM/CA] You are assigned as host for ESAF {form.esaf_id}"
            msg["From"] = _NOTIFICATION_FROM
            msg["To"] = staff.email
            msg.set_content(
                f"You have been assigned as local host for ESAF {form.esaf_id}\n"
                f"Beamline: {form.beamline or 'N/A'}\n"
                f"PI: {form.pi_name or 'N/A'}\n"
                f"Assigned by: {user}\n"
            )
            with smtplib.SMTP("localhost") as smtp:
                smtp.send_message(msg)
        except Exception as e:
            logger.warning(f"Failed to send host assignment notification: {e}")

    return {"status": "ok", "host_id": host.id}


@router.delete("/{esaf_id}/hosts/{host_id}")
def delete_host(esaf_id: str, host_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Remove a host assignment."""
    _check_esaf_access(user, esaf_id, session)
    form = _get_form_or_404(session, esaf_id)
    host = session.query(ExperimentHost).filter_by(id=host_id, experiment_id=form.id).first()
    if not host:
        raise HTTPException(status_code=404, detail="Host assignment not found")
    session.delete(host)
    return {"status": "ok"}
