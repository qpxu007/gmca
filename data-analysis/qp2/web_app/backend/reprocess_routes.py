import json
import os
import sys
from typing import List, Optional

import requests
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

try:
    from qp2.db import PipelineStatus
except ImportError:
    print("Warning: Failed to import PipelineStatus model", file=sys.stderr)
    PipelineStatus = None

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
    from qp2.config.servers import ServerConfig
except ImportError:
    ServerConfig = None

from qp2.log.logging_config import get_logger
logger = get_logger(__name__)

ugm = UserGroupManager() if UserGroupManager else None

SUPPORTED_PIPELINES = {"xds", "nxds", "xia2", "xia2_dials", "xia2_ssx", "autoproc", "crystfel"}
MERGE_COMPATIBLE = {"xds", "xia2", "xia2_dials", "xia2_ssx", "autoproc"}


def _validate_request(request) -> List[str]:
    """Return list of validation error messages, empty if valid."""
    errors = []

    if request.pipeline and request.pipeline.lower() not in SUPPORTED_PIPELINES:
        errors.append(f"pipeline '{request.pipeline}' not supported; choose from: {', '.join(sorted(SUPPORTED_PIPELINES))}")

    if request.highres is not None:
        if not (0.5 <= request.highres <= 10.0):
            errors.append(f"resolution {request.highres} Å out of range (0.5–10.0)")

    if request.unit_cell is not None:
        parts = request.unit_cell.strip().split()
        if len(parts) != 6:
            errors.append("unit_cell must have exactly 6 values (a b c α β γ)")
        else:
            try:
                vals = [float(p) for p in parts]
                if any(v <= 0 for v in vals[:3]):
                    errors.append("unit cell lengths (a, b, c) must be positive")
                if any(not (0.0 < v < 180.0) for v in vals[3:]):
                    errors.append("unit cell angles (α, β, γ) must be between 0 and 180 degrees")
            except ValueError:
                errors.append("unit_cell values must all be numbers")

    if request.space_group is not None:
        sg = request.space_group.strip()
        if len(sg) == 0:
            errors.append("space_group cannot be empty if provided")
        elif len(sg) > 20:
            errors.append("space_group too long (max 20 characters)")

    if request.nproc is not None:
        if not (1 <= request.nproc <= 128):
            errors.append(f"nproc {request.nproc} out of range (1–128)")

    return errors


def get_db_session():
    raise RuntimeError("get_db_session dependency not properly overridden")

router = APIRouter(prefix="/processing", tags=["reprocess"])


class ReprocessRequest(BaseModel):
    ids: List[int]
    pipeline: Optional[str] = None
    highres: Optional[float] = None
    space_group: Optional[str] = None
    unit_cell: Optional[str] = None
    merge: bool = False
    nproc: Optional[int] = None


class ReprocessResponse(BaseModel):
    submitted: int
    errors: List[str] = []


def _get_allowed_names(user: str) -> List[str]:
    """Return names the requesting user is authorised to act on.

    Includes the user's own username plus any ESAF group names they belong to.
    Auth succeeds when status.username matches any of these — this covers both
    personal accounts and shared beamline accounts whose username IS the group
    name (e.g. 'gmca-esaf12345').
    """
    allowed = [user]
    if ugm:
        try:
            groups = ugm.groupnames_from_username(user)
            if groups:
                allowed.extend([g['group_name'] for g in groups])
        except Exception:
            pass
    return allowed


def _parse_datasets(status: "PipelineStatus") -> List[dict]:
    """Parse datasets from PipelineStatus, returning list of {path, start, end} dicts."""
    if status.datasets:
        try:
            parsed = json.loads(status.datasets)
            if isinstance(parsed, list):
                result = []
                for item in parsed:
                    if isinstance(item, dict) and "path" in item:
                        result.append(item)
                    elif isinstance(item, str):
                        result.append({"path": item})
                if result:
                    return result
        except (json.JSONDecodeError, TypeError):
            pass

    # Fallback: look for master.h5 in imagedir
    if status.imagedir:
        import os
        import glob
        masters = glob.glob(os.path.join(status.imagedir, "*_master.h5"))
        if masters:
            return [{"path": masters[0]}]

    return []


def _build_payload(status: "PipelineStatus", request: ReprocessRequest, datasets: List[dict]) -> dict:
    pipeline = request.pipeline or status.pipeline
    payload = {
        "pipeline": pipeline,
        "username": status.username,
        "sample_id": status.sampleNumber or status.sampleName or f"id_{status.id}",
        "sampleName": status.sampleName,
        "esaf_id": status.esaf_id,
        # analysis_manager reads "groupname", not "primary_group", to set primary_group
        # on the new PipelineStatus — use the original job's group to preserve ownership.
        "groupname": status.primary_group,
        "pi_id": status.pi_id,
        "beamline": status.beamline,
        "datasets": datasets,
        # Inject slurm_qos only when the QOS tiers have been configured in Slurm.
        # Set env var QP2_SLURM_WEB_QOS=web after running sacctmgr setup.
        # See docs/SLURM_QOS_PRIORITY.md
        **({"slurm_qos": os.environ["QP2_SLURM_WEB_QOS"]} if os.environ.get("QP2_SLURM_WEB_QOS") else {}),
    }
    if request.highres is not None:
        payload["highres"] = request.highres
    if request.space_group:
        payload["space_group"] = request.space_group
    if request.unit_cell:
        payload["unit_cell"] = request.unit_cell
    if request.nproc is not None:
        payload["nproc"] = request.nproc
    return payload


@router.post("/reprocess", response_model=ReprocessResponse)
async def reprocess_datasets(
    request: ReprocessRequest,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    if PipelineStatus is None:
        raise HTTPException(status_code=500, detail="Models not loaded")
    if ServerConfig is None:
        raise HTTPException(status_code=500, detail="ServerConfig not available")

    if not request.ids:
        raise HTTPException(status_code=400, detail="No dataset IDs provided")

    validation_errors = _validate_request(request)
    if validation_errors:
        raise HTTPException(status_code=400, detail="; ".join(validation_errors))

    is_staff = is_staff_member(user)
    allowed_names = _get_allowed_names(user) if not is_staff else None

    dataproc_url = ServerConfig.get_dataproc_url()
    if not dataproc_url:
        raise HTTPException(status_code=503, detail="Data processing server URL not configured")

    launch_url = dataproc_url.rstrip("/") + "/launch_job"

    submitted = 0
    errors = []

    # Fetch all records
    records = []
    for record_id in request.ids:
        status = session.get(PipelineStatus, record_id)
        if not status:
            errors.append(f"ID {record_id}: not found")
            continue

        # Auth check
        if not is_staff and status.username not in allowed_names:
            errors.append(f"ID {record_id}: access denied")
            continue

        # Pipeline check
        effective_pipeline = (request.pipeline or status.pipeline or "").lower()
        if effective_pipeline not in SUPPORTED_PIPELINES:
            errors.append(f"ID {record_id}: pipeline '{effective_pipeline}' not supported for reprocessing")
            continue

        datasets = _parse_datasets(status)
        if not datasets:
            errors.append(f"ID {record_id}: no dataset files found")
            continue

        records.append((status, datasets))

    if not records:
        return ReprocessResponse(submitted=0, errors=errors)

    # Determine submission strategy
    if request.merge and len(records) > 1:
        # Check all use merge-compatible pipeline
        primary_status, primary_datasets = records[0]
        effective_pipeline = (request.pipeline or primary_status.pipeline or "").lower()

        if effective_pipeline not in MERGE_COMPATIBLE:
            errors.append(f"Pipeline '{effective_pipeline}' does not support merging — submitting individually")
            request = ReprocessRequest(**{**request.model_dump(), "merge": False})
        else:
            # Combine all datasets into one payload
            all_datasets = []
            for _, ds_list in records:
                all_datasets.extend(ds_list)

            payload = _build_payload(primary_status, request, all_datasets)
            try:
                resp = requests.post(launch_url, json=payload, timeout=10)
                if resp.status_code == 202:
                    submitted += 1
                else:
                    errors.append(f"Merged job: server returned {resp.status_code}: {resp.text[:200]}")
            except requests.exceptions.Timeout:
                errors.append("Merged job: data processing server timed out")
            except requests.exceptions.ConnectionError:
                errors.append("Merged job: could not connect to data processing server")
            return ReprocessResponse(submitted=submitted, errors=errors)

    # Submit one job per record
    for status, datasets in records:
        payload = _build_payload(status, request, datasets)
        try:
            resp = requests.post(launch_url, json=payload, timeout=10)
            if resp.status_code == 202:
                submitted += 1
            else:
                errors.append(f"ID {status.id}: server returned {resp.status_code}: {resp.text[:200]}")
        except requests.exceptions.Timeout:
            errors.append(f"ID {status.id}: data processing server timed out")
        except requests.exceptions.ConnectionError:
            errors.append(f"ID {status.id}: could not connect to data processing server")

    return ReprocessResponse(submitted=submitted, errors=errors)
