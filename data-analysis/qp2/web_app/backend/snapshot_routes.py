"""Crystal-snapshot API routes.

Two endpoints:

* ``GET /api/datasets/{data_id}/snapshots`` — returns indexed snapshots
  for a given dataset. Combines explicit FK links with implicit matches
  via port + beamline + time window. The UI can use the ``matched_via``
  field to distinguish them.

* ``GET /api/snapshots/{snapshot_id}/image`` — streams the JPEG file.
  Returns 410 Gone if the row exists but the file has been purged
  outside qp2 (storage lifecycle is not managed here — see
  ``qp2/db/models/crystals.py``).
"""

import os
import sys
from datetime import timedelta
from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import or_, and_
from sqlalchemy.orm import Session

try:
    from qp2.db import CrystalSnapshot, DatasetRun
except ImportError:
    print("Warning: Failed to import CrystalSnapshot / DatasetRun", file=sys.stderr)
    CrystalSnapshot = None
    DatasetRun = None

try:
    from qp2.web_app.backend.security import verify_token
except ImportError:
    def verify_token():
        return "user"


def get_db_session():
    raise RuntimeError("get_db_session dependency not properly overridden")


router = APIRouter(tags=["snapshots"])


class SnapshotResponse(BaseModel):
    id: int
    file_path: str
    captured_at: str
    esaf_id: Optional[str] = None
    beamline: Optional[str] = None
    username: Optional[str] = None
    port: Optional[str] = None
    sample_prefix: Optional[str] = None
    omega: Optional[float] = None
    camera_id: str
    dataset_run_id: Optional[int] = None
    file_size: Optional[int] = None
    matched_via: Literal["explicit_fk", "implicit_window"]

    @classmethod
    def from_row(cls, row, matched_via: str) -> "SnapshotResponse":
        return cls(
            id=row.id,
            file_path=row.file_path,
            captured_at=row.captured_at.isoformat(),
            esaf_id=row.esaf_id,
            beamline=row.beamline,
            username=row.username,
            port=row.port,
            sample_prefix=row.sample_prefix,
            omega=row.omega,
            camera_id=row.camera_id,
            dataset_run_id=row.dataset_run_id,
            file_size=row.file_size,
            matched_via=matched_via,
        )


@router.get("/datasets/{data_id}/snapshots", response_model=List[SnapshotResponse], tags=["datasets"])
def list_snapshots_for_dataset(
    data_id: int,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    if CrystalSnapshot is None or DatasetRun is None:
        raise HTTPException(status_code=500, detail="Snapshot models not loaded")

    run = session.query(DatasetRun).filter(DatasetRun.data_id == data_id).first()
    if run is None:
        raise HTTPException(status_code=404, detail=f"Dataset {data_id} not found")

    explicit = (
        session.query(CrystalSnapshot)
        .filter(CrystalSnapshot.dataset_run_id == data_id)
        .all()
    )
    explicit_ids = {r.id for r in explicit}

    implicit: list = []
    if run.mounted and run.beamline and run.created_at:
        window_start = run.created_at - timedelta(hours=1)
        window_end = run.created_at + timedelta(hours=4)
        implicit = (
            session.query(CrystalSnapshot)
            .filter(
                CrystalSnapshot.port == run.mounted,
                CrystalSnapshot.beamline == run.beamline,
                CrystalSnapshot.captured_at >= window_start,
                CrystalSnapshot.captured_at <= window_end,
                CrystalSnapshot.id.notin_(explicit_ids) if explicit_ids else True,
            )
            .all()
        )

    result: List[SnapshotResponse] = []
    result.extend(SnapshotResponse.from_row(r, "explicit_fk") for r in explicit)
    result.extend(SnapshotResponse.from_row(r, "implicit_window") for r in implicit)
    result.sort(key=lambda s: s.captured_at)
    return result


@router.get("/snapshots/{snapshot_id}/image", tags=["snapshots"])
def get_snapshot_image(
    snapshot_id: int,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    if CrystalSnapshot is None:
        raise HTTPException(status_code=500, detail="Snapshot model not loaded")

    row = session.query(CrystalSnapshot).filter(CrystalSnapshot.id == snapshot_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    if not os.path.exists(row.file_path):
        # File purged by external lifecycle (BeegFS policy, manual cleanup, etc.)
        raise HTTPException(status_code=410, detail="Snapshot file no longer available")
    return FileResponse(row.file_path, media_type="image/jpeg")
