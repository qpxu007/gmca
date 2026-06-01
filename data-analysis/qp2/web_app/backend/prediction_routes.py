"""
Structure prediction job management routes.

Program-agnostic design: supports AlphaFold 3 now,
extensible to Chai-1, Boltz, etc. via program-specific handlers.
"""

import json
import os
import re
import subprocess
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from qp2.web_app.backend.security import verify_token
from qp2.web_app.backend.auth import is_staff_member

from qp2.db import PredictionJob, StructureModel
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/predict", tags=["predict"])

# --- DB session dependency (overridden in main.py) ---
def get_db_session():
    raise NotImplementedError("Must be overridden via app.dependency_overrides")


# --- Constants ---

_BEEGFS_BASE = os.environ.get("QP2_PREDICTION_BASE", "/mnt/beegfs/dmadmin")
_TEST_BASE = os.path.join(os.path.expanduser("~"), ".data_viewer")
_BASE = _BEEGFS_BASE if os.path.exists(_BEEGFS_BASE) else _TEST_BASE
PREDICTION_DIR = os.path.join(_BASE, "predictions")
MODEL_STORAGE_DIR = os.path.join(_BASE, "models")

AF3_SIF = os.environ.get("QP2_AF3_SIF", "/mnt/alphafold3/alphafold3/alphafold3.sif")
AF3_DBS = os.environ.get("QP2_AF3_DBS", "/mnt/alphafold3/af3-DBs")
AF3_MODELS = os.environ.get("QP2_AF3_MODELS", "/mnt/alphafold3/af3-models")
AF3_RUN_SCRIPT = os.environ.get("QP2_AF3_RUN_SCRIPT", "/mnt/alphafold3/alphafold3/run_alphafold.py")

AVAILABLE_PROGRAMS = [
    {"id": "alphafold3", "name": "AlphaFold 3"},
]


# --- Helpers ---

def _check_esaf_access(user: str, esaf_id: str):
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
    except Exception:
        pass
    raise HTTPException(status_code=403, detail="Access denied")


def _check_slurm_job(slurm_job_id: str) -> str:
    """Check Slurm job status. Returns 'running', 'completed', or 'unknown'."""
    try:
        result = subprocess.run(
            ["squeue", "-j", slurm_job_id, "-h", "-o", "%T"],
            capture_output=True, text=True, timeout=10
        )
        state = result.stdout.strip()
        if state in ("RUNNING", "PENDING", "CONFIGURING"):
            return "running"
        if state in ("COMPLETED",):
            return "completed"
        if state:
            return state.lower()
        # Empty output means job is no longer in queue
        return "unknown"
    except Exception:
        return "unknown"


def _prepare_af3_job(job_dir: str, job_name: str, sequences: list, seeds: list):
    """Generate AF3 input JSON and return the singularity command string."""
    input_dir = os.path.join(job_dir, "input")
    output_dir = os.path.join(job_dir, "output")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # Build AF3 input JSON
    af3_sequences = []
    for seq in sequences:
        seq_type = seq.get("type", "protein")
        entry = {seq_type: {"id": seq.get("id", "A"), "sequence": seq["sequence"]}}
        af3_sequences.append(entry)

    fold_input = {
        "name": job_name,
        "modelSeeds": seeds if seeds else [42],
        "sequences": af3_sequences,
        "dialect": "alphafold3",
        "version": 3,
    }

    input_json_path = os.path.join(input_dir, "fold_input.json")
    with open(input_json_path, "w") as f:
        json.dump(fold_input, f, indent=2)

    # Build singularity command
    cmd = (
        f"export DBs={AF3_DBS} && export models={AF3_MODELS} && "
        f"singularity exec --nv "
        f"--bind {input_dir}:/root/af_input "
        f"--bind {output_dir}:/root/af_output "
        f"--bind $models:/root/models "
        f"--bind $DBs:/root/public_databases "
        f"--bind /mnt:/mnt "
        f"{AF3_SIF} "
        f"python {AF3_RUN_SCRIPT} "
        f"--json_path=/root/af_input/fold_input.json "
        f"--model_dir=/root/models "
        f"--db_dir=/root/public_databases "
        f"--output_dir=/root/af_output"
    )
    return cmd


# --- Request models ---

class SequenceInput(BaseModel):
    type: str = "protein"  # protein, rna, dna
    id: str = "A"
    sequence: str

class PredictRequest(BaseModel):
    job_name: str
    program: str = "alphafold3"
    sequences: List[SequenceInput]
    seeds: Optional[List[int]] = None


# --- Endpoints ---

@router.get("/programs")
def list_programs(user: str = Depends(verify_token)):
    return AVAILABLE_PROGRAMS


@router.post("/{esaf_id}/submit")
def submit_prediction(
    esaf_id: str,
    request: PredictRequest,
    user: str = Depends(verify_token),
    session: Session = Depends(get_db_session),
):
    _check_esaf_access(user, esaf_id)

    if request.program not in [p["id"] for p in AVAILABLE_PROGRAMS]:
        raise HTTPException(status_code=400, detail=f"Unknown program: {request.program}")

    if not request.sequences:
        raise HTTPException(status_code=400, detail="At least one sequence is required")

    for seq in request.sequences:
        if not seq.sequence or len(seq.sequence) < 10:
            raise HTTPException(status_code=400, detail="Sequence must be at least 10 residues")

    # Sanitize job name to prevent command injection/path traversal
    safe_job_name = re.sub(r'[^\w\-]', '_', request.job_name)

    # Create DB record first to get the ID
    job = PredictionJob(
        esaf_id=esaf_id,
        job_name=safe_job_name,
        program=request.program,
        status="pending",
        input_dir="",
        output_dir="",
        submitted_by=user,
    )
    session.add(job)
    session.flush()  # Get the ID

    job_dir = os.path.join(PREDICTION_DIR, esaf_id, str(job.id))
    os.makedirs(job_dir, exist_ok=True)
    job.input_dir = os.path.join(job_dir, "input")
    job.output_dir = os.path.join(job_dir, "output")

    # Prepare job based on program
    if request.program == "alphafold3":
        cmd = _prepare_af3_job(
            job_dir, safe_job_name,
            [s.model_dump() for s in request.sequences],
            request.seeds or [42],
        )
    else:
        raise HTTPException(status_code=400, detail=f"Program {request.program} not yet implemented")

    # Submit via Slurm
    try:
        from qp2.image_viewer.utils.run_job import run_command
        slurm_job_id = run_command(
            cmd=cmd,
            cwd=job_dir,
            method="slurm",
            gpu=True,
            job_name=f"af3_{safe_job_name}",
            background=True,
            walltime="24:00:00",
        )
        if slurm_job_id:
            job.slurm_job_id = str(slurm_job_id)
            job.status = "running"
            logger.info(f"AF3 job submitted: slurm_id={slurm_job_id}, job_id={job.id}")
        else:
            job.status = "failed"
            job.error_message = "Failed to submit Slurm job"
            logger.error(f"AF3 job submission failed for job_id={job.id}")
    except Exception as e:
        job.status = "failed"
        job.error_message = str(e)
        logger.error(f"AF3 job submission error: {e}")

    return {
        "job_id": job.id,
        "slurm_job_id": job.slurm_job_id,
        "status": job.status,
    }


@router.get("/{esaf_id}/jobs")
def list_jobs(esaf_id: str, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    _check_esaf_access(user, esaf_id)
    jobs = session.query(PredictionJob).filter_by(esaf_id=esaf_id).order_by(PredictionJob.submitted_at.desc()).all()
    return [
        {
            "id": j.id,
            "job_name": j.job_name,
            "program": j.program,
            "slurm_job_id": j.slurm_job_id,
            "status": j.status,
            "submitted_by": j.submitted_by,
            "submitted_at": j.submitted_at.isoformat() if j.submitted_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
            "error_message": j.error_message,
        }
        for j in jobs
    ]


@router.get("/{esaf_id}/jobs/{job_id}")
def get_job(esaf_id: str, job_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    _check_esaf_access(user, esaf_id)
    job = session.query(PredictionJob).filter_by(id=job_id, esaf_id=esaf_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Update status from Slurm if still running
    if job.status == "running" and job.slurm_job_id:
        slurm_state = _check_slurm_job(job.slurm_job_id)
        if slurm_state == "running":
            pass  # Still running
        elif slurm_state == "unknown":
            # Job left queue — check for output files
            output_dir = job.output_dir
            if output_dir and os.path.exists(output_dir):
                cif_files = [f for f in os.listdir(output_dir) if f.endswith(".cif")]
                if cif_files:
                    job.status = "completed"
                    job.completed_at = datetime.now()
                else:
                    job.status = "failed"
                    job.error_message = "No output CIF files found"
                    # Try to read error log
                    log_path = os.path.join(os.path.dirname(output_dir), f"af3_{job.job_name}.out")
                    if os.path.exists(log_path):
                        try:
                            with open(log_path, "r") as f:
                                lines = f.readlines()
                                job.error_message = "".join(lines[-20:])
                        except Exception:
                            pass
            else:
                job.status = "failed"
                job.error_message = "Output directory not found"
        else:
            job.status = "failed"
            job.error_message = f"Slurm job state: {slurm_state}"
        session.commit()

    # Count imported models
    model_count = session.query(StructureModel).filter_by(job_id=job.id).count()

    return {
        "id": job.id,
        "job_name": job.job_name,
        "program": job.program,
        "slurm_job_id": job.slurm_job_id,
        "status": job.status,
        "submitted_by": job.submitted_by,
        "submitted_at": job.submitted_at.isoformat() if job.submitted_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "error_message": job.error_message,
        "model_count": model_count,
    }


@router.post("/{esaf_id}/jobs/{job_id}/import")
def import_models(esaf_id: str, job_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    """Import completed prediction output models into the model store."""
    _check_esaf_access(user, esaf_id)
    job = session.query(PredictionJob).filter_by(id=job_id, esaf_id=esaf_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "completed":
        raise HTTPException(status_code=400, detail="Job is not completed")

    output_dir = job.output_dir
    if not output_dir or not os.path.exists(output_dir):
        raise HTTPException(status_code=404, detail="Output directory not found")

    # Find all CIF model files
    cif_files = sorted([f for f in os.listdir(output_dir) if f.endswith(".cif")])
    if not cif_files:
        raise HTTPException(status_code=404, detail="No CIF files found in output")

    # Check if already imported
    existing = session.query(StructureModel).filter_by(job_id=job.id).count()
    if existing > 0:
        raise HTTPException(status_code=400, detail=f"Models already imported ({existing} files)")

    imported = []
    for filename in cif_files:
        file_path = os.path.join(output_dir, filename)
        model = StructureModel(
            esaf_id=esaf_id,
            filename=filename,
            file_path=file_path,
            file_type="cif",
            source="prediction",
            job_id=job.id,
            uploaded_by=job.submitted_by,
        )
        session.add(model)
        imported.append(filename)

    session.flush()
    logger.info(f"Imported {len(imported)} models from job {job_id}")
    return {"status": "ok", "imported": imported, "count": len(imported)}
