import os
import json
import tempfile
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse
from qp2.web_app.backend.auth import is_staff_member
from qp2.web_app.backend.security import verify_token
from qp2.utils import institution_map
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/distribution", tags=["distribution"])

@router.post("/map")
async def generate_map(
    file: UploadFile = File(...),
    base_tile: str = Form("dark"),
    circle_color: str = Form("blue"),
    size_multiplier: float = Form(1.0),
    opacity: float = Form(0.6),
    corrections: str = Form("{}"),
    user: str = Depends(verify_token)
):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
        
    try:
        corrections_dict = json.loads(corrections)
    except json.JSONDecodeError:
        corrections_dict = {}
        
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in (".csv", ".xls", ".xlsx"):
        raise HTTPException(status_code=400, detail="Only CSV or Excel files allowed")
        
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large")
        
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
        
    try:
        if ext in (".xlsx", ".xls"):
            df = pd.read_excel(tmp_path)
        else:
            df = pd.read_csv(tmp_path)
            
        name_col, count_col = institution_map.detect_columns(df)
        df = df.dropna(subset=[name_col, count_col])
        df = institution_map.merge_institutions(df, name_col, count_col)
        
        logger.debug(f"Generating map with base_tile='{base_tile}'")
        
        html_content, missing_institutions, geocoded_institutions = institution_map.build_map(
            df, name_col, count_col, 
            output=None, 
            base_tile=base_tile, 
            non_interactive=True,
            circle_color_theme=circle_color,
            size_multiplier=size_multiplier,
            opacity=opacity,
            corrections=corrections_dict
        )
        return {"success": True, "html": html_content, "missing": missing_institutions, "geocoded": geocoded_institutions}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating map: {str(e)}")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
