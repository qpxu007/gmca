import requests
import os
import sys
import shutil
import tempfile
import json
from pathlib import Path
from typing import List, Dict, Optional, Any
from fastapi import FastAPI, UploadFile, File, HTTPException, Body, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from datetime import datetime

# Robustly find project root
# Current file is web_app/backend/main.py
# Project root is ../..
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.append(str(project_root))

from qp2.log.logging_config import setup_logging
setup_logging(root_name="qp2")

try:
    from qp2.config.servers import ServerConfig
    ServerConfig.log_all_configs()
except Exception as e:
    print(f"Warning: Failed to log server configurations: {e}")

try:
    from spreadsheet_editor.logic import SpreadsheetManager, Puck
except ImportError as e:
    print(f"Error importing spreadsheet_editor: {e}", file=sys.stderr)
    print(f"sys.path: {sys.path}", file=sys.stderr)
    # Re-raise to crash fast if critical dependency is missing
    raise e

try:
    from qp2.data_viewer.utils import get_rpc_url
except ImportError:
    def get_rpc_url():
        return None

try:
    from xio.db_manager import DBManager
    from qp2.data_viewer.models import Spreadsheet
except ImportError as e:
    print(f"Error importing DB components: {e}", file=sys.stderr)
    DBManager = None
    Spreadsheet = None

from auth import check_gmca_pw, is_staff_member
from security import create_access_token, verify_token
import scheduler # Import the module
import dataset_routes as datasets # Import dataset routes
import processing_routes as processing # Import processing routes
import h5_routes # Import h5grove wrapper
import chat_routes # Import chat routes

# Configure h5grove to allow absolute paths
os.environ["H5GROVE_BASE_DIR"] = "/"

app = FastAPI(title="Spreadsheet Editor API")

# Initialize DB Manager
# We rely on default behavior or explicit SQLite path for web app if needed
# For now, let it auto-detect or fail gracefully
db_manager = None
if DBManager:
    try:
        # Pass a beamline default or rely on hostname
        # If hostname doesn't match bl1/bl2, it uses default from DBManager
        db_manager = DBManager(beamline="default")
    except Exception as e:
        print(f"Failed to init DBManager: {e}", file=sys.stderr)

# Dependency to provide a database session
def get_db_session():
    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not initialized")
    
    # We use the db_manager's session factory directly to allow FastAPI to manage the scope via 'yield'
    # db_manager.get_session() is a context manager, which is good for blocks, 
    # but for FastAPI dependencies, yielding the session object is standard.
    if not db_manager.Session:
         raise HTTPException(status_code=503, detail="Database session factory not available")
         
    session = db_manager.Session()
    try:
        yield session
        # We can choose to commit here if we want auto-commit on success
        # session.commit() 
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

# Override the dependency in the scheduler and dataset modules
app.dependency_overrides[scheduler.get_db_session] = get_db_session
app.dependency_overrides[datasets.get_db_session] = get_db_session
app.dependency_overrides[processing.get_db_session] = get_db_session
app.dependency_overrides[h5_routes.get_db_session] = get_db_session

# Register the routers
print("Including scheduler router...", file=sys.stderr)
app.include_router(scheduler.router)
print(f"Scheduler router included. Prefix: {scheduler.router.prefix}", file=sys.stderr)

print("Including dataset router...", file=sys.stderr)
app.include_router(datasets.router)
print(f"Dataset router included. Prefix: {datasets.router.prefix}", file=sys.stderr)

print("Including processing router...", file=sys.stderr)
app.include_router(processing.router)
print(f"Processing router included. Prefix: {processing.router.prefix}", file=sys.stderr)

print("Including h5grove router...", file=sys.stderr)
app.include_router(h5_routes.router)

print("Including chat router...", file=sys.stderr)
app.include_router(chat_routes.router)

# Allow CORS for local development (React frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Determine path to static files (frontend build)
# In bundle: sys._MEIPASS/web_app/frontend/dist
# In dev: web_app/frontend/dist (relative to project root)
if getattr(sys, 'frozen', False):
    static_dir = os.path.join(sys._MEIPASS, 'web_app', 'frontend', 'dist')
else:
    # web_app/backend/main.py -> ../../web_app/frontend/dist
    static_dir = os.path.join(project_root, 'web_app', 'frontend', 'dist')

if os.path.exists(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")
    # We mount the root to serve index.html for SPA routing, 
    # but FastAPI's static files at root can mask API routes if not careful.
    # A common pattern for SPA:
    # Serve specific assets (js/css) via /assets (Vite default)
    # Catch-all route serves index.html
    
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        # Allow API routes to pass through (FastAPI handles this if defined before)
        # But this is a catch-all, so it matches everything not matched above.
        
        # Check if file exists in static (e.g. favicon.ico)
        file_path = os.path.join(static_dir, full_path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)
            
        # Fallback to index.html
        return FileResponse(os.path.join(static_dir, "index.html"))

else:
    @app.get("/")
    def read_root():
        return {"message": "Spreadsheet Editor API is running (Frontend not found)"}
    original_label: str
    rows: List[Dict[str, str]]

class ExportRequest(BaseModel):
    puck_names: List[str]
    slots: List[Optional[PuckData]] # Ordered list of pucks (or nulls) matching puck_names
    filename: str

class SendRequest(ExportRequest):
    rpc_url: Optional[str] = None

class LoginRequest(BaseModel):
    username: str
    password: str

# --- New Models for Saving ---
class SaveSpreadsheetRequest(BaseModel):
    name: str
    esaf_id: str
    puck_names: List[str]
    slots: List[Optional[PuckData]]

class SpreadsheetResponse(BaseModel):
    id: int
    name: str
    esaf_id: Optional[str]
    username: str
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class SpreadsheetDetail(SpreadsheetResponse):
    puck_names: List[str]
    slots: Any # Using Any to avoid complex recursive typing for now, essentially the JSON data

# --- Endpoints ---

@app.post("/login")
async def login(request: LoginRequest):
    if check_gmca_pw(request.username, request.password):
        token = create_access_token(request.username)
        is_admin = is_staff_member(request.username)
        return {"success": True, "token": token, "user": request.username, "is_admin": is_admin}
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")

@app.get("/")
def read_root():
    return {"message": "Spreadsheet Editor API is running"}

@app.post("/send_to_http")
async def send_to_http(request: SendRequest, user: str = Depends(verify_token)):
    """
    Sends the spreadsheet to an HTTP RPC service.
    """
    manager = SpreadsheetManager(puck_names=request.puck_names)
    
    # Reconstruct slots
    slots_for_logic = []
    for item in request.slots:
        if item:
            slots_for_logic.append(Puck(item.original_label, item.rows))
        else:
            slots_for_logic.append(None)
            
    # Determine URL
    url = request.rpc_url
    if not url:
        url = get_rpc_url()
    
    if not url:
        # Signal frontend to prompt user
        return {"success": False, "error_code": "URL_REQUIRED", "message": "RPC URL not found"}

    # Create temp file
    # Default to .xlsx as preferred
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp_path = tmp.name
        
    try:
        manager.save_file(tmp_path, slots_for_logic)
        
        # Send to RPC
        puck_map = "".join(request.puck_names)
        payload = {
            "module": "spreadsheet_import",
            "path": tmp_path,
            "map": puck_map
        }
        
        # Determine host for logging/debug?
        # print(f"Sending to {url}", file=sys.stderr)
        
        resp = requests.post(url, data=payload, timeout=10)
        
        if resp.status_code == 200:
            return {"success": True, "message": "Spreadsheet sent successfully."}
        else:
            return {"success": False, "message": f"RPC Error {resp.status_code}: {resp.text}"}
            
    except Exception as e:
        return {"success": False, "message": str(e)}
    finally:
        # We leave the file if RPC needs path on disk? 
        # Same logic as desktop app.
        pass

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...), 
    puck_names: str = None, 
    user: str = Depends(verify_token)
):
    """
    Receives a file, saves it temporarily, parses it using SpreadsheetManager,
    and returns the puck data structure.
    puck_names: Comma-separated string of puck names (optional)
    """
    
    # Parse puck names if provided
    names_list = None
    if puck_names:
        names_list = [n.strip() for n in puck_names.split(',') if n.strip()]
    
    manager = SpreadsheetManager(puck_names=names_list)
    
    # Create temp file to save upload
    suffix = os.path.splitext(file.filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
    
    try:
        # Use existing logic to load
        pucks_map = manager.load_file(tmp_path)
        
        # Check for errors
        if manager.errors:
            # We return errors as a successful response but with error field?
            # Or HTTP 400? Let's use 400 for errors.
            return {
                "success": False,
                "errors": manager.errors
            }
        
        # Convert Puck objects to JSON-friendly dict
        # { "A": { "original_label": "A", "rows": [...] } }
        result = {}
        for letter, puck in pucks_map.items():
            result[letter] = {
                "original_label": puck.original_label,
                "rows": puck.rows
            }
            
        return {
            "success": True,
            "filename": file.filename,
            "pucks": result,
            "puck_names": manager.puck_names
        }
        
    except Exception as e:
        return {"success": False, "errors": [str(e)]}
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

@app.post("/create_empty")
async def create_empty(puck_names: str = None, user: str = Depends(verify_token)):
    """
    Creates an empty structure.
    """
    names_list = None
    if puck_names:
        names_list = [n.strip() for n in puck_names.split(',') if n.strip()]
        
    manager = SpreadsheetManager(puck_names=names_list)
    pucks_map = manager.create_empty_pucks()
    
    result = {}
    for letter, puck in pucks_map.items():
        result[letter] = {
            "original_label": puck.original_label,
            "rows": puck.rows
        }
    
    return {
        "success": True,
        "filename": "New Spreadsheet",
        "pucks": result,
        "puck_names": manager.puck_names
    }

@app.post("/export")
async def export_file(request: ExportRequest, user: str = Depends(verify_token)):
    """
    Receives the grid state and generates a file.
    """
    manager = SpreadsheetManager(puck_names=request.puck_names)
    
    # Reconstruct slots list for logic.save_file
    # logic.save_file expects List[Optional[Puck]]
    slots_for_logic = []
    
    for item in request.slots:
        if item:
            # Reconstruct Puck object
            p = Puck(item.original_label, item.rows)
            slots_for_logic.append(p)
        else:
            slots_for_logic.append(None)
            
    # Create temp file for output
    # Use the filename extension provided by user or default to .csv
    fname = request.filename or "export.csv"
    suffix = os.path.splitext(fname)[1]
    if not suffix:
        suffix = ".csv"
        fname += suffix
        
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = tmp.name
        
    try:
        manager.save_file(tmp_path, slots_for_logic)
        
        # Return as downloadable file
        return FileResponse(
            path=tmp_path, 
            filename=fname, 
            media_type='application/octet-stream',
            background=None # We might want a background task to delete this later
        )
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise HTTPException(status_code=500, detail=str(e))

# --- Database Endpoints ---

@app.post("/spreadsheets/save")
async def save_spreadsheet(request: SaveSpreadsheetRequest, user: str = Depends(verify_token)):
    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not available")

    # Validate ESAF ID format: "esaf" + digits
    if not request.esaf_id.lower().startswith("esaf") or not request.esaf_id[4:].isdigit():
        raise HTTPException(status_code=400, detail="ESAF ID must be format 'esaf' followed by digits (e.g., esaf12345)")
    
    # Serialize the full state
    data_payload = {
        "puck_names": request.puck_names,
        # Convert Pydantic models to dicts
        "slots": [s.dict() if s else None for s in request.slots]
    }
    json_data = json.dumps(data_payload)
    
    # Check if a spreadsheet with this name already exists for this user?
    # For simplicity, we just create new or update if ID provided?
    # The request doesn't have ID, so it's a "Save New" or "Save As".
    # We could check name collision.
    
    existing = db_manager.find_first(Spreadsheet, username=user, name=request.name)
    
    if existing:
        # Update existing
        existing.data = json_data
        existing.esaf_id = request.esaf_id
        existing.updated_at = datetime.now()
        # db_manager.save_object uses session.add() which works for new objects.
        # For updates, since 'existing' is detached (session closed in find_first), we need to handle it.
        # Actually, db_manager.save_object does NOT handle detached updates cleanly without merge.
        # Let's use update_by_pk for safety.
        
        success = db_manager.update_by_pk(Spreadsheet, existing.id, {
            "data": json_data, 
            "esaf_id": request.esaf_id,
            "updated_at": datetime.now()
        })
        msg = "Spreadsheet updated."
    else:
        # Create new
        new_sheet = Spreadsheet(
            username=user,
            name=request.name,
            esaf_id=request.esaf_id,
            data=json_data
        )
        success = db_manager.save_object(new_sheet)
        msg = "Spreadsheet saved."
        
    if success:
        return {"success": True, "message": msg}
    else:
        raise HTTPException(status_code=500, detail="Database error saving spreadsheet")

@app.get("/spreadsheets/list", response_model=List[SpreadsheetResponse])
async def list_spreadsheets(user: str = Depends(verify_token)):
    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not available")
        
    sheets_orm = []
    # Explicitly manage session to ensure ORM objects are processed while session is active
    with db_manager.get_session() as session:
        if is_staff_member(user):
            # Admin/Staff sees all
            sheets_orm = session.query(Spreadsheet).all()
        else:
            # User sees their own
            sheets_orm = session.query(Spreadsheet).filter_by(username=user).all()
    
    # Now convert ORM objects to Pydantic models outside the session context
    # Pydantic v2 uses model_validate for ORM objects with from_attributes=True
    return [SpreadsheetResponse.model_validate(sheet) for sheet in sheets_orm]

@app.get("/spreadsheets/{sheet_id}", response_model=SpreadsheetDetail)
async def get_spreadsheet(sheet_id: int, user: str = Depends(verify_token)):
    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not available")
        
    sheet_orm = None
    with db_manager.get_session() as session:
        sheet_orm = session.get(Spreadsheet, sheet_id)
        
        if not sheet_orm:
            raise HTTPException(status_code=404, detail="Spreadsheet not found")
            
        # Permission check
        if sheet_orm.username != user and not is_staff_member(user):
            raise HTTPException(status_code=403, detail="Not authorized to view this spreadsheet")
            
        # Deserialize data
        try:
            data_payload = json.loads(sheet_orm.data)
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="Corrupted data in database")
            
        # Explicitly construct SpreadsheetDetail while in session
        return SpreadsheetDetail(
            id=sheet_orm.id,
            name=sheet_orm.name,
            esaf_id=sheet_orm.esaf_id,
            username=sheet_orm.username,
            created_at=sheet_orm.created_at,
            updated_at=sheet_orm.updated_at,
            puck_names=data_payload.get("puck_names", []),
            slots=data_payload.get("slots", [])
        )

@app.delete("/spreadsheets/{sheet_id}")
async def delete_spreadsheet(sheet_id: int, user: str = Depends(verify_token)):
    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not available")

    with db_manager.get_session() as session:
        sheet_orm = session.get(Spreadsheet, sheet_id)
        
        if not sheet_orm:
            raise HTTPException(status_code=404, detail="Spreadsheet not found")

        # Permission check
        if sheet_orm.username != user and not is_staff_member(user):
            raise HTTPException(status_code=403, detail="Not authorized to delete this spreadsheet")

        # Delete the object directly within the active session
        session.delete(sheet_orm)
        # session.flush() is not strictly necessary as the session.commit() in get_session context manager will handle it
    return {"success": True, "message": "Spreadsheet deleted"}