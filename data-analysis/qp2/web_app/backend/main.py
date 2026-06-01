import asyncio
import atexit
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
import requests
import os
import sys
import shutil
import tempfile
import json
import time as _time
from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Optional, Any
from fastapi import FastAPI, UploadFile, File, HTTPException, Body, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from qp2.log.logging_config import setup_logging, get_logger
setup_logging(root_name="qp2")
logger = get_logger(__name__)

try:
    from qp2.config.servers import ServerConfig
    ServerConfig.log_all_configs()
except Exception as e:
    print(f"Warning: Failed to log server configurations: {e}")

try:
    from qp2.spreadsheet_editor.logic import SpreadsheetManager, Puck
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
    from qp2.xio.db_manager import DBManager, get_beamline_from_hostname
    from qp2.db import Spreadsheet
except ImportError as e:
    print(f"Error importing DB components: {e}", file=sys.stderr)
    DBManager = None
    Spreadsheet = None

from qp2.web_app.backend.auth import check_gmca_pw, is_staff_member
from qp2.web_app.backend.security import (
    create_access_token, verify_token,
    set_auth_cookie, clear_auth_cookie,
    revoke_token, decode_token_claims,
    refresh_token_if_needed,
)
from qp2.web_app.backend import scheduler # Import the module
from qp2.web_app.backend import dataset_routes as datasets # Import dataset routes
from qp2.web_app.backend import processing_routes as processing # Import processing routes
from qp2.web_app.backend import h5_routes # Import h5grove wrapper
from qp2.web_app.backend import chat_routes # Import chat routes
from qp2.web_app.backend import viewer_routes # Import image viewer routes
from qp2.web_app.backend import experiment_routes # Import experiment preparation routes
from qp2.web_app.backend import reprocess_routes # Import reprocess routes
from qp2.web_app.backend import model_routes # Import structure model routes
from qp2.web_app.backend import prediction_routes # Import prediction job routes
from qp2.web_app.backend import rcsb_routes # Import RCSB report routes
from qp2.web_app.backend import snapshot_routes # Import crystal snapshot routes
from qp2.web_app.backend import archive_routes  # Import archive tracker routes
from qp2.web_app.backend import archive_worker  # Import archive background worker
from qp2.web_app.backend import distribution_routes # Import institution map routes

# Configure h5grove base directory — restrict to data paths
os.environ["H5GROVE_BASE_DIR"] = os.environ.get("QP2_DATA_DIR", "/mnt/beegfs/DATA")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Archive scan + DM status sync are registered as APScheduler jobs below
    # so cron state is shared across uvicorn workers via the SQLAlchemyJobStore.
    yield

app = FastAPI(title="Spreadsheet Editor API", lifespan=lifespan)

# Initialize DB Manager
# We rely on default behavior or explicit SQLite path for web app if needed
# For now, let it auto-detect or fail gracefully
db_manager = None
if DBManager:
    try:
        # Auto-detect beamline from hostname (bl1/bl2 → MySQL, other → PostgreSQL default)
        db_manager = DBManager()
    except Exception as e:
        print(f"Failed to init DBManager: {e}", file=sys.stderr)

# Wire archive_worker session factory after DB init
if db_manager:
    archive_worker.set_session_factory(db_manager.Session)
    rcsb_routes.set_session_factory(db_manager.Session)
    scheduler.set_session_factory(db_manager.Session)
    # Seed scheduled task defaults on first startup
    _seed_session = db_manager.Session()
    try:
        rcsb_routes._seed_scheduled_tasks(_seed_session)
    except Exception as _e:
        _seed_session.rollback()
        print(f"Warning: scheduled task seed failed: {_e}", file=sys.stderr)
    finally:
        _seed_session.close()

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
        session.commit()
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
app.dependency_overrides[viewer_routes.get_db_session] = get_db_session
app.dependency_overrides[experiment_routes.get_db_session] = get_db_session
app.dependency_overrides[reprocess_routes.get_db_session] = get_db_session
app.dependency_overrides[model_routes.get_db_session] = get_db_session
app.dependency_overrides[prediction_routes.get_db_session] = get_db_session
app.dependency_overrides[rcsb_routes.get_db_session] = get_db_session
app.dependency_overrides[archive_routes.get_db_session] = get_db_session
app.dependency_overrides[snapshot_routes.get_db_session] = get_db_session

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

print("Including viewer router...", file=sys.stderr)
app.include_router(viewer_routes.router)

print("Including experiment router...", file=sys.stderr)
app.include_router(experiment_routes.router)

print("Including reprocess router...", file=sys.stderr)
app.include_router(reprocess_routes.router)

print("Including model router...", file=sys.stderr)
app.include_router(model_routes.router)

print("Including prediction router...", file=sys.stderr)
app.include_router(prediction_routes.router)

print("Including RCSB router...", file=sys.stderr)
app.include_router(rcsb_routes.router)

print("Including distribution router...", file=sys.stderr)
app.include_router(distribution_routes.router)

app.include_router(archive_routes.router)

app.include_router(snapshot_routes.router)

# Ensure RCSB-related tables exist
try:
    from qp2.db import Base as ModelBase
    if db_manager and db_manager.engine:
        ModelBase.metadata.create_all(db_manager.engine, checkfirst=True)
except Exception as e:
    print(f"Warning: Failed to create RCSB tables: {e}", file=sys.stderr)

# Global exception handler — logs full traceback before returning 500
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception(f"Unhandled error on {request.method} {request.url.path}: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


# Security headers + request logging middleware (runs on every response)
@app.middleware("http")
async def add_security_headers(request, call_next):
    start = _time.monotonic()
    response = await call_next(request)
    elapsed_ms = (_time.monotonic() - start) * 1000
    # Skip logging for static assets to keep logs readable
    if not request.url.path.startswith("/assets"):
        logger.info(f"{request.method} {request.url.path} → {response.status_code} ({elapsed_ms:.0f}ms)")
    if os.environ.get("QP2_ENV") != "test":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    # COOP/COEP headers required for SharedArrayBuffer (Mol* WASM viewer).
    # Only applied to model/prediction routes to avoid breaking cross-origin
    # resources (fonts, CDN images) on other pages.
    path = request.url.path
    if path.startswith("/models") or path.startswith("/predictions"):
        response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
        response.headers.setdefault("Cross-Origin-Embedder-Policy", "require-corp")
    # CSP: restrict script/style sources; allow blob:/wasm for Mol* viewer
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval' blob:; "
        "style-src 'self' 'unsafe-inline'; img-src 'self' data: blob: https://cdn.rcsb.org; "
        "connect-src 'self' https://cdn.rcsb.org https://files.rcsb.org; "
        "worker-src 'self' blob:; font-src 'self'; frame-ancestors 'none'",
    )
    return response

@app.middleware("http")
async def sliding_session_refresh(request: Request, call_next):
    response = await call_next(request)
    if 200 <= response.status_code < 300:
        refresh_token_if_needed(request, response)
    return response

# Allow CORS for local development (React frontend)
_cors_origins = os.environ.get("QP2_CORS_ORIGINS", "http://localhost:5173,http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["X-Vmin", "X-Vmax", "X-Frame", "X-Total-Frames"],
)

# --- Weekly APS pub database auto-sync ---
import threading

def _weekly_aps_sync():
    """Background thread: sync APS pub DB if stale (>7 days).

    Race-safe across uvicorn workers: SELECT ... FOR UPDATE on APSSyncStatus
    means a second worker blocks until the first completes, then sees a fresh
    last_sync and skips. Without this, concurrent workers would race on
    DELETE+INSERT in _download_aps_pubdb and corrupt the table.
    """
    import time as _sync_time
    _sync_time.sleep(10)  # Wait for app to fully start

    while True:
        try:
            if db_manager and db_manager.Session:
                session = db_manager.Session()
                try:
                    from qp2.db import APSSyncStatus
                    status = (session.query(APSSyncStatus)
                              .with_for_update()
                              .first())
                    needs_sync = (
                        not status or
                        not status.last_sync or
                        (datetime.now(timezone.utc).replace(tzinfo=None) - status.last_sync).days >= 7
                    )
                    if needs_sync:
                        print("Auto-syncing APS pub database...", file=sys.stderr)
                        rcsb_routes._download_aps_pubdb(session)
                        session.commit()
                        print("APS pub database auto-sync complete.", file=sys.stderr)
                    else:
                        session.commit()   # release the FOR UPDATE lock
                except Exception as e:
                    session.rollback()
                    print(f"APS auto-sync failed: {e}", file=sys.stderr)
                finally:
                    session.close()
        except Exception as e:
            print(f"APS sync thread error: {e}", file=sys.stderr)

        _sync_time.sleep(86400)  # Check again in 24 hours

_sync_thread = threading.Thread(target=_weekly_aps_sync, daemon=True)
_sync_thread.start()

# --- APScheduler for RCSB report jobs ---
# SQLAlchemyJobStore shares trigger state across uvicorn workers via the DB.
# Without it, each worker would run its own in-memory scheduler and fire each
# cron N times. add_job(..., replace_existing=True) is required because every
# worker startup re-registers the same job id.
_scheduler = None
if db_manager and db_manager.Session and db_manager.engine:
    _scheduler = BackgroundScheduler(
        timezone="US/Central",
        jobstores={'default': SQLAlchemyJobStore(
            engine=db_manager.engine, tablename='apscheduler_jobs')},
        job_defaults={'coalesce': True, 'max_instances': 1},
    )
    # Session factory is injected into rcsb_routes at startup
    # (rcsb_routes.set_session_factory above). It is intentionally NOT passed
    # via kwargs — APScheduler's SQLAlchemyJobStore pickles kwargs, and a
    # SQLAlchemy sessionmaker references engine internals (e.g.
    # create_engine.<locals>.connect) that cannot be pickled.
    _scheduler.add_job(
        rcsb_routes._job_gmca_weekly,
        CronTrigger(day_of_week="wed", hour=8, timezone="US/Central"),
        id="gmca_weekly",
        name="GMCA Weekly Report",
        misfire_grace_time=3600,
        replace_existing=True,
    )
    _scheduler.add_job(
        rcsb_routes._job_aps_pub_monthly,
        CronTrigger(day=1, hour=8, timezone="US/Central"),
        id="aps_pub_monthly",
        name="APS Pub Monthly Report",
        misfire_grace_time=3600,
        replace_existing=True,
    )
    _scheduler.add_job(
        archive_worker.run_scan_job_scheduled,
        IntervalTrigger(hours=6),
        id="archive_scan",
        name="Archive Scan (every 6 h)",
        misfire_grace_time=3600,
        replace_existing=True,
    )
    _scheduler.add_job(
        archive_worker.run_status_sync_scheduled,
        IntervalTrigger(minutes=5),
        id="archive_status_sync",
        name="Archive DM Status Sync (every 5 min)",
        misfire_grace_time=180,
        replace_existing=True,
    )
    
    def _run_chat_archive():
        with db_manager.get_session() as session:
            from qp2.web_app.backend.chat_routes import archive_daily_chat_messages
            archive_daily_chat_messages(session)

    _scheduler.add_job(
        _run_chat_archive,
        CronTrigger(hour=8, minute=0),
        id="chat_archive_daily",
        name="Daily Chat Archive (8 AM)",
        misfire_grace_time=3600,
        replace_existing=True,
    )
    
    _scheduler.add_job(
        scheduler._job_send_staff_reminders,
        CronTrigger(hour=9, minute=0, timezone="US/Central"),
        id="staff_reminders_daily",
        name="Daily Staff Reminders (9 AM)",
        misfire_grace_time=3600,
        replace_existing=True,
    )
    
    _scheduler.start()
    atexit.register(_scheduler.shutdown)
    print("APScheduler started: gmca_weekly (Wed 8AM), aps_pub_monthly (1st 8AM), "
          "archive_scan (6h), archive_status_sync (5min), chat_archive_daily (8AM), staff_reminders_daily (9AM)", file=sys.stderr)
else:
    print("Warning: APScheduler not started — db_manager unavailable", file=sys.stderr)

# Determine path to static files (frontend build)
# In bundle: sys._MEIPASS/web_app/frontend/dist
# In dev: qp2/web_app/frontend/dist (resolved relative to this file)
if getattr(sys, 'frozen', False):
    static_dir = os.path.join(sys._MEIPASS, 'web_app', 'frontend', 'dist')
else:
    static_dir = str(Path(__file__).resolve().parent.parent / "frontend" / "dist")

if os.path.exists(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")
    # We mount the root to serve index.html for SPA routing, 
    # but FastAPI's static files at root can mask API routes if not careful.
    # A common pattern for SPA:
    # Serve specific assets (js/css) via /assets (Vite default)
    # Catch-all route serves index.html
    
    # NOTE: SPA catch-all is registered via _register_spa_catchall() at the
    # bottom of this file, AFTER all API routes, so it doesn't shadow them.
    pass

else:
    @app.get("/")
    def read_root():
        return {"message": "Spreadsheet Editor API is running (Frontend not found)"}

class PuckData(BaseModel):
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

def _ip_matches_pattern(ip: str, pattern: str) -> bool:
    """Check if IP matches a wildcard pattern like '10.20.*.*'."""
    ip_parts = ip.split(".")
    pat_parts = pattern.split(".")
    if len(ip_parts) != 4 or len(pat_parts) != 4:
        return False
    return all(p == "*" or p == i for p, i in zip(pat_parts, ip_parts))

# Facility network — always allowed
_FACILITY_PATTERNS = {"10.*.*.*", "164.54.*.*", "140.221.*.*", "146.137.*.*", "127.0.0.1"}

# --- Login rate limiting ---
_login_attempts: dict = defaultdict(list)
_RATE_WINDOW = 300   # seconds
_RATE_LIMIT = 5      # max attempts per window per IP

def _check_rate_limit(ip: str):
    now = _time.time()
    _login_attempts[ip] = [t for t in _login_attempts[ip] if now - t < _RATE_WINDOW]
    if len(_login_attempts[ip]) >= _RATE_LIMIT:
        logger.warning(f"Rate limit exceeded: ip={ip}")
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")
    _login_attempts[ip].append(now)

# Trusted reverse-proxy addresses — only trust X-Forwarded-For from these
_TRUSTED_PROXIES = {"127.0.0.1", "::1", "localhost"}

def _get_client_ip(req: Request) -> str:
    """Resolve real client IP, accounting for reverse proxies.

    Only trusts X-Forwarded-For when the direct TCP connection comes from a
    known local proxy (nginx/Apache on the same machine). This prevents clients
    from spoofing the header to bypass rate limiting or IP access control.
    Takes the rightmost entry added by the trusted proxy, not the leftmost
    (which is client-controlled).
    """
    direct_ip = req.client.host if req.client else "127.0.0.1"
    if direct_ip not in _TRUSTED_PROXIES:
        # Direct connection — header cannot be trusted
        return direct_ip
    forwarded = req.headers.get("X-Forwarded-For")
    if forwarded:
        # Rightmost entry was appended by our trusted proxy
        return forwarded.split(",")[-1].strip()
    return direct_ip

class FrontendLogRequest(BaseModel):
    level: str = "error"      # error | warn | info
    message: str
    stack: Optional[str] = None
    url: Optional[str] = None
    component: Optional[str] = None

_fe_log = logging.getLogger("qp2.frontend")

@app.post("/log")
async def frontend_log(entry: FrontendLogRequest, req: Request):
    """Receive client-side errors and write them to the server log."""
    client_ip = _get_client_ip(req)
    msg = f"[FRONTEND] [{client_ip}] {entry.message}"
    if entry.url:
        msg += f" | url={entry.url}"
    if entry.component:
        msg += f" | component={entry.component}"
    if entry.stack:
        msg += f"\n{entry.stack}"
    level = entry.level.lower()
    if level == "warn":
        _fe_log.warning(msg)
    elif level == "info":
        _fe_log.info(msg)
    else:
        _fe_log.error(msg)
    return {"ok": True}


@app.post("/login")
async def login(request: LoginRequest, req: Request):
    client_ip = _get_client_ip(req)
    _check_rate_limit(client_ip)
    logger.info(f"Login attempt: user={request.username} ip={client_ip}")

    if check_gmca_pw(request.username, request.password):
        is_admin = is_staff_member(request.username)

        # IP access control — staff and facility network always allowed
        on_facility = any(_ip_matches_pattern(client_ip, p) for p in _FACILITY_PATTERNS)

        beamline = None
        groups = []
        full_name = None
        try:
            from qp2.xio.user_group_manager import UserGroupManager
            ugm = UserGroupManager()
            group_info = ugm.latest_group_info_from_username(request.username)
            if group_info:
                beamline = group_info.get("beamline")
            all_groups = ugm.groupnames_from_username(request.username)
            groups = [g["group_name"] for g in all_groups] if all_groups else []
            user_info = ugm.get_user_info(request.username)
            full_name = user_info.get("full_name") if user_info else None

            # Check remote IP access if not staff and not on facility network
            if not is_admin and not on_facility:
                if os.environ.get("QP2_REQUIRE_REMOTE_IP_ALLOWLIST", "0") == "1":
                    allowed_patterns = ugm.get_remote_ip_patterns(request.username)
                    if not any(_ip_matches_pattern(client_ip, p) for p in allowed_patterns):
                        raise HTTPException(
                            status_code=403,
                            detail="Remote access not authorized for your IP address. Contact facility staff."
                        )
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Group lookup failed for {request.username}: {e}")

        # ESAF check: required for remote users; facility-LAN users are exempt
        if not is_admin and not on_facility:
            has_esaf = any(g.startswith("esaf") for g in groups)
            if not has_esaf:
                raise HTTPException(
                    status_code=403,
                    detail="No previous ESAF found for your account. Please submit an ESAF."
                )

        if not beamline:
            beamline = get_beamline_from_hostname()

        logger.info(f"Login success: user={request.username} ip={client_ip} admin={is_admin}")
        token, _jti = create_access_token(request.username)

        response = JSONResponse({
            "success": True,
            "user": request.username,
            "is_admin": is_admin,
            "beamline": beamline,
            "groups": groups,
            "full_name": full_name,
        })
        set_auth_cookie(response, token)
        return response
    else:
        logger.warning(f"Login failed: user={request.username} ip={client_ip}")
        raise HTTPException(status_code=401, detail="Invalid credentials")

@app.post("/logout")
async def logout(req: Request):
    claims = decode_token_claims(req)
    response = JSONResponse({"success": True})
    if claims and claims.get("jti"):
        revoke_token(claims["jti"], claims.get("exp", 0))
    clear_auth_cookie(response)
    return response

# --- Random GM/CA structure for login background ---
_structure_cache: dict = {"ids": [], "fetched_at": 0.0}

@app.get("/api/random_structure")
async def random_structure():
    """Return a random recent GM/CA PDB entry for the login page background."""
    import random
    now = _time.time()
    # Refresh cache every 24 hours
    if not _structure_cache["ids"] or now - _structure_cache["fetched_at"] > 86400:
        try:
            end = datetime.now()
            start = datetime(end.year - 2, end.month, end.day)
            query = {
                "query": {
                    "type": "group", "logical_operator": "and",
                    "nodes": [{"type": "group", "logical_operator": "and", "nodes": [
                        {"type": "terminal", "service": "text", "parameters": {
                            "attribute": "rcsb_accession_info.initial_release_date",
                            "operator": "range", "negation": False,
                            "value": {"from": start.strftime("%Y-%m-%dT00:00:00Z"),
                                      "to": end.strftime("%Y-%m-%dT23:59:59Z"),
                                      "include_lower": True, "include_upper": True}}},
                        {"type": "terminal", "service": "text", "parameters": {
                            "attribute": "diffrn_source.pdbx_synchrotron_site",
                            "operator": "exact_match", "negation": False, "value": "APS"}},
                        {"type": "group", "logical_operator": "or", "nodes": [
                            {"type": "terminal", "service": "text", "parameters": {
                                "attribute": "diffrn_source.pdbx_synchrotron_beamline",
                                "operator": "exact_match", "negation": False, "value": "23-ID-B"}},
                            {"type": "terminal", "service": "text", "parameters": {
                                "attribute": "diffrn_source.pdbx_synchrotron_beamline",
                                "operator": "exact_match", "negation": False, "value": "23-ID-D"}}]}
                    ]}]},
                "request_options": {
                    "paginate": {"start": 0, "rows": 50},
                    "sort": [{"sort_by": "rcsb_accession_info.initial_release_date", "direction": "desc"}]},
                "return_type": "entry"
            }
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(
                None, lambda: requests.post(
                    "https://search.rcsb.org/rcsbsearch/v2/query",
                    json=query, headers={"Content-Type": "application/json"}, timeout=10))
            if resp.status_code == 200:
                ids = [e["identifier"] for e in resp.json().get("result_set", [])]
                if ids:
                    _structure_cache["ids"] = ids
                    _structure_cache["fetched_at"] = now
        except Exception as e:
            logger.warning(f"RCSB query failed: {e}")

    ids = _structure_cache["ids"]
    if not ids:
        return {"pdb_id": None}
    weights = [1.0 / (i + 1) for i in range(len(ids))]
    pdb_id = random.choices(ids, weights=weights, k=1)[0]
    return {
        "pdb_id": pdb_id,
        "image_url": f"https://cdn.rcsb.org/images/structures/{pdb_id.lower()}_assembly-1.jpeg",
        "rcsb_url": f"https://www.rcsb.org/structure/{pdb_id}"
    }

@app.get("/user/info")
async def get_user_info(user: str = Depends(verify_token)):
    """Get current user's contact info and group details."""
    info = {"username": user, "is_admin": is_staff_member(user)}
    try:
        from qp2.xio.user_group_manager import UserGroupManager
        ugm = UserGroupManager()
        user_data = ugm.get_user_info(user)
        if user_data:
            info["full_name"] = user_data.get("full_name")
            info["email"] = user_data.get("email")
        group_info = ugm.latest_group_info_from_username(user)
        if group_info:
            info["group_name"] = group_info.get("group_name")
            info["beamline"] = group_info.get("beamline")
            info["esaf_title"] = group_info.get("esaf_title")
            info["pi_full_name"] = group_info.get("pi_full_name")
        all_groups = ugm.groupnames_from_username(user)
        if all_groups:
            info["groups"] = [g["group_name"] for g in all_groups]
    except Exception:
        pass
    return info

@app.post("/send_to_http")
async def send_to_http(request: SendRequest, user: str = Depends(verify_token)):
    """
    Sends the spreadsheet to an HTTP RPC service. Staff only.
    """
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Only staff can send spreadsheets to Bluice")
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

    # Save to shared filesystem so Bluice can read it (same as desktop app)
    shared_dir = os.path.join(os.path.expanduser("~"), ".qp2", "tmp")
    os.makedirs(shared_dir, exist_ok=True)

    puck_map = "".join(request.puck_names)
    tmp_path = None

    # Try .xlsx first, fall back to .xls (matches desktop app logic)
    for suffix in (".xlsx", ".xls"):
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=shared_dir) as tmp:
            tmp_path = tmp.name

        try:
            manager.save_file(tmp_path, slots_for_logic)

            payload = {
                "module": "spreadsheet_import",
                "path": tmp_path,
                "map": puck_map,
            }

            _loop = asyncio.get_running_loop()
            resp = await _loop.run_in_executor(
                None, lambda: requests.post(url, data=payload, timeout=10))

            if resp.status_code == 200:
                return {"success": True, "message": "Spreadsheet sent successfully."}
            else:
                # If .xlsx fails with openpyxl error, try .xls
                if suffix == ".xlsx" and "openpyxl" in resp.text.lower():
                    continue
                return {"success": False, "message": f"RPC Error {resp.status_code}: {resp.text}"}

        except Exception as e:
            if suffix == ".xlsx":
                continue
            return {"success": False, "message": str(e)}

    return {"success": False, "message": "Failed to send spreadsheet in both .xlsx and .xls formats"}

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
    _SPREADSHEET_ALLOWED = {".csv", ".xls", ".xlsx"}
    _SPREADSHEET_MAX = 5 * 1024 * 1024  # 5 MB
    # Magic bytes for binary spreadsheet formats
    _SPREADSHEET_MAGIC = {
        ".xlsx": b"PK\x03\x04",
        ".xls": b"\xd0\xcf\x11\xe0",  # Compound Document (OLE2)
    }
    _EXECUTABLE_MAGIC = [b"MZ", b"\x7fELF", b"\xca\xfe\xba\xbe", b"\xfe\xed\xfa\xce"]

    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in _SPREADSHEET_ALLOWED:
        raise HTTPException(status_code=400, detail=f"Only {', '.join(sorted(_SPREADSHEET_ALLOWED))} files are accepted")

    content = await file.read()
    if len(content) > _SPREADSHEET_MAX:
        raise HTTPException(status_code=400, detail=f"File too large. Max {_SPREADSHEET_MAX // (1024*1024)}MB")

    header = content[:8]
    for magic in _EXECUTABLE_MAGIC:
        if header.startswith(magic):
            raise HTTPException(status_code=400, detail="File rejected: executable content detected")
    if ext in _SPREADSHEET_MAGIC and not header.startswith(_SPREADSHEET_MAGIC[ext]):
        raise HTTPException(status_code=400, detail=f"File content does not match {ext} format")

    # Parse puck names if provided
    names_list = None
    if puck_names:
        names_list = [n.strip() for n in puck_names.split(',') if n.strip()]

    manager = SpreadsheetManager(puck_names=names_list)

    # Create temp file to save upload
    import io
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp.write(content)
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

    # Normalize ESAF ID: strip 'esaf' prefix if present, store plain digits
    esaf_id = request.esaf_id.strip()
    if esaf_id.lower().startswith("esaf"):
        esaf_id = esaf_id[4:]
    if not esaf_id.isdigit():
        raise HTTPException(status_code=400, detail="ESAF ID must be digits (e.g., 12345)")
    request.esaf_id = esaf_id
    
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
    
        # Convert ORM objects to Pydantic models while session is still active
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

# --- SPA catch-all (must be LAST so it doesn't shadow API routes) ---
if os.path.exists(static_dir):
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = os.path.realpath(os.path.join(static_dir, full_path))
        if not file_path.startswith(os.path.realpath(static_dir)):
            return FileResponse(os.path.join(static_dir, "index.html"))
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(static_dir, "index.html"))