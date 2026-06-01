from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from typing import List, Optional
from pydantic import BaseModel
from datetime import date, datetime, timedelta
import io
from sqlalchemy.orm import Session
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError # Import IntegrityError

# Import models (Assuming project structure allows this import)
# We might need to adjust python path in main.py to make this work smoothly
# Ideally, we import db_manager instance or session dependency
# For now, I will assume a get_db dependency can be provided or I'll use db_manager directly.

try:
    from qp2.db import (
        Run, DayType, Staff, ScheduleDay, ShiftAllocation, Beamline, StaffQuota, StaffAvailability
    )
    from qp2.xio.db_manager import DBManager
    from qp2.web_app.backend.security import verify_token
    from qp2.web_app.backend.auth import is_staff_member
except ImportError:
    # Fallback for dev/IDE context
    pass

# All scheduler endpoints require authentication
router = APIRouter(prefix="/scheduler", tags=["scheduler"], dependencies=[Depends(verify_token)])

# --- Pydantic Models ---

class RunBase(BaseModel):
    name: str
    start_date: date
    end_date: date

class RunCreate(RunBase):
    pass

class RunUpdate(RunBase):
    id: int

class RunResponse(RunBase):
    id: int
    class Config:
        from_attributes = True

class StaffBase(BaseModel):
    username: str
    full_name: str
    email: str
    is_active: bool = True
    is_host: bool = True
    is_computing: bool = False

class StaffCreate(StaffBase):
    pass

class StaffUpdate(StaffBase):
    id: int

class StaffResponse(StaffBase):
    id: int
    class Config:
        from_attributes = True

class DayTypeBase(BaseModel):
    name: str
    color_code: str
    requires_staff: bool = True

class DayTypeCreate(DayTypeBase):
    pass

class DayTypeResponse(DayTypeBase):
    id: int
    class Config:
        from_attributes = True

class DayTypeUpdate(DayTypeBase): # Added DayTypeUpdate
    id: int

class BeamlineBase(BaseModel):
    name: str
    alias: str

class BeamlineResponse(BeamlineBase):
    id: int
    class Config:
        from_attributes = True

class ShiftAllocationResponse(BaseModel):
    shift_index: int
    esaf_id: Optional[str]
    pi_name: Optional[str]
    project_id: Optional[str]
    description: Optional[str]
    class Config:
        from_attributes = True

class ScheduleDayResponse(BaseModel):
    id: int
    date: date
    beamline_id: int
    run_id: int
    day_type_id: int
    assigned_staff_id: Optional[int]
    assigned_computing_staff_id: Optional[int] = None
    
    # Enriched fields
    beamline_name: str
    day_type_name: str
    day_type_color: str
    staff_name: Optional[str]
    computing_staff_name: Optional[str] = None
    
    shifts: List[ShiftAllocationResponse] = []

    class Config:
        from_attributes = True

class StaffQuotaBase(BaseModel):
    staff_id: int
    run_id: int
    max_days: int = 0
    max_weekends: int = 0

class StaffQuotaCreate(StaffQuotaBase):
    pass

class StaffQuotaResponse(StaffQuotaBase):
    id: int
    class Config:
        from_attributes = True

class StaffAvailabilityBase(BaseModel):
    staff_id: int
    date: date
    preference: str # UNAVAILABLE, PREFERRED, NEUTRAL

class StaffAvailabilityCreate(StaffAvailabilityBase):
    pass

class StaffAvailabilityResponse(StaffAvailabilityBase):
    id: int
    class Config:
        from_attributes = True

class ShiftAllocationUpdate(BaseModel):
    shift_index: int
    esaf_id: Optional[str] = None
    pi_name: Optional[str] = None
    project_id: Optional[str] = None
    description: Optional[str] = None

class ScheduleDayUpdate(BaseModel):
    day_id: int
    day_type_id: int
    assigned_staff_id: Optional[int] = None
    assigned_computing_staff_id: Optional[int] = None
    shifts: Optional[List[ShiftAllocationUpdate]] = None

# --- Dependencies ---

def get_db_session():
    # This function is a placeholder and should always be overridden by main.py
    # If this is called, it means the dependency override failed.
    raise RuntimeError("get_db_session dependency not properly overridden in main.py")

_session_factory = None

def set_session_factory(factory):
    global _session_factory
    _session_factory = factory

def _job_send_staff_reminders():
    """APScheduler job: send staff reminders 5 and 1 days in advance."""
    if not _session_factory:
        return
        
    import logging
    from datetime import datetime, timedelta
    from qp2.web_app.backend.email_utils import send_mail
    
    logger = logging.getLogger("qp2.scheduler_reminders")
    
    with _session_factory() as session:
        try:
            today = datetime.now().date()
            target_dates = [today + timedelta(days=5), today + timedelta(days=1)]
            
            days_to_remind = session.query(ScheduleDay).filter(
                ScheduleDay.date.in_(target_dates),
                (ScheduleDay.assigned_staff_id.isnot(None)) | (ScheduleDay.assigned_computing_staff_id.isnot(None))
            ).all()
            
            if not days_to_remind:
                return
                
            # Pre-fetch lookup data
            beamlines = {b.id: b for b in session.query(Beamline).all()}
            day_types = {dt.id: dt for dt in session.query(DayType).all()}
            staff_map = {s.id: s for s in session.query(Staff).all()}
            
            day_ids = [d.id for d in days_to_remind]
            shifts = session.query(ShiftAllocation).filter(ShiftAllocation.schedule_day_id.in_(day_ids)).all()
            shifts_by_day = {}
            for s in shifts:
                shifts_by_day.setdefault(s.schedule_day_id, []).append(s)
            
            for day in days_to_remind:
                dt = day_types.get(day.day_type_id)
                if not dt or not dt.requires_staff:
                    continue
                    
                staff = staff_map.get(day.assigned_staff_id)
                comp_staff = staff_map.get(day.assigned_computing_staff_id)
                
                bl = beamlines.get(day.beamline_id)
                bl_name = bl.name if bl else "Unknown Beamline"
                days_away = (day.date - today).days
                
                # Fetch users/shifts
                day_shifts = shifts_by_day.get(day.id, [])
                user_details = ""
                if day_shifts:
                    user_info_list = []
                    for s in day_shifts:
                        info = []
                        if s.pi_name:
                            info.append(s.pi_name)
                        if s.esaf_id:
                            info.append(f"ESAF: {s.esaf_id}")
                        if info:
                            user_info_list.append(" - " + " ".join(info))
                    if user_info_list:
                        user_details = "\nUsers/Projects:\n" + "\n".join(user_info_list) + "\n"
                
                emails_to_send = set()
                staff_roles = {} # email -> list of roles
                
                if staff and staff.email:
                    emails_to_send.add(staff.email)
                    staff_roles[staff.email] = {"name": staff.full_name, "is_host": True, "is_comp": False}
                    
                if comp_staff and comp_staff.email:
                    emails_to_send.add(comp_staff.email)
                    if comp_staff.email in staff_roles:
                        staff_roles[comp_staff.email]["is_comp"] = True
                    else:
                        staff_roles[comp_staff.email] = {"name": comp_staff.full_name, "is_host": False, "is_comp": True}
                        
                if not emails_to_send:
                    continue
                
                for email in emails_to_send:
                    role_info = staff_roles[email]
                    role_str = ""
                    if role_info["is_host"] and role_info["is_comp"]:
                        role_str = "host AND provide computing support for"
                    elif role_info["is_host"]:
                        role_str = "host"
                    else:
                        role_str = "provide computing support for"

                    subject = f"Reminder: Beamline Shift ({role_str}) in {days_away} day{'s' if days_away > 1 else ''}"
                    body = (f"Hello {role_info['name']},\n\n"
                            f"This is a reminder that you are scheduled to {role_str} a beamline shift:\n\n"
                            f"Date: {day.date.strftime('%Y-%m-%d')}\n"
                            f"Beamline: {bl_name}\n"
                            f"Type: {dt.name}\n"
                            f"{user_details}\n"
                            f"Please ensure you are prepared for your shift.\n")
                    
                    send_mail(subject=subject, body=body, to=[email])
                    logger.info(f"Sent {days_away}-day reminder ({role_str}) for {day.date} to {email}")
                
        except Exception as e:
            logger.error(f"Error in _job_send_staff_reminders: {e}")

# --- Endpoints ---

# 1. Runs
@router.get("/runs", response_model=List[RunResponse])
async def list_runs(session: Session = Depends(get_db_session)):
    runs = session.query(Run).all()
    return runs

@router.post("/runs", response_model=RunResponse)
async def create_run(run: RunCreate, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    db_run = Run(**run.dict())
    session.add(db_run)
    session.commit()
    session.refresh(db_run)

    # Auto-generate ScheduleDay rows for each date × beamline
    beamlines = session.query(Beamline).all()
    default_day_type = session.query(DayType).filter_by(name="User beam time").first() or session.query(DayType).first()
    monday_day_type = session.query(DayType).filter_by(name="APS Studies").first()
    if beamlines and default_day_type:
        delta = (db_run.end_date - db_run.start_date).days
        for i in range(delta + 1):
            day_date = db_run.start_date + timedelta(days=i)
            # Monday = 0 in weekday()
            day_type = monday_day_type if (monday_day_type and day_date.weekday() == 0) else default_day_type
            for bl in beamlines:
                session.add(ScheduleDay(
                    date=day_date,
                    beamline_id=bl.id,
                    run_id=db_run.id,
                    day_type_id=day_type.id,
                    assigned_staff_id=None,
                ))
        session.commit()

    return db_run

@router.put("/runs", response_model=RunResponse)
async def update_run(run: RunUpdate, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    db_run = session.get(Run,run.id)
    if not db_run:
        raise HTTPException(status_code=404, detail="Run not found")
    
    for key, value in run.dict().items():
        setattr(db_run, key, value)
    
    session.commit()
    session.refresh(db_run)
    return db_run

@router.delete("/runs/{run_id}")
async def delete_run(run_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    db_run = session.get(Run,run_id)
    if not db_run:
        raise HTTPException(status_code=404, detail="Run not found")
    
    # Cascade delete schedule days, quotas, and shift allocations for this run
    schedule_days = session.query(ScheduleDay).filter(ScheduleDay.run_id == run_id).all()
    for sd in schedule_days:
        session.query(ShiftAllocation).filter(ShiftAllocation.schedule_day_id == sd.id).delete()
    session.query(ScheduleDay).filter(ScheduleDay.run_id == run_id).delete()
    session.query(StaffQuota).filter(StaffQuota.run_id == run_id).delete()

    session.delete(db_run)
    try:
        session.commit()
    except IntegrityError as e:
        session.rollback()
        raise HTTPException(status_code=400, detail="Database error during operation")
    return {"message": "Run deleted"}

# 2. Staff
@router.get("/staff", response_model=List[StaffResponse])
async def list_staff(session: Session = Depends(get_db_session)):
    staff = session.query(Staff).all()
    return staff

@router.post("/staff", response_model=StaffResponse)
async def create_staff(staff: StaffCreate, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    db_staff = Staff(**staff.dict())
    session.add(db_staff)
    session.commit()
    session.refresh(db_staff)
    return db_staff

@router.put("/staff", response_model=StaffResponse)
async def update_staff(staff: StaffUpdate, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    db_staff = session.get(Staff,staff.id)
    if not db_staff:
        raise HTTPException(status_code=404, detail="Staff not found")
    
    for key, value in staff.dict().items():
        setattr(db_staff, key, value)
    
    session.commit()
    session.refresh(db_staff)
    return db_staff

@router.delete("/staff/{staff_id}")
async def delete_staff(staff_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    db_staff = session.get(Staff,staff_id)
    if not db_staff:
        raise HTTPException(status_code=404, detail="Staff not found")
    
    # Check for dependencies
    usage_count = session.query(ScheduleDay).filter(ScheduleDay.assigned_staff_id == staff_id).count()
    if usage_count > 0:
        raise HTTPException(status_code=400, detail=f"Cannot delete Staff: Assigned to {usage_count} schedule days.")
    
    # Also check StaffQuota
    quota_count = session.query(StaffQuota).filter(StaffQuota.staff_id == staff_id).count()
    if quota_count > 0:
        raise HTTPException(status_code=400, detail=f"Cannot delete Staff: Has {quota_count} quota entries. Please delete associated quotas first.")

    # Also check StaffAvailability
    avail_count = session.query(StaffAvailability).filter(StaffAvailability.staff_id == staff_id).count()
    if avail_count > 0:
        raise HTTPException(status_code=400, detail=f"Cannot delete Staff: Has {avail_count} availability entries. Please delete associated availability first.")

    session.delete(db_staff)
    try:
        session.commit()
    except IntegrityError as e:
        session.rollback()
        raise HTTPException(status_code=400, detail="Database error during operation")
    return {"message": "Staff deleted"}

# 3. Day Types
@router.get("/day_types", response_model=List[DayTypeResponse])
async def list_day_types(session: Session = Depends(get_db_session)):
    types = session.query(DayType).all()
    return types

@router.post("/day_types", response_model=DayTypeResponse)
async def create_day_type(dtype: DayTypeCreate, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    db_type = DayType(**dtype.dict())
    session.add(db_type)
    session.commit()
    session.refresh(db_type)
    return db_type

# class DayTypeUpdate(DayTypeBase): # Moved to top
#     id: int

@router.put("/day_types", response_model=DayTypeResponse)
async def update_day_type(dtype: DayTypeUpdate, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    db_type = session.get(DayType,dtype.id)
    if not db_type:
        raise HTTPException(status_code=404, detail="Day Type not found")
    
    for key, value in dtype.dict().items():
        setattr(db_type, key, value)
    
    session.commit()
    session.refresh(db_type)
    return db_type

@router.delete("/day_types/{type_id}")
async def delete_day_type(type_id: int, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    db_type = session.get(DayType,type_id)
    if not db_type:
        raise HTTPException(status_code=404, detail="Day Type not found")
    
    # Check for dependencies
    usage_count = session.query(ScheduleDay).filter(ScheduleDay.day_type_id == type_id).count()
    if usage_count > 0:
        raise HTTPException(status_code=400, detail=f"Cannot delete Day Type: Used in {usage_count} schedule days.")
    
    session.delete(db_type)
    try:
        session.commit()
    except IntegrityError as e:
        session.rollback()
        raise HTTPException(status_code=400, detail="Database error during operation")
    return {"message": "Day Type deleted"}

# 3b. Beamlines
@router.post("/init_defaults")
async def init_defaults(user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    """Initialize default beamlines and day types if they don't exist."""
    created = []

    # Beamlines
    for name, alias in [("23IDD", "bl1"), ("23IDB", "bl2")]:
        if not session.query(Beamline).filter_by(alias=alias).first():
            session.add(Beamline(name=name, alias=alias))
            created.append(f"Beamline {name} ({alias})")

    # Day types
    defaults = [
        ("User beam time", "#800080", True),
        ("APS Studies", "#FF0000", True),
        ("Staff research", "#008000", True),
        ("Start-up", "#90EE90", True),
        ("Not assigned", "#FFFFFF", False),
        ("Weekends", "#808080", False),
    ]
    for name, color, requires_staff in defaults:
        if not session.query(DayType).filter_by(name=name).first():
            session.add(DayType(name=name, color_code=color, requires_staff=requires_staff))
            created.append(f"DayType {name}")

    if created:
        session.commit()

    # Generate schedule days for any existing runs that have none
    beamlines = session.query(Beamline).all()
    default_day_type = session.query(DayType).filter_by(name="User beam time").first() or session.query(DayType).first()
    monday_day_type = session.query(DayType).filter_by(name="APS Studies").first()
    if beamlines and default_day_type:
        runs = session.query(Run).all()
        for run in runs:
            existing = session.query(ScheduleDay).filter(ScheduleDay.run_id == run.id).count()
            if existing == 0:
                delta = (run.end_date - run.start_date).days
                for i in range(delta + 1):
                    day_date = run.start_date + timedelta(days=i)
                    day_type = monday_day_type if (monday_day_type and day_date.weekday() == 0) else default_day_type
                    for bl in beamlines:
                        session.add(ScheduleDay(
                            date=day_date,
                            beamline_id=bl.id,
                            run_id=run.id,
                            day_type_id=day_type.id,
                            assigned_staff_id=None,
                        ))
                created.append(f"Schedule days for run {run.name}")
        if runs:
            session.commit()

    return {"status": "ok", "created": created, "message": f"Initialized {len(created)} items" if created else "Defaults already exist"}

@router.get("/beamlines", response_model=List[BeamlineResponse])
async def list_beamlines(session: Session = Depends(get_db_session)):
    beamlines = session.query(Beamline).all()
    return beamlines

# 4. Schedule
@router.get("/schedule/{run_id}", response_model=List[ScheduleDayResponse])
async def get_schedule(run_id: int, session: Session = Depends(get_db_session)):
    # Query ScheduleDays with explicit joins to populate enriched fields
    # Note: For simple cases, we can fetch all and map in python or use SQLAlchemy relationships + joinedload
    
    # Assuming relationships are not explicitly defined in models (I appended them without backrefs),
    # we'll fetch manual or use relationships if I added them.
    # I didn't add relationships in the Phase 1 step, just FKs.
    
    # Let's fetch all necessary data and map in Python for simplicity/robustness against DetachedInstanceError
    days = session.query(ScheduleDay).filter(ScheduleDay.run_id == run_id).order_by(ScheduleDay.date).all()
    
    # Fetch lookups
    beamlines = {b.id: b for b in session.query(Beamline).all()}
    day_types = {d.id: d for d in session.query(DayType).all()}
    staff_map = {s.id: s for s in session.query(Staff).all()}
    
    # Fetch all shifts for these days
    day_ids = [d.id for d in days]
    shifts = []
    if day_ids:
        shifts = session.query(ShiftAllocation).filter(ShiftAllocation.schedule_day_id.in_(day_ids)).all()
    
    shifts_by_day = {}
    for s in shifts:
        if s.schedule_day_id not in shifts_by_day:
            shifts_by_day[s.schedule_day_id] = []
        shifts_by_day[s.schedule_day_id].append(s)

    # Construct response
    response_list = []
    for d in days:
        bl = beamlines.get(d.beamline_id)
        dt = day_types.get(d.day_type_id)
        st = staff_map.get(d.assigned_staff_id)
        comp_st = staff_map.get(d.assigned_computing_staff_id)
        
        response_list.append(ScheduleDayResponse(
            id=d.id,
            date=d.date,
            beamline_id=d.beamline_id,
            run_id=d.run_id,
            day_type_id=d.day_type_id,
            assigned_staff_id=d.assigned_staff_id,
            assigned_computing_staff_id=d.assigned_computing_staff_id,
            beamline_name=bl.name if bl else "Unknown",
            day_type_name=dt.name if dt else "Unknown",
            day_type_color=dt.color_code if dt else "#FFFFFF",
            staff_name=st.full_name if st else None,
            computing_staff_name=comp_st.full_name if comp_st else None,
            shifts=shifts_by_day.get(d.id, [])
        ))
        
    return response_list
@router.post("/day", response_model=ScheduleDayResponse)
async def update_schedule_day(update: ScheduleDayUpdate, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    day = session.get(ScheduleDay,update.day_id)
    if not day:
        raise HTTPException(status_code=404, detail="Schedule day not found")
        
    day.day_type_id = update.day_type_id
    day.assigned_staff_id = update.assigned_staff_id
    
    # Sync computing staff across all beamlines for this date
    session.query(ScheduleDay).filter(
        ScheduleDay.date == day.date,
        ScheduleDay.run_id == day.run_id
    ).update({ScheduleDay.assigned_computing_staff_id: update.assigned_computing_staff_id})

    # Process Shift Allocations
    if update.shifts is not None:
        for shift_data in update.shifts:
            has_content = any([shift_data.esaf_id, shift_data.pi_name, shift_data.project_id, shift_data.description])
            
            existing_shift = session.query(ShiftAllocation).filter(
                ShiftAllocation.schedule_day_id == day.id,
                ShiftAllocation.shift_index == shift_data.shift_index
            ).first()
            
            if has_content:
                if existing_shift:
                    existing_shift.esaf_id = shift_data.esaf_id
                    existing_shift.pi_name = shift_data.pi_name
                    existing_shift.project_id = shift_data.project_id
                    existing_shift.description = shift_data.description
                else:
                    new_shift = ShiftAllocation(
                        schedule_day_id=day.id,
                        shift_index=shift_data.shift_index,
                        esaf_id=shift_data.esaf_id,
                        pi_name=shift_data.pi_name,
                        project_id=shift_data.project_id,
                        description=shift_data.description
                    )
                    session.add(new_shift)
            else:
                if existing_shift:
                    session.delete(existing_shift)
    
    session.commit()
    session.refresh(day)
    
    # We need to return enriched response, so fetch lookup data
    bl = session.get(Beamline,day.beamline_id)
    dt = session.get(DayType,day.day_type_id)
    st = session.get(Staff,day.assigned_staff_id) if day.assigned_staff_id else None
    comp_st = session.get(Staff,day.assigned_computing_staff_id) if day.assigned_computing_staff_id else None
    shifts = session.query(ShiftAllocation).filter(ShiftAllocation.schedule_day_id == day.id).all()
    
    return ScheduleDayResponse(
        id=day.id,
        date=day.date,
        beamline_id=day.beamline_id,
        run_id=day.run_id,
        day_type_id=day.day_type_id,
        assigned_staff_id=day.assigned_staff_id,
        assigned_computing_staff_id=day.assigned_computing_staff_id,
        beamline_name=bl.name if bl else "Unknown",
        day_type_name=dt.name if dt else "Unknown",
        day_type_color=dt.color_code if dt else "#FFFFFF",
        staff_name=st.full_name if st else None,
        computing_staff_name=comp_st.full_name if comp_st else None,
        shifts=shifts
    )

# 6. Quotas
@router.get("/quotas/{run_id}", response_model=List[StaffQuotaResponse])
async def list_quotas(run_id: int, session: Session = Depends(get_db_session)):
    quotas = session.query(StaffQuota).filter(StaffQuota.run_id == run_id).all()
    return quotas

@router.post("/quotas", response_model=StaffQuotaResponse)
async def update_quota(quota: StaffQuotaCreate, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    # Check if exists
    db_quota = session.query(StaffQuota).filter(
        and_(StaffQuota.staff_id == quota.staff_id, StaffQuota.run_id == quota.run_id)
    ).first()
    
    if db_quota:
        db_quota.max_days = quota.max_days
        db_quota.max_weekends = quota.max_weekends
    else:
        db_quota = StaffQuota(**quota.dict())
        session.add(db_quota)
    
    session.commit()
    session.refresh(db_quota)
    return db_quota

# 7. Availability
@router.get("/availability/{staff_id}", response_model=List[StaffAvailabilityResponse])
async def list_availability(staff_id: int, session: Session = Depends(get_db_session)):
    # Optionally filter by date range if provided in query params
    avail = session.query(StaffAvailability).filter(StaffAvailability.staff_id == staff_id).all()
    return avail

@router.post("/availability", response_model=StaffAvailabilityResponse)
async def update_availability(avail: StaffAvailabilityCreate, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    db_avail = session.query(StaffAvailability).filter(
        and_(StaffAvailability.staff_id == avail.staff_id, StaffAvailability.date == avail.date)
    ).first()
    
    if db_avail:
        db_avail.preference = avail.preference
    else:
        db_avail = StaffAvailability(**avail.dict())
        session.add(db_avail)
        
    session.commit()
    session.refresh(db_avail)
    return db_avail

@router.post("/auto_assign/{run_id}")
async def auto_assign(run_id: int, overwrite: bool = False, user: str = Depends(verify_token), session: Session = Depends(get_db_session)):
    if not is_staff_member(user):
        raise HTTPException(status_code=403, detail="Staff only")
    # 1. Fetch Configuration Data
    run = session.get(Run,run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
        
    all_staff = session.query(Staff).filter(Staff.is_active == True).all()
    quotas = session.query(StaffQuota).filter(StaffQuota.run_id == run_id).all()
    quota_map = {q.staff_id: q for q in quotas}
    
    # Availability within run range
    availabilities = session.query(StaffAvailability).filter(
        and_(StaffAvailability.date >= run.start_date, StaffAvailability.date <= run.end_date)
    ).all()
    avail_map = {} # (staff_id, date) -> preference
    for a in availabilities:
        avail_map[(a.staff_id, a.date)] = a.preference

    # 2. Fetch Schedule Data
    schedule_days = session.query(ScheduleDay).filter(
        ScheduleDay.run_id == run_id
    ).order_by(ScheduleDay.date).all()
    
    # Get Day Types to know which require staff
    day_types = {dt.id: dt for dt in session.query(DayType).all()}

    # 3. Initialize State
    staff_usage = {s.id: {'days': 0, 'weekends': 0} for s in all_staff}
    daily_assignments = {} # date -> set(staff_ids) to prevent double booking on same day (different beamlines)

    # Pre-process existing assignments to populate usage and daily_assignments
    days_to_assign = []
    
    for day in schedule_days:
        dt = day_types.get(day.day_type_id)
        if not dt or not dt.requires_staff:
            continue
            
        date_key = day.date
        if date_key not in daily_assignments:
            daily_assignments[date_key] = set()

        if day.assigned_staff_id:
            if not overwrite:
                # Track existing assignment
                sid = day.assigned_staff_id
                if sid in staff_usage:
                    staff_usage[sid]['days'] += 1
                    if day.date.weekday() in [5, 6]: # Sat, Sun
                        staff_usage[sid]['weekends'] += 1
                daily_assignments[date_key].add(sid)
            else:
                # Mark for reassignment
                day.assigned_staff_id = None
                days_to_assign.append(day)
        else:
            days_to_assign.append(day)

    # 4. Greedy Assignment Loop
    assigned_count = 0
    
    for day in days_to_assign:
        date_key = day.date
        is_weekend = day.date.weekday() in [5, 6]
        
        candidates = []
        for staff in all_staff:
            if not getattr(staff, 'is_host', True):
                continue
                
            sid = staff.id
            
            # Constraint: Already assigned today (on another beamline)
            if sid in daily_assignments.get(date_key, set()):
                continue
                
            # Constraint: Availability
            pref = avail_map.get((sid, date_key), 'NEUTRAL')
            if pref == 'UNAVAILABLE':
                continue
                
            # Constraint: Quotas
            q = quota_map.get(sid)
            usage = staff_usage[sid]
            
            # Default quotas to infinite if not set? Or strict 0? 
            # Let's assume strict if set, otherwise maybe liberal or 0.
            # If no quota record, maybe they shouldn't work? Let's assume 0.
            max_days = q.max_days if q else 0
            max_weekends = q.max_weekends if q else 0
            
            if usage['days'] >= max_days:
                continue
            if is_weekend and usage['weekends'] >= max_weekends:
                continue
                
            # Scoring
            score = 0
            if pref == 'PREFERRED':
                score += 100
            
            # Load balancing: prefer those with lower utilization ratio
            usage_ratio = usage['days'] / (max_days if max_days > 0 else 1)
            score -= (usage_ratio * 50) 
            
            candidates.append((score, staff))
        
        # Sort candidates by score descending
        candidates.sort(key=lambda x: x[0], reverse=True)
        
        if candidates:
            best_score, best_staff = candidates[0]
            sid = best_staff.id
            
            # Assign
            day.assigned_staff_id = sid
            staff_usage[sid]['days'] += 1
            if is_weekend:
                staff_usage[sid]['weekends'] += 1
            
            if date_key not in daily_assignments:
                daily_assignments[date_key] = set()
            daily_assignments[date_key].add(sid)
            
            assigned_count += 1
    
    # 5. Assign Computing Staff (synchronized across beamlines for each date)
    unique_dates = {day.date for day in schedule_days}
    comp_staff_list = [s for s in all_staff if getattr(s, 'is_computing', False)]
    
    if comp_staff_list:
        for date_key in unique_dates:
            day_records = [d for d in schedule_days if d.date == date_key]
            if not day_records: continue
            
            existing_comp = None
            for d in day_records:
                if getattr(d, 'assigned_computing_staff_id', None):
                    existing_comp = d.assigned_computing_staff_id
                    break
            
            if existing_comp and not overwrite:
                continue
                
            hosts_today = daily_assignments.get(date_key, set())
            comp_host_candidates = [s for s in comp_staff_list if s.id in hosts_today]
            
            chosen_comp_id = None
            if comp_host_candidates:
                chosen_comp_id = comp_host_candidates[0].id
            else:
                comp_staff_list.sort(key=lambda s: staff_usage[s.id]['days'])
                chosen_comp_id = comp_staff_list[0].id
                
            if chosen_comp_id:
                for d in day_records:
                    d.assigned_computing_staff_id = chosen_comp_id

    session.commit()
    return {"message": f"Auto-assigned {assigned_count} slots.", "usage": staff_usage}

# 8. Export
@router.get("/export/ics/{staff_id}")
async def export_ics(staff_id: int, session: Session = Depends(get_db_session)):
    staff = session.get(Staff,staff_id)
    if not staff:
        raise HTTPException(status_code=404, detail="Staff not found")
        
    assignments = session.query(ScheduleDay).filter(
        ScheduleDay.assigned_staff_id == staff_id
    ).all()
    
    # Get all related entities for ICS export
    all_beamlines = {b.id: b for b in session.query(Beamline).all()}
    all_day_types = {dt.id: dt for dt in session.query(DayType).all()}
    all_runs = {r.id: r for r in session.query(Run).all()}

    # Generate ICS content
    ics_content = "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:-//QP2//Beamtime Scheduler//EN\n"
    
    for day in assignments:
        bl = all_beamlines.get(day.beamline_id)
        dt = all_day_types.get(day.day_type_id)
        run = all_runs.get(day.run_id)

        bl_name = bl.name if bl else "Unknown Beamline"
        day_type_name = dt.name if dt else "Unknown Day Type"
        run_name = run.name if run else "Unknown Run"

        # Format date: YYYYMMDD
        dt_start_str = day.date.strftime("%Y%m%d")
        # End date is inclusive start of next day for all-day events in ICS
        next_day = day.date + timedelta(days=1)
        dt_end_str = next_day.strftime("%Y%m%d")
        
        ics_content += "BEGIN:VEVENT\n"
        ics_content += f"SUMMARY:Hosting - {bl_name} ({run_name})\n"
        ics_content += f"DTSTART;VALUE=DATE:{dt_start_str}\n"
        ics_content += f"DTEND;VALUE=DATE:{dt_end_str}\n"
        ics_content += f"DESCRIPTION:Beamline: {bl_name}, Type: {day_type_name}, Run: {run_name}\n"
        # Add a UID for better calendar management (avoid duplicates)
        ics_content += f"UID:{day.id}-{day.date.isoformat()}@beamtime.scheduler\n"
        ics_content += "END:VEVENT\n"
        
    ics_content += "END:VCALENDAR"
    
    return StreamingResponse(
        io.BytesIO(ics_content.encode("utf-8")),
        media_type="text/calendar",
        headers={"Content-Disposition": f"attachment; filename=schedule_{staff.username}.ics"}
    )