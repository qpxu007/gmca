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
    from qp2.data_viewer.models import (
        Run, DayType, Staff, ScheduleDay, ShiftAllocation, Beamline, StaffQuota, StaffAvailability
    )
    from xio.db_manager import DBManager
except ImportError:
    # Fallback for dev/IDE context
    pass

router = APIRouter(prefix="/scheduler", tags=["scheduler"])

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
    
    # Enriched fields
    beamline_name: str
    day_type_name: str
    day_type_color: str
    staff_name: Optional[str]
    
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
    shifts: Optional[List[ShiftAllocationUpdate]] = None

# --- Dependencies ---

def get_db_session():
    # This function is a placeholder and should always be overridden by main.py
    # If this is called, it means the dependency override failed.
    raise RuntimeError("get_db_session dependency not properly overridden in main.py")

# --- Endpoints ---

# 1. Runs
@router.get("/runs", response_model=List[RunResponse])
async def list_runs(session: Session = Depends(get_db_session)):
    runs = session.query(Run).all()
    return runs

@router.post("/runs", response_model=RunResponse)
async def create_run(run: RunCreate, session: Session = Depends(get_db_session)):
    db_run = Run(**run.dict())
    session.add(db_run)
    session.commit()
    session.refresh(db_run)
    return db_run

@router.put("/runs", response_model=RunResponse)
async def update_run(run: RunUpdate, session: Session = Depends(get_db_session)):
    db_run = session.query(Run).get(run.id)
    if not db_run:
        raise HTTPException(status_code=404, detail="Run not found")
    
    for key, value in run.dict().items():
        setattr(db_run, key, value)
    
    session.commit()
    session.refresh(db_run)
    return db_run

@router.delete("/runs/{run_id}")
async def delete_run(run_id: int, session: Session = Depends(get_db_session)):
    db_run = session.query(Run).get(run_id)
    if not db_run:
        raise HTTPException(status_code=404, detail="Run not found")
    
    # Check for dependencies
    usage_count = session.query(ScheduleDay).filter(ScheduleDay.run_id == run_id).count()
    if usage_count > 0:
        raise HTTPException(status_code=400, detail=f"Cannot delete Run: It contains {usage_count} schedule days. Please delete the days or the schedule first.")

    session.delete(db_run)
    try:
        session.commit()
    except IntegrityError as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=f"Database error: {e.orig.pgerror if hasattr(e.orig, 'pgerror') else e}")
    return {"message": "Run deleted"}

# 2. Staff
@router.get("/staff", response_model=List[StaffResponse])
async def list_staff(session: Session = Depends(get_db_session)):
    staff = session.query(Staff).all()
    return staff

@router.post("/staff", response_model=StaffResponse)
async def create_staff(staff: StaffCreate, session: Session = Depends(get_db_session)):
    db_staff = Staff(**staff.dict())
    session.add(db_staff)
    session.commit()
    session.refresh(db_staff)
    return db_staff

@router.put("/staff", response_model=StaffResponse)
async def update_staff(staff: StaffUpdate, session: Session = Depends(get_db_session)):
    db_staff = session.query(Staff).get(staff.id)
    if not db_staff:
        raise HTTPException(status_code=404, detail="Staff not found")
    
    for key, value in staff.dict().items():
        setattr(db_staff, key, value)
    
    session.commit()
    session.refresh(db_staff)
    return db_staff

@router.delete("/staff/{staff_id}")
async def delete_staff(staff_id: int, session: Session = Depends(get_db_session)):
    db_staff = session.query(Staff).get(staff_id)
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
        raise HTTPException(status_code=400, detail=f"Database error: {e.orig.pgerror if hasattr(e.orig, 'pgerror') else e}")
    return {"message": "Staff deleted"}

# 3. Day Types
@router.get("/day_types", response_model=List[DayTypeResponse])
async def list_day_types(session: Session = Depends(get_db_session)):
    types = session.query(DayType).all()
    return types

@router.post("/day_types", response_model=DayTypeResponse)
async def create_day_type(dtype: DayTypeCreate, session: Session = Depends(get_db_session)):
    db_type = DayType(**dtype.dict())
    session.add(db_type)
    session.commit()
    session.refresh(db_type)
    return db_type

# class DayTypeUpdate(DayTypeBase): # Moved to top
#     id: int

@router.put("/day_types", response_model=DayTypeResponse)
async def update_day_type(dtype: DayTypeUpdate, session: Session = Depends(get_db_session)):
    db_type = session.query(DayType).get(dtype.id)
    if not db_type:
        raise HTTPException(status_code=404, detail="Day Type not found")
    
    for key, value in dtype.dict().items():
        setattr(db_type, key, value)
    
    session.commit()
    session.refresh(db_type)
    return db_type

@router.delete("/day_types/{type_id}")
async def delete_day_type(type_id: int, session: Session = Depends(get_db_session)):
    db_type = session.query(DayType).get(type_id)
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
        raise HTTPException(status_code=400, detail=f"Database error: {e.orig.pgerror if hasattr(e.orig, 'pgerror') else e}")
    return {"message": "Day Type deleted"}

# 3b. Beamlines
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
        
        response_list.append(ScheduleDayResponse(
            id=d.id,
            date=d.date,
            beamline_id=d.beamline_id,
            run_id=d.run_id,
            day_type_id=d.day_type_id,
            assigned_staff_id=d.assigned_staff_id,
            beamline_name=bl.name if bl else "Unknown",
            day_type_name=dt.name if dt else "Unknown",
            day_type_color=dt.color_code if dt else "#FFFFFF",
            staff_name=st.full_name if st else None,
            shifts=shifts_by_day.get(d.id, [])
        ))
        
    return response_list
@router.post("/day", response_model=ScheduleDayResponse)
async def update_schedule_day(update: ScheduleDayUpdate, session: Session = Depends(get_db_session)):
    day = session.query(ScheduleDay).get(update.day_id)
    if not day:
        raise HTTPException(status_code=404, detail="Schedule day not found")
        
    day.day_type_id = update.day_type_id
    day.assigned_staff_id = update.assigned_staff_id
    session.commit()
    session.refresh(day)
    
    # We need to return enriched response, so fetch lookup data
    # Ideally reuse logic from get_schedule or use a helper
    bl = session.query(Beamline).get(day.beamline_id)
    dt = session.query(DayType).get(day.day_type_id)
    st = session.query(Staff).get(day.assigned_staff_id) if day.assigned_staff_id else None
    shifts = session.query(ShiftAllocation).filter(ShiftAllocation.schedule_day_id == day.id).all()
    
    return ScheduleDayResponse(
        id=day.id,
        date=day.date,
        beamline_id=day.beamline_id,
        run_id=day.run_id,
        day_type_id=day.day_type_id,
        assigned_staff_id=day.assigned_staff_id,
        beamline_name=bl.name if bl else "Unknown",
        day_type_name=dt.name if dt else "Unknown",
        day_type_color=dt.color_code if dt else "#FFFFFF",
        staff_name=st.full_name if st else None,
        shifts=shifts
    )

# 6. Quotas
@router.get("/quotas/{run_id}", response_model=List[StaffQuotaResponse])
async def list_quotas(run_id: int, session: Session = Depends(get_db_session)):
    quotas = session.query(StaffQuota).filter(StaffQuota.run_id == run_id).all()
    return quotas

@router.post("/quotas", response_model=StaffQuotaResponse)
async def update_quota(quota: StaffQuotaCreate, session: Session = Depends(get_db_session)):
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
async def update_availability(avail: StaffAvailabilityCreate, session: Session = Depends(get_db_session)):
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
async def auto_assign(run_id: int, overwrite: bool = False, session: Session = Depends(get_db_session)):
    # 1. Fetch Configuration Data
    run = session.query(Run).get(run_id)
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
    
    session.commit()
    return {"message": f"Auto-assigned {assigned_count} slots.", "usage": staff_usage}

# 8. Export
@router.get("/export/ics/{staff_id}")
async def export_ics(staff_id: int, session: Session = Depends(get_db_session)):
    staff = session.query(Staff).get(staff_id)
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