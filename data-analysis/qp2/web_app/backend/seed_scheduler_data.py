
import sys
import os
from pathlib import Path
from datetime import date, timedelta

# Add project root to path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.append(str(project_root))

try:
    from xio.db_manager import DBManager
    from data_viewer.models import Run, ScheduleDay, Beamline, DayType
    from sqlalchemy import and_
except ImportError as e:
    print(f"ImportError: {e}")
    sys.exit(1)

def seed():
    print("Initializing DB Manager...")
    db = DBManager(beamline="default")
    
    with db.get_session() as session:
        # 1. Create Run
        run_name = "2025-1"
        existing_run = session.query(Run).filter_by(name=run_name).first()
        
        if existing_run:
            print(f"Run {run_name} already exists.")
            run = existing_run
        else:
            run = Run(name=run_name, start_date=date(2025, 1, 1), end_date=date(2025, 1, 31))
            session.add(run)
            session.commit()
            print(f"Created Run {run_name}")

        # 2. Ensure Beamlines
        bl1 = session.query(Beamline).filter_by(alias="bl1").first()
        bl2 = session.query(Beamline).filter_by(alias="bl2").first()
        
        if not bl1 or not bl2:
            print("Beamlines missing! Run 'Init Defaults' in UI or implemented logic here.")
            # Let's create them if missing for convenience
            if not bl1:
                bl1 = Beamline(name="23IDD", alias="bl1")
                session.add(bl1)
            if not bl2:
                bl2 = Beamline(name="23IDB", alias="bl2")
                session.add(bl2)
            session.commit()

        # 3. Ensure DayTypes
        user_time = session.query(DayType).filter_by(name="User beam time").first()
        if not user_time:
             print("Creating default DayTypes...")
             types = [
                DayType(name="User beam time", color_code="#800080", requires_staff=True),
                DayType(name="APS Studies", color_code="#FF0000", requires_staff=True),
                DayType(name="Staff research", color_code="#008000", requires_staff=True),
                DayType(name="Start-up", color_code="#90EE90", requires_staff=True),
                DayType(name="Not assigned", color_code="#FFFFFF", requires_staff=False),
                DayType(name="Weekends", color_code="#808080", requires_staff=False),
             ]
             session.add_all(types)
             session.commit()
             user_time = session.query(DayType).filter_by(name="User beam time").first()

        # 4. Generate ScheduleDays for this run if empty
        existing_days = session.query(ScheduleDay).filter_by(run_id=run.id).count()
        if existing_days == 0:
            print(f"Generating schedule days for run {run.name}...")
            delta = run.end_date - run.start_date
            
            for i in range(delta.days + 1):
                day_date = run.start_date + timedelta(days=i)
                
                # Add for BL1
                sd1 = ScheduleDay(
                    date=day_date,
                    beamline_id=bl1.id,
                    run_id=run.id,
                    day_type_id=user_time.id, # Default to User beam time
                    assigned_staff_id=None
                )
                session.add(sd1)
                
                # Add for BL2
                sd2 = ScheduleDay(
                    date=day_date,
                    beamline_id=bl2.id,
                    run_id=run.id,
                    day_type_id=user_time.id,
                    assigned_staff_id=None
                )
                session.add(sd2)
            
            session.commit()
            print("Schedule days generated.")
        else:
            print("Schedule days already exist.")

if __name__ == "__main__":
    seed()
