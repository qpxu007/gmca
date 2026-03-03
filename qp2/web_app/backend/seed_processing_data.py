import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import random

# Add project root to path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.append(str(project_root))

try:
    from xio.db_manager import DBManager
    from qp2.data_viewer.models import PipelineStatus, DataProcessResults
except ImportError as e:
    print(f"ImportError: {e}", file=sys.stderr)
    sys.exit(1)

def seed_processing(num_entries=30):
    print("Initializing DB Manager...")
    db = DBManager(beamline="default")

    with db.get_session() as session:
        print(f"Seeding {num_entries} sample Processing entries...")
        users = ["user1", "staff", "admin", "guest"]
        pipelines = ["autoproc", "xia2", "fast_dp"]
        states = ["SUCCESS", "FAILED", "RUNNING", "PENDING"]
        spacegroups = ["P212121", "C2", "P1", "I4122"]
        
        for i in range(num_entries):
            username = random.choice(users)
            pipeline = random.choice(pipelines)
            state = random.choice(states)
            sample_name = f"sample_{random.randint(100, 999)}"
            
            # Create PipelineStatus
            status = PipelineStatus(
                command=f"run_pipeline.sh {sample_name}",
                state=state,
                pipeline=pipeline,
                imagedir=f"/data/{username}/images/{sample_name}",
                workdir=f"/data/{username}/proc/{sample_name}_{pipeline}",
                log="Processing log...",
                warning="Minor warning" if random.random() > 0.8 else "",
                sampleName=sample_name,
                username=username,
                beamline="23ID-D",
                starttime=datetime.now() - timedelta(hours=random.randint(1, 48)),
                elapsedtime=f"{random.randint(10, 600)}s",
                imageSet="set1",
                logfile=f"/data/{username}/proc/{sample_name}_{pipeline}/log.txt"
            )
            session.add(status)
            session.flush() # Get ID

            # Create DataProcessResults if success
            if state == "SUCCESS":
                result = DataProcessResults(
                    pipelinestatus_id=status.id,
                    sampleName=sample_name,
                    state="SUCCESS",
                    software="XDS",
                    highresolution=f"{random.uniform(1.2, 3.5):.2f}",
                    rmerge=f"{random.uniform(0.02, 0.2):.3f}",
                    isigmai=f"{random.uniform(5.0, 30.0):.1f}",
                    completeness=f"{random.uniform(90.0, 100.0):.1f}",
                    multiplicity=f"{random.uniform(2.0, 10.0):.1f}",
                    spacegroup=random.choice(spacegroups),
                    unitcell="50.0 60.0 70.0 90 90 90",
                    report_url=f"/data/{username}/proc/{sample_name}_{pipeline}/report.html",
                    table1="Table 1 data...",
                    wavelength="0.979",
                    isa=f"{random.uniform(10.0, 25.0):.1f}"
                )
                session.add(result)
        
        try:
            session.commit()
            print(f"Successfully added {num_entries} processing entries.")
        except Exception as e:
            session.rollback()
            print(f"Failed to seed processing data: {e}", file=sys.stderr)

if __name__ == "__main__":
    seed_processing()
