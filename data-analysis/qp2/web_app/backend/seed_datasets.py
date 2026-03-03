import sys
import os
import json
from datetime import datetime, timedelta
import random

try:
    from qp2.xio.db_manager import DBManager
    from qp2.data_viewer.models import DatasetRun
except ImportError as e:
    print(f"ImportError: {e}", file=sys.stderr)
    sys.exit(1)

def generate_sample_headers():
    detector = random.choice(["Eiger", "Pilatus", "Rayonix"])
    exposure = round(random.uniform(0.1, 5.0), 2)
    wavelength = round(random.uniform(0.8, 1.5), 3)
    resolution = round(random.uniform(1.0, 3.0), 2)
    return {
        "detector": detector,
        "exposure_time": exposure,
        "wavelength": wavelength,
        "resolution": resolution,
        "comments": random.choice(["Good data", "Crystal diffracted well", "Low resolution", "Needs more work", "Protein sample"])
    }

def seed_datasets(num_datasets=20):
    print("Initializing DB Manager...")
    db = DBManager(beamline="default") # Assuming default beamline context is fine

    with db.get_session() as session:
        print(f"Seeding {num_datasets} sample DatasetRun entries...")
        users = ["user1", "staff", "admin", "guest"]
        collect_types = ["screening", "native", "derivative", "remote"]
        run_prefixes = ["run_A", "run_B", "run_C", "run_D"]

        for i in range(num_datasets):
            username = random.choice(users)
            run_prefix = f"{random.choice(run_prefixes)}_{random.randint(100, 999)}"
            collect_type = random.choice(collect_types)
            master_files = f"/data/{username}/project{random.randint(1,5)}/{run_prefix}_{random.randint(1,100)}/master.h5"
            total_frames = random.randint(100, 3000)
            headers_json = json.dumps(generate_sample_headers())
            
            # created_at spread over the last few days
            created_at = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0,23), minutes=random.randint(0,59))

            dataset = DatasetRun(
                username=username,
                run_prefix=run_prefix,
                collect_type=collect_type,
                master_files=master_files,
                total_frames=total_frames,
                headers=headers_json,
                created_at=created_at
            )
            session.add(dataset)
        
        try:
            session.commit()
            print(f"Successfully added {num_datasets} sample datasets.")
        except Exception as e:
            session.rollback()
            print(f"Failed to seed datasets: {e}", file=sys.stderr)

if __name__ == "__main__":
    seed_datasets()
