import sys
from pathlib import Path

# Add project root to path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.append(str(project_root))

try:
    from xio.db_manager import DBManager
    from qp2.data_viewer.models import DatasetRun
except ImportError:
    try:
        from xio.db_manager import DBManager
        from data_viewer.models import DatasetRun
    except ImportError as e:
        print(f"ImportError: {e}")
        sys.exit(1)

print("Connecting to DB...")
try:
    db = DBManager(beamline="default")
    with db.get_session() as session:
        count = session.query(DatasetRun).count()
        print(f"Total DatasetRun count: {count}")
        
        runs = session.query(DatasetRun).limit(5).all()
        for r in runs:
            print(f"Run: {r.run_prefix}, User: {r.username}, Created: {r.created_at}")
except Exception as e:
    print(f"DB Error: {e}")
