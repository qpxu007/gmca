
import sys
import os
from pathlib import Path

# Add project root to path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.append(str(project_root))

print(f"Project root: {project_root}")

try:
    from xio.db_manager import DBManager
    from qp2.data_viewer.models import Spreadsheet
    print("Successfully imported DBManager and Spreadsheet model.")
except ImportError as e:
    print(f"ImportError: {e}")
    sys.exit(1)

try:
    db = DBManager()
    print(f"DB initialized. URL: {db.db_url}")
    
    # Try to query
    sheets = db.find_all(Spreadsheet)
    print(f"Found {len(sheets)} spreadsheets.")
    
except Exception as e:
    print(f"DB Error: {e}")
    sys.exit(1)
