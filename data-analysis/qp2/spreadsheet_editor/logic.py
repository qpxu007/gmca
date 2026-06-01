
import csv
import re
import os
from collections import defaultdict
from typing import List, Dict, Optional

REQUIRED_HEADERS = [
    "Port", "Puck", "CrystalID", "Protein", "Comment", "Directory",
    "FreezingCondition", "CrystalCondition", "Metal", "Spacegroup",
    "ModelPath", "SequencePath", "Priority", "Person",
    "DesiredResolution", "DesiredDosage"
]

ROWS_PER_PUCK = 16

class Puck:
    def __init__(self, original_label: str, rows: List[Dict[str, str]]):
        self.original_label = original_label
        self.rows = rows
        
    @property
    def is_empty(self):
        # A puck is "empty" if all relevant fields (except Port) are empty
        # But for our purpose, a Puck object exists if it was parsed from the file.
        # We might want to check if it actually has data.
        for row in self.rows:
            if row.get("CrystalID") or row.get("Protein"):
                return False
        return True

    def get_summary(self, slot_name=None):
        count = sum(1 for r in self.rows if r.get("CrystalID"))

        first_id = "Empty"
        first_data_row = None
        for i, row in enumerate(self.rows):
            cid = row.get("CrystalID", "").strip()
            if cid:
                # Apply the same port→slot transform used at save time
                if slot_name:
                    old_port = row.get("Port", "").strip()
                    if cid == old_port:
                        cid = f"{slot_name}{i + 1}"
                first_id = cid
                first_data_row = row
                break

        summary_parts = [f"{count} Crystals\nFirst: {first_id}"]
        
        if first_data_row:
            if first_data_row.get("Protein"):
                summary_parts.append(f"(Protein: {first_data_row['Protein']})")
            if first_data_row.get("Comment"):
                summary_parts.append(f"(Comment: {first_data_row['Comment']})")
            if first_data_row.get("Person"):
                summary_parts.append(f"(Person: {first_data_row['Person']})")
        
        return " ".join(summary_parts)

class SpreadsheetManager:
    def __init__(self, puck_names: List[str] = None):
        self.puck_names = puck_names if puck_names else list("ABCDEFGHIJKLMNOPQR")
        self.pucks: Dict[str, Puck] = {} 
        self.errors = []

    def validate_headers(self, headers: List[str]) -> bool:
        # Optional headers that shouldn't fail validation if missing from older files
        optional_headers = {"Puck", "DesiredResolution", "DesiredDosage"}
        missing = [h for h in REQUIRED_HEADERS if h not in headers and h not in optional_headers]
        if missing:
            self.errors.append(f"Missing headers: {', '.join(missing)}")
            return False
        return True

    @staticmethod
    def fix_duplicate_crystal_ids(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Returns a new list of row dicts where duplicate CrystalIDs have been made
        unique by appending a numbered suffix (_1, _2, …) to every occurrence of
        each duplicated value.  Non-duplicated IDs are left untouched.
        """
        from collections import Counter

        counts: Counter = Counter(
            row.get("CrystalID", "").strip()
            for row in rows
            if row.get("CrystalID", "").strip()
        )
        duplicated = {cid for cid, n in counts.items() if n > 1}

        seen: Dict[str, int] = {}
        fixed = []
        for row in rows:
            row = row.copy()
            cid = row.get("CrystalID", "").strip()
            if cid in duplicated:
                occurrence = seen.get(cid, 0) + 1
                seen[cid] = occurrence
                row["CrystalID"] = f"{cid}_{occurrence}"
            fixed.append(row)
        return fixed

    def load_file(self, filepath: str, auto_fix_duplicates: bool = False) -> Dict[str, Puck]:
        self.errors = []
        self.pucks = {}
        
        _, ext = os.path.splitext(filepath)
        ext = ext.lower()
        rows = []

        try:
            if ext in ['.xls', '.xlsx']:
                try:
                    import pandas as pd
                except ImportError:
                    self.errors.append("Pandas library is required for Excel files.")
                    return {}
                
                try:
                    # Read excel, interpret all columns as strings to match CSV behavior
                    # keep_default_na=False prevents NaN for empty cells, making them empty strings
                    df = pd.read_excel(filepath, keep_default_na=False, dtype=str)
                    
                    # Convert column names to string just in case
                    headers = [str(c) for c in df.columns]
                    if not self.validate_headers(headers):
                        raise ValueError("Invalid Excel headers.")
                        
                    # Convert to list of dicts
                    # We strip whitespace from keys if necessary, but standard is exact match
                    rows = df.to_dict('records')
                    
                    # Ensure values are strings (pandas might infer some types even with dtype=str if mixed)
                    for r in rows:
                        for k, v in r.items():
                            if v is None:
                                r[k] = ""
                            else:
                                r[k] = str(v).strip()
                                
                except Exception as e:
                    self.errors.append(f"Excel read error: {str(e)}")
                    return {}

            else:
                # Default to CSV
                try:
                    with open(filepath, 'r', newline='', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        if not self.validate_headers(reader.fieldnames):
                            raise ValueError("Invalid CSV headers.")
                        rows = list(reader)
                except Exception as e:
                    self.errors.append(f"CSV read error: {str(e)}")
                    return {}

        except Exception as e:
            self.errors.append(f"File processing error: {str(e)}")
            return {}

        if len(rows) % ROWS_PER_PUCK != 0:
            self.errors.append(f"Total rows ({len(rows)}) is not a multiple of {ROWS_PER_PUCK}.")
            return {}

        # Apply rule: if CrystalID is empty, copy from Port
        for row in rows:
            if not row.get("CrystalID", "").strip():
                row["CrystalID"] = row.get("Port", "").strip()

        if auto_fix_duplicates:
            rows = self.fix_duplicate_crystal_ids(rows)

        # Validate CrystalIDs
        crystal_ids = []
        for i, row in enumerate(rows):
            cid = row.get("CrystalID", "").strip()
            if cid:
                if cid in crystal_ids:
                    self.errors.append(f"Duplicate CrystalID found: '{cid}' at row {i+2}")
                crystal_ids.append(cid)
        
        if self.errors:
            return {}

        # Group by Puck Letter
        # We expect Port to be like 'A1', 'B10', 'AA1', etc.
        # Regex: Start with non-digits (puck name), end with digits (position)
        grouped_rows = defaultdict(list)
        
        # Regex explanation:
        # ^([A-Za-z0-9]+?) -> Group 1: Puck Name (non-greedy, at least 1 char)
        # ([0-9]+)$       -> Group 2: Position Number (digits at end)
        # We need to be careful. 'A1' -> 'A', '1'. 'AB12' -> 'AB', '12'.
        # 'Puck1Position1' -> 'Puck1Position', '1'.
        # Let's assume standard format: "PuckName" + "Position"
        # Since user defines puck names, we can check if Port starts with any known puck name.
        
        for i, row in enumerate(rows):
            port = row.get("Port", "").strip()
            
            match = re.match(r"^([A-Za-z0-9]+?)(\d+)$", port)
            
            if not match:
                self.errors.append(f"Invalid Port format '{port}' at row {i+2}. Expected [PuckName][PositionNumber].")
                continue
            
            puck_letter = match.group(1)
            
            # Additional check: Is this puck name in our configured list?
            if puck_letter not in self.puck_names:
                # Try to be smarter? 
                # If we have pucks ['A', 'B'] and port is 'A1', match is A, 1. Correct.
                # If pucks ['P1', 'P2'] and port is 'P1_1', regex might fail or capture P1_ as name.
                # Let's enforce strict matching against configured names if possible.
                
                # If regex extracted 'A' but allowed is 'A', good.
                # If regex extracted 'A' but allowed is 'AA', maybe 'AA1' was parsed as A, A1? No.
                # The simple regex is usually fine for alphanumeric.
                self.errors.append(f"Unknown Puck '{puck_letter}' at row {i+2}. Configured pucks: {', '.join(self.puck_names)}")
                continue

            grouped_rows[puck_letter].append(row)

        if self.errors:
            return {}

        # Validate Group Sizes
        for letter, p_rows in grouped_rows.items():
            if len(p_rows) != ROWS_PER_PUCK:
                self.errors.append(f"Puck {letter} has {len(p_rows)} rows. Expected {ROWS_PER_PUCK}.")
                continue
            
            self.pucks[letter] = Puck(letter, p_rows)

        # 4. Global Sanity Checks (Completeness & Data Quality)
        self._validate_completeness(rows)
        self._validate_data_content(rows)

        if self.errors:
            return {}

        return self.pucks

    def _validate_completeness(self, rows: List[Dict[str, str]]):
        """
        Ensures all expected ports are present and no duplicates exist.
        """
        found_ports = set()
        for i, row in enumerate(rows):
            port = row.get("Port", "").strip()
            if port in found_ports:
                self.errors.append(f"Duplicate Port found: '{port}'.")
            found_ports.add(port)

        expected_ports = set()
        for name in self.puck_names:
            for i in range(1, ROWS_PER_PUCK + 1):
                expected_ports.add(f"{name}{i}")

        missing = expected_ports - found_ports
        if missing:
            # Sort for nicer error message
            missing_sorted = sorted(list(missing), key=lambda x: (x[0], int(x[1:])) if x[1:].isdigit() else x)
            self.errors.append(f"Missing required Ports: {', '.join(missing_sorted[:5])}...")
        
        # We don't necessarily error on EXTRA ports, but we could. 
        # For now, we focus on missing ports as requested "must be missing".

    def _validate_data_content(self, rows: List[Dict[str, str]]):
        """
        Validates CrystalID format/uniqueness and Directory format.
        """
        seen_crystal_ids = set()
        
        for i, row in enumerate(rows):
            # 1. CrystalID Validation
            cid = row.get("CrystalID", "").strip()
            if cid:
                if len(cid) >= 20:
                    self.errors.append(f"Row {i+1}: CrystalID '{cid}' exceeds 20 characters.")
                
                if not re.match(r"^[a-zA-Z0-9_-]+$", cid):
                    self.errors.append(f"Row {i+1}: CrystalID '{cid}' contains invalid characters. Allowed: Alphanumeric, '_', '-'.")
                
                if cid in seen_crystal_ids:
                    self.errors.append(f"Row {i+1}: Duplicate CrystalID '{cid}'.")
                seen_crystal_ids.add(cid)

            # 2. Puck Validation (letters followed by optional spaces then numbers)
            puck_val = row.get("Puck", "").strip()
            if puck_val:
                if not re.match(r"^[A-Za-z]+\s*\d+$", puck_val):
                    self.errors.append(f"Row {i+1}: Puck '{puck_val}' has invalid format. Expected: letters followed by optional spaces and numbers (e.g. 'AB 123').")

            # 3. Directory Validation
            directory = row.get("Directory", "").strip()
            if directory:
                if not re.match(r"^[a-zA-Z0-9_/-]+$", directory):
                    self.errors.append(f"Row {i+1}: Directory '{directory}' contains invalid characters. Allowed: Alphanumeric, '_', '-', '/'.")

    def create_empty_pucks(self) -> Dict[str, Puck]:
        """
        Generates a fresh set of pucks with empty data based on configured puck names.
        """
        self.pucks = {}
        for name in self.puck_names:
            rows = []
            for i in range(1, ROWS_PER_PUCK + 1):
                row_data = {h: "" for h in REQUIRED_HEADERS}
                row_data["Port"] = f"{name}{i}"
                row_data["CrystalID"] = row_data["Port"]
                row_data["Directory"] = row_data["Port"]
                rows.append(row_data)
            self.pucks[name] = Puck(name, rows)
        return self.pucks

    def save_file(self, filepath: str, slots: List[Optional[Puck]]):
        """
        slots: A list corresponding to self.puck_names order.
        """
        all_output_rows = []
        
        # 1. Generate all data first
        for i, puck_name in enumerate(self.puck_names):
            puck = slots[i] if i < len(slots) else None
            
            # If the puck is missing (None), do we generate empty rows? 
            # The template has rows A1..R16 even if empty.
            # Let's assume we fill 1..16 for empty slots.
            
            if puck:
                # Use the existing rows from the puck
                for row_idx, source_row in enumerate(puck.rows, start=1):
                    row_data = source_row.copy()
                    
                    old_port = source_row.get("Port", "").strip()
                    
                    # Force standard 1-16 numbering based on row index
                    # This guarantees "A1-16" structure as requested, overriding potentially non-standard input suffixes.
                    new_port = f"{puck_name}{row_idx}"
                    row_data["Port"] = new_port
                    
                    # 2. Update CrystalID if it matches Old Port (Point 2)
                    # We compare against the FULL old port (e.g. A1)
                    if row_data.get("CrystalID") == old_port:
                        row_data["CrystalID"] = new_port
                        
                    # 3. Update Directory if it contains Old Port (Point 3)
                    directory = row_data.get("Directory", "")
                    if directory and old_port:
                        # Use regex to replace old_port with new_port, avoiding partial matches (e.g. A1 in A10)
                        # We look for the old_port surrounded by non-alphanumeric chars or boundaries
                        pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(old_port)}(?![A-Za-z0-9])")
                        row_data["Directory"] = pattern.sub(new_port, directory)
                        
                    all_output_rows.append(row_data)
                    
            else:
                # Empty slot: generate blank rows 1..16
                for row_idx in range(1, ROWS_PER_PUCK + 1):
                    new_port = f"{puck_name}{row_idx}"
                    row_data = {h: "" for h in REQUIRED_HEADERS}
                    row_data["Port"] = new_port
                    all_output_rows.append(row_data)

        # 2. Write to file based on extension
        _, ext = os.path.splitext(filepath)
        ext = ext.lower()

        try:
            if ext == '.xls':
                # pandas 2.x dropped xlwt support; write directly with xlwt so the
                # file is readable by xlrd 2.x (the only Excel engine available in
                # the PyBluice server venv).
                try:
                    import xlwt
                except ImportError:
                    raise IOError("The 'xlwt' library is required to save .xls files.")
                wb = xlwt.Workbook()
                ws = wb.add_sheet('Sheet1')
                for col_idx, header in enumerate(REQUIRED_HEADERS):
                    ws.write(0, col_idx, header)
                for row_idx, row_data in enumerate(all_output_rows, start=1):
                    for col_idx, header in enumerate(REQUIRED_HEADERS):
                        ws.write(row_idx, col_idx, str(row_data.get(header, '') or ''))
                wb.save(filepath)
            elif ext == '.xlsx':
                try:
                    import pandas as pd
                except ImportError:
                    raise IOError("Pandas library is required to save .xlsx files.")
                try:
                    df = pd.DataFrame(all_output_rows, columns=REQUIRED_HEADERS)
                    df.to_excel(filepath, index=False, engine='openpyxl')
                except Exception as e:
                    raise IOError(f"Failed to save Excel file: {e}")
            else:
                # CSV (default)
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=REQUIRED_HEADERS)
                    writer.writeheader()
                    writer.writerows(all_output_rows)

        except IOError:
            raise
        except Exception as e:
            raise IOError(f"Failed to save file: {e}")
