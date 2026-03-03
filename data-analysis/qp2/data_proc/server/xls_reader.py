# qp2/data_proc/server/xls_reader.py
import logging
import os
import numpy as np
import pandas as pd

from qp2.utils.auxillary import sanitize_space_group, sanitize_unit_cell

logger = logging.getLogger(__name__)

# Global cache to avoid redundant parsing across multiple instances
# Key: file_path, Value: (modification_time, dataframe)
_SPREADSHEET_CACHE = {}


class xlsReader:
    """Reads screening spreadsheet to extract parameters like model, sequence, and reference data."""

    def __init__(self, xlsFile):
        self.xlsFile = xlsFile
        self.user_home = self._get_user_home()
        self.df = self._get_df()

    def _get_user_home(self):
        """
        Extracts the user's home directory from the spreadsheet path.
        For example: 
        - /mnt/beegfs/DATA/esaf.../../../USER/b337337/Downloads/... -> /mnt/beegfs/USER/b337337
        - /mnt/beegfs/qxu/Downloads/... -> /mnt/beegfs/qxu
        Fallback to os.environ.get("HOME") if not found.
        """
        import re
        
        abspath = os.path.abspath(self.xlsFile)
        
        # Match standard user pattern: /mnt/beegfs/USER/username
        match = re.search(r'^(/mnt/beegfs/USER/[^/]+)', abspath)
        if match:
            return match.group(1)
            
        # Match DATA pattern: /mnt/beegfs/DATA/esaf_or_proposal
        match = re.search(r'^(/mnt/beegfs/DATA/[^/]+)', abspath)
        if match:
            return match.group(1)

        # Match staff pattern directly under beegfs (or anything top-level)
        # e.g., /mnt/beegfs/qxu/spreadsheet.xls -> /mnt/beegfs/qxu
        match = re.search(r'^(/mnt/beegfs/[^/]+)', abspath)
        if match:
            return match.group(1)
            
        return os.environ.get("HOME", "")

    def _get_df(self):
        """Retrieves the dataframe from cache or parses it from the file."""
        if not os.path.exists(self.xlsFile):
            logger.error(f"{self.xlsFile} does not exist.")
            return None

        try:
            mtime = os.path.getmtime(self.xlsFile)
            
            # Check cache
            if self.xlsFile in _SPREADSHEET_CACHE:
                cached_mtime, cached_df = _SPREADSHEET_CACHE[self.xlsFile]
                if cached_mtime == mtime:
                    logger.debug(f"Using cached spreadsheet: {self.xlsFile}")
                    return cached_df

            # Parse and cache
            logger.info(f"Parsing spreadsheet: {self.xlsFile}")
            df = pd.read_excel(self.xlsFile).fillna("")
            _SPREADSHEET_CACHE[self.xlsFile] = (mtime, df)
            return df

        except Exception as e:
            logger.error(f"Failed to parse Excel file {self.xlsFile}: {e}")
            return None

    def get_row(self, mounted_crystal):
        if self.df is not None:
            # Cast to string to ensure matching works against numbers like '1' vs 1
            row = self.df[self.df["Port"].astype(str) == str(mounted_crystal).strip()]
            if row.empty:
                logger.warning(
                    f"No row matching requested crystal {mounted_crystal} was found."
                )
            return row
        return pd.DataFrame()

    def get_element(self, mounted_crystal, column_name):
        if self.df is None:
            return None
        row = self.get_row(mounted_crystal)
        if row.empty:
            return None

        if column_name not in row.columns:
            # Try case-insensitive matching
            cols = {c.lower(): c for c in self.df.columns}
            if column_name.lower() in cols:
                column_name = cols[column_name.lower()]
            else:
                return None

        val = row[column_name].values[0]
        element = str(val).strip() if val else ""

        if element:
            return element

        # Fallback: Look for rows with the same 'Protein' name
        if "Protein" in row.columns:
            protein = str(row["Protein"].values[0]).strip()
            if protein:
                same_protein_rows = self.df[
                    (self.df["Protein"] == protein) & (self.df[column_name] != "")
                ]
                if not same_protein_rows.empty:
                    element = str(same_protein_rows.iloc[0][column_name]).strip()

        return element if element else None

    def _looking_for_file(self, filename, ext=""):
        """Searches for file in absolute path, or relative to user's home/Downloads."""
        if not filename:
            return None

        original_filename = filename = str(filename).strip()
        checked_paths = []

        if "$HOME" in filename:
            filename = filename.replace("$HOME", self.user_home)

        # 1. Check absolute path (or path that might be resolved if we expanded ~)
        if filename.startswith("~/"):
            filename = os.path.join(self.user_home, filename[2:])
        elif filename == "~":
            filename = self.user_home

        if os.path.isabs(filename):
            checked_paths.append(filename)
            if os.path.exists(filename):
                logger.debug(f"File '{original_filename}' resolved to absolute path: {filename}")
                return filename

        # 2. Check common directories relative to the user's home
        search_dirs = [
            os.path.join(self.user_home, "Downloads"),
            os.path.join(self.user_home, "Desktop"),
            self.user_home,
            os.getcwd(),
        ]

        # Check exact filename first
        for d in search_dirs:
            full_path = os.path.join(d, filename)
            checked_paths.append(full_path)
            if os.path.exists(full_path):
                logger.debug(f"File '{original_filename}' resolved to relative path: {full_path}")
                return full_path

        # If extension provided, try appending it if missing?
        # (Usually user provides full name, so we skip auto-appending extensions to be safe)

        logger.debug(f"File '{original_filename}' not found. Checked paths: {checked_paths}")
        return None

    # --- Public Getters ---
    def get_model_path(self, mounted_crystal):
        return self._looking_for_file(self.get_element(mounted_crystal, "ModelPath"))

    def get_sequence_path(self, mounted_crystal):
        return self._looking_for_file(self.get_element(mounted_crystal, "SequencePath"))

    def get_reference_dataset(self, mounted_crystal):
        # Matches 'ReferenceDataSet' column
        return self._looking_for_file(
            self.get_element(mounted_crystal, "ReferenceDataSet")
        )

    def get_nmol(self, mounted_crystal):
        return self.get_element(mounted_crystal, "NMol")

    def get_space_group(self, mounted_crystal):
        raw = self.get_element(mounted_crystal, "Spacegroup")
        return sanitize_space_group(raw)

    def get_unit_cell(self, mounted_crystal):
        raw_cell = self.get_element(mounted_crystal, "UnitCell")
        if raw_cell:
            return sanitize_unit_cell(raw_cell)
        return None

    def get_metal(self, mounted_crystal):
        return self.get_element(mounted_crystal, "Metal")
