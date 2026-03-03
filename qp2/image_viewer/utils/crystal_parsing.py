import os
from typing import Optional, Dict

def parse_crystal_parameters(file_path: str) -> Dict[str, Optional[str]]:
    """
    Parses a PDB or mmCIF file to extract Space Group and Unit Cell parameters.

    Args:
        file_path: Path to the .pdb, .cif, or .mmcif file.

    Returns:
        A dictionary with keys:
        - "space_group": str or None
        - "unit_cell": str or None (format: "a b c alpha beta gamma")
    """
    if not file_path or not os.path.exists(file_path):
        return {"space_group": None, "unit_cell": None}

    ext = os.path.splitext(file_path)[1].lower()
    
    if ext == ".pdb":
        return _parse_pdb(file_path)
    elif ext in [".cif", ".mmcif"]:
        return _parse_mmcif(file_path)
    
    return {"space_group": None, "unit_cell": None}

def _parse_pdb(file_path: str) -> Dict[str, Optional[str]]:
    """
    Parses PDB CRYST1 record.
    Format:
    CRYST1   52.000   58.600   61.900  90.00  90.00  90.00 P 21 21 21    4
    """
    sg = None
    uc = None
    try:
        with open(file_path, "r") as f:
            for line in f:
                if line.startswith("CRYST1"):
                    # Fixed width parsing is safer for PDB, but split works for standard files
                    # Columns:
                    # 7-15: a
                    # 16-24: b
                    # 25-33: c
                    # 34-40: alpha
                    # 41-47: beta
                    # 48-54: gamma
                    # 56-66: space group
                    if len(line) >= 54:
                        try:
                            a = float(line[6:15])
                            b = float(line[15:24])
                            c = float(line[24:33])
                            al = float(line[33:40])
                            be = float(line[40:47])
                            ga = float(line[47:54])
                            uc = f"{a} {b} {c} {al} {be} {ga}"
                            
                            if len(line) >= 66:
                                sg = line[55:66].strip()
                        except ValueError:
                            pass # parsing failed
                    break # Only need the first CRYST1
    except Exception:
        pass
        
    return {"space_group": sg, "unit_cell": uc}

def _parse_mmcif(file_path: str) -> Dict[str, Optional[str]]:
    """
    Parses mmCIF tags for cell and symmetry.
    """
    sg = None
    cell = {}
    
    # Mapping CIF tags to cell parameters
    cell_tags = {
        "_cell.length_a": "a",
        "_cell.length_b": "b",
        "_cell.length_c": "c",
        "_cell.angle_alpha": "al",
        "_cell.angle_beta": "be",
        "_cell.angle_gamma": "ga"
    }
    
    # Possible SG tags
    sg_tags = ["_symmetry.space_group_name_H-M", "_space_group.name_H-M_alt"]

    try:
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                
                # Check for cell params
                for tag, key in cell_tags.items():
                    if line.startswith(tag):
                        parts = line.split()
                        if len(parts) >= 2:
                            # Handle loop_ structures? Basic parser assumes key-value pairs
                            try:
                                cell[key] = float(parts[1])
                            except ValueError:
                                pass
                
                # Check for SG
                for tag in sg_tags:
                    if line.startswith(tag):
                        # Extract value, handling quotes: 'P 21 21 21'
                        val_part = line[len(tag):].strip()
                        if val_part.startswith("'") or val_part.startswith('"'):
                            sg = val_part[1:-1]
                        else:
                            sg = val_part
    except Exception:
        pass

    uc = None
    if len(cell) == 6:
        # Order matters
        try:
            uc = f"{cell['a']} {cell['b']} {cell['c']} {cell['al']} {cell['be']} {cell['ga']}"
        except KeyError:
            pass

    return {"space_group": sg, "unit_cell": uc}
