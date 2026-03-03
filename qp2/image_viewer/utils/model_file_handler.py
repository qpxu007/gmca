import os
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication
from qp2.log.logging_config import get_logger
from qp2.image_viewer.utils.crystal_parsing import parse_crystal_parameters

logger = get_logger(__name__)

def handle_model_file_update(file_path_input, space_group_input=None, unit_cell_input=None, download_dir_input=None, ref_hkl_input=None):
    """
    Handles logic when a model file path is entered:
    1. Checks if it's a 4-letter PDB code.
    2. If so, downloads and cleans it.
    3. Updates the file path input with the absolute path.
    4. Parses the file for crystal parameters.
    5. Updates space group and unit cell inputs if provided.
    
    Args:
        file_path_input (QLineEdit): The input widget containing the file path or PDB code.
        space_group_input (QLineEdit, optional): The input widget for Space Group to update.
        unit_cell_input (QLineEdit, optional): The input widget for Unit Cell to update.
        download_dir_input (QLineEdit, optional): The input widget specifying a preferred download directory.
        
    Returns:
        bool: True if a PDB file was downloaded and updated, False otherwise.
    """
    file_path = file_path_input.text().strip()
    if not file_path:
        return False

    final_path = file_path
    downloaded = False

    # Check for PDB code
    if not os.path.exists(file_path):
        if len(file_path) == 4 and file_path.isalnum():
            try:
                from qp2.pipelines.gmcaproc.rcsb import RCSB
            except ImportError as e:
                logger.error(f"Failed to import RCSB module: {e}")
                return False

            try:
                QApplication.setOverrideCursor(Qt.WaitCursor)
                
                # Determine download directory
                download_dir = ""
                if download_dir_input:
                    download_dir = download_dir_input.text().strip()
                
                if not download_dir or not os.path.isdir(download_dir):
                    # Try Downloads first, then Desktop
                    downloads_path = os.path.expanduser("~/Downloads")
                    desktop_path = os.path.expanduser("~/Desktop")
                    if os.path.isdir(downloads_path):
                        download_dir = downloads_path
                    elif os.path.isdir(desktop_path):
                        download_dir = desktop_path
                    else:
                        download_dir = os.getcwd()
                    
                logger.debug(f"Downloading PDB {file_path} to {download_dir}")
                rcsb = RCSB(default_directory=download_dir)
                # Download and clean (clean_up removes waters and ligands)
                downloaded_file = rcsb.download(file_path, directory=download_dir, cleanup=True)
                
                if downloaded_file and os.path.exists(downloaded_file):
                    final_path = os.path.abspath(downloaded_file)
                    # Update the input field with the new path
                    file_path_input.setText(final_path)
                    downloaded = True
                else:
                    logger.warning(f"Could not download PDB {file_path}")
                    QApplication.restoreOverrideCursor()
                    return False
            except Exception as e:
                logger.error(f"Error downloading PDB {file_path}: {e}")
                QApplication.restoreOverrideCursor()
                return False
            finally:
                QApplication.restoreOverrideCursor()
        else:
            # Not a file and not a valid PDB code, do nothing
            return False

    if not os.path.exists(final_path):
        return False
        
    # Parse parameters from the (existing or downloaded) file
    try:
        data = parse_crystal_parameters(final_path)
        
        sg = data.get("space_group")
        uc = data.get("unit_cell")

        if space_group_input:
            if sg:
                # Remove all spaces from the space group string
                sg_clean = "".join(sg.split())
                space_group_input.setText(sg_clean)
            
        if unit_cell_input:
            if uc:
                unit_cell_input.setText(uc)
        
        # Generate reference HKL if requested and possible
        if ref_hkl_input and sg and uc:
            try:
                from qp2.utils.pdb_to_hkl import generate_reference_hkl
                
                # Construct output path: replace extension with .HKL
                base_name = os.path.splitext(final_path)[0]
                hkl_path = base_name + ".HKL"
                
                logger.info(f"Generating reference HKL: {hkl_path}")
                generate_reference_hkl(final_path, hkl_path)
                
                ref_hkl_input.setText(hkl_path)
            except ImportError:
                logger.error("Could not import generate_reference_hkl")
            except Exception as e:
                logger.error(f"Failed to generate reference HKL: {e}")
                
    except Exception as e:
        logger.error(f"Error parsing crystal parameters from {final_path}: {e}")
        
    return downloaded
