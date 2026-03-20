import h5py
import shutil
import os
import numpy as np
import subprocess
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

def h5repack_file(file_path):
    """
    Runs h5repack on the file to reclaim space from deleted objects.
    """
    if not shutil.which("h5repack"):
        logger.warning("h5repack not found in PATH. File size will not be reduced.")
        return False
    
    temp_path = file_path + ".tmp_repack"
    try:
        logger.info(f"Repacking {file_path} to reduce size...")
        subprocess.run(["h5repack", file_path, temp_path], check=True)
        os.replace(temp_path, file_path)
        logger.info(f"Repack complete. File size reduced.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"h5repack failed: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False
    except Exception as e:
        logger.error(f"Error during repack: {e}")
        return False

def fix_nexus_file_structure(file_path):
    """
    Applies NeXus compliance attributes to an HDF5 file.
    """
    try:
        with h5py.File(file_path, 'r+') as f:
            # 1. Root Group
            f.attrs['NX_class'] = np.array('NXroot', dtype='S')
            f.attrs['file_name'] = np.array(os.path.basename(file_path), dtype='S')
            
            # 2. Entry Group
            if 'entry' not in f:
                entry = f.create_group('entry')
            else:
                entry = f['entry']
            
            entry.attrs['NX_class'] = np.array('NXentry', dtype='S')
            
            # Handle 'definition' dataset safely
            if 'definition' in entry:
                del entry['definition']
            entry['definition'] = np.array('NXmx', dtype='S')
            
            # 3. Instrument Group
            if 'instrument' not in entry:
                instrument = entry.create_group('instrument')
            else:
                instrument = entry['instrument']
            instrument.attrs['NX_class'] = np.array('NXinstrument', dtype='S')

            # 4. Detector Group
            if 'detector' not in instrument:
                detector = instrument.create_group('detector')
            else:
                detector = instrument['detector']
            detector.attrs['NX_class'] = np.array('NXdetector', dtype='S')

            # 5. Beam Group
            if 'beam' not in instrument:
                beam = instrument.create_group('beam')
            else:
                beam = instrument['beam']
            beam.attrs['NX_class'] = np.array('NXbeam', dtype='S')

            # 6. Sample Group
            if 'sample' not in entry:
                sample = entry.create_group('sample')
            else:
                sample = entry['sample']
            sample.attrs['NX_class'] = np.array('NXsample', dtype='S')

            # 7. Data Group
            if 'data' in entry:
                data_grp = entry['data']
                data_grp.attrs['NX_class'] = np.array('NXdata', dtype='S')
                if 'data' in data_grp and 'signal' not in data_grp.attrs:
                    data_grp.attrs['signal'] = np.array('data', dtype='S')

            logger.info(f"Applied NX_class attributes to {file_path}")
            return True
            
    except Exception as e:
        logger.error(f"Error updating Nexus structure in {file_path}: {e}")
        return False

def update_beam_center_in_master_file(file_path, new_x, new_y, new_wavelength=None, new_det_dist=None, remove_correction=True, backup=True, save_nexus=False):
    """
    Updates the beam center X and Y in the HDF5 master file.
    
    Args:
        file_path (str): Path to the HDF5 master file.
        new_x (float): New beam center X coordinate.
        new_y (float): New beam center Y coordinate.
        new_wavelength (float, optional): New wavelength in Angstroms.
        new_det_dist (float, optional): New detector distance in mm.
        remove_correction (bool): Whether to remove flatfield. pixel_mask is preserved.
        backup (bool): Whether to create a backup (.bak) before modifying.
        save_nexus (bool): If True, creates a new file ending in _nexus.h5 instead of modifying original.
    
    Returns:
        tuple: (success (bool), result_path (str))
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False, None

    target_path = file_path

    if save_nexus:
        # Define Nexus filename convention: use .nxs extension
        base, _ = os.path.splitext(file_path)
        target_path = f"{base}.nxs"
        try:
            shutil.copy2(file_path, target_path)
            logger.info(f"Created copy for Nexus update: {target_path}")
            
            # Apply Nexus structure fixes
            if not fix_nexus_file_structure(target_path):
                logger.error("Failed to apply Nexus structure fixes.")
                return False, None
                
        except Exception as e:
            logger.error(f"Failed to create Nexus copy: {e}")
            return False, None
    elif backup:
        backup_path = file_path + ".bak"
        if os.path.exists(backup_path):
            logger.info(f"Backup already exists at {backup_path}. Preserving original copy.")
        else:
            try:
                shutil.copy2(file_path, backup_path)
                logger.info(f"Backup created at {backup_path}")
            except Exception as e:
                logger.error(f"Failed to create backup: {e}")
                return False, None

    try:
        with h5py.File(target_path, 'r+') as f:
            # Standard NeXus paths for beam center
            paths_to_update = {
                'x': [
                    "/entry/instrument/detector/beam_center_x",
                    "/entry/instrument/detector/detectorSpecific/beam_center_x"
                ],
                'y': [
                    "/entry/instrument/detector/beam_center_y",
                    "/entry/instrument/detector/detectorSpecific/beam_center_y"
                ]
            }

            updated_any = False
            
            for path in paths_to_update['x']:
                if path in f:
                    f[path][...] = new_x
                    logger.info(f"Updated {path} to {new_x}")
                    updated_any = True
            
            for path in paths_to_update['y']:
                if path in f:
                    f[path][...] = new_y
                    logger.info(f"Updated {path} to {new_y}")
                    updated_any = True

            # Update Wavelength (Angstrom)
            if new_wavelength is not None:
                wl_path = "/entry/instrument/beam/incident_wavelength"
                if wl_path in f:
                    f[wl_path][...] = new_wavelength
                    logger.info(f"Updated {wl_path} to {new_wavelength} A")
                    updated_any = True
                else:
                    logger.warning(f"Could not find {wl_path} to update wavelength")

            # Update Detector Distance (mm -> m)
            if new_det_dist is not None:
                dist_path = "/entry/instrument/detector/detector_distance"
                if dist_path in f:
                    # Convert mm to meters
                    f[dist_path][...] = new_det_dist / 1000.0
                    logger.info(f"Updated {dist_path} to {new_det_dist/1000.0} m ({new_det_dist} mm)")
                    updated_any = True
                else:
                    logger.warning(f"Could not find {dist_path} to update detector distance")

            # Update the NeXus detector transformation chain.
            # dxtbx (used by dials/xia2) reads the beam center from
            # the translation vector, NOT from beam_center_x/y.
            #   detector_origin = translation_vector * translation_distance
            #   beam_center_x = -origin_x / pixel_size
            #   beam_center_y = -origin_y / pixel_size
            #   det_distance  =  origin_z
            trans_path = "/entry/instrument/detector/transformations/translation"
            fast_path = "/entry/instrument/detector/module/fast_pixel_direction"
            if trans_path in f:
                # Read pixel size from fast_pixel_direction (meters)
                pixel_size = None
                if fast_path in f:
                    pixel_size = float(f[fast_path][()])
                if not pixel_size:
                    pixel_size = 75e-6  # 75 um default (Eiger)
                    logger.warning(f"Could not read pixel size, using default {pixel_size*1e6:.0f} um")

                # Determine detector distance in meters
                if new_det_dist is not None:
                    det_dist_m = new_det_dist / 1000.0
                else:
                    # Read current distance from existing translation
                    old_vec = f[trans_path].attrs.get("vector")
                    old_dist = float(f[trans_path][()])
                    if old_vec is not None:
                        det_dist_m = float(old_vec[2] * old_dist)
                    else:
                        det_dist_m = float(f.get("/entry/instrument/detector/detector_distance", [0.35])[()])

                # Compute new origin (meters)
                origin_x = -new_x * pixel_size
                origin_y = -new_y * pixel_size
                origin_z = det_dist_m
                origin = np.array([origin_x, origin_y, origin_z])

                # translation_distance = |origin|, translation_vector = origin / |origin|
                new_dist = float(np.linalg.norm(origin))
                new_vec = origin / new_dist

                f[trans_path][...] = new_dist
                f[trans_path].attrs["vector"] = new_vec
                logger.info(
                    f"Updated NeXus translation: distance={new_dist:.6f} m, "
                    f"vector=[{new_vec[0]:.6f}, {new_vec[1]:.6f}, {new_vec[2]:.6f}]"
                )
                updated_any = True
            else:
                logger.warning(
                    f"NeXus translation path {trans_path} not found. "
                    f"dials/xia2 may not see the updated beam center."
                )

            # Also remove large/redundant detectorSpecific items if they exist to save space
            if remove_correction:
                det_spec_path = "/entry/instrument/detector/detectorSpecific"
                if det_spec_path in f:
                    det_spec = f[det_spec_path]
                    # Only remove flatfield, preserve pixel_mask as it is critical for downstream tools
                    for name in ["flatfield"]:
                        if name in det_spec:
                            del det_spec[name]
                            logger.info(f"Removed {name} from {det_spec_path} in {target_path}")

            if not updated_any:
                logger.warning("No datasets found to update in standard paths.")
                return False, None

        # After all h5py operations are finished and file is closed, repack to reclaim space
        h5repack_file(target_path)

        return True, target_path

    except Exception as e:
        logger.error(f"Failed to update master file: {e}")
        return False, None