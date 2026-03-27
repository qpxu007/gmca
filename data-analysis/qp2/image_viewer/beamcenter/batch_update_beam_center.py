import os
import glob
import argparse
from qp2.log.logging_config import get_logger
from qp2.image_viewer.beamcenter.beam_center_updater import update_beam_center_in_master_file

logger = get_logger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Batch update beam center in HDF5 master files.")
    parser.add_argument("directory", help="Directory containing the HDF5 master files")
    parser.add_argument("new_x", type=float, help="New beam center X coordinate")
    parser.add_argument("new_y", type=float, help="New beam center Y coordinate")
    parser.add_argument("--wavelength", type=float, default=None, help="New incident wavelength in Angstroms (optional)")
    parser.add_argument("--det_dist", type=float, default=None, help="New detector distance in mm (optional)")
    parser.add_argument("--remove-correction", action="store_true", help="Remove flatfield correction (default is False, preserves master file content)")
    parser.add_argument("--pattern", type=str, default="*master.h5", help="Filename pattern to match (default: *master.h5)")
    parser.add_argument("--recursive", action="store_true", help="Search for master files recursively in the directory")
    
    args = parser.parse_args()
    
    if not os.path.isdir(args.directory):
        logger.error(f"Directory not found: {args.directory}")
        return
        
    if args.recursive:
        master_files = glob.glob(os.path.join(args.directory, "**", args.pattern), recursive=True)
    else:
        master_files = glob.glob(os.path.join(args.directory, args.pattern))
        
    if not master_files:
        logger.warning(f"No {args.pattern} files found in {args.directory}")
        return
        
    logger.info(f"Found {len(master_files)} master files in {args.directory}")
    
    success_count = 0
    failure_count = 0
    
    for file_path in master_files:
        logger.info(f"Processing {file_path}...")
        
        # Passing backup=True natively handles copying to .bak. It also checks
        # if the .bak already exists and preserves it, ensuring the first pristine
        # original is never overwritten during multiple runs
        success, final_path = update_beam_center_in_master_file(
            file_path=file_path,
            new_x=args.new_x,
            new_y=args.new_y,
            new_wavelength=args.wavelength,
            new_det_dist=args.det_dist,
            remove_correction=args.remove_correction,
            backup=True,
            save_nexus=False
        )
        
        if success:
            logger.info(f"Successfully updated beam center in {final_path}")
            success_count += 1
        else:
            logger.error(f"Failed to update beam center in {file_path}")
            failure_count += 1
            
    logger.info(f"Batch update completed. Success: {success_count}, Failed: {failure_count}")

if __name__ == "__main__":
    main()
