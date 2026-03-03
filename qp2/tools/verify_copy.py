import os
import sys
import argparse
import multiprocessing
import hashlib
from pathlib import Path
import time

def calculate_checksum(file_path, chunk_size=8192):
    """Calculates SHA256 checksum of a file."""
    sha256 = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
    except Exception as e:
        return str(e)

def verify_file(args):
    """
    Worker function to verify a single file pair.
    args: (rel_path, source_root, dest_root)
    """
    rel_path, source_root, dest_root = args
    src_path = source_root / rel_path
    dest_path = dest_root / rel_path
    
    # 1. Check existence
    if not dest_path.exists():
        return (str(rel_path), False, "Missing in destination")
    
    # 2. Check size (cheap fail-fast)
    if src_path.stat().st_size != dest_path.stat().st_size:
        return (str(rel_path), False, "Size mismatch")
        
    # 3. Check Content (SHA256)
    src_hash = calculate_checksum(src_path)
    dest_hash = calculate_checksum(dest_path)
    
    if src_hash == dest_hash:
        return (str(rel_path), True, "OK")
    else:
        return (str(rel_path), False, "Content mismatch (Checksum failed)")

def main():
    parser = argparse.ArgumentParser(description="Verify integrity of copied files using parallel checksums.")
    parser.add_argument("source", help="Source directory path")
    parser.add_argument("destination", help="Destination directory path")
    parser.add_argument("-j", "--jobs", type=int, default=multiprocessing.cpu_count(),
                        help=f"Number of parallel jobs (default: {multiprocessing.cpu_count()})")
    
    args = parser.parse_args()
    
    source_dir = Path(args.source).resolve()
    dest_dir = Path(args.destination).resolve()
    
    if not source_dir.exists():
        print(f"Error: Source directory '{source_dir}' does not exist.")
        sys.exit(1)
        
    print(f"Scanning source directory: {source_dir} ...")
    
    tasks = []
    
    # Walk source to build task list
    for root, dirs, files in os.walk(source_dir):
        rel_root = Path(root).relative_to(source_dir)
        for f in files:
            rel_path = rel_root / f
            tasks.append((rel_path, source_dir, dest_dir))
            
    print(f"Found {len(tasks)} files to verify.")
    print(f"Starting verification with {args.jobs} processes...")
    
    start_time = time.time()
    errors = []
    checked_count = 0
    
    with multiprocessing.Pool(processes=args.jobs) as pool:
        for i, result in enumerate(pool.imap_unordered(verify_file, tasks), 1):
            rel_path, success, msg = result
            checked_count += 1
            
            if not success:
                errors.append(f"{rel_path}: {msg}")
                # Print error immediately
                sys.stdout.write(f"\r\033[K[FAIL] {rel_path} -> {msg}\n")
            
            # Progress bar
            percent = (i / len(tasks)) * 100
            sys.stdout.write(f"\rProgress: [{i}/{len(tasks)}] {percent:.1f}%")
            sys.stdout.flush()

    print("\n")
    duration = time.time() - start_time
    
    print("-" * 40)
    print("Verification Summary:")
    print(f"  Files Checked: {checked_count}")
    print(f"  Successful:    {checked_count - len(errors)}")
    print(f"  Failures:      {len(errors)}")
    print(f"  Time taken:    {duration:.2f} seconds")
    print("-" * 40)
    
    if errors:
        print("\nFailures:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print("\nSUCCESS: Source and Destination match exactly.")

if __name__ == "__main__":
    main()
