import os
import sys
import shutil
import argparse
import multiprocessing
import time
import subprocess
from pathlib import Path

def process_batch(batch):
    """
    Worker function to process a batch of files.
    batch: list of (src_path, dest_path) tuples
    Returns: (list_of_results, total_bytes_copied)
    """
    results = []
    bytes_copied = 0
    
    for src, dst in batch:
        try:
            # Check if file exists and needs updating
            if os.path.exists(dst):
                # Simple check based on size and mtime
                src_stat = os.stat(src)
                dst_stat = os.stat(dst)
                if src_stat.st_size == dst_stat.st_size and src_stat.st_mtime == dst_stat.st_mtime:
                    results.append((src, True, "Skipped (up to date)"))
                    continue

            shutil.copy2(src, dst)
            copied_size = os.path.getsize(src)
            bytes_copied += copied_size
            results.append((src, True, "Copied"))
        except Exception as e:
            results.append((src, False, str(e)))
            
    return results, bytes_copied

def main():
    parser = argparse.ArgumentParser(description="Recursively copy a directory in parallel.")
    parser.add_argument("source", help="Source directory path")
    parser.add_argument("destination", help="Destination directory path")
    parser.add_argument("-j", "--jobs", type=int, default=multiprocessing.cpu_count(),
                        help=f"Number of parallel jobs (default: {multiprocessing.cpu_count()})")
    parser.add_argument("--chunksize", type=int, default=10,
                        help="Number of files per batch task (default: 10)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--verify", action="store_true", help="Verify copied files using rsync checksums")
    
    args = parser.parse_args()
    
    source_dir = Path(args.source).resolve()
    dest_dir = Path(args.destination).resolve()
    
    if not source_dir.exists():
        print(f"Error: Source directory '{source_dir}' does not exist.")
        sys.exit(1)
        
    if not source_dir.is_dir():
        print(f"Error: Source '{source_dir}' is not a directory.")
        sys.exit(1)

    # Create destination directory if it doesn't exist
    if not dest_dir.exists():
        try:
            os.makedirs(dest_dir)
        except OSError as e:
            print(f"Error creating destination directory '{dest_dir}': {e}")
            sys.exit(1)

    print(f"Scanning source directory: {source_dir} ...")
    
    tasks = []
    total_size = 0
    dirs_created = 0

    # Walk through source directory
    for root, dirs, files in os.walk(source_dir):
        rel_path = os.path.relpath(root, source_dir)
        dest_root = dest_dir / rel_path
        
        # Create directories in destination
        for d in dirs:
            dest_d = dest_root / d
            if not dest_d.exists():
                try:
                    os.makedirs(dest_d)
                    dirs_created += 1
                except OSError as e:
                    print(f"Error creating directory '{dest_d}': {e}")
        
        # Prepare file copy tasks
        for f in files:
            src_file = Path(root) / f
            dest_file = dest_root / f
            tasks.append((str(src_file), str(dest_file)))
            total_size += src_file.stat().st_size

    print(f"Found {len(tasks)} files ({total_size / (1024*1024):.2f} MB) to copy.")
    print(f"Created {dirs_created} new directories.")
    
    # Chunk the tasks
    chunked_tasks = [tasks[i:i + args.chunksize] for i in range(0, len(tasks), args.chunksize)]
    print(f"Split into {len(chunked_tasks)} batches (chunksize={args.chunksize}).")
    print(f"Starting copy with {args.jobs} processes...")
    
    start_time = time.time()
    
    success_count = 0
    skip_count = 0
    error_count = 0
    total_bytes_copied = 0
    files_processed = 0
    total_files = len(tasks)
    
    # Process pool
    with multiprocessing.Pool(processes=args.jobs) as pool:
        # Use imap_unordered for progress reporting
        for i, (batch_results, bytes_copied) in enumerate(pool.imap_unordered(process_batch, chunked_tasks), 1):
            total_bytes_copied += bytes_copied
            files_processed += len(batch_results)
            
            for src, success, msg in batch_results:
                if success:
                    if "Skipped" in msg:
                        skip_count += 1
                    else:
                        success_count += 1
                else:
                    error_count += 1
                    print(f"\nError copying {src}: {msg}")
                
                if args.verbose:
                    print(f"[{files_processed}/{total_files}] {src} -> {msg}")
            
            # Progress reporting
            elapsed = time.time() - start_time
            if elapsed > 0:
                rate = (total_bytes_copied / (1024 * 1024)) / elapsed
            else:
                rate = 0.0
                
            percent = (files_processed / total_files) * 100
            
            if not args.verbose:
                # Update progress bar
                bar_length = 30
                filled_length = int(bar_length * files_processed // total_files)
                bar = '=' * filled_length + '-' * (bar_length - filled_length)
                
                sys.stdout.write(f"\rProgress: [{bar}] {percent:.1f}% | {files_processed}/{total_files} | Rate: {rate:.2f} MB/s")
                sys.stdout.flush()

    print("\n")
    end_time = time.time()
    duration = end_time - start_time
    total_mb = total_bytes_copied / (1024 * 1024)
    avg_rate = total_mb / duration if duration > 0 else 0
    
    print("-" * 40)
    print(f"Summary:")
    print(f"  Total files scanned: {total_files}")
    print(f"  Copied: {success_count}")
    print(f"  Skipped: {skip_count}")
    print(f"  Errors: {error_count}")
    print(f"  Total data copied: {total_mb:.2f} MB")
    print(f"  Time taken: {duration:.2f} seconds")
    print(f"  Average Rate: {avg_rate:.2f} MB/s")
    print("-" * 40)
    
    if args.verify:
        print("\nStarting verification step using rsync...")
        src_str = str(source_dir)
        if not src_str.endswith(os.sep):
            src_str += os.sep
            
        dst_str = str(dest_dir)
        if not dst_str.endswith(os.sep):
            dst_str += os.sep
            
        cmd = ["rsync", "-rcn", "--out-format=%n", src_str, dst_str]
        
        try:
            print(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"Verification failed to run. Error: {result.stderr}")
                sys.exit(1)
            else:
                diffs = [line for line in result.stdout.splitlines() if line.strip()]
                if not diffs:
                    print("Verification successful: No differences found.")
                else:
                    print("Verification failed: The following files differ or are missing in destination:")
                    for d in diffs:
                        print(f"  {d}")
                    sys.exit(1)
                    
        except FileNotFoundError:
             print("Error: rsync command not found. Please ensure rsync is installed for verification.")
             sys.exit(1)
    
    if error_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
