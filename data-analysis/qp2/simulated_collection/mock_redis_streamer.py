# simulated_collection/mock_redis_streamer.py
import argparse
import json
import sys
import time
import uuid
from pathlib import Path
from typing import List, Dict, Any, Optional
import threading

import redis

# Add project root to path to allow finding qp2 modules
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from qp2.xio.hdf5_manager import HDF5Reader
    from qp2.log.logging_config import setup_logging, get_logger
except ImportError as e:
    print(f"CRITICAL: Failed to import qp2 modules. project_root={project_root}. Error: {e}")
    sys.exit(1)

logger = get_logger("mock_streamer")

class MockStreamer:
    def __init__(self, redis_host: str, redis_port: int, stream_name: str, rate_hz: float, 
                 loop: bool = False, override_mode: Optional[str] = None,
                 artificial_lag: float = 0.0, lag_frames: int = 100,
                 file_arrival_delay: float = 0.0):
        self.redis_conn = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.stream_name = stream_name
        self.rate_hz = rate_hz
        self.frame_interval_s = 1.0 / rate_hz
        self.loop = loop
        self.override_mode = override_mode
        self.artificial_lag = artificial_lag
        self.lag_frames = lag_frames
        self.file_arrival_delay = file_arrival_delay
        self.staged_directories = []
        self.cancel_event = threading.Event()
        self.copy_threads = []
        
    def cleanup(self, keep_data=False):
        """Cancel background threads and delete all staging directories."""
        self.cancel_event.set()
        
        # Wait for any active copy threads to finish cancelling or executing
        for t in self.copy_threads:
            if t.is_alive():
                t.join(timeout=self.file_arrival_delay + 1.0)
                
        if keep_data:
            logger.info("  [Cleanup] --keep-data is set. Preserving staging directories.")
            return
            
        import shutil
        for t_dir in self.staged_directories:
            if t_dir and t_dir.exists():
                try:
                    shutil.rmtree(t_dir)
                    logger.info(f"  [Cleanup] Removed mock staging directory {t_dir}")
                except Exception as e:
                    logger.error(f"Failed to remove {t_dir}: {e}")
        self.staged_directories.clear()
        
    def create_header_message(self, params: Dict[str, Any], series_id: int) -> Dict[str, Any]:
        """Mimics the dheader-1.0 message."""
        return {
            "0": {
                "header_detail": "all",
                "htype": "dheader-1.0",
                "series": series_id
            },
            "1": {
                "nimages": params.get('nimages', 1),
                "ntrigger": 1,
                "wavelength": params.get('wavelength', 1.0),
                "detector_distance": params.get('det_dist', 100.0) / 1000.0,
                "beam_center_x": params.get('beam_x', 512.0),
                "beam_center_y": params.get('beam_y', 512.0),
                "x_pixel_size": params.get('pixel_size', 0.075) / 1000.0,
                "y_pixel_size": params.get('pixel_size', 0.075) / 1000.0,
                "frame_time": params.get('exposure', 0.2),
                "count_time": params.get('exposure', 0.2),
                "description": params.get('detector', 'Mock Detector'),
                "x_pixels_in_detector": params.get('nx', 4150),
                "y_pixels_in_detector": params.get('ny', 4371),
            },
            "timestamp": time.time()
        }

    def create_image_message(self, params: Dict[str, Any], frame_idx: int, series_id: int, 
                             run_prefix: str, run_fr_start: int, total_run_frames: int) -> Dict[str, Any]:
        """Mimics the dimage-1.0 message."""
        master_path = Path(params.get('master_file', ''))
        abs_master = master_path.resolve()
        user_dir_path = abs_master.parent
        data_dir_path = user_dir_path.parent
        
        data_dir = str(data_dir_path)
        user_dir = str(user_dir_path.relative_to(data_dir_path))
        prefix = abs_master.name.replace("_master.h5", "")
        
        collect_mode = self.override_mode if self.override_mode else params.get('collect_mode', 'RASTER')

        return {
            "0": {
                "frame": frame_idx,
                "hash": uuid.uuid4().hex[:32],
                "htype": "dimage-1.0",
                "series": series_id
            },
            "4": {
                "username": "mock_user",
                "collect_mode": collect_mode,
                "run_fr_start": run_fr_start,
                "series_fr_count": params.get('nimages', 1),
                "run_fr_count": total_run_frames,
                "exposure_sec": params.get('exposure', 0.2),
                "data_dir": data_dir,
                "user_dir": user_dir,
                "prefix": prefix,
                "run_prefix": run_prefix,
                "images_per_hdf": params.get('images_per_hdf', 100),
                "energy_eV": params.get('energy_ev', 12000),
                "detector_dist_m": params.get('det_dist', 100.0) / 1000.0,
                "xbeam_px": params.get('beam_x', 512.0),
                "ybeam_px": params.get('beam_y', 512.0),
            },
            "timestamp": time.time()
        }

    def create_end_message(self, series_id: int) -> Dict[str, Any]:
        """Mimics the dseries_end-1.0 message."""
        return {
            "0": {
                "htype": "dseries_end-1.0",
                "series": series_id
            },
            "timestamp": time.time()
        }

    def stream_dataset(self, master_path: Path, run_prefix: str, 
                       run_fr_start: int, total_run_frames: int):
        logger.info("-" * 60)
        logger.info(f"Preparing to stream: {master_path} (Run Start: {run_fr_start})")
        try:
            reader = HDF5Reader(str(master_path), start_timer=False)
            params = reader.get_parameters()
            total_frames = params.get('nimages', 0)
            series_id = int(uuid.uuid4().int & (1 << 31) - 1)
            reader.close()  # Close early to free file handle before potential rename
            
            # --- FILE ARRIVAL DELAY SIMULATION ---
            copied_files = []
            frames_per_file = params.get('nimages_per_file')
            parent_dir = master_path.parent
            prefix_name = master_path.name.replace("_master.h5", "")
            target_dir = None
            
            if self.file_arrival_delay > 0:
                import shutil
                target_dir = Path(f"/tmp/mock_streaming/{series_id}")
                target_dir.mkdir(parents=True, exist_ok=True)
                self.staged_directories.append(target_dir)
                
                # Copy master file
                new_master = target_dir / master_path.name
                shutil.copy2(master_path, new_master)
                params["master_file"] = str(new_master)
                
                all_data_files = sorted(list(parent_dir.glob(f"{prefix_name}_data_*.h5")))
                
                if all_data_files:
                    # Immediately copy _data_000001.h5 if it exists so RedisManager passes validation
                    first_data = parent_dir / f"{prefix_name}_data_000001.h5"
                    if first_data.exists():
                        shutil.copy2(first_data, target_dir / first_data.name)
                
                    data_files = [df for df in all_data_files if not df.name.endswith("_data_000001.h5")]
                    
                    if data_files:
                        if not frames_per_file:
                            import math
                            frames_per_file = math.ceil(total_frames / len(all_data_files)) if all_data_files else 100
                            
                        logger.info(f"Simulating {self.file_arrival_delay}s delay targeting {target_dir}. Staging {len(data_files)} subsequent data files (assumed {frames_per_file} frames/file)...")
                        for df in data_files:
                            copied_files.append((df, target_dir / df.name))
            
            # -------------------------------------
            
            # 1. Send Header
            header = self.create_header_message(params, series_id)
            self.redis_conn.xadd(self.stream_name, {"message": json.dumps(header)})
            logger.info(f"Series {series_id} started (Header sent)")
            
            # 2. Send Images
            start_time = time.time()
            for i in range(total_frames):
                img_msg = self.create_image_message(params, i, series_id, run_prefix, run_fr_start, total_run_frames)
                self.redis_conn.xadd(self.stream_name, {"message": json.dumps(img_msg)})
                
                elapsed = time.time() - start_time
                target_time = (i + 1) * self.frame_interval_s
                if target_time > elapsed:
                    time.sleep(target_time - elapsed)
                
                # Introduce artificial lag if requested
                if self.artificial_lag > 0 and (i + 1) % self.lag_frames == 0:
                    logger.info(f"  --- Injecting {self.artificial_lag}s artificial lag ---")
                    time.sleep(self.artificial_lag)
                
                # Staggered File Arrival
                if copied_files and frames_per_file:
                    if (i + 1) % frames_per_file == 0 or (i + 1) == total_frames:
                        file_idx = i // frames_per_file
                        # File index 0 is _data_000001, which is already copied
                        hidden_idx = file_idx - 1 
                        if 0 <= hidden_idx < len(copied_files):
                            src_path, dst_path = copied_files[hidden_idx]
                            
                            def copy_file(source, dest):
                                # Wait for delay; if event is set (cancelled), exit immediately
                                if self.cancel_event.wait(self.file_arrival_delay):
                                    return
                                try:
                                    import shutil
                                    tmp_dest = dest.with_name(f".tmp_{dest.name}")
                                    shutil.copy2(source, tmp_dest)
                                    tmp_dest.rename(dest)
                                    logger.info(f"  [File Arrived] Copied {dest.name}")
                                except Exception as e:
                                    logger.error(f"Failed to copy {source} to {dest}: {e}")
                                    
                            t = threading.Thread(target=copy_file, args=(src_path, dst_path), daemon=True)
                            self.copy_threads.append(t)
                            t.start()
                
                if (i + 1) % 100 == 0 or (i + 1) == total_frames:
                    logger.info(f"  Published frame {i+1}/{total_frames}")

            # 3. Send End
            end_msg = self.create_end_message(series_id)
            self.redis_conn.xadd(self.stream_name, {"message": json.dumps(end_msg)})
            logger.info(f"Series {series_id} completed (End sent).")
            
            return total_frames
        except Exception as e:
            logger.error(f"Error streaming {master_path}: {e}", exc_info=True)
            return 0

    def run(self, master_files: List[Path]):
        """Finds all datasets and streams them as a single run."""
        try:
            self.redis_conn.ping()
        except redis.RedisError as e:
            logger.error(f"Could not connect to Redis: {e}")
            return

        while True:
            # 1. Pre-calculate run parameters
            total_run_frames = 0
            
            file_params = []
            for mf in master_files:
                try:
                    reader = HDF5Reader(str(mf), start_timer=False)
                    p = reader.get_parameters()
                    n = p.get('nimages', 0)
                    total_run_frames += n
                    
                    prefix = mf.name.replace("_master.h5", "")
                    
                    # Compute an individual run_prefix per file to prevent falsely grouping unrelated datasets
                    import re
                    file_run_prefix = prefix
                    if re.match(r"(.*)_run(\d{1,2})", prefix):
                        match = re.match(r"(.*)_run(\d{1,2})", prefix)
                        file_run_prefix = f"{match.group(1)}_run{match.group(2)}"
                    elif "_scr_" in prefix:
                        file_run_prefix = prefix.split("_scr_")[0] + "_scr"
                    else:
                        match = re.match(r"(.+)_(\d+)$", prefix)
                        if match:
                            file_run_prefix = match.group(1)
                    
                    file_params.append((mf, n, file_run_prefix))
                    reader.close()
                except Exception as e:
                    logger.error(f"Error reading {mf}: {e}")

            logger.info(f"Streaming {len(master_files)} series, {total_run_frames} total frames.")

            # 2. Stream each series
            current_fr_start = 0
            for mf, nimages, file_run_prefix in file_params:
                streamed = self.stream_dataset(mf, file_run_prefix, current_fr_start, total_run_frames)
                current_fr_start += streamed
                logger.info("Pausing 3 seconds before next series...")
                time.sleep(3)
            
            if not self.loop:
                break
        logger.info("Mock streamer finished.")

def main():
    parser = argparse.ArgumentParser(description="Mock Redis Stream Service for Eiger Detector Simulation")
    parser.add_argument("paths", nargs="+", help="Master H5 files or directories containing them.")
    parser.add_argument("--rate", type=float, default=100.0, help="Playback rate in Hz (default 100).")
    parser.add_argument("--loop", action="store_true", help="Loop the datasets infinitely.")
    parser.add_argument("--reset", action="store_true", help="Delete the stream before starting.")
    parser.add_argument("--stream", default="eiger", help="Redis stream name.")
    parser.add_argument("--host", default="127.0.0.1", help="Redis host.")
    parser.add_argument("--port", type=int, default=6379, help="Redis port.")
    parser.add_argument("--mode", help="Override collect_mode (e.g., STANDARD, VECTOR, RASTER, SITE).")
    parser.add_argument("--artificial-lag", type=float, default=0.0, help="Seconds to sleep periodically to simulate network/processing lag.")
    parser.add_argument("--lag-frames", type=int, default=100, help="Inject artificial lag every N frames.")
    parser.add_argument("--file-arrival-delay", type=float, default=0.0, help="Seconds to delay the physical appearance of HDF5 data files.")
    parser.add_argument("--keep-data", action="store_true", help="Do not delete staged mock data on exit.")
    
    args = parser.parse_args()
    
    setup_logging(root_name="qp2", log_level="INFO")
    
    master_files = []
    for p in args.paths:
        path = Path(p).resolve()
        if path.is_dir():
            found = sorted(path.rglob("*_master.h5"))
            logger.info(f"Scanning directory {path}... Found {len(found)} master files.")
            master_files.extend(found)
        elif path.is_file() and path.name.endswith("_master.h5"):
            master_files.append(path)
            
    if not master_files:
        logger.error("No valid '*_master.h5' files found at the specified paths.")
        sys.exit(1)
        
    streamer = MockStreamer(args.host, args.port, args.stream, args.rate, args.loop, 
                            override_mode=args.mode,
                            artificial_lag=args.artificial_lag,
                            lag_frames=args.lag_frames,
                            file_arrival_delay=args.file_arrival_delay)
    
    if args.reset:
        logger.info(f"Resetting stream '{args.stream}'...")
        streamer.redis_conn.delete(args.stream)
        
    try:
        streamer.run(master_files)
        if not args.loop:
            logger.info("Streaming complete. Press Ctrl+C to exit and clean up data.")
            import time
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Streamer stopped by user.")
    finally:
        streamer.cleanup(keep_data=args.keep_data)

if __name__ == "__main__":
    main()
