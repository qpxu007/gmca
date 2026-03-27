#!/usr/bin/env python
"""
Integration adapter for the QP2 Pipeline Driver.

This module provides compatibility between the new unified pipeline driver
and the existing QP2 image viewer plugins and data processing systems.
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional, List

import redis

from qp2.pipelines.pipeline_driver import (
    PipelineDriver, PipelineConfig, DatasetSpec, 
    PipelineType, JobStatus
)
from qp2.log.logging_config import get_logger
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.xio.user_group_manager import UserGroupManager

logger = get_logger(__name__)


class PluginCompatibilityAdapter:
    """
    Adapter to make the new pipeline driver compatible with existing 
    image viewer plugins and data processing workflows.
    """
    
    def __init__(self, redis_conn: Optional[redis.Redis] = None):
        self.driver = PipelineDriver()
        self.redis_conn = redis_conn
    
    def run_autoproc_compatible(self, master_file: str, metadata: Dict[str, Any], 
                               redis_key_prefix: str, **kwargs) -> str:
        """
        Run AutoPROC pipeline compatible with existing autoproc plugin.
        
        Returns the job identifier for compatibility.
        """
        return self._run_pipeline_compatible(
            pipeline_type="autoproc",
            master_file=master_file,
            metadata=metadata,
            redis_key_prefix=redis_key_prefix,
            prefix="autoproc",
            **kwargs
        )
    
    def run_xia2_compatible(self, master_file: str, metadata: Dict[str, Any], 
                           redis_key_prefix: str, **kwargs) -> str:
        """
        Run Xia2 pipeline compatible with existing xia2 plugin.
        
        Returns the job identifier for compatibility.
        """
        return self._run_pipeline_compatible(
            pipeline_type="xia2",
            master_file=master_file,
            metadata=metadata,
            redis_key_prefix=redis_key_prefix,
            prefix="xia2",
            **kwargs
        )
    
    def run_nxds_compatible(self, master_file: str, metadata: Dict[str, Any], 
                           redis_key_prefix: str, **kwargs) -> str:
        """
        Run nXDS pipeline compatible with existing nxds plugin.
        
        Returns the job identifier for compatibility.
        """
        kwargs.setdefault("variant", "nxds")  # Default to nXDS variant
        return self._run_pipeline_compatible(
            pipeline_type="gmcaproc",
            master_file=master_file,
            metadata=metadata,
            redis_key_prefix=redis_key_prefix,
            prefix="nxds",
            **kwargs
        )
    
    def _run_pipeline_compatible(self, pipeline_type: str, master_file: str, 
                                metadata: Dict[str, Any], redis_key_prefix: str,
                                prefix: str, **kwargs) -> str:
        """
        Common pipeline execution logic compatible with existing plugins.
        """
        # Set up Redis tracking
        results_key = f"{redis_key_prefix}:{master_file}"
        status_key = f"{results_key}:status"
        
        # Check if already submitted
        if self.redis_conn:
            initial_status = {"status": "SUBMITTED", "timestamp": time.time()}
            if not self.redis_conn.set(status_key, json.dumps(initial_status), ex=7 * 24 * 3600, nx=True):
                logger.info(f"{prefix} job for {os.path.basename(master_file)} already submitted.")
                return results_key
        
        try:
            # Determine work directory
            work_dir = self._determine_work_dir(master_file, prefix, **kwargs)
            
            # Create dataset spec
            datasets = [DatasetSpec(master_file)]
            
            # Add extra data files if specified
            if "extra_data_files" in kwargs:
                for extra_file in kwargs["extra_data_files"]:
                    datasets.append(DatasetSpec(extra_file))
            
            # Build configuration from kwargs
            config = self._build_config_from_kwargs(
                work_dir, pipeline_type, metadata, **kwargs
            )
            
            # Set Redis tracking in config
            if self.redis_conn:
                config.pipeline_options = config.pipeline_options or {}
                config.pipeline_options.update({
                    "redis_host": self.redis_conn.connection_pool.connection_kwargs.get("host"),
                    "redis_port": self.redis_conn.connection_pool.connection_kwargs.get("port"),
                    "redis_key": results_key,
                    "status_key": status_key
                })
                
                # Store processing directory in Redis
                self.redis_conn.hset(results_key, "_proc_dir", str(work_dir))
                self.redis_conn.expire(results_key, 7 * 24 * 3600)  # 1-week expiration
            
            # Run pipeline asynchronously to maintain plugin compatibility
            from concurrent.futures import ThreadPoolExecutor
            
            def execute_pipeline():
                try:
                    # Update status to RUNNING
                    if self.redis_conn:
                        running_status = {"status": "RUNNING", "timestamp": time.time()}
                        self.redis_conn.set(status_key, json.dumps(running_status), ex=7 * 24 * 3600)
                    
                    # Execute pipeline
                    result = self.driver.run_pipeline(pipeline_type, datasets, config)
                    
                    # Update Redis with results
                    if self.redis_conn and result.success:
                        # Store result data
                        for key, value in result.results.items():
                            if value is not None:
                                self.redis_conn.hset(results_key, key, str(value))
                        # Ensure the results_key itself has an expiration set, as hset doesn't affect it
                        self.redis_conn.expire(results_key, 7 * 24 * 3600)  # 1-week expiration
                        
                        # Update status
                        completed_status = {"status": "COMPLETED", "timestamp": time.time()}
                        self.redis_conn.set(status_key, json.dumps(completed_status), ex=7 * 24 * 3600)
                    elif self.redis_conn:
                        # Store failure
                        failed_status = {
                            "status": "FAILED", 
                            "timestamp": time.time(),
                            "error": result.error_message
                        }
                        self.redis_conn.set(status_key, json.dumps(failed_status), ex=7 * 24 * 3600)
                        
                except Exception as e:
                    logger.error(f"Pipeline execution failed: {e}", exc_info=True)
                    if self.redis_conn:
                        failed_status = {
                            "status": "FAILED",
                            "timestamp": time.time(), 
                            "error": str(e)
                        }
                        self.redis_conn.set(status_key, json.dumps(failed_status), ex=7 * 24 * 3600)
            
            # Execute in background
            executor = ThreadPoolExecutor(max_workers=1)
            executor.submit(execute_pipeline)
            
            return results_key
            
        except Exception as e:
            logger.error(f"Failed to submit {prefix} job: {e}", exc_info=True)
            if self.redis_conn:
                failed_status = {
                    "status": "FAILED",
                    "timestamp": time.time(),
                    "error": str(e)
                }
                self.redis_conn.set(status_key, json.dumps(failed_status))
            raise
    
    def _determine_work_dir(self, master_file: str, prefix: str, **kwargs) -> str:
        """Determine work directory using plugin-compatible logic."""
        if kwargs.get("output_proc_dir"):
            return str(Path(kwargs["output_proc_dir"]))
        
        # Use plugin-style directory structure
        default_proc_root = os.path.join(os.path.expanduser("~"), f"{prefix}_runs")
        proc_dir_root_str = kwargs.get(f"{prefix}_proc_dir_root", default_proc_root)
        proc_dir_root = Path(proc_dir_root_str).expanduser().resolve()
        
        master_basename = os.path.splitext(os.path.basename(master_file))[0]
        work_dir = proc_dir_root / master_basename
        
        work_dir.mkdir(parents=True, exist_ok=True)
        return str(work_dir)
    
    def _build_config_from_kwargs(self, work_dir: str, pipeline_type: str,
                                 metadata: Dict[str, Any], **kwargs) -> PipelineConfig:
        """Build PipelineConfig from plugin-style kwargs."""
        # Extract common parameters
        prefix = {
            "autoproc": "autoproc",
            "xia2": "xia2", 
            "gmcaproc": "nxds"
        }.get(pipeline_type, pipeline_type)
        
        # Extract processing parameters
        config_params = {
            "work_dir": work_dir,
            "runner": "slurm",  # Plugin compatibility
            "nproc": kwargs.get(f"{prefix}_nproc", 8),
            "njobs": kwargs.get(f"{prefix}_njobs", 1 if prefix == "nxds" else 4),
            "highres": kwargs.get(f"{prefix}_highres"),
            "space_group": kwargs.get(f"{prefix}_space_group"),
            "unit_cell": kwargs.get(f"{prefix}_unit_cell"),
            "model": kwargs.get(f"{prefix}_model"),
            "fast_mode": kwargs.get(f"{prefix}_fast", False),
            "sample_name": self._extract_sample_name(master_file),
            "beamline": get_beamline_from_hostname(),
        }
        
        # Extract metadata
        group_name = metadata.get("primary_group") or metadata.get("username")
        if group_name:
            config_params["primary_group"] = group_name
        
        if metadata.get("pi_badge"):
            config_params["pi_id"] = metadata["pi_badge"]
            
        if metadata.get("esaf_id"):
            config_params["esaf_id"] = metadata["esaf_id"]
        
        # Pipeline-specific options
        pipeline_options = {}
        
        if pipeline_type == "xia2":
            xia2_pipeline = kwargs.get("xia2_pipeline", "xia2_dials")
            if "dials" in xia2_pipeline:
                pipeline_options["pipeline_type"] = "dials" if "aimless" not in xia2_pipeline else "dials-aimless"
            else:
                pipeline_options["pipeline_type"] = "xds"
        
        elif pipeline_type == "gmcaproc":
            pipeline_options["variant"] = kwargs.get("variant", "nxds")
            if kwargs.get(f"{prefix}_powder"):
                pipeline_options["powder"] = True
            if kwargs.get(f"{prefix}_reference_hkl"):
                pipeline_options["reference_dataset"] = kwargs[f"{prefix}_reference_hkl"]
        
        config_params["pipeline_options"] = pipeline_options
        
        # Remove None values
        config_params = {k: v for k, v in config_params.items() if v is not None}
        
        return PipelineConfig(**config_params)
    
    def _extract_sample_name(self, master_file: str) -> str:
        """Extract sample name from master file path."""
        basename = os.path.basename(master_file)
        return (basename.replace("_master.h5", "")
                        .replace(".h5", "")
                        .replace(".cbf", ""))


def create_legacy_process_dataset_script(pipeline_type: str) -> str:
    """
    Generate a legacy-compatible process_dataset.py script that uses the new driver.
    
    This allows existing plugins to work without modification.
    """
    
    script_template = f'''#!/usr/bin/env python
"""
Legacy compatibility wrapper for {pipeline_type} processing.

This script provides backward compatibility with existing image viewer plugins
while using the new unified pipeline driver underneath.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import redis

# Add project root to path
def find_project_root(file_path):
    path = Path(file_path).resolve()
    for parent in path.parents:
        if (parent / "qp2").is_dir():
            return str(parent)
    return None

project_root = find_project_root(__file__)
if project_root and project_root not in sys.path:
    sys.path.insert(0, project_root)

from qp2.pipelines.pipeline_driver import (
    PipelineDriver, PipelineConfig, DatasetSpec
)
from qp2.log.logging_config import setup_logging, get_logger

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run {pipeline_type} processing via unified driver")
    
    # Core arguments that existing plugins expect
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--data", required=True, action="append")
    parser.add_argument("--work_dir", required=True)
    parser.add_argument("--status_key", required=True)
    parser.add_argument("--redis_host", required=True)
    parser.add_argument("--redis_port", required=True)
    
    # Processing parameters
    parser.add_argument("--highres", type=float)
    parser.add_argument("--space_group", type=str)
    parser.add_argument("--unit_cell", type=str)
    parser.add_argument("--model", type=str)
    parser.add_argument("--nproc", type=int, default=8)
    parser.add_argument("--njobs", type=int, default=1)
    parser.add_argument("--fast", action="store_true")
    
    # Metadata for database logging
    parser.add_argument("--group_name", type=str)
    parser.add_argument("--pi_badge", type=int)
    parser.add_argument("--esaf_number", type=int)
    parser.add_argument("--beamline", type=str)
    
    # Pipeline-specific arguments
    {"" if pipeline_type != "nxds" else '''
    parser.add_argument("--master_file", type=str)  # nXDS compatibility
    parser.add_argument("--proc_dir", type=str)     # nXDS compatibility
    parser.add_argument("--powder", action="store_true")
    parser.add_argument("--reference_hkl", type=str)
    '''}
    
    args = parser.parse_args()
    
    setup_logging(root_name="qp2.{pipeline_type}_legacy", log_level="INFO")
    redis_conn = redis.Redis(
        host=args.redis_host, port=args.redis_port, decode_responses=True
    )
    
    try:
        # Update status to RUNNING
        running_status = {{"status": "RUNNING", "timestamp": time.time()}}
        redis_conn.set(args.status_key, json.dumps(running_status), ex=7 * 24 * 3600)
        
        # Build dataset specs
        datasets = []
        data_files = args.data if hasattr(args, 'data') else [args.master_file]
        for data_file in data_files:
            datasets.append(DatasetSpec(data_file))
        
        # Build configuration
        pipeline_options = {{}}
        {"" if pipeline_type != "gmcaproc" else '''
        pipeline_options["variant"] = "nxds"  # Default to nXDS for compatibility
        if args.powder:
            pipeline_options["powder"] = True
        if args.reference_hkl:
            pipeline_options["reference_dataset"] = args.reference_hkl
        '''}
        
        config = PipelineConfig(
            work_dir=args.work_dir,
            runner="shell",  # Already on cluster node
            nproc=args.nproc,
            njobs=args.njobs,
            highres=args.highres,
            space_group=args.space_group,
            unit_cell=args.unit_cell,
            model=args.model,
            fast_mode=args.fast,
            primary_group=args.group_name,
            pi_id=args.pi_badge,
            esaf_id=args.esaf_number,
            beamline=args.beamline,
            pipeline_options=pipeline_options
        )
        
        # Run pipeline using new driver
        driver = PipelineDriver()
        result = driver.run_pipeline("{pipeline_type}", datasets, config)
        
        if result.success:
            # Store results in Redis for plugin compatibility
            results_key = args.status_key.replace(":status", "")
            for key, value in result.results.items():
                if value is not None:
                    redis_conn.hset(results_key, key, str(value))
            redis_conn.expire(results_key, 7 * 24 * 3600)

            # Update status to COMPLETED
            completed_status = {{"status": "COMPLETED", "timestamp": time.time()}}
            redis_conn.set(args.status_key, json.dumps(completed_status), ex=7 * 24 * 3600)
            
            logger.info(f"{pipeline_type} processing completed successfully")
        else:
            raise RuntimeError(f"Pipeline failed: {{result.error_message}}")
            
    except Exception as e:
        logger.error(f"{pipeline_type} processing failed: {{e}}", exc_info=True)
        failed_status = {{
            "status": "FAILED",
            "timestamp": time.time(),
            "error": str(e)
        }}
        redis_conn.set(args.status_key, json.dumps(failed_status), ex=7 * 24 * 3600)
        sys.exit(1)


if __name__ == "__main__":
    main()
'''
    
    return script_template


def update_plugin_integration():
    """
    Update existing plugin scripts to use the new pipeline driver.
    
    This creates backward-compatible wrappers that can be dropped into
    existing plugin directories.
    """
    
    plugin_scripts = {
        "autoproc": "qp2/image_viewer/plugins/autoproc/autoproc_process_dataset_unified.py",
        "xia2": "qp2/image_viewer/plugins/xia2/xia2_process_dataset_unified.py", 
        "gmcaproc": "qp2/image_viewer/plugins/nxds/nxds_process_dataset_unified.py"
    }
    
    for pipeline_type, script_path in plugin_scripts.items():
        script_content = create_legacy_process_dataset_script(pipeline_type)
        
        try:
            with open(script_path, 'w') as f:
                f.write(script_content)
            
            # Make script executable
            os.chmod(script_path, 0o755)
            
            logger.info(f"Created unified {pipeline_type} wrapper: {script_path}")
            
        except Exception as e:
            logger.error(f"Failed to create {script_path}: {e}")


if __name__ == "__main__":
    # Update plugin integration when run directly
    update_plugin_integration()
    print("Plugin integration updated. Existing plugins can now use unified pipeline driver.")