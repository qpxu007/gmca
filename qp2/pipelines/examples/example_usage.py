#!/usr/bin/env python
"""
Example usage of the QP2 Pipeline Driver.

This script demonstrates how to use the pipeline driver programmatically
for different crystallographic data processing tasks.
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from qp2.pipelines.pipeline_driver import (
    PipelineDriver, PipelineConfig, DatasetSpec, 
    PipelineType, JobStatus
)


def example_autoproc_single_dataset():
    """Example: Run AutoPROC on a single dataset."""
    print("Example: AutoPROC single dataset processing")
    
    # Define dataset
    datasets = [DatasetSpec("/path/to/your/data_master.h5")]
    
    # Configure pipeline
    config = PipelineConfig(
        work_dir="./autoproc_example",
        runner="slurm",  # or "shell" for local execution
        nproc=8,
        njobs=4,
        highres=2.0,
        lowres=50.0,
        sample_name="example_protein",
        username="researcher",
        fast_mode=True,
        wavelength=0.9795,
        detector_distance=300.0
    )
    
    # Run pipeline
    driver = PipelineDriver()
    result = driver.run_pipeline(PipelineType.AUTOPROC, datasets, config)
    
    # Check results
    if result.success:
        print(f"✓ AutoPROC completed successfully in {result.elapsed_time:.1f}s")
        print(f"  Results: {result.results}")
        print(f"  Database ID: {result.pipeline_status_id}")
    else:
        print(f"✗ AutoPROC failed: {result.error_message}")
    
    return result


def example_autoproc_molecular_replacement():
    """Example: Run AutoPROC with molecular replacement."""
    print("\nExample: AutoPROC with molecular replacement")
    
    # Define dataset
    datasets = [DatasetSpec("/path/to/your/data_master.h5")]
    
    # Configure pipeline with molecular replacement
    config = PipelineConfig(
        work_dir="./autoproc_mr_example",
        runner="slurm",
        nproc=8,
        njobs=2,
        highres=2.5,
        model="/path/to/search_model.pdb",  # PDB model for MR
        sample_name="mr_protein",
        username="researcher",
        anomalous=True,  # Process anomalous data
        beam_center=[1024.5, 1024.5],  # Override beam center
        wavelength=1.5418  # Cu Ka wavelength for anomalous
    )
    
    # Run pipeline
    driver = PipelineDriver()
    result = driver.run_pipeline(PipelineType.AUTOPROC, datasets, config)
    
    # Check results
    if result.success:
        print(f"✓ AutoPROC MR completed successfully in {result.elapsed_time:.1f}s")
        print(f"  Molecular replacement: {result.results.get('solve')}")
        print(f"  Anomalous signal: {result.results.get('anom_completeness')}")
    else:
        print(f"✗ AutoPROC MR failed: {result.error_message}")
    
    return result


def example_xia2_multi_dataset():
    """Example: Run Xia2 on multiple datasets with frame ranges."""
    print("\nExample: Xia2 multi-dataset processing")
    
    # Define multiple datasets with frame ranges
    datasets = [
        DatasetSpec("/path/to/sweep1_master.h5", [1, 90]),
        DatasetSpec("/path/to/sweep2_master.h5", [1, 180]),
    ]
    
    # Configure pipeline
    config = PipelineConfig(
        work_dir="./xia2_example",
        runner="slurm",
        nproc=4,
        njobs=2,
        space_group="P21212",
        unit_cell="78.9 95.2 114.6 90 90 90",
        sample_name="multi_sweep_crystal",
        pipeline_options={
            "pipeline_type": "dials-aimless",  # Use DIALS with Aimless scaling
            "project": "my_project"
        }
    )
    
    driver = PipelineDriver()
    result = driver.run_pipeline(PipelineType.XIA2, datasets, config)
    
    if result.success:
        print(f"✓ Xia2 completed successfully in {result.elapsed_time:.1f}s")
        print(f"  Final MTZ: {result.results.get('truncate_mtz')}")
        print(f"  Space group: {result.results.get('spacegroup')}")
        print(f"  Resolution: {result.results.get('highresolution')}")
    else:
        print(f"✗ Xia2 failed: {result.error_message}")
    
    return result


def example_strategy_calculation():
    """Example: Run strategy calculation with MOSFLM."""
    print("\nExample: Strategy calculation")
    
    datasets = [DatasetSpec("/path/to/test_master.h5", [1])]  # Single frame for strategy
    
    config = PipelineConfig(
        work_dir="./strategy_example",
        runner="shell",  # Strategy calculations are usually quick
        sample_name="strategy_test",
        pipeline_options={
            "program": "mosflm",  # or "xds"
            "molsize": 300  # Estimated protein size in residues
        }
    )
    
    driver = PipelineDriver()
    result = driver.run_pipeline(PipelineType.STRATEGY, datasets, config)
    
    if result.success:
        print(f"✓ Strategy completed successfully in {result.elapsed_time:.1f}s")
        print(f"  Recommended rotation: {result.results.get('osc_start')}-{result.results.get('osc_end')}°")
        print(f"  Oscillation width: {result.results.get('osc_delta')}°")
        print(f"  Expected completeness: {result.results.get('completeness_native')}%")
    else:
        print(f"✗ Strategy failed: {result.error_message}")
    
    return result


def example_gmca_nxds():
    """Example: Run GMCA nXDS processing."""
    print("\nExample: GMCA nXDS processing")
    
    datasets = [DatasetSpec("/path/to/data_master.h5")]
    
    config = PipelineConfig(
        work_dir="./gmca_example",
        runner="slurm",
        nproc=16,
        njobs=2,
        space_group="P212121",
        pipeline_options={
            "reference_dataset": "/path/to/reference.hkl",
            "powder": True  # Enable ice ring detection
        }
    )
    
    driver = PipelineDriver()
    result = driver.run_pipeline(PipelineType.GMCAPROC, datasets, config)
    
    if result.success:
        print(f"✓ GMCA nXDS completed successfully in {result.elapsed_time:.1f}s")
        print(f"  HKL file: {result.results.get('hkl_file')}")
        print(f"  Dataset directory: {result.results.get('dataset_dir')}")
    else:
        print(f"✗ GMCA nXDS failed: {result.error_message}")
    
    return result


def example_parallel_processing():
    """Example: Run multiple pipelines in parallel."""
    print("\nExample: Parallel pipeline execution")
    
    # Define multiple pipeline configurations
    pipeline_configs = [
        {
            "pipeline_type": "autoproc",
            "datasets": ["/path/to/data1_master.h5"],
            "work_dir": "./parallel_autoproc",
            "nproc": 4,
            "sample_name": "sample1"
        },
        {
            "pipeline_type": "xia2", 
            "datasets": ["/path/to/data2_master.h5"],
            "work_dir": "./parallel_xia2",
            "nproc": 4,
            "sample_name": "sample2",
            "pipeline_options": {"pipeline_type": "dials"}
        },
        {
            "pipeline_type": "strategy",
            "datasets": ["/path/to/data3_master.h5"],
            "work_dir": "./parallel_strategy",
            "sample_name": "sample3",
            "pipeline_options": {"program": "xds"}
        }
    ]
    
    driver = PipelineDriver()
    results = driver.run_multiple_pipelines(pipeline_configs, parallel=True)
    
    print(f"Completed {len(results)} pipelines:")
    for i, result in enumerate(results):
        status = "✓" if result.success else "✗"
        print(f"  {status} {result.pipeline_type}: {result.elapsed_time:.1f}s")
    
    # Get summary statistics
    summary = driver.get_results_summary()
    print(f"\nSummary: {summary['successful_runs']}/{summary['total_runs']} successful "
          f"({summary['success_rate']:.1%} success rate)")
    
    return results


def example_with_database_tracking():
    """Example: Pipeline execution with full database tracking."""
    print("\nExample: Pipeline with database tracking")
    
    datasets = [DatasetSpec("/path/to/experiment_master.h5")]
    
    config = PipelineConfig(
        work_dir="./tracked_example",
        runner="slurm",
        nproc=8,
        sample_name="tracked_crystal",
        username="researcher",
        beamline="23ID-B",
        esaf_id=12345,
        pi_id=67890,
        primary_group="structural_biology",
        highres=1.8
    )
    
    driver = PipelineDriver()
    result = driver.run_pipeline(PipelineType.AUTOPROC, datasets, config)
    
    if result.success:
        print(f"✓ Processing completed with full tracking")
        print(f"  Pipeline status ID: {result.pipeline_status_id}")
        print(f"  Result record ID: {result.result_pk_value}")
        print(f"  Summary file: {result.output_files}")
    else:
        print(f"✗ Processing failed: {result.error_message}")
    
    return result


def main():
    """Run all examples (with dummy data paths)."""
    print("QP2 Pipeline Driver - Usage Examples")
    print("=" * 50)
    
    # Note: These examples use dummy file paths
    # In real usage, replace with actual data file paths
    
    try:
        # Individual pipeline examples
        example_autoproc_single_dataset()
        example_autoproc_molecular_replacement()
        example_xia2_multi_dataset() 
        example_strategy_calculation()
        example_gmca_nxds()
        
        # Advanced examples
        example_parallel_processing()
        example_with_database_tracking()
        
    except Exception as e:
        print(f"\nNote: Examples use dummy data paths and will fail with actual execution.")
        print(f"Replace file paths with real data to run pipelines.")
        print(f"Error encountered: {e}")


if __name__ == "__main__":
    main()