# QP2 Pipeline Driver

A unified, standardized interface for running crystallographic data processing pipelines in QP2.

## Overview

The Pipeline Driver provides a consistent way to execute different crystallographic processing pipelines with:

- **Standardized Input/Output**: Common interface for all pipeline types
- **Database Integration**: Automatic tracking via PipelineTracker 
- **Job Management**: Support for both SLURM and local execution
- **Result Parsing**: Unified result extraction and formatting
- **Error Handling**: Comprehensive logging and failure recovery

## Supported Pipelines

| Pipeline | Description | File Types | Key Features |
|----------|-------------|------------|--------------|
| **autoproc** | AutoPROC data processing | HDF5 | XDS-based, automatic resolution cutoff |
| **xia2** | Xia2 processing pipeline | HDF5 | DIALS/XDS options, multi-sweep support |
| **gmcaproc** | GMCA XDS/nXDS processing | HDF5, CBF | Traditional XDS or optimized nXDS for serial crystallography |
| **strategy** | Data collection strategy | HDF5, CBF | MOSFLM/XDS strategy calculations |

## Quick Start

### Command Line Usage

```bash
# Basic AutoPROC run
python -m qp2.pipelines.pipeline_driver autoproc \
  --data /path/to/data_master.h5 \
  --work_dir ./autoproc_run \
  --sample_name my_protein

# AutoPROC with molecular replacement
python -m qp2.pipelines.pipeline_driver autoproc \
  --data /path/to/data_master.h5 \
  --work_dir ./autoproc_mr \
  --model /path/to/search_model.pdb \
  --highres 2.0 \
  --anomalous

# Xia2 with frame range, resolution limits and beam center
python -m qp2.pipelines.pipeline_driver xia2 \
  --data /path/to/data_master.h5:1:100 \
  --work_dir ./xia2_run \
  --fast \
  --highres 1.8 \
  --lowres 50.0 \
  --beam_center 1024 1024 \
  --pipeline_type_variant dials

# Xia2 with anomalous data processing
python -m qp2.pipelines.pipeline_driver xia2 \
  --data /path/to/native_master.h5 \
  --work_dir ./anomalous_run \
  --anomalous \
  --no_friedel_pairs \
  --wavelength 1.5418

# Strategy calculation with MOSFLM
python -m qp2.pipelines.pipeline_driver strategy \
  --data /path/to/test_master.h5 \
  --work_dir ./strategy_run \
  --program mosflm \
  --molsize 300

# GMCA nXDS for serial crystallography
python -m qp2.pipelines.pipeline_driver gmcaproc \
  --data /path/to/data_master.h5 \
  --work_dir ./nxds_run \
  --variant nxds \
  --scaling_reference /path/to/reference.hkl \
  --powder

# GMCA XDS (traditional) processing
python -m qp2.pipelines.pipeline_driver gmcaproc \
  --data /path/to/data_master.h5 \
  --work_dir ./xds_run \
  --variant xds \
  --space_group P212121 \
  --highres 2.5 \
  --detector_distance 300.0
```

### Programmatic Usage

```python
from qp2.pipelines.pipeline_driver import (
    PipelineDriver, PipelineConfig, DatasetSpec, PipelineType
)

# Configure pipeline
datasets = [DatasetSpec("/path/to/data_master.h5")]
config = PipelineConfig(
    work_dir="./processing",
    runner="slurm",
    nproc=8,
    sample_name="my_protein",
    highres=2.0
)

# Run pipeline
driver = PipelineDriver()
result = driver.run_pipeline(PipelineType.AUTOPROC, datasets, config)

if result.success:
    print(f"Processing completed! Results: {result.results}")
else:
    print(f"Processing failed: {result.error_message}")
```

## Configuration Options

### Core Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `work_dir` | Working directory | Required | `./processing` |
| `runner` | Execution method | `slurm` | `slurm`, `shell` |
| `nproc` | Processors per job | `8` | `16` |
| `njobs` | Parallel jobs | `1` | `4` |

### Processing Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `highres` | High resolution limit (Å) | `2.0` |
| `lowres` | Low resolution limit (Å) | `50.0` |
| `space_group` | Space group | `P212121` |
| `unit_cell` | Unit cell params | `"78.9 95.2 114.6 90 90 90"` |
| `fast_mode` | Enable fast processing | `True` |
| `model` | PDB model for MR | `"/path/to/model.pdb"` |
| `sequence` | FASTA sequence file | `"/path/to/seq.fasta"` |
| `anomalous` | Process anomalous data | `True` |
| `friedel_pairs` | Merge Friedel pairs | `False` |
| `scaling_reference` | Reference for scaling | `"/path/to/ref.hkl"` |

### Data Collection Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `wavelength` | X-ray wavelength (Å) | `0.9795` |
| `beam_center` | Beam center [x, y] pixels | `[1024, 1024]` |
| `detector_distance` | Detector distance (mm) | `300.0` |

### Output Options

| Parameter | Description | Example |
|-----------|-------------|---------|
| `output_format` | Output file format | `"mtz"` |
| `merge_data` | Merge multiple datasets | `True` |
| `generate_report` | Generate HTML reports | `True` |

### Dataset Specification

```python
# Single file, all frames
DatasetSpec("/path/to/data_master.h5")

# Specific frame range
DatasetSpec("/path/to/data_master.h5", [1, 100])

# Command line format
--data /path/to/data_master.h5:1:100
```

### Pipeline-Specific Options

```python
config = PipelineConfig(
    work_dir="./run",
    pipeline_options={
        # Xia2 options
        "pipeline_type": "dials-aimless",
        "project": "my_project",
        
        # Strategy options  
        "program": "mosflm",
        "molsize": 300,
        
        # GMCA XDS/nXDS options
        "variant": "nxds",  # or "xds" for traditional
        "reference_dataset": "/path/to/ref.hkl",
        "powder": True
    }
)
```

## Database Integration

The driver automatically tracks pipeline execution in the database:

- **PipelineStatus**: Job metadata and execution status
- **DataProcessResults**: Detailed processing results
- **ScreenStrategyResults**: Strategy calculation results

Results are accessible via:
- Database queries using the tracking IDs
- Redis pub/sub for real-time updates
- JSON summary files in work directories

## Advanced Usage

### Multiple Datasets

```bash
# Process multiple sweeps
python -m qp2.pipelines.pipeline_driver xia2 \
  --data sweep1_master.h5:1:90 \
  --data sweep2_master.h5:1:180 \
  --work_dir ./multi_sweep
```

### Parallel Execution

```python
# Run multiple pipelines in parallel
configs = [
    {
        "pipeline_type": "autoproc",
        "datasets": ["data1_master.h5"],
        "work_dir": "./run1"
    },
    {
        "pipeline_type": "xia2", 
        "datasets": ["data2_master.h5"],
        "work_dir": "./run2"
    }
]

driver = PipelineDriver()
results = driver.run_multiple_pipelines(configs, parallel=True)
```

### Full Metadata Tracking

```bash
python -m qp2.pipelines.pipeline_driver autoproc \
  --data /path/to/data_master.h5 \
  --work_dir ./tracked_run \
  --sample_name protein_crystal \
  --username researcher \
  --beamline 23ID-B \
  --esaf_id 12345 \
  --pi_id 67890 \
  --primary_group structural_biology
```

## Output Structure

Each pipeline run creates:

```
work_dir/
├── pipeline_summary.json     # Standardized result summary
├── {pipeline}.log            # Pipeline-specific logging
├── [pipeline output files]   # Processing results
└── [additional files]        # Reports, plots, etc.
```

### Result Summary Format

```json
{
  "pipeline_type": "autoproc",
  "job_status": "SUCCESS", 
  "work_dir": "/path/to/work",
  "start_time": 1234567890.0,
  "end_time": 1234567950.0,
  "elapsed_time": 60.0,
  "results": {
    "spacegroup": "P212121",
    "unitcell": "78.9 95.2 114.6 90 90 90",
    "highresolution": 2.1,
    "truncate_mtz": "/path/to/final.mtz",
    "rmerge": 0.085,
    "completeness": 98.5
  },
  "pipeline_status_id": 12345,
  "result_pk_value": 67890
}
```

## Error Handling

The driver provides robust error handling:

- **Input validation** before execution
- **Comprehensive logging** at all stages  
- **Graceful failure recovery** with detailed error messages
- **Database status updates** for failed runs
- **Partial result preservation** when possible

Check logs and status:
```python
result = driver.run_pipeline(...)
if not result.success:
    print(f"Error: {result.error_message}")
    print(f"Check logs: {result.work_dir}")
```

## Integration with Existing QP2 Components

The Pipeline Driver integrates seamlessly with:

- **PipelineTracker**: Database tracking and Redis updates
- **run_job**: SLURM/local execution handling
- **Existing parsers**: AutoPROC XML, Xia2, Aimless parsers
- **QP2 logging**: Centralized logging configuration
- **Database models**: DataProcessResults, ScreenStrategyResults

## Examples

See `qp2/pipelines/examples/example_usage.py` for comprehensive examples covering:

- Single and multi-dataset processing
- Different pipeline configurations
- Parallel execution
- Database tracking
- Error handling

## Troubleshooting

### Common Issues

1. **File not found errors**: Ensure HDF5/CBF files exist and are accessible
2. **Permission errors**: Check work directory write permissions
3. **SLURM submission fails**: Verify SLURM configuration and resource availability
4. **Database connection issues**: Check Redis/MySQL connectivity
5. **Parser failures**: Verify pipeline completed successfully and output files exist

### Debug Mode

Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Or check pipeline-specific logs in the work directory.

### Getting Help

- Check the pipeline-specific documentation in each subdirectory
- Review log files for detailed error messages
- Use the programmatic interface for more control over execution
- Consult the existing QP2 pipeline implementations for reference