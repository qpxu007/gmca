# QP2 Pipeline Driver Integration Analysis

## Current System Architecture

After analyzing the existing QP2 pipeline usage in `data_proc` and `image_viewer/plugins/`, here's how pipelines are currently integrated:

### 1. Image Viewer Plugin Architecture

**Current Flow:**
```
User GUI → Plugin Manager → Submit Job Worker → Process Dataset Script → Pipeline Implementation
```

**Key Components:**
- **Plugin Managers** (`autoproc_manager.py`, `xia2_manager.py`, `nxds_manager.py`)
- **Job Submission Workers** (`submit_*_job.py`) - PyQt5 QRunnable classes
- **Process Dataset Scripts** (`*_process_dataset.py`) - Individual pipeline wrappers
- **Pipeline Implementations** - Located in `qp2/pipelines/{pipeline_name}/`

### 2. Data Processing Server Architecture

**Current Flow:**
```
HTTP API → Analysis Manager → Data Processing Server → Pipeline Execution
```

**Key Components:**
- **Data Processing Server** (`data_processing_server.py`) - FastAPI-based REST server
- **Analysis Manager** (`analysis_manager.py`) - Job orchestration and tracking
- **Client Dialogs** (`dataset_processor_dialog.py`) - GUI for job submission

### 3. Current Pipeline Integration Patterns

#### AutoPROC Integration
```python
# In submit_autoproc_job.py
script_path = "autoproc_process_dataset.py"
command_list = [
    sys.executable, script_path,
    "--pipeline", "autoPROC",
    "--data", master_file,
    "--work_dir", proc_dir,
    "--status_key", status_key,
    # ... additional args
]
```

#### nXDS Integration  
```python
# In submit_nxds_job.py
script_path = "nxds_process_dataset.py"
command_list = [
    sys.executable, script_path,
    "--master_file", master_file,
    "--proc_dir", proc_dir,
    "--nproc", nproc,
    "--njobs", njobs,
    # ... nXDS specific args
]
```

#### Xia2 Integration
```python
# In submit_xia2_job.py  
script_path = "xia2_process_dataset.py"
command_list = [
    sys.executable, script_path,
    "--pipeline", pipeline_choice,
    "--data", master_file,
    # ... xia2 specific args
]
```

## Integration Strategy for New Pipeline Driver

### 1. Backward Compatibility Layer

I've created `integration_adapter.py` which provides:

**PluginCompatibilityAdapter Class:**
- `run_autoproc_compatible()` - Maintains existing AutoPROC plugin interface
- `run_xia2_compatible()` - Maintains existing Xia2 plugin interface  
- `run_nxds_compatible()` - Maintains existing nXDS plugin interface

**Legacy Script Generation:**
- Creates drop-in replacement scripts for existing `*_process_dataset.py` files
- Maintains identical command-line interfaces
- Routes execution through new pipeline driver

### 2. Redis Integration Compatibility

**Current Pattern:**
```python
# Existing plugins use Redis for status tracking
results_key = f"{redis_key_prefix}:{master_file}"
status_key = f"{results_key}:status"
redis_conn.set(status_key, json.dumps({"status": "RUNNING"}))
```

**New Driver Integration:**
```python
# Adapter maintains same Redis patterns
config.pipeline_options.update({
    "redis_host": redis_host,
    "redis_port": redis_port, 
    "redis_key": results_key,
    "status_key": status_key
})
```

### 3. Parameter Mapping

**Existing Plugin Parameters → New Driver Config:**

| Plugin Parameter | Driver Config | Notes |
|-----------------|---------------|-------|
| `autoproc_highres` | `config.highres` | Direct mapping |
| `autoproc_space_group` | `config.space_group` | Direct mapping |
| `xia2_pipeline` | `config.pipeline_options["pipeline_type"]` | Mapped to DIALS/XDS |
| `nxds_powder` | `config.pipeline_options["powder"]` | nXDS-specific option |
| `nxds_reference_hkl` | `config.scaling_reference` | Reference for scaling |

### 4. Workflow Directory Compatibility

**Current Directory Structure:**
```
~/autoproc_runs/dataset_name/     # AutoPROC results
~/xia2_runs/dataset_name/         # Xia2 results  
~/nxds_runs/dataset_name/         # nXDS results
```

**Maintained by Adapter:**
```python
def _determine_work_dir(self, master_file, prefix, **kwargs):
    proc_dir_root = Path(f"~/{prefix}_runs").expanduser()
    master_basename = os.path.splitext(os.path.basename(master_file))[0]
    return str(proc_dir_root / master_basename)
```

## Migration Strategies

### Option 1: Gradual Migration (Recommended)

1. **Phase 1**: Deploy integration adapter alongside existing systems
2. **Phase 2**: Update plugin submission workers to use adapter
3. **Phase 3**: Migrate data processing server to use pipeline driver
4. **Phase 4**: Remove legacy pipeline implementations

### Option 2: Direct Replacement

Replace existing `*_process_dataset.py` scripts with unified driver wrappers:

```python
# New autoproc_process_dataset.py
from qp2.pipelines.integration_adapter import create_legacy_process_dataset_script
# Uses unified driver with backward-compatible interface
```

### Option 3: Parallel Deployment  

Run both systems side-by-side:
- Legacy plugins continue using existing scripts
- New workflows use pipeline driver directly
- Gradual user migration based on preference

## Benefits of Integration

### 1. Unified Configuration
- **Before**: Separate parameter handling in each plugin
- **After**: Centralized configuration through PipelineConfig

### 2. Consistent Database Tracking
- **Before**: Custom tracking logic in each pipeline  
- **After**: Unified PipelineTracker integration

### 3. Improved Error Handling
- **Before**: Plugin-specific error handling
- **After**: Comprehensive error recovery across all pipelines

### 4. Enhanced Monitoring
- **Before**: Redis status updates only
- **After**: Full lifecycle tracking with detailed progress

### 5. Simplified Maintenance
- **Before**: Update multiple pipeline scripts separately
- **After**: Single driver update affects all pipelines

## Implementation Recommendations

### Immediate Actions (Week 1)

1. **Deploy Integration Adapter**
   ```bash
   # Add integration_adapter.py to qp2/pipelines/
   # Test with existing plugin workflows
   ```

2. **Create Legacy Wrappers**
   ```bash
   python qp2/pipelines/integration_adapter.py
   # Generates *_process_dataset_unified.py scripts
   ```

3. **Validate Compatibility**
   ```bash
   # Test existing plugin workflows with new wrappers
   # Ensure Redis tracking continues to work
   ```

### Short Term (2-4 weeks)

1. **Update Plugin Submission Workers**
   - Modify `submit_*_job.py` to use PluginCompatibilityAdapter
   - Maintain existing GUI interfaces
   - Test thoroughly with user workflows

2. **Enhance Data Processing Server**
   - Integrate pipeline driver into `analysis_manager.py`
   - Add new driver endpoints to REST API
   - Maintain backward compatibility

### Medium Term (1-2 months)

1. **User Interface Updates**
   - Add new pipeline options to settings dialogs
   - Expose additional driver features (model, anomalous, etc.)
   - Update documentation and user guides

2. **Performance Optimization**
   - Optimize pipeline driver for high-throughput processing
   - Add batch processing capabilities
   - Implement intelligent resource management

### Long Term (3-6 months)

1. **Legacy Cleanup**
   - Remove old pipeline implementations
   - Consolidate documentation
   - Training for users and administrators

2. **Advanced Features**
   - Web-based pipeline submission
   - Advanced workflow automation
   - Integration with external systems

## Testing Strategy

### Unit Tests
```python
# Test adapter compatibility
def test_autoproc_adapter():
    adapter = PluginCompatibilityAdapter()
    result = adapter.run_autoproc_compatible(
        master_file="test_master.h5",
        metadata={"username": "test"},
        redis_key_prefix="test"
    )
    assert result is not None
```

### Integration Tests
```python
# Test with actual plugin workflows
def test_plugin_workflow():
    # Submit job through existing plugin
    # Verify Redis status updates
    # Check result files match expected format
```

### User Acceptance Tests
- Test all existing plugin workflows
- Verify GUI functionality unchanged
- Validate result formats and locations
- Ensure performance meets expectations

## Risk Mitigation

### Data Loss Prevention
- Maintain existing result directories
- Preserve Redis key formats
- Backup configurations before migration

### Performance Regression
- Benchmark current vs new performance
- Monitor resource usage during transition
- Rollback plan if issues arise

### User Disruption
- Transparent migration where possible
- Clear communication of any changes
- Training materials for new features

## Conclusion

The integration strategy provides a smooth transition path from the current distributed pipeline system to the unified pipeline driver while maintaining full backward compatibility. The phased approach minimizes risk and allows for gradual adoption across the QP2 ecosystem.

Key success metrics:
- ✅ Zero downtime during migration
- ✅ Identical user experience initially
- ✅ Improved functionality over time  
- ✅ Simplified maintenance and development