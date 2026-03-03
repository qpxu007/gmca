# QP2 Pipeline Driver: User Transition Guide

## Overview

QP2 has introduced a new **Unified Pipeline Driver** that standardizes and enhances crystallographic data processing. This guide helps users transition from the existing plugin-based system to the new unified interface while maintaining all current functionality.

## What's Changing?

### ✅ **What Stays the Same**
- **All existing workflows continue to work** - No immediate changes required
- **Same GUI interfaces** in the image viewer plugins
- **Same result locations** (`~/autoproc_runs/`, `~/xia2_runs/`, etc.)
- **Same status tracking** via Redis notifications
- **Same output formats** and file structures

### 🚀 **What's New and Better**
- **Additional processing options** (molecular replacement, anomalous data, beam center correction)
- **Unified command-line interface** for all pipelines
- **Better error handling** and progress tracking
- **Enhanced database integration** 
- **Consistent configuration** across all processing types

## For Image Viewer Plugin Users

### Current Workflow (Still Works!)
```
1. Load dataset in image viewer
2. Right-click → Processing → AutoPROC/Xia2/nXDS
3. Configure parameters in dialog
4. Submit job
5. Monitor progress in status panel
```

### Enhanced Options Now Available

**AutoPROC with Molecular Replacement:**
- New **"Model File"** field for PDB search models
- **"Anomalous Data"** checkbox for anomalous processing
- **"Beam Center Override"** for problematic datasets

**Xia2 Enhancements:**
- **"Resolution Range"** controls (high and low limits)
- **"Wavelength Override"** for custom data collection setups
- **"Output Format"** selection (MTZ, SCA, etc.)

**nXDS/GMCA Improvements:**
- **"Processing Variant"** choice (traditional XDS vs optimized nXDS)
- **"Reference Dataset"** for improved scaling
- **"Detector Geometry"** overrides for difficult datasets

### Migration Timeline

| Phase | Timeline | User Impact | Action Required |
|-------|----------|-------------|-----------------|
| **Phase 1** | Week 1-2 | None | Continue normal usage |
| **Phase 2** | Week 3-4 | Enhanced options appear | Optional: Try new features |
| **Phase 3** | Month 2 | Improved performance | None - automatic |
| **Phase 4** | Month 3+ | Full unified system | None - transparent |

## For Command-Line Users

### New Unified Interface

**Single Command for All Pipelines:**
```bash
# New unified command (available immediately)
qp2-pipeline [autoproc|xia2|gmcaproc|strategy] [options]
```

**Migration from Old Commands:**

| Old Command | New Unified Command |
|-------------|-------------------|
| `python autoproc_process_dataset.py` | `qp2-pipeline autoproc` |
| `python xia2_process_dataset.py` | `qp2-pipeline xia2` |
| `python nxds_process_dataset.py` | `qp2-pipeline gmcaproc --variant nxds` |

### Enhanced Command Examples

**AutoPROC with Molecular Replacement:**
```bash
qp2-pipeline autoproc \
  --data /path/to/data_master.h5 \
  --work_dir ./autoproc_mr \
  --model /path/to/search_model.pdb \
  --highres 2.5 \
  --anomalous \
  --sample_name "lysozyme_derivative"
```

**Xia2 with Multiple Datasets:**
```bash
qp2-pipeline xia2 \
  --data sweep1_master.h5:1:90 \
  --data sweep2_master.h5:1:180 \
  --work_dir ./multi_sweep \
  --highres 1.8 \
  --lowres 50.0 \
  --space_group P212121 \
  --pipeline_type_variant dials-aimless
```

**nXDS for Serial Crystallography:**
```bash
qp2-pipeline gmcaproc \
  --data /path/to/data_master.h5 \
  --work_dir ./serial_run \
  --variant nxds \
  --scaling_reference /path/to/reference.hkl \
  --powder \
  --detector_distance 300
```

**Traditional XDS Processing:**
```bash
qp2-pipeline gmcaproc \
  --data /path/to/data_master.h5 \
  --work_dir ./xds_run \
  --variant xds \
  --space_group "P 21 21 21" \
  --unit_cell "78.9 95.2 114.6 90 90 90"
```

## For Beamline Staff/Administrators

### Deployment Schedule

**Phase 1: Compatibility Layer (Week 1)**
- New pipeline driver deployed alongside existing system
- Zero user impact - all existing workflows continue unchanged
- Staff can begin testing new features

**Phase 2: Enhanced Plugins (Week 2-3)**
- Plugin interfaces updated with new options
- Users can access enhanced functionality
- Old workflows remain fully functional

**Phase 3: Server Integration (Week 4-6)**
- Data processing server migrated to use new driver
- Improved performance and monitoring
- Enhanced database tracking

**Phase 4: Unified System (Month 2+)**
- Complete migration to unified system
- Legacy code cleanup
- Full documentation and training

### Testing and Validation

**Pre-Migration Checklist:**
- [ ] Test all plugin workflows with sample datasets
- [ ] Verify Redis status tracking continues working
- [ ] Validate result file formats and locations
- [ ] Check database integration and tracking
- [ ] Performance benchmarking vs current system

**Monitoring During Transition:**
- Pipeline execution success rates
- Processing time comparisons
- User error reports
- System resource utilization

### Configuration Management

**New Configuration Files:**
```
~/.qp2/pipeline_config.yaml     # User-specific defaults
/etc/qp2/pipeline_config.yaml   # System-wide defaults
```

**Example Configuration:**
```yaml
# Default processing parameters
default:
  runner: slurm
  nproc: 8
  beamline: auto-detect
  
# Pipeline-specific defaults
autoproc:
  njobs: 4
  fast_mode: false
  
xia2:
  pipeline_type: dials-aimless
  
gmcaproc:
  variant: nxds
  powder: true
```

## New Features Guide

### Molecular Replacement Support

**When to Use:**
- You have a related structure as search model
- Initial phases needed for structure solution
- AutoPROC processing with known space group

**How to Use:**
```bash
# Command line
qp2-pipeline autoproc \
  --data native_data.h5 \
  --model search_model.pdb \
  --space_group P212121

# Python API
config = PipelineConfig(
    model="/path/to/search_model.pdb",
    space_group="P212121"
)
```

### Anomalous Data Processing

**When to Use:**
- Heavy atom derivatives (Hg, Pt, etc.)
- Selenomethionine incorporation
- Cu Kα radiation with sulfur anomalous signal

**How to Use:**
```bash
# Command line
qp2-pipeline xia2 \
  --data selenium_data.h5 \
  --anomalous \
  --no_friedel_pairs \
  --wavelength 0.9795

# Note: Friedel pairs kept separate for anomalous signal
```

### Multiple Dataset Processing

**When to Use:**
- Multi-orientation data collection
- Radiation damage series
- Multiple crystals of same structure

**How to Use:**
```bash
# Multiple datasets with frame ranges
qp2-pipeline xia2 \
  --data crystal1_master.h5:1:90 \
  --data crystal2_master.h5:1:180 \
  --data crystal3_master.h5:1:120 \
  --work_dir ./multi_crystal
```

### Custom Geometry Parameters

**When to Use:**
- Problematic beam center detection
- Non-standard detector distances
- Custom data collection setups

**How to Use:**
```bash
# Override geometry parameters
qp2-pipeline autoproc \
  --data difficult_data.h5 \
  --beam_center 1024.5 1024.5 \
  --detector_distance 300.0 \
  --wavelength 1.5418
```

## Troubleshooting

### Common Issues and Solutions

**Issue: "Pipeline command not found"**
```bash
# Solution: Ensure QP2 is properly installed
pip install -e /path/to/qp2
# or
export PATH=$PATH:/path/to/qp2/bin
```

**Issue: "Permission denied on work directory"**
```bash
# Solution: Check directory permissions
chmod 755 /path/to/work/directory
# or use a directory you have write access to
qp2-pipeline autoproc --work_dir ~/my_processing
```

**Issue: "Redis connection failed"**
```bash
# Solution: Check Redis server status
redis-cli ping
# Should return "PONG"
# Contact system administrator if Redis is down
```

**Issue: "SLURM submission failed"**
```bash
# Solution: Check SLURM status or use local execution
squeue -u $USER  # Check your job queue
# or
qp2-pipeline autoproc --runner shell  # Run locally
```

**Issue: "Model file not found for molecular replacement"**
```bash
# Solution: Check file path and permissions
ls -la /path/to/model.pdb
# Ensure file exists and is readable
```

### Getting Help

**Documentation Resources:**
- Main documentation: `qp2/pipelines/README.md`
- Command-line help: `qp2-pipeline --help`
- Pipeline-specific help: `qp2-pipeline autoproc --help`

**Support Channels:**
- Email: qp2-support@example.com
- Issue tracker: [GitHub/GitLab link]
- Slack: #qp2-support

**Log Files for Debugging:**
```bash
# Check pipeline logs
tail -f ~/autoproc_runs/dataset_name/autoproc.log
tail -f ~/xia2_runs/dataset_name/xia2.log

# Check system logs
journalctl -u qp2-pipeline-server -f
```

## Training Resources

### Video Tutorials (Coming Soon)
- "Introduction to the Unified Pipeline Driver"
- "Using Molecular Replacement with AutoPROC"
- "Processing Anomalous Data with Xia2"
- "nXDS vs XDS: When to Use Which"

### Hands-On Workshops
- **Basic Workshop**: Transitioning from plugins to command line
- **Advanced Workshop**: Using new features for complex problems
- **Administrator Workshop**: Deployment and configuration management

### Practice Datasets
Available in `/path/to/qp2/test_data/`:
- `lysozyme_native/` - Basic processing example
- `insulin_anomalous/` - Anomalous data example
- `multi_crystal/` - Multiple dataset example

## FAQ

**Q: Do I need to change my existing scripts?**
A: No! All existing scripts and workflows continue to work unchanged during the transition.

**Q: Will my results be stored in the same location?**
A: Yes! The same directory structure (`~/autoproc_runs/`, etc.) is maintained.

**Q: Can I use both old and new systems simultaneously?**
A: Yes! During the transition period, both systems run side-by-side.

**Q: What happens to my Redis status tracking?**
A: Redis tracking continues to work exactly as before with the same key formats.

**Q: How do I know which system I'm using?**
A: Check the log files - new system logs will mention "unified pipeline driver".

**Q: Can I revert to the old system if needed?**
A: Yes! Rollback procedures are available for each phase of the transition.

**Q: Will processing be faster with the new system?**
A: Processing speed should be similar or better due to optimizations and better resource management.

**Q: Do I need new training to use enhanced features?**
A: Basic usage requires no new training. Enhanced features have intuitive interfaces and good documentation.

## Quick Reference

### Command Comparison
| Task | Old Command | New Command |
|------|-------------|-------------|
| Basic AutoPROC | `submit_autoproc_job.py` | `qp2-pipeline autoproc` |
| Basic Xia2 | `submit_xia2_job.py` | `qp2-pipeline xia2` |
| nXDS | `submit_nxds_job.py` | `qp2-pipeline gmcaproc --variant nxds` |
| Strategy | `submit_strategy_job.py` | `qp2-pipeline strategy` |

### Common Parameters
| Parameter | Description | Example |
|-----------|-------------|---------|
| `--data` | Dataset file(s) | `--data master.h5` |
| `--work_dir` | Processing directory | `--work_dir ./processing` |
| `--highres` | High resolution limit | `--highres 2.0` |
| `--space_group` | Space group | `--space_group P212121` |
| `--model` | PDB search model | `--model search.pdb` |
| `--anomalous` | Process anomalous data | `--anomalous` |

---

*This guide will be updated as new features are added and user feedback is incorporated. Please report any issues or suggestions to the support team.*