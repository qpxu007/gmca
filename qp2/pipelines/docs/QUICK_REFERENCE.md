# QP2 Pipeline Driver - Quick Reference Card

## Basic Usage

```bash
# General syntax
qp2-pipeline [pipeline] --data [file] --work_dir [directory] [options]
```

## Pipeline Types

| Pipeline | Description | Use Case |
|----------|-------------|----------|
| `autoproc` | AutoPROC processing | General structure solution |
| `xia2` | Xia2 processing | Multi-sweep, DIALS/XDS |
| `gmcaproc` | GMCA XDS/nXDS | Serial crystallography |
| `strategy` | Data collection planning | Optimize collection strategy |

## Common Examples

### AutoPROC - Basic
```bash
qp2-pipeline autoproc \
  --data /path/to/data_master.h5 \
  --work_dir ./autoproc_run \
  --sample_name my_protein
```

### AutoPROC - Molecular Replacement
```bash
qp2-pipeline autoproc \
  --data /path/to/data_master.h5 \
  --work_dir ./autoproc_mr \
  --model /path/to/search_model.pdb \
  --space_group P212121 \
  --anomalous
```

### Xia2 - Multi-sweep
```bash
qp2-pipeline xia2 \
  --data sweep1.h5:1:90 \
  --data sweep2.h5:1:180 \
  --work_dir ./xia2_multi \
  --highres 1.8 \
  --pipeline_type_variant dials
```

### Xia2 - Anomalous Data
```bash
qp2-pipeline xia2 \
  --data selenium_data.h5 \
  --work_dir ./anomalous \
  --anomalous \
  --no_friedel_pairs \
  --wavelength 0.9795
```

### nXDS - Serial Crystallography
```bash
qp2-pipeline gmcaproc \
  --data /path/to/data_master.h5 \
  --work_dir ./nxds_run \
  --variant nxds \
  --powder \
  --scaling_reference /path/to/ref.hkl
```

### XDS - Traditional
```bash
qp2-pipeline gmcaproc \
  --data /path/to/data_master.h5 \
  --work_dir ./xds_run \
  --variant xds \
  --space_group "P 21 21 21"
```

### Strategy Calculation
```bash
qp2-pipeline strategy \
  --data /path/to/test_master.h5 \
  --work_dir ./strategy \
  --program mosflm \
  --molsize 300
```

## Essential Parameters

### Core Options
| Parameter | Description | Example |
|-----------|-------------|---------|
| `--data` | Dataset file (with optional range) | `data.h5:1:100` |
| `--work_dir` | Output directory | `./processing` |
| `--runner` | Execution method | `slurm` / `shell` |
| `--nproc` | Number of processors | `--nproc 16` |

### Processing Parameters
| Parameter | Description | Example |
|-----------|-------------|---------|
| `--highres` | High resolution limit (Å) | `--highres 2.0` |
| `--lowres` | Low resolution limit (Å) | `--lowres 50.0` |
| `--space_group` | Space group | `--space_group P212121` |
| `--unit_cell` | Unit cell parameters | `--unit_cell "78.9 95.2 114.6 90 90 90"` |

### Advanced Options
| Parameter | Description | Example |
|-----------|-------------|---------|
| `--model` | PDB search model | `--model search.pdb` |
| `--anomalous` | Process anomalous data | `--anomalous` |
| `--no_friedel_pairs` | Keep Friedel pairs separate | `--no_friedel_pairs` |
| `--wavelength` | X-ray wavelength (Å) | `--wavelength 1.5418` |
| `--beam_center` | Beam center (pixels) | `--beam_center 1024 1024` |

### Pipeline-Specific Options
| Parameter | Pipeline | Description |
|-----------|----------|-------------|
| `--fast` | autoproc, xia2 | Fast processing mode |
| `--pipeline_type_variant` | xia2 | `dials`, `dials-aimless`, `xds` |
| `--variant` | gmcaproc | `xds` or `nxds` |
| `--powder` | gmcaproc | Enable ice ring detection |
| `--program` | strategy | `mosflm` or `xds` |
| `--molsize` | strategy | Protein size (residues) |

## Output Locations

| Pipeline | Default Directory | Key Files |
|----------|------------------|-----------|
| AutoPROC | `~/autoproc_runs/dataset/` | `summary.html`, `*.mtz` |
| Xia2 | `~/xia2_runs/dataset/` | `xia2.html`, `*_free.mtz` |
| nXDS | `~/nxds_runs/dataset/` | `XDS_ASCII.HKL`, `*.mtz` |
| Strategy | `~/strategy_runs/dataset/` | `strategy_results.json` |

## Status Monitoring

### Check Job Status
```bash
# SLURM jobs
squeue -u $USER

# Local jobs
ps aux | grep qp2-pipeline

# Check logs
tail -f ~/autoproc_runs/dataset/autoproc.log
```

### Result Summary
```bash
# Check result summary
cat ~/autoproc_runs/dataset/pipeline_summary.json
```

## Troubleshooting

### Common Issues
| Problem | Solution |
|---------|----------|
| Command not found | `export PATH=$PATH:/path/to/qp2/bin` |
| Permission denied | `chmod 755 /path/to/work/dir` |
| SLURM failed | Try `--runner shell` |
| File not found | Check file path and permissions |

### Get Help
```bash
# General help
qp2-pipeline --help

# Pipeline-specific help
qp2-pipeline autoproc --help
qp2-pipeline xia2 --help
qp2-pipeline gmcaproc --help
qp2-pipeline strategy --help
```

### Log Files
```bash
# Pipeline logs
~/[pipeline]_runs/dataset/[pipeline].log

# System logs
journalctl -u qp2-pipeline-server -f
```

## Migration from Old Commands

| Old Script | New Command |
|------------|-------------|
| `autoproc_process_dataset.py` | `qp2-pipeline autoproc` |
| `xia2_process_dataset.py` | `qp2-pipeline xia2` |
| `nxds_process_dataset.py` | `qp2-pipeline gmcaproc --variant nxds` |

## Python API

```python
from qp2.pipelines.pipeline_driver import (
    PipelineDriver, PipelineConfig, DatasetSpec
)

# Create configuration
config = PipelineConfig(
    work_dir="./processing",
    highres=2.0,
    model="/path/to/search.pdb"
)

# Run pipeline
driver = PipelineDriver()
result = driver.run_pipeline("autoproc", ["/path/to/data.h5"], config)

if result.success:
    print(f"Success! Database ID: {result.pipeline_status_id}")
else:
    print(f"Failed: {result.error_message}")
```

---

**💡 Pro Tips:**
- Use `--fast` for quick testing
- Add `--sample_name` for better tracking
- Check `pipeline_summary.json` for detailed results
- Use `--anomalous` for heavy atom/selenium data
- Try `--variant nxds` for serial crystallography

**📖 More Info:** See `/path/to/qp2/pipelines/docs/USER_TRANSITION_GUIDE.md`