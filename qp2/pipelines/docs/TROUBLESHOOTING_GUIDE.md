# QP2 Pipeline Driver Troubleshooting Guide

## Quick Diagnosis

### 🚨 **Is This a Pipeline Driver Issue?**

**Check if using new or old system:**
```bash
# Check which system is running your job
ps aux | grep -E "(process_dataset|qp2-pipeline)" | grep $USER

# Look in log files for system identifier
grep -i "unified\|pipeline_driver" ~/autoproc_runs/dataset_name/*.log
```

**If you see `pipeline_driver` or `unified` → you're using the new system**
**If you see old script names → you're using the legacy system**

## Installation and Setup Issues

### ❌ **Command Not Found: `qp2-pipeline`**

**Symptoms:**
```bash
$ qp2-pipeline autoproc --help
bash: qp2-pipeline: command not found
```

**Diagnosis:**
```bash
# Check if QP2 is installed
python -c "import qp2; print(qp2.__file__)"

# Check if binary is in PATH
which qp2-pipeline
echo $PATH | grep qp2
```

**Solutions:**
```bash
# Solution 1: Add to PATH
export PATH=$PATH:/path/to/qp2/bin
echo 'export PATH=$PATH:/path/to/qp2/bin' >> ~/.bashrc

# Solution 2: Use full path
/path/to/qp2/bin/qp2-pipeline autoproc --help

# Solution 3: Reinstall QP2
cd /path/to/qp2
pip install -e .

# Solution 4: Use Python module directly
python -m qp2.pipelines.pipeline_driver autoproc --help
```

### ❌ **Import Errors**

**Symptoms:**
```
ImportError: No module named 'qp2.pipelines.pipeline_driver'
ModuleNotFoundError: No module named 'qp2'
```

**Diagnosis:**
```bash
# Check Python path
python -c "import sys; print('\n'.join(sys.path))"

# Check QP2 installation
pip list | grep qp2
```

**Solutions:**
```bash
# Solution 1: Set PYTHONPATH
export PYTHONPATH=/path/to/qp2:$PYTHONPATH

# Solution 2: Install in development mode
cd /path/to/qp2
pip install -e .

# Solution 3: Check virtual environment
which python
source /path/to/correct/venv/bin/activate
```

## Job Submission Issues

### ❌ **SLURM Submission Failures**

**Symptoms:**
```
Error: SLURM job submission failed
sbatch: error: invalid partition specified
sbatch: error: QOSMaxJobsPerUserLimit
```

**Diagnosis:**
```bash
# Check SLURM status
squeue -u $USER
sinfo
sacct -u $USER --starttime=today

# Check partitions and QOS
sinfo -s
sacctmgr show qos format=Name,MaxJobs,MaxJobsPU
```

**Solutions:**
```bash
# Solution 1: Use different partition
qp2-pipeline autoproc --data file.h5 --work_dir ./run \
  --runner slurm --partition compute

# Solution 2: Wait for job slots
squeue -u $USER  # Check current jobs
# Wait for jobs to complete or cancel unnecessary ones

# Solution 3: Use local execution
qp2-pipeline autoproc --data file.h5 --work_dir ./run \
  --runner shell

# Solution 4: Adjust resource requirements
qp2-pipeline autoproc --data file.h5 --work_dir ./run \
  --nproc 4 --njobs 1  # Reduce resource requirements
```

### ❌ **Permission Denied on Work Directory**

**Symptoms:**
```
Error: Permission denied: '/path/to/work/directory'
OSError: [Errno 13] Permission denied
```

**Diagnosis:**
```bash
# Check directory permissions
ls -ld /path/to/work/directory
whoami
groups

# Check parent directory permissions
ls -ld /path/to/work/
```

**Solutions:**
```bash
# Solution 1: Fix permissions
chmod 755 /path/to/work/directory
# or
sudo chown $USER:$USER /path/to/work/directory

# Solution 2: Use writable directory
qp2-pipeline autoproc --data file.h5 --work_dir ~/my_processing

# Solution 3: Create directory first
mkdir -p ~/autoproc_runs/my_dataset
qp2-pipeline autoproc --data file.h5 --work_dir ~/autoproc_runs/my_dataset
```

## Data File Issues

### ❌ **Dataset File Not Found**

**Symptoms:**
```
Error: Dataset file not found: /path/to/data_master.h5
FileNotFoundError: [Errno 2] No such file or directory
```

**Diagnosis:**
```bash
# Check file exists
ls -la /path/to/data_master.h5

# Check file permissions
ls -la /path/to/data_master.h5

# Check if file is a symlink
file /path/to/data_master.h5
readlink /path/to/data_master.h5
```

**Solutions:**
```bash
# Solution 1: Use correct path
find /path/to/data/directory -name "*master.h5"

# Solution 2: Check file permissions
chmod 644 /path/to/data_master.h5

# Solution 3: Use absolute path
qp2-pipeline autoproc --data $(readlink -f data_master.h5) --work_dir ./run

# Solution 4: Check mount points
df -h /path/to/data/
mount | grep /path/to/data
```

### ❌ **Invalid Frame Range**

**Symptoms:**
```
Error: Invalid frame range in dataset argument
ValueError: Invalid dataset argument format
```

**Diagnosis:**
```bash
# Check your frame range syntax
echo "Your command: qp2-pipeline autoproc --data file.h5:start:end"

# Check available frames in HDF5 file
h5ls /path/to/data_master.h5
h5dump -d /entry/data/data_000001 /path/to/data_master.h5 | head
```

**Solutions:**
```bash
# Solution 1: Fix syntax
# Wrong: file.h5:1-100
# Right: file.h5:1:100
qp2-pipeline autoproc --data file.h5:1:100 --work_dir ./run

# Solution 2: Check available frames
# For HDF5 files, frames typically start at 1
qp2-pipeline autoproc --data file.h5:1:90 --work_dir ./run

# Solution 3: Use all frames (no range)
qp2-pipeline autoproc --data file.h5 --work_dir ./run
```

## Processing Parameter Issues

### ❌ **Invalid Space Group**

**Symptoms:**
```
Error: Invalid space group: P21221
Warning: Space group not recognized
```

**Diagnosis:**
```bash
# Check your space group format
echo "You specified: P21221"
echo "Try alternative formats"
```

**Solutions:**
```bash
# Solution 1: Use standard format
qp2-pipeline autoproc --space_group "P 21 21 21"  # With spaces
qp2-pipeline autoproc --space_group P212121       # Without spaces

# Solution 2: Use space group number
qp2-pipeline autoproc --space_group 19

# Solution 3: Let pipeline determine automatically
qp2-pipeline autoproc --data file.h5 --work_dir ./run
# (omit space group parameter)
```

### ❌ **Model File Issues**

**Symptoms:**
```
Error: Model file not found: /path/to/model.pdb
Error: Invalid PDB file format
```

**Diagnosis:**
```bash
# Check file exists and format
ls -la /path/to/model.pdb
file /path/to/model.pdb
head -5 /path/to/model.pdb

# Check PDB format
grep "^ATOM" /path/to/model.pdb | head -3
```

**Solutions:**
```bash
# Solution 1: Use absolute path
qp2-pipeline autoproc --model $(readlink -f model.pdb) --data file.h5

# Solution 2: Validate PDB format
# PDB files should start with HEADER or ATOM records
# Check if it's a valid PDB format

# Solution 3: Skip molecular replacement
qp2-pipeline autoproc --data file.h5 --work_dir ./run
# Process without model for initial structure solution
```

## Redis and Database Issues

### ❌ **Redis Connection Failed**

**Symptoms:**
```
Error: Redis connection failed
redis.exceptions.ConnectionError: Connection refused
```

**Diagnosis:**
```bash
# Check Redis server status
redis-cli ping
# Should return "PONG"

# Check Redis configuration
redis-cli config get bind
redis-cli config get port
```

**Solutions:**
```bash
# Solution 1: Start Redis server
sudo systemctl start redis
# or
redis-server &

# Solution 2: Check Redis configuration
sudo systemctl status redis
sudo journalctl -u redis

# Solution 3: Use different Redis host
# Contact system administrator for Redis server details

# Solution 4: Continue without Redis (reduced tracking)
# Pipeline will still work, but status updates may be limited
```

### ❌ **Database Connection Issues**

**Symptoms:**
```
Error: Database connection failed
mysql.connector.errors.DatabaseError: Access denied
```

**Diagnosis:**
```bash
# Check database connectivity
mysql -h hostname -u username -p database_name
```

**Solutions:**
```bash
# Solution 1: Check credentials
# Contact database administrator for correct credentials

# Solution 2: Continue without database tracking
# Processing will work, but results won't be stored in database

# Solution 3: Use alternative tracking
# Results will still be saved to JSON files in work directory
```

## Pipeline-Specific Issues

### ❌ **AutoPROC Processing Failures**

**Symptoms:**
```
AutoPROC failed: No reflections found
AutoPROC failed: Indexing unsuccessful
```

**Common Causes & Solutions:**

**Beam Center Issues:**
```bash
# Override beam center
qp2-pipeline autoproc --data file.h5 --beam_center 1024.5 1024.5
```

**Resolution Issues:**
```bash
# Adjust resolution limits
qp2-pipeline autoproc --data file.h5 --highres 3.0 --lowres 50.0
```

**Indexing Issues:**
```bash
# Try with known space group
qp2-pipeline autoproc --data file.h5 --space_group P212121
```

### ❌ **Xia2 Processing Failures**

**Symptoms:**
```
Xia2 failed: DIALS processing unsuccessful
Xia2 failed: No unit cell determined
```

**Common Solutions:**

**Pipeline Selection:**
```bash
# Try different pipeline
qp2-pipeline xia2 --data file.h5 --pipeline_type_variant xds
qp2-pipeline xia2 --data file.h5 --pipeline_type_variant dials
```

**Geometry Issues:**
```bash
# Override problematic geometry
qp2-pipeline xia2 --data file.h5 \
  --beam_center 1024 1024 \
  --detector_distance 300.0
```

### ❌ **nXDS/GMCA Processing Failures**

**Symptoms:**
```
nXDS failed: Spot finding unsuccessful
XDS failed: No diffraction spots found
```

**Common Solutions:**

**Try Different Variant:**
```bash
# Switch between nXDS and XDS
qp2-pipeline gmcaproc --data file.h5 --variant nxds  # For serial
qp2-pipeline gmcaproc --data file.h5 --variant xds   # For traditional
```

**Adjust Processing Parameters:**
```bash
# Enable powder mode for ice ring handling
qp2-pipeline gmcaproc --data file.h5 --powder

# Use reference for scaling
qp2-pipeline gmcaproc --data file.h5 --scaling_reference ref.hkl
```

## Performance Issues

### 🐌 **Slow Processing**

**Diagnosis:**
```bash
# Check system load
top
htop
iostat 1

# Check SLURM job status
squeue -u $USER
scontrol show job JOBID
```

**Solutions:**
```bash
# Solution 1: Reduce resource requirements
qp2-pipeline autoproc --data file.h5 --nproc 4 --njobs 1

# Solution 2: Use local execution for small jobs
qp2-pipeline autoproc --data file.h5 --runner shell

# Solution 3: Use fast mode
qp2-pipeline autoproc --data file.h5 --fast
qp2-pipeline xia2 --data file.h5 --fast
```

### 🐌 **Job Queue Delays**

**Diagnosis:**
```bash
# Check queue status
squeue
sinfo
showq  # if available
```

**Solutions:**
```bash
# Solution 1: Use different partition
qp2-pipeline autoproc --data file.h5 --partition express

# Solution 2: Reduce resource requirements
qp2-pipeline autoproc --data file.h5 --nproc 4

# Solution 3: Process locally
qp2-pipeline autoproc --data file.h5 --runner shell
```

## Migration-Related Issues

### ❌ **Mixed System Confusion**

**Symptoms:**
- Results appearing in unexpected locations
- Status updates not working correctly
- Different behavior from usual

**Diagnosis:**
```bash
# Check which system processed your data
find ~/autoproc_runs -name "*.log" -exec grep -l "unified\|pipeline_driver" {} \;

# Check current plugin configuration
ps aux | grep python | grep process_dataset
```

**Solutions:**
```bash
# Solution 1: Use consistent system
# Always use new system:
qp2-pipeline autoproc --data file.h5

# Or always use old system (during transition):
# Use existing plugin interfaces in image viewer

# Solution 2: Check result locations
# New system: Same directories, but check for pipeline_summary.json
ls ~/autoproc_runs/dataset_name/pipeline_summary.json
```

### ❌ **Legacy Script Conflicts**

**Symptoms:**
```
Error: Multiple pipeline systems detected
Conflicting process_dataset.py versions
```

**Solutions:**
```bash
# Solution 1: Clean up old processes
killall python  # Be careful!
# or
pkill -f process_dataset

# Solution 2: Use specific version
/path/to/qp2/bin/qp2-pipeline autoproc --data file.h5

# Solution 3: Update PATH
export PATH=/path/to/qp2/bin:$PATH
```

## Getting Help

### 📞 **Support Channels**

**Immediate Help:**
- Check this troubleshooting guide first
- Look in log files: `~/[pipeline]_runs/dataset/[pipeline].log`
- Check system status: `squeue`, `sinfo`, `redis-cli ping`

**Documentation:**
- Main guide: `qp2/pipelines/docs/USER_TRANSITION_GUIDE.md`
- Quick reference: `qp2/pipelines/docs/QUICK_REFERENCE.md`
- Command help: `qp2-pipeline --help`

**Contact Support:**
- Email: qp2-support@example.com
- Slack: #qp2-support
- Issue tracker: [GitHub/GitLab URL]

### 📋 **Information to Provide When Reporting Issues**

```bash
# Collect this information before contacting support:

# 1. Command that failed
echo "Command: qp2-pipeline autoproc --data file.h5 ..."

# 2. Error message
# Copy the exact error message

# 3. System information
hostname
whoami
which qp2-pipeline
python --version

# 4. Log files
tail -20 ~/autoproc_runs/dataset_name/autoproc.log

# 5. System status
squeue -u $USER
redis-cli ping
df -h ~/

# 6. File information
ls -la /path/to/data_master.h5
file /path/to/data_master.h5
```

### 🔧 **Self-Help Debugging**

**Enable Debug Mode:**
```bash
# Run with verbose output
qp2-pipeline autoproc --data file.h5 --work_dir ./debug_run -v

# Check detailed logs
tail -f ~/autoproc_runs/dataset/autoproc.log
```

**Test with Minimal Example:**
```bash
# Use known good dataset
qp2-pipeline autoproc \
  --data /path/to/test/data_master.h5 \
  --work_dir ./minimal_test \
  --runner shell \
  --nproc 1
```

**Isolate the Problem:**
```bash
# Test each component
qp2-pipeline --help                    # Basic functionality
qp2-pipeline autoproc --help          # Pipeline-specific help
python -c "import qp2"                 # Import test
redis-cli ping                         # Redis connectivity
```

---

**💡 Remember:**
- Most issues are configuration or environment-related
- Check log files first - they often contain the solution
- The old system still works during transition if needed
- Don't hesitate to ask for help - we're here to support you!