# QP2 Pipeline Driver Migration Checklist

## Pre-Migration Preparation

### 🔍 **System Assessment**
- [ ] **Current pipeline usage audit**
  - [ ] Identify all active users and their typical workflows
  - [ ] Document current processing volumes and patterns
  - [ ] List custom scripts and automation that depend on existing pipelines
  - [ ] Map current directory structures and file locations

- [ ] **Infrastructure readiness**
  - [ ] Verify SLURM cluster configuration and availability
  - [ ] Check Redis server status and connectivity
  - [ ] Confirm MySQL/database connectivity for tracking
  - [ ] Validate disk space in user processing directories
  - [ ] Test network connectivity to compute nodes

- [ ] **Backup and safety measures**
  - [ ] Backup current pipeline configurations
  - [ ] Export current Redis database state
  - [ ] Document rollback procedures for each phase
  - [ ] Create test user accounts for validation

### 📋 **Testing Environment Setup**
- [ ] **Deploy test instance**
  - [ ] Install pipeline driver in test environment
  - [ ] Configure test Redis and database connections
  - [ ] Set up test user accounts and permissions
  - [ ] Prepare test datasets representing typical usage

- [ ] **Validation test suite**
  - [ ] Test each pipeline type (AutoPROC, Xia2, nXDS, Strategy)
  - [ ] Verify result file formats and locations match current system
  - [ ] Validate Redis status tracking continues working
  - [ ] Check database integration and tracking accuracy
  - [ ] Performance benchmark against current system

## Phase 1: Compatibility Layer Deployment (Week 1)

### 📦 **Install Pipeline Driver**
- [ ] **Code deployment**
  - [ ] Deploy `qp2/pipelines/pipeline_driver.py` to production
  - [ ] Install `qp2/pipelines/integration_adapter.py`
  - [ ] Add `qp2/bin/qp2-pipeline` executable to system PATH
  - [ ] Update Python package installation (`pip install -e .`)

- [ ] **Configuration setup**
  - [ ] Create system-wide configuration file `/etc/qp2/pipeline_config.yaml`
  - [ ] Set up user configuration template `~/.qp2/pipeline_config.yaml`
  - [ ] Configure Redis connection parameters
  - [ ] Set up database connection strings and credentials

- [ ] **Permission and access**
  - [ ] Verify file permissions on pipeline scripts
  - [ ] Test SLURM submission from pipeline driver
  - [ ] Validate access to processing directories
  - [ ] Check log file creation and rotation

### ✅ **Initial Testing**
- [ ] **Smoke tests**
  - [ ] Run `qp2-pipeline --help` from user accounts
  - [ ] Test basic pipeline execution with small datasets
  - [ ] Verify no interference with existing plugin workflows
  - [ ] Check that current Redis tracking continues unchanged

- [ ] **User notification**
  - [ ] Send announcement about new capabilities
  - [ ] Provide link to documentation and quick reference
  - [ ] Set up support channels for questions
  - [ ] Schedule optional training sessions

## Phase 2: Plugin Integration (Week 2-3)

### 🔧 **Update Plugin Components**
- [ ] **Generate compatibility wrappers**
  - [ ] Run `python qp2/pipelines/integration_adapter.py`
  - [ ] Verify generation of `*_process_dataset_unified.py` scripts
  - [ ] Test wrapper scripts maintain exact command-line compatibility
  - [ ] Validate wrapper script permissions and execution

- [ ] **Plugin manager updates**
  - [ ] Update `submit_autoproc_job.py` to use `PluginCompatibilityAdapter`
  - [ ] Update `submit_xia2_job.py` with adapter integration
  - [ ] Update `submit_nxds_job.py` for unified nXDS/XDS handling
  - [ ] Modify any custom submission scripts

- [ ] **GUI enhancements**
  - [ ] Add new parameter fields to processing dialogs
  - [ ] Implement model file browser for molecular replacement
  - [ ] Add anomalous data processing checkboxes
  - [ ] Create beam center override controls
  - [ ] Add pipeline variant selection (XDS vs nXDS)

### 🧪 **Enhanced Feature Testing**
- [ ] **New parameter validation**
  - [ ] Test molecular replacement workflows with PDB models
  - [ ] Validate anomalous data processing with selenomethionine datasets
  - [ ] Check beam center override functionality
  - [ ] Test resolution range controls (high and low limits)
  - [ ] Verify wavelength override capabilities

- [ ] **Multi-dataset processing**
  - [ ] Test frame range specification (`file.h5:start:end`)
  - [ ] Validate multiple dataset submission
  - [ ] Check scaling reference dataset functionality
  - [ ] Test batch processing workflows

## Phase 3: Server Integration (Week 4-6)

### 🖥️ **Data Processing Server Updates**
- [ ] **Server component migration**
  - [ ] Integrate pipeline driver into `data_processing_server.py`
  - [ ] Update `analysis_manager.py` to use unified driver
  - [ ] Modify REST API endpoints for enhanced parameters
  - [ ] Update job scheduling and resource management

- [ ] **Database schema updates**
  - [ ] Verify `PipelineTracker` integration working correctly
  - [ ] Check `DataProcessResults` table population
  - [ ] Validate `ScreenStrategyResults` for strategy calculations
  - [ ] Test tracking of enhanced metadata (model files, anomalous flags, etc.)

- [ ] **API compatibility**
  - [ ] Ensure existing API clients continue working
  - [ ] Test new API endpoints for enhanced features
  - [ ] Validate JSON response formats maintain compatibility
  - [ ] Check authentication and authorization still working

### 📊 **Performance Monitoring**
- [ ] **Metrics collection**
  - [ ] Monitor pipeline execution times vs baseline
  - [ ] Track SLURM job submission success rates
  - [ ] Measure database update performance
  - [ ] Monitor Redis memory usage and response times
  - [ ] Check system resource utilization (CPU, memory, disk)

- [ ] **User experience tracking**
  - [ ] Collect user feedback on new features
  - [ ] Monitor support ticket volume and types
  - [ ] Track adoption rate of enhanced features
  - [ ] Document any workflow disruptions or issues

## Phase 4: Full Migration (Month 2+)

### 🔄 **Legacy System Retirement**
- [ ] **Gradual transition**
  - [ ] Monitor usage of old vs new pipeline scripts
  - [ ] Identify any remaining dependencies on legacy code
  - [ ] Plan retirement timeline for old scripts
  - [ ] Notify users of upcoming legacy system removal

- [ ] **Code cleanup**
  - [ ] Remove old `*_process_dataset.py` scripts
  - [ ] Clean up redundant pipeline implementations
  - [ ] Consolidate configuration files
  - [ ] Update documentation to remove legacy references

- [ ] **System optimization**
  - [ ] Optimize resource allocation for unified system
  - [ ] Tune database connection pooling
  - [ ] Adjust SLURM job templates for better efficiency
  - [ ] Implement advanced caching where beneficial

### 📚 **Training and Documentation**
- [ ] **User training program**
  - [ ] Conduct hands-on workshops for common workflows
  - [ ] Create video tutorials for enhanced features
  - [ ] Develop troubleshooting guides
  - [ ] Train local support staff

- [ ] **Documentation updates**
  - [ ] Update all user manuals and wikis
  - [ ] Create administrator deployment guide
  - [ ] Document new API endpoints and parameters
  - [ ] Update integration documentation for developers

## Validation and Testing Checklists

### 🧪 **Pre-Deployment Testing**
- [ ] **Functionality tests**
  - [ ] AutoPROC: Basic processing, molecular replacement, anomalous data
  - [ ] Xia2: Single sweep, multi-sweep, DIALS vs XDS modes
  - [ ] nXDS: Serial crystallography, reference scaling, ice ring handling
  - [ ] XDS: Traditional processing, space group constraints
  - [ ] Strategy: MOSFLM and XDS strategy calculations

- [ ] **Integration tests**
  - [ ] Plugin GUI → unified driver workflows
  - [ ] Data processing server → unified driver execution
  - [ ] Redis status tracking consistency
  - [ ] Database result storage accuracy
  - [ ] File system permissions and directory creation

- [ ] **Performance tests**
  - [ ] Large dataset processing (>1000 images)
  - [ ] Concurrent job execution (multiple users)
  - [ ] Resource utilization under load
  - [ ] Database performance with high insert rates
  - [ ] Network bandwidth usage patterns

### 🔍 **Post-Deployment Monitoring**
- [ ] **Weekly checks (first month)**
  - [ ] Pipeline success/failure rates
  - [ ] Average processing times vs baseline
  - [ ] User error reports and support tickets
  - [ ] System resource utilization trends
  - [ ] Database growth and performance

- [ ] **Monthly assessments**
  - [ ] User satisfaction surveys
  - [ ] Feature adoption metrics
  - [ ] Performance optimization opportunities
  - [ ] Support workload analysis
  - [ ] Capacity planning updates

## Rollback Procedures

### ⚠️ **Emergency Rollback (If Required)**
- [ ] **Phase 1 rollback**
  - [ ] Remove unified driver from PATH
  - [ ] Restore original pipeline script permissions
  - [ ] Verify existing plugin workflows still function
  - [ ] Communicate rollback status to users

- [ ] **Phase 2 rollback**
  - [ ] Revert plugin submission workers to original versions
  - [ ] Remove unified wrapper scripts
  - [ ] Restore original GUI dialog configurations
  - [ ] Test all plugin workflows functioning normally

- [ ] **Phase 3 rollback**
  - [ ] Revert data processing server to original implementation
  - [ ] Restore original analysis manager
  - [ ] Verify API endpoints working correctly
  - [ ] Check database integrity and tracking

### 📋 **Rollback Validation**
- [ ] Test all original workflows continue working
- [ ] Verify no data loss or corruption occurred
- [ ] Check that user directories and permissions intact
- [ ] Confirm Redis and database state consistent
- [ ] Document lessons learned and improvement plans

## Success Criteria

### ✅ **Phase Completion Criteria**

**Phase 1 Success:**
- [ ] All existing workflows continue unchanged
- [ ] New pipeline driver accessible via command line
- [ ] No performance degradation observed
- [ ] Zero user-reported issues with existing functionality

**Phase 2 Success:**
- [ ] Enhanced plugin features working correctly
- [ ] Users successfully adopting new molecular replacement workflows
- [ ] Anomalous data processing validated by users
- [ ] No increase in support ticket volume

**Phase 3 Success:**
- [ ] Data processing server fully migrated
- [ ] API clients working with new backend
- [ ] Database tracking enhanced and accurate
- [ ] Performance equal or better than baseline

**Phase 4 Success:**
- [ ] Legacy systems cleanly retired
- [ ] Users trained on new capabilities
- [ ] Documentation complete and accurate
- [ ] Support team comfortable with new system

### 📊 **Key Performance Indicators**
- **Uptime**: >99.9% availability throughout migration
- **Performance**: Processing times within 5% of baseline
- **User Satisfaction**: >90% positive feedback on enhanced features
- **Support Load**: No more than 10% increase in support tickets
- **Adoption**: >50% of users utilizing at least one new feature within 3 months

---

**📞 Emergency Contacts:**
- System Administrator: [contact info]
- Database Administrator: [contact info] 
- Pipeline Developer: [contact info]
- User Support: [contact info]

**📋 Status Tracking:**
Use this checklist to track migration progress. Check off completed items and note any issues or deviations in comments.