# QP2 Pipeline Driver Documentation

Welcome to the QP2 Pipeline Driver documentation! This comprehensive guide will help you transition to and use the new unified pipeline system.

## 📖 **Documentation Overview**

### 🚀 **Getting Started**
- **[User Transition Guide](USER_TRANSITION_GUIDE.md)** - Complete guide for users transitioning from the old system
- **[Quick Reference](QUICK_REFERENCE.md)** - Printable reference card with commands and examples
- **[Main README](../README.md)** - Technical overview and installation instructions

### 🔧 **For Administrators**
- **[Migration Checklist](MIGRATION_CHECKLIST.md)** - Step-by-step deployment and migration guide
- **[Integration Analysis](../INTEGRATION_ANALYSIS.md)** - Analysis of how new driver integrates with existing QP2 systems
- **[Integration Adapter](../integration_adapter.py)** - Backward compatibility layer implementation

### 🛠️ **Troubleshooting**
- **[Troubleshooting Guide](TROUBLESHOOTING_GUIDE.md)** - Comprehensive problem-solving guide
- **[Test Driver](../test_driver.py)** - Validation and testing utilities

### 💻 **For Developers**
- **[Pipeline Driver Source](../pipeline_driver.py)** - Main implementation
- **[Example Usage](../examples/example_usage.py)** - Programming examples and use cases

## 🎯 **Choose Your Path**

### 👤 **I'm a Regular User**
**Just want to keep doing what I'm doing?**
→ Everything continues to work as before! No changes needed.

**Want to try new features?**
→ Start with the **[Quick Reference](QUICK_REFERENCE.md)** for common examples.

**Need help with problems?**
→ Check the **[Troubleshooting Guide](TROUBLESHOOTING_GUIDE.md)** first.

**Want to understand the transition?**
→ Read the **[User Transition Guide](USER_TRANSITION_GUIDE.md)**.

### 👨‍💼 **I'm an Administrator**
**Planning the deployment?**
→ Follow the **[Migration Checklist](MIGRATION_CHECKLIST.md)** step-by-step.

**Want to understand the architecture?**
→ Review the **[Integration Analysis](../INTEGRATION_ANALYSIS.md)**.

**Need to troubleshoot user issues?**
→ Use the **[Troubleshooting Guide](TROUBLESHOOTING_GUIDE.md)**.

### 👨‍💻 **I'm a Developer**
**Integrating with the pipeline driver?**
→ Study the **[Example Usage](../examples/example_usage.py)** and **[Main README](../README.md)**.

**Maintaining the system?**
→ Review the **[Pipeline Driver Source](../pipeline_driver.py)** and **[Integration Adapter](../integration_adapter.py)**.

**Adding new features?**
→ Check the existing architecture in **[Integration Analysis](../INTEGRATION_ANALYSIS.md)**.

## 🔍 **What's New?**

### Enhanced Processing Options
- **Molecular Replacement**: Use PDB models for structure solution
- **Anomalous Data Processing**: Specialized handling for heavy atoms/selenium
- **Multiple Datasets**: Process several sweeps together
- **Custom Geometry**: Override beam center, detector distance, wavelength
- **Pipeline Variants**: Choose between XDS and nXDS for GMCA processing

### Unified Interface
- **Single Command**: `qp2-pipeline` for all processing types
- **Consistent Parameters**: Same options across all pipelines
- **Better Error Handling**: Clear messages and recovery procedures
- **Enhanced Tracking**: Comprehensive database integration

### Improved Workflows
- **Command-Line Power**: Full control for advanced users
- **Python API**: Programmatic access for automation
- **Better Monitoring**: Real-time status and progress tracking
- **Standardized Output**: Consistent result formats

## 🆘 **Quick Help**

### Common Questions
**Q: Do I need to change anything?**
A: No! All existing workflows continue to work unchanged.

**Q: Where are my results stored?**
A: Same locations as before (`~/autoproc_runs/`, `~/xia2_runs/`, etc.).

**Q: How do I access new features?**
A: Through enhanced plugin dialogs or the new `qp2-pipeline` command.

**Q: What if something breaks?**
A: Check the **[Troubleshooting Guide](TROUBLESHOOTING_GUIDE.md)** or contact support.

### Emergency Procedures
**If you encounter critical issues:**

1. **Check system status**: `squeue -u $USER`, `redis-cli ping`
2. **Review recent logs**: `tail ~/autoproc_runs/dataset_name/*.log`
3. **Try local execution**: Add `--runner shell` to commands
4. **Contact support**: Include error messages and system information

### Support Resources
- **Email**: qp2-support@example.com
- **Slack**: #qp2-support
- **Documentation**: This guide and linked resources
- **Command Help**: `qp2-pipeline --help`

## 🗂️ **Document Quick Access**

| Document | Purpose | When to Use |
|----------|---------|-------------|
| [User Transition Guide](USER_TRANSITION_GUIDE.md) | Complete user handbook | Learning about the new system |
| [Quick Reference](QUICK_REFERENCE.md) | Command examples and syntax | Daily usage and quick lookup |
| [Troubleshooting Guide](TROUBLESHOOTING_GUIDE.md) | Problem solving | When things don't work |
| [Migration Checklist](MIGRATION_CHECKLIST.md) | Deployment planning | Administrator implementation |
| [Integration Analysis](../INTEGRATION_ANALYSIS.md) | Technical architecture | Understanding system design |
| [Main README](../README.md) | Technical overview | Development and integration |

## 📅 **Transition Timeline**

```
Week 1-2:  📦 New system deployed alongside existing (no user impact)
Week 3-4:  🚀 Enhanced features available in plugins (optional use)
Month 2:   🔄 Full integration complete (automatic improvements)
Month 3+:  🎯 Complete unified system (legacy cleanup)
```

## 🎓 **Learning Path**

### **Beginner Path** (Just want basic usage)
1. Read **[Quick Reference](QUICK_REFERENCE.md)** sections: "Basic Usage" and "Common Examples"
2. Try one command: `qp2-pipeline autoproc --data your_data.h5 --work_dir ./test`
3. Bookmark **[Troubleshooting Guide](TROUBLESHOOTING_GUIDE.md)** for issues

### **Intermediate Path** (Want to use new features)
1. Review **[User Transition Guide](USER_TRANSITION_GUIDE.md)** sections: "New Features Guide"
2. Try molecular replacement: `qp2-pipeline autoproc --model search.pdb`
3. Experiment with anomalous processing: `qp2-pipeline xia2 --anomalous`

### **Advanced Path** (Want full control and automation)
1. Study **[Example Usage](../examples/example_usage.py)** for Python API
2. Read **[Main README](../README.md)** for complete technical details
3. Explore **[Pipeline Driver Source](../pipeline_driver.py)** for customization

### **Administrator Path** (Deploying and maintaining)
1. Review **[Integration Analysis](../INTEGRATION_ANALYSIS.md)** for architecture
2. Follow **[Migration Checklist](MIGRATION_CHECKLIST.md)** for deployment
3. Master **[Troubleshooting Guide](TROUBLESHOOTING_GUIDE.md)** for user support

## 📞 **Getting Help**

### Self-Service Resources
1. **Search this documentation** - Most answers are here
2. **Check command help** - `qp2-pipeline [pipeline] --help`
3. **Review log files** - Usually contain specific error information
4. **Try the troubleshooting guide** - Step-by-step problem solving

### Contact Support
When contacting support, please include:
- Exact command that failed
- Complete error message
- Output of `qp2-pipeline --help` and `hostname`
- Relevant log file excerpts

## 🔄 **Stay Updated**

This documentation is actively maintained. Key updates will be announced through:
- Email notifications to users
- Slack #qp2-announcements channel  
- Version updates in the documentation headers

**Last Updated**: [Current Date]
**Documentation Version**: 1.0
**Pipeline Driver Version**: 2.0.0

---

**Welcome to the enhanced QP2 Pipeline Driver system! 🎉**

*We've designed this transition to be as smooth as possible. Your existing workflows continue unchanged while powerful new capabilities are available when you're ready for them.*