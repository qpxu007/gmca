# QP2 Deployment Guide

This guide explains how to bundle and deploy the `qp2` package from your development environment (`~/data-analysis/qp2`) to a production location (e.g., `/usr/local/px/data-analysis`).

We offer two methods:
1.  **PyInstaller**: Creates a standalone, read-only "frozen" application. Best for end-users who just need to run the tools.
2.  **Conda Pack**: Bundles the entire Python environment. Best for flexibility, scripting, or if you need to install more packages later.

---

## Method 1: PyInstaller (Standalone App)

**Best for:** Easy distribution, no environment setup required for the user.

### 1. Build the Bundle
Run the build script in your project root:
```bash
cd ~/data-analysis/qp2
python3 build_standalone.py
```
*This creates a `dist/qp2_bundle` directory containing all executables and dependencies.*

### 2. Deploy to Destination
Copy the bundle to the target location.

```bash
# Create target directory if it doesn't exist
sudo mkdir -p /usr/local/px/data-analysis

# Copy the bundled application
# We use 'rsync' for efficiency, but 'cp -r' works too.
sudo rsync -av dist/qp2_bundle/ /usr/local/px/data-analysis/qp2_bundle/
```

### 3. Setup Permissions (Optional)
Ensure users can execute the binaries.
```bash
sudo chmod -R 755 /usr/local/px/data-analysis/qp2_bundle
```

### 4. Running the Tools
Users can run the tools directly:
```bash
/usr/local/px/data-analysis/qp2_bundle/qp2-image-viewer
/usr/local/px/data-analysis/qp2_bundle/qp2-data-viewer
# etc.
```

*Tip: You might want to add `/usr/local/px/data-analysis/qp2_bundle` to the system `$PATH` for easier access.*

---

## Method 2: Conda Pack (Full Environment)

**Best for:** Developers, scripting support, or if you need a writable Python environment.

### 1. Pack the Environment
Run the packing script:
```bash
cd ~/data-analysis/qp2
./build_conda_env.sh
```
*This creates `qp2_environment.tar.gz`.*

### 2. Deploy to Destination
Create a directory for the environment and unpack the tarball.

```bash
# Create target directories
sudo mkdir -p /usr/local/px/data-analysis/env
sudo mkdir -p /usr/local/px/data-analysis/src

# 1. Copy Source Code (Required for Conda Pack method as it runs from source)
# We exclude build artifacts and git history to save space
rsync -av --exclude='.git' --exclude='build' --exclude='dist' --exclude='*.tar.gz' ~/data-analysis/qp2/ /usr/local/px/data-analysis/src/

# 2. Deploy Environment
# Copy the packed environment
sudo cp qp2_environment.tar.gz /usr/local/px/data-analysis/

# Unpack it
sudo tar -xzf /usr/local/px/data-analysis/qp2_environment.tar.gz -C /usr/local/px/data-analysis/env/

# Clean up
sudo rm /usr/local/px/data-analysis/qp2_environment.tar.gz
```

### 3. Fix Paths
After unpacking a Conda environment in a new location, you must source the activation script to fix hardcoded paths.
```bash
source /usr/local/px/data-analysis/env/bin/activate
conda-unpack # This is run once automatically usually, but good to ensure
```

### 4. Running the Tools
To run tools, users must activate the environment and run the python module.

**Wrapper Script Example:**
You can create a simple wrapper script for users (e.g., in `/usr/local/bin/qp2-iv`):

```bash
#!/bin/bash
source /usr/local/px/data-analysis/env/bin/activate
export PYTHONPATH=$PYTHONPATH:/usr/local/px/data-analysis/src
python -m qp2.image_viewer.ui.main "$@"
```

---

## Summary Comparison

| Feature | PyInstaller (`dist/qp2_bundle`) | Conda Pack (`env/`) |
| :--- | :--- | :--- |
| **Location** | `/usr/local/px/data-analysis/qp2_bundle` | `/usr/local/px/data-analysis/env` + `/src` |
| **Dependencies** | All included, frozen. | All included, full python. |
| **Source Code** | Hidden/compiled inside executables. | **Must copy source code separately** to run it. |
| **Usage** | `./qp2-image-viewer` | `activate` -> `python -m ...` |
| **Updates** | Re-deploy bundle. | `git pull` in `/src` (fast updates). |
