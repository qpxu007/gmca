#!/usr/bin/env python3
import os
import sys
import subprocess
import shutil

def check_pyinstaller():
    """Checks if PyInstaller is installed."""
    try:
        import PyInstaller
        return True
    except ImportError:
        return False

def install_pyinstaller():
    """Installs PyInstaller via pip."""
    print("PyInstaller not found. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])

def build_frontend():
    """Builds the web_app frontend."""
    print("Building web_app frontend...")
    frontend_dir = os.path.join("web_app", "frontend")
    if not os.path.exists(frontend_dir):
        print("Frontend directory not found, skipping build.")
        return False
    
    try:
        # Check for npm
        subprocess.check_call(["npm", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.CalledProcessError):
        print("npm not found. Skipping frontend build.")
        return False

    try:
        subprocess.check_call(["npm", "install"], cwd=frontend_dir)
        subprocess.check_call(["npm", "run", "build"], cwd=frontend_dir)
        print("Frontend built successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Frontend build failed: {e}")
        return False

def generate_spec_file():
    """Generates the qp2.spec file for multi-entry bundling."""
    
    # Check if frontend dist exists
    frontend_dist = os.path.join("web_app", "frontend", "dist")
    include_frontend = os.path.exists(frontend_dist)
    
    spec_content = """# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

# Entry points
entry_points = [
    ('qp2-image-viewer', 'image_viewer/ui/main.py'),
    ('qp2-data-viewer', 'data_viewer/main.py'),
    ('qp2-dp-server', 'data_proc/server/data_processing_server.py'),
    ('qp2-dose-planner', 'radiation_decay/dose_planner.py'),
    ('qp2-spreadsheet-editor', 'spreadsheet_editor/main.py'),
    ('qp2-backup', 'backup/main.py'),
    ('qp2-web-server', 'web_app/backend/main.py'),
]

# Hidden imports to ensure they are included
hidden_imports = [
    'h5py', 'h5py.defs', 'h5py.utils', 'h5py.h5ac', 'h5py._proxy',
    'sqlalchemy', 'sqlalchemy.dialects.postgresql',
    'psycopg2',
    'redis',
    'uvicorn', 'uvicorn.logging', 'uvicorn.loops', 'uvicorn.loops.auto', 'uvicorn.protocols', 'uvicorn.protocols.http', 'uvicorn.protocols.http.auto', 'uvicorn.lifespan', 'uvicorn.lifespan.on',
    'fastapi',
    'pandas',
    'scipy', 'scipy.spatial.transform._rotation_groups',
    'skimage',
    'sklearn',
    'networkx',
    'matplotlib',
    'pyqtgraph',
    'PyQt5',
    'fabio',
    'PIL',
    'yaml',
    'gemmi',
    'passlib.handlers.bcrypt', # For auth
    'python_multipart', # For UploadFile
    'backup', # Ensure backup package is found
]

# Data files to include
# format: (source_path, dest_path)
# dest_path is relative to the bundle root
datas = [
    ('config/programs.json', 'config'),
]

# Add frontend dist if available
if True: # Logic handled outside spec for now, but hardcoded here for simplicity if path exists
    import os
    if os.path.exists('web_app/frontend/dist'):
        datas.append(('web_app/frontend/dist', 'web_app/frontend/dist'))

a_list = []
for name, script in entry_points:
    a = Analysis(
        [script],
        pathex=[],
        binaries=[],
        datas=datas,
        hiddenimports=hidden_imports,
        hookspath=[],
        hooksconfig={},
        runtime_hooks=[],
        excludes=[],
        win_no_prefer_redirects=False,
        win_private_assemblies=False,
        cipher=block_cipher,
        noarchive=False,
    )
    a_list.append((name, a))

# Create pyz for each
pyz_list = []
for name, a in a_list:
    pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
    pyz_list.append((name, pyz, a))

# Create EXE for each
exe_list = []
for name, pyz, a in pyz_list:
    exe = EXE(
        pyz,
        a.scripts,
        [],
        exclude_binaries=True,
        name=name,
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=True,
        console=True,
        disable_windowed_traceback=False,
        argv_emulation=False,
        target_arch=None,
        codesign_identity=None,
        entitlements_file=None,
    )
    exe_list.append(exe)

# Collect everything into one directory
coll_args = []
for name, pyz, a in pyz_list:
    coll_args.extend([a.binaries, a.zipfiles, a.datas])

# We need to flatten the list of executables for COLLECT
exe_args = []
for exe in exe_list:
    exe_args.append(exe)

# Combine everything
coll = COLLECT(
    *exe_args,
    *coll_args,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='qp2_bundle',
)
"""
    with open("qp2.spec", "w") as f:
        f.write(spec_content)
    print("Generated qp2.spec")

def build():
    """Runs PyInstaller."""
    print("Running PyInstaller...")
    # clean build
    if os.path.exists("build"):
        shutil.rmtree("build")
    if os.path.exists("dist"):
        shutil.rmtree("dist")
        
    subprocess.check_call(["pyinstaller", "qp2.spec"])
    
    print("\nBuild complete!")
    print(f"The bundled application is located in: {os.path.abspath('dist/qp2_bundle')}")
    print("Executables:")
    print(" - qp2-image-viewer")
    print(" - qp2-data-viewer")
    print(" - qp2-dp-server")
    print(" - qp2-dose-planner")
    print(" - qp2-spreadsheet-editor")
    print(" - qp2-backup")
    print(" - qp2-web-server")

if __name__ == "__main__":
    if not check_pyinstaller():
        print("PyInstaller is needed to bundle the application.")
        try:
            install_pyinstaller()
        except Exception as e:
            print(f"Failed to install PyInstaller: {e}")
            print("Please install it manually: pip install pyinstaller")
            sys.exit(1)
            
    # Attempt to build frontend
    build_frontend()
    
    generate_spec_file()
    build()
