# QP2 Standalone Bundle

This directory contains scripts and configurations to bundle QP2 as a standalone application that does not depend on the system Python distribution.

## How to Build

1.  Ensure you have the development environment set up (dependencies installed).
2.  Ensure `npm` is installed for building the frontend.
3.  Run the build script:
    ```bash
    python3 build_standalone.py
    ```
    This script will:
    *   Install `pyinstaller` if missing.
    *   Build the web frontend (`npm run build`).
    *   Generate `qp2.spec`.
    *   Run PyInstaller to create the bundle.

## Output

The bundled application is created in `dist/qp2_bundle`.
This folder contains everything needed to run the application (Python runtime, libraries, executables).

**Executables:**
*   `dist/qp2_bundle/qp2-image-viewer` (Image Viewer)
*   `dist/qp2_bundle/qp2-data-viewer` (Data Viewer)
*   `dist/qp2_bundle/qp2-dp-server` (Data Processing Server)
*   `dist/qp2_bundle/qp2-dose-planner` (Dose Planner)
*   `dist/qp2_bundle/qp2-spreadsheet-editor` (Spreadsheet Editor)
*   `dist/qp2_bundle/qp2-backup` (Backup Tool)
*   `dist/qp2_bundle/qp2-web-server` (Web Application Server)

## Web Application

The `qp2-web-server` executable runs the backend API and serves the frontend static files.
*   Run `./qp2-web-server`
*   Open browser at `http://localhost:8000` (or configured port).

## Distribution

To distribute the application to other Linux machines (assuming they have compatible glibc, which is usually true for recent distributions):

1.  Zip or Tar the `qp2_bundle` directory:
    ```bash
    cd dist
    tar -czvf qp2_bundle.tar.gz qp2_bundle
    ```
2.  Copy `qp2_bundle.tar.gz` to the target machine.
3.  Extract and run:
    ```bash
    tar -xzvf qp2_bundle.tar.gz
    cd qp2_bundle
    ./qp2-image-viewer
    ```

## Notes

*   **Configuration:** The `config/programs.json` is included in the bundle.
*   **External Programs:** The application still relies on external crystallographic software (XDS, dials, etc.) referenced in `config/programs.json`. These are NOT bundled. Ensure the target machine has access to these programs or configure them via `QP2_PROGRAMS_CONFIG` or environment variables.
*   **Bin Scripts:** The scripts in the original `qp2/bin` directory are shell wrappers around python modules. The bundled executables replace these directly.