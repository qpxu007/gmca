# GMCA Web Apps: Architecture Overview

This document explains how the Frontend (React) and Backend (FastAPI) components of the GMCA Web Apps are connected and deployed.

## High-Level Concept

The application follows a **Decoupled Client-Server Architecture**.

*   **Frontend (The Client):** A React application that runs in the **user's web browser**. It is responsible for the user interface (UI), displaying data, and handling user interactions (clicks, form inputs).
*   **Backend (The Server):** A Python FastAPI application that runs on the **server machine**. It is responsible for business logic, database access, file system operations, and serving data to the frontend via an API.

**They are tied together via HTTP Requests (REST API).** The frontend sends requests (e.g., "Get list of datasets") to the backend, and the backend responds with data (JSON).

---

## 1. How They Communicate

The communication happens over the network (localhost or internet).

1.  **User Action:** A user clicks "View Datasets" in the browser.
2.  **API Call:** The React app (running in the browser) executes JavaScript code (using `axios`) to send an HTTP GET request to a specific URL, e.g., `https://gmca.aps.anl.gov/datasets/list` (or `http://localhost:8000/datasets/list` in development).
3.  **Processing:** The Backend (FastAPI) receives this request, queries the PostgreSQL/SQLite database, and formatting the results.
4.  **Response:** The Backend sends a JSON response back to the browser.
5.  **Rendering:** The React app receives the JSON and updates the DOM to display the table of datasets.

**Key Configuration:**
The tie-in point is the `API_URL` in `web_app/frontend/src/api.js`. This tells the frontend where the backend lives.

---

## 2. Deployment Models

### A. Development (Current Setup)
*   **Machine:** Typically the same machine (your workstation or dev server).
*   **Frontend:** Runs via `vite` development server on port **5173** (e.g., `http://localhost:5173`). It serves the JS/HTML files on the fly.
*   **Backend:** Runs via `uvicorn` on port **8000** (e.g., `http://localhost:8000`).
*   **Connection:** The frontend is configured to call `http://localhost:8000`. Cross-Origin Resource Sharing (CORS) must be enabled on the backend to allow requests from port 5173 to port 8000.

### B. Production (Standard Deployment)
*   **Machine:** Typically **one server** hosts both, but they *could* be separate.
*   **Frontend:** The React app is **built** into static files (HTML, CSS, JS) using `npm run build`. These static files are served by a web server like **Nginx**.
*   **Backend:** Runs as a background service (e.g., via Systemd) on an internal port (e.g., 8000).
*   **The "Tie-In" (Nginx Reverse Proxy):**
    Nginx acts as the single entry point (port 80/443).
    *   Requests to `/` (root) -> Nginx serves the static Frontend files from the disk.
    *   Requests to `/api/*` (or specific routes like `/datasets`) -> Nginx forwards (proxies) them to the Backend running on port 8000.

    **Why this is good:**
    *   **Single Domain/Port:** The user just sees `https://gmca.aps.anl.gov`. No weird ports.
    *   **Security:** SSL/HTTPS is handled by Nginx. Backend can run in unencrypted HTTP internally.
    *   **CORS:** Since both are served from the same domain, CORS issues disappear.

### C. Separate Machines (Advanced)
*   You *could* host the static frontend files on a CDN (like Netlify or S3) and the backend on your Linux server.
*   In this case, `API_URL` in the frontend config would point to the full URL of the backend server.

---

## Summary Table

| Feature | Frontend (React) | Backend (FastAPI) |
| :--- | :--- | :--- |
| **Where it runs** | User's Browser | Server |
| **Primary Job** | UI & User Interaction | Data & Logic |
| **Language** | JavaScript (JSX) | Python |
| **Served by** | Nginx (Static files) | Uvicorn (App Server) |
| **Communication** | Sends Requests | Sends Responses |

**Do they run on the same machine?**
Physically, yes (usually). Logically, they are distinct entities that talk over a network interface.

---

## 3. Backend Modules

The backend is organized into route modules, each registered as a FastAPI router in `main.py`:

| Module | Prefix | Purpose |
| :--- | :--- | :--- |
| `main.py` | `/login`, `/logout`, `/api/*` | Auth, spreadsheet editor, SPA catch-all |
| `dataset_routes.py` | `/datasets` | Dataset discovery and listing |
| `processing_routes.py` | `/processing` | Data processing pipeline monitoring |
| `viewer_routes.py` | `/viewer` | Diffraction image viewer |
| `experiment_routes.py` | `/experiment` | Experiment preparation forms (ESAF-scoped) |
| `model_routes.py` | `/models` | Structure model upload/download/view (ESAF-scoped) |
| `prediction_routes.py` | `/predict` | Structure prediction job submission (AF3, extensible) |
| `chat_routes.py` | `/chat` | AI chat assistant |
| `scheduler.py` | `/scheduler` | Beamline support scheduler |
| `h5_routes.py` | `/h5grove` | HDF5 file serving |
| `reprocess_routes.py` | `/processing` | Dataset reprocessing |

## 4. Frontend Apps

Each dashboard card corresponds to a React page component:

| Component | Route | Purpose |
| :--- | :--- | :--- |
| `SpreadsheetApp` | `/spreadsheet` | Puck spreadsheet editor with drag-and-drop |
| `DatasetApp` | `/datasets` | Dataset viewer and search |
| `ProcessingApp` | `/processing` | Processing pipeline monitor |
| `ImageViewerApp` | `/viewer` | Diffraction image viewer |
| `ExperimentApp` | `/experiment` | Experiment preparation forms |
| `ModelViewerApp` | `/models` | Structure model management with Mol* 3D viewer |
| `ChatApp` | `/chat` | AI chat assistant |
| `SchedulerApp` | `/scheduler` | Staff scheduling (admin only) |

## 5. Structure Model & Prediction System

### Storage
- **Uploaded models:** `/mnt/beegfs/dmadmin/models/{esaf_id}/` (PDB/CIF files)
- **Prediction jobs:** `/mnt/beegfs/dmadmin/predictions/{esaf_id}/{job_id}/` (input JSON + output CIF)
- All on shared BeeGFS filesystem, accessible from API server and compute nodes

### 3D Viewer
Uses [Mol*](https://molstar.org/) (vanilla JS, framework-agnostic) for viewing PDB/CIF structures. Models are fetched with session credentials and passed as raw data to Mol*'s plugin API.

### Prediction Jobs (AlphaFold 3)
- Program-agnostic design: `prediction_routes.py` dispatches to program-specific handlers
- AF3 handler generates `fold_input.json` and submits via Slurm (`gpu` partition) using `qp2/image_viewer/utils/run_job.py`
- Job status polled via `squeue`; completed output CIF files imported into model store
- Extensible to Chai-1, Boltz, etc. via additional `_prepare_*_job()` handlers

### Access Control
All model and prediction endpoints are ESAF-scoped — users can only access models belonging to their ESAF groups. Staff members bypass access checks.

### Spreadsheet Integration
The puck editor's `ModelPath` column has a browse button that opens a model picker dialog, listing available models from the user's ESAF groups.
