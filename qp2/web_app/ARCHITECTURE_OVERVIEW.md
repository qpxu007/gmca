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
