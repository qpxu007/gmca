# Deploying GMCA Web Apps via HTTPS

This guide provides detailed instructions on how to deploy the GMCA Web Apps (frontend and backend) securely using HTTPS. We will use **Nginx** as a reverse proxy to handle SSL/TLS termination and forward requests to the application.

## Prerequisites

1.  **Domain Name:** A valid domain name pointing to your server's IP address (e.g., `gmca.aps.anl.gov`).
2.  **Server Access:** Root or sudo access to the Linux server.
3.  **Running Application:** The backend (FastAPI/Uvicorn) and frontend (React/Vite) should be ready to run.

---

## 1. Prepare the Application

### Backend (FastAPI)
Ensure your backend is running on a local port (e.g., `8000`). For production, use a process manager like **Systemd** or **Supervisor**.

**Example Systemd Service (`/etc/systemd/system/gmca-backend.service`):**
```ini
[Unit]
Description=GMCA Backend API
After=network.target

[Service]
User=qxu
Group=qxu
WorkingDirectory=/home/qxu/data-analysis/qp2
Environment="PATH=/home/qxu/data-analysis/qp2/venv/bin"
ExecStart=/home/qxu/data-analysis/qp2/venv/bin/uvicorn web_app.backend.main:app --host 127.0.0.1 --port 8000 --workers 4
Restart=always

[Install]
WantedBy=multi-user.target
```
*Note: Adjust paths and user/group as necessary.*

### Frontend (React)
For production, **build** the React application instead of running the dev server (`vite`).

1.  Navigate to the frontend directory:
    ```bash
    cd web_app/frontend
    ```
2.  Run the build command:
    ```bash
    npm run build
    ```
    This creates a `dist` folder containing the static files (`index.html`, JS, CSS).

---

## 2. Install and Configure Nginx

1.  **Install Nginx:**
    ```bash
    sudo apt update
    sudo apt install nginx
    ```

2.  **Create an Nginx Configuration File:**
    Create a new file `/etc/nginx/sites-available/gmca-webapp`.

    ```nginx
    server {
        listen 80;
        server_name gmca.aps.anl.gov;  # Replace with your domain

        # Redirect HTTP to HTTPS
        return 301 https://$host$request_uri;
    }

    server {
        listen 443 ssl http2;
        server_name gmca.aps.anl.gov;  # Replace with your domain

        # SSL Configuration (See Section 3 for Certificates)
        ssl_certificate /etc/letsencrypt/live/gmca.aps.anl.gov/fullchain.pem;
        ssl_certificate_key /etc/letsencrypt/live/gmca.aps.anl.gov/privkey.pem;
        include /etc/letsencrypt/options-ssl-nginx.conf;
        ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

        # Frontend: Serve Static Files
        location / {
            root /home/qxu/data-analysis/qp2/web_app/frontend/dist;
            index index.html;
            try_files $uri $uri/ /index.html;
        }

        # Backend: Reverse Proxy API Requests
        location /api/ {  # Assuming you prefix API calls or root
            # If your backend is at root (e.g. /login, /scheduler), use specific locations or prefix
            # Recommended: Configure frontend to call /api/... and rewrite here, OR match backend routes.
            
            # Since current app uses root routes (/login, /scheduler, /datasets), we can proxy those specific paths OR use a catch-all if frontend doesn't match.
            # BETTER STRATEGY: Proxy everything that isn't a static file? 
            # OR explicitly list backend routes:
            
            proxy_pass http://127.0.0.1:8000;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }

        # Specific Backend Routes (Update based on your API)
        location ~ ^/(login|scheduler|datasets|processing|h5grove|spreadsheets|upload|create_empty|export|send_to_http) {
            proxy_pass http://127.0.0.1:8000;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
    ```

3.  **Enable the Site:**
    ```bash
    sudo ln -s /etc/nginx/sites-available/gmca-webapp /etc/nginx/sites-enabled/
    sudo nginx -t  # Test configuration
    sudo systemctl restart nginx
    ```

---

## 3. Configure SSL/TLS (HTTPS)

### Option A: Let's Encrypt (Certbot) - Recommended
If your server is publicly accessible:

1.  **Install Certbot:**
    ```bash
    sudo apt install certbot python3-certbot-nginx
    ```
2.  **Obtain Certificate:**
    ```bash
    sudo certbot --nginx -d gmca.aps.anl.gov
    ```
    Follow the prompts. Certbot will automatically modify your Nginx config to add the SSL certificate paths.

### Option B: Self-Signed Certificate (Internal/Testing)
1.  **Generate Certificate:**
    ```bash
    sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/ssl/private/nginx-selfsigned.key -out /etc/ssl/certs/nginx-selfsigned.crt
    ```
2.  **Update Nginx Config:**
    Point `ssl_certificate` and `ssl_certificate_key` to these new files.

---

## 4. Application Configuration Updates

### Frontend (`api.js` / `config.js`)
Update the API base URL in your frontend configuration to point to the HTTPS domain instead of `localhost`.

*   **File:** `web_app/frontend/src/api.js` (and `H5Viewer.jsx`, `DatasetApp.jsx` if hardcoded)
*   **Change:**
    ```javascript
    // const API_URL = 'http://localhost:8000';
    const API_URL = 'https://gmca.aps.anl.gov'; // Your domain
    # OR relative if served from same domain
    const API_URL = ''; 
    ```
    *Using a relative URL (`''`) is best when Nginx serves both frontend and backend on the same domain.*

### Backend (`main.py`)
1.  **CORS:** Update `allow_origins` in `CORSMiddleware`.
    ```python
    origins = [
        "https://gmca.aps.anl.gov",
        "http://localhost:5173", # Keep for local dev if needed
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        # ...
    )
    ```
2.  **Cookies:** If using cookies for auth, ensure `secure=True` and `samesite='Lax'` (or 'Strict') are set when running over HTTPS.

---

## 5. Globus & H5Grove

*   **Globus:** Ensure the Redirect URIs in your Globus Developer Console allow `https://gmca.aps.anl.gov/...`.
*   **H5Grove:** The backend environment variable `H5GROVE_BASE_DIR` should be set in the Systemd service file (as shown in Step 1).

## 6. Verification

1.  Open `https://gmca.aps.anl.gov` in your browser.
2.  Ensure the "Lock" icon appears in the address bar.
3.  Test Login, Dataset Viewer, and Processing tabs.
4.  Check browser console for any Mixed Content errors (loading HTTP resources on HTTPS page).
