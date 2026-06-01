# Deploying Data Portal with Apache Reverse Proxy

These are the complete, end-to-end instructions for deploying the data portal using the **Apache Reverse Proxy** approach. 

> [!IMPORTANT]
> **This approach is highly recommended and practically required if you have users accessing the portal from outside the APS facility.** By using the public Apache web server as an internal bridge, it completely bypasses Split-DNS mismatches and perimeter firewall port-blocking. It ensures external visitors can reliably reach your backend APIs while avoiding SSL certificate warnings.

---

## Part 1: Configure the Backend Server (`bl2ws5`)

You need to modify your FastAPI backend to listen to connections from the network (so the `www` server can talk to it) and ensure it accepts requests coming from your public domain.

### 1. Edit the systemd service file
Open the service file (usually located at `/etc/systemd/system/qp2-web-backend.service` or `/mnt/beegfs/qxu/data-analysis/qp2/web_app/qp2-web-backend.service`) and make two adjustments:

*   **Change the Host IP:** In the `ExecStart` section, change `--host 127.0.0.1` to `--host 0.0.0.0` or `--host <BACKEND_HOST_IP>`.
*   **Update CORS Origins:** Ensure the `QP2_CORS_ORIGINS` environment variable allows requests from the main web server.

Your updated variables should look like this:
```ini
# At the top of the file:
Environment="QP2_CORS_ORIGINS=https://www.gmca.aps.anl.gov"

# In the ExecStart block:
ExecStart=/mnt/beegfs/qxu/data-analysis/qp2_env/bin/python -m uvicorn main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --workers 2
```

> **Required:** The `QP2_JWT_SECRET` environment variable must be set before the backend starts. The app will refuse to start without it. The systemd service file auto-generates this on first run via `ExecStartPre`. If setting it manually:
> ```bash
> python3 -c "import secrets; print(secrets.token_urlsafe(32))"
> ```
> Store the output in your `.env` file or directly in the service file:
> ```ini
> Environment="QP2_JWT_SECRET=your-generated-secret-here"
> ```

### 2. Restart the Backend Service
Apply your changes by reloading the system daemon and safely restarting the backend server.
```bash
sudo systemctl daemon-reload
sudo systemctl restart qp2-web-backend
```

---

## Part 2: Configure the Web Server (`www.gmca.aps.anl.gov`)

We will configure Apache to serve your built React files and proxy all `/api/` traffic directly to the backend.

### 1. Create the Frontend Directory
If it doesn't already exist, create the destination folder inside your Apache `public_html`.
```bash
mkdir -p /home/qxu/public_html/data_portal
```

### 2. Configure Apache Virtual Host
You must add the reverse proxy rules and frontend fallback rules to your global Apache configuration or virtual host file. Note that `ProxyPass` **cannot be used** in an `.htaccess` file, so this must go in the main configuration file for `www.gmca.aps.anl.gov` (often located in `/etc/httpd/conf.d/`, `/etc/apache2/sites-available/`, or similar).

Add the following block:
```apache
# 1. Reverse Proxy for API Calls to Backend (bl2ws5)
ProxyPass /qxu/data_portal/api/ http://<BACKEND_HOST_IP>:8000/
ProxyPassReverse /qxu/data_portal/api/ http://<BACKEND_HOST_IP>:8000/

# 2. Allow access to your public_html directory
<Directory /home/qxu/public_html/data_portal>
    Options Indexes FollowSymLinks
    AllowOverride All
    Require all granted

    # SPA Fallback: Redirect sub-routes to index.html inside the directory
    <IfModule mod_rewrite.c>
        RewriteEngine On
        RewriteBase /qxu/data_portal/
        RewriteRule ^index\.html$ - [L]
        RewriteCond %{REQUEST_FILENAME} !-f
        RewriteCond %{REQUEST_FILENAME} !-d
        RewriteRule . /qxu/data_portal/index.html [L]
    </IfModule>
</Directory>
```
*Note: Make sure Apache's reverse proxy and rewrite modules are enabled on the `www` server. For Ubuntu/Debian systems, you can enable them with:*
```bash
sudo a2enmod proxy proxy_http rewrite headers
```
*(If using CentOS/RHEL/AlmaLinux, ensure `proxy_module` and `proxy_http_module` are correctly loaded in your main Apache config).*

### 3. Restart Apache
```bash
sudo apachectl configtest
sudo systemctl restart apache2   # (or 'sudo systemctl restart httpd' depending on OS)
```

---

## Part 3: Build and Deploy the Frontend Web App

Now we will build the static React files to route seamlessly through Apache.

### 1. Navigate to the Frontend Directory
```bash
cd /mnt/beegfs/qxu/data-analysis/qp2/web_app/frontend
```

### 2. Build the Application
Run the Vite build command injecting the custom environment variables. 
*   `VITE_BASE_PATH` tells React Router that it lives in a sub-directory. 
*   `VITE_API_URL` tells Axios to send requests locally to your Apache proxy route.
```bash
VITE_BASE_PATH=/qxu/data_portal/ VITE_API_URL=/qxu/data_portal/api npm run build
```

### 3. Copy the Files to Deployment Folder
Your static files are placed in a new `dist/` directory. Copy the contents to the `public_html` location.
```bash
cp -r dist/* /home/qxu/public_html/data_portal/
```

### Done!
You should now be able to visit `https://www.gmca.aps.anl.gov/qxu/data_portal/` and log in without encountering mixed-content errors or browser warnings.




----------------------------

----------------------------

## Alternative Approach: Direct HTTPS on the Backend

If Apache proxy modules (`mod_proxy`) cannot be enabled on your web server, you must bypass Apache proxying entirely and have the frontend directly connect to the backend server (`bl2ws5`) over HTTPS.

> [!WARNING]
> **Do not use this method if you need to support out-of-facility visitors.**
> Direct HTTPS requires the client's web browser to independently connect directly to the backend machine (e.g., `https://bl2ws5-gmca.aps.anl.gov:8000`). While this works flawlessly for internal users on the APS network, **external visitors will fail to connect**. This is due to inward/outward IP resolution mismatches (Split-DNS) on the gateway and the APS perimeter firewall unconditionally blocking external traffic to non-standard ports like `8000`.

To maximize security and potentially avoid strict browser self-signed certificate warnings, you can reuse the official site SSL certificates from the `www` server.

### 1. Copy Site Certificates to Backend Workspace
Securely copy the official Apache certificates into your backend project directory.
```bash
mkdir -p /mnt/beegfs/dmadmin/qp2 /mnt/beegfs/dmadmin/.ssl


# Assuming you have read access or root privileges to copy from /etc/apache2/cert/
cp ... www_gmca_aps_anl_gov_cert.cer /mnt/beegfs/dmadmin/.ssl
cp ... www_gmca_aps_anl_gov.key /mnt/beegfs/dmadmin/.ssl
```

### 2. Configure Backend Service to use HTTPS
Update `/etc/systemd/system/qp2-web-backend.service` (or `/mnt/beegfs/qxu/data-analysis/qp2/web_app/qp2-web-backend.service`) and update the `ExecStart` variable to inject the SSL keys into the server process:
```ini
ExecStart=/mnt/beegfs/qxu/data-analysis/qp2_env/bin/python -m uvicorn main:app \
    --host <BACKEND_HOST_IP> \
    --ssl-certfile /mnt/beegfs/dmadmin/.ssl/__aps_anl_gov_cert.cer \
    --ssl-keyfile /mnt/beegfs/dmadmin/.ssl/star.aps.anl.gov.key \
    --port 8000 \
    --workers 2

```
Apply the changes and restart the backend server:
```bash
sudo systemctl daemon-reload
sudo systemctl restart qp2-web-backend
```

### 3. Rebuild the Frontend
When building the frontend on `bl2ws5`, instruct it to connect directly to the backend's IP address and port over HTTPS instead of the `www` server's API subpath.
```bash
cd /mnt/beegfs/qxu/data-analysis/qp2/web_app/frontend

VITE_BASE_PATH=/qxu/data_portal/ VITE_API_URL=https://bl2ws5-gmca.aps.anl.gov:8000 npm run build

# Copy the built files to the www server as usual
scp -r dist/* qxu@www.gmca.aps.anl.gov:/home/qxu/public_html/data_portal/
```
*(Note: Because the API URL targets an IP address `<BACKEND_HOST_IP>` rather than a domain name `www.gmca.aps.anl.gov`, some strict browsers may still require users to manually accept a name-mismatch security warning on their first visit if they browse directly to the backend API URL).*
