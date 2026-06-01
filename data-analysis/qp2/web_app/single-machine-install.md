# Single-Machine Deployment Guide (`bl2ws8`)

This guide deploys both the Frontend (React) and the Backend (FastAPI) on a **single unified machine** (`bl2ws8-gmca.aps.anl.gov` | `<INTERNAL_IP>`).

Apache (already installed on `bl2ws8`) handles SSL termination using the official APS certificates and reverse-proxies API calls to a local Python process. The backend therefore runs as plain HTTP on `127.0.0.1`, which is faster, simpler, and never exposes uvicorn to the network.

> **Target host facts (verified 2026-05-27)**
> - OS: **Linux Mint 22.3** (Ubuntu noble base) — uses `apache2`, `www-data`, `/etc/apache2/sites-available/`. **Not** RHEL/`httpd`.
> - Globus Connect Server has been **uninstalled**, so port `443` is free for our vhost. (A harmless leftover `tls-mod-globus.conf` exists but is gated by `<IfModule globus_module>` and never fires.)
> - `Listen 80` is disabled in `/etc/apache2/ports.conf`; only `443` is bound.
> - `npm` is **not** installed on `bl2ws8`. Build the frontend on a dev machine and copy the `dist/` over, or install Node 22 LTS via `nvm` first (see Step 3).
> - `/mnt/software/data-analysis` is a **symlink** to `/mnt/beegfs/.software_bl2/data-analysis`. Tracebacks may show either path; they refer to the same files.
>
> **External dependencies the backend expects**
> - **Postgres** at `bl1ws1:5432` (database `user_data`, role `dhs`). The `dhs` role must **own** the `dataset_runs` and `pipelinestatus` tables, otherwise `CREATE INDEX` at startup logs `InsufficientPrivilege` warnings (non-fatal, but indexes won't be created). Fix once with: `ALTER TABLE dataset_runs OWNER TO dhs; ALTER TABLE pipelinestatus OWNER TO dhs;`
> - **Postgres `pg_trgm` extension** in the same DB for substring search indexes. If missing, the backend logs a single warning and skips three trigram indexes; nothing else breaks.
> - **MySQL** at `bl1upper` (`gmca_accounts`) for user/group lookups (`qp2.xio.user_group_manager`).
> - **Redis** at the addresses listed in `qp2.config` (bl2/bl1/analysis_results); used by background jobs.

---

## Architecture Overview
1. **Frontend**: Apache serves the built React bundle at `https://bl2ws8-gmca.aps.anl.gov/data_portal/`.
2. **Reverse Proxy**: Apache intercepts `/data_portal/api/*` and forwards to the local Python process.
3. **Backend**: Uvicorn on `127.0.0.1:8000`, plain HTTP. Apache owns the TLS handshake on `:443`.

---

## Step 1: Configure the Local Backend Service

Place this file at `/etc/systemd/system/qp2-web-backend.service` (a canonical copy lives in `qp2/web_app/qp2-web-backend.service`):

```ini
[Unit]
Description=QP2 Web Backend (FastAPI/Uvicorn)
Requires=mnt-beegfs.mount
After=network.target mnt-beegfs.mount

[Service]
Type=simple
User=dmadmin
Group=dmadmin

# Canonical hostname (alias of bl2ws8.gmca.aps.anl.gov)
Environment="QP2_CORS_ORIGINS=https://bl2ws8-gmca.aps.anl.gov"
Environment="QP2_DATA_DIR=/mnt/beegfs/DATA"
Environment="PYTHONPATH=/mnt/software/data-analysis:/mnt/software/data-analysis/qp2/web_app/backend"

# JWT secret — generated once, then loaded as an env var
EnvironmentFile=-/mnt/beegfs/dmadmin/qp2/.env
ExecStartPre=/bin/bash -c 'mkdir -p /mnt/beegfs/dmadmin/qp2 && if [ ! -f /mnt/beegfs/dmadmin/qp2/.jwt_secret ]; then python3 -c "import secrets; print(secrets.token_urlsafe(32))" > /mnt/beegfs/dmadmin/qp2/.jwt_secret; chmod 600 /mnt/beegfs/dmadmin/qp2/.jwt_secret; fi'
ExecStartPre=/bin/bash -c 'echo "QP2_JWT_SECRET=$(cat /mnt/beegfs/dmadmin/qp2/.jwt_secret)" > /mnt/beegfs/dmadmin/qp2/.env'

WorkingDirectory=/mnt/software/data-analysis/qp2/web_app/backend

# Plain HTTP — Apache handles TLS on :443
ExecStart=/mnt/software/data-analysis/qp2_env/bin/python -m uvicorn main:app \
    --host 127.0.0.1 \
    --port 8000 \
    --workers 2

Restart=on-failure
RestartSec=5

# --- Sandboxing (systemd) ---
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=full
ProtectHome=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictNamespaces=yes
RestrictSUIDSGID=yes
LockPersonality=yes
UMask=0077

StandardOutput=append:/var/log/qp2-web-backend.log
StandardError=inherit

[Install]
WantedBy=multi-user.target
```

Notes:
- **No `--ssl-certfile` / `--ssl-keyfile`** — that's the whole point of this method. Apache terminates TLS.
- The `ExecStartPre` block auto-generates `QP2_JWT_SECRET` on first run and persists it in `/mnt/beegfs/dmadmin/qp2/.jwt_secret`. The app crashes at startup without this secret.
- `QP2_CORS_ORIGINS` must exactly match the URL the browser uses. The correct alias is `bl2ws8-gmca.aps.anl.gov` (an alias of `bl2ws8.gmca.aps.anl.gov`); `bl2ws8-gmca.gmca.aps.anl.gov` does **not** resolve.

Activate the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now qp2-web-backend
sudo systemctl status qp2-web-backend --no-pager
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8000/docs   # expect 200
```

---

## Step 2: Configure Apache (TLS + Reverse Proxy)

### 2a. Enable required modules

```bash
sudo a2enmod ssl proxy proxy_http rewrite headers
```

### 2b. Create the vhost

Write `/etc/apache2/sites-available/qp2-data-portal.conf`:

```apache
<VirtualHost *:443>
    ServerName bl2ws8-gmca.aps.anl.gov

    # ---- TLS (APS certificates) ----
    SSLEngine on
    SSLCertificateFile      /mnt/beegfs/dmadmin/.ssl/__aps_anl_gov_cert.cer
    SSLCertificateKeyFile   /mnt/beegfs/dmadmin/.ssl/star.aps.anl.gov.key
    SSLCertificateChainFile /mnt/beegfs/dmadmin/.ssl/__aps_anl_gov_interm.cer

    # Per-vhost TLS hardening — overrides the looser global defaults
    # in /etc/apache2/mods-enabled/ssl.conf (which still permits TLSv1/1.1).
    SSLProtocol             -all +TLSv1.2 +TLSv1.3
    SSLHonorCipherOrder     on
    SSLCipherSuite          ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384
    SSLSessionTickets       off
    SSLCompression          off
    SSLUseStapling          on

    # ---- Reverse proxy: /data_portal/api/* -> local FastAPI ----
    ProxyRequests Off
    ProxyPreserveHost On
    ProxyPass        /data_portal/api/ http://127.0.0.1:8000/
    ProxyPassReverse /data_portal/api/ http://127.0.0.1:8000/

    # Hide FastAPI's interactive docs / OpenAPI schema from the public Internet.
    # Anything matching these paths is dropped before it hits the proxy.
    RedirectMatch 404 ^/data_portal/api/(docs|redoc|openapi\.json)/?$

    # SSE / chat stream — disable response buffering & pooling
    <Location /data_portal/api/chat/stream>
        SetEnv proxy-sendchunked 1
        SetEnv proxy-initial-not-pooled 1
    </Location>

    # ---- Static React bundle ----
    Alias /data_portal /var/www/html/data_portal
    <Directory /var/www/html/data_portal>
        Options -Indexes +FollowSymLinks
        AllowOverride None
        Require all granted

        # SPA fallback: any non-file/non-dir URL serves index.html
        RewriteEngine On
        RewriteBase /data_portal/
        RewriteCond %{REQUEST_FILENAME} !-f
        RewriteCond %{REQUEST_FILENAME} !-d
        RewriteRule ^ index.html [L]
    </Directory>

    # Security headers on the portal namespace
    <Location /data_portal>
        # HSTS — pin browsers to HTTPS for 1 year (no subdomain takeover risk
        # because this vhost only serves bl2ws8-gmca.aps.anl.gov)
        Header always set Strict-Transport-Security "max-age=31536000"
        Header always set X-Content-Type-Options "nosniff"
        Header always set X-Frame-Options "SAMEORIGIN"
        Header always set Referrer-Policy "strict-origin-when-cross-origin"
        Header always set Permissions-Policy "geolocation=(), camera=(), microphone=()"
    </Location>

    ErrorLog  ${APACHE_LOG_DIR}/qp2-data-portal-error.log
    CustomLog ${APACHE_LOG_DIR}/qp2-data-portal-access.log combined
</VirtualHost>

# Shared stapling cache (referenced by SSLUseStapling above).
# Must be at server scope, not inside a VirtualHost.
SSLStaplingCache shmcb:/var/run/ocsp(128000)
```

### 2c. Tighten global Apache defaults

Edit `/etc/apache2/conf-enabled/security.conf`:

```apache
ServerTokens Prod      # was: OS  — hide Apache version and OS
ServerSignature Off    # was: On  — no server banner on error pages
# TraceEnable Off      # already set by Debian default — leave alone
```

### 2d. Enable, test, reload

```bash
sudo a2ensite qp2-data-portal
sudo apachectl configtest          # must report "Syntax OK"
sudo systemctl reload apache2
```

> If you previously enabled `default-ssl`, leave it alone — Apache will dispatch by SNI on `ServerName`.

---

## Step 3: Build & Deploy the Frontend

Because Apache serves both the static files and the API under the same origin, all axios calls can be relative. No IP addresses, no CORS preflight cost.

### Node version

The frontend pins `engines.node >= 20.19.0` (required by Vite 7) and ships a `.nvmrc` selecting **Node 22 LTS**. **Do not use** Node 23/24/25 — odd-numbered releases are non-LTS and have caused bundler issues. Do not use Ubuntu's `apt install nodejs npm` either; the noble repos ship Node 18, which is below the minimum and EOL.

Install / switch with nvm:
```bash
cd /path/to/data-analysis/qp2/web_app/frontend
nvm install   # picks up .nvmrc → installs Node 22 if not already present
nvm use       # switches the current shell to Node 22
node -v && npm -v   # expect v22.x.x and 10.x
```

> Many transitive deps (molstar, moorhen, `@graphql-tools/*`) emit `npm warn deprecated` lines during `npm ci`. They are upstream noise, not failures — the install is succeeding.

### Option A — Build on a dev machine, copy to bl2ws8 (recommended)

`bl2ws8` has no `npm` installed; this avoids touching it.

```bash
# On dev machine (after `nvm use`)
cd /path/to/data-analysis/qp2/web_app/frontend
npm ci
VITE_BASE_PATH=/data_portal/ VITE_API_URL=/data_portal/api npm run build

# Copy to bl2ws8
rsync -av --delete dist/ dmadmin@bl2ws8-gmca.aps.anl.gov:/tmp/data_portal_dist/
```

Then on `bl2ws8`:
```bash
sudo mkdir -p /var/www/html/data_portal
sudo rsync -av --delete /tmp/data_portal_dist/ /var/www/html/data_portal/
sudo chown -R www-data:www-data /var/www/html/data_portal
sudo find /var/www/html/data_portal -type d -exec chmod 755 {} \;
sudo find /var/www/html/data_portal -type f -exec chmod 644 {} \;
```

### Option B — Build on bl2ws8

Only do this if you cannot build elsewhere. Install nvm per user (not via apt) to get Node 22:
```bash
# As dmadmin (one-time setup)
curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
source ~/.nvm/nvm.sh
nvm install 22

# Build
cd /mnt/software/data-analysis/qp2/web_app/frontend
nvm use     # honours the .nvmrc → 22
npm ci
VITE_BASE_PATH=/data_portal/ VITE_API_URL=/data_portal/api npm run build
sudo mkdir -p /var/www/html/data_portal
sudo rsync -av --delete dist/ /var/www/html/data_portal/
sudo chown -R www-data:www-data /var/www/html/data_portal
```

---

## Step 4: Verify

From the machine itself:
```bash
# Backend reachable on loopback (with docs still enabled locally)
curl -s -o /dev/null -w 'backend     %{http_code}\n' http://127.0.0.1:8000/docs       # expect 200

# Public surface
curl -s -o /dev/null -w 'frontend    %{http_code}\n' https://bl2ws8-gmca.aps.anl.gov/data_portal/
curl -s -o /dev/null -w 'api root    %{http_code}\n' https://bl2ws8-gmca.aps.anl.gov/data_portal/api/

# Swagger / OpenAPI MUST NOT be exposed publicly
curl -s -o /dev/null -w 'docs (blk)  %{http_code}\n' https://bl2ws8-gmca.aps.anl.gov/data_portal/api/docs         # expect 404
curl -s -o /dev/null -w 'openapi(blk) %{http_code}\n' https://bl2ws8-gmca.aps.anl.gov/data_portal/api/openapi.json # expect 404
```

Then open `https://bl2ws8-gmca.aps.anl.gov/data_portal/` in a browser — the page should load with a valid APS certificate (no warnings), and login should succeed.

### Security spot-check

```bash
# TLS protocol & cipher negotiation
openssl s_client -connect bl2ws8-gmca.aps.anl.gov:443 -servername bl2ws8-gmca.aps.anl.gov </dev/null 2>/dev/null \
  | grep -E 'Protocol|Cipher\s*:'                    # expect TLSv1.3 (or TLSv1.2) and a strong cipher

# TLS 1.0/1.1 must be rejected
openssl s_client -connect bl2ws8-gmca.aps.anl.gov:443 -tls1_1 </dev/null 2>&1 | grep -E 'alert|error' | head -1

# HSTS + secure headers present
curl -sI https://bl2ws8-gmca.aps.anl.gov/data_portal/ | grep -iE 'strict-transport|x-content-type|x-frame|referrer'

# Backend port is loopback-only — must NOT be reachable from another host
ss -tlnp 2>/dev/null | grep ':8000'                   # expect 127.0.0.1:8000, never 0.0.0.0:8000
```

If something is wrong:
- `sudo journalctl -u qp2-web-backend -n 100` — backend startup errors (often a missing `QP2_JWT_SECRET`)
- `sudo tail -f /var/log/apache2/qp2-data-portal-error.log` — proxy or TLS errors
- `sudo apachectl -S` — confirm the vhost is bound and SNI dispatch looks right
- `sudo systemd-analyze security qp2-web-backend` — confirm the sandboxing additions took effect (lower exposure score is better)

---

## Troubleshooting / Known Conditions

### Port 8000 not listening even though `systemctl status` shows the service active
The uvicorn parent stays alive while every worker crashes during module import — the listening socket never gets opened. Almost always a startup-time exception in `main.py`. Check `sudo journalctl -u qp2-web-backend -n 200` for the actual traceback. Historical examples:
- **Unpicklable APScheduler job kwargs.** If you pass a SQLAlchemy `sessionmaker` (or anything that transitively references `engine.pool._creator`) into `_scheduler.add_job(kwargs=...)`, the `SQLAlchemyJobStore` will fail to `pickle.dumps` the job with `AttributeError: Can't get local object 'create_engine.<locals>.connect'`. Pass primitives only; have the job look up its session factory at run time. This was fixed for `rcsb_routes` in commit `cf61ac9a`.

### Upgrading from a previous deploy: stale `apscheduler_jobs` rows
If an earlier version persisted broken job state into the `apscheduler_jobs` table, the new backend will keep failing to deserialize those rows. Delete the affected job IDs **once** before restart:
```sql
-- On bl1ws1 / user_data
DELETE FROM apscheduler_jobs WHERE id IN ('gmca_weekly','aps_pub_monthly');
```
`replace_existing=True` on `add_job` re-inserts them cleanly on next startup.

### Verification curl commands fail from bl2ws8 itself (hairpin NAT)

`bl2ws8-gmca.aps.anl.gov` has two IPs: **<INTERNAL_IP>** (the machine's actual interface) and **<EXTERNAL_NAT_IP>** (external/NAT). When you run the Step 4 `curl` commands from `bl2ws8`, DNS returns <EXTERNAL_NAT_IP> and the connection is dropped if the border firewall doesn't support NAT hairpinning (it usually doesn't). Use `--resolve` to bypass DNS and connect directly through the loopback:

```bash
# Drop-in replacement for all Step 4 public-surface curls
alias curl_portal='curl --resolve bl2ws8-gmca.aps.anl.gov:443:127.0.0.1'
curl_portal -s -o /dev/null -w 'frontend     %{http_code}\n' https://bl2ws8-gmca.aps.anl.gov/data_portal/
curl_portal -s -o /dev/null -w 'api root     %{http_code}\n' https://bl2ws8-gmca.aps.anl.gov/data_portal/api/
curl_portal -s -o /dev/null -w 'docs (blk)   %{http_code}\n' https://bl2ws8-gmca.aps.anl.gov/data_portal/api/docs
curl_portal -sI https://bl2ws8-gmca.aps.anl.gov/data_portal/ | grep -iE 'strict-transport|x-content-type|x-frame|referrer'
```

The backend-only check (`http://127.0.0.1:8000/docs`) is unaffected — it never goes through the NAT.

**Internal clients (on-site browsers):** same hairpin problem. The permanent fix is a split-horizon DNS entry that resolves `bl2ws8-gmca.aps.anl.gov` → `<INTERNAL_IP>` for on-site hosts. Without that, internal access works only if the APS border firewall reflects NAT traffic. Never access the app via bare IP — the TLS certificate (`*.aps.anl.gov`) won't match and CORS will break.

### `Index creation skipped: must be owner of table dataset_runs`
The `dhs` role does not own the table, so it cannot `CREATE INDEX`. Postgres has no GRANT for index creation — you must transfer ownership (see Prerequisites at the top). Index-creation failures are isolated to their own statement (db_manager runs them in AUTOCOMMIT since `0b1d8e92`), so the warning is non-fatal but real query performance suffers without the indexes.

### `Index creation skipped: extension "pg_trgm" is not available`
The trigram indexes back fast `LIKE '%...%'` lookups. Install once as a superuser:
```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

### Browser shows TLS warning / wrong certificate
Run `sudo apachectl -S` and verify the SNI dispatch line for `bl2ws8-gmca.aps.anl.gov` points at `qp2-data-portal.conf`, not `default-ssl.conf`. If both vhosts share `*:443` without distinct `ServerName`s, Apache picks the first one alphabetically — `default-ssl` wins and serves the snakeoil cert.
