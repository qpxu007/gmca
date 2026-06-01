# Login Process and Authentication Flow

This document explains what happens technically when a user logs into the GMCA Web Apps.

## 1. The Login Request (Frontend -> Backend)

1.  **User Action:** The user enters their username and password in the login form (`Login.jsx`) and clicks "Sign In".
2.  **API Call:** The React application sends an **HTTP POST** request to the backend endpoint `/login` with `withCredentials: true` (so the browser accepts the session cookie in the response).
    *   **Payload:** `{ "username": "...", "password": "..." }`

## 2. Rate Limiting (Backend)

Before any credential check, the backend applies **per-IP rate limiting**:
*   A maximum of **10 login attempts per IP address per 60-second window** is enforced.
*   If exceeded, the backend returns **HTTP 429** ("Too many login attempts. Try again later.").
*   The frontend displays a distinct error message for this case.

## 3. Authentication (Backend)

The request is handled by the `login` function in `web_app/backend/main.py`.

1.  **Verification:** The backend calls `auth.check_gmca_pw(username, password)`.
    *   This function checks the credentials against Kerberos (`anl.gov` realm) first, then falls back to LDAP (`bl1upper.gmca.aps.anl.gov`).
    *   *Note: When `QP2_ENV=test`, it accepts test credentials (`admin`/`admin` and `user`/`user`).*
2.  **Validation:**
    *   **If Invalid:** The backend returns an HTTP 401 error. The frontend displays "Invalid credentials".
    *   **If Valid:** The process continues to Token Generation.

## 4. Token Generation (Backend)

If the user is authenticated:

1.  **Create JWT:** The backend calls `security.create_access_token(username)`.
    *   It constructs a payload containing:
        *   `sub` — the username
        *   `exp` — expiration time (24 hours from now)
        *   `jti` — a unique token ID (UUID) used for revocation tracking
    *   It signs this payload using `QP2_JWT_SECRET` and the **HS256** algorithm to produce a **JSON Web Token (JWT)**.
    *   The function returns a `(token, jti)` tuple.
2.  **Check Permissions:** It checks `auth.is_staff_member(username)` to determine if the user has administrative privileges.
3.  **Set Cookie:** The backend sets the JWT as an **HttpOnly, Secure, SameSite=Lax** cookie named `qp2_session` on the response. The cookie has a `max_age` matching the token lifetime (24 hours).
4.  **Response:** The backend sends a JSON response (without the raw token):
    ```json
    {
      "success": true,
      "user": "username",
      "is_admin": true,
      "beamline": "23IDB",
      "groups": ["esaf12345", "esaf67890"]
    }
    ```

> **Security note:** The JWT is never exposed to JavaScript. It is stored exclusively in an `HttpOnly` cookie, which protects it from XSS attacks. The `Secure` flag ensures it is only sent over HTTPS (disabled in test mode for local development).

## 5. Session Storage (Frontend)

1.  **Cookie:** The browser automatically stores the `qp2_session` cookie set by the backend. This cookie is invisible to JavaScript (`HttpOnly`).
2.  **Local metadata:** The frontend stores only non-sensitive display metadata in `localStorage`:
    *   `user` — username (for display purposes and route guarding)
    *   `is_admin` — admin flag (for UI display only, not security)
    *   `beamline` — current beamline (for chat room selection)
    *   `groups` — list of ESAF group names (for UI filtering)
3.  **Redirect:** The user is redirected to the Dashboard (`/dashboard`).

## 6. Authenticated Requests (Subsequent Interactions)

Once logged in, the browser automatically proves the user's identity on every API call.

1.  **Cookie transmission:** The `axios` instance in `api.js` is configured with `withCredentials: true`. This causes the browser to automatically attach the `qp2_session` cookie to every request to the same origin.
2.  **No explicit header:** Unlike the previous `Authorization: Bearer <token>` approach, no JavaScript code touches or reads the token. The browser handles cookie transmission automatically.
3.  **Backend Verification:**
    *   Protected backend endpoints include a dependency: `user: str = Depends(verify_token)`.
    *   The `verify_token` function (in `security.py`):
        1.  Checks for a token in the `Authorization` header first (backward compatibility).
        2.  Falls back to reading the `qp2_session` cookie.
        3.  Decodes the token using `QP2_JWT_SECRET`.
        4.  Verifies the signature (ensuring the token wasn't tampered with).
        5.  Checks the expiration time.
        6.  Checks whether the token's `jti` has been revoked (see Logout below).
        7.  Returns the `username` from the token payload.
    *   If the token is invalid, expired, or revoked, the backend returns HTTP 401, and the frontend redirects the user back to the login page.

## 7. Logout and Token Revocation

1.  **User Action:** The user clicks "Logout" on the Dashboard.
2.  **API Call:** The frontend sends `POST /logout` with `withCredentials: true`.
3.  **Backend:**
    *   Decodes the current token to extract the `jti` and `exp` claims.
    *   Adds the `jti` to an **in-memory revocation set** (the entry auto-expires when the token's natural expiry passes).
    *   Clears the `qp2_session` cookie from the response.
4.  **Frontend:** Removes `user`, `is_admin`, `beamline`, and `groups` from `localStorage`, then redirects to `/login`.

> **Note:** The revocation set is in-memory and resets on backend restart. This means a previously-issued token could theoretically be reused after a restart, but it will still be subject to its natural 24-hour expiry. This tradeoff avoids a Redis dependency.

## 8. SSE (Server-Sent Events) Authentication

The chat feature uses Server-Sent Events via the `/chat/stream` endpoint. Since `EventSource` cannot set custom HTTP headers:

1.  The browser sends the `qp2_session` cookie automatically (configured with `withCredentials: true` on the `EventSource` constructor).
2.  The backend reads the cookie directly from the request, decodes and validates the JWT.
3.  A `?token=` query param fallback is retained for backward compatibility during migration but is not used by the current frontend.

## 9. Security Headers

The backend middleware adds the following headers to every response:

| Header | Value | Purpose |
|--------|-------|---------|
| `Strict-Transport-Security` | `max-age=31536000; includeSubDomains` | Forces HTTPS for 1 year (production only) |
| `X-Content-Type-Options` | `nosniff` | Prevents MIME-type sniffing |
| `X-Frame-Options` | `SAMEORIGIN` | Prevents clickjacking |
| `Referrer-Policy` | `strict-origin-when-cross-origin` | Limits referrer leakage |

Apache/Nginx configs additionally set a `Content-Security-Policy` header.

## 10. Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `QP2_JWT_SECRET` | **Yes** (production) | JWT signing key. App crashes on startup if unset (except when `QP2_ENV=test`). Generate with: `python -c "import secrets; print(secrets.token_urlsafe(32))"` |
| `QP2_ENV` | No | Set to `test` for development mode (allows test credentials, uses non-secure cookies, uses fallback JWT secret) |
