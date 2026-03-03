# Login Process and Authentication Flow

This document explains what happens technically when a user logs into the GMCA Web Apps.

## 1. The Login Request (Frontend -> Backend)

1.  **User Action:** The user enters their username and password in the login form (`Login.jsx`) and clicks "Sign In".
2.  **API Call:** The React application sends an **HTTP POST** request to the backend endpoint `/login`.
    *   **Payload:** `{ "username": "...", "password": "..." }`

## 2. Authentication (Backend)

The request is handled by `login` function in `web_app/backend/main.py`.

1.  **Verification:** The backend calls `auth.check_gmca_pw(username, password)`.
    *   This function checks the credentials against the configured authentication source (e.g., system LDAP/Kerberos).
    *   *Note: In development/fallback mode, it might accept specific test credentials (e.g., admin/admin).*
2.  **Validation:**
    *   **If Invalid:** The backend returns an HTTP 401 error. The frontend displays "Invalid credentials".
    *   **If Valid:** The process continues to Token Generation.

## 3. Token Generation (Backend)

If the user is authenticated:

1.  **Create JWT:** The backend calls `security.create_access_token(username)`.
    *   It constructs a payload containing the username (`sub`) and an expiration time (`exp`, typically 24 hours).
    *   It signs this payload using a **SECRET_KEY** and the **HS256** algorithm to produce a **JSON Web Token (JWT)**.
2.  **Check Permissions:** It checks `auth.is_staff_member(username)` to determine if the user has administrative privileges.
3.  **Response:** The backend sends a JSON response to the client:
    ```json
    {
      "success": true,
      "token": "eyJhbGciOiJIUzI1Ni...", 
      "user": "username",
      "is_admin": true
    }
    ```

## 4. Session Storage (Frontend)

1.  **Store Token:** The frontend (`Login.jsx`) receives the response and stores the `token` in the browser's **localStorage**.
    *   `localStorage.setItem('token', response.data.token)`
2.  **Redirect:** The user is redirected to the Dashboard (`/dashboard`).

## 5. Authenticated Requests (Subsequent Interactions)

Once logged in, the frontend needs to prove its identity for every API call (e.g., fetching datasets, saving schedules).

1.  **Interceptor:** The `api.js` file configures an `axios` interceptor.
2.  **Attach Header:** Before any request is sent, the interceptor checks `localStorage`.
    *   If a token exists, it adds an HTTP Header:
        `Authorization: Bearer <token>`
3.  **Backend Verification:**
    *   Protected backend endpoints include a dependency: `user: str = Depends(verify_token)`.
    *   The `verify_token` function (in `security.py`):
        1.  Extracts the token from the `Authorization` header.
        2.  Decodes the token using the same `SECRET_KEY`.
        3.  Verifies the signature (ensuring the token wasn't tampered with).
        4.  Checks the expiration time.
        5.  Returns the `username` from the token payload.
    *   If the token is invalid or expired, the backend returns HTTP 401, and the frontend usually redirects the user back to the login page.
