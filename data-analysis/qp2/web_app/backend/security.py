import os
import logging
import datetime
import threading
import uuid

import jwt
from fastapi import HTTPException, Security, Request, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# JWT secret — mandatory in production, permissive in test
# ---------------------------------------------------------------------------
SECRET_KEY = os.environ.get("QP2_JWT_SECRET")
if not SECRET_KEY:
    if os.environ.get("QP2_ENV") == "test":
        SECRET_KEY = "test-secret-not-for-production"
        logger.warning("Using test JWT secret (QP2_ENV=test). Not for production.")
    else:
        raise RuntimeError(
            "QP2_JWT_SECRET environment variable is required. "
            'Generate one with: python -c "import secrets; print(secrets.token_urlsafe(32))"'
        )

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours
REFRESH_THRESHOLD_MINUTES = 120        # re-issue when < 2 h remain

# ---------------------------------------------------------------------------
# Cookie configuration
# ---------------------------------------------------------------------------
COOKIE_NAME = "qp2_session"
_IS_PROD = os.environ.get("QP2_ENV") != "test"


def set_auth_cookie(response: Response, token: str):
    """Set an HttpOnly, Secure session cookie on the response."""
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        secure=_IS_PROD,
        samesite="lax",
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        path="/",
    )


def clear_auth_cookie(response: Response):
    """Delete the session cookie."""
    response.delete_cookie(key=COOKIE_NAME, path="/")


# ---------------------------------------------------------------------------
# In-memory token revocation (no Redis required)
# ---------------------------------------------------------------------------
_revoked_jtis: dict = {}  # jti -> expiry timestamp (float)
_revoked_lock = threading.Lock()


def revoke_token(jti: str, exp_timestamp: float):
    """Mark a JTI as revoked until its natural expiry."""
    with _revoked_lock:
        _revoked_jtis[jti] = exp_timestamp


def _cleanup_expired():
    """Remove expired entries from the revocation set."""
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    with _revoked_lock:
        expired = [j for j, exp in _revoked_jtis.items() if exp < now]
        for j in expired:
            del _revoked_jtis[j]


def is_token_revoked(jti: str) -> bool:
    _cleanup_expired()
    with _revoked_lock:
        return jti in _revoked_jtis


# ---------------------------------------------------------------------------
# Token creation
# ---------------------------------------------------------------------------

def create_access_token(username: str):
    """Create a signed JWT with a unique jti claim. Returns (token, jti)."""
    jti = str(uuid.uuid4())
    expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )
    payload = {
        "sub": username,
        "exp": expiration,
        "jti": jti,
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return token, jti


# ---------------------------------------------------------------------------
# Token verification — accepts cookie *or* Authorization header
# ---------------------------------------------------------------------------
security_scheme = HTTPBearer(auto_error=False)


def verify_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Security(security_scheme),
):
    """FastAPI dependency: extract and validate JWT from cookie or header."""
    # Try Authorization header first
    token = credentials.credentials if credentials else None
    # Fall back to HttpOnly cookie
    if not token:
        token = request.cookies.get(COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        jti: str = payload.get("jti")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token payload")
        if jti and is_token_revoked(jti):
            logger.info(f"Revoked token rejected: jti={jti}")
            raise HTTPException(status_code=401, detail="Token has been revoked")
        return username
    except jwt.ExpiredSignatureError:
        logger.info("Expired token rejected")
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.PyJWTError as e:
        logger.warning(f"Invalid token: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")


def decode_token_claims(request: Request):
    """Decode token to get jti and exp for revocation. Returns dict or None."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
    if not token:
        return None
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.PyJWTError:
        return None


def refresh_token_if_needed(request: Request, response: Response) -> bool:
    """Re-issue the session cookie when the token has less than REFRESH_THRESHOLD_MINUTES remaining.

    Call this from middleware on successful (2xx) responses. Returns True if a new token was set.
    """
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return False
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        jti = payload.get("jti", "")
        exp = payload.get("exp")
        if not (username and exp):
            return False
        if is_token_revoked(jti):
            return False
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        if exp - now < REFRESH_THRESHOLD_MINUTES * 60:
            new_token, _ = create_access_token(username)
            set_auth_cookie(response, new_token)
            logger.info(f"Session refreshed: user={username}")
            return True
    except jwt.PyJWTError:
        pass
    return False
