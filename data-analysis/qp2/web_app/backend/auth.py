import logging
import os
import re
import subprocess
import sys
import threading
import time

# Attempt to import required authentication libraries
try:
    import ldap3
    import gssapi
    MISSING_LIBS = False
except ImportError as e:
    MISSING_LIBS = True
    logging.warning(
        f"Authentication libraries missing: {e}. "
        "Login will fail for all users. Install with: pip install -e '.[auth]'"
    )

# password check, Adopted from Mark's code
def check_ldap_pw(username, password):
    if MISSING_LIBS: return False

    # Validate username format before constructing LDAP DN
    if not re.match(r'^[a-zA-Z0-9._-]+$', username):
        logging.warning(f"LDAP auth rejected — invalid username format: {repr(username)}")
        return False

    # Always use ID beamline passwords for hosts
    ldap_server = "bl1upper.gmca.aps.anl.gov"
    # user_dn = "uid=" + username + ",ou=Users,dc=idin,dc=gmca,dc=aps,dc=anl,dc=gov"
    user_dn = "uid=" + username + ",ou=people,dc={},dc=gmca,dc=aps,dc=anl,dc=gov".format('idin')

    try:
        server = ldap3.Server(ldap_server, get_info=ldap3.NONE)
        conn = ldap3.Connection(server, user=user_dn, password=password, auto_bind=True)
        logging.debug("Successfully logged in to LDAP")
        logging.debug("dn = " + conn.extend.standard.who_am_i())
        conn.unbind()
        return True
    except ldap3.core.exceptions.LDAPBindError:
        logging.debug("Error logging in to LDAP server %s with dn %s" % (ldap_server, user_dn))
        return False
    except Exception as e:
        logging.error(f"LDAP error: {e}")
        return False


def check_krb5_pw(username, password, realm='anl.gov'):
    if MISSING_LIBS: return False

    principal = f"{username}@{realm}"
    try:
        user_name = gssapi.Name(principal, name_type=gssapi.NameType.user)
        # Attempt to acquire credentials using provided password
        creds_result = gssapi.raw.acquire_cred_with_password(
            user_name, password.encode(), usage='initiate'
        )
        # If no exception raised, authentication succeeded
        return True
    except gssapi.exceptions.GSSError as e:
        print(f"Authentication failed: {e}")
        return False
    except Exception as e:
        print(f"Kerberos error: {e}")
        return False


# is_staff_member spawns `id` per call (~2-10 ms with NSS/LDAP), which is
# the dominant cost on every authenticated route. Cache for STAFF_CACHE_TTL_S
# so a hot endpoint at 10 Hz doesn't fork 10 processes/sec/user. Staff group
# membership changes rarely (vs ESAF groups which rotate daily and use a
# tighter TTL in UserGroupManager), so 10 min is comfortable here.
STAFF_CACHE_TTL_S = 600
_staff_cache: dict = {}
_staff_cache_lock = threading.Lock()


def _is_staff_member_uncached(username):
    if username == "admin":
        return True
    if MISSING_LIBS:
        return False
    if not re.match(r'^[a-zA-Z0-9._-]+$', username):
        logging.warning(f"Invalid username format rejected: {repr(username)}")
        return False
    try:
        result = subprocess.run(['id', '-Gn', '--', username], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            groups = result.stdout.strip().split()
            return "staffGroup" in groups
    except Exception as e:
        logging.error(f"Error checking staff status: {e}")
    return False


def is_staff_member(username):
    """Cached check whether the user is in 'staffGroup'.

    Cache TTL is STAFF_CACHE_TTL_S seconds. Call clear_staff_cache() to
    force a re-lookup (e.g. after a membership change).
    """
    now = time.time()
    with _staff_cache_lock:
        cached = _staff_cache.get(username)
        if cached and cached[1] > now:
            return cached[0]
    result = _is_staff_member_uncached(username)
    with _staff_cache_lock:
        _staff_cache[username] = (result, now + STAFF_CACHE_TTL_S)
    return result


def clear_staff_cache(username=None):
    """Invalidate staff-membership cache. Pass a username for one entry,
    omit for all entries."""
    with _staff_cache_lock:
        if username is None:
            _staff_cache.clear()
        else:
            _staff_cache.pop(username, None)

def check_gmca_pw(username, password):
    logging.info(f"Checking credentials for user: '{username}'")
    # Bypass for test users — only when QP2_ENV=test
    if os.environ.get("QP2_ENV") == "test":
        if username == "admin" and password == "admin":
            logging.info("Test user 'admin' authenticated (QP2_ENV=test).")
            return True
        if username == "user" and password == "user":
            logging.info("Test user 'user' authenticated (QP2_ENV=test).")
            return True

    # Safety fallback if libraries are missing (so app doesn't lock out during dev)
    if MISSING_LIBS:
        return False

    if check_krb5_pw(username, password) or check_ldap_pw(username, password):
       return True
    return False
