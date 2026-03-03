import logging
import os
import sys

# Attempt to import required authentication libraries
try:
    import ldap3
    import gssapi
    MISSING_LIBS = False
except ImportError as e:
    MISSING_LIBS = True
    logging.warning(f"Authentication libraries missing: {e}. Set QP2_TEST_USER/QP2_TEST_PASS for dev login.")

# password check, Adopted from Mark's code
def check_ldap_pw(username, password):
    if MISSING_LIBS: return False

    ldap_server = os.environ.get("QP2_LDAP_SERVER", "ldap.example.org")
    user_dn = os.environ.get("QP2_LDAP_USER_DN_TEMPLATE", "uid={username},ou=people,dc=example,dc=org").format(username=username)

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

    realm = os.environ.get("QP2_KRB5_REALM", realm)
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


import subprocess

def is_staff_member(username):
    """
    Checks if the user is a member of the 'staffGroup'.
    """
    _test_user = os.environ.get("QP2_TEST_USER")
    if _test_user and username == _test_user:
        return True

    if MISSING_LIBS:
        return False

    try:
        # Get all group names for the user
        result = subprocess.run(['id', '-Gn', username], capture_output=True, text=True)
        if result.returncode == 0:
            groups = result.stdout.strip().split()
            return "staffGroup" in groups
    except Exception as e:
        logging.error(f"Error checking staff status: {e}")
        
    return False

def check_gmca_pw(username, password):
    logging.info(f"Checking credentials for user: '{username}'")
    # Dev bypass: opt-in via environment variables (never hardcoded)
    _test_user = os.environ.get("QP2_TEST_USER")
    _test_pass = os.environ.get("QP2_TEST_PASS")
    if _test_user and _test_pass and username == _test_user and password == _test_pass:
        logging.info(f"Dev test user '{username}' authenticated via QP2_TEST_USER.")
        return True

    if MISSING_LIBS:
        return False

    if check_krb5_pw(username, password) or check_ldap_pw(username, password):
       return True
    return False
