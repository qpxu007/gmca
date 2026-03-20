import grp
import os
import pwd
import re
import subprocess
from typing import Optional, Dict, Any

import pymysql

from qp2.log.logging_config import get_logger
from qp2.xio.db_manager import get_beamline_from_hostname
from qp2.config.servers import ServerConfig

logger = get_logger(__name__)


class UserGroupManager:
    """
    A class to manage user groups and ESAF information from the database,
    with a system-level fallback for ESAF group lookups.
    """

    def __init__(
        self,
        host=ServerConfig.MYSQL_GMCA_ACCOUNTS,
        user=ServerConfig.MYSQL_USER,
        password=ServerConfig.MYSQL_PASS,
        database=ServerConfig.MYSQL_DB_GMCA_ACCOUNTS,
    ):
        """
        Initialize the UserGroupManager with database connection parameters.

        Args:
            host (str): Database host
            user (str): Database username
            password (str): Database password
            database (str): Database name
        """
        self.db_config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "connect_timeout": 3,
        }
        logger.info(f"Accounts DB: mysql://{host}/{database}")
        # self.pool = None  # Pooling removed with pymysql
        self._group_info_cache = {}
        self._user_group_info_cache = {}
        self._username_all_groups_cache = {}
        self._username_esaf_groups_cache = {}
        self._is_staff_cache = {}

    def _get_connection(self):
        """
        Create and return a database connection.

        Returns:
            tuple: (connection, cursor) tuple
        """
        try:
            cnx = pymysql.connect(**self.db_config)
            cursor = cnx.cursor(pymysql.cursors.DictCursor)
            return cnx, cursor
        except pymysql.OperationalError as e:
            # Connection failed (e.g. refused, unknown host).
            # Log as warning without traceback to avoid noise, since we have fallbacks.
            logger.warning(f"Failed to connect to database ({self.db_config.get('host')}): {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to create database connection: {e}", exc_info=True)
            raise

    def _close_connection(self, cnx, cursor):
        """
        Close the database connection and cursor.

        Args:
            cnx: Database connection
            cursor: Database cursor
        """
        try:
            if cursor:
                cursor.close()
            if cnx:
                cnx.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")

    def _get_esaf_groups_from_system(self, username):
        """
        Fallback method. Finds all groups starting with "esaf" that the
        user belongs to by querying the local system.
        """
        try:
            user_info = pwd.getpwnam(username)
            user_gid = user_info.pw_gid
            group_names = []

            primary_group_name = grp.getgrgid(user_gid).gr_name
            if primary_group_name.startswith("esaf"):
                group_names.append(primary_group_name)
            elif primary_group_name == "staffGroup":
                group_names.append("staff")

            for group in grp.getgrall():
                if username in group.gr_mem and group.gr_name.startswith("esaf"):
                    group_names.append(group.gr_name)
                elif group.gr_name == "staffGroup":
                    group_names.append("staff")

            # Sort numerically by the ESAF number in descending order with 'staff' first
            def sort_key(group_name):
                if group_name == "staff":  #
                    return float("inf")

                if group_name.startswith("esaf") and group_name[4:].isdigit():
                    return int(group_name[4:])
                return 0  # Default for malformed names

            unique_groups = sorted(list(set(group_names)), key=sort_key, reverse=True)

            # Format the output to match the database query's return type
            return [{"group_name": name} for name in unique_groups]

        except KeyError:
            logger.error(f"System user '{username}' not found during fallback.", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"An error occurred during system group lookup fallback: {e}", exc_info=True)
            return []

    def groupnames_from_username(self, username):
        """
        Given a login username, find the group names (esaf) that the user belongs to.
        """
        if username in self._username_all_groups_cache:
            return self._username_all_groups_cache[username]

        cnx, cursor = None, None
        try:
            cnx, cursor = self._get_connection()
            query = """
                    SELECT `user_group`.group_name
                    FROM user
                             INNER JOIN user_group ON user.badge_number = user_group.badge_number
                    WHERE user.username = %s \
                    """
            cursor.execute(query, (username,))
            result = cursor.fetchall()
            self._username_all_groups_cache[username] = result
            return result
        except Exception as e:
            logger.warning(f"Error in groupnames_from_username: {e}")
            return []
        finally:
            self._close_connection(cnx, cursor)

    def get_esaf_groups_for_user(self, username):
        """
        Finds all groups for a given user where the group name starts with 'esaf'.
        Tries the database first, then falls back to a local system lookup on failure.
        """
        if username in self._username_esaf_groups_cache:
            return self._username_esaf_groups_cache[username]

        result = None
        cnx, cursor = None, None
        try:
            cnx, cursor = self._get_connection()
            query = """
                    SELECT `user_group`.group_name
                    FROM user
                             INNER JOIN user_group ON user.badge_number = user_group.badge_number
                    WHERE user.username = %s
                      AND `user_group`.group_name LIKE %s \
                    """
            pattern = "esaf%"
            cursor.execute(query, (username, pattern))
            result = cursor.fetchall()

            if not result:
                logger.warning(
                    f"No ESAF groups found for '{username}' in the database. Falling back to system lookup."
                )
                result = self._get_esaf_groups_from_system(username)

        except pymysql.Error as e:
            logger.warning(
                f"Database query for ESAF groups failed ({e}). Falling back to system lookup."
            )
            result = self._get_esaf_groups_from_system(username)
        except Exception as e:
            logger.warning(
                f"Unexpected error querying ESAF groups ({e}). Falling back to system lookup."
            )
            result = self._get_esaf_groups_from_system(username)
        finally:
            self._close_connection(cnx, cursor)
        
        # Cache the result (whether from DB or fallback)
        if result is not None:
             self._username_esaf_groups_cache[username] = result
        return result or []

    def is_staff(self, username):
        """
        Checks if the user is a member of 'staff' or 'staffGroup'.
        Checks database first, then falls back to system lookup.
        """
        if username in self._is_staff_cache:
            return self._is_staff_cache[username]

        is_member = False
        
        # 1. Check Database
        cnx, cursor = None, None
        try:
            cnx, cursor = self._get_connection()
            query = """
                    SELECT 1
                    FROM user
                             INNER JOIN user_group ON user.badge_number = user_group.badge_number
                    WHERE user.username = %s
                      AND user_group.group_name IN ('staff', 'staffGroup')
                    LIMIT 1
                    """
            cursor.execute(query, (username,))
            if cursor.fetchone():
                is_member = True
        except pymysql.Error as e:
            logger.warning(f"Database check for staff membership failed ({e}). Checking system.")
        except Exception as e:
            logger.warning(f"Error checking staff membership in DB: {e}")
        finally:
            self._close_connection(cnx, cursor)

        # 2. Check System if not found in DB
        if not is_member:
            try:
                staff_group = grp.getgrnam("staffGroup")
                # Check if user is a member
                if username in staff_group.gr_mem:
                    is_member = True
                # Check if it's the user's primary group
                user_info = pwd.getpwnam(username)
                if user_info.pw_gid == staff_group.gr_gid:
                    is_member = True
            except KeyError:
                # staffGroup or user not found on system
                pass
            except Exception as e:
                logger.error(f"Error checking system staff membership: {e}")

        self._is_staff_cache[username] = is_member
        return is_member

    def groupinfo_from_groupname(self, groupname):
        """
        Given a group name esaf\\d+, find the group information.
        MariaDB [gmca_accounts]> desc `group`;
        """
        if groupname in self._group_info_cache:
            return self._group_info_cache[groupname]

        cnx, cursor = None, None
        try:
            cnx, cursor = self._get_connection()
            query = """
                    SELECT *
                    FROM `group`
                    WHERE group_name = %s \
                    """
            cursor.execute(query, (groupname,))
            result = cursor.fetchone()
            if result:
                self._group_info_cache[groupname] = result
            return result
        except Exception as e:
            logger.warning(f"Error in groupinfo_from_groupname: {e}")
            return None
        finally:
            self._close_connection(cnx, cursor)

    def latest_group_info_from_username(self, username=None, beamline=None):
        """
        From username or group_name, get the latest group/esaf information.
        """
        cache_key = (username, beamline)
        if cache_key in self._user_group_info_cache:
            return self._user_group_info_cache[cache_key]

        result = None
        cnx, cursor = None, None
        try:
            cnx, cursor = self._get_connection()
            params = []
            query = """
                    SELECT `group`.*
                    FROM user
                             INNER JOIN user_group ON user.badge_number = user_group.badge_number
                             INNER JOIN `group` ON user_group.group_name = `group`.group_name
                    WHERE user.username = %s \
                    """
            params.append(username)
            if beamline is not None:
                query += " AND `group`.beamline = %s"
                params.append(beamline)
            query += " ORDER BY `group`.esaf_collect_start DESC LIMIT 1"
            cursor.execute(query, tuple(params))
            result = cursor.fetchone()
        except pymysql.Error as e:
            logger.warning(
                f"Could not get primary group info for '{username}' from database: {e}"
            )
        except Exception as e:
            logger.warning(f"Unexpected error in latest_group_info_from_username: {e}")
        finally:
            self._close_connection(cnx, cursor)

        if result is None:
            logger.warning(
                f"No primary group info found for '{username}' in the database. Using fallback."
            )
            # --- MODIFIED LOGIC: Prioritize 'staff' group for staff members ---
            if self.is_staff(username):
                result = {"group_name": "staff"}
            else:
                result = {"group_name": get_current_bluice_user()}

        if result:
            self._user_group_info_cache[cache_key] = result

        logger.info(f"Primary group info for '{username}': {result}")
        return result


ugm = UserGroupManager()


def get_esaf_from_data_path(
    dataset_path: str, login_username: Optional[str] = os.getenv("USER")
) -> Dict[str, Any]:
    """
    Extracts ESAF and user information with a clear priority:
    1. Parse the dataset path for an 'esafXXXXX' group name.
    2. If found, enrich it with database details (like pi_badge).
    3. If not found, fall back to the current system user and look up their info.
    4. If all else fails, provide a default.

    Args:
        dataset_path: The full path to the dataset file.
        login_username: An optional username to use for fallback lookup.

    Returns:
        A dictionary containing the extracted information.
    """
    esaf_info = {}

    try:
        # --- Step 1: Parse the path ---
        if dataset_path:
            match = re.search(r"esaf(\d+)", dataset_path, re.IGNORECASE)
            if match:
                group_name = match.group(0).lower()
                esaf_info["primary_group"] = group_name
                esaf_info["esaf_id"] = int(match.group(1))

                # Enrich with DB info if a group was found in the path
                additional_info = ugm.groupinfo_from_groupname(group_name)
                if additional_info:
                    esaf_info.update(additional_info)
                logger.debug(f"Parsed ESAF info from path: {esaf_info}")

        # --- Step 2: Fallback to current user if path parsing failed ---
        if not esaf_info.get("primary_group"):
            esaf_info["username"] = login_username
            # Look up the latest ESAF info for the current user
            group_info = ugm.latest_group_info_from_username(login_username)
            if group_info:
                esaf_info.update(group_info)
            logger.debug(
                f"Falling back to user '{login_username}', found info: {group_info}"
            )

        # --- Step 3: Final fallback to a default group ---
        if not esaf_info.get("primary_group"):
            esaf_info["primary_group"] = "staff"
            esaf_info["beamline"] = get_beamline_from_hostname()

    except Exception as e:
        logger.error(
            f"An unexpected error occurred in get_esaf_from_data_path: {e}",
            exc_info=True,
        )
        # Ensure a default is always returned on error
        if "primary_group" not in esaf_info:
            esaf_info["primary_group"] = "unknown"

    logger.info(f"Final extracted ESAF info: {esaf_info} from {dataset_path}")
    return esaf_info


def get_current_bluice_user():
    beamline = get_beamline_from_hostname()
    pv = f"{beamline}:bi:runs:currentUser"
    # -t: only value; -s: value as string; -S: char array as long string
    cmd = ["caget", "-t", "-s", "-S", pv]

    value = ""
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        if proc.returncode == 0:
            value = (proc.stdout or "").strip()
    except Exception:
        value = ""

    if value and value.lower().startswith("esaf"):
        return value

    # Fallback to current username (Linux/Unix-first, with getpass fallback)
    try:
        return pwd.getpwuid(os.getuid()).pw_name
    except Exception:
        try:
            import getpass

            return getpass.getuser()
        except Exception:
            # Last-resort generic
            return "unknown"


def find_esaf_directory(file_path: str) -> Optional[str]:
    """
    Parses a file path to find the parent ESAF directory (e.g., /path/to/esaf12345/).
    Returns the path as a string if found, otherwise None.
    """
    try:
        path = os.path.abspath(file_path)
        parts = path.split(os.sep)
        # Search for a directory part that matches the 'esaf' pattern
        for part in reversed(parts):
            if re.match(r"^esaf\d+$", part):
                # Reconstruct the path up to and including the esaf directory
                esaf_index = parts.index(part)
                esaf_path = os.path.join(*parts[: esaf_index + 1])
                # On Linux, paths start with '/', so we need to add it back
                if not esaf_path.startswith(os.sep):
                    esaf_path = os.sep + esaf_path
                return esaf_path
    except Exception as e:
        logger.error(f"Error finding ESAF directory from path '{file_path}': {e}")
    return None


if __name__ == "__main__":

    print(ugm.latest_group_info_from_username("cchang"))

    gi = ugm.groupinfo_from_groupname("esaf282339")
    if gi:
        print(gi.keys())
    else:
        print(gi)
    g = ugm.groupnames_from_username("b40088")
    print(g)
