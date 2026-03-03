import grp
import os
import pwd


def get_esaf_groups_for_user(username=None):
    """
    Finds all groups starting with "esaf" that the specified user belongs to.

    Args:
        username (str, optional): The username to check. If None, uses the current user.

    Returns:
        list: A list of group names starting with "esaf" that the user belongs to.  Returns an empty list
              if the user does not exist, or if there's an error retrieving groups.
    """

    if username is None:
        username = os.getenv(
            "USER"
        )  # Default to current user if username is not provided
        if not username:
            return []  # No username found, exit early

    try:
        user_info = pwd.getpwnam(username)  # Get user information
        user_gid = user_info.pw_gid  # Get the user's primary group ID

        # Find all groups the user belongs to
        group_names = []

        # First, add the user's primary group if it starts with "esaf"
        primary_group_name = grp.getgrgid(user_gid).gr_name
        if primary_group_name.startswith("esaf"):
            group_names.append(primary_group_name)

        # Now, find all other groups the user is a member of
        for group in grp.getgrall():
            if username in group.gr_mem and group.gr_name.startswith("esaf"):
                group_names.append(group.gr_name)

        # Ensure no duplicates in the list
        return sorted(list(set(group_names)))

    except KeyError:
        print(f"User '{username}' not found.")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


# Example usage:
if __name__ == "__main__":
    current_user_esaf_groups = get_esaf_groups_for_user()
    print(f"The current user's ESAF groups are: {current_user_esaf_groups}")

    specific_user_esaf_groups = get_esaf_groups_for_user(
        username="your_username"
    )  # Replace "your_username"
    print(f"Your username's ESAF groups are: {specific_user_esaf_groups}")
