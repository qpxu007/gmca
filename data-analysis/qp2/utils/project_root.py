import os


def find_qp2_parent(file_path):
    """
    Robustly finds the project root directory by walking up from the current
    script's location until it finds the 'qp2' package directory.
    """
    path = os.path.abspath(file_path)
    # Stop when we reach the filesystem root (e.g., '/')
    while path != os.path.dirname(path):
        if os.path.basename(path) == "qp2":
            # We found the 'qp2' directory, so its parent is the project root.
            return os.path.dirname(path)
        path = os.path.dirname(path)
    return None  # Return None if not found
