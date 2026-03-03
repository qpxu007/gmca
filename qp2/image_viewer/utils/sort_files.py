import re

def natural_sort_key(s):
    """
    Create a sort key that handles numbers in strings naturally.
    e.g., 'run10' comes after 'run2'.
    """
    return [
        int(text) if text.isdigit() else text.lower()
        for text in re.split("([0-9]+)", s)
    ]