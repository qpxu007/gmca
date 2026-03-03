
def test_import_image_viewer():
    """Smoke test to ensure image_viewer can be imported."""
    try:
        from qp2.image_viewer.ui import main
    except ImportError as e:
        assert False, f"Failed to import image_viewer: {e}"

def test_import_data_viewer():
    """Smoke test to ensure data_viewer can be imported."""
    try:
        from qp2.data_viewer import main
    except ImportError as e:
        assert False, f"Failed to import data_viewer: {e}"

def test_import_data_proc():
    """Smoke test to ensure data_proc can be imported."""
    try:
        from qp2.data_proc.server import data_processing_server
    except ImportError as e:
        assert False, f"Failed to import data_proc: {e}"
