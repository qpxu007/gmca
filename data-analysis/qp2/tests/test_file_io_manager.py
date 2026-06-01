
import unittest
from unittest.mock import MagicMock, patch, mock_open
import os
import sys

# Define a dummy QObject class
class MockQObject:
    def __init__(self, parent=None):
        pass

# Define a pass-through pyqtSlot decorator
def pyqtSlot(*args, **kwargs):
    def decorator(func):
        return func
    return decorator

def pyqtSignal(*args, **kwargs):
    return MagicMock()

# Mock PyQt5 modules
sys.modules["PyQt5"] = MagicMock()
sys.modules["PyQt5.QtCore"] = MagicMock()
sys.modules["PyQt5.QtCore"].QObject = MockQObject
sys.modules["PyQt5.QtCore"].pyqtSlot = pyqtSlot
sys.modules["PyQt5.QtCore"].pyqtSignal = pyqtSignal

sys.modules["PyQt5.QtWidgets"] = MagicMock()
sys.modules["pyqtgraph.Qt"] = MagicMock()

# Mock get_logger
sys.modules["qp2.log.logging_config"] = MagicMock()

from qp2.image_viewer.actions.file_io_manager import FileIOManager

class TestFileIOManager(unittest.TestCase):
    def setUp(self):
        self.mock_main_window = MagicMock()
        self.mock_main_window.reader = None
        self.mock_main_window.current_master_file = None
        self.mock_main_window.redis_manager = None
        
        self.manager = FileIOManager(self.mock_main_window)

    @patch("qp2.image_viewer.actions.file_io_manager.QtWidgets.QFileDialog.getOpenFileName")
    @patch("qp2.image_viewer.actions.file_io_manager.open")
    @patch("os.path.isfile")
    def test_load_from_list_file(self, mock_isfile, mock_file_open, mock_get_open_file_name):
        # Setup mocks
        mock_get_open_file_name.return_value = ("/path/to/list.txt", "Text Files (*.txt)")
        
        file_content = "path/to/file1.h5\npath/to/file2.h5\ninvalid/path"
        mock_file_open.return_value = mock_open(read_data=file_content).return_value
        
        # Mock isfile to return True only for the first two paths
        def side_effect(path):
            if path == "path/to/file1.h5": return True
            if path == "path/to/file2.h5": return True
            return False
            
        mock_isfile.side_effect = side_effect

        # Mock load_file to avoid actual loading
        self.manager.load_file = MagicMock()

        # Run the method
        self.manager.load_from_list_file()

        # Assertions
        # Check if file dialog was called
        mock_get_open_file_name.assert_called_once()
        
        # Check if file was opened
        mock_file_open.assert_called_once_with("/path/to/list.txt", "r")
        
        # Check if load_file was called for valid paths
        self.manager.load_file.assert_any_call("path/to/file1.h5")
        self.manager.load_file.assert_any_call("path/to/file2.h5")
        
        # Check if load_file was NOT called for invalid path
        calls = [c[0][0] for c in self.manager.load_file.call_args_list]
        self.assertNotIn("invalid/path", calls)
            
        # Check that we loaded exactly 2 files
        self.assertEqual(self.manager.load_file.call_count, 2)

    @patch("qp2.image_viewer.actions.file_io_manager.QtWidgets.QFileDialog.getOpenFileName")
    def test_load_from_list_file_cancel(self, mock_get_open_file_name):
        # Setup mocks for cancellation
        mock_get_open_file_name.return_value = ("", "")
        
        self.manager.load_file = MagicMock()

        # Run the method
        self.manager.load_from_list_file()

        # Assertions
        mock_get_open_file_name.assert_called_once()
        self.manager.load_file.assert_not_called()

if __name__ == "__main__":
    unittest.main()
