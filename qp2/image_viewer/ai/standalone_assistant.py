import sys
import os
import argparse
from PyQt5 import QtWidgets, QtCore
from qp2.image_viewer.ai.assistant import AIAssistantWindow

# Mock namespace for standalone testing
def mock_namespace_provider():
    return {
        "image": "Mock Image Data (numpy array)",
        "params": {"wavelength": 0.9795, "distance": 200},
        "test_var": 123,
    }

def main():
    parser = argparse.ArgumentParser(description="Standalone AI Assistant Chat")
    parser.add_argument("--widget", action="store_true", help="Run in widget mode (Always on Top)")
    # Filter out Qt arguments to avoid conflict if any
    # Actually, sys.argv includes script name, so we pass sys.argv[1:] to parser
    # But QApplication consumes some args. Let's parse known args and ignore others or parse first.
    # A safer way is to use parse_known_args
    args, unknown = parser.parse_known_args()

    # Pass original argv to QApplication so it can handle its own flags (like -platform)
    app = QtWidgets.QApplication(sys.argv)
    
    # Allow setting API key via env for testing
    window = AIAssistantWindow(mock_namespace_provider)
    window.standalone_mode = True # Ensure closing the window exits the app
    
    if args.widget:
        # Set Always on Top flag
        window.setWindowFlags(window.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)

    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
