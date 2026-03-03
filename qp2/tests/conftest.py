
import sys
import os

# Add the parent of the project root to sys.path so we can import 'qp2' as a package
# tests/ is inside qp2 (root), so we need to go up two levels: tests/ -> qp2/ -> data-analysis/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from unittest.mock import MagicMock

# Mock external dependencies that might be missing in the test environment
# This allows us to verify package structure and internal imports without full environment setup
MOCK_MODULES = [
    'redis',
    'pyqtgraph',
    'pyqtgraph.Qt',
    'PyQt5',
    'PyQt5.QtCore',
    'PyQt5.QtWidgets',
    'PyQt5.QtGui',
    'PyQt5.QtWebEngineWidgets',
    'sqlalchemy',
    'sqlalchemy.engine',
    'sqlalchemy.engine.url',
    'sqlalchemy.orm',
    'sqlalchemy.exc',
    'sqlalchemy.ext.declarative',
    'h5py',
    'h5grove',
    'requests',
    'fabio',
    'pymysql',
    'psycopg2',
    'openai',
    'matplotlib',
    'pyyaml',
    'gemmi',
    'scikit-learn',
    'scikit-image',
    'networkx',
    'markdown',
    'pyepics',
    'lxml',
    'websockets',
    'pydantic',
    'PyJWT',
    'filelock',
    'cv2',
    'OpenGL'
]

for mod_name in MOCK_MODULES:
    try:
        __import__(mod_name)
    except ImportError:
        # Create a mock that generates a new mock for any attribute access (submodules)
        m = MagicMock()
        # Ensure it has a path to be treated as a package if it's a top-level module
        if '.' not in mod_name:
            m.__path__ = []
        m.__spec__ = None
        sys.modules[mod_name] = m

