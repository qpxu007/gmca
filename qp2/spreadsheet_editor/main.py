
import sys
from PyQt5.QtWidgets import QApplication
from qp2.log.logging_config import setup_logging, get_logger
from .ui import MainWindow

logger = get_logger(__name__)

def main():
    setup_logging(root_name="qp2")
    
    try:
        from qp2.config.servers import ServerConfig
        ServerConfig.log_all_configs()
    except Exception as e:
        logger.warning(f"Failed to log server configurations: {e}")

    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
