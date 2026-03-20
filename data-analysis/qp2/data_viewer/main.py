import argparse
import json
import os
import subprocess
import sys
from functools import partial
from types import SimpleNamespace

import redis
from PyQt5.QtCore import QTimer, QThread, QObject, pyqtSignal
# --- PyQt5 Imports ---
from PyQt5.QtWidgets import (
    QApplication,
    QMainWindow,
    QTabWidget,
    QStatusBar,
    QAction,
    QInputDialog,
)

from qp2.data_viewer.query import (
    query_dataprocess,
    query_strategy,
    query_dataset_run,
    query_latest_dataset_run_id,
)
from qp2.data_viewer.tab_config import TAB_CONFIG
from qp2.data_viewer.ui import QueryTab
from qp2.log.logging_config import setup_logging, get_logger
from qp2.utils.icon import generate_icon_with_text
from qp2.xio.db_manager import DBManager, get_beamline_from_hostname
from qp2.xio.redis_manager import RedisConfig
from qp2.xio.user_group_manager import UserGroupManager
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)


class RedisListener(QObject):
    """
    Listens to a Redis Pub/Sub channel in a background thread and emits a signal
    when a new message is received.
    """

    message_received = pyqtSignal(str)

    def __init__(self, host, port, channel):
        super().__init__()
        self.redis_host = host
        self.redis_port = port
        self.redis_channel = channel
        self.is_running = True

    def run(self):
        """The main listening loop, modified for graceful shutdown."""
        pubsub = None
        while self.is_running:
            try:
                # Establish connection and subscription if not already active
                if pubsub is None:
                    r = redis.Redis(
                        host=self.redis_host,
                        port=self.redis_port,
                        decode_responses=True,
                    )
                    pubsub = r.pubsub(ignore_subscribe_messages=True)
                    pubsub.subscribe(self.redis_channel)
                    logger.info(
                        f"Redis listener connected and subscribed to '{self.redis_channel}'"
                    )

                # Use get_message with a timeout to make the loop non-blocking
                message = pubsub.get_message(timeout=1.0)
                if message:
                    self.message_received.emit(message["data"])

            except redis.exceptions.ConnectionError:
                logger.error(f"Redis connection failed. Retrying in 3 seconds...")
                pubsub = None  # Reset to force reconnection on next loop
                QThread.sleep(3)
            except Exception as e:
                logger.error(f"An error occurred in Redis listener: {e}")
                pubsub = None
                QThread.sleep(3)

        # Cleanup when the loop exits
        if pubsub:
            pubsub.close()
        logger.info(f"Redis pubsub listener for {self.redis_channel} has stopped.")

    def stop(self):
        self.is_running = False


class MainWindow(QMainWindow):
    def __init__(self, start_tab_index=1, log_file=None):
        super().__init__()
        self.base_title = "GMCA Data Viewer"
        self.setWindowTitle(self.base_title)
        self.setGeometry(100, 100, 1600, 900)
        app_icon = generate_icon_with_text(text="dv", bg_color="#e74c3c", size=128)
        self.setWindowIcon(app_icon)

        setup_logging(root_name="qp2", log_level="DEBUG", log_file=log_file)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.db_manager = DBManager(
            status_update_callback=self.status_bar.showMessage,
            error_callback=self.show_error_message,
        )
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        self.query_funcs = {
            "query_dataset_run": query_dataset_run,
            "query_dataprocess": query_dataprocess,
            "query_strategy": query_strategy,
        }

        try:
            from qp2.config.servers import ServerConfig
            ServerConfig.log_all_configs()
        except Exception as e:
            logger.warning(f"Failed to log server configurations: {e}")

        beamline = get_beamline_from_hostname()

        self.user_group_manager = UserGroupManager()
        username = os.getenv("USER", "default_user")

        # 1. Primary attempt: Find the user's most recent ESAF from the database.
        initial_group_info = self.user_group_manager.latest_group_info_from_username(
            username, beamline
        )
        logger.debug(f"Initial group info: {initial_group_info}")

        if initial_group_info:
            initial_primary_group = initial_group_info["group_name"]
        else:
            # 2. Fallback attempt: If the primary fails, get *any* ESAF group.
            #    (This method has its own system-level fallback).
            logger.info(
                "Could not determine most recent ESAF from DB. Falling back to any available ESAF group."
            )
            all_esaf_groups = self.user_group_manager.get_esaf_groups_for_user(username)

            logger.debug(f"User: {username}, All ESAF groups: {all_esaf_groups}")

            if all_esaf_groups:
                # 3. If fallback is successful, use the first group found.
                initial_primary_group = all_esaf_groups[0]["group_name"]
            else:
                # 4. If both attempts fail, use a hardcoded default.
                logger.warning(
                    "No ESAF groups found in database or system. Using a default."
                )
                initial_primary_group = "default_group"

        self.current_user = SimpleNamespace(
            username=username,
            primary_group=initial_primary_group,
            beamline=beamline,
        )
        logger.debug(f"Current user info: {self.current_user}")

        self._update_window_title()

        self._create_user_menu()

        if self.db_manager.engine:
            for tab_name, config in TAB_CONFIG.items():
                query_func = self.query_funcs[config["query_func_name"]]
                tab = QueryTab(
                    tab_name,
                    config,
                    self.db_manager,
                    partial(query_func, user=self.current_user),
                )
                self.tabs.addTab(tab, tab_name)
            
            # Set the startup tab
            if start_tab_index is not None and 0 <= start_tab_index < self.tabs.count():
                self.tabs.setCurrentIndex(start_tab_index)
            else:
                 # Default to 1 (Processing) if out of range or None, 
                 # provided we have enough tabs
                 if self.tabs.count() > 1:
                     self.tabs.setCurrentIndex(1)

        else:
            self.show_error_message("Database could not be initialized.")

        # --- NEW: Set up and start the Redis listener thread ---
        self.redis_thread = None
        self.redis_listener = None
        self._pending_refreshes = set()
        self._refresh_timer = QTimer(self)
        self._refresh_timer.setSingleShot(True)
        self._refresh_timer.setInterval(5000)  # 5 seconds
        self._refresh_timer.timeout.connect(self._process_pending_refreshes)
        self._start_redis_listener()

        # Path to the EPICS caget utility
        self.caget_path = ProgramConfig.get_program_path("caget")

        # State holders to track the last known status string
        self._last_process_state = ""
        self._last_strategy_state = ""

        self.status_poll_timer = QTimer(self)
        self.status_poll_timer.timeout.connect(self.poll_pipeline_status)
        self.status_poll_timer.start(2000)  # Poll every 2 seconds

        self._last_known_latest_run = None  # Store the last known result
        self.dataset_poll_timer = QTimer(self)
        self.dataset_poll_timer.timeout.connect(self.poll_for_new_datasets)
        self.dataset_poll_timer.start(5000)  # Poll every 5 seconds
        # Initial check shortly after startup
        QTimer.singleShot(2000, self.poll_for_new_datasets)

        self.status_bar.showMessage(
            f"Ready. Primary group: {self.current_user.primary_group}", 10000
        )

    def poll_for_new_datasets(self):
        """
        Periodically checks for new datasets using a fast query.
        Triggers a full refresh only if a new dataset is found.
        """
        # Don't poll if the window isn't visible
        if not self.isVisible():
            return

        try:
            # Run the fast query in a background thread to not block the GUI
            # This is overkill for a fast query, but good practice.
            # A simpler way is to just call it directly. Let's do that for simplicity.

            with self.db_manager.get_session() as session:
                latest_run = query_latest_dataset_run_id(session, self.current_user)

            # If this is the first time we're checking, just store the result
            if self._last_known_latest_run is None:
                self._last_known_latest_run = latest_run
                return

            # If a new run exists (or the latest one was deleted), trigger a refresh
            if latest_run != self._last_known_latest_run:
                logger.info(
                    f"New dataset detected (latest ID: {latest_run.data_id if latest_run else 'None'}). Triggering refresh."
                )
                self._last_known_latest_run = latest_run

                # Find the 'Datasets' tab and tell it to reload
                for i in range(self.tabs.count()):
                    if self.tabs.tabText(i) == "Datasets":
                        tab = self.tabs.widget(i)
                        if isinstance(tab, QueryTab):
                            tab.load_data()
                        break
        except Exception as e:
            # Silently ignore errors to prevent spamming the user if DB is temporarily down
            logger.warning(f"Failed to poll for new datasets: {e}")

    def _create_user_menu(self):
        """Creates the 'User' menu for changing the primary group."""
        menu_bar = self.menuBar()
        user_menu = menu_bar.addMenu("&ESAFs")

        # Fetch all ESAF groups the user belongs to (with system fallback)
        esaf_groups = self.user_group_manager.get_esaf_groups_for_user(
            self.current_user.username
        )

        if esaf_groups:
            change_group_menu = user_menu.addMenu("Change Primary Group")
            for group_info in esaf_groups:
                group_name = group_info["group_name"]
                action = QAction(group_name, self)
                # Use a partial to pass the group name to the handler
                action.triggered.connect(
                    partial(self._change_primary_group, group_name)
                )
                change_group_menu.addAction(action)
        else:
            user_menu.addAction(QAction("No ESAF groups found", self, enabled=False))

        # Check if user is staff to enable manual entry
        if self.user_group_manager.is_staff(self.current_user.username):
            user_menu.addSeparator()
            enter_esaf_action = QAction("Enter ESAF...", self)
            enter_esaf_action.triggered.connect(self._prompt_for_esaf)
            user_menu.addAction(enter_esaf_action)

    def _prompt_for_esaf(self):
        """Prompts the user to manually enter an ESAF ID."""
        text, ok = QInputDialog.getText(
            self, "Enter ESAF", "ESAF Name (e.g. esaf12345):"
        )
        if ok and text:
            group_name = text.strip()
            self._change_primary_group(group_name)

    # NEW: Method to handle changing the primary group
    def _change_primary_group(self, group_name):
        """Updates the primary group and refreshes all data tabs."""
        if self.current_user.primary_group == group_name:
            return  # No change needed

        self.current_user.primary_group = group_name
        self._update_window_title()

        self.status_bar.showMessage(
            f"Primary group set to '{group_name}'. Refreshing data...", 10000
        )

        # Trigger a refresh on all tabs
        for i in range(self.tabs.count()):
            tab = self.tabs.widget(i)
            if isinstance(tab, QueryTab):
                tab.load_data()

    def show_error_message(self, message):
        self.status_bar.showMessage(message, 0)
        self.status_bar.setStyleSheet("color: red;")

    def refresh_all_tabs(self):
        current_tab = self.tabs.currentWidget()
        if isinstance(current_tab, QueryTab):
            self.status_bar.showMessage(
                f"Auto-refreshing '{self.tabs.tabText(self.tabs.currentIndex())}'..."
            )
            current_tab.load_data()  # Reloads data respecting current search/sort
            self.status_bar.showMessage("Refresh complete.", 5000)

    def poll_pipeline_status(self):
        """
        Called every 2 seconds. Runs 'caget' to get the pipeline status
        and triggers a table refresh only if the status has changed.
        """
        if not self.isVisible():  # Don't poll if the window isn't visible
            return
        # Skip caget when viewing the Datasets tab — it only matters for Processing/Strategy
        current_tab = self.tabs.tabText(self.tabs.currentIndex())
        if current_tab not in ("Processing", "Strategy"):
            return

        pv_name = f"{self.current_user.beamline}:bi:analysis:pipelineStatus"
        try:
            # Run the caget command
            process = subprocess.run(
                [self.caget_path, pv_name],
                capture_output=True,
                text=True,
                timeout=1.5,  # Timeout to prevent hanging
            )
            if process.returncode != 0:
                # Silently ignore errors if caget fails, as it may be temporary
                return

            current_state = process.stdout.strip()
            if not current_state:
                return

            # Check if the PROCESSING table needs a refresh
            if "PROCESS" in current_state and current_state != self._last_process_state:
                # Only refresh if the state is not the initial empty state
                if self._last_process_state != "":
                    for i in range(self.tabs.count()):
                        if self.tabs.tabText(i) == "Processing":
                            self.tabs.widget(i).load_data()
                            self.status_bar.showMessage(
                                f"Processing job status changed. Refreshing table.",
                                5000,
                            )
                            break
                # Update the last known state
                self._last_process_state = current_state

            # Check if the STRATEGY table needs a refresh
            elif (
                    "STRATEGY" in current_state
                    and current_state != self._last_strategy_state
            ):
                if self._last_strategy_state != "":
                    for i in range(self.tabs.count()):
                        if self.tabs.tabText(i) == "Strategy":
                            self.tabs.widget(i).load_data()
                            self.status_bar.showMessage(
                                f"Strategy job status changed. Refreshing table.", 5000
                            )
                            break
                # Update the last known state
                self._last_strategy_state = current_state

        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.error(f"Error running 'caget' utility: {e}")
            # If caget isn't found or times out, stop the timer to prevent repeated errors
            self.status_poll_timer.stop()
            self.show_error_message(
                f"Error running 'caget' utility. Live updates disabled."
            )
        except Exception as e:
            logger.error(f"Unexpected error in poll_pipeline_status: {e}")
            # Ignore other potential transient errors
            pass

    def refresh_visible_datasets_tab(self):
        """
        Called every 10 minutes. If the 'Datasets' tab is currently visible,
        it reloads the data for that tab.
        """
        # Get the index and text of the currently active tab
        current_index = self.tabs.currentIndex()
        current_tab_text = self.tabs.tabText(current_index)

        # Check if the active tab is the one we want to refresh
        if current_tab_text == "Datasets":
            current_tab_widget = self.tabs.widget(current_index)
            # Ensure it's a QueryTab and call its data loading method
            if isinstance(current_tab_widget, QueryTab):
                current_tab_widget.load_data()

    def _start_redis_listener(self):
        redis_host = RedisConfig.HOSTS.get("analysis_results")
        redis_port = 6379
        redis_channel = "pipeline_updates"

        self.redis_thread = QThread(self)
        self.redis_listener = RedisListener(
            host=redis_host, port=redis_port, channel=redis_channel
        )
        self.redis_listener.moveToThread(self.redis_thread)

        self.redis_thread.started.connect(self.redis_listener.run)
        self.redis_listener.message_received.connect(self.on_pipeline_update)
        self.redis_thread.finished.connect(self.redis_thread.deleteLater)

        self.redis_thread.start()

    def on_pipeline_update(self, message: str):
        """
        This slot debounces Redis updates. It adds the relevant tab to a pending
        set and starts/restarts a timer to process the refresh.
        """
        try:
            data = json.loads(message)
            pipeline_name = data.get("pipeline_name", "")
            status = data.get("status", "UNKNOWN")
            sample_name = data.get("sample_name", "N/A")

            tab_to_refresh = (
                "Strategy" if "_strategy" in pipeline_name
                else "Datasets" if "dataset_run" in pipeline_name
                else "Processing"
            )

            self.status_bar.showMessage(
                f"Update received for '{sample_name}' ({status}). Refresh scheduled.",
                5000,
            )

            self._pending_refreshes.add(tab_to_refresh)
            self._refresh_timer.start()

        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error processing Redis message: {e} | Message: {message}")

    def _process_pending_refreshes(self):
        """
        When the debounce timer fires, this method refreshes all tabs that
        have received updates.
        """
        if not self._pending_refreshes:
            return

        tabs_to_refresh = self._pending_refreshes.copy()
        self._pending_refreshes.clear()

        self.status_bar.showMessage(
            f"Applying updates for: {', '.join(tabs_to_refresh)}...", 3000
        )

        for tab_name in tabs_to_refresh:
            for i in range(self.tabs.count()):
                if self.tabs.tabText(i) == tab_name:
                    self.tabs.widget(i).load_data()
                    break

    def _update_window_title(self):
        """Sets the main window title to include the active ESAF."""
        active_esaf = self.current_user.primary_group
        self.setWindowTitle(f"{self.base_title} - [ESAF: {active_esaf}]")

    def hideEvent(self, event):
        """Stop polling timers when window is hidden/minimized."""
        self.status_poll_timer.stop()
        self.dataset_poll_timer.stop()
        logger.debug("Window hidden — polling timers stopped.")
        super().hideEvent(event)

    def showEvent(self, event):
        """Restart polling timers when window becomes visible."""
        if not self.status_poll_timer.isActive():
            self.status_poll_timer.start(2000)
        if not self.dataset_poll_timer.isActive():
            self.dataset_poll_timer.start(5000)
        logger.debug("Window shown — polling timers restarted.")
        super().showEvent(event)

    def closeEvent(self, event):
        """Ensures timers and listener thread are stopped cleanly on exit."""
        self.status_poll_timer.stop()
        self.dataset_poll_timer.stop()
        self._refresh_timer.stop()

        if self.redis_listener:
            self.redis_listener.stop()
        if self.redis_thread:
            self.redis_thread.quit()
            self.redis_thread.wait(2000)

        super().closeEvent(event)


if __name__ == "__main__":
    import os
    from datetime import datetime

    parser = argparse.ArgumentParser(description="GMCA Data Viewer")
    parser.add_argument(
        "--tab",
        type=int,
        default=1,
        help="Startup tab index (1=Processing, 2=Strategy). Default is 1.",
    )
    parser.add_argument(
        "--log-file",
        help="Optional path to save log output to a file. Overrides QP2_LOG_FILE env var.",
    )
    args, _ = parser.parse_known_args()

    log_file = args.log_file
    if not log_file:
        try:
            from qp2.config.servers import ServerConfig
            log_file = ServerConfig.LOG_FILE
        except ImportError:
            pass
    if not log_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(os.path.expanduser("~"), f"dv-{timestamp}.log")

    app = QApplication(sys.argv)
    window = MainWindow(start_tab_index=args.tab, log_file=log_file)
    window.show()
    sys.exit(app.exec_())
