import logging
import importlib
from PyQt5 import QtCore

logger = logging.getLogger(__name__)

class AnalysisPluginManager(QtCore.QObject):
    """
    Manages the lifecycle, loading, and unloading of analysis plugins.
    """
    
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.ui_manager = main_window.ui_manager
        self.graphics_manager = main_window.graphics_manager
        self.playback_manager = main_window.playback_manager
        self.threadpool = main_window.threadpool
        
        self.active_plugin = None
        
        # Define available plugins
        self.available_plugins = {
            "Live Spot Finder": "qp2.image_viewer.plugins.spot_finder.spot_finder_manager.SpotFinderManager",
            "Dozor": "qp2.image_viewer.plugins.dozor.dozor_manager.DozorManager",
            "nXDS": "qp2.image_viewer.plugins.nxds.nxds_manager.NXDSManager",
            "Crystfel": "qp2.image_viewer.plugins.crystfel.crystfel_manager.CrystfelManager",
            "Dials SSX": "qp2.image_viewer.plugins.dials_ssx.dials_manager.DialsManager",
            "XDS": "qp2.image_viewer.plugins.xds.xds_manager.XDSManager",
            "autoPROC": "qp2.image_viewer.plugins.autoproc.autoproc_manager.AutoPROCManager",
            "xia2": "qp2.image_viewer.plugins.xia2.xia2_manager.Xia2Manager",
            "xia2 SSX": "qp2.image_viewer.plugins.xia2_ssx.xia2_ssx_manager.Xia2SSXManager",
        }

    def select_plugin(self, plugin_name: str):
        """
        Loads and activates the specified plugin by name.
        """
        self.clear_active_plugin()
        
        if plugin_name == "None" or plugin_name not in self.available_plugins:
            return

        # Dynamically import and instantiate the selected plugin
        try:
            full_path = self.available_plugins[plugin_name]
            module_path, class_name = full_path.rsplit(".", 1)

            logger.info(f"Dynamically loading plugin: {module_path}.{class_name}")

            module = importlib.import_module(module_path)
            ManagerClass = getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to load plugin '{plugin_name}': {e}", exc_info=True)
            self.ui_manager.show_critical_message(
                "Plugin Error", f"Could not load plugin:\n{e}"
            )
            return

        # Instantiate the plugin manager
        self.active_plugin = ManagerClass(self.main_window)
        
        # Connect signals
        self.active_plugin.status_update.connect(
            self.ui_manager.show_status_message
        )
        self.active_plugin.frame_selected.connect(
            self.playback_manager.go_to_frame
        )
        self.active_plugin.request_spots_display.connect(
            self.graphics_manager.display_spots
        )
        self.active_plugin.request_main_threadpool.connect(
            lambda worker: self.threadpool.start(worker)
        )
        
        # Add widget to UI
        self.ui_manager.analysis_widget_layout.addWidget(
            self.active_plugin.get_widget()
        )
        
        # Expand the splitter if it was collapsed, so the new plugin is visible
        splitter = self.ui_manager.right_panel_splitter
        sizes = splitter.sizes()
        if len(sizes) == 2 and sizes[1] < 150:
            available = sum(sizes)
            target_h = 250
            splitter.setSizes([max(100, available - target_h), target_h])
        
        # Update with current data if available
        if self.main_window.reader:
            self.active_plugin.update_source(
                self.main_window.reader, self.main_window.current_master_file
            )
            # This calling back to main_window might be redundant if the plugin does it, 
            # but in the original code it was called here:
            # self._fetch_and_apply_crystal_data(self.current_master_file) 
            # (Wait, _fetch_and_apply_crystal_data is a main window method. 
            # I should verify if I need to call it or if the plugin handles it via update_source)
            # In the original code (Line 1602), it calls _fetch_and_apply_crystal_data explicitly.
            # I should expose it or trigger it.
            if hasattr(self.main_window, "_fetch_and_apply_crystal_data"):
                self.main_window._fetch_and_apply_crystal_data(self.main_window.current_master_file)

    def clear_active_plugin(self):
        """Cleans up the currently active plugin."""
        if self.active_plugin is None:
            return

        self.graphics_manager.clear_spots()
        self.graphics_manager.clear_indexed_reflections()
        self.graphics_manager.clear_plugin_info_text()

        self.active_plugin.cleanup()

        try:
            self.active_plugin.status_update.disconnect(
                self.ui_manager.show_status_message
            )
            self.active_plugin.frame_selected.disconnect(
                self.playback_manager.go_to_frame
            )
            try:
                self.active_plugin.request_spots_display.disconnect(
                    self.graphics_manager.display_spots
                )
            except TypeError:
                pass  # Not connected
            self.active_plugin.request_main_threadpool.disconnect()
        except TypeError as e:
            logger.warning(
                f"Could not disconnect a signal, may have already been disconnected: {e}"
            )

        widget = self.active_plugin.get_widget()
        self.ui_manager.analysis_widget_layout.removeWidget(widget)
        widget.setParent(None)
        widget.deleteLater()
        self.active_plugin = None

        # Collapse the splitter to hide empty space, leaving only the title bar
        splitter = self.ui_manager.right_panel_splitter
        sizes = splitter.sizes()
        if len(sizes) == 2:
            available = sum(sizes)
            target_h = self.ui_manager.analysis_plot_container.minimumSizeHint().height()
            splitter.setSizes([max(100, available - target_h), target_h])

    def update_source(self, reader, master_file):
        """Updates the active plugin with a new data source."""
        if self.active_plugin:
            self.active_plugin.update_source(reader, master_file)
