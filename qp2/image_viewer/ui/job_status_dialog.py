import os
import subprocess
import shlex
from pyqtgraph.Qt import QtWidgets, QtGui, QtCore
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

class JobStatusWorker(QtCore.QThread):
    """Worker thread to fetch job statuses from Redis to avoid freezing UI."""
    data_ready = QtCore.pyqtSignal(list)

    def __init__(self, dataset_paths, redis_conn, known_plugins):
        super().__init__()
        self.dataset_paths = dataset_paths
        self.redis_conn = redis_conn
        self.known_plugins = known_plugins

    def run(self):
        job_data = []
        if not self.redis_conn:
            self.data_ready.emit(job_data)
            return

        for path in self.dataset_paths:
            found_any = False
            for plugin in self.known_plugins:
                # Check specific status key first
                if plugin == "dozor":
                    status_key = f"analysis:out:spots:dozor2:{path}:status"
                    # Dozor uses a HASH for status, where keys are job names and values are JSON status strings
                    try:
                        # hgetall returns a dict of byte keys/values
                        all_statuses = self.redis_conn.hgetall(status_key)
                        if all_statuses:
                            # Aggregate status
                            statuses = []
                            for k, v in all_statuses.items():
                                try:
                                    if isinstance(v, bytes):
                                        v = v.decode('utf-8')
                                    s_data = json.loads(v)
                                    if isinstance(s_data, dict) and 'status' in s_data:
                                        statuses.append(s_data['status'])
                                except Exception:
                                    pass
                            
                            # Simple aggregation logic
                            if any("FAIL" in s.upper() for s in statuses):
                                status = "Failed"
                            elif any("RUN" in s.upper() for s in statuses):
                                status = "Running"
                            elif statuses and all("COMPLET" in s.upper() or "FINISH" in s.upper() or "SUCCESS" in s.upper() for s in statuses):
                                status = "Finished"
                            elif statuses:
                                status = f"Prioritizing: {statuses[0]}" # Fallback
                            else:
                                 # If hgetall returned data but we couldn't parse it, maybe check output key
                                 if self.redis_conn.exists(f"analysis:out:spots:dozor2:{path}"):
                                      status = "Finished (Output Exists)"
                                 else:
                                      status = "Unknown"
                        else:
                             # No status key, check output
                             if self.redis_conn.exists(f"analysis:out:spots:dozor2:{path}"):
                                  status = "Finished (Output Exists)"
                             else:
                                  status = None
                    except Exception as e:
                        logger.error(f"Error fetching Dozor status: {e}")
                        status = "Error Fetching"
                else:
                    status_key = f"analysis:out:{plugin}:{path}:status"
                    status = self.redis_conn.get(status_key)
                
                # If no direct status key, check if output exists (implies success or at least run)
                if status is None:
                    # Some plugins might not set a status key but set the output key
                    out_key = f"analysis:out:{plugin}:{path}"
                    if self.redis_conn.exists(out_key):
                        status = "finished (unknown outcome)"
                
                if status:
                    if isinstance(status, bytes):
                        status = status.decode('utf-8')
                    
                    # Try to parse JSON and extract 'status' field if present (e.g. for CrystFEL)
                    try:
                        import json
                        status_data = json.loads(status)
                        if isinstance(status_data, dict) and 'status' in status_data:
                            status = status_data['status']
                    except (json.JSONDecodeError, TypeError, AttributeError):
                        pass

                    job_data.append({
                        'path': path,
                        'plugin': plugin,
                        'status': status
                    })
                    found_any = True
            
            if not found_any:
                job_data.append({
                    'path': path,
                    'plugin': "-",
                    'status': "Not Run / Unknown"
                })
        
        self.data_ready.emit(job_data)

class JobStatusDialog(QtWidgets.QDialog):
    # Signal emits: (list_of_paths, plugin_name)
    resubmit_plugin_jobs = QtCore.pyqtSignal(list, str)

    def __init__(self, dataset_paths, redis_conn, parent=None, active_plugin="nxds"):
        super().__init__(parent)
        self.dataset_paths = sorted(list(dataset_paths))
        self.redis_conn = redis_conn
        self.setWindowTitle("Processing Job Status")
        self.resize(800, 600)

        # Known plugins to check
        self.known_plugins = ["xds", "nxds", "xia2", "autoproc", "xia2_ssx", "crystfel", "xds_strategy", "mosflm_strategy", "dozor"]
        self.job_data = [] # List of dicts: {'path': str, 'plugin': str, 'status': str}

        # Determine default filter
        if active_plugin:
            self.default_plugin = active_plugin.lower()
        else:
            self.default_plugin = "nxds"

        # If the passed plugin isn't in our known list (e.g. it's an alias or new), default to All or nxds
        if self.default_plugin not in self.known_plugins:
             # Try to find a partial match or just fallback
             # E.g. "nXDS" -> "nxds" is handled by lower(), but "SomeOther" -> fallback
             if "nxds" in self.default_plugin:
                 self.default_plugin = "nxds"
             # else keep it, maybe it matches later or we just show All
        
        self._setup_ui()
        self._fetch_statuses()
        # populate is called when thread finishes

    def _setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)

        # Filter Section
        filter_layout = QtWidgets.QHBoxLayout()
        filter_layout.addWidget(QtWidgets.QLabel("Show Jobs for:"))
        self.plugin_combo = QtWidgets.QComboBox()
        self.plugin_combo.addItem("All")
        self.plugin_combo.addItems(self.known_plugins)
        
        # Set default selection
        index = self.plugin_combo.findText(self.default_plugin, QtCore.Qt.MatchFixedString)
        if index >= 0:
            self.plugin_combo.setCurrentIndex(index)
        else:
             # If not found (e.g. "None"), maybe select "All" or "nxds"
             self.plugin_combo.setCurrentIndex(0)

        self.plugin_combo.currentTextChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self.plugin_combo)
        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        # Summary Section
        self.summary_label = QtWidgets.QLabel("Loading status...")
        self.summary_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(self.summary_label)

        # Table Section
        self.table = QtWidgets.QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["Dataset Path", "Plugin", "Status"])
        self.table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QtWidgets.QHeaderView.ResizeToContents)
        
        # Enable Sorting
        self.table.setSortingEnabled(True)
        
        # Set Selection Mode
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        
        # Enable Context Menu
        self.table.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_context_menu)
        
        layout.addWidget(self.table)

        # Buttons Section
        btn_layout = QtWidgets.QHBoxLayout()
        
        self.refresh_btn = QtWidgets.QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self._refresh)
        btn_layout.addWidget(self.refresh_btn)

        btn_layout.addStretch()

        self.resubmit_btn = QtWidgets.QPushButton("Resubmit Failed Jobs")
        self.resubmit_btn.clicked.connect(self._on_resubmit_clicked)
        self.resubmit_btn.setStyleSheet("background-color: #d9534f; color: white; font-weight: bold;")
        # Disabled initially
        self.resubmit_btn.setEnabled(False)
        btn_layout.addWidget(self.resubmit_btn)

        self.close_btn = QtWidgets.QPushButton("Close")
        self.close_btn.clicked.connect(self.accept)
        btn_layout.addWidget(self.close_btn)

        layout.addLayout(btn_layout)

    def _fetch_statuses(self):
        """Start worker to query Redis for statuses."""
        self.refresh_btn.setEnabled(False)
        self.summary_label.setText("Fetching status...")
        
        self.worker = JobStatusWorker(self.dataset_paths, self.redis_conn, self.known_plugins)
        self.worker.data_ready.connect(self._on_data_fetched)
        self.worker.start()

    def _on_data_fetched(self, job_data):
        self.job_data = job_data
        self.refresh_btn.setEnabled(True)
        self._filter_and_populate()

    def _on_filter_changed(self, text):
        self._filter_and_populate()

    def _filter_and_populate(self):
        selected_plugin = self.plugin_combo.currentText()
        
        # Filter data
        filtered_data = []
        if selected_plugin == "All":
            filtered_data = self.job_data
        else:
            filtered_data = [d for d in self.job_data if d['plugin'] == selected_plugin]

        self.table.setSortingEnabled(False) # Disable during population
        self.table.setRowCount(len(filtered_data))

        for row, data in enumerate(filtered_data):
            # Path
            path_item = QtWidgets.QTableWidgetItem(os.path.basename(data['path']))
            path_item.setToolTip(data['path'])
            self.table.setItem(row, 0, path_item)

            # Plugin
            plugin_item = QtWidgets.QTableWidgetItem(data['plugin'])
            self.table.setItem(row, 1, plugin_item)

            # Status
            status = data['status']
            status_item = QtWidgets.QTableWidgetItem(status)
            
            # Color coding
            lower_status = status.lower()
            if "fail" in lower_status or "error" in lower_status:
                status_item.setForeground(QtGui.QColor("red"))
            elif "success" in lower_status or "finish" in lower_status or "complete" in lower_status:
                status_item.setForeground(QtGui.QColor("green"))
            elif "run" in lower_status or "pending" in lower_status or "submitted" in lower_status:
                status_item.setForeground(QtGui.QColor("blue"))
            
            self.table.setItem(row, 2, status_item)

        self.table.setSortingEnabled(True)
        # Default sort by path if not already sorted
        if self.table.horizontalHeader().sortIndicatorSection() == -1:
             self.table.sortItems(0, QtCore.Qt.AscendingOrder)
        
        self._update_summary(filtered_data)

    def _update_summary(self, displayed_data=None):
        # Use full data for "Total" or displayed? Usually displayed is better context if filtered.
        # But resubmit logic might depend on what is visible.
        
        data_to_count = displayed_data if displayed_data is not None else self.job_data
        total = len(data_to_count)
        counts = {}
        for data in data_to_count:
            s = data['status'].lower()
            # Simplify status for summary
            if "fail" in s or "error" in s:
                cat = "Failed"
            elif "success" in s or "finish" in s:
                cat = "Success"
            elif "run" in s or "pending" in s:
                cat = "Running/Pending"
            elif "not run" in s:
                cat = "Not Run"
            else:
                cat = "Other"
            counts[cat] = counts.get(cat, 0) + 1
        
        summary_text = f"Shown Jobs: {total} | " + " | ".join([f"{k}: {v}" for k, v in counts.items()])
        self.summary_label.setText(summary_text)

        # Enable resubmit only if failures exist IN THE VISIBLE LIST
        # This makes sense: if I filter by nxds, I only want to resubmit nxds failures.
        has_failures = counts.get("Failed", 0) > 0
        self.resubmit_btn.setEnabled(has_failures)

    def _refresh(self):
        self._fetch_statuses()

    def _on_resubmit_clicked(self):
        """Identify failed jobs and emit signals to resubmit them."""
        # Only resubmit what is currently filtered/visible
        selected_plugin = self.plugin_combo.currentText()
        data_to_check = []
        
        if selected_plugin == "All":
            data_to_check = self.job_data
        else:
            data_to_check = [d for d in self.job_data if d['plugin'] == selected_plugin]

        failed_by_plugin = {}

        for data in data_to_check:
            s = data['status'].lower()
            if "fail" in s or "error" in s:
                plugin = data['plugin']
                if plugin not in failed_by_plugin:
                    failed_by_plugin[plugin] = []
                failed_by_plugin[plugin].append(data['path'])

        if not failed_by_plugin:
            QtWidgets.QMessageBox.information(self, "Resubmit", "No failed jobs found in current view to resubmit.")
            return

        count = sum(len(l) for l in failed_by_plugin.values())
        confirm = QtWidgets.QMessageBox.question(
            self,
            "Confirm Resubmit", 
            f"Found {count} failed jobs across {len(failed_by_plugin)} plugins (in current view).\nResubmit them now?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
        )

        if confirm == QtWidgets.QMessageBox.Yes:
            for plugin, paths in failed_by_plugin.items():
                self.resubmit_plugin_jobs.emit(paths, plugin)
            self.accept()

    def _show_context_menu(self, pos):
        menu = QtWidgets.QMenu(self.table)
        
        # Actions
        clear_rerun_action = QtWidgets.QAction("Clear Status && Rerun Selected", self)
        open_folder_action = QtWidgets.QAction("Open Processing Directory", self)
        open_term_action = QtWidgets.QAction("Open Terminal Here", self)
        
        # Check selection
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows:
            clear_rerun_action.setEnabled(False)
            open_folder_action.setEnabled(False)
            open_term_action.setEnabled(False)
        
        # Connect
        clear_rerun_action.triggered.connect(self._clear_and_rerun_selected)
        open_folder_action.triggered.connect(self._open_processing_directory)
        open_term_action.triggered.connect(self._open_terminal)
        
        menu.addAction(clear_rerun_action)
        menu.addSeparator()
        menu.addAction(open_folder_action)
        menu.addAction(open_term_action)
        
        menu.exec_(self.table.viewport().mapToGlobal(pos))

    def _get_selected_jobs(self):
        """Helper to get job data for selected rows."""
        selected_rows = self.table.selectionModel().selectedRows()
        jobs = []
        for index in selected_rows:
            row = index.row()
            # We need to map back to the data source or extract from table items.
            # Since table is populated from filtered_data but might be sorted,
            # extracting path and plugin from columns is safer if we trust the display.
            
            path_item = self.table.item(row, 0)
            plugin_item = self.table.item(row, 1)
            
            if path_item and plugin_item:
                # The full path is stored in the tooltip of column 0
                full_path = path_item.toolTip()
                plugin = plugin_item.text()
                jobs.append({'path': full_path, 'plugin': plugin})
        return jobs

    def _clear_and_rerun_selected(self):
        jobs = self._get_selected_jobs()
        if not jobs: return
        
        confirm = QtWidgets.QMessageBox.question(
            self,
            "Confirm Clear & Rerun", 
            f"Are you sure you want to clear status and rerun {len(jobs)} jobs?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
        )
        if confirm != QtWidgets.QMessageBox.Yes:
            return

        # Group by plugin for batch submission
        jobs_by_plugin = {}
        for job in jobs:
            path = job['path']
            plugin = job['plugin']
            
            # 1. Clear Redis Status
            if self.redis_conn:
                status_key = f"analysis:out:{plugin}:{path}:status"
                # Also consider clearing the output list? 
                # Usually clearing status is enough to indicate 'pending' if we resubmit.
                try:
                    self.redis_conn.delete(status_key)
                except Exception as e:
                    logger.error(f"Failed to clear redis key {status_key}: {e}")

            if plugin not in jobs_by_plugin:
                jobs_by_plugin[plugin] = []
            jobs_by_plugin[plugin].append(path)

        # 2. Emit Resubmit Signals
        for plugin, paths in jobs_by_plugin.items():
            if plugin and plugin != "-":
                self.resubmit_plugin_jobs.emit(paths, plugin)
        
        # 3. Refresh UI
        self._refresh()

    def _determine_proc_dir(self, job):
        """
        Best-effort guess of the processing directory.
        Defaults to dataset directory if specific plugin subdir doesn't exist.
        """

        plugin = job.get('plugin')
        dataset_path = job.get('path')
        
        if not plugin or not dataset_path:
           return ""

        # Try to get from Redis first
        if self.redis_conn:
            try:
                # Construct the key. This matches logic in Submit scripts.
                # Key format: analysis:out:{plugin}:{path}
                # Be careful: "path" here is the dataset path (master file).
                # Some plugins might use a slightly different key structure but this is standard.
                key = f"analysis:out:{plugin}:{dataset_path}"
                
                # _proc_dir is stored in the hash
                proc_dir_bytes = self.redis_conn.hget(key, "_proc_dir")
                if proc_dir_bytes:
                    if isinstance(proc_dir_bytes, bytes):
                        proc_dir = proc_dir_bytes.decode('utf-8')
                    else:
                        proc_dir = proc_dir_bytes
                        
                    if os.path.isdir(proc_dir):
                        return proc_dir
            except Exception as e:
                logger.error(f"Failed to fetch _proc_dir from redis for {dataset_path}: {e}")

        dataset_dir = os.path.dirname(dataset_path)
        
        # Common plugin output subdirectories
        # This mapping could be improved if we shared config with the server
        plugin_subdirs = {
            "nxds": ["nxds", "process/nxds"],
            "xia2": ["xia2", "autoproc_xia2"],
            "autoproc": ["autoPROC"],
            "xia2_ssx": ["xia2_ssx"],
            "dozor": ["dozor_logs"]
        }
        
        candidates = plugin_subdirs.get(plugin, [])
        
        for sub in candidates:
            potential_path = os.path.join(dataset_dir, sub)
            if os.path.isdir(potential_path):
                return potential_path
        
        # Fallback to dataset directory
        return dataset_dir

    def _open_processing_directory(self):
        jobs = self._get_selected_jobs()
        if not jobs: return
        
        # If multiple selected, open for the first one only to avoid spam
        target_dir = self._determine_proc_dir(jobs[0])
        
        try:
            # Linux specific (xdg-open)
            subprocess.Popen(['xdg-open', target_dir])
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"Could not open directory: {e}")

    def _open_terminal(self):
        jobs = self._get_selected_jobs()
        if not jobs: return
        
        target_dir = self._determine_proc_dir(jobs[0])
        logger.info(f"Opening terminal in: {target_dir}")
        
        if not target_dir or not os.path.isdir(target_dir):
             logger.warning(f"Terminal target directory does not exist: {target_dir}")
             QtWidgets.QMessageBox.warning(self, "Error", f"Processing directory not found: {target_dir}")
             return
        
        try:
            # Try common terminals
            terminals = [
                ['gnome-terminal', '--working-directory', target_dir],
                ['konsole', '--workdir', target_dir],
                ['xterm', '-e', f"cd {shlex.quote(target_dir)} && /bin/bash"],
                ['xfce4-terminal', '--working-directory', target_dir]
            ]
            
            success = False
            for cmd in terminals:
                try:
                    # Check if executable exists in path
                    if subprocess.call(['which', cmd[0]], stdout=subprocess.DEVNULL) == 0:
                        logger.info(f"Found terminal {cmd[0]}, attempting to launch: {cmd}")
                        subprocess.Popen(cmd)
                        success = True
                        break
                except Exception:
                    continue
            
            if not success:
                QtWidgets.QMessageBox.warning(self, "Error", "No supported terminal emulator found (tried gnome-terminal, konsole, xterm, xfce4-terminal).")
                
        except Exception as e:
             QtWidgets.QMessageBox.warning(self, "Error", f"Could not open terminal: {e}")
