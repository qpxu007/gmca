# qp2/image_viewer/plugins/nxds/cluster_analysis_dialog.py
import json
from pathlib import Path

import numpy as np
from PyQt5 import QtWidgets, QtCore

# Assumes the analysis functions are in a utility file
from qp2.image_viewer.plugins.nxds.nxds_utils import run_dbscan_analysis, run_networkx_community_analysis
from qp2.image_viewer.ui.busy_cursor import BusyCursor

MAX_POINTS_FOR_DIRECT_CLUSTERING = 10000  # New constant


class ClusterAnalysisDialog(QtWidgets.QDialog):
    def __init__(self, dataset_paths, redis_conn, parent=None):
        super().__init__(parent)
        self.dataset_paths = dataset_paths
        self.redis_conn = redis_conn
        self.setWindowTitle("nXDS Unit Cell Cluster Analysis")
        self.setMinimumSize(800, 600)

        self.all_data = []  # To store loaded cell data

        # --- Main Layout ---
        layout = QtWidgets.QVBoxLayout(self)
        self.splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)

        # --- Left Panel (Controls) ---
        control_widget = QtWidgets.QWidget()
        control_layout = QtWidgets.QVBoxLayout(control_widget)

        # Data Source Selection
        source_group = QtWidgets.QGroupBox("Data Source")
        source_layout = QtWidgets.QVBoxLayout(source_group)
        self.source_combo = QtWidgets.QComboBox()
        self.source_combo.addItems(["unit_cell_parameters", "reduced_cell", "candidate_lattices"])
        self.lattice_combo = QtWidgets.QComboBox()  # For candidate lattices
        self.lattice_combo.setVisible(False)
        source_layout.addWidget(self.source_combo)
        source_layout.addWidget(self.lattice_combo)
        control_layout.addWidget(source_group)

        # Analysis Method Selection
        analysis_group = QtWidgets.QGroupBox("Analysis Method")
        analysis_layout = QtWidgets.QVBoxLayout(analysis_group)
        self.method_combo = QtWidgets.QComboBox()
        self.method_combo.addItems(["DBSCAN", "NetworkX Community"])
        analysis_layout.addWidget(self.method_combo)

        self.sampling_label = QtWidgets.QLabel("")
        self.sampling_label.setWordWrap(True)
        analysis_layout.addWidget(self.sampling_label)

        control_layout.addWidget(analysis_group)

        self.run_button = QtWidgets.QPushButton("Run Analysis")
        control_layout.addWidget(self.run_button)
        control_layout.addStretch()

        # --- Right Panel (Results) ---
        results_widget = QtWidgets.QWidget()
        results_layout = QtWidgets.QVBoxLayout(results_widget)
        self.results_table = QtWidgets.QTableWidget()
        self.results_table.setColumnCount(8)
        self.results_table.setHorizontalHeaderLabels(["Cluster ID", "Size", "a", "b", "c", "alpha", "beta", "gamma"])
        self.results_table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.penalty_label = QtWidgets.QLabel("")
        results_layout.addWidget(self.penalty_label)
        results_layout.addWidget(self.results_table)

        self.splitter.addWidget(control_widget)
        self.splitter.addWidget(results_widget)
        self.splitter.setSizes([250, 550])
        layout.addWidget(self.splitter)

        # --- Connections ---
        self.run_button.clicked.connect(self.perform_analysis)
        self.source_combo.currentTextChanged.connect(self._source_changed)

        # Use QTimer to ensure the dialog is shown before the slow part starts
        QtCore.QTimer.singleShot(50, self._load_data)

    def _load_data(self):
        """Load all nXDS.json files for the selected datasets."""
        with BusyCursor():
            self.all_data = []
            for master_file in self.dataset_paths:
                redis_key = f"analysis:out:nxds:{master_file}"
                json_path_str = self.redis_conn.hget(redis_key, "_results_json_path")
                if json_path_str and Path(json_path_str).exists():
                    with open(json_path_str, 'r') as f:
                        data = json.load(f)
                        self.all_data.extend(list(data.values()))

        if not self.all_data:
            QtWidgets.QMessageBox.warning(self, "No Data",
                                          "Could not find valid nXDS.json results for the selected datasets.")
            self.close()  # Close the dialog if there's no data
            return

        self._source_changed(self.source_combo.currentText())

    def _source_changed(self, source_name):
        """Update UI when the data source for clustering changes."""
        self.lattice_combo.setVisible(source_name == "candidate_lattices")
        self.penalty_label.setText("")
        self.sampling_label.setText("")

        if source_name == "candidate_lattices":
            self._populate_lattice_types()

    def _populate_lattice_types(self):
        """Find all unique lattice types from the candidate_lattices field."""
        lattice_types = set()
        for item in self.all_data:
            for candidate in item.get("candidate_lattices", []):
                parts = candidate.split()
                if len(parts) > 1:
                    lattice_types.add(parts[1])

        self.lattice_combo.clear()
        self.lattice_combo.addItems(sorted(list(lattice_types)))

    def perform_analysis(self):
        """Run the selected clustering algorithm and display results."""
        with BusyCursor():
            source_key = self.source_combo.currentText()
            method = self.method_combo.currentText()

            cells_to_cluster = self._extract_cells(source_key)

            if not cells_to_cluster:
                self.results_table.setRowCount(0)
                QtWidgets.QMessageBox.information(self, "No Data",
                                                  "No valid unit cells found for the selected criteria.")
                return

            data_array = np.array(cells_to_cluster)

            num_points = len(data_array)
            if num_points > MAX_POINTS_FOR_DIRECT_CLUSTERING:
                self.sampling_label.setText(
                    f"Large dataset ({num_points} points). "
                    f"Clustering a random sample of {MAX_POINTS_FOR_DIRECT_CLUSTERING} points, then classifying the rest."
                )
                QtWidgets.QApplication.processEvents()  # Force label update

                # Step 1: Sub-sample
                sample_indices = np.random.choice(num_points, MAX_POINTS_FOR_DIRECT_CLUSTERING, replace=False)
                data_sample = data_array[sample_indices]

                # Step 2: Cluster the sample
                prototype_clusters = self._run_clustering_algorithm(data_sample, method)

                # Step 3 & 4: Classify full dataset and refine
                analysis_results = self._classify_and_refine(data_array, prototype_clusters)
            else:
                self.sampling_label.setText(f"Clustering all {num_points} points directly.")
                analysis_results = self._run_clustering_algorithm(data_array, method)

            self._display_results(analysis_results)

    def _extract_cells(self, source_key):
        """Helper to extract cell parameters based on the selected source."""
        cells = []
        penalties = []

        if source_key == "candidate_lattices":
            target_lattice = self.lattice_combo.currentText()
            if not target_lattice: return []

            for item in self.all_data:
                for candidate in item.get("candidate_lattices", []):
                    parts = candidate.split()
                    if len(parts) > 1 and parts[1] == target_lattice:
                        try:
                            cell = [float(p) for p in parts[-6:]]
                            penalty = float(parts[2])
                            cells.append(cell)
                            penalties.append(penalty)
                        except (ValueError, IndexError):
                            continue
            if penalties:
                avg_penalty = np.mean(penalties)
                std_penalty = np.std(penalties)
                self.penalty_label.setText(
                    f"<b>Lattice '{target_lattice}' Penalty Stats:</b> Mean={avg_penalty:.2f}, StdDev={std_penalty:.2f}")
        else:  # unit_cell_parameters or reduced_cell
            for item in self.all_data:
                cell_data = item.get(source_key)
                if not cell_data: continue
                try:
                    if isinstance(cell_data, str):
                        cell = [float(p) for p in cell_data.split()]
                    else:
                        cell = cell_data
                    if len(cell) == 6:
                        cells.append(cell)
                except (ValueError, IndexError, TypeError):
                    continue
        return cells

    def _run_clustering_algorithm(self, data_array, method):
        """Runs the selected clustering algorithm on the given data array."""
        if method == "DBSCAN":
            return run_dbscan_analysis(data_array)
        else:  # NetworkX Community
            return run_networkx_community_analysis(data_array)

    def _classify_and_refine(self, full_data, prototype_clusters):
        """Classify all data points against prototypes, then refine stats."""
        if not prototype_clusters:
            return []

        means = np.array([p['mean'] for p in prototype_clusters])
        stds = np.array([p['std'] for p in prototype_clusters])

        # Avoid division by zero for stds
        stds[stds < 1e-6] = 1e-6

        # Scaled Euclidean distance - faster than Mahalanobis
        # For each point, calculate distance to all cluster centers
        scaled_distances = np.linalg.norm((full_data[:, np.newaxis, :] - means) / stds, axis=2)

        # Assign each point to the cluster with the minimum distance
        assignments = np.argmin(scaled_distances, axis=1)

        # Check if the point is within 3 standard deviations of its assigned cluster
        min_distances = np.min(scaled_distances, axis=1)
        # We check against sqrt(num_dimensions) because we summed squares
        is_inlier = min_distances < 3.0 * np.sqrt(full_data.shape[1])

        # Refine results
        final_results = []
        for i, proto_cluster in enumerate(prototype_clusters):
            cluster_mask = (assignments == i) & is_inlier
            cluster_data = full_data[cluster_mask]

            if len(cluster_data) > 1:
                final_results.append({
                    "cluster_id": proto_cluster['cluster_id'],
                    "size": len(cluster_data),
                    "mean": np.mean(cluster_data, axis=0),
                    "std": np.std(cluster_data, axis=0)
                })

        final_results.sort(key=lambda x: x['size'], reverse=True)
        return final_results

    def _display_results(self, results):
        """Populate the table with clustering results."""
        self.results_table.setRowCount(len(results))
        for row, cluster in enumerate(results):
            self.results_table.setItem(row, 0, QtWidgets.QTableWidgetItem(str(cluster['cluster_id'])))
            self.results_table.setItem(row, 1, QtWidgets.QTableWidgetItem(str(cluster['size'])))
            for i in range(6):
                mean_val = f"{cluster['mean'][i]:.2f} ± {cluster['std'][i]:.2f}"
                self.results_table.setItem(row, i + 2, QtWidgets.QTableWidgetItem(mean_val))
        self.results_table.resizeColumnsToContents()
