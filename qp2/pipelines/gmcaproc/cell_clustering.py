import functools
import json
import logging
import time

import numpy as np

from qp2.log.logging_config import setup_logging, get_logger

setup_logging(log_level=logging.DEBUG)
logger = get_logger(__name__)


# Set up logging


def timeit(func):
    """Decorator that reports the execution time."""

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        logger.debug(f"Function {func.__name__} Took {run_time:.4f} seconds")
        return value

    return wrapper_timer


def find_elbow_programmatically(parsed_data, key_name="reduced_cell"):
    from sklearn.preprocessing import StandardScaler
    from sklearn.neighbors import NearestNeighbors

    cell_params_list = [
        np.fromstring(item[key_name], sep=" ")
        for item in parsed_data
        if item.get("accepted")
           and item.get(key_name)
           and len(item[key_name].split()) == 6
    ]
    if len(cell_params_list) < 12:
        return 0.5  # Default for small samples
    scaled_data = StandardScaler().fit_transform(np.array(cell_params_list))
    k = 12
    neighbors = NearestNeighbors(n_neighbors=k).fit(scaled_data)
    distances, _ = neighbors.kneighbors(scaled_data)
    k_distances = np.sort(distances[:, k - 1])
    n_points = len(k_distances)
    all_coord = np.vstack((range(n_points), k_distances)).T
    line_vec = all_coord[-1] - all_coord[0]
    line_vec_norm = line_vec / np.sqrt(np.sum(line_vec ** 2))
    vec_from_first = all_coord - all_coord[0]
    scalar_product = np.dot(vec_from_first, line_vec_norm)
    dist_from_line = np.sqrt(np.sum(vec_from_first ** 2, axis=1) - scalar_product ** 2)
    elbow_index = np.argmax(dist_from_line)
    return k_distances[elbow_index]


def run_dbscan_analysis(parsed_data, key_name="reduced_cell"):
    """
    Performs DBSCAN clustering and returns a standardized list of cluster stats.
    """
    print("Running DBSCAN analysis...")
    from sklearn.cluster import DBSCAN

    # 1. Prepare data
    cell_params_list = []
    for item in parsed_data:
        if item.get("accepted") and item.get(key_name):
            try:
                params = [float(p) for p in item[key_name].split()]
                if len(params) == 6:
                    cell_params_list.append(params)
            except (ValueError, IndexError):
                continue

    if len(cell_params_list) < 2:
        return []
    data_for_clustering = np.array(cell_params_list)
    scaled_data = StandardScaler().fit_transform(data_for_clustering)

    # 2. Find optimal eps and run DBSCAN
    eps = find_elbow_programmatically(parsed_data, key_name)
    print(f"Using automatically determined eps: {eps:.4f}")
    db = DBSCAN(eps=eps, min_samples=12).fit(scaled_data)
    labels = db.labels_

    # 3. Compile standardized results for each cluster
    results = []
    unique_labels = set(labels)
    for label in sorted(list(unique_labels)):
        if label == -1:
            continue  # Skip noise points for this report

        cluster_mask = labels == label
        cluster_data = data_for_clustering[cluster_mask]

        results.append(
            {
                "cluster_id": label,
                "size": cluster_data.shape[0],
                "mean": np.mean(cluster_data, axis=0),
                "std": np.std(cluster_data, axis=0),
            }
        )

    return results


def run_networkx_analysis(parsed_data, key_name="reduced_cell"):
    """
    Performs NetworkX clique analysis and returns the single largest subset.
    The largest clique gives you the most homogenous and self-consistent "gold standard" subset of your data
    """
    print("Running NetworkX clique analysis...")

    import networkx as nx
    accepted_cells = []
    for i, item in enumerate(parsed_data):
        if item.get("accepted") and item.get(key_name):
            try:
                params = [float(p) for p in item[key_name].split()]
                if len(params) == 6:
                    accepted_cells.append(
                        {"original_index": i, "params": np.array(params)}
                    )
            except (ValueError, IndexError):
                continue
    if not accepted_cells:
        return []

    G = nx.Graph()
    for i in range(len(accepted_cells)):
        G.add_node(i)
    for i in range(len(accepted_cells)):
        for j in range(i + 1, len(accepted_cells)):
            diff = np.max(
                np.abs(accepted_cells[i]["params"] - accepted_cells[j]["params"])
                / (
                        (accepted_cells[i]["params"] + accepted_cells[j]["params"]) / 2.0
                        + 1e-9
                )
            )
            if diff < 0.05:
                G.add_edge(i, j)

    cliques = list(nx.find_cliques(G))
    if not cliques:
        return []
    max_clique = max(cliques, key=len)

    clique_params = [accepted_cells[i]["params"] for i in max_clique]

    # Return in the standardized format (a list with one dictionary)
    return [
        {
            "cluster_id": "max_clique",
            "size": len(clique_params),
            "mean": np.mean(clique_params, axis=0),
            "std": np.std(clique_params, axis=0),
        }
    ]


def run_networkx_community_analysis(
        parsed_data, key_name="reduced_cell", difference_threshold=0.05
):
    """
    Finds all clusters (communities) using a graph-based approach.
    The largest connected component gives you the most inclusive set of potentially related unit cells.
    """
    print("Running NetworkX community analysis (Connected Components)...")
    import networkx as nx
    accepted_cells = []
    for i, item in enumerate(parsed_data):
        if item.get(key_name):
            try:
                params = [float(p) for p in item[key_name].split()]
                if len(params) == 6:
                    accepted_cells.append(
                        {"original_index": i, "params": np.array(params)}
                    )
            except (ValueError, IndexError):
                continue

    if not accepted_cells:
        return []

    G = nx.Graph()
    for i in range(len(accepted_cells)):
        G.add_node(i)

    for i in range(len(accepted_cells)):
        for j in range(i + 1, len(accepted_cells)):
            diff = np.max(
                np.abs(accepted_cells[i]["params"] - accepted_cells[j]["params"])
                / (
                        (accepted_cells[i]["params"] + accepted_cells[j]["params"]) / 2.0
                        + 1e-9
                )
            )
            if diff < difference_threshold:
                G.add_edge(i, j)

    # Find all connected subgraphs (these are our clusters)
    communities = list(nx.connected_components(G))

    results = []
    for i, community in enumerate(communities):
        cluster_params = [
            accepted_cells[node_index]["params"] for node_index in community
        ]

        if len(cluster_params) > 1:
            results.append(
                {
                    "cluster_id": i,
                    "size": len(cluster_params),
                    "mean": np.mean(cluster_params, axis=0),
                    "std": np.std(cluster_params, axis=0),
                }
            )

    # Sort clusters by size (largest first) for a clean report
    results.sort(key=lambda x: x["size"], reverse=True)
    return results


def smart_analysis(parsed_data, size_threshold=150, key_name="reduced_cell"):
    """
    Automatically selects an analysis method and returns the results as a variable.
    """
    print(f"--- Starting Smart Analysis on '{key_name}' ---")
    valid_samples = [
        item for item in parsed_data if item.get("accepted") and item.get(key_name)
    ]
    sample_size = len(valid_samples)
    print(f"Found {sample_size} valid, accepted samples.")

    if sample_size >= size_threshold:
        print(f"Sample size is large (>= {size_threshold}).")
        return run_dbscan_analysis(valid_samples, key_name)
    else:
        print(f"Sample size is small (< {size_threshold}).")
        # return run_networkx_analysis(valid_samples, key_name)
        return run_networkx_community_analysis(valid_samples, key_name)


def format_analysis_report(analysis_results):
    """
    Takes the output from smart_analysis and formats it into a single string.
    """
    report_lines = []

    report_lines.append("--- Analysis Report ---")
    if not analysis_results:
        report_lines.append("No clusters or subsets could be determined.")
        return "\n".join(report_lines)

    report_lines.append(f"Found {len(analysis_results)} cluster(s).")
    param_names = ["a", "b", "c", "alpha", "beta", "gamma"]

    for cluster in analysis_results:
        report_lines.append(
            f"\n--- Cluster ID: {cluster['cluster_id']} (Size: {cluster['size']}) ---"
        )
        report_lines.append(f"{'Parameter':<10} {'Mean':>10} {'Std Dev':>10}")
        report_lines.append("-" * 32)
        for i, name in enumerate(param_names):
            report_lines.append(
                f"{name:<10} {cluster['mean'][i]:>10.2f} {cluster['std'][i]:>10.2f}"
            )

    return "\n".join(report_lines)


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: python idxref-nxds.py <path_to_IDXREF.LP.txt>")
    #     sys.exit(1)

    # Parse the log file and print the result as a JSON object
    filename = "IDXREF.LP.noSPG"
    # filename = "IDXREF.LP.SPG"
    # filename = "IDXREF.LP.raster"
    parsed_results = parse_nxds_idxref_log(filename)
    print(json.dumps(parsed_results, indent=4))

    analysis_output = smart_analysis(
        parsed_results, size_threshold=500, key_name="reduced_cell"
    )
    print(format_analysis_report(analysis_output))
    analysis_output = smart_analysis(
        parsed_results, size_threshold=500, key_name="unit_cell_parameters"
    )
    print(format_analysis_report(analysis_output))

    print(run_networkx_analysis(parsed_results, key_name="reduced_cell"))
    print(run_networkx_analysis(parsed_results, key_name="unit_cell_parameters"))
