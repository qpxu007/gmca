# qp2/image_viewer/plugins/nxds/nxds_utils.py
import numpy as np


def find_elbow_for_dbscan(data):
    """Find a suitable eps value for DBSCAN using the k-distance graph."""
    from sklearn.neighbors import NearestNeighbors
    from sklearn.preprocessing import StandardScaler

    if len(data) < 12: return 0.5
    scaled_data = StandardScaler().fit_transform(data)
    k = min(12, len(data) - 1)
    if k <= 0: return 0.5
    neighbors = NearestNeighbors(n_neighbors=k).fit(scaled_data)
    distances, _ = neighbors.kneighbors(scaled_data)
    k_distances = np.sort(distances[:, k - 1])
    # Very simple derivative-based elbow find
    try:
        gradients = np.gradient(np.gradient(k_distances))
        elbow_index = np.argmax(gradients)
        return k_distances[elbow_index]
    except IndexError:
        return 0.5


def run_dbscan_analysis(data_array):
    from sklearn.cluster import DBSCAN
    from sklearn.preprocessing import StandardScaler

    eps = find_elbow_for_dbscan(data_array)
    db = DBSCAN(eps=eps, min_samples=max(5, int(len(data_array) * 0.01))).fit(
        StandardScaler().fit_transform(data_array))
    labels = db.labels_

    results = []
    for label in sorted(list(set(labels))):
        if label == -1: continue
        cluster_mask = labels == label
        cluster_data = data_array[cluster_mask]
        results.append({
            "cluster_id": label, "size": len(cluster_data),
            "mean": np.mean(cluster_data, axis=0), "std": np.std(cluster_data, axis=0)
        })
    results.sort(key=lambda x: x['size'], reverse=True)
    return results


def run_networkx_community_analysis(data_array, threshold=0.05):
    import networkx as nx

    G = nx.Graph()
    for i in range(len(data_array)): G.add_node(i)

    for i in range(len(data_array)):
        for j in range(i + 1, len(data_array)):
            diff = np.max(np.abs(data_array[i] - data_array[j]) / ((data_array[i] + data_array[j]) / 2.0 + 1e-9))
            if diff < threshold: G.add_edge(i, j)

    communities = list(nx.connected_components(G))
    results = []
    for i, community in enumerate(communities):
        if len(community) > 1:
            cluster_params = [data_array[node_index] for node_index in community]
            results.append({
                "cluster_id": i, "size": len(cluster_params),
                "mean": np.mean(cluster_params, axis=0), "std": np.std(cluster_params, axis=0)
            })
    results.sort(key=lambda x: x['size'], reverse=True)
    return results
