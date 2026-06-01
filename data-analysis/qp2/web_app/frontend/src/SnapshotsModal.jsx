import React, { useEffect, useState } from 'react';
import Modal from 'react-modal';
import { Camera, X } from 'lucide-react';
import { api } from './api';
import './SnapshotsModal.css';

// Modal showing all crystal-camera snapshots associated with a single
// dataset. Combines explicit FK matches and implicit port+time-window
// matches; the matched_via field on each row tells which.
//
// Until pybluice's CAMERA-event patch ships, this modal will always
// show "No snapshots indexed" — that's expected. See
// qp2/data_proc/server/PYBLUICE_SNAPSHOT_INTEGRATION.md.

const formatTimestamp = (iso) => {
    if (!iso) return '';
    try {
        return new Date(iso).toLocaleString();
    } catch {
        return iso;
    }
};

const SnapshotThumbnail = ({ snapshot, onClick }) => {
    const [blobUrl, setBlobUrl] = useState(null);
    const [failed, setFailed] = useState(false);

    useEffect(() => {
        let cancelled = false;
        let url = null;
        api.fetchSnapshotBlobUrl(snapshot.id)
            .then((u) => {
                if (cancelled) {
                    URL.revokeObjectURL(u);
                } else {
                    url = u;
                    setBlobUrl(u);
                }
            })
            .catch(() => { if (!cancelled) setFailed(true); });
        return () => {
            cancelled = true;
            if (url) URL.revokeObjectURL(url);
        };
    }, [snapshot.id]);

    const caption = [
        snapshot.port,
        snapshot.omega !== null && snapshot.omega !== undefined ? `${snapshot.omega}°` : null,
        formatTimestamp(snapshot.captured_at),
    ].filter(Boolean).join(' · ');

    return (
        <div className="snapshot-thumb" onClick={() => blobUrl && onClick(blobUrl, snapshot)}>
            <div className="snapshot-thumb-img">
                {failed ? (
                    <div className="snapshot-thumb-error">image unavailable</div>
                ) : blobUrl ? (
                    <img src={blobUrl} alt={`Snapshot ${snapshot.id}`} loading="lazy" />
                ) : (
                    <div className="snapshot-thumb-loading">loading…</div>
                )}
                <span
                    className={`snapshot-badge snapshot-badge-${snapshot.matched_via === 'explicit_fk' ? 'linked' : 'near'}`}
                    title={snapshot.matched_via === 'explicit_fk'
                        ? 'Linked at index time via port + beamline + time window'
                        : 'Inferred match via port + beamline + time window at query time'}
                >
                    {snapshot.matched_via === 'explicit_fk' ? 'linked' : 'near'}
                </span>
            </div>
            <div className="snapshot-caption">{caption}</div>
        </div>
    );
};

const SnapshotsModal = ({ isOpen, onClose, dataset }) => {
    const [snapshots, setSnapshots] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [zoom, setZoom] = useState(null); // { url, snapshot }

    useEffect(() => {
        if (!isOpen || !dataset) {
            Promise.resolve().then(() => {
                setSnapshots([]);
                setError(null);
            });
            return;
        }
        let cancelled = false;
        Promise.resolve().then(() => {
            setLoading(true);
            setError(null);
        });
        api.listDatasetSnapshots(dataset.data_id)
            .then((rows) => { if (!cancelled) setSnapshots(rows || []); })
            .catch((e) => { if (!cancelled) setError(e?.message || 'Failed to load snapshots'); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [isOpen, dataset]);

    if (!isOpen) return null;

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Crystal Snapshots"
            style={{
                content: {
                    top: '5%', left: '5%', right: '5%', bottom: '5%',
                    padding: '0', overflow: 'hidden',
                }
            }}
        >
            <div className="snapshots-modal">
                <div className="snapshots-modal-header">
                    <h3>
                        <Camera size={18} style={{ verticalAlign: '-3px', marginRight: 6 }} />
                        Snapshots — {dataset?.run_prefix || 'dataset'}
                        {dataset?.mounted && <span className="snapshots-port"> · port {dataset.mounted}</span>}
                    </h3>
                    <button onClick={onClose} className="snapshots-close-btn">Close</button>
                </div>

                <div className="snapshots-modal-body">
                    {loading && <div className="snapshots-status">Loading…</div>}
                    {error && <div className="snapshots-status snapshots-error">Error: {error}</div>}
                    {!loading && !error && snapshots.length === 0 && (
                        <div className="snapshots-status">
                            <div>No snapshots indexed for this dataset.</div>
                            <div className="snapshots-status-hint">
                                Snapshot capture in pybluice is shipped, but the qp2-side index requires
                                the upstream patch described in{' '}
                                <code>qp2/data_proc/server/PYBLUICE_SNAPSHOT_INTEGRATION.md</code>.
                            </div>
                        </div>
                    )}
                    {!loading && !error && snapshots.length > 0 && (
                        <div className="snapshot-grid">
                            {snapshots.map((s) => (
                                <SnapshotThumbnail
                                    key={s.id}
                                    snapshot={s}
                                    onClick={(url, snap) => setZoom({ url, snapshot: snap })}
                                />
                            ))}
                        </div>
                    )}
                </div>

                {zoom && (
                    <div className="snapshot-zoom-overlay" onClick={() => setZoom(null)}>
                        <button
                            className="snapshot-zoom-close"
                            onClick={(e) => { e.stopPropagation(); setZoom(null); }}
                            title="Close"
                        >
                            <X size={20} />
                        </button>
                        <img src={zoom.url} alt={`Snapshot ${zoom.snapshot.id}`} />
                        <div className="snapshot-zoom-caption">
                            {[zoom.snapshot.port, zoom.snapshot.omega != null ? `${zoom.snapshot.omega}°` : null,
                              zoom.snapshot.sample_prefix, formatTimestamp(zoom.snapshot.captured_at),
                              zoom.snapshot.file_path].filter(Boolean).join(' · ')}
                        </div>
                    </div>
                )}
            </div>
        </Modal>
    );
};

export default SnapshotsModal;
