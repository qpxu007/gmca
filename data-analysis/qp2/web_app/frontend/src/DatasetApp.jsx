import React, { useState, useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';
import { Camera, Database, Download, ExternalLink, Eye, FileText, Globe, Home, List, Search } from 'lucide-react';
import { api } from './api';
import Modal from 'react-modal';
import H5Viewer from './H5Viewer';
import SnapshotsModal from './SnapshotsModal';
import { GLOBUS_CONFIG } from './config';
import './DatasetApp.css';

const _SORTED_ENDPOINTS = [...GLOBUS_CONFIG.endpoints]
    .sort((a, b) => b.pathPrefix.length - a.pathPrefix.length);

Modal.setAppElement('#root');

const DatasetApp = () => {
    const [datasets, setDatasets] = useState([]);
    const [loading, setLoading] = useState(false);
    const [search, setSearch] = useState('');
    const [page, setPage] = useState(0);
    const [selectedMetadata, setSelectedMetadata] = useState(null);
    const [viewingFile, setViewingFile] = useState(null);
    const [snapshotDataset, setSnapshotDataset] = useState(null);
    const limit = 50;

    useEffect(() => {
        fetchDatasets();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [page]); // Re-fetch on page change

    const handleSearch = (e) => {
        e.preventDefault();
        setPage(0); // Reset to first page
        fetchDatasets();
    };

    const fetchDatasets = async () => {
        setLoading(true);
        try {
            const data = await api.listDatasets({
                search,
                limit,
                offset: page * limit,
                sort_by: 'created_at',
                sort_desc: true
            });
            setDatasets(data);
        } catch (e) {
            console.error("Failed to fetch datasets", e);
        } finally {
            setLoading(false);
        }
    };

    const getGlobusUrl = (rawPath) => {
        if (!rawPath) return "#";
        let filePath = rawPath;
        try {
            const parsed = JSON.parse(rawPath);
            if (Array.isArray(parsed) && parsed.length > 0) filePath = parsed[0];
        } catch { /* not JSON, use as-is */ }
        const cleanPath = filePath.startsWith('/') ? filePath : '/' + filePath;
        
        const endpoint = _SORTED_ENDPOINTS.find(ep => cleanPath.startsWith(ep.pathPrefix));
            
        const originId = endpoint ? endpoint.id : GLOBUS_CONFIG.defaultEndpoint;
        
        const dir = cleanPath.substring(0, cleanPath.lastIndexOf('/')) || '/';
        return `https://app.globus.org/file-manager?origin_id=${originId}&origin_path=${encodeURIComponent(dir)}`;
    };

    const [zipBanner, setZipBanner] = useState(null); // {jobId, dataId, status, progress}
    const zipPollRef = useRef(null);

    const stopZipPoll = () => {
        if (zipPollRef.current) { clearInterval(zipPollRef.current); zipPollRef.current = null; }
    };

    useEffect(() => () => stopZipPoll(), []);

    const handleZip = async (id) => {
        stopZipPoll();
        setZipBanner({ dataId: id, status: 'running', progress: 'Starting…' });
        try {
            const { job_id } = await api.startZipJob(id);
            zipPollRef.current = setInterval(async () => {
                try {
                    const status = await api.getZipStatus(job_id);
                    setZipBanner(b => ({ ...b, jobId: job_id, status: status.status, progress: status.progress || '' }));
                    if (status.status === 'done') {
                        stopZipPoll();
                        setZipBanner(b => ({ ...b, status: 'downloading' }));
                        const response = await api.downloadZip(job_id);
                        const filename = status.zip_filename || 'dataset.zip';
                        const url = window.URL.createObjectURL(new Blob([response.data]));
                        const link = document.createElement('a');
                        link.href = url;
                        link.setAttribute('download', filename);
                        document.body.appendChild(link);
                        link.click();
                        link.remove();
                        setTimeout(() => setZipBanner(null), 3000);
                    } else if (status.status === 'error') {
                        stopZipPoll();
                        setZipBanner(b => ({ ...b, status: 'error', progress: status.error || 'Zip failed.' }));
                    }
                } catch { /* keep polling on transient errors */ }
            }, 2000);
        } catch {
            setZipBanner({ dataId: id, status: 'error', progress: 'Failed to start zip job.' });
        }
    };

    const renderMetadata = (jsonString) => {
        try {
            const obj = JSON.parse(jsonString);
            return JSON.stringify(obj, null, 2);
        } catch {
            return jsonString;
        }
    };

    return (
        <div className="dataset-container">
            <div className="dataset-toolbar">
                <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#BBE1FA', textDecoration: 'none', marginRight: '10px' }}>
                    <Home size={20} />
                </Link>
                <h2 style={{ display: 'flex', alignItems: 'center', margin: 0, fontSize: '1.1rem', fontWeight: 600, color: '#fff', marginRight: '20px' }}>
                    <Database size={20} style={{ marginRight: '8px' }} /> Dataset Viewer
                </h2>
                <form onSubmit={handleSearch} style={{ display: 'flex', gap: '10px', flexWrap: 'wrap', flex: 1, minWidth: '250px' }}>
                    <input 
                        type="text" 
                        placeholder="Search run, type, files..." 
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        style={{ flex: 1, minWidth: '150px' }}
                    />
                    <button type="submit" className="pagination-btn">Search</button>
                </form>
            </div>

            <div className="dataset-table-container">
                <table className="dataset-table">
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Prefix</th>
                            <th>Type</th>
                            <th>Frames</th>
                            <th>Mounted</th>
                            <th>Spreadsheet</th>
                            <th>Master Files</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {loading ? (
                            <tr><td colSpan="8" style={{ textAlign: 'center' }}>Loading...</td></tr>
                        ) : datasets.length === 0 ? (
                            <tr><td colSpan="8" style={{ textAlign: 'center' }}>No datasets found.</td></tr>
                        ) : (
                            datasets.map(ds => (
                                <tr key={ds.data_id}>
                                    <td>{ds.created_at ? new Date(ds.created_at).toLocaleString() : '-'}</td>
                                    <td>{ds.run_prefix}</td>
                                    <td>{ds.collect_type}</td>
                                    <td>{ds.total_frames}</td>
                                    <td>{ds.mounted || '-'}</td>
                                    <td>
                                        {ds.meta_user ? (
                                            <button 
                                                className="metadata-btn"
                                                onClick={() => setSelectedMetadata(ds.meta_user)}
                                                title="View Spreadsheet Data"
                                            >
                                                <FileText size={14} /> View
                                            </button>
                                        ) : '-'}
                                    </td>
                                    <td style={{ maxWidth: '300px' }}>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
                                            <button
                                                className="metadata-btn"
                                                onClick={() => {
                                                    let files = ds.master_files;
                                                    try {
                                                        const parsed = JSON.parse(files);
                                                        if (Array.isArray(parsed)) files = parsed.join('\n');
                                                    } catch { /* not JSON */ }
                                                    setSelectedMetadata(files);
                                                }}
                                                title="List master files"
                                            >
                                                <List size={14} />
                                            </button>
                                            <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                                {(() => {
                                                    try {
                                                        const parsed = JSON.parse(ds.master_files);
                                                        if (Array.isArray(parsed)) return parsed[0].split('/').pop();
                                                    } catch { /* not JSON */ }
                                                    return ds.master_files;
                                                })()}
                                            </span>
                                        </div>
                                    </td>
                                    <td>
                                        <div style={{ display: 'flex', gap: '5px', flexWrap: 'wrap' }}>
                                            <button 
                                                className="metadata-btn"
                                                onClick={() => setSelectedMetadata(ds.headers)}
                                                title="View Metadata"
                                            >
                                                <FileText size={14} /> Meta
                                            </button>
                                            <Link
                                                to={`/viewer?dataset_id=${ds.data_id}`}
                                                className="metadata-btn"
                                                title="Open Image Viewer"
                                            >
                                                <Eye size={14} /> Viewer
                                            </Link>
                                            <button
                                                className="metadata-btn"
                                                onClick={() => {
                                                    let path = ds.master_files;
                                                    try {
                                                        const parsed = JSON.parse(path);
                                                        if (Array.isArray(parsed)) path = parsed[0];
                                                    } catch { /* not JSON, use as-is */ }
                                                    setViewingFile({ path, name: ds.run_prefix });
                                                }}
                                                title="Browse HDF5 Structure"
                                            >
                                                <FileText size={14} /> H5
                                            </button>
                                            <button
                                                className="metadata-btn"
                                                onClick={() => setSnapshotDataset(ds)}
                                                title="View Crystal Snapshots"
                                            >
                                                <Camera size={14} /> Snapshots
                                            </button>
                                            <a
                                                href={getGlobusUrl(ds.master_files)}
                                                target="_blank"
                                                rel="noopener noreferrer"
                                                title="Open in Globus"
                                                className="metadata-btn"
                                            >
                                                <Globe size={14} /> Globus
                                            </a>
                                            <button
                                                className="metadata-btn"
                                                onClick={() => handleZip(ds.data_id)}
                                                title="Download Full Dataset (Zip)"
                                                disabled={zipBanner?.dataId === ds.data_id && ['running', 'downloading'].includes(zipBanner?.status)}
                                            >
                                                {zipBanner?.dataId === ds.data_id && zipBanner?.status === 'running'
                                                    ? <><span className="btn-spinner" /> Zipping…</>
                                                    : zipBanner?.dataId === ds.data_id && zipBanner?.status === 'downloading'
                                                        ? <><span className="btn-spinner" /> Downloading…</>
                                                        : <><Download size={14} /> Zip</>}
                                            </button>
                                        </div>
                                        {zipBanner?.dataId === ds.data_id && zipBanner.status !== null && (
                                            <div style={{
                                                marginTop: '4px', fontSize: '0.72rem',
                                                color: zipBanner.status === 'error' ? '#e74c3c' : '#7ec8e3',
                                                maxWidth: '280px', overflow: 'hidden',
                                                textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                                            }} title={zipBanner.progress}>
                                                {zipBanner.progress}
                                            </div>
                                        )}
                                    </td>
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>

            <div className="pagination-controls">
                <button 
                    className="pagination-btn" 
                    onClick={() => setPage(p => Math.max(0, p - 1))}
                    disabled={page === 0 || loading}
                >
                    Previous
                </button>
                <span style={{ display: 'flex', alignItems: 'center' }}>Page {page + 1}</span>
                <button 
                    className="pagination-btn" 
                    onClick={() => setPage(p => p + 1)}
                    disabled={datasets.length < limit || loading}
                >
                    Next
                </button>
            </div>

            <Modal
                isOpen={!!selectedMetadata}
                onRequestClose={() => setSelectedMetadata(null)}
                contentLabel="Metadata"
                style={{
                    content: {
                        top: '5%',
                        left: '5%',
                        right: '5%',
                        bottom: '5%',
                        padding: '15px',
                        overflow: 'auto',
                        resize: 'both',
                    }
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '10px' }}>
                    <h3 style={{ margin: 0 }}>Metadata</h3>
                    <button onClick={() => setSelectedMetadata(null)} style={{ cursor: 'pointer', padding: '5px 10px' }}>Close</button>
                </div>
                <pre style={{ backgroundColor: '#f4f4f4', padding: '10px', borderRadius: '4px', overflowX: 'auto', whiteSpace: 'pre', fontSize: '0.85rem' }}>
                    {selectedMetadata ? renderMetadata(selectedMetadata) : 'No data'}
                </pre>
            </Modal>

            <H5Viewer
                isOpen={!!viewingFile}
                onClose={() => setViewingFile(null)}
                filePath={viewingFile?.path}
                filename={viewingFile?.name}
            />

            <SnapshotsModal
                isOpen={!!snapshotDataset}
                onClose={() => setSnapshotDataset(null)}
                dataset={snapshotDataset}
            />
        </div>
    );
};

export default DatasetApp;