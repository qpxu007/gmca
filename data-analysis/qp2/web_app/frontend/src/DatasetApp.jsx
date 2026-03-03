import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft, Search, FileText, Download, ExternalLink, Eye } from 'lucide-react';
import { api } from './api';
import Modal from 'react-modal';
import H5Viewer from './H5Viewer';
import { GLOBUS_CONFIG } from './config';
import './DatasetApp.css';

Modal.setAppElement('#root');

const DatasetApp = () => {
    const [datasets, setDatasets] = useState([]);
    const [loading, setLoading] = useState(false);
    const [search, setSearch] = useState('');
    const [page, setPage] = useState(0);
    const [selectedMetadata, setSelectedMetadata] = useState(null);
    const [viewingFile, setViewingFile] = useState(null);
    const limit = 50;

    useEffect(() => {
        fetchDatasets();
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

    const getGlobusUrl = (path) => {
        if (!path) return "#";
        const cleanPath = path.startsWith('/') ? path : '/' + path;
        
        // Find matching endpoint: sort by length desc to match longest prefix
        const endpoint = GLOBUS_CONFIG.endpoints
            .sort((a, b) => b.pathPrefix.length - a.pathPrefix.length)
            .find(ep => cleanPath.startsWith(ep.pathPrefix));
            
        const originId = endpoint ? endpoint.id : GLOBUS_CONFIG.defaultEndpoint;
        
        const dir = cleanPath.substring(0, cleanPath.lastIndexOf('/')) || '/';
        return `https://app.globus.org/file-manager?origin_id=${originId}&origin_path=${encodeURIComponent(dir)}`;
    };

    const handleDownload = async (id, filePath, mode = 'master') => {
        try {
            let filename = filePath ? filePath.split('/').pop() : `dataset_${id}.h5`;
            if (mode === 'archive') {
                // Heuristic to create a nice zip filename
                filename = filename.replace('master.h5', '') + 'dataset.zip';
            }
            
            const response = await api.downloadDataset(id, mode);
            const url = window.URL.createObjectURL(new Blob([response.data]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', filename);
            document.body.appendChild(link);
            link.click();
            link.remove();
        } catch (e) {
            console.error("Download failed", e);
            alert("Failed to download file. It might not exist on the server or the archive creation failed.");
        }
    };

    const renderMetadata = (jsonString) => {
        try {
            const obj = JSON.parse(jsonString);
            return JSON.stringify(obj, null, 2);
        } catch (e) {
            return jsonString;
        }
    };

    return (
        <div className="dataset-container">
            <div className="dataset-toolbar">
                <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#333', textDecoration: 'none', marginRight: '10px' }}>
                    <ArrowLeft size={20} />
                </Link>
                <h2>Dataset Viewer</h2>
                <form onSubmit={handleSearch} style={{ display: 'flex', gap: '10px' }}>
                    <input 
                        type="text" 
                        placeholder="Search run, type, files..." 
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
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
                            <th>Master Files (Globus)</th>
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
                                    <td style={{ maxWidth: '300px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                        <a 
                                            href={getGlobusUrl(ds.master_files)} 
                                            target="_blank" 
                                            rel="noopener noreferrer"
                                            title={`Open ${ds.master_files} in Globus`}
                                            style={{ color: '#0056b3', display: 'flex', alignItems: 'center', gap: '5px' }}
                                        >
                                            <ExternalLink size={14} />
                                            {ds.master_files}
                                        </a>
                                    </td>
                                    <td>
                                        <div style={{ display: 'flex', gap: '5px' }}>
                                            <button 
                                                className="metadata-btn"
                                                onClick={() => setSelectedMetadata(ds.headers)}
                                                title="View Metadata"
                                            >
                                                <FileText size={14} /> Meta
                                            </button>
                                            <button 
                                                className="metadata-btn"
                                                onClick={() => setViewingFile({ path: ds.master_files, name: ds.run_prefix })}
                                                title="Open HDF5 Viewer"
                                            >
                                                <Eye size={14} /> Viewer
                                            </button>
                                            <button 
                                                className="metadata-btn"
                                                onClick={() => handleDownload(ds.data_id, ds.master_files, 'archive')}
                                                title="Download Full Dataset (Zip)"
                                            >
                                                <Download size={14} /> Zip
                                            </button>
                                        </div>
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
                        top: '50%',
                        left: '50%',
                        right: 'auto',
                        bottom: 'auto',
                        marginRight: '-50%',
                        transform: 'translate(-50%, -50%)',
                        width: '600px',
                        maxHeight: '80vh',
                        overflow: 'auto'
                    }
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '10px' }}>
                    <h3>Metadata</h3>
                    <button onClick={() => setSelectedMetadata(null)} style={{ cursor: 'pointer' }}>Close</button>
                </div>
                <pre style={{ backgroundColor: '#f4f4f4', padding: '10px', borderRadius: '4px', overflowX: 'auto', whiteSpace: 'pre-wrap', wordWrap: 'break-word' }}>
                    {selectedMetadata ? renderMetadata(selectedMetadata) : 'No data'}
                </pre>
            </Modal>

            <H5Viewer 
                isOpen={!!viewingFile}
                onClose={() => setViewingFile(null)}
                filePath={viewingFile?.path}
                filename={viewingFile?.name}
            />
        </div>
    );
};

export default DatasetApp;