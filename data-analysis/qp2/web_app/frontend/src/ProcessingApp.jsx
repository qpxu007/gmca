import React, { useState, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Activity, Atom, Columns, Download, FileText, Globe, Home, RefreshCw } from 'lucide-react';
import { api } from './api';
import TextModal from './TextModal';
import ReprocessModal from './ReprocessModal';
import StructureViewerModal from './StructureViewerModal';
import { GLOBUS_CONFIG } from './config';
import './ProcessingApp.css';

// Sort once at module level — getGlobusUrl called per row per render
const _SORTED_ENDPOINTS = [...GLOBUS_CONFIG.endpoints]
    .sort((a, b) => b.pathPrefix.length - a.pathPrefix.length);

// Config derived from data_viewer/tab_config.py
const ALL_COLUMNS = [
    { key: "id", display: "ID", defaultVisible: false },
    { key: "name", display: "Sample", defaultVisible: false },
    { key: "pipeline", display: "Pipeline", defaultVisible: true },
    { key: "imageSet", display: "Image Set", defaultVisible: true },
    { key: "state", display: "State", defaultVisible: true },
    { key: "Summary", display: "Report URL", defaultVisible: false },
    { key: "isa", display: "ISa", defaultVisible: false },
    { key: "wav", display: "Wavelength", defaultVisible: false },
    { key: "Symm", display: "Space Group", defaultVisible: true },
    { key: "Cell", display: "Unit Cell", defaultVisible: false },
    { key: "h_res", display: "Res.", defaultVisible: true },
    { key: "Rsym", display: "Rsym", defaultVisible: true },
    { key: "Rmeas", display: "Rmeas", defaultVisible: false },
    { key: "Rpim", display: "Rpim", defaultVisible: false },
    { key: "IsigI", display: "I/sig(I)", defaultVisible: true },
    { key: "multi", display: "Mult.", defaultVisible: true },
    { key: "Cmpl", display: "Compl. %", defaultVisible: true },
    { key: "a_Cmpl", display: "Anom. Compl. %", defaultVisible: false },
    { key: "warning", display: "Warning", defaultVisible: false },
    { key: "logfile", display: "Log File", defaultVisible: false },
    { key: "table1", display: "Table1", defaultVisible: true },
    { key: "elapsedtime", display: "Time", defaultVisible: false },
    { key: "imagedir", display: "Image Dir", defaultVisible: false },
    { key: "firstFrame", display: "Start Frame", defaultVisible: false },
    { key: "workdir", display: "Work Dir", defaultVisible: false },
    { key: "scale_log", display: "Scale Log", defaultVisible: false },
    { key: "truncate_log", display: "Truncate Log", defaultVisible: false },
    { key: "truncate_mtz", display: "MTZ File", defaultVisible: false },
    { key: "run_stats", display: "Run Stats", defaultVisible: false },
    { key: "reprocess", display: "Reprocess ID", defaultVisible: false },
    { key: "solve", display: "Solve", defaultVisible: false },
    { key: "delete", display: "Delete", defaultVisible: false },
];

const ProcessingApp = () => {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(false);
    const [search, setSearch] = useState('');
    const [page, setPage] = useState(0);
    
    // Initialize visible columns set
    const [visibleColumns, setVisibleColumns] = useState(() => {
        const initial = new Set();
        ALL_COLUMNS.forEach(c => {
            if (c.defaultVisible) initial.add(c.key);
        });
        return initial;
    });
    
    const getGlobusUrl = (path) => {
        if (!path) return "#";
        const cleanPath = path.startsWith('/') ? path : '/' + path;
        const endpoint = _SORTED_ENDPOINTS.find(ep => cleanPath.startsWith(ep.pathPrefix));
        const originId = endpoint ? endpoint.id : GLOBUS_CONFIG.defaultEndpoint;
        const dir = cleanPath.substring(0, cleanPath.lastIndexOf('/')) || cleanPath;
        return `https://app.globus.org/file-manager?origin_id=${originId}&origin_path=${encodeURIComponent(dir)}`;
    };

    const [showColumnMenu, setShowColumnMenu] = useState(false);
    const [viewingTable1, setViewingTable1] = useState(null);
    const [viewingStructure, setViewingStructure] = useState(null);
    const [selected, setSelected] = useState(new Set());
    const [showReprocessModal, setShowReprocessModal] = useState(false);
    const [reprocessStatus, setReprocessStatus] = useState(null);
    const limit = 50;

    const toggleRow = (id) => {
        setSelected(prev => {
            const next = new Set(prev);
            next.has(id) ? next.delete(id) : next.add(id);
            return next;
        });
    };

    const toggleAll = () => {
        if (selected.size === data.length && data.length > 0) {
            setSelected(new Set());
        } else {
            setSelected(new Set(data.map(r => r.id)));
        }
    };

    const selectedRows = useMemo(() => data.filter(r => selected.has(r.id)), [data, selected]);
    const activeColumns = useMemo(
        () => ALL_COLUMNS.filter(c => visibleColumns.has(c.key)),
        [visibleColumns]
    );

    useEffect(() => {
        fetchData();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [page]);

    const handleSearch = (e) => {
        e.preventDefault();
        setPage(0);
        fetchData();
    };

    const fetchData = async () => {
        setLoading(true);
        try {
            const res = await api.listProcessing({
                search,
                limit,
                offset: page * limit,
                sort_by: 'id',
                sort_desc: true
            });
            setData(res);
        } catch (e) {
            console.error(e);
        } finally {
            setLoading(false);
        }
    };

    const toggleColumn = (key) => {
        const newSet = new Set(visibleColumns);
        if (newSet.has(key)) {
            newSet.delete(key);
        } else {
            newSet.add(key);
        }
        setVisibleColumns(newSet);
    };

    const handleDownload = async (id, field, filePath) => {
        try {
            const filename = filePath ? filePath.split('/').pop() : `${field}_${id}.mtz`;
            const response = await api.downloadProcessingFile(id, field);
            const url = window.URL.createObjectURL(new Blob([response.data]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', filename);
            document.body.appendChild(link);
            link.click();
            link.remove();
        } catch (e) {
            console.error("Download failed", e);
            alert("Failed to download file. It might not exist on the server.");
        }
    };

    const handleViewReport = async (id) => {
        try {
            // Field name in DB model is report_url
            const response = await api.downloadProcessingFile(id, 'report_url');
            const blob = new Blob([response.data], { type: 'text/html' });
            const url = window.URL.createObjectURL(blob);
            window.open(url, '_blank');
            setTimeout(() => window.URL.revokeObjectURL(url), 60000); 
        } catch (e) {
            console.error("View report failed", e);
            alert("Failed to view report. It might not exist.");
        }
    };

    const renderCell = (row, colKey) => {
        const val = row[colKey];
        if (val === null || val === undefined) return '-';
        
        if (colKey === 'truncate_mtz') {
             if (!val) return '-';
             return (
                 <button 
                    onClick={() => handleDownload(row.id, 'truncate_mtz', val)}
                    title={val}
                    style={{ background: 'none', border: 'none', color: '#0056b3', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px', width: '100%' }}
                 >
                    <Download size={14} /> MTZ
                 </button>
             );
        }

        if (colKey === 'Summary') {
             if (!val) return '-';
             return (
                 <button 
                    onClick={() => handleViewReport(row.id)}
                    title="View HTML Report"
                    style={{ background: 'none', border: 'none', color: '#0056b3', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px', width: '100%' }}
                 >
                    <FileText size={14} /> Report
                 </button>
             );
        }

        if (colKey === 'table1') {
             if (!val || String(val).trim() === '') return '-';
             return (
                 <button 
                    onClick={() => setViewingTable1(val)}
                    title="View Table 1"
                    style={{ background: 'none', border: 'none', color: '#0056b3', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px', width: '100%' }}
                 >
                    <FileText size={14} /> View
                 </button>
             );
        }

        if (colKey === 'imagedir' || colKey === 'workdir') {
            if (!val) return '-';
            return (
                <a href={getGlobusUrl(val)} target="_blank" rel="noopener noreferrer"
                   title={val} style={{ color: '#0056b3', display: 'flex', alignItems: 'center', gap: '4px', justifyContent: 'center' }}>
                    <Globe size={14} /> {val.split('/').pop() || val}
                </a>
            );
        }

        if (colKey === 'logfile') {
             return val;
        }
        if (colKey === 'solve') {
            if (!val || String(val).trim() === '') return '-';
            return (
                <button
                    onClick={() => setViewingStructure(row)}
                    title={val}
                    style={{ background: 'none', border: 'none', color: '#0056b3', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px', width: '100%' }}
                >
                    <Atom size={14} /> View
                </button>
            );
        }
        return val;
    };

    return (
        <div className="processing-container">
            <div className="processing-toolbar">
                <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#BBE1FA', textDecoration: 'none', marginRight: '10px' }}>
                    <Home size={20} />
                </Link>
                <h2 style={{ display: 'flex', alignItems: 'center', margin: 0, fontSize: '1.1rem', fontWeight: 600, color: '#fff', marginRight: '20px' }}>
                    <Activity size={20} style={{ marginRight: '8px' }} /> Processing Results
                </h2>
                
                <form onSubmit={handleSearch} style={{ display: 'flex', gap: '10px', flex: 1 }}>
                    <input 
                        type="text" 
                        placeholder="Search sample, pipeline, state..." 
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                    />
                    <button type="submit" className="pagination-btn">Search</button>
                </form>

                {selected.size > 0 && (
                    <button
                        className="pagination-btn"
                        onClick={() => { setReprocessStatus(null); setShowReprocessModal(true); }}
                        style={{ display: 'flex', alignItems: 'center', gap: '5px', background: '#2980b9', color: '#fff', border: 'none' }}
                    >
                        <RefreshCw size={16} /> Reprocess ({selected.size})
                    </button>
                )}

                <div className="column-selector">
                    <button 
                        className="pagination-btn" 
                        onClick={() => setShowColumnMenu(!showColumnMenu)}
                        style={{ display: 'flex', alignItems: 'center', gap: '5px' }}
                    >
                        <Columns size={16} /> Columns
                    </button>
                    {showColumnMenu && (
                        <div className="column-menu">
                            <div className="column-menu-header">Show / Hide Columns</div>
                            {ALL_COLUMNS.map(col => (
                                <label key={col.key} className="column-item">
                                    <input
                                        type="checkbox"
                                        checked={visibleColumns.has(col.key)}
                                        onChange={() => toggleColumn(col.key)}
                                    />
                                    {col.display}
                                </label>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            <div className="processing-table-container">
                <table className="processing-table">
                    <thead>
                        <tr>
                            <th style={{ width: '32px', textAlign: 'center' }}>
                                <input
                                    type="checkbox"
                                    checked={data.length > 0 && selected.size === data.length}
                                    onChange={toggleAll}
                                    title="Select all"
                                />
                            </th>
                            <th style={{ textAlign: 'center', whiteSpace: 'nowrap' }}>Actions</th>
                            {activeColumns.map(col => (
                                <th key={col.key}>{col.display}</th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {loading ? (
                            <tr><td colSpan={activeColumns.length + 2} style={{ textAlign: 'center' }}>Loading...</td></tr>
                        ) : data.length === 0 ? (
                            <tr><td colSpan={activeColumns.length + 2} style={{ textAlign: 'center' }}>No results found.</td></tr>
                        ) : (
                            data.map(row => (
                                <tr key={row.id} style={{ background: selected.has(row.id) ? '#eaf4fb' : undefined }}>
                                    <td style={{ textAlign: 'center' }}>
                                        <input
                                            type="checkbox"
                                            checked={selected.has(row.id)}
                                            onChange={() => toggleRow(row.id)}
                                        />
                                    </td>
                                    <td>
                                        <div style={{ display: 'flex', gap: '5px', flexWrap: 'wrap' }}>
                                            {row.Summary && (
                                                <button onClick={() => handleViewReport(row.id)} title="View HTML Report"
                                                    className="metadata-btn">
                                                    <FileText size={14} /> Report
                                                </button>
                                            )}
                                            {row.truncate_mtz && (
                                                <button onClick={() => handleDownload(row.id, 'truncate_mtz', row.truncate_mtz)} title="Download truncate MTZ"
                                                    className="metadata-btn">
                                                    <Download size={14} /> MTZ
                                                </button>
                                            )}
                                            {row.workdir && (
                                                <a href={getGlobusUrl(row.workdir)} target="_blank" rel="noopener noreferrer"
                                                    title={`Open work dir in Globus:\n${row.workdir}`}
                                                    className="metadata-btn">
                                                    <Globe size={14} /> Globus
                                                </a>
                                            )}
                                            {row.solve && (
                                                <button onClick={() => setViewingStructure(row)} title="View Structure & Maps"
                                                    className="metadata-btn">
                                                    <Atom size={14} /> View
                                                </button>
                                            )}
                                        </div>
                                    </td>
                                    {activeColumns.map(col => (
                                        <td key={col.key}>{renderCell(row, col.key)}</td>
                                    ))}
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
                    disabled={data.length < limit || loading}
                >
                    Next
                </button>
            </div>

            <TextModal
                isOpen={!!viewingTable1}
                onClose={() => setViewingTable1(null)}
                title="Table 1"
                content={viewingTable1}
            />

            <StructureViewerModal
                isOpen={!!viewingStructure}
                onClose={() => setViewingStructure(null)}
                pipelineId={viewingStructure?.id}
                sampleName={viewingStructure?.name}
            />

            <ReprocessModal
                isOpen={showReprocessModal}
                onClose={() => setShowReprocessModal(false)}
                selectedRows={selectedRows}
                onSuccess={(result) => {
                    setReprocessStatus(`${result.submitted} job(s) submitted. Refresh to see new entries.`);
                    setSelected(new Set());
                    setTimeout(() => setReprocessStatus(null), 6000);
                }}
            />

            {reprocessStatus && (
                <div style={{
                    position: 'fixed',
                    bottom: '20px',
                    right: '20px',
                    background: '#27ae60',
                    color: '#fff',
                    padding: '12px 18px',
                    borderRadius: '6px',
                    zIndex: 2000,
                    fontSize: '0.9rem',
                    boxShadow: '0 2px 8px rgba(0,0,0,0.2)',
                }}>
                    {reprocessStatus}
                </div>
            )}
        </div>
    );
};

export default ProcessingApp;
