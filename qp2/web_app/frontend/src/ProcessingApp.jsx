import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft, Columns, Download, FileText } from 'lucide-react';
import { api } from './api';
import TextModal from './TextModal';
import './ProcessingApp.css';

// Config derived from data_viewer/tab_config.py
const ALL_COLUMNS = [
    { key: "id", display: "ID", defaultVisible: false },
    { key: "name", display: "Sample", defaultVisible: true },
    { key: "pipeline", display: "Pipeline", defaultVisible: true },
    { key: "imageSet", display: "Image Set", defaultVisible: true },
    { key: "state", display: "State", defaultVisible: true },
    { key: "Summary", display: "Report", defaultVisible: false },
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
    { key: "truncate_mtz", display: "MTZ File", defaultVisible: true },
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
    
    const [showColumnMenu, setShowColumnMenu] = useState(false);
    const [viewingTable1, setViewingTable1] = useState(null);
    const limit = 50;

    useEffect(() => {
        fetchData();
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

    const handleViewReport = async (id, filePath) => {
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
                    onClick={() => handleViewReport(row.id, val)}
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

        if (colKey === 'logfile' || colKey === 'imagedir' || colKey === 'workdir') {
             return val;
        }
        return val;
    };

    return (
        <div className="processing-container">
            <div className="processing-toolbar">
                <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#333', textDecoration: 'none', marginRight: '10px' }}>
                    <ArrowLeft size={20} />
                </Link>
                <h2>Processing Results</h2>
                
                <form onSubmit={handleSearch} style={{ display: 'flex', gap: '10px', flex: 1 }}>
                    <input 
                        type="text" 
                        placeholder="Search sample, pipeline, state..." 
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                    />
                    <button type="submit" className="pagination-btn">Search</button>
                </form>

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
                            {ALL_COLUMNS.filter(c => visibleColumns.has(c.key)).map(col => (
                                <th key={col.key}>{col.display}</th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {loading ? (
                            <tr><td colSpan={visibleColumns.size} style={{ textAlign: 'center' }}>Loading...</td></tr>
                        ) : data.length === 0 ? (
                            <tr><td colSpan={visibleColumns.size} style={{ textAlign: 'center' }}>No results found.</td></tr>
                        ) : (
                            data.map(row => (
                                <tr key={row.id}>
                                    {ALL_COLUMNS.filter(c => visibleColumns.has(c.key)).map(col => (
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
        </div>
    );
};

export default ProcessingApp;
