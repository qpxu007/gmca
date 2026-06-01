import React, { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { AlertTriangle, Archive, ExternalLink, Home, RefreshCw } from 'lucide-react';
import { api } from './api';
import './ArchiveApp.css';

const STATUS_LABELS = {
    done: '✅ done',
    running: '⏳ running',
    submitted: '🕐 submitted',
    pending: '🕐 pending',
    failed: '❌ failed',
    permission_denied: '⚠️ permission denied',
};

function StatusBadge({ status }) {
    return (
        <span className={`status-badge ${status}`}>
            {STATUS_LABELS[status] ?? status}
        </span>
    );
}

function TypeBadge({ type }) {
    return <span className={`type-badge ${type}`}>{type}</span>;
}

function ScanModal({ mode, onClose }) {
    // mode: 'scan' | 'audit'
    // Both always show dry-run preview first, then Confirm actually submits.
    const [step, setStep] = useState('preview'); // preview | running | done
    const [preview, setPreview] = useState(null);
    const [previewLoading, setPreviewLoading] = useState(true);
    const [result, setResult] = useState(null);
    const [error, setError] = useState(null);

    useEffect(() => {
        setTimeout(() => setPreviewLoading(true), 0);
        const fn = mode === 'audit' ? api.archiveAudit : api.archiveScan;
        fn(true)
            .then(data => { setPreview(data); setPreviewLoading(false); })
            .catch(e => { setError(String(e)); setPreviewLoading(false); });
    }, [mode]);

    const handleConfirm = async () => {
        setStep('running');
        try {
            const fn = mode === 'audit' ? api.archiveAudit : api.archiveScan;
            const res = await fn(false);
            setResult(res);
            setStep('done');
        } catch (e) {
            setError(String(e));
            setStep('preview');
        }
    };

    return (
        <div className="archive-modal-overlay" onClick={e => e.target === e.currentTarget && onClose()}>
            <div className="archive-modal">
                <h3>{mode === 'audit' ? 'Full Audit' : 'Scan Now'}</h3>
                {mode === 'audit' && (
                    <div className="audit-warning">
                        ⚠️ This will scan <strong>all ESAFs</strong> regardless of age.
                        Review the preview below before submitting.
                    </div>
                )}
                {error && <div style={{ color: 'red', marginBottom: 12 }}>{error}</div>}
                {previewLoading && step === 'preview' && <p style={{ color: '#888' }}>Loading preview… (this may take a minute)</p>}
                {step === 'running' && <p>Submitting uploads… please wait.</p>}
                {step === 'done' && result && (
                    <p>✅ {result.jobs_submitted ?? result.submitted?.length ?? 0} job(s) submitted.</p>
                )}
                {preview && step === 'preview' && (
                    <>
                        <p style={{ fontSize: '0.85rem', color: '#555' }}>
                            Found <strong>{preview.submitted?.length ?? 0}</strong> directories to upload.
                        </p>
                        {preview.submitted?.length > 0 && (
                            <table className="archive-table" style={{ marginBottom: 8 }}>
                                <thead>
                                    <tr>
                                        <th>ESAF</th><th>Type</th><th>Run</th><th>Command</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {preview.submitted.map((s, i) => (
                                        <tr key={i}>
                                            <td>{s.esaf_id}</td>
                                            <td><TypeBadge type={s.dir_type} /></td>
                                            <td>{s.run_name ?? '—'}</td>
                                            <td style={{ fontSize: '0.75rem', fontFamily: 'monospace', wordBreak: 'break-all' }}>
                                                {s.planned_command}
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        )}
                        {preview.permission_denied?.length > 0 && (
                            <div className="permission-denied-section">
                                <h4>⚠️ Permission Denied ({preview.permission_denied.length})</h4>
                                <ul>
                                    {preview.permission_denied.map((d, i) => (
                                        <li key={i}>{d.data_directory}</li>
                                    ))}
                                </ul>
                            </div>
                        )}
                    </>
                )}
                <div className="archive-modal-actions">
                    <button className="archive-btn secondary" onClick={onClose}>Close</button>
                    {step === 'preview' && preview && preview.submitted?.length > 0 && (
                        <button className="archive-btn primary" onClick={handleConfirm}>
                            Confirm & Submit
                        </button>
                    )}
                </div>
            </div>
        </div>
    );
}

export default function ArchiveApp() {
    const [jobs, setJobs] = useState([]);
    const [status, setStatus] = useState(null);
    const [loading, setLoading] = useState(false);
    const [isStaff, setIsStaff] = useState(false);
    const [filterStatus, setFilterStatus] = useState('');
    const [filterType, setFilterType] = useState('');
    const [search, setSearch] = useState('');
    const [selected, setSelected] = useState(new Set());
    const [expandedRows, setExpandedRows] = useState(new Set());
    const [modal, setModal] = useState(null); // null | 'scan' | 'audit'
    const [reuploadDry, setReuploadDry] = useState(true);
    const [reuploadSkipDone, setReuploadSkipDone] = useState(true);
    const [reuploadResult, setReuploadResult] = useState(null);

    const loadJobs = useCallback(async () => {
        setLoading(true);
        try {
            const params = {};
            if (filterStatus) params.status = filterStatus;
            if (filterType) params.dir_type = filterType;
            if (search) params.esaf_id = search;
            const data = await api.archiveListJobs(params);
            setJobs(data);
        } catch (e) {
            console.error('Failed to load archive jobs', e);
        } finally {
            setLoading(false);
        }
    }, [filterStatus, filterType, search]);

    const loadStatus = useCallback(async () => {
        try {
            const data = await api.archiveStatus();
            setStatus(data);
            setIsStaff(true);
        } catch (e) {
            if (e.response?.status === 403) setIsStaff(false);
        }
    }, []);

    useEffect(() => { loadJobs(); }, [loadJobs]);
    useEffect(() => { loadStatus(); }, [loadStatus]);

    const toggleRow = (id) => setExpandedRows(prev => {
        const next = new Set(prev);
        next.has(id) ? next.delete(id) : next.add(id);
        return next;
    });

    const toggleSelect = (id) => setSelected(prev => {
        const next = new Set(prev);
        next.has(id) ? next.delete(id) : next.add(id);
        return next;
    });

    const toggleSelectAll = () => {
        if (selected.size === jobs.length) setSelected(new Set());
        else setSelected(new Set(jobs.map(j => j.id)));
    };

    const handleReupload = async () => {
        const ids = [...selected];
        try {
            const res = await api.archiveReupload({
                ids,
                skip_completed: reuploadSkipDone,
                dry_run: reuploadDry,
            });
            setReuploadResult(res);
            if (!reuploadDry) { setSelected(new Set()); loadJobs(); }
        } catch (e) {
            console.error('Reupload failed', e);
        }
    };

    const formatDate = (iso) => iso ? new Date(iso).toLocaleString() : '—';
    const stalled = status?.last_scan?.stalled;
    const lastScanText = status?.last_scan?.started_at
        ? `Last scan: ${formatDate(status.last_scan.started_at)}`
        : 'No scan yet';

    return (
        <div className="archive-container">
            {/* Toolbar */}
            <div className="archive-toolbar">
                <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#BBE1FA', textDecoration: 'none', marginRight: '10px' }}>
                    <Home size={20} />
                </Link>
                <h2 style={{ display: 'flex', alignItems: 'center', margin: 0, fontSize: '1.1rem', fontWeight: 600, color: '#fff', marginRight: '20px' }}>
                    <Archive size={20} style={{ marginRight: '8px' }} /> APS Archive Status
                </h2>
                
                <div className="filters">
                    <select value={filterStatus} onChange={e => setFilterStatus(e.target.value)}>
                        <option value="">All statuses</option>
                        {['submitted','pending','running','done','failed','permission_denied'].map(s => (
                            <option key={s} value={s}>{s}</option>
                        ))}
                    </select>
                    <select value={filterType} onChange={e => setFilterType(e.target.value)}>
                        <option value="">All types</option>
                        <option value="DATA">DATA</option>
                        <option value="PROCESSING">PROCESSING</option>
                    </select>
                    <input
                        placeholder="Search ESAF…"
                        value={search}
                        onChange={e => setSearch(e.target.value)}
                        style={{ width: 160 }}
                    />
                    <button className="archive-btn secondary" onClick={loadJobs} disabled={loading} style={{ display: 'flex', alignItems: 'center' }}>
                        <RefreshCw size={14} style={{ marginRight: 4 }} />
                        Refresh
                    </button>
                </div>

                {isStaff && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginLeft: 'auto' }}>
                        <span className={`scan-status-chip${stalled ? ' stalled' : ''}`}>
                            {stalled && <AlertTriangle size={12} style={{ marginRight: 4 }} />}
                            {lastScanText}
                        </span>
                        <button className="archive-btn secondary" onClick={() => setModal('scan')}>
                            Dry Run / Scan Now
                        </button>
                        <button className="archive-btn warning" onClick={() => setModal('audit')}>
                            Full Audit
                        </button>
                    </div>
                )}
            </div>

            {/* Bulk retry bar */}
            {isStaff && selected.size > 0 && (
                <div className="archive-toolbar" style={{ background: '#eaf4ff', padding: '8px 12px', borderRadius: 6, marginBottom: 12 }}>
                    <span style={{ fontSize: '0.85rem' }}>{selected.size} selected</span>
                    <label style={{ fontSize: '0.82rem', display: 'flex', alignItems: 'center', gap: 4 }}>
                        <input type="checkbox" checked={reuploadSkipDone}
                               onChange={e => setReuploadSkipDone(e.target.checked)} />
                        Skip completed
                    </label>
                    <label style={{ fontSize: '0.82rem', display: 'flex', alignItems: 'center', gap: 4 }}>
                        <input type="checkbox" checked={reuploadDry}
                               onChange={e => setReuploadDry(e.target.checked)} />
                        Dry run
                    </label>
                    <button className="archive-btn primary" onClick={handleReupload}>
                        Retry Selected
                    </button>
                    {reuploadResult && (
                        <span style={{ fontSize: '0.82rem', color: '#1e8449' }}>
                            {reuploadResult.submitted} submitted, {reuploadResult.skipped} skipped
                        </span>
                    )}
                </div>
            )}

            {/* Table */}
            <div className="archive-table-wrap">
                <table className="archive-table">
                    <thead>
                        <tr>
                            {isStaff && (
                                <th>
                                    <input type="checkbox"
                                           checked={selected.size === jobs.length && jobs.length > 0}
                                           onChange={toggleSelectAll} />
                                </th>
                            )}
                            <th>ESAF</th>
                            <th>Type</th>
                            <th>Run</th>
                            <th>Status</th>
                            <th>Files</th>
                            <th>Submitted</th>
                            <th>Globus</th>
                            {isStaff && <th>Error</th>}
                        </tr>
                    </thead>
                    <tbody>
                        {loading && (
                            <tr><td colSpan={isStaff ? 9 : 8} style={{ textAlign: 'center', padding: 20 }}>Loading…</td></tr>
                        )}
                        {!loading && jobs.length === 0 && (
                            <tr><td colSpan={isStaff ? 9 : 8} style={{ textAlign: 'center', color: '#888', padding: 20 }}>No archive jobs found.</td></tr>
                        )}
                        {jobs.map(job => (
                            <React.Fragment key={job.id}>
                                <tr className={job.status === 'failed' || job.status === 'permission_denied' ? 'error-row' : ''}>
                                    {isStaff && (
                                        <td>
                                            <input type="checkbox"
                                                   checked={selected.has(job.id)}
                                                   onChange={() => toggleSelect(job.id)} />
                                        </td>
                                    )}
                                    <td>{job.esaf_id}</td>
                                    <td><TypeBadge type={job.dir_type} /></td>
                                    <td>{job.run_name ?? '—'}</td>
                                    <td><StatusBadge status={job.status} /></td>
                                    <td>{job.count_files ?? '—'}</td>
                                    <td style={{ whiteSpace: 'nowrap', fontSize: '0.8rem' }}>{formatDate(job.submitted_at)}</td>
                                    <td>
                                        <button
                                            className="globus-btn"
                                            disabled={job.status !== 'done' || !job.globus_url}
                                            onClick={() => job.globus_url && window.open(job.globus_url, '_blank')}
                                            title={job.globus_url ?? 'Not yet archived'}
                                        >
                                            <ExternalLink size={12} /> Globus
                                        </button>
                                    </td>
                                    {isStaff && (
                                        <td>
                                            {job.error_message && (
                                                <button className="archive-btn secondary"
                                                        style={{ fontSize: '0.75rem', padding: '2px 6px' }}
                                                        onClick={() => toggleRow(job.id)}>
                                                    {expandedRows.has(job.id) ? '▲' : '▼'}
                                                </button>
                                            )}
                                        </td>
                                    )}
                                </tr>
                                {isStaff && expandedRows.has(job.id) && job.error_message && (
                                    <tr>
                                        <td colSpan={9}>
                                            <div className="error-detail">{job.error_message}</div>
                                        </td>
                                    </tr>
                                )}
                            </React.Fragment>
                        ))}
                    </tbody>
                </table>
            </div>

            {/* Modals */}
            {(modal === 'scan' || modal === 'audit') && (
                <ScanModal
                    mode={modal}
                    onClose={() => { setModal(null); loadJobs(); }}
                />
            )}
        </div>
    );
}
