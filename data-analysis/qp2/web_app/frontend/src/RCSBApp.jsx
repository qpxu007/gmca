import React, { useState, useEffect, useMemo, useRef } from 'react';
import { Link } from 'react-router-dom';
import { BookOpen, Database, Download, Home, RefreshCw, Search, Upload } from 'lucide-react';
import { api } from './api';
import './RCSBApp.css';

const COLUMNS = {
  gmca: [
    'pdbcode', 'title', 'authors', 'deposit_date', 'release_date', 'collect_date',
    'detector', 'beamline', 'wavelength', 'resolution', 'rmerge', 'cchalf',
    'isigmai', 'redundancy', 'completeness', 'rmerge_shell', 'cchalf_shell',
    'rfree', 'rwork', 'data_reduction', 'data_scaling', 'doi', 'pubmed',
    'article_year', 'article',
  ],
  aps: [
    'pdbcode', 'title', 'authors', 'deposit_date', 'release_date', 'collect_date',
    'detector', 'beamline', 'wavelength', 'resolution', 'rmerge', 'cchalf',
    'isigmai', 'redundancy', 'completeness', 'rmerge_shell', 'cchalf_shell',
    'rfree', 'rwork', 'data_reduction', 'data_scaling', 'doi', 'pubmed',
    'article_year', 'article',
  ],
  aps_pub: [
    'in_aps_db', 'pdbcode', 'release_date', 'collect_date', 'beamline', 'doi', 'pubmed',
    'authors', 'title', 'article_year', 'journal_abbrev', 'journal_volume',
    'page_first', 'page_last',
  ],
  generic: [
    'pdbcode', 'beamline', 'source', 'authors', 'title', 'deposit_date',
    'release_date', 'collect_date', 'resolution', 'ligands', 'doi', 'pubmed',
    'article',
  ],
};

const REPORT_LABELS = {
  gmca: 'GMCA (23-ID-B/D)',
  aps: 'APS (all beamlines)',
  aps_pub: 'APS Publication',
  generic: 'Generic',
};

function isUrl(val) {
  return typeof val === 'string' && (val.startsWith('http://') || val.startsWith('https://'));
}

function CellValue({ col, value }) {
  if (value === null || value === undefined) return <td>—</td>;
  if (col === 'in_aps_db') {
    return <td className={value ? 'aps-yes' : 'aps-no'}>{value ? 'Yes' : 'No'}</td>;
  }
  if ((col === 'doi' || col === 'pubmed') && isUrl(String(value))) {
    return (
      <td>
        <a href={String(value)} target="_blank" rel="noopener noreferrer">
          {String(value).replace(/https?:\/\/(doi\.org|www\.ncbi\.nlm\.nih\.gov\/pubmed)\//, '')}
        </a>
      </td>
    );
  }
  return <td>{String(value)}</td>;
}

const RCSBApp = () => {
  const [reportType, setReportType] = useState('gmca');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [searchText, setSearchText] = useState('');
  const [pdbCodes, setPdbCodes] = useState('');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [progressMsg, setProgressMsg] = useState('');
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState('');
  const [sortCol, setSortCol] = useState(null);
  const [sortAsc, setSortAsc] = useState(true);
  const pollRef = useRef(null);

  // APS DB sync state
  const [apsStatus, setApsStatus] = useState(null);
  const [syncing, setSyncing] = useState(false);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef(null);
  const [scheduledRecipients, setScheduledRecipients] = useState({
    gmca_weekly: [],
    aps_pub_monthly: [],
  });
  const [newEmailInput, setNewEmailInput] = useState({ gmca_weekly: '', aps_pub_monthly: '' });
  const [saveStatus, setSaveStatus] = useState({ gmca_weekly: null, aps_pub_monthly: null });

  useEffect(() => {
    api.rcsbGetScheduledRecipients()
      .then(data => setScheduledRecipients(data))
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (reportType === 'aps_pub') {
      loadApsStatus();
    }
  }, [reportType]);

  const loadApsStatus = async () => {
    try {
      const data = await api.rcsbApsDbStatus();
      setApsStatus(data);
    } catch {
      // ignore
    }
  };

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  useEffect(() => () => stopPolling(), []);

  const handleSearch = async (e) => {
    e.preventDefault();
    stopPolling();
    setLoading(true);
    setError('');
    setResults([]);
    setSortCol(null);
    setProgressMsg('');

    const params = { report_type: reportType };
    if (startDate) params.start_date = startDate;
    if (endDate) params.end_date = endDate;
    if (searchText.trim()) params.search_text = searchText.trim();
    if (pdbCodes.trim()) {
      params.pdb_codes = pdbCodes.trim().split(/[\s,]+/).filter(Boolean);
    }

    try {
      const data = await api.rcsbSearch(params);
      if (!data.job_id) {
        // Synchronous fallback (shouldn't happen with current backend)
        setResults(data.results || []);
        if (!data.count) setError('No results found.');
        setLoading(false);
        return;
      }
      setProgressMsg('Starting…');
      pollRef.current = setInterval(async () => {
        try {
          const status = await api.rcsbSearchStatus(data.job_id);
          setProgressMsg(status.progress || '');
          if (status.status === 'done') {
            stopPolling();
            setLoading(false);
            setProgressMsg('');
            setResults(status.results || []);
            if (!status.count) setError('No results found.');
          } else if (status.status === 'error') {
            stopPolling();
            setLoading(false);
            setProgressMsg('');
            setError(status.error || 'Search failed.');
          }
        } catch {
          // keep polling on transient network errors
        }
      }, 2000);
    } catch (err) {
      setError(err.response?.data?.detail || 'Search failed.');
      setLoading(false);
    }
  };

  const handleExport = async () => {
    setExporting(true);
    const params = { report_type: reportType };
    if (startDate) params.start_date = startDate;
    if (endDate) params.end_date = endDate;
    if (searchText.trim()) params.search_text = searchText.trim();
    if (pdbCodes.trim()) {
      params.pdb_codes = pdbCodes.trim().split(/[\s,]+/).filter(Boolean);
    }

    try {
      const response = await api.rcsbExport(params);
      const blob = new Blob([response.data], { type: response.headers['content-type'] });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      const disposition = response.headers['content-disposition'] || '';
      const filenameMatch = disposition.match(/filename="?([^"]+)"?/);
      a.href = url;
      a.download = filenameMatch ? filenameMatch[1] : 'RCSB-report.xlsx';
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      setError(err.response?.data?.detail || 'Export failed.');
    } finally {
      setExporting(false);
    }
  };

  const handleSync = async () => {
    setSyncing(true);
    try {
      await api.rcsbSyncApsDb();
      await loadApsStatus();
    } catch (err) {
      setError(err.response?.data?.detail || 'Sync failed.');
    } finally {
      setSyncing(false);
    }
  };

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true);
    setError('');
    try {
      await api.rcsbUploadApsDb(file);
      await loadApsStatus();
    } catch (err) {
      setError(err.response?.data?.detail || 'Upload failed.');
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  const handleAddEmail = (taskName) => {
    const email = (newEmailInput[taskName] || '').trim();
    if (!email || !email.includes('@')) return;
    setScheduledRecipients(prev => ({
      ...prev,
      [taskName]: [...new Set([...prev[taskName], email])],
    }));
    setNewEmailInput(prev => ({ ...prev, [taskName]: '' }));
  };

  const handleRemoveEmail = (taskName, email) => {
    setScheduledRecipients(prev => ({
      ...prev,
      [taskName]: prev[taskName].filter(e => e !== email),
    }));
  };

  const handleSaveRecipients = async (taskName) => {
    try {
      await api.rcsbSetScheduledRecipients(taskName, scheduledRecipients[taskName]);
      setSaveStatus(prev => ({ ...prev, [taskName]: { ok: true, msg: 'Saved' } }));
      setTimeout(() => setSaveStatus(prev => ({ ...prev, [taskName]: null })), 3000);
    } catch (err) {
      setSaveStatus(prev => ({
        ...prev,
        [taskName]: { ok: false, msg: err.response?.data?.detail || 'Save failed' },
      }));
    }
  };

  const handleSort = (col) => {
    if (sortCol === col) {
      setSortAsc(!sortAsc);
    } else {
      setSortCol(col);
      setSortAsc(true);
    }
  };

  const sortedResults = useMemo(() => {
    if (!sortCol) return results;
    return [...results].sort((a, b) => {
      const va = a[sortCol] ?? '';
      const vb = b[sortCol] ?? '';
      if (va < vb) return sortAsc ? -1 : 1;
      if (va > vb) return sortAsc ? 1 : -1;
      return 0;
    });
  }, [results, sortCol, sortAsc]);

  const columns = COLUMNS[reportType] || COLUMNS.generic;

  return (
    <div className="rcsb-container">
      <div className="rcsb-toolbar">
        <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#BBE1FA', textDecoration: 'none', marginRight: '10px' }}>
                    <Home size={20} />
                </Link>
        <h2 style={{ display: 'flex', alignItems: 'center', margin: 0, fontSize: '1.1rem', fontWeight: 600, color: '#fff', marginRight: '20px' }}>
                    <BookOpen size={20} style={{ marginRight: '8px' }} /> RCSB PDB Reports
                </h2>
      </div>

      <form className="rcsb-form" onSubmit={handleSearch}>
        <div className="form-row">
          <label>
            Report Type
            <select value={reportType} onChange={e => setReportType(e.target.value)}>
              {Object.entries(REPORT_LABELS).map(([k, v]) => (
                <option key={k} value={k}>{v}</option>
              ))}
            </select>
          </label>
          <label>
            Start Date
            <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} />
          </label>
          <label>
            End Date
            <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} />
          </label>
        </div>

        {reportType === 'generic' && (
          <div className="form-row">
            <label>
              Text Search
              <input type="text" value={searchText} onChange={e => setSearchText(e.target.value)}
                placeholder="e.g. kinase inhibitor" />
            </label>
            <label>
              PDB Codes
              <input type="text" value={pdbCodes} onChange={e => setPdbCodes(e.target.value)}
                placeholder="e.g. 1abc 2def 3ghi" />
            </label>
          </div>
        )}

        <div className="form-actions">
          <button type="submit" disabled={loading} className="btn-primary">
            {loading ? <><RefreshCw size={16} className="spin" /> Searching...</> : <><Search size={16} /> Search</>}
          </button>
          {results.length > 0 && (
            <button type="button" onClick={handleExport} disabled={exporting} className="btn-secondary">
              {exporting ? <><RefreshCw size={16} className="spin" /> Exporting...</> : <><Download size={16} /> Export to Excel</>}
            </button>
          )}
        </div>
      </form>

      {reportType === 'aps_pub' && (
        <div className="aps-db-bar">
          <Database size={16} />
          {apsStatus ? (
            <span>
              APS DB: {apsStatus.record_count.toLocaleString()} records
              {apsStatus.last_sync && <> &middot; Last synced: {new Date(apsStatus.last_sync).toLocaleDateString()}</>}
              {apsStatus.status === 'failed' && <span className="sync-error"> &middot; Last sync failed</span>}
            </span>
          ) : (
            <span>APS DB: not synced yet</span>
          )}
          <button onClick={handleSync} disabled={syncing || uploading} className="btn-small">
            {syncing ? <><RefreshCw size={14} className="spin" /> Syncing...</> : 'Sync Now'}
          </button>
          <button onClick={() => fileInputRef.current?.click()} disabled={syncing || uploading} className="btn-small">
            {uploading ? <><RefreshCw size={14} className="spin" /> Uploading...</> : <><Upload size={14} /> Upload CSV</>}
          </button>
          <input type="file" ref={fileInputRef} onChange={handleUpload} accept=".csv" style={{ display: 'none' }} />
        </div>
      )}

      <div className="scheduled-emails-section">
        <h4 className="scheduled-emails-title">Scheduled Emails</h4>
        {[
          { key: 'gmca_weekly',     label: 'GMCA Weekly (Wed 8AM)' },
          { key: 'aps_pub_monthly', label: 'APS Pub Monthly (1st 8AM)' },
        ].map(({ key, label }) => (
          <div key={key} className="scheduled-task-row">
            <span className="task-label">{label}</span>
            <div className="email-chips">
              {scheduledRecipients[key].map(email => (
                <span key={email} className="email-chip">
                  {email}
                  <button className="chip-remove" onClick={() => handleRemoveEmail(key, email)} aria-label={`Remove ${email}`}>×</button>
                </span>
              ))}
              <input
                type="text"
                className="email-chip-input"
                placeholder="add email…"
                value={newEmailInput[key]}
                onChange={e => setNewEmailInput(prev => ({ ...prev, [key]: e.target.value }))}
                onKeyDown={e => e.key === 'Enter' && handleAddEmail(key)}
              />
              <button className="btn-small" onClick={() => handleAddEmail(key)} aria-label="Add email">+</button>
            </div>
            <button className="btn-small" onClick={() => handleSaveRecipients(key)}>Save</button>
            {saveStatus[key] && (
              <span className={saveStatus[key].ok ? 'save-ok' : 'save-err'}>{saveStatus[key].msg}</span>
            )}
          </div>
        ))}
      </div>

      {loading && progressMsg && (
        <div className="rcsb-progress">
          <RefreshCw size={14} className="spin" style={{ marginRight: '6px' }} />
          {progressMsg}
        </div>
      )}

      {error && <div className="rcsb-error">{error}</div>}

      {results.length > 0 && (
        <div className="rcsb-results">
          <div className="results-info">{results.length} results</div>
          <div className="table-wrapper">
            <table className="rcsb-table">
              <thead>
                <tr>
                  {columns.map(col => (
                    <th key={col} onClick={() => handleSort(col)} className={sortCol === col ? 'sorted' : ''}>
                      {col}{sortCol === col ? (sortAsc ? ' ▲' : ' ▼') : ''}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedResults.map((row, i) => (
                  <tr key={i}>
                    {columns.map(col => (
                      <CellValue key={col} col={col} value={row[col]} />
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

export default RCSBApp;
