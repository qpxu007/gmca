import React, { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { ClipboardList, ExternalLink, FileText, Globe, HardDrive, Home, Package, Plus, Save, Trash2, Upload, Users, X } from 'lucide-react';
import { api } from './api';
import './ExperimentApp.css';

const ExperimentApp = () => {
    const [esafGroups, setEsafGroups] = useState([]);
    const [selectedEsaf, setSelectedEsaf] = useState('');
    const [form, setForm] = useState(null);
    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [staffList, setStaffList] = useState([]);
    const [spreadsheets, setSpreadsheets] = useState([]);
    const [formsList, setFormsList] = useState(null); // null = not loaded, [] = loaded empty
    const isAdmin = localStorage.getItem('is_admin') === 'true';

    // Local editable fields
    const [instructions, setInstructions] = useState('');
    const [contactPhone, setContactPhone] = useState('');
    const [hasHardDrive, setHasHardDrive] = useState(false);
    const [spreadsheetId, setSpreadsheetId] = useState('');

    // Add-row states
    const [newIP, setNewIP] = useState('');
    const [newIPLabel, setNewIPLabel] = useState('');
    const [newTrackingCarrier, setNewTrackingCarrier] = useState('fedex_overnight');
    const [newTrackingNumber, setNewTrackingNumber] = useState('');
    const [newTrackingDirection, setNewTrackingDirection] = useState('inbound');
    const [newHostId, setNewHostId] = useState('');
    const [showCreateDialog, setShowCreateDialog] = useState(false);
    const [manualEsafId, setManualEsafId] = useState('');

    // Load ESAF groups + staff + spreadsheets on mount
    useEffect(() => {
        loadInitialData();
    }, []);

    const loadInitialData = async () => {
        // Load each independently so one failure doesn't block the others
        let groups = [];
        try {
            const res = await api.experimentEsafGroups();
            groups = res.groups || [];
        } catch (e) { console.error('Failed to load ESAF groups', e); }

        // Also load existing forms so manually-created entries appear in the dropdown
        try {
            const res = await api.experimentList();
            const forms = res.forms || [];
            const groupIds = new Set(groups.map(g => g.esaf_id));
            for (const f of forms) {
                if (!groupIds.has(f.esaf_id)) {
                    groups.push({
                        esaf_id: f.esaf_id,
                        beamline: f.beamline,
                        pi_name: f.pi_name,
                    });
                }
            }
        } catch (e) { console.error('Failed to load existing forms', e); }

        setEsafGroups(groups);

        try {
            const staff = await api.experimentStaffList();
            setStaffList(staff.staff || []);
        } catch (e) { console.error('Failed to load staff list', e); }

        try {
            const sheets = await api.listSpreadsheets();
            setSpreadsheets(Array.isArray(sheets) ? sheets : []);
        } catch (e) { console.error('Failed to load spreadsheets', e); }
    };

    // Load form when ESAF selected
    const loadForm = useCallback(async (esafId) => {
        if (!esafId) { setForm(null); return; }
        setLoading(true);
        try {
            const data = await api.experimentGet(esafId);
            setForm(data);
            setInstructions(data.instructions || '');
            setContactPhone(data.contact_phone || '');
            setHasHardDrive(data.has_hard_drive || false);
            setSpreadsheetId(data.spreadsheet_id || '');
        } catch (e) {
            if (e.response?.status === 404) {
                setForm(null); // Not created yet
            } else {
                console.error('Failed to load form', e);
            }
        } finally {
            setLoading(false);
        }
    }, []);

    // Staff view: load all forms
    const loadAllForms = useCallback(async () => {
        try {
            const data = await api.experimentList();
            setFormsList(data.forms || []);
        } catch (e) {
            console.error('Failed to load forms list', e);
            setFormsList([]); // Show empty state instead of infinite "Loading..."
        }
    }, []);

    useEffect(() => {
        if (isAdmin && !selectedEsaf) {
            loadAllForms();
        }
    }, [isAdmin, selectedEsaf, loadAllForms]);

    const handleEsafChange = (e) => {
        const esafId = e.target.value;
        setSelectedEsaf(esafId);
        setFormsList(null);
        if (esafId) loadForm(esafId);
        else setForm(null);
    };

    const handleCreate = async (esafId) => {
        const id = esafId || selectedEsaf;
        if (!id) return;
        const group = esafGroups.find(g => g.esaf_id === id);
        try {
            await api.experimentCreate({
                esaf_id: id,
                beamline: group?.beamline || null,
                pi_name: group?.pi_name || null,
                experiment_dates: group?.start_date && group?.end_date
                    ? `${group.start_date} to ${group.end_date}` : null,
            });
            // Add to dropdown if not already there
            if (!esafGroups.find(g => g.esaf_id === id)) {
                setEsafGroups(prev => [...prev, { esaf_id: id, beamline: group?.beamline || null, pi_name: group?.pi_name || null }]);
            }
            setSelectedEsaf(id);
            setFormsList(null);
            await loadForm(id);
        } catch (e) {
            if (e.response?.status === 409) {
                if (!esafGroups.find(g => g.esaf_id === id)) {
                    setEsafGroups(prev => [...prev, { esaf_id: id, beamline: null, pi_name: null }]);
                }
                setSelectedEsaf(id);
                setFormsList(null);
                await loadForm(id);
            } else {
                alert('Failed to create form: ' + (e.response?.data?.detail || e.message));
            }
        }
    };

    const handleManualCreate = async () => {
        const id = manualEsafId.trim();
        if (!id) return;
        setShowCreateDialog(false);
        setManualEsafId('');
        await handleCreate(id);
    };

    const handleDelete = async () => {
        if (!form) return;
        if (!window.confirm(`Delete experiment form for ESAF ${form.esaf_id}? This will remove all files, IPs, tracking, and host assignments.`)) return;
        try {
            await api.experimentDelete(form.esaf_id);
            setEsafGroups(prev => prev.filter(g => g.esaf_id !== form.esaf_id));
            setSelectedEsaf('');
            setForm(null);
            if (isAdmin) loadAllForms();
        } catch (e) {
            alert('Delete failed: ' + (e.response?.data?.detail || e.message));
        }
    };

    const handleSave = async () => {
        if (!form) return;
        setSaving(true);
        try {
            await api.experimentUpdate(form.esaf_id, {
                instructions,
                contact_phone: contactPhone || null,
                has_hard_drive: hasHardDrive,
                spreadsheet_id: spreadsheetId ? parseInt(spreadsheetId) : null,
            });
            await loadForm(form.esaf_id);
        } catch (e) {
            alert('Save failed: ' + (e.response?.data?.detail || e.message));
        } finally {
            setSaving(false);
        }
    };

    // --- Auto-detect IP on form load ---
    useEffect(() => {
        if (form && form.ips !== undefined) {
            autoDetectIP();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [form?.esaf_id]);

    const autoDetectIP = async () => {
        try {
            const data = await api.experimentMyIP();
            if (data.ip) setNewIP(data.ip);
        } catch { /* ignore */ }
    };

    // --- File upload ---
    const handleFileUpload = async () => {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.pdb,.cif,.fasta,.fa,.seq,.csv,.xls,.xlsx,.txt,.pdf';
        input.onchange = async (e) => {
            const file = e.target.files[0];
            if (!file) return;
            if (file.size > 5 * 1024 * 1024) {
                alert('File is too large. Maximum size is 5 MB.');
                return;
            }
            try {
                await api.experimentUploadFile(form.esaf_id, file);
                await loadForm(form.esaf_id);
            } catch (err) {
                alert('Upload failed: ' + (err.response?.data?.detail || err.message));
            }
        };
        input.click();
    };

    const handleDeleteFile = async (fileId) => {
        if (!window.confirm('Remove this file?')) return;
        try {
            await api.experimentDeleteFile(form.esaf_id, fileId);
            await loadForm(form.esaf_id);
        } catch (e) {
            alert('Delete failed: ' + (e.response?.data?.detail || e.message));
        }
    };

    const handleDownloadFile = async (fileId, filename) => {
        try {
            const response = await api.experimentDownloadFile(form.esaf_id, fileId);
            const url = window.URL.createObjectURL(new Blob([response.data]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', filename);
            document.body.appendChild(link);
            link.click();
            link.remove();
            window.URL.revokeObjectURL(url);
        } catch {
            alert('Download failed');
        }
    };

    // --- IP ---
    const handleAddIP = async () => {
        if (!newIP.trim()) return;
        try {
            await api.experimentAddIP(form.esaf_id, { ip_address: newIP.trim(), label: newIPLabel.trim() || null });
            setNewIP('');
            setNewIPLabel('');
            await loadForm(form.esaf_id);
        } catch (e) {
            alert('Failed to add IP: ' + (e.response?.data?.detail || e.message));
        }
    };

    const handleDeleteIP = async (ipId) => {
        if (!window.confirm('Remove this IP address?')) return;
        try {
            await api.experimentDeleteIP(form.esaf_id, ipId);
            await loadForm(form.esaf_id);
        } catch {
            alert('Failed to remove IP');
        }
    };

    // --- Tracking ---
    const handleAddTracking = async () => {
        if (!newTrackingNumber.trim()) return;
        try {
            await api.experimentAddTracking(form.esaf_id, {
                carrier: newTrackingCarrier,
                tracking_number: newTrackingNumber.trim(),
                direction: newTrackingDirection,
            });
            setNewTrackingNumber('');
            await loadForm(form.esaf_id);
        } catch (e) {
            alert('Failed to add tracking: ' + (e.response?.data?.detail || e.message));
        }
    };

    const handleDeleteTracking = async (trackId) => {
        if (!window.confirm('Remove this tracking entry?')) return;
        try {
            await api.experimentDeleteTracking(form.esaf_id, trackId);
            await loadForm(form.esaf_id);
        } catch {
            alert('Failed to remove tracking');
        }
    };

    // --- Hosts ---
    const handleAddHost = async () => {
        if (!newHostId) return;
        try {
            await api.experimentAddHost(form.esaf_id, { staff_id: parseInt(newHostId) });
            setNewHostId('');
            await loadForm(form.esaf_id);
        } catch (e) {
            alert('Failed to assign host: ' + (e.response?.data?.detail || e.message));
        }
    };

    const handleDeleteHost = async (hostId) => {
        if (!window.confirm('Remove this host assignment?')) return;
        try {
            await api.experimentDeleteHost(form.esaf_id, hostId);
            await loadForm(form.esaf_id);
        } catch {
            alert('Failed to remove host');
        }
    };

    // --- Render: Staff list view ---
    const renderStaffList = () => {
        if (formsList === null) return <div className="experiment-loading">Loading...</div>;
        if (formsList.length === 0) return (
            <div className="experiment-empty">
                <ClipboardList size={40} />
                <p>No experiment forms submitted yet.</p>
                <p style={{ fontSize: '0.85rem', color: '#888' }}>
                    Users can submit forms from this page, or click <strong>+ New</strong> above to create one.
                </p>
            </div>
        );
        return (
            <table className="experiment-table">
                <thead>
                    <tr>
                        <th>ESAF</th>
                        <th>Beamline</th>
                        <th>PI</th>
                        <th>Dates</th>
                        <th>Files</th>
                        <th>IPs</th>
                        <th>Hard Drive</th>
                        <th>Tracking</th>
                        <th>Host(s)</th>
                    </tr>
                </thead>
                <tbody>
                    {formsList.map(f => (
                        <tr key={f.esaf_id} onClick={() => { setSelectedEsaf(f.esaf_id); setFormsList(null); loadForm(f.esaf_id); }}>
                            <td>{f.esaf_id}</td>
                            <td>{f.beamline || '-'}</td>
                            <td>{f.pi_name || '-'}</td>
                            <td>{f.experiment_dates || '-'}</td>
                            <td>{f.files?.length || 0}</td>
                            <td>{f.ips?.length || 0}</td>
                            <td>{f.has_hard_drive ? <span className="badge badge-yes">Yes</span> : '-'}</td>
                            <td>{f.tracking?.length ? <span className="badge badge-tracking">{f.tracking.length}</span> : '-'}</td>
                            <td>{f.hosts?.map(h => h.full_name || h.username).join(', ') || '-'}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        );
    };

    // --- Render: Form detail ---
    const renderForm = () => {
        if (loading) return <div className="experiment-loading">Loading...</div>;

        if (!form) {
            return (
                <div className="experiment-empty">
                    <ClipboardList size={40} />
                    <p>No form exists for ESAF {selectedEsaf} yet.</p>
                    <button className="add-btn" onClick={() => handleCreate()} style={{ marginTop: 12 }}>
                        <Plus size={16} /> Create Experiment Form
                    </button>
                </div>
            );
        }

        const assignedStaffIds = new Set(form.hosts?.map(h => h.staff_id) || []);

        return (
            <>
                {/* ESAF Info */}
                <div className="experiment-section">
                    <h3><ClipboardList size={16} /> ESAF Info</h3>
                    <div className="esaf-info-grid">
                        <div className="esaf-info-item">
                            <span className="label">ESAF</span>
                            <span className="value">{form.esaf_id}</span>
                        </div>
                        <div className="esaf-info-item">
                            <span className="label">Beamline</span>
                            <span className="value">{form.beamline || 'N/A'}</span>
                        </div>
                        <div className="esaf-info-item">
                            <span className="label">PI</span>
                            <span className="value">{form.pi_name || 'N/A'}</span>
                        </div>
                        <div className="esaf-info-item">
                            <span className="label">Dates</span>
                            <span className="value">{form.experiment_dates || 'N/A'}</span>
                        </div>
                        <div className="esaf-info-item">
                            <span className="label">Created by</span>
                            <span className="value">{form.created_by}</span>
                        </div>
                    </div>
                    {isAdmin && (
                        <button
                            className="add-btn"
                            style={{ marginTop: 8 }}
                            onClick={() => {
                                const params = new URLSearchParams();
                                if (form.esaf_id) params.set('submitted[esaf_id]', form.esaf_id);
                                if (form.beamline) {
                                    // Normalize beamline to APS format: "23-ID-B", "23-ID-D"
                                    const bl = form.beamline.trim().toUpperCase();
                                    const blMap = {
                                        '23IDB': '23-ID-B', '23IDD': '23-ID-D',
                                        '23-ID-B': '23-ID-B', '23-ID-D': '23-ID-D',
                                        'BL2': '23-ID-B', 'BL1': '23-ID-D',
                                    };
                                    const apsBeamline = blMap[bl] || form.beamline;
                                    params.set('submitted[beamline]', apsBeamline);
                                }
                                if (form.pi_name) params.set('submitted[person_submitting]', form.pi_name);
                                if (form.experiment_dates) {
                                    const parts = form.experiment_dates.split(/\s+to\s+/i);
                                    if (parts[0]) params.set('submitted[start_date]', parts[0].trim());
                                    if (parts[1]) params.set('submitted[end_date]', parts[1].trim());
                                }
                                window.open(`https://www.aps.anl.gov/form/esaf?${params.toString()}`, '_blank');
                            }}
                        >
                            <ExternalLink size={14} /> Floor Coordinator Request
                        </button>
                    )}
                </div>

                {/* Local Host(s) */}
                <div className="experiment-section">
                    <h3><Users size={16} /> Local Host(s)</h3>
                    <ul className="experiment-list">
                        {(form.hosts || []).map(h => (
                            <li key={h.id} className="experiment-list-item">
                                <Users size={14} className="item-icon" />
                                <div className="item-main">
                                    <div>{h.full_name || h.username}</div>
                                    <div className="item-meta">{h.email || 'No email'} &middot; assigned by {h.assigned_by}</div>
                                </div>
                                <button className="remove-btn" onClick={() => handleDeleteHost(h.id)}><Trash2 size={14} /></button>
                            </li>
                        ))}
                    </ul>
                    <div className="add-row">
                        <select value={newHostId} onChange={e => setNewHostId(e.target.value)}>
                            <option value="">Select staff...</option>
                            {staffList.filter(s => !assignedStaffIds.has(s.id)).map(s => (
                                <option key={s.id} value={s.id}>{s.full_name} ({s.username})</option>
                            ))}
                        </select>
                        <button className="add-btn" onClick={handleAddHost} disabled={!newHostId}>
                            <Plus size={14} /> Assign
                        </button>
                    </div>
                </div>

                {/* Spreadsheet & Files */}
                <div className="experiment-section">
                    <h3><FileText size={16} /> Spreadsheet & Files</h3>
                    <div style={{ marginBottom: 12 }}>
                        <label style={{ fontSize: '0.8rem', color: '#888' }}>Linked Spreadsheet</label>
                        <select
                            className="spreadsheet-select"
                            value={spreadsheetId}
                            onChange={e => setSpreadsheetId(e.target.value)}
                        >
                            <option value="">None</option>
                            {spreadsheets.map(s => (
                                <option key={s.id} value={s.id}>{s.name} ({s.esaf_id || 'no ESAF'})</option>
                            ))}
                        </select>
                    </div>
                    <div style={{ marginBottom: 8, fontSize: '0.8rem', color: '#888' }}>Additional Files</div>
                    <ul className="experiment-list">
                        {(form.files || []).map(f => (
                            <li key={f.id} className="experiment-list-item">
                                <FileText size={14} className="item-icon" />
                                <div className="item-main">
                                    <a href="#" onClick={e => { e.preventDefault(); handleDownloadFile(f.id, f.filename); }} className="item-link">
                                        {f.filename}
                                    </a>
                                    <div className="item-meta">{f.file_type} &middot; {f.uploaded_by} &middot; {f.uploaded_at?.split('T')[0]}</div>
                                </div>
                                <button className="remove-btn" onClick={() => handleDeleteFile(f.id)}><Trash2 size={14} /></button>
                            </li>
                        ))}
                    </ul>
                    <button className="upload-btn" onClick={handleFileUpload} style={{ marginTop: 8 }}>
                        <Upload size={14} /> Upload File
                    </button>
                </div>

                {/* IP Addresses */}
                <div className="experiment-section">
                    <h3><Globe size={16} /> IP Addresses (for remote access)</h3>
                    <ul className="experiment-list">
                        {(form.ips || []).map(ip => (
                            <li key={ip.id} className="experiment-list-item">
                                <Globe size={14} className="item-icon" />
                                <div className="item-main">
                                    <div>{ip.ip_address}</div>
                                    <div className="item-meta">{ip.label || 'no label'} &middot; {ip.added_by}</div>
                                </div>
                                <button className="remove-btn" onClick={() => handleDeleteIP(ip.id)}><Trash2 size={14} /></button>
                            </li>
                        ))}
                    </ul>
                    <div className="add-row">
                        <input
                            placeholder="IP address"
                            value={newIP}
                            onChange={e => setNewIP(e.target.value)}
                            style={{ width: 160 }}
                        />
                        <input
                            placeholder="Label (optional)"
                            value={newIPLabel}
                            onChange={e => setNewIPLabel(e.target.value)}
                            style={{ width: 120 }}
                        />
                        <button className="add-btn" onClick={handleAddIP} disabled={!newIP.trim()}>
                            <Plus size={14} /> Add IP
                        </button>
                    </div>
                </div>

                {/* Contact Phone */}
                <div className="experiment-section">
                    <h3>Contact Phone</h3>
                    <input
                        type="tel"
                        className="experiment-input"
                        value={contactPhone}
                        onChange={e => setContactPhone(e.target.value)}
                        placeholder="Cell phone number for staff to reach you"
                        style={{ width: '100%', maxWidth: 300 }}
                    />
                </div>

                {/* Instructions */}
                <div className="experiment-section">
                    <h3><ClipboardList size={16} /> Experiment Instructions</h3>
                    <textarea
                        className="instructions-textarea"
                        value={instructions}
                        onChange={e => setInstructions(e.target.value)}
                        placeholder="Puck loading order, special handling instructions, notes for beamline staff..."
                    />
                </div>

                {/* Shipping */}
                <div className="experiment-section">
                    <h3><Package size={16} /> Shipping</h3>
                    <label className="experiment-checkbox">
                        <input
                            type="checkbox"
                            checked={hasHardDrive}
                            onChange={e => setHasHardDrive(e.target.checked)}
                        />
                        <HardDrive size={16} />
                        Hard drive included (staff will copy data before return)
                    </label>

                    <div style={{ marginTop: 16, marginBottom: 8, fontSize: '0.8rem', color: '#888' }}>Tracking Numbers</div>
                    <ul className="experiment-list">
                        {(form.tracking || []).map(t => (
                            <li key={t.id} className="experiment-list-item">
                                <Package size={14} className="item-icon" />
                                <div className="item-main">
                                    <div>{t.carrier_label}: {t.tracking_number} ({t.direction})</div>
                                    <div className="item-meta">{t.added_by}</div>
                                </div>
                                <a href={t.tracking_url} target="_blank" rel="noopener noreferrer" className="item-link">
                                    Track <ExternalLink size={12} />
                                </a>
                                <button className="remove-btn" onClick={() => handleDeleteTracking(t.id)}><Trash2 size={14} /></button>
                            </li>
                        ))}
                    </ul>
                    <div className="add-row">
                        <select value={newTrackingCarrier} onChange={e => setNewTrackingCarrier(e.target.value)}>
                            <option value="fedex_overnight">FedEx Overnight</option>
                            <option value="fedex_2day">FedEx 2-Day</option>
                            <option value="ups">UPS</option>
                        </select>
                        <input
                            placeholder="Tracking number"
                            value={newTrackingNumber}
                            onChange={e => setNewTrackingNumber(e.target.value)}
                            style={{ width: 180 }}
                        />
                        <select value={newTrackingDirection} onChange={e => setNewTrackingDirection(e.target.value)}>
                            <option value="inbound">Inbound</option>
                            <option value="outbound">Outbound</option>
                        </select>
                        <button className="add-btn" onClick={handleAddTracking} disabled={!newTrackingNumber.trim()}>
                            <Plus size={14} /> Add
                        </button>
                    </div>
                </div>

                {/* Save */}
                <div className="save-bar">
                    <button className="remove-btn" onClick={handleDelete} style={{ padding: '10px 16px', fontSize: '0.9rem' }}>
                        <Trash2 size={16} style={{ marginRight: 6, verticalAlign: 'middle' }} />
                        Delete Form
                    </button>
                    <button className="save-btn" onClick={handleSave} disabled={saving}>
                        <Save size={16} style={{ marginRight: 6, verticalAlign: 'middle' }} />
                        {saving ? 'Saving...' : 'Save'}
                    </button>
                </div>
            </>
        );
    };

    return (
        <div className="experiment-container">
            <div className="experiment-toolbar">
                <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#BBE1FA', textDecoration: 'none', marginRight: '10px' }}>
                    <Home size={20} />
                </Link>
                <div className="experiment-toolbar-title">
                    <ClipboardList size={20} />
                    Experiment Preparation
                </div>
            </div>

            <div className="experiment-content">
                {/* ESAF selector */}
                <div className="experiment-selector">
                    <select value={selectedEsaf} onChange={handleEsafChange}>
                        <option value="">
                            {isAdmin ? '-- All Experiments (Staff View) --' : '-- Select ESAF --'}
                        </option>
                        {esafGroups.map(g => (
                            <option key={g.esaf_id} value={g.esaf_id}>
                                ESAF {g.esaf_id} — {g.beamline || 'N/A'} — {g.pi_name || 'N/A'}
                            </option>
                        ))}
                    </select>
                    <button className="add-btn" onClick={() => setShowCreateDialog(true)} title="Create new experiment form">
                        <Plus size={14} /> New
                    </button>
                </div>

                {/* Manual ESAF create dialog */}
                {showCreateDialog && (
                    <div className="experiment-section" style={{ borderColor: '#3282B8' }}>
                        <h3><Plus size={16} /> Create New Experiment Form</h3>
                        <div className="add-row">
                            <input
                                placeholder="Enter ESAF number (e.g. 12345)"
                                value={manualEsafId}
                                onChange={e => setManualEsafId(e.target.value)}
                                onKeyDown={e => e.key === 'Enter' && handleManualCreate()}
                                style={{ width: 250 }}
                                autoFocus
                            />
                            <button className="add-btn" onClick={handleManualCreate} disabled={!manualEsafId.trim()}>
                                Create
                            </button>
                            <button className="remove-btn" onClick={() => { setShowCreateDialog(false); setManualEsafId(''); }} style={{ fontSize: '0.9rem' }}>
                                Cancel
                            </button>
                        </div>
                    </div>
                )}

                {/* Show staff list or form detail */}
                {!selectedEsaf && isAdmin ? renderStaffList() : null}
                {selectedEsaf ? renderForm() : null}
                {!selectedEsaf && !isAdmin && !showCreateDialog && (
                    <div className="experiment-empty">
                        <ClipboardList size={40} />
                        <p>Select an ESAF or click <strong>+ New</strong> to create an experiment form.</p>
                    </div>
                )}
            </div>
        </div>
    );
};

export default ExperimentApp;
