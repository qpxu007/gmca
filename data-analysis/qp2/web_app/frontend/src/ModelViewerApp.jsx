
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Link } from 'react-router-dom';
import { Box, CheckCircle, Clock, Download, Eye, Home, Loader, Play, Trash2, Upload, XCircle } from 'lucide-react';
import { api } from './api';
import MolstarViewer from './MolstarViewer';
import PredictionForm from './PredictionForm';
import './ModelViewerApp.css';

const STATUS_ICONS = {
    pending: <Clock size={14} color="#f39c12" />,
    running: <Loader size={14} color="#3498db" />,
    completed: <CheckCircle size={14} color="#27ae60" />,
    failed: <XCircle size={14} color="#e74c3c" />,
};

const ModelViewerApp = () => {
    const [esafGroups, setEsafGroups] = useState([]);
    const [selectedEsaf, setSelectedEsaf] = useState('');
    const [models, setModels] = useState([]);
    const [jobs, setJobs] = useState([]);
    const [loading, setLoading] = useState(false);
    const [viewingModel, setViewingModel] = useState(null);
    const [predictOpen, setPredictOpen] = useState(false);
    const [activeTab, setActiveTab] = useState('models'); // 'models' | 'jobs'
    const pollRef = useRef(null);

    const loadEsafGroups = useCallback(async () => {
        let groups = [];
        try {
            const res = await api.experimentEsafGroups();
            groups = res.groups || [];
        } catch (e) {
            console.error('Failed to load ESAF groups', e);
        }
        // Also include manually-created experiment forms (same pattern as ExperimentApp)
        try {
            const res = await api.experimentList();
            const forms = res.forms || [];
            const groupIds = new Set(groups.map(g => g.esaf_id));
            for (const f of forms) {
                if (!groupIds.has(f.esaf_id)) {
                    groups.push({ esaf_id: f.esaf_id, beamline: f.beamline, pi_name: f.pi_name });
                }
            }
        } catch (e) { console.error('Failed to load existing forms', e); }
        setEsafGroups(groups);
    }, []);

    useEffect(() => {
        loadEsafGroups();
        return () => { if (pollRef.current) clearInterval(pollRef.current); };
    }, [loadEsafGroups]);

    const loadModels = useCallback(async (esafId) => {
        if (!esafId) return;
        try {
            const data = await api.listModels(esafId);
            setModels(data);
        } catch (e) {
            console.error('Failed to load models', e);
            setModels([]);
        }
    }, []);

    const loadJobs = useCallback(async function fetchJobs(esafId) {
        if (!esafId) return;
        try {
            const data = await api.listPredictionJobs(esafId);
            setJobs(data);
            // Start polling if any jobs are running
            const hasRunning = data.some(j => j.status === 'running' || j.status === 'pending');
            if (hasRunning && !pollRef.current) {
                pollRef.current = setInterval(() => fetchJobs(esafId), 30000);
            } else if (!hasRunning && pollRef.current) {
                clearInterval(pollRef.current);
                pollRef.current = null;
            }
        } catch (e) {
            console.error('Failed to load jobs', e);
            setJobs([]);
        }
    }, []);

    const loadAll = useCallback(async (esafId) => {
        setLoading(true);
        await Promise.all([loadModels(esafId), loadJobs(esafId)]);
        setLoading(false);
    }, [loadModels, loadJobs]);

    const handleEsafChange = (e) => {
        const id = e.target.value;
        setSelectedEsaf(id);
        setViewingModel(null);
        if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
        if (id) loadAll(id);
        else { setModels([]); setJobs([]); }
    };

    const handleUpload = () => {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.pdb,.cif';
        input.onchange = async (e) => {
            const file = e.target.files[0];
            if (!file) return;
            try {
                await api.uploadModel(selectedEsaf, file);
                loadModels(selectedEsaf);
            } catch (err) {
                alert('Upload failed: ' + (err.response?.data?.detail || err.message));
            }
        };
        input.click();
    };

    const handleDelete = async (modelId) => {
        if (!confirm('Delete this model?')) return;
        try {
            await api.deleteModel(selectedEsaf, modelId);
            if (viewingModel?.id === modelId) setViewingModel(null);
            loadModels(selectedEsaf);
        } catch (err) {
            alert('Delete failed: ' + (err.response?.data?.detail || err.message));
        }
    };

    const handleImport = async (jobId) => {
        try {
            const result = await api.importPredictionModels(selectedEsaf, jobId);
            alert(`Imported ${result.count} model(s)`);
            loadModels(selectedEsaf);
            loadJobs(selectedEsaf);
        } catch (err) {
            alert('Import failed: ' + (err.response?.data?.detail || err.message));
        }
    };

    const handlePredictionSubmitted = () => {
        loadJobs(selectedEsaf);
        setActiveTab('jobs');
    };

    return (
        <div className="model-viewer-container">
            <div className="toolbar" style={{ position: 'relative', zIndex: 10 }}>
                <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#BBE1FA', textDecoration: 'none', marginRight: '10px' }}>
                    <Home size={20} />
                </Link>
                <select value={selectedEsaf} onChange={handleEsafChange} style={{ padding: '6px 10px', borderRadius: '4px', border: '1px solid #ccc', marginRight: '8px' }}>
                    <option value="">Select ESAF...</option>
                    {esafGroups.map(g => (
                        <option key={g.esaf_id} value={g.esaf_id}>
                            ESAF {g.esaf_id} {g.pi_name ? `(${g.pi_name})` : ''}
                        </option>
                    ))}
                </select>
                <button onClick={handleUpload} disabled={!selectedEsaf}>
                    <Upload size={16} style={{ marginRight: '4px', verticalAlign: 'text-bottom' }} />
                    Upload
                </button>
                <button onClick={() => setPredictOpen(true)} disabled={!selectedEsaf}>
                    <Play size={16} style={{ marginRight: '4px', verticalAlign: 'text-bottom' }} />
                    Predict
                </button>
                <span className="filename-label">Structure Models</span>
            </div>

            <div className="model-viewer-layout">
                {/* Left panel: tabs for Models / Jobs */}
                <div className="model-sidebar">
                    {/* Tab bar */}
                    <div style={{ display: 'flex', borderBottom: '1px solid #ddd' }}>
                        <button
                            onClick={() => setActiveTab('models')}
                            style={{
                                flex: 1, padding: '8px', border: 'none', cursor: 'pointer',
                                background: activeTab === 'models' ? '#fff' : '#f0f0f0',
                                borderBottom: activeTab === 'models' ? '2px solid #3282B8' : '2px solid transparent',
                                fontWeight: activeTab === 'models' ? 600 : 400,
                            }}
                        >Models ({models.length})</button>
                        <button
                            onClick={() => setActiveTab('jobs')}
                            style={{
                                flex: 1, padding: '8px', border: 'none', cursor: 'pointer',
                                background: activeTab === 'jobs' ? '#fff' : '#f0f0f0',
                                borderBottom: activeTab === 'jobs' ? '2px solid #3282B8' : '2px solid transparent',
                                fontWeight: activeTab === 'jobs' ? 600 : 400,
                            }}
                        >Jobs ({jobs.length})</button>
                    </div>

                    <div style={{ flex: 1, overflowY: 'auto', padding: '12px' }}>
                        {!selectedEsaf && (
                            <p style={{ color: '#999', textAlign: 'center', marginTop: '2rem' }}>Select an ESAF</p>
                        )}
                        {selectedEsaf && loading && <p style={{ color: '#999', textAlign: 'center' }}>Loading...</p>}

                        {/* Models tab */}
                        {selectedEsaf && !loading && activeTab === 'models' && (
                            <>
                                {models.length === 0 && (
                                    <p style={{ color: '#999', textAlign: 'center', marginTop: '2rem' }}>No models yet</p>
                                )}
                                {models.map(m => (
                                    <div
                                        key={m.id}
                                        style={{
                                            padding: '10px 12px', borderRadius: '6px', marginBottom: '6px', cursor: 'pointer',
                                            background: viewingModel?.id === m.id ? '#e6f7ff' : '#f8f9fa',
                                            border: viewingModel?.id === m.id ? '2px solid #3282B8' : '1px solid #eee',
                                        }}
                                        onClick={() => setViewingModel(m)}
                                    >
                                        <div style={{ fontWeight: 500, fontSize: '14px', marginBottom: '4px' }}>{m.filename}</div>
                                        <div style={{ fontSize: '12px', color: '#888' }}>
                                            {m.source === 'prediction' ? 'Predicted' : 'Uploaded'} by {m.uploaded_by}
                                            {m.created_at && ` on ${new Date(m.created_at).toLocaleDateString()}`}
                                        </div>
                                        <div style={{ marginTop: '6px', display: 'flex', gap: '6px' }}>
                                            <button
                                                onClick={(e) => { e.stopPropagation(); setViewingModel(m); }}
                                                style={{ padding: '3px 8px', fontSize: '11px', border: '1px solid #ccc', borderRadius: '3px', background: 'white', cursor: 'pointer' }}
                                            ><Eye size={12} /> View</button>
                                            <button
                                                onClick={(e) => { e.stopPropagation(); handleDelete(m.id); }}
                                                style={{ padding: '3px 8px', fontSize: '11px', border: '1px solid #e74c3c', borderRadius: '3px', background: 'white', color: '#e74c3c', cursor: 'pointer' }}
                                            ><Trash2 size={12} /> Delete</button>
                                        </div>
                                    </div>
                                ))}
                            </>
                        )}

                        {/* Jobs tab */}
                        {selectedEsaf && !loading && activeTab === 'jobs' && (
                            <>
                                {jobs.length === 0 && (
                                    <p style={{ color: '#999', textAlign: 'center', marginTop: '2rem' }}>No prediction jobs</p>
                                )}
                                {jobs.map(j => (
                                    <div
                                        key={j.id}
                                        style={{
                                            padding: '10px 12px', borderRadius: '6px', marginBottom: '6px',
                                            background: '#f8f9fa', border: '1px solid #eee',
                                        }}
                                    >
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '4px' }}>
                                            {STATUS_ICONS[j.status] || null}
                                            <span style={{ fontWeight: 500, fontSize: '14px' }}>{j.job_name}</span>
                                        </div>
                                        <div style={{ fontSize: '12px', color: '#888' }}>
                                            {j.program} &middot; {j.status} &middot; by {j.submitted_by}
                                            {j.submitted_at && ` on ${new Date(j.submitted_at).toLocaleDateString()}`}
                                        </div>
                                        {j.error_message && j.status === 'failed' && (
                                            <div style={{ fontSize: '11px', color: '#e74c3c', marginTop: '4px', maxHeight: '60px', overflow: 'auto' }}>
                                                {j.error_message.slice(0, 200)}
                                            </div>
                                        )}
                                        {j.status === 'completed' && (
                                            <button
                                                onClick={() => handleImport(j.id)}
                                                style={{ marginTop: '6px', padding: '4px 10px', fontSize: '12px', border: '1px solid #27ae60', borderRadius: '3px', background: 'white', color: '#27ae60', cursor: 'pointer' }}
                                            ><Download size={12} /> Import Models</button>
                                        )}
                                        {j.slurm_job_id && (
                                            <div style={{ fontSize: '11px', color: '#aaa', marginTop: '4px' }}>Slurm: {j.slurm_job_id}</div>
                                        )}
                                    </div>
                                ))}
                            </>
                        )}
                    </div>
                </div>

                {/* Mol* viewer panel */}
                <div className="model-main-panel">
                    {viewingModel ? (
                        <MolstarViewer
                            key={viewingModel.id}
                            modelUrl={api.viewModelUrl(selectedEsaf, viewingModel.id)}
                            fileType={viewingModel.file_type}
                        />
                    ) : (
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#999' }}>
                            Select a model to view
                        </div>
                    )}
                </div>
            </div>

            <PredictionForm
                isOpen={predictOpen}
                onClose={() => setPredictOpen(false)}
                esafId={selectedEsaf}
                onSubmitted={handlePredictionSubmitted}
            />
        </div>
    );
};

export default ModelViewerApp;
