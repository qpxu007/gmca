import React, { useState, useEffect } from 'react';
import Modal from 'react-modal';

Modal.setAppElement('#root');

const SUPPORTED_PIPELINES = ['xds', 'nxds', 'xia2', 'xia2_ssx', 'autoproc', 'crystfel'];
const MERGE_COMPATIBLE = new Set(['xds', 'xia2', 'xia2_ssx', 'autoproc']);

function mostCommonPipeline(rows) {
    if (!rows || rows.length === 0) return 'xds';
    const counts = {};
    for (const row of rows) {
        const p = (row.pipeline || '').toLowerCase();
        if (SUPPORTED_PIPELINES.includes(p)) {
            counts[p] = (counts[p] || 0) + 1;
        }
    }
    if (Object.keys(counts).length === 0) return 'xds';
    return Object.entries(counts).sort((a, b) => b[1] - a[1])[0][0];
}

const ReprocessModal = ({ isOpen, onClose, selectedRows, onSuccess }) => {
    const [pipeline, setPipeline] = useState('xds');
    const [highres, setHighres] = useState('');
    const [spaceGroup, setSpaceGroup] = useState('');
    const [unitCell, setUnitCell] = useState('');
    const [merge, setMerge] = useState(false);
    const [nproc, setNproc] = useState('');
    const [submitting, setSubmitting] = useState(false);
    const [result, setResult] = useState(null);

    useEffect(() => {
        if (isOpen && selectedRows?.length > 0) {
            setPipeline(mostCommonPipeline(selectedRows));
            setHighres('');
            setSpaceGroup('');
            setUnitCell('');
            setMerge(false);
            setNproc('');
            setResult(null);
        }
    }, [isOpen, selectedRows]);

    const canMerge = selectedRows?.length > 1 && MERGE_COMPATIBLE.has(pipeline);
    const n = selectedRows?.length || 0;

    const handleSubmit = async () => {
        setSubmitting(true);
        setResult(null);
        try {
            const { api } = await import('./api');
            const payload = {
                ids: selectedRows.map(r => r.id),
                pipeline,
                merge: canMerge && merge,
            };
            if (highres) payload.highres = parseFloat(highres);
            if (spaceGroup) payload.space_group = spaceGroup;
            if (unitCell) payload.unit_cell = unitCell;
            if (nproc) payload.nproc = parseInt(nproc, 10);

            const data = await api.reprocessDatasets(payload);
            setResult(data);
            if (data.submitted > 0 && onSuccess) {
                onSuccess(data);
            }
        } catch (e) {
            setResult({ submitted: 0, errors: [e.response?.data?.detail || e.message || 'Unknown error'] });
        } finally {
            setSubmitting(false);
        }
    };

    const inputStyle = {
        width: '100%',
        padding: '6px 8px',
        border: '1px solid #ccc',
        borderRadius: '4px',
        fontSize: '0.9rem',
        boxSizing: 'border-box',
    };

    const labelStyle = {
        display: 'block',
        marginBottom: '4px',
        fontWeight: '500',
        fontSize: '0.85rem',
        color: '#444',
    };

    const rowStyle = { marginBottom: '14px' };

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Reprocess Datasets"
            style={{
                content: {
                    top: '50%',
                    left: '50%',
                    right: 'auto',
                    bottom: 'auto',
                    transform: 'translate(-50%, -50%)',
                    width: '420px',
                    padding: '20px',
                    borderRadius: '8px',
                },
                overlay: { backgroundColor: 'rgba(0,0,0,0.5)', zIndex: 1000 },
            }}
        >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
                <h3 style={{ margin: 0 }}>Reprocess {n} Dataset{n !== 1 ? 's' : ''}</h3>
                <button onClick={onClose} style={{ background: 'none', border: 'none', fontSize: '1.2rem', cursor: 'pointer' }}>✕</button>
            </div>

            <div style={rowStyle}>
                <label style={labelStyle}>Pipeline</label>
                <select value={pipeline} onChange={e => setPipeline(e.target.value)} style={inputStyle}>
                    {SUPPORTED_PIPELINES.map(p => (
                        <option key={p} value={p}>{p}</option>
                    ))}
                </select>
            </div>

            <div style={rowStyle}>
                <label style={labelStyle}>Resolution cutoff (Å)</label>
                <input
                    type="number"
                    step="0.05"
                    min="0.5"
                    max="10"
                    value={highres}
                    onChange={e => setHighres(e.target.value)}
                    placeholder="keep original"
                    style={inputStyle}
                />
            </div>

            <div style={rowStyle}>
                <label style={labelStyle}>Space group</label>
                <input
                    type="text"
                    value={spaceGroup}
                    onChange={e => setSpaceGroup(e.target.value)}
                    placeholder="keep original (e.g. P 21 21 21)"
                    style={inputStyle}
                />
            </div>

            <div style={rowStyle}>
                <label style={labelStyle}>Unit cell</label>
                <input
                    type="text"
                    value={unitCell}
                    onChange={e => setUnitCell(e.target.value)}
                    placeholder="keep original (e.g. 78 78 39 90 90 90)"
                    style={inputStyle}
                />
            </div>

            <div style={rowStyle}>
                <label style={labelStyle}>CPU cores</label>
                <input
                    type="number"
                    min="1"
                    max="128"
                    value={nproc}
                    onChange={e => setNproc(e.target.value)}
                    placeholder="default"
                    style={inputStyle}
                />
            </div>

            {n > 1 && (
                <div style={{ ...rowStyle, display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <input
                        type="checkbox"
                        id="merge-check"
                        checked={merge}
                        onChange={e => setMerge(e.target.checked)}
                        disabled={!canMerge}
                    />
                    <label htmlFor="merge-check" style={{ fontSize: '0.9rem', color: canMerge ? '#333' : '#999' }}>
                        Merge all datasets into one job
                        {!canMerge && <span style={{ fontSize: '0.8rem' }}> (not supported for {pipeline})</span>}
                    </label>
                </div>
            )}

            {result && (
                <div style={{
                    padding: '10px',
                    borderRadius: '4px',
                    marginBottom: '14px',
                    backgroundColor: result.submitted > 0 ? '#d4edda' : '#f8d7da',
                    color: result.submitted > 0 ? '#155724' : '#721c24',
                    fontSize: '0.85rem',
                }}>
                    {result.submitted > 0 && <div>✓ {result.submitted} job{result.submitted !== 1 ? 's' : ''} submitted successfully.</div>}
                    {result.errors?.map((e, i) => <div key={i}>✗ {e}</div>)}
                </div>
            )}

            <div style={{ display: 'flex', gap: '10px', justifyContent: 'flex-end' }}>
                <button
                    onClick={onClose}
                    style={{ padding: '8px 16px', border: '1px solid #ccc', borderRadius: '4px', background: '#fff', cursor: 'pointer' }}
                >
                    {result ? 'Close' : 'Cancel'}
                </button>
                {!result && (
                    <button
                        onClick={handleSubmit}
                        disabled={submitting}
                        style={{
                            padding: '8px 16px',
                            border: 'none',
                            borderRadius: '4px',
                            background: submitting ? '#aaa' : '#2980b9',
                            color: '#fff',
                            cursor: submitting ? 'not-allowed' : 'pointer',
                            fontWeight: '500',
                        }}
                    >
                        {submitting ? 'Submitting...' : `Reprocess ${n} Dataset${n !== 1 ? 's' : ''}`}
                    </button>
                )}
            </div>
        </Modal>
    );
};

export default ReprocessModal;
