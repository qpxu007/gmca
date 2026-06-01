
import React, { useState, useEffect } from 'react';
import Modal from 'react-modal';
import { api } from './api';

Modal.setAppElement('#root');

const CHAIN_IDS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

function parseFasta(text) {
    const lines = text.trim().split('\n');
    const records = [];
    let header = null, seq = '';

    for (const line of lines) {
        if (line.startsWith('>')) {
            if (seq) records.push({ header, seq });
            header = line.slice(1).trim();
            seq = '';
        } else {
            seq += line.trim();
        }
    }
    if (seq) records.push({ header, seq });

    // If no FASTA headers, treat whole text as single plain sequence
    if (records.length === 0 && text.trim()) {
        records.push({ header: null, seq: text.replace(/\s/g, '') });
    }

    return records.map((r, i) => ({
        id: CHAIN_IDS[i] || String(i + 1),
        header: r.header,
        sequence: r.seq,
        type: 'protein',
    }));
}

export default function PredictionForm({ isOpen, onClose, esafId, onSubmitted }) {
    const [programs, setPrograms] = useState([]);
    const [program, setProgram] = useState('alphafold3');
    const [jobName, setJobName] = useState('');
    const [rawSequence, setRawSequence] = useState('');
    const [chains, setChains] = useState([]);
    const [seeds, setSeeds] = useState('42');
    const [submitting, setSubmitting] = useState(false);
    const [expSequences, setExpSequences] = useState([]);

    useEffect(() => {
        if (isOpen) {
            api.listPrograms().then(setPrograms).catch(() => {});
            if (esafId) {
                api.experimentSequences(esafId).then(setExpSequences).catch(() => setExpSequences([]));
            }
        }
    }, [isOpen, esafId]);

    useEffect(() => {
        setChains(parseFasta(rawSequence));
    }, [rawSequence]);

    const setChainType = (idx, type) => {
        setChains(prev => prev.map((c, i) => i === idx ? { ...c, type } : c));
    };

    const handleSubmit = async () => {
        if (!jobName.trim()) return alert('Job name is required');
        if (chains.length === 0) return alert('At least one sequence is required');

        for (const c of chains) {
            if (c.sequence.length < 10)
                return alert(`Chain ${c.id}: sequence must be at least 10 residues`);
        }

        const seedList = seeds.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n));
        if (seedList.length === 0) return alert('At least one seed is required');

        setSubmitting(true);
        try {
            const result = await api.submitPrediction(esafId, {
                job_name: jobName.trim(),
                program,
                sequences: chains.map(c => ({ type: c.type, id: c.id, sequence: c.sequence })),
                seeds: seedList,
            });
            if (result.status === 'failed') {
                alert('Job submission failed: ' + (result.error_message || 'Unknown error'));
            } else {
                onSubmitted(result);
                onClose();
            }
        } catch (err) {
            alert('Submission failed: ' + (err.response?.data?.detail || err.message));
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Predict Structure"
            className="custom-small-modal"
            overlayClassName="custom-overlay"
            style={{ content: { width: '560px', height: 'auto', maxHeight: '85vh' } }}
        >
            <h2 className="modal-header">Predict Structure</h2>

            <div style={{ marginBottom: '12px' }}>
                <label style={{ display: 'block', fontSize: '13px', fontWeight: 500, marginBottom: '4px' }}>Program</label>
                <select
                    value={program}
                    onChange={e => setProgram(e.target.value)}
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
                >
                    {programs.map(p => (
                        <option key={p.id} value={p.id}>{p.name}</option>
                    ))}
                </select>
            </div>

            <div style={{ marginBottom: '12px' }}>
                <label style={{ display: 'block', fontSize: '13px', fontWeight: 500, marginBottom: '4px' }}>Job Name</label>
                <input
                    type="text"
                    value={jobName}
                    onChange={e => setJobName(e.target.value)}
                    placeholder="e.g. mykinase"
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc', boxSizing: 'border-box' }}
                />
            </div>

            <div style={{ marginBottom: '12px' }}>
                <label style={{ display: 'block', fontSize: '13px', fontWeight: 500, marginBottom: '4px' }}>
                    Sequences — FASTA format (multiple chains supported)
                </label>
                {expSequences.length > 0 && (
                    <select
                        onChange={e => {
                            const sel = expSequences.find(s => String(s.id) === e.target.value);
                            if (sel) setRawSequence(prev => prev ? prev + '\n' + sel.content : sel.content);
                        }}
                        defaultValue=""
                        style={{ width: '100%', padding: '6px', borderRadius: '4px', border: '1px solid #ccc', marginBottom: '6px', fontSize: '12px' }}
                    >
                        <option value="">Import from experiment files...</option>
                        {expSequences.map(s => (
                            <option key={s.id} value={s.id}>{s.filename}</option>
                        ))}
                    </select>
                )}
                <textarea
                    value={rawSequence}
                    onChange={e => setRawSequence(e.target.value)}
                    placeholder={">chainA\nMVLSPADKTNVKAAWGKVGAHAGEYG...\n>chainB\nATCGATCGATCG..."}
                    rows={8}
                    style={{
                        width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc',
                        fontFamily: 'monospace', fontSize: '12px', resize: 'vertical', boxSizing: 'border-box'
                    }}
                />
            </div>

            {/* Parsed chain preview */}
            {chains.length > 0 && (
                <div style={{ marginBottom: '14px' }}>
                    <label style={{ display: 'block', fontSize: '13px', fontWeight: 500, marginBottom: '6px' }}>
                        Parsed chains ({chains.length})
                    </label>
                    <table style={{ width: '100%', fontSize: '12px', borderCollapse: 'collapse' }}>
                        <thead>
                            <tr style={{ background: '#f4f6f8' }}>
                                <th style={{ padding: '4px 8px', textAlign: 'center', border: '1px solid #ddd' }}>ID</th>
                                <th style={{ padding: '4px 8px', textAlign: 'center', border: '1px solid #ddd' }}>Header</th>
                                <th style={{ padding: '4px 8px', textAlign: 'center', border: '1px solid #ddd' }}>Length</th>
                                <th style={{ padding: '4px 8px', textAlign: 'center', border: '1px solid #ddd' }}>Type</th>
                            </tr>
                        </thead>
                        <tbody>
                            {chains.map((c, i) => (
                                <tr key={i}>
                                    <td style={{ padding: '4px 8px', border: '1px solid #ddd', fontWeight: 600, textAlign: 'center' }}>{c.id}</td>
                                    <td style={{ padding: '4px 8px', border: '1px solid #ddd', color: '#666', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', textAlign: 'center' }}>{c.header || '—'}</td>
                                    <td style={{ padding: '4px 8px', border: '1px solid #ddd', textAlign: 'center' }}>{c.sequence.length} aa</td>
                                    <td style={{ padding: '4px 8px', border: '1px solid #ddd' }}>
                                        <select
                                            value={c.type}
                                            onChange={e => setChainType(i, e.target.value)}
                                            style={{ fontSize: '12px', padding: '2px 4px', border: '1px solid #ccc', borderRadius: '3px' }}
                                        >
                                            <option value="protein">protein</option>
                                            <option value="dna">dna</option>
                                            <option value="rna">rna</option>
                                        </select>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}

            <div style={{ marginBottom: '16px' }}>
                <label style={{ display: 'block', fontSize: '13px', fontWeight: 500, marginBottom: '4px' }}>
                    Random Seeds (comma-separated)
                </label>
                <input
                    type="text"
                    value={seeds}
                    onChange={e => setSeeds(e.target.value)}
                    placeholder="42"
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc', boxSizing: 'border-box' }}
                />
            </div>

            <div className="modal-footer">
                <button onClick={onClose} style={{ backgroundColor: '#e9ecef', color: '#333' }}>Cancel</button>
                <button
                    onClick={handleSubmit}
                    disabled={submitting || chains.length === 0}
                    style={{ backgroundColor: '#3498db', color: 'white' }}
                >
                    {submitting ? 'Submitting...' : `Submit${chains.length > 1 ? ` (${chains.length} chains)` : ''}`}
                </button>
            </div>
        </Modal>
    );
}
