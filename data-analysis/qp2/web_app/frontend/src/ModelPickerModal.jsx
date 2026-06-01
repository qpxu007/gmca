
import React, { useState, useEffect } from 'react';
import Modal from 'react-modal';
import { api } from './api';

Modal.setAppElement('#root');

export default function ModelPickerModal({ isOpen, onClose, onSelect }) {
    const [esafGroups, setEsafGroups] = useState([]);
    const [selectedEsaf, setSelectedEsaf] = useState('');
    const [models, setModels] = useState([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        if (isOpen) {
            (async () => {
                let groups = [];
                try {
                    const res = await api.experimentEsafGroups();
                    groups = res.groups || [];
                } catch (e) { console.error('Failed to load ESAF groups:', e); }
                try {
                    const res = await api.experimentList();
                    const forms = res.forms || [];
                    const ids = new Set(groups.map(g => g.esaf_id));
                    for (const f of forms) {
                        if (!ids.has(f.esaf_id)) {
                            groups.push({ esaf_id: f.esaf_id, beamline: f.beamline, pi_name: f.pi_name });
                        }
                    }
                } catch (e) { console.error('Failed to load experiment list:', e); }
                setEsafGroups(groups);
            })();
        }
    }, [isOpen]);

    const handleEsafChange = async (e) => {
        const id = e.target.value;
        setSelectedEsaf(id);
        if (!id) { setModels([]); return; }
        setLoading(true);
        try {
            const data = await api.listModelsForSpreadsheet(id);
            setModels(data);
        } catch {
            setModels([]);
        } finally {
            setLoading(false);
        }
    };

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Select Model"
            className="custom-small-modal puck-assign-modal"
            overlayClassName="custom-overlay"
        >
            <h2 className="modal-header">Select Model</h2>
            <div style={{ marginBottom: '12px' }}>
                <select
                    value={selectedEsaf}
                    onChange={handleEsafChange}
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
                >
                    <option value="">Select ESAF...</option>
                    {esafGroups.map(g => (
                        <option key={g.esaf_id} value={g.esaf_id}>
                            ESAF {g.esaf_id} {g.pi_name ? `(${g.pi_name})` : ''}
                        </option>
                    ))}
                </select>
            </div>
            <div style={{ maxHeight: '300px', overflowY: 'auto', marginBottom: '12px' }}>
                {loading && <p style={{ color: '#999', textAlign: 'center' }}>Loading...</p>}
                {!loading && selectedEsaf && models.length === 0 && (
                    <p style={{ color: '#999', textAlign: 'center' }}>No models available</p>
                )}
                {models.map(m => (
                    <div
                        key={m.id}
                        onClick={() => { onSelect(m.file_path); onClose(); }}
                        style={{
                            padding: '8px 12px', borderRadius: '4px', marginBottom: '4px',
                            background: '#f8f9fa', border: '1px solid #eee', cursor: 'pointer',
                            fontSize: '13px',
                        }}
                    >
                        {m.filename}
                        <div style={{ fontSize: '11px', color: '#aaa', marginTop: '2px' }}>{m.file_path}</div>
                    </div>
                ))}
            </div>
            <div className="modal-footer">
                <button onClick={onClose} style={{ backgroundColor: '#e9ecef', color: '#333' }}>Cancel</button>
            </div>
        </Modal>
    );
}
