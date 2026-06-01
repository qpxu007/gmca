
import React, { useState, useEffect } from 'react';
import Modal from 'react-modal';
import { api } from './api';

Modal.setAppElement('#root');

export default function SequencePickerModal({ isOpen, onClose, onSelect }) {
    const [esafGroups, setEsafGroups] = useState([]);
    const [selectedEsaf, setSelectedEsaf] = useState('');
    const [files, setFiles] = useState([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        if (isOpen) {
            (async () => {
                let groups = [];
                try {
                    const res = await api.experimentEsafGroups();
                    groups = res.groups || [];
                } catch { /* ignore */ }
                try {
                    const res = await api.experimentList();
                    const forms = res.forms || [];
                    const ids = new Set(groups.map(g => g.esaf_id));
                    for (const f of forms) {
                        if (!ids.has(f.esaf_id)) {
                            groups.push({ esaf_id: f.esaf_id, beamline: f.beamline, pi_name: f.pi_name });
                        }
                    }
                } catch { /* ignore */ }
                setEsafGroups(groups);
            })();
        }
    }, [isOpen]);

    const handleEsafChange = async (e) => {
        const id = e.target.value;
        setSelectedEsaf(id);
        if (!id) { setFiles([]); return; }
        setLoading(true);
        try {
            const data = await api.experimentSequenceFiles(id);
            setFiles(data);
        } catch {
            setFiles([]);
        } finally {
            setLoading(false);
        }
    };

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Select Sequence File"
            className="custom-small-modal puck-assign-modal"
            overlayClassName="custom-overlay"
        >
            <h2 className="modal-header">Select Sequence File</h2>
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
                {!loading && selectedEsaf && files.length === 0 && (
                    <p style={{ color: '#999', textAlign: 'center' }}>No sequence files available</p>
                )}
                {files.map(f => (
                    <div
                        key={f.id}
                        onClick={() => { onSelect(f.file_path); onClose(); }}
                        style={{
                            padding: '8px 12px', borderRadius: '4px', marginBottom: '4px',
                            background: '#f8f9fa', border: '1px solid #eee', cursor: 'pointer',
                            fontSize: '13px',
                        }}
                    >
                        {f.filename}
                        <div style={{ fontSize: '11px', color: '#aaa', marginTop: '2px' }}>{f.file_path}</div>
                    </div>
                ))}
            </div>
            <div className="modal-footer">
                <button onClick={onClose} style={{ backgroundColor: '#e9ecef', color: '#333' }}>Cancel</button>
            </div>
        </Modal>
    );
}
