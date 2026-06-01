
import React, { useState } from 'react';
import Modal from 'react-modal';

Modal.setAppElement('#root');

export default function PuckAssignmentModal({ isOpen, onClose, puckNames, slotsMap, onSave }) {
    // Initialize from existing Puck column values
    const [assignments, setAssignments] = useState(() => {
        const init = {};
        puckNames.forEach(name => {
            const puck = slotsMap[name];
            const existing = puck?.rows?.find(r => r.Puck?.trim())?.Puck?.trim() || "";
            init[name] = existing;
        });
        return init;
    });

    const handleChange = (slot, value) => {
        setAssignments(prev => ({ ...prev, [slot]: value }));
    };

    const handleSave = () => {
        onSave(assignments);
        onClose();
    };

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Puck Assignment"
            className="custom-small-modal puck-assign-modal"
            overlayClassName="custom-overlay"
        >
            <h2 className="modal-header">Puck Assignment</h2>
            <p style={{ marginBottom: '12px', fontSize: '14px', color: '#666' }}>
                Assign a physical puck name to each slot:
            </p>
            <div style={{ maxHeight: '400px', overflowY: 'auto', marginBottom: '16px' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                    <thead>
                        <tr>
                            <th style={{ textAlign: 'center', padding: '6px 8px', borderBottom: '1px solid #ddd', fontSize: '13px', color: '#666' }}>Slot</th>
                            <th style={{ textAlign: 'center', padding: '6px 8px', borderBottom: '1px solid #ddd', fontSize: '13px', color: '#666' }}>Puck Name</th>
                        </tr>
                    </thead>
                    <tbody>
                        {puckNames.map(name => {
                            const hasPuck = !!slotsMap[name];
                            return (
                                <tr key={name} style={{ opacity: hasPuck ? 1 : 0.4 }}>
                                    <td style={{ padding: '4px 8px', fontWeight: 500, width: '60px', textAlign: 'center' }}>{name}</td>
                                    <td style={{ padding: '4px 8px', textAlign: 'center' }}>
                                        <input
                                            type="text"
                                            value={assignments[name] || ""}
                                            onChange={e => handleChange(name, e.target.value)}
                                            disabled={!hasPuck}
                                            placeholder={hasPuck ? "e.g. CU 1234" : "Empty slot"}
                                            style={{
                                                width: '100%',
                                                padding: '6px 8px',
                                                border: '1px solid #ccc',
                                                borderRadius: '4px',
                                                boxSizing: 'border-box',
                                                fontSize: '13px',
                                            }}
                                        />
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
            <div className="modal-footer">
                <button onClick={onClose} style={{ backgroundColor: '#e9ecef', color: '#333' }}>Cancel</button>
                <button onClick={handleSave} style={{ backgroundColor: '#3498db', color: 'white' }}>Apply</button>
            </div>
        </Modal>
    );
}
