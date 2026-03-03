
import React, { useState, useEffect } from 'react';
import Modal from 'react-modal';

Modal.setAppElement('#root');

export default function ConfigurePucksModal({ isOpen, onClose, currentNames, onSave }) {
    const [text, setText] = useState("");

    useEffect(() => {
        if (isOpen && currentNames) {
            setText(currentNames.join(", "));
        }
    }, [currentNames, isOpen]);

    const handleSave = () => {
        const names = text.split(",").map(s => s.trim()).filter(Boolean);
        if (names.length === 0) {
            alert("Puck list cannot be empty.");
            return;
        }
        onSave(names);
        onClose();
    };

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Configure Pucks"
            className="custom-small-modal"
            overlayClassName="custom-overlay"
        >
            <h2 className="modal-header">Configure Pucks</h2>
            <p style={{marginBottom: '10px', fontSize: '14px', color: '#666'}}>
                Enter Puck Names separated by commas (e.g. A, B, C or P1, P2):
            </p>
            <textarea
                value={text}
                onChange={(e) => setText(e.target.value)}
                style={{
                    width: '100%',
                    height: '150px', // Slightly larger
                    padding: '10px',
                    marginBottom: '20px',
                    border: '1px solid #ccc',
                    borderRadius: '4px',
                    resize: 'vertical'
                }}
            />
            <div className="modal-footer">
                <button onClick={onClose} style={{backgroundColor: '#e9ecef', color: '#333'}}>Cancel</button>
                <button onClick={handleSave} style={{backgroundColor: '#3498db', color: 'white'}}>Save Configuration</button>
            </div>
        </Modal>
    );
}
