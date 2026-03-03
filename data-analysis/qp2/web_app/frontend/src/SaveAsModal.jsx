
import React, { useState, useEffect } from 'react';
import Modal from 'react-modal';

Modal.setAppElement('#root');

export default function SaveAsModal({ isOpen, onClose, currentFilename, onSave }) {
    const [filename, setFilename] = useState("");

    useEffect(() => {
        if (isOpen) {
            // Pre-fill with current filename or default
            let name = currentFilename;
            if (name === "No file loaded." || name === "New Spreadsheet") {
                name = "spreadsheet_export.xlsx"; // Default to modern Excel
            }
            setFilename(name);
        }
    }, [isOpen, currentFilename]);

    const handleSave = () => {
        if (!filename.trim()) {
            alert("Filename cannot be empty.");
            return;
        }
        onSave(filename);
        onClose();
    };

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Save As"
            className="custom-small-modal custom-save-modal"
            overlayClassName="custom-overlay"
        >
            <h2 className="modal-header">Export Spreadsheet</h2>
            <p style={{marginBottom: '10px', fontSize: '14px', color: '#666'}}>
                Enter filename with extension (.csv, .xlsx, .xls):
            </p>
            <input
                type="text"
                value={filename}
                onChange={(e) => setFilename(e.target.value)}
                style={{
                    width: '100%',
                    padding: '10px',
                    marginBottom: '20px',
                    border: '1px solid #ccc',
                    borderRadius: '4px'
                }}
            />
            <div className="modal-footer">
                <button onClick={onClose} style={{backgroundColor: '#e9ecef', color: '#333'}}>Cancel</button>
                <button onClick={handleSave} style={{backgroundColor: '#27ae60', color: 'white'}}>Download</button>
            </div>
        </Modal>
    );
}
