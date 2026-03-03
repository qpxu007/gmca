import React, { useState } from 'react';
import Modal from 'react-modal';
import './SaveAsModal.css'; // Reuse styles

Modal.setAppElement('#root');

const SaveToDatabaseModal = ({ isOpen, onClose, currentName, onSave }) => {
    const [name, setName] = useState(currentName || "");
    const [esafId, setEsafId] = useState("");

    const handleSubmit = (e) => {
        e.preventDefault();
        if (!esafId.toLowerCase().startsWith("esaf") || isNaN(esafId.substring(4))) {
            alert("ESAF ID must start with 'esaf' followed by digits (e.g., esaf12345)");
            return;
        }
        onSave(name, esafId);
    };

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Save Spreadsheet"
            className="modal-content"
            overlayClassName="modal-overlay"
        >
            <h2>Save Spreadsheet</h2>
            <form onSubmit={handleSubmit}>
                <div className="form-group">
                    <label>Name:</label>
                    <input 
                        type="text" 
                        value={name} 
                        onChange={(e) => setName(e.target.value)} 
                        autoFocus
                        required
                    />
                </div>
                <div className="form-group">
                    <label>ESAF ID (e.g., esaf12345):</label>
                    <input 
                        type="text" 
                        value={esafId} 
                        onChange={(e) => setEsafId(e.target.value)} 
                        placeholder="esaf..."
                        required
                    />
                </div>
                <div className="modal-actions">
                    <button type="button" onClick={onClose} className="cancel-btn">Cancel</button>
                    <button type="submit" className="save-btn">Save</button>
                </div>
            </form>
        </Modal>
    );
};

export default SaveToDatabaseModal;
