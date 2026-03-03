import React, { useEffect, useState } from 'react';
import Modal from 'react-modal';
import { api } from './api';
import { Trash2, FolderOpen } from 'lucide-react';
import './OpenSpreadsheetModal.css'; // We'll create this

Modal.setAppElement('#root');

const OpenSpreadsheetModal = ({ isOpen, onClose, onLoad }) => {
    const [spreadsheets, setSpreadsheets] = useState([]);
    const [filterText, setFilterText] = useState("");
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const fetchSpreadsheets = async () => {
        setLoading(true);
        setError(null);
        try {
            const data = await api.listSpreadsheets();
            setSpreadsheets(data);
        } catch (err) {
            setError("Failed to load spreadsheets.");
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        if (isOpen) {
            fetchSpreadsheets();
            setFilterText(""); // Reset filter on open
        }
    }, [isOpen]);

    const handleLoad = async (id) => {
        try {
            const data = await api.getSpreadsheet(id);
            onLoad(data);
            onClose();
        } catch (err) {
            alert("Failed to load spreadsheet details.");
        }
    };

    const handleDelete = async (e, id) => {
        e.stopPropagation(); // Prevent triggering row click
        if (window.confirm("Are you sure you want to delete this spreadsheet?")) {
            try {
                await api.deleteSpreadsheet(id);
                fetchSpreadsheets(); // Refresh list
            } catch (err) {
                alert("Failed to delete spreadsheet.");
            }
        }
    };

    const filteredSpreadsheets = spreadsheets.filter(sheet => {
        const text = filterText.toLowerCase();
        return (
            sheet.name.toLowerCase().includes(text) ||
            sheet.username.toLowerCase().includes(text) ||
            (sheet.esaf_id && sheet.esaf_id.toLowerCase().includes(text))
        );
    });

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Open Spreadsheet"
            className="modal-content large-modal"
            overlayClassName="modal-overlay"
        >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                <h2 style={{ margin: 0 }}>Open Spreadsheet</h2>
                <input
                    type="text"
                    placeholder="Filter by name, user, or ESAF..."
                    value={filterText}
                    onChange={(e) => setFilterText(e.target.value)}
                    style={{ padding: '8px', width: '250px', borderRadius: '4px', border: '1px solid #ccc' }}
                />
            </div>
            
            {loading && <p>Loading...</p>}
            {error && <p className="error">{error}</p>}
            
            {!loading && !error && (
                <div className="spreadsheet-list">
                    {filteredSpreadsheets.length === 0 ? (
                        <p>No spreadsheets match your filter.</p>
                    ) : (
                        <table>
                            <thead>
                                <tr>
                                    <th>Name</th>
                                    <th>ESAF ID</th>
                                    <th>User</th>
                                    <th>Last Updated</th>
                                    <th>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                {filteredSpreadsheets.map(sheet => (
                                    <tr key={sheet.id} onClick={() => handleLoad(sheet.id)} className="clickable-row">
                                        <td>
                                            <div className="sheet-name-cell">
                                                <FolderOpen size={16} />
                                                {sheet.name}
                                            </div>
                                        </td>
                                        <td>{sheet.esaf_id}</td>
                                        <td>{sheet.username}</td>
                                        <td>{new Date(sheet.updated_at).toLocaleString()}</td>
                                        <td>
                                            <button 
                                                className="icon-btn delete-btn"
                                                onClick={(e) => handleDelete(e, sheet.id)}
                                                title="Delete"
                                            >
                                                <Trash2 size={18} />
                                            </button>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    )}
                </div>
            )}

            <div className="modal-actions">
                <button onClick={onClose} className="cancel-btn">Cancel</button>
            </div>
        </Modal>
    );
};

export default OpenSpreadsheetModal;
