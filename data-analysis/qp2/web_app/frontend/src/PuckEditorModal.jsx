
import React, { useState, useEffect } from 'react';
import Modal from 'react-modal';

// Must bind modal to app element (set in App.jsx usually, or body)
Modal.setAppElement('#root');

const REQUIRED_HEADERS = [
    "Port", "CrystalID", "Protein", "Comment", "Directory",
    "FreezingCondition", "CrystalCondition", "Metal", "Spacegroup",
    "ModelPath", "SequencePath", "Priority", "Person"
];

export default function PuckEditorModal({ isOpen, onClose, puck, slotName, onSave }) {
    const [rows, setRows] = useState([]);

    useEffect(() => {
        if (puck && puck.rows) {
            // Clone rows to avoid direct mutation until save
            setRows(JSON.parse(JSON.stringify(puck.rows)));
        }
    }, [puck]);

    const getDisplayValue = (row, header, index) => {
        const oldPort = (row.Port || "").trim();
        let displayValue = row[header] || "";

        // Simulate transformations if we are in a specific slot
        if (slotName) {
            const newPort = `${slotName}${index + 1}`;
            
            if (header === "Port") {
                return newPort;
            }
            if (header === "CrystalID") {
                if (displayValue === oldPort) {
                    return newPort;
                }
            }
            if (header === "Directory") {
                if (displayValue && oldPort) {
                    // Regex replacement similar to backend
                    // JS Regex for word boundary is \b, but backend used lookarounds (?<![A-Za-z0-9])
                    // We can approximate or try to match exactly.
                    // Let's implement a simple robust replacement for display purposes.
                    // Escape oldPort for regex
                    const escapedOld = oldPort.replace(/[.*+?^${}()|[\\]/g, '\\$&');
                    // Lookbehind support in JS is good in modern browsers (Chrome 62+, Firefox 51+, Safari 78+)
                    // We'll use a simpler approach: split and join or replace with regex.
                    try {
                        const regex = new RegExp(`(?<![A-Za-z0-9])${escapedOld}(?![A-Za-z0-9])`, 'g');
                        displayValue = displayValue.replace(regex, newPort);
                    } catch (e) {
                        // Fallback for browsers without lookbehind
                        // Just simple replace if complex regex fails
                         displayValue = displayValue.split(oldPort).join(newPort);
                    }
                }
            }
        }
        return displayValue;
    };

    const handleCellChange = (rowIndex, header, newValue) => {
        const newRows = [...rows];
        newRows[rowIndex][header] = newValue;
        
        // Auto-fill Directory from CrystalID logic
        // "Logic: If Directory is empty, default to CrystalID"
        // This usually happens on "Save" (Accept) in the desktop app.
        // We can do it here or on save. Let's do it on save to be consistent.
        
        setRows(newRows);
    };

    const handleSave = () => {
        // Apply final logic before saving
        const finalRows = rows.map((row, index) => {
            // 1. If Directory empty, default to CrystalID
            let newRow = { ...row };
            if (!newRow.Directory && newRow.CrystalID) {
                newRow.Directory = newRow.CrystalID;
            }
            
            // Note: We do NOT bake in the "Display Value" transformations (Port/CrystalID/Directory changes).
            // Those happen on Export (Backend) or are just visual here.
            // If the user *edited* a cell, handleCellChange updated `rows`.
            // If they didn't touch it, it remains original (e.g. "O1"), which is correct for `save_file` logic.
            // However, if the user sees "A1" and clicks Save, they might expect "A1" to be persisted?
            // In the desktop app logic I implemented earlier, I decided to keep the display virtual in the editor
            // unless the user edits it? No, in the desktop app `PuckEditorDialog`, I only updated the DISPLAY.
            // I did NOT save the transformed values back to `puck.rows` automatically.
            // So `save_file` (backend) is responsible for the transformation.
            // So here, we just save the `rows` state (which contains manual edits + original data).
            
            return newRow;
        });

        onSave(finalRows);
        onClose();
    };

    if (!puck) return null;

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Puck Editor"
            style={{
                overlay: {
                    backgroundColor: 'rgba(0, 0, 0, 0.5)',
                    zIndex: 2000
                },
                content: {
                    inset: '40px',
                    border: '1px solid #ccc',
                    background: '#fff',
                    borderRadius: '8px',
                    outline: 'none',
                    padding: '20px',
                    display: 'flex',
                    flexDirection: 'column',
                    zIndex: 2001
                }
            }}
        >
            <div className="modal-header">
                Edit Puck {puck.original_label} {slotName ? `(in Slot ${slotName})` : ''}
            </div>
            
            <table className="data-table">
                <thead>
                    <tr>
                        {REQUIRED_HEADERS.map(h => <th key={h}>{h}</th>)}
                    </tr>
                </thead>
                <tbody>
                    {rows.map((row, rIndex) => (
                        <tr key={rIndex}>
                            {REQUIRED_HEADERS.map((header, cIndex) => {
                                const isPort = header === "Port";
                                const displayVal = getDisplayValue(row, header, rIndex);
                                
                                return (
                                    <td key={cIndex} className={isPort ? "read-only" : ""}>
                                        {isPort ? (
                                            displayVal
                                        ) : (
                                            <input
                                                type="text"
                                                value={displayVal} // Input shows transformed value?
                                                // If we show transformed value, onChange needs to handle it.
                                                // If user edits "A1" (which was "O1"), `row.CrystalID` becomes "A1".
                                                // This effectively "bakes in" the change. This is good.
                                                onChange={(e) => handleCellChange(rIndex, header, e.target.value)}
                                            />
                                        )}
                                    </td>
                                );
                            })}
                        </tr>
                    ))}
                </tbody>
            </table>

            <div className="modal-footer">
                <button onClick={onClose} style={{backgroundColor: '#e9ecef', color: '#333'}}>Cancel</button>
                <button onClick={handleSave} style={{backgroundColor: '#3498db', color: 'white'}}>Save Changes</button>
            </div>
        </Modal>
    );
}
