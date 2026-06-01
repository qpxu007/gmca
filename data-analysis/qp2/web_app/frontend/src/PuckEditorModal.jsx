
import React, { useState } from 'react';
import Modal from 'react-modal';
import ModelPickerModal from './ModelPickerModal';
import SequencePickerModal from './SequencePickerModal';

// Must bind modal to app element (set in App.jsx usually, or body)
Modal.setAppElement('#root');

const REQUIRED_HEADERS = [
    "Port", "Puck", "CrystalID", "Protein", "Comment", "Directory",
    "FreezingCondition", "CrystalCondition", "Metal", "Spacegroup",
    "ModelPath", "SequencePath", "Priority", "Person",
    "DesiredResolution", "DesiredDosage"
];

export default function PuckEditorModal({ isOpen, onClose, puck, slotName, onSave }) {
    const [rows, setRows] = useState(
        puck && puck.rows ? JSON.parse(JSON.stringify(puck.rows)) : []
    );

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
                    } catch {
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
        if (header === "Puck") {
            // Bulk update: set all rows to the same Puck value
            newRows.forEach(row => { row.Puck = newValue; });
        } else {
            newRows[rowIndex][header] = newValue;
        }
        setRows(newRows);
    };

    const [modelPickerOpen, setModelPickerOpen] = useState(false);
    const [modelPickerRow, setModelPickerRow] = useState(null);
    const [seqPickerOpen, setSeqPickerOpen] = useState(false);
    const [seqPickerRow, setSeqPickerRow] = useState(null);

    const NO_ACTIONS_COLUMNS = new Set(["Port", "Puck"]);

    const handlePropagate = (header) => {
        const newRows = rows.map(r => ({ ...r }));
        // Find the LAST row whose displayed value differs from the default
        // Port-derived value (i.e. the most recently edited row).
        // "Fill rest" then only affects rows below this seed row.
        let seedIndex = -1;
        for (let i = newRows.length - 1; i >= 0; i--) {
            const val = getDisplayValue(newRows[i], header, i);
            const defaultVal = `${slotName || ""}${i + 1}`;
            if (val.trim() && val !== defaultVal) {
                seedIndex = i;
                break;
            }
        }
        if (seedIndex === -1) return;

        const seedValue = getDisplayValue(newRows[seedIndex], header, seedIndex).trim();
        // Extract trailing number and prefix: "PROTEIN_1" → ("PROTEIN_", 1)
        const match = seedValue.match(/^(.*?)(\d+)$/);
        let prefix, startNum;
        if (match) {
            prefix = match[1];
            startNum = parseInt(match[2], 10);
        } else {
            prefix = seedValue + "_";
            startNum = 1;
        }

        // Collect existing values from rows above seed to avoid duplicates
        // (e.g. CrystalID must be unique across the puck)
        const existing = new Set();
        for (let i = 0; i <= seedIndex; i++) {
            const v = getDisplayValue(newRows[i], header, i).trim();
            if (v) existing.add(v);
        }

        let nextNum = startNum + 1;
        for (let i = seedIndex + 1; i < newRows.length; i++) {
            // Skip numbers that would duplicate an existing row
            let candidate = `${prefix}${nextNum}`;
            while (existing.has(candidate)) {
                nextNum++;
                candidate = `${prefix}${nextNum}`;
            }
            newRows[i][header] = candidate;
            existing.add(candidate);
            nextNum++;
        }
        setRows(newRows);
    };

    const handleFillDown = (header) => {
        const firstValue = rows.find(r => (r[header] || "").trim())?.[header] || "";
        if (!firstValue.trim()) return;
        const newRows = rows.map(r => ({ ...r, [header]: firstValue }));
        setRows(newRows);
    };

    const handleSave = () => {
        // Apply final logic before saving
        const finalRows = rows.map((row) => {
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
                        {REQUIRED_HEADERS.map(h => {
                            const hasActions = !NO_ACTIONS_COLUMNS.has(h);
                            return (
                                <th key={h}>
                                    {h}
                                    {hasActions && (
                                        <div className="column-actions">
                                            <button
                                                className="col-action-btn"
                                                title={`Auto-number rows after the first filled row`}
                                                onClick={() => handlePropagate(h)}
                                            >Fill rest</button>
                                            <button
                                                className="col-action-btn"
                                                title={`Copy first value to all rows`}
                                                onClick={() => handleFillDown(h)}
                                            >Fill all</button>
                                        </div>
                                    )}
                                </th>
                            );
                        })}
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
                                        ) : header === "ModelPath" ? (
                                            <div style={{ display: 'flex', gap: '2px' }}>
                                                <input
                                                    type="text"
                                                    value={displayVal}
                                                    onChange={(e) => handleCellChange(rIndex, header, e.target.value)}
                                                    style={{ flex: 1 }}
                                                />
                                                <button
                                                    className="col-action-btn"
                                                    title="Browse models"
                                                    onClick={() => { setModelPickerRow(rIndex); setModelPickerOpen(true); }}
                                                    style={{ flexShrink: 0 }}
                                                >...</button>
                                            </div>
                                        ) : header === "SequencePath" ? (
                                            <div style={{ display: 'flex', gap: '2px' }}>
                                                <input
                                                    type="text"
                                                    value={displayVal}
                                                    onChange={(e) => handleCellChange(rIndex, header, e.target.value)}
                                                    style={{ flex: 1 }}
                                                />
                                                <button
                                                    className="col-action-btn"
                                                    title="Browse sequences"
                                                    onClick={() => { setSeqPickerRow(rIndex); setSeqPickerOpen(true); }}
                                                    style={{ flexShrink: 0 }}
                                                >...</button>
                                            </div>
                                        ) : (
                                            <input
                                                type="text"
                                                value={displayVal}
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

            <ModelPickerModal
                isOpen={modelPickerOpen}
                onClose={() => setModelPickerOpen(false)}
                onSelect={(filePath) => {
                    if (modelPickerRow !== null) {
                        handleCellChange(modelPickerRow, "ModelPath", filePath);
                    }
                }}
            />
            <SequencePickerModal
                isOpen={seqPickerOpen}
                onClose={() => setSeqPickerOpen(false)}
                onSelect={(filePath) => {
                    if (seqPickerRow !== null) {
                        handleCellChange(seqPickerRow, "SequencePath", filePath);
                    }
                }}
            />
        </Modal>
    );
}
