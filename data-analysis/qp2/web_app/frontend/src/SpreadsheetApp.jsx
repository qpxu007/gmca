import React, { useState, useEffect } from 'react';
import { DndContext, closestCenter, MouseSensor, useSensor, useSensors } from '@dnd-kit/core';
import { Link } from 'react-router-dom';
import { Grid, Home } from 'lucide-react';
import { api } from './api';
import PuckGrid from './PuckGrid';
import PuckEditorModal from './PuckEditorModal';
import ConfigurePucksModal from './ConfigurePucksModal';
import SaveAsModal from './SaveAsModal';
import SaveToDatabaseModal from './SaveToDatabaseModal';
import OpenSpreadsheetModal from './OpenSpreadsheetModal';
import PuckAssignmentModal from './PuckAssignmentModal';

const DEFAULT_PUCK_NAMES = "ABCDEFGHIJKLMNOPQR".split("");

function SpreadsheetApp() {
    const [puckNames, setPuckNames] = useState(DEFAULT_PUCK_NAMES);
    const [slotsMap, setSlotsMap] = useState({}); // { "A": { original_label: "A", ... } }
    const [filename, setFilename] = useState("No file loaded.");
    const [isSaveEnabled, setIsSaveEnabled] = useState(false);
    const [isAdmin] = useState(localStorage.getItem('is_admin') === 'true');
    
    // Modal State
    const [modalOpen, setModalOpen] = useState(false);
    const [configModalOpen, setConfigModalOpen] = useState(false);
    const [saveModalOpen, setSaveModalOpen] = useState(false);
    const [dbSaveModalOpen, setDbSaveModalOpen] = useState(false);
    const [openModalOpen, setOpenModalOpen] = useState(false);
    const [puckAssignOpen, setPuckAssignOpen] = useState(false);
    const [editingPuck, setEditingPuck] = useState(null);
    const [editingSlot, setEditingSlot] = useState(null);

    // Initialize with empty structure or wait for user
    // Desktop app starts empty grid.
    useEffect(() => {
        // Initial empty slots
        // But really we wait for New or Load.
    }, []);

    // Drag Sensors
    const sensors = useSensors(useSensor(MouseSensor, {
        activationConstraint: { distance: 5 }, // Prevent accidental drags on click
    }));

    const handleDragEnd = (event) => {
        const { active, over } = event;
        
        if (!over) return;

        const sourcePuck = active.data.current.puckData;
        
        // Find source slot (where this puck was)
        // Since we don't have back-pointer easily, we search slotsMap
        const sourceSlot = Object.keys(slotsMap).find(key => 
            slotsMap[key] && slotsMap[key].original_label === sourcePuck.original_label
        );
        
        const targetSlot = over.id; // "A"

        if (sourceSlot === targetSlot) return;

        // Perform Swap
        const targetPuck = slotsMap[targetSlot]; // Might be undefined/null

        setSlotsMap(prev => ({
            ...prev,
            [sourceSlot]: targetPuck, // Swap
            [targetSlot]: sourcePuck
        }));
    };

    // Actions
    const handleNew = async () => {
        try {
            const res = await api.createEmpty(puckNames.join(","));
            if (res.success) {
                setSlotsMap(res.pucks);
                setFilename("New Spreadsheet");
                setIsSaveEnabled(true);
            }
        } catch (e) {
            alert("Error creating new: " + e.message);
        }
    };

    const handleLoad = async () => {
        // Create a hidden file input
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.csv, .xls, .xlsx';
        input.onchange = async (e) => {
            const file = e.target.files[0];
            if (!file) return;
            
            try {
                const res = await api.uploadFile(file, puckNames.join(","));
                if (res.success) {
                    setSlotsMap(res.pucks);
                    setFilename(res.filename);
                    setIsSaveEnabled(true);
                } else {
                    alert("Errors loading file:\n" + res.errors.join("\n"));
                }
            } catch (err) {
                alert("Upload failed: " + err.message);
            }
        };
        input.click();
    };

    const handleExportClick = () => {
        setSaveModalOpen(true);
    };

    const handleSaveClick = () => {
        setDbSaveModalOpen(true);
    };

    const handleOpenClick = () => {
        setOpenModalOpen(true);
    };

    const handlePerformDbSave = async (saveName, saveEsafId) => {
        const orderedSlots = puckNames.map(name => slotsMap[name] || null);
        try {
            const res = await api.saveSpreadsheet({
                name: saveName,
                esaf_id: saveEsafId,
                puck_names: puckNames,
                slots: orderedSlots
            });
            if (res.success) {
                setFilename(saveName); // Update title
                alert(res.message);
                setDbSaveModalOpen(false);
            }
        } catch (e) {
            alert("Save failed: " + (e.response?.data?.detail || e.message));
        }
    };

    const handlePerformOpen = (data) => {
        // data contains { puck_names, slots, name, ... }
        // slots is a list matching puck_names order.
        // We need to reconstruct slotsMap.
        const newSlotsMap = {};
        
        // Ensure data.slots matches data.puck_names length
        if (data.puck_names && data.slots) {
            data.puck_names.forEach((name, index) => {
                const slotData = data.slots[index];
                if (slotData) {
                    newSlotsMap[name] = slotData;
                }
            });
        }
        
        setPuckNames(data.puck_names);
        setSlotsMap(newSlotsMap);
        setFilename(data.name);
        setIsSaveEnabled(true);
    };

    const handlePerformExport = async (exportFilename) => {
        // Construct ordered slots list
        const orderedSlots = puckNames.map(name => slotsMap[name] || null);
        
        try {
            const res = await api.exportFile({
                puck_names: puckNames,
                slots: orderedSlots,
                filename: exportFilename
            });
            
            // Trigger download
            const url = window.URL.createObjectURL(new Blob([res.data]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', exportFilename);
            document.body.appendChild(link);
            link.click();
            link.remove();
            
            setSaveModalOpen(false);
        } catch (e) {
            alert("Export failed: " + e.message);
        }
    };

    const [sendDialog, setSendDialog] = useState(null); // {beamline, url, payload}

    const BEAMLINE_RPC = {
        "23-ID-D (BL1)": "http://bl1ws3-40g:8008/rpc",
        "23-ID-B (BL2)": "http://bl2ws3-40g:8008/rpc",
    };

    const handleSendToHttp = () => {
        const orderedSlots = puckNames.map(name => slotsMap[name] || null);
        const filledCount = orderedSlots.filter(s => s !== null).length;
        setSendDialog({
            payload: { puck_names: puckNames, slots: orderedSlots, filename },
            filledCount,
            selectedBeamline: Object.keys(BEAMLINE_RPC)[0],
        });
    };

    const confirmSend = async () => {
        if (!sendDialog) return;
        const url = BEAMLINE_RPC[sendDialog.selectedBeamline];
        const payload = { ...sendDialog.payload, rpc_url: url };
        setSendDialog(null);
        try {
            const res = await api.sendToHttp(payload);
            if (res.success) {
                alert("Success: " + res.message);
            } else {
                alert("Error: " + res.message);
            }
        } catch (e) {
            alert("Send failed: " + (e.response?.data?.detail || e.message));
        }
    };

    const handleConfigure = () => {
        setConfigModalOpen(true);
    };

    const handleConfigSave = (newNames) => {
        setPuckNames(newNames);
        setConfigModalOpen(false);
    };

    const handlePuckDoubleClick = (slotName, puckData) => {
        setEditingPuck(puckData);
        setEditingSlot(slotName);
        setModalOpen(true);
    };

    const handlePuckAssignSave = (assignments) => {
        setSlotsMap(prev => {
            const updated = { ...prev };
            for (const [slot, puckName] of Object.entries(assignments)) {
                if (updated[slot]) {
                    updated[slot] = {
                        ...updated[slot],
                        rows: updated[slot].rows.map(row => ({ ...row, Puck: puckName }))
                    };
                }
            }
            return updated;
        });
    };

    const handleModalSave = (newRows) => {
        if (editingPuck && editingSlot) {
            const updatedPuck = { ...editingPuck, rows: newRows };
            setSlotsMap(prev => ({
                ...prev,
                [editingSlot]: updatedPuck
            }));
        }
    };

    return (
        <>
            <div className="toolbar">
                <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#BBE1FA', textDecoration: 'none', marginRight: '10px' }}>
                    <Home size={20} />
                </Link>
                <button onClick={handleNew}>New</button>
                <button onClick={handleOpenClick}>Open</button>
                <button onClick={handleSaveClick} disabled={!isSaveEnabled}>Save</button>
                <button onClick={handleLoad}>Import</button>
                <button onClick={handleExportClick} disabled={!isSaveEnabled}>Export</button>
                <button onClick={() => setPuckAssignOpen(true)} disabled={!isSaveEnabled}>Puck Assignment</button>
                {isAdmin && (
                    <button onClick={handleSendToHttp} disabled={!isSaveEnabled}>Send to pyBluice</button>
                )}
                {isAdmin && (
                    <button onClick={handleConfigure}>Configure</button>
                )}
                <span className="filename-label">{filename}</span>
            </div>

            <div className="main-content">
                <DndContext 
                    sensors={sensors} 
                    collisionDetection={closestCenter} 
                    onDragEnd={handleDragEnd}
                >
                    <PuckGrid 
                        puckNames={puckNames} 
                        slotsMap={slotsMap} 
                        onPuckDoubleClick={handlePuckDoubleClick} 
                    />
                </DndContext>
            </div>

            <PuckEditorModal 
                key={editingPuck?.original_label}
                isOpen={modalOpen} 
                onClose={() => setModalOpen(false)} 
                puck={editingPuck}
                slotName={editingSlot}
                onSave={handleModalSave}
            />

            <ConfigurePucksModal
                key={configModalOpen ? 'config-open' : 'config-closed'}
                isOpen={configModalOpen}
                onClose={() => setConfigModalOpen(false)}
                currentNames={puckNames}
                onSave={handleConfigSave}
            />

            <SaveAsModal
                key={saveModalOpen ? filename : 'save-closed'}
                isOpen={saveModalOpen}
                onClose={() => setSaveModalOpen(false)}
                currentFilename={filename}
                onSave={handlePerformExport}
            />

            <SaveToDatabaseModal
                isOpen={dbSaveModalOpen}
                onClose={() => setDbSaveModalOpen(false)}
                currentName={filename === "No file loaded." || filename === "New Spreadsheet" ? "" : filename}
                onSave={handlePerformDbSave}
            />

            <PuckAssignmentModal
                key={puckAssignOpen ? 'assign-open' : 'assign-closed'}
                isOpen={puckAssignOpen}
                onClose={() => setPuckAssignOpen(false)}
                puckNames={puckNames}
                slotsMap={slotsMap}
                onSave={handlePuckAssignSave}
            />

            <OpenSpreadsheetModal
                isOpen={openModalOpen}
                onClose={() => setOpenModalOpen(false)}
                onLoad={handlePerformOpen}
            />

            {sendDialog && (
                <div style={{
                    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)',
                    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 2000
                }}>
                    <div style={{
                        background: 'white', borderRadius: '8px', padding: '24px',
                        width: '400px', boxShadow: '0 8px 32px rgba(0,0,0,0.3)'
                    }}>
                        <h3 style={{ margin: '0 0 16px 0' }}>Send to pyBluice</h3>
                        <p style={{ margin: '0 0 12px 0', color: '#555' }}>
                            Send <strong>{sendDialog.filledCount}</strong> puck(s) from <strong>{filename}</strong> to:
                        </p>
                        <div style={{ margin: '0 0 16px 0' }}>
                            {Object.keys(BEAMLINE_RPC).map(bl => (
                                <label key={bl} style={{
                                    display: 'flex', alignItems: 'center', gap: '8px',
                                    padding: '8px 12px', borderRadius: '6px', cursor: 'pointer',
                                    background: sendDialog.selectedBeamline === bl ? '#e6f7ff' : 'transparent',
                                    border: sendDialog.selectedBeamline === bl ? '2px solid #3282B8' : '2px solid transparent',
                                    marginBottom: '6px',
                                }}>
                                    <input
                                        type="radio"
                                        name="beamline"
                                        checked={sendDialog.selectedBeamline === bl}
                                        onChange={() => setSendDialog({ ...sendDialog, selectedBeamline: bl })}
                                    />
                                    <span style={{ fontWeight: 500 }}>{bl}</span>
                                    <span style={{ color: '#888', fontSize: '0.8rem', marginLeft: 'auto' }}>
                                        {BEAMLINE_RPC[bl]}
                                    </span>
                                </label>
                            ))}
                        </div>
                        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '10px' }}>
                            <button
                                onClick={() => setSendDialog(null)}
                                style={{ padding: '8px 16px', border: '1px solid #ccc', borderRadius: '4px', cursor: 'pointer', background: 'white' }}
                            >
                                Cancel
                            </button>
                            <button
                                onClick={confirmSend}
                                style={{ padding: '8px 16px', border: 'none', borderRadius: '4px', cursor: 'pointer', background: '#3282B8', color: 'white', fontWeight: 600 }}
                            >
                                Send
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </>
    );
}

export default SpreadsheetApp;