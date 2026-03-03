import React, { useState, useEffect } from 'react';
import { DndContext, closestCenter, MouseSensor, useSensor, useSensors } from '@dnd-kit/core';
import { Link } from 'react-router-dom';
import { ArrowLeft } from 'lucide-react';
import { api } from './api';
import PuckGrid from './PuckGrid';
import PuckEditorModal from './PuckEditorModal';
import ConfigurePucksModal from './ConfigurePucksModal';
import SaveAsModal from './SaveAsModal';
import SaveToDatabaseModal from './SaveToDatabaseModal';
import OpenSpreadsheetModal from './OpenSpreadsheetModal';
import './App.css'; // Make sure this exists or use index.css

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

        const sourceId = active.id; // "puck-X"
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

    const handleSendToHttp = async () => {
        const orderedSlots = puckNames.map(name => slotsMap[name] || null);
        const payload = {
            puck_names: puckNames,
            slots: orderedSlots,
            filename: filename
        };

        try {
            let res = await api.sendToHttp(payload);
            
            if (!res.success && res.error_code === 'URL_REQUIRED') {
                const url = window.prompt("Enter RPC URL:", "http://bl1ws3-40g:8001/rpc");
                if (url) {
                    res = await api.sendToHttp({ ...payload, rpc_url: url });
                } else {
                    return; // User cancelled
                }
            }

            if (res.success) {
                alert("Success: " + res.message);
            } else {
                alert("Error: " + res.message);
            }
        } catch (e) {
            alert("Send failed: " + e.message);
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
                <Link to="/dashboard" className="back-link" style={{ display: 'flex', alignItems: 'center', color: 'inherit', textDecoration: 'none', marginRight: '1rem' }}>
                    <ArrowLeft size={20} style={{ marginRight: '5px' }} />
                    Dashboard
                </Link>
                <button onClick={handleNew}>New</button>
                <button onClick={handleOpenClick}>Open</button>
                <button onClick={handleSaveClick} disabled={!isSaveEnabled}>Save</button>
                <button onClick={handleLoad}>Import</button>
                <button onClick={handleExportClick} disabled={!isSaveEnabled}>Export</button>
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
                isOpen={modalOpen} 
                onClose={() => setModalOpen(false)} 
                puck={editingPuck}
                slotName={editingSlot}
                onSave={handleModalSave}
            />

            <ConfigurePucksModal
                isOpen={configModalOpen}
                onClose={() => setConfigModalOpen(false)}
                currentNames={puckNames}
                onSave={handleConfigSave}
            />

            <SaveAsModal
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

            <OpenSpreadsheetModal
                isOpen={openModalOpen}
                onClose={() => setOpenModalOpen(false)}
                onLoad={handlePerformOpen}
            />
        </>
    );
}

export default SpreadsheetApp;