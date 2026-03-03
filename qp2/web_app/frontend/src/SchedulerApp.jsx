import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft, Calendar, Settings, Edit, Sparkles, Download, ChevronDown } from 'lucide-react';
import { api } from './api';
import SchedulerGrid from './SchedulerGrid';
import ConfigModal from './ConfigModal';
import ConfirmationModal from './ConfirmationModal';
import StaffStatsWindow from './StaffStatsWindow'; // Import
import './SchedulerApp.css';

const SchedulerApp = () => {
    const [runs, setRuns] = useState([]);
    const [selectedRunId, setSelectedRunId] = useState(null);
    const [schedule, setSchedule] = useState([]);
    const [quotas, setQuotas] = useState([]); // State for quotas
    const [loading, setLoading] = useState(false);
    const [allBeamlines, setAllBeamlines] = useState([]);
    const [allDayTypes, setAllDayTypes] = useState([]);
    const [allStaff, setAllStaff] = useState([]);
    const [configModalOpen, setConfigModalOpen] = useState(false);
    const [activePaintType, setActivePaintType] = useState(null);
    const [showPaintPalette, setShowPaintPalette] = useState(false);
    
    // Confirmation Modal State
    const [confirmModal, setConfirmModal] = useState({
        isOpen: false,
        title: "",
        message: "",
        onConfirm: null
    });

    useEffect(() => {
        fetchInitialData();
    }, [configModalOpen]); // Re-fetch when config closes to update lists

    useEffect(() => {
        if (selectedRunId) {
            fetchSchedule(selectedRunId);
            fetchQuotas(selectedRunId); // Fetch quotas
        }
    }, [selectedRunId]);

    const fetchInitialData = async () => {
        try {
            console.log("Fetching initial scheduler data...");
            const [runsData, beamlinesData, dayTypesData, staffData] = await Promise.all([
                api.listRuns(),
                api.listBeamlines(),
                api.listDayTypes(),
                api.listStaff()
            ]);
            setRuns(runsData);
            setAllBeamlines(beamlinesData);
            setAllDayTypes(dayTypesData);
            setAllStaff(staffData);

            if (runsData.length > 0 && !selectedRunId) {
                // Only select default if none selected or current one deleted
                if (!selectedRunId || !runsData.find(r => r.id === selectedRunId)) {
                    console.log("Selecting default run:", runsData[0].id);
                    setSelectedRunId(runsData[0].id);
                }
            }
        } catch (e) {
            console.error("Failed to fetch initial scheduler data", e);
        }
    };

    const fetchSchedule = async (runId) => {
        setLoading(true);
        try {
            const data = await api.getSchedule(runId);
            setSchedule(data);
        } catch (e) {
            console.error("Failed to fetch schedule", e);
        } finally {
            setLoading(false);
        }
    };

    const fetchQuotas = async (runId) => {
        try {
            const data = await api.listQuotas(runId);
            setQuotas(data);
        } catch (e) {
            console.error("Failed to fetch quotas", e);
        }
    };

    const handleInitDefaults = async () => {
        if (window.confirm("Initialize default Beamlines and Day Types?")) {
            try {
                await api.initDefaults();
                alert("Defaults initialized!");
                fetchInitialData(); 
            } catch (e) {
                alert("Failed to init defaults");
            }
        }
    };

    const handleUpdateDay = async (updatedData) => {
        try {
            await api.updateScheduleDay(updatedData);
            if (selectedRunId) {
                fetchSchedule(selectedRunId);
            }
        } catch (e) {
            alert("Failed to update schedule day: " + e.message);
        }
    };

    const executeAutoAssign = async (overwrite) => {
        setLoading(true);
        try {
            const res = await api.autoAssign(selectedRunId, overwrite);
            alert(res.message);
            fetchSchedule(selectedRunId);
        } catch (e) {
            alert("Auto-assign failed: " + e.message);
        } finally {
            setLoading(false);
            setConfirmModal({ ...confirmModal, isOpen: false });
        }
    };

    const handleAutoAssign = () => {
        if (!selectedRunId) {
            alert("Please select a run first.");
            return;
        }

        // Open Confirmation Modal
        setConfirmModal({
            isOpen: true,
            title: "Auto Assign Staff",
            message: "Do you want to auto-assign staff to the current run? You can choose to overwrite existing assignments or only fill empty slots.",
            onConfirm: () => {
                // Secondary confirmation for overwrite - could be improved with a checkbox in modal, 
                // but for now let's just ask via another state or assume "No Overwrite" is default safest?
                // Or maybe just ask "Overwrite?" in a second step? 
                // Let's implement a simpler flow: Always ask Overwrite? 
                // Wait, custom modal is better.
                // Let's make the modal just trigger the overwrite confirm or pass a param?
                // Simplest: The modal confirms the ACTION. The overwrite choice is implicit or we add a second button?
                // Let's use `window.confirm` for the overwrite part INSIDE the execution if we want, 
                // OR better: Just default to NO overwrite for safety, or make the modal have two buttons?
                // "Assign Empty Only" vs "Overwrite All".
                // I will add a `window.confirm` for overwrite INSIDE `executeAutoAssign` as a quick fix, 
                // or rely on the user's intent. 
                
                // Let's stick to the original behavior but wrapped in the modal start.
                // Actually, let's keep it simple: Click OK -> Ask Overwrite (native) -> Execute.
                // Or: Custom modal asking "Overwrite existing assignments?" with Yes/No?
                
                // Re-reading original requirement: "Auto Assign can be easily clicked by accident".
                // So the MAIN goal is to prevent accidental clicks. 
                // The current modal serves that purpose.
                
                const overwrite = window.confirm("Do you want to overwrite EXISTING assignments? Click Cancel to only fill EMPTY slots.");
                executeAutoAssign(overwrite);
            }
        });
    };

    const handleExportMySchedule = async () => {
        const username = localStorage.getItem('user');
        if (!username) {
            alert("You are not logged in.");
            return;
        }
        
        const myStaff = allStaff.find(s => s.username === username);
        if (!myStaff) {
            alert(`Could not find a staff profile for username '${username}'. Please ensure you are listed in the Staff configuration.`);
            return;
        }

        try {
            const res = await api.exportStaffSchedule(myStaff.id);
            // Download file
            const url = window.URL.createObjectURL(new Blob([res.data]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', `schedule_${username}.ics`);
            document.body.appendChild(link);
            link.click();
            link.remove();
        } catch (e) {
            alert("Failed to export schedule: " + e.message);
        }
    };

    const getCurrentPaintName = () => {
        if (activePaintType === null) return "Cursor (Edit)";
        const dt = allDayTypes.find(d => d.id === activePaintType);
        return dt ? dt.name : "Unknown";
    };

    return (
        <div className="scheduler-container">
            <div className="toolbar">
                <Link to="/dashboard" className="back-link" style={{ display: 'flex', alignItems: 'center', color: 'inherit', textDecoration: 'none', marginRight: '1rem' }}>
                    <ArrowLeft size={20} style={{ marginRight: '5px' }} />
                    Dashboard
                </Link>
                <div className="toolbar-title">
                    <Calendar size={20} style={{ marginRight: '8px' }} />
                    Beamtime Scheduler
                </div>
                
                <div className="run-selector">
                    <label>Run:</label>
                    <select 
                        value={selectedRunId || ""} 
                        onChange={(e) => setSelectedRunId(parseInt(e.target.value))}
                        disabled={runs.length === 0}
                    >
                        <option value="" disabled>Select a Run...</option>
                        {runs.length === 0 && <option value="" disabled>No Runs Configured</option>}
                        {runs.map(run => (
                            <option key={run.id} value={run.id}>{run.name}</option>
                        ))}
                    </select>
                </div>

                <div className="paint-palette-container">
                    <button 
                        className="secondary-btn" 
                        onClick={() => setShowPaintPalette(!showPaintPalette)}
                        style={{ minWidth: '160px', justifyContent: 'space-between' }}
                        title="Select Schedule Type to Paint"
                    >
                        <span>{getCurrentPaintName()}</span>
                        <ChevronDown size={14} />
                    </button>
                    
                    {showPaintPalette && (
                        <div className="paint-palette-dropdown">
                            <button 
                                className={`palette-item ${activePaintType === null ? 'active' : ''}`}
                                onClick={() => { setActivePaintType(null); setShowPaintPalette(false); }}
                            >
                                Cursor (Edit Mode)
                            </button>
                            {allDayTypes.map(dt => (
                                <button
                                    key={dt.id}
                                    className={`palette-item ${activePaintType === dt.id ? 'active' : ''}`}
                                    onClick={() => { setActivePaintType(dt.id); setShowPaintPalette(false); }}
                                >
                                    <span 
                                        className="color-dot" 
                                        style={{ backgroundColor: dt.color_code }}
                                    ></span>
                                    {dt.name}
                                </button>
                            ))}
                        </div>
                    )}
                </div>

                <div style={{ flex: 1 }}></div>

                <button onClick={handleExportMySchedule} className="secondary-btn" style={{ marginRight: '10px' }} title="Download My Schedule (.ics)">
                    <Download size={16} style={{ marginRight: '5px' }}/> 
                    My Schedule
                </button>

                <button 
                    onClick={handleAutoAssign} 
                    className="auto-assign-btn"
                >
                    <Sparkles size={16} style={{ marginRight: '5px' }}/> 
                    Auto Assign
                </button>

                <button onClick={() => setConfigModalOpen(true)} className="secondary-btn" style={{ marginRight: '10px' }}>
                    <Edit size={16} style={{ marginRight: '5px' }}/> 
                    Manage Config
                </button>

                <button onClick={handleInitDefaults} className="secondary-btn">
                    <Settings size={16} style={{ marginRight: '5px' }}/> 
                    Init Defaults
                </button>
            </div>

            <div className="scheduler-content">
                {loading ? (
                    <p>Loading schedule...</p>
                ) : (
                    selectedRunId && allBeamlines.length > 0 && allDayTypes.length > 0 ? (
                        <SchedulerGrid 
                            schedule={schedule}
                            selectedRun={runs.find(r => r.id === selectedRunId)}
                            allBeamlines={allBeamlines}
                            allDayTypes={allDayTypes}
                            allStaff={allStaff}
                            onUpdateDay={handleUpdateDay}
                            activePaintType={activePaintType}
                        />
                    ) : (
                        <div className="empty-state">
                            <p>No schedule data or configuration found.</p>
                            <p>Use "Manage Config" to create runs or "Init Defaults" to seed data.</p>
                        </div>
                    )
                )}
            </div>

            <ConfigModal 
                isOpen={configModalOpen} 
                onClose={() => setConfigModalOpen(false)} 
            />

            <ConfirmationModal
                isOpen={confirmModal.isOpen}
                title={confirmModal.title}
                message={confirmModal.message}
                onConfirm={confirmModal.onConfirm}
                onCancel={() => setConfirmModal({ ...confirmModal, isOpen: false })}
            />
            
            {/* Floating Stats Window */}
            {selectedRunId && (
                <StaffStatsWindow 
                    schedule={schedule}
                    allStaff={allStaff}
                    quotas={quotas}
                    allBeamlines={allBeamlines}
                    allDayTypes={allDayTypes}
                />
            )}
        </div>
    );
};

export default SchedulerApp;
