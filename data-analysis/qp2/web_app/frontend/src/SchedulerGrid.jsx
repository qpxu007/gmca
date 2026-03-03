import React, { useState, useEffect } from 'react';
import { api } from './api';
import DayEditorModal from './DayEditorModal';
import './SchedulerGrid.css';

const SchedulerGrid = ({ schedule, selectedRun, allBeamlines, allDayTypes, allStaff, onUpdateDay, activePaintType }) => {
    const [editingDay, setEditingDay] = useState(null); // For Modal
    const [isModalOpen, setIsModalOpen] = useState(false);
    
    // Group schedule data by date for easier rendering
    const scheduleByDate = {};
    schedule.forEach(day => {
        if (!scheduleByDate[day.date]) {
            scheduleByDate[day.date] = {};
        }
        scheduleByDate[day.date][day.beamline_id] = day;
    });

    // Generate dates for the selected run
    const dates = [];
    if (selectedRun) {
        let currentDate = new Date(selectedRun.start_date);
        const endDate = new Date(selectedRun.end_date);
        while (currentDate <= endDate) {
            dates.push(currentDate.toISOString().split('T')[0]); // YYYY-MM-DD
            currentDate.setDate(currentDate.getDate() + 1);
        }
    }

    // Helper to get lookup maps
    const beamlineMap = allBeamlines.reduce((map, bl) => ({ ...map, [bl.id]: bl }), {});
    const dayTypeMap = allDayTypes.reduce((map, dt) => ({ ...map, [dt.id]: dt }), {});
    const staffMap = allStaff.reduce((map, st) => ({ ...map, [st.id]: st }), {});

    const handleCellClick = (dayData) => {
        if (!dayData) return;

        if (activePaintType !== null) {
            // Paint Mode: Apply the selected day type immediately
            if (dayData.day_type_id !== activePaintType) {
                const updatedData = {
                    day_id: dayData.id,
                    day_type_id: activePaintType,
                    assigned_staff_id: dayData.assigned_staff_id
                };
                onUpdateDay(updatedData);
            }
        } else {
            // Cursor Mode: Open Modal
            setEditingDay(dayData);
            setIsModalOpen(true);
        }
    };

    const handleStaffChange = async (e, dayData) => {
        const newStaffId = e.target.value ? parseInt(e.target.value) : null;
        if (!dayData) return;

        // Optimistic / Immediate update
        const updatedData = {
            day_id: dayData.id,
            day_type_id: dayData.day_type_id,
            assigned_staff_id: newStaffId
        };
        
        onUpdateDay(updatedData);
    };

    const handleSaveDay = (updatedData) => {
        onUpdateDay(updatedData);
        setIsModalOpen(false);
        setEditingDay(null);
    };

    // Grid Template: Date (120px) + For each beamline: [Type (1fr) Staff (1fr)]
    const gridStyle = {
        gridTemplateColumns: `120px repeat(${allBeamlines.length * 2}, 1fr)`
    };

    return (
        <div className="scheduler-grid-container" style={gridStyle}>
            <div className="grid-header">
                <div className="header-cell date-header">Date</div>
                {allBeamlines.map(beamline => (
                    <React.Fragment key={beamline.id}>
                        <div className="header-cell beamline-header">
                            {beamline.name} ({beamline.alias})
                        </div>
                        <div className="header-cell beamline-header">
                            Host
                        </div>
                    </React.Fragment>
                ))}
            </div>
            <div className="grid-body">
                {dates.map((dateStr) => {
                    const dateObj = new Date(dateStr);
                    // getDay() returns 0 for Sunday, 6 for Saturday
                    const dayOfWeek = dateObj.getDay();
                    const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
                    
                    return (
                        <React.Fragment key={dateStr}>
                            <div 
                                className="date-cell"
                                style={isWeekend ? { backgroundColor: '#e0e0e0', color: '#666' } : {}}
                            >
                                {dateStr}
                            </div>
                            {allBeamlines.map((beamline) => {
                                const dayData = scheduleByDate[dateStr] ? scheduleByDate[dateStr][beamline.id] : null;
                                const dayType = dayData ? dayTypeMap[dayData.day_type_id] : null;
                                // const staff = dayData && dayData.assigned_staff_id ? staffMap[dayData.assigned_staff_id] : null;

                                return (
                                    <React.Fragment key={`${dateStr}-${beamline.id}`}>
                                        {/* Day Type Cell */}
                                        <div 
                                            className="schedule-cell clickable"
                                            style={{ backgroundColor: dayType ? dayType.color_code : '#FFFFFF' }}
                                            onClick={() => handleCellClick(dayData)}
                                        >
                                            {dayData ? (
                                                <div className="cell-day-type">{dayType ? dayType.name : 'N/A'}</div>
                                            ) : (
                                                <div className="cell-day-type">Not Scheduled</div>
                                            )}
                                        </div>

                                        {/* Staff/Host Cell with Stealth Select */}
                                        <div 
                                            className="schedule-cell"
                                            style={{ backgroundColor: isWeekend ? '#e0e0e0' : '#f9f9f9', padding: '0' }}
                                        >
                                            {dayData ? (
                                                <select 
                                                    className="stealth-select"
                                                    value={dayData.assigned_staff_id || ""}
                                                    onChange={(e) => handleStaffChange(e, dayData)}
                                                >
                                                    <option value="">-</option>
                                                    {allStaff.map(st => (
                                                        <option key={st.id} value={st.id}>{st.full_name}</option>
                                                    ))}
                                                </select>
                                            ) : (
                                                <div className="cell-staff placeholder">-</div>
                                            )}
                                        </div>
                                    </React.Fragment>
                                );
                            })}
                        </React.Fragment>
                    );
                })}
            </div>

            <DayEditorModal
                isOpen={isModalOpen}
                onClose={() => setIsModalOpen(false)}
                dayData={editingDay}
                allDayTypes={allDayTypes}
                allStaff={allStaff}
                onSave={handleSaveDay}
            />
        </div>
    );
};

export default SchedulerGrid;