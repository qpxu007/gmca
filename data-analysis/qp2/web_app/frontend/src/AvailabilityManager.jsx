import React, { useState, useEffect } from 'react';
import { api } from './api';
import './AvailabilityManager.css';

const AvailabilityManager = () => {
    const [staff, setStaff] = useState([]);
    const [selectedStaffId, setSelectedStaffId] = useState(null);
    const [availability, setAvailability] = useState({}); // Map: "YYYY-MM-DD" -> "PREFERENCE"
    
    // Display range: Start from current month, show next 6 months
    const [startMonth, setStartMonth] = useState(new Date());

    useEffect(() => {
        const fetchStaff = async () => {
            try {
                const data = await api.listStaff();
                setStaff(data);
                if (data.length > 0) setSelectedStaffId(data[0].id);
            } catch (e) {
                console.error("Failed to fetch staff", e);
            }
        };
        fetchStaff();
    }, []);

    useEffect(() => {
        if (selectedStaffId) {
            fetchAvailability(selectedStaffId);
        }
    }, [selectedStaffId]);

    const fetchAvailability = async (staffId) => {
        try {
            const data = await api.listAvailability(staffId);
            const map = {};
            data.forEach(item => {
                map[item.date] = item.preference;
            });
            setAvailability(map);
        } catch (e) {
            console.error("Failed to fetch availability", e);
        }
    };

    const handleDayClick = async (dateStr) => {
        if (!selectedStaffId) return;

        const currentPref = availability[dateStr];
        let newPref;
        
        if (currentPref === 'UNAVAILABLE') {
            newPref = 'PREFERRED';
        } else if (currentPref === 'PREFERRED') {
            newPref = 'NEUTRAL';
        } else {
            newPref = 'UNAVAILABLE';
        }
        
        setAvailability(prev => ({ ...prev, [dateStr]: newPref }));

        try {
            await api.updateAvailability({
                staff_id: selectedStaffId,
                date: dateStr,
                preference: newPref
            });
        } catch (e) {
            console.error("Failed to update availability", e);
        }
    };

    const renderMonth = (monthDate) => {
        const year = monthDate.getFullYear();
        const month = monthDate.getMonth();
        const daysInMonth = new Date(year, month + 1, 0).getDate();
        const firstDay = new Date(year, month, 1).getDay(); // 0 = Sunday
        const monthName = monthDate.toLocaleString('default', { month: 'long', year: 'numeric' });

        const blanks = Array(firstDay).fill(null);
        const dayNumbers = Array.from({ length: daysInMonth }, (_, i) => i + 1);
        const allCells = [...blanks, ...dayNumbers];

        return (
            <div key={`${year}-${month}`} className="month-container">
                <div className="month-title">{monthName}</div>
                <div className="avail-calendar-grid">
                    {['S', 'M', 'T', 'W', 'T', 'F', 'S'].map(d => (
                        <div key={d} className="avail-day-header">{d}</div>
                    ))}
                    {allCells.map((day, index) => {
                        if (!day) return <div key={index} className="avail-day-cell empty"></div>;
                        
                        const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
                        const pref = availability[dateStr];
                        
                        let className = "avail-day-cell clickable";
                        if (pref === 'UNAVAILABLE') className += " unavailable";
                        else if (pref === 'PREFERRED') className += " preferred";

                        return (
                            <div 
                                key={dateStr} 
                                className={className}
                                onClick={() => handleDayClick(dateStr)}
                            >
                                {day}
                            </div>
                        );
                    })}
                </div>
            </div>
        );
    };

    const renderAllMonths = () => {
        const months = [];
        for (let i = 0; i < 6; i++) {
            const d = new Date(startMonth);
            d.setMonth(startMonth.getMonth() + i);
            months.push(renderMonth(d));
        }
        return <div className="months-wrapper">{months}</div>;
    };

    return (
        <div className="manager-container">
            <h3>Staff Availability</h3>
            <div style={{ marginBottom: '1rem', display: 'flex', gap: '10px', alignItems: 'center' }}>
                <label>Staff: </label>
                <select 
                    value={selectedStaffId || ""} 
                    onChange={(e) => setSelectedStaffId(parseInt(e.target.value))}
                    style={{ padding: '6px', borderRadius: '4px', border: '1px solid #ccc' }}
                >
                    {staff.map(s => <option key={s.id} value={s.id}>{s.full_name}</option>)}
                </select>
                
                <div className="legend">
                    <span className="legend-item"><span className="dot red"></span> Unavailable</span>
                    <span className="legend-item"><span className="dot green"></span> Preferred</span>
                    <span className="legend-item"><span className="dot lightgray"></span> Neutral</span>
                </div>
            </div>
            
            <p style={{ textAlign: 'center', fontSize: '0.9em', color: '#666', marginBottom: '10px' }}>
                Click a day to toggle: <span style={{color: '#cf1322', fontWeight: 'bold'}}>Unavailable</span> &rarr; <span style={{color: '#389e0d', fontWeight: 'bold'}}>Preferred</span> &rarr; Neutral
            </p>

            {renderAllMonths()}
        </div>
    );
};

export default AvailabilityManager;