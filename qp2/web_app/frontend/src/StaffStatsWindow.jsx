import React, { useMemo, useState, useEffect, useRef } from 'react';
import { Minus, Square, GripHorizontal } from 'lucide-react';
import './StaffStatsWindow.css';

const StaffStatsWindow = ({ schedule, allStaff, quotas, allBeamlines = [], allDayTypes = [] }) => {
    const [isMinimized, setIsMinimized] = useState(false);
    const [position, setPosition] = useState({ x: window.innerWidth - 320, y: window.innerHeight - 420 });
    const [isDragging, setIsDragging] = useState(false);
    const dragOffset = useRef({ x: 0, y: 0 });

    const stats = useMemo(() => {
        const data = {};
        
        // Initialize for all staff
        allStaff.forEach(staff => {
            const quota = quotas.find(q => q.staff_id === staff.id) || { max_days: 0, max_weekends: 0 };
            data[staff.id] = {
                name: staff.full_name,
                quotaDays: quota.max_days,
                quotaWeekends: quota.max_weekends,
                assignedDays: 0,
                assignedWeekends: 0
            };
        });

        // Calculate usage from schedule
        const staffWorkDays = {}; // staffId -> Set(dateStr)
        const openSlotsByBeamline = {}; // beamlineId -> count
        allBeamlines.forEach(b => openSlotsByBeamline[b.id] = 0);

        // Lookup map for day types
        const dayTypeMap = allDayTypes.reduce((acc, dt) => {
            acc[dt.id] = dt;
            return acc;
        }, {});

        schedule.forEach(day => {
            // Count Staff Usage
            if (day.assigned_staff_id) {
                const sid = day.assigned_staff_id;
                if (!staffWorkDays[sid]) staffWorkDays[sid] = new Set();
                
                staffWorkDays[sid].add(day.date);
            } 
            // Count Open Slots
            else {
                const dt = dayTypeMap[day.day_type_id];
                if (dt && dt.requires_staff) {
                    if (openSlotsByBeamline[day.beamline_id] !== undefined) {
                        openSlotsByBeamline[day.beamline_id]++;
                    }
                }
            }
        });

        // Aggregate Staff Stats
        Object.keys(staffWorkDays).forEach(sid => {
            const days = Array.from(staffWorkDays[sid]);
            if (data[sid]) {
                data[sid].assignedDays = days.length;
                data[sid].assignedWeekends = days.filter(d => {
                    const [y, m, dayNum] = d.split('-').map(Number);
                    const localDate = new Date(y, m - 1, dayNum);
                    const w = localDate.getDay();
                    return w === 0 || w === 6;
                }).length;
            }
        });

        // Format Open Slots for display
        const openStats = allBeamlines.map(b => ({
            name: b.name,
            count: openSlotsByBeamline[b.id] || 0
        }));

        return {
            staffStats: Object.values(data),
            openStats: openStats,
            totalOpen: openStats.reduce((sum, item) => sum + item.count, 0)
        };
    }, [schedule, allStaff, quotas, allBeamlines, allDayTypes]);

    // Dragging Logic
    const handleMouseDown = (e) => {
        setIsDragging(true);
        dragOffset.current = {
            x: e.clientX - position.x,
            y: e.clientY - position.y
        };
    };

    useEffect(() => {
        const handleMouseMove = (e) => {
            if (isDragging) {
                setPosition({
                    x: e.clientX - dragOffset.current.x,
                    y: e.clientY - dragOffset.current.y
                });
            }
        };

        const handleMouseUp = () => {
            setIsDragging(false);
        };

        if (isDragging) {
            window.addEventListener('mousemove', handleMouseMove);
            window.addEventListener('mouseup', handleMouseUp);
        }

        return () => {
            window.removeEventListener('mousemove', handleMouseMove);
            window.removeEventListener('mouseup', handleMouseUp);
        };
    }, [isDragging]);

    return (
        <div 
            className={`staff-stats-window ${isMinimized ? 'minimized' : ''}`}
            style={{ 
                left: `${position.x}px`, 
                top: `${position.y}px`,
                // Remove bottom/right if they conflict, but CSS 'fixed' usually needs explicit handling.
                // We'll override CSS bottom/right with 'auto' in style or CSS
                bottom: 'auto',
                right: 'auto'
            }}
        >
            <div 
                className="stats-header" 
                onMouseDown={handleMouseDown}
                style={{ cursor: isDragging ? 'grabbing' : 'grab', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}
            >
                <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
                    <GripHorizontal size={14} color="#888" />
                    <span>Staff Assignments</span>
                </div>
                <button 
                    className="icon-btn-small" 
                    onClick={(e) => { e.stopPropagation(); setIsMinimized(!isMinimized); }}
                >
                    {isMinimized ? <Square size={14} /> : <Minus size={14} />}
                </button>
            </div>
            
            {!isMinimized && (
                <>
                    <div className="stats-list">
                        {stats.staffStats.map((s, i) => (
                            <div key={i} className="stats-row">
                                <div className="stats-name">{s.name}</div>
                                <div className="stats-metrics">
                                    <span title="Total Days">
                                        {s.assignedDays} / {s.quotaDays} D
                                    </span>
                                    <span className="separator">|</span>
                                    <span title="Weekends">
                                        {s.assignedWeekends} / {s.quotaWeekends} W
                                    </span>
                                </div>
                            </div>
                        ))}
                    </div>
                    
                    {(stats.totalOpen > 0 || allBeamlines.length > 0) && (
                        <div className="open-stats-section" style={{ marginTop: '10px', borderTop: '1px solid #ddd', paddingTop: '5px' }}>
                            <div className="stats-header-sub" style={{ fontSize: '0.9rem', marginBottom: '5px', padding: '0 10px', fontWeight: 'bold', color: '#555' }}>
                                Open Shifts (Total: {stats.totalOpen})
                            </div>
                            {stats.openStats.map((item, i) => (
                                <div key={i} className="stats-row" style={{ color: '#d35400' }}>
                                    <div className="stats-name">{item.name}</div>
                                    <div className="stats-metrics">
                                        {item.count}
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </>
            )}
        </div>
    );
};

export default StaffStatsWindow;
