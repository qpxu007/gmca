import React, { useState } from 'react';
import Modal from 'react-modal';
import './SaveAsModal.css'; // Reuse styles

Modal.setAppElement('#root');

const shiftHasData = (s) => s.esaf_id || s.pi_name || s.project_id || s.description;

const ShiftFields = ({ shiftIndex, shiftState, onChange }) => (
    <>
        <div className="form-group">
            <label>ESAF ID:</label>
            <input
                type="text"
                value={shiftState.esaf_id}
                onChange={(e) => onChange(shiftIndex, 'esaf_id', e.target.value)}
                placeholder="e.g. 12345"
                style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
            />
        </div>
        <div className="form-group">
            <label>PI Name:</label>
            <input
                type="text"
                value={shiftState.pi_name}
                onChange={(e) => onChange(shiftIndex, 'pi_name', e.target.value)}
                placeholder="Principal Investigator"
                style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
            />
        </div>
        <div className="form-group">
            <label>Project ID:</label>
            <input
                type="text"
                value={shiftState.project_id}
                onChange={(e) => onChange(shiftIndex, 'project_id', e.target.value)}
                placeholder="Project Code"
                style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
            />
        </div>
        <div className="form-group">
            <label>Description:</label>
            <textarea
                value={shiftState.description}
                onChange={(e) => onChange(shiftIndex, 'description', e.target.value)}
                placeholder="Experiment Description"
                style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc', minHeight: '60px' }}
            />
        </div>
    </>
);

const DayEditorModal = ({ isOpen, onClose, dayData, allDayTypes, allStaff, onSave }) => {
    const shift1Init = dayData?.shifts?.find(s => s.shift_index === 1) || {};
    const shift2Init = dayData?.shifts?.find(s => s.shift_index === 2) || {};

    const [dayTypeId, setDayTypeId] = useState(dayData?.day_type_id || "");
    const [staffId, setStaffId] = useState(dayData?.assigned_staff_id || "");
    const [computingStaffId, setComputingStaffId] = useState(dayData?.assigned_computing_staff_id || "");
    const [activeShift, setActiveShift] = useState(1);

    const [shiftsState, setShiftsState] = useState({
        1: { esaf_id: shift1Init.esaf_id || "", pi_name: shift1Init.pi_name || "", project_id: shift1Init.project_id || "", description: shift1Init.description || "" },
        2: { esaf_id: shift2Init.esaf_id || "", pi_name: shift2Init.pi_name || "", project_id: shift2Init.project_id || "", description: shift2Init.description || "" }
    });

    const handleShiftChange = (index, field, value) => {
        setShiftsState(prev => ({ ...prev, [index]: { ...prev[index], [field]: value } }));
    };

    const handleSave = () => {
        onSave({
            day_id: dayData.id,
            day_type_id: parseInt(dayTypeId),
            assigned_staff_id: staffId ? parseInt(staffId) : null,
            assigned_computing_staff_id: computingStaffId ? parseInt(computingStaffId) : null,
            shifts: [
                { shift_index: 1, ...shiftsState[1] },
                { shift_index: 2, ...shiftsState[2] }
            ]
        });
    };

    const tabStyle = (active) => ({
        background: 'none',
        border: 'none',
        borderBottom: active ? '2px solid #1890ff' : '2px solid transparent',
        padding: '6px 16px',
        cursor: 'pointer',
        fontSize: '0.95rem',
        fontWeight: active ? 600 : 400,
        color: active ? '#1890ff' : '#666',
    });

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Edit Day"
            className="modal-content"
            overlayClassName="modal-overlay"
        >
            <h2>Edit Schedule Day</h2>
            <p><strong>Date:</strong> {dayData?.date}</p>
            <p><strong>Beamline:</strong> {dayData?.beamline_name}</p>

            <div className="form-group">
                <label>Day Type:</label>
                <select
                    value={dayTypeId}
                    onChange={(e) => setDayTypeId(e.target.value)}
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
                >
                    {allDayTypes.map(dt => (
                        <option key={dt.id} value={dt.id}>{dt.name}</option>
                    ))}
                </select>
            </div>

            <div className="form-group">
                <label>Assigned Staff:</label>
                <select
                    value={staffId}
                    onChange={(e) => setStaffId(e.target.value)}
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
                >
                    <option value="">-- None --</option>
                    {allStaff.filter(st => st.is_host !== false).map(st => (
                        <option key={st.id} value={st.id}>{st.full_name}</option>
                    ))}
                </select>
            </div>

            <div className="form-group">
                <label>Computing Staff (Applies to all beamlines for this date):</label>
                <select
                    value={computingStaffId}
                    onChange={(e) => setComputingStaffId(e.target.value)}
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
                >
                    <option value="">-- None --</option>
                    {allStaff.filter(st => st.is_computing).map(st => (
                        <option key={st.id} value={st.id}>{st.full_name}</option>
                    ))}
                </select>
            </div>

            <hr style={{ margin: '16px 0 0', border: '0', borderTop: '1px solid #eee' }} />

            <div style={{ display: 'flex', borderBottom: '1px solid #eee', marginBottom: '16px' }}>
                <button style={tabStyle(activeShift === 1)} onClick={() => setActiveShift(1)}>
                    Shift 1
                </button>
                <button style={tabStyle(activeShift === 2)} onClick={() => setActiveShift(2)}>
                    Shift 2{shiftHasData(shiftsState[2]) ? ' •' : ''}
                </button>
            </div>

            <ShiftFields
                shiftIndex={activeShift}
                shiftState={shiftsState[activeShift]}
                onChange={handleShiftChange}
            />

            <div className="modal-actions">
                <button onClick={onClose} className="cancel-btn">Cancel</button>
                <button onClick={handleSave} className="save-btn">Save</button>
            </div>
        </Modal>
    );
};

export default DayEditorModal;