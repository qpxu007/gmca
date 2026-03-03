import React, { useState, useEffect } from 'react';
import Modal from 'react-modal';
import './SaveAsModal.css'; // Reuse styles

Modal.setAppElement('#root');

const DayEditorModal = ({ isOpen, onClose, dayData, allDayTypes, allStaff, onSave }) => {
    const [dayTypeId, setDayTypeId] = useState(dayData?.day_type_id || "");
    const [staffId, setStaffId] = useState(dayData?.assigned_staff_id || "");
    
    // Shift Details (assuming Shift 1 for now)
    const [esafId, setEsafId] = useState("");
    const [piName, setPiName] = useState("");
    const [projectId, setProjectId] = useState("");
    const [description, setDescription] = useState("");

    useEffect(() => {
        if (dayData) {
            setDayTypeId(dayData.day_type_id);
            setStaffId(dayData.assigned_staff_id || "");
            
            // Populate Shift 1 data if available
            const shift1 = dayData.shifts?.find(s => s.shift_index === 1);
            if (shift1) {
                setEsafId(shift1.esaf_id || "");
                setPiName(shift1.pi_name || "");
                setProjectId(shift1.project_id || "");
                setDescription(shift1.description || "");
            } else {
                setEsafId("");
                setPiName("");
                setProjectId("");
                setDescription("");
            }
        }
    }, [dayData]);

    const handleSave = () => {
        onSave({
            day_id: dayData.id,
            day_type_id: parseInt(dayTypeId),
            assigned_staff_id: staffId ? parseInt(staffId) : null,
            shifts: [
                {
                    shift_index: 1,
                    esaf_id: esafId,
                    pi_name: piName,
                    project_id: projectId,
                    description: description
                }
            ]
        });
    };

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
                    {allStaff.map(st => (
                        <option key={st.id} value={st.id}>{st.full_name}</option>
                    ))}
                </select>
            </div>

            <hr style={{ margin: '20px 0', border: '0', borderTop: '1px solid #eee' }} />
            <h3>Shift Details</h3>
            
            <div className="form-group">
                <label>ESAF ID:</label>
                <input 
                    type="text" 
                    value={esafId} 
                    onChange={(e) => setEsafId(e.target.value)}
                    placeholder="e.g. 12345"
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
                />
            </div>

            <div className="form-group">
                <label>PI Name:</label>
                <input 
                    type="text" 
                    value={piName} 
                    onChange={(e) => setPiName(e.target.value)}
                    placeholder="Principal Investigator"
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
                />
            </div>

            <div className="form-group">
                <label>Project ID:</label>
                <input 
                    type="text" 
                    value={projectId} 
                    onChange={(e) => setProjectId(e.target.value)}
                    placeholder="Project Code"
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
                />
            </div>

            <div className="form-group">
                <label>Description:</label>
                <textarea 
                    value={description} 
                    onChange={(e) => setDescription(e.target.value)}
                    placeholder="Experiment Description"
                    style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ccc', minHeight: '60px' }}
                />
            </div>

            <div className="modal-actions">
                <button onClick={onClose} className="cancel-btn">Cancel</button>
                <button onClick={handleSave} className="save-btn">Save</button>
            </div>
        </Modal>
    );
};

export default DayEditorModal;