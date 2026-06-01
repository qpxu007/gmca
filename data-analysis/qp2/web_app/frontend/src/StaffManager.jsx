import React, { useState, useEffect } from 'react';
import { api } from './api';
import { Trash2, Plus, Edit2, X, Save } from 'lucide-react';

const StaffManager = () => {
    const [staff, setStaff] = useState([]);
    const [newStaff, setNewStaff] = useState({ username: '', full_name: '', email: '', is_host: true, is_computing: false });
    const [editingStaffId, setEditingStaffId] = useState(null);
    const [editForm, setEditForm] = useState(null);

    const fetchStaff = async () => {
        try {
            const data = await api.listStaff();
            setStaff(data);
        } catch (e) {
            console.error("Failed to fetch staff", e);
        }
    };

    useEffect(() => {
        const doFetch = async () => {
            try {
                const data = await api.listStaff();
                setStaff(data);
            } catch (e) {
                console.error("Failed to fetch staff", e);
            }
        };
        doFetch();
    }, []);

    const handleAdd = async (e) => {
        e.preventDefault();
        try {
            await api.createStaff(newStaff);
            setNewStaff({ username: '', full_name: '', email: '', is_host: true, is_computing: false });
            fetchStaff();
        } catch (err) {
            alert("Failed to add staff: " + err.message);
        }
    };

    const handleEditClick = (staffObj) => {
        setEditingStaffId(staffObj.id);
        setEditForm({ ...staffObj });
    };

    const handleSaveEdit = async () => {
        try {
            await api.updateStaff(editForm);
            setEditingStaffId(null);
            fetchStaff();
        } catch (err) {
            alert("Failed to update staff: " + err.message);
        }
    };

    const handleCancelEdit = () => {
        setEditingStaffId(null);
    };

    const handleDelete = async (id) => {
        try {
            await api.deleteStaff(id);
            fetchStaff();
        } catch (err) {
            console.error("Delete failed:", err);
            const msg = err.response?.data?.detail || err.message;
            alert("Failed to delete staff: " + msg);
        }
    };

    return (
        <div className="manager-container">
            <h3>Manage Staff</h3>
            <ul className="list-group">
                {staff.map(s => (
                    <li key={s.id} className="list-item">
                        {editingStaffId === s.id ? (
                            <div style={{display: 'flex', gap: '10px', width: '100%', alignItems: 'center'}}>
                                <input type="text" value={editForm.username} onChange={e => setEditForm({...editForm, username: e.target.value})} style={{width: '80px', padding: '4px', border: '1px solid #ccc', borderRadius: '4px'}} />
                                <input type="text" value={editForm.full_name} onChange={e => setEditForm({...editForm, full_name: e.target.value})} style={{flex: 1, padding: '4px', border: '1px solid #ccc', borderRadius: '4px'}} />
                                <input type="email" value={editForm.email} onChange={e => setEditForm({...editForm, email: e.target.value})} style={{flex: 1, padding: '4px', border: '1px solid #ccc', borderRadius: '4px'}} />
                                <label style={{display: 'flex', alignItems: 'center', gap: '5px', whiteSpace: 'nowrap', fontSize: '0.9em'}}>
                                    <input type="checkbox" checked={editForm.is_host} onChange={e => setEditForm({...editForm, is_host: e.target.checked})} />
                                    Host
                                </label>
                                <label style={{display: 'flex', alignItems: 'center', gap: '5px', whiteSpace: 'nowrap', fontSize: '0.9em'}}>
                                    <input type="checkbox" checked={editForm.is_computing} onChange={e => setEditForm({...editForm, is_computing: e.target.checked})} />
                                    Comp.
                                </label>
                                <div style={{display: 'flex', gap: '5px'}}>
                                    <button onClick={handleSaveEdit} className="icon-btn" style={{color: '#2ecc71'}} title="Save"><Save size={16} /></button>
                                    <button onClick={handleCancelEdit} className="icon-btn" style={{color: '#e74c3c'}} title="Cancel"><X size={16} /></button>
                                </div>
                            </div>
                        ) : (
                            <>
                                <span>{s.full_name} ({s.username}){s.is_host ? ' [Host]' : ''}{s.is_computing ? ' [Computing]' : ''}</span>
                                <div style={{display: 'flex', gap: '5px'}}>
                                    <button onClick={() => handleEditClick(s)} className="icon-btn" style={{color: '#3498db'}} title="Edit">
                                        <Edit2 size={16} />
                                    </button>
                                    <button onClick={() => handleDelete(s.id)} className="icon-btn delete-btn" title="Delete">
                                        <Trash2 size={16} />
                                    </button>
                                </div>
                            </>
                        )}
                    </li>
                ))}
            </ul>
            
            <form onSubmit={handleAdd} className="add-form">
                <input 
                    type="text" placeholder="Username" 
                    value={newStaff.username} onChange={e => setNewStaff({...newStaff, username: e.target.value})} required 
                />
                <input 
                    type="text" placeholder="Full Name" 
                    value={newStaff.full_name} onChange={e => setNewStaff({...newStaff, full_name: e.target.value})} required 
                />
                <input 
                    type="email" placeholder="Email" 
                    value={newStaff.email} onChange={e => setNewStaff({...newStaff, email: e.target.value})} required 
                />
                <label style={{display: 'flex', alignItems: 'center', gap: '5px', whiteSpace: 'nowrap'}}>
                    <input type="checkbox" checked={newStaff.is_host} onChange={e => setNewStaff({...newStaff, is_host: e.target.checked})} />
                    Host
                </label>
                <label style={{display: 'flex', alignItems: 'center', gap: '5px', whiteSpace: 'nowrap'}}>
                    <input type="checkbox" checked={newStaff.is_computing} onChange={e => setNewStaff({...newStaff, is_computing: e.target.checked})} />
                    Computing
                </label>
                <button type="submit" className="add-btn"><Plus size={16}/> Add</button>
            </form>
        </div>
    );
};

export default StaffManager;
