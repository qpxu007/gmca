import React, { useState, useEffect } from 'react';
import { api } from './api';
import { Trash2, Plus } from 'lucide-react';

const StaffManager = () => {
    const [staff, setStaff] = useState([]);
    const [newStaff, setNewStaff] = useState({ username: '', full_name: '', email: '' });

    useEffect(() => {
        fetchStaff();
    }, []);

    const fetchStaff = async () => {
        try {
            const data = await api.listStaff();
            setStaff(data);
        } catch (e) {
            console.error("Failed to fetch staff", e);
        }
    };

    const handleAdd = async (e) => {
        e.preventDefault();
        try {
            await api.createStaff(newStaff);
            setNewStaff({ username: '', full_name: '', email: '' });
            fetchStaff();
        } catch (err) {
            alert("Failed to add staff: " + err.message);
        }
    };

    const handleDelete = async (id) => {
        if (window.confirm("Delete this staff member?")) {
            try {
                await api.deleteStaff(id);
                fetchStaff();
            } catch (err) {
                console.error("Delete failed:", err);
                const msg = err.response?.data?.detail || err.message;
                alert("Failed to delete staff: " + msg);
            }
        }
    };

    return (
        <div className="manager-container">
            <h3>Manage Staff</h3>
            <ul className="list-group">
                {staff.map(s => (
                    <li key={s.id} className="list-item">
                        <span>{s.full_name} ({s.username})</span>
                        <button onClick={() => handleDelete(s.id)} className="icon-btn delete-btn">
                            <Trash2 size={16} />
                        </button>
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
                <button type="submit" className="add-btn"><Plus size={16}/> Add</button>
            </form>
        </div>
    );
};

export default StaffManager;
