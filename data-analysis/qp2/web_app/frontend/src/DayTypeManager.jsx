import React, { useState, useEffect } from 'react';
import { api } from './api';
import { Trash2, Plus } from 'lucide-react';

const DayTypeManager = () => {
    const [dayTypes, setDayTypes] = useState([]);
    const [newType, setNewType] = useState({ name: '', color_code: '#000000', requires_staff: true });

    useEffect(() => {
        fetchDayTypes();
    }, []);

    const fetchDayTypes = async () => {
        try {
            const data = await api.listDayTypes();
            setDayTypes(data);
        } catch (e) {
            console.error("Failed to fetch day types", e);
        }
    };

    const handleAdd = async (e) => {
        e.preventDefault();
        try {
            await api.createDayType(newType);
            setNewType({ name: '', color_code: '#000000', requires_staff: true });
            fetchDayTypes();
        } catch (err) {
            alert("Failed to add day type: " + err.message);
        }
    };

    const handleDelete = async (id) => {
        // Debugging: Bypassing confirm to test click and API
        // if (window.confirm("Delete this day type?")) {
            try {
                console.log("Attempting to delete ID:", id);
                await api.deleteDayType(id);
                console.log("Delete success");
                fetchDayTypes();
                // alert("Deleted successfully"); // Optional feedback
            } catch (err) {
                console.error("Delete failed:", err);
                const msg = err.response?.data?.detail || err.message;
                alert("Failed to delete day type: " + msg);
            }
        // }
    };

    return (
        <div className="manager-container">
            <h3>Manage Day Types</h3>
            <ul className="list-group">
                {dayTypes.map(dt => (
                    <li key={dt.id} className="list-item">
                        <span style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <span style={{ width: '20px', height: '20px', backgroundColor: dt.color_code, borderRadius: '4px', border: '1px solid #ccc' }}></span>
                            {dt.name}
                        </span>
                        <button onClick={() => handleDelete(dt.id)} className="icon-btn delete-btn">
                            <Trash2 size={16} />
                        </button>
                    </li>
                ))}
            </ul>
            
            <form onSubmit={handleAdd} className="add-form">
                <input 
                    type="text" placeholder="Name" 
                    value={newType.name} onChange={e => setNewType({...newType, name: e.target.value})} required 
                />
                <input 
                    type="color" 
                    value={newType.color_code} onChange={e => setNewType({...newType, color_code: e.target.value})} required 
                    style={{ width: '50px', padding: '2px', height: '38px' }}
                />
                <label style={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: '0.9em', whiteSpace: 'nowrap' }}>
                    <input 
                        type="checkbox" 
                        checked={newType.requires_staff} 
                        onChange={e => setNewType({...newType, requires_staff: e.target.checked})} 
                    />
                    Req. Staff
                </label>
                <button type="submit" className="add-btn"><Plus size={16}/> Add</button>
            </form>
        </div>
    );
};

export default DayTypeManager;
