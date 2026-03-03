import React, { useState, useEffect } from 'react';
import { api } from './api';
import { Trash2, Plus } from 'lucide-react';

const RunManager = () => {
    const [runs, setRuns] = useState([]);
    const [newRun, setNewRun] = useState({ name: '', start_date: '', end_date: '' });

    useEffect(() => {
        fetchRuns();
    }, []);

    const fetchRuns = async () => {
        try {
            const data = await api.listRuns();
            setRuns(data);
        } catch (e) {
            console.error("Failed to fetch runs", e);
        }
    };

    const handleAdd = async (e) => {
        e.preventDefault();
        try {
            await api.createRun(newRun);
            setNewRun({ name: '', start_date: '', end_date: '' });
            fetchRuns();
        } catch (err) {
            alert("Failed to add run: " + err.message);
        }
    };

    const handleDelete = async (id) => {
        if (window.confirm("Delete this run?")) {
            try {
                await api.deleteRun(id);
                fetchRuns();
            } catch (err) {
                console.error("Delete failed:", err);
                const msg = err.response?.data?.detail || err.message;
                alert("Failed to delete run: " + msg);
            }
        }
    };

    return (
        <div className="manager-container">
            <h3>Manage Runs</h3>
            <ul className="list-group">
                {runs.map(run => (
                    <li key={run.id} className="list-item">
                        <span>{run.name} ({run.start_date} to {run.end_date})</span>
                        <button onClick={() => handleDelete(run.id)} className="icon-btn delete-btn">
                            <Trash2 size={16} />
                        </button>
                    </li>
                ))}
            </ul>
            
            <form onSubmit={handleAdd} className="add-form">
                <input 
                    type="text" placeholder="Name (e.g. 2025-2)" 
                    value={newRun.name} onChange={e => setNewRun({...newRun, name: e.target.value})} required 
                />
                <input 
                    type="date" 
                    value={newRun.start_date} onChange={e => setNewRun({...newRun, start_date: e.target.value})} required 
                />
                <input 
                    type="date" 
                    value={newRun.end_date} onChange={e => setNewRun({...newRun, end_date: e.target.value})} required 
                />
                <button type="submit" className="add-btn"><Plus size={16}/> Add</button>
            </form>
        </div>
    );
};

export default RunManager;
