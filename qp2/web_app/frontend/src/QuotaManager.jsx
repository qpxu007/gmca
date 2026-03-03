import React, { useState, useEffect } from 'react';
import { api } from './api';
import { Save } from 'lucide-react';

const QuotaManager = () => {
    const [runs, setRuns] = useState([]);
    const [staff, setStaff] = useState([]);
    const [selectedRunId, setSelectedRunId] = useState(null);
    const [quotas, setQuotas] = useState({}); // Map of staffId -> quota object

    useEffect(() => {
        const fetchData = async () => {
            try {
                const [runsData, staffData] = await Promise.all([api.listRuns(), api.listStaff()]);
                setRuns(runsData);
                setStaff(staffData);
                if (runsData.length > 0) setSelectedRunId(runsData[0].id);
            } catch (e) {
                console.error("Failed to fetch initial data", e);
            }
        };
        fetchData();
    }, []);

    useEffect(() => {
        if (selectedRunId) {
            fetchQuotas(selectedRunId);
        }
    }, [selectedRunId]);

    const fetchQuotas = async (runId) => {
        try {
            const data = await api.listQuotas(runId);
            // Convert list to map for easier lookup by staff_id
            const quotaMap = {};
            data.forEach(q => quotaMap[q.staff_id] = q);
            setQuotas(quotaMap);
        } catch (e) {
            console.error("Failed to fetch quotas", e);
        }
    };

    const updateLocalQuota = (staffId, field, value) => {
        setQuotas(prev => ({
            ...prev,
            [staffId]: {
                ...prev[staffId],
                [field]: parseInt(value) || 0,
                staff_id: staffId, 
                run_id: selectedRunId
            }
        }));
    };

    const saveQuota = async (staffId) => {
        const quotaData = quotas[staffId];
        // If no data edited yet, create default
        const payload = {
            staff_id: staffId,
            run_id: selectedRunId,
            max_days: quotaData?.max_days || 0,
            max_weekends: quotaData?.max_weekends || 0
        };
        
        try {
            await api.updateQuota(payload);
            // Optional: visual feedback
        } catch (e) {
            alert("Failed to save quota");
        }
    };

    return (
        <div className="manager-container">
            <h3>Manage Staff Quotas</h3>
            <div style={{ marginBottom: '1rem' }}>
                <label>Select Run: </label>
                <select 
                    value={selectedRunId || ""} 
                    onChange={(e) => setSelectedRunId(parseInt(e.target.value))}
                    style={{ padding: '6px', borderRadius: '4px', border: '1px solid #ccc' }}
                >
                    {runs.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
                </select>
            </div>

            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                    <tr style={{ borderBottom: '1px solid #eee', textAlign: 'left', backgroundColor: '#f9f9f9' }}>
                        <th style={{ padding: '10px' }}>Staff</th>
                        <th style={{ padding: '10px' }}>Max Days</th>
                        <th style={{ padding: '10px' }}>Max Weekends</th>
                        <th style={{ padding: '10px' }}>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {staff.map(s => {
                        const q = quotas[s.id] || { max_days: 0, max_weekends: 0 };
                        return (
                            <tr key={s.id} style={{ borderBottom: '1px solid #f9f9f9' }}>
                                <td style={{ padding: '10px' }}>{s.full_name}</td>
                                <td style={{ padding: '10px' }}>
                                    <input 
                                        type="number" 
                                        value={q.max_days} 
                                        onChange={(e) => updateLocalQuota(s.id, 'max_days', e.target.value)}
                                        style={{ width: '80px', padding: '6px', border: '1px solid #ddd', borderRadius: '4px' }}
                                    />
                                </td>
                                <td style={{ padding: '10px' }}>
                                    <input 
                                        type="number" 
                                        value={q.max_weekends} 
                                        onChange={(e) => updateLocalQuota(s.id, 'max_weekends', e.target.value)}
                                        style={{ width: '80px', padding: '6px', border: '1px solid #ddd', borderRadius: '4px' }}
                                    />
                                </td>
                                <td style={{ padding: '10px' }}>
                                    <button 
                                        onClick={() => saveQuota(s.id)} 
                                        className="icon-btn" 
                                        title="Save"
                                        style={{ color: '#27ae60', background: 'none', border: 'none', cursor: 'pointer' }}
                                    >
                                        <Save size={20} />
                                    </button>
                                </td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </div>
    );
};

export default QuotaManager;
