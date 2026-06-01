import React, { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { api } from './api';
import { Grid, FileSpreadsheet, PlusCircle, LogOut, Calendar, Database, Activity, MessageCircle, Eye, ClipboardList, Box, BarChart3, Radio, Archive, Map } from 'lucide-react';
import './Dashboard.css'; // We will create this

const Dashboard = () => {
    const navigate = useNavigate();
    const user = localStorage.getItem('user') || 'User';
    const isAdmin = localStorage.getItem('is_admin') === 'true';
    const fullName = localStorage.getItem('full_name');
    const displayName = fullName ? fullName.split(' ')[0] : user;

    useEffect(() => {
        api.verifySession().catch(() => {});
    }, []);

    const apps = [
        // ── Pre-experiment ───────────────────────────────────────────────────
        {
            id: 'experiment',
            name: 'Experiment Prep',
            description: 'Submit spreadsheets, files, IPs, and shipping info for your experiment.',
            icon: <ClipboardList size={48} color="#f39c12" />,
            route: '/experiment'
        },
        {
            id: 'spreadsheet',
            name: 'Spreadsheet Editor',
            description: 'Manage and edit puck spreadsheet data.',
            icon: <FileSpreadsheet size={48} color="#2ecc71" />,
            route: '/spreadsheet'
        },
        // ── During collection ────────────────────────────────────────────────
        {
            id: 'live',
            name: 'Live Viewer',
            description: 'Follow data collection in real time as frames arrive from the detector.',
            icon: <Radio size={48} color="#e74c3c" />,
            route: '/live'
        },
        {
            id: 'viewer',
            name: 'Image Viewer',
            description: 'View diffraction images with contrast, zoom, and resolution rings.',
            icon: <Eye size={48} color="#1abc9c" />,
            route: '/viewer'
        },
        // ── Post-collection ──────────────────────────────────────────────────
        {
            id: 'datasets',
            name: 'Dataset Viewer',
            description: 'View and search dataset runs.',
            icon: <Database size={48} color="#9b59b6" />,
            route: '/datasets'
        },
        {
            id: 'processing',
            name: 'Data Processing',
            description: 'Monitor and reprocess data processing pipelines.',
            icon: <Activity size={48} color="#e67e22" />,
            route: '/processing'
        },
        // ── Analysis ─────────────────────────────────────────────────────────
        {
            id: 'models',
            name: 'Structure Models',
            description: 'Upload, predict, and view 3D structure models.',
            icon: <Box size={48} color="#8e44ad" />,
            route: '/models'
        },
        {
            id: 'chat',
            name: 'Chat',
            description: 'Chat with the AI assistant and your team.',
            icon: <MessageCircle size={48} color="#e74c3c" />,
            route: '/chat'
        },
        // ── Staff only ───────────────────────────────────────────────────────
        {
            id: 'scheduler',
            name: 'BL Support Scheduler',
            description: 'Schedule beamtime and assign staff.',
            icon: <Calendar size={48} color="#3498db" />,
            route: '/scheduler',
            staffOnly: true
        },
        {
            id: 'rcsb',
            name: 'PDB Reports',
            description: 'Search RCSB PDB and generate beamline reports.',
            icon: <BarChart3 size={48} color="#16a085" />,
            route: '/rcsb',
            staffOnly: true
        },
        {
            name: 'APS Archive',
            description: 'APS data archive status',
            icon: <Archive size={48} color="#16a085" />,
            route: '/archive',
        },
        {
            id: 'distribution',
            name: 'User Distribution',
            description: 'Generate user distribution maps from spreadsheets.',
            icon: <Map size={48} color="#2980b9" />,
            route: '/distribution',
            staffOnly: true
        },
        {
            id: 'upcoming',
            name: 'Coming Soon',
            description: 'More tools are being built.',
            icon: <PlusCircle size={48} color="#bdc3c7" />,
            route: '#'
        }
    ];

    const handleLogout = async () => {
        try {
            const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            await axios.post(`${API_URL}/logout`, {}, { withCredentials: true });
        } catch {
            // Best effort — clear local state even if request fails
        }
        localStorage.removeItem('user');
        localStorage.removeItem('beamline');
        localStorage.removeItem('is_admin');
        localStorage.removeItem('groups');
        localStorage.removeItem('full_name');
        navigate('/login');
    };

    return (
        <div className="dashboard-container">
            <header className="dashboard-header">
                <div className="header-left">
                    <Grid size={20} />
                    <h1>GM/CA Data Portal</h1>
                </div>
                <div className="header-right">
                    <span>Hello, {displayName}</span>
                    <button onClick={handleLogout} className="logout-btn" title="Logout">
                        <LogOut size={20} />
                    </button>
                </div>
            </header>

            <main className="dashboard-main">
                <h2>Available Applications</h2>
                <div className="apps-grid">
                    {apps.filter(app => !app.staffOnly || isAdmin).map(app => (
                        <div
                            key={app.id}
                            className={`app-card ${app.route === '#' ? 'disabled' : ''}`}
                            onClick={() => app.route !== '#' && navigate(app.route)}
                        >
                            <div className="app-icon">
                                {app.icon}
                            </div>
                            <div className="app-info">
                                <h3>{app.name}</h3>
                                <p>{app.description}</p>
                            </div>
                        </div>
                    ))}
                </div>
            </main>
        </div>
    );
};

export default Dashboard;
