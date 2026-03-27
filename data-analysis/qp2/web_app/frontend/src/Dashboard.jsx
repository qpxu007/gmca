import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Grid, FileSpreadsheet, PlusCircle, LogOut, Calendar, Database, Activity, MessageCircle } from 'lucide-react';
import './Dashboard.css'; // We will create this

const Dashboard = () => {
    const navigate = useNavigate();
    const user = localStorage.getItem('user') || 'User';

    const apps = [
        {
            id: 'spreadsheet',
            name: 'Spreadsheet Editor',
            description: 'Manage and edit puck spreadsheet data.',
            icon: <FileSpreadsheet size={48} color="#2ecc71" />,
            route: '/spreadsheet'
        },
        {
            id: 'scheduler',
            name: 'Beamtime Scheduler',
            description: 'Schedule beamtime and assign staff.',
            icon: <Calendar size={48} color="#3498db" />,
            route: '/scheduler'
        },
        {
            id: 'datasets',
            name: 'Dataset Viewer',
            description: 'View and search dataset runs.',
            icon: <Database size={48} color="#9b59b6" />,
            route: '/datasets'
        },
        {
            id: 'processing',
            name: 'Processing',
            description: 'Monitor data processing pipelines.',
            icon: <Activity size={48} color="#e67e22" />,
            route: '/processing'
        },
        {
            id: 'chat',
            name: 'AI Chat',
            description: 'Chat with the AI assistant and your team.',
            icon: <MessageCircle size={48} color="#e74c3c" />,
            route: '/chat'
        },
        // Future apps can be added here
        {
            id: 'upcoming',
            name: 'Coming Soon',
            description: 'More tools are being built.',
            icon: <PlusCircle size={48} color="#bdc3c7" />,
            route: '#'
        }
    ];

    const handleLogout = () => {
        localStorage.removeItem('token');
        localStorage.removeItem('user');
        navigate('/login');
    };

    return (
        <div className="dashboard-container">
            <header className="dashboard-header">
                <div className="header-left">
                    <Grid size={24} />
                    <h1>GMCA Web Apps</h1>
                </div>
                <div className="header-right">
                    <span>Hello, {user}</span>
                    <button onClick={handleLogout} className="logout-btn" title="Logout">
                        <LogOut size={20} />
                    </button>
                </div>
            </header>

            <main className="dashboard-main">
                <h2>Available Applications</h2>
                <div className="apps-grid">
                    {apps.map(app => (
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
