import React, { useState, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import axios from 'axios';
import { Lock, User } from 'lucide-react';
import './Login.css';

const Login = () => {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [error, setError] = useState('');
    const [structure, setStructure] = useState(null);
    const navigate = useNavigate();
    const [searchParams] = useSearchParams();
    const expired = searchParams.get('expired');

    useEffect(() => {
        const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        axios.get(`${API_URL}/api/random_structure`)
            .then(res => { if (res.data.pdb_id) setStructure(res.data); })
            .catch(() => {});
    }, []);

    const handleLogin = async (e) => {
        e.preventDefault();
        setError('');
        try {
            const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const response = await axios.post(`${API_URL}/login`, {
                username,
                password
            }, { withCredentials: true });

            if (response.data.success) {
                // Token is set as HttpOnly cookie by the backend
                localStorage.setItem('user', username);
                localStorage.setItem('is_admin', response.data.is_admin);
                if (response.data.beamline) localStorage.setItem('beamline', response.data.beamline);
                if (response.data.groups) localStorage.setItem('groups', JSON.stringify(response.data.groups));
                if (response.data.full_name) localStorage.setItem('full_name', response.data.full_name);
                navigate('/dashboard');
            }
        } catch (err) {
            if (err.response && err.response.status === 429) {
                setError('Too many login attempts. Please wait a moment.');
            } else {
                setError('Invalid credentials');
            }
        }
    };

    return (
        <div className="login-container">
            {structure && (
                <>
                    <img
                        className="login-bg-structure"
                        src={structure.image_url}
                        alt=""
                        draggable={false}
                        onError={() => setStructure(null)}
                    />
                    <a
                        className="login-rcsb-link"
                        href={structure.rcsb_url}
                        target="_blank"
                        rel="noopener noreferrer"
                    >{structure.pdb_id}</a>
                </>
            )}
            <div className="login-card">
                <div className="login-hero">
                    <img className="login-logo" src={`${import.meta.env.BASE_URL}gmca-logo.png`} alt="GM/CA @ APS" />
                    <p className="login-facility-full">
                        The National Institute of General Medical Sciences and National Cancer Institute
                        Structural Biology Facility at the Advanced Photon Source
                    </p>
                    <div className="login-tagline">GM/CA DATA PORTAL</div>
                </div>
                <p className="login-subtitle" style={{marginTop: '1.5rem'}}>Access your datasets, processing results, and beamline tools</p>

                <form onSubmit={handleLogin}>
                    <div className="input-group">
                        <User className="input-icon" size={20} />
                        <input
                            type="text"
                            placeholder="Username"
                            value={username}
                            onChange={(e) => { setUsername(e.target.value); setError(''); }}
                        />
                    </div>
                    <div className="input-group">
                        <Lock className="input-icon" size={20} />
                        <input
                            type="password"
                            placeholder="Password"
                            value={password}
                            onChange={(e) => { setPassword(e.target.value); setError(''); }}
                        />
                    </div>

                    {expired && <div className="error-message">Session expired. Please sign in again.</div>}
                    {error && <div className="error-message">{error}</div>}

                    <button type="submit" className="login-btn">Sign In</button>
                </form>
                <p className="login-footer">Use your facility credentials to log in</p>
            </div>
            <footer className="login-site-footer">
                <div className="footer-sponsors">
                    Funded by <a href="https://www.nigms.nih.gov/" target="_blank" rel="noopener noreferrer">NIGMS</a> and <a href="https://www.cancer.gov/" target="_blank" rel="noopener noreferrer">NCI</a> of
                    the <a href="https://www.nih.gov/" target="_blank" rel="noopener noreferrer">National Institutes of Health</a>
                </div>
                <div className="footer-ops">
                    <a href="https://www.anl.gov/" target="_blank" rel="noopener noreferrer">Argonne National Laboratory</a>
                    <span className="footer-sep">&middot;</span>
                    <a href="https://www.energy.gov/" target="_blank" rel="noopener noreferrer">U.S. Department of Energy</a>
                    <span className="footer-sep">&middot;</span>
                    <a href="https://www.uchicagoargonnellc.org/" target="_blank" rel="noopener noreferrer">UChicago Argonne LLC</a>
                </div>
                <div className="footer-links">
                    <a href="https://www.anl.gov/privacy-security-notice" target="_blank" rel="noopener noreferrer">Privacy &amp; Security</a>
                    <span className="footer-sep">&middot;</span>
                    <a href="https://www.gmca.aps.anl.gov/contacts.html" target="_blank" rel="noopener noreferrer">Contact Us</a>
                </div>
            </footer>
        </div>
    );
};

export default Login;
