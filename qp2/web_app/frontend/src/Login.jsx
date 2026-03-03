import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { Lock, User } from 'lucide-react';
import './Login.css'; // We will create this

const Login = () => {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [error, setError] = useState('');
    const navigate = useNavigate();

    const handleLogin = async (e) => {
        e.preventDefault();
        setError('');
        try {
            const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const response = await axios.post(`${API_URL}/login`, {
                username,
                password
            });

            if (response.data.success) {
                localStorage.setItem('token', response.data.token);
                localStorage.setItem('user', username);
                localStorage.setItem('is_admin', response.data.is_admin);
                navigate('/dashboard');
            }
        } catch (err) {
            setError('Invalid credentials');
        }
    };

    return (
        <div className="login-container">
            <div className="login-card">
                <h2>Welcome Back</h2>
                <p className="login-subtitle">Please sign in to continue</p>
                
                <form onSubmit={handleLogin}>
                    <div className="input-group">
                        <User className="input-icon" size={20} />
                        <input 
                            type="text" 
                            placeholder="Username" 
                            value={username}
                            onChange={(e) => setUsername(e.target.value)}
                        />
                    </div>
                    <div className="input-group">
                        <Lock className="input-icon" size={20} />
                        <input 
                            type="password" 
                            placeholder="Password" 
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                        />
                    </div>
                    
                    {error && <div className="error-message">{error}</div>}
                    
                    <button type="submit" className="login-btn">Sign In</button>
                </form>
            </div>
        </div>
    );
};

export default Login;
