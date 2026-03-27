import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Login from './Login';
import Dashboard from './Dashboard';
import SpreadsheetApp from './SpreadsheetApp';
import SchedulerApp from './SchedulerApp';
import DatasetApp from './DatasetApp';
import ProcessingApp from './ProcessingApp';
import ChatApp from './ChatApp';

const ProtectedRoute = ({ children }) => {
    const token = localStorage.getItem('token');
    if (!token) {
        return <Navigate to="/login" replace />;
    }
    return children;
};

function App() {
    return (
        <BrowserRouter>
            <Routes>
                <Route path="/login" element={<Login />} />
                
                <Route path="/dashboard" element={
                    <ProtectedRoute>
                        <Dashboard />
                    </ProtectedRoute>
                } />
                
                <Route path="/spreadsheet" element={
                    <ProtectedRoute>
                        <SpreadsheetApp />
                    </ProtectedRoute>
                } />

                <Route path="/scheduler" element={
                    <ProtectedRoute>
                        <SchedulerApp />
                    </ProtectedRoute>
                } />

                <Route path="/datasets" element={
                    <ProtectedRoute>
                        <DatasetApp />
                    </ProtectedRoute>
                } />

                <Route path="/processing" element={
                    <ProtectedRoute>
                        <ProcessingApp />
                    </ProtectedRoute>
                } />

                <Route path="/chat" element={
                    <ProtectedRoute>
                        <ChatApp />
                    </ProtectedRoute>
                } />

                {/* Default redirect */}
                <Route path="/" element={<Navigate to="/dashboard" replace />} />
            </Routes>
        </BrowserRouter>
    );
}

export default App;
