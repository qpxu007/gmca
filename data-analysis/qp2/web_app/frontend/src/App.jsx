import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Login from './Login';
import Dashboard from './Dashboard';
import SpreadsheetApp from './SpreadsheetApp';
import SchedulerApp from './SchedulerApp';
import DatasetApp from './DatasetApp';
import ProcessingApp from './ProcessingApp';
import ChatApp from './ChatApp';
import ImageViewerApp from './ImageViewerApp';
import LiveViewerApp from './LiveViewerApp';
import ExperimentApp from './ExperimentApp';
import ModelViewerApp from './ModelViewerApp';
import RCSBApp from './RCSBApp';
import ArchiveApp from './ArchiveApp';
import DistributionApp from './DistributionApp';

// UX-only gate: localStorage values are user-controlled and provide no security guarantee.
// All real authorization is enforced server-side via JWT verification on every API request.
const ProtectedRoute = ({ children, staffOnly = false }) => {
    const user = localStorage.getItem('user');
    if (!user) {
        return <Navigate to="/login" replace />;
    }
    if (staffOnly && localStorage.getItem('is_admin') !== 'true') {
        return <Navigate to="/dashboard" replace />;
    }
    return children;
};

function App() {
    return (
        <BrowserRouter basename={import.meta.env.BASE_URL}>
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
                    <ProtectedRoute staffOnly>
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

                <Route path="/viewer" element={
                    <ProtectedRoute>
                        <ImageViewerApp />
                    </ProtectedRoute>
                } />

                <Route path="/live" element={
                    <ProtectedRoute>
                        <LiveViewerApp />
                    </ProtectedRoute>
                } />

                <Route path="/experiment" element={
                    <ProtectedRoute>
                        <ExperimentApp />
                    </ProtectedRoute>
                } />

                <Route path="/models" element={
                    <ProtectedRoute>
                        <ModelViewerApp />
                    </ProtectedRoute>
                } />

                <Route path="/rcsb" element={
                    <ProtectedRoute staffOnly>
                        <RCSBApp />
                    </ProtectedRoute>
                } />

                <Route path="/archive" element={
                    <ProtectedRoute>
                        <ArchiveApp />
                    </ProtectedRoute>
                } />

                <Route path="/distribution" element={
                    <ProtectedRoute staffOnly>
                        <DistributionApp />
                    </ProtectedRoute>
                } />

                {/* Default redirect */}
                <Route path="/" element={<Navigate to="/dashboard" replace />} />
            </Routes>
        </BrowserRouter>
    );
}

export default App;
