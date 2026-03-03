import React from 'react';
import { App, H5GroveProvider } from '@h5web/app';
import Modal from 'react-modal';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const H5Viewer = ({ isOpen, onClose, filePath, filename }) => {
    if (!isOpen || !filePath) return null;

    const token = localStorage.getItem('token');

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="HDF5 Viewer"
            style={{
                content: {
                    top: '5%',
                    left: '5%',
                    right: '5%',
                    bottom: '5%',
                    padding: '0', 
                    overflow: 'hidden'
                }
            }}
        >
            <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
                <div style={{ padding: '10px', backgroundColor: '#f0f0f0', borderBottom: '1px solid #ccc', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <h3 style={{ margin: 0 }}>Viewer: {filename}</h3>
                    <button onClick={onClose} style={{ cursor: 'pointer', padding: '5px 10px' }}>Close</button>
                </div>
                <div style={{ flex: 1, position: 'relative' }}>
                    <H5GroveProvider
                        url={`${API_URL}/h5grove`}
                        filepath={filePath}
                        axiosConfig={{ headers: { Authorization: `Bearer ${token}` } }}
                    >
                        <App />
                    </H5GroveProvider>
                </div>
            </div>
        </Modal>
    );
};

export default H5Viewer;
