import React, { useMemo } from 'react';
import { App, H5GroveProvider } from '@h5web/app';
import '@h5web/app/dist/styles.css';
import Modal from 'react-modal';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

// H5GroveProvider requires an absolute URL (it uses new URL() internally).
// When deployed behind a reverse proxy, VITE_API_URL is a relative path like
// /data_portal/api, so we prepend the current origin.
const H5GROVE_URL = API_URL.startsWith('http')
    ? `${API_URL}/h5grove`
    : `${window.location.origin}${API_URL}/h5grove`;

function createAuthFetcher() {
    return async (url, params, opts = {}) => {
        const { abortSignal } = opts;
        const searchParams = new URLSearchParams(params);
        const response = await fetch(`${url}?${searchParams.toString()}`, {
            credentials: 'include',
            signal: abortSignal,
        });
        const buffer = await response.arrayBuffer();
        if (response.ok) return buffer;
        throw new Error(`${response.status} ${response.statusText}`);
    };
}

const H5Viewer = ({ isOpen, onClose, filePath, filename }) => {
    const fetcher = useMemo(() => createAuthFetcher(), []);

    if (!isOpen || !filePath) return null;

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
                        url={H5GROVE_URL}
                        filepath={filePath}
                        fetcher={fetcher}
                    >
                        <App initialPath="/entry/data" />
                    </H5GroveProvider>
                </div>
            </div>
        </Modal>
    );
};

export default H5Viewer;
