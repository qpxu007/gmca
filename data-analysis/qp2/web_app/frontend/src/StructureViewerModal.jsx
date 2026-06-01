import React, { useState, useEffect, useMemo } from 'react';
import Modal from 'react-modal';
import { api } from './api';
import MolstarViewer from './MolstarViewer';
import './StructureViewerModal.css';

Modal.setAppElement('#root');

const BASE_URL = import.meta.env.VITE_API_URL || '';

export default function StructureViewerModal({ isOpen, onClose, pipelineId, sampleName }) {
    const [info, setInfo] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!isOpen || !pipelineId) { setTimeout(() => setInfo(null), 0); return; }
        setTimeout(() => {
            setLoading(true);
            setError(null);
        }, 0);
        api.modelInfo(pipelineId)
            .then(setInfo)
            .catch(e => setError(e.response?.data?.detail || 'Failed to load model info'))
            .finally(() => setLoading(false));
    }, [isOpen, pipelineId]);

    const hasWebGL = useMemo(() => {
        try {
            const canvas = document.createElement('canvas');
            return !!(canvas.getContext('webgl') || canvas.getContext('experimental-webgl'));
        } catch { return false; }
    }, []);

    const pdbUrl      = info?.has_structure ? `${BASE_URL}/processing/${pipelineId}/model/pdb`    : null;
    const map2fofcUrl = info?.has_maps      ? `${BASE_URL}/processing/${pipelineId}/model/2fofc` : null;
    const mapFofcUrl  = info?.has_maps      ? `${BASE_URL}/processing/${pipelineId}/model/fofc`  : null;

    return (
        <Modal
            isOpen={isOpen}
            onRequestClose={onClose}
            contentLabel="Structure Viewer"
            className="structure-modal-content"
            overlayClassName="structure-modal-overlay"
        >
            {/* Header */}
            <div className="structure-modal-header">
                <div className="structure-modal-title-group">
                    <strong style={{ fontSize: '1rem' }}>{sampleName || 'Structure Viewer'}</strong>
                    {info?.has_maps && (
                        <span className="structure-modal-legend">
                            <span style={{ color: '#3377bb', fontWeight: 600 }}>■</span> 2Fo-Fc (1σ) &nbsp;
                            <span style={{ color: '#33aa44', fontWeight: 600 }}>■</span> Fo-Fc +3σ &nbsp;
                            <span style={{ color: '#cc3333', fontWeight: 600 }}>■</span> Fo-Fc −3σ
                        </span>
                    )}
                    {info && !info.has_maps && (
                        <span className="structure-modal-legend">No MTZ found — structure only</span>
                    )}
                </div>
                <button onClick={onClose} style={{ background: 'none', border: 'none', fontSize: '1.2rem', cursor: 'pointer', lineHeight: 1 }}>✕</button>
            </div>

            {/* Body */}
            <div style={{ flex: 1, minHeight: 0 }}>
                {loading && (
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#666' }}>
                        Loading…
                    </div>
                )}
                {error && (
                    <div style={{ color: '#c0392b', padding: 16 }}>{error}</div>
                )}
                {!loading && !error && info && !info.has_structure && (
                    <div style={{ color: '#888', padding: 16 }}>No solved structure found for this result.</div>
                )}
                {!loading && !error && info?.has_structure && !hasWebGL && (
                    <div style={{ padding: 24, textAlign: 'center', color: '#666' }}>
                        <div style={{ fontSize: '2rem', marginBottom: 12 }}>⚠️</div>
                        <div style={{ fontWeight: 600, marginBottom: 8 }}>WebGL not available</div>
                        <div style={{ fontSize: '0.9rem', maxWidth: 360, margin: '0 auto' }}>
                            The 3D structure viewer requires WebGL. Please use a browser with WebGL support
                            (Chrome, Firefox, or Safari with hardware acceleration enabled).
                        </div>
                    </div>
                )}
                {!loading && hasWebGL && pdbUrl && (
                    <MolstarViewer
                        key={`${pipelineId}-${pdbUrl}`}
                        modelUrl={pdbUrl}
                        fileType="pdb"
                        map2fofcUrl={map2fofcUrl}
                        mapFofcUrl={mapFofcUrl}
                        style={{ height: '100%' }}
                    />
                )}
            </div>
        </Modal>
    );
}
