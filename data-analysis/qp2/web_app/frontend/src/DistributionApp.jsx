import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { AlertCircle, Home, Map, Upload } from 'lucide-react';
import { api } from './api';
import './DistributionApp.css';

const DistributionApp = () => {
    const [file, setFile] = useState(null);
    const [loading, setLoading] = useState(false);
    const [mapHtml, setMapHtml] = useState(null);
    const [error, setError] = useState('');
    const [baseTile, setBaseTile] = useState('dark');
    const [circleColor, setCircleColor] = useState('blue');
    const [sizeMultiplier, setSizeMultiplier] = useState(1.0);
    const [opacity, setOpacity] = useState(0.6);
    const [missingInstitutions, setMissingInstitutions] = useState([]);
    const [geocodedInstitutions, setGeocodedInstitutions] = useState([]);
    const [corrections, setCorrections] = useState({});
    const [showEditPanel, setShowEditPanel] = useState(false);



    const handleFileChange = (e) => {
        if (e.target.files && e.target.files[0]) {
            setFile(e.target.files[0]);
            setError('');
        }
    };

    const handleSettingChange = (setting, value) => {
        if (setting === 'tile') setBaseTile(value);
        if (setting === 'color') setCircleColor(value);
        if (setting === 'size') setSizeMultiplier(value);
        if (setting === 'opacity') setOpacity(value);
    };

    const handleCorrectionChange = (inst, value) => {
        setCorrections(prev => ({ ...prev, [inst]: value }));
    };

    const handleRetryCorrections = () => {
        if (file && mapHtml) {
            generateMapWithOptions({
                tile: baseTile,
                color: circleColor,
                size: sizeMultiplier,
                opacity: opacity
            });
        }
    };

    const handleSettingCommit = (setting, value) => {
        if (file && mapHtml) {
            generateMapWithOptions({
                tile: setting === 'tile' ? value : baseTile,
                color: setting === 'color' ? value : circleColor,
                size: setting === 'size' ? value : sizeMultiplier,
                opacity: setting === 'opacity' ? value : opacity
            });
        }
    };

    const generateMapWithOptions = async (opts) => {
        if (!file) {
            setError('Please select a CSV or Excel file first.');
            return;
        }

        setLoading(true);
        setError('');

        const formData = new FormData();
        formData.append('file', file);
        formData.append('base_tile', opts.tile);
        formData.append('circle_color', opts.color);
        formData.append('size_multiplier', opts.size);
        formData.append('opacity', opts.opacity);
        formData.append('corrections', JSON.stringify(corrections));

        try {
            const response = await api.distributionGenerateMap(formData);

            if (response && response.success) {
                setMapHtml(response.html);
                setMissingInstitutions(response.missing || []);
                setGeocodedInstitutions(response.geocoded || []);
            } else {
                setError('Failed to generate map.');
            }
        } catch (err) {
            console.error(err);
            setError(err.response?.data?.detail || err.message || 'An error occurred during map generation.');
        } finally {
            setLoading(false);
        }
    };

    const handleGenerate = () => generateMapWithOptions({
        tile: baseTile,
        color: circleColor,
        size: sizeMultiplier,
        opacity: opacity
    });

    return (
        <div className="distribution-container">
            <div className="distribution-toolbar">
                <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#BBE1FA', textDecoration: 'none', marginRight: '10px' }}>
                    <Home size={20} />
                </Link>
                <h2 style={{ display: 'flex', alignItems: 'center', margin: 0, fontSize: '1.1rem', fontWeight: 600, color: '#fff', marginRight: '20px' }}>
                    <Map size={20} style={{ marginRight: '8px' }} /> GMCA User Distribution
                </h2>
            </div>

            <main className="app-main" style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 60px)', overflow: 'hidden' }}>
                <div style={{ padding: '20px', background: '#f8f9fa', borderBottom: '1px solid #ddd' }}>
                    <div style={{ display: 'flex', gap: '20px', alignItems: 'flex-end', flexWrap: 'wrap' }}>
                        <div>
                            <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'bold' }}>Spreadsheet File (CSV/Excel)</label>
                            <input type="file" accept=".csv,.xls,.xlsx" onChange={handleFileChange} />
                        </div>
                        <div>
                            <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'bold' }}>Base Map Style</label>
                            <select 
                                value={baseTile} 
                                onChange={e => { handleSettingChange('tile', e.target.value); handleSettingCommit('tile', e.target.value); }} 
                                style={{ padding: '6px', width: '100%' }}
                            >
                                <option value="dark">Dark Matter</option>
                                <option value="positron">Positron (Light)</option>
                                <option value="voyager">Voyager</option>
                                <option value="openstreetmap">OpenStreetMap</option>
                                <option value="terrain">Terrain</option>
                            </select>
                        </div>
                        <div style={{ flex: '1', minWidth: '150px' }}>
                            <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'bold' }}>Circle Color</label>
                            <select 
                                value={circleColor} 
                                onChange={e => { handleSettingChange('color', e.target.value); handleSettingCommit('color', e.target.value); }} 
                                style={{ padding: '6px', width: '100%' }}
                            >
                                <option value="blue">Blue</option>
                                <option value="red">Red</option>
                                <option value="green">Green</option>
                                <option value="orange">Orange</option>
                                <option value="purple">Purple</option>
                            </select>
                        </div>
                        <div style={{ flex: '1', minWidth: '150px' }}>
                            <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'bold' }}>Size ({Math.round(sizeMultiplier * 100)}%)</label>
                            <input 
                                type="range" min="0.1" max="3.0" step="0.1" 
                                value={sizeMultiplier} 
                                onChange={e => handleSettingChange('size', parseFloat(e.target.value))} 
                                onMouseUp={e => handleSettingCommit('size', parseFloat(e.target.value))}
                                onTouchEnd={e => handleSettingCommit('size', parseFloat(e.target.value))}
                                onKeyUp={e => handleSettingCommit('size', parseFloat(e.target.value))}
                                style={{ width: '100%' }} 
                            />
                        </div>
                        <div style={{ flex: '1', minWidth: '150px' }}>
                            <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'bold' }}>Opacity ({Math.round(opacity * 100)}%)</label>
                            <input 
                                type="range" min="0.1" max="1.0" step="0.1" 
                                value={opacity} 
                                onChange={e => handleSettingChange('opacity', parseFloat(e.target.value))} 
                                onMouseUp={e => handleSettingCommit('opacity', parseFloat(e.target.value))}
                                onTouchEnd={e => handleSettingCommit('opacity', parseFloat(e.target.value))}
                                onKeyUp={e => handleSettingCommit('opacity', parseFloat(e.target.value))}
                                style={{ width: '100%' }} 
                            />
                        </div>
                        <button 
                            onClick={handleGenerate} 
                            disabled={!file || loading}
                            style={{
                                display: 'flex', alignItems: 'center', gap: '8px',
                                padding: '8px 16px', backgroundColor: '#3498db', color: 'white',
                                border: 'none', borderRadius: '4px', cursor: (!file || loading) ? 'not-allowed' : 'pointer',
                                opacity: (!file || loading) ? 0.6 : 1
                            }}
                        >
                            {loading ? <div className="spinner" style={{ width: '16px', height: '16px', borderWidth: '2px' }}></div> : <Map size={18} />}
                            {loading ? 'Geocoding...' : 'Generate Map'}
                        </button>
                    </div>

                    {loading && (
                        <div style={{ marginTop: '16px', padding: '12px', background: '#fff3cd', color: '#856404', borderRadius: '4px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <AlertCircle size={18} />
                            <span><strong>Note:</strong> Generating the map may take several minutes if the spreadsheet contains many new, uncached institutions due to rate limits.</span>
                        </div>
                    )}

                    {missingInstitutions.length > 0 && !loading && (
                        <div style={{ marginTop: '16px', padding: '12px', background: '#f8d7da', color: '#721c24', borderRadius: '4px', border: '1px solid #f5c6cb' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px', fontWeight: 'bold' }}>
                                <AlertCircle size={18} />
                                <span>The following {missingInstitutions.length} institutions could not be found. Provide an alternative city, ZIP, or 'lat,lng', then retry:</span>
                            </div>
                            <div style={{ maxHeight: '200px', overflowY: 'auto', marginBottom: '12px' }}>
                                {missingInstitutions.map((inst, idx) => (
                                    <div key={idx} style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
                                        <div style={{ flex: 1, fontSize: '13px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={inst}>
                                            {inst}
                                        </div>
                                        <input 
                                            type="text" 
                                            placeholder="e.g. Chicago, IL or 41.87,-87.62" 
                                            value={corrections[inst] || ''} 
                                            onChange={e => handleCorrectionChange(inst, e.target.value)}
                                            style={{ flex: 1, padding: '4px', fontSize: '13px', border: '1px solid #ccc', borderRadius: '3px' }}
                                        />
                                    </div>
                                ))}
                            </div>
                            <button 
                                onClick={handleRetryCorrections}
                                style={{
                                    padding: '6px 12px', backgroundColor: '#dc3545', color: 'white',
                                    border: 'none', borderRadius: '4px', cursor: 'pointer', fontSize: '13px',
                                    fontWeight: 'bold'
                                }}
                            >
                                Retry with Corrections
                            </button>
                        </div>
                    )}

                    {geocodedInstitutions.length > 0 && !loading && (
                        <div style={{ marginTop: '16px', padding: '12px', background: '#e2e8f0', borderRadius: '4px', border: '1px solid #cbd5e1', color: '#334155' }}>
                            <div 
                                style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', cursor: 'pointer', fontWeight: 'bold' }}
                                onClick={() => setShowEditPanel(!showEditPanel)}
                            >
                                <span>Review/Edit All Locations ({geocodedInstitutions.length})</span>
                                <span>{showEditPanel ? '▲' : '▼'}</span>
                            </div>
                            
                            {showEditPanel && (
                                <div style={{ marginTop: '12px' }}>
                                    <p style={{ fontSize: '13px', marginBottom: '8px' }}>
                                        If any of the locations above are incorrect, provide an alternative city, ZIP, or 'lat,lng' below and retry:
                                    </p>
                                    <div style={{ maxHeight: '300px', overflowY: 'auto', marginBottom: '12px', background: 'white', padding: '8px', border: '1px solid #cbd5e1', borderRadius: '4px' }}>
                                        {geocodedInstitutions.map((inst, idx) => (
                                            <div key={idx} style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px', borderBottom: '1px solid #f1f5f9', paddingBottom: '8px' }}>
                                                <div style={{ flex: 1, fontSize: '13px', overflow: 'hidden' }}>
                                                    <div style={{ fontWeight: 'bold', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={inst.name}>{inst.name}</div>
                                                    <div style={{ fontSize: '11px', color: '#64748b', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={inst.address}>{inst.address}</div>
                                                </div>
                                                <input 
                                                    type="text" 
                                                    placeholder="e.g. Chicago, IL or 41.87,-87.62" 
                                                    value={corrections[inst.name] || ''} 
                                                    onChange={e => handleCorrectionChange(inst.name, e.target.value)}
                                                    style={{ flex: 1, padding: '4px', fontSize: '13px', border: '1px solid #ccc', borderRadius: '3px' }}
                                                />
                                            </div>
                                        ))}
                                    </div>
                                    <button 
                                        onClick={handleRetryCorrections}
                                        style={{
                                            padding: '6px 12px', backgroundColor: '#3b82f6', color: 'white',
                                            border: 'none', borderRadius: '4px', cursor: 'pointer', fontSize: '13px',
                                            fontWeight: 'bold'
                                        }}
                                    >
                                        Apply Corrections
                                    </button>
                                </div>
                            )}
                        </div>
                    )}

                    {error && (
                        <div style={{ marginTop: '16px', padding: '12px', background: '#f8d7da', color: '#721c24', borderRadius: '4px' }}>
                            {error}
                        </div>
                    )}
                </div>

                <div style={{ flex: 1, position: 'relative', background: '#eee' }}>
                    {mapHtml ? (
                        <iframe 
                            srcDoc={mapHtml} 
                            style={{ width: '100%', height: '100%', border: 'none' }} 
                            title="Institution Map"
                        />
                    ) : (
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#7f8c8d' }}>
                            <Map size={64} style={{ marginBottom: '16px', opacity: 0.5 }} />
                            <p>Upload a spreadsheet and click Generate Map to preview the distribution.</p>
                        </div>
                    )}
                </div>
            </main>
        </div>
    );
};

export default DistributionApp;
