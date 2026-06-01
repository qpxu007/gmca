import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { ChevronLeft, ChevronRight, ChevronUp, Eye, FolderOpen, Home, Image, Search, SkipBack, SkipForward, Menu } from 'lucide-react';
import { api, API_URL } from './api';
import './ImageViewerApp.css';

const ImageViewerApp = () => {
    const [searchParams] = useSearchParams();
    const initialDatasetId = searchParams.get('dataset_id');

    // Dataset panel
    const [datasets, setDatasets] = useState([]);
    const [searchText, setSearchText] = useState('');
    const [scanPath, setScanPath] = useState('');
    const [scanResults, setScanResults] = useState([]);
    const [browserOpen, setBrowserOpen] = useState(false);
    const [browserData, setBrowserData] = useState(null); // {path, root, parent, subdirs}
    const [mobilePanelOpen, setMobilePanelOpen] = useState(false);

    // Selected dataset
    const [selectedId, setSelectedId] = useState(initialDatasetId ? parseInt(initialDatasetId) : null);
    const [selectedPath, setSelectedPath] = useState(null);
    const [masterIndex, setMasterIndex] = useState(0);
    const [expandedId, setExpandedId] = useState(null);
    const [params, setParams] = useState(null);

    // Frame
    const [frame, setFrame] = useState(0);
    const [totalFrames, setTotalFrames] = useState(0);
    const [frameInput, setFrameInput] = useState('0');

    // Contrast
    const [vmin, setVmin] = useState(null);
    const [vmax, setVmax] = useState(null);
    const [autoContrast, setAutoContrast] = useState(true);

    // Pixel info
    const [pixelInfo, setPixelInfo] = useState(null);

    // Rings
    const [rings, setRings] = useState(null);
    const [showRings, setShowRings] = useState(true);

    // Zoom/pan
    const [zoom, setZoom] = useState(1);
    const [pan, setPan] = useState({ x: 0, y: 0 });

    // Abort controller — cancel in-flight frame requests when a new one starts
    const abortRef = useRef(null);

    // Debounced contrast — only fire a request after 600ms of no changes
    const [appliedVmin, setAppliedVmin] = useState(null);
    const [appliedVmax, setAppliedVmax] = useState(null);
    const contrastDebounceRef = useRef(null);
    const [dragging, setDragging] = useState(false);
    const [dragStart, setDragStart] = useState({ x: 0, y: 0 });

    // Image
    const [imageUrl, setImageUrl] = useState(null);
    const [loading, setLoading] = useState(false);
    const [frameError, setFrameError] = useState(null);
    const [imgSize, setImgSize] = useState({ w: 0, h: 0 });
    const imageContainerRef = useRef(null);
    const imgRef = useRef(null);

    // --- Data loading functions (declared before effects that use them) ---

    const loadDatasets = async () => {
        try {
            const data = await api.viewerDatasets();
            setDatasets(data);
            if (initialDatasetId && !selectedId) {
                setSelectedId(parseInt(initialDatasetId));
            }
        } catch (e) {
            console.error('Failed to load datasets', e);
        }
    };

    const loadParams = async (id, path, mi = 0) => {
        try {
            const p = await api.viewerParams(id, path, mi);
            setParams(p);
            setTotalFrames(p.total_frames || 0);
        } catch (e) {
            console.error('Failed to load params', e);
        }
    };

    const loadRings = async (id, path, mi = 0) => {
        try {
            const r = await api.viewerRings(id, path, mi);
            setRings(r);
        } catch (e) {
            console.error('Failed to load rings', e);
        }
    };

    const loadFrame = useCallback(() => {
        if (selectedId === null) return;

        // Cancel any in-flight request to prevent stale responses from
        // overwriting vmin/vmax and triggering an infinite render loop
        if (abortRef.current) abortRef.current.abort();
        const controller = new AbortController();
        abortRef.current = controller;

        setLoading(true);
        setFrameError(null);
        const v1 = autoContrast ? '' : (appliedVmin ?? '');
        const v2 = autoContrast ? '' : (appliedVmax ?? '');
        const displayWidth = imageContainerRef.current
            ? Math.round(imageContainerRef.current.clientWidth * Math.max(1, zoom)) : '';
        const url = `${API_URL}/viewer/frame/${selectedId}?frame=${frame}&master_index=${masterIndex}&colormap=plasma${v1 !== '' ? `&vmin=${v1}` : ''}${v2 !== '' ? `&vmax=${v2}` : ''}${displayWidth ? `&width=${displayWidth}` : ''}${selectedPath ? `&path=${encodeURIComponent(selectedPath)}` : ''}&_t=${Date.now()}`;

        fetch(url, { credentials: 'include', signal: controller.signal })
            .then(async resp => {
                if (!resp.ok) {
                    // Try to get the detail message from JSON body
                    let detail = resp.statusText;
                    try { const j = await resp.json(); detail = j.detail || detail; } catch { /* ignore */ }
                    throw new Error(`${resp.status}: ${detail}`);
                }
                const respVmin = resp.headers.get('X-Vmin');
                const respVmax = resp.headers.get('X-Vmax');
                // Only update vmin/vmax from server in auto-contrast mode.
                // In manual mode the user owns these values — updating them
                // from the response would cause an oscillation loop.
                if (autoContrast && respVmin && respVmax) {
                    setVmin(parseFloat(respVmin));
                    setVmax(parseFloat(respVmax));
                }
                return resp.blob();
            })
            .then(blob => {
                const objUrl = URL.createObjectURL(blob);
                setImageUrl(prev => {
                    if (prev) URL.revokeObjectURL(prev);
                    return objUrl;
                });
                setLoading(false);
            })
            .catch(e => {
                if (e.name === 'AbortError') return; // expected — ignore
                console.error('Frame load error', e);
                setFrameError(e.message || 'Failed to load frame');
                setImageUrl(prev => { if (prev) URL.revokeObjectURL(prev); return null; });
                setLoading(false);
            });
    }, [selectedId, selectedPath, frame, masterIndex, appliedVmin, appliedVmax, autoContrast, zoom]);

    // --- Effects ---

    // Load datasets
    useEffect(() => {
        loadDatasets();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    // Load params + rings when dataset or master file changes
    useEffect(() => {
        if (selectedId !== null) {
            loadParams(selectedId, selectedPath, masterIndex);
            loadRings(selectedId, selectedPath, masterIndex);
            setFrame(0);
            setFrameInput('0');
            setAutoContrast(true);
            setVmin(null);
            setVmax(null);
            setZoom(1);
            setPan({ x: 0, y: 0 });
            setFrameError(null);
            setImageUrl(prev => { if (prev) URL.revokeObjectURL(prev); return null; });
        }
    }, [selectedId, selectedPath, masterIndex]);

    // When vmin/vmax change in manual mode, debounce before applying
    useEffect(() => {
        if (autoContrast) return;
        if (contrastDebounceRef.current) clearTimeout(contrastDebounceRef.current);
        contrastDebounceRef.current = setTimeout(() => {
            setAppliedVmin(vmin);
            setAppliedVmax(vmax);
        }, 600);
        return () => clearTimeout(contrastDebounceRef.current);
    }, [vmin, vmax, autoContrast]);

    // Load frame when frame changes or applied contrast changes
    const contrastKey = autoContrast ? 'auto' : `${appliedVmin}_${appliedVmax}`;
    useEffect(() => {
        if (selectedId !== null) {
            loadFrame();
        }
    }, [selectedId, selectedPath, frame, contrastKey, loadFrame]);



    // Frame navigation
    const goToFrame = (f) => {
        const clamped = Math.max(0, Math.min(totalFrames - 1, f));
        setFrame(clamped);
        setFrameInput(String(clamped));
    };

    const handleFrameInput = (e) => {
        if (e.key === 'Enter') {
            const val = parseInt(frameInput);
            if (!isNaN(val)) goToFrame(val);
        }
    };

    // Pixel info on mouse move (debounced)
    const pixelTimer = useRef(null);
    const handleMouseMove = (e) => {
        if (dragging || !imgRef.current || !params) return;
        const rect = imgRef.current.getBoundingClientRect();
        const imgW = imgRef.current.naturalWidth;
        const imgH = imgRef.current.naturalHeight;
        const nativeW = params.image_width || imgW;
        const nativeH = params.image_height || imgH;

        // Map display coords to native pixel coords
        const dispX = (e.clientX - rect.left) / rect.width;
        const dispY = (e.clientY - rect.top) / rect.height;
        const px = Math.floor(dispX * nativeW);
        const py = Math.floor(dispY * nativeH);

        if (px < 0 || py < 0 || px >= nativeW || py >= nativeH) return;

        clearTimeout(pixelTimer.current);
        pixelTimer.current = setTimeout(async () => {
            try {
                const info = await api.viewerPixel(selectedId, selectedPath, frame, px, py, masterIndex);
                setPixelInfo(info);
            } catch { /* ignore */ }
        }, 80);
    };

    // Zoom with scroll
    const handleWheel = (e) => {
        e.preventDefault();
        const delta = e.deltaY > 0 ? 0.9 : 1.1;
        setZoom(z => Math.max(0.1, Math.min(20, z * delta)));
    };

    // Pan with drag
    const handleMouseDown = (e) => {
        if (e.button !== 0) return;
        setDragging(true);
        setDragStart({ x: e.clientX - pan.x, y: e.clientY - pan.y });
    };
    const handleMouseUp = () => setDragging(false);
    const handleDragMove = (e) => {
        if (!dragging) return;
        setPan({ x: e.clientX - dragStart.x, y: e.clientY - dragStart.y });
    };

    // Reset zoom
    const resetView = () => {
        setZoom(1);
        setPan({ x: 0, y: 0 });
    };

    // Directory browser
    const openBrowser = async (path = '') => {
        try {
            const res = await api.viewerBrowse(path);
            setBrowserData(res);
            setBrowserOpen(true);
        } catch (e) {
            alert('Browse failed: ' + (e.response?.data?.detail || e.message));
        }
    };

    const handleBrowseUp = () => browserData?.parent && openBrowser(browserData.parent);
    const handleBrowseInto = (subdir) => openBrowser(`${browserData.path}/${subdir}`);

    const handleScanHere = async () => {
        const path = browserData?.path;
        if (!path) return;
        setScanPath(path);
        setBrowserOpen(false);
        try {
            const res = await api.viewerScan(path);
            setScanResults(res);
        } catch (e) {
            alert('Scan failed: ' + (e.response?.data?.detail || e.message));
        }
    };

    // Scan directory (manual path input fallback)
    const handleScan = async () => {
        if (!scanPath) return;
        try {
            const res = await api.viewerScan(scanPath);
            setScanResults(res);
        } catch (e) {
            alert('Scan failed: ' + (e.response?.data?.detail || e.message));
        }
    };

    // Filter datasets
    const filteredDatasets = datasets.filter(ds =>
        !searchText || ds.name.toLowerCase().includes(searchText.toLowerCase())
    );

    return (
        <div className="iv-container" style={{ position: 'relative' }}>
            {/* Left panel */}
            <div className={`iv-panel ${mobilePanelOpen ? 'mobile-open' : ''}`}>
                <div className="iv-panel-header">
                    <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#BBE1FA', textDecoration: 'none', marginRight: '10px' }}>
                    <Home size={20} />
                </Link>
                    <h3>Image Viewer</h3>
                </div>

                <div className="iv-search">
                    <Search size={14} />
                    <input
                        type="text"
                        placeholder="Search datasets..."
                        value={searchText}
                        onChange={e => setSearchText(e.target.value)}
                    />
                </div>

                <div className="iv-dataset-list">
                    {filteredDatasets.map(ds => (
                        <div key={ds.id}>
                            <div
                                className={`iv-dataset-item ${selectedId === ds.id ? 'active' : ''}`}
                                onClick={() => {
                                    if (ds.master_files && ds.master_files.length > 1) {
                                        setExpandedId(expandedId === ds.id ? null : ds.id);
                                    }
                                    setSelectedId(ds.id);
                                    setSelectedPath(null);
                                    setMasterIndex(0);
                                    if (window.innerWidth <= 768 && (!ds.master_files || ds.master_files.length <= 1)) setMobilePanelOpen(false);
                                }}
                            >
                                {ds.master_files && ds.master_files.length > 1
                                    ? <span style={{ fontSize: '10px', width: 14, textAlign: 'center' }}>{expandedId === ds.id ? '▼' : '►'}</span>
                                    : <Eye size={14} />}
                                <span className="iv-dataset-name">{ds.name}</span>
                                <span className="iv-dataset-frames">
                                    {ds.master_files && ds.master_files.length > 1 ? `${ds.master_files.length}×` : ''}{ds.total_frames}f
                                </span>
                            </div>
                            {expandedId === ds.id && ds.master_files && ds.master_files.length > 1 && (
                                ds.master_files.map((mf, i) => (
                                    <div
                                        key={i}
                                        className={`iv-dataset-item iv-sub-item ${selectedId === ds.id && masterIndex === i ? 'active' : ''}`}
                                        onClick={() => { setSelectedId(ds.id); setSelectedPath(null); setMasterIndex(i); if (window.innerWidth <= 768) setMobilePanelOpen(false); }}
                                    >
                                        <Eye size={12} />
                                        <span className="iv-dataset-name">{mf.split('/').pop().replace('_master.h5', '')}</span>
                                    </div>
                                ))
                            )}
                        </div>
                    ))}
                </div>

                <div className="iv-scan-section">
                    <div className="iv-scan-row">
                        <button className="iv-browse-btn" onClick={() => openBrowser()} title="Browse directories">
                            <FolderOpen size={14} /> Browse
                        </button>
                        <input
                            type="text"
                            placeholder="or type path..."
                            value={scanPath}
                            onChange={e => setScanPath(e.target.value)}
                            onKeyDown={e => e.key === 'Enter' && handleScan()}
                        />
                        <button onClick={handleScan}>Scan</button>
                    </div>

                    {/* Directory browser panel */}
                    {browserOpen && browserData && (
                        <div className="iv-browser-panel">
                            <div className="iv-browser-header">
                                <button
                                    className="iv-browser-up"
                                    onClick={handleBrowseUp}
                                    disabled={!browserData.parent}
                                    title="Go up"
                                >
                                    <ChevronUp size={14} />
                                </button>
                                <span className="iv-browser-path" title={browserData.path}>
                                    {browserData.path.replace(browserData.root, '…') || '/'}
                                </span>
                                <button className="iv-browser-scan-here" onClick={handleScanHere}>
                                    Scan Here
                                </button>
                                <button className="iv-browser-close" onClick={() => setBrowserOpen(false)}>✕</button>
                            </div>
                            <div className="iv-browser-list">
                                {browserData.subdirs.length === 0 && (
                                    <div className="iv-browser-empty">No subdirectories</div>
                                )}
                                {browserData.subdirs.map(d => (
                                    <div key={d} className="iv-browser-entry" onClick={() => handleBrowseInto(d)}>
                                        <FolderOpen size={13} />
                                        <span>{d}</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}

                    {scanResults.map((sr, i) => (
                        <div key={i} className={`iv-dataset-item ${selectedId === 0 && selectedPath === sr.master_file ? 'active' : ''}`} title={sr.master_file} onClick={() => {
                            setSelectedId(0);
                            setSelectedPath(sr.master_file);
                            setMasterIndex(0);
                            if (window.innerWidth <= 768) setMobilePanelOpen(false);
                        }}>
                            <Eye size={14} />
                            <span className="iv-dataset-name">{sr.name}</span>
                        </div>
                    ))}
                </div>
            </div>

            {mobilePanelOpen && <div className="iv-mobile-overlay" onClick={() => setMobilePanelOpen(false)}></div>}

            {/* Main viewer */}
            <div className="iv-main">
                {/* Image area */}
                <div
                    className="iv-image-area"
                    ref={imageContainerRef}
                    onWheel={handleWheel}
                    onMouseDown={handleMouseDown}
                    onMouseUp={handleMouseUp}
                    onMouseLeave={handleMouseUp}
                    onMouseMove={dragging ? handleDragMove : handleMouseMove}
                >
                    <button className="iv-mobile-toggle" onMouseDown={(e) => e.stopPropagation()} onClick={() => setMobilePanelOpen(true)}>
                        <Menu size={20} />
                    </button>
                    {imageUrl ? (
                        <div
                            className="iv-image-wrapper"
                            style={{
                                transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
                                cursor: dragging ? 'grabbing' : 'crosshair',
                            }}
                        >
                            <img
                                ref={imgRef}
                                src={imageUrl}
                                alt={`Frame ${frame}`}
                                draggable={false}
                                onMouseMove={handleMouseMove}
                                onLoad={() => {
                                    if (imgRef.current) {
                                        setImgSize({ w: imgRef.current.clientWidth, h: imgRef.current.clientHeight });
                                    }
                                }}
                            />
                            {/* Resolution rings SVG overlay — matches img natural size */}
                            {showRings && rings && rings.rings.length > 0 && imgSize.w > 0 && (
                                <svg
                                    className="iv-rings-overlay"
                                    viewBox={`0 0 ${rings.image_width} ${rings.image_height}`}
                                    style={{ width: imgSize.w, height: imgSize.h }}
                                >
                                    {rings.rings.map((r, i) => (
                                        <g key={i}>
                                            <circle
                                                cx={rings.beam_x}
                                                cy={rings.beam_y}
                                                r={r.radius_px}
                                                fill="none"
                                                stroke="rgba(0,255,0,0.6)"
                                                strokeWidth={Math.max(rings.image_width, rings.image_height) / 500}
                                            />
                                            <text
                                                x={rings.beam_x + r.radius_px * 0.707 + 15}
                                                y={rings.beam_y - r.radius_px * 0.707 - 15}
                                                fill="rgba(0,255,0,0.9)"
                                                fontSize={Math.max(rings.image_width, rings.image_height) / 80}
                                                fontWeight="bold"
                                            >
                                                {r.label}
                                            </text>
                                        </g>
                                    ))}
                                    {/* Beam center marker */}
                                    {(() => { const s = Math.max(rings.image_width, rings.image_height) / 100; const sw = s / 3; return (<>
                                        <line x1={rings.beam_x - s} y1={rings.beam_y} x2={rings.beam_x + s} y2={rings.beam_y} stroke="red" strokeWidth={sw} />
                                        <line x1={rings.beam_x} y1={rings.beam_y - s} x2={rings.beam_x} y2={rings.beam_y + s} stroke="red" strokeWidth={sw} />
                                    </>); })()}
                                </svg>
                            )}
                        </div>
                    ) : (
                        <div className="iv-placeholder">
                            {frameError
                                ? <span style={{ color: '#e74c3c' }}>⚠ {frameError}</span>
                                : selectedId ? (loading ? 'Loading...' : 'No image') : 'Select a dataset'}
                        </div>
                    )}
                    {loading && <div className="iv-loading">Loading frame {frame}...</div>}
                </div>

                {/* Controls bar */}
                <div className="iv-controls">
                    {/* Frame navigation */}
                    <div className="iv-control-group">
                        <label>Frame:</label>
                        <button className="iv-btn" onClick={() => goToFrame(0)} title="First"><SkipBack size={14} /></button>
                        <button className="iv-btn" onClick={() => goToFrame(frame - 1)} title="Previous"><ChevronLeft size={14} /></button>
                        <input
                            type="text"
                            className="iv-frame-input"
                            value={frameInput}
                            onChange={e => setFrameInput(e.target.value)}
                            onKeyDown={handleFrameInput}
                            onBlur={() => { const v = parseInt(frameInput); if (!isNaN(v)) goToFrame(v); }}
                        />
                        <span className="iv-frame-total">/ {totalFrames}</span>
                        <button className="iv-btn" onClick={() => goToFrame(frame + 1)} title="Next"><ChevronRight size={14} /></button>
                        <button className="iv-btn" onClick={() => goToFrame(totalFrames - 1)} title="Last"><SkipForward size={14} /></button>
                        <input
                            type="range"
                            className="iv-slider"
                            min={0}
                            max={Math.max(0, totalFrames - 1)}
                            value={frame}
                            onChange={e => goToFrame(parseInt(e.target.value))}
                        />
                    </div>

                    {/* Contrast */}
                    <div className="iv-control-group">
                        <label>Contrast:</label>
                        <input
                            type="number"
                            className="iv-contrast-input"
                            value={vmin !== null ? Math.round(vmin) : ''}
                            onChange={e => { setAutoContrast(false); setVmin(parseFloat(e.target.value) || 0); }}
                            placeholder="min"
                        />
                        <span>-</span>
                        <input
                            type="number"
                            className="iv-contrast-input"
                            value={vmax !== null ? Math.round(vmax) : ''}
                            onChange={e => { setAutoContrast(false); setVmax(parseFloat(e.target.value) || 0); }}
                            placeholder="max"
                        />
                        <button
                            className={`iv-btn ${autoContrast ? 'active' : ''}`}
                            onClick={() => { setAutoContrast(true); setVmin(null); setVmax(null); }}
                        >
                            Auto
                        </button>
                    </div>

                    {/* View controls */}
                    <div className="iv-control-group">
                        <button className="iv-btn" onClick={resetView}>Reset View</button>
                        <button
                            className={`iv-btn ${showRings ? 'active' : ''}`}
                            onClick={() => setShowRings(!showRings)}
                        >
                            Rings
                        </button>
                        <span className="iv-zoom-label">{Math.round(zoom * 100)}%</span>
                    </div>

                    {/* Pixel info */}
                    <div className="iv-pixel-info">
                        {pixelInfo ? (
                            <>
                                ({pixelInfo.coords}) I={pixelInfo.intensity}
                                {pixelInfo.resolution && ` d=${parseFloat(pixelInfo.resolution).toFixed(2)}\u00c5`}
                                {pixelInfo.two_theta && ` 2\u03b8=${parseFloat(pixelInfo.two_theta).toFixed(2)}\u00b0`}
                            </>
                        ) : (
                            <span className="iv-pixel-placeholder">Hover over image for pixel info</span>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default ImageViewerApp;
