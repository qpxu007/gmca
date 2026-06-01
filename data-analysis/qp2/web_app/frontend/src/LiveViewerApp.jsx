import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { Home, Pause, Play, Radio, Zap } from 'lucide-react';
const BASE_URL = import.meta.env.VITE_API_URL || '';

const COLORMAPS = ['plasma', 'viridis', 'gray', 'hot', 'inferno'];

const LiveViewerApp = () => {
    const isAdmin = localStorage.getItem('is_admin') === 'true';

    // Connection state
    const [status, setStatus] = useState('connecting'); // connecting | waiting | pending | live | error
    const [beamline, setBeamline] = useState('');       // '' | 'bl1' | 'bl2' (staff only)
    const [series, setSeries] = useState(null);         // metadata from new_series event
    const [latestFrame, setLatestFrame] = useState(0);
    const [currentFrame, setCurrentFrame] = useState(0);

    // Playback
    const [autoFollow, setAutoFollow] = useState(true);

    // Display
    const [imgSrc, setImgSrc] = useState(null);
    const [vmin, setVmin] = useState(null);
    const [vmax, setVmax] = useState(null);
    const [colormap, setColormap] = useState('plasma');
    const [params, setParams] = useState(null);
    const [rings, setRings] = useState([]);
    const [showRings, setShowRings] = useState(true);
    const [fetchError, setFetchError] = useState(null);
    const [switchToast, setSwitchToast] = useState(null);
    const [frameSource, setFrameSource] = useState(null);

    const containerRef = useRef(null);
    const imgRef = useRef(null);
    const esRef = useRef(null);
    const fetchingRef = useRef(false);
    const seriesRef = useRef(null);        // always current without stale closure
    const autoFollowRef = useRef(true);
    const beamlineRef = useRef('');        // always current beamline selector value
    const latestFrameRef = useRef(0);      // latest frame known from SSE
    const sidecarAvailableRef = useRef(null); // null=unknown, true=yes, false=no

    // Keep refs in sync
    seriesRef.current = series;
    autoFollowRef.current = autoFollow;
    beamlineRef.current = beamline;

    // ── SSE connection ────────────────────────────────────────────────────────
    useEffect(() => {
        let cancelled = false;

        const connect = () => {
            if (cancelled) return;
            const qs = beamline ? `?beamline=${beamline}` : '';
            const es = new EventSource(`${BASE_URL}/viewer/live/events${qs}`, { withCredentials: true });
            esRef.current = es;
            setStatus('waiting');

            es.onmessage = (e) => {
                let data;
                try { data = JSON.parse(e.data); } catch { return; }

                if (data.type === 'heartbeat') return;
                if (data.type === 'error') { setStatus('error'); return; }

                if (data.type === 'pending') {
                    setStatus('pending');
                    return;
                }

                if (data.type === 'new_series') {
                    // Detect conflict: "Both beamlines" selected but a second beamline
                    // started collecting while we're already watching another one.
                    const existingKey = seriesRef.current?.beamline_key;
                    if (beamlineRef.current === '' && existingKey && existingKey !== data.beamline_key) {
                        // Auto-switch to the newly active beamline
                        setBeamline(data.beamline_key);
                        setSeries(null);
                        setImgSrc(null);
                        setStatus('connecting');
                        const label = data.beamline_key === 'bl1' ? 'BL1 (23-ID-D)' : 'BL2 (23-ID-B)';
                        setSwitchToast(`Both beamlines active — switched to ${label}. Select manually to change.`);
                        setTimeout(() => setSwitchToast(null), 8000);
                        return;
                    }

                    const startFrame = data.frame ?? 0;
                    latestFrameRef.current = startFrame;
                    setSeries(data);   // data includes beamline_key
                    setLatestFrame(startFrame);
                    setCurrentFrame(startFrame);
                    setStatus('live');
                    setAutoFollow(true);
                    setVmin(null);
                    setVmax(null);
                    setFetchError(null);
                    loadParams(data.master_file, data.owner);
                }

                if (data.type === 'frame_update') {
                    const f = data.frame ?? 0;
                    latestFrameRef.current = f;
                    setLatestFrame(f);
                    // Only set state if no fetch is running; otherwise the
                    // fetch completion will pick up latestFrameRef itself.
                    if (autoFollowRef.current && !fetchingRef.current) setCurrentFrame(f);
                    if (data.beamline_key) {
                        setSeries(prev => prev ? { ...prev, beamline_key: data.beamline_key } : prev);
                    }
                }
            };

            es.onerror = () => {
                es.close();
                if (!cancelled) {
                    setStatus('error');
                    setTimeout(connect, 5000);
                }
            };
        };

        connect();
        return () => {
            cancelled = true;
            esRef.current?.close();
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [beamline]);

    const computeRings = useCallback((p) => {
        if (!p.beam_x || !p.beam_y || !p.wavelength_a || !p.det_dist_mm || !p.pixel_size_mm) {
            setRings([]); return;
        }
        const dSpacings = [20, 10, 7, 5, 4, 3.5, 3, 2.5, 2, 1.8, 1.5, 1.2, 1.0];
        const computed = dSpacings.flatMap(d => {
            try {
                const sinTheta = p.wavelength_a / (2 * d);
                if (sinTheta >= 1) return [];
                const theta = Math.asin(sinTheta);
                const r_mm = p.det_dist_mm * Math.tan(2 * theta);
                const r_px = r_mm / p.pixel_size_mm;
                const maxR = Math.max(p.image_width, p.image_height);
                return r_px > 0 && r_px < maxR ? [{ d, r_px }] : [];
            } catch { return []; }
        });
        setRings(computed);
    }, []);

    // ── Detector params + rings ───────────────────────────────────────────────
    const loadParams = useCallback(async (path, owner) => {
        try {
            const res = await fetch(
                `${BASE_URL}/viewer/live/params?path=${encodeURIComponent(path)}&owner=${encodeURIComponent(owner)}`,
                { credentials: 'include' }
            );
            if (!res.ok) return;
            const p = await res.json();
            setParams(p);
            computeRings(p);
        } catch { /* silent */ }
    }, [computeRings]);



    // ── Frame fetching ────────────────────────────────────────────────────────
    useEffect(() => {
        const s = seriesRef.current;
        if (!s) return;
        if (fetchingRef.current) return;
        fetchingRef.current = true;

        const controller = new AbortController();
        const { signal } = controller;
        const w = containerRef.current ? containerRef.current.offsetWidth - 32 : 800;

        const fileUrl = () => {
            let u = `${BASE_URL}/viewer/live/frame`
                + `?path=${encodeURIComponent(s.master_file)}`
                + `&frame=${currentFrame}`
                + `&owner=${encodeURIComponent(s.owner)}`
                + `&colormap=${colormap}&width=${w}`;
            if (vmin !== null) u += `&vmin=${vmin}`;
            if (vmax !== null) u += `&vmax=${vmax}`;
            return u;
        };

        const processResponse = (res) => {
            if (!res.ok) { setFetchError(`Frame ${currentFrame} not ready`); return null; }
            setFetchError(null);
            const hVmin = res.headers.get('X-Vmin');
            const hVmax = res.headers.get('X-Vmax');
            const hSource = res.headers.get('X-Source') || 'file';
            if (vmin === null && hVmin) setVmin(parseFloat(hVmin));
            if (vmax === null && hVmax) setVmax(parseFloat(hVmax));
            setFrameSource(hSource);
            return res.blob();
        };

        // Prefer sidecar/ZMQ cache; skip if we already know it's unavailable
        const useSidecar = s.beamline_key && sidecarAvailableRef.current !== false;
        let url = useSidecar
            ? `${BASE_URL}/viewer/live/latest?beamline=${s.beamline_key}&owner=${encodeURIComponent(s.owner)}`
            : fileUrl();

        fetch(url, { credentials: 'include', signal })
            .then(res => {
                if (useSidecar && (res.status === 404 || res.status === 503)) {
                    // Sidecar not running — remember and fall back to file endpoint
                    sidecarAvailableRef.current = false;
                    if (!s.master_file) return null;
                    return fetch(fileUrl(), { credentials: 'include', signal })
                        .then(processResponse);
                }
                if (useSidecar && res.ok) sidecarAvailableRef.current = true;
                return processResponse(res);
            })
            .then(blob => {
                if (!blob) return;
                const objUrl = URL.createObjectURL(blob);
                setImgSrc(prev => {
                    if (prev) URL.revokeObjectURL(prev);
                    return objUrl;
                });
                // Chain immediately to the next frame if SSE has already
                // reported a newer one — eliminates the throttle wait.
                if (autoFollowRef.current) {
                    const next = latestFrameRef.current;
                    if (next > currentFrame) {
                        setCurrentFrame(next);   // triggers effect again immediately
                    }
                }
            })
            .catch(e => { if (e.name !== 'AbortError') setFetchError('Fetch failed'); })
            .finally(() => { fetchingRef.current = false; });

        return () => controller.abort();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [currentFrame, series, colormap]);   // vmin/vmax intentionally excluded to avoid loop

    // Revoke final object URL on unmount
    const lastImgSrc = useRef(null);
    useEffect(() => { lastImgSrc.current = imgSrc; }, [imgSrc]);
    useEffect(() => () => { if (lastImgSrc.current) URL.revokeObjectURL(lastImgSrc.current); }, []);

    // ── Resolution ring overlay ───────────────────────────────────────────────
    // Use viewBox matching the image's native pixel dimensions so beam_x/beam_y
    // and ring radii are used directly — same pattern as ImageViewerApp.
    const renderRings = () => {
        if (!showRings || !rings.length || !params) return null;
        const { image_width, image_height, beam_x, beam_y } = params;
        if (!image_width || !image_height) return null;
        const sw = Math.max(image_width, image_height) / 500;
        const fontSize = Math.max(image_width, image_height) / 80;

        return (
            <svg
                viewBox={`0 0 ${image_width} ${image_height}`}
                style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: '100%', pointerEvents: 'none' }}
            >
                {rings.map(ring => (
                    <g key={ring.d}>
                        <circle
                            cx={beam_x} cy={beam_y} r={ring.r_px}
                            fill="none" stroke="rgba(255,255,0,0.5)" strokeWidth={sw}
                        />
                        <text
                            x={beam_x + ring.r_px * 0.707 + 15}
                            y={beam_y - ring.r_px * 0.707 - 15}
                            fill="rgba(255,255,0,0.85)" fontSize={fontSize}
                        >
                            {ring.d}Å
                        </text>
                    </g>
                ))}
                {/* Beam centre crosshair */}
                {(() => {
                    const s = Math.max(image_width, image_height) / 100;
                    return (
                        <>
                            <line x1={beam_x - s} y1={beam_y} x2={beam_x + s} y2={beam_y} stroke="red" strokeWidth={sw} />
                            <line x1={beam_x} y1={beam_y - s} x2={beam_x} y2={beam_y + s} stroke="red" strokeWidth={sw} />
                        </>
                    );
                })()}
            </svg>
        );
    };

    // ── Status indicator ──────────────────────────────────────────────────────
    const statusColor = { connecting: '#888', waiting: '#f39c12', pending: '#3498db', live: '#2ecc71', error: '#e74c3c' }[status];
    const statusLabel = { connecting: 'Connecting…', waiting: 'Waiting for collection…', pending: 'Collection started — waiting for file…', live: 'Live', error: 'Reconnecting…' }[status];

    const sourceDisplayLabel = {
        'sidecar': 'ZMQ Stream',
        'monitor-preview': 'Monitor (10Hz)',
        'zmq-cache': 'Legacy ZMQ',
        'file': 'File (Disk)',
    }[frameSource] || frameSource;

    // ── Render ────────────────────────────────────────────────────────────────
    return (
        <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', background: '#1a1a2e', color: '#eee' }}>

            {/* Toolbar */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '8px 16px', background: '#16213e', borderBottom: '1px solid #333', flexShrink: 0, flexWrap: 'wrap' }}>
                <Link to="/dashboard" style={{ display: 'flex', alignItems: 'center', color: '#BBE1FA', textDecoration: 'none', marginRight: '10px' }}>
                    <Home size={20} />
                </Link>
                <Radio size={16} color={statusColor} />
                <span style={{ fontWeight: 600 }}>Live Viewer</span>
                <span style={{ fontSize: '0.8rem', color: statusColor }}>{statusLabel}</span>
                {series && (
                    <span style={{ fontSize: '0.85rem', color: '#aaa' }}>
                        {series.prefix}{series.beamline ? ` · ${series.beamline}` : ''}
                    </span>
                )}

                <div style={{ flex: 1 }} />

                {/* Beamline selector — staff only, for when both beamlines collect simultaneously */}
                {isAdmin && (
                    <select
                        value={beamline}
                        onChange={e => { setBeamline(e.target.value); setSeries(null); setImgSrc(null); setStatus('connecting'); }}
                        style={{ background: '#333', color: '#fff', border: '1px solid #555', borderRadius: 4, padding: '3px 6px', fontSize: '0.85rem' }}
                        title="Select beamline (staff)"
                    >
                        <option value="">Both beamlines</option>
                        <option value="bl1">BL1 (23-ID-D)</option>
                        <option value="bl2">BL2 (23-ID-B)</option>
                    </select>
                )}

                {/* Auto-follow toggle */}
                <button
                    onClick={() => setAutoFollow(a => !a)}
                    style={{ background: autoFollow ? '#27ae60' : '#555', border: 'none', borderRadius: 4, color: '#fff', padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4, fontSize: '0.85rem' }}
                >
                    {autoFollow ? <><Play size={13} /> Following</> : <><Pause size={13} /> Paused</>}
                </button>

                {/* Colormap */}
                <select
                    value={colormap}
                    onChange={e => { setColormap(e.target.value); setVmin(null); setVmax(null); }}
                    style={{ background: '#333', color: '#fff', border: '1px solid #555', borderRadius: 4, padding: '3px 6px', fontSize: '0.85rem' }}
                >
                    {COLORMAPS.map(c => <option key={c} value={c}>{c}</option>)}
                </select>

                {/* Rings toggle */}
                <button
                    onClick={() => setShowRings(r => !r)}
                    style={{ background: showRings ? '#2980b9' : '#555', border: 'none', borderRadius: 4, color: '#fff', padding: '4px 10px', cursor: 'pointer', fontSize: '0.85rem' }}
                >
                    Rings
                </button>
            </div>

            {/* Metadata bar */}
            {series && (
                <div style={{ display: 'flex', gap: 24, padding: '4px 16px', background: '#0f3460', fontSize: '0.78rem', color: '#bbb', flexShrink: 0, flexWrap: 'wrap' }}>
                    <span>Frame <strong style={{ color: '#fff' }}>{currentFrame}</strong> / {series.total_frames ?? '?'}</span>
                    {series.energy_ev && <span>Energy <strong style={{ color: '#fff' }}>{(series.energy_ev / 1000).toFixed(3)} keV</strong></span>}
                    {series.exposure_sec && <span>Exposure <strong style={{ color: '#fff' }}>{series.exposure_sec} s</strong></span>}
                    {series.det_dist_m && <span>Distance <strong style={{ color: '#fff' }}>{(series.det_dist_m * 1000).toFixed(0)} mm</strong></span>}
                    <span>User <strong style={{ color: '#fff' }}>{series.owner}</strong></span>
                    {frameSource && <span>Source <strong style={{ color: '#3498db' }}>{sourceDisplayLabel}</strong></span>}
                    {fetchError && <span style={{ color: '#e74c3c' }}>{fetchError}</span>}
                </div>
            )}

            {/* Main image area */}
            <div
                ref={containerRef}
                style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', overflow: 'hidden', padding: 16, minHeight: 0 }}
            >
                {!imgSrc && (
                    <div style={{ textAlign: 'center', color: '#666' }}>
                        <Radio size={56} style={{ marginBottom: 12, opacity: 0.4 }} />
                        <div style={{ fontSize: '1.1rem' }}>{statusLabel}</div>
                        {status === 'waiting' && (
                            <div style={{ fontSize: '0.85rem', marginTop: 8, color: '#555' }}>
                                Frames will appear automatically when your collection starts.
                            </div>
                        )}
                    </div>
                )}
                {imgSrc && (
                    <div style={{ position: 'relative', display: 'inline-block' }}>
                        <img
                            ref={imgRef}
                            src={imgSrc}
                            alt={`Frame ${currentFrame}`}
                            style={{ display: 'block', maxWidth: '100%', maxHeight: 'calc(100vh - 180px)', objectFit: 'contain' }}
                        />
                        {renderRings()}
                    </div>
                )}
            </div>

            {/* Frame scrubber — only when paused */}
            {!autoFollow && series && latestFrame > 0 && (
                <div style={{ padding: '6px 16px', background: '#16213e', borderTop: '1px solid #333', display: 'flex', alignItems: 'center', gap: 12, flexShrink: 0 }}>
                    <span style={{ fontSize: '0.8rem', whiteSpace: 'nowrap' }}>Frame {currentFrame}</span>
                    <input
                        type="range" min={0} max={latestFrame} value={currentFrame}
                        onChange={e => setCurrentFrame(Number(e.target.value))}
                        style={{ flex: 1 }}
                    />
                    <span style={{ fontSize: '0.8rem', whiteSpace: 'nowrap' }}>{latestFrame}</span>
                </div>
            )}

            {/* Contrast controls */}
            <div style={{ padding: '6px 16px', background: '#16213e', borderTop: '1px solid #333', display: 'flex', alignItems: 'center', gap: 16, fontSize: '0.8rem', flexShrink: 0, flexWrap: 'wrap' }}>
                <Zap size={13} color="#f39c12" />
                <span style={{ color: '#888' }}>Contrast:</span>
                <label style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    Min
                    <input
                        type="number"
                        value={vmin ?? ''}
                        onChange={e => setVmin(e.target.value !== '' ? parseFloat(e.target.value) : null)}
                        placeholder="auto"
                        style={{ width: 80, background: '#2a2a2a', color: '#fff', border: '1px solid #555', borderRadius: 3, padding: '2px 5px' }}
                    />
                </label>
                <label style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    Max
                    <input
                        type="number"
                        value={vmax ?? ''}
                        onChange={e => setVmax(e.target.value !== '' ? parseFloat(e.target.value) : null)}
                        placeholder="auto"
                        style={{ width: 80, background: '#2a2a2a', color: '#fff', border: '1px solid #555', borderRadius: 3, padding: '2px 5px' }}
                    />
                </label>
                <button
                    onClick={() => { setVmin(null); setVmax(null); }}
                    style={{ background: '#444', border: 'none', borderRadius: 3, color: '#fff', padding: '3px 8px', cursor: 'pointer' }}
                >
                    Auto
                </button>
            </div>

            {switchToast && (
                <div style={{
                    position: 'fixed', bottom: 24, left: '50%', transform: 'translateX(-50%)',
                    background: '#e67e22', color: '#fff', padding: '10px 20px',
                    borderRadius: 6, zIndex: 2000, fontSize: '0.88rem',
                    boxShadow: '0 2px 10px rgba(0,0,0,0.4)', maxWidth: 420, textAlign: 'center',
                }}>
                    {switchToast}
                </div>
            )}
        </div>
    );
};

export default LiveViewerApp;
