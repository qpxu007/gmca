
import React, { useRef, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { createPluginUI } from 'molstar/lib/mol-plugin-ui';
import { DefaultPluginUISpec } from 'molstar/lib/mol-plugin-ui/spec';
import { PluginConfig } from 'molstar/lib/mol-plugin/config';
import { Color } from 'molstar/lib/mol-util/color';
import { StateTransforms } from 'molstar/lib/mol-plugin-state/transforms';
import { createVolumeRepresentationParams } from 'molstar/lib/mol-plugin-state/helpers/volume-representation-params';
import { Volume } from 'molstar/lib/mol-model/volume';
import { Loci } from 'molstar/lib/mol-model/loci';
import { Vec3 } from 'molstar/lib/mol-math/linear-algebra';

import 'molstar/lib/mol-plugin-ui/skin/light.scss';

// ── Helpers ───────────────────────────────────────────────────────────────────

// Fetch a CCP4 file and return the parsed Mol* volume StateObjectSelector.
// In Mol* 5.x, Ccp4Provider.parse returns { format, volume } (singular).
async function _loadCcp4Volume(plugin, url, label) {
    const resp = await fetch(url, { credentials: 'include' });
    if (!resp.ok) throw new Error(`HTTP ${resp.status} fetching ${label}`);
    const buffer = await resp.arrayBuffer();
    const data = await plugin.builders.data.rawData(
        { data: new Uint8Array(buffer), label },
        { state: { isGhost: true } }
    );
    const provider = plugin.dataFormats.get('ccp4');
    if (!provider) throw new Error('CCP4 format provider not registered in Mol*');
    const parsed = await provider.parse(plugin, data, { entryId: label });
    // Mol* 5.x: { format, volume }; older versions: { volumes[] }
    const volumeRef = parsed.volume ?? parsed.volumes?.[0];
    if (!volumeRef) throw new Error(`No volume parsed for ${label}`);
    return volumeRef;
}

// Build a clip parameter object
function _getClipParam(center, radius, showFull) {
    if (showFull) return { variant: 'none' };
    return {
        variant: 'pixel',
        objects: [
            {
                type: 'sphere',
                invert: true,  // true: clip everything OUTSIDE the sphere. false: clip inside.
                position: Vec3.clone(center),
                rotation: { axis: Vec3.create(1, 0, 0), angle: 0 },
                scale: Vec3.create(radius, radius, radius),
            },
        ],
    };
}

// Add a single isosurface representation to a volume StateObjectSelector.
// Returns the state-tree cell ref so we can update its clip params later.
async function _addIsosurface(plugin, volumeRef, colorHex, isoValue, clipCenter, clipRadius, showFull) {
    const params = createVolumeRepresentationParams(plugin, volumeRef.data, {
        type: 'isosurface',
        typeParams: {
            isoValue: Volume.IsoValue.relative(isoValue),
            visuals: ['wireframe'],   // classic crystallographic cage look
            alpha: 1.0,
            tryUseGpu: false,
            // Negative isosurfaces have inward-pointing normals — flip them.
            flipSided: isoValue < 0,
            doubleSided: false,
            clip: _getClipParam(clipCenter, clipRadius, showFull),
        },
        color: 'uniform',
        colorParams: { value: Color(colorHex) },
    });
    const repr = await plugin.build()
        .to(volumeRef)
        .apply(StateTransforms.Representation.VolumeRepresentation3D, params)
        .commit();
    return repr;
}

// Update the clip sphere on multiple volume representation nodes in a single commit.
async function _updateClipSpheres(plugin, reprRefs, center, radius, showFull) {
    const update = plugin.build();
    let hasChanges = false;
    for (const reprRef of reprRefs) {
        if (!reprRef?.ref) continue;
        if (!plugin.state.data.cells.has(reprRef.ref)) continue;
        update.to(reprRef).update(old => ({
            ...old,
            type: {
                ...old.type,
                params: {
                    ...old.type?.params,
                    clip: _getClipParam(center, radius, showFull),
                },
            },
        }));
        hasChanges = true;
    }
    if (hasChanges) {
        await update.commit();
    }
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function MolstarViewer({ modelUrl, fileType, map2fofcUrl, mapFofcUrl, style }) {
    const containerRef = useRef(null);
    const pluginRef = useRef(null);
    const rootRef = useRef(null);

    const [clipRadius, setClipRadius] = useState(10);
    const [showFullMap, setShowFullMap] = useState(false);
    const activeReprRefs = useRef([]);
    const lastFocusCenter = useRef(null);
    const clipRadiusRef = useRef(10);
    const showFullMapRef = useRef(false);

    // Keep the ref in sync so the async callbacks always see the latest value
    // without needing to re-bind the subscription
    useEffect(() => {
        clipRadiusRef.current = clipRadius;
        showFullMapRef.current = showFullMap;
    }, [clipRadius, showFullMap]);

    // When clipRadius or showFullMap changes, update all existing density spheres (debounced)
    useEffect(() => {
        if (!pluginRef.current || !lastFocusCenter.current) return;
        const handler = setTimeout(() => {
            _updateClipSpheres(pluginRef.current, activeReprRefs.current, lastFocusCenter.current, clipRadius, showFullMap).catch(() => {});
        }, 150);
        return () => clearTimeout(handler);
    }, [clipRadius, showFullMap]);

    useEffect(() => {
        if (!containerRef.current) return;

        let cancelled = false;
        let focusSub = null;
        activeReprRefs.current = [];

        (async () => {
            // ── Create plugin with RCSB-style layout ──────────────────────────
            const plugin = await createPluginUI({
                target: containerRef.current,
                spec: {
                    ...DefaultPluginUISpec(),

                    layout: {
                        initial: {
                            isExpanded: false,
                            showControls: true,
                            controlsDisplay: 'reactive',
                            regionState: {
                                left: 'hidden',      // hide left state-tree panel
                                top: 'full',         // show top sequence bar
                                right: window.innerWidth < 768 ? 'hidden' : 'full', // hide right sidebar on mobile
                                bottom: 'hidden',    // hide bottom developer log
                            }
                        },
                    },
                    config: [
                        [PluginConfig.VolumeStreaming.Enabled, false],
                        [PluginConfig.Viewport.ShowAnimation, false],
                    ],
                },
                render: (component, element) => {
                    if (!rootRef.current) rootRef.current = createRoot(element);
                    rootRef.current.render(component);
                },
            });

            if (cancelled) { plugin.dispose(); return; }
            pluginRef.current = plugin;

            // Force hide left and bottom panels (safeguard for Molstar 5.x)
            plugin.layout.setProps({
                regionState: {
                    left: 'hidden',
                    bottom: 'hidden',
                    top: 'full',
                    right: window.innerWidth < 768 ? 'hidden' : 'full'
                }
            });

            // ── Load structure ────────────────────────────────────────────────
            let structureCenter = Vec3.create(0, 0, 0);

            if (modelUrl) {
                const format = fileType === 'pdb' ? 'pdb' : 'mmcif';
                try {
                    const response = await fetch(modelUrl, { credentials: 'include' });
                    if (!response.ok) throw new Error(`HTTP ${response.status}`);
                    const text = await response.text();
                    const data = await plugin.builders.data.rawData(
                        { data: text, label: 'model' }, { state: { isGhost: true } }
                    );
                    const trajectory = await plugin.builders.structure.parseTrajectory(data, format);
                    const model = await plugin.builders.structure.createModel(trajectory);
                    const structure = await plugin.builders.structure.createStructure(model, { name: 'model', params: {} });
                    
                    // Apply Molstar's default preset which automatically handles Polymer (cartoon), 
                    // Ligand/Water/Ion (ball-and-stick) and the dynamic Focus target/surroundings.
                    await plugin.builders.structure.representation.applyPreset(structure, 'auto');

                    // Compute the center of the loaded structure for the initial
                    // clip sphere position.
                    if (structure?.obj?.data) {
                        const { boundary } = structure.obj.data;
                        if (boundary?.sphere?.center) {
                            Vec3.copy(structureCenter, boundary.sphere.center);
                        }
                    }
                } catch (err) {
                    console.error('Failed to load model:', err);
                }
            }

            if (cancelled) return;
            lastFocusCenter.current = Vec3.clone(structureCenter);

            // ── 2Fo-Fc map: blue at σ = 1.0 ──────────────────────────────────
            if (map2fofcUrl) {
                try {
                    const vol = await _loadCcp4Volume(plugin, map2fofcUrl, '2Fo-Fc');
                    const r = await _addIsosurface(plugin, vol, 0x3377bb, 1.0, structureCenter, clipRadiusRef.current, showFullMapRef.current);
                    activeReprRefs.current.push(r);
                } catch (err) {
                    console.warn('2Fo-Fc map not loaded:', err.message);
                }
            }

            // ── Fo-Fc map: green +3σ and red −3σ from the same volume ─────────
            if (mapFofcUrl) {
                try {
                    const vol = await _loadCcp4Volume(plugin, mapFofcUrl, 'Fo-Fc');
                    const r1 = await _addIsosurface(plugin, vol, 0x33aa44,  3.0, structureCenter, clipRadiusRef.current, showFullMapRef.current);
                    const r2 = await _addIsosurface(plugin, vol, 0xcc3333, -3.0, structureCenter, clipRadiusRef.current, showFullMapRef.current);
                    activeReprRefs.current.push(r1, r2);
                } catch (err) {
                    console.warn('Fo-Fc map not loaded:', err.message);
                }
            }

            // ── Subscribe to focus changes ────────────────────────────────────
            // When the user clicks a residue / entity the density display
            // sphere follows the new focus center.
            if (activeReprRefs.current.length > 0) {
                focusSub = plugin.managers.structure.focus.behaviors.current.subscribe(
                    async (entry) => {
                        if (!entry?.loci) return;
                        const center = Loci.getCenter(entry.loci);
                        if (!center) return;
                        
                        lastFocusCenter.current = Vec3.clone(center);
                        
                        _updateClipSpheres(plugin, activeReprRefs.current, center, clipRadiusRef.current, showFullMapRef.current).catch(() => {});
                    }
                );
            }
        })();

        return () => {
            cancelled = true;
            if (focusSub) { focusSub.unsubscribe(); focusSub = null; }
            if (pluginRef.current) { pluginRef.current.dispose(); pluginRef.current = null; }
            if (rootRef.current) { rootRef.current.unmount(); rootRef.current = null; }
        };
    }, [modelUrl, fileType, map2fofcUrl, mapFofcUrl]);

    const hasMaps = !!(map2fofcUrl || mapFofcUrl);

    return (
        <div style={{ width: '100%', height: '100%', position: 'relative', ...style }}>
            <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
            {hasMaps && (
                <div style={{
                    position: 'absolute',
                    bottom: '20px',
                    left: '20px',
                    zIndex: 100,
                    background: 'rgba(255, 255, 255, 0.95)',
                    padding: '12px 16px',
                    borderRadius: '8px',
                    boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                    fontFamily: 'system-ui, -apple-system, sans-serif',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '8px'
                }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span style={{ fontSize: '13px', fontWeight: '600', color: '#333' }}>Density Radius</span>
                        <span style={{ fontSize: '13px', fontWeight: '600', color: showFullMap ? '#aaa' : '#007bff' }}>
                            {showFullMap ? '∞' : `${clipRadius} Å`}
                        </span>
                    </div>
                    
                    <label style={{ fontSize: '12px', fontWeight: 'normal', display: 'flex', alignItems: 'center', cursor: 'pointer', paddingLeft: '8px' }}>
                        <input 
                            type="checkbox" 
                            checked={showFullMap} 
                            onChange={(e) => setShowFullMap(e.target.checked)} 
                            style={{ margin: 0, marginRight: '6px' }}
                        />
                        Full Map
                    </label>

                    <input 
                        type="range" 
                        min="3" 
                        max="25" 
                        step="1" 
                        value={clipRadius} 
                        onChange={(e) => setClipRadius(Number(e.target.value))} 
                        disabled={showFullMap}
                        style={{ width: '150px', cursor: showFullMap ? 'not-allowed' : 'pointer', opacity: showFullMap ? 0.5 : 1 }}
                    />
                </div>
            )}
        </div>
    );
}