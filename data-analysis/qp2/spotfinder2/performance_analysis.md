# spotfinder2 Performance Analysis

Analysis of bottlenecks and optimization opportunities, ordered from highest to lowest impact.

---

## 🔴 Critical Bottlenecks

### 1. `refine_centroids()` — Python loop + `scipy.optimize.minimize` per spot

**File:** `refinement.py:70-109`

This is almost certainly the single slowest stage. It calls `scipy.optimize.minimize(method="L-BFGS-B")` **once per spot** in a Python for-loop. For a frame with 200 spots, that's 200 separate optimizer calls with function evaluations, gradient computations, etc.

**Speed-up options:**
- **Analytic sub-pixel centroid** — Replace the full MLE optimizer with a closed-form parabolic/Gaussian fit on the 3×3 or 5×5 peak (one `np.linalg.solve` call per spot, or even fully vectorized). This is what CrystFEL and DIALS actually use for initial refinement — the MLE is overkill for sub-pixel positioning.
- **Vectorized batch refinement** — Pre-extract all cutouts into a 3D array `(n_spots, size, size)`, then do one vectorized Cash statistic + grid search instead of N separate optimizer calls.
- **Skip refinement for weak spots** — Only run MLE on spots above an SNR threshold (e.g. SNR > 5).

**Estimated speed-up: 10–50×** for this stage.

### 2. `integrate_with_tds()` — Same problem, worse

**File:** `refinement.py:160-199`

Same per-spot optimizer loop, with a 2-parameter fit (Bragg + TDS amplitude). Even more expensive since the cutout is larger (11×11 vs 7×7).

**Speed-up:** Same strategies as above, or use **linear least-squares** (since PSF shapes are fixed, only amplitudes vary — this is a linear problem solvable via `np.linalg.lstsq` in one vectorized call for all spots).

**Estimated speed-up: 20–100×** for this stage.

### 3. `_dispersion_filter_fast()` — Python loop over components

**File:** `detection.py:149-189`

Despite the name `_fast`, this loops over every component in Python calling `np.where`, `comp_mask.sum()`, etc. For 500+ candidate components this is slow.

**Speed-up:**
- Use `scipy.ndimage.labeled_comprehension` or `scipy.ndimage.sum/mean` to vectorize the per-component statistics.
- Or extract centroid + peak value using `scipy.ndimage.center_of_mass` + `scipy.ndimage.maximum` in one call each (they operate on all labels simultaneously).

**Estimated speed-up: 5–20×** for this stage.

---

## 🟡 Medium Impact

### 4. `_compute_properties_fast()` — Python loop over components

**File:** `detection.py:230-281`

Same pattern: `find_objects` + Python loop. Vectorizable with `scipy.ndimage.sum`, `center_of_mass`, etc.

### 5. `filter_by_shape()` — `np.argwhere(labels == comp_id)` per component

**File:** `filtering.py:40-64`

`np.argwhere(labels == comp_id)` scans the **entire image** for each component — O(N_pixels × N_components). Replace with `scipy.ndimage.find_objects` + bounding-box-local masks (like `detection.py` does) to reduce this dramatically.

### 6. `_add_ring_component_fast()` — inner Python loop over azimuthal bins

**File:** `background.py:253-260`

The `for b in range(self.n_azimuthal)` loop with `np.median` per bin. Consider:
- Using `np.percentile` on the sorted array with precomputed offsets (avoiding the per-bin median call)
- Or approximating with mean + sigma-clip (which is the radial background strategy already used)

### 7. Background `estimate()` — multiple full-image scans

**File:** `background.py:104-163`

Several full-image `bincount` operations and array copies. Could benefit from:
- **Downsampling** for the initial background estimate (e.g. 4× downsample → 16× fewer pixels → upsample result)
- **Caching between frames** — if backgrounds change slowly in a dataset, use exponential moving average

---

## 🟢 Lower-hanging Fruit

### 8. Use `numba` for inner loops

The dispersion filter, property computation, and ring fitting inner loops are perfect candidates for `@numba.jit(nopython=True)` — minimal code changes, often 10–50× faster than pure NumPy loops.

### 9. Multi-frame parallelism

`process_dataset()` processes frames **sequentially**. Use `concurrent.futures.ProcessPoolExecutor` or `multiprocessing.Pool` to process multiple frames in parallel. The pipeline is stateless per-frame (after mask initialization), so this is trivially parallelizable.

### 10. ThresholdTable init is slow

**File:** `threshold.py:50-73`

The `__init__` loops over 10,000+ entries calling `stats.poisson.isf()` individually. Vectorize this:
```python
mu_arr = self._mu_values[poisson_mask]
self._thresholds[poisson_mask] = stats.poisson.isf(p_false_alarm, mu_arr)
```

This only runs once at startup, but it adds several seconds of latency.

---

## Summary — Priority Ranking

| Priority | Target | Technique | Expected Gain |
|---|---|---|---|
| **1** | `refine_centroids` | Analytic centroid or vectorized batch | 10–50× for MLE stage |
| **2** | `integrate_with_tds` | Linear least-squares (vectorized) | 20–100× for TDS stage |
| **3** | `_dispersion_filter_fast` | `ndimage.sum/maximum` vectorized | 5–20× for filter stage |
| **4** | `filter_by_shape` | `find_objects` instead of `argwhere` | 5–10× for shape stage |
| **5** | `process_dataset` | Multi-frame parallelism | ~N× (N = cores) |
| **6** | Inner loops | `numba.jit` | 10–50× per loop |
| **7** | Background | Downsample + cache | 2–4× for bg stage |
