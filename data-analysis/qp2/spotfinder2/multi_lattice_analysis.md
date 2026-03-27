# Multi-Lattice Spot Separation Analysis

Approaches for separating Bragg spots from multiple crystals in a single diffraction image.

---

## 1. Reciprocal-Space Clustering (most common)

Convert spot positions to reciprocal-space vectors **q**, then find lattice periodicity:

- **CrystFEL `indexamajig`** feeds all spots to an indexing algorithm (e.g. MOSFLM, XDS, Felix, `pinkIndexer`) and then **removes indexed spots** and re-indexes the remainder. Repeat until no more lattices are found.
- **DIALS `dials.find_spots` → `dials.index`** has a `max_lattices=N` option that does the same iterative peel-off.

**Implementation sketch:**
```python
spots = find_all_spots(frame)
remaining = spots
crystals = []
while remaining.count > min_spots:
    lattice = try_index(remaining)   # FFT-based or DPS
    if lattice is None:
        break
    assigned, remaining = partition(remaining, lattice)
    crystals.append((lattice, assigned))
```

This is the **gold standard** but requires a full indexing engine.

---

## 2. Inter-Spot Distance Clustering (lightweight, no unit cell needed)

Cluster spots by their **pairwise distance patterns** in reciprocal space:

- Compute all pairwise **Δq** vectors between spots
- Look for **repeated Δq vectors** — spots from the same crystal will have the same lattice vectors, so their pairwise differences cluster around lattice vector multiples
- Use a 3D Hough transform or FFT on the Δq distribution to find dominant periodicities
- Assign spots to whichever lattice they're consistent with

This is essentially what **Felix** and **SPIND** do. It's faster than full indexing and doesn't require a known unit cell.

---

## 3. Azimuthal Pairing (very fast heuristic)

If crystals have different orientations, their spots fall on different **lunes** (circles in reciprocal space). A quick heuristic:

- Group spots by resolution shell
- Within each shell, look for azimuthal clusters (peaks that form regular angular spacings)
- Each regular angular pattern = one crystal

This doesn't fully solve the problem but can quickly estimate the **number of crystals** and flag multi-lattice frames.

---

## 4. Intensity-Based Separation

Different crystals often have systematically different intensities (due to size, orientation, exposure):

- After indexing one lattice, check if the **residual spots** have a systematically different intensity distribution
- Can help as a secondary criterion alongside geometric methods

---

## Recommended Architecture for spotfinder2

A **two-stage approach** keeps concerns separated:

| Stage | Module | Purpose |
|---|---|---|
| **Detection** | `spotfinder2` (current) | Find ALL spots regardless of crystal |
| **Separation** | New `lattice_finder` module | Assign spots to crystals |

The separation module could implement:
1. **FFT-based auto-indexing** (like CrystFEL's `dirax` or `mosflm` algorithms) for the first lattice
2. **Iterative peel-off** — remove indexed spots, repeat
3. Return a `crystal_id` array that maps each spot to its crystal

This keeps `spotfinder2` focused on detection (where it's fast) and puts the crystallographic logic in a dedicated module.
