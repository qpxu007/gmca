


# the following was included in the j app cryst manuscript:

The Raster3d pipeline combines several tools in the package to analyze two orthogonal raster scans and generate recommendations for subsequent data collection. Building on the 3D volume reconstruction and hotspot detection capabilities described in Section 2.2, it automates the workflow from diffraction analysis to collection planning. A collection tracker first identifies candidate raster pairs from consecutive collection events with the same base name and compatible scan modes; the pipeline worker then verifies that the two scans are approximately orthogonal from their omega angles. Once a valid pair is established, the pipeline proceeds through four stages: (1) polling for DOZOR or nXDS analysis results, with optional resubmission of missing jobs; (2) reconstructing a 3D diffraction volume by combining the two 2D raster maps, followed by hotspot detection and PCA-based estimation of crystal size and orientation; (3) running XDS and MOSFLM strategy calculations on the best crystal position to estimate indexing and collection parameters, including space group, unit cell, mosaicity, oscillation range, and detector distance; and (4) performing a dose-aware search over beam size, attenuation, exposure time, and number of images, followed by RADDOSE-3D validation of the best candidate using crystal dimensions derived from the 3D reconstruction. Before reconstruction, a configurable quality gate can reject samples that do not show sufficient diffraction, based on score, resolution, and the number of strong frames. By default, the pipeline reports up to ten candidate crystal sites. For each accepted site, the output can include both voxel-space coordinates and motor-space centering positions, enabling direct transfer of the recommended target to beamline control software. For elongated crystals, the principal crystal axis is compared with the rotation axis: rods aligned with the rotation axis can be assigned to vector (helical) collection with defined start and end centering points, whereas rods with other orientations are treated with standard single-position collection. The pipeline also detects crystals that overlap along the rotation axis and applies a configurable policy to retain the strongest site, keep all sites, or skip overlapping candidates.



# Next-Generation AI Enhancements for the 3D Raster Analysis Pipeline




AI suggestions:


The 3D Raster Analysis pipeline (`qp2/pipelines/raster_3d`) currently relies on mathematical reconstructions (multiplying orthogonal 2D heatmaps), algebraic overlap detection, and deterministic classical grid searches to find collection strategies. Introducing Artificial Intelligence to this specific pipeline can drastically improve recovery rates on weak diffraction and optimize radiation dose logic.

The following represent concrete AI mechanisms that can be bolted directly into the `raster3d` logic chain:

---

### 1. 3D Generative CNNs for Crystal Volume Reconstruction (Stage 1)
Currently, Stage 1 builds a 3D volume by mapping 2D DOZOR score arrays against each other and finding hotspots. Because protein crystals are non-uniform and routinely jagged rather than perfect cubes, algebraic intersection often assumes bounding-box approximations or ghosts void spaces.
* **The AI Role:** A **3D Convolutional Neural Network (3D CNN)** or Volumetric Autoencoder could be trained to ingest the two 2D heatmaps and predict the exact 3D morphology (shape, boundaries, and hidden void spaces) of the crystal within the loop mount. This approach would provide drastically more accurate sub-pixel voxel mapping than orthogonal multiplication.

### 2. Deep-Learning Driven Sparse Indexing (Stage 2 Fallback)
Grid scan frames generated during rastering are kept intentionally weak to prevent radiation damage before the actual primary collection begins. Because of this weak signal, classical indexing via `XDS` or `MOSFLM` (in Stage 2) remains highly prone to returning un-indexed datasets or incorrect unit cells.
* **The AI Role:** Train a Vision Transformer or equivalent specialized vision network directly on sparse, ultra-low-dose diffraction patterns. Even when only 5 to 10 hazy spots are visibly apparent, the AI could predict the **Space Group and Unit Cell** computationally across a trained probability matrix. The pipeline could query this network to confidently formulate a collection strategy even when classical indexers fail entirely.

### 3. Smart Overlap Deconvolution
In the current implementation, if two crystals overlap on the rotation axis (defined by the Stage 1 `overlap_policy`), the pipeline uses blunt logic to either discard the lesser crystal (`"best"`) or try to naively collect overlapping multi-crystal patterns (`"all"`).
* **The AI Role:** An AI pathing agent could dynamically generate specialized, multi-pass collection paths. For example, if it detects a crystal overlap directly at exactly the 45° rotation vector, it might instruct Bluice to only collect a sub-wedge to avoid the collision angle. This maximizes usable data stripped from *both* crystals while sidestepping overlapping frames entirely.

### 4. Reinforcement Learning for Dynamic Dose Grids (Stage 3)
Presently, Stage 3 executes a fixed, discrete grid search across hardcoded arrays (e.g., `[5, 10, 20]` for beam sizes) and validates the best combinatorial hit against `RADDOSE-3D`.
* **The AI Role:** A **Reinforcement Learning (RL)** model can natively learn optimal attenuations, beam-sizes, and exposure times. Most radically, instead of forcing a flat, homogenized exposure across an entire 360-degree sweep, the RL agent could output a *variable-exposure plan*. It could trigger the motors to spin faster or attenuate the beam heavily precisely when the crystal's thickest volume rotates to face the beam, actively minimizing localized radiation burn on the fly.

### 5. Protein-Sequence Aware Radiation Damage Prediction
Currently, the pipeline uses global generalized metrics to calculate dosage limits (utilizing Howells criterion alongside RADDOSE-3D estimates).
* **The AI Role:** Coupled with in-silico integrations like AlphaFold, a Graph Neural Network could automatically ingest the raw protein sequence string. The network would identify structurally vulnerable disulfide bonds and heavy atom radiolytic hotspots and predict the *exact* radiation decay degradation curve specific to that molecule. This shifts the dose recommendation architecture from empirically generalized math to targeted structural biology awareness.
