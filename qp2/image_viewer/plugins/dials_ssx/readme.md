
cd $DIALS
./conda_base/condabin/conda install redis-py

--- from chatgpt5

After `dials.ssx_integrate` on many datasets, the usual next steps are: enforce consistent symmetry/indexing across all crystals, scale (and filter) the integrated intensities, then merge and export for phasing/refinement.[1][2][3]

Below is a typical manual workflow once all `integrated_*.expt` / `integrated_*.refl` exist.

## Gather integrated outputs

- Collect all the `integrated_*.expt` and `integrated_*.refl` files produced by `dials.ssx_integrate` across batches; these form your multi-crystal stills dataset.[1]
- You can either pass all of these directly to downstream programs, or first collapse them to a single experiments/reflections pair with `dials.combine_experiments integrated_*.expt integrated_*.refl` if you prefer.[3][4]

## Symmetry and consistent indexing

- If Laue group/space group is not completely trusted, run `dials.cosym` on all integrated files to determine Laue symmetry and reindex everything to a consistent setting, e.g.  
  `dials.cosym integrated_0.expt ... integrated_N.expt integrated_0.refl ... integrated_N.refl`.[3]
- If the space group is known and you just want to enforce it, you can instead use `dials.symmetry` to apply the chosen symmetry and output symmetrized experiments/reflections for scaling.[3]

## Scaling and filtering

- Scale all (symmetrized) datasets together with `dials.scale`, which applies stills-aware scaling and error models and outputs `scaled.expt` / `scaled.refl` plus a `dials.scale.html` report, for example:  
  `dials.scale symmetrized.expt symmetrized.refl`.[2][3]
- Use the scaling report and options like `filtering.method=deltacchalf` and `filtering.deltacchalf.mode=dataset` to identify and remove bad crystals or datasets, then rerun `dials.scale` on the pruned set.[2][3]

## Merging and export

- From the scaled data, either export directly from `dials.scale` (e.g. `output.unmerged_mtz=unmerged.mtz` or `output.merged_mtz=merged.mtz`) or run `dials.merge scaled.expt scaled.refl` to produce merged intensities.[5][2]
- Finally, convert to the format expected by your downstream pipeline (e.g. MTZ/mmCIF) using `dials.export` or via the MTZ output from `dials.scale`, then proceed to phasing and refinement in e.g. PHENIX or CCP4.[4][2]

If you want this automated end-to-end for SSX, the `xia2.ssx` pipeline wraps essentially these same steps (index → ssx_integrate → cosym/symmetry → scale → merge/export) using DIALS under the hood.[6][4]

[1](https://dials.github.io/ssx_processing_guide.html)
[2](https://dials.github.io/documentation/programs/dials_scale.html)
[3](https://dials.diamond.ac.uk/documentation/tutorials/multi_crystal_symmetry_and_scaling.html)
[4](https://xia2.github.io/serial_crystallography.html)
[5](https://journals.iucr.org/d/issues/2020/04/00/di5035/di5035.pdf)
[6](https://pubmed.ncbi.nlm.nih.gov/39608945/)
[7](https://github.com/dials/dials.github.io/blob/master/ssx_processing_guide.html)
[8](https://www.sciencedirect.com/science/article/abs/pii/S0076687924005226)
[9](https://gist.github.com/jbeilstenedmands/34c99139a64efa10e956d26f0a4483e7)
[10](https://sst.dev/docs/workflow/)
[11](https://pmc.ncbi.nlm.nih.gov/articles/PMC8740827/)
[12](https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html)
[13](https://cci.lbl.gov/publications/download/DIALS.pdf)
[14](https://dials.diamond.ac.uk/dials_scale_user_guide.html)
[15](https://github.com/dials/dials/releases)
[16](https://dials.diamond.ac.uk/documentation/tutorials/index.html)
[17](https://dials.diamond.ac.uk/documentation/tutorials/processing_in_detail_betalactamase.html)
[18](https://dials.github.io/documentation/tutorials/3DED/MyD88.html)
[19](https://dials.github.io/dials-2.2/documentation/programs/dials_refine.html)
[20](https://www.youtube.com/watch?v=moeaBbg2ewg)
[21](https://pmc.ncbi.nlm.nih.gov/articles/PMC4822564/)
[22](https://github.com/dials/dials/discussions/1407)
[23](https://journals.iucr.org/paper?di5035)
[24](https://www.biorxiv.org/content/10.1101/2025.05.04.652045v1.full-text)
[25](https://journals.iucr.org/paper?qq5001)
[26](https://www.biorxiv.org/content/10.1101/2022.07.28.501725.full)
[27](https://www.nature.com/articles/s42004-024-01360-7)
[28](https://keedylab.org/pdf/2023_sharma_ssx.pdf)

