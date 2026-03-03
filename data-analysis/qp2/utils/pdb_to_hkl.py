
import gemmi
import math
import os

def generate_reference_hkl(pdb_path, output_path, dmin=2.0):
    """
    Generate a reference HKL file from a PDB file using gemmi.
    
    Args:
        pdb_path (str): Path to the input PDB file.
        output_path (str): Path to the output HKL file.
        dmin (float): Minimum resolution in Angstroms.
    """
    # 1) Read model
    st = gemmi.read_structure(pdb_path)
    model = st[0]

    # 2) Put model density on a grid up to dmin (X-ray; fast FFT method)
    dc = gemmi.DensityCalculatorX()
    dc.d_min = dmin
    dc.grid.setup_from(st)                       # grid dimensions from cell + d_min
    dc.set_refmac_compatible_blur(model)         # sets dc.blur in a Refmac-like way
    dc.put_model_density_on_grid(model)

    # 3) FFT: map -> (F,phi) reciprocal grid
    sf_grid = gemmi.transform_map_to_f_phi(dc.grid, half_l=True)

    # 4) Extract unique reflections in ASU up to dmin
    # prepare_asu_data() returns an AsuData with HKLs and complex values (F * exp(i phi))
    asu = sf_grid.prepare_asu_data(dmin=dmin, unblur=dc.blur)

    # 5) Write XDS_ASCII format
    # Header
    unit_cell = st.cell
    uc_str = f"{unit_cell.a:.3f} {unit_cell.b:.3f} {unit_cell.c:.3f} {unit_cell.alpha:.3f} {unit_cell.beta:.3f} {unit_cell.gamma:.3f}"
    
    try:
        sg_num = st.find_spacegroup().number
    except:
        sg_num = 0 # Fallback or error handling
        
    with open(output_path, "w") as f:
        f.write("!FORMAT=XDS_ASCII\n")
        f.write(f"!UNIT_CELL_CONSTANTS= {uc_str}\n")
        f.write(f"!SPACE_GROUP_NUMBER= {sg_num}\n")
        f.write("!NUMBER_OF_ITEMS_IN_EACH_DATA_RECORD=   5\n")
        f.write("!ITEM_H=1\n")
        f.write("!ITEM_K=2\n")
        f.write("!ITEM_L=3\n")
        f.write("!ITEM_IOBS=4\n")
        f.write("!ITEM_SIGMA(IOBS)=5\n")
        f.write("!END_OF_HEADER\n")
        
        for hkl, val in zip(asu.miller_array, asu.value_array):
            h, k, l = hkl
            F = abs(val)
            IOBS = F * F
            SIGMA = 1.0 # arbitrary sigma for reference
            f.write(f"{h:4d} {k:4d} {l:4d} {IOBS:12.4e} {SIGMA:12.4e}\n")
            
        f.write("!END_OF_DATA\n")

    return output_path
