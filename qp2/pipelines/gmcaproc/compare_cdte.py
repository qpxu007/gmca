
import sys
import numpy as np
import matplotlib.pyplot as plt
from io import StringIO

# Add path to access signal_CdTe
sys.path.insert(0, "/home/qxu/data-analysis")
from qp2.pipelines.gmcaproc import signal_CdTe

def parse_nist_data():
    data = []
    lines = signal_CdTe.CdTe_NIST.strip().split('\n')
    for line in lines:
        parts = line.split()
        if len(parts) >= 3:
            try:
                e = float(parts[0]) * 1000.0 # keV to eV
                mu_rho = float(parts[1])
                mu_en_rho = float(parts[2])
                data.append([e, mu_rho, mu_en_rho])
            except ValueError:
                continue
    return np.array(data)

def compare():
    print("Comparing cal_silicon_CdTe with NIST data...")
    nist_data = parse_nist_data()
    
    energies = nist_data[:, 0]
    mu_rho_vals = nist_data[:, 1]
    mu_en_rho_vals = nist_data[:, 2]
    
    cal_vals = []
    density_CdTe = 5.85 # g/cm^3
    
    # Calculate cal_silicon_CdTe for each energy in NIST data
    for e in energies:
        try:
            # cal_silicon_CdTe returns linear attenuation coefficient in mm^-1
            val = signal_CdTe.cal_silicon_CdTe(e) 
            cal_vals.append(val)
        except Exception as e:
            cal_vals.append(np.nan)
            
    cal_vals = np.array(cal_vals)
    
    # Convert linear attenuation (mm^-1) to mass attenuation (cm^2/g)
    # mu_mass = mu_linear * 10 / density
    # mu_linear is in mm^-1 = 10 cm^-1
    # so mu_linear (cm^-1) = val * 10
    # mu_mass = (val * 10) / density
    
    cal_mass_vals = (cal_vals * 10.0) / density_CdTe
    
    print(f"{'Energy(eV)':<12} {'NIST_mu/rho':<15} {'Calc_mu/rho':<15} {'Ratio':<10}")
    print("-" * 55)
    
    # Select a few points to print
    indices = np.linspace(0, len(energies)-1, 15, dtype=int)
    for i in indices:
        e = energies[i]
        n_val = mu_rho_vals[i]
        c_val = cal_mass_vals[i]
        ratio = c_val / n_val if n_val != 0 else 0
        print(f"{e:<12.1f} {n_val:<15.4e} {c_val:<15.4e} {ratio:<10.4f}")

    # Check correlation with mu_en/rho as well
    print("\n")
    print(f"{'Energy(eV)':<12} {'NIST_mu_en/rho':<15} {'Calc_mu/rho':<15} {'Ratio':<10}")
    print("-" * 55)
    for i in indices:
        e = energies[i]
        n_val = mu_en_rho_vals[i] # mu_en/rho
        c_val = cal_mass_vals[i]
        ratio = c_val / n_val if n_val != 0 else 0
        print(f"{e:<12.1f} {n_val:<15.4e} {c_val:<15.4e} {ratio:<10.4f}")

if __name__ == "__main__":
    compare()
