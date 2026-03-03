import numpy as np
import re

def hkl_to_matrix(op):
    """Convert 'h+k, -h, l' to 3x3 matrix."""
    # Define mapping for indices
    idx_map = {'h': 0, 'k': 1, 'l': 2}
    
    terms = op.replace(' ', '').split(',')
    rows = []
    
    for t in terms:
        row = [0, 0, 0]
        # Find all occurrences of [+-]?[number]?[hkl]
        # Regex: ([+-]?\d*)([hkl])
        matches = re.findall(r'([+-]?\d*)([\.?\d]*)([hkl])', t)
        
        for sign, num, var in matches:
            idx = idx_map[var]
            
            # Determine coefficient
            val = 1.0
            if sign == '-': val = -1.0
            elif sign == '+': val = 1.0
            
            if num:
                val *= float(num)
                
            row[idx] += val
            
        rows.append(row)
        
    return np.array(rows)

if __name__ == "__main__":
    op = 'k, h-l, l'
    print(f"Op: {op}")
    print(hkl_to_matrix(op))
    
    op2 = 'h+k, -h, l'
    print(f"Op: {op2}")
    print(hkl_to_matrix(op2))