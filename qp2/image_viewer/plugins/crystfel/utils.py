
import numpy as np
import logging

def calculate_robust_threshold_mad(data, mask=None):
    """
    Calculates a robust threshold using Median Absolute Deviation (MAD).
    Formula: Threshold = Median + 10 * MAD + 1
    
    Args:
        data (np.ndarray): The image data.
        mask (np.ndarray, optional): Boolean mask where True indicates masked (bad) pixels.
    
    Returns:
        float: Calculated threshold, or None if calculation fails or no valid data.
    """
    if data is None or data.size == 0:
        return None

    valid_data = data
    if mask is not None:
        if mask.shape == data.shape:
             valid_data = data[~mask]
        else:
             logging.warning(f"calculate_robust_threshold: Mask shape {mask.shape} != Image shape {data.shape}")
    
    # Exclude non-positive values (background/masked 0s)
    valid_data = valid_data[valid_data > 0]
    
    if valid_data.size == 0:
        return None

    try:
        median = np.nanmedian(valid_data)
        mad = np.nanmedian(np.abs(valid_data - median))
        threshold = median + 10.0 * mad + 1.0
        return max(threshold, 1.0)
    except Exception as e:
        logging.error(f"Error calculating robust threshold: {e}")
        return None
