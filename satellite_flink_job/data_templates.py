INDICES_THRESHOLDS = {
    
    "NDVI_low":  0.3,
    "NDVI_verylow":  0.1,
    
    "NDMI_low": 0.1,
    "NDMI_verylow": -0.1,
    
    "NDWI_low": 0.0,
    
    "NBR_potential_burn":0.2,
    "NBR_strong_burn":-0.1
}

BANDS_THRESHOLDS = {

    "healthy_nir": 0.3,

    "low_termal_B11": 0.2,
    "low_termal_B12": 0.2,

    "vegetation_ratio": 2.0
}

# Constants for fire detection thresholds
FIRE_DETECTION_THRESHOLDS = {
    'NDVI': -0.3,      # Normalized Difference Vegetation Index (healthy vegetation > 0.2)
    'NDMI': -0.15,     # Normalized Difference Moisture Index (dry conditions < -0.1)
    'NDWI': -0.2,      # Normalized Difference Water Index (water stress < -0.1)
    'NBR': -0.2        # Normalized Burn Ratio (burned areas < -0.1)
}

# Band-based fire indicators (higher values suggest fire/burn)
FIRE_BAND_THRESHOLDS = {
    'B4': 0.3,    # Red band - elevated in fire/smoke
    'B11': 0.2,   # SWIR1 - sensitive to burn scars
    'B12': 0.2    # SWIR2 - hot spot detection
}

# Area covered per pixel in km²
PIXEL_AREA_KM2 = 20