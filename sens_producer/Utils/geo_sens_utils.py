
# Utilities
from Utils.db_utils import connect_to_db
import hashlib
import random


def get_number_of_stats(macroarea_i: int, microarea_i:int) -> int:
    """
    
    """
    try:
        # Database connection
        conn = connect_to_db()
        cur = conn.cursor()
        
        # Retrive number of sensor stations for given microarea
        microarea_id = f"A{macroarea_i}-M{microarea_i}"
        
        cur.execute(("""
            SELECT numof_sens_stations 
            FROM n_sens_stations
            WHERE microarea_id = %s
        """), (microarea_id,))
        result =  cur.fetchone()

        if result is None:
            raise ValueError(f"No entry found for microarea ID '{microarea_id}'")
        
        return result[0]
    
    except Exception as e:
        raise ValueError(f"Error during connection to 'n_sens_stations': {e}")
    
    finally:
        try: 
            if cur: cur.close()
            if conn: conn.close()
        except Exception as e:
            print(f"[ERROR] Not able to close connection: {e}")


def generate_measurements_json(
    stations_i: int, 
    microarea_i: int, 
    macroarea_i: int, 
    timestamp: str, 
    margin: float = 0.95
) -> dict:
    """
    Generates a dict simulating environmental sensor data.
    For consistency, fire_event is deterministically assigned based on station ID.
    
    Parameters:
        stations_i (int): Station index
        microarea_i (int): Microarea index
        macroarea_i (int): Macroarea index
        margin (float): Percentage of threshold to use as safe upper bound
    """
    try:
        # Define sensor thresholds
        thresholds = {
            "temperature_c": 35.0,
            "humidity_percent": 80.0,
            "co2_ppm": 600.0,
            "pm25_ugm3": 12.0,
            "smoke_index": 20.0,
            "infrared_intensity": 0.2,
            "battery_voltage": 3.7
        }

        # Build station_id
        station_id = f"S_A{macroarea_i}-M{microarea_i}_{stations_i:03}"

        # Deterministic fire_event based on station hash
        hash_digest = hashlib.md5(station_id.encode()).hexdigest()
        # Example: "a1b2c3d4e5f6789012345678abcdef90"
        # Uniform Distribution: MD5 hash distributes values uniformly across 0-99 range
        hash_value = int(hash_digest[:8], 16)  # Use part of hash to avoid full int overflow
        # Takes first 8 chars: "a1b2c3d4" → converts to integer
        # Example: 2712847316 (decimal)
        fire_event = (hash_value % 100) < 20  # 20% of stations deterministically simulate fire
        # 2712847316 % 100 = 16
        # If fire_probability = 20: 16 < 20 → True (fire)
        # If fire_probability = 20: 85 < 20 → False (no fire)
        # 20% chance of getting a value under 20
        
        if fire_event:
            measurements = {
                "temperature_c": round(random.uniform(thresholds["temperature_c"] + 2, thresholds["temperature_c"] + 10), 2),
                "humidity_percent": round(random.uniform(5.0, 20.0), 2),
                "co2_ppm": round(random.uniform(thresholds["co2_ppm"] + 50, thresholds["co2_ppm"] + 300), 2),
                "pm25_ugm3": round(random.uniform(thresholds["pm25_ugm3"] + 5, thresholds["pm25_ugm3"] + 50), 2),
                "smoke_index": round(random.uniform(thresholds["smoke_index"] + 5, thresholds["smoke_index"] + 20), 2),
                "infrared_intensity": round(random.uniform(thresholds["infrared_intensity"] + 0.1, 1.0), 3),
                "battery_voltage": round(random.uniform(3.4, thresholds["battery_voltage"]), 2)
            }
        else:
            safe_upper = {key: val * margin for key, val in thresholds.items()}
            measurements = {
                "temperature_c": round(random.uniform(15.0, safe_upper["temperature_c"]), 2),
                "humidity_percent": round(random.uniform(30.0, safe_upper["humidity_percent"]), 2),
                "co2_ppm": round(random.uniform(350.0, safe_upper["co2_ppm"]), 2),
                "pm25_ugm3": round(random.uniform(2.0, safe_upper["pm25_ugm3"]), 2),
                "smoke_index": round(random.uniform(0.0, safe_upper["smoke_index"]), 2),
                "infrared_intensity": round(random.uniform(0.0, safe_upper["infrared_intensity"]), 3),
                "battery_voltage": round(random.uniform(3.4, safe_upper["battery_voltage"]), 2)
            }

        return {
            "station_id": station_id,
            "timestamp": timestamp,
            "measurements": measurements
        }

    except Exception as e:
        raise SystemError(f"Failed to generate measurements for station '{station_id}', error: {e}")
    
    