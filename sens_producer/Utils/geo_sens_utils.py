
# Utilities
from Utils.db_utils import connect_to_db
from typing import Dict, Tuple
from psycopg2 import sql
import random
import math


class StationsLocationManager():
    def __init__(self):
        self.locations = {}
        
    def get_locations(
            self, 
            microarea_id: str,
            macroarea_id: str,
            min_long: float,
            min_lat: float,
            max_long: float,
            max_lat: float
    ) -> list[Tuple]:
        """Generate or retrieve pixel locations for a specific region"""
        location_id = f"{microarea_id}"
        
        if location_id not in self.locations.keys():
            cells_coords = self._divide_microarea(
                min_long,
                min_lat,
                max_long,
                max_lat
            )
            self.locations[location_id] = cells_coords
        else:
            cells_coords = self.locations[location_id]
            
        return cells_coords

    def _divide_microarea(
            self,
            min_long: float,
            min_lat: float,
            max_long: float,
            max_lat: float,
            max_area_km2: float = 20
    ) -> list[Tuple]:
        """
        Divide microarea into cells and label them as wildfire or vegetation
        """
        # Compute mean latitude to adjust longitude distance
        mean_lat = (min_lat + max_lat) / 2
        km_per_deg_lat = 111  # approx constant
        km_per_deg_long = 111 * math.cos(math.radians(mean_lat))

        # Dimensions of the bounding box in kilometers
        width_km = (max_long - min_long) * km_per_deg_long
        height_km = (max_lat - min_lat) * km_per_deg_lat

        total_area_km2 = width_km * height_km

        # Estimated number of microareas
        num_microareas = math.ceil(total_area_km2 / max_area_km2)

        # Approximate number of columns and rows
        n_cols = math.ceil(math.sqrt(num_microareas * (width_km / height_km)))
        n_rows = math.ceil(num_microareas / n_cols)

        long_step = (max_long - min_long) / n_cols
        lat_step = (max_lat - min_lat) / n_rows

        cells_coords = []

        for i in range(n_rows):
            for j in range(n_cols):

                # Cells boundaries 
                cell_min_long = min_long + j * long_step
                cell_max_long = cell_min_long + long_step
                cell_min_lat = min_lat + i * lat_step
                cell_max_lat = cell_min_lat + lat_step

                # Dynamic center calculation
                center_row_start = n_rows // 3
                center_row_end = (2 * n_rows) // 3
                center_col_start = n_cols // 3  
                center_col_end = (2 * n_cols) // 3

                if center_row_start <= i < center_row_end and center_col_start <= j < center_col_end:
                    label = "wildfire"                
                else:
                    label = "vegetation"

                cells_coords.append((
                    label,
                    cell_min_long,
                    cell_max_long,
                    cell_min_lat,
                    cell_max_lat,
                ))

        return cells_coords


def fetch_micro_bbox_from_db(macro_i: int, micro_i) -> tuple | None:
    """
    Retrieves the bounding box of a specific microarea within a macroarea from the database.

    Args:
        macro_i (int): The index of the macroarea (e.g., 1 for 'A1').
        micro_i (int): The index of the microarea (e.g., 25 for 'A1-M25')

    Returns:
        tuple or None: A tuple of (min_long, min_lat, max_long, max_lat) if a bounding box is found,
                       or None if the macroarea or microarea does not exist.
    """
    conn = connect_to_db()
    cur = conn.cursor()

    table_name = f"microareas"
    microarea_id = f"A{macro_i}-M{micro_i}"

    # Step 3: Fetch bounding box from microarea table
    cur.execute(sql.SQL("""
        SELECT min_long, min_lat, max_long, max_lat
        FROM {}
        WHERE microarea_id = %s
    """).format(sql.Identifier(table_name)), (microarea_id,))

    bbox = cur.fetchone()

    cur.close()
    conn.close()

    if bbox is None:
        raise ValueError(f"[ERROR] No bounding box found for microarea '{microarea_id}' in table '{table_name}'. This indicates a data integrity issue in your DB population process.")

    return bbox, microarea_id


def get_number_of_stats(macroarea_i: int, microarea_i:int) -> int:
    """
        Comment Here!
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


def extract_station_coords(station_id: str) -> Tuple[float, float]:
    """
    Given a station ID, extract its coordinates from the `stations` table.

    Args:
        station_id (str): Station ID to look up.

    Returns:
        Tuple[float, float]: Tuple containing (latitude, longitude).
    """
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        query = sql.SQL("""
            SELECT station_id, latitude, longitude
            FROM stations
            WHERE station_id = %s
        """)

        cur.execute(query, (station_id,))
        rows = cur.fetchall()
        
        if not rows:
            raise ValueError(f"Station {station_id} not found in database")
        
        # Extract the first (and should be only) row
        _, stat_lat, stat_long = rows[0]

        return stat_lat, stat_long

    except Exception as e:
        raise SystemError(f"Failed to extract coordinates: {e}")

    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception as e:
            print(f"[WARNING] Cleanup failed: {e}")


def generate_measurements_json(
    stations_i: int, 
    microarea_i: int, 
    macroarea_i: int, 
    timestamp: str,
    min_long: float,
    min_lat: float, 
    max_long: float,
    max_lat: float,
    margin: float = 0.95
) -> dict:
    """
    Generates a dict simulating environmental sensor data.
    Fire_event is determined by checking if the station is located in a wildfire cell.
    
    Parameters:
        stations_i (int): Station index
        microarea_i (int): Microarea index
        macroarea_i (int): Macroarea index
        timestamp (str): Timestamp for the measurement
        min_long (float): Minimum longitude of microarea bounding box
        min_lat (float): Minimum latitude of microarea bounding box
        max_long (float): Maximum longitude of microarea bounding box
        max_lat (float): Maximum latitude of microarea bounding box
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

        # Fetch coords of station
        stat_lat, stat_long = extract_station_coords(station_id=station_id)

        # Initialize StationsLocationManager and get cell information
        location_manager = StationsLocationManager()
        cells_coords = location_manager.get_locations(
            microarea_id=f"M{microarea_i}",
            macroarea_id=f"A{macroarea_i}",
            min_long=min_long,
            min_lat=min_lat,
            max_long=max_long,
            max_lat=max_lat
        )

        # Determine fire_event by checking which cell the station belongs to
        fire_event = False
        for cell in cells_coords:
            label, cell_min_long, cell_max_long, cell_min_lat, cell_max_lat = cell
            
            # Check if station coordinates fall within this cell's bounding box
            if (cell_min_long <= stat_long <= cell_max_long and 
                cell_min_lat <= stat_lat <= cell_max_lat):
                
                if label == "wildfire":
                    fire_event = True
                break  # Station found in this cell, no need to check others
        
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
    
    