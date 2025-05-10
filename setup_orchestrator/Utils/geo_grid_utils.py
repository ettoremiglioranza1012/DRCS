
# Utilities
from datetime import date, timedelta
from db_utils import connect_to_db
from typing import Dict, Tuple
from psycopg2 import sql
import psycopg2
import random
import json
import math


def read_json(macroarea_input_path: str) -> Dict:
    """
    Reads a JSON file and returns the loaded Python dictionary.

    Args:
        macroarea_input_path (str): Path to the .json file.

    Returns:
        dict: The JSON content as a Python dictionary.
    """
    with open(macroarea_input_path, 'r') as f:
        data = json.load(f)
    return data


def write_json(macroarea_output_path: str, poly: Dict) -> None:
    """
    Writes a GeoJSON object (e.g., a polygon) to a file in readable JSON format.

    Args:
        macroarea_output_path (str): Path to the output file, including the filename and extension (.geojson or .json).
        poly (dict): Dictionary representing a GeoJSON object (typically a 'Polygon').
    """
    with open(macroarea_output_path, "w") as w:
        json.dump(poly, w, indent=4)


def polygon_to_bbox(geom: Dict) -> Tuple[float, float, float, float]:
    """
    Converts a GeoJSON Polygon into a bounding box (min_lon, min_lat, max_lon, max_lat).

    Args:
        geom (dict): A GeoJSON dictionary containing a polygon.

    Returns:
        tuple: Bounding box in the format (min_lon, min_lat, max_lon, max_lat)
    """
    if geom["type"] != "Polygon":
        raise ValueError("Solo poligoni supportati.")

    coords = geom["coordinates"][0]  # list of [lon, lat]

    lons = [point[0] for point in coords]
    lats = [point[1] for point in coords]

    min_lon = min(lons)
    max_lon = max(lons)
    min_lat = min(lats)
    max_lat = max(lats)

    return (min_lon, min_lat, max_lon, max_lat)


def create_microareas_grid(
    bbox: Tuple[float, float, float, float],
    max_area_km2: float,
    macro_area_n: int
) -> Dict[str, Tuple[float, float, float, float]]:
    """
    Splits a bounding box into rectangular microareas, each with a maximum area 
    approximately equal to `max_area_km2`.

    Args:
        bbox (Tuple[float, float, float, float]): (min_lon, min_lat, max_lon, max_lat)
        max_area_km2 (float): Maximum area per microarea in square kilometers.
        macro_area_n (int): Macro area identifier used in the microarea keys.

    Returns:
        Dict[str, Tuple[float, float, float, float]]: Dictionary with keys like 
        'macroarea_1_micro_1', ..., and values as bounding boxes in the form 
        (min_lon, min_lat, max_lon, max_lat)
    """
    min_lon, min_lat, max_lon, max_lat = bbox

    # Compute mean latitude to adjust longitude distance
    mean_lat = (min_lat + max_lat) / 2
    km_per_deg_lat = 111  # approx constant
    km_per_deg_lon = 111 * math.cos(math.radians(mean_lat))

    # Dimensions of the bounding box in kilometers
    width_km = (max_lon - min_lon) * km_per_deg_lon
    height_km = (max_lat - min_lat) * km_per_deg_lat

    total_area_km2 = width_km * height_km

    # Estimated number of microareas
    num_microareas = math.ceil(total_area_km2 / max_area_km2)

    # Approximate number of columns and rows
    n_cols = math.ceil(math.sqrt(num_microareas * (width_km / height_km)))
    n_rows = math.ceil(num_microareas / n_cols)

    lon_step = (max_lon - min_lon) / n_cols
    lat_step = (max_lat - min_lat) / n_rows

    microareas: Dict[str, Tuple[float, float, float, float]] = {}
    count = 1

    for i in range(n_rows):
        for j in range(n_cols):
            cell_min_lon = min_lon + j * lon_step
            cell_max_lon = cell_min_lon + lon_step
            cell_min_lat = min_lat + i * lat_step
            cell_max_lat = cell_min_lat + lat_step

            microareas[f"macroarea_{macro_area_n}_micro_{count}"] = (
                cell_min_lon,
                cell_min_lat,
                cell_max_lon,
                cell_max_lat
            )
            count += 1

    return microareas


def dict_to_polygon(microdict: Dict[str, Tuple[float, float, float, float]]) -> Dict:
    """
    Receives a dictionary of microareas (with bounding boxes) and returns a GeoJSON
    Polygon representing the overall bounding box of the macroarea.

    Args:
        microdict (dict): Keys like 'micro_1', 'micro_2', ... with values as tuples
                          (min_lon, min_lat, max_lon, max_lat)

    Returns:
        dict: A GeoJSON Polygon object
    """
    min_lon = float('inf')
    min_lat = float('inf')
    max_lon = float('-inf')
    max_lat = float('-inf')

    for _, (lon_min, lat_min, lon_max, lat_max) in microdict.items():
        min_lon = min(min_lon, lon_min)
        min_lat = min(min_lat, lat_min)
        max_lon = max(max_lon, lon_max)
        max_lat = max(max_lat, lat_max)

    # Building bounding box
    polygon = {
        "type": "Polygon",
        "coordinates": [[
            [min_lon, max_lat],
            [max_lon, max_lat],
            [max_lon, min_lat],
            [min_lon, min_lat],
            [min_lon, max_lat]  # closing polygon
        ]]
    }

    return polygon


def generate_random_coord(min_val: float, max_val: float) -> float:
    """
    Generate a random float value between a minimum and maximum value.
    
    Args:
        min_val (float): Minimum bound.
        max_val (float): Maximum bound.
    
    Returns:
        float: Random coordinate between min_val and max_val.
    """
    return random.uniform(min_val, max_val)


def random_date(start_year: int = 2022, end_year: int = 2025) -> date:
    """
    Generate a random date between January 1st of start_year and December 31st of end_year.
    
    Args:
        start_year (int): Start of date range (inclusive).
        end_year (int): End of date range (inclusive).
    
    Returns:
        date: Randomly generated date.
    """
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def process_sensor_stations_microarea(
    result: tuple[str, float, float, float, float],
    cur: psycopg2.extensions.cursor,
    macroarea_id: str,
) -> None:
    """
    Generate a random number of sensor stations within a given microarea bounding box,
    and insert their metadata into a dedicated PostgreSQL table called 'stations'.

    Also tracks the number of sensor stations created per microarea in the 
    'n_sens_stations' dimension table.

    Args:
        result (tuple): Tuple containing microarea_id and its bounding box (min_long, min_lat, max_long, max_lat).
        cur (psycopg2.extensions.cursor): Active PostgreSQL cursor.
        macroarea_id tracking which macroarea is being processed.
    
    Returns:
        None
    """
    try:
        microarea_id, min_long, min_lat, max_long, max_lat = result
        num_stations = random.randint(50, 100)
        table_name = "stations"

        # Insert metadata for each sensor station
        try:
            insertion_query = sql.SQL("""
                INSERT INTO {} (
                    station_id, microarea_id, latitude, longitude, install_date, model,
                    temp_sens, hum_sens, co2_sens, pm25_sens, smoke_sens, ir_sens,
                    elevation_m, battery_type, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_id) DO UPDATE SET
                    microarea_id = EXCLUDED.microarea_id,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    install_date = EXCLUDED.install_date,
                    model = EXCLUDED.model,
                    temp_sens = EXCLUDED.temp_sens,
                    hum_sens = EXCLUDED.hum_sens,
                    co2_sens = EXCLUDED.co2_sens,
                    pm25_sens = EXCLUDED.pm25_sens,
                    smoke_sens = EXCLUDED.smoke_sens,
                    ir_sens = EXCLUDED.ir_sens,
                    elevation_m = EXCLUDED.elevation_m,
                    battery_type = EXCLUDED.battery_type,
                    status = EXCLUDED.status;
            """).format(sql.Identifier(table_name))

            for k in range(num_stations):
                station_id = f"S_{microarea_id}_{str(k+1).zfill(3)}"
                lat = generate_random_coord(min_lat, max_lat)
                lon = generate_random_coord(min_long, max_long)
                cur.execute(insertion_query, (
                    station_id,
                    microarea_id,
                    lat,
                    lon,
                    random_date(),
                    random.choice(["WildSense-3000", "EnviroNode-X", "FireWatch-Pro", "ForestSentinel", "PyroScan-MKII"]),
                    random.choice(["TempPro-A1", "HeatTrack-200", "T-SenseX", "ClimaWatch-3", "ThermoPlus"]),
                    random.choice(["HumidX-100", "MoistTrack", "AirSense-H2", "DHT22", "EnviroHum"]),
                    random.choice(["CO2Safe-50", "GasTrack-X", "CarbonIQ", "GreenAir-300", "AQM-CO2"]),
                    random.choice(["AirFine-25", "Dusty-X", "ClearAir-100", "PartTrack", "EnviroPM"]),
                    random.choice(["SmokeCheck", "FlameSniff", "SensoSmoke", "AlarmX", "SmkSensePro"]),
                    random.choice(["IRFlame-900", "ThermEye", "FlareSense", "PyroWatch", "HeatSig"]),
                    round(random.uniform(50, 800), 2),
                    random.choice(["Li-Ion", "Solar", "Grid Power"]),
                    "active"
                ))

        except Exception as e:
            raise SystemError(f"Failed to insert sensor data into `{table_name}`: {e}")

        # Insert/update tracking info
        try:
            cur.execute(sql.SQL("""
                INSERT INTO n_sens_stations (microarea_id, macroarea_id, numof_sens_stations)
                VALUES (%s, %s, %s)
                ON CONFLICT (microarea_id) DO UPDATE SET
                    macroarea_id = EXCLUDED.macroarea_id,
                    numof_sens_stations = EXCLUDED.numof_sens_stations;
            """), (microarea_id, macroarea_id, num_stations))

        except Exception as e:
            raise ValueError(f"Error during `{microarea_id}` processing for `n_sens_stations` : {e}")

    except Exception as e:
        raise SyntaxError(f"[WARNING] Failed to process microarea {microarea_id}: {e}")
    

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

