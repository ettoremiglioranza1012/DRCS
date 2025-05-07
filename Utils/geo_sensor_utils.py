
# Utilities
import random
import os
import json
from datetime import date, timedelta, datetime
from psycopg2 import sql
import psycopg2


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


def log_to_json(message: str, level: str = "INFO", log_file: str = "Macro_data/Macro_output/sensor_log.json") -> None:
    """
    Log a message to a JSON file with a timestamp and severity level.

    This function appends structured log entries to a JSON file. If the file doesn't exist,
    it creates one and starts a new list. If it exists, it loads the existing list,
    appends the new log entry, and writes everything back.

    Args:
        message (str): The log message to record.
        level (str): The severity level of the message ("INFO", "WARNING", "ERROR", etc.).
        log_file (str): Path to the JSON file where logs are stored.

    Returns:
        None
    """
    log_entry = {
        "timestamp": datetime.now().isoformat() + "Z",
        "level": level,
        "message": message
    }
    if not os.path.isfile(log_file):
        with open(log_file, 'w') as f:
            json.dump([log_entry], f, indent=4)
    else:
        with open(log_file, 'r+', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = []
            data.append(log_entry)
            f.seek(0)
            json.dump(data, f, indent=4)
            f.truncate()


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
        log_to_json(f"Updating table `{table_name}`: inserting {num_stations} stations for microarea {microarea_id}.")

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
            log_to_json(f"Inserted {num_stations} sensor stations into `{table_name}`.")
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
            log_to_json(f"Updated `n_sens_stations` for microarea {microarea_id} with {num_stations} stations.")
        except Exception as e:
            raise ValueError(f"Error during `{microarea_id}` processing for `n_sens_stations` : {e}")

    except Exception as e:
        log_to_json(f"Failed to process microarea {microarea_id}: {e}", level="WARNING")
        raise SyntaxError(f"[WARNING] Failed to process microarea {microarea_id}: {e}")


