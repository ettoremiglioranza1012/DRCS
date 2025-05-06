
# Utilities
import random
from datetime import date, timedelta
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


def process_sensor_stations_microarea(
    result: tuple[str, float, float, float, float], 
    cur: psycopg2.extensions.cursor
) -> None:
    """
    Generate a random number of sensor stations within a given microarea bounding box,
    and insert their metadata into a dedicated PostgreSQL table.

    Also tracks the number of sensor stations created per microarea in the 
    'n_sens_stations' dimension table.

    Args:
        result (tuple): Tuple containing microarea_id and its bounding box (min_long, min_lat, max_long, max_lat).
        cur (psycopg2.extensions.cursor): Active PostgreSQL cursor.
    
    Returns:
        None
    """
    try:
        microarea_id, min_long, min_lat, max_long, max_lat = result
        num_stations = random.randint(50, 100)
        sensor_table = f"sens_stations_{microarea_id.replace('-', '_')}"

        print(f"[INFO] Creating sensor table `{sensor_table}` for microarea {microarea_id} with {num_stations} stations.")

        # Create the table to hold metadata for sensor stations in this microarea
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                station_id TEXT PRIMARY KEY,
                microarea_id TEXT,
                latitude FLOAT,
                longitude FLOAT,
                install_date DATE,
                model TEXT,
                temp_sens TEXT,
                hum_sens TEXT,
                co2_sens TEXT,
                pm25_sens TEXT,
                smoke_sens TEXT,
                ir_sens TEXT,
                elevation_m FLOAT,
                battery_type TEXT,
                status TEXT
            );
        """).format(sql.Identifier(sensor_table)))
        print(f"[INFO] Table `{sensor_table}` created or already exists.")

        # Create (if needed) the tracking table for number of sensor stations per microarea
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS n_sens_stations (
                microarea_id TEXT PRIMARY KEY,
                numof_sens_stations INTEGER
            );
        """))

        # Insert each sensor station into the sensor_table
        for k in range(num_stations):
            station_id = f"S_{microarea_id}_{str(k+1).zfill(3)}"
            lat = generate_random_coord(min_lat, max_lat)
            lon = generate_random_coord(min_long, max_long)

            cur.execute(sql.SQL(f"""
                INSERT INTO {sensor_table} (
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
            """), (
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

        print(f"[INFO] Inserted {num_stations} sensor stations into `{sensor_table}`.")

        # Track the number of stations created in the dimension table
        cur.execute(sql.SQL("""
            INSERT INTO n_sens_stations (microarea_id, numof_sens_stations)
            VALUES (%s, %s)
            ON CONFLICT (microarea_id) DO UPDATE
            SET numof_sens_stations = EXCLUDED.numof_sens_stations;
        """), (microarea_id, num_stations))

        print(f"[INFO] Updated `n_sens_stations` for microarea {microarea_id} with {num_stations} stations.")

    except Exception as e:
        raise SyntaxError(f"[WARNING] Failed to process microarea {microarea_id}: {e}")

