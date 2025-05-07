
# Utilities
import os
import psycopg2
from psycopg2 import sql
from Utils.db_utils import connect_to_db
from Utils.geo_sensor_utils import process_sensor_stations_microarea

from Utils.geo_img_utils import (
    read_json,
    write_json,
    polygon_to_bbox,
    create_microareas_grid,
    dict_to_polygon,
)


def macro_loading(
    i: int, 
    numof: int, 
    cur: psycopg2.extensions.cursor,
    macro_bbox: tuple[float, float, float, float]
) -> None:
    """
    Creates or updates the `Macroareas` table with metadata for a specific macroarea.

    This function:
    - Ensures the existence of the `Macroareas` table, which stores information about each macroarea.
    - Inserts or updates the record for macroarea A{i}, including its bounding box and the number of microareas it contains.

    Args:
        i (int): Index of the macroarea.
        numof (int): Number of microareas within this macroarea.
        cur (psycopg2.extensions.cursor): Active database cursor to execute SQL queries.
        macro_bbox (tuple): A 4-tuple (min_long, min_lat, max_long, max_lat) representing the macroarea bounding box.
    """
    table_name = "macroareas"
    macroarea_id = f"A{i}"
    min_long, min_lat, max_long, max_lat = macro_bbox

    try:
        print(f"[INFO] Update or create (if not exists) {table_name}.")
        
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                macroarea_id TEXT PRIMARY KEY,
                min_long FLOAT,
                min_lat FLOAT,
                max_long FLOAT,
                max_lat FLOAT,
                numof_microareas INTEGER
            );
        """).format(sql.Identifier(table_name)))
        
        upsert_query = sql.SQL("""
            INSERT INTO {} (macroarea_id, min_long, min_lat, max_long, max_lat, numof_microareas)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (macroarea_id) DO UPDATE SET
                min_long = EXCLUDED.min_long,
                min_lat = EXCLUDED.min_lat,
                max_long = EXCLUDED.max_long,
                max_lat = EXCLUDED.max_lat,
                numof_microareas = EXCLUDED.numof_microareas;
        """).format(sql.Identifier(table_name))

        cur.execute(upsert_query, (macroarea_id, min_long, min_lat, max_long, max_lat, numof))

        print(f"[INFO] Correctly inserted macroarea_{macroarea_id} informations into table {table_name}.")
    
    except Exception as e:
        print(f"[ERROR] Failed to insert number of microareas for macroarea_{macroarea_id} in n_microareas, "
              f"Error: {e}")


def grids_loading(
    microareas_bbox_dict: dict, 
    macro_bbox: tuple[float, float, float, float], 
    i: int
) -> None:
    """
    Loads microarea bounding boxes and registers them in the `Microareas` table.

    For the given macroarea index `i`, this function:
    - Ensures the existence of the shared `Microareas` table.
    - Inserts or updates the bounding box data of each microarea, assigning microarea IDs like 'A{i}-M1', 'A{i}-M2', etc.
    - Each microarea record includes its corresponding macroarea ID and coordinates.
    - Also calls `macro_loading` to update metadata for the parent macroarea, including its bounding box and microarea count.

    Args:
        microareas_bbox_dict (dict): Dictionary of microarea bounding boxes, where each value is a 4-tuple 
                                     (min_long, min_lat, max_long, max_lat).
        macro_bbox (tuple): A 4-tuple (min_long, min_lat, max_long, max_lat) representing the bounding box of the macroarea.
        i (int): Index of the macroarea used to generate identifiers.
    """
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        table_name = f"microareas"

        # Create the table for the macroarea
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                microarea_id TEXT PRIMARY KEY,
                macroarea_id TEXT,
                min_long FLOAT,
                min_lat FLOAT,
                max_long FLOAT,
                max_lat FLOAT
            );
        """).format(sql.Identifier(table_name)))

        print(f"[INFO] Table {table_name} update or create (if not exists).")

        # Insert each microarea with formatted ID
        insert_query = sql.SQL("""
            INSERT INTO {}(microarea_id, macroarea_id, min_long, min_lat, max_long, max_lat)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (microarea_id) DO UPDATE
            SET macroarea_id = EXCLUDED.macroarea_id,
                min_long = EXCLUDED.min_long,
                min_lat = EXCLUDED.min_lat,
                max_long = EXCLUDED.max_long,
                max_lat = EXCLUDED.max_lat
        """).format(sql.Identifier(table_name))

        temp_macroarea_id = f"A{i}"
        for idx, value in enumerate(microareas_bbox_dict.values(), start=1):
            temp_microarea_id = f"A{i}-M{idx}"
            cur.execute(insert_query, (temp_microarea_id, temp_macroarea_id, value[0], value[1], value[2], value[3]))

        print(f"[INFO] Inserted {len(microareas_bbox_dict)} microareas.")

        # Track the number of microareas
        macro_loading(i, len(microareas_bbox_dict), cur, macro_bbox)

    except Exception as e:
        print(f"[ERROR] Failed to load microareas for macroarea_A{i}: {e}")

    finally:
        try:
            if conn: conn.commit()
            if cur: cur.close()
            if conn: conn.close()
        except Exception as e:
            print(f"[WARNING] Final cleanup failed: {e}")


def macrogrid_reconstruction(microareas_bbox_dict: dict, i: int) -> None:
    """
    Reconstructs a macroarea's spatial grid as a GeoJSON file.

    This function converts the dictionary of microarea bounding boxes
    into a unified polygon structure and writes it to a GeoJSON file.
    The output is useful for visual validation and compatibility with Browser Copernicus.

    Args:
        microareas_bbox_dict (dict): Dictionary containing bounding boxes of microareas.
        i (int): The macroarea index used to name the output file.
    """
    path = "Macro_data/Macro_output"
    os.makedirs(path, exist_ok=True)
    macrogrid_outcome_polygon = dict_to_polygon(microareas_bbox_dict)
    macrogrid_outcome_path = f"{path}/macroarea_{i}.json"
    write_json(macrogrid_outcome_path, macrogrid_outcome_polygon)
    
    print(f"[INFO] Saved reconstructed macrogrid in GeoJSON for macroarea {i} to: {macrogrid_outcome_path}")


def generate_sensor_stations(i: int) -> None:
    """
    For a given macroarea index `i`, this function:
    - Create `stations` and `n_sens_stations` tables to keep record of sensor stations dimensional data.
    - Retrieves the number of microareas in the macroarea from the dimension table `macroareas`.
    - Iterates through each microarea, fetches its bounding box coordinates from the corresponding table `microareas`.
    - Calls `process_sensor_stations_microarea` to populate a separate sensor `stations` and `n_sens_stations` tables.
    - Handles database connection and commits changes at the end.

    Args:
        i (int): Index of the macroarea to process (e.g., 1 for A1)

    Returns:
        None
    """
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        table_name = "stations"
        # Create stations table
        try:
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
            """).format(sql.Identifier(table_name)))
            print(f"[INFO] Table `{table_name}` created or already exists.")
        except Exception as e:
            raise SystemError(f"Failed to create sensor table `{table_name}`: {e}")
        
        # Create tracking table if not exists
        try:
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS n_sens_stations (
                    microarea_id TEXT PRIMARY KEY,
                    macroarea_id TEXT,
                    numof_sens_stations INTEGER
                );
            """))
        except Exception as e:
            raise SystemError(f"Failed to create tracking table `n_sens_stations`: {e}")

        # Process each macro area sensor generating and saving task
        macroarea_id = f"A{i}"
        print("---")
        print(f"[INFO] Starting generation of sensor stations for macroarea {macroarea_id}.")

        # Fetch number of microareas for this macroarea from tracking table
        cur.execute("""
            SELECT numof_microareas
            FROM macroareas
            WHERE macroarea_id = %s
        """, (macroarea_id,))
        n = cur.fetchone()
        if not n:
            raise SystemError(f"No microareas found for {macroarea_id}, check data integrity.")

        num_microareas = n[0]
        print(f"[INFO] Found {num_microareas} microareas in macroarea {macroarea_id}.")
        print(f"[INFO] Processing macroarea {macroarea_id}...")
        print("[INFO] Generating sensor stations and saving data into database... Please wait.")
        
        # Loop over all microareas
        for j in range(num_microareas):
            temp_microid = f"A{i}-M{j+1}"

            # Select the bounding box of the microarea
            select_query = sql.SQL("""
                SELECT
                    microarea_id,
                    min_long,
                    min_lat,
                    max_long,
                    max_lat
                FROM microareas
                WHERE microarea_id = %s
            """)

            # Execute query to retrive relative micro area info
            cur.execute(select_query, (temp_microid,))
            result = cur.fetchone()

            if result:
                process_sensor_stations_microarea(result, cur, macroarea_id)
            else:
                raise ValueError(f"Microarea {temp_microid} not found in `macroareas`, check data integrity.")

        print(f"[INFO] Successfully completed sensor station generation for macroarea {macroarea_id}.")

    except Exception as e:
        print(f"[ERROR] Failed to generate and load sensor stations for macroarea {macroarea_id}: {e}")

    finally:
        try:
            if conn:
                conn.commit()
                print(f"[INFO] Committed changes to the database for macroarea {macroarea_id}.\n")
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception as e:
            print(f"[WARNING] Final cleanup failed: {e}")


def process_macroareas():
    """
    Processes all macroareas by generating, saving, and loading their microarea grids.

    For each macroarea (indexed from 1 to 7), this function:
    - Loads its polygon geometry from a GeoJSON input file.
    - Computes the bounding box of the polygon.
    - Creates a regular grid of microareas (each 500m square) within that bounding box.
    - Reconstructs and exports the resulting macrogrid to a GeoJSON file for visualization and sanity check.
    - Loads the grid into a PostgreSQL database for real time images fetching.
    - Call the generate sensor stations function to process of sensor generating and saves infos in database.

    This function is intended to be run once as part of the initial geodata ingestion phase.
    """
    print("\n[GEO-GRID-PROCESSOR]")
    n_of_macroareas = 5
    for i in range(1, n_of_macroareas + 1):
        
        print(f"\n[INFO] Processing macroarea {i}...")

        # Read macroarea geometry from file
        path_to_current_geoJson_macro = f"Macro_data/Macro_input/macroarea_{i}.json"
        if not os.path.exists(path_to_current_geoJson_macro):
            raise ValueError(f"File not found: {path_to_current_geoJson_macro}")

        macro_geom = read_json(path_to_current_geoJson_macro)
        
        # Generate microarea grid within macroarea bounding box
        macro_bbox = polygon_to_bbox(macro_geom)
        microareas_bbox_dict = create_microareas_grid(macro_bbox, 500, i)
        print("[INFO] Microgrids generated sucessfully")

        # Save reconstructed macrogrid as GeoJSON for inspection or reuse
        macrogrid_reconstruction(microareas_bbox_dict, i)

        # Load microareas into PostgreSQL database
        grids_loading(microareas_bbox_dict, macro_bbox, i)

        # Randomly generate sensor stations location on microareas grid
        generate_sensor_stations(i)
        
    print(f"\n[INFO] The logs of sensor stations data in: Macro_data/Macro_output")
    n_reconstructed = len([f for f in os.listdir("Macro_data/Macro_output") if f.endswith(".json")])
    print(f"[INFO] Reconstructed {n_reconstructed} macrogrids successfully.")


if __name__ == "__main__":
    process_macroareas()