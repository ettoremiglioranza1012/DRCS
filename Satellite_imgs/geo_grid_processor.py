
# Utilities
import os
from psycopg2 import sql
from db_utils import connect_to_db

from geo_utils import (
    read_json,
    write_json,
    polygon_to_bbox,
    create_microareas_grid,
    dict_to_polygon,
)


def insert_numofmicro(numof, cur):
    """
    Creates or updates a tracking table with the number of microareas for each macroarea.

    This function:
    - Ensures the existence of the `n_microareas` table, which stores the count of microareas per macroarea.
    - Inserts a new record with the provided number of microareas.

    Note:
    - The table uses an auto-incremented primary key (`macro_area_num`) to track each macroarea entry.
    - This function assumes the cursor is already connected and within a transaction.

    Args:
        numof (int): Number of microareas in the current macroarea.
        cur (psycopg2.extensions.cursor): Active cursor to execute SQL queries.
    """
    table_name = f"n_microareas"
    cur.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            macro_area_num SERIAL PRIMARY KEY,
            numof_microareas INTEGER
        );
    """).format(sql.Identifier(table_name)))
    
    insert_query = sql.SQL("""
        INSERT INTO {} (numof_microareas) VALUES (%s)             
    """).format(sql.Identifier(table_name))
    cur.execute(insert_query, (numof,))


def grids_loading(microareas_bbox_dict, i):
    """
    Loads the bounding box data of microareas into a PostgreSQL table.

    For the given macroarea index `i`, this function:
    - Creates a table named `macro_area_i` if it doesn't exist.
    - Inserts the bounding box coordinates of each microarea into the table.

    The table includes:
    - `micro_area_num`: auto-incremented primary key.
    - `min_long`, `min_lat`, `max_long`, `max_lat`: bounding box coordinates.

    Args:
        microareas_bbox_dict (dict): Dictionary with bounding boxes of microareas.
                                     Each value is expected to be a tuple/list of (min_long, min_lat, max_long, max_lat).
        i (int): Index of the macroarea used to name the table.
    """
    # Connect to the PostgreSQL database
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        # Create table for the macroarea if it doesn't already exist
        table_name = f"macro_area_{i}"
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                micro_area_num SERIAL PRIMARY KEY,
                min_long FLOAT,
                min_lat FLOAT,
                max_long FLOAT,
                max_lat FLOAT
            );         
        """).format(sql.Identifier(table_name)))
        
        print(f"[INFO] Table macro_area_{i} created (if not exists).")

        # Insert each microarea's bounding box into the table
        insert_query = sql.SQL("""
            INSERT INTO {}(min_long, min_lat, max_long, max_lat)
            VALUES (%s, %s, %s, %s)
        """).format(sql.Identifier(table_name))
        
        for value in microareas_bbox_dict.values(): 
            cur.execute(insert_query, (value[0], value[1], value[2], value[3]))
        
        print(f"[INFO] Inserted {len(microareas_bbox_dict)} microareas.")

        # Create or Update table to keep truck of the number of microareas
        insert_numofmicro(len(microareas_bbox_dict), cur)


    except Exception as e:
        print(f"[ERROR] Failed to load microareas for macro_area_{i}: {e}")
    
    finally:
        # Commit the transaction and close the connection
        conn.commit()
        cur.close()
        conn.close()


def macrogrid_reconstruction(microareas_bbox_dict, i):
    """
    Reconstructs a macroarea's spatial grid as a GeoJSON file.

    This function converts the dictionary of microarea bounding boxes
    into a unified polygon structure and writes it to a GeoJSON file.
    The output is useful for visual validation and compatibility with Browser Copernicus.

    Args:
        microareas_bbox_dict (dict): Dictionary containing bounding boxes of microareas.
        i (int): The macroarea index used to name the output file.
    """
    path = "Satellite/Macro_output"
    os.makedirs(path, exist_ok=True)
    macrogrid_outcome_polygon = dict_to_polygon(microareas_bbox_dict)
    macrogrid_outcome_path = f"{path}/macroarea_{i}.json"
    write_json(macrogrid_outcome_path, macrogrid_outcome_polygon)
    
    print(f"[INFO] Saved macrogrid GeoJSON for macroarea {i} to: {macrogrid_outcome_path}")


def process_macroareas():
    """
    Processes all macroareas by generating, saving, and loading their microarea grids.

    For each macroarea (indexed from 1 to 7), this function:
    - Loads its polygon geometry from a GeoJSON input file.
    - Computes the bounding box of the polygon.
    - Creates a regular grid of microareas (each 500m square) within that bounding box.
    - Reconstructs and exports the resulting macrogrid to a GeoJSON file for visualization and sanity check.
    - Loads the grid into a PostgreSQL database for real time images fetching.

    This function is intended to be run once as part of the initial geodata ingestion phase.
    """
    n_of_macroareas = 7
    for i in range(1, n_of_macroareas + 1):

        print(f"[INFO] Processing macroarea {i}...")

        # Read macroarea geometry from file
        path_to_current_geoJson_macro = f"Satellite/Macro_input/macroarea_{i}.json"
        if not os.path.exists(path_to_current_geoJson_macro):
            print(f"[WARNING] File not found: {path_to_current_geoJson_macro}")
            continue

        macro_geom = read_json(path_to_current_geoJson_macro)
        
        # Generate microarea grid within macroarea bounding box
        macro_bbox = polygon_to_bbox(macro_geom)
        microareas_bbox_dict = create_microareas_grid(macro_bbox, 500, i)

        # Save reconstructed macrogrid as GeoJSON for inspection or reuse
        macrogrid_reconstruction(microareas_bbox_dict, i)

        # Load microareas into PostgreSQL database
        grids_loading(microareas_bbox_dict, i)

    n_reconstructed = len([f for f in os.listdir("Satellite/Macro_output") if f.endswith(".json")])
    print(f"\n[INFO] Reconstructed {n_reconstructed} macrogrids successfully.")


if __name__ == "__main__":
    process_macroareas()