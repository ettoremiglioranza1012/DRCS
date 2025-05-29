
# Utilities 
from Utils.db_utils import connect_to_db
from psycopg2 import OperationalError
import psycopg2
import time


def connect_to_db_with_retry(max_retries=30, delay=2):
    """
    Connects to the database with retry logic.
    """
    for attempt in range(max_retries):
        try:
            conn = connect_to_db()
            print(f"Successfully connected to database on attempt {attempt + 1}")
            return conn
        except OperationalError as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Unable to connect to database.")
                raise
        except Exception as e:
            print(f"Unexpected error on attempt {attempt + 1}: {e}")
            raise


def create_meteo_hist_table(cur: psycopg2.extensions.cursor) -> None:
    """
    Create the 'meteo_hist' table in the PostgreSQL database if it doesn't exist.
    """
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meteo_hist (
                date DATE,
                cell_id TEXT,
                latitude FLOAT,
                longitude FLOAT,
                number FLOAT,
                surface FLOAT,
                u10 FLOAT,
                v10 FLOAT,
                d2m FLOAT,
                t2m FLOAT,
                msl FLOAT,
                meanSea FLOAT,
                sst FLOAT,
                sp FLOAT,
                u100 FLOAT,
                v100 FLOAT,
                depthBelowLandLayer FLOAT,
                stl1 FLOAT,
                swvl1 FLOAT,
                cvh FLOAT,
                cell_lat FLOAT,
                cell_lon FLOAT,
                frp_max FLOAT,
                fire_class INT
            );
        """)
        print("[INFO] Sucessfully executed creation command for 'meteo_hist' table.")
    
    except Exception as e:
        print(f"[ERROR] Failed to execute creation command for 'meteo_hist' table: {e}")


def main():
    """
    Connects to the database, creates the table, and loads historical meteo data from CSV.
    """
    try:
        conn = connect_to_db_with_retry()
        cur = conn.cursor()

        create_meteo_hist_table(cur)
        
        with open('/data/meteo_totrain.csv', 'r') as f:
            next(f)
            cur.copy_expert("COPY meteo_hist FROM STDIN WITH CSV", f)
        
        conn.commit()
        print("[INFO] Sucessfully created meteo_hist table and inserted data, READY to commit!")

    except Exception as e:
        print(f"[ERROR] Failed connection, table creation and data insertion: {e}")

    finally:
        try:
            if conn:
                conn.commit()
                print(f"[INFO] Committed changes to the database.")
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception as e:
            print(f"[WARNING] Final cleanup failed: {e}")


if __name__ == "__main__":
    main()

