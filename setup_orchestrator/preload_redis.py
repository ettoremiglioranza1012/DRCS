# Utilities
from Utils.db_utils import connect_to_db
import redis
import json
import os
import sys

# -----------------------------------------------------------
# Purpose:
# This script loads dimensional metadata about sensor stations 
# from PostgreSQL into Redis. Redis is used as a fast key-value
# store to enable real-time enrichment of sensor streams in Flink.
# -----------------------------------------------------------

try:
    # Connecting to PostgreSQL
    print("[INFO] Connecting to PostgreSQL...")
    conn = connect_to_db()
    cur = conn.cursor()
    print("[INFO] PostgreSQL connection established.")
except Exception as e:
    print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
    sys.exit(1)

try:
    # Connecting to Redis
    print("[INFO] Connecting to Redis...")
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=6379,
        decode_responses=True
    )
    # Test Redis connection
    r.ping()
    print("[INFO] Redis connection established.")
except Exception as e:
    print(f"[ERROR] Failed to connect to Redis: {e}")
    sys.exit(1)

try:
    print("[INFO] Fetching station metadata from PostgreSQL...")

    cur.execute("""
        SELECT
            station_id,  
            microarea_id,
            latitude, 
            longitude,  
            install_date,
            model,       
            temp_sens,   
            hum_sens,    
            co2_sens,    
            pm25_sens,   
            smoke_sens,  
            ir_sens,    
            elevation_m,
            battery_type
        FROM stations
    """)

    rows = cur.fetchall()
    print(f"[INFO] Retrieved {len(rows)} rows from 'stations' table.")

    for row in rows:
        station_id, \
        microarea_id, \
        latitude, \
        longitude, \
        install_date, \
        model, \
        temp_sens, \
        hum_sens, \
        co2_sens, \
        pm25_sens, \
        smoke_sens, \
        ir_sens, \
        elevation_m, \
        battery_type = row

        key = f"station:{station_id}"
        value = json.dumps({
            'microarea_id': microarea_id,
            'latitude': latitude,
            'longitude': longitude,
            'install_date': install_date.strftime("%Y-%m-%d") if install_date else None,
            'model': model,
            'temp_sens': temp_sens,
            'hum_sens': hum_sens,
            'co2_sens': co2_sens,
            'pm25_sens': pm25_sens,
            'smoke_sens': smoke_sens,
            'ir_sens': ir_sens,
            'elevation_m': elevation_m,
            'battery_type': battery_type
        })

        r.set(key, value)

    print("[INFO] Redis populated successfully with station metadata.")

except Exception as e:
    print(f"[ERROR] Failed to fetch or write metadata: {e}")

finally:
    # Always close DB resources
    try:
        cur.close()
        conn.close()
        print("[INFO] PostgreSQL connection closed.")
    except Exception as e:
        print(f"[ERROR] Failed to close PostgreSQL connection: {e}")

  