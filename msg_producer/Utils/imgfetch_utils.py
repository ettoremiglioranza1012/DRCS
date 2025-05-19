
# Utilities
from Utils.db_utils import connect_to_db
from psycopg2 import sql


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

