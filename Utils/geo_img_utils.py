
# Utilities
from Utils.db_utils import connect_to_db
import matplotlib.pyplot as plt
from typing import Dict, Tuple
from psycopg2 import sql
from PIL import Image
import numpy as np
import psycopg2
import logging
import time
import math
import json
import io


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


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


def compress_image_with_pil(img: np.ndarray, quality: int = 85) -> bytes:
    """
    Compress a NumPy image array using JPEG format with the specified quality.

    Args:
        img (np.ndarray): Image array of shape (H, W, 3) with values in [0, 255], dtype can be any numeric type.
        quality (int): JPEG compression quality (1–100), where higher means better quality.

    Returns:
        bytes: The compressed image in JPEG format as a byte stream.
    """
    start_time = time.perf_counter()
    # Run some tests
    assert img.ndim == 3 and img.shape[2] == 3, "[ERROR] Input image must be (H, W, 3)"
    assert img.min() >= 0 and img.max() <= 255, "[ERROR] Image values must be in range [0, 255]"

    if not isinstance(img, np.ndarray):
        raise ValueError("[ERROR]Input type is not np.ndarray")
    
    logger.info(f"Compressing image of shape {img.shape} with quality={quality}...")

    # Ensure the image is in 8-bit unsigned integer format and convert to a PIL image
    pil_img = Image.fromarray(img.astype('uint8'), 'RGB')

    # Create an in-memory byte buffer
    buffer = io.BytesIO()

    # Save the image in JPEG format into the buffer with the specified quality
    pil_img.save(buffer, format="JPEG", quality=quality)

    # Print size
    compressed_size = buffer.tell()
    elapsed = time.perf_counter() - start_time
    logger.info(f"Compression complete. Compressed size: {compressed_size} bytes in {elapsed:.3f} s")

    # Return the byte content of the compressed image
    return buffer.getvalue()


def save_image_in_database(image_bytes: bytes, timestamp: str, macroarea_id: str, microarea_id: str) -> str:
    """
    Saves a compressed image to the PostgreSQL database and returns a unique image ID.

    This function:
    - Ensures the existence of the `satellite_images` table with an `image_id` primary key.
    - Inserts or updates the image using the generated `image_id`.

    Args:
        image_bytes (bytes): Compressed image data.
        timestamp (str): ISO 8601 formatted timestamp.
        macroarea_id (str): Macroarea identifier.
        microarea_id (str): Microarea identifier.

    Returns:
        str: A unique image ID in the format "{macro}_{micro}_{timestamp}".
    """
    table_name = "satellite_images"
    image_id = f"{macroarea_id}_{microarea_id}_{timestamp}"

    try:
        conn = connect_to_db()
        cur = conn.cursor()

        logger.info(f"Update or create (if not exists) {table_name}.")

        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                image_id TEXT PRIMARY KEY,
                timestamp TEXT,
                macroarea_id TEXT,
                microarea_id TEXT,
                image BYTEA
            );
        """).format(sql.Identifier(table_name)))

        upsert_query = sql.SQL("""
            INSERT INTO {} (image_id, timestamp, macroarea_id, microarea_id, image)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (image_id) DO UPDATE SET
                image = EXCLUDED.image;
        """).format(sql.Identifier(table_name))

        cur.execute(upsert_query, (
            image_id,
            timestamp,
            macroarea_id,
            microarea_id,
            psycopg2.Binary(image_bytes)
        ))
        conn.commit()

        logger.info(f"Image stored with image_id={image_id} in table {table_name}.")

        return image_id

    except Exception as e:
        print(f"[ERROR] Failed to store image with image_id={image_id}, Error: {e}")
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def serialize_image_payload(image_bytes: bytes, metadata: Dict, macroarea_id:str, microarea_id:str) -> str:
    """
    Save the compressed image in the database and serialize metadata + image pointer into JSON.

    Args:
        image_bytes (bytes): Compressed image data.
        metadata (dict): Metadata including location, etc. Timestamp will be added automatically.

    Returns:
        str: JSON string with image pointer and associated metadata.
    """
    start_time = time.perf_counter()

    if not isinstance(image_bytes, bytes):
        raise ValueError("[ERROR] image_bytes must be of type bytes")
    if not isinstance(metadata, dict):
        raise ValueError("[ERROR] metadata must be a dictionary")

    # Get timestamp (ISO 8601 format)
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    metadata["timestamp"] = timestamp

    logger.info(f"Saving image of size {len(image_bytes)} bytes to database...")

    # Save image and get pointer
    image_pointer = save_image_in_database(image_bytes, timestamp, macroarea_id, microarea_id)

    # Create payload with metadata and image pointer
    payload = {
        "metadata": metadata,
        "image_pointer": image_pointer
    }

    json_str = json.dumps(payload)

    elapsed = time.perf_counter() - start_time
    logger.info(f"Serialization complete. Payload size: {len(json_str)} characters in {elapsed:.3f} s\n")

    return json_str


def plot_image(image: np.ndarray, factor: float = 3.5/255, clip_range: Tuple[float, float] = (0, 1)) -> None:
    """
    Plots an RGB image after rescaling and clipping, and saves it to the output directory.

    Args:
        image (np.ndarray): RGB image of shape (H, W, 3), typically with pixel values in [0, 255].
        img_name (str): Filename to save the image as (e.g., "output.jpg").
        output_dir (str): Directory where the image will be saved.
        factor (float): Multiplicative rescaling factor applied to the image.
                        Useful when pixel values represent physical quantities (e.g., reflectance, temperature).
                        Satellite images often need rescaling because of what they measure.
                        For example, factor=3.5/255 maps raw values in [0, 255] to approx [0, 3.5].
        clip_range (Tuple[float, float]): Range to clip the rescaled pixel values to (e.g., (0, 1)),
                                          for proper display using matplotlib.

    Returns:
        None
    """
    # Apply rescaling factor
    image = image * factor

    # Clip the values to the specified range
    image = np.clip(image, clip_range[0], clip_range[1])

    # Plot the image
    plt.figure(figsize=(10, 10))
    plt.imshow(image)
    plt.axis('off')

    plt.show()




