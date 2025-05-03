
# Utilities
import matplotlib.pyplot as plt
from typing import Dict, Tuple
from PIL import Image
import numpy as np
import base64
import math
import json
import io


# |---Utils functions---|

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
    # Run some tests
    assert img.ndim == 3 and img.shape[2] == 3, "[ERROR] Input image must be (H, W, 3)"
    assert img.min() >= 0 and img.max() <= 255, "[ERROR] Image values must be in range [0, 255]"

    if not isinstance(img, np.ndarray):
        raise ValueError("[ERROR]Input type is not np.ndarray")
    
    print(f"[INFO] Compressing image of shape {img.shape} with quality={quality}...")

    # Ensure the image is in 8-bit unsigned integer format and convert to a PIL image
    pil_img = Image.fromarray(img.astype('uint8'), 'RGB')

    # Create an in-memory byte buffer
    buffer = io.BytesIO()

    # Save the image in JPEG format into the buffer with the specified quality
    pil_img.save(buffer, format="JPEG", quality=quality)

    # Print size
    compressed_size = buffer.tell()
    print(f"[INFO] Compression complete. Compressed size: {compressed_size} bytes")

    # Return the byte content of the compressed image
    return buffer.getvalue()


def serialize_image_payload(image_bytes: bytes, metadata: dict) -> str:
    """
    Encode a compressed image and metadata into a JSON-formatted string.

    Args:
        image_bytes (bytes): Compressed image data (e.g., from JPEG compression).
        metadata (dict): Dictionary containing metadata (timestamp, location, etc.).

    Returns:
        str: JSON string with base64-encoded image and associated metadata.
    """
    # Run some tests
    if not isinstance(image_bytes, bytes):
        raise ValueError("[ERROR] image_bytes must be of type bytes")

    if not isinstance(metadata, dict):
        raise ValueError("[ERROR] metadata must be a dictionary")

    print(f"[INFO] Serializing image of size {len(image_bytes)} bytes...")
    
    # Encode image bytes into base64 to safely embed in JSON
    encoded = base64.b64encode(image_bytes).decode('utf-8')

    # Create the payload with image data and metadata
    payload = {
        "image_data": encoded,
        "metadata": metadata
    }

    # Convert dict to json 
    json_str = json.dumps(payload)

    print(f"[INFO] Serialization complete. Payload size: {len(json_str)} characters")

    # Convert the payload dictionary into a JSON string
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
                                          for proper display using matplotlib or saving.

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




