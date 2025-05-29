
# Utilities
from botocore.client import Config
from datetime import datetime
import matplotlib.pyplot as plt
from typing import Dict, Tuple
from PIL import Image
import numpy as np
import logging
import hashlib
import random
import boto3
import math
import uuid
import time
import json
import io


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


# --- MinIO S3-compatible client setup ---
s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)


class PixelLocationManager():
    def __init__(self):
        self.locations = {}

    def get_locations(
            self, 
            microarea_id: str,
            macroarea_id: str,
            min_long: float,
            min_lat: float,
            max_long: float,
            max_lat: float
    ) -> list[Tuple]:
        """Generate or retrieve pixel locations for a specific region"""
        location_id = f"{microarea_id}"
        
        if location_id not in self.locations.keys():
            self.locations[location_id] = []
            centroids_pixels = self._divide_microarea(
                min_long,
                min_lat,
                max_long,
                max_lat
            )
            
            for value in centroids_pixels:
                curr_label, curr_lat, curr_long = value
                self.locations[location_id].append((curr_label, curr_lat, curr_long))
        
        return self.locations[location_id]

    def _divide_microarea(
            self,
            min_long: float,
            min_lat: float,
            max_long: float,
            max_lat: float,
            max_area_km2: float = 20
    ):
        """
            Comment Here!
        """
        # Compute mean latitude to adjust longitude distance
        mean_lat = (min_lat + max_lat) / 2
        km_per_deg_lat = 111  # approx constant
        km_per_deg_long = 111 * math.cos(math.radians(mean_lat))

        # Dimensions of the bounding box in kilometers
        width_km = (max_long - min_long) * km_per_deg_long
        height_km = (max_lat - min_lat) * km_per_deg_lat

        total_area_km2 = width_km * height_km

        # Estimated number of microareas
        num_microareas = math.ceil(total_area_km2 / max_area_km2)

        # Approximate number of columns and rows
        n_cols = math.ceil(math.sqrt(num_microareas * (width_km / height_km)))
        n_rows = math.ceil(num_microareas / n_cols)

        long_step = (max_long - min_long) / n_cols
        lat_step = (max_lat - min_lat) / n_rows

        cells_centroids = []

        for i in range(n_rows):
            for j in range(n_cols):
                cell_min_long = min_long + j * long_step
                cell_max_long = cell_min_long + long_step
                cell_min_lat = min_lat + i * lat_step
                cell_max_lat = cell_min_lat + lat_step
                
                centroid_lat = (cell_min_lat + cell_max_lat) / 2
                centroid_long = (cell_min_long + cell_max_long) / 2

                if 2 < i < 5 and 2 < j < 5:
                    label = "wildfire"
                else:
                    label = "vegetation"

                cells_centroids.append((
                    label,
                    centroid_lat, 
                    centroid_long
                ))

        return cells_centroids


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


def save_image_in_S3(image_bytes: bytes, timestamp: str, macroarea_id: str, microarea_id: str) -> str:
    """
        To comment!
    """
    bucket_name = "satellite-imgs"
    image_file_id = f"sat_img_{macroarea_id}_{microarea_id}_{timestamp}"
    
    # Extract date for partitioned path
    year_month_day = timestamp.split("T")[0]  # YYYY-MM-DD
    year = year_month_day.split("-")[0]
    month = year_month_day.split("-")[1]
    day = year_month_day.split("-")[2]
    
    # Unique uuid hex code
    unique_id = uuid.uuid4().hex[:8]
    
    # Unique Img ID
    object_key = f"sat_imgs/year={year}/month={month}/day={day}/{image_file_id}_{unique_id}.jpg"
    
    # Put object in bucket
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=image_bytes,
            ContentType='image/jpeg'
        )
        logger.info(f"Uploaded to bucket '{bucket_name}' at key '{object_key}'")

        return object_key

    except Exception as e:
        raise SystemError(f"[ERROR] Failed to store image with image_id={image_file_id}, Error: {e}")


def generate_pixel_data(label: str, lat: float, lon: float, microarea_id: str, fire_probability: int = 20) -> dict:
    """
        To comment!
    """
    # pixel_id = f"{str(lat)}_{str(lon)}_{microarea_id}"

    # # Deterministic fire_event based on pixel location hash
    # hash_digest = hashlib.md5(pixel_id.encode()).hexdigest()
    # # Example: "a1b2c3d4e5f6789012345678abcdef90"
    # # Uniform Distribution: MD5 hash distributes values uniformly across 0-99 range
    # hash_value = int(hash_digest[:8], 16) # Use part of hash to avoid full int overflow
    # # Takes first 8 chars: "a1b2c3d4" → converts to integer
    # # Example: 2712847316 (decimal)
    # is_fire = (hash_value % 100) < fire_probability # 20% of pixels deterministically simulate fire
    # # 2712847316 % 100 = 16
    # # If fire_probability = 20: 16 < 20 → True (fire)
    # # If fire_probability = 20: 85 < 20 → False (no fire)
    # # 20% chance of getting a value under 20

    if label == "wildfire":
        # Simulated fire conditions
        B4 = random.uniform(0.3, 0.4)
        B8 = random.uniform(0.1, 0.2)
        B3 = random.uniform(0.05, 0.1)
        B11 = random.uniform(0.2, 0.3)
        B12 = random.uniform(0.2, 0.3)
    else:
        # Normal vegetation
        B4 = random.uniform(0.05, 0.2)
        B8 = random.uniform(0.3, 0.5)
        B3 = random.uniform(0.1, 0.3)
        B11 = random.uniform(0.05, 0.2)
        B12 = random.uniform(0.05, 0.2)


    pixel_json = {
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "microarea_id": microarea_id,
        "bands": {
            "B2": round(random.uniform(0.05, 0.2), 3),
            "B3": round(B3, 3),
            "B4": round(B4, 3),
            "B8": round(B8, 3),
            "B8A": round(random.uniform(0.05, 0.3), 3),
            "B11": round(B11, 3),
            "B12": round(B12, 3)
        }
    }

    return pixel_json


def firedet_bands_metadata(bbox_list: list, microarea_id: str, macroarea_id: str, fire_probability: int = 20) -> dict:
    """
        To comment!
    """
    # Validate bounding box
    if not isinstance(bbox_list, (list, tuple)) or len(bbox_list) != 4:
        raise ValueError("[ERROR] bbox_list must be a list of 4 coordinates [min_long, min_lat, max_long, max_lat]")
    
    location_manager = PixelLocationManager()

    min_long, min_lat, max_long, max_lat = bbox_list
    sampled_pixels = []

    location = location_manager.get_locations(
        microarea_id, 
        macroarea_id,
        min_long, 
        min_lat, 
        max_long,
        max_lat
    )

    for i in range(len(location)):
        try:
            label, lat, lon = location[i]
            pixel_data = generate_pixel_data(label, lat, lon, microarea_id=microarea_id, fire_probability=fire_probability)
            sampled_pixels.append(pixel_data)

        except Exception as e:
            print(f"[WARNING] Failed to generate pixel {i+1}/{len(location)}: {e}")
            continue

    metadata = {
        "satellite_data": sampled_pixels
    }

    return metadata


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
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    metadata['timestamp'] = timestamp
    metadata["microarea_id"] = microarea_id
    metadata["macroarea_id"] = macroarea_id

    logger.info(f"Saving image of size {len(image_bytes)} bytes to database...")

    # Save image and get pointer
    image_pointer = save_image_in_S3(image_bytes, timestamp, macroarea_id, microarea_id)

    # Create payload with metadata and image pointer
    payload = {
        "image_pointer": image_pointer,
        "metadata": metadata,
    }

    json_str = json.dumps(payload)

    elapsed = time.perf_counter() - start_time
    logger.info("Meta data appended to img successfully.")
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

