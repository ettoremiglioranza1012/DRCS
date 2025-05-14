
# Utilities
from botocore.client import Config
import matplotlib.pyplot as plt
from typing import Dict, Tuple
from PIL import Image
import numpy as np
import logging
import random
import boto3
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
    Compress a NumPy image array and upload it to MinIO using boto3.

    Args:
        bucket_name (str): The name of the MinIO bucket.
        object_key (str): The key (path) to store the image, e.g. 'region1/2025-05-09/image1.jpg'.
        img (np.ndarray): Image array of shape (H, W, 3).
        quality (int): JPEG compression quality (default 85).
    """
    bucket_name = "satellite-imgs"
    image_file_id = f"sat_img_{macroarea_id}_{microarea_id}_{timestamp}"
    object_key = f"{image_file_id}.jpg"

    # Make sure the bucket exists
    try:
        s3.head_bucket(Bucket=bucket_name)
    except s3.exceptions.ClientError:
        s3.create_bucket(Bucket=bucket_name)
    
    # Put object in bucket
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=image_bytes,
            ContentType='image/jpeg'
        )
        logger.info(f"Uploaded to bucket '{bucket_name}' at key '{object_key}'")

        return image_file_id

    except Exception as e:
        raise SystemError(f"[ERROR] Failed to store image with image_id={image_file_id}, Error: {e}")


def generate_pixel_data(lat: float, lon: float, macroarea_id: str, fire_probability: float = 0.2) -> dict:
    """
    Generate synthetic satellite data for a given pixel.
    In fire_probability fraction of the cases, simulate fire conditions
    by adjusting band values and classification.

    Args:
        lat (float): Latitude of the pixel.
        lon (float): Longitude of the pixel.
        tile_id (str): Sentinel-2 tile ID. Default is "T11SML".
        fire_probability (float): Probability that the pixel simulates fire conditions.

    Returns:
        dict: A dictionary representing the pixel with coordinates, band values,
              vegetation indices, and classification (either 'fire' or 'vegetation').
    """
    is_fire = random.random() < fire_probability

    if is_fire:
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

    NDVI = round((B8 - B4) / (B8 + B4), 3)
    NDMI = round((B8 - B11) / (B8 + B11), 3)
    NDWI = round((B3 - B8) / (B3 + B8), 3)
    NBR = round((B8 - B12) / (B8 + B12), 3)

    pixel_json = {
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "tile_id": macroarea_id,
        "bands": {
            "B2": round(random.uniform(0.05, 0.2), 3),
            "B3": round(B3, 3),
            "B4": round(B4, 3),
            "B8": round(B8, 3),
            "B8A": round(random.uniform(0.05, 0.3), 3),
            "B11": round(B11, 3),
            "B12": round(B12, 3)
        },
        "indices": {
            "NDVI": NDVI,
            "NDMI": NDMI,
            "NDWI": NDWI,
            "NBR": NBR
        },
        "classification": {
            "scene_class": "fire" if is_fire else "vegetation"
        }
    }

    return pixel_json


def firedet_bands_metadata(bbox_list: list, macroarea_id: str, n: int = 80, fire_probability: float = 0.2) -> dict:
    """
    Generate synthetic satellite pixel data for a geographic area.
    Randomly samples n pixels within the bounding box and computes whether
    any pixel is classified as 'fire' based on fire_probability.

    Args:
        bbox_list (list): Bounding box defined as [min_long, min_lat, max_long, max_lat].
        n (int): Number of pixels to generate. Default is 50.
        fire_probability (float): Probability that a pixel simulates fire conditions.

    Returns:
        dict: {
            'fire_detected': bool,
            'satellite_data': list of valid pixel dictionaries
        }

    Raises:
        ValueError: If bbox_list is invalid or n <= 0
    """
    # Validate bounding box
    if not isinstance(bbox_list, (list, tuple)) or len(bbox_list) != 4:
        raise ValueError("[ERROR] bbox_list must be a list of 4 coordinates [min_long, min_lat, max_long, max_lat]")

    try:
        n = int(n)
    except Exception:
        raise ValueError(f"[ERROR] Invalid value for n: {n}. Must be an integer.")

    if n <= 0:
        raise ValueError("[ERROR] n must be a positive integer")

    min_long, min_lat, max_long, max_lat = bbox_list
    sampled_pixels = []
    fire_detected = False

    for i in range(n):
        try:
            lon = random.uniform(min_long, max_long)
            lat = random.uniform(min_lat, max_lat)
            pixel_data = generate_pixel_data(lat, lon, macroarea_id=macroarea_id, fire_probability=fire_probability)

            if pixel_data["classification"]["scene_class"] == "fire":
                fire_detected = True

            sampled_pixels.append(pixel_data)

        except Exception as e:
            print(f"[WARNING] Failed to generate pixel {i+1}/{n}: {e}")
            continue

    metadata = {
        "fire_detected": fire_detected,
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
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    metadata['timestamp'] = timestamp

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


