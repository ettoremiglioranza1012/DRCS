
"""
    Comment here!
"""


# Utiliies 
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.common import Time, Types

from typing import List, Dict, Any, Optional, Tuple, Iterator, Union
from data_templates import INDICES_THRESHOLDS, BANDS_THRESHOLDS
from kafka.admin import KafkaAdminClient
import logging
import random
import redis
import boto3
import time
import json
import uuid
import os


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Kafka configuration - Kafka topic name and access point
KAFKA_TOPIC = "satellite_img"
KAFKA_SERVERS = "kafka:9092"

# MinIO configuration - read from environment variables if available
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")


class S3MinIOSinkBase(MapFunction):
    """
    Base class for MinIO (S3) data persistence functionality.
    
    Provides common functionality for saving data to MinIO in the
    lakahouse architecture layers (bronze, silver, gold).
    """

    def __init__(self) -> None:
        """Initialize the S3 MinIO sink base class."""
        self.s3_client = None

    def open(self, runtime_context: Any) -> None:
        """
        Initialize S3 client connection when the function is first called.
        
        Args:
            runtime_context: Flink runtime context
        """
        try:
            # Initialize S3 client for MinIO
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                region_name='us-east-1',  # Can be any value for MinIO
                config=boto3.session.Config(signature_version='s3v4')
            )
            logger.info(f"Connected to MinIO at: {MINIO_ENDPOINT}")
        except Exception as e:
            logger.error(f"Failed to create S3 client: {str(e)}")
            # Re-raise to fail fast if we can't connect to storage
            raise

    def save_record_to_minio(self, value: str, the_id: str, timestamp: str, 
                           bucket_name: str, partition: str) -> None:
        """
        Save a record to MinIO with appropriate partitioning.
        
        Args:
            value: JSON string data to store
            the_id: ID to use in the filename
            timestamp: Timestamp to use for partitioning and filename
            bucket_name: Target MinIO bucket
            partition: Top-level partition name
        """
        try:
            # Clean timestamp
            timestamp = timestamp.replace(":", "-")
            
            # Extract date for partitioned path
            year_month_day = timestamp.split("T")[0]  # YYYY-MM-DD
            year = year_month_day.split("-")[0]
            month = year_month_day.split("-")[1]
            day = year_month_day.split("-")[2]
            
            # Create a unique file ID
            unique_id = uuid.uuid4().hex[:8]
            
            # Build the file path
            filepath = f"{partition}/year={year}/month={month}/day={day}/{the_id}_{timestamp.replace('T', '_')}_{unique_id}.json"
            
            # Save the record to MinIO
            if self.s3_client:
                try:
                    self.s3_client.put_object(
                        Bucket=bucket_name,
                        Key=filepath,
                        Body=value.encode('utf-8'),
                        ContentType="application/json"
                    )
                    logger.debug(f"Saved to {bucket_name} bucket: {filepath}")
                except Exception as e:
                    logger.error(f"Failed to save to {bucket_name} bucket: {filepath}: {e}")
            else:
                logger.error("S3 client not initialized")
        except Exception as e:
            logger.error(f"Error processing record for MinIO: {e}")


class S3MinIOSinkBronze(S3MinIOSinkBase):
    """
    MinIO sink for raw (bronze) data layer.
    
    Persists raw sensor data to the bronze data layer in MinIO.
    """

    def map(self, value: str) -> str:
        """
        Save raw sensor data to the bronze layer in MinIO.
        
        Args:
            value: JSON string containing raw sensor data
            
        Returns:
            str: Original value (passed through for downstream processing)
        """
        try:
            data = json.loads(value)
            metadata = data.get("metadata", {})
            event_timestamp = metadata.get("timestamp")
            region_id = metadata.get("microarea_id")
            self.save_record_to_minio(value, region_id, event_timestamp, bucket_name='bronze', partition='satellite_row')
           
        except Exception as e:
            logger.error(f"Error while saving to bronze layer: {str(e)}")

        # Pass through for downstream processing
        return value


class S3MinIOSinkSilver(S3MinIOSinkBase):
    """
    MinIO sink for processed (silver) data layer.
    
    Persists processed sensor data to the silver data layer in MinIO,
    separating normal readings from anomalies.
    """

    def map(self, value: str) -> str:
        """
        Save processed sensor data to the silver layer in MinIO.
        
        Args:
            value: JSON string containing processed sensor data
            
        Returns:
            str: Original value (passed through for downstream processing)
        """
        try:
            data = json.loads(value)
            metadata = data.get("metadata", {})
            event_timestamp = metadata.get("timestamp")
            region_id = metadata.get("microarea_id")
            self.save_record_to_minio(value, region_id, event_timestamp, bucket_name='silver', partition='satellite_processed')

        except Exception as e:
            logger.error(f"Error while saving to silver layer: {str(e)}")

        # Pass through for downstream processing
        return value


class IndexAndClassifyFunction(MapFunction):
    """
    Simplified fire detection with early filtering to avoid expensive calculations
    when pixels clearly show normal vegetation
    """
    def __init__(self, indices_thresholds: dict, bands_thresholds: dict) -> str:
        """
        Initialize with thresholds for fire detection
        """
        self.indices_thresholds = indices_thresholds
        self.bands_thresholds = bands_thresholds
    
    def map(self, value: str) -> str:
        """
        Process satellite data with early filtering for performance optimization
        """
        data = json.loads(value)
        pixels_bands = data.get("metadata", {}).get("satellite_data")
        
        for pixel_bands in pixels_bands:
            curr_bands = pixel_bands.get("bands")
            
            # Early filtering - check if clearly normal vegetation
            if self._is_normal_vegetation(curr_bands):
                pixel_bands["classification"] = {
                    "status": "OK",
                    "scene_class": "vegetation",
                    "level": "normal",
                    "confidence": 95,
                    "processing": "quick_screen"
                }
            else:
                # Only do expensive calculations if not clearly normal
                idx = self._calculate_indices(curr_bands)
                scene_class, level, confidence = self._classify_fire_confidence(idx)
                
                pixel_bands["indices"] = idx
                pixel_bands["classification"] = {
                    "scene_class": scene_class,
                    "level": level,
                    "confidence": confidence,
                    "processing": "full_analysis"
                }

        return json.dumps(data)

    def _is_normal_vegetation(self, bands):
        """
        Quick screening using raw bands to identify normal vegetation
        Returns True if clearly normal vegetation (no fire risk)
        """
        B4 = bands.get("B4", 0)  # Red
        B8 = bands.get("B8", 0)  # NIR
        B11 = bands.get("B11", 0)  # SWIR 1.6μm
        B12 = bands.get("B12", 0)  # SWIR 2.2μm
        
        # Healthy vegetation indicators (hardcoded thresholds)
        healthy_nir = B8 > self.bands_thresholds["healthy_nir"]  # Good NIR reflectance
        low_thermal = B11 < self.bands_thresholds["low_termal_B11"] and B12 < self.bands_thresholds["low_termal_B12"]  # Low thermal signature
        vegetation_ratio = (B8 / B4) > self.bands_thresholds["vegetation_ratio"] if B4 > 0 else False  # Strong NIR/Red ratio
        
        # If 2 or more indicators suggest healthy vegetation, skip expensive calculations
        indicators = sum([healthy_nir, low_thermal, vegetation_ratio])
        return indicators >= 2

    def _calculate_indices(self, bands):
        """Calculate vegetation and fire indices from Sentinel-2 bands"""
        B3 = bands.get("B3", 0)
        B4 = bands.get("B4", 0)
        B8 = bands.get("B8", 0)
        B8A = bands.get("B8A", 0)
        B11 = bands.get("B11", 0)
        B12 = bands.get("B12", 0)

        # NDVI (Normalized Difference Vegetation Index)
        NDVI = (B8 - B4) / (B8 + B4) if (B8 + B4) != 0 else 0
        
        # NDMI (Normalized Difference Moisture Index) 
        NDMI = (B8 - B11) / (B8 + B11) if (B8 + B11) != 0 else 0
        
        # NDWI (Normalized Difference Water Index)
        NDWI = (B3 - B8) / (B3 + B8) if (B3 + B8) != 0 else 0
        
        # NBR (Normalized Burn Ratio)
        NBR = (B8 - B12) / (B8 + B12) if (B8 + B12) != 0 else 0
        
        return {
            "NDVI": round(NDVI, 4),
            "NDMI": round(NDMI, 4), 
            "NDWI": round(NDWI, 4),
            "NBR": round(NBR, 4)
        }

    def _classify_fire_confidence(self, indices):
        """
        Multi-criteria fire classification based on vegetation indices
        Returns classification with confidence level
        """
        NDVI = indices["NDVI"]
        NDMI = indices["NDMI"] 
        NDWI = indices["NDWI"]
        NBR = indices["NBR"]
        
        # Fire indication criteria
        fire_indicators = 0
        confidence_score = 0
        
        # NDVI thresholds
        if NDVI < self.indices_thresholds["NDVI_low"]:
            fire_indicators += 1
            confidence_score += 25
        if NDVI < self.indices_thresholds["NDVI_verylow"]:
            fire_indicators += 1
            confidence_score += 35
        
        # NDMI thresholds  
        if NDMI < self.indices_thresholds["NDMI_low"]:
            fire_indicators += 1
            confidence_score += 20
        if NDMI < self.indices_thresholds["NDMI_verylow"]:
            fire_indicators += 1
            confidence_score += 40
            
        # NBR thresholds
        if NBR < self.indices_thresholds["NBR_potential_burn"]:
            fire_indicators += 1
            confidence_score += 15
        if NBR < self.indices_thresholds["NBR_strong_burn"]:
            fire_indicators += 1
            confidence_score += 25
        
        # NDWI thresholds
        if NDWI < self.indices_thresholds["NDWI_low"]:
            fire_indicators += 1
            confidence_score += 10
            
        # Classification logic
        if fire_indicators >= 4 and confidence_score >= 80:
            return "fire", "high", confidence_score
        elif fire_indicators >= 3 and confidence_score >= 60:
            return "fire", "medium", confidence_score  
        elif fire_indicators >= 2 and confidence_score >= 40:
            return "fire", "low", confidence_score
        else:
            return "vegetation", "normal", confidence_score
        

class EnrichFromRedis(MapFunction):
    """
        Comment Here!
    """

    def __init__(self, max_retries: int = 5, initial_backoff: float = 1.0, 
                 max_backoff: float = 30.0) -> None:
        """
        Initialize the Redis enrichment function.
        
        Args:
            max_retries: Maximum number of retries for Redis connection
            initial_backoff: Initial backoff time in seconds
            max_backoff: Maximum backoff time in seconds
        """
        self.redis_client = None
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff

    def _default_region_data(self) -> Dict[str, Any]:
        """
        Return default metadata when Redis data is unavailable.
        
        Returns:
            Dict[str, Any]: Default station metadata structure
        """
        return {
            "min_long": None,
            "min_lat": None,
            "max_long": None,
            "max_lat": None
        }

    def _wait_for_redis_data(self) -> bool:
        """
        Wait until Redis has some station metadata populated.
        
        Returns:
            bool: True if Redis is ready with data, False otherwise
        """
        wait_time = self.initial_backoff
        current_waited_time_interval = 0
        max_total_wait = 120  # Maximum 2 minutes total wait time

        for attempt in range(1, self.max_retries + 1):
            try:
                if self.redis_client.ping() and self.redis_client.keys("microarea:*"):
                    logger.info(f"Redis is ready with data after {attempt} attempts")
                    return True
                else:
                    logger.warning(f"Redis is running but no microarea   data available yet. Attempt {attempt}/{self.max_retries}")
            except redis.exceptions.ConnectionError as e:
                logger.warning(f"Redis connection failed on attempt {attempt}/{self.max_retries}: {e}")
            
            # Exponential backoff with jitter
            jitter = random.uniform(0, 0.1 * wait_time)
            sleep_time = min(wait_time + jitter, self.max_backoff)

            # Check if we would exceed our maximum allowed wait time
            if current_waited_time_interval + sleep_time > max_total_wait:
                logger.warning(f"Maximum wait time of {max_total_wait}s exceeded. Proceeding with default metadata.")
                return False

            logger.info(f"Waiting {sleep_time:.2f}s before retrying Redis connection...")
            time.sleep(sleep_time)
            current_waited_time_interval += sleep_time
            wait_time *= 2
        
        logger.error("Failed to connect to Redis with data after maximum retries")
        return False

    def open(self, runtime_context: Any) -> None:
        """
        Initialize Redis client connection when the function is first called.
        
        Args:
            runtime_context: Flink runtime context
        """
        try:
            self.redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                health_check_interval=30
            )
            logger.info("Connected to Redis")

            if not self._wait_for_redis_data():
                logger.warning("Proceeding without confirmed Redis data. Using default metadata as fallback.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None
        
    def map(self, value:str)-> str:
        """
            Comment Here!
        """
        try:
            data = json.loads(value)
            metadata = data.get("metadata", {})
            microarea_id = metadata.get("microarea_id")
            redis_key = f"microarea:{microarea_id}"

            try:
                if self.redis_client:
                    region_info_json = self.redis_client.get(redis_key)
                    region_info = json.loads(region_info_json)

                    if region_info:
                        metadata["microarea_info"] = region_info

                    else:
                        logger.warning(f"No microarea infos found for {redis_key}, using default.")
                        metadata["microarea_info"] = self._default_region_data()                        
                
                else:
                    logger.warning(f"Redis client is None. Using default micro area infos for {redis_key}")
                    metadata["microarea_info"] = self._default_region_data()                    
                
            except Exception as e:
                logger.error(f"Failed to enrich {redis_key}: {e}")
                metadata["microarea_info"] = self._default_region_data()
                
            return json.dumps(data)

        except Exception as e:
            logger.error(f"Error in Redis enrichment: {str(e)}")
            # Return original value to ensure pipeline continues
            return value            


def wait_for_minio_ready(
    endpoint: str, 
    access_key: str, 
    secret_key: str, 
    max_retries: int = 20, 
    retry_interval: int = 5
) -> None:
    """
    Wait for MinIO service to be ready and accessible.
    
    This function attempts to connect to a MinIO service and verifies it's operational
    by listing the available buckets. It will retry the connection based on the specified
    parameters.
    
    Args:
        endpoint: The host:port address of the MinIO service
        access_key: The MinIO access key for authentication
        secret_key: The MinIO secret key for authentication
        max_retries: Maximum number of connection attempts (default: 20)
        retry_interval: Time in seconds between retry attempts (default: 5)
    """
    for i in range(max_retries):
        try:
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{endpoint}",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            s3.list_buckets()  # just ping
            logger.info("MinIO is ready")
            return
        except Exception as e:
            logger.warning(f"MinIO not ready (attempt {i+1}/{max_retries}): {e}")
            time.sleep(retry_interval)
    raise Exception("MinIO is not ready after retries")


def wait_for_kafka_ready(
    bootstrap_servers: Union[str, List[str]], 
    max_retries: int = 30, 
    retry_interval: int = 10,
) -> None:
    """
    Wait for Kafka cluster to be ready and accessible.
    
    This function attempts to connect to a Kafka cluster and verifies it's operational
    by listing the available topics. It will retry the connection based on the specified
    parameters.
    
    Args:
        bootstrap_servers: Kafka broker address(es) as string or list of strings
        max_retries: Maximum number of connection attempts (default: 30)
        retry_interval: Time in seconds between retry attempts (default: 10)
    """

    for i in range(max_retries):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            admin_client.list_topics() # just ping
            admin_client.close()
            print("Kafka cluster is ready")
            return True
        except Exception as e:
            print(f"Kafka not ready (attempt {i+1}/{max_retries}): {e}")
            time.sleep(retry_interval)
    
    raise Exception("Kafka cluster not yet configured after maximum retries")


def main():

    logger.info("Starting Flink job initialization")

    # Wait for minIO to be ready
    wait_for_minio_ready(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    # Wait for kafka server and configuration to be ready
    wait_for_kafka_ready(KAFKA_SERVERS)

    # Flink job configuration
    env = StreamExecutionEnvironment.get_execution_environment()
    logger.info("Flink environment created")
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    env.enable_checkpointing(120000)  # Check point every two minutes

    # Kafka consumer configuration
    properties = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'satellite_flink_consumer_group',  
        'request.timeout.ms': '60000',  # longer timeout
        'retry.backoff.ms': '5000',     # backoff between retries
        'reconnect.backoff.ms': '5000', # backoff for reconnections
        'reconnect.backoff.max.ms': '30000', # max backoff
    }

    # Establish Kafka consumer 
    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )

    # Source with timestamps and watermarks
    ds = env.add_source(kafka_consumer, type_info=Types.STRING())

    # Save each record to MinIO
    ds.map(S3MinIOSinkBronze(), output_type=Types.STRING())

    # Filter and indices computations
    processed_stream = (
        ds.key_by(lambda x: json.loads(x).get("metadata", {}).get("macroarea_id"), key_type=Types.STRING()) # Processing macroarea in parallel
        .map(IndexAndClassifyFunction(INDICES_THRESHOLDS, BANDS_THRESHOLDS), output_type=Types.STRING())
        .map(EnrichFromRedis(), output_type=Types.STRING())
    )

    # Save each processed record to MinIO
    processed_stream.map(S3MinIOSinkSilver(), output_type=Types.STRING())


    logger.info("Executing Flink job")
    env.execute("IoT Measurements Processing Pipeline")
    logger.info("Flink job execution initiated")


if __name__ == "__main__":
    main()

