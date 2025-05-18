
"""
Wildfire Detection System - Real-time Sensor Data Processing

This module implements a Flink-based streaming data processing system for wildfire detection
using IoT sensor data. It processes data through a lambda architecture with bronze, silver,
and gold data layers.

The system:
1. Ingests sensor data from Kafka
2. Processes and filters based on threshold values
3. Enriches data with station metadata from Redis
4. Aggregates data by geographical regions
5. Persists data to MinIO (S3-compatible storage) in the lambda architecture layers
6. Generates wildfire alerts with severity scores and recommended actions

Dependencies:
    - Apache Flink (PyFlink)
    - Redis
    - MinIO
    - Kafka
    - boto3
"""


# Utilities
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Time, Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction

from typing import List, Dict, Any, Optional, Tuple, Iterator, Union
from kafka.admin import KafkaAdminClient
from datetime import datetime
import logging
import random
import redis
import boto3
import time
import json
import uuid
import sys
import os


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Kafka configuration - Kafka topic name and access point
KAFKA_TOPIC = "sensor_meas"
KAFKA_SERVERS = "kafka:9092"

# MinIO configuration - read from environment variables if available
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Threshold values
THRESHOLDS = {
            "temperature_c": 35.0,
            "humidity_percent": 80.0,
            "co2_ppm": 600.0,
            "pm25_ugm3": 12.0,
            "smoke_index": 20.0,
            "infrared_intensity": 0.2,
            "battery_voltage": 3.7
        }


# Hardcoded notifications simulation, to improve with more dynamical stuff
SENT_NOTIFICATION_TO = [
      {
        "agency": "local_fire_department",
        "delivery_status": "confirmed"
      },
      {
        "agency": "emergency_management",
        "delivery_status": "confirmed"
      }
    ]

RECOMMENDED_ACTIONS = [
      {
        "action": "deploy_fire_units",
        "priority": "high",
        "recommended_resources": ["engine_company", "brush_unit", "water_tender"]
      },
      {
        "action": "evacuate_area",
        "priority": "medium",
        "radius_meters": 3000,
        "evacuation_direction": "east"
      }
    ]

AT_RISK_ASSETS = {
      "population_centers": [
        {
          "name": "Highland Park",
          "distance_meters": 2500,
          "population": 12500,
          "evacuation_priority": "high"
        }
      ],
      "critical_infrastructure": [
        {
          "type": "power_substation",
          "name": "Highland Substation",
          "distance_meters": 1800,
          "priority": "high"
        },
        {
          "type": "water_reservoir",
          "name": "Eagle Rock Reservoir",
          "distance_meters": 3200,
          "priority": "medium"
        }
      ]
    }

ENVIRONMENTAL_CONTEXT = {
    "weather_conditions": {
      "temperature": 35.2,
      "humidity": 18.4,
      "wind_speed": 25.1,
      "wind_direction": 285.3,
      "precipitation_chance": 0.02
    },
    "terrain_info": {
      "vegetation_type": "chaparral",
      "vegetation_density": "high",
      "slope": "moderate",
      "aspect": "west-facing"
    }
  }


class JsonTimestampAssigner(TimestampAssigner):
    """
    Extracts event timestamps from JSON data for Flink's event time processing.
    
    This class implements Flink's TimestampAssigner interface to extract timestamps
    from the JSON payload of sensor measurements, enabling proper event time processing.
    
    Returns:
        int: Timestamp in milliseconds since epoch
    """
    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        """
        Extract timestamp from JSON string value.
        
        Args:
            value (str): JSON string containing sensor data
            record_timestamp (int): Default timestamp provided by Flink
            
        Returns:
            int: Timestamp in milliseconds since epoch
        """
        try:
            data = json.loads(value)
            ts = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%S.%f")
            return int(ts.timestamp() * 1000)
        except Exception:
            return 0


class S3MinIOSinkBase(MapFunction):
    """
    Base class for MinIO (S3) data persistence functionality.
    
    Provides common functionality for saving data to MinIO in the
    lambda architecture layers (bronze, silver, gold).
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
        # Clean timestamp
        timestamp = timestamp.replace(":", "-")
        
        try:
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
            station_id = data["station_id"]
            timestamp = data["timestamp"]
            self.save_record_to_minio(value, station_id, timestamp, bucket_name='bronze', partition='iot_raw')
           
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
            station_id = data["station_id"]
            timestamp_epoch_millis = data["response_timestamp"]
            # Convert epoch milliseconds to datetime
            timestamp = datetime.fromtimestamp(timestamp_epoch_millis / 1000.0).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] 
            
            # Route to appropriate partition based on whether anomaly was detected
            if "detection_flags" in data:
                # Save record to anomalies partition
                self.save_record_to_minio(value, station_id, timestamp, 
                                        bucket_name='silver', partition='iot_processed/anomalies')
            else:
                # Save record to normal partition
                self.save_record_to_minio(value, station_id, timestamp, 
                                        bucket_name='silver', partition='iot_processed/normal')

        except Exception as e:
            logger.error(f"Error while saving to silver layer: {str(e)}")

        # Pass through for downstream processing
        return value
    

class S3MinIOSinkGold(S3MinIOSinkBase):
    """
    MinIO sink for aggregated (gold) data layer.
    
    Persists aggregated and enriched data to the gold data layer in MinIO,
    separating normal readings from wildfire events.
    """

    def map(self, value: str) -> str:
        """
        Save aggregated data to the gold layer in MinIO.
        
        Args:
            value: JSON string containing aggregated event data
            
        Returns:
            str: Original value (passed through for downstream processing)
        """
        try:
            data = json.loads(value)
            region_id = data["region_id"]
            timestamp_epoch_millis = data["response_timestamp"]
            # Convert epoch milliseconds to datetime
            timestamp = datetime.fromtimestamp(timestamp_epoch_millis / 1000.0).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            
            # Route to appropriate partition based on event type
            if data["event_type"] == "normal":
                self.save_record_to_minio(value, region_id, timestamp, 
                                        bucket_name='gold', partition='iot_gold/normal')
            elif data["event_type"] == "wildfire":
                self.save_record_to_minio(value, region_id, timestamp, 
                                        bucket_name='gold', partition='iot_gold/anomalies')
            else:
                logger.error(f"Unknown event type for region: {data['region_id']}")

        except Exception as e:
            logger.error(f"Error while saving to gold layer: {str(e)}")

        # Pass through for downstream processing
        return value   


class ThresholdFilterWindowFunction(ProcessWindowFunction):
    """
    Process window function that filters and aggregates sensor data based on thresholds.
    
    For each time window, this function:
    1. Checks if any measurements exceed configured thresholds
    2. If any exceed thresholds, computes means of measurements across the window
    3. Adds detection flags for wildfire detection
    
    Args:
        thresholds: Dictionary of measurement thresholds to compare against
    """

    def __init__(self, thresholds: Dict[str, float]):
        """
        Initialize the threshold filter.
        
        Args:
            thresholds: Dictionary of measurement thresholds to compare against
        """
        self.thresholds = thresholds

    def process(self, key: str, context: Any, elements: List[str]) -> List[str]:
        """
        Process sensor data within a time window, filtering by thresholds.
        
        Args:
            key: Grouping key (station_id)
            context: Window context
            elements: List of JSON strings containing sensor data
            
        Returns:
            List[str]: List containing either an anomaly detection or normal status message
        """
        filtered_results = []
        raw_json_objects = []

        try:
            # Step 1: filter by threshold
            for element in elements:
                try:
                    data = json.loads(element)
                    raw_json_objects.append(data)  # Save parsed data for reuse

                    exceeded_thresholds = {}
                    for field, threshold_value in self.thresholds.items():
                        if field not in data.get("measurements", {}):
                            continue
                        if data["measurements"][field] > threshold_value:
                            exceeded_thresholds[field] = data["measurements"][field]

                    if exceeded_thresholds:
                        data["exceeded_thresholds"] = exceeded_thresholds
                        filtered_results.append(data)  # Save as dict for mean computation later

                except Exception as e:
                    logger.error(f"Error processing element in threshold filter: {str(e)}")

            # Step 2: compute mean only if something was filtered
            if filtered_results:
                measurements_accumulator = {}
                counts = {}

                for record in filtered_results:
                    for field, value in record.get("measurements", {}).items():
                        if field in self.thresholds:
                            measurements_accumulator[field] = measurements_accumulator.get(field, 0.0) + value
                            counts[field] = counts.get(field, 0) + 1

                mean_measurements_data = {
                    field: round(measurements_accumulator[field] / counts[field], 2)
                    for field in measurements_accumulator
                }

                response_timestamp = context.window().end  # Use window end time
                mean_measurements = {
                    'station_id': filtered_results[0]['station_id'],
                    'response_timestamp': response_timestamp,
                    'measurements': mean_measurements_data
                }

                # Aggregate detection flags
                detection_flags = {
                    "wildfire_detected": True,
                    "smoke_detected": True,
                    "flame_detected_ir": True,
                    "anomaly_detected": True,
                    "anomaly_type": "wildfire"
                }

                mean_measurements['detection_flags'] = detection_flags

                # Return a list of field values that match Flink's expected Row structure
                result_json = json.dumps(mean_measurements)
                return [result_json]
            
            # Nothing exceeded: return status in same format
            station_id = key
            if raw_json_objects:
                station_id = raw_json_objects[0]["station_id"]
                
            status_message = {
                "status": "OK",
                "message": "No anomalies detected",
                "station_id": station_id,
                "response_timestamp": context.window().end  # Use window end time
            }
            # Return a list of field values that match Flink's expected Row structure
            status_json = json.dumps(status_message)
            return [status_json]
            
        except Exception as e:
            logger.error(f"Error in threshold filter window function: {str(e)}")
            # Return a basic error status - ensures the pipeline continues
            error_message = {
                "status": "ERROR",
                "message": f"Error processing window: {str(e)}",
                "station_id": key,
                "response_timestamp": context.window().end
            }
            return [json.dumps(error_message)]


class EnrichFromRedis(MapFunction):
    """
    Enriches sensor data with station metadata from Redis.
    
    Retrieves station metadata from Redis and adds it to the sensor data,
    with retry logic and default fallbacks if metadata is unavailable.
    
    Args:
        max_retries: Maximum number of retries for Redis connection
        initial_backoff: Initial backoff time in seconds
        max_backoff: Maximum backoff time in seconds
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

    def _default_metadata(self) -> Dict[str, Any]:
        """
        Return default metadata when Redis data is unavailable.
        
        Returns:
            Dict[str, Any]: Default station metadata structure
        """
        return {
            "position": {
                'microarea_id': "UNKNOWN",
                'latitude': None,
                'longitude': None,
                'elevation_m': None
            },
            "station_model": "UNKNOWN",
            "deployment_date": None,
            "maintenance_status": "unknown",
            "sensors": {
                'temp_sens': False,
                'hum_sens': False,
                'co2_sens': False,
                'pm25_sens': False,
                'smoke_sens': False,
                'ir_sens': False
            },
            "battery_type": "UNKNOWN"
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
                if self.redis_client.ping() and self.redis_client.keys("station:*"):
                    logger.info(f"Redis is ready with data after {attempt} attempts")
                    return True
                else:
                    logger.warning(f"Redis is running but no station data available yet. Attempt {attempt}/{self.max_retries}")
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

    def map(self, value: str) -> str:
        """
        Enrich sensor data with station metadata from Redis.
        
        Args:
            value: JSON string containing sensor data
            
        Returns:
            str: JSON string with added station metadata
        """
        try:
            data = json.loads(value)
            station_id = data.get("station_id")
            redis_key = f"station:{station_id}"

            try:
                if self.redis_client:
                    metadata_json = self.redis_client.get(redis_key)
                    if metadata_json:
                        station_metadata = json.loads(metadata_json)
                        data["station_metadata"] = {
                            "position": {
                                'microarea_id': station_metadata.get("microarea_id", "UNKNOWN"),
                                'latitude': station_metadata.get("latitude"),
                                'longitude': station_metadata.get("longitude"),
                                'elevation_m': station_metadata.get("elevation_m")
                            },
                            "station_model": station_metadata.get("model", "UNKNOWN"),
                            "deployment_date": station_metadata.get("install_date"),
                            "maintenance_status": "operational",
                            "sensors": {
                                'temp_sens': station_metadata.get("temp_sens", False),
                                'hum_sens': station_metadata.get("hum_sens", False),
                                'co2_sens': station_metadata.get("co2_sens", False),
                                'pm25_sens': station_metadata.get("pm25_sens", False),
                                'smoke_sens': station_metadata.get("smoke_sens", False),
                                'ir_sens': station_metadata.get("ir_sens", False)
                            },
                            "battery_type": station_metadata.get("battery_type", "UNKNOWN")
                        }
                    else:
                        logger.warning(f"No metadata found for {redis_key}, using default.")
                        data["station_metadata"] = self._default_metadata()
                else:
                    logger.warning(f"Redis client is None. Using default metadata for {redis_key}")
                    data["station_metadata"] = self._default_metadata()
            except Exception as e:
                logger.error(f"Failed to enrich {redis_key}: {e}")
                data["station_metadata"] = self._default_metadata()

            return json.dumps(data)
            
        except Exception as e:
            logger.error(f"Error in Redis enrichment: {str(e)}")
            # Return original value to ensure pipeline continues
            return value

    def close(self) -> None:
        """Close Redis connection when the function is done."""
        if self.redis_client:
            try:
                self.redis_client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")


class GoldAggregatorWindowFunction(ProcessWindowFunction):
    """
    Window function that aggregates sensor data from multiple stations to detect wildfire events.
    
    This class processes batches of station data within time windows, identifying anomalies
    and computing various metrics to determine if a wildfire event is occurring. It dynamically
    handles varying numbers of sensors across stations and produces rich event data.
    
    Attributes:
        None - This class doesn't maintain state between window processing calls.
    """
    
    def process(self, key: str, context, elements: Iterator[str]) -> List[str]:
        """
        Process sensor data from multiple stations within a window to detect wildfire events.
        
        Dynamically computes the number of stations that detected anomalies and creates a 
        dictionary with the number of fields that detected an anomaly for each station. This 
        approach allows computing the average of measurements above threshold among all stations,
        even when stations have different sensor configurations.
        
        Args:
            key: The key for the current window (typically a grouping identifier)
            context: The window processing context containing window metadata
            elements: Iterator of JSON strings containing station data
            
        Returns:
            List[str]: A single-element list containing the JSON string of the processed gold data
            
        Raises:
            Exception: Any unhandled exception during processing will be logged
        """
        try:
            gold_data = {}
            gold_data_stations = []

            # Initialize tracking variables
            microarea_id = None
            response_timestamp = None
            anomalous_stations_count = 0
            total_stations_count = 0  # Will count actual valid elements

            # Measurements accumulators
            anomalous_measurements_accumulator = {}
            anomalous_fields_counts = {}

            # Initialize max values for measurements (to track for critical values)
            max_critical_meas = {}
            
            # Convert iterator to list to get accurate element count and avoid issues with iterator exhaustion
            elements_list = list(elements)
            total_stations_count = len(elements_list)
            
            if total_stations_count == 0:
                logger.warning("No elements to process in the current window")
                return []
            
            for element in elements_list:
                try:
                    # Parse the station data 
                    data = json.loads(element)

                    # Extract common fields
                    if microarea_id is None and "station_metadata" in data:
                        microarea_id = data["station_metadata"]["position"]["microarea_id"]
                        logger.debug(f"Processing data for microarea: {microarea_id}")

                    # Extract timestamp using the appropriate field
                    curr_timestamp = data.get("response_timestamp")
                    if response_timestamp is None or curr_timestamp > response_timestamp:
                        response_timestamp = curr_timestamp

                    # Add to station list
                    gold_data_stations.append(data)

                    # Check if this station detected an anomaly
                    has_anomaly = False
                    if "detection_flags" in data and data["detection_flags"].get("anomaly_detected", False):
                        anomalous_stations_count += 1
                        has_anomaly = True
                        logger.debug(f"Anomaly detected in station data: {data.get('station_id', 'unknown')}")

                    # Process measurements
                    self._process_measurements(data, has_anomaly, max_critical_meas, 
                                              anomalous_measurements_accumulator, anomalous_fields_counts)

                except json.JSONDecodeError as je:
                    logger.error(f"JSON parsing error: {str(je)}")
                    continue
                except KeyError as ke:
                    logger.error(f"Missing key in station data: {str(ke)}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing element: {str(e)}")
                    continue
        
            # Return a simplified record in case of no anomalies
            if anomalous_stations_count == 0 or total_stations_count == 0:
                return self._create_normal_event(microarea_id, response_timestamp, gold_data_stations)
            
            # Calculate metrics and create gold data for wildfire events
            return self._create_wildfire_event(
                microarea_id, response_timestamp, gold_data_stations,
                anomalous_stations_count, total_stations_count,
                anomalous_measurements_accumulator, anomalous_fields_counts,
                max_critical_meas, context
            )
            
        except Exception as e:
            logger.error(f"Critical error in process method: {str(e)}", exc_info=True)
            return []
    
    def _process_measurements(self, data: Dict[str, Any], has_anomaly: bool, 
                             max_critical_meas: Dict[str, float],
                             anomalous_measurements_accumulator: Dict[str, float],
                             anomalous_fields_counts: Dict[str, int]) -> None:
        """
        Process measurements from a single station, updating accumulators and tracking max values.
        
        Args:
            data: Dictionary containing station data
            has_anomaly: Boolean indicating if the station detected an anomaly
            max_critical_meas: Dictionary tracking maximum values for each measurement type
            anomalous_measurements_accumulator: Dictionary accumulating values for anomalous stations
            anomalous_fields_counts: Dictionary counting fields for anomalous stations
            Dinamically computes the number of stations that detected anomalies, and also create a dictionary with the number
            of fields that detected an anomaly for each stations to then compute the average of measurements above threshold
            among all stations. Useful because if in the future some stations will have only some of the sensors the code is able
            to compute the averages in a dynamic way.
            
        Returns:
            None - Updates the provided dictionaries in-place
        """
        measurements = data.get("measurements")
        if not measurements:
            return
            
        # Initialize max_critical_meas with the first measurements we see
        if not max_critical_meas:
            max_critical_meas = {field: value for field, value in measurements.items()
                                if field != "battery_voltage"}
        
        # Process each measurement
        for curr_field, curr_value in measurements.items():
            if curr_field == "battery_voltage":
                continue
                
            # Update anomalous measurements accumulator (only for stations with anomalies)
            if has_anomaly:
                if curr_field in anomalous_measurements_accumulator:
                    anomalous_measurements_accumulator[curr_field] += curr_value
                    anomalous_fields_counts[curr_field] += 1
                else:
                    anomalous_measurements_accumulator[curr_field] = curr_value
                    anomalous_fields_counts[curr_field] = 1
            
            # Update max critical values (handle humidity differently)
            if curr_field == "humidity_percent":
                # Lower humidity is worse for fire risk
                if curr_field not in max_critical_meas or curr_value < max_critical_meas[curr_field]:
                    max_critical_meas[curr_field] = curr_value
            else:
                # Higher values are worse for all other metrics
                if curr_field not in max_critical_meas or curr_value > max_critical_meas[curr_field]:
                    max_critical_meas[curr_field] = curr_value
    
    def _create_normal_event(self, microarea_id: Optional[str], response_timestamp: Optional[int], 
                            gold_data_stations: List[Dict[str, Any]]) -> List[str]:
        """
        Create a normal (non-anomalous) event record when no anomalies are detected.
        
        Args:
            microarea_id: Identifier for the monitored area
            response_timestamp: Timestamp for the response
            gold_data_stations: List of station data dictionaries
            
        Returns:
            List[str]: A single-element list containing the JSON string of the normal event
        """
        if microarea_id is None:
            logger.warning("Missing microarea_id for normal event")
            microarea_id = "unknown"
            
        if response_timestamp is None:
            logger.warning("Missing response_timestamp for normal event")
            response_timestamp = int(datetime.now().timestamp() * 1000)
            
        unique_id = uuid.uuid4().hex[:8]
        gold_data = {
            "event_id": f"{microarea_id}_{response_timestamp}_{unique_id}",
            "region_id": microarea_id,
            "response_timestamp": response_timestamp,
            "event_type": "normal",
            "detection_source": "sensor_network",
            "aggregated_detection": {
                "wildfire_detected": False,
                "detection_confidence": 0.0,
                "severity_score": 0.0,
                "anomaly_detected": False,
                "anomaly_type": None
            },
            "stations": gold_data_stations
        }
        
        logger.info(f"Created normal event for microarea: {microarea_id}")
        return [json.dumps(gold_data)]
    
    def _create_wildfire_event(self, microarea_id: Optional[str], response_timestamp: Optional[int],
                              gold_data_stations: List[Dict[str, Any]],
                              anomalous_stations_count: int, total_stations_count: int,
                              anomalous_measurements_accumulator: Dict[str, float],
                              anomalous_fields_counts: Dict[str, int],
                              max_critical_meas: Dict[str, float],
                              context) -> List[str]:
        """
        Create a wildfire event record when anomalies are detected.
        
        Calculates various metrics such as severity score, confidence, air quality,
        estimated ignition time, and fire behavior based on sensor data.
        
        Args:
            microarea_id: Identifier for the monitored area
            response_timestamp: Timestamp for the response
            gold_data_stations: List of station data dictionaries
            anomalous_stations_count: Number of stations that detected anomalies
            total_stations_count: Total number of stations
            anomalous_measurements_accumulator: Dictionary of accumulated measurements from anomalous stations
            anomalous_fields_counts: Dictionary counting fields for anomalous stations
            max_critical_meas: Dictionary of maximum values for each measurement type
            context: The window processing context
            
        Returns:
            List[str]: A single-element list containing the JSON string of the wildfire event
        """
        if microarea_id is None:
            logger.warning("Missing microarea_id for wildfire event")
            microarea_id = "unknown"
            
        if response_timestamp is None:
            logger.warning("Missing response_timestamp for wildfire event")
            response_timestamp = int(datetime.now().timestamp() * 1000)
            
        # Calculate averages values only for anomalous stations
        anomalous_avgs_values = self._calculate_anomalous_averages(
            anomalous_measurements_accumulator, anomalous_fields_counts)

        # Get number of sensors (different measurement types)
        number_of_measurements = len([k for k in anomalous_measurements_accumulator.keys() 
                                     if k != "battery_voltage"])

        # Compute severity score and related metrics
        severity_score, critical_value_quota = self._calculate_severity_score(
            anomalous_stations_count, total_stations_count,
            anomalous_fields_counts, anomalous_avgs_values,
            max_critical_meas, number_of_measurements)
        
        # Calculate detection confidence
        detection_confidence = min(0.7 + (anomalous_stations_count/total_stations_count) * 0.2 + 
                                  severity_score * 0.1, 0.99)

        # Calculate air quality metrics
        air_quality_index, air_quality = self._calculate_air_quality(
            anomalous_measurements_accumulator, anomalous_fields_counts)

        # Estimate ignition time
        estimated_ignition_time = self._estimate_ignition_time(response_timestamp, severity_score)
        
        # Determine fire behavior
        fire_behavior = self._determine_fire_behavior(
            anomalous_measurements_accumulator, anomalous_fields_counts)

        # Determine alert level based on severity
        alert_level = self._determine_alert_level(severity_score)
        
        # Create notifications
        send_notifications = self._create_notifications(context)
        
        # Build the gold data structure
        unique_id = uuid.uuid4().hex[:8]
        gold_data = {
            "event_id": f"{microarea_id}_{response_timestamp}_{unique_id}",
            "region_id": microarea_id,
            "response_timestamp": response_timestamp,
            "event_type": "wildfire",
            "detection_source": "sensor_network",
            "aggregated_detection": {
                "wildfire_detected": True,
                "detection_confidence": detection_confidence,
                "severity_score": severity_score,
                "anomaly_detected": True,
                "anomaly_type": "wildfire",
                "air_quality_index": air_quality_index,
                "air_quality_status": air_quality,
                "estimated_ignition_time": estimated_ignition_time,
                "fire_behavior": fire_behavior
            },
            "environmental_context": ENVIRONMENTAL_CONTEXT,
            "system_response": {
                "event_triggered": "wildfire_alert",
                "alert_level": alert_level,
                "action_taken": "activated_coordination_and_recovery_system",
                "automated": True,
                "at_risk_assets": AT_RISK_ASSETS,
                "recommended_actions": RECOMMENDED_ACTIONS,
                "sent_notifications_to": send_notifications
            },
            "stations": gold_data_stations
        }
        
        logger.info(f"Created wildfire event for microarea: {microarea_id} with severity: {severity_score}")
        return [json.dumps(gold_data)]
        
    def _calculate_anomalous_averages(self, anomalous_measurements_accumulator: Dict[str, float],
                                     anomalous_fields_counts: Dict[str, int]) -> Dict[str, float]:
        """
        Calculate average values for each measurement type from anomalous stations.
        
        Args:
            anomalous_measurements_accumulator: Dictionary of accumulated measurements
            anomalous_fields_counts: Dictionary counting fields for anomalous stations
            
        Returns:
            Dict[str, float]: Dictionary of average values with keys prefixed by 'anom_avg_'
        """
        return {
            f"anom_avg_{field}": round(anomalous_measurements_accumulator[field] / 
                                      anomalous_fields_counts[field], 2)
            for field in anomalous_measurements_accumulator.keys()
        }
    
    def _calculate_severity_score(self, anomalous_stations_count: int, total_stations_count: int,
                                 anomalous_fields_counts: Dict[str, int],
                                 anomalous_avgs_values: Dict[str, float],
                                 max_critical_meas: Dict[str, float],
                                 number_of_measurements: int) -> Tuple[float, float]:
        """
        Calculate severity score based on anomalous stations ratio and measurement criticality.
        
        Args:
            anomalous_stations_count: Number of stations that detected anomalies
            total_stations_count: Total number of stations
            anomalous_fields_counts: Dictionary counting fields for anomalous stations
            anomalous_avgs_values: Dictionary of average values for measurements
            max_critical_meas: Dictionary of maximum values for each measurement type
            number_of_measurements: Number of different measurement types
            
        Returns:
            Tuple[float, float]: Severity score and critical value quota
        """
        # Compute critical value quota based on measurements
        if len(anomalous_fields_counts) > 0 and number_of_measurements > 0:
            critical_value_quota = 0
            for field, max_value in max_critical_meas.items():
                avg_key = f"anom_avg_{field}"
                if avg_key not in anomalous_avgs_values:
                    continue
                    
                # Special case for humidity (Lower is worse)
                if field == "humidity_percent":
                    ratio = anomalous_avgs_values[avg_key] / max_value if max_value > 0 else 1
                    critical_value_quota += (1-ratio) * (1/number_of_measurements)
                    """
                    1 - ratio serves to transform a "low value = worse" into an indicator of increasing criticality, 
                    keeping the logic uniform with the other parameters where 'high value = worse.
                    """
                else:
                    # Current value / maximum value (higher ratio = worse)
                    ratio = anomalous_avgs_values[avg_key] / max_value if max_value > 0 else 0
                    critical_value_quota += ratio * (1/number_of_measurements)
        else:
            critical_value_quota = 0
        
        # Calculate final severity score
        stations_ratio = anomalous_stations_count / total_stations_count if total_stations_count > 0 else 0
        severity_score = stations_ratio * 0.4 + (critical_value_quota * 0.6) 
        
        # Cap at 1.0 and round
        severity_score = min(round(severity_score, 2), 1.0)
        
        return severity_score, critical_value_quota
    
    def _calculate_air_quality(self, anomalous_measurements_accumulator: Dict[str, float],
                              anomalous_fields_counts: Dict[str, int]) -> Tuple[Optional[float], str]:
        """
        Calculate air quality index based on PM2.5 and CO2 measurements.
        
        Args:
            anomalous_measurements_accumulator: Dictionary of accumulated measurements
            anomalous_fields_counts: Dictionary counting fields for anomalous stations
            
        Returns:
            Tuple[Optional[float], str]: Air quality index and descriptive status
        """
        if "pm25_ugm3" in anomalous_measurements_accumulator and "co2_ppm" in anomalous_measurements_accumulator:
            pm25_avg = anomalous_measurements_accumulator["pm25_ugm3"] / anomalous_fields_counts["pm25_ugm3"]
            co2_avg = anomalous_measurements_accumulator["co2_ppm"] / anomalous_fields_counts["co2_ppm"]
            
            # AQI formula (simplified)
            # PM2.5 threshold ~35 μg/m³, CO2 threshold ~1000 ppm
            pm25_component = min(pm25_avg / 35, 3)  # Cap at 3x threshold
            co2_component = min(max((co2_avg - 400) / 600, 0), 3)  # Normalize CO2 (400ppm baseline, 1000ppm threshold)
            
            air_quality_index = round(50 + (pm25_component * 0.7 + co2_component * 0.3) * 150, 1)
            air_quality_index = min(air_quality_index, 500)  # Cap at 500
        else:
            air_quality_index = None

        # Create some useful thresholds and classification marks for air quality index
        if air_quality_index is None:
            air_quality = "Unknown"
        elif air_quality_index <= 50:
            air_quality = "Good"
        elif air_quality_index <= 100:
            air_quality = "Moderate"
        elif air_quality_index <= 150:
            air_quality = "Unhealthy for Sensitive Groups"
        elif air_quality_index <= 200:
            air_quality = "Unhealthy"
        elif air_quality_index <= 300:
            air_quality = "Very Unhealthy"
        else:
            air_quality = "Hazardous"
            
        return air_quality_index, air_quality
    
    def _estimate_ignition_time(self, response_timestamp: int, severity_score: float) -> str:
        """
        Estimate the time when the fire might have started based on detection time and severity.
        
        Args:
            response_timestamp: Timestamp when the detection occurred (milliseconds)
            severity_score: Calculated severity score (0.0 to 1.0)
            
        Returns:
            str: ISO formatted timestamp string of estimated ignition time
        """
        try:
            # Estimate ignition time (approximately 10-30 minutes before detection based on severity)
            backtrack_minutes = 10 + (severity_score * 20)  # 10-30 minutes
            estimated_timestamp = response_timestamp - int(backtrack_minutes * 60 * 1000)
            return datetime.fromtimestamp(estimated_timestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        except Exception as e:
            logger.error(f"Error estimating ignition time: {str(e)}")
            # Return current time minus 20 minutes as fallback
            return datetime.fromtimestamp((response_timestamp / 1000) - 1200).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    
    def _determine_fire_behavior(self, anomalous_measurements_accumulator: Dict[str, float],
                                anomalous_fields_counts: Dict[str, int]) -> Optional[Dict[str, Any]]:
        """
        Determine fire behavior based on temperature and humidity measurements.
        
        Args:
            anomalous_measurements_accumulator: Dictionary of accumulated measurements
            anomalous_fields_counts: Dictionary counting fields for anomalous stations
            
        Returns:
            Optional[Dict[str, Any]]: Fire behavior dictionary or None if data is insufficient
        """
        if "temperature_c" in anomalous_measurements_accumulator and "humidity_percent" in anomalous_measurements_accumulator:
            temp_avg = anomalous_measurements_accumulator["temperature_c"] / anomalous_fields_counts["temperature_c"]
            humidity_avg = anomalous_measurements_accumulator["humidity_percent"] / anomalous_fields_counts["humidity_percent"]
            
            # Estimate spread rate based on temperature and humidity
            # Higher temp and lower humidity = faster spread
            spread_factor = (temp_avg / 30) * (30 / max(humidity_avg, 1))
            
            if spread_factor > 5:
                spread_rate = "extreme"
                estimated_speed_mph = round(10 + (spread_factor - 5) * 2, 1)
            elif spread_factor > 3:
                spread_rate = "rapid"
                estimated_speed_mph = round(5 + (spread_factor - 3) * 2.5, 1)
            elif spread_factor > 1.5:
                spread_rate = "moderate"
                estimated_speed_mph = round(2 + (spread_factor - 1.5) * 2, 1)
            else:
                spread_rate = "slow"
                estimated_speed_mph = round(0.5 + spread_factor, 1)
            
            # Simple direction approximation (would need actual weather data)
            directions = ["north", "northeast", "east", "southeast", "south", "southwest", "west", "northwest"]
            direction = random.choice(directions)
            
            return {
                "spread_rate": spread_rate,
                "direction": direction,
                "estimated_speed_mph": estimated_speed_mph
            }
        else:
            return None
    
    def _determine_alert_level(self, severity_score: float) -> str:
        """
        Determine alert level based on severity score.
        
        Args:
            severity_score: Calculated severity score (0.0 to 1.0)
            
        Returns:
            str: Alert level code ('green', 'yellow', 'orange', 'deep_orange', or 'red')
        """
        if severity_score <= 0.2:
            return "green"       
        elif severity_score <= 0.4:
            return "yellow"      
        elif severity_score <= 0.6:
            return "orange"      
        elif severity_score <= 0.8:
            return "deep_orange" 
        else:
            return "red"
    
    def _create_notifications(self, context) -> List[Dict[str, Any]]:
        """
        Create notification records for the event.
        
        Args:
            context: The window processing context containing window metadata
            
        Returns:
            List[Dict[str, Any]]: List of notification records
        """
        try:
            send_notifications = SENT_NOTIFICATION_TO
            timestamp_epoch_millis = context.window().end
            notification_timestamp = datetime.fromtimestamp(timestamp_epoch_millis / 1000.0).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            
            # Create a copy to avoid modifying the original
            notifications = []
            for notification in send_notifications:
                notification_copy = notification.copy()
                notification_copy["notification_timestamp"] = notification_timestamp
                notifications.append(notification_copy)
                
            return notifications
        except Exception as e:
            logger.error(f"Error creating notifications: {str(e)}")
            return []


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
            logger.info("[INFO] MinIO is ready")
            return
        except Exception as e:
            logger.warning(f"[WARN] MinIO not ready (attempt {i+1}/{max_retries}): {e}")
            time.sleep(retry_interval)
    raise Exception("[ERROR] MinIO is not ready after retries")


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
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.enable_checkpointing(30000) # Check point every 30s

    # Kafka consumer configuration
    properties = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'flink_consumer_group',
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

    # Define watermark strategy
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(JsonTimestampAssigner())

    # Source with timestamps and watermarks
    ds = env.add_source(kafka_consumer, type_info=Types.STRING()).assign_timestamps_and_watermarks(watermark_strategy)

    # Save each record to MinIO
    ds.map(S3MinIOSinkBronze(), output_type=Types.STRING())

    # Apply windowing logic and print aggregated result
    processed_stream = (
        ds.key_by(lambda x: json.loads(x).get("station_id", "UNKNOWN"), key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(ThresholdFilterWindowFunction(THRESHOLDS), output_type=Types.STRING())
        .map(EnrichFromRedis(), output_type=Types.STRING())
    )

    processed_stream.map(S3MinIOSinkSilver(), output_type=Types.STRING())

    gold_layer_stream = (
        processed_stream
        .key_by(lambda x: (json.loads(x)["station_metadata"]["position"].get("microarea_id", "UNKNOWN"),
                           json.loads(x).get("response_timestamp", 0)//(60*1000)), 
                key_type=Types.TUPLE([Types.STRING(), Types.LONG()]))
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(GoldAggregatorWindowFunction(), output_type=Types.STRING())
    )

    gold_layer_stream.map(S3MinIOSinkGold(), output_type=Types.STRING())

    logger.info("Executing Flink job")
    env.execute("Flink Job")
    logger.info("Flink job execution initiated")


if __name__ == "__main__":
    main()

# BIG PROBLEM WITH SEVERITY SCORE COMPUTATION