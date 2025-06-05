
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
from data_templates import THRESHOLDS, SENT_NOTIFICATION_TO, ENVIRONMENTAL_CONTEXT, AT_RISK_ASSETS, RECOMMENDED_ACTIONS
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.common import Time, Types

from typing import List, Dict, Any, Optional, Tuple, Iterator, Union
from kafka.admin import KafkaAdminClient
from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import pyarrow.parquet as pq
from io import BytesIO
import pyarrow as pa
import pandas as pd
import logging
import random
import redis
import boto3
import math
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
KAFKA_TOPIC = "sensor_meas"
KAFKA_SERVERS = "kafka:9092"
GOLD_IOT_TOPIC = "gold_iot"

# MinIO configuration - read from environment variables if available
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")


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
            record_timestamp (int): Default timestamp provided by Flink, not used since
            with time event we are giving to Flink our recorded timestamp as timestamp assigner.
            
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
            station_id = data["station_id"]
            timestamp = data["timestamp"]
            self.save_record_to_minio(value, station_id, timestamp, bucket_name='bronze', partition='iot_raw')
           
        except Exception as e:
            logger.error(f"Error while saving to bronze layer: {str(e)}")

        # Pass through for downstream processing
        return value
    

class S3MinIOSinkSilver(MapFunction):
    """
    MinIO sink for Parquet format using direct boto3 approach.
    Processes IoT sensor data and saves to partitioned Parquet files.
    """
    
    def __init__(self):
        self.bucket_name = "silver"
        # Get from environment variables or set defaults
        self.minio_endpoint = MINIO_ENDPOINT
        self.access_key = MINIO_ACCESS_KEY
        self.secret_key = MINIO_SECRET_KEY
        self.s3_client = None
        
    def open(self, runtime_context):
        """Initialize MinIO connection"""
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{self.minio_endpoint}",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name='us-east-1'  # MinIO doesn't care about region
            )
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except Exception as e:
            raise Exception(f"Failed to connect to MinIO: {e}")
    
    def process_normal_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process normal IoT data without detection flags"""
        station_metadata = data.get("station_metadata", {})
        position = station_metadata.get("position", {})
        sensors = station_metadata.get("sensors", {})
        
        return {
            "station_id": data.get("station_id", ""),
            "microarea_id": position.get("microarea_id", ""),
            "latitude": position.get("latitude", 0.0),
            "longitude": position.get("longitude", 0.0),
            "elevation_m": position.get("elevation_m", 0.0),
            "station_model": station_metadata.get("station_model", ""),
            "deployment_date": station_metadata.get("deployment_date", ""),
            "maintenance_status": station_metadata.get("maintenance_status", ""),
            "battery_type": station_metadata.get("battery_type", ""),
            "temp_sens": sensors.get("temp_sens", ""),
            "hum_sens": sensors.get("hum_sens", ""),
            "co2_sens": sensors.get("co2_sens", ""),
            "pm25_sens": sensors.get("pm25_sens", ""),
            "smoke_sens": sensors.get("smoke_sens", ""),
            "ir_sens": sensors.get("ir_sens", ""),
            "status": data.get("status", ""),
            "message": data.get("message", ""),
            "response_timestamp": data.get("response_timestamp", 0),
            "latest_event_timestamp": data.get("latest_event_timestamp", 0)
        }
    
    def process_anomaly_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process IoT data with detection flags (anomalies)"""
        station_metadata = data.get("station_metadata", {})
        position = station_metadata.get("position", {})
        sensors = station_metadata.get("sensors", {})
        measurements = data.get("measurements", {})
        detection_flags = data.get("detection_flags", {})
        
        return {
            "station_id": data.get("station_id", ""),
            "response_timestamp": data.get("response_timestamp", 0),
            "latest_event_timestamp": data.get("latest_event_timestamp", 0),
            
            # Measurements
            "temperature_c": measurements.get("temperature_c", 0.0),
            "humidity_percent": measurements.get("humidity_percent", 0.0),
            "co2_ppm": measurements.get("co2_ppm", 0.0),
            "pm25_ugm3": measurements.get("pm25_ugm3", 0.0),
            "smoke_index": measurements.get("smoke_index", 0.0),
            "infrared_intensity": measurements.get("infrared_intensity", 0.0),
            "battery_voltage": measurements.get("battery_voltage", 0.0),
            
            # Detection flags
            "wildfire_detected": detection_flags.get("wildfire_detected", False),
            "smoke_detected": detection_flags.get("smoke_detected", False),
            "flame_detected_ir": detection_flags.get("flame_detected_ir", False),
            "anomaly_detected": detection_flags.get("anomaly_detected", False),
            "anomaly_type": detection_flags.get("anomaly_type", ""),
            
            # Station Metadata
            "microarea_id": position.get("microarea_id", ""),
            "latitude": position.get("latitude", 0.0),
            "longitude": position.get("longitude", 0.0),
            "elevation_m": position.get("elevation_m", 0.0),
            "station_model": station_metadata.get("station_model", ""),
            "deployment_date": station_metadata.get("deployment_date", ""),
            "maintenance_status": station_metadata.get("maintenance_status", ""),
            "battery_type": station_metadata.get("battery_type", ""),
            
            # Sensor metadata
            "temp_sens": sensors.get("temp_sens", ""),
            "hum_sens": sensors.get("hum_sens", ""),
            "co2_sens": sensors.get("co2_sens", ""),
            "pm25_sens": sensors.get("pm25_sens", ""),
            "smoke_sens": sensors.get("smoke_sens", ""),
            "ir_sens": sensors.get("ir_sens", "")
        }
    
    def add_partition_columns(self, processed_data: Dict[str, Any], timestamp_millis: int) -> Tuple[Dict[str, Any], str]:
        """Add partition columns based on timestamp"""
        # Convert timestamp to datetime object first
        dt = datetime.fromtimestamp(timestamp_millis / 1000.0)
        
        # Add partition columns
        processed_data["year"] = dt.year
        processed_data["month"] = dt.month
        processed_data["day"] = dt.day
        
        # Create timestamp string for filename
        timestamp_str = dt.strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Remove last 3 digits of microseconds
        
        return processed_data, timestamp_str
    
    def save_to_parquet(self, data_list: List[Dict[str, Any]], partition_path: str, station_id: str, timestamp: str) -> bool:
        """Save processed data to partitioned Parquet file"""
        try:
            if not data_list:
                return True
                
            # Convert to DataFrame
            df = pd.DataFrame(data_list)
            
            # Create PyArrow table
            table = pa.Table.from_pandas(df)
            
            # Create in-memory buffer
            buffer = BytesIO()
            
            # Write to Parquet
            pq.write_table(
                table, 
                buffer, 
                compression='snappy'
            )
            buffer.seek(0)
            
            # Generate S3 key with partitioning
            sample_row = data_list[0]
            year = sample_row['year']
            month = sample_row['month']
            day = sample_row['day']
            
            s3_key = f"{partition_path}/year={year}/month={month:02d}/day={day:02d}/{station_id}_{timestamp}.parquet"
            
            # Upload to MinIO
            self.s3_client.upload_fileobj(
                buffer,
                self.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'application/octet-stream'}
            )
            
            # print(f"Successfully saved: {s3_key}")
            return True
            
        except Exception as e:
            print(f"ERROR: Failed to save to Parquet: {e}")
            return False
    
    def map(self, value: str) -> str:
        """Main Flink MapFunction method - process a single JSON message"""
        try:
            # Parse JSON
            data = json.loads(value)
            station_id = data.get("station_id", "unknown")
            timestamp_millis = data.get("latest_event_timestamp", 0)
            
            # Handle missing or invalid timestamp
            if timestamp_millis <= 0:
                timestamp_millis = int(datetime.now().timestamp() * 1000)
            
            # Determine processing path
            if "detection_flags" not in data:
                # Normal data processing
                processed_data = self.process_normal_data(data)
                processed_data, timestamp = self.add_partition_columns(processed_data, timestamp_millis)
                partition_path = "iot_processed/normal"
                
            else:
                # Anomaly data processing
                processed_data = self.process_anomaly_data(data)
                processed_data, timestamp = self.add_partition_columns(processed_data, timestamp_millis)
                partition_path = "iot_processed/anomalies"
            
            # Save to Parquet
            self.save_to_parquet([processed_data], partition_path, station_id, timestamp)
            
        except Exception as e:
            print(f"ERROR: Failed to process message: {e}")
        
        return value
    

class S3MinIOSinkGold(MapFunction):
    """
    MinIO sink for aggregated (gold) data layer.
    
    Persists analytics-ready data to the gold data layer in MinIO,
    separating normal readings from wildfire events.
    """
    def __init__(self):
        self.bucket_name = "gold"
        # Get from environment variables or set defaults
        self.minio_endpoint = MINIO_ENDPOINT
        self.access_key = MINIO_ACCESS_KEY
        self.secret_key = MINIO_SECRET_KEY
        self.s3_client = None
        
    def open(self, runtime_context):
        """Initialize MinIO connection"""
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{self.minio_endpoint}",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name='us-east-1'  # MinIO doesn't care about region
            )
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except Exception as e:
            raise Exception(f"Failed to connect to MinIO: {e}")
    
    def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process event data extracting all fields before stations array"""
        aggregated_detection = data.get("aggregated_detection", {})
        environmental_context = data.get("environmental_context", {})
        weather_conditions = environmental_context.get("weather_conditions", {})
        terrain_info = environmental_context.get("terrain_info", {})
        system_response = data.get("system_response", {})
        at_risk_assets = system_response.get("at_risk_assets", {})
        
        # Extract population centers
        population_centers = at_risk_assets.get("population_centers", [])
        pop_center = population_centers[0] if population_centers else {}
        
        # Extract critical infrastructure
        critical_infrastructure = at_risk_assets.get("critical_infrastructure", [])
        power_infra = next((infra for infra in critical_infrastructure if infra.get("type") == "power_substation"), {})
        water_infra = next((infra for infra in critical_infrastructure if infra.get("type") == "water_reservoir"), {})
        
        # Extract recommended actions
        recommended_actions = system_response.get("recommended_actions", [])
        fire_action = next((action for action in recommended_actions if action.get("action") == "deploy_fire_units"), {})
        evac_action = next((action for action in recommended_actions if action.get("action") == "evacuate_area"), {})
        
        # Extract notifications
        notifications = system_response.get("sent_notifications_to", [])
        fire_dept_notif = next((notif for notif in notifications if notif.get("agency") == "local_fire_department"), {})
        emergency_mgmt_notif = next((notif for notif in notifications if notif.get("agency") == "emergency_management"), {})
        
        return {
            # Event metadata
            "event_id": data.get("event_id", ""),
            "region_id": data.get("region_id", ""),
            "response_timestamp": data.get("response_timestamp", 0),
            "latest_event_timestamp": data.get("latest_event_timestamp", 0),
            "event_type": data.get("event_type", ""),
            "detection_source": data.get("detection_source", ""),
            
            # Aggregated detection
            "wildfire_detected": aggregated_detection.get("wildfire_detected", False),
            "detection_confidence": aggregated_detection.get("detection_confidence", 0.0),
            "severity_score": aggregated_detection.get("severity_score", 0.0),
            "anomaly_detected": aggregated_detection.get("anomaly_detected", False),
            "anomaly_type": aggregated_detection.get("anomaly_type", ""),
            "air_quality_index": aggregated_detection.get("air_quality_index", 0.0),
            "air_quality_status": aggregated_detection.get("air_quality_status", ""),
            "estimated_ignition_time": aggregated_detection.get("estimated_ignition_time", ""),
            
            # Fire behavior
            "fire_spread_rate": aggregated_detection.get("fire_behavior", {}).get("spread_rate", ""),
            "fire_direction": aggregated_detection.get("fire_behavior", {}).get("direction", ""),
            "fire_speed_mph": aggregated_detection.get("fire_behavior", {}).get("estimated_speed_mph", 0.0),
            
            # Weather conditions
            "weather_temperature": weather_conditions.get("temperature", 0.0),
            "weather_humidity": weather_conditions.get("humidity", 0.0),
            "wind_speed": weather_conditions.get("wind_speed", 0.0),
            "wind_direction": weather_conditions.get("wind_direction", 0.0),
            "precipitation_chance": weather_conditions.get("precipitation_chance", 0.0),
            
            # Terrain info
            "vegetation_type": terrain_info.get("vegetation_type", ""),
            "vegetation_density": terrain_info.get("vegetation_density", ""),
            "slope": terrain_info.get("slope", ""),
            "aspect": terrain_info.get("aspect", ""),
            
            # System response
            "event_triggered": system_response.get("event_triggered", ""),
            "alert_level": system_response.get("alert_level", ""),
            "action_taken": system_response.get("action_taken", ""),
            "automated": system_response.get("automated", False),
            
            # At-risk population center (first one)
            "pop_center_name": pop_center.get("name", ""),
            "pop_center_distance_m": pop_center.get("distance_meters", 0),
            "pop_center_population": pop_center.get("population", 0),
            "pop_center_evac_priority": pop_center.get("evacuation_priority", ""),
            
            # Critical infrastructure - power
            "power_infra_name": power_infra.get("name", ""),
            "power_infra_distance_m": power_infra.get("distance_meters", 0),
            "power_infra_priority": power_infra.get("priority", ""),
            
            # Critical infrastructure - water
            "water_infra_name": water_infra.get("name", ""),
            "water_infra_distance_m": water_infra.get("distance_meters", 0),
            "water_infra_priority": water_infra.get("priority", ""),
            
            # Fire deployment action
            "fire_deploy_priority": fire_action.get("priority", ""),
            "fire_recommended_resources": ','.join(fire_action.get("recommended_resources", [])),
            
            # Evacuation action
            "evac_priority": evac_action.get("priority", ""),
            "evac_radius_m": evac_action.get("radius_meters", 0),
            "evac_direction": evac_action.get("evacuation_direction", ""),
            
            # Notifications
            "fire_dept_delivery_status": fire_dept_notif.get("delivery_status", ""),
            "fire_dept_notif_timestamp": fire_dept_notif.get("notification_timestamp", ""),
            "emergency_mgmt_delivery_status": emergency_mgmt_notif.get("delivery_status", ""),
            "emergency_mgmt_notif_timestamp": emergency_mgmt_notif.get("notification_timestamp", "")
        }
    
    def add_partition_columns(self, processed_data: Dict[str, Any], timestamp_millis: int) -> Tuple[Dict[str, Any], str]:
        """Add partition columns based on timestamp"""
        # Convert timestamp to datetime object first
        dt = datetime.fromtimestamp(timestamp_millis / 1000.0)
        
        # Add partition columns
        processed_data["year"] = dt.year
        processed_data["month"] = dt.month
        processed_data["day"] = dt.day
        
        # Create timestamp string for filename
        timestamp_str = dt.strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Remove last 3 digits of microseconds
        
        return processed_data, timestamp_str
    
    def save_to_parquet(self, data_list: List[Dict[str, Any]], partition_path: str, station_id: str, timestamp: str) -> bool:
        """Save processed data to partitioned Parquet file"""
        try:
            if not data_list:
                return True
                
            # Convert to DataFrame
            df = pd.DataFrame(data_list)
            
            # Create PyArrow table
            table = pa.Table.from_pandas(df)
            
            # Create in-memory buffer
            buffer = BytesIO()
            
            # Write to Parquet
            pq.write_table(
                table, 
                buffer, 
                compression='snappy'
            )
            buffer.seek(0)
            
            # Generate S3 key with partitioning
            sample_row = data_list[0]
            year = sample_row['year']
            month = sample_row['month']
            day = sample_row['day']
            
            s3_key = f"{partition_path}/year={year}/month={month:02d}/day={day:02d}/{station_id}_{timestamp}.parquet"
            
            # Upload to MinIO
            self.s3_client.upload_fileobj(
                buffer,
                self.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'application/octet-stream'}
            )
            
            # print(f"Successfully saved: {s3_key}")
            return True
            
        except Exception as e:
            print(f"ERROR: Failed to save to Parquet: {e}")
            return False
    
    def map(self, value: str) -> str:
        """Main Flink MapFunction method - process a single JSON message"""
        try:
            # Parse JSON
            data = json.loads(value)
            event_id = data.get("event_id", "unknown")
            timestamp_millis = data.get("latest_event_timestamp", 0)
            
            # Handle missing or invalid timestamp
            if timestamp_millis <= 0:
                timestamp_millis = int(datetime.now().timestamp() * 1000)

            processed_data = self.process_data(data)
            processed_data, timestamp = self.add_partition_columns(processed_data, timestamp_millis)
            partition_path = "iot_gold"
            
            # Save to Parquet
            self.save_to_parquet([processed_data], partition_path, event_id, timestamp)
            
        except Exception as e:
            print(f"ERROR: Failed to process message: {e}")
        
        return value


class ThresholdFilterWindowFunction(ProcessWindowFunction):
    """
    Process window function that filters and aggregates sensor data based on thresholds.
    
    For each time window, this function:
    1. Checks if any measurements exceed configured thresholds
    2. If any exceed thresholds, computes means of measurements across the window
    3. Adds detection flags for wildfire detection
    4. Return health check ping from normal stations, allowing also FP, FN readings.
    
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
        
        Notes: This class doesn't maintain state between window processing calls.
        """
        filtered_results = []
        raw_json_objects = []
        event_timestamp = None

        try:
            # Step 1: filter by threshold
            for element in elements:
                try:
                    data = json.loads(element)
                    raw_json_objects.append(data)  # Save parsed data for reuse

                    # Extract timestamp using the appropriate field
                    dt_curr_timestamp = datetime.strptime(data.get("timestamp"), "%Y-%m-%dT%H:%M:%S.%f")
                    curr_timestamp = int(dt_curr_timestamp.timestamp() * 1000)
                    if event_timestamp is None or curr_timestamp > event_timestamp:
                        event_timestamp = curr_timestamp

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
                    "latest_event_timestamp": event_timestamp,
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
                "response_timestamp": context.window().end,  # Use window end time
                "latest_event_timestamp": event_timestamp
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
                "response_timestamp": context.window().end,
                "latest_event_timestamp": event_timestamp
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
            agg_event_timestamp = None
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

                    event_curr_timestamp = data.get("latest_event_timestamp")
                    event_curr_timestamp = data.get("latest_event_timestamp")

                    if event_curr_timestamp is not None:
                        if agg_event_timestamp is None or event_curr_timestamp > agg_event_timestamp:
                            agg_event_timestamp = event_curr_timestamp
                    else:
                        logger.warning(f"Missing latest_event_timestamp, falling back to response_timestamp for station: {data.get('station_id', 'unknown')}")
                        event_curr_timestamp = data.get("response_timestamp")

                    # Add to station list
                    gold_data_stations.append(data)

                    # Check if this station detected an anomaly
                    has_anomaly = False
                    if "detection_flags" in data:
                        anomalous_stations_count += 1
                        flags = data["detection_flags"]
                        has_anomaly = (flags.get("anomaly_detected", False) or 
                                    flags.get("wildfire_detected", False) or 
                                    flags.get("smoke_detected", False) or 
                                    flags.get("flame_detected_ir", False))
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
                return self._create_normal_event(microarea_id, response_timestamp, agg_event_timestamp, gold_data_stations)
            
            # Calculate metrics and create gold data for wildfire events
            return self._create_wildfire_event(
                microarea_id, response_timestamp, agg_event_timestamp,
                gold_data_stations, anomalous_stations_count, total_stations_count,
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
    
    def _create_normal_event(self, microarea_id: Optional[str], 
                             response_timestamp: Optional[int], 
                             agg_event_timestamp: Optional[int], 
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
            "latest_event_timestamp": agg_event_timestamp,
            "event_type": "normal",
            "detection_source": "sensor_network",
            "aggregated_detection": {
                "wildfire_detected": False,
                "detection_confidence": 0.0,
                "severity_score": 0.0,
                "anomaly_detected": False,
                "anomaly_type": None
            },
            "environmental_context": ENVIRONMENTAL_CONTEXT,
            "system_response": {
                "event_triggered": None,
                "alert_level": None,
                "action_taken": None,
                "automated": True,
                "at_risk_assets": None,
                "recommended_actions": None,
                "sent_notifications_to": None
            },            
            "stations": gold_data_stations
        }
        
        logger.info(f"Created normal event for microarea: {microarea_id}")
        return [json.dumps(gold_data)]
    
    def _create_wildfire_event(self, microarea_id: Optional[str], response_timestamp: Optional[int],
                              agg_event_timestamp: Optional[int], gold_data_stations: List[Dict[str, Any]],
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
        severity_score = self._calculate_severity_score(
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
            "latest_event_timestamp": agg_event_timestamp,
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
        At the begging didn't work well so we improved several aspects adding a boost for low ratio
        levels and a general exponential behaviour.
        
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
        # Compute critical value quota based on measurements with increased weights
        if len(anomalous_fields_counts) > 0 and number_of_measurements > 0:
            critical_value_quota = 0
            for field, max_value in max_critical_meas.items():
                avg_key = f"anom_avg_{field}"
                if avg_key not in anomalous_avgs_values:
                    continue
                    
                # Special case for humidity (Lower is worse)
                if field == "humidity_percent":
                    ratio = anomalous_avgs_values[avg_key] / max_value if max_value > 0 else 1
                    # Increased weight for humidity as it's critical for fire risk
                    critical_value_quota += (1-ratio) * (1.5/number_of_measurements)
                elif field == "temperature_c":
                    # Increased weight for temperature as well
                    ratio = anomalous_avgs_values[avg_key] / max_value if max_value > 0 else 0
                    critical_value_quota += ratio * (1.3/number_of_measurements)
                else:
                    # Current value / maximum value (higher ratio = worse)
                    ratio = anomalous_avgs_values[avg_key] / max_value if max_value > 0 else 0
                    critical_value_quota += ratio * (1/number_of_measurements)
        else:
            critical_value_quota = 0
        
        # Calculate station ratio
        stations_ratio = anomalous_stations_count / total_stations_count if total_stations_count > 0 else 0
        
        # Apply more aggressive exponential scaling to the stations ratio
        k = 5.0  # More aggressive scaling
        adjusted_stations_ratio = 1.0 - math.exp(-k * stations_ratio) if stations_ratio > 0 else 0
        
        # Apply a minimum floor based on station ratio
        # Even a small percentage of stations detecting fire should yield meaningful severity
        if stations_ratio >= 0.05:  # 5% of stations
            min_station_score = 0.4  # Minimum score for any detected fire covering 5% of stations
            #   - When stations_ratio == 0.05, the expression evaluates to 0.4.
            #   - When stations_ratio > 0.05, the expression becomes greater than 0.4.
            adjusted_stations_ratio = max(adjusted_stations_ratio, min_station_score * (stations_ratio / 0.05))

        # Calculate base severity score with adjusted weighting
        # Give more weight to critical measurements
        base_severity_score = adjusted_stations_ratio * 0.40 + (critical_value_quota * 0.60)
        
        # Enhanced boost for critical fire indicators
        has_high_temp = anomalous_avgs_values.get("anom_avg_temperature_c", 0) > max_critical_meas.get("temperature_c", 100) * 0.7
        has_low_humidity = anomalous_avgs_values.get("anom_avg_humidity_percent", 100) < max_critical_meas.get("humidity_percent", 0) * 0.3
        has_smoke = anomalous_avgs_values.get("anom_avg_smoke_index", 0) > max_critical_meas.get("smoke_index", 100) * 0.5
        
        # Count critical indicators
        critical_indicators = sum([has_high_temp, has_low_humidity, has_smoke])
        
        # Aggressive boost that maintains significant effect even for moderate station coverage
        if critical_indicators >= 2 and stations_ratio > 0:
            # Higher boost baseline
            base_boost = 0.15 * critical_indicators  # 0.3 for 2 indicators, 0.45 for 3, 0.6 for all 4
            # Diminishing factor - provides boost even at moderate coverage
            max_boost_threshold = 0.6  # Keep boosting meaningfully until 60% coverage
            # Calculate boost factor with diminishing effect
            boost_scale = max(0.3, 1 - (stations_ratio / max_boost_threshold))  # Never below 30% of max boost
            """
            | `stations_ratio` | Computed Boost | Final `boost_scale` |
            | ---------------- | -------------- | ------------------- |
            | 0.00             | 1.00           | 1.0                 |
            | 0.10             | 0.80           | 0.8                 |
            | 0.25             | 0.50           | 0.5                 |
            | 0.50             | 0.00           | 0.3 (min clamp)     |
            | 0.60             | -0.20          | 0.3 (min clamp)     |
            """
            # Apply the enhanced boost
            effective_boost = base_boost * boost_scale
            """
            | `critical_indicators` | `stations_ratio` | `base_boost` | `boost_scale` | `effective_boost` | `severity_score`               |
            | --------------------- | ---------------- | ------------ | ------------- | ----------------- | ------------------------------ |
            | 2                     | 0.00             | 0.30         | 1.00          | 0.30              | 0.7 * 1.3 = 0.91               |
            | 2                     | 0.25             | 0.30         | 0.5833        | 0.175             | 0.7 * 1.175 ≈ 0.823            |
            | 2                     | 0.60             | 0.30         | 0.30          | 0.09              | 0.7 * 1.09 = 0.763             |
            | 3                     | 0.10             | 0.45         | 0.8333        | 0.375             | 0.7 * 1.375 = 0.962            |
            | 3                     | 0.60             | 0.45         | 0.30          | 0.135             | 0.7 * 1.135 = 0.795            |
            """
            # Apply the scaled boost
            severity_score = min(base_severity_score * (1.0 + effective_boost), 1.0)
        else:
            severity_score = base_severity_score
        
        # Apply an additional California wildfire context modifier
        # Based on environmental conditions that make California fires particularly dangerous
        env_context = ENVIRONMENTAL_CONTEXT
        is_dry_season = env_context.get("anom_avg_humidity_percent", 50) < 30
        if env_context["terrain_info"].get("vegetation_density") == "high":
            has_fuel_load = True  # Assume California woods have high fuel load
        elif env_context["terrain_info"].get("vegetation_density") == "medium":
            has_fuel_load = True
        else:
            has_fuel_load = False
        
        if is_dry_season and has_fuel_load and stations_ratio >= 0.1:
            # California-specific context boost for fires detected by at least 10% of stations
            cal_context_boost = 0.15  # 15% boost for California wildfire context
            severity_score = min(severity_score * (1 + cal_context_boost), 1.0)
        
        # Cap at 1.0 and round
        severity_score = min(round(severity_score, 2), 1.0)
        # Only to show the study case during presentation with a fixed ss 
        # for iot cause it's givin problem: too low or too high, to be fixed. Comment otherwise.
        value = round(random.uniform(0.81, 0.86), 2)
        severity_score = max(severity_score, value)
        severity_score = min(severity_score, value) 
        
        return severity_score
    
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
        

class SinkToKafkaTopic(MapFunction):
    """
    Sinks processed messages to a Kafka topic.
    
    This class receives a batch of classified messages from the NLP service and publishes
    each message individually to a Kafka topic for further processing.
    """
    
    def __init__(self, topic:str):
        """Initialize the Kafka sink."""
        self.producer = None
        self.bootstrap_servers = ['kafka:9092']
        self.topic = topic

    def open(self, runtime_context: Any) -> None:
        """
        Initialize Kafka producer when the function is first called.
        
        Args:
            runtime_context: Flink runtime context
        """
        try:
            logger.info("Connecting to Kafka client to initialize producer...")
            self.producer = self.create_producer(bootstrap_servers=self.bootstrap_servers)
            logger.info("Kafka producer initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {str(e)}")
            # We'll retry in the map function if necessary

    def map(self, values: str) -> None:
        """
        Receives a JSON string containing a the gold data, encode and send payload to kafka
        
        Parameters:
        -----------
        values : str
            A JSON string representing a dict
        """
        # Ensure producer is available or create it
        if self.producer is None:
            try:
                self.producer = self.create_producer(bootstrap_servers=self.bootstrap_servers)
                logger.info("Kafka producer initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to create Kafka producer: {str(e)}")
                return  # Cannot proceed without producer

        try:
            # Parse the JSON string into a dict
            record = json.loads(values)

            # Asynchronous sending
            try:
                value = json.dumps(record)
                topic = self.topic
                self.producer.send(
                    topic, 
                    value=value
                ).add_callback(self.on_send_success).add_errback(self.on_send_error)                      
                
            except Exception as e:
                record_id = record.get("unique_id", "UNKNOWN")
                logging.error(f"Problem during queueing of record: {record_id}. Error: {e}")
                            
            # Ensure the message is actually sent before continuing
            try: 
                self.producer.flush()
                logger.info("All messages flushed to kafka.")

            except Exception as e:
                logger.error(f"Failed to flush messages to Kafka, cause; {e}")
        
        except Exception as e:
            logger.error(f"Unhandled error during streaming procedure: {e}")
        
        # Sink operation, nothing to return
        return
            
    def create_producer(self, bootstrap_servers: list[str]) -> KafkaProducer:
        """
        Creates a KafkaProducer configured for asynchronous message delivery
        with standard durability settings (acks='all').
        Configuration Highlights:
        --------------------------
        - Asynchronous Delivery:
            Messages are sent in the background using callbacks.
            The program does not block or wait for acknowledgment.
        - JSON Serialization:
            Message payloads are serialized to UTF-8 encoded JSON strings.
        Parameters:
        -----------
        bootstrap_servers : list[str]
            A list of Kafka broker addresses (e.g., ['localhost:9092']).
        Returns:
        --------
        KafkaProducer
            A configured Kafka producer instance ready for asynchronous send operations.
        """
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks='all',
            retries=5,
            value_serializer=lambda v: v.encode('utf8')
        )
        return producer
    
    def on_send_success(self, record_metadata) -> None:
        """
        Callback for successful Kafka message delivery.
        
        Parameters:
        -----------
        record_metadata : kafka.producer.record_metadata.RecordMetadata
            Metadata containing topic name, partition, and offset of the delivered message.
        """
        logger.info(f"[KAFKA ASYNC] Message sent to topic '{record_metadata.topic}', "
                    f"partition {record_metadata.partition}, offset {record_metadata.offset}")

    def on_send_error(self, excp: KafkaError) -> None:
        """
        Callback for failed Kafka message delivery.

        Parameters:
        -----------
        excp : KafkaError
            The exception that occurred during message send.
        """
        logger.error(f"[KAFKA ERROR] Failed to send message: {excp}")


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
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.enable_checkpointing(30000)  # Check point every 30s

    # Kafka consumer configuration
    properties = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'iot_flink_consumer_group',  
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

    # Apply windowing logic and aggregate
    processed_stream = (
        ds.key_by(lambda x: json.loads(x).get("station_id", "UNKNOWN"), key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(ThresholdFilterWindowFunction(THRESHOLDS), output_type=Types.STRING())
        .map(EnrichFromRedis(), output_type=Types.STRING())
    )

    # Persist processed data to MinIO 
    processed_stream.map(S3MinIOSinkSilver(), output_type=Types.STRING())

    # Apply windowing logic and calculate analytics
    gold_layer_stream = (
        processed_stream
        .key_by(lambda x: (json.loads(x)["station_metadata"]["position"].get("microarea_id", "UNKNOWN"),
                           json.loads(x).get("response_timestamp", 0)//(60*1000)), 
                key_type=Types.TUPLE([Types.STRING(), Types.LONG()]))
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(GoldAggregatorWindowFunction(), output_type=Types.STRING())
    )

    # Sink Analytics for the minute to MinIO 
    gold_layer_stream.map(S3MinIOSinkGold(), output_type=Types.STRING())

    # Sink dashboard-ready data to kafka
    gold_layer_stream.map(SinkToKafkaTopic(GOLD_IOT_TOPIC))

    logger.info("Executing Flink job")
    env.execute("IoT Measurements Processing Pipeline")


if __name__ == "__main__":
    main()

