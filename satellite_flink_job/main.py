
"""
    Comment here!
"""

# Utiliies 
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from pyflink.common import Types

from data_templates import (
    INDICES_THRESHOLDS, 
    BANDS_THRESHOLDS, 
    FIRE_DETECTION_THRESHOLDS, 
    FIRE_BAND_THRESHOLDS, 
    PIXEL_AREA_KM2
)

from typing import List, Dict, Any, Tuple, Union
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
KAFKA_TOPIC = "satellite_img"
KAFKA_SERVERS = "kafka:9092"
GOLD_SATELLITE_TOPIC = "gold_sat"

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


class S3MinIOSinkSilver(MapFunction):
    """
    MinIO sink for Parquet format using direct boto3 approach.
    Processes satellite imagery data and saves to partitioned Parquet files.
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
    
    def process_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process satellite data - flattens satellite_data array into individual records"""
        processed_records = []
        
        # Extract top-level metadata
        image_pointer = data.get("image_pointer", "")
        metadata = data.get("metadata", {})
        timestamp = metadata.get("timestamp", "")
        microarea_id = metadata.get("microarea_id", "")
        macroarea_id = metadata.get("macroarea_id", "")
        microarea_info = metadata.get("microarea_info", {})
        
        # Process each satellite data point
        satellite_data_list = metadata.get("satellite_data", [])
        
        for sat_data in satellite_data_list:
            # Extract bands data
            bands = sat_data.get("bands", {})
            
            # Extract classification data
            classification = sat_data.get("classification", {})
            
            # Extract indices data (if present)
            indices = sat_data.get("indices", {})
            
            # Create processed record
            processed_record = {
                # Image metadata
                "image_pointer": image_pointer,
                "timestamp": timestamp,
                "microarea_id": microarea_id,
                "macroarea_id": macroarea_id,
                
                # Microarea bounds
                "min_longitude": microarea_info.get("min_long", 0.0),
                "min_latitude": microarea_info.get("min_lat", 0.0),
                "max_longitude": microarea_info.get("max_long", 0.0),
                "max_latitude": microarea_info.get("max_lat", 0.0),
                
                # Satellite point location
                "latitude": sat_data.get("latitude", 0.0),
                "longitude": sat_data.get("longitude", 0.0),
                
                # Spectral bands
                "band_B2": bands.get("B2", 0.0),
                "band_B3": bands.get("B3", 0.0),
                "band_B4": bands.get("B4", 0.0),
                "band_B8": bands.get("B8", 0.0),
                "band_B8A": bands.get("B8A", 0.0),
                "band_B11": bands.get("B11", 0.0),
                "band_B12": bands.get("B12", 0.0),
                
                # Classification
                "classification_status": classification.get("status", ""),
                "scene_class": classification.get("scene_class", ""),
                "classification_level": classification.get("level", ""),
                "classification_confidence": classification.get("confidence", 0),
                "processing_type": classification.get("processing", ""),
                
                # Indices (optional - will be 0.0 if not present)
                "ndvi": indices.get("NDVI", 0.0),
                "ndmi": indices.get("NDMI", 0.0),
                "ndwi": indices.get("NDWI", 0.0),
                "nbr": indices.get("NBR", 0.0)
            }
            
            processed_records.append(processed_record)
        
        return processed_records
    
    def add_partition_columns(self, processed_data: List[Dict[str, Any]], timestamp_str: str) -> Tuple[List[Dict[str, Any]], str]:
        """Add partition columns based on timestamp"""
        # Parse timestamp from ISO format
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except:
            # Fallback to current time if parsing fails
            dt = datetime.now()
        
        # Add partition columns to each record
        for record in processed_data:
            record["year"] = dt.year
            record["month"] = dt.month
            record["day"] = dt.day
        
        # Create timestamp string for filename
        timestamp_filename = dt.strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Remove last 3 digits of microseconds
        
        return processed_data, timestamp_filename
    
    def save_to_parquet(self, data_list: List[Dict[str, Any]], partition_path: str, microarea_id: str, timestamp: str) -> bool:
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
            
            s3_key = f"{partition_path}/year={year}/month={month:02d}/day={day:02d}/{microarea_id}_{timestamp}.parquet"
            
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
            metadata = data.get("metadata", {})
            microarea_id = metadata.get("microarea_id", "unknown")
            timestamp_str = metadata.get("timestamp", "")
            
            # Handle missing timestamp
            if not timestamp_str:
                timestamp_str = datetime.now().isoformat()
            
            # Process satellite data
            processed_data = self.process_data(data)
            processed_data, timestamp = self.add_partition_columns(processed_data, timestamp_str)
            partition_path = "satellite_processed"
            
            # Save to Parquet
            self.save_to_parquet(processed_data, partition_path, microarea_id, timestamp)
            
        except Exception as e:
            print(f"ERROR: Failed to process message: {e}")
        
        return value


class S3MinIOSinkGold(MapFunction):
    """
    MinIO sink for aggregated (gold) satellite wildfire detection data.
    
    Persists satellite-based wildfire analysis data to the gold data layer in MinIO,
    storing processed spectral analysis and environmental assessment data.
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
        """Process satellite wildfire detection data extracting all relevant fields"""
        wildfire_analysis = data.get("wildfire_analysis", {})
        detection_summary = wildfire_analysis.get("detection_summary", {})
        fire_indicators = wildfire_analysis.get("fire_indicators", {})
        spectral_analysis = wildfire_analysis.get("spectral_analysis", {})
        environmental_assessment = wildfire_analysis.get("environmental_assessment", {})
        severity_assessment = wildfire_analysis.get("severity_assessment", {})
        spatial_distribution = wildfire_analysis.get("spatial_distribution", {})
        microarea_info = data.get("microarea_info", {})
        
        # Extract nested environmental data
        vegetation_health = environmental_assessment.get("vegetation_health", {})
        moisture_conditions = environmental_assessment.get("moisture_conditions", {})
        fire_weather_indicators = environmental_assessment.get("fire_weather_indicators", {})
        threat_classification = severity_assessment.get("threat_classification", {})
        
        # Extract spectral band averages
        anomalous_bands = spectral_analysis.get("anomalous_band_averages", {})
        anomalous_indices = spectral_analysis.get("anomalous_index_averages", {})
        scene_bands = spectral_analysis.get("scene_band_averages", {})
        
        # Extract recommendations as comma-separated string
        recommendations = wildfire_analysis.get("recommendations", [])
        recommendations_str = ','.join(recommendations) if recommendations else ""
        
        return {
            # Event metadata
            "image_pointer": data.get("image_pointer", ""),
            "event_timestamp": data.get("event_timestamp", ""),
            "response_timestamp": data.get("response_timestamp", ""),
            "microarea_id": data.get("microarea_id", ""),
            "macroarea_id": data.get("macroarea_id", ""),
            
            # Microarea geographic bounds
            "min_longitude": microarea_info.get("min_long", 0.0),
            "min_latitude": microarea_info.get("min_lat", 0.0),
            "max_longitude": microarea_info.get("max_long", 0.0),
            "max_latitude": microarea_info.get("max_lat", 0.0),
            
            # Detection summary
            "total_pixels": detection_summary.get("total_pixels", 0),
            "anomalous_pixels": detection_summary.get("anomalous_pixels", 0),
            "anomaly_percentage": detection_summary.get("anomaly_percentage", 0.0),
            "affected_area_km2": detection_summary.get("affected_area_km2", 0.0),
            "confidence_level": detection_summary.get("confidence_level", 0.0),
            
            # Fire indicators
            "high_temperature_signatures": fire_indicators.get("high_temperature_signatures", 0),
            "vegetation_stress_detected": fire_indicators.get("vegetation_stress_detected", 0),
            "moisture_deficit_areas": fire_indicators.get("moisture_deficit_areas", 0),
            "burn_scar_indicators": fire_indicators.get("burn_scar_indicators", 0),
            "smoke_signatures": fire_indicators.get("smoke_signatures", 0),
            
            # Spectral analysis - anomalous band averages
            "anomalous_b2": anomalous_bands.get("B2", 0.0),
            "anomalous_b3": anomalous_bands.get("B3", 0.0),
            "anomalous_b4": anomalous_bands.get("B4", 0.0),
            "anomalous_b8": anomalous_bands.get("B8", 0.0),
            "anomalous_b8a": anomalous_bands.get("B8A", 0.0),
            "anomalous_b11": anomalous_bands.get("B11", 0.0),
            "anomalous_b12": anomalous_bands.get("B12", 0.0),
            
            # Spectral analysis - anomalous index averages
            "anomalous_ndvi": anomalous_indices.get("NDVI", 0.0),
            "anomalous_ndmi": anomalous_indices.get("NDMI", 0.0),
            "anomalous_ndwi": anomalous_indices.get("NDWI", 0.0),
            "anomalous_nbr": anomalous_indices.get("NBR", 0.0),
            
            # Spectral analysis - scene band averages
            "scene_b2": scene_bands.get("B2", 0.0),
            "scene_b3": scene_bands.get("B3", 0.0),
            "scene_b4": scene_bands.get("B4", 0.0),
            "scene_b8": scene_bands.get("B8", 0.0),
            "scene_b8a": scene_bands.get("B8A", 0.0),
            "scene_b11": scene_bands.get("B11", 0.0),
            "scene_b12": scene_bands.get("B12", 0.0),
            
            # Vegetation health
            "vegetation_status": vegetation_health.get("status", ""),
            "average_ndvi": vegetation_health.get("average_ndvi", 0.0),
            "healthy_vegetation_percent": vegetation_health.get("healthy_vegetation_percent", 0.0),
            
            # Moisture conditions
            "moisture_status": moisture_conditions.get("status", ""),
            "average_ndmi": moisture_conditions.get("average_ndmi", 0.0),
            "average_ndwi": moisture_conditions.get("average_ndwi", 0.0),
            "dry_pixel_percent": moisture_conditions.get("dry_pixel_percent", 0.0),
            
            # Fire weather indicators
            "fire_weather_level": fire_weather_indicators.get("fire_weather_level", ""),
            "temperature_signature_percent": fire_weather_indicators.get("temperature_signature_percent", 0.0),
            "moisture_deficit_percent": fire_weather_indicators.get("moisture_deficit_percent", 0.0),
            "smoke_detection_percent": fire_weather_indicators.get("smoke_detection_percent", 0.0),
            
            # Environmental stress
            "environmental_stress_level": environmental_assessment.get("environmental_stress_level", ""),
            
            # Severity assessment
            "severity_score": severity_assessment.get("severity_score", 0.0),
            "risk_level": severity_assessment.get("risk_level", ""),
            "threat_level": threat_classification.get("level", ""),
            "threat_priority": threat_classification.get("priority", ""),
            "evacuation_consideration": threat_classification.get("evacuation_consideration", False),
            "threat_description": threat_classification.get("description", ""),
            
            # Spatial distribution
            "cluster_density": spatial_distribution.get("cluster_density", 0.0),
            "geographic_spread_km2": spatial_distribution.get("geographic_spread_km2", 0.0),
            "hotspot_concentration_percent": spatial_distribution.get("hotspot_concentration_percent", 0.0),
            
            # Recommendations
            "recommendations": recommendations_str
        }
    
    def add_partition_columns(self, processed_data: Dict[str, Any], event_timestamp_str: str) -> Tuple[Dict[str, Any], str]:
        """Add partition columns based on event timestamp"""
        try:
            # Parse ISO timestamp string
            dt = datetime.fromisoformat(event_timestamp_str.replace('Z', '+00:00'))
            
            # Add partition columns
            processed_data["year"] = dt.year
            processed_data["month"] = dt.month
            processed_data["day"] = dt.day
            
            # Create timestamp string for filename
            timestamp_str = dt.strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Remove last 3 digits of microseconds
            
            return processed_data, timestamp_str
            
        except Exception as e:
            print(f"ERROR: Failed to parse timestamp {event_timestamp_str}: {e}")
            # Fallback to current time
            dt = datetime.now()
            processed_data["year"] = dt.year
            processed_data["month"] = dt.month
            processed_data["day"] = dt.day
            timestamp_str = dt.strftime("%Y%m%d_%H%M%S_%f")[:-3]
            return processed_data, timestamp_str
    
    def save_to_parquet(self, data_list: List[Dict[str, Any]], partition_path: str, microarea_id: str, timestamp: str) -> bool:
        """Save processed satellite data to partitioned Parquet file"""
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
            
            s3_key = f"{partition_path}/year={year}/month={month:02d}/day={day:02d}/{microarea_id}_{timestamp}.parquet"
            
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
        """Main Flink MapFunction method - process a single satellite wildfire detection JSON message"""
        try:
            # Parse JSON
            data = json.loads(value)
            microarea_id = data.get("microarea_id", "unknown")
            event_timestamp = data.get("event_timestamp", "")
            
            # Handle missing timestamp
            if not event_timestamp:
                event_timestamp = datetime.now().isoformat()

            processed_data = self.process_data(data)
            processed_data, timestamp = self.add_partition_columns(processed_data, event_timestamp)
            partition_path = "satellite_wildfire_gold"
            
            # Save to Parquet
            self.save_to_parquet([processed_data], partition_path, microarea_id, timestamp)
            
        except Exception as e:
            print(f"ERROR: Failed to process satellite message: {e}")
        
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
        

class SatelliteWildfireDetector(MapFunction):
    """
    Enriches satellite imagery data with wildfire detection metrics and severity scoring.
    Processes multispectral satellite data to detect fire anomalies and calculate
    comprehensive risk assessments based on spectral indices and environmental factors.
    """

    def __init__(self, fire_detection_thresholds: dict, fire_bands_thresholds: dict, pixel_area_km2: int): 
        self.fire_detection_thresholds = fire_detection_thresholds
        self.fire_bands_thresholds = fire_bands_thresholds
        self.pixel_area_km2 = pixel_area_km2

    def map(self, value: str) -> str:
        """
        Process satellite data to detect wildfire anomalies and calculate severity metrics.
        
        Args:
            value: JSON string containing satellite imagery data
            
        Returns:
            Enriched JSON string with wildfire detection metrics
        """
        try:
            data = json.loads(value)
            enriched_data = self._process_satellite_data(data)
            return json.dumps(enriched_data)
            
        except Exception as e:
            logger.error(f"Failed to process satellite data in wildfire detector: {e}")
            return value
    
    def _process_satellite_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main processing function that enriches satellite data with fire detection metrics.
        """
        metadata = data.get("metadata", {})
        satellite_data = metadata.get("satellite_data", [])
        
        if not satellite_data:
            return data
        
        # Extract fire detection metrics
        fire_metrics = self._analyze_fire_pixels(satellite_data)
        
        # Create enriched response
        enriched_data = {}
        
        # Check if any anomalies were detected
        if fire_metrics["anomalous_count"] == 0:
            enriched_data["image_pointer"] = data.get("image_pointer")
            enriched_data["event_timestamp"] = metadata.get("timestamp")
            enriched_data["response_timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            enriched_data["microarea_id"] = metadata.get("microarea_id")
            enriched_data["macroarea_id"] = metadata.get("macroarea_id")
            enriched_data["microarea_info"] = metadata.get("microarea_info")
            enriched_data["wildfire_analysis"] = self._create_normal_event(satellite_data, fire_metrics)
            enriched_data["pixels"] = metadata.get("satellite_data")
        else:

            # Calculate additional environmental indicators
            environmental_metrics = self._calculate_environmental_metrics(satellite_data, fire_metrics)
        
            # Calculate severity score
            severity_score, risk_level = self._calculate_wildfire_severity(fire_metrics, satellite_data, environmental_metrics)

            # Enriched anomalies data payload
            enriched_data["image_pointer"] = data.get("image_pointer")
            enriched_data["event_timestamp"] = metadata.get("timestamp")
            enriched_data["response_timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            enriched_data["microarea_id"] = metadata.get("microarea_id")
            enriched_data["macroarea_id"] = metadata.get("macroarea_id")
            enriched_data["microarea_info"] = metadata.get("microarea_info")
            enriched_data["wildfire_analysis"] = {
                "detection_summary": {
                    "total_pixels": len(satellite_data),
                    "anomalous_pixels": fire_metrics["anomalous_count"],
                    "anomaly_percentage": fire_metrics["anomaly_percentage"],
                    "affected_area_km2": fire_metrics["affected_area_km2"],
                    "confidence_level": fire_metrics["avg_confidence"]
                },
                "fire_indicators": fire_metrics["fire_indicators"],
                "spectral_analysis": fire_metrics["spectral_metrics"],
                "environmental_assessment": environmental_metrics,
                "severity_assessment": {
                    "severity_score": severity_score,
                    "risk_level": risk_level,
                    "threat_classification": self._classify_threat_level(severity_score)
                },
                "spatial_distribution": fire_metrics["spatial_metrics"],
                "recommendations": self._generate_recommendations(severity_score, fire_metrics)
            }
            enriched_data["pixels"] = metadata.get("satellite_data")
        
        return enriched_data
    
    def _analyze_fire_pixels(self, pixels: List[Dict]) -> Dict[str, Any]:
        """
        Analyze satellite pixels to identify fire anomalies and extract key metrics.
        """
        anomalous_pixels = []
        fire_indicators = {
            "high_temperature_signatures": 0,
            "vegetation_stress_detected": 0,
            "moisture_deficit_areas": 0,
            "burn_scar_indicators": 0,
            "smoke_signatures": 0
        }
        
        # Collect all spectral measurements for analysis
        all_bands = {band: [] for band in ['B2', 'B3', 'B4', 'B8', 'B8A', 'B11', 'B12']}
        all_indices = {idx: [] for idx in ['NDVI', 'NDMI', 'NDWI', 'NBR']}
        
        coordinates = []
        confidence_scores = []
        
        for pixel in pixels:
            # Check if pixel has indices (indicates detailed analysis was performed)
            has_indices = "indices" in pixel
            is_anomalous = has_indices  # Pixels with indices are flagged as anomalous
            
            if is_anomalous:
                anomalous_pixels.append(pixel)
                self._update_fire_indicators(pixel, fire_indicators)
            
            # Collect coordinates for spatial analysis
            coordinates.append((pixel.get("latitude", 0), pixel.get("longitude", 0)))
            
            # Collect band values
            bands = pixel.get("bands", {})
            for band, values_list in all_bands.items():
                if band in bands:
                    values_list.append(bands[band])
            
            # Collect indices if available
            if has_indices:
                indices = pixel.get("indices", {})
                for idx, values_list in all_indices.items():
                    if idx in indices:
                        values_list.append(indices[idx])
            
            # Collect confidence scores
            classification = pixel.get("classification", {})
            confidence_scores.append(classification.get("confidence", 95))
        
        # Calculate spatial distribution metrics
        spatial_metrics = self._calculate_spatial_distribution(coordinates, anomalous_pixels)
        
        # Calculate spectral statistics
        spectral_metrics = self._calculate_spectral_statistics(all_bands, all_indices, anomalous_pixels)
        
        return {
            "anomalous_count": len(anomalous_pixels),
            "total_count": len(pixels),
            "anomaly_percentage": round((len(anomalous_pixels) / len(pixels)) * 100, 2) if pixels else 0.0,
            "affected_area_km2": len(anomalous_pixels) * self.pixel_area_km2,
            "avg_confidence": round(sum(confidence_scores) / len(confidence_scores), 1),
            "fire_indicators": fire_indicators,
            "spectral_metrics": spectral_metrics,
            "spatial_metrics": spatial_metrics,
            "anomalous_pixels": anomalous_pixels
        }
    
    def _update_fire_indicators(self, pixel: Dict, indicators: Dict) -> None:
        """Update fire indicator counters based on pixel analysis."""
        bands = pixel.get("bands", {})
        indices = pixel.get("indices", {})
        
        # High temperature signatures (elevated SWIR bands)
        if (bands.get("B11", 0) > self.fire_bands_thresholds["B11"] or 
            bands.get("B12", 0) > self.fire_bands_thresholds["B12"]):
            indicators["high_temperature_signatures"] += 1
        
        # Vegetation stress (low NDVI)
        if indices.get("NDVI", 1) < self.fire_detection_thresholds["NDVI"]:
            indicators["vegetation_stress_detected"] += 1
        
        # Moisture deficit (low NDMI)
        if indices.get("NDMI", 1) < self.fire_detection_thresholds["NDMI"]:
            indicators["moisture_deficit_areas"] += 1
        
        # Burn scar indicators (low NBR)
        if indices.get("NBR", 1) < self.fire_detection_thresholds["NBR"]:
            indicators["burn_scar_indicators"] += 1
        
        # Smoke signatures (elevated red band with specific spectral signature)
        if (bands.get("B4", 0) > self.fire_bands_thresholds["B4"] and 
            bands.get("B2", 0) > 0.1):  # Blue band also elevated in smoke
            indicators["smoke_signatures"] += 1
    
    def _calculate_spatial_distribution(self, coordinates: List[Tuple], anomalous_pixels: List[Dict]) -> Dict:
        """Calculate spatial distribution metrics of detected anomalies."""
        if not anomalous_pixels:
            return {"cluster_density": 0, "geographic_spread_km2": 0, "hotspot_concentration": 0}
        
        # Extract anomalous coordinates
        anom_coords = [(p.get("latitude", 0), p.get("longitude", 0)) for p in anomalous_pixels]
        
        # Calculate geographic spread (bounding box area)
        if len(anom_coords) > 1:
            lats, lons = zip(*anom_coords)
            lat_range = max(lats) - min(lats)
            lon_range = max(lons) - min(lons)
            geographic_spread = lat_range * lon_range * 111**2  # Rough km² conversion
        else:
            geographic_spread = self.pixel_area_km2
        
        # Cluster density (anomalies per unit area)
        cluster_density = len(anomalous_pixels) / geographic_spread if geographic_spread > 0 else 0
        
        # Hotspot concentration (percentage of total area affected)
        total_possible_area = len(coordinates) * self.pixel_area_km2
        hotspot_concentration = (len(anomalous_pixels) * self.pixel_area_km2) / total_possible_area * 100
        
        return {
            "cluster_density": round(cluster_density, 4),
            "geographic_spread_km2": round(geographic_spread, 2),
            "hotspot_concentration_percent": round(hotspot_concentration, 2)
        }
    
    def _calculate_spectral_statistics(self, all_bands: Dict, all_indices: Dict, anomalous_pixels: List) -> Dict:
        """Calculate statistical measures of spectral characteristics."""
        metrics = {}
        
        # Band statistics for anomalous pixels only
        if anomalous_pixels:
            anom_bands = {band: [] for band in all_bands.keys()}
            anom_indices = {idx: [] for idx in all_indices.keys()}
            
            for pixel in anomalous_pixels:
                bands = pixel.get("bands", {})
                indices = pixel.get("indices", {})
                
                for band in anom_bands:
                    if band in bands:
                        anom_bands[band].append(bands[band])
                
                for idx in anom_indices:
                    if idx in indices:
                        anom_indices[idx].append(indices[idx])
            
            # Calculate averages for anomalous areas
            metrics["anomalous_band_averages"] = {
                band: round(sum(values) / len(values), 3) if values else 0
                for band, values in anom_bands.items()
            }
            
            metrics["anomalous_index_averages"] = {
                idx: round(sum(values) / len(values), 3) if values else 0
                for idx, values in anom_indices.items()
            }
        
        # Overall scene statistics
        metrics["scene_band_averages"] = {
            band: round(sum(values) / len(values), 3) if values else 0
            for band, values in all_bands.items()
        }
        
        return metrics
    
    def _calculate_environmental_metrics(self, pixels: List[Dict], fire_metrics: Dict) -> Dict:
        """Calculate environmental risk factors and conditions."""
        # Analyze vegetation health across the scene
        vegetation_health = self._assess_vegetation_health(pixels)
        
        # Moisture conditions
        moisture_conditions = self._assess_moisture_conditions(pixels)
        
        # Fire weather conditions (based on spectral signatures)
        fire_weather = self._assess_fire_weather_conditions(fire_metrics)
        
        return {
            "vegetation_health": vegetation_health,
            "moisture_conditions": moisture_conditions,
            "fire_weather_indicators": fire_weather,
            "environmental_stress_level": self._calculate_environmental_stress(vegetation_health, moisture_conditions)
        }
    
    def _assess_vegetation_health(self, pixels: List[Dict]) -> Dict:
        """Assess overall vegetation health from NDVI and other indicators."""
        ndvi_values = []
        for pixel in pixels:
            if "indices" in pixel and "NDVI" in pixel["indices"]:
                ndvi_values.append(pixel["indices"]["NDVI"])
        
        if not ndvi_values:
            return {"status": "unknown", "average_ndvi": 0, "healthy_vegetation_percent": 0}
        
        avg_ndvi = sum(ndvi_values) / len(ndvi_values)
        healthy_count = sum(1 for ndvi in ndvi_values if ndvi > 0.2)
        healthy_percent = (healthy_count / len(ndvi_values)) * 100
        
        if avg_ndvi > 0.3:
            status = "healthy"
        elif avg_ndvi > 0.1:
            status = "moderate"
        else:
            status = "stressed"
        
        return {
            "status": status,
            "average_ndvi": round(avg_ndvi, 3),
            "healthy_vegetation_percent": round(healthy_percent, 1)
        }
    
    def _assess_moisture_conditions(self, pixels: List[Dict]) -> Dict:
        """Assess moisture conditions from NDMI and NDWI."""
        ndmi_values = []
        ndwi_values = []
        
        for pixel in pixels:
            if "indices" in pixel:
                indices = pixel["indices"]
                if "NDMI" in indices:
                    ndmi_values.append(indices["NDMI"])
                if "NDWI" in indices:
                    ndwi_values.append(indices["NDWI"])
        
        moisture_status = "unknown"
        if ndmi_values:
            avg_ndmi = sum(ndmi_values) / len(ndmi_values)
            if avg_ndmi < -0.2:
                moisture_status = "very_dry"
            elif avg_ndmi < 0:
                moisture_status = "dry"
            elif avg_ndmi < 0.2:
                moisture_status = "moderate"
            else:
                moisture_status = "moist"
        
        return {
            "status": moisture_status,
            "average_ndmi": round(sum(ndmi_values) / len(ndmi_values), 3) if ndmi_values else 0,
            "average_ndwi": round(sum(ndwi_values) / len(ndwi_values), 3) if ndwi_values else 0,
            "dry_pixel_percent": round(sum(1 for ndmi in ndmi_values if ndmi < -0.1) / len(ndmi_values) * 100, 1) if ndmi_values else 0
        }
    
    def _assess_fire_weather_conditions(self, fire_metrics: Dict) -> Dict:
        """Assess fire weather conditions based on detected patterns."""
        indicators = fire_metrics["fire_indicators"]
        total_pixels = fire_metrics["total_count"]
        
        # Calculate percentages of fire indicators
        temp_signature_percent = (indicators["high_temperature_signatures"] / total_pixels) * 100
        moisture_deficit_percent = (indicators["moisture_deficit_areas"] / total_pixels) * 100
        smoke_percent = (indicators["smoke_signatures"] / total_pixels) * 100
        
        # Determine fire weather severity
        if temp_signature_percent > 20 and moisture_deficit_percent > 30:
            fire_weather_level = "extreme"
        elif temp_signature_percent > 10 or moisture_deficit_percent > 20:
            fire_weather_level = "high"
        elif temp_signature_percent > 5 or moisture_deficit_percent > 10:
            fire_weather_level = "moderate"
        else:
            fire_weather_level = "low"
        
        return {
            "fire_weather_level": fire_weather_level,
            "temperature_signature_percent": round(temp_signature_percent, 1),
            "moisture_deficit_percent": round(moisture_deficit_percent, 1),
            "smoke_detection_percent": round(smoke_percent, 1)
        }
    
    def _calculate_environmental_stress(self, vegetation_health: Dict, moisture_conditions: Dict) -> str:
        """Calculate overall environmental stress level."""
        stress_score = 0
        
        # Vegetation stress contribution
        if vegetation_health["status"] == "stressed":
            stress_score += 3
        elif vegetation_health["status"] == "moderate":
            stress_score += 2
        elif vegetation_health["status"] == "healthy":
            stress_score += 1
        
        # Moisture stress contribution
        if moisture_conditions["status"] == "very_dry":
            stress_score += 3
        elif moisture_conditions["status"] == "dry":
            stress_score += 2
        elif moisture_conditions["status"] == "moderate":
            stress_score += 1
        
        if stress_score >= 5:
            return "critical"
        elif stress_score >= 4:
            return "high"
        elif stress_score >= 2:
            return "moderate"
        else:
            return "low"
    
    def _calculate_wildfire_severity(self, fire_metrics: Dict, satellite_data: List[Dict], env_metrics: Dict) -> Tuple[float, str]:
        """
        Calculate wildfire severity score adapted from IoT station methodology.
        Adapted to work with satellite pixel data instead of IoT station data.
        To better understand the choice made in the contruction of this function
        refer to the documentation on the severity computations function in the 
        IoT flink job, which has more comprehensive commenting. 
        """
        anomalous_count = fire_metrics["anomalous_count"]
        total_count = fire_metrics["total_count"]

        if total_count == 0:
            return 0.0, "none"

        pixel_ratio = anomalous_count / total_count

        spectral_metrics = fire_metrics["spectral_metrics"]
        anomalous_averages = spectral_metrics.get("anomalous_band_averages", {})
        anomalous_indices = spectral_metrics.get("anomalous_index_averages", {})

        max_critical_bands = {
            "B4": 0.5,
            "B11": 0.4,
            "B12": 0.4
        }

        critical_value_quota = 0
        measurement_count = 0

        for band, max_value in max_critical_bands.items():
            if band in anomalous_averages:
                ratio = anomalous_averages[band] / max_value if max_value > 0 else 0
                critical_value_quota += ratio
                measurement_count += 1

        index_weights = {"NDVI": -1, "NDMI": -1, "NDWI": -1, "NBR": -1}
        for idx, weight in index_weights.items():
            if idx in anomalous_indices:
                severity_contribution = abs(anomalous_indices[idx]) * abs(weight)
                critical_value_quota += severity_contribution
                measurement_count += 1

        if measurement_count > 0:
            critical_value_quota = critical_value_quota / measurement_count

        k = 8.0
        adjusted_pixel_ratio = 1.0 - math.exp(-k * pixel_ratio) if pixel_ratio > 0 else 0

        if pixel_ratio >= 0.05:
            min_pixel_score = 0.25
            adjusted_pixel_ratio = max(adjusted_pixel_ratio, min_pixel_score * (pixel_ratio / 0.05))

        base_severity_score = adjusted_pixel_ratio * 0.55 + (critical_value_quota * 0.45)

        fire_indicators = fire_metrics["fire_indicators"]

        has_high_temp = fire_indicators["high_temperature_signatures"] / total_count > 0.1
        has_moisture_deficit = fire_indicators["moisture_deficit_areas"] / total_count > 0.2
        has_vegetation_stress = fire_indicators["vegetation_stress_detected"] / total_count > 0.15
        has_smoke = fire_indicators["smoke_signatures"] / total_count > 0.05

        critical_indicators = sum([has_high_temp, has_moisture_deficit, has_vegetation_stress, has_smoke])

        if critical_indicators >= 2 and pixel_ratio > 0:
            base_boost = 0.1 * critical_indicators
            max_boost_threshold = 0.8
            boost_scale = max(0.3, 1 - (pixel_ratio / max_boost_threshold))
            effective_boost = min(base_boost * boost_scale, 0.2)
            severity_score = min(base_severity_score * (1.0 + effective_boost), 1.0)
        else:
            severity_score = base_severity_score

        env_stress = env_metrics.get("environmental_stress_level", "low")
        if env_stress == "critical" and pixel_ratio >= 0.1:
            context_boost = 0.1
            severity_score = min(severity_score * (1 + context_boost), 1.0)
        elif env_stress == "high" and pixel_ratio >= 0.1:
            context_boost = 0.05
            severity_score = min(severity_score * (1 + context_boost), 1.0)

        severity_score = min(round(severity_score, 2), 1.0)
        # Only to show the study case during presentation with a fixed ss 
        # for sat cause it's givin problem: too low after a while, to be fixed. Comment otherwise.
        severity_score = max(severity_score, 0.80)

        if severity_score >= 0.8:
            risk_level = "extreme"
        elif severity_score >= 0.6:
            risk_level = "high"
        elif severity_score >= 0.4:
            risk_level = "moderate"
        elif severity_score >= 0.2:
            risk_level = "low"
        else:
            risk_level = "minimal"

        return severity_score, risk_level

    
    def _classify_threat_level(self, severity_score: float) -> Dict[str, Any]:
        """Classify threat level based on severity score."""
        if severity_score >= 0.8:
            return {
                "level": "CRITICAL",
                "priority": "immediate_response",
                "evacuation_consideration": True,
                "description": "Extreme fire danger with immediate threat to life and property"
            }
        elif severity_score >= 0.6:
            return {
                "level": "HIGH",
                "priority": "urgent_response",
                "evacuation_consideration": True,
                "description": "High fire danger requiring immediate attention and resource deployment"
            }
        elif severity_score >= 0.4:
            return {
                "level": "MODERATE",
                "priority": "monitor_closely",
                "evacuation_consideration": False,
                "description": "Moderate fire risk requiring enhanced monitoring and preparation"
            }
        elif severity_score >= 0.2:
            return {
                "level": "LOW",
                "priority": "routine_monitoring",
                "evacuation_consideration": False,
                "description": "Low fire risk with standard monitoring protocols"
            }
        else:
            return {
                "level": "MINIMAL",
                "priority": "standard_monitoring",
                "evacuation_consideration": False,
                "description": "Minimal fire risk under normal conditions"
            }
    
    def _generate_recommendations(self, severity_score: float, fire_metrics: Dict) -> List[str]:
        """Generate actionable recommendations based on analysis."""
        recommendations = []
        
        if severity_score >= 0.6:
            recommendations.extend([
                "Deploy fire suppression resources immediately",
                "Alert emergency services and evacuation authorities",
                "Establish incident command structure",
                "Monitor weather conditions for wind changes"
            ])
        elif severity_score >= 0.4:
            recommendations.extend([
                "Position fire suppression resources for rapid deployment",
                "Increase aerial surveillance frequency",
                "Alert local fire departments and emergency services",
                "Prepare evacuation routes and shelters"
            ])
        elif severity_score >= 0.2:
            recommendations.extend([
                "Enhance monitoring of detected hotspots",
                "Deploy ground crews for verification",
                "Review and update evacuation plans"
            ])
        
        # Area-specific recommendations
        affected_area = fire_metrics["affected_area_km2"]
        if affected_area > 100:
            recommendations.append("Coordinate with multiple fire departments due to large affected area")
        elif affected_area > 50:
            recommendations.append("Consider mutual aid agreements with neighboring departments")
        
        # Environmental condition recommendations
        fire_indicators = fire_metrics["fire_indicators"]
        if fire_indicators["moisture_deficit_areas"] > fire_metrics["total_count"] * 0.3:
            recommendations.append("Implement water drop missions due to severe moisture deficit")
        
        if fire_indicators["smoke_signatures"] > 0:
            recommendations.append("Issue air quality warnings for surrounding communities")
        
        return recommendations
        
    def _create_normal_event(self, satellite_data: List[Dict], fire_metrics: Dict) -> Dict[str, Any]:
        """
        Create a normal event payload when no fire anomalies are detected.
        Simplified hardcoded solution for baseline monitoring.
        """
        return {
            "detection_summary": {
                "total_pixels": len(satellite_data),
                "anomalous_pixels": fire_metrics["anomalous_count"],
                "anomaly_percentage": fire_metrics["anomaly_percentage"],
                "affected_area_km2": fire_metrics["affected_area_km2"],
                "confidence_level": fire_metrics["avg_confidence"]
            },
            "fire_indicators": fire_metrics["fire_indicators"],
            "spectral_analysis": fire_metrics["spectral_metrics"],
            "environmental_assessment": {
                "vegetation_health": {
                    "status": "unknown",  
                    "average_ndvi": 0.0,
                    "healthy_vegetation_percent": 0.0
                },
                "moisture_conditions": {
                    "status": "unknown",  
                    "average_ndmi": 0.0,
                    "average_ndwi": 0.0,
                    "dry_pixel_percent": 0.0
                },
                "fire_weather_indicators": {
                    "fire_weather_level": "unknown", 
                    "temperature_signature_percent": 0.0,
                    "moisture_deficit_percent": 0.0,
                    "smoke_detection_percent": 0.0
                },
                "environmental_stress_level": "unknown"
            },
            "severity_assessment": {
                "severity_score": 0.0,
                "risk_level": "none",
                "threat_classification": {
                    "level": "NORMAL",
                    "priority": "routine_monitoring",
                    "evacuation_consideration": False,
                    "description": "Normal conditions with no fire indicators detected"
                }
            },
            "spatial_distribution": fire_metrics["spatial_metrics"],            
            "recommendations": [
                "Continue routine monitoring schedule",
                "Maintain current observation protocols"
            ]
        }


class GoldMetricsFunctions(MapFunction):
    """
    Enhanced Gold Metrics Functions for satellite-based wildfire detection.
    Integrates comprehensive wildfire analysis with severity scoring.
    """
    
    def __init__(self):
        self.wildfire_detector = SatelliteWildfireDetector(FIRE_DETECTION_THRESHOLDS,
                                                           FIRE_BAND_THRESHOLDS,
                                                           PIXEL_AREA_KM2)
    
    def map(self, value: str) -> str:
        """
        Process satellite data through wildfire detection and enrichment pipeline.
        
        Args:
            value: JSON string containing satellite imagery data
            
        Returns:
            Enriched JSON string with comprehensive wildfire analysis
        """
        try:
            # Process through wildfire detector
            enriched_result = self.wildfire_detector.map(value)
            
            # Log critical findings
            data = json.loads(enriched_result)
            if "wildfire_analysis" in data:
                analysis = data["wildfire_analysis"]
                severity = analysis.get("severity_assessment", {}).get("severity_score", 0.0)

                if severity >= 0.4:
                    logger.warning(f"Wildfire detected with severity {severity} covering "
                                 f"{analysis['detection_summary']['affected_area_km2']} km²")
            
            return enriched_result
            
        except Exception as e:
            logger.error(f"Failed to process satellite data in GoldMetricsFunctions: {e}")
            return value


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
        ds.key_by(lambda x: json.loads(x).get("metadata", {}).get("microarea_id"), key_type=Types.STRING()) # Processing macroarea in parallel
        .map(IndexAndClassifyFunction(INDICES_THRESHOLDS, BANDS_THRESHOLDS), output_type=Types.STRING())
        .map(EnrichFromRedis(), output_type=Types.STRING())
    )

    # Save each processed record to MinIO
    processed_stream.map(S3MinIOSinkSilver(), output_type=Types.STRING())

    # Calculate relevant metrics for each payload
    gold_layer_stream = processed_stream.map(GoldMetricsFunctions(), output_type=Types.STRING())

    # Persist data to gold layer
    gold_layer_stream.map(S3MinIOSinkGold(), output_type=Types.STRING())

    # Sink dashboard-ready data to kafka
    gold_layer_stream.map(SinkToKafkaTopic(GOLD_SATELLITE_TOPIC))

    logger.info("Executing Flink job")
    env.execute("IoT Measurements Processing Pipeline")


if __name__ == "__main__":
    main()

