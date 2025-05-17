
# Utilities
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Time, Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction

from kafka.admin import KafkaAdminClient
from datetime import datetime
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


class JsonTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            data = json.loads(value)
            ts = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%S.%f")
            return int(ts.timestamp() * 1000)
        except Exception:
            return 0


class S3MinIOSinkBronze(MapFunction):
    """MapFunction to persist data to MinIO using boto3 (already installed in the environment)"""

    def __init__(self):
        self.s3_client = None

    def open(self, runtime_context):
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
            print(f"Connected to MinIO at: {MINIO_ENDPOINT}")
        except Exception as e:
            print(f"Failed to create S3 client: {str(e)}")

    def save_record_to_minio(self, value, station_id, timestamp,  bucket_name, partition):
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
        filepath = f"{partition}/year={year}/month={month}/day={day}/{station_id}_{timestamp.replace('T', '_')}_{unique_id}.json"
        
        # Save the record to MinIO
        if self.s3_client:
            try:
                self.s3_client.put_object(
                    Bucket=bucket_name,
                    Key=filepath,
                    Body=value.encode('utf-8'),
                    ContentType="application/json"
                )
                print(f"Saved to {bucket_name} bucket: {filepath}")
            except Exception as e:
                print(f"Failed to save to {bucket_name} bucket: {filepath}: {e}")
        else:
            print("S3 client not initialized")

    def map(self, value):
        try:
            data = json.loads(value)
            station_id = data["station_id"]
            timestamp = data["timestamp"]
            self.save_record_to_minio(value, station_id, timestamp, bucket_name='bronze', partition='iot_raw')
           
        except Exception as e:
            print(f"Error while saving to MinIO: {str(e)}")

        # Pass through for downstream processing
        return value
    

class S3MinIOSinkSilver(S3MinIOSinkBronze):
    """MapFunction to persist data to MinIO using boto3 (already installed in the environment)"""

    def __init__(self):
        super().__init__

    def map(self, value):
        try:
            data = json.loads(value)
            if "detection_flags" in data.keys():
                station_id = data["station_id"]
                timestamp_epoch_millis = data["response_timestamp"]
                # Convert epoch milliseconds to datetime
                timestamp = datetime.fromtimestamp(timestamp_epoch_millis / 1000.0).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] 
                # Save record to MinIO
                self.save_record_to_minio(value, station_id, timestamp, bucket_name='silver', partition='iot_processed/anomalies')
            
            else:
                station_id = data["station_id"]
                timestamp_epoch_millis = data["response_timestamp"]
                # Convert epoch milliseconds to datetime
                timestamp = datetime.fromtimestamp(timestamp_epoch_millis / 1000.0).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]            
                # Save record to MinIO
                self.save_record_to_minio(value, station_id, timestamp, bucket_name='silver', partition='iot_processed/normal')

        except Exception as e:
                print(f"Error while saving to MinIO: {str(e)}")

        # Pass through for downstream processing
        return value


class ThresholdFilterWindowFunction(ProcessWindowFunction):
    def __init__(self, thresholds):
        self.thresholds = thresholds

    def process(self, key, context, elements):
        filtered_results = []
        raw_json_objects = []

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
                print(f"Error processing element: {str(e)}")

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
                'measurements' : mean_measurements_data
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
        status_message = {
            "status": "OK",
            "message": "No anomalies detected",
            "station_id": key,  # Use the window key (station_id)
            "response_timestamp": context.window().end  # Use window end time
        }
        # Return a list of field values that match Flink's expected Row structure
        status_json = json.dumps(status_message)
        return [status_json]


class EnrichFromRedis(MapFunction):
    def __init__(self, max_retries=5, initial_backoff=1.0, max_backoff=30.0):
        self.redis_client = None
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff

    def _default_metadata(self):
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

    def _wait_for_redis_data(self):
        """Wait until Redis has some station metadata populated."""
        wait_time = self.initial_backoff
        current_waited_time_interval = 0
        max_total_wait = 120 # Maximum 2 minutes total wait time

        for attempt in range(1, self.max_retries + 1):
            try:
                if self.redis_client.ping() and self.redis_client.keys("station:*"):
                    logger.info(f"[INFO] Redis is ready with data after {attempt} attempts")
                    return True
                else:
                    logger.warning(f"[WARN] Redis is running but no station data available yet. Attempt {attempt}/{self.max_retries}")
            except redis.exceptions.ConnectionError as e:
                logger.warning(f"[WARN] Redis connection failed on attempt {attempt}/{self.max_retries}: {e}")
            
            jitter = random.uniform(0, 0.1 * wait_time)
            sleep_time = min(wait_time + jitter, self.max_backoff)

            # Check if we would exceed our maximum allowed wait time
            if current_waited_time_interval + sleep_time > max_total_wait:
                logger.warning(f"[WARN] Maximum wait time of {max_total_wait}s exceeded. Proceeding with default metadata.")
                return False

            logger.info(f"[INFO] Waiting {sleep_time:.2f}s before retrying Redis connection...")
            time.sleep(sleep_time)
            current_waited_time_interval += sleep_time
            wait_time *= 2
        
        logger.error("[ERROR] Failed to connect to Redis with data after maximum retries")
        return False

    def open(self, runtime_context):
        try:
            self.redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                health_check_interval=30
            )
            logger.info("[INFO] Connected to Redis")

            if not self._wait_for_redis_data():
                logger.warning("[WARN] Proceeding without confirmed Redis data. Using default metadata as fallback.")
        except Exception as e:
            logger.error(f"[ERROR] Failed to connect to Redis: {e}")
            self.redis_client = None

    def map(self, value):
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
                    logger.warning(f"[WARN] No metadata found for {redis_key}, using default.")
                    data["station_metadata"] = self._default_metadata()
            else:
                logger.warning(f"[WARN] Redis client is None. Using default metadata for {redis_key}")
                data["station_metadata"] = self._default_metadata()
        except Exception as e:
            logger.error(f"[ERROR] Failed to enrich {redis_key}: {e}")
            data["station_metadata"] = self._default_metadata()

        return json.dumps(data)

    def close(self):
        if self.redis_client:
            self.redis_client.close()
            logger.info("[INFO] Redis connection closed")


def wait_for_minio_ready(endpoint, access_key, secret_key, max_retries=20, retry_interval=5):
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


def wait_for_kafka_ready(bootstrap_servers, max_retries=30, retry_interval=10):
    """Wait for Kafka to be ready before proceeding."""

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

    # Wait for minIO to be ready
    wait_for_minio_ready(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    # Wait for kafka server and configuration to be ready
    wait_for_kafka_ready(KAFKA_SERVERS)

    # Flink job configuration
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

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
    processd_stream = (
        ds.key_by(lambda x: json.loads(x)["station_id"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(ThresholdFilterWindowFunction(THRESHOLDS), output_type=Types.STRING())
        .map(EnrichFromRedis(), output_type=Types.STRING())
    )

    processd_stream.print()
    processd_stream.map(S3MinIOSinkSilver(), output_type=Types.STRING())

    env.execute("Flink Job")


if __name__ == "__main__":
    main()

