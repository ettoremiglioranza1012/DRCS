
# Utilities
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Time, Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction

from datetime import datetime
import random
import boto3
import json
import uuid
import os


KAFKA_TOPIC = "sensor_meas"
KAFKA_SERVERS = "kafka:9092"

# MinIO configuration - read from environment variables if available
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
MINIO_BUCKET = "bronze"  # The pre-created bronze bucket

# Treshold values
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
                "severity_score": round(random.uniform(0.6, 1.0), 2),
                "detection_confidence": round(random.uniform(0.75, 0.99), 2),
                "air_quality_index": round(random.uniform(150, 300), 1),
                "anomaly_detected": True,
                "anomaly_type": "wildfire"
            }

            system_response = {
                "event_triggered": "wildfire_alert",
                "action_taken": "activated_coordination_and_recovery_system",
                "automated": True
            }

            mean_measurements['detection_flags'] = detection_flags
            mean_measurements['system_response'] = system_response

            # Return a list of field values that match Flink's expected Row structure
            result_json = json.dumps(mean_measurements)
            return [result_json]
        
        # Nothing exceeded: return status in same format
        status_message = {
            "status": "OK",
            "message": "No anomalies detected",
            "station_id": key,  # Use the window key (station_id)
            "timestamp": context.window().end  # Use window end time
        }
        # Return a list of field values that match Flink's expected Row structure
        status_json = json.dumps(status_message)
        return [status_json]


class S3MinIOSink(MapFunction):
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

    def map(self, value):
        try:
            data = json.loads(value)
            station_id = data["station_id"]
            timestamp = data["timestamp"].replace(":", "-")

            # Extract date for partitioned path
            year_month_day = timestamp.split("T")[0]  # YYYY-MM-DD
            year = year_month_day.split("-")[0]
            month = year_month_day.split("-")[1]
            day = year_month_day.split("-")[2]

            # Create a unique file ID
            unique_id = uuid.uuid4().hex[:8]

            # Build the file path
            filepath = f"iot_raw/year={year}/month={month}/day={day}/{station_id}_{timestamp.replace('T', '_')}_{unique_id}.json"

            # Save the record to MinIO
            if self.s3_client:
                try:
                    self.s3_client.put_object(
                        Bucket=MINIO_BUCKET,
                        Key=filepath,
                        Body=value.encode('utf-8'),
                        ContentType="application/json"
                    )
                    print(f"Saved to bronze bucket: {filepath}")
                except Exception as e:
                    print(f"Failed to save to bronze bucket: {filepath}")
            else:
                print("S3 client not initialized")
        except Exception as e:
            print(f"Error while saving to MinIO: {str(e)}")

        # Pass through for downstream processing
        return value


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

# Kafka consumer configuration
properties = {
    'bootstrap.servers': KAFKA_SERVERS,
    'group.id': 'flink_consumer_group'
}

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
ds.map(S3MinIOSink(), output_type=Types.STRING())

# Apply windowing logic and print aggregated result
(
    ds.key_by(lambda x: json.loads(x)["station_id"], key_type=Types.STRING())
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .process(ThresholdFilterWindowFunction(THRESHOLDS), output_type=Types.STRING())
      .print()
)

env.execute("Windowed Kafka Stream with S3 MinIO Sink")

