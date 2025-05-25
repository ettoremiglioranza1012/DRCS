
"""
    Comment here!
"""


# Utilities
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.common import Time, Types

from typing import List, Dict, Any, Optional, Union
from data_templates import SIGNAL_CATEGORIES
from kafka.admin import KafkaAdminClient
from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import requests
import logging
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
ORIGINAL_KAFKA_TOPIC = "social_msg"
NLP_KAFKA_TOPIC = "nlp_social_msg"
KAFKA_SERVERS = "kafka:9092"

# MinIO configuration - read from environment variables if available
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# NLP microservice configuration
NLP_SERVICE_URL = os.environ.get("NLP_SERVICE_URL", "http://nlp_service:8000/classify")
NLP_MAX_RETRIES = 3
NLP_RETRY_DELAY = 1  # seconds


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
            filepath = f"{partition}/year={year}/month={month}/day={day}/{the_id}_{unique_id}.json"
            
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
            unique_msg_id = data["unique_msg_id"]
            timestamp = data["timestamp"]
            self.save_record_to_minio(value, unique_msg_id, timestamp, bucket_name='bronze', partition='social_msg_raw')
           
        except Exception as e:
            logger.error(f"Error while saving to bronze layer: {str(e)}")

        # Pass through for downstream processing
        return value


class S3MinIOSinkGold(S3MinIOSinkBase):
    """
    MinIO sink for processed (gold) data layer.
    
    Persists classified and filtered social messages to the gold data layer in MinIO,
    organizing them by their category for easier dashboard consumption.
    """

    def map(self, value: str) -> str:
        """
        Save filtered data to the gold layer in MinIO, organized by category.
        
        Args:
            value: JSON string containing filtered and classified data
            
        Returns:
            str: Original value (passed through for downstream processing)
        """
        try:
            data = json.loads(value)
            unique_msg_id = data["unique_msg_id"]
            label = data["category"]
            timestamp = data["timestamp"]
            self.save_record_to_minio(value, unique_msg_id, timestamp, bucket_name='gold', partition=f'filtered_social_msg/{label}')
        
        except Exception as e:
            logger.error(f"Error while saving to gold layer: {str(e)}")


class SendToNLPProcessWindowFunction(ProcessWindowFunction):
    """
    Processes windowed social media messages by sending them to NLP microservice for classification.
    
    This window function aggregates messages over a time window, then sends the batch to an
    external NLP microservice that performs classification. The time batch is used to avoid
    saturating the http protocol with too many requests per minute. It includes retry logic 
    and error handling to ensure resilience.
    """

    def process(self, key: str, context: Any, elements: List[str]) -> List[str]:
        """
        Process a window of elements by sending them to NLP service for classification.
        
        Args:
            key: The key for the window (microarea_id)
            context: The window context
            elements: List of JSON strings containing social media messages
            
        Returns:
            List[str]: A single list containing a JSON string with classified messages
        """
        # Parse JSON elements to create request payload
        try:
            json_elements = [json.loads(element) for element in elements]
            
            # Implement retry logic
            for attempt in range(NLP_MAX_RETRIES):
                try:
                    print(f"Sending batch of {len(json_elements)} messages to NLP service (attempt {attempt+1})")
                    response = requests.post(
                        NLP_SERVICE_URL,
                        json=json_elements,
                        timeout=10  # 10 seconds timeout
                    )
                    
                    if response.status_code == 200:
                        records = response.json()
                        print(f"Successfully processed {len(records)} messages from NLP service")
                        return [json.dumps(records)]
                    else:
                        print(f"NLP service returned status code {response.status_code}. Response: {response.text}")
                        if attempt < NLP_MAX_RETRIES - 1:
                            time.sleep(NLP_RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                        
                except requests.exceptions.RequestException as e:
                    print(f"Request to NLP service failed: {e}")
                    if attempt < NLP_MAX_RETRIES - 1:
                        time.sleep(NLP_RETRY_DELAY * (2 ** attempt))  # Exponential backoff
            
            # If we reach here, all retries failed
            print(f"All attempts to connect to NLP service failed after {NLP_MAX_RETRIES} retries, returning empty JSON array as string")
            return ["[]"]  # Return empty JSON array as string
            
        except Exception as e:
            print(f"Failed to process window for NLP classification: {e}, returning empty JSON array as string")
            return ["[]"]  # Return empty JSON array as string


class SinkToKafkaTopic(MapFunction):
    """
    Sinks processed messages to a Kafka topic.
    
    This class receives a batch of classified messages from the NLP service and publishes
    each message individually to a Kafka topic for further processing.
    """
    
    def __init__(self):
        """Initialize the Kafka sink."""
        self.producer = None
        self.bootstrap_servers = ['kafka:9092']

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
        Receives a JSON string containing a list of records and sends each record to Kafka.
        
        Parameters:
        -----------
        values : str
            A JSON string representing a list of dictionaries/records
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
            # Parse the JSON string back into a Python list
            records = json.loads(values)

            # Asynchronous sending
            for record in records:
                try:
                    value = json.dumps(record)
                    key = record.get("macroarea_id", "UNKNOWN").encode("utf-8")
                    topic = NLP_KAFKA_TOPIC

                    self.producer.send(
                        topic, 
                        key=key,
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


class FilterMapFunction(MapFunction):
    """
    Filters social media messages based on their classification category.
    
    This function checks if a message belongs to a predefined list of important
    signal categories and passes through only those messages for further processing.
    """
    
    def __init__(self, signals: List[str]) -> None:
        """
        Initialize the filter with a list of signal categories to keep.
        
        Args:
            signals: List of category labels that should be considered signals
        """
        self.signals = signals
    
    def map(self, value: str) -> Optional[str]:
        """
        Filter messages based on their category classification.
        
        Args:
            value: JSON string containing a classified social media message
            
        Returns:
            str: The original message if it belongs to a signal category, None otherwise
        """
        try:
            data = json.loads(value)
            
            # Filter messages from noise
            if data.get("category", "UNKNOWN") in self.signals:
                return value  # Pass through messages in signal categories
            
            # Return None for messages not in signal categories
            return None
            
        except Exception as e:
            logger.error(f"Error in filter map function: {e}")
            return None  # Skip problematic messages


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
    env.enable_checkpointing(5000)  # Every 5 seconds

    """
        Since at this point resourcers use starts to be an issue,
        we use the same flink job instead of building nother one,
        to process also the nlp topic.
        --------------------------------------------------------
        Offset management: Using 'auto.offset.reset': 'earliest' 
        
        Since social media msgs may contain important help requests,
        we enable flink to start reading from the earliest offset
        during first start, to avoid loosing potential important
        msgs during an eventual 'cold start'. Later, we make 
        sure to don't lose anything with the proper checkpointing.
    """

    # Kafka consumer configuration
    original_properties = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'msg_flink_consumer_group',
        'auto.offset.reset': 'earliest',
        'request.timeout.ms': '60000',  # longer timeout
        'retry.backoff.ms': '5000',     # backoff between retries
        'reconnect.backoff.ms': '5000', # backoff for reconnections
        'reconnect.backoff.max.ms': '30000', # max backoff
    }

    # Establish original Kafka consumer 
    original_kafka_consumer = FlinkKafkaConsumer(
        topics=ORIGINAL_KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=original_properties
    )

    # Define watermark strategy
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(2)) \
        .with_timestamp_assigner(JsonTimestampAssigner())

    # Source 
    original_stream = env.add_source(original_kafka_consumer, type_info=Types.STRING()).assign_timestamps_and_watermarks(watermark_strategy)

    # Save each record to MinIO
    original_stream.map(S3MinIOSinkBronze(), output_type=Types.STRING())

    # Apply windowing logic, process by location in parallel and aggregate by tumbling window of 5 seconds
    processed_stream = (
        original_stream
        .key_by(lambda x: json.loads(x).get("microarea_id", "UNKNOWN"), key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .process(SendToNLPProcessWindowFunction(), output_type=Types.STRING())
    )

    # Sink aggregated result to kafka topic 'nlp_processed'
    processed_stream.map(SinkToKafkaTopic())

    nlp_properties = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'nlp_flink_consumer_group',
        'auto.offset.reset': 'earliest',
        'request.timeout.ms': '60000',  # longer timeout
        'retry.backoff.ms': '5000',     # backoff between retries
        'reconnect.backoff.ms': '5000', # backoff for reconnections
        'reconnect.backoff.max.ms': '30000', # max backoff
    }

    # Establish nlp Kafka consumer
    nlp_kafka_consumer = FlinkKafkaConsumer(
        topics=NLP_KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=nlp_properties
    )

    # Source, with out watermarks strategy since here we process event one by one
    nlp_stream = env.add_source(nlp_kafka_consumer, type_info=Types.STRING())

    # Filter messages by category and save important ones to gold layer
    filtered_stream = (
        nlp_stream
        .key_by(lambda x: json.loads(x).get("microarea_id", "UNKNOWN"), key_type=Types.STRING())
        .map(FilterMapFunction(SIGNAL_CATEGORIES), output_type=Types.STRING())
        .filter(lambda x: x is not None)  # Remove None values from FilterMapFunction
    )

    # Save filtered messages to Gold layer
    filtered_stream.map(S3MinIOSinkGold())

    # Execute the job
    logger.info("Executing Flink job")
    env.execute("Social Media Processing Pipeline")


if __name__ == "__main__":
    main()

