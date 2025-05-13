
# Utilities
from botocore.client import Config
from kafka import KafkaConsumer
import logging
import boto3
import time
import json


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


def create_consumer(bootstrap_servers: list[str]) -> KafkaConsumer:
    """
        Comment here!
    """
    consumer = KafkaConsumer(
        'sensor_meas',
        bootstrap_servers=bootstrap_servers,
        group_id='sens-consumer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True,
        auto_offset_reset='latest'
    )

    return consumer


def save_to_minio(msg_payload: dict, key: str, bucket_name:str, ContentType:str) -> None:
    """
        Comment here!
    """
    object_key = f"{key}.json"

    try:
        s3.head_bucket(Bucket=bucket_name)
    except s3.exceptions.ClientError:
        s3.create_bucket(Bucket=bucket_name)

    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(msg_payload),
            ContentType=ContentType
        )
        
    except Exception as e:
        logger.error(f"Failed to load payload {object_key} inside bucket {bucket_name} for: {e}")
        raise


def process_payload(msg_payload: list[dict]) -> None:
    """
        Comment here 
    """
    
    for record in msg_payload:
        try:
            meas_payload_id = f"{record['station_id']}_{record['timestamp']}"
            save_to_minio(
                msg_payload,
                key=meas_payload_id,
                bucket_name='sensor-stations-data-payload-backup',
                ContentType='application/json'
            )
            logger.info(f"Saved sensors payload: {meas_payload_id}")

        except KeyError:
            raise ValueError("Missing 'sens_meas_id' in payload.")
        except Exception as e:
            logger.error(f"Failed to process payload: {e}")
            raise


def stream_consumer() -> None:
    """
        Comment here!
    """
    print("\n[SENS-CONSUMER-STREAM-PROCESS\n]")

    # Initialize Kafka Producer with retry logic
    bootstrap_servers = ['kafka:9092']
    consumer = None

    while consumer is None:
        try:
            logger.info("Connecting to Kafka client to initialize consumer...")
            consumer = create_consumer(bootstrap_servers=bootstrap_servers)
            logger.info("Kafka consumer initialized successfully.\n")
        except Exception as e:
            logger.warning(f"Kafka connection failed: {e}. Retrying...\n")
            continue

    # Set up data stream parameters
    stream = True   # Stream until False

    while stream:
        try:
            for msg in consumer:
                """
                __iter__() method of the KafkaConsumer object
                This method internally calls the Kafka client’s poll() method in a loop, waiting for new messages.
                code inside the for loop won’t execute until a message is available.
                """
                try:
                    logger.info(f"Received msg from partition key: {msg.key}")
                    process_payload(msg.value)
                    logger.info("Message auto-commited.\n")
                except Exception as e:
                    logger.error(f"Process failed: {e} -> skipping commit\n")
            
        except Exception as e:
            logger.error(f"Consumer crashed (likely Kafka down): {e}. Retrying in 5s...\n")
            time.sleep(5)
            continue

