
# Utilities
from Utils.data_templates import SIGNAL_CATEGORIES, NOISE_CATEGORIES, TEMPLATES, SYNONYMS
from Utils.msg_utils import fetch_micro_bbox_from_db, GenerateMsg
from kafka.errors import KafkaError
from kafka import KafkaProducer
from datetime import datetime
import logging
import random
import json
import time


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: list[str]) -> KafkaProducer:
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


def on_send_success(record_metadata):
    """
    Callback for successful Kafka message delivery.
    
    Parameters:
    -----------
    record_metadata : kafka.producer.record_metadata.RecordMetadata
        Metadata containing topic name, partition, and offset of the delivered message.
    """
    logger.info(f"[KAFKA ASYNC] Message sent to topic '{record_metadata.topic}', "
                f"partition {record_metadata.partition}, offset {record_metadata.offset}")


def on_send_error(excp: KafkaError):
    """
    Callback for failed Kafka message delivery.

    Parameters:
    -----------
    excp : KafkaError
        The exception that occurred during message send.
    """
    logger.error(f"[KAFKA ERROR] Failed to send message: {excp}")


def stream_micro_msg(macroarea_i: int, microarea_i: int):
    """
    Streams synthetic social media messages for a specific macroarea and microarea.

    Parameters:
    - macroarea_i (int or str): Identifier for the macroarea.
    - microarea_i (int or str): Identifier for the microarea within the macroarea.

    This function generates and sends messages to Kafka at regular intervals using the producer.
    """
    print("\n[MSG-PRODUCER-STREAM-PROCESS]\n")
    
    # Initialize Kafka Producer with retry logic
    bootstrap_servers = ['kafka:9092']
    producer = None

    while producer is None:
        try:
            logger.info("Connecting to Kafka client to initialize producer...")
            producer = create_producer(bootstrap_servers=bootstrap_servers)
            logger.info("Kafka producer initialized successfully.")
        except Exception as e:
            logger.warning(f"Kafka connection failed: {e}. Retrying...")
            continue

    # Set up data stream parameters
    stream = True   # Stream until False

    print("\n")
    logger.info(f"Streaming data for microarea_A{macroarea_i}-M{microarea_i}...\n")

    while stream:
        try:
            min_long, min_lat, max_long, max_lat = fetch_micro_bbox_from_db(macroarea_i, microarea_i)[0]
            msg_lat = random.uniform(min_lat, max_lat)
            msg_long = random.uniform(min_long, max_long)
            try:
                msg_gen = GenerateMsg(
                    TEMPLATES,
                    NOISE_CATEGORIES,
                    SIGNAL_CATEGORIES,
                    SYNONYMS,
                    msg_lat,
                    msg_long,
                    macroarea_i,
                    microarea_i
                )
                message = msg_gen.generate()
                logger.info(f"Generated message in category at location ({msg_lat:.4f}, {msg_long:.4f})")

            except Exception as e:
                logger.error(f"GenerateMsg Class failed: {e}")

            # Topic selection
            topic = "social_msg"
            
            try:
                # Convert payload in str
                value = json.dumps(message)
                
                # Hashing Key to identify partition
                key = message["macroarea_id"].encode('utf8')
                
                # Asynchronous sending
                producer.send(topic, key=key, value=value).add_callback(on_send_success).add_errback(on_send_error)
                logger.info("Message sent successfully.")
            
            except KafkaError as e:
                logger.exception(f"[KAFKA ERROR] Failed to send message to Kafka after retries: {e}.")
            
            except Exception as e:
                logger.exception(f"[UNEXPECTED ERROR] An unknown error occurred while sending Kafka message: {e}")
            
            # Ensure the message is actually sent before continuing to the next iteration
            try:
                producer.flush()
                logger.info("Message flushed.\n")
            except Exception as e:
                logger.error(f"Failed to flush the message: {e}")
                continue
            
            time.sleep(1) # One message every second
    
        except Exception as e:
            logger.error(f"Failed to generate data for microarea_A{macroarea_i}-M{microarea_i}: {e}")
            continue
        
        


