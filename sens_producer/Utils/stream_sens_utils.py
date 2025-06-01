
# Utilities
from Utils.geo_sens_utils import *
from kafka.errors import KafkaError
from kafka import KafkaProducer
from datetime import datetime
import logging
import time
import json


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


def stream_micro_sens(macroarea_i: int, microarea_i:int) -> None:
    """
    Continuously simulates and streams fake IoT sensor measurements for a given microarea to a Kafka topic.

    Steps:
    1. Initializes a Kafka producer with retry logic.
    2. Repeatedly fetches the number of stations for the specified microarea.
    3. Generates and validates fake measurements for each station.
    4. Serializes the data into JSON and sends it asynchronously to a Kafka topic.
    5. Flushes the Kafka producer buffer and sleeps before the next iteration.

    Parameters:
        macroarea_i (int): The macroarea index (e.g., region identifier).
        microarea_i (int): The microarea index (e.g., sub-region or cluster of sensors).

    Returns:
        None
    """
    print("\n[SENS-PRODUCER-STREAM-PROCESS]\n")
    
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
            logger.info("Fetching microarea sensor stations information...")
            n_stats = get_number_of_stats(macroarea_i, microarea_i)
            if not isinstance(n_stats, int):
                logger.error("'n_stats' must be an integer.")
                continue
            logger.info("Fetching completed successfully.")

            if not n_stats:
                logger.error(f"Number of stations inconsistent: {n_stats}, check data integrity.")
                continue

            # Generate fake measurements for i-th station in microarea
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            records = list()
            
            logger.info(f"Fetching measurements for each station in microarea: 'A{macroarea_i}-M{microarea_i}'")
            for i in range(n_stats):
                temp_mes = generate_measurements_json(i+1, microarea_i, macroarea_i, timestamp)
                if not temp_mes:
                    logger.error(f"Measurements for 'S_A{macroarea_i}-M{microarea_i}_{i:03}' not consistent, check 'generate_measurements_json()' function.")
                    continue
                records.append(temp_mes)
            
            if not records:
                logger.error("Message not consistent or too few stations, check data integrity.")
                continue
            logger.info("All tests passed. Message data OK -> ready to send to Kafka.")

            # Queues the message in memory (producer buffer)
            topic = f"sensor_meas"  # Custom topic per area
            logger.info("Sending IoT sensor data to Kafka asynchronously...")

            try:
                # Create str area id
                macroarea_id = f"A{macroarea_i}"

                # Asynchronous sending
                for record in records:
                    value = json.dumps(record)
                    producer.send(topic, value=value).add_callback(on_send_success).add_errback(on_send_error)
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

            time.sleep(5)  # One message every 5 s

        except Exception as e:
            logger.error(f"Unhandled error during streaming procedure: {e}")
            continue

        