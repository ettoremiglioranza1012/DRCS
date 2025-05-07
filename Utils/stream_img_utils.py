
# Utilities
from kafka.errors import KafkaError
from sentinelhub import SHConfig
from kafka import KafkaProducer
import logging
import time

from Utils.imgfetch_utils import (
    get_aoi_bbox_and_size,
    true_color_image_request_processing,
    process_image,
    fetch_micro_bbox_from_db
)


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: list[str]) -> KafkaProducer:
    """
    Developer Notes: Message Durability and Design Choices
    -------------------------------------------------------

    Our disaster recovery system requires reliable, time-synchronized delivery of large satellite images.
    To guarantee that **no image is lost or silently dropped**, we adopt the following Kafka producer strategies:

    1. Why In-Sync Replicas (ISR) Matter:
    - Each Kafka partition can have multiple replicas: 1 leader + N followers.
    - Only followers that are fully up-to-date with the leader are considered "in-sync" (ISR).
    - We want to ensure that **messages are not just written to the leader**, but also **replicated to at least one other broker**.
    - In production, this ensures that **if a broker crashes, no data is lost**.

    2. Why Synchronous Sends (`future.get()`):
    - Kafka sends are asynchronous by default, which is high-performance but risky in critical systems.
    - We use `.get(timeout=...)` to block the producer until Kafka confirms the message was received and written.
    - This gives **immediate failure feedback** if something goes wrong (e.g., not enough ISRs, network issue).
    - It ensures we don’t move on until we’re sure the image is safely persisted.

    3. Why `acks='all'`:
    - This setting waits for **all in-sync replicas** to confirm the write.
    - It provides the strongest delivery guarantee Kafka supports.
    - In production (with replication.factor=3+), this ensures that data is acknowledged by at least 2+ brokers (given min.insync.replicas=2+).
    - In development (single broker), it behaves like `acks='1'` but prepares the system for stronger consistency when scaled.

    4. Why `retries=5`:
    - Transient errors like leader reelection, broker unavailability, or network glitches may cause temporary send failures.
    - By configuring `retries=5`, the producer will automatically attempt to resend a failed message up to 5 times **before giving up**.
    - This significantly improves robustness under load or during broker transitions, without requiring manual retry logic.
    - Retried sends still respect `acks='all'` and ISR guarantees — meaning no compromise on consistency.

    Kafka Producer Configuration Notes
    ----------------------------------

    This code uses `acks='all'` to ensure strong delivery guarantees.
    While prototyping on a single broker, this setup is safe and valid.

    Current Development Setup:
    - Brokers: 1
    - replication.factor = 1
    - min.insync.replicas = 1 (default)
    - acks = 'all' behaves like acks = '1' (only the leader exists)
    - Synchronous send (`future.get()`) ensures message delivery or raises exception

    Future Production Setup (Scalable):
    - Brokers: 3+
    - replication.factor = 3+
    - min.insync.replicas = 2+
    - acks = 'all' ensures write is replicated to at least 2+ brokers
    - Protects against data loss if one broker crashes
    - No code changes needed (at least not here) — same producer config becomes stronger automatically

    Note:
    Avoid overriding `min.insync.replicas` in development if using default broker configs.
    Just document the intent and apply topic-level configs at deployment time.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks='all',
        retries=5,
        max_request_size=2000000,   # Some imgs have more than default 1 MB (1048576 bytes) -> override the default;
        value_serializer=lambda v: v.encode('utf8')
    )

    return producer


def send_message_to_kafka(message_payload: str, topic: str, producer: KafkaProducer, microarea_id: str) -> None:
    """
    Sends a message to the given Kafka topic and waits for confirmation.

    Assumes:
    - message_payload is a JSON string.
    - The KafkaProducer is configured with a value_serializer that encodes the string to bytes.
    - acks='all' and retries are already set on the producer.
    - Hash microarea_id retrived from database to identify partition. 

    Includes:
    - Synchronous blocking send with .get(timeout=15)
    - Logs success or failure with partition and offset info
    """
    try:
        key = microarea_id.encode('utf8')
        future = producer.send(topic, key=key, value=message_payload)
        record_metadata = future.get(timeout=15) # timeout in s;
        logger.info(f"[KAFKA] Message sent to topic '{record_metadata.topic}', "
                    f"partition {record_metadata.partition}, offset {record_metadata.offset}")
    
    except KafkaError as e:
        logger.exception("[KAFKA ERROR] Failed to send message to Kafka after retries.")
        # fallback mechanism?
    
    except Exception as e:
        logger.exception(f"[UNEXPECTED ERROR] An unknown error occurred while sending Kafka message: {e}")


def stream_macro_imgs(macroarea_i:int) -> None:
    """
    Streams satellite image data for a given macroarea and publishes it to a Kafka topic.

    Context:
    --------
    This function is part of a disaster recovery and coordination system designed to
    collect, process, and transmit high-resolution satellite imagery in near real-time.
    Each macroarea consists of several microareas for which we regularly acquire satellite
    images. These are compressed, serialized, and sent to a Kafka topic for downstream
    processing, alerting, or dashboard visualization.

    Function Workflow:
    ------------------
    1. Initializes Sentinel Hub configuration and a Kafka producer with strong delivery guarantees.
    2. Fetches the coordinates (bounding box) of a ***random*** microarea from the given macroarea.
    3. Converts the bounding box into sentinelHub objects suitable for Sentinel Hub API.
    4. Submits a request to Sentinel Hub to fetch the latest true-color image data.
    5. Applies processing to simulate a disaster overlay (e.g., fire).
    6. Compresses and serializes the image into a Base64-encoded JSON payload.
    7. Sends the serialized image to the appropriate Kafka topic partitioned by microarea ID (hashed key is db's table primary key).
    8. Repeats the process every X (e.g. 60 s) seconds to simulate a continuous image stream.

    Parameters:
    -----------
    macroarea_i : int
        The numeric ID of the macroarea (e.g., 1 to 5) used to build topic names and fetch spatial data.

    Notes:
    ------
    - Kafka delivery is synchronous and blocking (via `future.get()`), ensuring guaranteed write acknowledgment.
    - The topic is expected to be named like 'satellite_imgs_A{macroarea_i}' and pre-created with partitions.
    - The loop will wait for the remainder of a X-second interval to maintain consistent image intervals.
    - This function is designed to run indefinitely unless an error or empty image response occurs.
    """
    print("\n[STREAM-PROCESS]\n")

    # Initialize SentinelHub Client
    config = SHConfig()
    
    # Initialize Kafka Producer
    bootstrap_servers = ['localhost:29092']
    logger.info("Connecting to Kafka client to initialize producer...")
    producer = create_producer(bootstrap_servers=bootstrap_servers)

    # Set up data stream parameters
    stream = True               # Stream until False
    iteration_time = None       # Default Iteration time
    start_time = "2024-05-01"   # Date interval
    end_time = "2024-05-20"     # Date interval

    print("\n")
    logger.info(f"Streaming data for macroarea_A{macroarea_i}...\n")

    while stream:
        try:
            t_macro_start = time.perf_counter()

            logger.info("Fetching bounding box from DB...")
            microarea_example_bbox, microarea_example_id = fetch_micro_bbox_from_db(macroarea_i)
            if microarea_example_bbox is None:
                logger.error(f"No bounding box found for macroarea {macroarea_i}, skipping.")
                break

            curr_aoi_coords_wgs84 = list(microarea_example_bbox)

            logger.info("Converting BBox and calculating image size...")
            t_bbox = time.perf_counter()
            curr_aoi_bbox, curr_aoi_size = get_aoi_bbox_and_size(
                curr_aoi_coords_wgs84, resolution=10)
            logger.info("BBox ready in %.2f s", time.perf_counter() - t_bbox)

            logger.info("Building SentinelHub request...")
            t_req = time.perf_counter()
            request_true_color = true_color_image_request_processing(
                curr_aoi_bbox, curr_aoi_size, config, start_time, end_time)
            logger.info("Request created in %.2f s", time.perf_counter() - t_req)

            logger.info("⬇Downloading image data from SentinelHub...")
            t_dl = time.perf_counter()
            true_color_imgs = request_true_color.get_data()
            logger.info("Data fetched in %.2f s", time.perf_counter() - t_dl)

            if not true_color_imgs:
                logger.warning("No image data returned. Skipping.")
                continue

            logger.info("Image np.ndarray shape: %s", true_color_imgs[0].shape)
            logger.info("Processing image...\n")
            t_proc = time.perf_counter()
            img_payload_str = process_image(true_color_imgs)
            logger.info("Image processed in %.2f s", time.perf_counter() - t_proc)

            topic = f"satellite_imgs_A{macroarea_i}"
            logger.info("Sending payload to Kafka...")
            t_send = time.perf_counter()
            send_message_to_kafka(img_payload_str, topic, producer, microarea_example_id)
            logger.info("Sending procedure closed in %.2f s\n", time.perf_counter() - t_send)

            iteration_time = time.perf_counter() - t_macro_start
            logger.info("Total macroarea cycle time: %.2f s", iteration_time)

        except Exception as e:
            logger.error("Error occurred during streaming cycle: %s", str(e))
            logger.warning("Skip to next fetch...")
            continue
        
        finally:
            min_cycle_duration = 60  # seconds
            if iteration_time is None:
                elapsed = time.perf_counter() - t_macro_start
            else:
                elapsed = iteration_time
            
            remaining_time = max(0, min_cycle_duration - elapsed)
            logger.info("Waiting %.2f s before next image fetching...\n\n", remaining_time)
            if remaining_time > 0:
                time.sleep(remaining_time)

