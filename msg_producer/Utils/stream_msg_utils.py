
# Utilities
from Utils.data_templates import SIGNAL_CATEGORIES, NOISE_CATEGORIES, TEMPLATES, SYNONYMS
from Utils.imgfetch_utils import fetch_micro_bbox_from_db
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
    Initializes and returns a KafkaProducer with strong delivery guarantees.

    Configuration:
    - `acks='all'`: Waits for acknowledgment from all in-sync replicas before confirming success.
    - `retries=5`: Automatically retries sending a message up to 5 times on transient failures.
    - `value_serializer`: Serializes the message value to UTF-8 encoded bytes (assumes input is a JSON string).

    Args:
        bootstrap_servers (list[str]): List of Kafka bootstrap server addresses (e.g., ['kafka:9092']).

    Returns:
        KafkaProducer: Configured Kafka producer instance.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks='all',
        retries=5,
        value_serializer=lambda v: json.dumps(v).encode('utf8')
    )
    return producer


def send_message_to_kafka(message_payload: dict, topic: str, producer: KafkaProducer, macroarea_id: str) -> None:
    """
    Sends a message to the given Kafka topic and waits for confirmation.

    Assumes:
    - message_payload is a dictionary (JSON-like).
    - The KafkaProducer is configured with a value_serializer that encodes the dictionary to bytes.
    - acks='all' and retries are already set on the producer.
    - Hash microarea_id retrieved from database to identify partition.

    Includes:
    - Synchronous blocking send with .get(timeout=15)
    - Logs success or failure with partition and offset info
    """
    try:
        key = macroarea_id.encode('utf8')
        future = producer.send(topic, key=key, value=message_payload)
        record_metadata = future.get(timeout=15)  # timeout in s;
        logger.info(f"[KAFKA] Message sent to topic '{record_metadata.topic}', "
                    f"partition {record_metadata.partition}, offset {record_metadata.offset}")
    
    except KafkaError as e:
        logger.exception("[KAFKA ERROR] Failed to send message to Kafka after retries.")
    
    except Exception as e:
        logger.exception(f"[UNEXPECTED ERROR] An unknown error occurred while sending Kafka message: {e}")

def select_category():
    return random.choices(
        SIGNAL_CATEGORIES + NOISE_CATEGORIES,
        weights=[70/len(SIGNAL_CATEGORIES)] * len(SIGNAL_CATEGORIES) +
                [30/len(NOISE_CATEGORIES)] * len(NOISE_CATEGORIES)
    )[0]

def fill_template(TEMPLATES):
    """
    Fills a message template with randomly selected synonyms.

    Parameters:
    - TEMPLATES (str): A message template string containing placeholders (e.g., "{disaster}", "{urgency}").

    Returns:
    - str: The template with all placeholders replaced by randomly selected synonyms from the SYNONYMS dictionary.
    """
    return TEMPLATES.format(**{key: random.choice(values) for key, values in SYNONYMS.items() if f"{{{key}}}" in TEMPLATES})

def generate_text(category: str) -> str:
    """
    Generates a synthetic message text for a given category by selecting a random template and filling it with synonyms.

    Parameters:
    - category (str): The category of the message (e.g., "help_request", "damage_report").

    Returns:
    - str: A fully formed message string, either filled using a template or a fallback placeholder if the category is unknown.
    """
    if category in TEMPLATES:
        template = random.choice(TEMPLATES[category])
        return fill_template(template)
    else:
        return f"This is a placeholder message for category '{category}'."

def stream_micro_msg(macroarea_i: int, microarea_i: int):
    """
    Streams synthetic social media messages for a specific macroarea and microarea.

    Parameters:
    - macroarea_i (int or str): Identifier for the macroarea.
    - microarea_i (int or str): Identifier for the microarea within the macroarea.

    This function generates and sends messages to Kafka at regular intervals using the producer.
    """
    producer = create_producer(['kafka:9092'])

    while True:
        category = select_category()
        text = generate_text(category)

        try:
            min_long, min_lat, max_long, max_lat = fetch_micro_bbox_from_db(macroarea_i, microarea_i)[0]
            lat = random.uniform(min_lat, max_lat)
            long = random.uniform(min_long, max_long)
            logger.info(f"Generated message in category '{category}' at location ({lat:.4f}, {long:.4f})")
        except Exception as e:
            logger.error(f"Failed to generate coordinates for macro {macroarea_i}, micro {microarea_i}: {e}")
            continue
        
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        macroarea_id = f"A{macroarea_i}"
        microarea_id = f"M{microarea_i}"
        unique_msg_id = f"{macroarea_id}-{microarea_id}_{timestamp.replace('T', '_')}"

        message = {
            "unique_msg_id": unique_msg_id,
            "macroarea_id": macroarea_id,
            "microarea_id": microarea_id,
            "text": text,
            "latitude": lat,
            "longitude": long,
            "timestamp": timestamp
        }

        key = macroarea_id.encode('utf8')
        topic = "social_msg"

        try:
            future = producer.send(topic, key=key, value=message)
            record_metadata = future.get(timeout=15)
            logger.info(f"[KAFKA] Message sent to topic '{record_metadata.topic}', "
                        f"partition {record_metadata.partition}, offset {record_metadata.offset}")
        except KafkaError as e:
            logger.exception("[KAFKA ERROR] Failed to send message to Kafka after retries.")
        except Exception as e:
            logger.exception(f"[UNEXPECTED ERROR] An unknown error occurred while sending Kafka message: {e}")

        time.sleep(1)

