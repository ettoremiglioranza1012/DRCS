
# Utilities
from kafka import KafkaConsumer
from typing import Tuple
import streamlit as st
import threading
import queue
import json
import time


class KafkaConsumerManager:
    """Manages Kafka consumers and their associated background threads for streaming social, IoT, and satellite data.

    This class sets up consumers for each data stream using the Kafka Python client,
    processes incoming messages in real time, and stores them in bounded queues for later use by the dashboard.
    """

    def __init__(self):
        """Initialize Kafka consumer manager with:
        - predefined topic mappings (social, IoT, satellite)
        - a fixed consumer group ID
        - independent FIFO queues for each data type (with max size)
        - empty containers for consumers and worker threads
        """
        self.bootstrap_servers = "kafka:9092"
        self.group_id = "dashboard-consumer"
        self.topics = {
            "social": "gold_social",
            "iot": "gold_iot",
            "satellite": "gold_sat"
        }
        self.queues = {
            "social": queue.Queue(maxsize=1000),
            "iot": queue.Queue(maxsize=1000),
            "satellite": queue.Queue(maxsize=1000)
        }
        self.consumers = {}
        self.threads = {}

    def start(self) -> None:
        """Initialize Kafka consumers for each topic and launch a background thread for polling.

        For each data source:
        - Create a KafkaConsumer with JSON deserialization
        - Launch a daemon thread that continuously reads from the topic
        - Store both the consumer and its thread for management
        """
        for key, topic in self.topics.items():
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                    auto_offset_reset="latest",
                    group_id=self.group_id
                )
                self.consumers[key] = consumer

                thread = threading.Thread(
                    target=self._consumer_loop,
                    args=(key,),
                    daemon=True,
                    name=f"{key.capitalize()}ConsumerThread"
                )
                thread.start()
                self.threads[key] = thread

                print(f"[Kafka] Started thread for '{key}' on topic '{topic}'")
            except Exception as e:
                st.error(f"Error creating consumer for {key}: {e}")
                raise

    def _consumer_loop(self, key: str) -> None:
        """Run the main Kafka polling loop for a given data stream (social, IoT, or satellite).

        Continuously polls messages from Kafka, and pushes them into the corresponding queue.
        If the queue is full, it evicts the oldest item before inserting a new one to ensure freshness.

        This function runs in a dedicated daemon thread per stream.
        """
        consumer = self.consumers[key]
        message_queue = self.queues[key]

        while True:
            try:
                records = consumer.poll(timeout_ms=1000)
                for messages in records.values():
                    for msg in messages:
                        try:
                            message_queue.put(msg.value, block=False)
                        except queue.Full:
                            try:
                                message_queue.get_nowait()
                                message_queue.put(msg.value, block=False) 
                            except queue.Empty:
                                pass
            except Exception as e:
                print(f"Error in {key} consumer: {e}")
                time.sleep(1)

    def get_queues(self) -> Tuple[queue.Queue, queue.Queue, queue.Queue]:
        """Return the message queues for social, IoT, and satellite streams.

        These queues are used by the Streamlit app to access the latest data.
        """
        return self.queues["social"], self.queues["iot"], self.queues["satellite"]


@st.cache_resource
def initialize_kafka_system() -> Tuple[queue.Queue, queue.Queue, queue.Queue]:
    """Initialize the Kafka consumer manager and return the data queues.

    This function is decorated with `st.cache_resource` to ensure that the Kafka setup
    is performed only once during the session lifecycle.
    """
    manager = KafkaConsumerManager()
    manager.start()
    return manager.get_queues()

 