
"""
Kafka Consumer Manager for Streamlit Dashboard
"""

# Utilities
from kafka import KafkaConsumer
from typing import Tuple
import streamlit as st
import threading
import queue
import json
import time


class KafkaConsumerManager:
    """Manages Kafka consumers and threads for social, IoT, and satellite data"""

    def __init__(self):
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

    def start(self):
        """Initialize and start all consumers in background threads"""
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

    def _consumer_loop(self, key: str):
        """Threaded loop to poll and store Kafka messages"""
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
        return self.queues["social"], self.queues["iot"], self.queues["satellite"]


@st.cache_resource
def initialize_kafka_system() -> Tuple[queue.Queue, queue.Queue, queue.Queue]:
    """Initialize Kafka manager and return queues"""
    manager = KafkaConsumerManager()
    manager.start()
    return manager.get_queues()

 