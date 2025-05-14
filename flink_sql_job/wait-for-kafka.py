
# Utilities
import time
import subprocess
from kafka import KafkaAdminClient
from typing import List

KAFKA_BROKER = "kafka:9092"
REQUIRED_TOPICS = ["sensor_meas", "satellite_img"]
RETRY_INTERVAL = 5


def topics_exist(admin: KafkaAdminClient, required_topics: List[str]) -> bool:
    """
    Check whether all required Kafka topics exist.

    Args:
        admin (KafkaAdminClient): Kafka admin client connected to the broker.
        required_topics (List[str]): List of topic names to check.

    Returns:
        bool: True if all required topics exist in the Kafka cluster, False otherwise.
    """
    try:
        existing_topics = admin.list_topics()
        return all(topic in existing_topics for topic in required_topics)
    except Exception as e:
        print(f"[ERROR] Kafka not reachable yet: {e}")
        return False


def main() -> None:
    """
    Main entry point of the script.
    Continuously checks if all required Kafka topics exist.
    Once available, launches a Flink SQL job using the embedded SQL client.
    """
    print(f"Waiting for Kafka topics to exist: {REQUIRED_TOPICS}")

    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, client_id="flink-waiter")
            if topics_exist(admin, REQUIRED_TOPICS):
                print("[INFO] All topics exist. Launching Flink SQL job...")
                break
            else:
                print("[WARNING] Topics not ready. Retrying...")
        except Exception as e:
            print(f"[ERROR] Kafka connection error: {e}")
        time.sleep(RETRY_INTERVAL)

    subprocess.run(["bin/sql-client.sh", "embedded", "-f", "/opt/flink/scripts/job.sql"])


if __name__ == "__main__":
    main()


