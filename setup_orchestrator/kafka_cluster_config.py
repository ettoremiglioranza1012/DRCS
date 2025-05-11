
# Utilities
import os
from kafka.admin import KafkaAdminClient, NewTopic


def verify_topics(admin: KafkaAdminClient, expected_partitions: dict) -> None:
    print("[INFO] Verifying that all topics exist and have the correct number of partitions...")
    try:
        existing_topics = set(admin.list_topics())
        all_ok = True

        for topic, expected_count in expected_partitions.items():
            if topic not in existing_topics:
                print(f"[ERROR] Topic '{topic}' does not exist.")
                all_ok = False
                continue

            topic_metadata = admin.describe_topics([topic])[0]
            actual_count = len(topic_metadata['partitions'])

            if actual_count != expected_count:
                print(f"[ERROR] Topic '{topic}': expected {expected_count} partitions, but got {actual_count}.")
                all_ok = False
            else:
                print(f"[INFO] Topic '{topic}' has the correct number of partitions ({actual_count}).")

        if all_ok:
            print("[INFO] All topics verified successfully.")
        else:
            print("[WARNING] One or more topics are missing or have incorrect partition counts.")

    except Exception as e:
        print("[ERROR] Failed to verify topics:", e)
        raise


def kafka_cluster_config() -> None:
    # Count macroareas from files in Macro_data/Macro_input
    macro_dir = "Macro_data/Macro_input"
    n_macros = len([f for f in os.listdir(macro_dir) if os.path.isfile(os.path.join(macro_dir, f))])

    # Initialize Kafka admin client
    admin = KafkaAdminClient(
        bootstrap_servers="kafka:9092",
        client_id="setup-script"
    )

    # Define only two topics
    topics = [
        NewTopic(name="satellite_img", num_partitions=n_macros, replication_factor=1),
        NewTopic(name="sensor_meas", num_partitions=n_macros, replication_factor=1)
    ]

    try:
        admin.create_topics(new_topics=topics, validate_only=False, timeout_ms=120000)
        print("[INFO] Successfully created satellite_img and sensor_meas topics.")

        expected_partitions = {topic.name: topic.num_partitions for topic in topics}
        verify_topics(admin, expected_partitions)

    except Exception as e:
        print(f"[ERROR] Topics not created: {e}")


if __name__ == "__main__":
    kafka_cluster_config()


