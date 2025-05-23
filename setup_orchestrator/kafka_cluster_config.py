
# Utilities
from kafka.admin import KafkaAdminClient, NewTopic
import os


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
        NewTopic(name="sensor_meas", num_partitions=n_macros, replication_factor=1),
        NewTopic(name="social_msg", num_partitions=n_macros, replication_factor=1),
        NewTopic(name="nlp_social_msg", num_partitions=n_macros, replication_factor=1),
    ]

    try:
        admin.create_topics(new_topics=topics, validate_only=False, timeout_ms=120000)
        print("[INFO] Successfully created satellite_img, sensor_meas, social_msg  and nlp_social_msg topics.")

    except Exception as e:
        print(f"[ERROR] Topics not created: {e}")


if __name__ == "__main__":
    kafka_cluster_config()

