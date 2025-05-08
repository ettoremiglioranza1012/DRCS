
# Utilities
from kafka.admin import KafkaAdminClient, NewTopic
from Utils.db_utils import connect_to_db
from psycopg2 import sql, OperationalError


def get_partitions(i: int) -> int:
    """
    Fetches the number of microareas for macroarea A{i} from the DB.
    Raises:
        ValueError: if the macroarea has no entry or null value.
    """
    conn, cur = None, None
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        macroarea_id = f"A{i}"
        query_execute = sql.SQL("""
            SELECT numof_microareas
            FROM macroareas
            WHERE macroarea_id = {}
        """).format(sql.Literal(macroarea_id))

        cur.execute(query_execute)
        result = cur.fetchone()

        if result is None or result[0] is None:
            raise ValueError(f"[ERROR] No data or NULL for macroarea_id = '{macroarea_id}' in table 'n_microareas'.")

        return int(result[0])

    except OperationalError as e:
        print("[ERROR] Database connection error:", e)
        raise
    except Exception as ex:
        print("[ERROR] Error while executing query:", ex)
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def verify_topics(admin: KafkaAdminClient, expected_partitions: dict) -> None:
    """
    Verifies that each Kafka topic has the expected number of partitions and checks their existence.

    Args:
        admin (KafkaAdminClient): Initialized KafkaAdminClient instance.
        expected_partitions (dict): Dictionary mapping topic names to the expected number of partitions.

    Prints:
        [INFO] if a topic has the correct number of partitions.
        [ERROR] if a topic has an incorrect number of partitions or does not exist.
        [WARNING] if at least one topic is misconfigured or missing.
    """
    print("[INFO] Verifying that all topics exist and have the correct number of partitions...")

    try:
        existing_topics_metadata = admin.list_topics()
        existing_topics = set(existing_topics_metadata)
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
    """
    Connects to a Kafka cluster and creates sets of topics for satellite image and sensor station ingestion,
    with the number of partitions dynamically determined from the database based on microareas per macroarea.
    """

    # Initialize Kafka admin client
    admin = KafkaAdminClient(
        bootstrap_servers="localhost:29092",
        client_id="setup-script"
    )

    # Number of macroareas
    n_macros = 5

    # Fetch partitions number from DB
    partitions_per_macroarea = {}
    for i in range(1, n_macros + 1):
        partitions = get_partitions(i)
        partitions_per_macroarea[f"A{i}"] = partitions
        print(f"[INFO] Found {partitions} partitions for macroarea_id: A{i}")

    # Create topic definitions for satellite images
    satellite_topics = [
        NewTopic(name=f"satellite_imgs_A{i}", num_partitions=partitions_per_macroarea[f"A{i}"], replication_factor=1)
        for i in range(1, n_macros + 1)
    ]

    # Create topic definitions for sensor stations (same partition counts)
    sensor_topics = [
        NewTopic(name=f"sensor_stations_A{i}", num_partitions=partitions_per_macroarea[f"A{i}"], replication_factor=1)
        for i in range(1, n_macros + 1)
    ]

    # Combine topic lists
    all_topics = satellite_topics + sensor_topics

    try:
        # Create topics
        admin.create_topics(new_topics=all_topics, validate_only=False, timeout_ms=120000)
        print(f"[INFO] Successfully created {len(all_topics)} topics.")

        # Create expected partitions dictionary
        expected_partitions = {topic.name: topic.num_partitions for topic in all_topics}

        # Verify topics
        verify_topics(admin, expected_partitions)

    except Exception as e:
        print(f"[ERROR] Topics not created: {e}")


if __name__ == "__main__":
    kafka_cluster_config()

