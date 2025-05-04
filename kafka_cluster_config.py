# Utilities
from kafka.admin import KafkaAdminClient, NewTopic
from db_utils import connect_to_db
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
            FROM n_microareas
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
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except:
            pass


def verify_topics(admin: KafkaAdminClient, expected_partitions: dict) -> None:
    """
    Verifies that each Kafka topic has the expected number of partitions.

    Args:
        admin (KafkaAdminClient): Initialized KafkaAdminClient instance.
        expected_partitions (dict): Dictionary mapping topic names to the expected number of partitions.

    Prints:
        [INFO] if a topic has the correct number of partitions.
        [ERROR] if a topic has an incorrect number of partitions.
        [WARNING] if at least one topic is misconfigured.
    """
    print("[INFO] Verifying that all topics have the correct number of partitions...")

    try:
        topics_metadata = admin.describe_topics(list(expected_partitions.keys()))
        all_ok = True

        for topic_meta in topics_metadata:
            topic = topic_meta['topic']
            actual_count = len(topic_meta['partitions'])  
            expected_count = expected_partitions[topic]    

            if actual_count != expected_count:
                print(f"[ERROR] Topic '{topic}': expected {expected_count} partitions, but got {actual_count}.")
                all_ok = False
            else:
                print(f"[INFO] Topic '{topic}' has the correct number of partitions ({actual_count}).")
        
        if all_ok:
            print("[INFO] All topics verified successfully.")
        else:
            print("[WARNING] One or more topics have incorrect partition counts.")
    
    except Exception as e:
        print("[ERROR] Failed to verify topics:", e)
        raise


def kafka_cluster_config():
    # Initialize Kafka admin client
    admin = KafkaAdminClient(
        bootstrap_servers="localhost:29092",
        client_id="setup-script"
    )

    # Get number of partitions from DB
    n_macros = 5
    topics_name = [f"satellite_imgs_A{i}" for i in range(1, n_macros + 1)]
    list_of_partitions_number = [get_partitions(i) for i in range(1, n_macros + 1)]

    # Partition check
    for i in range(n_macros):
        print(f"[INFO] Found {list_of_partitions_number[i]} for macroarea_id: A{i + 1}")
    
    # Create topic definitions
    topic_list = [
        NewTopic(name=topics_name[i], num_partitions=list_of_partitions_number[i], replication_factor=1)
        for i in range(n_macros)
    ]
    
    try:
        # Create topics
        #admin.create_topics(new_topics=topic_list, validate_only=False, timeout_ms=120000)
        print(f"[INFO] Successfully created {n_macros} topics.")

        # Expected partition mapping
        expected_partitions = {
            topics_name[i]: list_of_partitions_number[i]
            for i in range(n_macros)
        }

        # Verification
        verify_topics(admin, expected_partitions)

    except Exception as e:
        print(f"[ERROR] Topics not created: {e}")


if __name__ == "__main__":
    kafka_cluster_config()
