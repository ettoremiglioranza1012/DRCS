
# Utilities
from kafka import KafkaConsumer
from datetime import datetime
import streamlit as st
import threading
import boto3
import queue
import json
import time
import os


# Page config
st.set_page_config(
    page_title="Disaster Response Dashboard",
    page_icon="🚨",
    layout="wide"
)

# MinIO configuration - read from environment variables if available
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")


# Kafka consumers (one per topic)
@st.cache_resource
def create_consumers():
    consumer_social = KafkaConsumer(
        "gold_social",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-consumer-social"  # Fixed: era "prices"
    )
    consumer_iot = KafkaConsumer(
        "gold_iot",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-consumer-iot"
    )
    consumer_satellite = KafkaConsumer(
        "gold_sat",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-consumer-satellite"
    )
    return consumer_social, consumer_iot, consumer_satellite


def social_consumer_loop(consumer_social, social_queue):
    """Consumer loop for social media messages"""
    while True:
        try:
            # Poll for messages with timeout
            records = consumer_social.poll(timeout_ms=1000)
            
            for _, messages in records.items():
                for message in messages:
                    try:
                        # Put message in queue for main thread
                        social_queue.put(message.value, block=False)
                    except queue.Full:
                        # If queue is full, remove oldest and add new
                        try:
                            social_queue.get_nowait()
                            social_queue.put(message.value, block=False)
                        except queue.Empty:
                            pass
        except Exception as e:
            print(f"Error in social consumer: {e}")
            time.sleep(1)  # Brief pause before retrying


def iot_consumer_loop(consumer_iot, iot_queue):
    """Consumer loop for IoT sensor data"""
    while True:
        try:
            records = consumer_iot.poll(timeout_ms=1000)
            
            for _, messages in records.items():
                for message in messages:
                    try:
                        iot_queue.put(message.value, block=False)
                    except queue.Full:
                        try:
                            iot_queue.get_nowait()
                            iot_queue.put(message.value, block=False)
                        except queue.Empty:
                            pass
        except Exception as e:
            print(f"Error in IoT consumer: {e}")
            time.sleep(1)


def satellite_consumer_loop(consumer_satellite, sat_queue):
    """Consumer loop for satellite data"""
    while True:
        try:
            records = consumer_satellite.poll(timeout_ms=1000)
            
            for _, messages in records.items():
                for message in messages:
                    try:
                        sat_queue.put(message.value, block=False)
                    except queue.Full:
                        try:
                            sat_queue.get_nowait()
                            sat_queue.put(message.value, block=False)
                        except queue.Empty:
                            pass
        except Exception as e:
            print(f"Error in satellite consumer: {e}")
            time.sleep(1)


def update_all_data_batch(social_queue, iot_queue, sat_queue):
    """
    Update buffer in batch with some limits - more efficient
    """
    # Social data
    social_batch = []
    processed = 0
    while not social_queue.empty() and processed < 50:  # Limit processing
        try:
            social_batch.append(social_queue.get_nowait())
            processed += 1
        except queue.Empty:
            break
    
    if social_batch:
        st.session_state.social_messages.extend(social_batch)
        if len(st.session_state.social_messages) > 30:
            st.session_state.social_messages = st.session_state.social_messages[-20:]
    
    # IoT data
    iot_batch = []
    processed = 0
    while not iot_queue.empty() and processed < 10:  # Limit processing
        try:
            iot_batch.append(iot_queue.get_nowait())
            processed += 1
        except queue.Empty:
            break
    
    if iot_batch:
        st.session_state.iot_data.extend(iot_batch)
        if len(st.session_state.iot_data) > 15:  # Fixed: era > 5
            st.session_state.iot_data = st.session_state.iot_data[-10:]
    
    # Satellite data
    sat_batch = []
    processed = 0
    while not sat_queue.empty() and processed < 10:  # Limit processing
        try:
            sat_batch.append(sat_queue.get_nowait())
            processed += 1
        except queue.Empty:
            break
    
    if sat_batch:
        st.session_state.sat_data.extend(sat_batch)
        if len(st.session_state.sat_data) > 15:  # Fixed: era > 5
            st.session_state.sat_data = st.session_state.sat_data[-10:]
    
    return len(social_batch), len(iot_batch), len(sat_batch)


def wait_for_minio_ready(
    endpoint: str, 
    access_key: str, 
    secret_key: str, 
    max_retries: int = 20, 
    retry_interval: int = 5
) -> None:
    """
    Wait for MinIO service to be ready and accessible.
    
    This function attempts to connect to a MinIO service and verifies it's operational
    by listing the available buckets. It will retry the connection based on the specified
    parameters.
    
    Args:
        endpoint: The host:port address of the MinIO service
        access_key: The MinIO access key for authentication
        secret_key: The MinIO secret key for authentication
        max_retries: Maximum number of connection attempts (default: 20)
        retry_interval: Time in seconds between retry attempts (default: 5)
    """
    for i in range(max_retries):
        try:
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{endpoint}",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            s3.list_buckets()  # just ping
            print("MinIO is ready")
            return
        except Exception as e:
            print(f"MinIO not ready (attempt {i+1}/{max_retries}): {e}")
            time.sleep(retry_interval)
    raise Exception("MinIO is not ready after retries")


def create_minio_client():
    """
        Comment here!
    """
    try:
        # Initialize S3 client for MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1',  # Can be any value for MinIO
            config=boto3.session.Config(signature_version='s3v4')
        )
        print(f"Connected to MinIO at: {MINIO_ENDPOINT}")

        return s3_client
    except Exception as e:
        print(f"Failed to create S3 client: {str(e)}")
        # Re-raise to fail fast if we can't connect to storage
        raise


def dashboard():
    # Initialize session state
    if 'social_messages' not in st.session_state:
        st.session_state.social_messages = []
    if 'iot_data' not in st.session_state:
        st.session_state.iot_data = []
    if 'sat_data' not in st.session_state:
        st.session_state.sat_data = []
    if 'consumer_started' not in st.session_state:
        st.session_state.consumer_started = False
    if 'queues' not in st.session_state:
        st.session_state.queues = None

    # Create consumers and queues only once
    if st.session_state.queues is None:
        consumer_social, consumer_iot, consumer_satellite = create_consumers()
        
        # Queues for thread communication
        social_queue = queue.Queue(maxsize=100)
        iot_queue = queue.Queue(maxsize=10)
        sat_queue = queue.Queue(maxsize=10)
        
        st.session_state.queues = (social_queue, iot_queue, sat_queue)
        
        # Start consumers once
        if not st.session_state.consumer_started:
            # Start background threads
            social_thread = threading.Thread(target=social_consumer_loop, args=(consumer_social, social_queue), daemon=True)
            iot_thread = threading.Thread(target=iot_consumer_loop, args=(consumer_iot, iot_queue), daemon=True)
            sat_thread = threading.Thread(target=satellite_consumer_loop, args=(consumer_satellite, sat_queue), daemon=True)

            social_thread.start()
            iot_thread.start()
            sat_thread.start()

            st.session_state.consumer_started = True
    
    # Get queues from session state
    social_queue, iot_queue, sat_queue = st.session_state.queues
    
    # Update data from queues
    social_count, iot_count, sat_count = update_all_data_batch(social_queue, iot_queue, sat_queue)

    # Wait for minIO to be ready
    wait_for_minio_ready(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    if 'minio_client' not in st.session_state:
        st.session_state.minio_client = create_minio_client()  

    # ====================================== #
    # ============ DASHBOARD =============== #
    # ====================================== #

    # DASHBOARD LAYOUT
    st.title("🚨 Disaster Response Coordination System")

    # Create tabs
    tab1, tab2, tab3  = st.tabs(["🛰️ Satellite Environmental Data", "🔧 IoT Environmental Sensors", "📱 Social Media Alerts", ])

    # TAB 1: Environmental Monitoring (IoT + Satellite)
    with tab1:
        st.header("🛰️ Satellite Environmental Data")

        if st.session_state.sat_data:
            latest_sat = st.session_state.sat_data[-1]
            st.json(latest_sat)

    with tab2:
        st.header("🔧 IoT Environmental Sensors")

    # TAB 2: Social Media Alerts
    with tab3:
        st.header("📱 Emergency Social Media Monitoring")
    
    # SYSTEM STATUS SIDEBAR
    st.sidebar.header("🖥️ System Status")
    st.sidebar.write(f"📱 Social Messages: {len(st.session_state.social_messages)}")
    st.sidebar.write(f"🔧 IoT Readings: {len(st.session_state.iot_data)}")
    st.sidebar.write(f"🛰️ Satellite Data: {len(st.session_state.sat_data)}")
    st.sidebar.write(f"⏱️ Last Update: {datetime.now().strftime('%H:%M:%S')}")
    
    # # Show processing stats
    # if social_count > 0 or iot_count > 0 or sat_count > 0:
    #     st.sidebar.write("📊 Last Batch:")
    #     st.sidebar.write(f"  Social: {social_count}")
    #     st.sidebar.write(f"  IoT: {iot_count}")
    #     st.sidebar.write(f"  Satellite: {sat_count}")

    # System health indicator
    if st.session_state.social_messages or st.session_state.iot_data or st.session_state.sat_data:
        st.sidebar.success("🟢 System Operational")
    else:
        st.sidebar.warning("🟡 Waiting for Data Streams")
    
    # Control buttons
    if st.sidebar.button("🔄 Force Refresh"):
        st.rerun()

    if st.sidebar.button("🗑️ Clear All Data"):
        st.session_state.social_messages = []
        st.session_state.iot_data = []
        st.session_state.sat_data = []
        st.rerun()

    # Auto-refresh every 3 seconds
    time.sleep(3)
    st.rerun()


if __name__ == "__main__":
    dashboard()

