
# Utilities
from kafka import KafkaConsumer
from datetime import datetime
import streamlit as st
import threading
import queue
import json
import time

# Page config
st.set_page_config(
    page_title="Disaster Response Dashboard",
    page_icon="🚨",
    layout="wide"
)

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

    # ====================================== #
    # ============ DASHBOARD =============== #
    # ====================================== #

    # DASHBOARD LAYOUT
    st.title("🚨 Disaster Response Coordination System")

    # Create tabs
    tab1, tab2 = st.tabs(["📡 Environmental Monitoring", "📱 Social Media Alerts"])

    # TAB 1: Environmental Monitoring (IoT + Satellite)
    with tab1:
        st.header("🌍 Environmental Data & Monitoring")
        
        # Create columns for IoT and Satellite data
        col1, col2 = st.columns([1, 1])
        
        # IOT SENSOR SECTION
        with col1:
            st.subheader("🔧 IoT Environmental Sensors")

        # SATELLITE SECTION
        with col2:
            st.subheader("🛰️ Satellite Environmental Data")

    # TAB 2: Social Media Alerts
    with tab2:
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

