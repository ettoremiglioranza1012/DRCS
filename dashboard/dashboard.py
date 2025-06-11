

"""Streamlit application for the Disaster Response Coordination System (DRCS) Dashboard.

This dashboard displays real-time environmental data from three sources:
1. Satellite imagery (e.g., NDVI, fire detection)
2. IoT environmental sensors (e.g., temperature, humidity, ecc...)
3. Social media alerts related to disasters

It initializes external services (MinIO, Redis) to harvest geospatial informations and real-time satellite images, 
consumes data from Kafka topics, updates session state, and renders a multi-tab interface for monitoring incoming data.

This specific instance of the dashboard is configured to display information
only for a single microarea, selected among the many microareas that compose
the larger macroarea or region provided by the user.
"""

# Utilities
from streamlit_autorefresh import st_autorefresh
import streamlit as st

# Import utility modules
from Utils.external_clients import initialize_external_connections
from Utils.kafka_consumers import initialize_kafka_system
from Utils.update_stream import update_all_data_batch
from Utils.ui_components import (
    render_satellite_tab,
    render_iot_tab, 
    render_social_tab,
    render_sidebar_status
)


def initialize_session_state():
    """Initialize session state variables used to store data and status flags."""
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
    if 'external_clients' not in st.session_state:
        st.session_state.external_clients = None


def setup_page_config():
    """Configure Streamlit page layout and title."""
    st.set_page_config(
        page_title="DRCS Dashboard",
        layout="wide"
    )


def setup_external_connections():
    """Initialize connections to external services (MinIO, Redis)"""
    if not st.session_state.external_clients:
        try:
            clients = initialize_external_connections()
            st.session_state.external_clients = clients
        except Exception as e:
            st.error(f"Failed to initialize external connections: {e}")
            st.stop()


def setup_kafka_system():
    """Initialize Kafka consumers and processing queues"""
    if st.session_state.queues is None:
        try:
            queues = initialize_kafka_system()
            st.session_state.queues = queues
            st.session_state.consumer_started = True
        except Exception as e:
            st.error(f"Failed to initialize Kafka system: {e}")
            st.stop()


def update_data_streams():
    """Update data from all Kafka streams"""
    if st.session_state.queues:
        social_queue, iot_queue, sat_queue = st.session_state.queues
        return update_all_data_batch(social_queue, iot_queue, sat_queue, st.session_state)
    return 0, 0, 0


def render_main_header():
    """Render the main dashboard header"""
    st.title(f"DRCS Dashboard")


def render_main_tabs():
    """Render the three main data visualization tabs: Satellite, IoT, and Social Media."""
    tab1, tab2, tab3 = st.tabs([
        "🛰️ Satellite Environmental Data",
        "🔧 IoT Environmental Sensors", 
        "📱 Social Media Alerts"
    ])
    
    with tab1:
        if st.session_state.sat_data:
            render_satellite_tab(
                st.session_state.sat_data[-1],
                st.session_state.external_clients.get_minio().get_client(),
                img_bucket="satellite-imgs"
            )
        else:
            st.info("🔄 Waiting for satellite data...")
            st.write("The system is ready to receive and display real-time satellite environmental data.")
    
    with tab2:
        if st.session_state.iot_data:
            render_iot_tab(
                st.session_state.iot_data[-1], 
                st.session_state.external_clients.get_redis().get_client()
            )
        else:
            st.info("🔄 Waiting for IoT data...")
            st.write("The system is ready to receive and display real-time IoT environmental data.")
    
    with tab3:
        if st.session_state.social_messages:
            render_social_tab(
                st.session_state.social_messages
            )
        else:
            st.info("Waiting for social media messages...")


def render_sidebar(processing_stats):
    """Render sidebar with system status"""
    social_count, iot_count, sat_count = processing_stats
    render_sidebar_status(
        st.session_state.social_messages,
        st.session_state.iot_data,
        st.session_state.sat_data,
        social_count, 
        iot_count, 
        sat_count
    )


def main():
    """Main entry point of the dashboard application."""
    # Setup
    setup_page_config()
    initialize_session_state()
    setup_external_connections()
    setup_kafka_system()
    
    # Update data streams
    processing_stats = update_data_streams()
    
    # Render UI
    render_main_header()
    render_main_tabs()
    render_sidebar(processing_stats)
    
    # Auto-refresh
    st_autorefresh(
        interval=15000, 
        limit=None, 
        key="global-autorefresh"
    )


if __name__ == "__main__":
    main()

