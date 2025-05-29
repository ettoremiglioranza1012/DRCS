
# Utilities
from streamlit_autorefresh import st_autorefresh
from streamlit_folium import st_folium
from kafka import KafkaConsumer
from datetime import datetime
from io import BytesIO
import streamlit as st
from PIL import Image
import threading
import psycopg2
import folium
import redis
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
IMG_BUCKET = "satellite-imgs"


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
    while not sat_queue.empty() and processed < 10:
        try:
            item = sat_queue.get_nowait()
            # st.write(f"DEBUG: Item from sat_queue type: {type(item)}")
            # st.write(f"DEBUG: Item content: {item if isinstance(item, str) else 'dict object'}")
            sat_batch.append(item)
            processed += 1
        except queue.Empty:
            break
    
    if sat_batch:
        # st.write(f"DEBUG: About to extend sat_data with {len(sat_batch)} items")
        # st.write(f"DEBUG: First item in batch type: {type(sat_batch[0])}")
        st.session_state.sat_data.extend(sat_batch)
        if len(st.session_state.sat_data) > 15:
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
    """
    for _ in range(max_retries):
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{endpoint}",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            if s3_client.list_buckets():  # just ping
                return s3_client
        except Exception:
            time.sleep(retry_interval)
    raise Exception("MinIO is not ready after retries")


def wait_for_redis_ready(
    max_retries: int = 20,
    retry_interval: int = 5
) -> redis.Redis:
    """
    """
    for _ in range(max_retries):
        try:
            client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                health_check_interval=30
            )
            if client.ping():
                return client
        except redis.exceptions.ConnectionError:
            time.sleep(retry_interval)
    
    raise Exception("Redis is not ready after maximum retries.")


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

    if 'minio_client' not in st.session_state:
        st.session_state.minio_client = wait_for_minio_ready(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY) 
    if 'redis_client' not in st.session_state:
        st.session_state.redis_client = wait_for_redis_ready()

    # ====================================== #
    # ============ DASHBOARD =============== #
    # ====================================== #

    # DASHBOARD LAYOUT
    st.title("🚨 Disaster Response Coordination System")

    # Create tabs
    tab1, tab2, tab3  = st.tabs(["🛰️ Satellite Environmental Data", "🔧 IoT Environmental Sensors", "📱 Social Media Alerts", ])

    with tab1:
        st.header("🛰️ Satellite Environmental Data")

        if st.session_state.sat_data:
            latest_sat = st.session_state.sat_data[-1]
            
            wildfire_analysis = latest_sat.get('wildfire_analysis', {})
            detection_summary = wildfire_analysis.get('detection_summary', {})
            fire_indicators = wildfire_analysis.get('fire_indicators', {})
            spectral_analysis = wildfire_analysis.get('spectral_analysis', {})
            environmental_assessment = wildfire_analysis.get('environmental_assessment', {})
            severity_assessment = wildfire_analysis.get('severity_assessment', {})
            spatial_distribution = wildfire_analysis.get('spatial_distribution', {})
            microarea_info = latest_sat.get('microarea_info', {})
            
            # ==================== HEADER MAIN ====================
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                st.metric(
                    "📅 Event Timestamp", 
                    latest_sat.get('event_timestamp', 'N/A')[:19].replace('T', ' ')
                )
            
            with col2:
                st.metric(
                    "🗺️ Area ID", 
                    f"{latest_sat.get('microarea_id', 'N/A')}"
                )
            
            with col3:
                risk_level = severity_assessment.get('risk_level', 'unknown')
                if risk_level == 'extreme':
                    st.error("🚨 EXTREME")
                elif risk_level == 'high':
                    st.warning("⚠️ HIGH")
                elif risk_level == 'moderate':
                    st.info("🟡 MODERATE")
                else:
                    st.success("✅ NORMAL")
            
            # Response timestamp
            st.caption(f"Response processed: {latest_sat.get('response_timestamp', 'N/A')[:19].replace('T', ' ')}")
            
            st.divider()

            # ==================== GEOGRAPHIC INFO ====================
            st.subheader("🗺️ Geographic Details")
            if microarea_info:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Min Latitude", f"{microarea_info.get('min_lat', 0):.6f}")
                    st.metric("Max Latitude", f"{microarea_info.get('max_lat', 0):.6f}")
                with col2:
                    st.metric("Min Longitude", f"{microarea_info.get('min_long', 0):.6f}")
                    st.metric("Max Longitude", f"{microarea_info.get('max_long', 0):.6f}")
    
            # ==================== IMAGE REFERENCE ====================
            if 'image_pointer' in latest_sat:
                st.divider()
                object_key = latest_sat.get("image_pointer")
                bucket_name = IMG_BUCKET

            try:
                response = st.session_state.minio_client.get_object(Bucket=bucket_name, Key=object_key)
                image_bytes = response['Body'].read()

                image = Image.open(BytesIO(image_bytes))
                st.image(image, caption=object_key)

            except Exception as e:
                st.error(f"Errore nel recupero dell'immagine: {e}")

            st.divider()
            
            # ==================== DETECTION SUMMARY ====================
            st.subheader("🎯 Detection Summary")
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Total Pixels", detection_summary.get('total_pixels', 0))
            with col2:
                st.metric("Anomalous Pixels", detection_summary.get('anomalous_pixels', 0))
            with col3:
                st.metric("Anomaly %", f"{detection_summary.get('anomaly_percentage', 0)}%")
            with col4:
                st.metric("Affected Area", f"{detection_summary.get('affected_area_km2', 0)} km²")
            with col5:
                st.metric("Confidence", f"{detection_summary.get('confidence_level', 0)}")
            
            st.divider()
            
            # ==================== FIRE INDICATORS ====================
            st.subheader("🔥 Fire Indicators")
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("🌡️ High Temp Signatures", fire_indicators.get('high_temperature_signatures', 0))
            with col2:
                st.metric("🌿 Vegetation Stress", fire_indicators.get('vegetation_stress_detected', 0))
            with col3:
                st.metric("💧 Moisture Deficit", fire_indicators.get('moisture_deficit_areas', 0))
            with col4:
                st.metric("🔥 Burn Scars", fire_indicators.get('burn_scar_indicators', 0))
            with col5:
                st.metric("💨 Smoke Signatures", fire_indicators.get('smoke_signatures', 0))
            
            st.divider()
            
            # ==================== SPECTRAL ANALYSIS ====================
            st.subheader("📊 Spectral Analysis")
            
            # Anomalous Band Averages
            st.write("**Anomalous Band Averages:**")
            anom_bands = spectral_analysis.get('anomalous_band_averages', {})
            col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
            with col1:
                st.metric("B2 (Blue)", f"{anom_bands.get('B2', 0):.3f}")
            with col2:
                st.metric("B3 (Green)", f"{anom_bands.get('B3', 0):.3f}")
            with col3:
                st.metric("B4 (Red)", f"{anom_bands.get('B4', 0):.3f}")
            with col4:
                st.metric("B8 (NIR)", f"{anom_bands.get('B8', 0):.3f}")
            with col5:
                st.metric("B8A (NIR)", f"{anom_bands.get('B8A', 0):.3f}")
            with col6:
                st.metric("B11 (SWIR1)", f"{anom_bands.get('B11', 0):.3f}")
            with col7:
                st.metric("B12 (SWIR2)", f"{anom_bands.get('B12', 0):.3f}")
            
            # Scene Band Averages
            st.write("**Scene Band Averages:**")
            scene_bands = spectral_analysis.get('scene_band_averages', {})
            col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
            with col1:
                st.metric("B2 (Blue)", f"{scene_bands.get('B2', 0):.3f}")
            with col2:
                st.metric("B3 (Green)", f"{scene_bands.get('B3', 0):.3f}")
            with col3:
                st.metric("B4 (Red)", f"{scene_bands.get('B4', 0):.3f}")
            with col4:
                st.metric("B8 (NIR)", f"{scene_bands.get('B8', 0):.3f}")
            with col5:
                st.metric("B8A (NIR)", f"{scene_bands.get('B8A', 0):.3f}")
            with col6:
                st.metric("B11 (SWIR1)", f"{scene_bands.get('B11', 0):.3f}")
            with col7:
                st.metric("B12 (SWIR2)", f"{scene_bands.get('B12', 0):.3f}")
            
            # Anomalous Index Averages
            st.write("**Anomalous Index Averages:**")
            anom_indices = spectral_analysis.get('anomalous_index_averages', {})
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("NDVI", f"{anom_indices.get('NDVI', 0):.3f}")
            with col2:
                st.metric("NDMI", f"{anom_indices.get('NDMI', 0):.3f}")
            with col3:
                st.metric("NDWI", f"{anom_indices.get('NDWI', 0):.3f}")
            with col4:
                st.metric("NBR", f"{anom_indices.get('NBR', 0):.3f}")
            
            st.divider()
            
            # ==================== ENVIRONMENTAL ASSESSMENT ====================
            st.subheader("🌿 Environmental Assessment")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Vegetation Health:**")
                veg_health = environmental_assessment.get('vegetation_health', {})
                veg_status = veg_health.get('status', 'unknown')
                
                if veg_status == 'stressed':
                    st.error(f"Status: **{veg_status.upper()}**")
                elif veg_status == 'healthy':
                    st.success(f"Status: **{veg_status.upper()}**")
                else:
                    st.info(f"Status: **{veg_status.upper()}**")
                
                st.metric("Average NDVI", f"{veg_health.get('average_ndvi', 0):.3f}")
                st.metric("Healthy Vegetation %", f"{veg_health.get('healthy_vegetation_percent', 0)}%")
            
            with col2:
                st.write("**Moisture Conditions:**")
                moisture = environmental_assessment.get('moisture_conditions', {})
                moisture_status = moisture.get('status', 'unknown')
                
                if moisture_status == 'very_dry':
                    st.error(f"Status: **{moisture_status.upper()}**")
                elif moisture_status == 'dry':
                    st.warning(f"Status: **{moisture_status.upper()}**")
                else:
                    st.info(f"Status: **{moisture_status.upper()}**")
                
                st.metric("Average NDMI", f"{moisture.get('average_ndmi', 0):.3f}")
                st.metric("Average NDWI", f"{moisture.get('average_ndwi', 0):.3f}")
                st.metric("Dry Pixel %", f"{moisture.get('dry_pixel_percent', 0)}%")
            
            # Fire Weather Indicators
            st.write("**Fire Weather Indicators:**")
            fire_weather = environmental_assessment.get('fire_weather_indicators', {})
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                fw_level = fire_weather.get('fire_weather_level', 'unknown')
                if fw_level == 'high':
                    st.error(f"Fire Weather Level: **{fw_level.upper()}**")
                elif fw_level == 'moderate':
                    st.warning(f"Fire Weather Level: **{fw_level.upper()}**")
                else:
                    st.info(f"Fire Weather Level: **{fw_level.upper()}**")
            with col2:
                st.metric("Temperature Signature %", f"{fire_weather.get('temperature_signature_percent', 0)}%")
            with col3:
                st.metric("Moisture Deficit %", f"{fire_weather.get('moisture_deficit_percent', 0)}%")
            with col4:
                st.metric("Smoke Detection %", f"{fire_weather.get('smoke_detection_percent', 0)}%")
            
            # Environmental Stress Level
            env_stress = environmental_assessment.get('environmental_stress_level', 'unknown')
            if env_stress == 'critical':
                st.error(f"🚨 Environmental Stress Level: **{env_stress.upper()}**")
            elif env_stress == 'high':
                st.warning(f"⚠️ Environmental Stress Level: **{env_stress.upper()}**")
            else:
                st.info(f"Environmental Stress Level: **{env_stress.upper()}**")
            
            st.divider()
            
            # ==================== SEVERITY ASSESSMENT ====================
            st.subheader("⚡ Severity Assessment")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Severity Score", f"{severity_assessment.get('severity_score', 0):.2f}")
                risk_level = severity_assessment.get('risk_level', 'unknown')
                if risk_level == 'extreme':
                    st.error(f"Risk Level: **{risk_level.upper()}**")
                elif risk_level == 'high':
                    st.warning(f"Risk Level: **{risk_level.upper()}**")
                else:
                    st.info(f"Risk Level: **{risk_level.upper()}**")
                
                threat_class = severity_assessment.get('threat_classification', {})
                threat_level = threat_class.get('level', 'unknown')
                if threat_level == 'CRITICAL':
                    st.error(f"Threat Level: **{threat_level}**")
                elif threat_level == 'HIGH':
                    st.warning(f"Threat Level: **{threat_level}**")
                else:
                    st.info(f"Threat Level: **{threat_level}**")
            
            with col2:
                st.write(f"Priority: **{threat_class.get('priority', 'N/A')}**")
                st.write(f"Evacuation needed: **{threat_class.get('evacuation_consideration', False)}**")
            
            # Threat Description
            threat_desc = severity_assessment.get('threat_classification', {}).get('description', '')
            if threat_desc:
                st.info(f"📋 **Description:** {threat_desc}")
            
            st.divider()
            
            # ==================== SPATIAL DISTRIBUTION ====================
            st.subheader("📍 Spatial Distribution")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Cluster Density", f"{spatial_distribution.get('cluster_density', 0):.4f}")
            with col2:
                st.metric("Geographic Spread", f"{spatial_distribution.get('geographic_spread_km2', 0):.2f} km²")
            with col3:
                st.metric("Hotspot Concentration %", f"{spatial_distribution.get('hotspot_concentration_percent', 0)}%")
            
            st.divider()
            
            # ==================== RECOMMENDATIONS ====================
            recommendations = wildfire_analysis.get('recommendations', [])
            if recommendations:
                st.subheader("📋 Emergency Recommendations")
                
                for i, rec in enumerate(recommendations, 1):
                    if i <= 3:  
                        st.error(f"🚨 **{i}.** {rec}")
                    else:  
                        st.warning(f"⚠️ **{i}.** {rec}")
        
        else:
            st.info("🔄 Waiting for satellite data...")
            st.write("The system is ready to receive and display real-time satellite environmental data.")
            

    with tab2:
        st.header("🔧 IoT Environmental Sensors")
        
        if st.session_state.iot_data:
            latest_iot = st.session_state.iot_data[-1]

            # """
            #     TO BE CHANGED; NEEDS REDIS UPDATE; 
            # """
            # conn = psycopg2.connect(dbname="california_db", user="gruppo3", password="gruppo3", host="postgres", port="5432")
            # cur = conn.cursor()

            # cur.execute("SELECT DISTINCT macroarea_id FROM microareas ORDER BY macroarea_id")
            # macroareas = [row[0] for row in cur.fetchall()]
            # macroarea = st.selectbox("Select Macroarea", macroareas, key="macroarea_select")

            # # 🔹 Fetch microareas for selected macroarea
            # cur.execute("SELECT microarea_id FROM microareas WHERE macroarea_id = %s", (macroarea,))
            # microareas = [row[0] for row in cur.fetchall()]
            # microarea = st.selectbox("Select Microarea", microareas, key="microarea_select")

            # ==================== HEADER MAIN ====================
            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                st.metric(
                    "Event ID", 
                    latest_iot.get('event_id', 'N/A')
                )
            
            with col2:
                timestamp_ms = latest_iot.get('latest_event_timestamp')
                
                if timestamp_ms is not None:
                    try:
                        ts = int(timestamp_ms) / 1000  # converti da ms a s
                        dt = datetime.fromtimestamp(ts)  # crea oggetto datetime
                        formatted_ts = dt.strftime('%Y-%m-%d %H:%M:%S')  # formato leggibile
                    except Exception:
                        formatted_ts = "Invalid timestamp"
                else:
                    formatted_ts = "N/A"

                st.metric("📅 Event Timestamp", formatted_ts)
            
            with col3:
                st.metric(
                    "Area ID", 
                    f"{latest_iot.get('region_id', 'N/A')}"
                )

            microarea_id = latest_iot.get("region_id")
            redis_key = f"microarea:{microarea_id}"

            if st.session_state.redis_client:
                region_info_json = st.session_state.redis_client.get(redis_key)
                region_info = json.loads(region_info_json)
                min_long = region_info.get("min_long")
                min_lat = region_info.get("min_lat")
                max_long = region_info.get("max_long")
                max_lat = region_info.get("max_lat")
                
                # Compute center & polygon
                center_lat = (min_lat + max_lat) / 2
                center_long = (min_long + max_long) / 2
                polygon_coords = [
                    [min_lat, min_long],
                    [min_lat, max_long],
                    [max_lat, max_long],
                    [max_lat, min_long],
                    [min_lat, min_long]
                ]     
        
                # Build map
                m = folium.Map(location=[center_lat, center_long], zoom_start=12)
                m.fit_bounds(polygon_coords)

                folium.Polygon(
                    locations=polygon_coords,
                    color="blue",
                    weight=2,
                    fill=True,
                    fill_color="blue",
                    fill_opacity=0.1,
                    tooltip=f"Microarea: {microarea_id}"
                ).add_to(m)

                st_folium(m, width=800, height=500)
            
            else:
                print(f" Redis client not initiliazed")

            # === AGGREGATE DATA ===
            st.divider()
            st.subheader("📊 Aggregated Detection Data")
            
            # Estrai i dati aggregati
            aggregated = latest_iot.get("aggregated_detection", {})
            environmental = latest_iot.get("environmental_context", {})
            system_response = latest_iot.get("system_response", {})
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                wildfire_detected = aggregated.get("wildfire_detected", False)
                st.metric(
                    "🔥 Wildfire Detection", 
                    "DETECTED" if wildfire_detected else "CLEAR",
                    delta=f"{aggregated.get('detection_confidence', 0):.1%} confidence"
                )
            
            with col2:
                severity = aggregated.get("severity_score", 0)
                st.metric(
                    "⚠️ Severity Score", 
                    f"{severity:.2f}",
                    delta="High Risk" if severity > 0.7 else "Moderate" if severity > 0.4 else "Low"
                )
            
            with col3:
                aqi = aggregated.get("air_quality_index", 0)
                aqi_status = aggregated.get("air_quality_status", "Unknown")
                st.metric(
                    "🌬️ Air Quality Index", 
                    f"{aqi:.1f}",
                    delta=aqi_status
                )
            
            with col4:
                alert_level = system_response.get("alert_level", "unknown")
                st.metric(
                    "🚨 Alert Level", 
                    alert_level.replace("_", " ").title(),
                    delta="ACTIVE" if system_response.get("event_triggered") else "INACTIVE"
                )

            # === ENVIRONMENTAL CONDITIONS ===
            st.divider()
            st.subheader("🌤️ Environmental Conditions")
            
            weather = environmental.get("weather_conditions", {})
            terrain = environmental.get("terrain_info", {})
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Weather Conditions:**")
                st.write(f"🌡️ Temperature: {weather.get('temperature', 'N/A')}°C")
                st.write(f"💧 Humidity: {weather.get('humidity', 'N/A')}%")
                st.write(f"💨 Wind Speed: {weather.get('wind_speed', 'N/A')} km/h")
                st.write(f"🧭 Wind Direction: {weather.get('wind_direction', 'N/A')}°")
                st.write(f"🌧️ Precipitation Chance: {weather.get('precipitation_chance', 0)*100:.1f}%")
            
            with col2:
                st.write("**Terrain Information:**")
                st.write(f"🌿 Vegetation: {terrain.get('vegetation_type', 'N/A').title()}")
                st.write(f"🌲 Density: {terrain.get('vegetation_density', 'N/A').title()}")
                st.write(f"⛰️ Slope: {terrain.get('slope', 'N/A').title()}")
                st.write(f"🧭 Aspect: {terrain.get('aspect', 'N/A').title()}")

            # === FIRE BEHAVOUR ===
            if "fire_behavior" in aggregated and wildfire_detected:
                st.divider()
                st.subheader("🔥 Fire Behavior Analysis")
                
                fire_behavior = aggregated["fire_behavior"]
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric(
                        "Spread Rate", 
                        fire_behavior.get("spread_rate", "Unknown").title()
                    )
                
                with col2:
                    st.metric(
                        "Direction", 
                        fire_behavior.get("direction", "Unknown").title()
                    )
                
                with col3:
                    st.metric(
                        "Speed", 
                        f"{fire_behavior.get('estimated_speed_mph', 0)} mph"
                    )
                
                ignition_time = aggregated.get("estimated_ignition_time")
                if ignition_time:
                    st.info(f"🕐 Estimated Ignition Time: {ignition_time}")

            # === SYSTEM RESPONSE ===
            st.divider()
            st.subheader("🚨 System Response")
            
            at_risk = system_response.get("at_risk_assets", {})
            
            if "population_centers" in at_risk:
                st.write("**Population Centers at Risk:**")
                for center in at_risk["population_centers"]:
                    st.warning(f"🏘️ {center.get('name')}: {center.get('population')} people at {center.get('distance_meters')}m distance (Priority: {center.get('evacuation_priority')})")
            
            if "critical_infrastructure" in at_risk:
                st.write("**Critical Infrastructure at Risk:**")
                for infra in at_risk["critical_infrastructure"]:
                    st.error(f"🏭 {infra.get('name')} ({infra.get('type')}): {infra.get('distance_meters')}m distance (Priority: {infra.get('priority')})")

            if "recommended_actions" in system_response:
                st.write("**Recommended Actions:**")
                for action in system_response["recommended_actions"]:
                    priority_color = "🔴" if action.get('priority') == 'high' else "🟡" if action.get('priority') == 'medium' else "🟢"
                    st.write(f"{priority_color} {action.get('action', '').replace('_', ' ').title()} (Priority: {action.get('priority', 'N/A')})")
                    
                    if 'recommended_resources' in action:
                        resources = ", ".join([r.replace('_', ' ').title() for r in action['recommended_resources']])
                        st.write(f"   Resources: {resources}")
                    
                    if 'radius_meters' in action:
                        st.write(f"   Radius: {action['radius_meters']}m")
                    if 'evacuation_direction' in action:
                        st.write(f"   Direction: {action['evacuation_direction']}")

            if "sent_notifications_to" in system_response:
                st.write("**Notifications Sent:**")
                for notification in system_response["sent_notifications_to"]:
                    status_icon = "✅" if notification.get('delivery_status') == 'confirmed' else "❌"
                    st.write(f"{status_icon} {notification.get('agency', '').replace('_', ' ').title()} - {notification.get('delivery_status')} at {notification.get('notification_timestamp')}")

            # === TECHNICAL REPORT ===
            with st.expander("🔧 Technical Information"):
                st.write(f"**Event ID:** {latest_iot.get('event_id')}")
                st.write(f"**Region ID:** {latest_iot.get('region_id')}")
                st.write(f"**Event Type:** {latest_iot.get('event_type', '').title()}")
                st.write(f"**Detection Source:** {latest_iot.get('detection_source', '').replace('_', ' ').title()}")
                st.write(f"**Response Timestamp:** {latest_iot.get('response_timestamp')}")
                st.write(f"**Latest Event Timestamp:** {latest_iot.get('latest_event_timestamp')}")
                
                if aggregated.get("anomaly_detected"):
                    st.write(f"**Anomaly Type:** {aggregated.get('anomaly_type', '').title()}")

        else:
            st.info("🔄 Waiting for IoT data...")
            st.write("The system is ready to receive and display real-time IoT environmental data.")

    with tab3:
        st.header("📱 Emergency Social Media Monitoring")
    
    # SYSTEM STATUS SIDEBAR
    st.sidebar.header("🖥️ System Status")
    st.sidebar.write(f"📱 Social Messages: {len(st.session_state.social_messages)}")
    st.sidebar.write(f"🔧 IoT Readings: {len(st.session_state.iot_data)}")
    st.sidebar.write(f"🛰️ Satellite Data: {len(st.session_state.sat_data)}")
    st.sidebar.write(f"⏱️ Last Update: {datetime.now().strftime('%H:%M:%S')}")
    
    # Show processing stats
    if social_count > 0 or iot_count > 0 or sat_count > 0:
        st.sidebar.write("📊 Last Batch:")
        st.sidebar.write(f"  Social: {social_count}")
        st.sidebar.write(f"  IoT: {iot_count}")
        st.sidebar.write(f"  Satellite: {sat_count}")

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
    st_autorefresh(interval=3000, limit=None, key="global-autorefresh")


if __name__ == "__main__":
    dashboard()

