
"""
    UI components for the dashboard.
"""

# Utilities
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
from datetime import datetime
from io import BytesIO
import streamlit as st
from io import BytesIO
from PIL import Image
import pandas as pd
import folium
import json


def render_satellite_tab(latest_sat, minio_client, img_bucket="satellite-imgs"):
    """Render the satellite environmental data tab"""
    st.header("🛰️ Satellite Environmental Data")

    if latest_sat:
        wildfire_analysis = latest_sat.get('wildfire_analysis', {})
        detection_summary = wildfire_analysis.get('detection_summary', {})
        fire_indicators = wildfire_analysis.get('fire_indicators', {})
        spectral_analysis = wildfire_analysis.get('spectral_analysis', {})
        environmental_assessment = wildfire_analysis.get('environmental_assessment', {})
        severity_assessment = wildfire_analysis.get('severity_assessment', {})
        spatial_distribution = wildfire_analysis.get('spatial_distribution', {})
        microarea_info = latest_sat.get('microarea_info', {})

        col = st.columns((1.5, 4.5, 2), gap='medium')

        
        with col[0]:
            # ==================== EVENT METADATA ====================  
            # DataFrame 1: Event Metadata
            df_event_metadata = pd.DataFrame({
                'Metric': [
                    'Event Ts',
                    'Area ID',
                    'Risk Level'
                ],
                'Value': [
                    latest_sat.get('event_timestamp', 'N/A')[:19].replace('T', ' '),
                    f"{latest_sat.get('microarea_id', 'N/A')}",
                    severity_assessment.get('risk_level', 'unknown').upper()
                ]
            })

            # ==================== GEOGRAPHIC INFORMATION ====================            
            # DataFrame 2: Geographic Information
            df_geographic_info = pd.DataFrame({
                'Geographic Metric': [
                    'Min Latitude',
                    'Max Latitude', 
                    'Min Longitude',
                    'Max Longitude'
                ],
                'Coordinate': [
                    f"{microarea_info.get('min_lat', 0):.6f}",
                    f"{microarea_info.get('max_lat', 0):.6f}",
                    f"{microarea_info.get('min_long', 0):.6f}",
                    f"{microarea_info.get('max_long', 0):.6f}"
                ]
            })
            
            # ==================== FIRE INDICATORS ====================            
            # DataFrame 3: Fire Indicators
            df_fire_indicators = pd.DataFrame({
                'Fire Indicator': [
                    'High Temp Signatures',
                    'Vegetation Stress',
                    'Moisture Deficit',
                    'Burn Scars',
                    'Smoke Signatures'
                ],
                'Count': [
                    fire_indicators.get('high_temperature_signatures', 0),
                    fire_indicators.get('vegetation_stress_detected', 0),
                    fire_indicators.get('moisture_deficit_areas', 0),
                    fire_indicators.get('burn_scar_indicators', 0),
                    fire_indicators.get('smoke_signatures', 0)
                ]
            })

            # ==================== ENVIROMENTAL ASSESSMENT ====================
            # DataFrame 4: Environmental Assessment
            environmental_data = []

            # Vegetation Health
            veg_health = environmental_assessment.get('vegetation_health', {})
            environmental_data.extend([
                ['Vegetation Status', veg_health.get('status', 'unknown').upper()],
                ['Average NDVI', f"{veg_health.get('average_ndvi', 0):.3f}"],
                ['Healthy Vegetation %', f"{veg_health.get('healthy_vegetation_percent', 0)}%"]
            ])

            # Moisture Conditions
            moisture = environmental_assessment.get('moisture_conditions', {})
            environmental_data.extend([
                ['Moisture Status', moisture.get('status', 'unknown').upper()],
                ['Average NDMI', f"{moisture.get('average_ndmi', 0):.3f}"],
                ['Average NDWI', f"{moisture.get('average_ndwi', 0):.3f}"],
                ['Dry Pixel %', f"{moisture.get('dry_pixel_percent', 0)}%"]
            ])

            # Fire Weather Indicators
            fire_weather = environmental_assessment.get('fire_weather_indicators', {})
            environmental_data.extend([
                ['Fire Weather Level', fire_weather.get('fire_weather_level', 'unknown').upper()],
                ['Temperature Signature %', f"{fire_weather.get('temperature_signature_percent', 0)}%"],
                ['Moisture Deficit %', f"{fire_weather.get('moisture_deficit_percent', 0)}%"],
                ['Smoke Detection %', f"{fire_weather.get('smoke_detection_percent', 0)}%"]
            ])

            # Environmental Stress Level
            env_stress = environmental_assessment.get('environmental_stress_level', 'unknown')
            environmental_data.append(['Environmental Stress', env_stress.upper()])

            df_environmental_assessment = pd.DataFrame(environmental_data, columns=['Environmental Metric', 'Value'])
            df_environmental_assessment["Value"] = df_environmental_assessment["Value"].astype(str)

            # ==================== DETECTION SUMMARY ====================
            # Create DataFrame for Detection Summary
            df_detection_summary = pd.DataFrame({
                "Metric": [
                    "Total Pixels",
                    "Anomalous Pixels",
                    "Anomaly %",
                    "Affected Area",
                    "Confidence"
                ],
                "Value": [
                    detection_summary.get('total_pixels', 0),
                    detection_summary.get('anomalous_pixels', 0),
                    f"{detection_summary.get('anomaly_percentage', 0)}%",
                    f"{detection_summary.get('affected_area_km2', 0)} km²",
                    f"{detection_summary.get('confidence_level', 0)}"
                ]
            })
            df_detection_summary["Value"] = df_detection_summary["Value"].astype(str)

            # ==================== DISPLAY TABLES ====================
            # Display 1: Event Metadata
            st.markdown("#### Event Metadata")
            st.dataframe(
                df_event_metadata,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Metric": st.column_config.TextColumn("Metric", width="medium"),
                    "Value": st.column_config.TextColumn("Value", width="small")
                }
            )
            # Display 2: Detection Summary as DataFrame
            st.markdown("#### Detection Summary")
            st.dataframe(
                df_detection_summary,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Metric": st.column_config.TextColumn("Metric", width="medium"),
                    "Value": st.column_config.TextColumn("Value", width="small")
                }
            )              
            # Display 3: Geographic Information
            st.markdown("#### Geographic INFO")
            st.dataframe(
                df_geographic_info,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Geographic Metric": st.column_config.TextColumn("Geographic Metric", width="medium"),
                    "Coordinate": st.column_config.TextColumn("Coordinate", width="small")
                }
            )
            # Display 4: Fire Indicators
            st.markdown("#### Fire Indicators")
            st.dataframe(
                df_fire_indicators,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Fire Indicator": st.column_config.TextColumn("Fire Indicator", width="medium"),
                    "Count": st.column_config.NumberColumn("Count", format="%d", width="small")
                }
            )
            # Display 5: Environmental Assessment
            st.markdown("#### Env. Assessment")
            st.dataframe(
                df_environmental_assessment,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Environmental Metric": st.column_config.TextColumn("Environmental Metric", width="medium"),
                    "Value": st.column_config.TextColumn("Value", width="small")
                }
            )          


        with col[1]:
            # ==================== IMAGE REFERENCE ====================
            st.markdown("#### LAAP ")
            st.write("Latest Available Assesment Picture")
            
            if 'image_pointer' in latest_sat:
                st.divider()
                object_key = latest_sat.get("image_pointer")
                bucket_name = img_bucket

                try:
                    response = minio_client.get_object(Bucket=bucket_name, Key=object_key)
                    image_bytes = response['Body'].read()
                    image = Image.open(BytesIO(image_bytes))
                    st.image(image, caption=object_key)
                except Exception as e:
                    st.error(f"Error retrieving image: {e}")

            # ==================== RECOMMENDATIONS ====================
            recommendations = wildfire_analysis.get('recommendations', [])
            if recommendations:
                st.markdown("#### Emergency Recommendations")
                
                for i, rec in enumerate(recommendations, 1):
                    if i <= 3:  
                        st.error(f"**{i}.** {rec}")
                    else:  
                        st.warning(f"**{i}.** {rec}")
        
        
        with col[2]:
            # ==================== SEVERITY ASSESSMENT ====================
            st.markdown("#### Severity Assessment")
            
            st.metric("Severity Score", f"{severity_assessment.get('severity_score', 0):.2f}")
            
            # Extract threat classification info
            threat_class = severity_assessment.get('threat_classification', {})
            threat_level = threat_class.get('level', 'unknown').upper()
            priority = threat_class.get('priority', 'N/A')
            evacuation = str(threat_class.get('evacuation_consideration', False))

            # Optional: Display colored alert based on threat level
            if threat_level == 'CRITICAL':
                st.error(f"🚨 Threat: {threat_level}")
            elif threat_level == 'HIGH':
                st.warning(f"⚠️ Threat: {threat_level}")
            else:
                st.info(f"📊 Threat: {threat_level}")

            # Create DataFrame for Threat Classification
            df_threat_classification = pd.DataFrame({
                "Metric": [
                    "Threat Level",
                    "Priority",
                    "Evacuation Needed"
                ],
                "Value": [
                    threat_level,
                    priority,
                    evacuation
                ]
            })

            # Display Threat Classification as DataFrame
            st.markdown("#### 🔐 Threat Classification")
            st.dataframe(
                df_threat_classification,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Metric": st.column_config.TextColumn("Metric", width="medium"),
                    "Value": st.column_config.TextColumn("Value", width="small")
                }
            )
            
            # ==================== SPATIAL DISTRIBUTION ====================
            # Create DataFrame for Spatial Distribution
            df_spatial_distribution = pd.DataFrame({
                "Metric": [
                    "Cluster Density",
                    "Geographic Spread",
                    "Hotspot Concentration %"
                ],
                "Value": [
                    f"{spatial_distribution.get('cluster_density', 0):.4f}",
                    f"{spatial_distribution.get('geographic_spread_km2', 0):.2f} km²",
                    f"{spatial_distribution.get('hotspot_concentration_percent', 0)}%"
                ]
            })
            df_spatial_distribution["Value"] = df_spatial_distribution["Value"].astype(str)

            # Display Spatial Distribution as DataFrame
            st.markdown("#### 📍 Spatial Distribution")
            st.dataframe(
                df_spatial_distribution,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Metric": st.column_config.TextColumn("Metric", width="medium"),
                    "Value": st.column_config.TextColumn("Value", width="small")
                }
            )
            
            # ==================== SPECTRAL ANALYSIS ====================
            # Anomalous Band Averages
            st.write("**Anomalous Band Averages:**")
            anom_bands = spectral_analysis.get('anomalous_band_averages', {})
            df_anom_band_avg = pd.DataFrame({
                'Band': [
                    "B2 (Blue)",
                    "B3 (Green)",
                    "B4 (Red)", 
                    "B8 (NIR)", 
                    "B8A (NIR)",
                    "B11 (SWIR1)",
                    "B12 (SWIR2)"
                ],
                'Value': [
                    f"{anom_bands.get('B2', 0):.3f}",
                    f"{anom_bands.get('B3', 0):.3f}",
                    f"{anom_bands.get('B4', 0):.3f}",
                    f"{anom_bands.get('B8', 0):.3f}",
                    f"{anom_bands.get('B8A', 0):.3f}",
                    f"{anom_bands.get('B11', 0):.3f}",
                    f"{anom_bands.get('B12', 0):.3f}"
                ]
            })

            st.dataframe(
                df_anom_band_avg,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Band": st.column_config.TextColumn(
                        "Band Name",
                        width="medium"
                    ),
                    "Value": st.column_config.TextColumn(
                        "Average Value",
                        width="small"
                    )
                }
            )        
            
            # Scene Band Averages
            st.write("**Scene Band Averages:**")
            scene_bands = spectral_analysis.get('scene_band_averages', {})
            df_scene_band_avg = pd.DataFrame({
                'Band': [
                    "B2 (Blue)",
                    "B3 (Green)",
                    "B4 (Red)", 
                    "B8 (NIR)", 
                    "B8A (NIR)",
                    "B11 (SWIR1)",
                    "B12 (SWIR2)"
                ],
                'Value': [
                    f"{scene_bands.get('B2', 0):.3f}",
                    f"{scene_bands.get('B3', 0):.3f}",
                    f"{scene_bands.get('B4', 0):.3f}",
                    f"{scene_bands.get('B8', 0):.3f}",
                    f"{scene_bands.get('B8A', 0):.3f}",
                    f"{scene_bands.get('B11', 0):.3f}",
                    f"{scene_bands.get('B12', 0):.3f}"
                ]
            })

            st.dataframe(
                df_scene_band_avg,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Band": st.column_config.TextColumn(
                        "Band Name",
                        width="medium"
                    ),
                    "Value": st.column_config.TextColumn(
                        "Average Value",
                        width="small"
                    )
                }
            )
            
            # Anomalous Index Averages
            st.write("**Anomalous Index Averages:**")
            anom_indices = spectral_analysis.get('anomalous_index_averages', {})
            df_anom_indices = pd.DataFrame({
                'Index': [
                    "NDVI",
                    "NDMI",
                    "NDWI",
                    "NBR"
                ],
                'Value': [
                    f"{anom_indices.get('NDVI', 0):.3f}",
                    f"{anom_indices.get('NDMI', 0):.3f}",
                    f"{anom_indices.get('NDWI', 0):.3f}",
                    f"{anom_indices.get('NBR', 0):.3f}"
                ]
            })

            st.dataframe(
                df_anom_indices,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Index": st.column_config.TextColumn(
                        "Index Name",
                        width="medium"
                    ),
                    "Value": st.column_config.TextColumn(
                        "Average Value",
                        width="small"
                    )
                }
            )
        
    else:
        st.info("🔄 Waiting for satellite data...")
        st.write("The system is ready to receive and display real-time satellite environmental data.")


def render_iot_tab(latest_iot, redis_client):
    if latest_iot:
        aggregated = latest_iot.get("aggregated_detection", {})
        system_response = latest_iot.get("system_response", {})
        stations = latest_iot.get("stations", [])

        col = st.columns((1.3, 2.5, 1.3), gap="medium")

        # === LEFT COLUMN ===
        with col[0]:
            st.markdown("**Event Metadata**")
            ts = int(latest_iot.get("latest_event_timestamp", 0)) / 1000
            timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            region_id = latest_iot.get("region_id", "N/A")

            metadata_df = pd.DataFrame({
                "Field": ["Event Timestamp", "Area ID"],
                "Value": [timestamp, region_id]
            })
            metadata_df["Value"] = metadata_df["Value"].astype(str)
            st.dataframe(metadata_df, hide_index=True, use_container_width=True)

            st.markdown("**Detection Status**")
            detection_df = pd.DataFrame({
                "Field": ["Wildfire", "Alert Level", "Air Quality"],
                "Value": [
                    "Detected" if aggregated.get("wildfire_detected") else "Clear",
                    system_response.get("alert_level", "N/A").replace("_", " ").title(),
                    f"{aggregated.get('air_quality_status', 'N/A')} ({aggregated.get('air_quality_index', 0)})"
                ]
            })
            detection_df["Value"] = detection_df["Value"].astype(str)
            st.dataframe(detection_df, hide_index=True, use_container_width=True)

            st.markdown("**Fire Behavior Analysis**")
            fb = aggregated.get("fire_behavior", {})
            fire_df = pd.DataFrame({
                "Field": ["Spread Rate", "Direction", "Speed (mph)", "Ignition Time"],
                "Value": [
                    fb.get("spread_rate", "N/A"),
                    fb.get("direction", "N/A"),
                    fb.get("estimated_speed_mph", "N/A"),
                    aggregated.get("estimated_ignition_time", "N/A")
                ]
            })
            fire_df["Value"] = fire_df["Value"].astype(str)
            st.dataframe(fire_df, hide_index=True, use_container_width=True)

            st.markdown("**Weather Conditions**")
            environmental = latest_iot.get("environmental_context", {})
            weather = environmental.get("weather_conditions", {})
            weather_df = pd.DataFrame({
                "Field": ["Temperature (°C)", "Humidity (%)", "Wind Speed", "Wind Direction", "Precipitation Chance"],
                "Value": [
                    weather.get("temperature", "N/A"),
                    weather.get("humidity", "N/A"),
                    weather.get("wind_speed", "N/A"),
                    weather.get("wind_direction", "N/A"),
                    weather.get("precipitation_chance", "N/A")
                ]
            })
            weather_df["Value"] = weather_df["Value"].astype(str)
            st.dataframe(weather_df, hide_index=True, use_container_width=True)


        # === CENTER COLUMN ===
        with col[1]:
            st.markdown("**Sensor Map**")
            region_info = json.loads(redis_client.get(f"microarea:{latest_iot['region_id']}"))
            lat_center = (region_info["min_lat"] + region_info["max_lat"]) / 2
            lon_center = (region_info["min_long"] + region_info["max_long"]) / 2

            m = folium.Map(location=[lat_center, lon_center], zoom_start=10)
            folium.Polygon([
                [region_info["min_lat"], region_info["min_long"]],
                [region_info["min_lat"], region_info["max_long"]],
                [region_info["max_lat"], region_info["max_long"]],
                [region_info["max_lat"], region_info["min_long"]],
                [region_info["min_lat"], region_info["min_long"]],
            ], color="blue", fill_opacity=0.1).add_to(m)

            for station in stations:
                pos = station["station_metadata"]["position"]
                wildfire = station.get("detection_flags", {}).get("wildfire_detected", False)
                folium.Marker(
                    location=[pos["latitude"], pos["longitude"]],
                    popup=station.get("station_id", "Unknown"),
                    icon=folium.Icon(color="red" if wildfire else "blue")
                ).add_to(m)

            map_data = st_folium(m, width=650, height=380)

            # Enhanced Recommended Actions Section
            st.markdown("**Recommended Actions**")
            actions = system_response.get("recommended_actions", [])

            if actions:
                for i, action in enumerate(actions):
                    # Create different styling based on action type or priority
                    action_text = action['action'].replace('_', ' ').title()
                    
                    # Use different colors for different action types
                    if 'fire' in action['action'].lower() or 'deploy' in action['action'].lower():
                        color = "#ff6b6b"  # Red for urgent
                        
                    elif 'evacuate' in action['action'].lower():
                        color = "#ffa726"  # Orange for warning
                        
                    else:
                        color = "#42a5f5"  # Blue for general
                        
                    
                    st.markdown(
                        f"""
                        <div style='
                            background: linear-gradient(135deg, {color}15, {color}08);
                            border-left: 4px solid {color};
                            padding: 15px 20px;
                            border-radius: 8px;
                            margin-bottom: 12px;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        '>
                            <div style='
                                font-size: 16px;
                                font-weight: 600;
                                color: #2c3e50;
                                display: flex;
                                align-items: center;
                                gap: 8px;
                            '>
                                {action_text}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
            else:
                st.info("No recommended actions at this time")

            st.markdown("<div style='height: 30px;'></div>", unsafe_allow_html=True)


            # Enhanced Notifications Section
            st.markdown("**Notifications**")
            notifications = system_response.get("sent_notifications_to", [])

            if notifications:
                for notif in notifications:
                    agency_name = notif['agency'].replace('_', ' ').title()
                    status = notif['delivery_status']
                    timestamp = notif['notification_timestamp']
                    
                    # Style based on delivery status
                    if status.lower() == 'confirmed':
                        status_color = "#4caf50"  # Green
                    
                        bg_color = "#e8f5e8"
                    elif status.lower() == 'pending':
                        status_color = "#ff9800"  # Orange
                        
                        bg_color = "#fff3e0"
                    else:
                        status_color = "#f44336"  # Red
                        
                        bg_color = "#ffebee"
                    
                    st.markdown(
                        f"""
                        <div style='
                            background: {bg_color};
                            border-radius: 8px;
                            padding: 8px 12px;
                            margin-bottom: 8px;
                            border: 1px solid {status_color}30;
                        '>
                            <div style='
                                display: flex;
                                justify-content: space-between;
                                align-items: center;
                            '>
                                <div style='
                                    font-weight: 600;
                                    color: #2c3e50;
                                    font-size: 15px;
                                '>
                                    {agency_name}
                                </div>
                                <div style='
                                    font-size: 12px;
                                    color: #666;
                                    font-weight: 500;
                                '>
                                    {timestamp}
                                </div>
                            </div>
                            <div style='
                                margin-top: 4px;
                                font-size: 13px;
                                color: {status_color};
                                font-weight: 500;
                            '>
                                Status: {status.title()}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
            else:
                st.info("No notifications sent yet")

        # === RIGHT COLUMN ===
        with col[2]:
            st.markdown("**Severity Assessment**")
            severity = aggregated.get("severity_score", 0)
            st.metric(label="Severity", value=f"{severity:.2f}")

            st.markdown("**Sensor Measurements**")
            measurement_types = ["Temperature C", "Humidity Percent", "Co2 Ppm", "Pm25 Ugm3", "Smoke Index"]
            sensor_data = {m: " " for m in measurement_types}

            if map_data and map_data.get("last_object_clicked"):
                clicked = map_data["last_object_clicked"]
                for station in stations:
                    pos = station["station_metadata"]["position"]
                    if abs(pos["latitude"] - clicked["lat"]) < 0.0001 and abs(pos["longitude"] - clicked["lng"]) < 0.0001:
                        measurements = station.get("measurements", {})
                        if measurements:
                            for k, v in measurements.items():
                                label = k.replace("_", " ").title()
                                if label in sensor_data:
                                    sensor_data[label] = v
                        break

            measurement_df = pd.DataFrame(sensor_data.items(), columns=["Measurement", "Value"])
            measurement_df["Value"] = measurement_df["Value"].astype(str)
            st.dataframe(measurement_df, hide_index=True, use_container_width=True)

            st.markdown("**Terrain Info**")
            terrain = environmental.get("terrain_info", {})
            terrain_df = pd.DataFrame({
                "Field": ["Vegetation Type", "Vegetation Density", "Slope", "Aspect"],
                "Value": [
                    terrain.get("vegetation_type", "N/A"),
                    terrain.get("vegetation_density", "N/A"),
                    terrain.get("slope", "N/A"),
                    terrain.get("aspect", "N/A")
                ]
            })
            terrain_df["Value"] = terrain_df["Value"].astype(str)
            st.dataframe(terrain_df, hide_index=True, use_container_width=True)


            st.markdown("**At-Risk Assets**")
            asset_rows = []
            for c in system_response.get("at_risk_assets", {}).get("population_centers", []):
                asset_rows.append([
                    f"Population Center - {c['name']}",
                    f"{c['population']} people",
                    f"{c['distance_meters']} m",
                    f"{c['evacuation_priority'].title()} priority"
                ])
            for i in system_response.get("at_risk_assets", {}).get("critical_infrastructure", []):
                asset_rows.append([
                    f"Infrastructure - {i['name']}",
                    f"Type: {i['type'].title()}",
                    f"{i['distance_meters']} m",
                    f"{i['priority'].title()} priority"
                ])
            if asset_rows:
                asset_df = pd.DataFrame(asset_rows, columns=["Asset", "Details", "Distance", "Priority"])
                st.dataframe(asset_df, hide_index=True, use_container_width=True)
    else:
        st.info("🔄 Waiting for IoT data...")
        st.write("The system is ready to receive and display real-time IoT environmental data.")


def render_social_tab(latests_msg, redis_client=None):
    if "map_points" not in st.session_state:
        st.session_state.map_points = []
    if "category_counts" not in st.session_state:
        st.session_state.category_counts = {}
    if "category_history" not in st.session_state:
        st.session_state.category_history = {}
    if "processed_message_ids" not in st.session_state:
        st.session_state.processed_message_ids = set()

    pretty_labels = {
        "emergency_help_request": "Emergency Help",
        "infrastructure_or_property_damage": "Infrastructure Damage",
        "emotional_reaction_to_wildfire": "Emotional Reaction",
        "official_emergency_announcement": "Official Announcement"
    }
    label_to_category = {v: k for k, v in pretty_labels.items()}
    icon_map = {
        "emergency_help_request": ("exclamation-triangle", "red"),
        "infrastructure_or_property_damage": ("tools", "orange"),
        "emotional_reaction_to_wildfire": ("comment", "blue"),
        "official_emergency_announcement": ("bullhorn", "green"),
    }

    for cat in pretty_labels:
        st.session_state.category_counts.setdefault(cat, 0)
        st.session_state.category_history.setdefault(cat, [])

    st.markdown("## Social Media Alerts")

    top_col1, top_col2, top_col3 = st.columns([6, 1, 1])
    with top_col1:
        selected_label = st.selectbox("Category Filter", list(pretty_labels.values()), label_visibility="collapsed")
        selected_category = label_to_category[selected_label]
    with top_col2:
        update_clicked = st.button("Update", use_container_width=True)
    with top_col3:
        reset_clicked = st.button("Reset", use_container_width=True)

    if update_clicked:
        previous_counts = st.session_state.category_counts.copy()
        if latests_msg:
            for i, msg in enumerate(latests_msg):
                msg_id = f"{msg.get('timestamp')}_{i}_{msg.get('microarea_id')}"
                if msg_id not in st.session_state.processed_message_ids:
                    st.session_state.map_points.append(msg)
                    st.session_state.processed_message_ids.add(msg_id)
                    cat = msg.get("category")
                    if cat in st.session_state.category_counts:
                        st.session_state.category_counts[cat] += 1
        for cat in pretty_labels:
            st.session_state.category_history[cat].append(previous_counts.get(cat, 0))
            st.session_state.category_history[cat] = st.session_state.category_history[cat][-2:]

    if reset_clicked:
        st.session_state.map_points = []
        st.session_state.category_counts = {cat: 0 for cat in pretty_labels}
        st.session_state.category_history = {cat: [] for cat in pretty_labels}
        st.session_state.processed_message_ids = set()

    col_stats, col_map, col_msgs = st.columns([1.2, 2, 1.5])

    with col_stats:
        st.markdown("### Statistics")
        for cat in pretty_labels:
            count = st.session_state.category_counts.get(cat, 0)
            history = st.session_state.category_history.get(cat, [])
            delta = count - history[-1] if len(history) >= 2 else 0

            if delta > 0:
                delta_str = f"<span style='color: lightgreen;'> (+{delta})</span>"
            elif delta < 0:
                delta_str = f"<span style='color: #ff4d4d;'> ({delta})</span>"
            else:
                delta_str = f"<span style='color: gray;'> (0)</span>"

            st.markdown(
                f"<div style='color:white; font-size: 14px; margin-bottom: 8px;'>"
                f"<b>{pretty_labels[cat]}:</b> {count}{delta_str}</div>",
                unsafe_allow_html=True
            )

    with col_map:
        st.markdown("### Geographic Distribution")
        m = folium.Map(location=[36.7783, -119.4179], zoom_start=6)
        cluster = MarkerCluster().add_to(m)

        # Mostrar microárea (si hay mensajes y redis)
        if redis_client and st.session_state.map_points:
            first_msg = st.session_state.map_points[0]
            microarea_id = first_msg.get("microarea_id")
            if microarea_id:
                redis_key = f"microarea:{microarea_id}"
                region_info_json = redis_client.get(redis_key)
                if region_info_json:
                    try:
                        region_info = json.loads(region_info_json)
                        min_long = region_info.get("min_long")
                        min_lat = region_info.get("min_lat")
                        max_long = region_info.get("max_long")
                        max_lat = region_info.get("max_lat")

                        polygon_coords = [
                            [min_lat, min_long],
                            [min_lat, max_long],
                            [max_lat, max_long],
                            [max_lat, min_long],
                            [min_lat, min_long]
                        ]

                        folium.Polygon(
                            locations=polygon_coords,
                            color="blue",
                            weight=2,
                            fill=True,
                            fill_color="blue",
                            fill_opacity=0.08,
                            tooltip=f"Microarea {microarea_id}"
                        ).add_to(m)
                    except Exception as e:
                        st.warning(f"Error drawing microarea: {e}")

        for msg in st.session_state.map_points:
            if msg.get("category") != selected_category:
                continue
            try:
                lat, lon = float(msg["latitude"]), float(msg["longitude"])
                content = msg.get("message", "No content")
                truncated = content[:150] + "..." if len(content) > 150 else content
                popup = (
                    f"<b>Area:</b> {msg.get('microarea_id', 'N/A')}<br>"
                    f"<b>Time:</b> {msg.get('timestamp', 'N/A')}<br>"
                    f"<b>Category:</b> {selected_label}<hr>{truncated}"
                )
                icon_name, color = icon_map[selected_category]
                folium.Marker(
                    location=[lat, lon],
                    popup=folium.Popup(popup, max_width=280),
                    icon=folium.Icon(color=color, icon=icon_name, prefix="fa")
                ).add_to(cluster)
            except:
                continue

        st_folium(m, use_container_width=True, height=430)

    with col_msgs:
        st.markdown("### Recent Messages")
        filtered_msgs = [m for m in reversed(latests_msg[-30:]) if m.get("category") == selected_category]
        if filtered_msgs:
            for msg in filtered_msgs[:5]:
                ts = msg.get("timestamp", "N/A")
                area = msg.get("microarea_id", "N/A")
                cat = msg.get("category", "N/A").replace("_", " ").title()
                msg_txt = msg.get("message", "No content")
                msg_short = msg_txt[:110] + "..." if len(msg_txt) > 110 else msg_txt
                st.markdown(
                    f"""
                    <div style="background:#1e1e1e; color:white; padding:10px 12px;
                                border-radius:8px; margin-bottom:8px; height:auto;">
                        <div style="font-size:11px; color:#ccc;"><b>{area}</b> • {ts}</div>
                        <div style="font-size:12px; color:#aaa;">{cat}</div>
                        <div style="margin-top:4px; font-size:13px;">{msg_short}</div>
                    </div>
                    """, unsafe_allow_html=True
                )
        else:
            st.info("No messages found for selected category.")


def render_sidebar_status(social_messages, iot_data, sat_data, social_count=0, iot_count=0, sat_count=0):
    """Render the system status sidebar"""
    st.sidebar.header("🖥️ System Status")
    st.sidebar.write(f"📱 Social Messages: {len(social_messages)}")
    st.sidebar.write(f"🔧 IoT Readings: {len(iot_data)}")
    st.sidebar.write(f"🛰️ Satellite Data: {len(sat_data)}")
    st.sidebar.write(f"⏱️ Last Update: {datetime.now().strftime('%H:%M:%S')}")
    
    # Show processing stats
    if social_count > 0 or iot_count > 0 or sat_count > 0:
        st.sidebar.write("📊 Last Batch:")
        st.sidebar.write(f"  Social: {social_count}")
        st.sidebar.write(f"  IoT: {iot_count}")
        st.sidebar.write(f"  Satellite: {sat_count}")

    # System health indicator
    if social_messages or iot_data or sat_data:
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

        