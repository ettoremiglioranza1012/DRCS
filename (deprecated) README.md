# 🌍 Disaster Response Coordination System (DRCS)

A modular Big Data pipeline for real-time acquisition and processing of multi-source data in disaster-affected areas.  
This system collects information from **satellite images (Sentinel-2)**, **IoT sensors**, and integrates it via Kafka-based streaming with PostgreSQL and MinIO for analysis and visualization.

---

## 🛰️ System Overview

The **DCS** performs the following:

1. **Geospatial Grid Construction**
   - Splits each macroarea (e.g., `A1`, `A2`, ..., `A5`) into smaller **microareas**.
   - Stores bounding boxes and identifiers in a PostgreSQL database.
   - Each microarea is uniquely identified (e.g., `A1-M10`) and stored in dedicated tables.
   - Generate dimensional data for sensors stations (e.g., `A1-M10-S1`, `A1-M10-S2`, etc.) for each microarea.

2. **Image Acquisition and Preprocessing**
   - Fetches true color images for each microarea from SentinelHub.
   - Applies synthetic filters to simulate wildfires.
   - Compresses and serializes each image into a base64 string.
   - Combines the image with synthetic metadata into a JSON payload.

3. **Sensors Data Generation and Acquisition**
   - Generates synthetic sensor data for each microarea.
   - Combines the sensor data with the image payload into a single JSON object.

4. **Kafka Streaming**
   - For each image sends a pointer-to-the-image+metadata payload to a Kafka topic for satellite images. Uses high-durability Kafka producer settings (`acks='all'`, `retries=5`, `sync send`) for guaranteed delivery for satellite images.
   - Sends each sensor data payload to a different Kafka topic for sensors data. Uses asynchronous producer settings (`acks='all'`, `retries=5`, `async send`) for sensor data.
   - Use macroarea ID as the partition key for the satellite images topic to ensure that all images from a specific macroarea are sent to the same partition. This allows for easier processing and retrieval of images based on their geographic location, and maximisation of parallel processing.
   - Hashes the macroarea ID for both topics to ensure consistent partitioning.

5. **Storage**:
   - Stores images in MinIO, a lightweight S3-compatible object storage.
   - Uses PostgreSQL for structured data storage and querying.

---

### 🛠️ General notes 

- The system is designed to be modular and extensible. Macro areas can be added or removed easily. For each macro area, a separate image producer and sensor producer container must be created manually. Right now, the system is not designed to automatically create new containers for new macro areas.
- The system is designed to be run in a Docker container. The orchestrator and producers are all running in separate containers, which communicate with each other via Kafka topics.
- The bounding boxes are inserted dynamically and vary by macroarea geometry
- SentinelHub requires registration at [Copernicus Open Access Hub](https://dataspace.copernicus.eu)
- Kafka cluster is a single broker configuration for this prototype version. 

---

### Current Limitations

- Currently, every image producer and sensor producer is hardcoded to a specific macro area. This means that if you want to add a new macro area, you need to create a new image producer and sensor producer container manually.
- Additionally, every producer is right now capable of producing data only for a specific micro area out of all the micro areas in the macro area. This current set up is done due to the fact that when prototyping, we wanted to keep the system as simple as possible to avoid overhead. With proper hardware and a proper setup, the system can be easily extended to produce data for all microareas in a macro area. To do this, there are two possible ways:
  - Create a new producer for each micro area in the macro area. This is the easiest way to do it, but it requires a lot of manual work.
   - Create a new producer for each macro area, and then use the existing producers to produce data for all micro areas in the macro area, maybe through subprocessing or threads. This is a more complex solution, but it allows for more flexibility and scalability in the future.
- Currently, the timestamp of the event satellite image is handled during serialisation. However, this removes the delay of the fetching phase from the timestamp, making things simpler, but more inacurate. To make things more precise, the timestamp should be taken before the fetch and the watermark interval should be wider than the current 10s, I think. 

---

### 🔐 SentinelHub Configuration (REQUIRED)

Before running image acquisition containers, you must **configure your SentinelHub credentials** in a `config.toml` file. This file is used by the `sentinelhub-py` library to authenticate requests:

- Go in the Config folder of the image producers containers (e.g. imgproducer_m5) and make sure to replace placeholders in `config.toml` with your own `client_id` and `client_secret`.  
- These are obtained by registering your app at:  
👉 [https://dataspace.copernicus.eu](https://dataspace.copernicus.eu)
- An example of a configuration file .toml ready to use is provided inside the image producers containers.
- For detailed help, refer to the official setup guide:  
🔗 [sentinelhub-py configuration](https://sentinelhub-py.readthedocs.io/en/latest/configure.html)

```toml
[default]
sh_client_id = "your-client-id"
sh_client_secret = "your-client-secret"
sh_base_url = "https://sh.dataspace.copernicus.eu"
sh_token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
```

---

### 🧩 Setup Ochestrator

The setup_orchestrator/ service is responsible for initializing the environment:

- Divide input geoJSONs macro areas into micro areas that are at max. 500 km^2 (APIs limit for fetching imgs). Generate sensors stations for each microarea.
- Populates PostgreSQL with microarea geometries and simulated IoT sensor station metadata.
- Reconstructs spatial macroarea grids from generated microareas in GeoJSONs in Macro_data/Macro_input/ so that user can check the results.
- Creates required Kafka topics (satellite_img, sensor_meas, ...).
- Can be run independently or automatically via docker-compose. 

---

### 🛰️ Image Producer (Sentinel-2 Acquisition + Processing)

Located in imgproducer_mN/, where N means macro area number ID, this container:

- Streams real-time true-color Sentinel-2 images for each macroarea at regular intervals
- Applies simulated wildfire filters and encodes the images
- Pushes metadata (synthetically generated) + pointers to images (stored in MinIO) into the Kafka satellite_img topic
- Uses the SentinelHub API and your config.toml credentials

🧠 Image size, resolution, and streaming intervals are fully configurable.

Make sure you have a valid `config.toml` file in `~/Config`.

---

### Sensor Data Producer

Located in sensproducer_mN/, where N is the macro area number ID, this container:

- Simulates real-time emission of environmental data (e.g., CO₂, temperature, PM2.5)
- Fetches the number and coordinates of sensor stations from the PostgreSQL database
- Streams real-time measurements to Kafka topic sensor_meas, partitioned by macroarea
- Sensors and their dimensional metadata are initially generated by the orchestrator.

---

### 🚀 Getting Started

1. Configure config.toml, e.g. in `imgproducer_m5/Config/config.toml`:

```bash
cd imgproducer_m5/Config/
# Edit config.toml with your Sentinel credentials
```

2. Run the entire stack with Docker Compose:

```bash
docker-compose up --build
```

⏳ On first run, allow 30–90s for services (Kafka, MinIO, Postgres) to initialize.
The orchestrator will retry until everything is ready.

--- 

### 🧪 Useful Commands

```bash
# View logs of producers
docker-compose logs -f img_producer
docker-compose logs -f sens_producer

# Clean up everything (be careful!)
docker-compose down -v --remove-orphans
```

