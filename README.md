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

--- 

**Developer Notes: Message Durability and Design Choices**
-------------------------------------------------------

Our disaster recovery system requires reliable, time-synchronized delivery of large satellite images.
To guarantee that **no image is lost or silently dropped**, we adopt the following Kafka producer strategies:

1. Why In-Sync Replicas (ISR) Matter:
- Each Kafka partition can have multiple replicas: 1 leader + N followers.
- Only followers that are fully up-to-date with the leader are considered "in-sync" (ISR).
- We want to ensure that **messages are not just written to the leader**, but also **replicated to at least one other broker**.
- In production, this ensures that **if a broker crashes, no data is lost**
2. Why Synchronous Sends (`future.get()`):
- Kafka sends are asynchronous by default, which is high-performance but risky in critical systems.
- We use `.get(timeout=...)` to block the producer until Kafka confirms the message was received and written.
- This gives **immediate failure feedback** if something goes wrong (e.g., not enough ISRs, network issue).
- It ensures we don’t move on until we’re sure the image is safely persisted
3. Why `acks='all'`:
- This setting waits for **all in-sync replicas** to confirm the write.
- It provides the strongest delivery guarantee Kafka supports.
- In production (with replication.factor=3+), this ensures that data is acknowledged by at least 2+ brokers (given min.insync.replicas=2+).
- In development (single broker), it behaves like `acks='1'` but prepares the system for stronger consistency when scaled
4. Why `retries=5`:
- Transient errors like leader reelection, broker unavailability, or network glitches may cause temporary send failures.
- By configuring `retries=5`, the producer will automatically attempt to resend a failed message up to 5 times **before giving up**.
- This significantly improves robustness under load or during broker transitions, without requiring manual retry logic.
- Retried sends still respect `acks='all'` and ISR guarantees — meaning no compromise on consistency.

**Kafka Producer Configuration Notes** 
----------------------------------

This code uses `acks='all'` to ensure strong delivery guarantees.
While prototyping on a single broker, this setup is safe and valid.

Current Development Setup:
- Brokers: 1
- replication.factor = 1
- min.insync.replicas = 1 (default)
- acks = 'all' behaves like acks = '1' (only the leader exists)
- Synchronous send (`future.get()`) ensures message delivery or raises exceptio

Future Production Setup (Scalable):
- Brokers: 3+
- replication.factor = 3+
- min.insync.replicas = 2+
- acks = 'all' ensures write is replicated to at least 2+ brokers
- Protects against data loss if one broker crashes
- No code changes needed (at least not here) — same producer config becomes stronger automaticall
Note:
Avoid overriding `min.insync.replicas` in development if using default broker configs.
Just document the intent and apply topic-level configs at deployment time.

**Flink consumer Configuration Notes**
----------------------------------

### Current Implementation

* **Retry Logic**
  Implemented robust retry strategies for connecting Flink to all dependent services (Redis, MinIO, Kafka, etc.) after container startup. Ensures full system readiness via a single `docker-compose up`, with no manual intervention required.

* **Raw Data Ingestion – Bronze Layer**
  Incoming raw sensor data is streamed and persisted to an S3-compatible MinIO data lake, under the `bronze` bucket. No transformation is applied at this stage.

* **Windowed Processing in Flink**

  * Tumbling window of **1 minute**.
  * **Watermark** strategy: 5 seconds.
  * Grouped by `station_id`.
  * Uses **event timestamp** (not processing time), providing a more realistic and meaningful temporal basis despite the added complexity.

* **Anomaly Filtering + Aggregation**

  * Data is processed **in parallel per `station_id`** within each window.
  * A set of hard-coded threshold-based rules filters the data.
  * Only measurements exceeding thresholds are kept.
  * For those, a **mean aggregation** is computed → results in **one JSON per minute per station with anomalies**.

  #### Design Rationale

  This approach is ideal for wildfire and environmental monitoring:

  * 🔻 **Efficient Volume Reduction**: Detailed data from stations with normal readings is discarded, reducing payload size.
  * 👁 **Maintained Situational Awareness**: All stations are tracked—whether they report anomalies or not.
  * 📍 **Complete Metadata**: Both normal and anomaly records include metadata for spatial context.
  * ⚠️ **Clear Status Indicators**: Differentiation between normal and anomaly records via `status`/`message` vs. `measurements`/`detection_flags`.
  * 🕒 **Timestamp Preservation**: Timestamps are kept in both cases to ensure temporal continuity.

  The current design strikes the right balance:

  * Keeps “heartbeat” data for normal stations (they’re alive and reporting).
  * Preserves full measurement detail only when anomalies are detected.
  * Enables temporal and spatial analysis across all stations.

* **Enrichment via Redis (In-Memory)**

  * A Redis container is integrated into the system.
  * Dimensional metadata is preloaded into Redis at startup.
  * Flink queries Redis during stream processing for super-fast enrichment, with full retry logic ensuring reliability.

---

### Next Steps

* [ ] **Silver Layer Output**
  Save the enriched, per-minute anomaly summaries into the MinIO `silver` bucket for persistent storage and downstream usage.

* [ ] **Gold Layer Aggregation**

  * Aggregate all per-minute enriched records into a **single JSON per minute**.
  * Compute a **severity score**, based on:

    * The degree to which measurements exceed thresholds.
    * The proportion of affected stations out of the total.
  * (Optional) Add enrichment such as:

    * Geographic bounding boxes for event area.
    * Nearby facilities or infrastructure that may require evacuation.
    * Vegetation type and density.
  * (Optional) Enable automatic response logic (e.g. alerts to firefighters or civilians).

* [ ] **Forecasting & Advanced Augmentation (Optional)**

  * Integrate **weather forecast APIs** (Gino is already exploring this for early warning features).
  * Use forecasted and real-time data in the **severity score computation**.
  * (Very Optional) Add **unsupervised ML models** (e.g. anomaly detection, decision trees) to support detection of unusual event patterns and severity scoring.

---

Notes (To remove):

Maybe necessary
ettor@LAPTOP-GTACQJV3 MINGW64 /c/Projects/DisasterPipeline/Dockeraize_DCRS/flink_sql_job (ettore)
$ chmod +x wait-for-it.sh

Tot line of code= roughly 3500 lines of code

Maven Central
Docker Hub

