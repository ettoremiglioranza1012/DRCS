## 🛰️ System Overview

The **DCS** performs the following:

1. **Geospatial Grid Construction**
   - Splits each macroarea (e.g., `A1`, `A2`, ..., `A5`) into smaller **microareas**.
   - Stores bounding boxes and identifiers in a PostgreSQL database.
   - Each microarea is uniquely identified (e.g., `A1-M10`) and stored in dedicated tables.

2. **Image Acquisition and Preprocessing**
   - Fetches true color images for each microarea from SentinelHub.
   - Applies synthetic filters to simulate wildfires.
   - Compresses and serializes each image into a base64 string.
   - Combines the image with synthetic metadata into a JSON payload.

3. **Kafka Streaming**
   - Sends each image+metadata payload to a Kafka topic based on macroarea ID.
   - Hashes the microarea ID to ensure consistent partitioning.
   - Uses high-durability Kafka producer settings (`acks='all'`, `retries=5`, `sync send`) for guaranteed delivery.

---

## 🐳 Docker Setup

The `docker-compose.yml` spins up the necessary services:

```bash
docker-compose up -d
```

It launches:
- ✅ PostgreSQL (bound on port `5433`)
- ✅ Zookeeper (Kafka cluster coordination)
- ✅ Kafka broker

Verify connection to database:
Then verify connection with:

```bash
psql -h localhost -p 5433 -U gruppo3 -d california_db
```

---

### 🔐 SentinelHub Configuration (REQUIRED)

Before running image acquisition scripts, you must **configure your SentinelHub credentials** locally:

1. Create the config directory (if it doesn't exist):

```bash
mkdir -p ~/.config/sentinelhub
```

2. Copy the provided config file into it:

```bash
cp Satellite_imgs/config.toml ~/.config/sentinelhub/config.toml
```

3. Make sure to replace placeholders in `config.toml` with your own `client_id` and `client_secret`.  
These are obtained by registering your app at:  
👉 [https://dataspace.copernicus.eu](https://dataspace.copernicus.eu)

An example of a configuration file .toml ready to use is provided inside the Satellite_imgs.

For detailed help, refer to the official setup guide:  
🔗 [sentinelhub-py configuration](https://sentinelhub-py.readthedocs.io/en/latest/configure.html)

---

### 🧩 3. Configure Kafka Topics and Partitions

```bash
python kafka_cluster_config.py
```

- Creates 5 Kafka topics (`satellite_imgs_A1`, ..., `A5`)
- Assigns each microarea to a Kafka partition using its ID
- Partitions are based on the number of microareas in each macroarea

---

### 🛰️ 4. Fetch Sentinel-2 True Color Images

Once the config is in place and the microarea grid is stored in DB, run:

```bash
python stream_example.py
```

This script will:

- Randomly sample one microarea per macroarea
- Send a SentinelHub request for a true color image (RGB)
- Display the image after rescaling and clipping

Make sure you have a valid `config.toml` file in `~/.config/sentinelhub/`.

---

### 🧪 Utilities Used

- `sentinelhub` library for API calls (install via `pip install sentinelhub`)
- `geo_utils.py`: geometry transformations and reconstruction, plotting, compression and serialization
- `db_utils.py`: PostgreSQL connection helpers
- `imgfetch_utils.py`: fetching images from dataspace copernicus EU sentinel hub and processing them
- `imgfilter_utils.py`: modify images to apply wildfires effects
- `stream_utils.py`: Data flow streaming functions, with Kafka producer init and message sending

---

### 🛠️ Notes

- PostgreSQL binds on port `5433`, not the default `5432`
- The bounding boxes are inserted dynamically and vary by macroarea geometry
- SentinelHub requires registration at [Copernicus Open Access Hub](https://dataspace.copernicus.eu)
- Kafka cluster is a single broker configuration for this prototype version. 
- Some Kafka default configurations, such as message_max_bytes have been overrided for playload size reasons.

---

Developer Notes: Message Durability and Design Choices
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

Kafka Producer Configuration Notes 
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


