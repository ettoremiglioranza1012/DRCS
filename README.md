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

## 🧪 Enviroment Setup

> Create a virtual environment and install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

### 🗂️ 1. Generate Microarea Grid

Run this script once to populate the PostgreSQL database with grid data for each macroarea:

```bash
python geo_grid_processor.py
```

It will:

- Read each `macroarea_{i}.json` GeoJSON file in `Macro_data/Macro_input`
- Create a grid of microareas inside it
- Store each bounding box in tables `macro_area_i`
- Update a global tracking table `n_microareas`

You can inspect the output GeoJSONs in `Macro_data/Macro_output` (reconstructed macrogrid for sanity check from microgrid areas).

---

### 🔐 2. SentinelHub Configuration (REQUIRED)

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



