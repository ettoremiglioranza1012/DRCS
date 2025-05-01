## 🛰️ Input Layer Setup: Satellite Data Grid & Image Acquisition

This section outlines how to set up the **Input Layer** of the Disaster Response Coordination System, responsible for preparing and fetching geospatial data from SentinelHub and storing it in PostgreSQL.

---

### 🧩 Architecture Overview

The Input Layer is divided into two main Python modules:

1. **`geo_grid_processor.py`**  
   Generates a grid of microareas from macroarea GeoJSONs and stores their bounding boxes in a PostgreSQL database.

2. **`image_fetcher_and_processor.py`**  
   Fetches true color images for microareas via SentinelHub API and displays them for validation.

Each macroarea is defined in a file `Satellite/Macro_input/macroarea_i.json`, where `i ∈ [1,7]`.

---

### 🐳 Docker Setup for PostgreSQL

A PostgreSQL service is defined via Docker Compose:

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:14
    restart: always
    environment:
      POSTGRES_USER: gruppo3
      POSTGRES_PASSWORD: gruppo3
      POSTGRES_DB: california_db
    ports:
      - "5433:5432"
    volumes:
      - ./db_data:/var/lib/postgresql/data
```

Start the DB with:

```bash
docker-compose up -d
```

Then verify connection with:

```bash
psql -h localhost -p 5433 -U gruppo3 -d california_db
```

---

### 🗂️ 1. Generate Microarea Grid

Run this script once to populate the PostgreSQL database with grid data for each macroarea:

```bash
python geo_grid_processor.py
```

It will:

- Read each `macroarea_{i}.json` GeoJSON file
- Create a grid of microareas inside it
- Store each bounding box in tables `macro_area_i`
- Update a global tracking table `n_microareas`

You can inspect the output GeoJSONs in `Satellite/Macro_output` (reconstructed macrogrid for sanity check from microgrid areas).

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

### 🛰️ 3. Fetch Sentinel-2 True Color Images

Once the config is in place and the microarea grid is stored in DB, run:

```bash
python image_fetcher_and_processor.py
```

This script will:

- Randomly sample one microarea per macroarea
- Send a SentinelHub request for a true color image (RGB)
- Display the image after rescaling and clipping

Make sure you have a valid `config.toml` file in `~/.config/sentinelhub/`.

---

### 🧪 Utilities Used

- `geo_utils.py`: geometry transformations and plotting
- `db_utils.py`: PostgreSQL connection helpers
- `sentinelhub` library for API calls (install via `pip install sentinelhub`)

---

### 🛠️ Notes

- PostgreSQL binds on port `5433`, not the default `5432`
- The bounding boxes are inserted dynamically and vary by macroarea geometry
- SentinelHub requires registration at [Copernicus Open Access Hub](https://dataspace.copernicus.eu)

---

