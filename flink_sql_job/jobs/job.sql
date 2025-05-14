-- Flink SQL job: ingest from Kafka, persist to Iceberg (MinIO), aligned to 1-minute tumbling windows

-- Step 1: Create the Iceberg catalog (MinIO-backed)
CREATE CATALOG my_iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hadoop',
  'warehouse' = 's3a://bronze/',
  'property-version' = '1',
  'io-impl' = 'org.apache.iceberg.hadoop.HadoopFileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key-id' = 'minioadmin',
  's3.secret-access-key' = 'minioadmin'
);

-- Switch to default catalog to create Kafka sources
USE CATALOG default_catalog;

-- Step 2: Kafka sources
CREATE TABLE iot_input (
  station_id STRING,
  ts TIMESTAMP(3),
  measurements ROW<
    temperature_c DOUBLE,
    humidity_percent DOUBLE,
    co2_ppm DOUBLE,
    pm25_ugm3 DOUBLE,
    smoke_index DOUBLE,
    infrared_intensity DOUBLE,
    battery_voltage DOUBLE
  >,
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor_meas',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-iot-consumer',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE sat_input (
  image_pointer STRING,
  ts TIMESTAMP(3),
  bands ROW<
    B2 DOUBLE,
    B3 DOUBLE,
    B4 DOUBLE,
    B8 DOUBLE,
    B8A DOUBLE,
    B11 DOUBLE,
    B12 DOUBLE
  >,
  WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'satellite_img',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-satimg-consumer',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Switch back to Iceberg catalog for sink tables
USE CATALOG my_iceberg_catalog;

-- Step 3: Iceberg sinks (bronze layer)
CREATE TABLE bronze.iot_raw (
  station_id STRING,
  ts TIMESTAMP(3),
  temperature_c DOUBLE,
  humidity_percent DOUBLE,
  co2_ppm DOUBLE,
  pm25_ugm3 DOUBLE,
  smoke_index DOUBLE,
  infrared_intensity DOUBLE,
  battery_voltage DOUBLE
)
PARTITIONED BY (ts)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet'
);

CREATE TABLE bronze.sat_raw (
  image_pointer STRING,
  ts TIMESTAMP(3),
  B2 DOUBLE,
  B3 DOUBLE,
  B4 DOUBLE,
  B8 DOUBLE,
  B8A DOUBLE,
  B11 DOUBLE,
  B12 DOUBLE
)
PARTITIONED BY (ts)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet'
);

-- Step 4: Align to 1-minute tumbling window and persist
INSERT INTO bronze.iot_raw
SELECT
  station_id,
  TUMBLE_START(ts, INTERVAL '1' MINUTE) AS ts,
  measurements.temperature_c,
  measurements.humidity_percent,
  measurements.co2_ppm,
  measurements.pm25_ugm3,
  measurements.smoke_index,
  measurements.infrared_intensity,
  measurements.battery_voltage
FROM iot_input
GROUP BY
  station_id,
  TUMBLE(ts, INTERVAL '1' MINUTE),
  measurements;

INSERT INTO bronze.sat_raw
SELECT
  image_pointer,
  TUMBLE_START(ts, INTERVAL '1' MINUTE) AS ts,
  bands.B2,
  bands.B3,
  bands.B4,
  bands.B8,
  bands.B8A,
  bands.B11,
  bands.B12
FROM sat_input
GROUP BY
  image_pointer,
  TUMBLE(ts, INTERVAL '1' MINUTE),
  bands;

