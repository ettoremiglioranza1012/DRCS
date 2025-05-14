USE CATALOG default_catalog;

CREATE TABLE iot_stream (
    station_id STRING,
    timestamp TIMESTAMP(0),

    measurements ROW<
        temperature_c DOUBLE,
        humidity_percent DOUBLE,
        co2_ppm DOUBLE,
        pm25_ugm3 DOUBLE,
        smoke_index DOUBLE,
        infrared_intensity DOUBLE,
        battery_voltage DOUBLE
    >,

    WATERMARK FOR timestamp AS timestamp - INTERVAL '5' SECOND  
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor_meas',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-iot-consumer',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE sat_stream (
    image_pointer STRING,
    timestamp TIMESTAMP(0),

    bands ROW<
        B2 DOUBLE,
        B3 DOUBLE,
        B4 DOUBLE,
        B8 DOUBLE,
        B8A DOUBLE,
        B11 DOUBLE,
        B12 DOUBLE
    >,

    WATERMARK FOR timestamp AS timestamp - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'satellite_img',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-satimg-consumer',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE CATALOG my_iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hadoop',
  'warehouse' = 's3a://bronze/',
  'property-version' = '1',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key-id' = 'minioadmin',
  's3.secret-access-key' = 'minioadmin'
);

USE CATALOG my_iceberg_catalog;

CREATE TABLE persisted_iot (
    station_id STRING,
    timestamp TIMESTAMP(0),
    temperature_c DOUBLE,
    humidity_percent DOUBLE,
    co2_ppm DOUBLE,
    pm25_ugm3 DOUBLE,
    smoke_index DOUBLE,
    infrared_intensity DOUBLE,
    battery_voltage DOUBLE
) WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'partition-spec' = 'ts_hour:hour(timestamp)'
);

CREATE TABLE persisted_sat (
    image_pointer STRING,
    timestamp TIMESTAMP(0),
    B2 DOUBLE,
    B3 DOUBLE,
    B4 DOUBLE,
    B8 DOUBLE,
    B8A DOUBLE,
    B11 DOUBLE,
    B12 DOUBLE
) WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'partition-spec' = 'ts_hour:hour(timestamp)'
);

INSERT INTO persisted_iot
SELECT
    station_id,
    timestamp,
    measurements.temperature_c,
    measurements.humidity_percent,
    measurements.co2_ppm,
    measurements.pm25_ugm3,
    measurements.smoke_index,
    measurements.infrared_intensity,
    measurements.battery_voltage   
FROM default_catalog.iot_stream;

INSERT INTO persisted_sat
SELECT
    image_pointer,
    timestamp,
    bands.B2,
    bands.B3,
    bands.B4,
    bands.B8,
    bands.B8A,
    bands.B11,
    bands.B12
FROM default_catalog.sat_stream;

