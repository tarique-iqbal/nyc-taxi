-- Migration: 001
-- Description: Initial schema -- database, migration tracking, zones and trips tables
-- Applied by: infrastructure/clickhouse/schema_manager.py

CREATE DATABASE IF NOT EXISTS taxi;

CREATE TABLE IF NOT EXISTS taxi.schema_migrations
(
    version     String,
    applied_at  DateTime DEFAULT now(),
    checksum    String
)
ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version;

CREATE TABLE IF NOT EXISTS taxi.zones
(
    location_id     UInt16,
    borough         LowCardinality(String),
    zone            String,
    service_zone    LowCardinality(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY location_id;

CREATE TABLE IF NOT EXISTS taxi.trips
(
    trip_id                 String,
    vendor_id               LowCardinality(String),
    pickup_datetime         DateTime,
    dropoff_datetime        DateTime,
    trip_duration_seconds   UInt32,
    passenger_count         UInt8,
    trip_distance           Float32,
    pickup_location_id      UInt16,
    dropoff_location_id     UInt16,
    pickup_zone             LowCardinality(String),
    dropoff_zone            LowCardinality(String),
    pickup_borough          LowCardinality(String),
    dropoff_borough         LowCardinality(String),
    fare_amount             Decimal(9, 2),
    extra                   Decimal(9, 2),
    mta_tax                 Decimal(9, 2),
    tip_amount              Decimal(9, 2),
    tolls_amount            Decimal(9, 2),
    improvement_surcharge   Decimal(9, 2),
    congestion_surcharge    Decimal(9, 2),
    airport_fee             Decimal(9, 2),
    total_amount            Decimal(9, 2),
    payment_type            LowCardinality(String),
    rate_code               LowCardinality(String),
    store_and_fwd_flag      LowCardinality(String),
    ingested_at             DateTime DEFAULT now(),
    batch_id                String,
    source_file             LowCardinality(String)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (pickup_datetime, vendor_id, trip_id)
SETTINGS index_granularity = 8192;
