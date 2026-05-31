-- Core schema for the NYC Taxi ETL pipeline.

CREATE DATABASE IF NOT EXISTS taxi;

-- Migration tracking.
-- Records applied schema migrations and their checksums.

CREATE TABLE IF NOT EXISTS taxi.schema_migrations
(
    version     String,
    applied_at  DateTime DEFAULT now(),
    checksum    String
)
ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version;

-- Taxi zone lookup table.
-- Static reference data used during enrichment and for ad-hoc analysis.
-- Trips are enriched at ingest time, so analytical queries typically do not
-- require joining against this table.

CREATE TABLE IF NOT EXISTS taxi.zones
(
    location_id     UInt16,
    borough         LowCardinality(String),
    zone            String,
    service_zone    LowCardinality(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY location_id;

-- Main trips fact table.
--
-- Engine: ReplacingMergeTree(ingested_at)
--   trip_id is a deterministic SHA-256 hash of
--   (vendor_id, pickup_datetime, dropoff_datetime, pickup_location_id).
--   Kafka replays produce duplicate records with the same trip_id and
--   ORDER BY key. ReplacingMergeTree keeps the latest version by
--   ingested_at during background merges, enabling idempotent ingestion.
--
-- Partitioning:
--   Data is partitioned by pickup month using
--   PARTITION BY toYYYYMM(pickup_datetime).
--   Monthly partitions make retention management and large deletes efficient.
--
-- ORDER BY:
--   Data is sorted by (pickup_datetime, vendor_id, trip_id).
--   The leading columns match common query filters, enabling efficient
--   time-range scans and vendor-specific analysis.

CREATE TABLE IF NOT EXISTS taxi.trips
(
    -- Identity
    trip_id                 String,
    vendor_id               LowCardinality(String),

    -- Timestamps
    pickup_datetime         DateTime,
    dropoff_datetime        DateTime,
    trip_duration_seconds   UInt32,

    -- Passengers and distance
    passenger_count         UInt8,
    trip_distance           Float32,

    -- Location (raw IDs and enriched zone metadata)
    pickup_location_id      UInt16,
    dropoff_location_id     UInt16,
    pickup_zone             LowCardinality(String),
    dropoff_zone            LowCardinality(String),
    pickup_borough          LowCardinality(String),
    dropoff_borough         LowCardinality(String),

    -- Fare breakdown
    fare_amount             Decimal(9, 2),
    extra                   Decimal(9, 2),
    mta_tax                 Decimal(9, 2),
    tip_amount              Decimal(9, 2),
    tolls_amount            Decimal(9, 2),
    improvement_surcharge   Decimal(9, 2),
    congestion_surcharge    Decimal(9, 2),
    airport_fee             Decimal(9, 2),
    total_amount            Decimal(9, 2),

    -- Classification
    payment_type            LowCardinality(String),
    rate_code               LowCardinality(String),
    store_and_fwd_flag      LowCardinality(String),

    -- ETL metadata
    ingested_at             DateTime DEFAULT now(),
    batch_id                String,
    source_file             LowCardinality(String)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (pickup_datetime, vendor_id, trip_id)
SETTINGS index_granularity = 8192;
