-- Migration: 002
-- Description: Materialized views for sub-second dashboard aggregations
-- Depends on: 001_initial_schema.sql
-- Applied by: infrastructure/clickhouse/schema_manager.py

CREATE TABLE IF NOT EXISTS taxi.trips_hourly_mv
(
    hour                    DateTime,
    vendor_id               LowCardinality(String),
    trip_count              AggregateFunction(count),
    total_fare              AggregateFunction(sum, Decimal(9, 2)),
    avg_fare                AggregateFunction(avg, Decimal(9, 2)),
    total_distance          AggregateFunction(sum, Float32),
    avg_distance            AggregateFunction(avg, Float32),
    avg_duration_seconds    AggregateFunction(avg, UInt32),
    total_passengers        AggregateFunction(sum, UInt8)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, vendor_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS taxi.trips_hourly
TO taxi.trips_hourly_mv
AS
SELECT
    toStartOfHour(pickup_datetime)      AS hour,
    vendor_id,
    countState()                        AS trip_count,
    sumState(total_amount)              AS total_fare,
    avgState(total_amount)              AS avg_fare,
    sumState(trip_distance)             AS total_distance,
    avgState(trip_distance)             AS avg_distance,
    avgState(trip_duration_seconds)     AS avg_duration_seconds,
    sumState(passenger_count)           AS total_passengers
FROM taxi.trips
GROUP BY hour, vendor_id;

CREATE TABLE IF NOT EXISTS taxi.trips_daily_mv
(
    day                     Date,
    vendor_id               LowCardinality(String),
    payment_type            LowCardinality(String),
    trip_count              AggregateFunction(count),
    total_fare              AggregateFunction(sum, Decimal(9, 2)),
    avg_fare                AggregateFunction(avg, Decimal(9, 2)),
    avg_tip                 AggregateFunction(avg, Decimal(9, 2)),
    total_distance          AggregateFunction(sum, Float32),
    avg_distance            AggregateFunction(avg, Float32),
    avg_duration_seconds    AggregateFunction(avg, UInt32)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, vendor_id, payment_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS taxi.trips_daily
TO taxi.trips_daily_mv
AS
SELECT
    toDate(pickup_datetime)             AS day,
    vendor_id,
    payment_type,
    countState()                        AS trip_count,
    sumState(total_amount)              AS total_fare,
    avgState(total_amount)              AS avg_fare,
    avgState(tip_amount)                AS avg_tip,
    sumState(trip_distance)             AS total_distance,
    avgState(trip_distance)             AS avg_distance,
    avgState(trip_duration_seconds)     AS avg_duration_seconds
FROM taxi.trips
GROUP BY day, vendor_id, payment_type;

CREATE TABLE IF NOT EXISTS taxi.trips_by_borough_mv
(
    day                     Date,
    pickup_borough          LowCardinality(String),
    dropoff_borough         LowCardinality(String),
    trip_count              AggregateFunction(count),
    total_fare              AggregateFunction(sum, Decimal(9, 2)),
    avg_fare                AggregateFunction(avg, Decimal(9, 2)),
    avg_distance            AggregateFunction(avg, Float32)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, pickup_borough, dropoff_borough);

CREATE MATERIALIZED VIEW IF NOT EXISTS taxi.trips_by_borough
TO taxi.trips_by_borough_mv
AS
SELECT
    toDate(pickup_datetime)             AS day,
    pickup_borough,
    dropoff_borough,
    countState()                        AS trip_count,
    sumState(total_amount)              AS total_fare,
    avgState(total_amount)              AS avg_fare,
    avgState(trip_distance)             AS avg_distance
FROM taxi.trips
GROUP BY day, pickup_borough, dropoff_borough;

CREATE TABLE IF NOT EXISTS taxi.trips_by_payment_mv
(
    day                     Date,
    payment_type            LowCardinality(String),
    trip_count              AggregateFunction(count),
    total_fare              AggregateFunction(sum, Decimal(9, 2)),
    avg_tip                 AggregateFunction(avg, Decimal(9, 2))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, payment_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS taxi.trips_by_payment
TO taxi.trips_by_payment_mv
AS
SELECT
    toDate(pickup_datetime)             AS day,
    payment_type,
    countState()                        AS trip_count,
    sumState(total_amount)              AS total_fare,
    avgState(tip_amount)                AS avg_tip
FROM taxi.trips
GROUP BY day, payment_type;

CREATE TABLE IF NOT EXISTS taxi.trips_by_zone_mv
(
    day                     Date,
    pickup_zone             LowCardinality(String),
    pickup_borough          LowCardinality(String),
    trip_count              AggregateFunction(count),
    avg_fare                AggregateFunction(avg, Decimal(9, 2))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, pickup_zone);

CREATE MATERIALIZED VIEW IF NOT EXISTS taxi.trips_by_zone
TO taxi.trips_by_zone_mv
AS
SELECT
    toDate(pickup_datetime)             AS day,
    pickup_zone,
    pickup_borough,
    countState()                        AS trip_count,
    avgState(total_amount)              AS avg_fare
FROM taxi.trips
GROUP BY day, pickup_zone, pickup_borough;
