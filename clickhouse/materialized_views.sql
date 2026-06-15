-- Materialized views for sub-second dashboard queries.
--
-- Engine: AggregatingMergeTree
--   Stores partial aggregation states produced by aggregate combiners
--   (-State suffix: countState(), sumState(), avgState()).
--   Background merges combine partial states into final states.
--   Dashboard queries use the corresponding -Merge combiners
--   (countMerge(), sumMerge(), avgMerge()) to read pre-computed results
--   instead of scanning raw rows.
--
-- Each view has:
--   1. An inner _mv table (AggregatingMergeTree) that stores the states.
--   2. A MATERIALIZED VIEW that populates the _mv table on every insert
--      into taxi.trips.
--
-- Applied via: bash scripts/apply_schema.sh
-- Also tracked in migrations/002_materialized_views.sql

-- 1. Hourly trip stats
-- Aggregates per hour bucket. Powers the main throughput time series panel
-- in the Grafana dashboard.

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

-- 2. Daily trip stats
-- Aggregates per calendar day. Powers daily summary panels and trend lines.

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

-- 3. Borough-level stats
-- Aggregates per pickup borough. Powers the geographic breakdown panel.

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

-- 4. Payment type breakdown
-- Aggregates per payment type per day. Powers the payment mix panel.

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

-- 5. Zone-level pickup heatmap
-- Top pickup zones per day. Powers zone heatmap / ranking panel.

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
