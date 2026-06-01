-- Reference queries for dashboards, operations, and ad-hoc analysis.
-- Dashboard queries use materialized view (_mv) tables with -Merge combiners.
-- Raw table scans are intended for ad-hoc use only.

-- DASHBOARD QUERIES (materialized views)

-- Hourly trip volume
-- Grafana throughput time series.
SELECT
    hour,
    countMerge(trip_count)              AS trips,
    sumMerge(total_fare)                AS revenue,
    avgMerge(avg_fare)                  AS avg_fare,
    avgMerge(avg_distance)              AS avg_distance_miles,
    avgMerge(avg_duration_seconds) / 60 AS avg_duration_minutes
FROM taxi.trips_hourly_mv
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour;

-- Daily summary
-- Grafana daily metrics.
SELECT
    day,
    countMerge(trip_count)      AS trips,
    sumMerge(total_fare)        AS revenue,
    avgMerge(avg_fare)          AS avg_fare,
    avgMerge(avg_tip)           AS avg_tip,
    avgMerge(avg_distance)      AS avg_distance_miles
FROM taxi.trips_daily_mv
WHERE day >= today() - 30
GROUP BY day
ORDER BY day;

-- Borough breakdown (today)
-- Grafana geographic view.
SELECT
    pickup_borough,
    countMerge(trip_count)      AS trips,
    sumMerge(total_fare)        AS revenue,
    avgMerge(avg_fare)          AS avg_fare,
    avgMerge(avg_distance)      AS avg_distance_miles
FROM taxi.trips_by_borough_mv
WHERE day = today()
GROUP BY pickup_borough
ORDER BY trips DESC;

-- Borough-to-borough flow
SELECT
    pickup_borough,
    dropoff_borough,
    countMerge(trip_count)      AS trips
FROM taxi.trips_by_borough_mv
WHERE day = today()
GROUP BY pickup_borough, dropoff_borough
ORDER BY trips DESC
LIMIT 20;

-- Payment type mix (last 7 days)
-- Grafana payment breakdown.
SELECT
    payment_type,
    countMerge(trip_count)      AS trips,
    round(countMerge(trip_count) * 100.0 / sum(countMerge(trip_count)) OVER (), 2) AS pct,
    avgMerge(avg_tip)           AS avg_tip
FROM taxi.trips_by_payment_mv
WHERE day >= today() - 7
GROUP BY payment_type
ORDER BY trips DESC;

-- Top pickup zones (today)
-- Grafana zone ranking.
SELECT
    pickup_zone,
    pickup_borough,
    countMerge(trip_count)      AS trips,
    avgMerge(avg_fare)          AS avg_fare
FROM taxi.trips_by_zone_mv
WHERE day = today()
GROUP BY pickup_zone, pickup_borough
ORDER BY trips DESC
LIMIT 20;

-- OPERATIONAL QUERIES

-- Row count per partition
-- Monthly partition size and row count.
SELECT
    partition,
    formatReadableQuantity(rows)        AS row_count,
    formatReadableSize(bytes_on_disk)   AS disk_size,
    modification_time
FROM system.parts
WHERE database = 'taxi'
  AND table = 'trips'
  AND active = 1
ORDER BY partition;

-- Deduplication check
-- Compare raw rows with distinct trip IDs.
-- Large differences may indicate pending merges.
SELECT
    count()                             AS raw_rows,
    countDistinct(trip_id)              AS distinct_trip_ids
FROM taxi.trips;

-- Deduplicated count (FINAL can be expensive).
SELECT count()
FROM taxi.trips FINAL;

-- Recent ingestion health
-- Recent inserts by source file and batch.
SELECT
    source_file,
    batch_id,
    count()                             AS rows,
    min(pickup_datetime)                AS earliest_pickup,
    max(pickup_datetime)                AS latest_pickup,
    min(ingested_at)                    AS ingested_at
FROM taxi.trips
WHERE ingested_at >= now() - INTERVAL 1 HOUR
GROUP BY source_file, batch_id
ORDER BY ingested_at DESC
LIMIT 50;

-- Schema migration status
SELECT version, applied_at, checksum
FROM taxi.schema_migrations FINAL
ORDER BY version;

-- AD-HOC ANALYSIS (raw table scans)

-- Fare distribution
SELECT
    quantiles(0.25, 0.50, 0.75, 0.95, 0.99)(toFloat64(total_amount)) AS fare_quantiles,
    min(total_amount)                   AS min_fare,
    max(total_amount)                   AS max_fare,
    avg(total_amount)                   AS avg_fare
FROM taxi.trips
WHERE toDate(pickup_datetime) = today();

-- Trip distance distribution
SELECT
    multiIf(
        trip_distance < 1,  '< 1 mile',
        trip_distance < 3,  '1-3 miles',
        trip_distance < 5,  '3-5 miles',
        trip_distance < 10, '5-10 miles',
        '>= 10 miles'
    )                                   AS distance_bucket,
    count()                             AS trips,
    avg(total_amount)                   AS avg_fare
FROM taxi.trips
WHERE toDate(pickup_datetime) = today()
GROUP BY distance_bucket
ORDER BY min(trip_distance);

-- Peak hour analysis
SELECT
    toHour(pickup_datetime)             AS hour_of_day,
    count()                             AS trips,
    avg(total_amount)                   AS avg_fare,
    avg(trip_duration_seconds) / 60     AS avg_duration_minutes
FROM taxi.trips
WHERE toDate(pickup_datetime) BETWEEN today() - 7 AND today()
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- Vendor comparison
SELECT
    vendor_id,
    count()                             AS trips,
    avg(total_amount)                   AS avg_fare,
    avg(tip_amount)                     AS avg_tip,
    avg(trip_distance)                  AS avg_distance,
    avg(trip_duration_seconds) / 60     AS avg_duration_minutes
FROM taxi.trips
WHERE toYYYYMM(pickup_datetime) = toYYYYMM(today())
GROUP BY vendor_id
ORDER BY trips DESC;
