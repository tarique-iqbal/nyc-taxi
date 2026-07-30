from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Throughput
# Labelled by status so a single counter covers both outcomes.
# Query: rate(trips_processed_total{status="valid"}[1m])
trips_processed_total = Counter(
    "trips_processed_total",
    "Total number of trips processed by the ETL pipeline",
    labelnames=["status"],  # "valid" | "invalid"
)

# DLQ
# Labelled by stage so operators can see where failures concentrate.
dlq_records_total = Counter(
    "dlq_records_total",
    "Total number of records sent to the dead letter queue",
    labelnames=["stage"],  # parsing | validation | enrichment | persist
)

# ClickHouse insert latency
# Histogram with buckets covering the expected range for columnar inserts.
# p50/p95/p99 are derived by Prometheus from the bucket observations.
batch_insert_duration_seconds = Histogram(
    "batch_insert_duration_seconds",
    "Wall-clock duration of each ClickHouse batch insert",
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

# Successfully persisted trips
# Incremented by the consumer only after a ClickHouse insert has been
# confirmed -- distinct from trips_processed_total{status="valid"}, which
# reflects the producer's validation outcome, not persistence.
trips_persisted_total = Counter(
    "trips_persisted_total",
    "Total number of trips successfully persisted to ClickHouse",
)

# Kafka consumer lag
# Gauge updated by KafkaLagMonitor. Grafana alert fires when this exceeds
# a configurable threshold (default 10,000 messages).
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Current consumer group lag in number of unconsumed messages",
)

# Batch size distribution
# Tracks how many rows are in each batch yielded by BatchAccumulator.
# Useful for tuning KAFKA_BATCH_SIZE against observed throughput.
batch_size_histogram = Histogram(
    "batch_size_rows",
    "Number of rows in each accumulated batch",
    buckets=[50, 100, 250, 500, 1000, 2500, 5000],
)

# DLQ replay
dlq_replay_recovered_total = Counter(
    "dlq_replay_recovered_total",
    "Total number of DLQ records successfully recovered via replay",
)

dlq_replay_failed_total = Counter(
    "dlq_replay_failed_total",
    "Total number of DLQ records that remain invalid after replay",
)
