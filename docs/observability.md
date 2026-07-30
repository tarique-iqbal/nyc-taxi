# Observability

## Overview

The pipeline exposes three observability signals: structured logs, Prometheus metrics, and optional OpenTelemetry traces. All three share the same `correlation_id` (batch UUID) so a single value ties together log lines, metric labels, and trace spans for any given batch.


## Structured Logging

Every log line is a single JSON object written to stdout. Docker captures it via `docker logs`. Loki, ELK, or Datadog ingest it without a parsing pipeline.

### Format

```json
{
  "timestamp": "2024-01-15T10:30:00.123456+00:00",
  "level": "INFO",
  "logger": "etl.infrastructure.kafka.consumer",
  "message": "Batch persisted",
  "correlation_id": "a1b2c3d4-...",
  "batch_id": "a1b2c3d4-...",
  "size": 498,
  "total_rows": 12450
}
```

`correlation_id` is injected automatically by `CorrelationFilter` -- callers do not pass it manually.

### Setup

`lifecycle.startup()` calls `setup_logging(level=settings.monitoring.log_level)` as the first step. All subsequent log lines from any module use the JSON formatter.

Log level is controlled by `LOG_LEVEL` in `.env` (default `INFO`). Set `DEBUG` for verbose per-row tracing during development.

### Key log events

| Logger | Message | Key extra fields |
|---|---|---|
| `etl.infrastructure.storage.parquet_reader` | `Parquet read complete` | `total_rows`, `total_batches` |
| `etl.domain.trip.services` | `Batch processed` | `valid`, `invalid`, `duplicates` |
| `etl.infrastructure.kafka.producer` | `Published messages to Kafka` | `topic`, `count` |
| `etl.infrastructure.kafka.consumer` | `Batch persisted` | `size`, `batch_id` |
| `etl.infrastructure.kafka.consumer` | `Kafka offsets committed` | |
| `etl.infrastructure.clickhouse.inserter` | `Columnar insert complete` | `table`, `rows` |
| `etl.infrastructure.kafka.dead_letter_publisher` | `DLQ record delivered` | `topic`, `partition`, `offset` |
| `etl.application.services.ingestion_service` | `Ingestion complete` | `total_rows`, `reject_rate` |


## Prometheus Metrics

Each long-running entrypoint serves its own `/metrics` endpoint, started by `lifecycle.startup()`: the producer on `PROMETHEUS_PORT_PRODUCER` (default `9100`), the consumer on `PROMETHEUS_PORT_CONSUMER` (default `9101`). Prometheus scrapes them as two separate jobs, `nyc-taxi-producer` and `nyc-taxi-consumer`, as configured in `deployments/docker/prometheus/prometheus.yml` -- keeping them separate avoids ambiguous aggregation, since `trips_processed_total` only ever originates from the producer job and `batch_insert_duration_seconds`/`batch_size_rows`/`kafka_consumer_lag`/`trips_persisted_total` only from the consumer job.

### Metric reference

**`trips_processed_total{status}`** — Counter

Total trips processed since startup, by outcome.

```promql
rate(trips_processed_total{status="valid"}[1m])    # valid trips per second
rate(trips_processed_total{status="invalid"}[1m])  # rejection rate
```

**`trips_persisted_total`** — Counter

Total trips successfully persisted to ClickHouse, incremented by the consumer only after a batch insert is confirmed. Distinct from `trips_processed_total{status="valid"}`, which reflects the producer's validation outcome, not persistence -- a batch that fails to insert (and is left for Kafka to replay) does not increment this counter.

```promql
rate(trips_persisted_total[1m])  # persisted trips per second
```

**`dlq_records_total{stage}`** — Counter

Total records sent to the DLQ, by the pipeline stage where they failed.

```promql
sum by (stage) (increase(dlq_records_total[5m]))
```

**`batch_insert_duration_seconds`** — Histogram

Wall-clock duration of each ClickHouse `execute(columnar=True)` call. Buckets: 10ms to 10s.

```promql
histogram_quantile(0.95, rate(batch_insert_duration_seconds_bucket[5m]))  # p95 latency
rate(batch_insert_duration_seconds_sum[5m])
  / rate(batch_insert_duration_seconds_count[5m])                         # average
```

**`kafka_consumer_lag`** — Gauge

Current consumer group lag in unconsumed messages, updated by `KafkaLagMonitor.poll()`.

```promql
kafka_consumer_lag > 10000  # alert condition
```

**`batch_size_rows`** — Histogram

Rows per accumulated batch. Use to tune `KAFKA_BATCH_SIZE` against observed throughput.

```promql
histogram_quantile(0.50, rate(batch_size_rows_bucket[5m]))  # median batch size
```

**`dlq_replay_recovered_total`** / **`dlq_replay_failed_total`** — Counters

Outcomes from `replay_dlq.sh` runs.

### ClickHouse native metrics

ClickHouse exposes its own Prometheus endpoint on port `9363` (configured in `deployments/docker/clickhouse/config/config.xml`). Key metrics:

| Metric | Description |
|---|---|
| `ClickHouseProfileEvents_InsertedRows` | Rows inserted since startup |
| `ClickHouseProfileEvents_MergedRows` | Rows merged by background processes |
| `ClickHouseMetrics_BackgroundMergesAndMutationsPoolTask` | Active merge threads |
| `ClickHouseAsyncInsertCacheHits` | Async insert buffer utilisation |


## Grafana Dashboards

Dashboard provisioned at startup from `deployments/docker/grafana/dashboards/etl_overview.json`. Access at `http://localhost:3000` (default credentials: `admin/admin`).

**Throughput row** — valid and invalid trips per second over time, total trip count, total DLQ record count. Four stat panels at the top give an immediate health summary.

**ClickHouse Insert Latency row** — p50/p95/p99 histogram from `batch_insert_duration_seconds`, average insert duration, batch insert rate per second.

**Kafka Consumer Lag row** — current lag gauge (green below 1,000, yellow below 10,000, red above) and lag over time as a time series. A lag that trends upward and does not return to zero means the consumer is falling behind the producer.

Both Prometheus and ClickHouse are wired as datasources. Prometheus powers the ETL metrics panels. ClickHouse can be queried directly in Grafana Explore for ad-hoc SQL against `taxi.trips` and the materialized view tables.


## Health Endpoint

`health_server.py` serves FastAPI on `HEALTH_PORT` (default `8000`).

```
GET /health   -- full connectivity check
GET /ready    -- process alive check (no downstream checks)
```

### Response

```json
{
  "status": "ok",
  "components": {
    "clickhouse": { "status": "ok", "detail": "" },
    "kafka":      { "status": "ok", "detail": "2 topics visible" }
  }
}
```

HTTP status codes: `200` (ok), `207` (degraded), `503` (down).

Used by Docker health checks in `docker-compose.yml` and by Kubernetes liveness probes.


## Correlation IDs

Every batch receives a UUID at entry via `BatchCorrelationContext`. The ID is stored in a `contextvars.ContextVar` and injected into every log record by `CorrelationFilter` without callers needing to pass it explicitly.

### Tracing a batch across all systems

```bash
# All log lines for a batch
grep '"correlation_id":"a1b2c3d4"' application.log

# ClickHouse rows for a batch
clickhouse-client --query "SELECT * FROM taxi.trips WHERE batch_id = 'a1b2c3d4'"

# Rejected records for a batch
zcat data/rejected/a1b2c3d4.jsonl.gz | python3 -m json.tool
```

### Checking Kafka lag

```bash
bash scripts/check_kafka_lag.sh                # one-shot report
bash scripts/check_kafka_lag.sh --watch        # refresh every 5 seconds
bash scripts/check_kafka_lag.sh --alert=10000  # exit 1 if lag > 10000
```


## OpenTelemetry (optional)

`tracing.py` wraps key operations in OTLP trace spans. If `opentelemetry` is not installed or `OTEL_EXPORTER_OTLP_ENDPOINT` is not set, all tracing calls are no-ops and the pipeline runs normally.

To enable: add `opentelemetry-sdk` and `opentelemetry-exporter-otlp` to your environment, set `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317`, and deploy an OTLP-compatible backend (Jaeger, Tempo, Honeycomb).

Instrumented operations: `batch.insert` (ColumnarInserter), `kafka.publish_batch` (KafkaEventPublisher), `domain.process_batch` (TripDomainService).
