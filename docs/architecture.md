# Architecture

## Overview

NYC Taxi ETL is a production-grade streaming pipeline that ingests NYC Yellow Taxi trip data from Parquet files, streams it through Kafka, validates and enriches it through a domain-driven design (DDD) layer, and persists it into ClickHouse for sub-second analytical queries.

```
Parquet File
    │
    ▼
ParquetReader (chunked, columnar)
    │
    ▼
Domain Pipeline
(validate → normalise → enrich → deduplicate)
    │
    ├── valid trips
    │       ▼
    │   Kafka: nyc-taxi-trips
    │       ▼
    │   Consumer (BatchAccumulator)
    │       ▼
    │   ClickHouse: taxi.trips
    │       ▼
    │   Materialized Views (auto-aggregate)
    │       ▼
    │   Dashboard Queries (sub-second)
    │
    └── invalid records
            ▼
        Kafka: nyc-taxi-trips-dlq
        data/rejected/<batch_id>.jsonl.gz
```


## Layer Structure

The project follows a strict dependency rule: layers only import inward. Domain never imports from infrastructure. Infrastructure never imports from entrypoints.

```
entrypoints  →  application  →  domain
                    │
                    ▼
              infrastructure
```

### Config (`etl/config/`)

Loaded first. Reads all environment variables via pydantic-settings and validates them at startup. Every other layer imports from config. Nothing hardcodes connection strings.

Sub-configs: `KafkaSettings`, `ClickHouseSettings`, `MonitoringSettings`, `ETLSettings`. Composed into a single `Settings` object via `get_settings()` which is `@lru_cache`'d after first call.

### Domain (`etl/domain/`)

The heart of the system. No Kafka, ClickHouse, or Parquet imports anywhere in this layer. Pure Python business logic testable without infrastructure.

`domain/trip/` contains:

- `models.py` — value objects (`Money`, `Distance`, `Duration`, `Location`, `Payment`) and the `Trip` aggregate root
- `events.py` — `TripCreated`, `TripEnriched`, `TripValidated`, `InvalidTripDetected`
- `exceptions.py` — typed domain exceptions for each business rule violation
- `repositories.py` — abstract interfaces (`TripRepository`, `ZoneRepository`)
- `normalizers.py` — maps raw TLC numeric codes to readable strings
- `validators.py` — enforces business rules (duration, passenger count, datetime range, non-negative fares)
- `enrichers.py` — resolves `PULocationID`/`DOLocationID` to zone names and boroughs
- `deduplicator.py` — generates deterministic SHA-256 `trip_id`, removes within-batch duplicates
- `services.py` — `TripDomainService.process_batch()` orchestrates the full pipeline per batch

`domain/dead_letter/` defines `DeadLetterRecord` and the abstract `DeadLetterService`.

### Application (`etl/application/`)

Orchestrates domain and infrastructure via ports (interfaces). Knows about both but depends on neither directly.

- `application/__init__.py` — defines `EventPublisher` ABC (the Kafka port)
- `ingestion/process_trip.py` — single-trip use case for tests and tooling
- `ingestion/process_batch.py` — batch use case: domain pipeline + publish + dead-letter
- `ingestion/replay_dlq.py` — DLQ replay use case: re-runs rejected records through domain
- `services/ingestion_service.py` — full producer loop: ParquetReader → batches → use case
- `services/validation_service.py` — Pydantic schema validation (runs before domain)
- `services/enrichment_service.py` — guards zone repository lifecycle

### Infrastructure (`etl/infrastructure/`)

Concrete implementations of domain interfaces. Only layer that knows about external systems.

- `kafka/producer.py` — `KafkaEventPublisher`, implements `EventPublisher` port
- `kafka/consumer.py` — `KafkaConsumerAdapter`, manual offset commit after insert
- `kafka/serializer.py` — JSON serialisation handling `datetime`, `Decimal`, `UUID`
- `kafka/dead_letter_publisher.py` — dual-writes to Kafka DLQ + `data/rejected/`
- `kafka/topic_manager.py` — idempotent topic creation at startup
- `clickhouse/client.py` — `ClickHouseClient`, retry-decorated connection wrapper
- `clickhouse/inserter.py` — `ColumnarInserter`, list[dict] → PyArrow → DataFrame → insert
- `clickhouse/repository.py` — `ClickHouseTripRepository`, implements `TripRepository`
- `clickhouse/schema_manager.py` — applies migrations, tracks versions in ClickHouse
- `storage/parquet_reader.py` — `ParquetReader`, chunked columnar reads via `iter_batches()`
- `storage/zone_lookup.py` — `CsvZoneRepository`, O(1) in-memory zone lookup
- `monitoring/metrics.py` — Prometheus counters, histograms, gauges
- `monitoring/health.py` — `HealthChecker`, checks ClickHouse and Kafka connectivity
- `monitoring/kafka_lag.py` — `KafkaLagMonitor`, polls and exposes consumer group lag

### Runtime (`etl/runtime/`)

Production concerns that belong neither in domain nor infrastructure.

- `lifecycle.py` — `startup()` and `shutdown()` in dependency order
- `shutdown.py` — `ShutdownHandler`, catches `SIGTERM`/`SIGINT`, sets shutdown flag
- `batching.py` — `BatchAccumulator`, size-or-timeout flush triggers
- `retry.py` — `@retry` decorator with exponential backoff and named `RetryConfig` policies

### Observability (`etl/observability/`)

- `structured_logging.py` — `JsonFormatter`, every log line is machine-readable JSON
- `correlation.py` — `BatchCorrelationContext`, UUID per batch propagated via `contextvars`
- `tracing.py` — OpenTelemetry span wrappers, no-op if not installed

### Entrypoints (`etl/entrypoints/`)

Process entry points. Not imported by anything. Only consume from the layers above.

- `producer.py` — reads Parquet, runs domain pipeline, publishes to Kafka; `lifecycle.startup()` also starts a Prometheus `/metrics` server on `PROMETHEUS_PORT_PRODUCER`
- `consumer.py` — consumes Kafka, inserts into ClickHouse, commits offset after insert; `lifecycle.startup()` also starts a Prometheus `/metrics` server on `PROMETHEUS_PORT_CONSUMER`, and a `LagMonitorLoop` polls consumer group lag on a background thread
- `health_server.py` — serves FastAPI `/health` and `/ready` endpoints


## Key Technical Decisions

### ReplacingMergeTree for idempotent inserts

`trip_id` is a deterministic SHA-256 hash of `vendor_id | pickup_datetime | dropoff_datetime | pickup_location_id`. When Kafka replays a message after a consumer restart, the same trip arrives twice. ClickHouse keeps only the row with the latest `ingested_at` in the background. Queries use `FINAL` or `countMerge()` via materialized views to read deduplicated counts.

### Manual Kafka offset commit

`enable.auto.commit=false`. The consumer offset advances only after ClickHouse confirms the insert. If the insert fails, Kafka replays the same batch on restart. Combined with `ReplacingMergeTree`, this gives exactly-once semantics without a distributed transaction.

### Columnar insert via PyArrow

`list[dict]` → coerce types → PyArrow Table (explicit schema) → per-column Python lists via `to_pylist()` → `execute(columnar=True)`. ClickHouse stores data column by column. Sending data in columnar format means it arrives in the layout ClickHouse needs to write, with no transposition on the server side.

### AggregatingMergeTree for sub-second dashboards

Five materialized views populate `_mv` tables with `-State` combiners (`countState()`, `sumState()`, `avgState()`) on every insert. Dashboard queries use `-Merge` combiners (`countMerge()`, `sumMerge()`, `avgMerge()`) to read pre-computed partial aggregation states rather than scanning raw rows.

### Within-batch deduplication before insert

`TripDeduplicator` removes duplicate `trip_id`s within a single batch before they reach ClickHouse. Without this, duplicate rows land in ClickHouse before `ReplacingMergeTree` has a chance to merge them — queries without `FINAL` would return doubled counts until the background merge runs.

### Dual-write DLQ

Failed records are written to both the Kafka DLQ topic (`nyc-taxi-trips-dlq`) and `data/rejected/<batch_id>.jsonl.gz`. Kafka DLQ enables automated replay via `ReplayDlqUseCase`. Disk copy enables human inspection without Kafka access and survives a Kafka outage.


## Dependency Injection

`etl/entrypoints/producer.py` and `consumer.py` wire everything together at startup. Abstract interfaces (ports) are bound to concrete implementations:

| Interface | Implementation |
|---|---|
| `ZoneRepository` | `CsvZoneRepository` |
| `TripRepository` | `ClickHouseTripRepository` |
| `EventPublisher` | `KafkaEventPublisher` |
| `DeadLetterService` | `KafkaDeadLetterPublisher` |

Swapping the backing store means implementing the interface and changing one line in the entrypoint. The domain and application layers are unaffected.
