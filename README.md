# NYC Taxi ETL

A production-grade streaming ETL pipeline that ingests NYC Yellow Taxi trip data from Parquet files, streams it through Kafka, validates and enriches it through a DDD domain layer, and persists it into ClickHouse for sub-second analytical queries and dashboard visualization.

## Architecture

```
Parquet File
    ↓
ParquetReader (chunked, columnar)
    ↓
Domain Pipeline (validate → normalize → enrich)
    ↓
Kafka Topic: nyc-taxi-trips
    ↓
Consumer (accumulate batches)
    ↓
ClickHouse: trips table
    ↓
Materialized Views (auto-aggregate)
    ↓
Dashboard Queries (sub-second)
```

Bad records at any stage:
```
    ↓
Kafka DLQ: nyc-taxi-trips-dlq
    ↓
data/rejected/ (on disk)
    ↓
replay_dlq.sh (manual recovery)
```

## Quick Start

```bash
# 1. Copy env and configure
cp .env.example .env

# 2. Infrastructure up
make up

# 3. Create Kafka topics
bash scripts/create_topics.sh

# 4. Apply ClickHouse schema + migrations
bash scripts/apply_schema.sh

# 5. Download data
bash scripts/download_data.sh

# 6. Start metrics + health servers
python -m etl.entrypoints.metrics_server &
python -m etl.entrypoints.health_server &

# 7. Start consumer (Terminal 2)
python -m etl.entrypoints.consumer

# 8. Start producer (Terminal 3)
python -m etl.entrypoints.producer

# 9. Monitor
bash scripts/check_kafka_lag.sh
open http://localhost:3000   # Grafana
open http://localhost:8080   # Kafka UI
open http://localhost:8000/health
```

## Requirements

- Python 3.11+
- Docker + Docker Compose
- 4 GB RAM minimum

## Project Structure

```
etl/
├── config/         # Environment-driven settings (pydantic-settings)
├── domain/         # Pure business logic — no infrastructure dependencies
├── application/    # Use cases and orchestration
├── infrastructure/ # Kafka, ClickHouse, storage adapters
├── runtime/        # Lifecycle, batching, retry, graceful shutdown
├── observability/  # Structured logging, correlation IDs, tracing
├── utils/          # Shared helpers
└── entrypoints/    # DI wiring, process entry points
```

## Key Design Decisions

- **ReplacingMergeTree** — idempotent ClickHouse inserts; safe to replay Kafka messages
- **Manual Kafka offset commit** — offset advances only after ClickHouse confirms insert
- **Columnar protocol insert** — faster than row-based
- **Correlation IDs** — every log line for a batch shares one UUID for easy tracing
- **Graceful shutdown** — SIGTERM finishes current batch before exit; no mid-insert loss
- **DLQ dual-write** — failed records go to both Kafka DLQ topic and `data/rejected/` on disk
