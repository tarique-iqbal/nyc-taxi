# Event Flow

## Producer Path

```
ParquetReader.iter_batches()
        │
        │  list[dict]  (raw TLC column names, e.g. VendorID, PULocationID)
        ▼
ValidationService.validate_batch()
        │
        ├── schema-invalid rows  ──►  DeadLetterRecord (stage=VALIDATION)
        │                                     │
        │                             dead_letter_service.send()
        │                              ├── Kafka DLQ topic
        │                              └── data/rejected/<batch_id>.jsonl.gz
        │
        │  schema-valid rows
        ▼
TripNormalizer.normalize()
        │  (VendorID int → string, payment_type int → string,
        │   passenger_count null → 1, store_and_fwd_flag Y/N → Yes/No)
        ▼
_build_trip()
        │  (constructs Trip entity, generates trip_id via SHA-256)
        ▼
TripEnricher.enrich()
        │  (PULocationID → zone name + borough via CsvZoneRepository)
        │  (never raises -- unknown zones → "Unknown")
        ▼
TripValidator.validate()
        │  (duration > 0 and < 24h, passenger 1-9,
        │   pickup >= 2009-01-01, all fares >= 0)
        │
        ├── invalid  ──►  InvalidTripDetected event
        │                       │
        │                 dead_letter_service.send()
        │
        │  valid Trip entities
        ▼
TripDeduplicator.deduplicate()
        │  (removes duplicate trip_ids within this batch)
        │
        ├── duplicates  ──►  InvalidTripDetected (stage=DEDUPLICATION)
        │
        │  unique Trip entities
        ▼
KafkaEventPublisher.publish_batch()
        │  (each Trip.to_dict() → JSON bytes, keyed by trip_id)
        │  (acks=all, enable.idempotence=true)
        ▼
Kafka topic: nyc-taxi-trips
```


## Consumer Path

```
Kafka topic: nyc-taxi-trips
        │
        ▼
KafkaConsumerAdapter.consume_batches()
        │  (enable.auto.commit=false)
        │  (BatchAccumulator: flush on size=500 OR timeout=10s)
        │
        │  AccumulatedBatch (list[dict] from JSON deserialise)
        ▼
ClickHouseTripRepository.save_batch_from_dicts()
        │
        ▼
ColumnarInserter.insert()
        │  (list[dict] → coerce types → PyArrow Table → to_pylist())
        │  (execute(columnar=True) -- columnar protocol)
        ▼
ClickHouse: taxi.trips (ReplacingMergeTree)
        │
        │  -- on every INSERT --
        ▼
Materialized Views (auto-populated):
        ├── taxi.trips_hourly_mv      (countState, sumState, avgState per hour+vendor)
        ├── taxi.trips_daily_mv       (per day+vendor+payment_type)
        ├── taxi.trips_by_borough_mv  (per day+pickup_borough+dropoff_borough)
        ├── taxi.trips_by_payment_mv  (per day+payment_type)
        └── taxi.trips_by_zone_mv     (per day+pickup_zone)
        │
        ▼
KafkaConsumerAdapter.commit()
        │  (offset advances ONLY after confirmed insert)
        │  (if insert fails: no commit, Kafka replays batch on restart)
        ▼
Offset committed to Kafka
```


## DLQ Flow

```
InvalidTripDetected event
        │
        ▼
DeadLetterRecord.from_invalid_event()
        │  (wraps original_record, error_message, error_type, stage,
        │   batch_id, source_file, trip_id, retry_count=0)
        ▼
KafkaDeadLetterPublisher.send()
        │
        ├── _write_to_kafka()
        │       │  (individual message, not batched)
        │       │  (key = trip_id or batch_id)
        │       ▼
        │   Kafka topic: nyc-taxi-trips-dlq
        │   (14-day retention for operator inspection)
        │
        └── _write_to_disk()
                │  (append to data/rejected/<batch_id>.jsonl.gz)
                │  (gzip JSON Lines, one record per line)
                ▼
            data/rejected/<batch_id>.jsonl.gz
```

Each destination is written independently. A Kafka outage does not prevent the disk write and vice versa. Neither failure crashes the pipeline — errors are logged and processing continues.


## Replay Flow

```
data/rejected/<batch_id>.jsonl.gz
        │
        ▼
ReplayDlqUseCase.handle(ReplayDlqCommand)
        │
        │  reads DeadLetterRecord.original_record for each line
        ▼
TripDomainService.process_batch()
        │  (same pipeline as producer: normalise, build, enrich, validate, deduplicate)
        │
        ├── recovered (now passes validation)
        │       │
        │       ▼
        │   KafkaEventPublisher.publish_batch()
        │       ▼
        │   Kafka topic: nyc-taxi-trips
        │       ▼
        │   Consumer picks up, inserts into ClickHouse
        │
        └── still invalid (still fails validation)
                │
                ▼
            DeadLetterRecord (retry_count += 1, updated error_message)
                │
                ▼
            KafkaDeadLetterPublisher.send()
                ├── Kafka DLQ topic
                └── data/rejected/<new_batch_id>.jsonl.gz
```


## Domain Events

| Event | When raised | Key fields |
|---|---|---|
| `TripCreated` | Raw row successfully parsed into `Trip` | `trip_id`, `vendor_id`, `batch_id` |
| `TripEnriched` | Zone names resolved | `trip_id`, `pickup_zone`, `dropoff_zone` |
| `TripValidated` | All business rules passed | `trip_id`, `batch_id` |
| `InvalidTripDetected` | Failure at any stage | `stage`, `error_type`, `error_message`, `original_record` |

`InvalidTripDetected.stage` values:

| Stage | Cause |
|---|---|
| `PARSING` | Missing required field, unparseable datetime |
| `NORMALIZATION` | Unexpected type preventing normalisation |
| `ENRICHMENT` | `ZoneNotFoundError` (rare -- enricher falls back to Unknown) |
| `VALIDATION` | Business rule violation |
| `DEDUPLICATION` | Duplicate `trip_id` within the same batch |
| `PERSIST` | ClickHouse insert failed after retries |


## Correlation ID Propagation

Every batch receives a UUID at entry. The ID propagates through every function call and log line via `contextvars.ContextVar`.

```
ParquetReader yields batch
        │
        ▼
BatchCorrelationContext(correlation_id=uuid4())
        │
        ├── TripDomainService.process_batch(batch_id=correlation_id)
        │       └── every Trip.batch_id = correlation_id
        │
        ├── KafkaEventPublisher  -- message contains batch_id
        │
        ├── ClickHouse row       -- batch_id column
        │
        └── every log line      -- correlation_id JSON field (via CorrelationFilter)
```

To trace a batch end-to-end, filter the producer and consumer stdout logs by the correlation ID.

**Producer:**

```bash
python -m etl.entrypoints.producer 2>&1 | grep '"correlation_id":"<uuid>"'
```

**Consumer:**

```bash
python -m etl.entrypoints.consumer 2>&1 | grep '"correlation_id":"<uuid>"'
```

If you redirect stdout to log files, grep those files instead:

```bash
grep '"correlation_id":"<uuid>"' logs/producer.log
grep '"correlation_id":"<uuid>"' logs/consumer.log
```

To find all ClickHouse rows from a batch:

```sql
SELECT * FROM taxi.trips WHERE batch_id = '<uuid>';
```
