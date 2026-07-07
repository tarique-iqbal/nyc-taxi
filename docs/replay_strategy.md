# Replay Strategy

## Why Replay Exists

The pipeline has two categories of failure:

**Transient infrastructure failures** — ClickHouse unavailable, Kafka broker restarting. These are handled automatically: the consumer does not commit the Kafka offset until the insert succeeds, so Kafka replays the batch on consumer restart. No manual intervention needed. `ReplacingMergeTree` on `trip_id` ensures the re-insert is idempotent.

**Permanent domain failures** — records that fail business rule validation and will always fail: negative fares, passenger counts out of range, pickup dates before 2009, zero-duration trips. These are routed to the DLQ. They require either fixing the source data or relaxing a validation rule before replay will recover them.

Replay is the mechanism for recovering permanently-failed records after the root cause has been fixed.


## DLQ Dual-Write

Every `InvalidTripDetected` event produces a `DeadLetterRecord` written to two destinations atomically:

**Kafka DLQ topic** (`nyc-taxi-trips-dlq`)
- 14-day retention
- Partitioned by `trip_id` (or `batch_id` if `trip_id` is unknown)
- For automated replay pipelines

**Disk** (`data/rejected/<batch_id>.jsonl.gz`)
- Gzip-compressed JSON Lines
- One file per `batch_id` (correlation ID)
- For human inspection and `replay_dlq.sh`

Both writes are independent. A Kafka outage does not prevent the disk write. The disk file is the primary replay source because it does not require a Kafka consumer group and survives broker failure.


## What Gets Dead-Lettered

| Stage | Error type | Example |
|---|---|---|
| `PARSING` | `TripParseError` | Missing `pickup_datetime`, unparseable timestamp |
| `VALIDATION` | `InvalidTripDurationError` | `dropoff == pickup` |
| `VALIDATION` | `InvalidPassengerCountError` | `passenger_count > 9` |
| `VALIDATION` | `NegativeMoneyError` | `fare_amount < 0` |
| `VALIDATION` | `InvalidPickupDatetimeError` | Pickup before 2009-01-01 |
| `DEDUPLICATION` | `WithinBatchDuplicate` | Same `trip_id` twice in one batch |
| `PERSIST` | Any | ClickHouse insert failed after all retries |

Schema validation failures (Pydantic) are dead-lettered before the domain pipeline with `stage=VALIDATION` and `error_type=SchemaValidationError`.


## DeadLetterRecord Structure

```json
{
  "trip_id": "a3f8...",
  "original_record": { "vendor_id": 2, "pickup_datetime": "...", ... },
  "error_message": "Trip duration 0s is invalid. Must be > 0 and < 86400 (24 hours).",
  "error_type": "InvalidTripDurationError",
  "stage": "validation",
  "batch_id": "c1d2e3f4-...",
  "source_file": "yellow_tripdata_2024-01.parquet",
  "failed_at": "2024-01-15T10:30:00.000000+00:00",
  "retry_count": 0
}
```

`original_record` contains the raw row exactly as it arrived from `ParquetReader` (post column rename, pre-normalisation). This ensures replay runs the full pipeline again, including normalisation and enrichment, rather than replaying an already-processed state.

`retry_count` is incremented on each unsuccessful replay so operators can identify records that have failed multiple times after repeated attempts.


## Replay with replay_dlq.sh

```bash
# Replay all rejected files
bash scripts/replay_dlq.sh

# Replay a single batch
bash scripts/replay_dlq.sh --batch-id=0a6ff5fb-5c77-4b1d-816d-177de5e523d1

# Inspect without publishing (dry run)
bash scripts/replay_dlq.sh --dry-run
```

### What happens during replay

1. `ReplayDlqUseCase` reads `original_record` from each `.jsonl.gz` file
2. Runs `TripDomainService.process_batch()` with the same pipeline as the producer
3. **Recovered records** — domain pipeline now passes — published to `nyc-taxi-trips`
4. **Still-invalid records** — domain pipeline still fails — re-written to DLQ with `retry_count += 1`

Recovered records flow through the consumer normally and land in ClickHouse. `ReplacingMergeTree` handles the case where the original record already reached ClickHouse through a partial failure.

### Recovery rate

After replay completes, `replay_dlq.sh` prints:

```
  Total replayed:  47
  Recovered:       43
  Still invalid:   4
  Recovery rate:   91.5%
```


## ReplayDlqUseCase Internals

```python
ReplayDlqCommand(
    source=ReplaySource.DISK,
    batch_id=None,           # None = all files, UUID = single batch
    rejected_dir=Path("data/rejected"),
)
```

`ReplaySource.KAFKA` is defined but raises `NotImplementedError`. Kafka replay would require a dedicated consumer group reading from `nyc-taxi-trips-dlq`, which is a valid future enhancement but was not needed for the disk-based workflow.


## Deciding When to Replay

| Scenario | Action |
|---|---|
| Infrastructure was down (ClickHouse, Kafka) | Not needed. Kafka offset was not committed; consumer replays automatically on restart. |
| Validation rule was too strict and has been relaxed | Replay. Records that were rejected will now pass. |
| Zone CSV was missing zones | Add the missing zones to `data/reference/taxi_zone_lookup.csv`, reload zone repository, replay. |
| Source data had a known upstream bug now corrected | Discard rejected files. Ingest corrected data from source. |
| Same record rejected 3+ times | Investigate `error_message` in the `.jsonl.gz` file. The data itself may be permanently corrupt. |


## Inspecting Rejected Files

```bash
# List all rejected files with record counts
ls -lh data/rejected/

# Read a specific batch's rejected records
python3 -c "
from etl.utils.compression import read_jsonl_gz
from pathlib import Path
import json

records = read_jsonl_gz(Path('data/rejected/<batch_id>.jsonl.gz'))
for r in records:
    print(r['error_type'], '|', r['error_message'])
"

# Group errors by type
python3 -c "
from etl.utils.compression import list_rejected_files, read_jsonl_gz
from pathlib import Path
from collections import Counter

counts = Counter()
for f in list_rejected_files(Path('data/rejected')):
    for r in read_jsonl_gz(f):
        counts[r['error_type']] += 1
for error_type, count in counts.most_common():
    print(f'{count:>6}  {error_type}')
"
```


## Kafka DLQ Topic (Alternative Consumer)

For automated replay pipelines, a consumer can read from `nyc-taxi-trips-dlq` and re-publish recovered records to `nyc-taxi-trips`. The topic has 1 partition and 14-day retention. Key design constraints:

- Use a separate consumer group ID from the main pipeline consumer
- Do not commit the DLQ offset until the recovered record has been confirmed by the main topic publisher
- Records that still fail after replay must be re-produced to the DLQ with `retry_count` incremented; otherwise they will be replayed in an infinite loop
