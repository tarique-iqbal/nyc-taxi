# Scaling Notes

## Current Baseline

The pipeline is designed for a single-node local deployment with a clear path to horizontal and vertical scaling. All architectural decisions were made with production scale in mind even though the initial deployment target is Docker Compose on one machine.

Approximate throughput on a single consumer with default settings:
- `KAFKA_BATCH_SIZE=500`, `PARQUET_BATCH_SIZE=1000`
- Columnar insert via PyArrow: faster than row-based INSERT for bulk loads
- January 2024 NYC Yellow Taxi dataset: ~2.9 million rows, ingested faster than row-based INSERT


## Kafka Scaling

### Partitions

The main topic (`nyc-taxi-trips`) has 4 partitions by default. This is the upper bound on consumer parallelism — you cannot have more consumers in a group than partitions.

To increase throughput: increase partition count before first use (partition count cannot be reduced after creation without deleting and recreating the topic).

```bash
kafka-topics --bootstrap-server localhost:9092 \
  --alter --topic nyc-taxi-trips --partitions 8
```

Update `kafka-topics.sh` to match so the CI and new deployments get the correct count.

### Consumer parallelism

Each consumer process handles one or more partitions. To run multiple consumers:

```bash
# Terminal 1
python -m etl.entrypoints.consumer

# Terminal 2
python -m etl.entrypoints.consumer

# Both consumers share KAFKA_CONSUMER_GROUP_ID -- Kafka assigns partitions automatically
```

With 4 partitions: 4 consumers maximum. Beyond that, extra consumers sit idle.

### Producer parallelism

The producer is I/O-bound on the Parquet read and Kafka publish. A single producer process is typically sufficient for one Parquet file. For multiple files in parallel:

- Run one producer process per file
- Use separate `batch_id` correlation IDs (automatic -- each batch generates its own UUID)
- `trip_id` deduplication handles overlaps if the same trip appears in multiple files

### Batch sizing

| Setting | Effect |
|---|---|
| `KAFKA_BATCH_SIZE` | Messages accumulated by the consumer before a ClickHouse insert. Larger = fewer inserts, better ClickHouse throughput, higher latency. |
| `KAFKA_BATCH_TIMEOUT_SECONDS` | Max seconds before a partial batch is flushed. Prevents lag accumulation at low throughput. |
| `PARQUET_BATCH_SIZE` | Rows per batch from the ParquetReader. Larger = more memory per batch, fewer Kafka publish calls. |

Tuning starting point for high throughput: `KAFKA_BATCH_SIZE=2000`, `PARQUET_BATCH_SIZE=5000`.


## ClickHouse Scaling

### Vertical scaling

ClickHouse is heavily single-node optimised. The most effective first scaling move is vertical: more CPU cores (parallelises merge operations), more RAM (larger mark cache and query buffer), faster NVMe storage (I/O-bound on large merges).

Key config knobs in `deployments/docker/clickhouse/config/config.xml`:

```xml
<max_server_memory_usage_to_ram_ratio>0.9</max_server_memory_usage_to_ram_ratio>
<mark_cache_size>5368709120</mark_cache_size>  <!-- increase with available RAM -->
<merge_tree>
    <max_bytes_to_merge_at_max_space_in_pool>161061273600</max_bytes_to_merge_at_max_space_in_pool>
</merge_tree>
```

### Async insert tuning

`async_insert=1` and `wait_for_async_insert=1` buffer multiple small inserts before flushing. At higher insert rates, increase the buffer limits:

```xml
<async_insert_max_data_size>52428800</async_insert_max_data_size>   <!-- 50 MB -->
<async_insert_busy_timeout_ms>500</async_insert_busy_timeout_ms>
```

Larger buffer = fewer physical inserts = fewer small parts = less merge pressure.

### Replication

For production with replication:
1. Deploy a ClickHouse Keeper cluster (3 nodes) or ZooKeeper
2. Change `ReplacingMergeTree` to `ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}', ingested_at)` in `schema.sql`
3. Change all `AggregatingMergeTree` materialized view tables to `ReplicatedAggregatingMergeTree`
4. Update `config.xml` with `<zookeeper>` or `<keeper_server>` configuration
5. Set replication factor to 2 or 3 in `kafka-topics.sh`

The `ClickHouseTripRepository` and `ColumnarInserter` code is unchanged — replication is transparent to the insert path.

### Partitioning

The `trips` table is partitioned by `toYYYYMM(pickup_datetime)`. This means:

- Old partitions (months) can be dropped cheaply: `ALTER TABLE taxi.trips DROP PARTITION 202401`
- Queries filtered to a date range skip entire partitions at the storage level
- Monthly data growth is predictable and manageable

For more granular partitioning at very high volume: `toYYYYMMDD(pickup_datetime)`.


## Memory Footprint

### Zone lookup

`CsvZoneRepository` loads ~265 TLC zones into a Python dict at startup. Memory footprint is negligible (~100 KB). No scaling concern.

### ParquetReader

`iter_batches()` never loads the full Parquet file into memory. Memory usage per batch: `PARQUET_BATCH_SIZE × ~50 bytes ≈ 50 MB` for 1,000 rows. Increase `PARQUET_BATCH_SIZE` freely on machines with available RAM.

### BatchAccumulator

Holds at most `KAFKA_BATCH_SIZE` trip dicts in memory before flush. At 500 rows × ~1 KB per dict ≈ 500 KB. Negligible.

### ColumnarInserter

Conversion pipeline: `list[dict]` → PyArrow Table → `to_pylist()`. Peak memory: approximately 3× the raw dict size (three representations in memory simultaneously). At batch size 500 this is ~1.5 MB. For large batches (5,000+ rows) monitor RSS and tune `KAFKA_BATCH_SIZE` down if memory pressure is observed.


## AWS Production Architecture

The Terraform directory (`deployments/terraform/`) targets this stack:

```
Producers (EC2)
    │
    ▼
Amazon MSK (Kafka)
    ├── nyc-taxi-trips  (4+ partitions, replication factor 3)
    └── nyc-taxi-trips-dlq
    │
    ▼
Consumers (EKS pods, one per partition)
    │
    ▼
ClickHouse (EC2, NVMe, replicated)
    │
    ▼
Grafana Cloud / self-hosted Grafana
```

### EKS consumer deployment

```yaml
replicas: 4  # matches partition count
resources:
  requests:
    memory: "512Mi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "2000m"
env:
  - name: KAFKA_CONSUMER_GROUP_ID
    value: nyc-taxi-etl-consumer
  # All consumers share the same group ID.
  # Kafka assigns one partition per consumer automatically.
```

Horizontal pod autoscaling is not recommended for Kafka consumers: adding a pod mid-run triggers a partition rebalance which pauses consumption until the group stabilises. Scale by changing `replicas` in a planned deployment instead.

### MSK settings

- `auto.create.topics.enable=false` (managed by `create_topics.sh` / Terraform)
- `log.retention.hours=168` for main topic, `336` for DLQ
- `num.partitions=4` default (overridden per topic)
- Enable in-transit encryption and IAM auth for production

### ClickHouse on EC2

Recommended instance: `r6i.4xlarge` (128 GB RAM, 16 vCPU) with 4× NVMe instance storage in RAID-0 for the ClickHouse data directory. The `mark_cache_size` in `config.xml` should be 60–70% of available RAM.

For cross-AZ replication: 3-node ClickHouse cluster with Keeper on separate instances. Each node in a different availability zone.


## Bottleneck Identification

Use these queries to identify where the pipeline is slow before scaling:

```bash
# Is the consumer keeping up with the producer?
bash scripts/check_kafka_lag.sh

# Is ClickHouse merge pressure building?
docker compose exec clickhouse clickhouse-client --query "
SELECT table, sum(rows) as rows, count() as parts, sum(bytes_on_disk) as bytes
FROM system.parts WHERE active = 1 AND database = 'taxi'
GROUP BY table ORDER BY bytes DESC"

# What is the p95 insert latency from Prometheus?
# http://localhost:9090/graph?g0.expr=histogram_quantile(0.95,rate(batch_insert_duration_seconds_bucket[5m]))

# Are there records being dead-lettered at a high rate?
ls -lh data/rejected/ | tail -20
```

The most common bottleneck at moderate scale is ClickHouse merge pressure from too many small inserts. Fix: increase `KAFKA_BATCH_SIZE` and `async_insert_max_data_size` before scaling horizontally.
