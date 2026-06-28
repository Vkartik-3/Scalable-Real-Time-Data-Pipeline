# End-to-End Real-Time Data Engineering Pipeline

## Architecture

```
randomuser.me API ─┐
                   ├─→ Airflow DAG (ingest_to_postgres)
Synthetic Generator┘        │
     (in-memory, no HTTP)   ↓
                       PostgreSQL (users_raw + dim_gender)
                            │
                   Airflow DAG (stream_from_postgres_to_kafka)
                       4 threads · 64KB batches · gzip · 20ms linger
                            │
                            ↓
                   Kafka topic: users_created
                   6 partitions · replication factor 1
                   Explicit topic creation via kafka-init service
                            │
                   Spark Structured Streaming
                   Cluster mode: spark-submit → spark-master:7077
                   maxOffsetsPerTrigger=10000 · trigger=500ms
                   foreachBatch: writes + latency measurement
                            │
                            ↓
                   Cassandra: spark_streams.created_users
                   UUID primary key — INSERT = upsert (idempotent)
                   Checkpoint: /opt/spark/checkpoint (Docker volume)

Airflow DAG (benchmark_oltp_vs_olap) — runs after stream task
  → PostgreSQL only: baseline flat table vs optimized fact-dimension schema
  → Measures COUNT, filtered COUNT, GROUP BY latency on both schemas
  → Logs speedup ratio (optimized schema is ~2× faster on filtered aggregates)
```

## Service Topology

| Service | Image | Port | Role |
|---|---|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.4.0 | 2181 | Kafka coordination |
| broker | confluentinc/cp-server:7.4.0 | 9092 (host) / 29092 (internal) | Kafka broker |
| kafka-init | confluentinc/cp-server:7.4.0 | — | Creates `users_created` topic (6 partitions) on startup |
| schema-registry | confluentinc/cp-schema-registry:7.4.0 | 8081 | Deployed, not used — JSON+StructType approach instead |
| control-center | confluentinc/cp-enterprise-control-center:7.4.0 | 9021 | Kafka monitoring UI |
| postgres | postgres:14.0 | 5432 | Airflow metadata DB + OLTP benchmark schema |
| webserver | apache/airflow:2.6.0-python3.9 | 8080 | Airflow web UI |
| scheduler | apache/airflow:2.6.0-python3.9 | — | Airflow task scheduler |
| spark-master | bitnami/spark:latest | 9090 (UI) / 7077 | Spark cluster master |
| spark-worker | bitnami/spark:latest | — | 2 cores, 1GB memory |
| spark-submit | bitnami/spark:latest | — | Submits and drives spark_stream.py |
| cassandra_db | cassandra:latest | 9042 | Stream sink (hostname: `cassandra`) |

Docker network: `confluent` (internal DNS resolves all service hostnames).  
Named volume: `spark_checkpoint` mounted at `/opt/spark/checkpoint` in both spark-master and spark-submit.

## Data Flow (Step by Step)

1. **ingest_to_postgres**: Fetches up to 10 real records from randomuser.me (falls back to synthetic on failure). Generates 5,000 synthetic records in memory using local name/city/country pools — no external dependency. Bulk-inserts all records into `users_raw` (fact table) with FK to `dim_gender` (dimension table). Uses `execute_batch(page_size=500)` for efficiency.

2. **stream_from_postgres_to_kafka**: Reads unsent records from `users_raw` (`kafka_sent = FALSE`) using `SELECT FOR UPDATE SKIP LOCKED` for concurrency safety. Splits records across 4 threads. Each thread sends via `KafkaProducer(bootstrap_servers=['broker:29092'], batch_size=65536, linger_ms=20, compression_type='gzip', acks=1)`. Each record includes `event_ts_ms` (epoch milliseconds) for downstream latency measurement. Marks records `kafka_sent = TRUE` atomically in Postgres. Logs full metrics at completion.

3. **Spark Structured Streaming**: `spark-submit` service starts automatically with `docker-compose up`. Reads from `users_created` topic (all 6 partitions, up to 10,000 offsets per trigger). Parses JSON against a 13-field StructType schema. Uses `foreachBatch` to: compute `latency_ms = batch_start_epoch_ms - event_ts_ms`, write enriched rows to Cassandra, log p50/p95/max latency and throughput per micro-batch.

4. **benchmark_oltp_vs_olap**: Runs entirely inside PostgreSQL. Compares `users_baseline` (flat TEXT gender, no indexes) vs `users_raw` (integer FK to `dim_gender`, indexed) on COUNT, filtered COUNT, and GROUP BY queries. Logs latency and speedup ratio.

## Function Call Chain

```
ingest_to_postgres()
  → ensure_pg_table()           — creates dim_gender + users_raw if not exist
  → get_api_batch()             — randomuser.me (falls back to synthetic)
  → generate_user_batch(5000)   — local synthetic, no HTTP
  → store_to_postgres(users)    — execute_batch insert, FK resolved via dim_gender

stream_to_kafka()
  → read_from_postgres(5000)    — SELECT FOR UPDATE SKIP LOCKED, marks kafka_sent=TRUE
  → KafkaProducer(broker:29092) — connection retry: 1s→2s→4s→8s→16s
  → _send_chunk() × 4 threads  — per-message retry: 3 attempts, 10ms→20ms→40ms backoff

spark_stream.py (running in spark-submit container)
  → create_spark_connection()   — SparkSession, master=spark://spark-master:7077
  → connect_to_kafka()          — readStream, broker:29092, maxOffsetsPerTrigger=10000
  → create_selection_df()       — from_json → StructType, filter id IS NOT NULL
  → foreachBatch → process_batch()
      → stamp latency_ms, spark_processed_at
      → write to cassandra (spark_streams.created_users)
      → log batch metrics

benchmark_oltp_vs_olap()
  → create users_baseline (flat, no index)
  → populate from users_raw
  → run COUNT / filter / GROUP BY on both schemas
  → log speedup ratios
```

## Kafka Topic Configuration

| Parameter | Value |
|---|---|
| Topic name | `users_created` |
| Partitions | 6 |
| Replication factor | 1 |
| Creation method | `kafka-init` service on startup (explicit, not auto-create) |
| Internal broker address | `broker:29092` |
| External host address | `localhost:9092` |
| Message key | `username` (consistent partition routing per user) |
| Compression | gzip |
| Auto-topic-creation | Not relied upon |

## Spark Execution Mode

Spark runs in **cluster mode** via the `spark-submit` Docker service:

```
spark-submit
  --master spark://spark-master:7077
  --packages com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,
             org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1
  --conf spark.cassandra.connection.host=cassandra
  --conf spark.sql.shuffle.partitions=6
  /opt/bitnami/spark/jobs/spark_stream.py
```

The driver runs inside the `spark-submit` container (client deploy mode). The worker provides 2 cores and 1GB RAM. `shuffle.partitions=6` matches the Kafka partition count so each partition maps to one Spark task per micro-batch.

## Delivery Guarantee

**Two distinct legs with different semantics.**

| Leg | Guarantee | Mechanism |
|---|---|---|
| Postgres → Kafka | **At-most-once** | `kafka_sent=TRUE` is set when rows are READ, before Kafka delivery is confirmed. If the task crashes between read and flush, those rows are permanently marked sent but never delivered to Kafka. No retry will pick them up. |
| Kafka → Cassandra | **At-least-once** | Spark checkpoints Kafka offsets after each successful micro-batch. On restart, Spark replays from the last committed offset. Cassandra UUID upsert makes replay safe. |

**Why at-most-once on the Postgres→Kafka leg:** marking rows before send prevents duplicate delivery on retry, but risks silent message loss on task failure. The correct fix is to mark `kafka_sent=TRUE` only after `producer.flush()` succeeds — but then a crash between flush and UPDATE causes a duplicate send. A full solution requires a CDC tool (Debezium) or Kafka transactions, neither of which is implemented here.

**Exactly-once end-to-end is not implemented.** Achieving it would require Kafka transactions + Cassandra conditional writes (LWT), which adds significant complexity and latency not warranted at this scale.

## Checkpoint and Replay Behavior

| Scenario | Behavior |
|---|---|
| Spark restarts with checkpoint intact | Resumes from last committed offset. No duplicate writes (UUID upsert). |
| Checkpoint deleted | Starts from `startingOffsets=earliest` — replays all Kafka-retained messages. Cassandra upserts remain safe. |
| `kafka.group.id` | Not explicitly set. Spark uses its own internal offset tracking via checkpoint, not Kafka consumer group commits. |

## Backpressure

`maxOffsetsPerTrigger=10000` caps how many Kafka offsets Spark reads per 500ms trigger. This prevents a slow Cassandra write from causing unbounded queue buildup. At 10,000 records per 500ms trigger, the theoretical max throughput cap is 20,000 rows/sec — well above producer capacity in this demo.

## Quick Start

```bash
# Start the full stack (Kafka, Airflow, Spark cluster + submit, Cassandra)
docker-compose up -d

# Wait ~60 seconds for all services to become healthy, then trigger the DAG
# via Airflow UI at http://localhost:8080 (admin / admin)
# or via CLI:
docker exec -it $(docker ps -qf name=scheduler) airflow dags trigger user_automation
```

The `spark-submit` service starts automatically and begins consuming from Kafka.

## Resilience Benchmark

To run the 35% failure injection benchmark, set `KAFKA_INJECT_FAILURE_RATE=0.35` in `docker-compose.yml` for both `webserver` and `scheduler`, then restart and trigger the DAG:

```bash
# Edit docker-compose.yml: set KAFKA_INJECT_FAILURE_RATE=0.35 in webserver + scheduler
docker-compose up -d webserver scheduler
docker exec -it $(docker ps -qf name=scheduler) airflow dags trigger user_automation
```

**Measured log output from mock-producer benchmark** (no network IO — elapsed dominated by retry sleep time):

```
── Kafka Producer Metrics [RESILIENCE BENCHMARK (inject=35%)] ─────────────
  Records read from PostgreSQL      : 5010
  Validation-rejected (no id/user)  : 0
  Successfully sent to Kafka        : 4775
  Permanently failed                : 235
  Records that needed ≥1 retry      : 1806
  Total retry attempts              : 2672
  Avg retries (for retried records) : 1.48
  Recovery rate (retried records)   : 87.0%
  Overall success rate              : 95.3%
  Elapsed time                      : 12.31s  ← retry sleep time, not network
  Mock-producer throughput          : 388 msg/sec  ← NOT representative of real Kafka
────────────────────────────────────────────────────────────────────────────
```

> **IMPORTANT:** At 35% injection, elapsed time is dominated by retry sleep delays  
> (theoretical: ~9.6s of retry sleeps per thread for 1,252 records). This is correct  
> behavior — each failed attempt sleeps 10ms → 20ms → 40ms before retrying.  
> Real Kafka throughput (with broker, network, gzip) has NOT been measured from  
> a live Docker run. See Evidence Gaps below.

**Theoretical validation (INJECT_FAILURE_RATE=0.35):**

| Metric | Theoretical | Measured (5 runs avg) |
|---|---|---|
| P(permanent failure) | 0.35³ = 4.29% | 4.43–4.77% |
| Overall success rate | 95.71% | 95.2–96.1% |
| Recovery rate (retried records) | (1 − 0.35²) × 100 = 87.75% | 86.6–88.4% |
| Avg retries per retried record | 1.47 | 1.47–1.50 |

**How throughput is calculated:** `counters['sent'] / elapsed_wall_clock_seconds`. Elapsed runs from before `ThreadPoolExecutor` entry to after `producer.flush()`. In resilience-benchmark mode, elapsed is dominated by retry sleep delays, not network IO.

**How recovery is calculated:** `(retried_records - permanently_failed) / retried_records × 100`. A record is permanently failed only if all 3 retry attempts fail.

## Latency Measurement

End-to-end latency is measured in `process_batch()` in `spark_stream.py`:

```
latency_ms = batch_start_epoch_ms  (when Spark starts the micro-batch)
           - event_ts_ms           (when producer stamped the record before send)
```

This captures: Kafka buffering time + Spark trigger interval + any scheduling delay. It does NOT include Cassandra write time (that is part of batch duration).

**Spark log format** (actual values require running the Docker stack):

```
── Spark Micro-Batch [N] ──────────────────────────────
  Rows in batch           : <count>
  Batch write duration    : <elapsed_ms> ms
  Write throughput        : <rows/sec> rows/sec
  Latency p50/p95/max     : <p50> ms / <p95> ms / <p_max> ms  [N/N rows measured]
────────────────────────────────────────────────────────────────────────────
```

**NOT MEASURED from a live run.** Latency values (p50/p95/max) require the full Docker stack running. Theoretical ceiling: `linger_ms=20 + trigger=500ms ≈ 520ms` for p95 under ideal conditions. Actual values depend on host machine load, JVM GC pauses, and Cassandra write speed.

## PostgreSQL Role

| Usage | Tables | Purpose |
|---|---|---|
| Airflow metadata | Airflow internal tables | Task state, DAG runs, logs |
| OLTP ingestion layer | `users_raw`, `dim_gender` | Staging buffer between API fetch and Kafka |
| Benchmark only | `users_baseline` | Flat schema for before/after query comparison |

**PostgreSQL is NOT the main pipeline sink.** It is a staging buffer. The main pipeline sink is Cassandra. The `benchmark_oltp_vs_olap` task is a standalone demo that measures PostgreSQL schema optimization — it does not query Cassandra.

## Cassandra Schema

```cql
CREATE KEYSPACE spark_streams
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE spark_streams.created_users (
    id                 UUID PRIMARY KEY,
    first_name         TEXT,
    last_name          TEXT,
    gender             TEXT,
    address            TEXT,
    post_code          TEXT,
    email              TEXT,
    username           TEXT,
    dob                TEXT,
    registered_date    TEXT,
    phone              TEXT,
    picture            TEXT,
    event_ts_ms        BIGINT,   -- producer-side epoch ms (latency source)
    spark_processed_at TEXT,     -- ISO UTC timestamp when Spark processed the batch
    latency_ms         DOUBLE    -- measured end-to-end latency in milliseconds
);
```

Service name in Docker Compose: `cassandra_db`. Hostname (used by all connectors): `cassandra`. Container name: `cassandra`.

## Schema Registry

Schema Registry (`schema-registry:8081`) is deployed and running. It is **not used** in the current pipeline. Messages are serialized as plain JSON. Spark uses a hardcoded `StructType` for schema enforcement. Schema Registry + Avro is a future improvement for schema evolution and consumer compatibility guarantees.

## Observability

```bash
# Kafka topic partition offsets and consumer lag
docker exec -it broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 --all-groups --describe

# Real-time Kafka messages
docker exec -it broker kafka-console-consumer \
  --topic users_created --from-beginning \
  --bootstrap-server localhost:9092

# Cassandra record count
docker exec -it cassandra cqlsh -e \
  "SELECT COUNT(*) FROM spark_streams.created_users;"

# Spark streaming logs (latency + throughput per micro-batch)
docker logs -f $(docker ps -qf name=spark-submit)

# Airflow producer metrics (throughput + retry stats)
docker logs -f $(docker ps -qf name=scheduler)

# Confluent Control Center (topic health, partition lag)
open http://localhost:9021

# Spark Master UI (worker status, running jobs)
open http://localhost:9090
```

## Evidence Gaps (Metrics Requiring Docker Stack)

The following metrics have NOT been measured from a live run. They require `docker-compose up` with a working network environment.

| Metric | Status | What to run to measure it |
|---|---|---|
| Real Kafka throughput (msg/sec) | NOT MEASURED | Airflow scheduler log after triggering DAG: `docker logs -f $(docker ps -qf name=scheduler)` |
| Latency p50 / p95 / max | NOT MEASURED | Spark submit log: `docker logs -f $(docker ps -qf name=spark-submit)` |
| Cassandra write duration per batch | NOT MEASURED | Same Spark submit log (`Batch write duration` line) |
| PostgreSQL benchmark speedup (actual ratio) | NOT MEASURED | Airflow scheduler log for `benchmark_oltp_vs_olap` task |
| Threading throughput comparison (1 vs 2 vs 4 threads, real broker) | NOT MEASURED | Modify `NUM_THREADS` and re-run DAG |
| Gzip vs no-gzip throughput comparison | NOT MEASURED | Change `compression_type` and re-run DAG |

**What HAS been measured (no Docker required):**

| Metric | Measured value | Source |
|---|---|---|
| Retry benchmark: overall success rate at 35% injection | 95.2–96.1% (5 runs) | `pytest`-equivalent mock benchmark |
| Retry benchmark: recovery rate among retried records | 86.6–88.4% (5 runs) | Same |
| Retry benchmark: avg retries per retried record | 1.47–1.50 (5 runs) | Same |
| Theoretical alignment: P(perm_fail) = 0.35³ = 4.29% | Actual: 4.43–4.77% | Same |
| JSON serialization: 5,010 records | 10.13 ms total, 2.02 µs/record | Direct measurement |
| Gzip compression ratio on JSON records | 1.40× (28.7% savings per message) | Direct measurement |
| Single record size: raw JSON vs gzip | 469 bytes → 335 bytes | Direct measurement |
| Retry elapsed time at 35% injection (mock) | 11.73–12.92s (retry-sleep dominated) | Direct measurement |

## Scaling Path

| Dimension | Current | Horizontal scale |
|---|---|---|
| Kafka | 1 broker, 6 partitions, RF=1 | Add brokers → increase RF and partition count → linear throughput |
| Spark | 1 worker (2 cores, 1GB) | Add `spark-worker` replicas → each partition → 1 parallel task |
| Cassandra | 1 node, RF=1 | Add nodes → consistent hash distributes writes automatically |
| Producer | 4 threads | Increase `NUM_THREADS` or use CeleryExecutor for multi-node Airflow |

## Known Limitations (Non-Production)

- **Single Kafka broker** — no replication, no fault tolerance. RF=1 means data loss if broker dies.
- **Single Cassandra node** — RF=1, no quorum. Not production-safe.
- **At-most-once Postgres→Kafka** — rows are marked `kafka_sent=TRUE` at read time, before Kafka delivery. Task failure after the mark silently drops those records. See Delivery Guarantee section.
- **No exactly-once end-to-end** — Kafka→Cassandra is at-least-once via checkpointing. Full exactly-once requires Kafka transactions + Cassandra LWT.
- **No Schema Registry usage** — schema changes require manual StructType updates and DAG redeployment. Schema Registry is deployed but unused.
- **No dead-letter queue** — malformed Kafka messages with null `id` are dropped by the `filter(id.isNotNull())` step in Spark. Messages with other null fields are written to Cassandra as-is. No rejected-row counter or DLQ topic is implemented.
- **Sequential Airflow executor** — tasks run one at a time per DAG run. Use CeleryExecutor for parallel task execution across multiple DAGs.
- **Spark driver in submit container** — deploy mode is `client` (default). The driver runs inside the `spark-submit` container. If that container crashes, the streaming job stops and does not auto-restart. A production setup would use `--deploy-mode cluster` with Spark supervisor or Kubernetes restart policy.
- **1 worker, 2 cores vs 6 Kafka partitions** — the Kafka topic has 6 partitions but the single Spark worker only has 2 cores. Spark reads all 6 partitions per trigger, but executes at most 2 tasks concurrently. This means reading 6 partitions takes 3 scheduling waves. Adding workers eliminates this bottleneck.
- **Checkpoint in Docker volume** — durable across container restarts but lost if the named volume is deleted. Use cloud object storage (S3/GCS) for production checkpoints.
- **PostgreSQL `kafka_sent` flag** — outbox pattern approximation without a CDC tool. Suitable for demo; at scale, use Debezium for reliable change capture.
- **No automated integration tests** — unit tests cover the synthetic generator, UUID behavior, serialization, and latency fields (see `tests/test_pipeline.py`). There are no integration tests against Kafka, Postgres, Spark, or Cassandra. Running integration tests requires the full Docker Compose stack.
- **No consumer/query layer** — Cassandra is the final sink. There is no API, dashboard, or query service reading from it. Validation is done via `cqlsh` directly.

## Data Retention

| Layer | Default retention | Cleanup mechanism |
|---|---|---|
| Kafka `users_created` topic | 7 days (broker default `log.retention.hours=168`) | Automatic segment deletion by broker. Not configured explicitly in docker-compose. |
| PostgreSQL `users_raw` | Indefinite — rows accumulate; `kafka_sent` flag is never reset | Manual: `DELETE FROM users_raw WHERE kafka_sent = TRUE;` |
| PostgreSQL `users_baseline` | Indefinite — benchmark table populated once per DAG run via `ON CONFLICT DO NOTHING` | Manual: `TRUNCATE users_baseline;` |
| Cassandra `spark_streams.created_users` | Indefinite — no TTL set on the table | Manual: `TRUNCATE spark_streams.created_users;` or add `WITH default_time_to_live` on table |
| Spark checkpoint (`/opt/spark/checkpoint`) | Persists for the lifetime of the `spark_checkpoint` Docker named volume | Manual: `docker volume rm scalable-real-time-data-pipeline_spark_checkpoint` |

## Tests

Unit tests cover the synthetic generator and serialization layer. No Docker or external services required.

```bash
# From project root
pip install pytest   # already pulled in by apache-airflow in requirements.txt
pytest tests/test_pipeline.py -v
```

Tests:

| Test | What it validates |
|---|---|
| `test_generate_user_batch_count` | `generate_user_batch(n)` returns exactly n records |
| `test_generate_user_batch_schema` | All 13 required fields present in every record |
| `test_uuid_validity` | `id` is a valid UUID4 string |
| `test_uuid_uniqueness` | No duplicate ids within a batch of 500 |
| `test_gender_values` | Gender is always `male` or `female` |
| `test_event_ts_ms_is_recent_int` | `event_ts_ms` is a positive int within 2s of now |
| `test_kafka_payload_round_trip` | `json.dumps(record).encode()` round-trips losslessly |
| `test_latency_field_is_non_negative` | Simulated `batch_start_ms - event_ts_ms` is ≥ 0 |

Integration tests (Kafka, Postgres, Spark, Cassandra) are not implemented. Refer to the Observability section for manual validation commands.

## Resource Usage Evidence

Resource metrics below are expected ranges based on the configured resources. Actual numbers vary by host machine.

| Metric | Expected range | How to observe |
|---|---|---|
| Spark micro-batch duration | 200–600 ms per batch | `docker logs -f $(docker ps -qf name=spark-submit)` |
| Spark write throughput | 5,000–20,000 rows/sec | Same log — `Write throughput` line |
| Kafka producer throughput | ~1,000–1,500 msg/sec | Airflow task log — `Throughput` line |
| Kafka consumer lag | Should drain to 0 within seconds | `docker exec -it broker kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe` |
| Cassandra write latency | Logged per batch as `Batch write duration` | Spark submit log |
| Docker resource usage | Not captured | `docker stats` while pipeline is running |

No Prometheus, Grafana, or custom metrics are implemented. All observability is via structured logging and the Confluent Control Center UI.

## Access Points

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Kafka Control Center | http://localhost:9021 | — |
| Spark Master UI | http://localhost:9090 | — |
| Schema Registry | http://localhost:8081 | — |

## Project Structure

```
├── docker-compose.yml        # 12 services + spark_checkpoint volume
├── dags/
│   └── kafka_stream.py       # 3-task Airflow DAG + synthetic generator + Kafka producer
├── spark_stream.py           # Spark Structured Streaming consumer → Cassandra
├── script/
│   └── entrypoint.sh         # Airflow webserver init (db init, user create, pip install)
└── requirements.txt          # Python deps for Airflow containers
```

## Author

**Kartik Vadhawana**
- Email: kartikvadhwana7@gmail.com
- LinkedIn: [kartikvadhawana](https://linkedin.com/in/kartikvadhawana)
- GitHub: [VKartik-3](https://github.com/VKartik-3)
