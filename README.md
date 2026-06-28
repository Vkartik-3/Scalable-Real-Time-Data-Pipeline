# End-to-End Real-Time Data Engineering Pipeline

Airflow → PostgreSQL → Kafka → Spark Structured Streaming → Cassandra, with a
**genuinely multi-node, replicated** deployment whose fault tolerance is proven
by killing nodes/brokers and observing the system survive (not just configured
on paper).

## Two deployment modes

| | `docker-compose.yml` (single-node demo) | `docker-compose.multinode.yml` (replicated) |
|---|---|---|
| Cassandra | 1 node, `SimpleStrategy`, RF=1 | **3 nodes, `NetworkTopologyStrategy`, RF=3**, racks 1/2/3 |
| Cassandra consistency | QUORUM client (1 replica at RF=1) | **QUORUM** reads + writes (2 of 3 replicas) |
| Kafka | 1 broker, topic RF=1 | **3 brokers, topic RF=3**, `min.insync.replicas=2` |
| Kafka producer acks | `all` (leader only at 1 broker) | **`all`** (waits for 2 in-sync replicas) |
| PostgreSQL | 1 instance | **primary + 1 hot-standby read replica** (streaming) |
| Purpose | fast local demo, low RAM | distributed replication + fault-tolerance proofs |

The **same application code** drives both — topology is selected by environment
variables (no code fork). See [Configuration](#configuration-env-driven).

---

## Architecture

```
randomuser.me API ─┐
                   ├─→ Airflow DAG: ingest_to_postgres
Synthetic Generator┘        │  (5,000 synthetic + ~10 live API records, in-memory)
     (in-memory, no HTTP)   ↓
                       PostgreSQL (users_raw fact + dim_gender dimension)
                       multi-node: primary ──stream WAL──▶ read replica
                            │
                   Airflow DAG: stream_from_postgres_to_kafka
                       4 threads · 64KB batches · gzip · 20ms linger · acks=all
                            │
                            ↓
                   Kafka topic: users_created   (key = username)
                   single-node: 6 partitions · RF=1
                   multi-node : 6 partitions · RF=3 · minISR=2
                            │
                   Spark Structured Streaming  (spark-submit → spark-master:7077)
                   maxOffsetsPerTrigger=10000 · trigger=500ms
                   foreachBatch: enrich + latency + write to Cassandra at QUORUM
                            │
                            ↓
                   Cassandra: spark_streams.created_users
                   single-node: SimpleStrategy RF=1
                   multi-node : NetworkTopologyStrategy RF=3, QUORUM
                   UUID primary key — INSERT = upsert (idempotent on replay)

Airflow DAG: benchmark_oltp_vs_olap  (PostgreSQL-only, after stream task)
  → baseline flat table vs optimized fact-dimension schema (see Schema Optimization)
```

## Service Topology

**Shared services** (both modes): Airflow webserver + scheduler, Spark master +
worker + submit.

| Service | Image | Port | Role |
|---|---|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.4.0 | 2181 | Kafka coordination |
| broker(s) | confluentinc/cp-kafka:7.4.0 (multi) / cp-server (single) | 9092–9094 | Kafka broker(s) |
| schema-registry | confluentinc/cp-schema-registry:7.4.0 | 8081 | Deployed, **unused** (JSON+StructType instead) |
| postgres | bitnamilegacy/postgresql:14 (multi) / postgres:14.0 (single) | 5432/5544 | OLTP staging + Airflow metadata |
| postgres-replica | bitnamilegacy/postgresql:14 | 5545 | **Hot-standby read replica** (multi-node only) |
| webserver / scheduler | apache/airflow:2.6.0-python3.9 | 8080 | Airflow UI + scheduler |
| spark-master / worker / submit | bitnami/spark:latest | 9090 / 7077 | Spark cluster + driver |
| cassandra(-1/2/3) | cassandra:4.1 | 9042–9044 | Stream sink (3 nodes in multi-node) |

> **Note (Bitnami images):** Docker Hub restructured the Bitnami catalog in 2025;
> `bitnami/postgresql:14` no longer resolves, so the multi-node Postgres uses
> `bitnamilegacy/postgresql:14`. `bitnami/spark:latest` may need the same change.

---

## Multi-Node Replication

This is the core of the project. Each store is genuinely replicated, and each
claim was **verified live** (see [Fault-Tolerance Verification](#fault-tolerance-verification)).

### Cassandra — NetworkTopologyStrategy, RF=3, QUORUM

- **3 nodes**, `GossipingPropertyFileSnitch`, single DC `datacenter1`, racks
  `rack1 / rack2 / rack3` (so RF=3 spreads one replica per rack).
- Nodes **bootstrap one at a time** (health-gated `depends_on` chain) — Cassandra
  requires sequential joins.
- Keyspace replication (built from env in [spark_stream.py](spark_stream.py)):
  ```cql
  CREATE KEYSPACE spark_streams
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'};
  ```
- **Consistency level is explicit, not the driver default.** The DDL session sets
  `ConsistencyLevel.QUORUM`; the Spark Cassandra connector sets
  `spark.cassandra.output.consistency.level=QUORUM` and `input.consistency.level=QUORUM`.
  At RF=3, QUORUM = 2 replicas → **survives one node down** for both reads and writes.

### Kafka — 3 brokers, RF=3, min.insync.replicas=2, acks=all

- **3 brokers** registered in ZooKeeper, topic `users_created`: **6 partitions,
  replication factor 3**, `min.insync.replicas=2`.
- Producer uses **`acks=all`** ([dags/kafka_stream.py](dags/kafka_stream.py)) — a
  write is only acknowledged once ≥2 in-sync replicas have it. This is what makes
  RF meaningful: with the old `acks=1`, a leader-only ack could be lost on failover.
- **Partition key = `username`** (`producer.send('users_created', key=username, …)`)
  → consistent-hash routing, per-user ordering, even spread across partitions.

### PostgreSQL — primary + hot-standby read replica (streaming replication)

- `postgres` (primary) streams WAL to `postgres-replica` (read-only hot standby).
- Replica is in continuous recovery (`pg_is_in_recovery() = t`) and **rejects writes**.
- Primary keeps hostname `postgres` so Airflow's connection string is unchanged.
- Host ports remapped to **5544 (primary) / 5545 (replica)** to avoid clashing with
  a local Postgres on 5432.

### Configuration (env-driven)

The same code runs both topologies; the multi-node compose sets these:

| Env var | Single-node default | Multi-node value |
|---|---|---|
| `CASSANDRA_HOSTS` | `cassandra` | `cassandra-1,cassandra-2,cassandra-3` |
| `CASSANDRA_REPLICATION_STRATEGY` | `SimpleStrategy` | `NetworkTopologyStrategy` |
| `CASSANDRA_REPLICATION_FACTOR` | `1` | `3` |
| `CASSANDRA_DC` | `datacenter1` | `datacenter1` |
| `KAFKA_BOOTSTRAP` | `broker:29092` | `broker-1:29092,broker-2:29092,broker-3:29092` |
| `KAFKA_ACKS` | `all` | `all` |

---

## Fault-Tolerance Verification

Replication is only real if it survives a failure. Three scripts in [verify/](verify/)
inject a real failure and assert recovery. **All three passed on a live run
(2026-06-28)** — see [verify/MULTINODE_RUNBOOK.md](verify/MULTINODE_RUNBOOK.md).

| Store | Script | What it does | Result |
|---|---|---|---|
| Cassandra | [verify/cassandra_failure_test.sh](verify/cassandra_failure_test.sh) | Write a row at QUORUM → **stop cassandra-3** (goes `DN`) → re-read at QUORUM with node down → restart, confirm rejoin | **PASS** — row stayed readable on the 2 surviving replicas |
| Kafka | [verify/kafka_failure_test.sh](verify/kafka_failure_test.sh) | Produce 1,000 at acks=all → **stop broker-2** (ISR shrinks 3→2, leaders fail over) → produce 500 more + consume all | **PASS** — **1,500/1,500 messages, 0 lost** |
| PostgreSQL | [verify/postgres_replication_test.sh](verify/postgres_replication_test.sh) | Write on primary → read it on replica → attempt a write on replica | **PASS** — row streamed across; replica rejected the write (read-only) |

> The run was executed **one cluster at a time** because Docker was allocated 7.7 GB;
> the full 15-service stack needs ~12 GB. The proofs are independent and each fits.

---

## Measured Performance

These are **measured from live runs**, not estimates, unless explicitly noted.

### Kafka producer throughput — measured ~15,800 msg/sec

| Metric | Value | Conditions |
|---|---|---|
| Records sent | 5,000 / 5,000 (100%) | 3-broker Kafka, RF=3, `acks=all`, gzip, 64KB batches, 4 threads |
| Elapsed | 0.32 s | from `ThreadPoolExecutor` entry to after `producer.flush()` |
| **Throughput** | **~15,801 msg/sec** | producer-side enqueue + flush |

> Caveat to state honestly: `producer.send()` is non-blocking (enqueues into the
> producer buffer); `flush()` confirms broker delivery. So this is **producer-side
> throughput to a replicated cluster**, not end-to-end through Spark.

### Retry resilience under fault injection (simulated)

A configurable failure injector (`KAFKA_INJECT_FAILURE_RATE`) raises a simulated
transient error before each send to exercise the 3-attempt, 10→20→40ms backoff.
Recovery/retry counts are computed from real in-process counters; the failures are
**simulated, not a live broker fault**. At 35% injection (formula-consistent):

| Metric | Value |
|---|---|
| Overall success rate (`1 − p³`) | **95.7%** |
| Recovery rate among retried records (`1 − p²`) | **87.8%** |
| Avg retries per retried record (`1 + p + p²`) | **1.47** |

### OLAP query acceleration via pre-aggregation — measured ~1,600×

Analytical (GROUP BY / aggregate) queries are accelerated with a **materialized
view** that pre-computes the rollup, instead of scanning the fact table on every
read. Benchmarked at **5,000,000 rows** (warm cache) — reproducible via
[verify/olap_materialized_view_benchmark.sql](verify/olap_materialized_view_benchmark.sql):

| Query (5M rows) | Time | Speedup |
|---|---|---|
| Live `GROUP BY` over flat baseline | ~138 ms | 1× |
| Live `GROUP BY` over fact-dimension join | ~210 ms | 0.65× |
| **`SELECT` from materialized view** (pre-aggregated) | **~0.08 ms** | **~1,600×** |

```sql
CREATE MATERIALIZED VIEW mv_gender_counts AS
SELECT d.gender_label, COUNT(*) AS cnt
FROM users_raw u JOIN dim_gender d ON u.gender_id = d.gender_id
GROUP BY d.gender_label;
```

**Honest trade-off:** a materialized view is a precomputed snapshot — it must be
refreshed (`REFRESH MATERIALIZED VIEW [CONCURRENTLY]`) when the underlying data
changes. The speedup is the read latency of a pre-aggregated result vs. scanning
5M rows each time.

> Note: **normalization alone does not speed these queries up.** Measured at both
> 10K and 5M rows, the fact-dimension schema with an index on `gender_id` was
> *slower* than the flat table (~0.65–0.8×) — at 50% selectivity (male/female) the
> planner correctly sequential-scans either way and the join adds cost (`EXPLAIN`
> confirms `Parallel Seq Scan` on both). The win comes from **pre-aggregation**,
> not the FK/index. The fact-dimension model's value here is data modeling and
> referential integrity, not raw query speed.

### Not yet measured

- **Spark end-to-end latency (p50/p95/max)** and **Cassandra write duration per
  batch** — require Spark + Cassandra + Kafka up together (~12 GB), which did not
  fit in the 7.7 GB Docker allocation used for verification. Theoretical p95 ceiling:
  `linger 20ms + trigger 500ms ≈ 520ms`.

---

## Kafka Topic Configuration

| Parameter | Single-node | Multi-node |
|---|---|---|
| Topic | `users_created` | `users_created` |
| Partitions | 6 | 6 |
| Replication factor | 1 | **3** |
| `min.insync.replicas` | 1 | **2** |
| Producer `acks` | `all` | `all` |
| Message key | `username` (consistent per-user routing) | same |
| Compression | gzip | gzip |
| Creation | explicit (`kafka-topics --create`), not auto-create | same |

## Cassandra Schema

```cql
-- multi-node replication
CREATE KEYSPACE spark_streams
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'};

CREATE TABLE spark_streams.created_users (
    id                 UUID PRIMARY KEY,
    first_name TEXT, last_name TEXT, gender TEXT, address TEXT, post_code TEXT,
    email TEXT, username TEXT, dob TEXT, registered_date TEXT, phone TEXT, picture TEXT,
    event_ts_ms        BIGINT,   -- producer-side epoch ms (latency source)
    spark_processed_at TEXT,     -- ISO UTC when Spark processed the batch
    latency_ms         DOUBLE    -- measured end-to-end latency (ms)
);
```

Writes go through the Spark Cassandra connector at **QUORUM**. `id` is the partition
key, so re-inserts are idempotent upserts — safe on Spark micro-batch replay.

## Delivery Guarantee

| Leg | Guarantee | Mechanism |
|---|---|---|
| Postgres → Kafka | **At-most-once** | rows marked `kafka_sent=TRUE` at read time, before delivery is confirmed. Prevents duplicates; risks loss on task crash. A full outbox/CDC (Debezium) would give at-least-once. |
| Kafka → Cassandra | **At-least-once** | Spark checkpoints offsets per micro-batch; replay on restart; Cassandra UUID upsert makes replay safe. |

Exactly-once end-to-end is **not** implemented (would need Kafka transactions +
Cassandra LWT).

---

## Bugs Found & Fixed During Verification

Running the pipeline for real (not just reading config) surfaced four genuine bugs.
Each is fixed; together they indicate the pipeline had not previously been run
end-to-end.

1. **Cassandra retry never reached its advertised backoff.** [spark_stream.py](spark_stream.py)
   claimed `1s → 2s → 4s` but the loop raised after 3 attempts, so it only ever slept
   `1s, 2s`. Fixed to 4 attempts → genuine `1s → 2s → 4s`.
2. **Airflow could not start.** [requirements.txt](requirements.txt) pinned
   `Flask-AppBuilder==4.3.3`, conflicting with `apache-airflow==2.6.0` (needs `4.3.0`).
   Fixed to `4.3.0`.
3. **`stream_to_kafka()` had never worked.** [dags/kafka_stream.py](dags/kafka_stream.py)
   used `FOR UPDATE SKIP LOCKED` on a query with a `LEFT JOIN dim_gender`; Postgres
   rejects this ("FOR UPDATE cannot be applied to the nullable side of an outer join").
   Fixed to `FOR UPDATE OF u SKIP LOCKED` (lock only the fact-table rows).
4. **Mislabeled retry metrics.** Comments called the 95.7% *success* rate "recovery"
   and quoted 1.35 retries; corrected to success 95.7% / recovery 87.8% / 1.47 retries.

---

## Quick Start

### Single-node demo (low RAM)
```bash
docker-compose up -d
# Airflow UI http://localhost:8080 (admin/admin) → trigger DAG 'user_automation'
```

### Multi-node replicated stack
```bash
# Raise Docker Desktop → Settings → Resources → Memory to >=12 GB first.
docker compose -f docker-compose.multinode.yml up -d
watch -n5 'docker exec cassandra-1 nodetool status'   # wait for 3x UN

# Prove fault tolerance:
./verify/cassandra_failure_test.sh
./verify/kafka_failure_test.sh
./verify/postgres_replication_test.sh
```

See [verify/MULTINODE_RUNBOOK.md](verify/MULTINODE_RUNBOOK.md) for the full sequence,
recorded results, and a template for measuring the remaining (Spark-latency) numbers.

## Known Limitations

- **Single-node demo has no redundancy** (RF=1) — use the multi-node compose for
  fault tolerance.
- **Spark driver in `client` mode** inside the submit container — no auto-restart if
  it crashes. Production would use `--deploy-mode cluster` + supervisor/k8s.
- **At-most-once Postgres→Kafka** (see Delivery Guarantee).
- **Schema Registry deployed but unused** — JSON + Spark `StructType` instead.
- **No dead-letter queue** — malformed records with null `id` are dropped in Spark.
- **Normalization is not a query-speed optimization** — the fact-dimension schema
  measured ~0.65–0.8× (slower) vs. the flat table at 10K and 5M rows. OLAP speed
  comes from the materialized-view pre-aggregation (~1,600×), not the FK/index.
- **Materialized view requires refresh** — pre-aggregated rollup is a snapshot;
  stale until `REFRESH MATERIALIZED VIEW`.
- **Spark end-to-end latency unmeasured** — needs the full stack (~12 GB).

## Observability

```bash
docker exec -it cassandra-1 nodetool status                      # Cassandra ring
docker exec -it broker-1 kafka-topics --bootstrap-server broker-1:29092 \
  --describe --topic users_created                               # RF / ISR
docker exec -it cassandra-1 cqlsh -e \
  "SELECT COUNT(*) FROM spark_streams.created_users;"            # sink count
docker logs -f spark-submit                                      # Spark batch metrics
docker logs -f $(docker ps -qf name=scheduler)                   # producer metrics
```

## Project Structure

```
├── docker-compose.yml             # single-node demo
├── docker-compose.multinode.yml   # 3-node Cassandra + 3-broker Kafka + PG primary/replica
├── dags/kafka_stream.py           # Airflow DAG: ingest, stream-to-kafka, OLAP benchmark
├── spark_stream.py                # Spark Structured Streaming consumer → Cassandra (QUORUM)
├── verify/
│   ├── cassandra_failure_test.sh  # RF=3 / QUORUM node-kill proof
│   ├── kafka_failure_test.sh      # RF=3 / acks=all broker-kill proof
│   ├── postgres_replication_test.sh
│   └── MULTINODE_RUNBOOK.md       # run steps + recorded results + measured numbers
├── script/entrypoint.sh           # Airflow init
└── requirements.txt               # Airflow deps (Flask-AppBuilder pinned to 4.3.0)
```

## Author

**Kartik Vadhawana** — [GitHub](https://github.com/Vkartik-3) ·
[LinkedIn](https://linkedin.com/in/kartikvadhawana)
