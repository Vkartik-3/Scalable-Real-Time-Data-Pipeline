# Multi-Node Replication — Runbook & Results

This stack runs a **3-node Cassandra cluster** (NetworkTopologyStrategy, RF=3, QUORUM)
and a **3-broker Kafka cluster** (RF=3, min.insync.replicas=2, acks=all), and proves
fault tolerance by killing a node/broker and confirming reads/writes still succeed.

> **Cannot be executed in the Claude environment** (no Docker daemon, 16 GB host).
> Run on your machine with Docker memory raised to **≥ 12 GB** (Settings → Resources).

## What changed vs single-node

| Component | Single-node (`docker-compose.yml`) | Multi-node (`docker-compose.multinode.yml`) |
|---|---|---|
| Cassandra | 1 node, SimpleStrategy, RF=1, default `LOCAL_ONE` | 3 nodes, **NetworkTopologyStrategy RF=3**, **QUORUM** reads+writes, GossipingPropertyFileSnitch, 3 racks |
| Kafka | 1 broker, topic RF=1, acks=1 | **3 brokers**, topic **RF=3**, **min.insync.replicas=2**, **acks=all** |
| App code | env defaults reproduce single-node | same code, env-driven (no fork) |
| PostgreSQL | 1 instance | **primary + 1 hot-standby read replica** (streaming replication, replica on :5433) |

App code is identical; topology is selected by env vars set in each compose file
(`CASSANDRA_HOSTS`, `CASSANDRA_REPLICATION_STRATEGY`, `CASSANDRA_REPLICATION_FACTOR`,
`KAFKA_BOOTSTRAP`, `KAFKA_ACKS`).

## Run sequence

```bash
# 0. Raise Docker Desktop memory to >=12 GB first.

# 1. Stand up the multi-node stack (Cassandra bootstraps ONE node at a time;
#    full come-up takes several minutes — watch for 3x UN).
docker compose -f docker-compose.multinode.yml up -d

# 2. Wait until all 3 Cassandra nodes are UN and all 3 brokers are healthy:
watch -n5 'docker exec cassandra-1 nodetool status'
docker compose -f docker-compose.multinode.yml ps

# 3. PROVE Cassandra fault tolerance (RF=3 / QUORUM):
./verify/cassandra_failure_test.sh

# 4. PROVE Kafka fault tolerance (RF=3 / minISR=2 / acks=all):
./verify/kafka_failure_test.sh

# 4b. PROVE PostgreSQL streaming replication (primary → read replica):
./verify/postgres_replication_test.sh

# 5. MEASURE real numbers (replaces the README "Evidence Gaps" estimates):
docker exec -it $(docker ps -qf name=scheduler) airflow dags trigger user_automation
docker logs -f $(docker ps -qf name=scheduler)     # Kafka producer throughput + retry stats
docker logs -f spark-submit                        # Spark latency p50/p95/max + Cassandra write duration
# benchmark_oltp_vs_olap task log → real PostgreSQL speedup ratio (the "2x" claim)
```

## RECORDED RESULTS — live run 2026-06-28 (single-cluster-at-a-time, 7.7 GB Docker)

All three replication proofs executed and **PASSED**. Run one cluster at a time
because Docker was allocated only 7.7 GB (full stack needs ~12 GB).

| Store | Config verified | Failure injected | Outcome |
|---|---|---|---|
| Cassandra | `NetworkTopologyStrategy {datacenter1:3}`, 3 nodes UN across rack1/2/3, QUORUM | stopped cassandra-3 (→ DN) | **PASS** — row written at QUORUM stayed readable on 2 surviving replicas; node rejoined UN |
| Kafka | topic `users_created` RF=3, 6 partitions, minISR=2, ISR=[1,2,3] | stopped broker-2 (ISR→[1,3], leaders failed over) | **PASS** — produced 500 more at acks=all (1000→1500 offsets), consumed **1500/1500, 0 lost**; ISR returned to 3 |
| PostgreSQL | primary + 1 hot standby, `pg_stat_replication`=1, replica `pg_is_in_recovery()=t` | n/a (replication+read-only check) | **PASS** — row on primary appeared on replica; replica rejected writes (read-only) |

Environment notes from the run:
- `bitnami/postgresql:14` tag no longer resolves (Bitnami catalog change, 2025);
  switched to `bitnamilegacy/postgresql:14`.
- Host port 5432/5433 were occupied; remapped primary→5544, replica→5545 (host side
  only — tests use `docker exec`, so internal 5432 is unchanged).

## STEP 4 — MEASURED NUMBERS (live run 2026-06-28, real 3-broker Kafka + Postgres)

Ran the DAG's tasks directly (Airflow bypassed — see bugs below) against the real
multi-node Kafka + Postgres. Numbers are MEASURED, not estimated.

| Claim | Estimate in docs | MEASURED | Verdict |
|---|---|---|---|
| Kafka producer throughput "1,000+ events/sec" | ~1,000–1,500 (never measured) | **15,801 msg/sec** (5,000 msgs in 0.32s, acks=all, gzip, 3 brokers) | ✅ TRUE — and conservative. Caveat: producer-side enqueue+flush, non-blocking send |
| Producer success rate (no injection) | — | 5,000/5,000 = 100% | ✅ |
| Query "doubled performance (2×)" | ~2× faster | **0.7–0.8× (optimized was SLOWER)** at 10,020 rows | ❌ FALSE as measured — do not claim |

2× detail (10,020 rows): COUNT 0.3ms→0.4ms (0.8×), Filtered COUNT 0.4ms→0.6ms (0.7×),
GROUP BY 1.1ms→1.5ms (0.7×). At demo scale the indexed fact-dimension JOIN costs more
than the flat sequential scan saves. The optimization could win at millions of rows
(index scan avoids full-table read) — but that is UNTESTED. As measured, 2× is false.

Spark end-to-end latency p50/p95: NOT measured — needs full stack (Spark+Cassandra+
Kafka together ≈12 GB), which doesn't fit in this 7.7 GB Docker allocation.

### Pre-existing bugs found by actually running it (all fixed)
1. `requirements.txt`: `Flask-AppBuilder==4.3.3` conflicts with airflow 2.6.0
   (`==4.3.0`) → Airflow webserver couldn't start. Fixed to 4.3.0.
2. `read_from_postgres()`: `FOR UPDATE SKIP LOCKED` on a `LEFT JOIN` →
   Postgres error "FOR UPDATE cannot be applied to the nullable side of an outer
   join". `stream_to_kafka()` had never run successfully. Fixed to `FOR UPDATE OF u`.

## Results template — FILL IN FROM YOUR RUN (do not guess)

### Cassandra fault tolerance
- [ ] `nodetool status` showed **3x UN** before test: ____
- [ ] `system_schema.keyspaces` replication = `{'NetworkTopologyStrategy','datacenter1':'3'}`: ____
- [ ] Wrote row at QUORUM, then stopped `cassandra-3`
- [ ] Read same row back at QUORUM with node down → **PASS / FAIL**: ____
- [ ] Node rejoined (UN) after restart: ____

### Kafka fault tolerance
- [ ] Topic describe showed **ReplicationFactor: 3**, ISR=3 per partition: ____
- [ ] Stopped `broker-2`; under-replicated partitions appeared (ISR=2): ____
- [ ] Produced + consumed with broker down → **PASS / FAIL**: ____
- [ ] ISR returned to 3 after restart: ____

### PostgreSQL streaming replication
- [ ] `pg_stat_replication` on primary shows ≥1 connected replica (state=streaming): ____
- [ ] Replica `pg_is_in_recovery()` = `t`: ____
- [ ] Row written on primary appeared on replica → **PASS / FAIL**: ____
- [ ] Replica rejected a write (read-only standby): ____

### Measured numbers (multi-node)
| Metric | Single-node estimate (old) | Multi-node MEASURED (this run) |
|---|---|---|
| Kafka producer throughput (msg/sec) | ~1,000–1,500 (estimate, never measured) | __________ |
| Spark latency p50 / p95 / max (ms) | not measured | __________ |
| Cassandra write duration / batch (ms) | not measured | __________ |
| PostgreSQL benchmark speedup ("2x") | ~2x (expected, never measured) | __________ |

> Expect throughput to be **lower** than single-node for the same hardware:
> `acks=all` + RF=3 means every write waits for 2 replicas to ack instead of 1.
> That is the correct, honest trade-off — durability for latency. Report it as such.

## Interview-ready summary (fill after the run)

- Cassandra: **NetworkTopologyStrategy, RF=3, QUORUM** reads/writes. Proved fault
  tolerance by killing 1 of 3 nodes and confirming the row stayed readable at
  QUORUM (2/3 replicas satisfy quorum).
- Kafka: **3 brokers, RF=3, min.insync.replicas=2, acks=all**. Proved fault
  tolerance by killing 1 broker and confirming produce+consume continued with
  ISR=2, no data loss.
- Throughput trade-off measured: ____ msg/sec multi-node (acks=all) vs ____ single-node.
