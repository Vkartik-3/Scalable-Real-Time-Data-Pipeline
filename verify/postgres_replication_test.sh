#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# PostgreSQL streaming-replication proof (primary → hot-standby replica).
#
# Proves replication is REAL:
#   1. Confirm primary sees a connected streaming replica (pg_stat_replication).
#   2. Confirm replica is in recovery / read-only (pg_is_in_recovery = t).
#   3. Write a row on the PRIMARY.
#   4. Read that same row from the REPLICA — must appear (WAL streamed across).
#   5. Confirm the replica REJECTS writes (read-only standby).
#
# Run after `docker compose -f docker-compose.multinode.yml up -d` once both
# `postgres` and `postgres-replica` are healthy.
#
# Usage:  ./verify/postgres_replication_test.sh
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

PRIMARY=postgres
REPLICA=postgres-replica
DB=airflow
U=airflow
export PGPASSWORD=airflow
MARK="repltest_$(date +%s)"

pq() { docker exec -e PGPASSWORD=airflow -i "$1" psql -U "$U" -d "$DB" -tAc "$2"; }

echo "════════════════════════════════════════════════════════════════"
echo " POSTGRESQL STREAMING REPLICATION TEST (primary → replica)"
echo "════════════════════════════════════════════════════════════════"

echo; echo "── Step 1: primary sees a streaming replica ──────────────────"
docker exec -e PGPASSWORD=airflow "$PRIMARY" psql -U "$U" -d "$DB" \
  -c "SELECT client_addr, state, sync_state FROM pg_stat_replication;"
REPL_COUNT=$(pq "$PRIMARY" "SELECT count(*) FROM pg_stat_replication;")
echo "Connected replicas: $REPL_COUNT (expected >= 1)"

echo; echo "── Step 2: replica is read-only standby ──────────────────────"
IN_REC=$(pq "$REPLICA" "SELECT pg_is_in_recovery();")
echo "replica pg_is_in_recovery() = $IN_REC (expected t)"

echo; echo "── Step 3: write a row on the PRIMARY ────────────────────────"
pq "$PRIMARY" "CREATE TABLE IF NOT EXISTS repl_check (mark TEXT PRIMARY KEY, ts TIMESTAMP DEFAULT now());"
pq "$PRIMARY" "INSERT INTO repl_check (mark) VALUES ('$MARK');"
echo "Inserted mark=$MARK on primary."

echo; echo "── Step 4: read it back from the REPLICA ─────────────────────"
sleep 2   # allow WAL to stream
FOUND=$(pq "$REPLICA" "SELECT mark FROM repl_check WHERE mark='$MARK';" || true)
if [ "$FOUND" = "$MARK" ]; then
  echo "PASS ✅  mark replicated to standby → streaming replication is REAL."
  R1=PASS
else
  echo "FAIL ❌  mark not found on replica (got: '$FOUND')."
  R1=FAIL
fi

echo; echo "── Step 5: replica must REJECT writes (read-only) ────────────"
if pq "$REPLICA" "INSERT INTO repl_check (mark) VALUES ('should_fail');" 2>/dev/null; then
  echo "FAIL ❌  replica accepted a write — not a true standby."
  R2=FAIL
else
  echo "PASS ✅  replica rejected the write (read-only hot standby, as expected)."
  R2=PASS
fi

echo; echo "════════════════════════════════════════════════════════════════"
echo " RESULT: replication=$R1  read-only=$R2  (mark=$MARK)"
echo "════════════════════════════════════════════════════════════════"
[ "$R1" = PASS ] && [ "$R2" = PASS ]
