#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Cassandra fault-tolerance proof for RF=3 / QUORUM.
#
# Proves replication is REAL, not just configured on paper:
#   1. Confirm 3 nodes are UP/Normal and replication strategy is NTS RF=3.
#   2. Write a row at CONSISTENCY QUORUM.
#   3. Kill one node (cassandra-3).
#   4. Read the SAME row back at CONSISTENCY QUORUM — must still succeed
#      (2 of 3 replicas alive satisfies QUORUM). THIS is the proof of fault
#      tolerance: the data survives and stays readable with a node down.
#   5. Restart the node and confirm it rejoins.
#
# Run AFTER `docker compose -f docker-compose.multinode.yml up -d` and the
# Cassandra nodes are healthy (all three show UN in `nodetool status`).
#
# Usage:  ./verify/cassandra_failure_test.sh
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NODE1=cassandra-1
KILL=cassandra-3
KS=spark_streams
TEST_ID=$(uuidgen 2>/dev/null || python3 -c "import uuid;print(uuid.uuid4())")

cql() { docker exec -i "$NODE1" cqlsh -e "$1"; }

echo "════════════════════════════════════════════════════════════════"
echo " CASSANDRA RF=3 / QUORUM FAULT-TOLERANCE TEST"
echo "════════════════════════════════════════════════════════════════"

echo; echo "── Step 1: cluster topology ──────────────────────────────────"
docker exec "$NODE1" nodetool status
UP_COUNT=$(docker exec "$NODE1" nodetool status | grep -c '^UN' || true)
echo "Nodes UP/Normal (UN): $UP_COUNT"
if [ "$UP_COUNT" -ne 3 ]; then
  echo "FAIL: expected 3 UN nodes before starting. Wait for bootstrap to finish."
  exit 1
fi

echo; echo "── Step 2: ensure keyspace (NTS RF=3) + table, then write at QUORUM ──"
cql "CREATE KEYSPACE IF NOT EXISTS $KS WITH replication =
     {'class':'NetworkTopologyStrategy','datacenter1':'3'};"
cql "CREATE TABLE IF NOT EXISTS $KS.created_users (id UUID PRIMARY KEY, username TEXT);"
echo "Replication strategy in effect:"
cql "SELECT keyspace_name, replication FROM system_schema.keyspaces WHERE keyspace_name='$KS';"
docker exec -i "$NODE1" cqlsh -e \
  "CONSISTENCY QUORUM; INSERT INTO $KS.created_users (id, username) VALUES ($TEST_ID, 'failtest');"
echo "Wrote row id=$TEST_ID at QUORUM."
echo "Replica nodes that own this key:"
docker exec "$NODE1" nodetool getendpoints "$KS" created_users "$TEST_ID"

echo; echo "── Step 3: KILL one node ($KILL) ─────────────────────────────"
docker stop "$KILL"
sleep 8
docker exec "$NODE1" nodetool status | sed -n '1,12p'

echo; echo "── Step 4: read the row back at QUORUM with $KILL DOWN ───────"
echo "   (QUORUM for RF=3 = 2 replicas; 2 of 3 alive → must succeed)"
if docker exec -i "$NODE1" cqlsh -e \
     "CONSISTENCY QUORUM; SELECT id, username FROM $KS.created_users WHERE id=$TEST_ID;" \
     | grep -q failtest; then
  echo "PASS ✅  Row is still readable at QUORUM with one node down."
  echo "        → RF=3/QUORUM is providing REAL fault tolerance."
  RESULT=PASS
else
  echo "FAIL ❌  Row not readable — replication/consistency not behaving as expected."
  RESULT=FAIL
fi

echo; echo "── Step 5: restart $KILL and let it rejoin ───────────────────"
docker start "$KILL"
echo "Waiting for $KILL to rejoin (UN)…"
for i in $(seq 1 24); do
  sleep 5
  if docker exec "$NODE1" nodetool status | grep -E "^UN" | grep -q "$KILL\|$(docker inspect -f '{{.NetworkSettings.Networks.confluent.IPAddress}}' $KILL 2>/dev/null)"; then break; fi
done
docker exec "$NODE1" nodetool status

echo; echo "════════════════════════════════════════════════════════════════"
echo " RESULT: $RESULT  (id=$TEST_ID)"
echo "════════════════════════════════════════════════════════════════"
[ "$RESULT" = PASS ]
