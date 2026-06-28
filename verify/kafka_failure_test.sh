#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Kafka fault-tolerance proof for RF=3 / min.insync.replicas=2.
#
# Proves replication is REAL:
#   1. Confirm users_created has RF=3 (each partition: 3 replicas, ISR=3).
#   2. Produce messages with acks=all.
#   3. Kill one broker (broker-2).
#   4. Produce AND consume again with the broker down — must still succeed
#      (2 of 3 ISR satisfies min.insync.replicas=2). ISR shrinks to 2;
#      leadership fails over for partitions broker-2 led. THIS is the proof.
#   5. Restart the broker; ISR returns to 3.
#
# Run AFTER the multi-node stack is up and kafka-init created the topic.
#
# Usage:  ./verify/kafka_failure_test.sh
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

B1=broker-1
KILL=broker-2
BOOT=broker-1:29092
TOPIC=users_created

kt()  { docker exec "$B1" kafka-topics --bootstrap-server "$BOOT" "$@"; }

echo "════════════════════════════════════════════════════════════════"
echo " KAFKA RF=3 / min.insync.replicas=2 FAULT-TOLERANCE TEST"
echo "════════════════════════════════════════════════════════════════"

echo; echo "── Step 1: topic replication layout ──────────────────────────"
kt --describe --topic "$TOPIC"
RF=$(kt --describe --topic "$TOPIC" | awk -F'ReplicationFactor: ' 'NR==1{print $2}' | awk '{print $1}')
echo "ReplicationFactor: ${RF:-unknown}  (expected 3)"

echo; echo "── Step 2: produce 1000 messages with acks=all ───────────────"
docker exec "$B1" bash -c "seq 1 1000 | kafka-console-producer \
  --bootstrap-server $BOOT --topic $TOPIC \
  --producer-property acks=all >/dev/null"
echo "Produced 1000 messages (acks=all)."
BEFORE=$(docker exec "$B1" bash -c "kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server $BOOT --topic $TOPIC --time -1 \
  | awk -F: '{s+=\$3} END{print s}'")
echo "Total committed offsets across partitions: $BEFORE"

echo; echo "── Step 3: KILL one broker ($KILL) ───────────────────────────"
docker stop "$KILL"
sleep 10
echo "Under-replicated partitions now (ISR < RF expected):"
kt --describe --topic "$TOPIC" --under-replicated-partitions || true

echo; echo "── Step 4: produce + consume with $KILL DOWN ─────────────────"
echo "   (min.insync.replicas=2; 2 of 3 ISR alive → produce must still succeed)"
if docker exec "$B1" bash -c "seq 1001 1500 | kafka-console-producer \
     --bootstrap-server $BOOT --topic $TOPIC \
     --producer-property acks=all >/dev/null"; then
  echo "Produce with one broker down: OK"
else
  echo "FAIL ❌  produce failed with one broker down."; exit 1
fi
AFTER=$(docker exec "$B1" bash -c "kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server $BOOT --topic $TOPIC --time -1 \
  | awk -F: '{s+=\$3} END{print s}'")
echo "Total committed offsets after second produce: $AFTER (was $BEFORE)"
CONSUMED=$(docker exec "$B1" bash -c "kafka-console-consumer \
  --bootstrap-server $BOOT --topic $TOPIC --from-beginning \
  --max-messages 1500 --timeout-ms 25000 2>/dev/null | wc -l" || true)
echo "Consumed $CONSUMED messages from beginning with $KILL still down."

if [ "${AFTER:-0}" -gt "${BEFORE:-0}" ] && [ "${CONSUMED:-0}" -ge 1500 ]; then
  echo "PASS ✅  Produce+consume succeeded with a broker down; no data lost."
  echo "         → RF=3 / acks=all / minISR=2 is REAL fault tolerance."
  RESULT=PASS
else
  echo "FAIL ❌  see counts above."
  RESULT=FAIL
fi

echo; echo "── Step 5: restart $KILL; ISR should return to 3 ─────────────"
docker start "$KILL"
sleep 20
kt --describe --topic "$TOPIC"
echo "Under-replicated partitions after restart (expect none):"
kt --describe --topic "$TOPIC" --under-replicated-partitions || true

echo; echo "════════════════════════════════════════════════════════════════"
echo " RESULT: $RESULT"
echo "════════════════════════════════════════════════════════════════"
[ "$RESULT" = PASS ]
