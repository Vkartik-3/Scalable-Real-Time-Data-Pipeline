"""
Minimal unit tests for the pipeline's data layer.

These tests run without Docker, Kafka, Postgres, Spark, or Cassandra.
They validate the synthetic generator schema, UUID behaviour, serialization,
and latency-field contracts that are core interview claims.

Run:
    pip install pytest        # (already in requirements.txt via airflow dep)
    pytest tests/test_pipeline.py -v
"""
import json
import time
import uuid
import sys
import os

# Allow importing from the dags directory without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))
from kafka_stream import generate_user_batch

REQUIRED_FIELDS = [
    'id', 'first_name', 'last_name', 'gender', 'address',
    'post_code', 'email', 'username', 'dob', 'registered_date',
    'phone', 'picture', 'event_ts_ms',
]


def test_generate_user_batch_count():
    """generate_user_batch(n) returns exactly n records."""
    assert len(generate_user_batch(1))    == 1
    assert len(generate_user_batch(10))   == 10
    assert len(generate_user_batch(5000)) == 5000


def test_generate_user_batch_schema():
    """Every record contains all 13 required fields — no silent omissions."""
    for record in generate_user_batch(50):
        for field in REQUIRED_FIELDS:
            assert field in record, f"Field '{field}' missing from record: {record}"


def test_uuid_validity():
    """The 'id' field is a valid UUID4 string for every generated record."""
    for record in generate_user_batch(50):
        parsed = uuid.UUID(record['id'])          # raises ValueError if invalid
        assert parsed.version == 4


def test_uuid_uniqueness():
    """No two records in the same batch share an id."""
    batch = generate_user_batch(500)
    ids = [r['id'] for r in batch]
    assert len(ids) == len(set(ids)), "Duplicate UUIDs found in batch"


def test_gender_values():
    """Gender is always one of the two canonical values."""
    for record in generate_user_batch(100):
        assert record['gender'] in ('male', 'female'), (
            f"Unexpected gender value: {record['gender']!r}"
        )


def test_event_ts_ms_is_recent_int():
    """event_ts_ms is a positive integer within 2 seconds of 'now'."""
    before = int(time.time() * 1000)
    batch  = generate_user_batch(5)
    after  = int(time.time() * 1000)
    for record in batch:
        ts = record['event_ts_ms']
        assert isinstance(ts, int),   f"event_ts_ms is not int: {type(ts)}"
        assert ts > 0,                f"event_ts_ms is not positive: {ts}"
        assert before <= ts <= after, f"event_ts_ms {ts} outside [{before}, {after}]"


def test_kafka_payload_round_trip():
    """
    Records survive JSON encode → bytes → decode with no data loss.
    This validates the exact path used in _send_chunk:
        json.dumps(record).encode('utf-8')
    and the reverse parse Spark applies via from_json.
    """
    for record in generate_user_batch(20):
        payload = json.dumps(record).encode('utf-8')
        decoded = json.loads(payload.decode('utf-8'))
        assert decoded['id']           == record['id']
        assert decoded['event_ts_ms']  == record['event_ts_ms']
        assert decoded['username']     == record['username']
        assert decoded['gender']       == record['gender']


def test_latency_field_is_non_negative():
    """
    Simulates the Spark latency calculation:
        latency_ms = batch_start_ms - event_ts_ms
    Asserts the value is >= 0 when computed immediately after generation.
    """
    batch          = generate_user_batch(10)
    batch_start_ms = int(time.time() * 1000)
    for record in batch:
        latency = batch_start_ms - record['event_ts_ms']
        assert latency >= 0, (
            f"Negative latency {latency}ms — event_ts_ms is in the future"
        )


if __name__ == '__main__':
    test_generate_user_batch_count()
    test_generate_user_batch_schema()
    test_uuid_validity()
    test_uuid_uniqueness()
    test_gender_values()
    test_event_ts_ms_is_recent_int()
    test_kafka_payload_round_trip()
    test_latency_field_is_non_negative()
    print("All 8 tests passed.")
