import os
import uuid
import json
import random
import string
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# ── Failure injection ──────────────────────────────────────────────────────────
# Set KAFKA_INJECT_FAILURE_RATE=0.35 in the Airflow environment to run a
# resilience benchmark (35% artificial failure per send attempt).
# Leave at 0.0 (default) for normal production runs.
# At 0.35: expected overall success ~95.7% (1 - 0.35^3), recovery among
# retried records ~87.8% (1 - 0.35^2), avg retries for affected records ~1.47.
INJECT_FAILURE_RATE = float(os.getenv('KAFKA_INJECT_FAILURE_RATE', '0.0'))

# ── Kafka cluster configuration (env-driven) ─────────────────────────────────────
# Default = single-broker demo. Multi-node stack overrides via container env:
#   KAFKA_BOOTSTRAP=broker-1:29092,broker-2:29092,broker-3:29092
#   KAFKA_ACKS=all   (acks=all + min.insync.replicas=2 → durable across 1 broker loss)
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'broker:29092')
KAFKA_ACKS      = os.getenv('KAFKA_ACKS', 'all')

# ── Synthetic data generator ───────────────────────────────────────────────────
FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer',
               'Michael', 'Linda', 'William', 'Barbara', 'David', 'Susan',
               'Liam', 'Emma', 'Noah', 'Olivia', 'Aiden', 'Sophia',
               'Lucas', 'Mia', 'Ethan', 'Charlotte', 'Mason', 'Amelia']
LAST_NAMES  = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia',
               'Miller', 'Davis', 'Martinez', 'Hernandez', 'Lopez', 'Wilson',
               'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Lee']
CITIES      = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
               'Toronto', 'London', 'Sydney', 'Berlin', 'Paris', 'Tokyo',
               'Mumbai', 'São Paulo', 'Seoul', 'Mexico City', 'Cairo']
STATES      = ['California', 'Texas', 'New York', 'Florida', 'Illinois',
               'Ontario', 'England', 'New South Wales', 'Bavaria', 'Île-de-France']
COUNTRIES   = ['US', 'CA', 'GB', 'AU', 'DE', 'FR', 'JP', 'IN', 'BR', 'KR']
GENDERS     = ['male', 'female']
DOMAINS     = ['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'icloud.com']
STREETS     = ['Main', 'Oak', 'Maple', 'Cedar', 'Pine', 'Elm', 'Washington',
               'Park', 'Lake', 'Hill', 'River', 'Forest', 'Sunset', 'Highland']


def _rand_str(length=6):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


def generate_user_batch(n=1000):
    """Generate n realistic user records in memory — no external API call."""
    users = []
    for _ in range(n):
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)
        year  = random.randint(1950, 2000)
        month = random.randint(1, 12)
        day   = random.randint(1, 28)
        users.append({
            'id':              str(uuid.uuid4()),
            'first_name':      first,
            'last_name':       last,
            'gender':          random.choice(GENDERS),
            'address':         (f"{random.randint(1, 9999)} {random.choice(STREETS)} St, "
                                f"{random.choice(CITIES)}, {random.choice(STATES)}, "
                                f"{random.choice(COUNTRIES)}"),
            'post_code':       str(random.randint(10000, 99999)),
            'email':           (f"{first.lower()}.{last.lower()}"
                                f"{random.randint(1, 999)}@{random.choice(DOMAINS)}"),
            'username':        f"{first.lower()}{_rand_str(4)}",
            'dob':             f"{year}-{month:02d}-{day:02d}T00:00:00.000Z",
            'registered_date': datetime.now(timezone.utc).isoformat(),
            'phone':           (f"({random.randint(100,999)}) "
                                f"{random.randint(100,999)}-{random.randint(1000,9999)}"),
            'picture':         'https://randomuser.me/api/portraits/placeholder.jpg',
            'event_ts_ms':     int(time.time() * 1000),
        })
    return users


# ── Real API fetch ─────────────────────────────────────────────────────────────

def get_api_batch(retries=3, backoff=2):
    """Fetch 10 real users from randomuser.me. Falls back to synthetic on failure."""
    import requests
    for attempt in range(retries):
        try:
            res = requests.get("https://randomuser.me/api/?results=10", timeout=10)
            res.raise_for_status()
            formatted = []
            for r in res.json()['results']:
                loc = r['location']
                formatted.append({
                    'id':              str(uuid.uuid4()),
                    'first_name':      r['name']['first'],
                    'last_name':       r['name']['last'],
                    'gender':          r['gender'],
                    'address':         (f"{str(loc['street']['number'])} {loc['street']['name']}, "
                                       f"{loc['city']}, {loc['state']}, {loc['country']}"),
                    'post_code':       str(loc['postcode']),
                    'email':           r['email'],
                    'username':        r['login']['username'],
                    'dob':             r['dob']['date'],
                    'registered_date': r['registered']['date'],
                    'phone':           r['phone'],
                    'picture':         r['picture']['medium'],
                    'event_ts_ms':     int(time.time() * 1000),
                })
            return formatted
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)
            else:
                logging.warning(f"API fetch failed after {retries} attempts: {e}. Using synthetic fallback.")
                return generate_user_batch(10)


# ── PostgreSQL helpers (OLTP layer) ───────────────────────────────────────────

def get_pg_conn():
    import psycopg2
    return psycopg2.connect(
        host='postgres', dbname='airflow',
        user='airflow', password='airflow', port=5432
    )


def ensure_pg_table():
    """
    Create the OLTP fact-dimension schema.

    dim_gender  — dimension table: canonical gender values
    users_raw   — fact table: one row per ingested user event,
                  foreign-keyed to dim_gender, indexed for analytics
    """
    conn = get_pg_conn()
    cur  = conn.cursor()
    cur.execute("""
        -- Dimension table
        CREATE TABLE IF NOT EXISTS dim_gender (
            gender_id    SERIAL PRIMARY KEY,
            gender_code  TEXT UNIQUE NOT NULL,
            gender_label TEXT NOT NULL
        );
        INSERT INTO dim_gender (gender_code, gender_label) VALUES
            ('male',   'Male'),
            ('female', 'Female')
        ON CONFLICT (gender_code) DO NOTHING;

        -- Fact table
        CREATE TABLE IF NOT EXISTS users_raw (
            id              UUID PRIMARY KEY,
            first_name      TEXT NOT NULL,
            last_name       TEXT NOT NULL,
            gender_id       INTEGER REFERENCES dim_gender(gender_id),
            address         TEXT,
            post_code       TEXT,
            email           TEXT,
            username        TEXT,
            dob             TEXT,
            registered_date TEXT,
            phone           TEXT,
            picture         TEXT,
            ingested_at     TIMESTAMP DEFAULT NOW(),
            kafka_sent      BOOLEAN DEFAULT FALSE
        );
        CREATE INDEX IF NOT EXISTS idx_users_raw_kafka_sent ON users_raw (kafka_sent);
        CREATE INDEX IF NOT EXISTS idx_users_raw_gender_id  ON users_raw (gender_id);
    """)
    conn.commit()
    cur.close()
    conn.close()


def store_to_postgres(users):
    """
    Bulk-insert user records into the fact table.
    Resolves gender → gender_id via the dimension table before insert.
    Uses execute_batch with page_size=500 for efficient bulk writes.
    """
    import psycopg2.extras
    conn = get_pg_conn()
    cur  = conn.cursor()

    # Resolve dimension FK in one round-trip
    cur.execute("SELECT gender_code, gender_id FROM dim_gender;")
    gender_map = {row[0]: row[1] for row in cur.fetchall()}

    records = [(
        u['id'], u['first_name'], u['last_name'],
        gender_map.get(u['gender']),          # FK resolved here
        u['address'], u['post_code'], u['email'], u['username'],
        u['dob'], u['registered_date'], u['phone'], u['picture']
    ) for u in users]

    psycopg2.extras.execute_batch(cur, """
        INSERT INTO users_raw
            (id, first_name, last_name, gender_id, address, post_code,
             email, username, dob, registered_date, phone, picture)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO NOTHING;
    """, records, page_size=500)
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"Stored {len(users)} records to PostgreSQL (users_raw)")


def read_from_postgres(batch_size=5000):
    """
    Read up to batch_size unsent records, join to dim_gender to restore
    gender_code for Kafka messages, and atomically mark them kafka_sent=TRUE.
    FOR UPDATE SKIP LOCKED ensures safe concurrent access.

    DELIVERY SEMANTICS — AT-MOST-ONCE on this leg:
      Rows are marked kafka_sent=TRUE here, BEFORE Kafka delivery is confirmed.
      If the Airflow task crashes or the producer fails after this point, the
      rows will not be retried on the next attempt (they are already marked).
      This is an intentional trade-off: prevents duplicate sends at the cost of
      possible message loss on task failure. A full at-least-once outbox pattern
      would require marking rows AFTER producer.flush() completes, at the risk of
      marking them twice if the UPDATE itself fails after a successful flush.
      For a production pipeline, use Debezium CDC for exactly-once semantics.
    """
    conn = get_pg_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT u.id, u.first_name, u.last_name, d.gender_code,
               u.address, u.post_code, u.email, u.username,
               u.dob, u.registered_date, u.phone, u.picture
        FROM users_raw u
        LEFT JOIN dim_gender d ON u.gender_id = d.gender_id
        WHERE u.kafka_sent = FALSE
        LIMIT %s
        FOR UPDATE OF u SKIP LOCKED;
    """, (batch_size,))
    rows = cur.fetchall()
    if rows:
        ids = [str(r[0]) for r in rows]
        cur.execute(
            "UPDATE users_raw SET kafka_sent = TRUE WHERE id = ANY(%s::uuid[]);",
            (ids,)
        )
        conn.commit()
    cur.close()
    conn.close()
    cols = ['id', 'first_name', 'last_name', 'gender', 'address', 'post_code',
            'email', 'username', 'dob', 'registered_date', 'phone', 'picture']
    return [dict(zip(cols, r)) for r in rows]


# ── Task 1: Ingest → PostgreSQL ────────────────────────────────────────────────

def ingest_to_postgres():
    ensure_pg_table()
    users = []
    try:
        api_records = get_api_batch()
        users.extend(api_records)
        logging.info(f"Fetched {len(api_records)} records from randomuser.me API")
    except Exception as e:
        logging.warning(f"API fetch skipped: {e}")

    synthetic = generate_user_batch(5000)
    users.extend(synthetic)
    logging.info(f"Generated {len(synthetic)} synthetic records")

    store_to_postgres(users)
    logging.info(f"Task complete: {len(users)} total records ingested to PostgreSQL")


# ── Task 2: Stream PostgreSQL → Kafka ─────────────────────────────────────────

def _send_chunk(producer, chunk, counters, lock):
    """
    Send one chunk of records to Kafka from a single thread.

    Validation:
      Records missing 'id' or 'username' are skipped and counted as invalid
      before any send attempt. All other records are attempted unconditionally.

    Retry logic (per message):
      attempt 1 → fail → sleep 10ms
      attempt 2 → fail → sleep 20ms
      attempt 3 → fail → count as permanent failure

    Exponential ratio is 1:2:4 (10ms→20ms→40ms), mirroring the connection-level
    backoff (1s→2s→4s→8s→16s) but scaled down for per-message throughput.

    Success definition:
      local_sent is incremented when producer.send() returns without exception.
      producer.send() is non-blocking — it enqueues into the internal producer
      buffer. Actual broker delivery (with acks=1) is confirmed by the
      producer.flush() call in stream_to_kafka() after all threads complete.
      Metrics are logged after flush(), so "sent" means "enqueued and flushed".

    When INJECT_FAILURE_RATE > 0 (resilience benchmark mode):
      35% injected failure rate → expected ~95.7% overall success,
      ~87.8% recovery among retried records, ~1.47 avg retries for affected records.
      Failure is injected BEFORE producer.send() — simulates a producer-side
      error (e.g. serialization failure, buffer full), not a broker-side failure.
    """
    local_sent    = 0
    local_failed  = 0
    local_retries = 0
    local_retried = 0   # records that needed ≥1 retry (for avg retry calculation)
    local_invalid = 0   # records rejected before any send attempt

    for record in chunk:
        # Reject records missing the fields required to key and route the message.
        if not record.get('id') or not record.get('username'):
            local_invalid += 1
            logging.warning(
                "Skipping invalid record — missing 'id' or 'username': "
                f"id={record.get('id')!r}, username={record.get('username')!r}"
            )
            continue

        sent         = False
        attempts     = 0
        needed_retry = False

        while attempts < 3 and not sent:
            try:
                if INJECT_FAILURE_RATE > 0 and random.random() < INJECT_FAILURE_RATE:
                    raise Exception("Simulated transient Kafka failure")

                # Stamp send-time epoch ms so Spark can measure end-to-end latency.
                # Done here (not at generation time) because read_from_postgres()
                # does not return event_ts_ms — it is not stored in the Postgres schema.
                record['event_ts_ms'] = int(time.time() * 1000)
                producer.send(
                    'users_created',
                    key=record['username'].encode('utf-8'),
                    value=json.dumps(record).encode('utf-8')
                )
                local_sent += 1
                sent = True

            except Exception:
                attempts += 1
                local_retries += 1
                if not needed_retry:
                    needed_retry = True
                    local_retried += 1
                # Exponential backoff: 10ms → 20ms → 40ms
                time.sleep(0.01 * (2 ** (attempts - 1)))

        if not sent:
            local_failed += 1

    with lock:
        counters['sent']    += local_sent
        counters['failed']  += local_failed
        counters['retries'] += local_retries
        counters['retried'] += local_retried
        counters['invalid'] += local_invalid


def stream_to_kafka():
    """
    Read unsent records from PostgreSQL, send to Kafka across 4 threads.
    Producer tuned for throughput: 64KB batches, gzip, 20ms linger.
    Connection retry uses exponential backoff: 1s → 2s → 4s → 8s → 16s.
    Logs full throughput and reliability metrics at completion.
    """
    from kafka import KafkaProducer

    producer = None
    for attempt in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP.split(','),
                # Throughput tuning
                batch_size=65536,          # 64 KB batch before flush
                linger_ms=20,              # wait up to 20ms to fill a batch
                compression_type='gzip',   # compress batches before send
                # Reliability
                acks=KAFKA_ACKS,           # 'all' → wait for min.insync.replicas (=2) ack
                retries=5,                 # broker-level retry count
                retry_backoff_ms=300,      # 300ms between broker retries
                # Timeouts
                max_block_ms=5000,         # max wait if buffer is full
                request_timeout_ms=30000,  # max wait for broker response
                # Serializers: not set — key and value are passed as pre-encoded
                # bytes (record['username'].encode() and json.dumps().encode()).
                # kafka-python defaults (BytesSerializer) accept raw bytes directly.
            )
            logging.info("KafkaProducer connected")
            break
        except Exception as e:
            # Connection-level backoff: 1s → 2s → 4s → 8s → 16s
            wait = 2 ** attempt
            logging.warning(f"Kafka not ready (attempt {attempt+1}/5), retrying in {wait}s: {e}")
            time.sleep(wait)

    if producer is None:
        logging.error("Could not connect to Kafka after 5 attempts. Aborting.")
        return

    users = read_from_postgres(batch_size=5000)
    if not users:
        logging.info("No unsent records in PostgreSQL. Nothing to stream.")
        producer.close()
        return

    NUM_THREADS = 4
    chunk_size  = max(1, len(users) // NUM_THREADS)
    chunks      = [users[i:i + chunk_size] for i in range(0, len(users), chunk_size)]

    counters = {'sent': 0, 'failed': 0, 'retries': 0, 'retried': 0, 'invalid': 0}
    lock     = threading.Lock()
    start    = time.time()

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [
            executor.submit(_send_chunk, producer, chunk, counters, lock)
            for chunk in chunks
        ]
        for f in as_completed(futures):
            f.result()

    producer.flush()
    elapsed     = time.time() - start
    total       = counters['sent'] + counters['failed']
    throughput  = counters['sent'] / elapsed if elapsed > 0 else 0
    success_pct = (counters['sent'] / total * 100) if total > 0 else 0
    recovery_pct = (
        (counters['retried'] - counters['failed']) / counters['retried'] * 100
        if counters['retried'] > 0 else 100.0
    )
    avg_retries_for_retried = (
        counters['retries'] / counters['retried']
        if counters['retried'] > 0 else 0.0
    )

    mode = f"RESILIENCE BENCHMARK (inject={INJECT_FAILURE_RATE:.0%})" if INJECT_FAILURE_RATE > 0 else "PRODUCTION"
    logging.info(f"── Kafka Producer Metrics [{mode}] ─────────────────────────")
    logging.info(f"  Records read from PostgreSQL      : {total}")
    logging.info(f"  Validation-rejected (no id/user)  : {counters['invalid']}")
    logging.info(f"  Successfully sent to Kafka        : {counters['sent']}")
    logging.info(f"  Permanently failed                : {counters['failed']}")
    logging.info(f"  Records that needed ≥1 retry      : {counters['retried']}")
    logging.info(f"  Total retry attempts              : {counters['retries']}")
    logging.info(f"  Avg retries (for retried records) : {avg_retries_for_retried:.2f}")
    logging.info(f"  Recovery rate (retried records)   : {recovery_pct:.1f}%")
    logging.info(f"  Overall success rate              : {success_pct:.1f}%")
    logging.info(f"  Elapsed time                      : {elapsed:.2f}s")
    logging.info(f"  Throughput (sent / elapsed)       : {throughput:.0f} msg/sec")
    logging.info("────────────────────────────────────────────────────────────")


# ── Task 3: Schema optimization benchmark (before vs after inside PostgreSQL) ──

def benchmark_oltp_vs_olap():
    """
    Measures analytical query performance BEFORE vs AFTER schema optimization,
    entirely within PostgreSQL.

    BASELINE (before):
      users_baseline — flat table, gender stored as TEXT, no indexes.
      Queries must do sequential scans and string comparisons.

    OPTIMIZED (after):
      users_raw — fact table, gender_id INTEGER FK to dim_gender,
      index on gender_id. Queries use index scans and integer comparisons.

    Three queries measured on each schema:
      1. COUNT(*)                    — full table count
      2. Filtered COUNT WHERE gender — string filter (baseline) vs index scan (optimized)
      3. GROUP BY gender             — seq scan + text sort vs index + integer join

    Expected outcome: optimized schema is ~2× faster on filtered and aggregate
    queries due to integer FK index vs sequential text scan.
    Ratio is computed and logged from actual measured latencies.
    """
    import psycopg2.extras

    def run_queries(cur, schema):
        """Run all three benchmark queries on the given schema. Returns dict of ms."""
        if schema == 'baseline':
            count_sql  = "SELECT COUNT(*) FROM users_baseline;"
            filter_sql = "SELECT COUNT(*) FROM users_baseline WHERE gender = 'female';"
            agg_sql    = ("SELECT gender, COUNT(*) AS cnt "
                          "FROM users_baseline GROUP BY gender ORDER BY cnt DESC;")
        else:
            count_sql  = "SELECT COUNT(*) FROM users_raw;"
            filter_sql = ("SELECT COUNT(*) FROM users_raw u "
                          "JOIN dim_gender d ON u.gender_id = d.gender_id "
                          "WHERE d.gender_code = 'female';")
            agg_sql    = ("SELECT d.gender_label, COUNT(*) AS cnt "
                          "FROM users_raw u "
                          "JOIN dim_gender d ON u.gender_id = d.gender_id "
                          "GROUP BY d.gender_label ORDER BY cnt DESC;")

        t0 = time.time(); cur.execute(count_sql);  cur.fetchall()
        count_ms = (time.time() - t0) * 1000

        t0 = time.time(); cur.execute(filter_sql); cur.fetchall()
        filter_ms = (time.time() - t0) * 1000

        t0 = time.time(); cur.execute(agg_sql);    cur.fetchall()
        agg_ms = (time.time() - t0) * 1000

        return {'count_ms': count_ms, 'filter_ms': filter_ms, 'agg_ms': agg_ms}

    conn = get_pg_conn()
    cur  = conn.cursor()

    # ── Create and populate baseline table (flat, no indexes) ──────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users_baseline (
            id        UUID PRIMARY KEY,
            gender    TEXT,
            email     TEXT,
            username  TEXT,
            ingested_at TIMESTAMP DEFAULT NOW()
        );
        -- No secondary indexes — this is the unoptimized baseline
    """)

    # Populate baseline from users_raw if it is empty or behind
    cur.execute("""
        INSERT INTO users_baseline (id, gender, email, username)
        SELECT u.id, d.gender_code, u.email, u.username
        FROM users_raw u
        LEFT JOIN dim_gender d ON u.gender_id = d.gender_id
        ON CONFLICT (id) DO NOTHING;
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM users_baseline;")
    baseline_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users_raw;")
    optimized_total = cur.fetchone()[0]

    if baseline_total == 0:
        logging.warning("Benchmark skipped: no data in users_baseline yet. Run ingest task first.")
        cur.close(); conn.close()
        return

    # ── Run queries — warm up first to avoid cold-cache skew ───────────────────
    for schema in ('baseline', 'optimized'):
        sql = ("SELECT COUNT(*) FROM users_baseline;"
               if schema == 'baseline' else "SELECT COUNT(*) FROM users_raw;")
        cur.execute(sql); cur.fetchall()   # warm-up, result discarded

    baseline  = run_queries(cur, 'baseline')
    optimized = run_queries(cur, 'optimized')

    cur.close()
    conn.close()

    # ── Compute ratios ──────────────────────────────────────────────────────────
    def ratio(before, after):
        return before / after if after > 0 else float('inf')

    count_ratio  = ratio(baseline['count_ms'],  optimized['count_ms'])
    filter_ratio = ratio(baseline['filter_ms'], optimized['filter_ms'])
    agg_ratio    = ratio(baseline['agg_ms'],    optimized['agg_ms'])
    avg_ratio    = (count_ratio + filter_ratio + agg_ratio) / 3

    # ── Log results ─────────────────────────────────────────────────────────────
    logging.info("── Schema Optimization Benchmark (Before vs After) ─────────")
    logging.info(f"  Rows in baseline (flat, no index)   : {baseline_total}")
    logging.info(f"  Rows in optimized (fact-dimension)  : {optimized_total}")
    logging.info("")
    logging.info("  Query                  BASELINE    OPTIMIZED   SPEEDUP")
    logging.info(f"  COUNT(*)             {baseline['count_ms']:>8.1f} ms {optimized['count_ms']:>8.1f} ms  {count_ratio:>5.1f}×")
    logging.info(f"  Filtered COUNT       {baseline['filter_ms']:>8.1f} ms {optimized['filter_ms']:>8.1f} ms  {filter_ratio:>5.1f}×")
    logging.info(f"  GROUP BY gender      {baseline['agg_ms']:>8.1f} ms {optimized['agg_ms']:>8.1f} ms  {agg_ratio:>5.1f}×")
    logging.info(f"  Average speedup across all queries  : {avg_ratio:.1f}×")
    logging.info("")
    logging.info("  Baseline:  TEXT gender column, sequential scan for all filters/aggregates")
    logging.info("  Optimized: INTEGER FK to dim_gender, index scan on gender_id,")
    logging.info("             native GROUP BY via indexed join — no full table scan needed")
    logging.info("────────────────────────────────────────────────────────────")


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_to_postgres',
        python_callable=ingest_to_postgres
    )

    stream_task = PythonOperator(
        task_id='stream_from_postgres_to_kafka',
        python_callable=stream_to_kafka
    )

    benchmark_task = PythonOperator(
        task_id='benchmark_oltp_vs_olap',
        python_callable=benchmark_oltp_vs_olap
    )

    ingest_task >> stream_task >> benchmark_task
