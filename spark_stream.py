import logging
import os
import time
from datetime import datetime, timezone

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ── Cluster / replication configuration (env-driven) ─────────────────────────────
# Defaults reproduce the single-node demo (docker-compose.yml). The multi-node
# stack (docker-compose.multinode.yml) overrides these via container env:
#   CASSANDRA_HOSTS=cassandra-1,cassandra-2,cassandra-3
#   CASSANDRA_REPLICATION_STRATEGY=NetworkTopologyStrategy
#   CASSANDRA_REPLICATION_FACTOR=3
#   CASSANDRA_DC=datacenter1
#   KAFKA_BOOTSTRAP=broker-1:29092,broker-2:29092,broker-3:29092
CASSANDRA_HOSTS    = os.getenv('CASSANDRA_HOSTS', 'cassandra').split(',')
CASSANDRA_STRATEGY = os.getenv('CASSANDRA_REPLICATION_STRATEGY', 'SimpleStrategy')
CASSANDRA_RF       = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')
CASSANDRA_DC       = os.getenv('CASSANDRA_DC', 'datacenter1')
KAFKA_BOOTSTRAP    = os.getenv('KAFKA_BOOTSTRAP', 'broker:29092')


def _replication_clause():
    """Build the CQL replication map from env. NetworkTopologyStrategy keys by DC."""
    if CASSANDRA_STRATEGY == 'NetworkTopologyStrategy':
        return "{'class': 'NetworkTopologyStrategy', '%s': '%s'}" % (CASSANDRA_DC, CASSANDRA_RF)
    return "{'class': 'SimpleStrategy', 'replication_factor': '%s'}" % CASSANDRA_RF


def create_keyspace(session):
    clause = _replication_clause()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {clause};
    """)
    logging.info(f"Keyspace created successfully! (replication = {clause})")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
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
            event_ts_ms        BIGINT,
            spark_processed_at TEXT,
            latency_ms         DOUBLE
        );
    """)
    logging.info("Table created successfully!")


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .master('spark://spark-master:7077') \
            .config('spark.jars.packages',
                    'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,'
                    'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1') \
            .config('spark.cassandra.connection.host', ','.join(CASSANDRA_HOSTS)) \
            .config('spark.cassandra.output.consistency.level', 'QUORUM') \
            .config('spark.cassandra.input.consistency.level', 'QUORUM') \
            .config('spark.sql.shuffle.partitions', '6') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option('maxOffsetsPerTrigger', 10000) \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created: {e}")
    return spark_df


def create_cassandra_connection():
    try:
        cluster = Cluster(CASSANDRA_HOSTS)
        cas_session = cluster.connect()
        # Explicit QUORUM for the DDL session. With RF=3 this requires 2 of 3
        # replicas to ack — survives one node down. (Driver default is LOCAL_ONE.)
        cas_session.default_consistency_level = ConsistencyLevel.QUORUM
        logging.info(f"Cassandra connection established to {CASSANDRA_HOSTS} "
                     f"at consistency QUORUM")
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection: {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    # Schema enforced by Spark StructType — not Schema Registry.
    # Schema Registry is deployed but unused; JSON + StructType is the active strategy.
    # Future improvement: register Avro/Protobuf schema for schema evolution guarantees.
    schema = StructType([
        StructField("id",               StringType(), False),
        StructField("first_name",       StringType(), False),
        StructField("last_name",        StringType(), False),
        StructField("gender",           StringType(), False),
        StructField("address",          StringType(), False),
        StructField("post_code",        StringType(), False),
        StructField("email",            StringType(), False),
        StructField("username",         StringType(), False),
        StructField("dob",              StringType(), False),
        StructField("registered_date",  StringType(), False),
        StructField("phone",            StringType(), False),
        StructField("picture",          StringType(), False),
        StructField("event_ts_ms",      LongType(),   True),
    ])

    # from_json silently returns null for any field that cannot be parsed.
    # The filter(id.isNotNull()) drops rows where JSON was entirely malformed
    # (non-parseable payload → all fields null → id is null).
    # Rows with partial nulls (e.g. null event_ts_ms) are passed through and
    # handled in process_batch (latency is not computed for those rows).
    # LIMITATION: There is no dead-letter queue. Malformed records that pass
    # the null-id filter but have other missing fields are written to Cassandra
    # with those fields null. A production pipeline would route such rows to
    # a separate Kafka topic or object-store path for manual inspection.
    sel = (spark_df
           .selectExpr("CAST(value AS STRING)")
           .select(from_json(col('value'), schema).alias('data'))
           .select("data.*")
           .filter(col('id').isNotNull()))

    return sel


def process_batch(df, epoch_id):
    """
    foreachBatch sink handler.

    Responsibilities:
      1. Stamp each row with spark_processed_at (ISO UTC) and latency_ms
         (epoch_ms_at_batch_start - event_ts_ms from producer).
      2. Write enriched rows to Cassandra (upsert by UUID — idempotent on replay).
      3. Log per-batch metrics: row count, write duration, throughput, latency
         p50/p95/max derived from actual measured timestamps — not estimated.

    Latency definition:
      latency_ms = time Spark starts processing the batch   (batch_start_ms)
                 - time producer stamped the message        (event_ts_ms)
      Covers: Kafka buffering time + Spark trigger interval + scheduling delay.
      Does NOT include Cassandra write time — batch_start_ms is captured before
      the write. Cassandra write duration is reported separately as elapsed_ms.
    """
    batch_start_ms = int(time.time() * 1000)
    count = df.count()

    if count == 0:
        logging.info(f"[Batch {epoch_id}] Empty micro-batch, skipping.")
        return

    now_iso = datetime.now(timezone.utc).isoformat()

    df_enriched = (df
                   .withColumn("spark_processed_at", lit(now_iso))
                   .withColumn("latency_ms",
                               (lit(batch_start_ms) - col("event_ts_ms")).cast("double")))

    rows_with_ts = df_enriched.filter(col("event_ts_ms").isNotNull())
    has_ts_count = rows_with_ts.count()

    if has_ts_count > 0:
        stats = rows_with_ts.selectExpr(
            "percentile_approx(latency_ms, 0.5)  as p50",
            "percentile_approx(latency_ms, 0.95) as p95",
            "max(latency_ms)                     as p_max",
        ).collect()[0]
        lat_line = (f"  Latency p50/p95/max     : "
                    f"{stats['p50']:.0f} ms / {stats['p95']:.0f} ms / {stats['p_max']:.0f} ms"
                    f"  [{has_ts_count}/{count} rows measured]")
    else:
        lat_line = "  Latency                 : NOT MEASURED (event_ts_ms absent in records)"

    # Cassandra write with application-level exponential backoff: 1s → 2s → 4s.
    # Up to 4 attempts (3 retries). If every attempt fails the exception
    # propagates to Spark's streaming engine, which replays this micro-batch
    # from the last checkpoint offset.
    cassandra_attempts = 0
    max_cassandra_attempts = 4
    while True:
        try:
            df_enriched.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(keyspace="spark_streams", table="created_users") \
                .mode("append") \
                .save()
            break
        except Exception as e:
            cassandra_attempts += 1
            if cassandra_attempts >= max_cassandra_attempts:
                logging.error(
                    f"[Batch {epoch_id}] Cassandra write failed permanently "
                    f"after {max_cassandra_attempts} attempts: {e}"
                )
                raise
            wait_s = 2 ** (cassandra_attempts - 1)   # 1s, 2s, 4s
            logging.warning(
                f"[Batch {epoch_id}] Cassandra write failed "
                f"(attempt {cassandra_attempts}/{max_cassandra_attempts}), retrying in {wait_s}s: {e}"
            )
            time.sleep(wait_s)

    elapsed_ms = int(time.time() * 1000) - batch_start_ms
    throughput = count / (elapsed_ms / 1000) if elapsed_ms > 0 else 0

    logging.info(f"── Spark Micro-Batch [{epoch_id}] ──────────────────────────────")
    logging.info(f"  Rows in batch           : {count}")
    logging.info(f"  Batch write duration    : {elapsed_ms} ms")
    logging.info(f"  Write throughput        : {throughput:.0f} rows/sec")
    logging.info(lat_line)
    logging.info("────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)

        if spark_df is None:
            logging.error("Could not connect to Kafka. Exiting.")
            spark_conn.stop()
            exit(1)

        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream
                               .foreachBatch(process_batch)
                               .option('checkpointLocation', '/opt/spark/checkpoint')
                               .trigger(processingTime='500 milliseconds')
                               .start())

            streaming_query.awaitTermination()
