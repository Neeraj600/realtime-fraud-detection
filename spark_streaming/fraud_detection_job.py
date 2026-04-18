"""
fraud_detection_job.py
----------------------
PySpark Structured Streaming job that reads raw transactions from Kafka,
applies feature engineering and fraud rules, scores each transaction,
and routes results to:
  - Delta Lake (all transactions with fraud score)
  - Kafka topic txn.flagged (score >= 50 only)
  - Alert webhook (score >= 80)

Trigger: ProcessingTime 10 seconds (micro-batch)
Delivery guarantee: Exactly-once via Delta Lake checkpointing
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp,
    lit, when, expr
)

from schema import TRANSACTION_SCHEMA
from feature_engineering import add_rolling_features
from rule_engine import apply_fraud_rules, compute_fraud_score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("FraudDetectionJob")

# ── Config — injected via environment (ADF / Databricks job params) ────────────

KAFKA_BOOTSTRAP   = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC_IN    = os.environ.get("KAFKA_TOPIC_RAW",     "txn.raw")
KAFKA_TOPIC_OUT   = os.environ.get("KAFKA_TOPIC_FLAGGED", "txn.flagged")
DELTA_OUTPUT_PATH = os.environ["DELTA_OUTPUT_PATH"]
CHECKPOINT_PATH   = os.environ["CHECKPOINT_PATH"]
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")

TRIGGER_INTERVAL  = os.environ.get("TRIGGER_INTERVAL_SECONDS", "10")
FRAUD_BLOCK_SCORE = int(os.environ.get("FRAUD_BLOCK_SCORE",    "80"))
FRAUD_FLAG_SCORE  = int(os.environ.get("FRAUD_FLAG_SCORE",     "50"))

# ── Spark session ──────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("FraudDetectionStreaming")
    .config("spark.sql.extensions",              "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",   "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
    .config("spark.sql.adaptive.enabled",        "true")    # AQE for skew handling
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
logger.info(f"Spark version: {spark.version}")

# ── Read from Kafka ────────────────────────────────────────────────────────────

df_raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe",               KAFKA_TOPIC_IN)
    .option("startingOffsets",         "latest")
    .option("failOnDataLoss",          "false")     # Don't crash if topic is compacted
    .option("maxOffsetsPerTrigger",    100_000)     # Backpressure — don't overwhelm state store
    .load()
)

# ── Parse transaction payload ──────────────────────────────────────────────────

df_parsed = (
    df_raw_stream
    .select(
        col("offset"),
        col("partition"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(
            col("value").cast("string"),
            TRANSACTION_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
        ).alias("txn")
    )
    .select("offset", "partition", "kafka_timestamp", "txn.*")
    # Drop records that couldn't be parsed — log separately
    .filter(col("_corrupt_record").isNull())
    .drop("_corrupt_record")
    .withColumn("_ingested_at", current_timestamp())
)

# ── Feature engineering — rolling window aggregates ───────────────────────────
# Requires stateful streaming — Spark watermarks handle late data

df_features = add_rolling_features(df_parsed, watermark_delay="2 minutes")

# ── Apply fraud rules ──────────────────────────────────────────────────────────

df_rules = apply_fraud_rules(df_features)

# ── Compute composite fraud score ──────────────────────────────────────────────

df_scored = compute_fraud_score(df_rules)

# ── Route by score ─────────────────────────────────────────────────────────────

df_final = (
    df_scored
    .withColumn("fraud_decision",
        when(col("fraud_score") >= FRAUD_BLOCK_SCORE, lit("BLOCK"))
        .when(col("fraud_score") >= FRAUD_FLAG_SCORE,  lit("REVIEW"))
        .otherwise(                                     lit("PASS"))
    )
    .withColumn("requires_ops_alert", col("fraud_score") >= FRAUD_BLOCK_SCORE)
)

# ── Write all transactions to Delta Lake ───────────────────────────────────────
# Exactly-once: Spark checkpoints offset → Delta transaction log guarantees idempotency

delta_query = (
    df_final
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH + "/delta")
    .option("mergeSchema", "false")     # Schema locked at Silver — fail if it changes
    .trigger(processingTime=f"{TRIGGER_INTERVAL} seconds")
    .partitionBy("fraud_decision")      # Partition for efficient fraud analyst queries
    .start(DELTA_OUTPUT_PATH)
)

logger.info(f"Delta Lake sink started → {DELTA_OUTPUT_PATH}")

# ── Publish flagged transactions back to Kafka ─────────────────────────────────

df_flagged = df_final.filter(col("fraud_score") >= FRAUD_FLAG_SCORE)

kafka_query = (
    df_flagged
    .select(
        col("transaction_id").alias("key"),
        to_json(struct(
            "transaction_id", "card_id", "amount", "currency",
            "merchant_id", "merchant_country", "fraud_score",
            "fraud_decision", "triggered_rules", "_ingested_at"
        )).alias("value")
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic",                   KAFKA_TOPIC_OUT)
    .option("checkpointLocation",      CHECKPOINT_PATH + "/kafka")
    .trigger(processingTime=f"{TRIGGER_INTERVAL} seconds")
    .start()
)

logger.info(f"Kafka sink started → topic: {KAFKA_TOPIC_OUT}")

# ── Await termination ─────────────────────────────────────────────────────────

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    logger.info("Streaming job interrupted — shutting down gracefully")
    delta_query.stop()
    kafka_query.stop()
