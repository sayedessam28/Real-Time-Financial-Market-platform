import logging
import sys

from pyspark.sql.functions import col, from_json, current_timestamp, to_utc_timestamp

from spark_streaming.common.spark_session import create_spark_session
from spark_streaming.config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from shared.schema import market_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = create_spark_session("BronzeStreamingJob")

# ── Read raw Kafka stream ─────────────────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "latest")
    .option("failOnDataLoss",          "false")
    .load()
)

# ── Parse + flatten ───────────────────────────────────────────────
parsed_df = (
    raw_df
    .select(from_json(col("value").cast("string"), market_schema).alias("data"))
    .select(
        col("data.symbol").alias("symbol"),
        col("data.price").alias("price"),
        col("data.source").alias("source"),
        col("data.timestamp").cast("timestamp").alias("event_time"),
    )
    .withColumn("ingestion_time", to_utc_timestamp(current_timestamp(), "Africa/Cairo"))
)

# ── Ensure table exists ───────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bronze.market_ticks (
        symbol         STRING,
        price          DOUBLE,
        source         STRING,
        event_time     TIMESTAMP,
        ingestion_time TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(event_time))
""")

# ── Write stream ──────────────────────────────────────────────────
query = (
    parsed_df
    .repartition(1)
    .writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "checkpoints/bronze_market")
    .option("failOnDataLoss",     "false")
    .trigger(processingTime="1 minute")
    .toTable("local.bronze.market_ticks")
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Bronze job interrupted — stopping cleanly")
    query.stop()
except Exception as e:
    logger.error(f"Bronze stream failed: {e}", exc_info=True)
    query.stop()
    sys.exit(1)