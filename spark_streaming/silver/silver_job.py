import logging
import os
import shutil
import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

from spark_streaming.common.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHECKPOINT = "checkpoints/silver_market"
MAX_RESTARTS = 3

spark = create_spark_session("SilverStreamingJob")

# ── Ensure Silver table exists ────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.market_prices_clean (
        symbol         STRING,
        price          DOUBLE,
        source         STRING,
        event_time     TIMESTAMP,
        ingestion_time TIMESTAMP,
        processed_at   TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(event_time))
""")


# ── Helpers ───────────────────────────────────────────────────────

def _build_stream() -> DataFrame:
    """
    Read Bronze Iceberg table as a stream.

    streaming-skip-delete-snapshots / streaming-skip-overwrite-snapshots:
      Tolerate snapshot deletions by Bronze maintenance jobs instead of
      crashing with "snapshot was expired or removed".
    """
    raw = (
        spark.readStream
        .format("iceberg")
        .option("streaming-skip-delete-snapshots",    "true")
        .option("streaming-skip-overwrite-snapshots", "true")
        .load("local.bronze.market_ticks")
    )
    return (
        raw
        .filter(col("price") > 0)
        .filter(col("symbol").isNotNull())
        .withWatermark("event_time", "5 minutes")
        .dropDuplicates(["symbol", "event_time", "price"])
        .withColumn("processed_at", current_timestamp())
    )


def _is_snapshot_expired(e: Exception) -> bool:
    return "snapshot was expired or removed" in str(e)


def _reset_checkpoint() -> None:
    """
    Delete the silver checkpoint so the stream restarts cleanly from
    the latest Bronze snapshot.  Data already in Silver is safe because
    dropDuplicates(symbol, event_time, price) prevents double-counting.
    """
    if os.path.exists(CHECKPOINT):
        shutil.rmtree(CHECKPOINT)
        logger.warning(f"Checkpoint reset: {CHECKPOINT} — restarting from latest snapshot")


# ── Main loop ─────────────────────────────────────────────────────

for attempt in range(1, MAX_RESTARTS + 1):
    logger.info(f"Silver stream starting (attempt {attempt}/{MAX_RESTARTS})")

    query = (
        _build_stream()
        .writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="1 minute")
        .toTable("local.silver.market_prices_clean")
    )

    try:
        query.awaitTermination()
        break   # clean shutdown

    except KeyboardInterrupt:
        logger.info("Silver job interrupted — stopping cleanly")
        query.stop()
        break

    except Exception as e:
        query.stop()

        if _is_snapshot_expired(e) and attempt < MAX_RESTARTS:
            logger.warning(
                f"Expired snapshot on attempt {attempt} — "
                "resetting checkpoint and retrying"
            )
            _reset_checkpoint()
            # loop continues → _build_stream() called fresh next iteration

        else:
            logger.error(f"Silver stream failed permanently: {e}", exc_info=True)
            sys.exit(1)