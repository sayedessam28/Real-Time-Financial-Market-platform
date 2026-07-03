"""
Alert Job
─────────
Polls the Silver Iceberg table every ALERT_POLL_INTERVAL seconds,
runs spike/trend/gap detection, writes alerts to Gold, and sends
Telegram notifications.

State (last prices, cooldown timestamps, price history) is persisted
in local.gold.alert_engine_state so restarts don't lose context.
"""
import logging
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql.functions import col

from spark_streaming.common.spark_session import create_spark_session
from spark_streaming.common.schemas import alert_schema
from spark_streaming.config.settings import (
    ALERT_LOOKBACK_HOURS,
    ALERT_POLL_INTERVAL,
    GAP_SEVERITY_HIGH,
    GAP_SEVERITY_MEDIUM,
)
from spark_streaming.alerts.state_store import AlertStateStore
from spark_streaming.alerts.engine import AlertEngine
from spark_streaming.alerts.gap_detector import GoldGapDetector
from spark_streaming.notifications.telegram import send_alert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Spark + tables ────────────────────────────────────────────────
spark = create_spark_session("AlertJob")

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.gold.market_alerts (
        alert_id       STRING,
        symbol         STRING,
        alert_type     STRING,
        severity       STRING,
        message        STRING,
        current_price  DOUBLE,
        change_percent DOUBLE,
        change_amount  DOUBLE,
        created_at     TIMESTAMP,
        extra_data     MAP<STRING, STRING>
    )
    USING iceberg
    PARTITIONED BY (days(created_at))
""")

# ── State store + engines (initialised once) ──────────────────────
state_store  = AlertStateStore(spark)
engine       = AlertEngine(state_store)
gap_detector = GoldGapDetector(state_store)

from datetime import timedelta

# On first startup, only look back ALERT_LOOKBACK_HOURS instead of
# replaying all historical Silver data — prevents bulk stale alerts.
_last_processed_time = datetime.now(timezone.utc) - timedelta(hours=ALERT_LOOKBACK_HOURS)


# ── Severity ──────────────────────────────────────────────────────

def _spike_severity(change_amount) -> str:
    """
    Severity for SPIKE_UP / SPIKE_DOWN / UPTREND / DOWNTREND.
    change_amount here is a price difference in the symbol's currency.
    """
    if change_amount is None:
        return "LOW"
    if change_amount >= 200:
        return "HIGH"
    if change_amount >= 120:
        return "MEDIUM"
    return "LOW"


def _gap_severity(gap_egp) -> str:
    """
    Severity for GOLD_GAP — uses EGP gap amount, not % change.
    Thresholds are configurable via GAP_SEVERITY_HIGH / GAP_SEVERITY_MEDIUM env vars.

    Default bands (based on observed data):
      HIGH   ≥ 300 EGP  (e.g. 333, 355, 376, 396, 417 EGP seen in logs)
      MEDIUM ≥ 150 EGP  (e.g. 167, 186 EGP)
      LOW    < 150 EGP  (e.g. 91, 118, 137 EGP)
    """
    if gap_egp is None:
        return "LOW"
    if gap_egp >= GAP_SEVERITY_HIGH:
        return "HIGH"
    if gap_egp >= GAP_SEVERITY_MEDIUM:
        return "MEDIUM"
    return "LOW"


def _severity(alert: dict) -> str:
    """Route to the correct severity function based on alert type."""
    if alert.get("alert_type") == "GOLD_GAP":
        return _gap_severity(alert.get("change_amount"))
    return _spike_severity(alert.get("change_amount"))


# ── Row builder ───────────────────────────────────────────────────

def _build_alert_row(alert: dict, event_time: datetime) -> dict:
    """
    Build a Gold-layer alert row.
    created_at = event_time from the Silver tick (when the price actually
    moved), NOT the current wall-clock time (when we processed it).
    """
    extra = {str(k): str(v) for k, v in (alert.get("extra_data") or {}).items()}
    return {
        "alert_id":       str(uuid.uuid4()),
        "symbol":         alert.get("symbol"),
        "alert_type":     alert.get("alert_type"),
        "severity":       _severity(alert),
        "message":        alert.get("message"),
        "current_price":  alert.get("current_price"),
        "change_percent": alert.get("change_percent"),
        "change_amount":  alert.get("change_amount"),
        "created_at":     event_time,
        "extra_data":     extra,
    }


# ── Main batch ────────────────────────────────────────────────────

def run_batch() -> None:
    global _last_processed_time

    try:
        silver_df = spark.read.table("local.silver.market_prices_clean")

        silver_df = silver_df.filter(col("event_time") > _last_processed_time)

        rows = silver_df.orderBy("event_time").collect()

        if not rows:
            logger.info("No new rows — sleeping")
            return

        alert_rows = []

        for row in rows:
            tick_alerts = (
                engine.process(row.symbol, row.price)
                + gap_detector.process(row.symbol, row.price)
            )
            for alert in tick_alerts:
                built = _build_alert_row(alert, event_time=row.event_time)
                alert_rows.append(built)
                send_alert(built)

        state_store.flush()

        _last_processed_time = max(row.event_time for row in rows)
        logger.info(
            f"Batch done — {len(rows)} rows, "
            f"{len(alert_rows)} alerts, "
            f"last_time={_last_processed_time}"
        )

        if alert_rows:
            alerts_df = spark.createDataFrame(alert_rows, schema=alert_schema)
            alerts_df.writeTo("local.gold.market_alerts").append()
            logger.warning(f"Wrote {len(alert_rows)} alerts to Gold layer")

    except Exception as e:
        logger.error(f"Batch error: {e}", exc_info=True)


# ── Entry point ───────────────────────────────────────────────────

logger.info(f"Alert job started — polling every {ALERT_POLL_INTERVAL}s")

while True:
    run_batch()
    time.sleep(ALERT_POLL_INTERVAL)