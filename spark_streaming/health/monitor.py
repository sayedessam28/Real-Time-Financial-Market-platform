"""
Pipeline Health Monitor
────────────────────────
Standalone process — no Spark, no Kafka, no JVM.
Reads Iceberg Parquet files directly via DuckDB.

Run:
    python -m spark_streaming.health.monitor

What it checks every HEALTH_CHECK_INTERVAL seconds:
  ✓ Feed freshness  — last tick age per symbol vs max silence threshold
  ✓ Pipeline lag    — how far Silver is behind Bronze
  ✓ Daily heartbeat — 09:00 Cairo "system alive" message

Notifications:
  → Telegram: only when WARN or DEAD (+ daily heartbeat)
  → Log file: every cycle (HEALTH_LOG_FILE)
"""
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from spark_streaming.config.settings import HEALTH_CHECK_INTERVAL, HEALTH_LOG_FILE
from spark_streaming.health.reader   import HealthReader
from spark_streaming.health.checks   import run_all_checks, Status
from spark_streaming.health.notifier import write_log, send_telegram, maybe_send_heartbeat

# ── Logging setup ─────────────────────────────────────────────────
Path(HEALTH_LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(HEALTH_LOG_FILE),
    ],
)
logger = logging.getLogger(__name__)

# ── State: track which checks already fired to avoid repeat alerts ─
_alerted: set[str] = set()   # check names currently in WARN/DEAD state


def _should_notify(report) -> bool:
    """
    Send Telegram if:
      - Any new check transitioned into WARN/DEAD (wasn't alerted before)
      - Any previously DEAD/WARN check recovered to OK (send recovery msg)
    """
    current_bad = {c.name for c in report.checks if c.status != Status.OK}

    newly_bad       = current_bad - _alerted
    newly_recovered = _alerted - current_bad

    return bool(newly_bad or newly_recovered)


def _update_alert_state(report) -> None:
    global _alerted
    _alerted = {c.name for c in report.checks if c.status != Status.OK}


def _log_summary(report) -> None:
    status = report.worst_status.value
    bad    = [c for c in report.checks if c.status != Status.OK]
    if bad:
        details = " | ".join(c.message for c in bad)
        logger.warning(f"Health [{status}]: {details}")
    else:
        logger.info(f"Health [OK] — all {len(report.checks)} checks passed")


# ── Main loop ─────────────────────────────────────────────────────

def run() -> None:
    reader = HealthReader()
    logger.info(
        f"Health monitor started — "
        f"checking every {HEALTH_CHECK_INTERVAL}s"
    )

    while True:
        try:
            # Collect metrics
            last_ticks  = reader.last_tick_per_symbol()
            lag_seconds = reader.pipeline_lag_seconds()
            row_counts  = reader.row_counts()

            # Run checks
            report = run_all_checks(last_ticks, lag_seconds)

            # Always write to log
            write_log(report)
            _log_summary(report)

            # Telegram: only on state change
            if _should_notify(report):
                send_telegram(report)
            _update_alert_state(report)

            # Daily heartbeat regardless of state
            maybe_send_heartbeat(report)

            logger.info(
                f"Row counts — "
                f"bronze={row_counts.get('bronze', '?')} | "
                f"silver={row_counts.get('silver', '?')} | "
                f"gold={row_counts.get('gold', '?')}"
            )

        except Exception as e:
            logger.error(f"Health check cycle failed: {e}", exc_info=True)

        time.sleep(HEALTH_CHECK_INTERVAL)


if __name__ == "__main__":
    run()