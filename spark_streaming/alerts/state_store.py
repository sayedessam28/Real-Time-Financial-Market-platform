"""
AlertStateStore
───────────────
Persists alert-engine state in an Iceberg table so restarts don't lose context.

Table: local.gold.alert_engine_state
  state_key   STRING  (e.g. "XAU/USD__last_price", "XAU/USD__SPIKE_UP__alert_time")
  state_value STRING  (always stored as string; caller casts)
  updated_at  TIMESTAMP

Design notes:
  - All reads happen once at startup (load_all).
  - Writes are batched: call flush() to persist pending changes.
  - We use MERGE (upsert) so the table never grows unboundedly.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_TABLE = "local.gold.alert_engine_state"
_SEP   = "__"   # key segment separator


def _now():
    return datetime.now(timezone.utc)


class AlertStateStore:
    def __init__(self, spark: SparkSession) -> None:
        self._spark   = spark
        self._pending: dict[str, str] = {}   # key → value, waiting to be flushed
        self._ensure_table()

    # ── Public API ────────────────────────────────────────────────

    def load_all(self) -> dict[str, str]:
        """Read entire state table into a plain dict. Call once at startup."""
        try:
            rows = self._spark.read.table(_TABLE).collect()
            state = {r["state_key"]: r["state_value"] for r in rows}
            logger.info(f"Loaded {len(state)} state entries from Iceberg")
            return state
        except Exception as e:
            logger.warning(f"Could not load state (fresh start?): {e}")
            return {}

    def set(self, key: str, value: str) -> None:
        """Stage a value for the next flush."""
        self._pending[key] = value

    def flush(self) -> None:
        """Write all pending changes to Iceberg via MERGE (upsert)."""
        if not self._pending:
            return

        now = _now()
        rows = [
            {"state_key": k, "state_value": v, "updated_at": now}
            for k, v in self._pending.items()
        ]

        try:
            updates_df = self._spark.createDataFrame(rows)
            updates_df.createOrReplaceTempView("_state_updates")

            self._spark.sql(f"""
                MERGE INTO {_TABLE} AS target
                USING _state_updates AS source
                ON target.state_key = source.state_key
                WHEN MATCHED     THEN UPDATE SET
                    target.state_value = source.state_value,
                    target.updated_at  = source.updated_at
                WHEN NOT MATCHED THEN INSERT *
            """)

            logger.debug(f"Flushed {len(self._pending)} state entries")
            self._pending.clear()

        except Exception as e:
            logger.error(f"State flush failed: {e}")

    # ── Key helpers (used by engine / detector) ───────────────────

    @staticmethod
    def price_key(symbol: str) -> str:
        return f"{symbol}{_SEP}last_price"

    @staticmethod
    def alert_time_key(symbol: str, alert_type: str) -> str:
        return f"{symbol}{_SEP}{alert_type}{_SEP}alert_time"

    @staticmethod
    def history_key(symbol: str) -> str:
        return f"{symbol}{_SEP}price_history"

    @staticmethod
    def gap_time_key() -> str:
        return f"GAP{_SEP}last_alert_time"

    @staticmethod
    def gap_amount_key() -> str:
        return f"GAP{_SEP}last_gap_amount"

    # ── Private ───────────────────────────────────────────────────

    def _ensure_table(self) -> None:
        self._spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
        self._spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {_TABLE} (
                state_key   STRING    NOT NULL,
                state_value STRING,
                updated_at  TIMESTAMP
            )
            USING iceberg
        """)
        logger.info(f"State table ready: {_TABLE}")