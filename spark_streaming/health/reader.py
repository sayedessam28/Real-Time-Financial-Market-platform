"""
HealthReader
────────────
Reads Iceberg Parquet files directly via DuckDB — no Spark needed.
This keeps the health monitor lightweight and fast to start.

How Iceberg stores data:
  warehouse/
    bronze/market_ticks/data/*.parquet
    silver/market_prices_clean/data/*.parquet
    gold/market_alerts/data/*.parquet

We scan the parquet files directly — no catalog, no JVM.
"""
from __future__ import annotations

import glob
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import duckdb

from spark_streaming.config.settings import ICEBERG_WAREHOUSE

logger = logging.getLogger(__name__)


def _parquet_glob(layer: str, table: str) -> str:
    """Build a glob pattern for an Iceberg table's data files."""
    return os.path.join(ICEBERG_WAREHOUSE, layer, table, "data", "**", "*.parquet")


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:")


class HealthReader:
    """
    Thin DuckDB wrapper that answers health questions about the pipeline.
    All timestamps returned are UTC-aware datetimes.
    """

    # ── Feed freshness ────────────────────────────────────────────

    def last_tick_per_symbol(self) -> dict[str, Optional[datetime]]:
        """
        Return the most recent event_time per symbol in the Silver table.
        Symbol missing from result → never received.
        """
        pattern = _parquet_glob("silver", "market_prices_clean")
        files   = glob.glob(pattern, recursive=True)

        if not files:
            logger.warning("No Silver parquet files found")
            return {}

        con = _connect()
        try:
            rows = con.execute(f"""
                SELECT
                    symbol,
                    MAX(event_time) AS last_seen
                FROM read_parquet({files!r}, union_by_name=true)
                GROUP BY symbol
            """).fetchall()
            return {
                row[0]: row[1].replace(tzinfo=timezone.utc) if row[1] else None
                for row in rows
            }
        except Exception as e:
            logger.error(f"last_tick_per_symbol failed: {e}")
            return {}
        finally:
            con.close()

    # ── Pipeline lag ──────────────────────────────────────────────

    def pipeline_lag_seconds(self) -> Optional[float]:
        """
        How many seconds behind is Silver vs Bronze?
        Returns None if either table has no data.
        """
        bronze_pattern = _parquet_glob("bronze", "market_ticks")
        silver_pattern = _parquet_glob("silver", "market_prices_clean")

        bronze_files = glob.glob(bronze_pattern, recursive=True)
        silver_files = glob.glob(silver_pattern, recursive=True)

        if not bronze_files or not silver_files:
            return None

        con = _connect()
        try:
            bronze_max = con.execute(
                f"SELECT MAX(event_time) FROM read_parquet({bronze_files!r}, union_by_name=true)"
            ).fetchone()[0]

            silver_max = con.execute(
                f"SELECT MAX(event_time) FROM read_parquet({silver_files!r}, union_by_name=true)"
            ).fetchone()[0]

            if bronze_max is None or silver_max is None:
                return None

            return (bronze_max - silver_max).total_seconds()
        except Exception as e:
            logger.error(f"pipeline_lag_seconds failed: {e}")
            return None
        finally:
            con.close()

    # ── Row counts ────────────────────────────────────────────────

    def row_counts(self) -> dict[str, int]:
        """Total row count per layer — useful for sanity checks."""
        counts = {}
        layers = [
            ("bronze", "market_ticks",        "bronze"),
            ("silver", "market_prices_clean",  "silver"),
            ("gold",   "market_alerts",        "gold"),
        ]
        con = _connect()
        try:
            for layer, table, key in layers:
                files = glob.glob(_parquet_glob(layer, table), recursive=True)
                if files:
                    n = con.execute(
                        f"SELECT COUNT(*) FROM read_parquet({files!r}, union_by_name=true)"
                    ).fetchone()[0]
                    counts[key] = n
                else:
                    counts[key] = 0
        except Exception as e:
            logger.error(f"row_counts failed: {e}")
        finally:
            con.close()
        return counts

    # ── Recent alerts ─────────────────────────────────────────────

    def recent_alert_count(self, minutes: int = 60) -> int:
        """How many alerts fired in the last N minutes."""
        files = glob.glob(_parquet_glob("gold", "market_alerts"), recursive=True)
        if not files:
            return 0

        con = _connect()
        try:
            cutoff = datetime.now(timezone.utc).replace(tzinfo=None)  # DuckDB stores naive UTC
            result = con.execute(f"""
                SELECT COUNT(*)
                FROM read_parquet({files!r}, union_by_name=true)
                WHERE created_at >= (CURRENT_TIMESTAMP - INTERVAL '{minutes} minutes')
            """).fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"recent_alert_count failed: {e}")
            return 0
        finally:
            con.close()