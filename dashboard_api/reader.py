"""
DashboardReader
───────────────
Reads Iceberg Parquet files directly via DuckDB — standalone, no Spark,
no pyspark import. This module is intentionally self-contained so the
dashboard-api container can be tiny (no JVM, no Spark wheel ~300MB).

Mirrors the read patterns from spark_streaming/health/reader.py but
returns plain dicts/lists ready for JSON serialization (Grafana JSON
API datasource consumes JSON, not DataFrames).
"""
from __future__ import annotations

import glob
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import duckdb

WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "/warehouse")


def _glob(layer: str, table: str) -> list[str]:
    pattern = os.path.join(WAREHOUSE, layer, table, "data", "**", "*.parquet")
    return glob.glob(pattern, recursive=True)


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:")


class DashboardReader:

    # ── Alerts ────────────────────────────────────────────────────

    def recent_alerts(self, hours: int = 24, limit: int = 500) -> list[dict]:
        files = _glob("gold", "market_alerts")
        if not files:
            return []

        con = _connect()
        try:
            rows = con.execute(f"""
                SELECT
                    alert_id, symbol, alert_type, severity, message,
                    current_price, change_percent, change_amount,
                    created_at, extra_data
                FROM read_parquet({files!r}, union_by_name=true)
                WHERE created_at >= (CURRENT_TIMESTAMP - INTERVAL '{hours} hours')
                ORDER BY created_at DESC
                LIMIT {limit}
            """).fetchall()

            cols = [
                "alert_id", "symbol", "alert_type", "severity", "message",
                "current_price", "change_percent", "change_amount",
                "created_at", "extra_data",
            ]
            return [
                {
                    **dict(zip(cols, row)),
                    "created_at": row[8].isoformat() if row[8] else None,
                }
                for row in rows
            ]
        finally:
            con.close()

    def alert_counts_by_type(self, hours: int = 24) -> list[dict]:
        files = _glob("gold", "market_alerts")
        if not files:
            return []

        con = _connect()
        try:
            rows = con.execute(f"""
                SELECT alert_type, severity, COUNT(*) AS cnt
                FROM read_parquet({files!r}, union_by_name=true)
                WHERE created_at >= (CURRENT_TIMESTAMP - INTERVAL '{hours} hours')
                GROUP BY alert_type, severity
                ORDER BY cnt DESC
            """).fetchall()
            return [
                {"alert_type": r[0], "severity": r[1], "count": r[2]}
                for r in rows
            ]
        finally:
            con.close()

    def alerts_timeseries(self, hours: int = 24, bucket_minutes: int = 30) -> list[dict]:
        """Alert counts bucketed by time — for a Grafana time series panel."""
        files = _glob("gold", "market_alerts")
        if not files:
            return []

        con = _connect()
        try:
            rows = con.execute(f"""
                SELECT
                    time_bucket(INTERVAL '{bucket_minutes} minutes', created_at) AS bucket,
                    alert_type,
                    COUNT(*) AS cnt
                FROM read_parquet({files!r}, union_by_name=true)
                WHERE created_at >= (CURRENT_TIMESTAMP - INTERVAL '{hours} hours')
                GROUP BY bucket, alert_type
                ORDER BY bucket
            """).fetchall()
            return [
                {"time": r[0].isoformat(), "alert_type": r[1], "count": r[2]}
                for r in rows
            ]
        finally:
            con.close()

    # ── Prices ────────────────────────────────────────────────────

    def price_history(self, symbol: str, hours: int = 24, limit: int = 2000) -> list[dict]:
        files = _glob("silver", "market_prices_clean")
        if not files:
            return []

        con = _connect()
        try:
            rows = con.execute(f"""
                SELECT event_time, price
                FROM read_parquet({files!r}, union_by_name=true)
                WHERE symbol = ?
                  AND event_time >= (CURRENT_TIMESTAMP - INTERVAL '{hours} hours')
                ORDER BY event_time
                LIMIT {limit}
            """, [symbol]).fetchall()
            return [{"time": r[0].isoformat(), "price": r[1]} for r in rows]
        finally:
            con.close()

    def latest_price(self, symbol: str) -> dict | None:
        files = _glob("silver", "market_prices_clean")
        if not files:
           return None

        con = _connect()
        try:
            row = con.execute(
            f"""
            SELECT symbol, price, event_time
            FROM read_parquet({files!r}, union_by_name=true)
            WHERE symbol = ?
            ORDER BY event_time DESC
            LIMIT 1
            """,
            [symbol],
        ).fetchone()

            if row is None:
               return None

            return {
            "symbol": row[0],
            "price": row[1],
            "event_time": row[2].isoformat(),
        }

        finally:
           con.close()

    # ── Pipeline health (mirrors health/reader.py) ───────────────

    def last_tick_per_symbol(self) -> dict[str, Optional[str]]:
        files = _glob("silver", "market_prices_clean")
        if not files:
            return {}

        con = _connect()
        try:
            rows = con.execute(f"""
                SELECT symbol, MAX(event_time) AS last_seen
                FROM read_parquet({files!r}, union_by_name=true)
                GROUP BY symbol
            """).fetchall()
            return {r[0]: r[1].isoformat() if r[1] else None for r in rows}
        finally:
            con.close()

    def pipeline_lag_seconds(self) -> Optional[float]:
        bronze_files = _glob("bronze", "market_ticks")
        silver_files = _glob("silver", "market_prices_clean")
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
        finally:
            con.close()

    def row_counts(self) -> dict[str, int]:
        counts = {}
        con = _connect()
        try:
            for layer, table, key in [
                ("bronze", "market_ticks", "bronze"),
                ("silver", "market_prices_clean", "silver"),
                ("gold", "market_alerts", "gold"),
            ]:
                files = _glob(layer, table)
                counts[key] = (
                    con.execute(
                        f"SELECT COUNT(*) FROM read_parquet({files!r}, union_by_name=true)"
                    ).fetchone()[0]
                    if files else 0
                )
        finally:
            con.close()
        return counts

    # ── Gold gap specific ──────────────────────────────────────────

    def gap_history(self, hours: int = 48, limit: int = 500) -> list[dict]:
        """Extracted gap amounts + expected/actual for a dedicated gap chart."""
        files = _glob("gold", "market_alerts")
        if not files:
            return []

        con = _connect()
        try:
            rows = con.execute(f"""
                SELECT
                    created_at,
                    change_amount AS gap_egp,
                    severity,
                    extra_data['expected_local'] AS expected,
                    extra_data['actual_local']   AS actual,
                    extra_data['global_gold']    AS global_gold,
                    extra_data['usd_egp']        AS usd_egp
                FROM read_parquet({files!r}, union_by_name=true)
                WHERE alert_type = 'GOLD_GAP'
                  AND created_at >= (CURRENT_TIMESTAMP - INTERVAL '{hours} hours')
                ORDER BY created_at
                LIMIT {limit}
            """).fetchall()
            cols = ["created_at", "gap_egp", "severity", "expected", "actual", "global_gold", "usd_egp"]
            return [
                {**dict(zip(cols, r)), "created_at": r[0].isoformat() if r[0] else None}
                for r in rows
            ]
        finally:
            con.close()
