"""
Dashboard API
─────────────
Tiny FastAPI service that exposes pipeline data as JSON for Grafana
(via the "JSON API" / "Infinity" datasource plugin).

No Spark, no JVM — pure Python + DuckDB reading Parquet files directly
from the Iceberg warehouse on disk.

Run locally (from inside the dashboard_api/ folder — main.py imports
`reader` as a flat module, matching the Docker container's layout):
    cd dashboard_api
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload

Run in Docker: see dashboard_api/Dockerfile, or just
    docker compose up -d dashboard-api
"""
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from reader import DashboardReader

app = FastAPI(title="Financial Market Dashboard API")

# Grafana (running in its own container) needs CORS to call this API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

reader = DashboardReader()


@app.get("/")
def root():
    return {"status": "ok", "service": "dashboard-api"}


# ── Alerts ──────────────────────────────────────────────────────────

@app.get("/alerts/recent")
def alerts_recent(hours: int = Query(24, ge=1, le=720), limit: int = Query(500, ge=1, le=5000)):
    return reader.recent_alerts(hours=hours, limit=limit)


@app.get("/alerts/counts")
def alerts_counts(hours: int = Query(24, ge=1, le=720)):
    return reader.alert_counts_by_type(hours=hours)


@app.get("/alerts/timeseries")
def alerts_timeseries(
    hours: int = Query(24, ge=1, le=720),
    bucket_minutes: int = Query(30, ge=1, le=1440),
):
    return reader.alerts_timeseries(hours=hours, bucket_minutes=bucket_minutes)


@app.get("/alerts/gap-history")
def gap_history(hours: int = Query(48, ge=1, le=720), limit: int = Query(500, ge=1, le=5000)):
    return reader.gap_history(hours=hours, limit=limit)


# ── Prices ──────────────────────────────────────────────────────────

@app.get("/prices/history")
def price_history(
    symbol: str = Query(..., description="e.g. XAU/USD, USD/EGP, XAU/EGP_LOCAL"),
    hours: int = Query(24, ge=1, le=720),
    limit: int = Query(2000, ge=1, le=20000),
):
    return reader.price_history(symbol=symbol, hours=hours, limit=limit)


@app.get("/prices/latest")
def prices_latest(
    symbol: Optional[str] = None
):
    if symbol:
        row = reader.latest_price(symbol)
        return [] if row is None else [row]

    return reader.latest_prices()


# ── Health ──────────────────────────────────────────────────────────

@app.get("/health/feeds")
def health_feeds():
    last_ticks = reader.last_tick_per_symbol()
    now = datetime.now(timezone.utc)
    result = []
    for symbol, last_seen_iso in last_ticks.items():
        age_sec: Optional[float] = None
        if last_seen_iso:
            last_seen = datetime.fromisoformat(last_seen_iso)
            if last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=timezone.utc)
            age_sec = (now - last_seen).total_seconds()
        result.append({
            "symbol": symbol,
            "last_seen": last_seen_iso,
            "age_seconds": age_sec,
        })
    return result


@app.get("/health/lag")
def health_lag():
    lag = reader.pipeline_lag_seconds()
    return {"bronze_to_silver_lag_seconds": lag}


@app.get("/health/row-counts")
def health_row_counts():
    return reader.row_counts()