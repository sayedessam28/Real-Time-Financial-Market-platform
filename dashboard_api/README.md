# dashboard-api

FastAPI service that exposes the Iceberg warehouse data (Bronze/Silver/Gold)
as JSON for Grafana — no Spark, no JVM, reads Parquet directly via DuckDB.

## Run

### Via docker-compose (recommended — same stack as Kafka/Postgres)

```bash
docker compose up -d dashboard-api grafana
```

Grafana will be available at **http://localhost:3000** (admin/admin),
with the datasource and dashboard pre-provisioned automatically.

### Standalone (for local development)

```bash
cd dashboard_api
pip install -r requirements.txt
export ICEBERG_WAREHOUSE=../warehouse
uvicorn main:app --reload --port 8000
```

Then browse http://localhost:8000/docs for interactive API docs.

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /alerts/recent?hours=24&limit=500` | Recent alert rows |
| `GET /alerts/counts?hours=24` | Alert counts grouped by type+severity |
| `GET /alerts/timeseries?hours=24&bucket_minutes=30` | Alert counts bucketed over time |
| `GET /alerts/gap-history?hours=48` | GOLD_GAP history with expected/actual prices |
| `GET /prices/history?symbol=XAU/USD&hours=24` | Price history for one symbol |
| `GET /prices/latest` | Latest price per symbol |
| `GET /health/feeds` | Last-seen timestamp + age per symbol |
| `GET /health/lag` | Bronze → Silver lag in seconds |
| `GET /health/row-counts` | Row counts per layer |

## Why this architecture instead of Trino or a Postgres sync job

- **No Spark/JVM dependency** — this container stays under 200MB vs ~1GB+ for a Spark image.
- **No second source of truth** — reads the same Parquet files the pipeline already writes; no sync job to keep alive or get stale.
- **Trino would work too**, but is overkill for this data volume and adds another JVM to a memory-constrained setup.

## Grafana dashboard

Pre-built dashboard: `grafana_provisioning/dashboards/financial_market_overview.json`

Panels included:
- Latest prices (stat)
- XAU/USD and XAU/EGP_LOCAL price history (time series)
- Gold gap history in EGP (time series)
- Alert counts by type (bar chart)
- Recent alerts (table)
- Bronze→Silver lag, row counts, feed freshness (health panels)

If you edit the dashboard in the Grafana UI, **export it back to this JSON file**
so it survives container restarts (provisioned dashboards are read from disk,
not the Grafana database).
