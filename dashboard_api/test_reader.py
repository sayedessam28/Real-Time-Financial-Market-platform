"""
Smoke tests for DashboardReader — run against a real (small) warehouse
if available, otherwise just verify graceful empty-result behavior.

Run:
    cd dashboard_api
    pytest test_reader.py -v
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from reader import DashboardReader


def test_empty_warehouse_returns_empty_lists(tmp_path, monkeypatch):
    """When no parquet files exist, every method should return [] / {} / None
    instead of raising."""
    monkeypatch.setenv("ICEBERG_WAREHOUSE", str(tmp_path))
    import importlib
    import reader as reader_module
    importlib.reload(reader_module)

    r = reader_module.DashboardReader()

    assert r.recent_alerts() == []
    assert r.alert_counts_by_type() == []
    assert r.alerts_timeseries() == []
    assert r.gap_history() == []
    assert r.price_history(symbol="XAU/USD") == []
    assert r.latest_prices() == []
    assert r.last_tick_per_symbol() == {}
    assert r.pipeline_lag_seconds() is None
    assert r.row_counts() == {"bronze": 0, "silver": 0, "gold": 0}
