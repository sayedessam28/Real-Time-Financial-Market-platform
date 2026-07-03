"""
Unit tests for GoldGapDetector.
Uses a stub StateStore so no Spark is needed.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import spark_streaming.alerts.gap_detector as gd_module
from spark_streaming.alerts.gap_detector import GoldGapDetector


def _make_detector(
    gap_threshold=60.0,
    min_gap_change=50.0,
    gap_cooldown=3600,
    stale_seconds=300,
):
    store = MagicMock()
    store.load_all.return_value = {}

    with (
        patch.object(gd_module, "GAP_THRESHOLD",    gap_threshold),
        patch.object(gd_module, "MIN_GAP_CHANGE",   min_gap_change),
        patch.object(gd_module, "GAP_COOLDOWN",     gap_cooldown),
        patch.object(gd_module, "STALE_PRICE_SECONDS", stale_seconds),
    ):
        d = GoldGapDetector(store)

    # Apply runtime config to the *real* attribute names on the instance.
    # GoldGapDetector tracks global and local staleness independently —
    # there is no single "_stale_window" attribute.
    d._stale_global_window = timedelta(seconds=stale_seconds)
    d._stale_local_window  = timedelta(seconds=stale_seconds)
    d._cooldown            = timedelta(seconds=gap_cooldown)
    return d


def _feed_all(detector, xau_usd, usd_egp, xau_egp_local):
    alerts = []
    alerts += detector.process("XAU/USD",      xau_usd)
    alerts += detector.process("USD/EGP",       usd_egp)
    alerts += detector.process("XAU/EGP_LOCAL", xau_egp_local)
    return alerts


class TestGoldGapBasic:
    def test_no_alert_when_gap_small(self):
        d = _make_detector(gap_threshold=60)
        # expected ≈ 3000 * 50 * 0.875 / 31.1035 ≈ 4219 → local=4220 → gap≈1
        alerts = _feed_all(d, 3000, 50, 4220)
        assert alerts == []

    def test_alert_when_gap_large(self):
        d = _make_detector(gap_threshold=60)
        # expected ≈ 4219, local=4400 → gap≈181
        alerts = _feed_all(d, 3000, 50, 4400)
        assert len(alerts) == 1
        assert alerts[0]["alert_type"] == "GOLD_GAP"

    def test_no_alert_before_all_prices_available(self):
        d = _make_detector()
        assert d.process("XAU/USD", 3000) == []
        assert d.process("USD/EGP", 50)   == []

    def test_none_price_ignored(self):
        d = _make_detector()
        assert d.process("XAU/USD", None) == []

    def test_global_age_in_extra_data(self):
        d = _make_detector(gap_threshold=60)
        alerts = _feed_all(d, 3000, 50, 4400)
        assert "global_age_sec" in alerts[0]["extra_data"]


class TestStalePrice:
    def test_stale_global_price_suppresses_alert(self):
        d = _make_detector(gap_threshold=60, stale_seconds=300)
        # Feed all three prices to initialise
        _feed_all(d, 3000, 50, 4400)

        # Wind back the global update time beyond the stale window
        d._global_gold_updated_at = datetime.now(timezone.utc) - timedelta(seconds=400)

        # Now only local price updates — global is stale, should not alert
        alerts = d.process("XAU/EGP_LOCAL", 4500)
        assert alerts == [], "Should suppress alert when global price is stale"

    def test_fresh_global_price_fires_alert(self):
        d = _make_detector(gap_threshold=60, stale_seconds=300)
        # Fresh XAU/USD update, gap should fire normally
        alerts = _feed_all(d, 3000, 50, 4400)
        assert len(alerts) == 1


class TestCooldown:
    def test_no_realert_within_cooldown_if_gap_unchanged(self):
        d = _make_detector(gap_threshold=60, gap_cooldown=3600, min_gap_change=50)
        _feed_all(d, 3000, 50, 4400)   # first alert fires

        # Same gap — should be suppressed by cooldown
        alerts = _feed_all(d, 3000, 50, 4400)
        assert alerts == []

    def test_realert_if_gap_jumps_significantly(self):
        d = _make_detector(gap_threshold=60, gap_cooldown=3600, min_gap_change=50)
        _feed_all(d, 3000, 50, 4400)   # gap ≈ 181 EGP

        # Gap jumps by > 50 EGP → should re-alert even within cooldown
        alerts = _feed_all(d, 3000, 50, 4460)   # gap ≈ 241 EGP → change ≈ 60
        assert len(alerts) == 1

    def test_no_realert_if_gap_change_below_min(self):
        d = _make_detector(gap_threshold=60, gap_cooldown=3600, min_gap_change=50)
        _feed_all(d, 3000, 50, 4400)   # gap ≈ 181 EGP

        # Gap changes by only 10 EGP — below MIN_GAP_CHANGE=50
        alerts = _feed_all(d, 3000, 50, 4410)
        assert alerts == []


class TestLocalPriceAgeGuard:
    def test_stale_local_price_suppresses_alert(self):
        d = _make_detector(gap_threshold=60, stale_seconds=300)

        # Feed all three to initialise, gap fires
        _feed_all(d, 3000, 50, 4400)

        # Wind back local price timestamp beyond the local stale window
        d._local_gold_updated_at = datetime.now(timezone.utc) - timedelta(seconds=250)
        d._stale_local_window    = timedelta(seconds=180)

        # Global gold updates — local is stale, should NOT fire
        alerts = d.process("XAU/USD", 3000)
        assert alerts == [], "Should suppress when local price is stale"

    def test_fresh_local_price_allows_alert(self):
        d = _make_detector(gap_threshold=60, gap_cooldown=0)  # no cooldown
        # Both prices fresh → should alert
        alerts = _feed_all(d, 3000, 50, 4400)
        assert len(alerts) == 1

    def test_local_age_in_extra_data(self):
        d = _make_detector(gap_threshold=60)
        alerts = _feed_all(d, 3000, 50, 4400)
        assert "local_age_sec" in alerts[0]["extra_data"]