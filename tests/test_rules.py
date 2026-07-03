"""Unit tests for spark_streaming/alerts/rules.py — pure functions, no Spark needed."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from spark_streaming.alerts.rules import calculate_percentage_change, detect_trend


class TestCalculatePercentageChange:
    def test_basic_increase(self):
        assert calculate_percentage_change(110, 100) == pytest.approx(10.0)

    def test_basic_decrease(self):
        # Returns absolute value
        assert calculate_percentage_change(90, 100) == pytest.approx(10.0)

    def test_zero_baseline_returns_zero(self):
        assert calculate_percentage_change(100, 0) == 0.0

    def test_none_baseline_returns_zero(self):
        assert calculate_percentage_change(100, None) == 0.0

    def test_same_price(self):
        assert calculate_percentage_change(100, 100) == 0.0


class TestDetectTrend:
    def test_uptrend(self):
        assert detect_trend([1, 2, 3, 4, 5]) == "UPTREND"

    def test_downtrend(self):
        assert detect_trend([5, 4, 3, 2, 1]) == "DOWNTREND"

    def test_no_trend_flat(self):
        assert detect_trend([1, 1, 1, 1, 1]) is None

    def test_no_trend_mixed(self):
        assert detect_trend([1, 3, 2, 4, 3]) is None

    def test_too_short_returns_none(self):
        assert detect_trend([1, 2]) is None

    def test_exactly_three_uptrend(self):
        assert detect_trend([1, 2, 3]) == "UPTREND"