"""Pure functions — no side effects, easy to unit-test."""
from __future__ import annotations


def calculate_percentage_change(current: float, baseline: float) -> float:
    if not baseline:
        return 0.0
    return abs((current - baseline) / baseline * 100)


def detect_trend(prices: list[float]) -> str | None:
    """Return 'UPTREND', 'DOWNTREND', or None."""
    if len(prices) < 3:
        return None
    if all(prices[i] > prices[i - 1] for i in range(1, len(prices))):
        return "UPTREND"
    if all(prices[i] < prices[i - 1] for i in range(1, len(prices))):
        return "DOWNTREND"
    return None