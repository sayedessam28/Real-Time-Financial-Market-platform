"""
PriceValidator
──────────────
Stateful per-symbol outlier filter for producer price ticks.

Problem it solves:
  The XAU/USD API occasionally returns wildly inconsistent values in
  back-to-back calls (observed: 4066 → 4192 in 2 seconds = +3% in 2s,
  impossible for gold). These outliers cause false GOLD_GAP alerts because
  the expected local price jumps instantly while the local scraper price
  (updated every 60s) hasn't moved yet.

How it works:
  - Keeps the last accepted price per symbol.
  - Rejects any new price that deviates more than PRICE_OUTLIER_THRESHOLD_PCT
    from the last accepted value.
  - Rejected prices are logged as warnings; the producer skips sending them
    to Kafka.
  - First tick for a symbol is always accepted (no baseline to compare).

Usage:
    validator = PriceValidator()

    data = get_gold_price()
    if validator.accept(data["symbol"], data["price"]):
        producer.send(TOPIC, value=data)
    else:
        logger.warning(f"Outlier rejected: {data}")
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Import here so the validator works standalone without Spark on PATH.
# Falls back to default if settings aren't available.
try:
    from spark_streaming.config.settings import PRICE_OUTLIER_THRESHOLD_PCT
except ImportError:
    PRICE_OUTLIER_THRESHOLD_PCT = 10.0


class PriceValidator:
    def __init__(self, threshold_pct: float = PRICE_OUTLIER_THRESHOLD_PCT) -> None:
        """
        Args:
            threshold_pct: Max allowed % change from last accepted price.
                           Default from PRICE_OUTLIER_THRESHOLD_PCT env var.
        """
        self._threshold_pct  = threshold_pct
        self._last_prices: dict[str, float] = {}

    def accept(self, symbol: str, price: Optional[float]) -> bool:
        """
        Return True if the price should be sent to Kafka, False if rejected.

        None prices are always rejected (handled separately by producers).
        First tick per symbol is always accepted.
        """
        if price is None:
            return False

        last = self._last_prices.get(symbol)

        if last is None:
            # First tick — accept and store
            self._last_prices[symbol] = price
            return True

        pct_change = abs(price - last) / last * 100

        if pct_change > self._threshold_pct:
            logger.warning(
                f"OUTLIER REJECTED | {symbol} | "
                f"last={last:.4f} new={price:.4f} "
                f"change={pct_change:.2f}% > threshold={self._threshold_pct}%"
            )
            return False

        self._last_prices[symbol] = price
        return True

    def last_price(self, symbol: str) -> Optional[float]:
        """Return the last accepted price for a symbol, or None."""
        return self._last_prices.get(symbol)