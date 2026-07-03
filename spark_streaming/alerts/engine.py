"""
AlertEngine
───────────
Detects price spikes and trends.
State is loaded from AlertStateStore at init and staged for flush after each batch.
"""
from __future__ import annotations

import json
import logging
from collections import deque
from datetime import datetime, timedelta, timezone

from spark_streaming.config.settings import SPIKE_THRESHOLD, TREND_COUNT, ALERT_COOLDOWN
from spark_streaming.alerts.rules import calculate_percentage_change, detect_trend
from spark_streaming.alerts.state_store import AlertStateStore

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AlertEngine:
    def __init__(self, state_store: AlertStateStore) -> None:
        self._store        = state_store
        self._cooldown     = timedelta(seconds=ALERT_COOLDOWN)

        # ── Restore state from Iceberg ────────────────────────────
        persisted = state_store.load_all()

        self._last_prices: dict[str, float] = {}
        self._price_history: dict[str, deque] = {}
        self._last_alert_time: dict[str, datetime] = {}

        for symbol_key, raw in persisted.items():
            if symbol_key.endswith("__last_price"):
                symbol = symbol_key.replace("__last_price", "")
                self._last_prices[symbol] = float(raw)

            elif symbol_key.endswith("__price_history"):
                symbol = symbol_key.replace("__price_history", "")
                self._price_history[symbol] = deque(
                    json.loads(raw), maxlen=TREND_COUNT
                )

            elif "__alert_time" in symbol_key:
                # key format: SYMBOL__ALERT_TYPE__alert_time
                parts = symbol_key.rsplit("__", 2)
                if len(parts) == 3:
                    key = f"{parts[0]}__{parts[1]}"
                    self._last_alert_time[key] = datetime.fromisoformat(raw)

        logger.info(
            f"AlertEngine ready — "
            f"{len(self._last_prices)} symbols, "
            f"{len(self._last_alert_time)} cooldowns restored"
        )

    # ── Public ────────────────────────────────────────────────────

    def process(self, symbol: str, price: float) -> list[dict]:
        """Process one tick. Returns a (possibly empty) list of alert dicts."""
        if price is None:
            return []

        alerts = []
        prev   = self._last_prices.get(symbol)

        # ── Spike detection ───────────────────────────────────────
        if prev is not None:
            change_pct = round(calculate_percentage_change(price, prev), 3)
            if change_pct >= SPIKE_THRESHOLD:
                direction = "SPIKE_UP" if price > prev else "SPIKE_DOWN"
                if self._can_alert(symbol, direction):
                    alerts.append({
                        "symbol":         symbol,
                        "alert_type":     direction,
                        "message":        f"Price moved {change_pct}%",
                        "current_price":  price,
                        "change_percent": change_pct,
                        "change_amount":  None,
                        "extra_data":     {"previous_price": str(prev)},
                    })
                    self._record_alert(symbol, direction)
                    logger.warning(f"SPIKE: {symbol} {direction} {change_pct}%")

        # ── Trend detection ───────────────────────────────────────
        history = self._price_history.setdefault(
            symbol, deque(maxlen=TREND_COUNT)
        )
        history.append(price)

        if len(history) == TREND_COUNT:
            trend = detect_trend(list(history))
            if trend and self._can_alert(symbol, trend):
                alerts.append({
                    "symbol":         symbol,
                    "alert_type":     trend,
                    "message":        f"{trend.capitalize()} detected",
                    "current_price":  price,
                    "change_percent": None,
                    "change_amount":  None,
                    "extra_data":     {"prices": str(list(history))},
                })
                self._record_alert(symbol, trend)
                logger.warning(f"TREND: {symbol} {trend}")

        # ── Update state (staged, not written yet) ────────────────
        self._last_prices[symbol] = price
        self._store.set(AlertStateStore.price_key(symbol),   str(price))
        self._store.set(AlertStateStore.history_key(symbol), json.dumps(list(history)))

        return alerts

    # ── Private ───────────────────────────────────────────────────

    def _can_alert(self, symbol: str, alert_type: str) -> bool:
        key  = f"{symbol}__{alert_type}"
        last = self._last_alert_time.get(key)
        return last is None or (_now() - last) > self._cooldown

    def _record_alert(self, symbol: str, alert_type: str) -> None:
        key  = f"{symbol}__{alert_type}"
        now  = _now()
        self._last_alert_time[key] = now
        self._store.set(
            AlertStateStore.alert_time_key(symbol, alert_type),
            now.isoformat(),
        )