"""
GoldGapDetector
───────────────
Fires an alert when the local EGP gold price diverges significantly
from the globally-derived expected price.

Expected price formula:
    expected = global_usd_per_oz × usd_egp_rate × 0.875 / 31.1035
    (0.875 = 21k purity factor, 31.1035 = troy oz → gram conversion)

Guards (in order of evaluation):
  1. Stale global price: skip if XAU/USD hasn't updated within
     STALE_PRICE_SECONDS — prevents false alerts from a frozen global price.

  2. Stale local price: skip if XAU/EGP_LOCAL hasn't updated within
     LOCAL_PRICE_MAX_AGE_SECONDS — the local scraper runs every 60s;
     if global gold drops fast and local hasn't responded yet, the gap
     is temporary lag, not a real arbitrage opportunity.

  3. Cooldown: won't re-alert within GAP_COOLDOWN seconds unless the gap
     changed by more than MIN_GAP_CHANGE EGP.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from spark_streaming.config.settings import (
    GAP_THRESHOLD,
    MIN_GAP_CHANGE,
    GAP_COOLDOWN,
    STALE_PRICE_SECONDS,
    LOCAL_PRICE_MAX_AGE_SECONDS,
)
from spark_streaming.alerts.state_store import AlertStateStore

logger = logging.getLogger(__name__)

_PURITY     = 0.875      # 21k purity factor
_OZ_TO_GRAM = 31.1035    # troy oz → gram


def _now() -> datetime:
    return datetime.now(timezone.utc)


class GoldGapDetector:
    def __init__(self, state_store: AlertStateStore) -> None:
        self._store              = state_store
        self._cooldown           = timedelta(seconds=GAP_COOLDOWN)
        self._stale_global_window = timedelta(seconds=STALE_PRICE_SECONDS)
        self._stale_local_window  = timedelta(seconds=LOCAL_PRICE_MAX_AGE_SECONDS)

        # ── Live prices ───────────────────────────────────────────
        self.global_gold_price: Optional[float]   = None
        self.local_gold_price:  Optional[float]   = None
        self.usd_egp:           Optional[float]   = None

        # Track freshness of each price independently
        self._global_gold_updated_at: Optional[datetime] = None
        self._local_gold_updated_at:  Optional[datetime] = None

        # ── Restore persisted state ───────────────────────────────
        persisted = state_store.load_all()

        raw_time   = persisted.get(AlertStateStore.gap_time_key())
        raw_amount = persisted.get(AlertStateStore.gap_amount_key())

        self._last_alert_time: Optional[datetime] = (
            datetime.fromisoformat(raw_time) if raw_time else None
        )
        self._last_gap_amount: Optional[float] = (
            float(raw_amount) if raw_amount else None
        )

        logger.info("GoldGapDetector ready")

    # ── Public ────────────────────────────────────────────────────

    def process(self, symbol: str, price: float) -> list[dict]:
        if price is None:
            return []

        now = _now()

        if symbol == "XAU/USD":
            self.global_gold_price       = price
            self._global_gold_updated_at = now
        elif symbol == "USD/EGP":
            self.usd_egp = price
        elif symbol == "XAU/EGP_LOCAL":
            self.local_gold_price       = price
            self._local_gold_updated_at = now
        else:
            return []

        # Need all three prices before computing
        if None in (self.global_gold_price, self.local_gold_price, self.usd_egp):
            return []

        # ── Guard 1: stale global price ───────────────────────────
        if self._global_gold_updated_at is not None:
            global_age = now - self._global_gold_updated_at
            if global_age > self._stale_global_window:
                logger.warning(
                    f"Skipping gap — XAU/USD is stale "
                    f"({int(global_age.total_seconds())}s old, "
                    f"threshold={STALE_PRICE_SECONDS}s)"
                )
                return []

        # ── Guard 2: stale local price ────────────────────────────
        # The local scraper runs every 60s. If the local price hasn't
        # updated recently, the gap may be pure lag — global moved but
        # local market hasn't responded yet. Don't alert on that.
        if self._local_gold_updated_at is not None:
            local_age = now - self._local_gold_updated_at
            if local_age > self._stale_local_window:
                logger.warning(
                    f"Skipping gap — XAU/EGP_LOCAL is stale "
                    f"({int(local_age.total_seconds())}s old, "
                    f"threshold={LOCAL_PRICE_MAX_AGE_SECONDS}s)"
                )
                return []

        expected = self.global_gold_price * self.usd_egp * _PURITY / _OZ_TO_GRAM
        gap      = round(abs(self.local_gold_price - expected), 2)

        if gap < GAP_THRESHOLD:
            return []

        # ── Guard 3: cooldown ─────────────────────────────────────
        if self._last_alert_time is not None:
            time_since_last = now - self._last_alert_time
            if time_since_last < self._cooldown:
                gap_change = (
                    abs(gap - self._last_gap_amount)
                    if self._last_gap_amount is not None
                    else float("inf")
                )
                if gap_change < MIN_GAP_CHANGE:
                    logger.debug(
                        f"GAP cooldown active "
                        f"({int(time_since_last.total_seconds())}s/{GAP_COOLDOWN}s), "
                        f"gap change {gap_change:.1f} EGP < {MIN_GAP_CHANGE} — skipping"
                    )
                    return []

        self._last_alert_time = now
        self._last_gap_amount = gap

        self._store.set(AlertStateStore.gap_time_key(),   now.isoformat())
        self._store.set(AlertStateStore.gap_amount_key(), str(gap))

        global_age_sec = (
            int((now - self._global_gold_updated_at).total_seconds())
            if self._global_gold_updated_at else "?"
        )
        local_age_sec = (
            int((now - self._local_gold_updated_at).total_seconds())
            if self._local_gold_updated_at else "?"
        )

        logger.warning(
            f"GOLD GAP ALERT: {gap} EGP | "
            f"global_age={global_age_sec}s | local_age={local_age_sec}s"
        )

        return [{
            "symbol":         "XAU_GAP",
            "alert_type":     "GOLD_GAP",
            "message":        f"Gold gap detected ({gap} EGP)",
            "current_price":  self.local_gold_price,
            "change_percent": 0.0,
            "change_amount":  gap,
            "extra_data": {
                "global_gold":     str(self.global_gold_price),
                "usd_egp":         str(self.usd_egp),
                "expected_local":  str(round(expected, 2)),
                "actual_local":    str(self.local_gold_price),
                "global_age_sec":  str(global_age_sec),
                "local_age_sec":   str(local_age_sec),
            },
        }]