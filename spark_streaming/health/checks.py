"""
HealthCheck results and check logic.
Pure functions — easy to unit test without DuckDB or Telegram.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from spark_streaming.config.settings import (
    HEALTH_MAX_SILENCE,
    HEALTH_MAX_LAG_SECONDS,
)


class Status(str, Enum):
    OK      = "OK"
    WARN    = "WARN"
    DEAD    = "DEAD"     # feed completely silent


@dataclass
class CheckResult:
    name:    str
    status:  Status
    message: str
    value:   Optional[float] = None   # e.g. seconds since last tick


@dataclass
class HealthReport:
    generated_at: datetime
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def is_healthy(self) -> bool:
        return all(c.status == Status.OK for c in self.checks)

    @property
    def has_dead(self) -> bool:
        return any(c.status == Status.DEAD for c in self.checks)

    @property
    def worst_status(self) -> Status:
        if self.has_dead:
            return Status.DEAD
        if any(c.status == Status.WARN for c in self.checks):
            return Status.WARN
        return Status.OK


# ── Individual check functions ────────────────────────────────────

def check_feed_freshness(
    symbol: str,
    last_seen: Optional[datetime],
    max_silence_seconds: int,
) -> CheckResult:
    """Is the symbol's feed alive and recent?"""
    if last_seen is None:
        return CheckResult(
            name    = f"feed:{symbol}",
            status  = Status.DEAD,
            message = f"{symbol} — never received any data",
        )

    now     = datetime.now(timezone.utc)
    age_sec = (now - last_seen).total_seconds()

    # WARN threshold = 50% of max silence (early warning)
    warn_threshold = max_silence_seconds * 0.5

    if age_sec >= max_silence_seconds:
        return CheckResult(
            name    = f"feed:{symbol}",
            status  = Status.DEAD,
            message = f"{symbol} — no data for {int(age_sec)}s (max={max_silence_seconds}s)",
            value   = age_sec,
        )
    if age_sec >= warn_threshold:
        return CheckResult(
            name    = f"feed:{symbol}",
            status  = Status.WARN,
            message = f"{symbol} — last tick {int(age_sec)}s ago (warn>{int(warn_threshold)}s)",
            value   = age_sec,
        )
    return CheckResult(
        name    = f"feed:{symbol}",
        status  = Status.OK,
        message = f"{symbol} — OK ({int(age_sec)}s ago)",
        value   = age_sec,
    )


def check_pipeline_lag(lag_seconds: Optional[float]) -> CheckResult:
    """Is Silver keeping up with Bronze?"""
    if lag_seconds is None:
        return CheckResult(
            name    = "pipeline:lag",
            status  = Status.WARN,
            message = "Could not compute Bronze→Silver lag (missing data?)",
        )

    warn_threshold = HEALTH_MAX_LAG_SECONDS * 0.5

    if lag_seconds >= HEALTH_MAX_LAG_SECONDS:
        return CheckResult(
            name    = "pipeline:lag",
            status  = Status.DEAD,
            message = f"Bronze→Silver lag = {int(lag_seconds)}s (max={HEALTH_MAX_LAG_SECONDS}s)",
            value   = lag_seconds,
        )
    if lag_seconds >= warn_threshold:
        return CheckResult(
            name    = "pipeline:lag",
            status  = Status.WARN,
            message = f"Bronze→Silver lag = {int(lag_seconds)}s (warn>{int(warn_threshold)}s)",
            value   = lag_seconds,
        )
    return CheckResult(
        name    = "pipeline:lag",
        status  = Status.OK,
        message = f"Bronze→Silver lag = {int(lag_seconds)}s ✓",
        value   = lag_seconds,
    )


def run_all_checks(
    last_ticks: dict[str, Optional[datetime]],
    lag_seconds: Optional[float],
) -> HealthReport:
    """Run every check and return a consolidated HealthReport."""
    now    = datetime.now(timezone.utc)
    checks = []

    # Feed freshness — one check per tracked symbol
    for symbol, max_silence in HEALTH_MAX_SILENCE.items():
        checks.append(
            check_feed_freshness(symbol, last_ticks.get(symbol), max_silence)
        )

    # Pipeline lag
    checks.append(check_pipeline_lag(lag_seconds))

    return HealthReport(generated_at=now, checks=checks)