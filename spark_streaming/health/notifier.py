"""
Health Monitor Notifier
────────────────────────
Sends HealthReport summaries to Telegram and writes to a log file.

Telegram message design:
  - One message per check cycle (not one per check)
  - Only sends if something is WARN or DEAD — silent when all OK
  - Exception: sends a daily heartbeat at 09:00 Cairo to confirm system alive
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pytz
import requests

from spark_streaming.config.settings import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    HEALTH_LOG_FILE,
)
from spark_streaming.health.checks import HealthReport, Status

logger    = logging.getLogger(__name__)
CAIRO_TZ  = pytz.timezone("Africa/Cairo")
_BASE_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

_STATUS_EMOJI = {
    Status.OK:   "✅",
    Status.WARN: "⚠️",
    Status.DEAD: "🔴",
}


# ── Log file ──────────────────────────────────────────────────────

def _ensure_log_dir() -> None:
    Path(HEALTH_LOG_FILE).parent.mkdir(parents=True, exist_ok=True)


def write_log(report: HealthReport) -> None:
    """Append a structured line to the health log file."""
    _ensure_log_dir()
    cairo_time = report.generated_at.astimezone(CAIRO_TZ).strftime("%Y-%m-%d %H:%M:%S")
    status     = report.worst_status.value

    lines = [f"\n[{cairo_time}] STATUS={status}"]
    for check in report.checks:
        lines.append(f"  {_STATUS_EMOJI[check.status]} {check.message}")

    try:
        with open(HEALTH_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n".join(lines))
    except Exception as e:
        logger.error(f"Failed to write health log: {e}")


# ── Telegram ──────────────────────────────────────────────────────

def _format_telegram(report: HealthReport) -> str:
    cairo_time = report.generated_at.astimezone(CAIRO_TZ).strftime("%Y-%m-%d %H:%M:%S")
    overall    = _STATUS_EMOJI[report.worst_status]

    lines = [
        f"{overall} *Pipeline Health* — `{cairo_time} (Cairo)`",
        "",
    ]

    for check in report.checks:
        emoji = _STATUS_EMOJI[check.status]
        # Bold the check name, plain message
        name = check.name.replace("feed:", "").replace("pipeline:", "")
        lines.append(f"{emoji} *{name}*: {check.message}")

    return "\n".join(lines)


def send_telegram(report: HealthReport) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured — skipping health notification")
        return False

    try:
        response = requests.post(
            _BASE_URL,
            json={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       _format_telegram(report),
                "parse_mode": "Markdown",
            },
            timeout=10,
        )
        response.raise_for_status()
        logger.info("Health report sent to Telegram")
        return True
    except Exception as e:
        logger.error(f"Telegram health send failed: {e}")
        return False


# ── Heartbeat ─────────────────────────────────────────────────────

_last_heartbeat_date: str | None = None

def maybe_send_heartbeat(report: HealthReport) -> None:
    """
    Send a daily 'system alive' message at 09:00 Cairo even when healthy.
    Prevents the false comfort of 'no news is good news'.
    """
    global _last_heartbeat_date

    cairo_now  = report.generated_at.astimezone(CAIRO_TZ)
    today      = cairo_now.strftime("%Y-%m-%d")
    hour       = cairo_now.hour

    if hour == 9 and _last_heartbeat_date != today:
        _last_heartbeat_date = today
        counts_line = ""  # will be filled by monitor.py

        heartbeat_text = (
            "💚 *Daily Heartbeat* — Pipeline is running\n"
            f"🕐 `{cairo_now.strftime('%Y-%m-%d %H:%M:%S')} (Cairo)`"
        )
        try:
            requests.post(
                _BASE_URL,
                json={
                    "chat_id":    TELEGRAM_CHAT_ID,
                    "text":       heartbeat_text,
                    "parse_mode": "Markdown",
                },
                timeout=10,
            )
            logger.info("Daily heartbeat sent")
        except Exception as e:
            logger.error(f"Heartbeat failed: {e}")