import logging
from datetime import datetime, timezone

import pytz
import requests

from spark_streaming.config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger    = logging.getLogger(__name__)
CAIRO_TZ  = pytz.timezone("Africa/Cairo")
_BASE_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# ── Emoji maps ────────────────────────────────────────────────────
_TYPE_EMOJI = {
    "SPIKE_UP":   "📈",
    "SPIKE_DOWN": "📉",
    "UPTREND":    "🚀",
    "DOWNTREND":  "⬇️",
    "GOLD_GAP":   "🔔",   # was broken unicode — fixed
}

_SEVERITY_EMOJI = {
    "HIGH":   "🔴",
    "MEDIUM": "🟡",
    "LOW":    "🟢",
}


def _format_message(alert: dict) -> str:
    alert_type = alert.get("alert_type", "")
    severity   = alert.get("severity",   "LOW")
    symbol     = alert.get("symbol",     "")
    message    = alert.get("message",    "")
    price      = alert.get("current_price")
    extra      = alert.get("extra_data", {}) or {}
    created_at = alert.get("created_at", datetime.now(timezone.utc))

    # Normalise timezone
    if isinstance(created_at, datetime):
        if created_at.tzinfo is None:
            created_at = pytz.utc.localize(created_at)
        time_str = created_at.astimezone(CAIRO_TZ).strftime("%Y-%m-%d %H:%M:%S (Cairo)")
    else:
        time_str = str(created_at)

    lines = [
        f"{_TYPE_EMOJI.get(alert_type, '⚠️')} *{alert_type}* {_SEVERITY_EMOJI.get(severity, '')}",
        f"🕐 `{time_str}`",
        f"*Symbol:* `{symbol}`",
        f"*Message:* {message}",
    ]

    if price is not None:
        lines.append(f"*Price:* `{price:,.2f}`")

    if alert_type == "GOLD_GAP" and extra:
        lines += [
            f"*Expected:*    `{extra.get('expected_local', 'N/A')} EGP`",
            f"*Actual:*      `{extra.get('actual_local',   'N/A')} EGP`",
            f"*USD/EGP:*     `{extra.get('usd_egp',        'N/A')}`",
            f"*Global Gold:* `{extra.get('global_gold',    'N/A')} USD`",
        ]
    elif alert_type in ("SPIKE_UP", "SPIKE_DOWN") and extra:
        lines.append(f"*Previous:* `{extra.get('previous_price', 'N/A')}`")

    return "\n".join(lines)


def send_alert(alert: dict) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured — skipping notification")
        return False

    try:
        response = requests.post(
            _BASE_URL,
            json={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       _format_message(alert),
                "parse_mode": "Markdown",
            },
            timeout=10,
        )
        response.raise_for_status()
        logger.info(f"Telegram sent: {alert.get('alert_type')} / {alert.get('symbol')}")
        return True

    except Exception as e:
        logger.error(f"Telegram error: {e}")
        return False