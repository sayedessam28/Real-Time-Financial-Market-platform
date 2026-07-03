import os
from dotenv import load_dotenv

load_dotenv()

# ── Paths ─────────────────────────────────────────────────────────
_BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
_WAREHOUSE_DEFAULT = os.path.join(_BASE_DIR, "../../warehouse")

# ── Kafka ─────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC:             str = os.getenv("KAFKA_TOPIC",             "market_prices")

# ── Iceberg ───────────────────────────────────────────────────────
ICEBERG_WAREHOUSE: str = os.getenv("ICEBERG_WAREHOUSE", _WAREHOUSE_DEFAULT)

# ── Postgres ──────────────────────────────────────────────────────
POSTGRES_URL: str = os.getenv(
    "POSTGRES_URL", "jdbc:postgresql://localhost:5433/financial_db"
)
POSTGRES_PROPS: dict = {
    "user":     os.getenv("POSTGRES_USER",     "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin123"),
    "driver":   "org.postgresql.Driver",
}

# ── Spike / Trend thresholds ──────────────────────────────────────
SPIKE_THRESHOLD: float = float(os.getenv("SPIKE_THRESHOLD", "0.5"))  # %
TREND_COUNT:     int   = int(os.getenv("TREND_COUNT",       "5"))     # ticks
ALERT_COOLDOWN:  int   = int(os.getenv("ALERT_COOLDOWN",    "300"))   # seconds

# ── Gold Gap thresholds ───────────────────────────────────────────
# Separate from spike — global gold moves frequently so the GAP detector
# needs a longer cooldown and higher re-alert threshold to avoid spam.
GAP_THRESHOLD:    float = float(os.getenv("GAP_THRESHOLD",    "60.0"))   # EGP — min gap to fire
MIN_GAP_CHANGE:   float = float(os.getenv("MIN_GAP_CHANGE",   "50.0"))   # EGP — min change to re-alert within cooldown
GAP_COOLDOWN:     int   = int(os.getenv("GAP_COOLDOWN",       "3600"))   # seconds (1 hour)

# GAP severity bands (EGP) — independent of SPIKE severity which uses % change
GAP_SEVERITY_HIGH:   float = float(os.getenv("GAP_SEVERITY_HIGH",   "300.0"))
GAP_SEVERITY_MEDIUM: float = float(os.getenv("GAP_SEVERITY_MEDIUM", "150.0"))

# Stale price guard — if XAU/USD hasn't updated within this window,
# skip gap calculation to avoid false alerts from a frozen global price.
STALE_PRICE_SECONDS: int = int(os.getenv("STALE_PRICE_SECONDS", "300"))  # 5 minutes

# ── Alert job ─────────────────────────────────────────────────────
ALERT_POLL_INTERVAL: int = int(os.getenv("ALERT_POLL_INTERVAL", "30"))  # seconds

# ── Telegram ──────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID:   str = os.getenv("TELEGRAM_CHAT_ID",   "")

# ── Producer data quality ─────────────────────────────────────────
# Reject a price tick if it moved more than this % from the last
# accepted value — guards against API outliers / bad data.
# XAU/USD historically moves < 3% in a single day; 10% is very generous.
PRICE_OUTLIER_THRESHOLD_PCT: float = float(os.getenv("PRICE_OUTLIER_THRESHOLD_PCT", "10.0"))

# If the local gold price hasn't updated within this many seconds,
# skip the gap calculation — local price lag creates false gaps.
LOCAL_PRICE_MAX_AGE_SECONDS: int = int(os.getenv("LOCAL_PRICE_MAX_AGE_SECONDS", "180"))  # 3 minutes

# ── Alert job initial watermark ───────────────────────────────────
# On first startup (no persisted last_processed_time), only process
# Silver rows from the last N hours — prevents replaying weeks of
# historical data and generating stale alerts in bulk.
ALERT_LOOKBACK_HOURS: int = int(os.getenv("ALERT_LOOKBACK_HOURS", "1"))

# ── Health Monitor ────────────────────────────────────────────────
HEALTH_CHECK_INTERVAL: int = int(os.getenv("HEALTH_CHECK_INTERVAL", "300"))  # seconds (5 min)

# Max silence per symbol before firing a DEAD_FEED alert
HEALTH_MAX_SILENCE: dict = {
    "XAU/USD":       int(os.getenv("HEALTH_MAX_SILENCE_XAU_USD",   "600")),   # 10 min (updates every 2s)
    "USD/EGP":       int(os.getenv("HEALTH_MAX_SILENCE_USD_EGP",   "600")),   # 10 min (updates every 2s)
    "XAU/EGP_LOCAL": int(os.getenv("HEALTH_MAX_SILENCE_LOCAL",     "300")),   # 5 min  (updates every 60s)
}

# Silver lag: if Silver is this many seconds behind Bronze, warn
HEALTH_MAX_LAG_SECONDS: int = int(os.getenv("HEALTH_MAX_LAG_SECONDS", "300"))  # 5 min

# Health log file path
HEALTH_LOG_FILE: str = os.getenv("HEALTH_LOG_FILE", "logs/health.log")