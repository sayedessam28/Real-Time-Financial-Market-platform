import re
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from shared.logger import get_logger

usd_logger  = get_logger("fxapi_service")
gold_logger = get_logger("goldapi_service")

USD_API_URL  = "https://fxapi.app/api/USD/EGP.json"
GOLD_API_URL = "https://api.gold-api.com/price/XAU"


def _now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def get_usd_price() -> dict:
    try:
        response = requests.get(USD_API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        usd_logger.info(f"USD/EGP fetched: {data['rate']}")
        return {
            "symbol":    "USD/EGP",
            "price":     data["rate"],
            "timestamp": _now_iso(),
            "source":    "fxapi",
        }
    except Exception as e:
        usd_logger.error(f"USD API error: {e}")
        return {
            "symbol":    "USD/EGP",
            "price":     None,
            "timestamp": _now_iso(),
            "source":    "fallback",
        }


def get_gold_price() -> dict:
    try:
        response = requests.get(GOLD_API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        gold_logger.info(f"XAU/USD fetched: {data['price']}")
        return {
            "symbol":    "XAU/USD",
            "price":     data["price"],
            "timestamp": _now_iso(),
            "source":    "gold_api",
        }
    except Exception as e:
        gold_logger.error(f"Gold API error: {e}")
        return {
            "symbol":    "XAU/USD",
            "price":     None,
            "timestamp": _now_iso(),
            "source":    "fallback",
        }


def get_gold_price_egypt_21() -> dict:
    """
    Scrape 21-karat gold price per gram in EGP from gold-price-live.com.
    Raises ValueError if the table or price row is not found.
    Note: scraping is fragile — monitor for HTML changes.
    """
    url = "https://gold-price-live.com/view/kerat-21"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()

    soup  = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="local-cur")

    if not table:
        raise ValueError("Gold price table not found — page structure may have changed")

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) != 2:
            continue

        label = cells[0].get_text(strip=True)
        value = cells[1].get_text(strip=True)

        if "1 جرام" in label and "21" in label:
            numbers = re.findall(r"[\d,]+", value)
            if numbers:
                price = float(numbers[0].replace(",", ""))
                return {
                    "symbol":    "XAU/EGP_LOCAL",
                    "price":     price,
                    "timestamp": _now_iso(),
                    "source":    "gold-price-live",
                }

    raise ValueError("Could not parse 21k gold price — row not found")