import time
from shared.logger import get_logger
from producer.utils.kafka_config import producer
from producer.services.api_clients import get_gold_price
from producer.services.price_validator import PriceValidator

logger     = get_logger("gold_producer")
TOPIC_NAME = "market_prices"
INTERVAL   = 2  # seconds

validator = PriceValidator()


def start_gold_producer() -> None:
    logger.info("Gold (XAU/USD) producer started")
    while True:
        data = get_gold_price()
        if validator.accept(data["symbol"], data["price"]):
            producer.send(TOPIC_NAME, value=data)
            logger.info(f"Sent: {data['symbol']} = {data['price']}")
        elif data["price"] is None:
            logger.warning("Skipped null price")
        # outlier case already logged inside validator.accept()
        time.sleep(INTERVAL)