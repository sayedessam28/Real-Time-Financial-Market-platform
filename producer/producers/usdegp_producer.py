import time
import sys
from shared.logger import get_logger
from producer.services.api_clients import get_usd_price
from producer.utils.kafka_config import producer

logger = get_logger("usdegp_producer")

TOPIC_NAME = "market_prices"

def start_usdegp_stream():
    logger.info("USD/EGP producer started")
    while True:

        data = get_usd_price()

        if data["price"] is not None:

            producer.send(TOPIC_NAME, value=data)

            logger.info(f"Sent to Kafka: {data}")

        else:
            logger.warning("Skipped null price data")

        time.sleep(2)