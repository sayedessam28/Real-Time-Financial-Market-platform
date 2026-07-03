import time
from shared.logger import get_logger
from producer.utils.kafka_config import producer
from producer.services.api_clients import get_gold_price_egypt_21

logger = get_logger("local_gold_producer")
TOPIC_NAME = "market_prices"

def start_local_gold_producer():
    logger.info("Local gold producer started")
    while True:
        try:
            data = get_gold_price_egypt_21()
            if data["price"] is not None:
                producer.send(TOPIC_NAME, value=data)
                logger.info(f"Sent: {data['symbol']} = {data['price']}")
            else:
                logger.warning("Skipped null price")
        except Exception as e:
            logger.error(f"Error: {e}")
        time.sleep(60)
