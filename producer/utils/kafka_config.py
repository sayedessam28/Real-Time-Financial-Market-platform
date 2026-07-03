import json
from kafka import KafkaProducer
from spark_streaming.config.settings import KAFKA_BOOTSTRAP_SERVERS

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    # Retry on transient failures
    retries=3,
    retry_backoff_ms=500,
)