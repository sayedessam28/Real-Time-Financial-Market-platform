from pyspark.sql import SparkSession
from spark_streaming.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC
)

def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
