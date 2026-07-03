from spark_streaming.common.spark_session import (
    create_spark_session
)

spark = create_spark_session(
    "InspectIceberg"
)

from pyspark.sql import functions as F

print("\n=== TABLES ===")
spark.sql(
    "SHOW TABLES IN local.bronze"
).show(truncate=False)

print("\n=== TABLE DETAILS ===")
spark.sql("""
DESCRIBE TABLE EXTENDED local.bronze.market_ticks
""").show(200, truncate=False)

print("\n=== SNAPSHOTS ===")
spark.sql("""
SELECT *
FROM local.bronze.market_ticks.snapshots
""").show(truncate=False)

print("\n=== FILES ===")
spark.sql("""
SELECT *
FROM local.bronze.market_ticks.files
""").show(truncate=False)

print("\n=== DATA ===")
df = spark.read.table(
    "local.bronze.market_ticks"
)

df.orderBy(F.col("ingestion_time").desc()).limit(50).show(truncate=False)

df.show(50 ,truncate=False)