from spark_streaming.common.spark_session import (
    create_spark_session
)

spark = create_spark_session(
    "ReadSilver"
)

df = spark.sql("""

SELECT *
FROM local.silver.market_prices_clean
LIMIT 20

""")

df.show(truncate=False)