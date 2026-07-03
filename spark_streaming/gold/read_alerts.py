from spark_streaming.common.spark_session import (
    create_spark_session
)

spark = create_spark_session(
    "ReadAlerts"
)

spark.sql("""

SELECT *
FROM local.gold.market_alerts
ORDER BY created_at DESC

""").show(
    truncate=False
)