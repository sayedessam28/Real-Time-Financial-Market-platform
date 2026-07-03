from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    TimestampType, MapType,
)

# NOTE: "timestamp" is StringType, not TimestampType.
# Producers send timestamps as ISO-8601 strings (datetime.now(timezone.utc).isoformat()),
# and bronze_job.py casts it explicitly with .cast("timestamp") after parsing.
# If this is TimestampType, from_json() will fail to parse the incoming
# string and silently produce nulls instead of raising — a hard bug to spot.
market_schema = StructType([
    StructField("symbol",    StringType(), True),
    StructField("price",     DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source",    StringType(), True),
])

alert_schema = StructType([
    StructField("alert_id",       StringType(),                        True),
    StructField("symbol",         StringType(),                        True),
    StructField("alert_type",     StringType(),                        True),
    StructField("severity",       StringType(),                        True),
    StructField("message",        StringType(),                        True),
    StructField("current_price",  DoubleType(),                        True),
    StructField("change_percent", DoubleType(),                        True),
    StructField("change_amount",  DoubleType(),                        True),
    StructField("created_at",     TimestampType(),                     True),
    StructField("extra_data",     MapType(StringType(), StringType()), True),
])

# Alert engine state — persisted to Iceberg so restarts don't lose context.
alert_state_schema = StructType([
    StructField("state_key",   StringType(),    False),
    StructField("state_value", StringType(),    True),
    StructField("updated_at",  TimestampType(), True),
])