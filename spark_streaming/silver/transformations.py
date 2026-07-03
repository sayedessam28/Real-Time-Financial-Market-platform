from pyspark.sql.functions import col, avg, max, min, window

def create_windowed_df(df):
    return (
        df
        .withWatermark("event_time", "1 minute")
        .groupBy(
            window(col("event_time"), "30 seconds"),
            col("symbol")
        )
        .agg(
            avg("price").alias("moving_avg"),
            max("price").alias("max_price"),
            min("price").alias("min_price")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("moving_avg"),
            col("max_price"),
            col("min_price")
        )
    )
