from pyspark.sql import SparkSession
from spark_streaming.config.settings import ICEBERG_WAREHOUSE


def create_spark_session(app_name: str = "FinancialMarket") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.postgresql:postgresql:42.7.3",
            ]),
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # ── Iceberg catalog ───────────────────────────────────────
        .config("spark.sql.catalog.local",           "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type",      "hadoop")
        .config("spark.sql.catalog.local.warehouse", ICEBERG_WAREHOUSE)
        # ── Snapshot retention ────────────────────────────────────
        # Keep at least 100 snapshots so the Silver streaming job
        # never hits "snapshot was expired or removed" during normal ops.
        # The silver job reads Bronze as a stream — if Bronze expires a
        # snapshot that Silver's checkpoint still references, the stream
        # crashes. 100 snapshots at 1-min micro-batch = ~1.5 hrs buffer.
        .config("spark.sql.catalog.local.snapshot-retention-period", "7d")
        .config("spark.sql.catalog.local.min-snapshots-to-keep",     "100")
        # ── Performance ───────────────────────────────────────────
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark