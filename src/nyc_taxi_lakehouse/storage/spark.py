"""Shared Spark session builder for filesystem and Iceberg jobs."""

from __future__ import annotations

from pyspark.sql import SparkSession

from nyc_taxi_lakehouse.storage.config import StorageConfig


def create_spark_session(
    app_name: str = "nyc-taxi-lakehouse", config: StorageConfig | None = None
) -> SparkSession:
    """Create Spark with the historical local defaults or an explicit Iceberg catalog."""
    selected = config or StorageConfig.from_env()
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if selected.backend == "iceberg":
        selected.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        settings = {
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
            "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.lakehouse.type": "jdbc",
            "spark.sql.catalog.lakehouse.uri": selected.jdbc_uri,
            "spark.sql.catalog.lakehouse.warehouse": selected.warehouse,
            "spark.sql.catalog.lakehouse.clients": "1",
            "spark.hadoop.fs.s3a.endpoint": selected.endpoint,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl.disable.cache": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            ),
            "spark.hadoop.fs.s3a.access.key": selected.access_key,
            "spark.hadoop.fs.s3a.secret.key": selected.secret_key,
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        }
        for name, value in settings.items():
            builder = builder.config(name, value)
    session = builder.getOrCreate()
    if selected.backend == "iceberg":
        # getOrCreate may reuse a filesystem-mode SparkContext (notably in pytest).
        # spark.hadoop.* builder options only initialize a new context, so update
        # the live Hadoop configuration before any S3A filesystem is instantiated.
        hadoop = session.sparkContext._jsc.hadoopConfiguration()
        hadoop.set("fs.s3a.endpoint", selected.endpoint)
        hadoop.set("fs.s3a.path.style.access", "true")
        hadoop.set("fs.s3a.impl.disable.cache", "true")
        hadoop.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        hadoop.set("fs.s3a.access.key", selected.access_key)
        hadoop.set("fs.s3a.secret.key", selected.secret_key)
    return session
