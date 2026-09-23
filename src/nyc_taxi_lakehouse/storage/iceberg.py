"""Iceberg table management and explicit period-scoped replacement."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod

NAMESPACE = "lakehouse.nyc_taxi"
TABLES = {
    "bronze": "bronze_trips",
    "silver": "silver_trips",
    "quarantine": "quarantine_trips",
    "daily_trip_metrics": "gold_daily_trip_metrics",
    "hourly_demand": "gold_hourly_demand",
    "pickup_location_performance": "gold_pickup_location_performance",
    "payment_type_summary": "gold_payment_type_summary",
    "pickup_zone_performance": "gold_pickup_zone_performance",
}
GOLD_DATASETS = tuple(name for name in TABLES if name not in {"bronze", "silver", "quarantine"})
PERIOD_COLUMNS = ("_source_taxi_type", "_source_year", "_source_month")


class IcebergStorageError(RuntimeError):
    """Raised when an Iceberg table operation cannot be safely completed."""


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: int
    operation: str
    committed_at: str


def table_name(dataset: str, namespace: str = NAMESPACE) -> str:
    """Return a controlled fully qualified table name."""
    if dataset not in TABLES:
        raise IcebergStorageError(f"Unknown Iceberg dataset: {dataset}")
    if not all(part.isidentifier() for part in namespace.split(".")):
        raise IcebergStorageError(f"Invalid namespace: {namespace}")
    return f"{namespace}.{TABLES[dataset]}"


def period_filter(taxi_type: str, period: ProcessingPeriod):
    """Build the exact source-period predicate used for overwrite and read-back."""
    return (
        (F.col("_source_taxi_type") == taxi_type)
        & (F.col("_source_year") == period.year)
        & (F.col("_source_month") == period.month)
    )


def ensure_namespace(spark: SparkSession, namespace: str = NAMESPACE) -> None:
    """Create an isolated logical namespace; no business data is stored in the catalog DB."""
    if not all(part.isidentifier() for part in namespace.split(".")):
        raise IcebergStorageError(f"Invalid namespace: {namespace}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def latest_snapshot(spark: SparkSession, table: str) -> Snapshot | None:
    """Inspect the latest committed Iceberg snapshot."""
    row = spark.sql(
        f"SELECT snapshot_id, operation, committed_at FROM {table}.snapshots "
        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    ).first()
    return Snapshot(int(row.snapshot_id), row.operation, str(row.committed_at)) if row else None


def replace_period(
    spark: SparkSession,
    dataset: str,
    frame: DataFrame,
    taxi_type: str,
    period: ProcessingPeriod,
    *,
    namespace: str = NAMESPACE,
) -> tuple[int, Snapshot]:
    """Commit one monthly overwrite and validate its Iceberg read-back.

    Each table commit is atomic. Multi-table migration is intentionally not one transaction.
    """
    table = table_name(dataset, namespace)
    missing = set(PERIOD_COLUMNS) - set(frame.columns)
    if missing:
        raise IcebergStorageError(f"{dataset} lacks period columns: {sorted(missing)}")
    matches = period_filter(taxi_type, period)
    invalid = frame.filter(~matches | matches.isNull()).limit(1).count()
    if invalid:
        raise IcebergStorageError(f"{dataset} contains rows outside {period.identifier}")
    source_count = frame.count()
    if dataset != "quarantine" and source_count == 0:
        raise IcebergStorageError(f"{dataset} source is empty for {period.identifier}")
    ensure_namespace(spark, namespace)
    if not spark.catalog.tableExists(table):
        (
            frame.limit(0)
            .writeTo(table)
            .using("iceberg")
            .partitionedBy(*PERIOD_COLUMNS)
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .create()
        )
    else:
        existing = spark.table(table).schema
        if [(f.name, f.dataType) for f in existing] != [
            (f.name, f.dataType) for f in frame.schema
        ]:
            raise IcebergStorageError(f"Schema mismatch for existing table {table}")
    frame.writeTo(table).overwrite(period_filter(taxi_type, period))
    readback = spark.table(table).filter(period_filter(taxi_type, period))
    actual = readback.count()
    if actual != source_count:
        raise IcebergStorageError(
            f"Iceberg read-back mismatch for {table}: source={source_count} actual={actual}"
        )
    snapshot = latest_snapshot(spark, table)
    if snapshot is None:
        raise IcebergStorageError(f"No committed snapshot for {table}")
    return actual, snapshot
