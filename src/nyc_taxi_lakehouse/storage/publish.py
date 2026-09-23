"""Publish an existing local stage output to the corresponding Iceberg table."""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.storage.iceberg import (
    GOLD_DATASETS,
    TABLES,
    period_filter,
    replace_period,
    table_name,
)

LOGGER = logging.getLogger(__name__)


def publish_dataset(
    spark: SparkSession,
    dataset: str,
    path: Path,
    taxi_type: str,
    period: ProcessingPeriod,
) -> dict[str, object]:
    """Commit exactly one local output period to Iceberg, with read-back validation."""
    if not path.is_dir() or not any(path.glob("part-*.parquet")):
        raise FileNotFoundError(f"Iceberg publish source is incomplete: {path}")
    rows, snapshot = replace_period(
        spark, dataset, spark.read.parquet(str(path)), taxi_type, period
    )
    LOGGER.info(
        "Iceberg publish: dataset=%s period=%s rows=%s snapshot=%s",
        dataset, period.identifier, rows, snapshot.snapshot_id,
    )
    return {"rows": rows, "snapshot_id": snapshot.snapshot_id}


def validate_published_period(
    spark: SparkSession, taxi_type: str, period: ProcessingPeriod
) -> dict[str, int]:
    """Check every expected Iceberg table before accepting an incremental skip."""
    counts = {}
    for dataset in TABLES:
        table = table_name(dataset)
        if not spark.catalog.tableExists(table):
            raise FileNotFoundError(f"Iceberg table missing: {table}")
        counts[dataset] = spark.table(table).filter(period_filter(taxi_type, period)).count()
        if dataset != "quarantine" and counts[dataset] == 0:
            raise ValueError(f"Iceberg period missing: {table} {period.identifier}")
    if counts["bronze"] != counts["silver"] + counts["quarantine"]:
        raise ValueError(f"Iceberg quality reconciliation failed for {period.identifier}")
    for dataset in GOLD_DATASETS:
        represented = (
            spark.table(table_name(dataset))
            .filter(period_filter(taxi_type, period))
            .agg(F.sum("trip_count"))
            .first()[0]
        )
        if represented != counts["silver"]:
            raise ValueError(f"Iceberg Gold reconciliation failed for {dataset}")
    return counts
