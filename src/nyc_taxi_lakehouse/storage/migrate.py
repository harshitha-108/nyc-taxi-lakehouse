"""Restartable migration of validated local Parquet periods into Iceberg."""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from nyc_taxi_lakehouse.ingestion.nyc_taxi import TaxiDataRequest, raw_data_path
from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod, period_range
from nyc_taxi_lakehouse.schema.validator import Compatibility, validate_and_report
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.iceberg import (
    GOLD_DATASETS,
    TABLES,
    IcebergStorageError,
    period_filter,
    replace_period,
    table_name,
)
from nyc_taxi_lakehouse.storage.spark import create_spark_session

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MigrationPaths:
    data_dir: Path = Path("data")

    def source(self, dataset: str, taxi_type: str, period: ProcessingPeriod) -> Path:
        """Locate one existing filesystem partition without rewriting it."""
        suffix = Path(taxi_type) / f"year={period.year}" / f"month={period.month:02d}"
        if dataset in {"bronze", "silver"}:
            return self.data_dir / dataset / suffix
        if dataset == "quarantine":
            return self.data_dir / "quarantine" / "silver" / suffix
        if dataset in GOLD_DATASETS:
            return self.data_dir / "gold" / dataset / suffix
        raise IcebergStorageError(f"Unknown migration dataset: {dataset}")


def schema_gate(taxi_type: str, period: ProcessingPeriod, paths: MigrationPaths) -> None:
    """Block all Iceberg writes if the existing Phase 8 source contract is breaking."""
    raw_path = raw_data_path(
        TaxiDataRequest(taxi_type, period.year, period.month), paths.data_dir / "raw"
    )
    result = validate_and_report(raw_path, period, taxi_type, paths.data_dir / "state")
    if result["compatibility"] == Compatibility.BREAKING:
        raise IcebergStorageError(f"Breaking raw schema contract for {period.identifier}")


def migrate_period(
    spark: SparkSession,
    taxi_type: str,
    period: ProcessingPeriod,
    datasets: tuple[str, ...] = tuple(TABLES),
    paths: MigrationPaths | None = None,
) -> dict[str, object]:
    """Migrate selected existing outputs, validating each committed table."""
    started = time.monotonic()
    active_paths = paths or MigrationPaths()
    if any(name not in TABLES for name in datasets):
        raise IcebergStorageError("Unknown dataset requested for migration")
    # All prerequisites are checked before the first commit for this period.
    schema_gate(taxi_type, period, active_paths)
    for name in datasets:
        source = active_paths.source(name, taxi_type, period)
        if not source.is_dir() or not any(source.glob("part-*.parquet")):
            raise IcebergStorageError(f"Missing filesystem source: {source}")

    counts: dict[str, int] = {}
    snapshots: dict[str, int] = {}
    for name in datasets:
        source = active_paths.source(name, taxi_type, period)
        frame = spark.read.parquet(str(source))
        count, snapshot = replace_period(spark, name, frame, taxi_type, period)
        counts[name] = count
        snapshots[name] = snapshot.snapshot_id
        if name in GOLD_DATASETS:
            readback = spark.table(table_name(name)).filter(period_filter(taxi_type, period))
            if (
                frame.exceptAll(readback).limit(1).count()
                or readback.exceptAll(frame).limit(1).count()
            ):
                raise IcebergStorageError(f"Gold values changed during migration: {name}")
        LOGGER.info(
            "Migrated %s %s: rows=%s snapshot=%s source=%s",
            period.identifier, name, count, snapshot.snapshot_id, source,
        )
    if {"bronze", "silver", "quarantine"}.issubset(counts):
        if counts["bronze"] != counts["silver"] + counts["quarantine"]:
            raise IcebergStorageError(f"Silver quality reconciliation failed: {period.identifier}")
    if "silver" in counts:
        for name in GOLD_DATASETS:
            if name not in counts:
                continue
            represented = spark.table(table_name(name)).filter(
                period_filter(taxi_type, period)
            ).agg(F.sum("trip_count")).first()[0]
            if represented != counts["silver"]:
                raise IcebergStorageError(
                    f"Gold trip reconciliation failed for {name} {period.identifier}"
                )
    return {
        "period": period.identifier,
        "counts": counts,
        "snapshots": snapshots,
        "elapsed_seconds": round(time.monotonic() - started, 2),
    }


def main() -> int:
    """Run one or more monthly migrations using existing local output files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--datasets", nargs="+", choices=tuple(TABLES), default=list(TABLES))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = create_spark_session("nyc-taxi-iceberg-migration", StorageConfig.from_env("iceberg"))
    try:
        periods = period_range(ProcessingPeriod.parse(args.start), ProcessingPeriod.parse(args.end))
        for period in periods:
            result = migrate_period(spark, args.taxi_type, period, tuple(args.datasets))
            LOGGER.info("Migration result: %s", result)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
