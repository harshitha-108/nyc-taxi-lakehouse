"""Stage-level adapters for Airflow; the existing processors remain the data plane."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from pyspark.sql import functions as F

from nyc_taxi_lakehouse.bronze.processor import BronzeRequest, write_bronze_partition
from nyc_taxi_lakehouse.gold.geographic import (
    pickup_zone_partition_path,
    process_geographic_enrichment,
)
from nyc_taxi_lakehouse.gold.processor import (
    GOLD_DATASETS,
    GoldRequest,
    gold_partition_path,
    process_gold_partition,
)
from nyc_taxi_lakehouse.ingestion.nyc_taxi import TaxiDataRequest, ingest_taxi_data, raw_data_path
from nyc_taxi_lakehouse.orchestration.pipeline import PipelinePaths, validate_period_artifacts
from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.reference.taxi_zones import ingest_taxi_zones
from nyc_taxi_lakehouse.schema.validator import Compatibility, validate_and_report
from nyc_taxi_lakehouse.serving.config import ServingConfig
from nyc_taxi_lakehouse.serving.publisher import publish_period, validate_period
from nyc_taxi_lakehouse.silver.processor import (
    SilverRequest,
    bronze_partition_path,
    process_silver_partition,
    quarantine_partition_path,
    silver_partition_path,
)
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.iceberg import TABLES
from nyc_taxi_lakehouse.storage.publish import publish_dataset, validate_published_period
from nyc_taxi_lakehouse.storage.spark import create_spark_session

LOGGER = logging.getLogger(__name__)


class SchemaContractFailure(ValueError):
    """A deterministic breaking source contract; Airflow must block downstream tasks."""


def processing_period(interval_start: datetime, override: str | None = None) -> ProcessingPeriod:
    """Use the monthly interval's start, or an explicit manual replay period."""
    if override:
        return ProcessingPeriod.parse(override)
    if interval_start is None or interval_start.tzinfo is None:
        raise ValueError("A timezone-aware Airflow data_interval_start is required.")
    if interval_start.day != 1 or interval_start.hour or interval_start.minute:
        raise ValueError("The Airflow data interval must begin at month start.")
    return ProcessingPeriod(interval_start.year, interval_start.month)


def _spark_stage(name: str, operation: Any) -> dict[str, object]:
    spark = create_spark_session(f"nyc-taxi-airflow-{name}", StorageConfig.from_env("filesystem"))
    try:
        return operation(spark)
    finally:
        spark.stop()


def execute_stage(
    stage: str,
    period: ProcessingPeriod,
    taxi_type: str = "yellow",
    storage_backend: str = "filesystem",
    paths: PipelinePaths | None = None,
) -> dict[str, object]:
    """Execute exactly one existing pipeline stage and return small JSON-safe metrics."""
    if taxi_type != "yellow":
        raise ValueError(f"Unsupported taxi type: {taxi_type}")
    if storage_backend not in {"filesystem", "iceberg"}:
        raise ValueError(f"Unsupported storage backend: {storage_backend}")
    active = paths or PipelinePaths()
    request = TaxiDataRequest(taxi_type, period.year, period.month)
    silver_request = SilverRequest(taxi_type, period.year, period.month)
    gold_request = GoldRequest(taxi_type, period.year, period.month)
    LOGGER.info("Airflow stage=%s period=%s taxi_type=%s backend=%s", stage, period.identifier,
                taxi_type, storage_backend)

    if stage == "ingest_raw":
        result = ingest_taxi_data(request, active.raw_dir)
        return {"raw_file_size_bytes": result.file_size_bytes, "skipped": result.skipped}
    if stage == "validate_schema":
        report = validate_and_report(
            raw_data_path(request, active.raw_dir), period, taxi_type, active.state_dir
        )
        if report["compatibility"] == Compatibility.BREAKING:
            raise SchemaContractFailure(
                f"Breaking schema contract for {period.identifier}: {report['changes']}"
            )
        if report["compatibility"] == Compatibility.WARNING:
            LOGGER.warning("Schema contract warning for %s: %s", period.identifier,
                           report["changes"])
        return {"compatibility": str(report["compatibility"]),
                "actual_fingerprint": report["actual_fingerprint"],
                "change_count": len(report["changes"])}
    if stage == "bronze":
        def run_bronze(spark: Any) -> dict[str, object]:
            result = write_bronze_partition(
                spark, BronzeRequest(taxi_type, period.year, period.month),
                raw_dir=active.raw_dir, bronze_dir=active.bronze_dir,
            )
            return {"bronze_rows": result.bronze_rows}
        return _spark_stage(stage, run_bronze)
    if stage == "silver":
        def run_silver(spark: Any) -> dict[str, object]:
            result = process_silver_partition(
                spark, silver_request, bronze_dir=active.bronze_dir,
                silver_dir=active.silver_dir, quarantine_dir=active.quarantine_dir,
            )
            return {"valid_rows": result.valid_rows, "rejected_rows": result.rejected_rows}
        return _spark_stage(stage, run_silver)
    if stage == "gold":
        def run_gold(spark: Any) -> dict[str, object]:
            result = process_gold_partition(
                spark, gold_request, silver_dir=active.silver_dir, gold_dir=active.gold_dir
            )
            return {"input_rows": result.input_rows,
                    "datasets": {name: item["rows"] for name, item in result.datasets.items()}}
        return _spark_stage(stage, run_gold)
    if stage == "geographic":
        ingest_taxi_zones(reference_dir=active.reference_dir)
        def run_geographic(spark: Any) -> dict[str, object]:
            result = process_geographic_enrichment(
                spark, gold_request, gold_dir=active.gold_dir, reference_dir=active.reference_dir
            )
            return {"matched_locations": result.matched_location_ids,
                    "unmatched_locations": result.unmatched_location_ids,
                    "match_percentage": result.match_percentage}
        return _spark_stage(stage, run_geographic)
    if stage == "publish_iceberg":
        if storage_backend == "filesystem":
            return {"skipped": True}
        spark = create_spark_session(
            "nyc-taxi-airflow-publish", StorageConfig.from_env("iceberg")
        )
        try:
            snapshots = {}
            for dataset in TABLES:
                if dataset == "bronze":
                    path = bronze_partition_path(silver_request, active.bronze_dir)
                elif dataset == "silver":
                    path = silver_partition_path(silver_request, active.silver_dir)
                elif dataset == "quarantine":
                    path = quarantine_partition_path(silver_request, active.quarantine_dir)
                elif dataset in GOLD_DATASETS:
                    path = gold_partition_path(gold_request, active.gold_dir, dataset)
                else:
                    path = pickup_zone_partition_path(gold_request, active.gold_dir)
                result = publish_dataset(spark, dataset, path, taxi_type, period)
                snapshots[dataset] = result["snapshot_id"]
            return {"snapshots": snapshots}
        finally:
            spark.stop()
    if stage == "publish_serving":
        result = publish_period(
            ServingConfig.from_env(), period, taxi_type=taxi_type, gold_dir=active.gold_dir
        )
        return {"marts": {name: item.rows for name, item in result.marts.items()},
                "trip_count": next(iter(result.marts.values())).trip_count}
    if stage == "validate_reconciliation":
        validate_period_artifacts(period, taxi_type, active)
        spark = create_spark_session(
            "nyc-taxi-airflow-reconcile",
            StorageConfig.from_env("iceberg" if storage_backend == "iceberg" else "filesystem"),
        )
        try:
            sources = {
                "bronze": bronze_partition_path(silver_request, active.bronze_dir),
                "silver": silver_partition_path(silver_request, active.silver_dir),
                "quarantine": quarantine_partition_path(silver_request, active.quarantine_dir),
            }
            counts = {name: spark.read.parquet(str(path)).count() for name, path in sources.items()}
            if counts["bronze"] != counts["silver"] + counts["quarantine"]:
                raise ValueError(f"Silver reconciliation failed for {period.identifier}: {counts}")
            for dataset in (*GOLD_DATASETS, "pickup_zone_performance"):
                path = (gold_partition_path(gold_request, active.gold_dir, dataset)
                        if dataset in GOLD_DATASETS
                        else pickup_zone_partition_path(gold_request, active.gold_dir))
                represented = spark.read.parquet(str(path)).agg(F.sum("trip_count")).first()[0]
                if represented != counts["silver"]:
                    raise ValueError(f"Gold reconciliation failed for {dataset}: {represented}")
            if storage_backend == "iceberg":
                iceberg_counts = validate_published_period(spark, taxi_type, period)
                for name in counts:
                    if iceberg_counts[name] != counts[name]:
                        raise ValueError(f"Iceberg/local count mismatch for {name}")
            serving = validate_period(
                ServingConfig.from_env(), period, taxi_type=taxi_type, gold_dir=active.gold_dir
            )
            if any(item.trip_count != counts["silver"] for item in serving.values()):
                raise ValueError(f"Serving/Silver count mismatch for {period.identifier}")
            return {"bronze_rows": counts["bronze"], "valid_rows": counts["silver"],
                    "quarantine_rows": counts["quarantine"],
                    "gold_marts_reconciled": len(GOLD_DATASETS) + 1,
                    "serving_marts_reconciled": len(serving)}
        finally:
            spark.stop()
    raise ValueError(f"Unknown Airflow stage: {stage}")
