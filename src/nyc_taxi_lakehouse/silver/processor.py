"""Idempotent Silver processing with valid and quarantine outputs."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType

from nyc_taxi_lakehouse.bronze.processor import LINEAGE_COLUMNS, create_spark_session
from nyc_taxi_lakehouse.ingestion.nyc_taxi import InvalidRequestError, TaxiDataRequest

LOGGER = logging.getLogger(__name__)
QUALITY_REASONS = (
    "MISSING_PICKUP_TIMESTAMP",
    "MISSING_DROPOFF_TIMESTAMP",
    "INVALID_TIMESTAMP_ORDER",
    "NEGATIVE_TRIP_DISTANCE",
    "NEGATIVE_FARE_AMOUNT",
    "NEGATIVE_TOTAL_AMOUNT",
    "INVALID_PICKUP_LOCATION",
    "INVALID_DROPOFF_LOCATION",
)
COLUMN_RENAMES = {
    "VendorID": "vendor_id",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "Airport_fee": "airport_fee",
}
MONEY_COLUMNS = (
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
)
SILVER_LINEAGE_COLUMN = "_silver_processed_at"
QUALITY_REASONS_COLUMN = "_quality_failure_reasons"


class SilverError(Exception):
    """Raised when Silver processing cannot complete safely."""


@dataclass(frozen=True)
class SilverRequest:
    """Identifies the Bronze year/month partition to standardize."""

    taxi_type: str
    year: int
    month: int

    def __post_init__(self) -> None:
        TaxiDataRequest(taxi_type=self.taxi_type, year=self.year, month=self.month)


@dataclass(frozen=True)
class SilverResult:
    """Quality and operational metrics from one completed Silver partition run."""

    bronze_path: Path
    silver_path: Path
    quarantine_path: Path
    manifest_path: Path | None
    input_rows: int
    valid_rows: int
    rejected_rows: int
    valid_percentage: float
    rejected_percentage: float
    rule_failure_counts: dict[str, int]
    silver_columns: int
    silver_file_count: int
    quarantine_file_count: int
    elapsed_seconds: float


def _partition_path(base_dir: Path, request: SilverRequest) -> Path:
    return base_dir / request.taxi_type / f"year={request.year}" / f"month={request.month:02d}"


def bronze_partition_path(request: SilverRequest, bronze_dir: Path) -> Path:
    """Return the input Bronze partition location."""
    return _partition_path(bronze_dir, request)


def silver_partition_path(request: SilverRequest, silver_dir: Path) -> Path:
    """Return the valid Silver partition location."""
    return _partition_path(silver_dir, request)


def quarantine_partition_path(request: SilverRequest, quarantine_dir: Path) -> Path:
    """Return the rejected-record Silver quarantine location."""
    return (
        quarantine_dir
        / "silver"
        / request.taxi_type
        / f"year={request.year}"
        / f"month={request.month:02d}"
    )


def standardize_bronze_columns(bronze_dataframe: DataFrame) -> DataFrame:
    """Apply the documented canonical names and exact decimal monetary types."""
    expressions = []
    for column_name in bronze_dataframe.columns:
        canonical_name = COLUMN_RENAMES.get(column_name, column_name)
        expression = F.col(column_name)
        if canonical_name in MONEY_COLUMNS:
            expression = expression.cast(DecimalType(14, 2))
        expressions.append(expression.alias(canonical_name))
    return bronze_dataframe.select(*expressions)


def classify_records(
    bronze_dataframe: DataFrame,
    processed_at: datetime,
) -> DataFrame:
    """Standardize Bronze records and attach all applicable quality-failure reasons."""
    if processed_at.tzinfo is None:
        raise SilverError("Silver processing timestamp must be timezone-aware.")
    standardized = standardize_bronze_columns(bronze_dataframe)
    required_columns = {
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "pickup_location_id",
        "dropoff_location_id",
        *LINEAGE_COLUMNS,
    }
    missing_columns = required_columns.difference(standardized.columns)
    if missing_columns:
        raise SilverError(f"Bronze input is missing required columns: {sorted(missing_columns)}")

    reasons = F.array(
        F.when(F.col("tpep_pickup_datetime").isNull(), "MISSING_PICKUP_TIMESTAMP"),
        F.when(F.col("tpep_dropoff_datetime").isNull(), "MISSING_DROPOFF_TIMESTAMP"),
        F.when(
            F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
            "INVALID_TIMESTAMP_ORDER",
        ),
        F.when(F.col("trip_distance") < 0, "NEGATIVE_TRIP_DISTANCE"),
        F.when(F.col("fare_amount") < 0, "NEGATIVE_FARE_AMOUNT"),
        F.when(F.col("total_amount") < 0, "NEGATIVE_TOTAL_AMOUNT"),
        F.when(
            F.col("pickup_location_id").isNull() | (F.col("pickup_location_id") <= 0),
            "INVALID_PICKUP_LOCATION",
        ),
        F.when(
            F.col("dropoff_location_id").isNull() | (F.col("dropoff_location_id") <= 0),
            "INVALID_DROPOFF_LOCATION",
        ),
    )
    return (
        standardized.withColumn(
            "trip_duration_minutes",
            F.round(
                F.expr(
                    "timestampdiff(SECOND, tpep_pickup_datetime, "
                    "tpep_dropoff_datetime) / 60.0"
                ),
                2,
            ).cast(DecimalType(12, 2)),
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn(SILVER_LINEAGE_COLUMN, F.lit(processed_at).cast(TimestampType()))
        .withColumn(QUALITY_REASONS_COLUMN, F.filter(reasons, lambda reason: reason.isNotNull()))
    )


def calculate_quality_metrics(
    classified_dataframe: DataFrame,
) -> tuple[int, int, int, dict[str, int]]:
    """Calculate reconciliation metrics in one aggregate action."""
    metrics = classified_dataframe.agg(
        F.count("*").alias("input_rows"),
        F.sum(F.when(F.size(QUALITY_REASONS_COLUMN) == 0, 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.size(QUALITY_REASONS_COLUMN) > 0, 1).otherwise(0)).alias("rejected_rows"),
        *[
            F.sum(
                F.when(F.array_contains(QUALITY_REASONS_COLUMN, reason), 1).otherwise(0)
            ).alias(reason)
            for reason in QUALITY_REASONS
        ],
    ).first()
    input_rows = int(metrics["input_rows"])
    valid_rows = int(metrics["valid_rows"] or 0)
    rejected_rows = int(metrics["rejected_rows"] or 0)
    failure_counts = {reason: int(metrics[reason] or 0) for reason in QUALITY_REASONS}
    if input_rows != valid_rows + rejected_rows:
        raise SilverError(
            "Quality reconciliation failed: input rows do not equal valid plus rejected rows."
        )
    return input_rows, valid_rows, rejected_rows, failure_counts


def _temporary_path(target_path: Path, label: str) -> Path:
    return target_path.with_name(f"{target_path.name}.__{label}__{uuid.uuid4().hex}")


def _remove_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _promote_partitions(outputs: list[tuple[Path, Path]]) -> None:
    """Promote valid and quarantine output together, restoring prior targets on failure."""
    backups: list[tuple[Path, Path]] = []
    promoted: list[Path] = []
    try:
        for _, target_path in outputs:
            if target_path.exists():
                backup_path = _temporary_path(target_path, "backup")
                target_path.replace(backup_path)
                backups.append((target_path, backup_path))
        for temporary_path, target_path in outputs:
            temporary_path.replace(target_path)
            promoted.append(target_path)
    except OSError as exc:
        for target_path in promoted:
            _remove_directory(target_path)
        for target_path, backup_path in backups:
            if backup_path.exists():
                backup_path.replace(target_path)
        raise SilverError("Unable to promote Silver and quarantine partitions safely.") from exc
    else:
        for _, backup_path in backups:
            _remove_directory(backup_path)


def _write_manifest(path: Path, result: SilverResult) -> Path | None:
    """Write an atomic, ignored runtime manifest without making data promotion depend on it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".json.part")
    payload = asdict(result)
    for key in ("bronze_path", "silver_path", "quarantine_path", "manifest_path"):
        if payload[key] is not None:
            payload[key] = str(payload[key])
    try:
        with temporary_path.open("w", encoding="utf-8") as manifest_file:
            json.dump(payload, manifest_file, indent=2, sort_keys=True)
            manifest_file.write("\n")
        os.replace(temporary_path, path)
    except OSError as exc:
        temporary_path.unlink(missing_ok=True)
        LOGGER.warning("Could not write Silver runtime manifest %s: %s", path, exc)
        return None
    return path


def process_silver_partition(
    spark: SparkSession,
    request: SilverRequest,
    *,
    bronze_dir: Path = Path("data/bronze"),
    silver_dir: Path = Path("data/silver"),
    quarantine_dir: Path = Path("data/quarantine"),
) -> SilverResult:
    """Standardize, validate, and safely replace one Silver and quarantine partition pair."""
    started_at = time.monotonic()
    input_path = bronze_partition_path(request, bronze_dir)
    valid_path = silver_partition_path(request, silver_dir)
    rejected_path = quarantine_partition_path(request, quarantine_dir)
    if not input_path.is_dir():
        raise SilverError(f"Bronze partition does not exist: {input_path}")

    LOGGER.info(
        "Silver job started: taxi_type=%s year=%s month=%02d input=%s",
        request.taxi_type,
        request.year,
        request.month,
        input_path,
    )
    processed_at = datetime.now(UTC)
    classified = classify_records(spark.read.parquet(str(input_path)), processed_at).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    temporary_valid_path = _temporary_path(valid_path, "temporary")
    temporary_rejected_path = _temporary_path(rejected_path, "temporary")
    _remove_directory(temporary_valid_path)
    _remove_directory(temporary_rejected_path)
    try:
        input_rows, valid_rows, rejected_rows, failure_counts = (
            calculate_quality_metrics(classified)
        )
        valid_dataframe = classified.filter(F.size(QUALITY_REASONS_COLUMN) == 0).drop(
            QUALITY_REASONS_COLUMN
        )
        rejected_dataframe = classified.filter(F.size(QUALITY_REASONS_COLUMN) > 0)
        valid_dataframe.coalesce(1).write.mode("overwrite").parquet(str(temporary_valid_path))
        rejected_dataframe.coalesce(1).write.mode("overwrite").parquet(str(temporary_rejected_path))

        valid_readback = spark.read.parquet(str(temporary_valid_path))
        rejected_readback = spark.read.parquet(str(temporary_rejected_path))
        if valid_readback.count() != valid_rows or rejected_readback.count() != rejected_rows:
            raise SilverError("Read-back row counts do not match quality metrics.")
        if QUALITY_REASONS_COLUMN in valid_readback.columns:
            raise SilverError("Valid Silver output must not contain quality-failure reasons.")
        if QUALITY_REASONS_COLUMN not in rejected_readback.columns:
            raise SilverError("Quarantine output is missing quality-failure reasons.")
        if rejected_readback.filter(F.size(QUALITY_REASONS_COLUMN) == 0).count():
            raise SilverError("Quarantine output contains records without a failure reason.")
        _promote_partitions(
            [(temporary_valid_path, valid_path), (temporary_rejected_path, rejected_path)]
        )
    except Exception:
        _remove_directory(temporary_valid_path)
        _remove_directory(temporary_rejected_path)
        raise
    finally:
        classified.unpersist()

    elapsed_seconds = time.monotonic() - started_at
    result = SilverResult(
        bronze_path=input_path,
        silver_path=valid_path,
        quarantine_path=rejected_path,
        manifest_path=None,
        input_rows=input_rows,
        valid_rows=valid_rows,
        rejected_rows=rejected_rows,
        valid_percentage=valid_rows / input_rows * 100,
        rejected_percentage=rejected_rows / input_rows * 100,
        rule_failure_counts=failure_counts,
        silver_columns=len(valid_readback.columns),
        silver_file_count=len(list(valid_path.glob("part-*.parquet"))),
        quarantine_file_count=len(list(rejected_path.glob("part-*.parquet"))),
        elapsed_seconds=elapsed_seconds,
    )
    manifest_path = (
        silver_dir
        / "_manifests"
        / request.taxi_type
        / f"year={request.year}"
        / f"month={request.month:02d}.json"
    )
    written_manifest_path = _write_manifest(manifest_path, result)
    result = SilverResult(**{**asdict(result), "manifest_path": written_manifest_path})
    LOGGER.info(
        "Silver job succeeded: input_rows=%s valid_rows=%s rejected_rows=%s elapsed_seconds=%.2f",
        result.input_rows,
        result.valid_rows,
        result.rejected_rows,
        result.elapsed_seconds,
    )
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create valid and quarantined NYC Taxi Silver partitions."
    )
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    parser.add_argument("--bronze-dir", type=Path, default=Path("data/bronze"))
    parser.add_argument("--silver-dir", type=Path, default=Path("data/silver"))
    parser.add_argument("--quarantine-dir", type=Path, default=Path("data/quarantine"))
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser.parse_args()


def main() -> int:
    """Run the Silver command-line interface."""
    args = _parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        spark = create_spark_session("nyc-taxi-silver")
        try:
            process_silver_partition(
                spark,
                SilverRequest(args.taxi_type, args.year, args.month),
                bronze_dir=args.bronze_dir,
                silver_dir=args.silver_dir,
                quarantine_dir=args.quarantine_dir,
            )
        finally:
            spark.stop()
    except (SilverError, InvalidRequestError) as exc:
        LOGGER.error("Silver job failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
