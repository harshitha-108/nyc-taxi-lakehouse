"""Build idempotent Gold analytics datasets from one valid Silver partition."""

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
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from nyc_taxi_lakehouse.bronze.processor import create_spark_session
from nyc_taxi_lakehouse.ingestion.nyc_taxi import InvalidRequestError, TaxiDataRequest
from nyc_taxi_lakehouse.silver.processor import silver_partition_path

LOGGER = logging.getLogger(__name__)

DAILY_TRIP_METRICS = "daily_trip_metrics"
HOURLY_DEMAND = "hourly_demand"
PICKUP_LOCATION_PERFORMANCE = "pickup_location_performance"
PAYMENT_TYPE_SUMMARY = "payment_type_summary"
GOLD_DATASETS = (
    DAILY_TRIP_METRICS,
    HOURLY_DEMAND,
    PICKUP_LOCATION_PERFORMANCE,
    PAYMENT_TYPE_SUMMARY,
)
GOLD_LINEAGE_COLUMNS = (
    "_gold_processed_at",
    "_source_taxi_type",
    "_source_year",
    "_source_month",
)
REQUIRED_SILVER_COLUMNS = {
    "tpep_pickup_datetime",
    "pickup_date",
    "trip_distance",
    "trip_duration_minutes",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "pickup_location_id",
    "payment_type",
    "_source_taxi_type",
    "_source_year",
    "_source_month",
}
GRAINS = {
    DAILY_TRIP_METRICS: ("pickup_date",),
    HOURLY_DEMAND: ("pickup_date", "pickup_hour"),
    PICKUP_LOCATION_PERFORMANCE: ("pickup_location_id",),
    PAYMENT_TYPE_SUMMARY: ("payment_type",),
}


class GoldError(Exception):
    """Raised when a Gold analytics partition cannot be completed safely."""


@dataclass(frozen=True)
class GoldRequest:
    """Identifies one valid Silver source period and its Gold output partitions."""

    taxi_type: str
    year: int
    month: int

    def __post_init__(self) -> None:
        TaxiDataRequest(taxi_type=self.taxi_type, year=self.year, month=self.month)


@dataclass(frozen=True)
class GoldDatasetResult:
    """Operational metadata for one completed Gold dataset partition."""

    name: str
    target_path: Path
    rows: int
    columns: int
    output_file_count: int
    output_size_bytes: int


@dataclass(frozen=True)
class GoldResult:
    """Operational metadata for a completed set of Gold datasets."""

    silver_path: Path
    input_rows: int
    input_total_revenue: Decimal
    datasets: dict[str, GoldDatasetResult]
    manifest_path: Path | None
    elapsed_seconds: float


def gold_partition_path(request: GoldRequest, gold_dir: Path, dataset_name: str) -> Path:
    """Return the Hive-style target directory for one Gold dataset and source period."""
    if dataset_name not in GOLD_DATASETS:
        raise GoldError(f"Unsupported Gold dataset: {dataset_name}")
    return (
        gold_dir
        / dataset_name
        / request.taxi_type
        / f"year={request.year}"
        / f"month={request.month:02d}"
    )


def _with_gold_lineage(
    dataframe: DataFrame,
    request: GoldRequest,
    processed_at: datetime,
) -> DataFrame:
    """Attach period-level lineage appropriate for aggregated Gold rows."""
    if processed_at.tzinfo is None:
        raise GoldError("Gold processing timestamp must be timezone-aware.")
    return (
        dataframe.withColumn("_gold_processed_at", F.lit(processed_at).cast(TimestampType()))
        .withColumn("_source_taxi_type", F.lit(request.taxi_type))
        .withColumn("_source_year", F.lit(request.year))
        .withColumn("_source_month", F.lit(request.month))
    )


def _standard_metrics() -> list[F.Column]:
    """Return common non-rounded aggregate metric expressions."""
    return [
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("fare_amount").alias("average_fare_amount"),
        F.avg("total_amount").alias("average_total_amount"),
        F.avg("trip_distance").alias("average_trip_distance"),
        F.avg("trip_duration_minutes").alias("average_trip_duration_minutes"),
        F.sum("trip_distance").alias("total_trip_distance"),
        F.avg("tip_amount").alias("average_tip_amount"),
        F.sum("tip_amount").alias("total_tip_amount"),
    ]


def build_daily_trip_metrics(
    silver_dataframe: DataFrame,
    request: GoldRequest,
    processed_at: datetime,
) -> DataFrame:
    """Aggregate trips to one row per pickup date."""
    return _with_gold_lineage(
        silver_dataframe.groupBy("pickup_date").agg(*_standard_metrics()), request, processed_at
    )


def build_hourly_demand(
    silver_dataframe: DataFrame,
    request: GoldRequest,
    processed_at: datetime,
) -> DataFrame:
    """Aggregate demand to one row per pickup date and pickup hour."""
    metrics = [
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("trip_distance").alias("average_trip_distance"),
        F.avg("trip_duration_minutes").alias("average_trip_duration_minutes"),
    ]
    with_hour = silver_dataframe.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    return _with_gold_lineage(
        with_hour.groupBy("pickup_date", "pickup_hour").agg(*metrics), request, processed_at
    )


def build_pickup_location_performance(
    silver_dataframe: DataFrame,
    request: GoldRequest,
    processed_at: datetime,
) -> DataFrame:
    """Aggregate trip and total-charge metrics to one row per pickup location ID."""
    metrics = [
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("average_revenue_per_trip"),
        F.avg("trip_distance").alias("average_trip_distance"),
        F.avg("trip_duration_minutes").alias("average_trip_duration_minutes"),
        F.sum("tip_amount").alias("total_tip_amount"),
    ]
    return _with_gold_lineage(
        silver_dataframe.groupBy("pickup_location_id").agg(*metrics), request, processed_at
    )


def build_payment_type_summary(
    silver_dataframe: DataFrame,
    request: GoldRequest,
    processed_at: datetime,
    *,
    input_rows: int,
    input_total_revenue: Decimal,
) -> DataFrame:
    """Aggregate trip and total-charge distribution to one row per payment type."""
    metrics = [
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("average_total_amount"),
        F.avg("tip_amount").alias("average_tip_amount"),
        F.sum("tip_amount").alias("total_tip_amount"),
    ]
    summary = silver_dataframe.groupBy("payment_type").agg(*metrics)
    trip_percentage = (F.col("trip_count") / F.lit(input_rows) * F.lit(100.0)).cast("double")
    if input_total_revenue == 0:
        revenue_percentage = F.lit(None).cast("double")
    else:
        revenue_percentage = (
            F.col("total_revenue") / F.lit(input_total_revenue) * F.lit(100.0)
        ).cast("double")
    return _with_gold_lineage(
        summary.withColumn("trip_percentage", trip_percentage).withColumn(
            "revenue_percentage", revenue_percentage
        ),
        request,
        processed_at,
    )


def build_gold_datasets(
    silver_dataframe: DataFrame,
    request: GoldRequest,
    processed_at: datetime,
    *,
    input_rows: int,
    input_total_revenue: Decimal,
) -> dict[str, DataFrame]:
    """Build the complete Gold mart set without writing output."""
    missing_columns = REQUIRED_SILVER_COLUMNS.difference(silver_dataframe.columns)
    if missing_columns:
        raise GoldError(f"Silver input is missing required columns: {sorted(missing_columns)}")
    return {
        DAILY_TRIP_METRICS: build_daily_trip_metrics(silver_dataframe, request, processed_at),
        HOURLY_DEMAND: build_hourly_demand(silver_dataframe, request, processed_at),
        PICKUP_LOCATION_PERFORMANCE: build_pickup_location_performance(
            silver_dataframe, request, processed_at
        ),
        PAYMENT_TYPE_SUMMARY: build_payment_type_summary(
            silver_dataframe,
            request,
            processed_at,
            input_rows=input_rows,
            input_total_revenue=input_total_revenue,
        ),
    }


def _temporary_path(target_path: Path, label: str) -> Path:
    return target_path.with_name(f"{target_path.name}.__{label}__{uuid.uuid4().hex}")


def _remove_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _promote_partitions(outputs: list[tuple[Path, Path]]) -> None:
    """Promote all Gold outputs together and restore old partitions if promotion fails."""
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
        raise GoldError("Unable to promote Gold partitions safely.") from exc
    else:
        for _, backup_path in backups:
            _remove_directory(backup_path)


def _validate_dataset(
    dataset_name: str,
    dataframe: DataFrame,
    *,
    input_rows: int,
) -> tuple[int, int]:
    """Validate a written Gold dataset's schema, uniqueness, and trip-count reconciliation."""
    required_columns = {*GRAINS[dataset_name], "trip_count", *GOLD_LINEAGE_COLUMNS}
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise GoldError(f"{dataset_name} is missing columns: {sorted(missing_columns)}")

    row_count = dataframe.count()
    if row_count == 0:
        raise GoldError(f"{dataset_name} output is empty.")
    duplicate_grains = (
        dataframe.groupBy(*GRAINS[dataset_name])
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_grains:
        raise GoldError(f"{dataset_name} contains duplicate aggregation grains.")
    represented_trips = dataframe.agg(F.sum("trip_count").alias("represented_trips")).first()[
        "represented_trips"
    ]
    if int(represented_trips or 0) != input_rows:
        raise GoldError(
            f"{dataset_name} trip-count reconciliation failed: "
            f"expected={input_rows}, actual={represented_trips}."
        )
    return row_count, len(dataframe.columns)


def _validate_payment_percentages(dataframe: DataFrame) -> None:
    """Confirm payment percentages sum to 100 within floating-point tolerance."""
    percentages = dataframe.agg(
        F.sum("trip_percentage").alias("trip_percentage"),
        F.sum("revenue_percentage").alias("revenue_percentage"),
    ).first()
    for metric_name in ("trip_percentage", "revenue_percentage"):
        value = percentages[metric_name]
        if value is not None and abs(float(value) - 100.0) > 1e-9:
            raise GoldError(f"Payment {metric_name} does not reconcile to 100 percent: {value}.")


def _write_manifest(path: Path, result: GoldResult) -> Path | None:
    """Write an atomic, ignored runtime manifest after successful data promotion."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".json.part")
    payload = asdict(result)
    payload["silver_path"] = str(result.silver_path)
    payload["manifest_path"] = str(path)
    payload["input_total_revenue"] = str(result.input_total_revenue)
    for dataset in payload["datasets"].values():
        dataset["target_path"] = str(dataset["target_path"])
    try:
        with temporary_path.open("w", encoding="utf-8") as manifest_file:
            json.dump(payload, manifest_file, indent=2, sort_keys=True)
            manifest_file.write("\n")
        os.replace(temporary_path, path)
    except OSError as exc:
        temporary_path.unlink(missing_ok=True)
        LOGGER.warning("Could not write Gold runtime manifest %s: %s", path, exc)
        return None
    return path


def process_gold_partition(
    spark: SparkSession,
    request: GoldRequest,
    *,
    silver_dir: Path = Path("data/silver"),
    gold_dir: Path = Path("data/gold"),
) -> GoldResult:
    """Build, validate, and safely replace all Gold outputs for one source month."""
    started_at = time.monotonic()
    input_path = silver_partition_path(request, silver_dir)
    if not input_path.is_dir():
        raise GoldError(f"Valid Silver partition does not exist: {input_path}")

    LOGGER.info(
        "Gold job started: taxi_type=%s year=%s month=%02d input=%s",
        request.taxi_type,
        request.year,
        request.month,
        input_path,
    )
    silver_dataframe = spark.read.parquet(str(input_path))
    temporary_paths: dict[str, Path] = {}
    try:
        input_metrics = silver_dataframe.agg(
            F.count("*").alias("input_rows"), F.sum("total_amount").alias("input_total_revenue")
        ).first()
        input_rows = int(input_metrics["input_rows"])
        input_total_revenue = input_metrics["input_total_revenue"]
        if input_rows == 0 or input_total_revenue is None:
            raise GoldError("Silver input must contain rows and a non-null total amount sum.")

        processed_at = datetime.now(UTC)
        datasets = build_gold_datasets(
            silver_dataframe,
            request,
            processed_at,
            input_rows=input_rows,
            input_total_revenue=input_total_revenue,
        )
        readback_dataframes: dict[str, DataFrame] = {}
        for dataset_name, dataframe in datasets.items():
            target_path = gold_partition_path(request, gold_dir, dataset_name)
            temporary_path = _temporary_path(target_path, "temporary")
            _remove_directory(temporary_path)
            temporary_paths[dataset_name] = temporary_path
            dataframe.coalesce(1).write.mode("overwrite").parquet(str(temporary_path))
            readback_dataframes[dataset_name] = spark.read.parquet(str(temporary_path))

        dataset_results: dict[str, GoldDatasetResult] = {}
        for dataset_name, dataframe in readback_dataframes.items():
            row_count, column_count = _validate_dataset(
                dataset_name, dataframe, input_rows=input_rows
            )
            if dataset_name == PAYMENT_TYPE_SUMMARY:
                _validate_payment_percentages(dataframe)
            target_path = gold_partition_path(request, gold_dir, dataset_name)
            dataset_results[dataset_name] = GoldDatasetResult(
                name=dataset_name,
                target_path=target_path,
                rows=row_count,
                columns=column_count,
                output_file_count=0,
                output_size_bytes=0,
            )
            LOGGER.info(
                "Gold dataset validated: dataset=%s rows=%s columns=%s output=%s",
                dataset_name,
                row_count,
                column_count,
                target_path,
            )

        _promote_partitions(
            [
                (
                    temporary_paths[dataset_name],
                    gold_partition_path(request, gold_dir, dataset_name),
                )
                for dataset_name in GOLD_DATASETS
            ]
        )
    except Exception:
        for temporary_path in temporary_paths.values():
            _remove_directory(temporary_path)
        raise
    completed_datasets: dict[str, GoldDatasetResult] = {}
    for dataset_name, result in dataset_results.items():
        output_files = list(result.target_path.glob("part-*.parquet"))
        completed_datasets[dataset_name] = GoldDatasetResult(
            **{
                **asdict(result),
                "output_file_count": len(output_files),
                "output_size_bytes": sum(path.stat().st_size for path in output_files),
            }
        )
    elapsed_seconds = time.monotonic() - started_at
    result = GoldResult(
        silver_path=input_path,
        input_rows=input_rows,
        input_total_revenue=input_total_revenue,
        datasets=completed_datasets,
        manifest_path=None,
        elapsed_seconds=elapsed_seconds,
    )
    manifest_path = (
        gold_dir
        / "_manifests"
        / request.taxi_type
        / f"year={request.year}"
        / f"month={request.month:02d}.json"
    )
    written_manifest_path = _write_manifest(manifest_path, result)
    result = GoldResult(**{**asdict(result), "manifest_path": written_manifest_path})
    LOGGER.info(
        "Gold job succeeded: input_rows=%s datasets=%s elapsed_seconds=%.2f",
        result.input_rows,
        len(result.datasets),
        result.elapsed_seconds,
    )
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build complete NYC Taxi Gold analytics datasets from valid Silver data."
    )
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    parser.add_argument("--silver-dir", type=Path, default=Path("data/silver"))
    parser.add_argument("--gold-dir", type=Path, default=Path("data/gold"))
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser.parse_args()


def main() -> int:
    """Run the Gold command-line interface."""
    args = _parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        spark = create_spark_session("nyc-taxi-gold")
        try:
            process_gold_partition(
                spark,
                GoldRequest(args.taxi_type, args.year, args.month),
                silver_dir=args.silver_dir,
                gold_dir=args.gold_dir,
            )
        finally:
            spark.stop()
    except (GoldError, InvalidRequestError) as exc:
        LOGGER.error("Gold job failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
