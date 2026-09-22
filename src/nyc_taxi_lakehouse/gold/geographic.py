"""Enrich the existing pickup-location Gold mart with official TLC Taxi Zone attributes."""

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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from nyc_taxi_lakehouse.bronze.processor import create_spark_session
from nyc_taxi_lakehouse.gold.processor import GoldError, GoldRequest, gold_partition_path
from nyc_taxi_lakehouse.reference.taxi_zones import load_taxi_zones, taxi_zone_csv_path

LOGGER = logging.getLogger(__name__)
PICKUP_ZONE_PERFORMANCE = "pickup_zone_performance"
GOLD_LINEAGE_COLUMNS = (
    "_gold_processed_at",
    "_source_taxi_type",
    "_source_year",
    "_source_month",
)


@dataclass(frozen=True)
class GeographicResult:
    """Operational metrics for one enriched pickup-zone Gold partition."""

    source_path: Path
    target_path: Path
    reference_path: Path
    source_rows: int
    output_rows: int
    distinct_location_ids: int
    matched_location_ids: int
    unmatched_location_ids: int
    match_percentage: float
    unmatched_trip_count: int
    columns: int
    output_file_count: int
    output_size_bytes: int
    manifest_path: Path | None
    elapsed_seconds: float


def pickup_zone_partition_path(request: GoldRequest, gold_dir: Path) -> Path:
    """Return the target location for the Phase 6 enriched Gold mart."""
    return (
        gold_dir
        / PICKUP_ZONE_PERFORMANCE
        / request.taxi_type
        / f"year={request.year}"
        / f"month={request.month:02d}"
    )


def enrich_with_taxi_zone(
    dataframe: DataFrame,
    taxi_zones: DataFrame,
    *,
    location_column: str = "pickup_location_id",
) -> DataFrame:
    """Left-enrich rows with a broadcasted, unique TLC location dimension without dropping facts."""
    required_reference_columns = {"location_id", "borough", "zone", "service_zone"}
    missing_reference_columns = required_reference_columns.difference(taxi_zones.columns)
    if location_column not in dataframe.columns:
        raise GoldError(f"Source dataframe does not contain location column: {location_column}")
    if missing_reference_columns:
        raise GoldError(
            f"Taxi Zone reference is missing columns: {sorted(missing_reference_columns)}"
        )
    if taxi_zones.groupBy("location_id").count().filter(F.col("count") > 1).limit(1).count():
        raise GoldError(
            "Taxi Zone reference must have unique location_id values before enrichment."
        )
    reference = taxi_zones.select(
        F.col("location_id").alias("_reference_location_id"), "borough", "zone", "service_zone"
    )
    return dataframe.join(
        F.broadcast(reference),
        F.col(location_column) == F.col("_reference_location_id"),
        "left",
    )


def _temporary_path(target_path: Path, label: str) -> Path:
    return target_path.with_name(f"{target_path.name}.__{label}__{uuid.uuid4().hex}")


def _remove_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _replace_partition(temporary_path: Path, target_path: Path) -> None:
    backup_path: Path | None = None
    try:
        if target_path.exists():
            backup_path = _temporary_path(target_path, "backup")
            target_path.replace(backup_path)
        temporary_path.replace(target_path)
    except OSError as exc:
        if backup_path is not None and backup_path.exists() and not target_path.exists():
            backup_path.replace(target_path)
        raise GoldError(f"Unable to promote enriched Gold partition: {target_path}") from exc
    else:
        if backup_path is not None:
            _remove_directory(backup_path)


def _write_manifest(path: Path, result: GeographicResult) -> Path | None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".json.part")
    payload = asdict(result)
    for key in ("source_path", "target_path", "reference_path", "manifest_path"):
        if payload[key] is not None:
            payload[key] = str(payload[key])
    try:
        temporary_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary_path, path)
    except OSError as exc:
        temporary_path.unlink(missing_ok=True)
        LOGGER.warning("Could not write geographic enrichment manifest %s: %s", path, exc)
        return None
    return path


def _validate_enriched_output(
    dataframe: DataFrame,
    *,
    source_rows: int,
    source_trip_count: int,
) -> tuple[int, int]:
    required_columns = {
        "pickup_location_id",
        "borough",
        "zone",
        "service_zone",
        "trip_count",
        *GOLD_LINEAGE_COLUMNS,
    }
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise GoldError(f"Enriched Gold output is missing columns: {sorted(missing_columns)}")
    output_rows = dataframe.count()
    if output_rows != source_rows:
        raise GoldError(
            f"Enrichment row count changed: source={source_rows}, output={output_rows}."
        )
    if dataframe.groupBy("pickup_location_id").count().filter(F.col("count") > 1).limit(1).count():
        raise GoldError("Enriched Gold output has duplicate pickup_location_id values.")
    output_trip_count = dataframe.agg(F.sum("trip_count").alias("trip_count")).first()["trip_count"]
    if int(output_trip_count or 0) != source_trip_count:
        raise GoldError(
            f"Enrichment trip-count reconciliation failed: expected={source_trip_count}, "
            f"actual={output_trip_count}."
        )
    return output_rows, len(dataframe.columns)


def process_geographic_enrichment(
    spark: SparkSession,
    request: GoldRequest,
    *,
    gold_dir: Path = Path("data/gold"),
    reference_dir: Path = Path("data/reference"),
) -> GeographicResult:
    """Enrich existing pickup-location performance safely with official Taxi Zone attributes."""
    started_at = time.monotonic()
    source_path = gold_partition_path(request, gold_dir, "pickup_location_performance")
    target_path = pickup_zone_partition_path(request, gold_dir)
    reference_path = taxi_zone_csv_path(reference_dir)
    if not source_path.is_dir():
        raise GoldError(f"Pickup-location Gold source does not exist: {source_path}")

    source = spark.read.parquet(str(source_path))
    source_metrics = source.agg(
        F.count("*").alias("source_rows"), F.sum("trip_count").alias("source_trip_count")
    ).first()
    source_rows = int(source_metrics["source_rows"])
    source_trip_count = int(source_metrics["source_trip_count"] or 0)
    zones = load_taxi_zones(spark, reference_path)
    enriched = enrich_with_taxi_zone(source, zones).persist()
    temporary_path = _temporary_path(target_path, "temporary")
    _remove_directory(temporary_path)
    try:
        monitoring = enriched.agg(
            F.countDistinct("pickup_location_id").alias("distinct_ids"),
            F.countDistinct(
                F.when(F.col("_reference_location_id").isNotNull(), F.col("pickup_location_id"))
            ).alias("matched_ids"),
            F.countDistinct(
                F.when(F.col("_reference_location_id").isNull(), F.col("pickup_location_id"))
            ).alias("unmatched_ids"),
            F.sum(
                F.when(F.col("_reference_location_id").isNull(), F.col("trip_count")).otherwise(0)
            ).alias("unmatched_trip_count"),
        ).first()
        processed_at = datetime.now(UTC)
        output = (
            enriched.drop("_reference_location_id")
            .drop("_gold_processed_at")
            .withColumn("_gold_processed_at", F.lit(processed_at).cast(TimestampType()))
        )
        output.coalesce(1).write.mode("overwrite").parquet(str(temporary_path))
        readback = spark.read.parquet(str(temporary_path))
        output_rows, columns = _validate_enriched_output(
            readback, source_rows=source_rows, source_trip_count=source_trip_count
        )
        _replace_partition(temporary_path, target_path)
    except Exception:
        _remove_directory(temporary_path)
        raise
    finally:
        enriched.unpersist()

    distinct_ids = int(monitoring["distinct_ids"] or 0)
    matched_ids = int(monitoring["matched_ids"] or 0)
    unmatched_ids = int(monitoring["unmatched_ids"] or 0)
    result = GeographicResult(
        source_path=source_path,
        target_path=target_path,
        reference_path=reference_path,
        source_rows=source_rows,
        output_rows=output_rows,
        distinct_location_ids=distinct_ids,
        matched_location_ids=matched_ids,
        unmatched_location_ids=unmatched_ids,
        match_percentage=(matched_ids / distinct_ids * 100) if distinct_ids else 0.0,
        unmatched_trip_count=int(monitoring["unmatched_trip_count"] or 0),
        columns=columns,
        output_file_count=len(list(target_path.glob("part-*.parquet"))),
        output_size_bytes=sum(path.stat().st_size for path in target_path.glob("part-*.parquet")),
        manifest_path=None,
        elapsed_seconds=time.monotonic() - started_at,
    )
    manifest_path = (
        gold_dir
        / "_manifests"
        / "geographic"
        / request.taxi_type
        / f"year={request.year}"
        / f"month={request.month:02d}.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    written_manifest_path = _write_manifest(manifest_path, result)
    result = GeographicResult(**{**asdict(result), "manifest_path": written_manifest_path})
    LOGGER.info(
        "Geographic enrichment succeeded: locations=%s matched=%s unmatched=%s "
        "elapsed_seconds=%.2f",
        result.distinct_location_ids,
        result.matched_location_ids,
        result.unmatched_location_ids,
        result.elapsed_seconds,
    )
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create enriched pickup-zone Gold performance data."
    )
    parser.add_argument("--taxi-type", default="yellow", choices=("yellow",))
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    parser.add_argument("--gold-dir", type=Path, default=Path("data/gold"))
    parser.add_argument("--reference-dir", type=Path, default=Path("data/reference"))
    parser.add_argument(
        "--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR")
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        spark = create_spark_session("nyc-taxi-geographic-enrichment")
        try:
            process_geographic_enrichment(
                spark,
                GoldRequest(args.taxi_type, args.year, args.month),
                gold_dir=args.gold_dir,
                reference_dir=args.reference_dir,
            )
        finally:
            spark.stop()
    except GoldError as exc:
        LOGGER.error("Geographic enrichment failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
