"""Validate optimized January Gold outputs in isolated downstream destinations.

The local historical Gold partitions, production Iceberg catalog and analytics
schema are read-only. This script writes a fresh local benchmark catalog and a
uniquely named PostgreSQL schema, then removes only that PostgreSQL schema.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import psycopg2
import pyarrow.dataset as ds
from psycopg2 import sql
from pyspark.sql import functions as F

from nyc_taxi_lakehouse.gold.geographic import (
    pickup_zone_partition_path,
    process_geographic_enrichment,
)
from nyc_taxi_lakehouse.gold.processor import GOLD_DATASETS, GoldRequest, gold_partition_path
from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.serving.config import ServingConfig
from nyc_taxi_lakehouse.serving.publisher import publish_period, validate_period
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.iceberg import period_filter, replace_period, table_name
from nyc_taxi_lakehouse.storage.spark import create_spark_session

ROOT = Path("data/state/benchmarks")
GOLD_ROOT = ROOT / "gold_production" / "run_3"
PERIOD = ProcessingPeriod(2024, 1)
REQUEST = GoldRequest("yellow", 2024, 1)
NAMESPACE = "lakehouse.phase14_validation"
EXPECTED_TRIPS = 2_927_000


def _geographic_digest(path: Path) -> str:
    """Compare geographic business values without the run-specific processing time."""
    rows = ds.dataset(str(path), format="parquet").to_table().to_pylist()
    rows = [{key: value for key, value in row.items() if key != "_gold_processed_at"}
            for row in rows]
    rows.sort(key=lambda row: row["pickup_location_id"])
    return hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()


def _validate_geographic_and_iceberg() -> dict[str, object]:
    config = replace(
        StorageConfig.from_env("iceberg"),
        catalog_path=ROOT / "phase14_validation_iceberg_catalog.db",
    )
    spark = create_spark_session("phase14-downstream", config)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        geographic = process_geographic_enrichment(spark, REQUEST, gold_dir=GOLD_ROOT)
        geo_digest = _geographic_digest(geographic.target_path)
        historical_geo_digest = _geographic_digest(
            pickup_zone_partition_path(REQUEST, Path("data/gold")))
        if geo_digest != historical_geo_digest:
            raise AssertionError("Optimized geographic business values changed")
        results = {}
        for dataset in (*GOLD_DATASETS, "pickup_zone_performance"):
            path = (pickup_zone_partition_path(REQUEST, GOLD_ROOT)
                    if dataset == "pickup_zone_performance"
                    else gold_partition_path(REQUEST, GOLD_ROOT, dataset))
            frame = spark.read.parquet(str(path))
            represented = int(frame.agg(F.sum("trip_count")).first()[0])
            if represented != EXPECTED_TRIPS:
                raise AssertionError(f"{dataset} represents {represented} trips")
            rows, snapshot = replace_period(
                spark, dataset, frame, "yellow", PERIOD, namespace=NAMESPACE,
            )
            published = spark.table(table_name(dataset, NAMESPACE)).filter(
                period_filter("yellow", PERIOD)
            ).count()
            if rows != published:
                raise AssertionError(f"{dataset} Iceberg read-back mismatch")
            replay_rows, replay_snapshot = replace_period(
                spark, dataset, frame, "yellow", PERIOD, namespace=NAMESPACE,
            )
            if replay_rows != rows or replay_snapshot.snapshot_id == snapshot.snapshot_id:
                raise AssertionError(f"{dataset} Iceberg replay failed")
            results[dataset] = {
                "local_rows": frame.count(),
                "iceberg_rows": published,
                "represented_trips": represented,
                "snapshot_id": snapshot.snapshot_id,
                "replay_snapshot_id": replay_snapshot.snapshot_id,
            }
        return {
            "geographic_business_digest_equal": True,
            "geographic_match_percentage": geographic.match_percentage,
            "geographic_unmatched_trip_count": geographic.unmatched_trip_count,
            "iceberg_catalog": str(config.catalog_path),
            "iceberg_namespace": NAMESPACE,
            "datasets": results,
        }
    finally:
        spark.stop()


def _validate_postgres() -> dict[str, object]:
    config = replace(ServingConfig.from_env(), schema=f"phase14_{uuid4().hex[:12]}")
    created = False
    try:
        with psycopg2.connect(**config.connect_kwargs()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(config.schema)))
        created = True
        publication = publish_period(config, PERIOD, gold_dir=GOLD_ROOT)
        validated = validate_period(config, PERIOD, gold_dir=GOLD_ROOT)
        results = {
            name: {"rows": item.rows, "trips": item.trip_count}
            for name, item in validated.items()
        }
        if len(results) != 5 or any(item["trips"] != EXPECTED_TRIPS for item in results.values()):
            raise AssertionError("Isolated serving mart reconciliation failed")
        publish_period(config, PERIOD, gold_dir=GOLD_ROOT)
        replayed = validate_period(config, PERIOD, gold_dir=GOLD_ROOT)
        if {name: (item.rows, item.trip_count) for name, item in replayed.items()} != {
            name: (item["rows"], item["trips"]) for name, item in results.items()
        }:
            raise AssertionError("Isolated serving replay changed row/trip counts")
        return {"schema": config.schema, "publish_seconds": publication.elapsed_seconds,
                "marts": results, "reconciliation": "PASS", "replay": "PASS"}
    finally:
        if created:
            with psycopg2.connect(**config.connect_kwargs()) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(
                        sql.Identifier(config.schema)))


def main() -> None:
    if not all(gold_partition_path(REQUEST, GOLD_ROOT, name).is_dir()
               for name in GOLD_DATASETS):
        raise FileNotFoundError("Production Gold benchmark output is incomplete")
    results = {"iceberg_and_geographic": _validate_geographic_and_iceberg(),
               "postgres": _validate_postgres()}
    ROOT.mkdir(parents=True, exist_ok=True)
    (ROOT / "downstream.json").write_text(
        json.dumps(results, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, sort_keys=True))


if __name__ == "__main__":
    main()
