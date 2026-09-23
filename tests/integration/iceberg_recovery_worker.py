"""Isolated JVM worker for the S3A outage/recovery integration test."""

from __future__ import annotations

import json
import sys
from dataclasses import replace
from pathlib import Path

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.iceberg import (
    latest_snapshot,
    period_filter,
    replace_period,
    table_name,
)
from nyc_taxi_lakehouse.storage.spark import create_spark_session


def main() -> None:
    mode, catalog, namespace, *extra = sys.argv[1:]
    config = replace(StorageConfig.from_env("iceberg"), catalog_path=Path(catalog))
    if mode == "outage":
        config = replace(config, endpoint=extra[0])
    spark = create_spark_session(f"reliability-{mode}", config)
    table = table_name("bronze", namespace)
    january, february = ProcessingPeriod(2024, 1), ProcessingPeriod(2024, 2)
    schema = "trip_id long, _source_taxi_type string, _source_year int, _source_month int"
    try:
        first = spark.createDataFrame([(1, "yellow", 2024, 1)], schema)
        replacement = spark.createDataFrame([(2, "yellow", 2024, 1)], schema)
        other = spark.createDataFrame([(3, "yellow", 2024, 2)], schema)
        if mode == "baseline":
            replace_period(spark, "bronze", first, "yellow", january, namespace=namespace)
            replace_period(spark, "bronze", other, "yellow", february, namespace=namespace)
            result = {"snapshot_id": latest_snapshot(spark, table).snapshot_id}
        elif mode == "outage":
            hadoop = spark.sparkContext._jsc.hadoopConfiguration()
            for key, value in {
                "fs.s3a.attempts.maximum": "2", "fs.s3a.retry.limit": "1",
                "fs.s3a.connection.establish.timeout": "1000",
                "fs.s3a.connection.timeout": "1000",
            }.items():
                hadoop.set(key, value)
            try:
                replace_period(spark, "bronze", replacement, "yellow", january,
                               namespace=namespace)
            except Exception as exc:
                message = str(exc)
                result = {"failed": True, "error_type": type(exc).__name__,
                          "storage_error": "minio:1" in message or "Connection refused" in message}
            else:
                result = {"failed": False, "error_type": None, "storage_error": False}
        elif mode == "recover":
            prior = latest_snapshot(spark, table)
            preserved = prior is not None and prior.snapshot_id == int(extra[0])
            _, new_snapshot = replace_period(spark, "bronze", replacement, "yellow", january,
                                             namespace=namespace)
            replace_period(spark, "bronze", replacement, "yellow", january,
                           namespace=namespace)
            result = {"prior_snapshot_preserved": preserved,
                      "new_snapshot_id": new_snapshot.snapshot_id,
                      "retry_rows": spark.table(table).filter(
                          period_filter("yellow", january)).count(),
                      "historical_rows": spark.read.option(
                          "snapshot-id", int(extra[0])).table(table).count()}
        elif mode == "cleanup":
            if spark.catalog.tableExists(table):
                spark.sql(f"DROP TABLE {table} PURGE")
                spark.sql(f"DROP NAMESPACE {namespace}")
            result = {"cleaned": True}
        else:
            raise ValueError(f"Unknown worker mode: {mode}")
        if mode in {"baseline", "recover"}:
            result["january_rows"] = spark.table(table).filter(
                period_filter("yellow", january)).count()
            result["february_rows"] = spark.table(table).filter(
                period_filter("yellow", february)).count()
        print(json.dumps(result))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
