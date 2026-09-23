"""Small real MinIO/Iceberg integration smoke test."""

from __future__ import annotations

import logging
import uuid

from nyc_taxi_lakehouse.orchestration.state import ProcessingPeriod
from nyc_taxi_lakehouse.storage.config import StorageConfig
from nyc_taxi_lakehouse.storage.iceberg import latest_snapshot, period_filter, replace_period
from nyc_taxi_lakehouse.storage.spark import create_spark_session

LOGGER = logging.getLogger(__name__)


def main() -> int:
    """Prove a real isolated write, read, replacement and prior-snapshot query."""
    logging.basicConfig(level=logging.INFO)
    namespace = f"lakehouse.smoke_{uuid.uuid4().hex[:12]}"
    spark = create_spark_session("nyc-taxi-iceberg-smoke", StorageConfig.from_env("iceberg"))
    table = f"{namespace}.bronze_trips"
    try:
        period = ProcessingPeriod(2024, 1)
        data = [(1, "yellow", 2024, 1), (2, "yellow", 2024, 1)]
        frame = spark.createDataFrame(
            data, "trip_id long, _source_taxi_type string, _source_year int, _source_month int"
        )
        rows, first = replace_period(spark, "bronze", frame, "yellow", period, namespace=namespace)
        assert rows == 2
        assert [(field.name, field.dataType) for field in spark.table(table).schema] == [
            (field.name, field.dataType) for field in frame.schema
        ]
        other = spark.createDataFrame(
            [(3, "yellow", 2024, 2)], frame.schema
        )
        replace_period(
            spark, "bronze", other, "yellow", ProcessingPeriod(2024, 2), namespace=namespace
        )
        rows, replay = replace_period(spark, "bronze", frame, "yellow", period, namespace=namespace)
        assert rows == 2 and replay.snapshot_id != first.snapshot_id
        assert replay.operation == "overwrite"
        assert spark.table(table).count() == 3
        assert (
            spark.table(table)
            .filter(period_filter("yellow", ProcessingPeriod(2024, 2)))
            .count()
            == 1
        )
        historical = spark.read.option("snapshot-id", first.snapshot_id).table(table).count()
        assert historical == 2
        LOGGER.info(
            "Smoke PASS: table=%s rows=%s first_snapshot=%s replay_snapshot=%s "
            "historical_rows=%s",
            table, spark.table(table).count(), first.snapshot_id, replay.snapshot_id, historical,
        )
        assert latest_snapshot(spark, table) is not None
        return 0
    finally:
        if spark.catalog.tableExists(table):
            spark.sql(f"DROP TABLE {table} PURGE")
            spark.sql(f"DROP NAMESPACE {namespace}")
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
